/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2019, Joyent, Inc.
 */

use crate::agent::{AgentAssignmentState, Assignment as AgentAssignment};
use crate::config::Config;
use crate::error::{CrossbeamError, Error, InternalError, InternalErrorCode};
use crate::jobs::{
    Assignment, AssignmentId, AssignmentPayload, AssignmentState, ObjectId,
    StorageId, Task, TaskStatus,
};
use crate::picker::{self as mod_picker, SharkSource, StorageNode};
use crate::util;

use std::collections::HashMap;
use std::error::Error as _Error;
use std::io::ErrorKind;
use std::sync::{Arc, Mutex, RwLock};
use std::thread;
use std::time::Duration;

use crossbeam_channel as crossbeam;
use crossbeam_channel::TryRecvError;
use crossbeam_deque::{Injector, Steal};
use libmanta::moray::{MantaObject, MantaObjectShark};
use moray::objects::{Etag, MethodOptions};
use reqwest;
use slog::{o, Drain, Logger};
use std::borrow::Borrow;
use threadpool::ThreadPool;

// --- Diesel Stuff, TODO This should be refactored --- //

use diesel::prelude::*;

// TODO: move database stuff somewhere.
table! {
    use diesel::sql_types::{Text, Integer};
    use super::EvacuateObjectStatusMapping;
    evacuateobjects (id) {
        id -> Text,
        assignment_id -> Text,
        object -> Text,
        shard -> Integer,
        status -> EvacuateObjectStatusMapping,
    }
}

#[derive(Insertable, Queryable, Identifiable)]
#[table_name = "evacuateobjects"]
struct UpdateEvacuateObject<'a> {
    id: &'a str,
}

#[derive(Insertable, Queryable, Identifiable, AsChangeset, Debug, PartialEq)]
#[table_name = "evacuateobjects"]
pub struct EvacuateObjectDB {
    pub id: String,
    pub assignment_id: AssignmentId,
    pub object: String,
    pub shard: i32,
    pub status: EvacuateObjectStatus,
}

// --- END Diesel Stuff --- //

struct FiniMsg;

#[derive(Debug)]
pub struct SharkSpotterObject {
    pub shard: i32,
    pub object: MantaObject,
}

#[derive(Debug, Clone, PartialEq, DbEnum)]
pub enum EvacuateObjectStatus {
    Unprocessed,    // Default state
    Assigned,       // Object has been included in an assignment
    Skipped,        // Could not find a shark to put this object in. TODO: Why?
    PostProcessing, // Object has been moved, metadata update in progress
    Complete,       // Object moved, and metadata updated
                    // TODO: Failed,   // Failed to Evacuate Object ???
                    // TODO: Retrying, // Retrying a failed evacuate attempt
                    // TODO: A Status for being part of a submitted assignment?
}

impl Default for EvacuateObjectStatus {
    fn default() -> Self {
        EvacuateObjectStatus::Unprocessed
    }
}

/// Wrap a given MantaObject in another structure so that we can track it's
/// progress through the evacuation process.
#[derive(Debug, Default, Clone)]
pub struct EvacuateObject {
    pub assignment_id: AssignmentId,
    // UUID of assignment this object was most recently part of.
    pub id: ObjectId,        // MantaObject ObjectId
    pub object: MantaObject, // The MantaObject being rebalanced
    pub shard: i32,          // shard number of metadata object record
    pub status: EvacuateObjectStatus,
    // Status of the object in the evacuation job
}

impl EvacuateObject {
    fn new(ssobj: SharkSpotterObject) -> Self {
        Self {
            assignment_id: String::new(),
            id: ssobj.object.object_id.to_owned(),
            object: ssobj.object,
            shard: ssobj.shard,
            ..Default::default()
        }
    }
}

impl EvacuateObject {
    // TODO: ToSql for EvacuateObject MANTA-4474
    fn to_insertable(&self) -> Result<EvacuateObjectDB, Error> {
        Ok(EvacuateObjectDB {
            assignment_id: self.assignment_id.clone(),
            id: self.id.clone(),
            object: serde_json::to_string(&self.object)?,
            shard: self.shard,
            status: self.status.clone(),
        })
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum DestSharkStatus {
    Init,
    Assigned,
    Ready,
}

#[derive(Clone, Debug)]
pub struct EvacuateDestShark {
    pub shark: StorageNode,
    pub status: DestSharkStatus,
}

/// Evacuate a given shark
pub struct EvacuateJob {
    /// Hash of destination sharks that may change during the job execution.
    pub dest_shark_list: RwLock<HashMap<StorageId, EvacuateDestShark>>,

    /// Hash of in progress assignments.
    pub assignments: RwLock<HashMap<AssignmentId, Assignment>>,

    /// The shark to evacuate.
    pub from_shark: MantaObjectShark,

    /// The minimum available space for a shark to be considered a destination.
    pub min_avail_mb: Option<u64>,

    /// Maximum number of tasks to include in a single assignment.
    pub max_tasks_per_assignment: Option<u32>,

    /// SqliteConnection to local database.
    pub conn: Mutex<SqliteConnection>,

    /// Accumulator for total time spent on DB inserts. (test/dev)
    pub total_db_time: Mutex<u128>,
}

impl EvacuateJob {
    /// Create a new EvacauteJob instance.
    /// As part of this initialization also create a new SqliteConnection.
    pub fn new<S: Into<String>>(from_shark: S, db_url: &str) -> Self {
        let manta_storage_id = from_shark.into();
        let conn = SqliteConnection::establish(db_url)
            .unwrap_or_else(|_| panic!("Error connecting to {}", db_url));
        Self {
            min_avail_mb: Some(1000),
            max_tasks_per_assignment: Some(100),
            dest_shark_list: RwLock::new(HashMap::new()),
            assignments: RwLock::new(HashMap::new()),
            from_shark: MantaObjectShark {
                manta_storage_id,
                ..Default::default()
            },
            conn: Mutex::new(conn),
            total_db_time: Mutex::new(0),
        }
    }

    pub fn run(self, config: &Config) -> Result<(), Error> {
        // TODO: check if table exists first and if so issue warning.  We may
        // need to handle this a bit more gracefully in the future for
        // restarting jobs...

        let conn = self.conn.lock().expect("DB conn lock");
        conn.execute(r#"DROP TABLE evacuateobjects"#)
            .unwrap_or_else(|e| {
                debug!("Table doesn't exist: {}", e);
                0
            });

        conn.execute(
            r#"
                CREATE TABLE evacuateobjects(
                    id TEXT PRIMARY KEY,
                    assignment_id TEXT,
                    object TEXT,
                    shard Integer,
                    status TEXT CHECK(status IN ('unprocessed', 'assigned',
                    'skipped', 'post_processing', 'complete')) NOT NULL
                );
            "#,
        )?;

        drop(conn);

        // job_action will be shared between threads so create an Arc for it.
        let job_action = Arc::new(self);

        // get what the evacuate job needs from the config structure
        let domain = &config.domain_name;
        let min_shard = config.min_shard_num();
        let max_shard = config.max_shard_num();

        // TODO: How big should each channel be?
        // Set up channels for thread to communicate.
        let (obj_tx, obj_rx) = crossbeam::bounded(5);
        let (empty_assignment_tx, empty_assignment_rx) = crossbeam::bounded(5);
        let (full_assignment_tx, full_assignment_rx) = crossbeam::bounded(5);
        let (md_update_tx, md_update_rx) = crossbeam::bounded(5);
        let (checker_fini_tx, checker_fini_rx) = crossbeam::bounded(1);

        // TODO: lock evacuating server to readonly
        // TODO: add thread barriers MANTA-4457

        // start threads to process objects
        let sharkspotter_thread = start_sharkspotter(
            obj_tx,
            domain.as_str(),
            Arc::clone(&job_action),
            min_shard,
            max_shard,
        )?;

        let metadata_update_thread =
            start_metadata_update_broker(Arc::clone(&job_action), md_update_rx)
                .expect("start metadata updater thread");

        let assignment_checker_thread = start_assignment_checker(
            Arc::clone(&job_action),
            checker_fini_rx,
            md_update_tx,
        )
        .expect("start assignment checker thread");

        let post_thread =
            start_assignment_post(full_assignment_rx, Arc::clone(&job_action))?;

        let generator_thread = start_assignment_generator(
            obj_rx,
            empty_assignment_rx,
            full_assignment_tx,
            Arc::clone(&job_action),
        )?;

        // start picker thread which will periodically update the list of
        // available sharks.
        let mut picker = mod_picker::Picker::new();
        picker.start().map_err(Error::from)?;
        let picker = Arc::new(picker);

        let assignment_manager = start_assignment_manager(
            empty_assignment_tx,
            checker_fini_tx,
            Arc::clone(&job_action),
            Arc::clone(&picker),
        )?;

        // At this point the rebalance job is running and we are blocked at
        // the assignment_manager thread join.
        assignment_manager
            .join()
            .expect("Assignment Manager")
            .expect("Error joining assignment manager thread");

        picker.fini();

        sharkspotter_thread
            .join()
            .expect("Sharkspotter Thread")
            .unwrap_or_else(|e| {
                error!("Error joining sharkspotter handle: {}\n", e);
                std::process::exit(1);
            });

        generator_thread
            .join()
            .expect("Generator Thread")
            .expect("Error joining assignment generator thread");

        post_thread
            .join()
            .expect("Post Thread")
            .expect("Error joining assignment processor thread");

        assignment_checker_thread
            .join()
            .expect("Checker Thread")
            .expect("Error joining assignment checker thread");

        metadata_update_thread
            .join()
            .expect("MD Update Thread")
            .expect("Error joining metadata update thread");

        Ok(())
    }

    /// If a shark is in the Assigned state then it is busy.
    fn shark_busy(&self, shark: &StorageNode) -> bool {
        self.dest_shark_list
            .read()
            .expect("dest_shark_list read lock")
            .get(shark.manta_storage_id.as_str())
            .map_or(false, |eds| {
                debug!(
                    "shark '{}' status: {:?}",
                    shark.manta_storage_id, eds.status
                );
                eds.status == DestSharkStatus::Assigned
            })
    }

    /// Iterate over a new set of storage nodes and update our destination
    /// shark list accordingly.  This may need to change so that we update
    /// available_mb more judiciously (i.e. based on timestamp).
    fn update_dest_sharks(&self, new_sharks: &[StorageNode]) {
        let mut dest_shark_list = self
            .dest_shark_list
            .write()
            .expect("update dest_shark_list write lock");
        for sn in new_sharks.iter() {
            if let Some(dest_shark) =
                dest_shark_list.get_mut(sn.manta_storage_id.as_str())
            {
                if dest_shark.status == DestSharkStatus::Ready {
                    dest_shark.shark.available_mb = sn.available_mb;
                }
            } else {
                // create new dest shark and add it to the hash
                let new_shark = EvacuateDestShark {
                    shark: sn.to_owned(),
                    status: DestSharkStatus::Init,
                };
                debug!("Adding new destination shark {:?} ", new_shark);
                dest_shark_list.insert(sn.manta_storage_id.clone(), new_shark);
            }
        }

        // Walk the list of our destination sharks, if it doesn't exist in
        // new_sharks Vec then remove it from the hash.  Perhaps a pre-walk
        // of marking every dest_shark dirty and then a post walk
        // marking each found shark as clean, and removing all dirty sharks
        // would be more efficient.
        *dest_shark_list = dest_shark_list
            .iter()
            .filter(|&(ds, _)| {
                new_sharks.iter().any(|s| &s.manta_storage_id == ds)
            })
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
    }

    // TODO: Consider doing batched inserts: MANTA-4464.
    fn insert_into_db(&self, obj: &EvacuateObject) -> Result<usize, Error> {
        use self::evacuateobjects::dsl::*;

        let insertable = obj.to_insertable()?;
        let locked_conn = self.conn.lock().expect("DB conn lock");
        let now = std::time::Instant::now();

        // TODO: Is panic the right thing to do here?
        let ret = diesel::insert_into(evacuateobjects)
            .values(&insertable)
            .execute(&*locked_conn)
            .unwrap_or_else(|e| {
                let msg = format!("Error inserting object into DB: {}", e);
                error!("{}", msg);
                panic!(msg);
            });

        let mut total_time = self.total_db_time.lock().expect("DB time lock");
        *total_time += now.elapsed().as_millis();

        Ok(ret)
    }

    // Insert multiple EvacuateObjects into the database at once.
    fn insert_many_into_db<V>(&self, vec_objs: V) -> Result<usize, Error>
    where
        V: Borrow<Vec<EvacuateObject>>,
    {
        use self::evacuateobjects::dsl::*;

        let objs = vec_objs.borrow();
        let insertable_objs: Vec<EvacuateObjectDB> = objs
            .iter()
            .map(|o| o.to_insertable())
            .collect::<Result<Vec<_>, _>>()?;
        let locked_conn = self.conn.lock().expect("db conn lock");

        let now = std::time::Instant::now();
        let ret = diesel::insert_into(evacuateobjects)
            .values(insertable_objs)
            .execute(&*locked_conn)
            .unwrap_or_else(|e| {
                let msg = format!("Error inserting object into DB: {}", e);
                error!("{}", msg);
                panic!(msg);
            });
        let mut total_time = self.total_db_time.lock().expect("DB time lock");
        *total_time += now.elapsed().as_millis();

        Ok(ret)
    }

    /// Mark all objects with a given assignment ID with the specified
    /// EvacuateObjectStatus
    fn mark_assignment_objects(
        &self,
        id: &AssignmentId,
        to_status: EvacuateObjectStatus,
    ) -> usize {
        use self::evacuateobjects::dsl::{
            assignment_id, evacuateobjects, status,
        };

        let locked_conn = self.conn.lock().expect("DB conn lock");

        debug!("Marking objects in assignment ({}) as {:?}", id, to_status);
        diesel::update(evacuateobjects)
            .filter(assignment_id.eq(id))
            .set(status.eq(to_status))
            .execute(&*locked_conn)
            .unwrap_or_else(|e| {
                let msg = format!("Error updating assignment: {} ({})", id, e);
                error!("{}", msg);
                panic!(msg);
            })
    }

    fn mark_dest_shark_ready(&self, dest_shark: &StorageNode) {
        if let Some(shark) = self
            .dest_shark_list
            .write()
            .expect("dest_shark_list write")
            .get_mut(dest_shark.manta_storage_id.as_str())
        {
            debug!(
                "Updating shark '{}' to Ready state",
                dest_shark.manta_storage_id.as_str()
            );
            shark.status = DestSharkStatus::Ready;
        } else {
            warn!(
                "Could not find shark: '{}'",
                dest_shark.manta_storage_id.as_str()
            );
        }
    }

    fn get_assignment_objects(
        &self,
        id: &AssignmentId,
        status_filter: EvacuateObjectStatus,
    ) -> Vec<EvacuateObjectDB> {
        use self::evacuateobjects::dsl::{
            assignment_id, evacuateobjects, status,
        };

        let locked_conn = self.conn.lock().expect("DB conn");

        evacuateobjects
            .filter(assignment_id.eq(id))
            .filter(status.eq(status_filter))
            .load::<EvacuateObjectDB>(&*locked_conn)
            .expect("getting filtered objects")
    }
}

/// 1. Set AssignmentState to Assigned.
/// 2. Update assignment that has been successfully posted to the Agent into the
///    EvacauteJob's hash of assignments.
/// 3. Update shark available_mb.
fn assignment_post_success(
    job_action: &EvacuateJob,
    mut assignment: Assignment,
) -> Result<(), Error> {
    match job_action
        .dest_shark_list
        .write()
        .expect("desk_shark_list")
        .get_mut(&assignment.dest_shark.manta_storage_id)
    {
        Some(evac_dest_shark) => {
            if assignment.total_size > evac_dest_shark.shark.available_mb {
                warn!(
                    "Attempting to set available space on destination shark \
                     to a negative value.  Setting to 0 instead."
                );
            }

            evac_dest_shark.shark.available_mb -= assignment.total_size;
        }
        None => {
            // This could happen in the event that while this assignment was
            // being filled out by the assignment generator thread and being
            // posted to the agent, the assignment manager might have
            // received an updated list of sharks from the picker and as a
            // result removed this one from it's active hash.  Regardless the
            // assignment has been posted and is actively running on the
            // shark's rebalancer agent at this point.
            warn!(
                "Could not find destination shark ({}) to update available \
                 MB.",
                &assignment.dest_shark.manta_storage_id
            );
        }
    }

    // TODO: Consider dropping the tasks from the assignment at this point
    // since it is no longer used, and just taking up memory.
    assignment.state = AssignmentState::Assigned;
    let mut assignments = job_action
        .assignments
        .write()
        .expect("Assignments hash write lock");

    assignments.insert(assignment.id.clone(), assignment);

    Ok(())
}

impl PostAssignment for EvacuateJob {
    fn post(&self, assignment: Assignment) -> Result<(), Error> {
        let payload = AssignmentPayload {
            id: assignment.id.clone(),
            tasks: assignment.tasks.values().map(|t| t.to_owned()).collect(),
        };

        let client = reqwest::Client::new();
        let agent_uri = format!(
            "http://{}:7878/assignments",
            assignment.dest_shark.manta_storage_id
        );

        trace!("Sending {:#?} to {}", payload, agent_uri);
        let res = client.post(&agent_uri).json(&payload).send()?;

        if !res.status().is_success() {
            // TODO: how to handle errors?  Need to pick different agent?
            error!(
                "Error posting assignment {} to {} ({})",
                payload.id,
                assignment.dest_shark.manta_storage_id,
                res.status()
            );

            return Err(
                InternalError::new(None, "Error posting assignment").into()
            );
        }

        debug!("Post of {} was successful", payload.id);
        Ok(assignment_post_success(self, assignment).map(|_| ())?)
    }
}

impl GetAssignment for EvacuateJob {
    fn get(&self, assignment: &Assignment) -> Result<AgentAssignment, Error> {
        let uri = format!(
            "http://{}:7878/assignments/{}",
            assignment.dest_shark.manta_storage_id, assignment.id
        );

        debug!("Getting Assignment: {:?}", uri);
        let mut resp = reqwest::get(&uri)?;
        if !resp.status().is_success() {
            let msg =
                format!("Could not get assignment from Agent: {:#?}", resp);
            return Err(InternalError::new(
                Some(InternalErrorCode::AssignmentGetError),
                msg,
            )
            .into());
        }
        debug!("RET: {:#?}", resp);
        resp.json::<AgentAssignment>().map_err(Error::from)
    }
}

impl UpdateMetadata for EvacuateJob {
    fn update_object_shark(
        &self,
        mut object: MantaObject,
        new_shark: &StorageNode,
    ) -> Result<MantaObject, Error> {
        let old_shark = &self.from_shark;
        let obj = serde_json::to_string(&object)?;
        let etag = match util::crc_hex_str(&obj) {
            Some(o) => o,
            None => {
                return Err(InternalError::new(
                    None,
                    "Error getting etag from Manta Object",
                )
                .into())
            }
        };

        // Replace shark value
        let mut shark_found = false;
        for shark in object.sharks.iter_mut() {
            if shark.manta_storage_id == old_shark.manta_storage_id {
                shark.manta_storage_id = new_shark.manta_storage_id.clone();
                shark.datacenter = new_shark.datacenter.clone();
                if !shark_found {
                    shark_found = true;
                } else {
                    error!("Found duplicate shark");
                }
            }
        }

        // Prepare metadata message.
        let key = object.key.as_str();
        let value =
            serde_json::to_value(&object).expect("Serialize Manta Object");
        let mut opts = MethodOptions::default();

        opts.etag = Etag::Specified(etag);

        trace!(
            "Updating metadata. Key: {}\nValue: {}\nopts: {:?}",
            key,
            value,
            opts
        );

        // TODO: The EvacuateJob struct should implement a moray client hash
        // and we can call a "get_moray_client()" method here that will
        // get an existing client, or create a new client if need be.
        /*
        mclient.put_object("manta", key, value, &opts, |o| {
                debug!("Updated object metadata: {}", &o);
                Ok(())
            },
        ).expect("put_object");
        */
        Ok(object)
    }
}

impl ProcessAssignment for EvacuateJob {
    fn process(&self, agent_assignment: AgentAssignment) -> Result<(), Error> {
        let uuid = &agent_assignment.uuid;
        let mut assignments =
            self.assignments.write().expect("assignments read lock");

        // std::option::NoneError is still nightly-only experimental
        let assignment = match assignments.get_mut(uuid) {
            Some(a) => a,
            None => {
                let msg = format!(
                    "Error getting assignment.  Couldn't find \
                     assignment {} in {} assignments.",
                    uuid,
                    assignments.len()
                );

                error!("{}", &msg);

                return Err(InternalError::new(
                    Some(InternalErrorCode::AssignmentLookupError),
                    msg,
                )
                .into());
            }
        };

        // If for some reason this assignment is in the wrong state don't
        // update it.
        match assignment.state {
            AssignmentState::Assigned => (),
            _ => {
                warn!(
                    "Assignment in unexpected state '{:?}', skipping",
                    assignment.state
                );
                // TODO: this should never happen but should we panic?
                // If we create more threads to check for assignments or
                // process them this may be possible.
                panic!("Assignment in wrong state {:?}", assignment);
            }
        }

        debug!(
            "Checking agent assignment state: {:#?}",
            &agent_assignment.stats.state
        );

        match agent_assignment.stats.state {
            AgentAssignmentState::Scheduled | AgentAssignmentState::Running => {
                warn!(
                    "Trying to process an assignment that is Scheduled or \
                     Running: {}",
                    &uuid
                );
                return Ok(());
            }

            AgentAssignmentState::Complete(None) => {
                // mark all EvacuateObjects with this assignment id as
                // successful
                assignment.state = AssignmentState::AgentComplete;
                self.mark_assignment_objects(
                    &assignment.id,
                    EvacuateObjectStatus::PostProcessing,
                );

                // Couple options here.  We could:
                // - mark all tasks in as Complete (waiting for md
                // update) and then pass the entire assignment to the
                // metadata update broker
                // - pass the assignment uuid to the metadata update
                // broker and have it do a DB lookup for all objects
                // matching this Assignment Uuid.
                // - Build a new structure for metadata updates that
                // looks like:
                //  struct AssignmentMetadataUpdate {
                //      dest_shark: <StorageId>,
                //      Objects: <MantaObject>, // this could be updated
                // with the dest_shark above
                //  }
            }
            AgentAssignmentState::Complete(Some(failed_tasks)) => {
                dbg!(&failed_tasks);
                // TODO
                // 1. mark all EvacuateObjects from failed tasks as needs retry
                // 2. mark all other EvacuateObjects with this assignment_id
                // as successful
            }
        }

        Ok(())
    }
}

/// Start the sharkspotter thread and feed the objects into the assignment
/// thread.  If the assignment thread (the rx side of the channel) exits
/// prematurely the sender.send() method will return a SenderError and that
/// needs to be handled properly.
fn start_sharkspotter(
    obj_tx: crossbeam::Sender<SharkSpotterObject>,
    domain: &str,
    job_action: Arc<EvacuateJob>,
    min_shard: u32,
    max_shard: u32,
) -> Result<thread::JoinHandle<Result<(), Error>>, Error> {
    let shark = &job_action.from_shark.manta_storage_id;
    let config = sharkspotter::config::Config {
        domain: String::from(domain),
        min_shard,
        max_shard,
        shark: String::from(shark.as_str()),
        ..Default::default()
    };

    debug!("Starting sharkspotter thread: {:?}", &config);

    let plain = slog_term::PlainSyncDecorator::new(std::io::stdout());
    let log = Logger::root(
        Mutex::new(slog_term::FullFormat::new(plain).build()).fuse(),
        o!("build-id" => "0.1.0"),
    );

    thread::Builder::new()
        .name(String::from("sharkspotter"))
        .spawn(move || {
            let mut count = 0;
            sharkspotter::run(config, log, move |object, shard| {
                // while testing, limit the number of objects processed for now
                count += 1;
                if count > 2000 {
                    return Err(std::io::Error::new(
                        ErrorKind::Other,
                        "Just stop already",
                    ));
                }

                let ssobj = SharkSpotterObject {
                    shard: shard as i32,
                    object,
                };
                obj_tx
                    .send(ssobj)
                    .map_err(CrossbeamError::from)
                    .map_err(|e| {
                        error!("Sharkspotter: Error sending object: {}", e);
                        std::io::Error::new(ErrorKind::Other, e.description())
                    })
            })
            .map_err(Error::from)
        })
        .map_err(Error::from)
}

/// The assignment manager manages the destination sharks and
/// posts assignments to the remora agents running on the destination sharks.
/// Given a set of sharks that meet a set of parameters outlined by the
/// configuration the assignment manager
///     1. Initializes a new assignment with certain destination shark
///     specific the parameters.
///     2. Passes that assignment to the assignment generator thread which
///     adds tasks and sends the assignment back to this thread.
///     3. Post the assignment to the remora agent on the destination shark.
///
/// In the future another thread may be created to handle step 3.
///
/// Restrictions:
/// * Only 1 outstanding assignment per storage node (could change this in
/// the future, or make it tunable)
/// * If all storage ndoes with availableMb > Some TBD threshold have an
/// outstanding assignment, sleep/wait for an assignment to complete.
fn start_assignment_manager<S>(
    empty_assignment_tx: crossbeam::Sender<Assignment>,
    checker_fini_tx: crossbeam_channel::Sender<FiniMsg>,
    job_action: Arc<EvacuateJob>,
    picker: Arc<S>,
) -> Result<thread::JoinHandle<Result<(), Error>>, Error>
where
    S: SharkSource + 'static,
{
    thread::Builder::new()
        .name(String::from("assignment_manager"))
        .spawn(move || {
            let from_shark_datacenter =
                job_action.from_shark.datacenter.to_owned();
            let mut shark_index = 0;
            let algo = mod_picker::DefaultPickerAlgorithm {
                min_avail_mb: job_action.min_avail_mb,
                blacklist: vec![from_shark_datacenter],
            };

            let mut valid_sharks = vec![];
            let mut shark_list = vec![];

            loop {
                // TODO: allow for premature cancellation
                trace!("shark index: {}", shark_index);
                if valid_sharks.is_empty()
                    || shark_index
                        >= job_action
                            .dest_shark_list
                            .read()
                            .expect("desk_shark_list read lock len")
                            .len()
                {
                    shark_index = 0;

                    // Check for a new picker snapshot
                    // TODO: MANTA-4519
                    valid_sharks = match picker
                        .choose(&mod_picker::PickerAlgorithm::Default(&algo))
                    {
                        Some(sharks) => sharks,
                        None => {
                            if valid_sharks.is_empty() {
                                return Err(InternalError::new(
                                    Some(InternalErrorCode::PickerError),
                                    "No valid sharks available.",
                                )
                                .into());
                            }
                            valid_sharks
                        }
                    };

                    // update destination shark list
                    // TODO: perhaps this should be a BTreeMap or just a vec.
                    job_action.update_dest_sharks(&valid_sharks);

                    // TODO: Think about this a bit more.  On one hand making
                    // a 1 time copy and cycling through the whole list makes
                    // sense if we want to update the dest_shark_list while
                    // iterating over existing sharks.  On the other hand it
                    // doesn't seem like we would need to update the list of
                    // sharks except when we enter this block, so we could
                    // take the reader lock for each iteration and only take
                    // the writer lock inside the job_action methods called
                    // above.
                    shark_list = job_action
                        .dest_shark_list
                        .read()
                        .expect("dest_shark_list read lock")
                        .values()
                        .map(|v| v.shark.to_owned())
                        .collect();

                    shark_list.sort_by_key(|s| s.available_mb);
                }

                let cur_shark = &shark_list[shark_index];

                shark_index += 1;

                if job_action.shark_busy(cur_shark) {
                    info!(
                        "Shark '{}' is busy, trying next shark.",
                        cur_shark.manta_storage_id
                    );
                    continue;
                }

                let assignment = Assignment::new(cur_shark.clone());
                if let Err(e) = empty_assignment_tx.send(assignment) {
                    error!(
                        "Manager: Error sending assignment to generator \
                         thread: {}",
                        CrossbeamError::from(e)
                    );
                    break;
                }
            }
            // TODO: MANTA-4527
            info!("Manager: Shutting down assignment checker");
            checker_fini_tx.send(FiniMsg).expect("Fini Msg");
            Ok(())
        })
        .map_err(Error::from)
}

// If we have exceeded the per shark number of tasks then move on.  If
// max_tasks is not specified then we continue to fill it up, and rely on the
// max_size limit to tell us when the assignment is full.
fn _continue_adding_tasks(
    max_tasks: Option<u32>,
    assignment: &Assignment,
) -> bool {
    max_tasks.map_or(true, |m| assignment.tasks.len() < m as usize)
}

/// Assignment Generation:
/// 1. Get snapshot from picker
/// 2. Get initialized assignment from assignment manager thread.
/// 3. Fill out assignment with tasks according to the parameters outlined
/// in the assignment template received.
/// 4. Send filled out assignment back to assignment manager thread.
fn start_assignment_generator(
    obj_rx: crossbeam::Receiver<SharkSpotterObject>,
    empty_assignment_rx: crossbeam::Receiver<Assignment>,
    full_assignment_tx: crossbeam::Sender<Assignment>,
    job_action: Arc<EvacuateJob>,
) -> Result<thread::JoinHandle<Result<(), Error>>, Error> {
    let mut eobj: EvacuateObject = EvacuateObject::default();
    let max_tasks = job_action.max_tasks_per_assignment;
    let from_shark_host = job_action.from_shark.manta_storage_id.clone();
    let mut done = false;

    thread::Builder::new()
        .name(String::from("assignment_generator"))
        .spawn(move || {
            // Start Assignment Loop
            while !done {
                let mut assignment = match empty_assignment_rx.recv() {
                    Ok(assignment) => assignment,
                    Err(_) => break,
                };

                let mut available_space = assignment.max_size;
                let mut eobj_vec: Vec<EvacuateObject> = vec![];

                debug!(
                    "Filling up new assignment with max_tasks: {:#?}, \
                     and available_space: {}",
                    &max_tasks, &available_space
                );

                // Start Task Loop
                while _continue_adding_tasks(max_tasks, &assignment) {
                    eobj = match obj_rx.recv() {
                        Ok(obj) => {
                            trace!("Received object {:#?}", &obj);
                            EvacuateObject::new(obj)
                        }
                        Err(e) => {
                            warn!("Generator: Didn't receive object. {}\n", e);
                            info!(
                                "Generator: Sending last assignment: {}\n",
                                &assignment.id
                            );
                            done = true;
                            break;
                        }
                    };

                    let content_mb = eobj.object.content_length / (1024 * 1024);
                    if content_mb > available_space {
                        eobj.status = EvacuateObjectStatus::Skipped;
                        info!(
                            "Skipping object, need: {}, available: {} | {:?}\n",
                            content_mb, available_space, eobj
                        );
                        job_action.insert_into_db(&eobj)?;

                        break;
                    }

                    let obj = &eobj.object;
                    let obj_on_dest = obj
                        .sharks
                        .iter()
                        .find(|s| {
                            s.manta_storage_id
                                == assignment.dest_shark.manta_storage_id
                        })
                        .is_some();

                    // We've found the object on the destination shark.  We will
                    // need to skip this object for now and find a destination
                    // for it later.  If we don't do this check it would
                    // essentially reduce the durability level of the object.
                    if obj_on_dest {
                        info!(
                            "Skipping object already on dest shark {}",
                            &obj.object_id
                        );
                        // TODO sqlite: put skipped object in persistent store.
                        eobj.status = EvacuateObjectStatus::Skipped;
                        job_action.insert_into_db(&eobj)?;
                        continue;
                    }

                    // pick source shark
                    let source = match obj
                        .sharks
                        .iter()
                        .find(|s| s.manta_storage_id != from_shark_host)
                    {
                        Some(src) => src,
                        None => {
                            eobj.status = EvacuateObjectStatus::Skipped;
                            job_action.insert_into_db(&eobj)?;
                            continue;
                        }
                    };

                    assignment.tasks.insert(
                        obj.object_id.to_owned(),
                        Task {
                            object_id: obj.object_id.to_owned(),
                            owner: obj.owner.to_owned(),
                            md5sum: obj.content_md5.to_owned(),
                            source: source.to_owned(),
                            status: TaskStatus::Pending,
                        },
                    );

                    assignment.total_size += content_mb;
                    available_space -= content_mb;

                    trace!(
                        "Available space: {} | Tasks: {}",
                        &available_space,
                        &assignment.tasks.len()
                    );

                    eobj.status = EvacuateObjectStatus::Assigned;
                    eobj.assignment_id = assignment.id.clone();
                    eobj_vec.push(eobj.clone());
                } // End Task Loop

                info!("sending assignment to post thread: {:?}", &assignment);

                job_action.insert_many_into_db(&eobj_vec)?;

                // The assignment could be empty if the last object received
                // from sharkspotter can't be rebalanced to the destination
                // shark (e.g. not enough space, object already on
                // destination shark, etc).  Such objects would be skipped
                // and be reconsidered when we rescan skipped objects in the
                // local DB after sharkspotter has completed.
                if !assignment.tasks.is_empty() {
                    // Insert the Assignment into the hash of assignments so
                    // that the assignment checker thread knows to wait for
                    // it to be posted and to check for it later on.
                    job_action
                        .assignments
                        .write()
                        .expect("assignments write lock")
                        .insert(assignment.id.clone(), assignment.clone());

                    full_assignment_tx.send(assignment).map_err(|e| {
                        error!("Error sending assignment to be posted: {}", e);

                        InternalError::new(
                            Some(InternalErrorCode::Crossbeam),
                            CrossbeamError::from(e).description(),
                        )
                    })?;
                }
            } // End Assignment Loop

            Ok(())
        })
        .map_err(Error::from)
}

fn assignment_post<T>(
    assign_rx: crossbeam::Receiver<Assignment>,
    job_action: Arc<T>,
) -> Result<(), Error>
where
    T: PostAssignment,
{
    loop {
        match assign_rx.recv() {
            Ok(assignment) => {
                {
                    info!(
                        "Posting Assignment: {}\n",
                        serde_json::to_string(&assignment)?
                    );

                    match job_action.post(assignment) {
                        Ok(()) => (),
                        Err(e) => {
                            // TODO: persistent error / retry.  Here or in
                            // post()?
                            error!("Error posting assignment: {}", e);
                            continue;
                        }
                    }

                    // TODO sqlite: put assignment into persistent store?
                    // Currently considering allowing the assignment to be
                    // transient and only keep EvacuateObjects in persistent
                    // store.
                }
            }
            Err(_) => {
                info!("Post Thread: Channel closed, exiting.");
                break;
            }
        }
    }
    Ok(())
}

fn start_assignment_post(
    full_assignment_rx: crossbeam::Receiver<Assignment>,
    job_action: Arc<EvacuateJob>,
) -> Result<thread::JoinHandle<Result<(), Error>>, Error> {
    thread::Builder::new()
        .name(String::from("assignment_poster"))
        .spawn(move || assignment_post(full_assignment_rx, job_action))
        .map_err(Error::from)
}

/// Structures implementing this trait are able to post assignments to an agent.
trait PostAssignment: Sync + Send {
    fn post(&self, assignment: Assignment) -> Result<(), Error>;
}

/// Structures implementing this trait are able to process assignments
/// received from an agent.
pub trait ProcessAssignment: Sync + Send {
    fn process(&self, assignment: AgentAssignment) -> Result<(), Error>;
}

trait UpdateMetadata: Sync + Send {
    fn update_object_shark(
        &self,
        object: MantaObject,
        new_shark: &StorageNode,
    ) -> Result<MantaObject, Error>;
}

// XXX: async / surf candidate
/// Structures implementing this trait are able to get assignments from an
/// agent.
trait GetAssignment: Sync + Send {
    fn get(&self, assignment: &Assignment) -> Result<AgentAssignment, Error>;
}

fn assignment_get<T>(
    job_action: Arc<T>,
    assignment: &Assignment,
) -> Result<AgentAssignment, Error>
where
    T: GetAssignment,
{
    job_action.get(assignment)
}

/// Responsible for:
/// 1. periodically checking the Evacuate Job's hash of assignments that have
/// reached the Assigned state and, if so, querying the associated Agent for an
/// update on an that Assigned Assignment.
///
/// 2. Upon receipt of a completed assignment from the agent, the assignment is
/// passed to the process function of the EvacuateJob (which implements the
/// ProcessAssignment trait), which in turn updates all state of all the
/// EvacauteObject's in the local DB.
///
/// 3. Finally the assignment is sent to the metadata update broker which will
/// handle updating the metadata of every object in the assignment in the
/// Manta Metadata tier.
fn start_assignment_checker(
    job_action: Arc<EvacuateJob>,
    checker_fini_rx: crossbeam::Receiver<FiniMsg>,
    md_update_tx: crossbeam::Sender<Assignment>,
) -> Result<thread::JoinHandle<Result<(), Error>>, Error> {
    thread::Builder::new()
        .name(String::from("Assignment Checker"))
        .spawn(move || {
            let mut run = true;
            loop {
                if run {
                    run = match checker_fini_rx.try_recv() {
                        Ok(_) => {
                            info!(
                                "Assignment Checker thread received \
                                 shutdown message."
                            );
                            false
                        }
                        Err(e) => match e {
                            TryRecvError::Disconnected => {
                                warn!(
                                    "checker fini channel disconnected \
                                     before sending message.  Shutting \
                                     down."
                                );
                                false
                            }
                            TryRecvError::Empty => {
                                trace!(
                                    "No shutdown message, keep Checker \
                                     running"
                                );
                                true
                            }
                        },
                    };
                }

                // We'd rather not hold the lock here while we run
                // through all the HTTP GETs and while each completed
                // assignment is processed.  Furthermore, there's really no need
                // to hold the read lock here.  If another thread is created at
                // some point to query other assignments then we simply make an
                // extra HTTP GET call, perhaps get an already reported
                // assignment back, and when we take the write lock later we
                // will realize that this assignment is already in the
                // PostProcess state and skip it.  Under the current
                // implementation that will never happen, and if it does happen
                // in the future, no harm, no foul.  One alternative would be
                // to take the write lock for the duration of this loop and
                // process all the assignments right here, but the number of
                // assignments could grow significantly and have a
                // significantly negative impact on performance.
                let assignments = job_action
                    .assignments
                    .read()
                    .expect("assignments read lock")
                    .clone();

                debug!("Checking Assignments");
                if !run
                    && assignments
                        .values()
                        .find(|a| {
                            a.state == AssignmentState::Assigned
                                || a.state == AssignmentState::Init
                        })
                        .is_none()
                {
                    info!(
                        "Assignment Checker: Shutdown received and there \
                         are no remaining assignments in the assigned state to \
                         check on.  Exiting."
                    );
                    break;
                }

                for assignment in assignments.values() {
                    if assignment.state != AssignmentState::Assigned {
                        trace!(
                            "Skipping unassigned assignment {:?}",
                            assignment
                        );
                        continue;
                    }

                    debug!(
                        "Assignment Checker, checking: {} | {:?}",
                        assignment.id, assignment.state
                    );

                    // TODO: Async/await candidate
                    let ag_assignment = match assignment_get(
                        Arc::clone(&job_action),
                        &assignment,
                    ) {
                        Ok(a) => a,
                        Err(e) => {
                            // TODO: Log persistent error
                            error!("{}", e);
                            continue;
                        }
                    };

                    debug!("Got Assignment: {:?}", ag_assignment);
                    // If agent assignment is complete, process it and pass
                    // it to the metadata update broker.  Otherwise, continue
                    // to next assignment.
                    match ag_assignment.stats.state {
                        AgentAssignmentState::Complete(_) => {
                            job_action.process(ag_assignment)?
                        }
                        _ => continue,
                    }

                    // Mark the shark associated with this assignment as Ready
                    job_action.mark_dest_shark_ready(&assignment.dest_shark);

                    // XXX: We only really need the assignment ID and the
                    // dest_shark, so maybe we should create a new struct to
                    // send this data.  Also, the assignment state is
                    // probably out of date since we just ran process above.
                    // Some alternate approaches would be for the
                    // job_action.process() to return an updated assignment
                    // and pass that along, or pass this Crossbeam::Sender to
                    // job_action.process() and send it from there.  The
                    // latter approach may make testing a bit more difficult.
                    match md_update_tx.send(assignment.to_owned()) {
                        Ok(()) => (),
                        Err(e) => {
                            error!(
                                "Assignment Checker: Error sending \
                                 assignment to the metadata \
                                 broker {}",
                                e
                            );
                        }
                    }
                }

                // TODO: Tunable?
                thread::sleep(Duration::from_secs(5));
            }
            Ok(())
        })
        .map_err(Error::from)
}

fn metadata_update_worker(
    job_action: Arc<EvacuateJob>,
    queue_front: Arc<Injector<Assignment>>,
) -> impl Fn() {
    move || {
        loop {
            let assignment = match queue_front.steal() {
                Steal::Success(a) => a,
                Steal::Retry => continue,
                Steal::Empty => break,
            };

            let dest_shark = &assignment.dest_shark;

            let objects = job_action.get_assignment_objects(
                &assignment.id,
                EvacuateObjectStatus::PostProcessing,
            );

            let mut updated_objects = vec![];
            for obj in objects {
                let mut mobj: MantaObject =
                    match serde_json::from_str(&obj.object) {
                        Ok(o) => o,
                        Err(e) => {
                            // TODO: log a persistent error for final job report.
                            error!(
                                "Error decoding object {}: {}",
                                &obj.object, e
                            );
                            continue;
                        }
                    };

                // This function updates the manta object with the new
                // sharks in the Manta Metadata tier, and then returns the
                // updated Manta metadata object.  It does not update the
                // state of the associated EvacuateObject in the local database.
                mobj = match job_action.update_object_shark(mobj, dest_shark) {
                    Ok(o) => o,
                    Err(e) => {
                        // TODO: log a persistent error for final job report.
                        error!(
                            "Error updating {}, with dest_shark {:?}: {}",
                            &obj.object, dest_shark, e
                        );
                        continue;
                    }
                };

                updated_objects.push(mobj.object_id.clone());
            }

            debug!("Updated Objects: {:?}", updated_objects);

            // TODO: Should the assignment be removed from the hash of
            // assignments or entered into some DB somewhere for a persistent
            // log?
            match job_action
                .assignments
                .write()
                .expect("assignment write lock")
                .get_mut(&assignment.id)
            {
                Some(a) => {
                    a.state = AssignmentState::PostProcessed;
                    info!("Done processing assignment {}", &a.id);
                }
                None => {
                    error!(
                        "Error updating assignment state for: {}",
                        &assignment.id
                    );
                }
            }

            // TODO: batch update all objects in `updated_objects` with
            // EvacuateObjectStatus::Complete in the local DB meaning we are
            // completely done and this object has been rebalanced.
            // This is the finish line.
        }
    }
}

/// This thread runs until EvacuateJob Completion.
/// When it receives a completed Assignment it will enqueue it into a work queue
/// and then possibly starts worker thread to do the work.  The worker thread
/// comes from a pool with a tunable size.  If the max number of worker threads
/// are already running the completed Assignment stays in the queue to be picked
/// up by the next available and running worker thread.  Worker threads will
/// exit if the queue is empty when they finish their current work and check the
/// queue for the next Assignment.
///
/// The plan is for this thread pool size to be the main tunable
/// controlling our load on the Manta Metadata tier.  The thread pool size can
/// be changed while the job is running with `.set_num_threads()`.
/// How we communicate with a running job to tell it to alter its tunables is
/// still TBD.
///
/// One trade off here is whether or not the messages being sent to this
/// thread are Assignments or individual EvacuateObjects (or
/// Vec<EvacauteObject>).  By opting for an Assignment (or Vec<EvacuateObject>)
/// we provide some "tunability" without providing too much rope.  We also
/// allow for the possibility of doing batched updates on a per worker thread
/// basis.
///
/// Before enqueuing any work it is imperative that that state of the
/// EvacauteObjects that are about be updated in the metadata tier are in the
/// correct state in the local DB.  In the event that this thread and/or its
/// worker(s) die before the metadata is updated in the Manta Metadata tier
/// we must be able to restart the job, scan the DB for EvacuatedObjects in
/// the in the PostProcessing state without having to look them up with
/// sharkspotter again, and download them onto another shark.
fn start_metadata_update_broker(
    job_action: Arc<EvacuateJob>,
    md_update_rx: crossbeam::Receiver<Assignment>,
) -> Result<thread::JoinHandle<Result<(), Error>>, Error> {
    // TODO: tunable
    let pool = ThreadPool::new(2);
    let queue = Arc::new(Injector::<Assignment>::new());
    let queue_back = Arc::clone(&queue);

    thread::Builder::new()
        .name(String::from("Metadata Update broker"))
        .spawn(move || {
            loop {
                let assignment = match md_update_rx.recv() {
                    Ok(assignment) => assignment,
                    Err(e) => {
                        error!(
                            "MD Update: Error receiving metadata from \
                             assignment checker thread: {}",
                            e
                        );
                        break;
                    }
                };

                queue_back.push(assignment);

                // XXX: async/await candidate?
                let worker_job_action = Arc::clone(&job_action);
                let queue_front = Arc::clone(&queue);

                let worker =
                    metadata_update_worker(worker_job_action, queue_front);

                pool.execute(worker);
            }
            Ok(())
        })
        .map_err(Error::from)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::picker::PickerAlgorithm;
    use crate::util;
    use quickcheck::{Arbitrary, StdThreadGen};
    use rand::Rng;

    fn generate_sharks(num_sharks: u8, local_only: bool) -> Vec<StorageNode> {
        let mut rng = rand::thread_rng();
        let mut ret = vec![];

        for _ in 0..num_sharks {
            let percent_used: u8 = rng.gen_range(0, 101);
            let timestamp: u64 = rng.gen();
            let available_mb: u64 = rng.gen();
            let filesystem: String = util::random_string(rng.gen_range(1, 20));
            let datacenter: String = util::random_string(rng.gen_range(1, 20));
            let manta_storage_id = match local_only {
                true => String::from("localhost"),
                false => format!("{}.stor.joyent.us", rng.gen_range(1, 100)),
            };

            ret.push(StorageNode {
                available_mb,
                percent_used,
                filesystem,
                datacenter,
                manta_storage_id,
                timestamp,
            });
        }
        ret
    }

    struct MockPicker;

    impl MockPicker {
        fn new() -> Self {
            MockPicker {}
        }
    }

    impl SharkSource for MockPicker {
        fn choose(&self, _: &PickerAlgorithm) -> Option<Vec<StorageNode>> {
            let mut rng = rand::thread_rng();
            let random = rng.gen_range(0, 10);

            if random == 0 {
                return None;
            }

            Some(generate_sharks(random, true))
        }
    }

    #[derive(Default)]
    struct EmptyPicker {}
    impl SharkSource for EmptyPicker {
        fn choose(&self, _algo: &PickerAlgorithm) -> Option<Vec<StorageNode>> {
            None
        }
    }

    #[test]
    fn empty_picker_test() {
        let picker = Arc::new(EmptyPicker {});
        let (empty_assignment_tx, _) = crossbeam::bounded(5);
        let (checker_fini_tx, _) = crossbeam::bounded(1);

        let job_action = EvacuateJob::new(
            String::from("1.stor.joyent.us"),
            "empty_picker_test.db",
        );
        let job_action = Arc::new(job_action);

        let assignment_manager_handle = match start_assignment_manager(
            empty_assignment_tx,
            checker_fini_tx,
            Arc::clone(&job_action),
            Arc::clone(&picker),
        ) {
            Ok(h) => h,
            Err(e) => {
                assert_eq!(
                    true, false,
                    "Could not start assignment manager {}",
                    e
                );
                return;
            }
        };

        let ret = assignment_manager_handle
            .join()
            .expect("assignment manager handle");

        assert_eq!(ret.is_err(), true);

        match ret.unwrap_err() {
            Error::Internal(e) => {
                assert_eq!(e.code, InternalErrorCode::PickerError);
            }
            _ => {
                assert_eq!(1, 0, "Incorrect Error Code");
            }
        }
    }

    #[test]
    fn skip_object_test() {
        // TODO: add test that includes skipped objects
    }

    #[test]
    fn duplicate_object_id_test() {
        // TODO: add test that includes duplicate object IDs
    }

    #[test]
    fn full_test() {
        pretty_env_logger::init();
        let now = std::time::Instant::now();
        let picker = MockPicker::new();
        let picker = Arc::new(picker);

        let (empty_assignment_tx, empty_assignment_rx) = crossbeam::bounded(5);
        let (full_assignment_tx, full_assignment_rx) = crossbeam::bounded(5);
        let (obj_tx, obj_rx) = crossbeam::bounded(5);
        let (md_update_tx, md_update_rx) = crossbeam::bounded(5);
        let (checker_fini_tx, checker_fini_rx) = crossbeam::bounded(1);

        let job_action =
            EvacuateJob::new(String::from("1.stor.joyent.us"), "full_test.db");
        let conn = job_action.conn.lock().expect("db connection lock");
        conn.execute(r#"DROP TABLE evacuateobjects"#)
            .unwrap_or_else(|e| {
                debug!("Table doesn't exist: {}", e);
                0
            });

        conn.execute(
            r#"CREATE TABLE evacuateobjects(
                id TEXT PRIMARY KEY,
                assignment_id TEXT,
                object TEXT,
                shard Integer,
                status TEXT CHECK(status IN ('unprocessed', 'assigned',
                'skipped', 'post_processing', 'complete')) NOT NULL
            );"#,
        )
        .expect("create table");

        drop(conn);

        let job_action = Arc::new(job_action);

        let mut test_objects = vec![];

        let mut g = StdThreadGen::new(10);
        for _ in 0..1000 {
            let mobj = MantaObject::arbitrary(&mut g);
            test_objects.push(mobj);
        }

        let test_objects_copy = test_objects.clone();

        let builder = thread::Builder::new();
        let obj_generator_th = builder
            .name(String::from("object_generator_test"))
            .spawn(move || {
                for o in test_objects_copy.into_iter() {
                    let ssobj = SharkSpotterObject {
                        shard: 1,
                        object: o.clone(),
                    };
                    match obj_tx.send(ssobj) {
                        Ok(()) => (),
                        Err(e) => {
                            error!(
                                "Could not send object.  Assignment \
                                 generator must have shutdown {}.",
                                e
                            );
                            break;
                        }
                    }
                }
            })
            .expect("failed to build object generator thread");

        let metadata_update_thread =
            start_metadata_update_broker(Arc::clone(&job_action), md_update_rx)
                .expect("start metadata updater thread");

        let assignment_checker_thread = start_assignment_checker(
            Arc::clone(&job_action),
            checker_fini_rx,
            md_update_tx,
        )
        .expect("start assignment checker thread");

        let assign_post_thread =
            start_assignment_post(full_assignment_rx, Arc::clone(&job_action))
                .expect("assignment post thread");

        let generator_thread = start_assignment_generator(
            obj_rx,
            empty_assignment_rx,
            full_assignment_tx,
            Arc::clone(&job_action),
        )
        .expect("start assignment generator");

        let manager_thread = start_assignment_manager(
            empty_assignment_tx,
            checker_fini_tx,
            Arc::clone(&job_action),
            Arc::clone(&picker),
        )
        .expect("start assignment manager");

        obj_generator_th.join().expect("object generator thread");

        match manager_thread
            .join()
            .expect("test assignment manager thread")
        {
            Ok(()) => (),
            Err(e) => {
                if let Error::Internal(err) = e {
                    if err.code == InternalErrorCode::PickerError {
                        error!(
                            "Enountered empty picker on startup, exiting \
                             safely"
                        );
                    } else {
                        panic!("error {}", err);
                    }
                } else {
                    panic!("error {}", e);
                }
            }
        }

        generator_thread
            .join()
            .expect("assignment generator thread")
            .expect("Error joining assignment generator thread");

        metadata_update_thread
            .join()
            .expect("joining MD update thread")
            .expect("internal MD update thread");

        assignment_checker_thread
            .join()
            .expect("joining assignment checker thread")
            .expect("internal assignment checker thread");

        assign_post_thread
            .join()
            .expect("joining assignment post thread")
            .expect("internal assignment post thread");

        debug!("TOTAL TIME: {}ms", now.elapsed().as_millis());
        debug!(
            "TOTAL INSERT DB TIME: {}ms",
            job_action.total_db_time.lock().expect("db time lock")
        );
    }
}
