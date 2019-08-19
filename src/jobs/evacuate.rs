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
    Assignment, AssignmentId, AssignmentState, ObjectId, StorageId, Task,
    TaskStatus,
};
use crate::picker::{self as mod_picker, SharkSource, StorageNode};

use std::collections::HashMap;
use std::error::Error as _Error;
use std::io::ErrorKind;
use std::sync::{Arc, Mutex, RwLock};
use std::thread;

use bytes::Bytes;
use crossbeam_channel as crossbeam;
use libmanta::moray::{MantaObject, MantaObjectShark};
use slog::{o, Drain, Logger};
use std::borrow::Borrow;
use uuid::Uuid;

// --- Diesel Stuff, TODO This should be refactored --- //

use diesel::prelude::*;

// TODO: move database stuff somewhere.
table! {
    use diesel::sql_types::Text;
    use super::EvacuateObjectStatusMapping;
    evacuateobjects (id) {
        id -> Text,
        object -> Text,
        assignment_id -> Text,
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
pub struct EvacuateObjectInsertable {
    pub id: String,
    pub object: String,
    pub assignment_id: AssignmentId,
    pub status: EvacuateObjectStatus,
}

// --- END Diesel Stuff --- //

#[derive(Debug, Clone, PartialEq, DbEnum)]
pub enum EvacuateObjectStatus {
    Unprocessed,    // Default state
    Processing,     // Object has been included in an assignment
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
    pub id: ObjectId,        // MantaObject ObjectId
    pub object: MantaObject, // The MantaObject being rebalanced
    pub assignment_id: AssignmentId,
    // UUID of assignment this object was most recently part of.
    pub status: EvacuateObjectStatus,
    // Status of the object in the evacuation job
}

impl EvacuateObject {
    fn new(object: MantaObject) -> Self {
        Self {
            id: object.object_id.to_owned(),
            object,
            assignment_id: Uuid::new_v4().to_string(),
            ..Default::default()
        }
    }
}

impl EvacuateObject {
    // TODO: ToSql for EvacuateObject MANTA-4474
    fn to_insertable(&self) -> Result<EvacuateObjectInsertable, Error> {
        Ok(EvacuateObjectInsertable {
            id: self.id.clone(),
            assignment_id: self.assignment_id.clone(),
            object: serde_json::to_string(&self.object)?,
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
    pub assignments: RwLock<HashMap<String, Assignment>>,
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

        let conn = self.conn.lock().unwrap();
        conn.execute(r#"DROP TABLE evacuateobjects"#)
            .unwrap_or_else(|e| {
                debug!("Table doesn't exist: {}", e);
                0
            });

        conn.execute(
            r#"
                CREATE TABLE evacuateobjects(
                    id TEXT PRIMARY KEY,
                    object TEXT,
                    assignment_id TEXT,
                    status TEXT CHECK(status IN ('unprocessed', 'processing',
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
            Arc::clone(&job_action),
            Arc::clone(&picker),
        )?;

        // At this point the rebalance job is running and we are blocked at
        // the assignment_manager thread join.
        assignment_manager
            .join()
            .unwrap()
            .expect("Error joining assignment generator thread");

        picker.fini();

        sharkspotter_thread.join().unwrap().unwrap_or_else(|e| {
            error!("Error joining sharkspotter handle: {}\n", e);
            std::process::exit(1);
        });

        generator_thread
            .join()
            .unwrap()
            .expect("Error joining assignment generator thread");

        post_thread
            .join()
            .unwrap()
            .expect("Error joining assignment processor thread");

        Ok(())
    }

    /// If a shark is in the Assigned state then it is busy.
    fn shark_busy(&self, shark: &StorageNode) -> bool {
        self.dest_shark_list
            .read()
            .unwrap()
            .get(shark.manta_storage_id.as_str())
            .map_or(false, |eds| {
                debug!(
                    "shark '{}' status: {:?}",
                    shark.manta_storage_id, eds.status
                );
                eds.status == DestSharkStatus::Assigned
            })
    }

    /// checks all assigned assignments for their current status from the
    /// agent returning number of assignments that changed state
    fn check_assignments(&self) -> Result<u32, Error> {
        // TODO: This should probably all be handled by the ProcessAssignment
        // Trait methods.  Somethings that need to be considered here are how
        // often to poll and if and if so when we should force polling.
        // Once we have the Zone talking to the Agents we can make
        // a decision on what exactly to do here.

        debug!("checking assignments");
        let mut updated_shark_count = 0;
        let mut assignments = self.assignments.write().unwrap();
        for assignment in assignments.values_mut() {
            if assignment.state != AssignmentState::Assigned {
                continue;
            }

            let _uri = format!(
                "http://{}/assignment/{}",
                assignment.dest_shark, assignment.id
            );
            // TODO agent: make request to agent "uri" for assignment status

            // update assignment status based on return value
            // for now just mark as complete
            assignment.state = AssignmentState::Complete;

            // update shark status
            if let Some(dest_shark) = self
                .dest_shark_list
                .write()
                .unwrap()
                .get_mut(assignment.dest_shark.as_str())
            {
                debug!(
                    "Updating shark '{}' to Ready state",
                    assignment.dest_shark.as_str()
                );
                dest_shark.status = DestSharkStatus::Ready;
                updated_shark_count += 1;
            } else {
                warn!(
                    "Could not find shark: '{}'",
                    assignment.dest_shark.as_str()
                );
            }

            // post process Assignment by updating objects in persistent store
            // this could be it's own thread
            // TODO metadata: for now just mark them as post processed
            assignment.state = AssignmentState::PostProcessed;
        }
        Ok(updated_shark_count)
    }

    /// Iterate over a new set of storage nodes and update our destination
    /// shark list accordingly.  This may need to change so that we update
    /// available_mb more judiciously (i.e. based on timestamp).
    fn update_dest_sharks(&self, new_sharks: &[StorageNode]) {
        let mut dest_shark_list = self.dest_shark_list.write().unwrap();
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
        let locked_conn = self.conn.lock().unwrap();
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

        let mut total_time = self.total_db_time.lock().unwrap();
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
        let insertable_objs: Vec<EvacuateObjectInsertable> = objs
            .iter()
            .map(|o| o.to_insertable())
            .collect::<Result<Vec<_>, _>>()?;
        let locked_conn = self.conn.lock().unwrap();

        let now = std::time::Instant::now();
        let ret = diesel::insert_into(evacuateobjects)
            .values(insertable_objs)
            .execute(&*locked_conn)
            .unwrap_or_else(|e| {
                let msg = format!("Error inserting object into DB: {}", e);
                error!("{}", msg);
                panic!(msg);
            });
        let mut total_time = self.total_db_time.lock().unwrap();
        *total_time += now.elapsed().as_millis();

        Ok(ret)
    }

    // TODO: Batched update: MANTA-4464
    fn mark_object_post_processing(&self, obj_id: &str) -> usize {
        use self::evacuateobjects::dsl::status;

        let insertable = UpdateEvacuateObject { id: obj_id };

        let locked_conn = self.conn.lock().unwrap();
        diesel::update(&insertable)
            .set(status.eq(EvacuateObjectStatus::PostProcessing))
            .execute(&*locked_conn)
            .unwrap_or_else(|e| {
                let msg = format!("Error updating object: {} ({})", obj_id, e);
                error!("{}", msg);
                panic!(msg);
            })
    }
}

fn assignment_post_success(
    job_action: &EvacuateJob,
    mut assignment: Assignment,
) -> Result<Assignment, Error> {
    assignment.state = AssignmentState::Assigned;
    let mut assignments = match job_action.assignments.write() {
        Ok(a) => a,
        Err(e) => {
            let msg = format!("Error locking assignments Hash: {}", e);
            error!("{}", &msg);

            return Err(InternalError::new(
                Some(InternalErrorCode::LockError),
                msg,
            )
            .into());
        }
    };
    assignments.insert(assignment.id.clone(), assignment.clone());

    Ok(assignment)
}

impl PostAssignment for EvacuateJob {
    fn post(&self, assignment: Assignment) -> Result<(), Error> {
        // TODO agent: Send assignment to agent
        Ok(assignment_post_success(self, assignment).map(|_| ())?)
    }
}

impl ProcessAssignment for EvacuateJob {
    fn process(&self, returned_assignment: Bytes) -> Result<(), Error> {
        // TODO: dont unwrap throughout

        let assignment_u8: &[u8] = &returned_assignment;
        let agent_assignment: AgentAssignment =
            serde_json::from_slice(assignment_u8)?;
        let uuid = &agent_assignment.uuid;

        let mut assignments = self.assignments.write().unwrap();

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

        debug!(
            "Checking agent assignment state: {:#?}",
            &agent_assignment.stats.state
        );

        match agent_assignment.stats.state {
            AgentAssignmentState::Scheduled | AgentAssignmentState::Running => {
                return Ok(());
            }
            AgentAssignmentState::Complete(None) => {
                // mark all EvacuateObjects with this assignment id as
                // successful

                for id in assignment.tasks.keys() {
                    self.mark_object_post_processing(id);
                }
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
    sender: crossbeam::Sender<MantaObject>,
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
            sharkspotter::run(config, log, move |obj, _shard| {
                // while testing, limit the number of objects processed for now
                count += 1;
                if count > 2000 {
                    return Err(std::io::Error::new(
                        ErrorKind::Other,
                        "Just stop already",
                    ));
                }

                // TODO: add shard number?
                sender.send(obj).map_err(CrossbeamError::from).map_err(|e| {
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
                        >= job_action.dest_shark_list.read().unwrap().len()
                {
                    shark_index = 0;

                    // Check on assignments and update sharks accordingly
                    job_action.check_assignments()?;

                    // Check for a new picker snapshot
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
                        .unwrap()
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

                // Only use half of the available space per shark per assignment
                let assignment = Assignment {
                    id: Uuid::new_v4().to_string(),
                    dest_shark: cur_shark.manta_storage_id.clone(),
                    max_size: cur_shark.available_mb / 2,
                    total_size: 0,
                    tasks: HashMap::new(),
                    state: AssignmentState::Init,
                };

                if let Err(e) = empty_assignment_tx.send(assignment) {
                    error!("{}", CrossbeamError::from(e));
                    break;
                }
            }
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
    obj_rx: crossbeam::Receiver<MantaObject>,
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
                            warn!("Didn't receive object. {}\n", e);
                            info!(
                                "Sending last assignment: {}\n",
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
                    let obj_not_on_dest = obj
                        .sharks
                        .iter()
                        .find(|s| s.manta_storage_id == assignment.dest_shark)
                        .is_none();

                    // We've found the object on the destination shark.  We will
                    // need to skip this object for now and find a destination
                    // for it later.  If we don't do this check it would
                    // essentially reduce the durability level of the object.
                    if !obj_not_on_dest {
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

                    available_space -= content_mb;

                    trace!(
                        "Available space: {} | Tasks: {}",
                        &available_space,
                        &assignment.tasks.len()
                    );

                    eobj.status = EvacuateObjectStatus::Processing;
                    eobj_vec.push(eobj.clone());
                } // End Task Loop

                info!(
                    "sending assignment to processor thread: {:?}",
                    &assignment
                );

                job_action.insert_many_into_db(&eobj_vec)?;

                if !assignment.tasks.is_empty() {
                    full_assignment_tx.send(assignment).map_err(|e| {
                        error!(
                            "Error sending assignment back to manager: {}",
                            e
                        );

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
                    // TODO: update shark with space avail?

                    info!(
                        "Posting Assignment: {}\n",
                        serde_json::to_string(&assignment)?
                    );

                    job_action.post(assignment)?;

                    // TODO sqlite: put assignment into persistent store?
                    // Currently considering allowing the assignment to be
                    // transient and only keep EvacuateObjects in persistent
                    // store.
                }
            }
            Err(err) => {
                return Err(InternalError::new(
                    Some(InternalErrorCode::Crossbeam),
                    err.description(),
                )
                .into());
            }
        }
    }
}

fn start_assignment_post(
    assign_rx: crossbeam::Receiver<Assignment>,
    job_action: Arc<EvacuateJob>,
) -> Result<thread::JoinHandle<Result<(), Error>>, Error> {
    thread::Builder::new()
        .name(String::from("assignment_poster"))
        .spawn(move || assignment_post(assign_rx, job_action))
        .map_err(Error::from)
}

trait PostAssignment: Sync + Send {
    fn post(&self, assignment: Assignment) -> Result<(), Error>;
}

pub trait ProcessAssignment: Sync + Send {
    fn process(&self, assignment: Bytes) -> Result<(), Error>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent::AgentAssignmentStats;
    use crate::picker::PickerAlgorithm;
    use crate::util;
    use quickcheck::{Arbitrary, StdThreadGen};
    use rand::Rng;

    fn zone_assignment_to_agent(assign: &Assignment) -> AgentAssignment {
        AgentAssignment {
            uuid: assign.id.clone(),
            stats: AgentAssignmentStats {
                state: match assign.state {
                    AssignmentState::Init => AgentAssignmentState::Scheduled,
                    AssignmentState::Assigned
                    | AssignmentState::Complete
                    | AssignmentState::Rejected
                    | AssignmentState::PostProcessed => {
                        AgentAssignmentState::Complete(None)
                    }
                },
                failed: 0,
                complete: 0,
                total: 0,
            },
            tasks: vec![crate::jobs::Task::default()],
        }
    }

    fn generate_sharks(num_sharks: u8) -> Vec<StorageNode> {
        let mut rng = rand::thread_rng();
        let mut ret = vec![];

        for _ in 0..num_sharks {
            let percent_used: u8 = rng.gen_range(0, 101);
            let timestamp: u64 = rng.gen();
            let available_mb: u64 = rng.gen();
            let filesystem: String = util::random_string(rng.gen_range(1, 20));
            let datacenter: String = util::random_string(rng.gen_range(1, 20));
            let manta_storage_id: String =
                format!("{}.stor.joyent.us", rng.gen_range(1, 100));

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

            Some(generate_sharks(random))
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

        let job_action = EvacuateJob::new(
            String::from("1.stor.joyent.us"),
            "empty_picker_test.db",
        );
        let job_action = Arc::new(job_action);

        let handle = start_assignment_manager(
            empty_assignment_tx,
            Arc::clone(&job_action),
            Arc::clone(&picker),
        );

        let handle = match handle {
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

        let ret = handle.join().unwrap();

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
        let (assignment_process_tx, assignment_process_rx) =
            crossbeam::bounded(20);

        let assignment_process_tx: crossbeam_channel::Sender<Assignment> =
            assignment_process_tx;

        let assignment_process_rx: crossbeam_channel::Receiver<Assignment> =
            assignment_process_rx;

        let job_action =
            EvacuateJob::new(String::from("1.stor.joyent.us"), "full_test.db");
        let conn = job_action.conn.lock().unwrap();
        conn.execute(r#"DROP TABLE evacuateobjects"#)
            .unwrap_or_else(|e| {
                debug!("Table doesn't exist: {}", e);
                0
            });

        conn.execute(
            r#"CREATE TABLE evacuateobjects(
                id TEXT PRIMARY KEY,
                object TEXT,
                assignment_id TEXT,
                status TEXT CHECK(status IN ('unprocessed', 'processing',
                'skipped', 'post_processing', 'complete')) NOT NULL
            );"#,
        )
        .unwrap();

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
                    match obj_tx.send(o) {
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

        let generator_thread = start_assignment_generator(
            obj_rx,
            empty_assignment_rx,
            full_assignment_tx,
            Arc::clone(&job_action),
        )
        .unwrap();

        let manager_thread = start_assignment_manager(
            empty_assignment_tx,
            Arc::clone(&job_action),
            Arc::clone(&picker),
        )
        .unwrap();

        // TODO: When we add a thread to process completed assignments this
        // will likely have to be refactored.
        let proc_job_action = Arc::clone(&job_action);
        let processing_thread = thread::Builder::new()
            .name(String::from("processor thread"))
            .spawn(move || {
                loop {
                    let ag_assign = match assignment_process_rx.recv() {
                        Ok(assignment) => {
                            debug!("Received Assignment: {}", assignment.id);

                            // This is a little hacky.  We want to mock up the
                            // post action of the JobAction but we also want to
                            // make sure the assignment is inserted into the
                            // correct Job Action's outstanding assignments.  So
                            // either we factor out all the EvacuateJob's
                            // methods and call them from the MockJobAction, or
                            // we do this.  Maybe there's another more rust-ish way?
                            let new_assignment = match assignment_post_success(
                                &proc_job_action,
                                assignment,
                            ) {
                                Ok(a) => a,
                                Err(e) => {
                                    panic!("Error {}", e);
                                }
                            };

                            zone_assignment_to_agent(&new_assignment)
                        }
                        Err(e) => {
                            error!("Error receiving assignment: {}", e);
                            break;
                        }
                    };

                    let assign_vec = serde_json::to_vec(&ag_assign)
                        .unwrap_or_else(|e| {
                            error!("Error convering assignment to bytes {}", e);
                            panic!("Error");
                        });
                    let assign_u8 = Bytes::from(assign_vec);

                    assert!(proc_job_action.process(assign_u8).is_ok());
                }
                info!("Processor thread complete");
            })
            .unwrap();

        struct MockJobAction {
            objects: Vec<MantaObject>,
            process_tx: crossbeam_channel::Sender<Assignment>,
        }

        impl PostAssignment for MockJobAction {
            fn post(&self, assignment: Assignment) -> Result<(), Error> {
                for (_, t) in assignment.tasks.iter() {
                    assert!(
                        self.objects
                            .iter()
                            .any(|o| { t.object_id == o.object_id }),
                        "Missing {} from objects",
                        t.object_id
                    );
                }

                debug!(
                    "Sending assignment {:?} with {:?} tasks",
                    assignment.id,
                    assignment.tasks.len()
                );

                // Normally we'd just send this to the Agent, but instead we
                // send it directly to the process assignment thread which
                // handles updating the state of the object as well as the
                // metadata updates.
                match self.process_tx.send(assignment) {
                    Ok(()) => (),
                    Err(e) => {
                        error!("Error sending {}", e);
                    }
                }

                Ok(())
            }
        }

        let mock_job_action = Arc::clone(&Arc::new(MockJobAction {
            objects: test_objects,
            process_tx: assignment_process_tx,
        }));

        match assignment_post(full_assignment_rx, mock_job_action) {
            Ok(()) => (),
            Err(_) => info!("Done"),
        };

        obj_generator_th.join().unwrap();

        match manager_thread.join().unwrap() {
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
            .unwrap()
            .expect("Error joining assignment generator thread");

        processing_thread
            .join()
            .expect("Error joining processing thread");

        debug!("TOTAL TIME: {}ms", now.elapsed().as_millis());
        debug!(
            "TOTAL INSERT DB TIME: {}ms",
            job_action.total_db_time.lock().unwrap()
        );
    }
}
