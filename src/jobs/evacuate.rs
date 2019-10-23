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
    ObjectSkippedReason, StorageId, Task, TaskStatus,
};
use crate::moray_client;
use crate::picker::{self as mod_picker, SharkSource, StorageNode};

use std::collections::hash_map::Entry::{Occupied, Vacant};
use std::collections::HashMap;
use std::error::Error as _Error;
use std::io::{ErrorKind, Write};
use std::str::FromStr;
use std::string::ToString;
use std::sync::{Arc, Mutex, RwLock};
use std::thread;
use std::time::Duration;

use strum::IntoEnumIterator;

use crossbeam_channel as crossbeam;
use crossbeam_channel::TryRecvError;
use crossbeam_deque::{Injector, Steal};
use libmanta::moray::{MantaObject, MantaObjectShark};
use moray::client::MorayClient;
use reqwest;
use slog::{o, Drain, Logger};
use threadpool::ThreadPool;

// --- Diesel Stuff, TODO This should be refactored --- //

use diesel::backend;
use diesel::deserialize::{self, FromSql};
use diesel::prelude::*;
use diesel::serialize::{self, IsNull, Output, ToSql};
use diesel::sql_types;
use diesel::sqlite::Sqlite;

// Note: The ordering of the fields in this table must match the ordering of
// the fields in 'struct EvacuateObject'
table! {
    use diesel::sql_types::{Text, Integer, Nullable};
    evacuateobjects (id) {
        id -> Text,
        assignment_id -> Text,
        object -> Text,
        shard -> Integer,
        etag -> Text,
        status -> Text,
        skipped_reason -> Nullable<Text>,
        error -> Nullable<Text>,
    }
}

#[derive(Insertable, Queryable, Identifiable)]
#[table_name = "evacuateobjects"]
struct UpdateEvacuateObject<'a> {
    id: &'a str,
}

#[derive(
    Display,
    EnumString,
    EnumVariantNames,
    Debug,
    Clone,
    Copy,
    PartialEq,
    FromSqlRow,
    AsExpression,
)]
#[strum(serialize_all = "snake_case")]
#[sql_type = "sql_types::Text"]
pub enum EvacuateObjectStatus {
    Unprocessed,    // Default state.
    Assigned,       // Object has been included in an assignment.
    Skipped,        // Could not find a shark to put this object in. Will retry.
    Error,          // A persistent error has occurred.
    PostProcessing, // Updating metadata and any other postprocessing steps.
    Complete,       // Object has been fully rebalanced.
}

impl ToSql<sql_types::Text, Sqlite> for EvacuateObjectStatus {
    fn to_sql<W: Write>(
        &self,
        out: &mut Output<W, Sqlite>,
    ) -> serialize::Result {
        let s = self.to_string();
        out.write_all(s.as_bytes())?;
        Ok(IsNull::No)
    }
}

impl FromSql<sql_types::Text, Sqlite> for EvacuateObjectStatus {
    fn from_sql(
        bytes: Option<backend::RawValue<Sqlite>>,
    ) -> deserialize::Result<Self> {
        let t = not_none!(bytes).read_text();
        Self::from_str(t).map_err(std::convert::Into::into)
    }
}

#[derive(
    Display,
    EnumString,
    EnumVariantNames,
    Debug,
    Clone,
    Copy,
    PartialEq,
    FromSqlRow,
    AsExpression,
)]
#[strum(serialize_all = "snake_case")]
#[sql_type = "sql_types::Text"]
pub enum EvacuateObjectError {
    BadMorayClient,
    BadShardNumber,
    DuplicateShark,
    InternalError,
    MetadataUpdateFailed,
}

// Evacuate Object Error
impl ToSql<sql_types::Text, Sqlite> for EvacuateObjectError {
    fn to_sql<W: Write>(
        &self,
        out: &mut Output<W, Sqlite>,
    ) -> serialize::Result {
        let s = self.to_string();
        out.write_all(s.as_bytes())?;
        Ok(IsNull::No)
    }
}

impl FromSql<sql_types::Text, Sqlite> for EvacuateObjectError {
    fn from_sql(
        bytes: Option<backend::RawValue<Sqlite>>,
    ) -> deserialize::Result<Self> {
        let t = not_none!(bytes).read_text();
        Self::from_str(t).map_err(std::convert::Into::into)
    }
}

// --- END Diesel Stuff --- //

struct FiniMsg;

#[derive(Debug)]
pub struct SharkSpotterObject {
    pub shard: i32,
    pub object: MantaObject,
    pub etag: String,
}

impl Default for EvacuateObjectStatus {
    fn default() -> Self {
        EvacuateObjectStatus::Unprocessed
    }
}

/// Wrap a given MantaObject in another structure so that we can track it's
/// progress through the evacuation process.
#[derive(
    Insertable,
    Queryable,
    Identifiable,
    AsChangeset,
    Debug,
    Default,
    Clone,
    PartialEq,
)]
#[table_name = "evacuateobjects"]
#[changeset_options(treat_none_as_null = "true")]
pub struct EvacuateObject {
    pub id: ObjectId, // MantaObject ObjectId

    // UUID of assignment this object was most recently part of.
    pub assignment_id: AssignmentId,

    pub object: MantaObject, // The MantaObject being rebalanced
    pub shard: i32,          // shard number of metadata object record
    pub etag: String,
    pub status: EvacuateObjectStatus,
    pub skipped_reason: Option<ObjectSkippedReason>,
    pub error: Option<EvacuateObjectError>,
    // TODO: Consider adding a free form status message.
    // We would/could do this as part of the skipped_reason or error enums,
    // but enum variants with fields doesn't work well with databases.
    // Perhaps a field here (message: String) that would contain data that
    // relates to either the skipped reason or the error (which ever is not
    // None).
    // Alternatively we could simply expand the skipped_reasons or errors to
    // be more specific.
}

impl EvacuateObject {
    fn new(ssobj: SharkSpotterObject) -> Self {
        Self {
            assignment_id: String::new(),
            id: ssobj.object.object_id.to_owned(),
            object: ssobj.object,
            shard: ssobj.shard,
            etag: ssobj.etag,
            ..Default::default()
        }
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

    /// domain_name of manta deployment
    pub domain_name: String,

    /// Logger
    pub log: Logger,

    /// Accumulator for total time spent on DB inserts. (test/dev)
    pub total_db_time: Mutex<u128>,
}

impl EvacuateJob {
    /// Create a new EvacauteJob instance.
    /// As part of this initialization also create a new SqliteConnection.
    pub fn new<S: Into<String>>(
        from_shark: S,
        domain_name: &str,
        db_url: &str,
    ) -> Self {
        let manta_storage_id = from_shark.into();
        let plain = slog_term::PlainSyncDecorator::new(std::io::stdout());
        let log = Logger::root(
            Mutex::new(slog_term::FullFormat::new(plain).build()).fuse(),
            o!("build-id" => "0.1.0"),
        );
        let conn = SqliteConnection::establish(db_url)
            .unwrap_or_else(|_| panic!("Error connecting to {}", db_url));

        Self {
            min_avail_mb: Some(1000),
            max_tasks_per_assignment: Some(1000),
            dest_shark_list: RwLock::new(HashMap::new()),
            assignments: RwLock::new(HashMap::new()),
            from_shark: MantaObjectShark {
                manta_storage_id,
                ..Default::default()
            },
            conn: Mutex::new(conn),
            total_db_time: Mutex::new(0),
            domain_name: domain_name.to_string(),
            log,
        }
    }

    fn create_table(&self) -> Result<usize, Error> {
        let status_strings = EvacuateObjectStatus::variants();
        let error_strings = EvacuateObjectError::variants();
        let mut skipped_strings: Vec<String> = vec![];

        for reason in ObjectSkippedReason::iter() {
            match reason {
                ObjectSkippedReason::HTTPStatusCode(_) => continue,
                _ => {
                    skipped_strings.push(reason.to_string());
                }
            }
        }

        for code in 100..600 {
            let reason = format!(
                "{{{}:{}}}",
                // This value doesn't matter.  The to_string() method only
                // returns the variant name.
                ObjectSkippedReason::HTTPStatusCode(0).to_string(),
                code
            );

            skipped_strings.push(reason);
        }

        let status_check = format!("'{}'", status_strings.join("', '"));
        let error_check = format!("'{}'", error_strings.join("', '"));
        let skipped_check = format!("'{}'", skipped_strings.join("', '"));

        // TODO: check if table exists first and if so issue warning.  We may
        // need to handle this a bit more gracefully in the future for
        // restarting jobs...

        let conn = self.conn.lock().expect("DB conn lock");
        conn.execute(r#"DROP TABLE evacuateobjects"#)
            .unwrap_or_else(|e| {
                debug!("Table doesn't exist: {}", e);
                0
            });

        let create_query = format!(
            "
            CREATE TABLE evacuateobjects(
                id TEXT PRIMARY KEY,
                assignment_id TEXT,
                object TEXT,
                shard Integer,
                etag TEXT,
                status TEXT CHECK(status IN ({})) NOT NULL,
                skipped_reason TEXT CHECK(skipped_reason IN ({})),
                error TEXT CHECK(error IN ({}))
            );",
            status_check, skipped_check, error_check
        );
        conn.execute(&create_query).map_err(Error::from)
    }

    pub fn run(self, config: &Config) -> Result<(), Error> {
        // We could call this from within the EvacuateJob constructor, but we may
        // want to enable job restarts.  So for now lets call it from here, and
        // depending on how we implement job restarts we may want to move it out.
        self.create_table()?;

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

        // TODO: should we "expect()" all these joins?  If we expect and
        // return Err() what does that mean for the other threads?
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

    fn set_assignment_state(
        &self,
        assignment_id: &str,
        state: AssignmentState,
    ) -> Result<(), Error> {
        match self
            .assignments
            .write()
            .expect("assignment write lock")
            .get_mut(assignment_id)
        {
            Some(a) => {
                a.state = state;
                info!("Done processing assignment {}", &a.id);
            }
            None => {
                let msg = format!(
                    "Error updating assignment state for: {}",
                    &assignment_id
                );
                error!("{}", &msg);

                return Err(InternalError::new(
                    Some(InternalErrorCode::AssignmentLookupError),
                    msg,
                )
                .into());
            }
        }
        Ok(())
    }

    fn skip_assignment(
        &self,
        assign_id: &AssignmentId,
        skip_reason: ObjectSkippedReason,
        assignment_state: AssignmentState,
    ) {
        self.mark_assignment_skipped(assign_id, skip_reason);

        self.set_assignment_state(assign_id, assignment_state)
            .unwrap_or_else(|e| {
                // TODO: should we panic? if so just replace with expect()
                panic!("{}", e);
            });
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

    fn skip_object(
        &self,
        eobj: &mut EvacuateObject,
        reason: ObjectSkippedReason,
    ) -> Result<(), Error> {
        info!("Skipping object {}: {}.", &eobj.object.object_id, reason);

        eobj.status = EvacuateObjectStatus::Skipped;
        eobj.skipped_reason = Some(reason);
        self.insert_into_db(&eobj)?;
        Ok(())
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

        let locked_conn = self.conn.lock().expect("DB conn lock");
        let now = std::time::Instant::now();

        // TODO: Is panic the right thing to do here?
        // TODO: consider checking record count to ensure insert success
        let ret = diesel::insert_into(evacuateobjects)
            .values(obj)
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
    fn insert_many_into_db(
        &self,
        vec_objs: &[EvacuateObject],
    ) -> Result<usize, Error> {
        use self::evacuateobjects::dsl::*;

        let locked_conn = self.conn.lock().expect("db conn lock");
        let now = std::time::Instant::now();
        // TODO: consider checking record count to ensure update success
        let ret = diesel::insert_into(evacuateobjects)
            .values(vec_objs)
            .execute(&*locked_conn)
            .unwrap_or_else(|e| {
                let msg = format!("Error inserting object into DB: {}", e);
                error!("{}", msg);
                panic!(msg);
            });

        // TODO: remove.
        // Leave this in here for now so that we can use the functionality
        // quickly if we need to check something during development.
        let mut total_time = self.total_db_time.lock().expect("DB time lock");
        *total_time += now.elapsed().as_millis();

        Ok(ret)
    }

    // There are other possible approaches here, and we should check to see which one performs the
    // best.  MANTA-4578
    fn mark_many_objects(
        &self,
        vec_obj_ids: Vec<String>,
        to_status: EvacuateObjectStatus,
    ) -> usize {
        use self::evacuateobjects::dsl::*;

        let locked_conn = self.conn.lock().expect("db conn lock");
        let len = vec_obj_ids.len();

        debug!("Marking {} objects as {:?}", len, to_status);
        let now = std::time::Instant::now();
        // TODO: consider checking record count to ensure update success
        let ret = diesel::update(evacuateobjects)
            .filter(id.eq_any(vec_obj_ids))
            .set(status.eq(to_status))
            .execute(&*locked_conn)
            .unwrap_or_else(|e| {
                let msg = format!("LocalDB: Error updating {}", e);
                error!("{}", msg);
                panic!(msg);
            });
        debug!(
            "eq_any update of {} took {}ms",
            len,
            now.elapsed().as_millis()
        );

        ret
    }

    // TODO: MANTA-4585
    fn mark_assignment_skipped(
        &self,
        assignment_uuid: &str,
        reason: ObjectSkippedReason,
    ) -> usize {
        use self::evacuateobjects::dsl::{
            assignment_id, error, evacuateobjects, skipped_reason, status,
        };
        let locked_conn = self.conn.lock().expect("DB conn lock");

        debug!(
            "Marking objects in assignment ({}) as skipped:{:?}",
            assignment_uuid, reason
        );
        // TODO: consider checking record count to ensure update success
        diesel::update(evacuateobjects)
            .filter(assignment_id.eq(assignment_uuid))
            .set((
                status.eq(EvacuateObjectStatus::Skipped),
                skipped_reason.eq(Some(reason)),
                error.eq::<Option<EvacuateObjectError>>(None),
            ))
            .execute(&*locked_conn)
            .unwrap_or_else(|e| {
                let msg = format!(
                    "Error updating assignment: {} ({})",
                    assignment_uuid, e
                );
                error!("{}", msg);
                panic!(msg);
            })
    }

    // TODO: MANTA-4585
    fn mark_assignment_error(
        &self,
        assignment_uuid: &str,
        err: EvacuateObjectError,
    ) -> usize {
        use self::evacuateobjects::dsl::{
            assignment_id, error, evacuateobjects, skipped_reason, status,
        };
        let locked_conn = self.conn.lock().expect("DB conn lock");

        debug!(
            "Marking objects in assignment ({}) as error:{:?}",
            assignment_uuid, err
        );
        // TODO: consider checking record count to ensure update success
        diesel::update(evacuateobjects)
            .filter(assignment_id.eq(assignment_uuid))
            .set((
                status.eq(EvacuateObjectStatus::Error),
                skipped_reason.eq::<Option<ObjectSkippedReason>>(None),
                error.eq(Some(err)),
            ))
            .execute(&*locked_conn)
            .unwrap_or_else(|e| {
                let msg = format!(
                    "Error updating assignment: {} ({})",
                    assignment_uuid, e
                );
                error!("{}", msg);
                panic!(msg);
            })
    }

    // Given a vector of Tasks that need to be marked as skipped do the
    // following:
    // 1. Generate a hash of <ObjectSkippedReason, Vec<ObjectId>>
    // 2. For each "reason" do a bulk update of the associated objects.
    // The assumption here is that all Objects should be skipped and are not
    // errors.
    fn mark_many_task_objects_skipped(&self, task_vec: Vec<Task>) {
        use self::evacuateobjects::dsl::{
            evacuateobjects, id, skipped_reason, status,
        };

        let mut updates: HashMap<ObjectSkippedReason, Vec<String>> =
            HashMap::new();

        for t in task_vec {
            if let TaskStatus::Failed(reason) = t.status {
                let entry = updates.entry(reason).or_insert_with(|| vec![]);
                entry.push(t.object_id);
            } else {
                warn!("Attempt to skip object with status {:?}", t.status);
                continue;
            }
        }

        let locked_conn = self.conn.lock().expect("db conn lock");

        for (reason, vec_obj_ids) in updates {
            // TODO: consider checking record count to ensure update success
            diesel::update(evacuateobjects)
                .filter(id.eq_any(vec_obj_ids))
                .set((
                    status.eq(EvacuateObjectStatus::Skipped),
                    skipped_reason.eq(reason),
                ))
                .execute(&*locked_conn)
                .unwrap_or_else(|e| {
                    let msg = format!("LocalDB: Error updating {}", e);
                    error!("{}", msg);
                    panic!(msg);
                });
        }
    }

    fn mark_object_error(
        &self,
        object_id: &str, // ObjectId
        err: EvacuateObjectError,
    ) -> usize {
        use self::evacuateobjects::dsl::{
            error, evacuateobjects, id, skipped_reason, status,
        };

        let locked_conn = self.conn.lock().expect("db conn lock");

        // TODO: consider asserting that record count equals 1 here because
        // callers make the assumption that the update was successful.  If
        // the object is not updated then there was some error that needs to
        // be tracked, or possibly panic.
        diesel::update(evacuateobjects)
            .filter(id.eq(object_id))
            .set((
                status.eq(EvacuateObjectStatus::Error),
                error.eq(Some(err)),
                skipped_reason.eq::<Option<ObjectSkippedReason>>(None),
            ))
            .execute(&*locked_conn)
            .unwrap_or_else(|e| {
                let msg =
                    format!("Error updating assignment: {} ({})", object_id, e);
                error!("{}", msg);
                panic!(msg);
            })
    }

    /// Mark all objects with a given assignment ID with the specified
    /// EvacuateObjectStatus.  This should not be used for statuses of
    /// Skipped or Error as those require reasons.  See asserts below.
    fn mark_assignment_objects(
        &self,
        id: &str,
        to_status: EvacuateObjectStatus,
    ) -> usize {
        use self::evacuateobjects::dsl::{
            assignment_id, error, evacuateobjects, skipped_reason, status,
        };

        assert_ne!(to_status, EvacuateObjectStatus::Skipped);
        assert_ne!(to_status, EvacuateObjectStatus::Error);

        let locked_conn = self.conn.lock().expect("DB conn lock");

        debug!("Marking objects in assignment ({}) as {:?}", id, to_status);
        diesel::update(evacuateobjects)
            .filter(assignment_id.eq(id))
            .set((
                status.eq(to_status),
                error.eq::<Option<EvacuateObjectError>>(None),
                skipped_reason.eq::<Option<ObjectSkippedReason>>(None),
            ))
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

    fn load_assignment_objects(
        &self,
        id: &str,
        status_filter: EvacuateObjectStatus,
    ) -> Vec<EvacuateObject> {
        use self::evacuateobjects::dsl::{
            assignment_id, evacuateobjects, status,
        };

        let locked_conn = self.conn.lock().expect("DB conn");

        evacuateobjects
            .filter(assignment_id.eq(id))
            .filter(status.eq(status_filter))
            .load::<EvacuateObject>(&*locked_conn)
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
) {
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
        let res = match client.post(&agent_uri).json(&payload).send() {
            Ok(r) => r,
            Err(e) => {
                // TODO: Should we blacklist this destination shark?
                self.skip_assignment(
                    &assignment.id,
                    ObjectSkippedReason::DestinationUnreachable,
                    AssignmentState::AgentUnavailable,
                );

                return Err(e.into());
            }
        };

        if !res.status().is_success() {
            // TODO: Should we blacklist this destination shark?
            error!(
                "Error posting assignment {} to {} ({})",
                payload.id,
                assignment.dest_shark.manta_storage_id,
                res.status()
            );

            self.skip_assignment(
                &assignment.id,
                ObjectSkippedReason::AssignmentRejected,
                AssignmentState::Rejected,
            );

            return Err(
                InternalError::new(None, "Error posting assignment").into()
            );
        }

        debug!("Post of {} was successful", payload.id);
        assignment_post_success(self, assignment);
        Ok(())
    }
}

impl GetAssignment for EvacuateJob {
    fn get(&self, assignment: &Assignment) -> Result<AgentAssignment, Error> {
        let uri = format!(
            "http://{}:7878/assignments/{}",
            assignment.dest_shark.manta_storage_id, assignment.id
        );

        debug!("Getting Assignment: {:?}", uri);
        match reqwest::get(&uri) {
            Ok(mut resp) => {
                if !resp.status().is_success() {
                    self.skip_assignment(
                        &assignment.id,
                        ObjectSkippedReason::AgentAssignmentNoEnt,
                        AssignmentState::AgentUnavailable,
                    );

                    let msg = format!(
                        "Could not get assignment from Agent: {:#?}",
                        resp
                    );
                    return Err(InternalError::new(
                        Some(InternalErrorCode::AssignmentGetError),
                        msg,
                    )
                    .into());
                }
                debug!("RET: {:#?}", resp);
                resp.json::<AgentAssignment>().map_err(Error::from)
            }
            Err(e) => {
                self.skip_assignment(
                    &assignment.id,
                    ObjectSkippedReason::NetworkError,
                    AssignmentState::AgentUnavailable,
                );

                Err(e.into())
            }
        }
    }
}

impl UpdateMetadata for EvacuateJob {
    /// This function only updates the local database in the event that an
    /// error is encountered.  This allows the caller to easily batch database
    /// updates for successful objects.
    fn update_object_shark(
        &self,
        mut object: MantaObject,
        new_shark: &StorageNode,
        etag: String,
        mclient: &mut MorayClient,
    ) -> Result<MantaObject, Error> {
        let old_shark = &self.from_shark;
        let log = self.log.clone();

        // Replace shark value
        let mut shark_found = false;
        for shark in object.sharks.iter_mut() {
            if shark.manta_storage_id == old_shark.manta_storage_id {
                shark.manta_storage_id = new_shark.manta_storage_id.clone();
                shark.datacenter = new_shark.datacenter.clone();
                if !shark_found {
                    shark_found = true;
                } else {
                    let msg =
                        format!("Found duplicate shark while attempting \
                        to update metadata. Manta Object: {:?}, New Shark: \
                        {:?}",
                         object, new_shark);
                    error!("{}", &msg);
                    self.mark_object_error(
                        &object.object_id,
                        EvacuateObjectError::DuplicateShark,
                    );
                    return Err(InternalError::new(
                        Some(InternalErrorCode::DuplicateShark),
                        msg,
                    )
                    .into());
                }
            }
        }

        if let Err(e) = moray_client::put_object(mclient, &object, &etag, log) {
            self.mark_object_error(
                &object.object_id,
                EvacuateObjectError::MetadataUpdateFailed,
            );
            return Err(e);
        }

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

                // Note that we don't set the assignment state here because
                // we JUST tried looking it up and that failed.  The best we
                // can do is update the associated objects in the local DB
                // with this function call below.
                self.mark_assignment_skipped(
                    &agent_assignment.uuid,
                    ObjectSkippedReason::AssignmentMismatch,
                );
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
                self.mark_assignment_objects(
                    &assignment.id,
                    EvacuateObjectStatus::PostProcessing,
                );
                assignment.state = AssignmentState::AgentComplete;
            }
            AgentAssignmentState::Complete(Some(failed_tasks)) => {
                info!("Assignment {} resulted in {} failed tasks.",
                      &assignment.id, failed_tasks.len());
                trace!("{:#?}", &failed_tasks);

                // failed_tasks: Vec<Task>
                // assignment.tasks: HashMap<ObjectId, Vec<Task>>
                //
                // So we iterate over the keys of assignment.tasks (which
                // are object ids), and filter out those that are in the
                // failed_tasks Vec.
                let successful_tasks: Vec<ObjectId> = assignment
                    .tasks
                    .keys()
                    .filter(|obj_id| {
                        !failed_tasks.iter().any(|ft| &ft.object_id == *obj_id)
                    })
                    .cloned()
                    .collect();

                self.mark_many_task_objects_skipped(failed_tasks);
                self.mark_many_objects(
                    successful_tasks,
                    EvacuateObjectStatus::PostProcessing,
                );

                assignment.state = AssignmentState::AgentComplete;
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
            sharkspotter::run(config, log, move |object, shard, etag| {
                trace!("Sharkspotter discovered object: {:#?}", &object);
                // while testing, limit the number of objects processed for now
                count += 1;
                if count > 20 {
                    return Err(std::io::Error::new(
                        ErrorKind::Other,
                        "Just stop already",
                    ));
                }

                // TODO: build a test for this
                if shard > std::i32::MAX as u32 {
                    error!(
                        "Found shard number over int32 max for: {}",
                        object.object_id
                    );

                    let eobj = EvacuateObject {
                        id: object.object_id.clone(),
                        object: object.clone(),
                        status: EvacuateObjectStatus::Error,
                        error: Some(EvacuateObjectError::BadShardNumber),
                        etag,
                        ..Default::default()
                    };

                    job_action
                        .insert_into_db(&eobj)
                        .expect("Error inserting bad EvacuateObject into DB");

                    return Err(std::io::Error::new(
                        ErrorKind::Other,
                        "Exceeded max number of shards",
                    ));
                }

                let ssobj = SharkSpotterObject {
                    shard: shard as i32,
                    object: object.clone(),
                    etag,
                };

                obj_tx
                    .send(ssobj)
                    .map_err(CrossbeamError::from)
                    .map_err(|e| {
                        error!("Sharkspotter: Error sending object: {}", e);
                        job_action.mark_object_error(
                            &object.object_id,
                            EvacuateObjectError::InternalError,
                        );
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

fn validate_destination(
    obj: &MantaObject,
    evac_shark: &MantaObjectShark,
    dest_shark: &StorageNode,
) -> Option<ObjectSkippedReason> {
    let obj_on_dest = obj
        .sharks
        .iter()
        .any(|s| s.manta_storage_id == dest_shark.manta_storage_id);

    // We've found the object on the destination shark.  We will need to skip
    // this object for now and find a destination for it later.  If we don't do
    // this check it would reduce the durability level of the object.  That is,
    // it would reduce the number of copies of the object in the region by one.
    if obj_on_dest {
        return Some(ObjectSkippedReason::ObjectAlreadyOnDestShark);
    }

    // It's ok to send an object to a storage node that is in the same
    // data center as the one being evacuated.  However, in order to ensure
    // we do not decrease the fault domain, we do not want to remove a copy from
    // a data center and add an additional copy to another data center that
    // already has a copy of this object.
    // So if the destination is NOT in the same data center, and this object
    // already has a copy in the destination data center, we must skip for now.
    if evac_shark.datacenter != dest_shark.datacenter
        && obj
            .sharks
            .iter()
            .any(|s| s.datacenter == dest_shark.datacenter)
    {
        return Some(ObjectSkippedReason::ObjectAlreadyInDatacenter);
    }
    None
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
                    "Filling up new assignment for {} with max_tasks: {:#?}, \
                     and available_space: {}",
                    &assignment.dest_shark.manta_storage_id,
                    &max_tasks,
                    &available_space
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
                        job_action.skip_object(
                            &mut eobj,
                            ObjectSkippedReason::DestinationInsufficientSpace,
                        )?;

                        break;
                    }

                    let obj = &eobj.object;

                    if let Some(reason) = validate_destination(
                        obj,
                        &job_action.from_shark,
                        &assignment.dest_shark,
                    ) {
                        job_action.skip_object(&mut eobj, reason)?;
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
                            job_action.skip_object(
                                &mut eobj,
                                ObjectSkippedReason::SourceIsEvacShark,
                            )?;
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

                    let assignment_uuid = assignment.id.clone();
                    full_assignment_tx.send(assignment).map_err(|e| {
                        error!("Error sending assignment to be posted: {}", e);

                        job_action.mark_assignment_error(
                            &assignment_uuid,
                            EvacuateObjectError::InternalError,
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
                    info!(
                        "Posting Assignment: {}\n",
                        serde_json::to_string(&assignment)?
                    );

                    match job_action.post(assignment) {
                        Ok(()) => (),
                        Err(e) => {
                            // Just log here.  Marking the errors in the local
                            // DB is handled by the post() function.
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
        etag: String,
        mclient: &mut MorayClient,
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

                // We'd rather not hold the assignment hash lock here while we
                // run through all the HTTP GETs and while each completed
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
                // assignments could grow significantly and that approach
                // would have a significantly negative impact on performance.
                let assignments = job_action
                    .assignments
                    .read()
                    .expect("assignments read lock")
                    .clone();

                debug!("Checking Assignments");

                let outstanding_assignments = || {
                    assignments.values().any(|a| {
                        a.state == AssignmentState::Assigned
                            || a.state == AssignmentState::Init
                    })
                };

                if !run && !outstanding_assignments() {
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
                            // Assignment and its associated objects are
                            // marked as skipped in the get() function.
                            // TODO: Should the assignment marking happen
                            // here instead?
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
                            // We don't want to shut this thread down simply
                            // because we have issues handling one assignment.
                            // The process() function should mark the
                            // associated objects appropriately.
                            job_action.process(ag_assignment).unwrap_or_else(
                                |e| {
                                    error!("Error Processing Assignment {}", e);
                                },
                            );
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
                            // here rui  Do you need to set the assignment
                            // state in the hash?  no because we are exiting,
                            // right?
                            job_action.mark_assignment_error(
                                &assignment.id,
                                EvacuateObjectError::InternalError,
                            );
                            error!(
                                "Assignment Checker: Error sending \
                                 assignment to the metadata \
                                 broker {}",
                                e
                            );
                            return Err(InternalError::new(
                                Some(InternalErrorCode::Crossbeam),
                                CrossbeamError::from(e).description(),
                            )
                            .into());
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

// This worker continues to run as long as the queue has entries for it to
// work on.  If, when the worker attempts to "steal" from the queue, the
// queue is emtpy the worker exits.
fn metadata_update_worker(
    job_action: Arc<EvacuateJob>,
    queue_front: Arc<Injector<Assignment>>,
) -> impl Fn() {
    move || {
        // For each worker we create a hash of moray clients indexed by shard.
        // If the worker exits then the clients and the associated
        // connections are dropped.  This avoids having to place locks around
        // the shard connections.  It also allows us to manage our max
        // number of per-shard connections by simply tuning the number of
        // metadata update worker threads.
        let mut client_hash: HashMap<u32, MorayClient> = HashMap::new();

        loop {
            let assignment = match queue_front.steal() {
                Steal::Success(a) => a,
                Steal::Retry => continue,
                Steal::Empty => break,
            };

            let mut updated_objects = vec![];
            let dest_shark = &assignment.dest_shark;
            let objects = job_action.load_assignment_objects(
                &assignment.id,
                EvacuateObjectStatus::PostProcessing,
            );

            for obj in objects {
                let etag = obj.etag.clone();
                let mut mobj: MantaObject = obj.object.clone();

                // Unfortunately sqlite only accepts signed integers.  So we
                // have to do the conversion here and cross our fingers that
                // we don't have more than 2.1 billion shards.
                // We do check this value coming in from sharkspotter as well.
                if obj.shard < 0 {
                    job_action.mark_object_error(
                        &obj.object.object_id,
                        EvacuateObjectError::BadShardNumber,
                    );

                    // TODO: panic for now, but for release we should
                    // continue to next object.
                    panic!("Cannot have a negative shard {:#?}", obj);
                }
                let shard = obj.shard as u32;

                // We can't use or_insert_with() here because in the event
                // that client creation fails we want to handle that error.
                let mclient = match client_hash.entry(shard) {
                    Occupied(entry) => entry.into_mut(),
                    Vacant(entry) => {
                        let client = match moray_client::create_client(
                            shard,
                            &job_action.domain_name,
                            &job_action.log,
                        ) {
                            Ok(client) => client,
                            Err(e) => {
                                job_action.mark_object_error(
                                    &obj.id,
                                    EvacuateObjectError::BadMorayClient,
                                );
                                format!(
                                    "MD Update Worker: failed to get moray \
                                     client for shard number {}. Cannot update \
                                     metadata for {:#?}\n{}",
                                    shard, mobj, e
                                );

                                continue;
                            }
                        };
                        entry.insert(client)
                    }
                };

                // This function updates the manta object with the new
                // sharks in the Manta Metadata tier, and then returns the
                // updated Manta metadata object.  It only updates the state of
                // the associated EvacuateObject in the local database if an
                // error is encountered.  It is done this way in order to
                // batch database updates in the happy path.
                mobj = match job_action
                    .update_object_shark(mobj, dest_shark, etag, mclient)
                {
                    Ok(o) => o,
                    Err(e) => {
                        error!(
                            "MD Update worker: Error updating \n\n{:#?}, with \
                             dest_shark {:?}\n\n{}",
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
            match job_action.set_assignment_state(
                &assignment.id,
                AssignmentState::PostProcessed,
            ) {
                Ok(()) => (),
                Err(e) => panic!("{}", e),
            }

            // https://stackoverflow
            // .com/questions/47626047/execute-an-insert-or-update-using-diesel
            // TODO: batch update all objects in `updated_objects` with
            // EvacuateObjectStatus::Complete in the local DB meaning we are
            // completely done and this object has been rebalanced.
            // This is the finish line.

            // TODO: check for DB insert error
            job_action.mark_many_objects(
                updated_objects,
                EvacuateObjectStatus::Complete,
            );
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
    use crate::agent::AgentAssignmentStats;
    use crate::picker::PickerAlgorithm;
    use crate::util;
    use quickcheck::{Arbitrary, StdThreadGen};
    use quickcheck_helpers::random::string as random_string;
    use rand::Rng;

    fn generate_storage_node(local: bool) -> StorageNode {
        let mut rng = rand::thread_rng();
        let available_mb: u64 = rng.gen();
        let percent_used: u8 = rng.gen_range(0, 101);
        let filesystem: String = util::random_string(rng.gen_range(1, 20));
        let datacenter: String = util::random_string(rng.gen_range(1, 20));
        let manta_storage_id = match local {
            true => String::from("localhost"),
            false => format!("{}.stor.joyent.us", rng.gen_range(1, 100)),
        };
        let timestamp: u64 = rng.gen();

        StorageNode {
            available_mb,
            percent_used,
            filesystem,
            datacenter,
            manta_storage_id,
            timestamp,
        }
    }

    fn generate_sharks(num_sharks: u8, local_only: bool) -> Vec<StorageNode> {
        let mut ret = vec![];

        for _ in 0..num_sharks {
            ret.push(generate_storage_node(local_only));
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
    fn assignment_processing_test() {
        let mut g = StdThreadGen::new(10);
        let job_action = EvacuateJob::new(
            String::from("1.stor.fakedomain.us"),
            "fakedomain.us",
            "assignment_processing_test.db",
        );

        // Create the database table
        assert!(job_action.create_table().is_ok());

        // Create a vector to hold the evacuate objects and a new assignment.
        let mut eobjs = vec![];
        let mut assignment = Assignment::new(StorageNode::arbitrary(&mut g));

        // We will use this uuid throughout the test.
        let uuid = assignment.id.clone();

        // Create some EvacuateObjects
        for _ in 0..100 {
            let mobj = MantaObject::arbitrary(&mut g);
            let ssobj = SharkSpotterObject {
                shard: 1,
                object: mobj,
                etag: random_string(&mut g, 10),
            };

            let mut eobj = EvacuateObject::new(ssobj);
            eobj.assignment_id = uuid.clone();
            eobjs.push(eobj);
        }

        // Put the EvacuateObject's into the DB so that the process function
        // can look them up later.
        job_action
            .insert_many_into_db(&eobjs)
            .expect("process test: insert many");

        let mut tasks = HashMap::new();
        let mut failed_tasks = vec![];

        // We create a hash of the counts of each arbitrarily generated
        // status.  This way we can test to ensure that we end up with exactly
        // the right amount of EvacuateObject's each with the correct status in
        // the DB.
        let mut status_hash = HashMap::new();

        for i in 0..eobjs.len() {
            let mut task = Task::arbitrary(&mut g);
            task.object_id = eobjs[i].object.object_id.clone();

            if i % 2 != 0 {
                let stat = ObjectSkippedReason::arbitrary(&mut g);
                let entry_count = status_hash.entry(stat).or_insert(0);

                *entry_count += 1;
                task.status = TaskStatus::Failed(stat);
                failed_tasks.push(task.clone());
            } else {
                task.status = TaskStatus::Complete;
            }
            tasks.insert(task.object_id.clone(), task);
        }

        // Add the tasks, and pretend as though this assignment has been
        // assigned.
        assignment.tasks = tasks.clone();
        assignment.state = AssignmentState::Assigned;

        let mut assignments =
            job_action.assignments.write().expect("write lock");
        assignments.insert(uuid.clone(), assignment);

        drop(assignments);

        let mut agent_assignment_stats = AgentAssignmentStats::new(10);
        agent_assignment_stats.state =
            AgentAssignmentState::Complete(Some(failed_tasks));

        let agent_assignment = AgentAssignment {
            uuid: uuid.clone(),
            stats: agent_assignment_stats,
            tasks: vec![],
        };

        job_action
            .process(agent_assignment)
            .expect("Process assignment");

        use super::evacuateobjects::dsl::{
            assignment_id, evacuateobjects, skipped_reason, status,
        };

        let locked_conn = job_action.conn.lock().expect("DB conn");

        let records: Vec<EvacuateObject> = evacuateobjects
            .filter(assignment_id.eq(&uuid))
            .filter(status.eq(EvacuateObjectStatus::Skipped))
            .load::<EvacuateObject>(&*locked_conn)
            .expect("getting filtered objects");

        debug!("records: {:#?}", &records);
        assert_eq!(records.len(), eobjs.len() / 2);

        status_hash.iter().for_each(|(reason, count)| {
            debug!("Checking {:#?}, count of {}", reason, count);
            let skipped_reason_records = evacuateobjects
                .filter(assignment_id.eq(&uuid))
                .filter(status.eq(EvacuateObjectStatus::Skipped))
                .filter(skipped_reason.eq(reason))
                .load::<EvacuateObject>(&*locked_conn)
                .expect("getting filtered objects");

            assert_eq!(skipped_reason_records.len(), *count as usize);
        });
    }

    #[test]
    fn empty_picker_test() {
        let picker = Arc::new(EmptyPicker {});
        let (empty_assignment_tx, _) = crossbeam::bounded(5);
        let (checker_fini_tx, _) = crossbeam::bounded(1);

        // These tests are run locally and don't go out over the network so any properly formatted
        // host/domain name is valid here.
        let job_action = EvacuateJob::new(
            String::from("1.stor.fakedomain.us"),
            "fakedomain.us",
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
    fn validate_destination_test() {
        let mut g = StdThreadGen::new(10);
        let obj = MantaObject::arbitrary(&mut g);

        // Currently, the arbitrary implementation for a MantaObject
        // automatically gives it 2 sharks by default.  If that ever changes
        // in the future, this assertion will fail, letting us know that we
        // need to revisit it.  Until then, it seems more count on 2 sharks.
        assert_eq!(
            obj.sharks.len(),
            2,
            "Expected two sharks as part of the MantaObject."
        );

        let from_shark = obj.sharks[0].clone();
        let mut to_shark = generate_storage_node(true);

        // Test evacuation to different shark in the same datacenter.
        to_shark.datacenter = from_shark.datacenter.clone();
        assert!(
            validate_destination(&obj, &from_shark, &to_shark).is_none(),
            "Failed to evacuate to another shark in the same data center."
        );

        // Test compromising fault domain.
        to_shark.datacenter = obj.sharks[1].datacenter.clone();
        assert_eq!(
            validate_destination(&obj, &from_shark, &to_shark),
            Some(ObjectSkippedReason::ObjectAlreadyInDatacenter),
            "Attempt to place more than one object in the same data center."
        );

        // Test evacuating an object to the mako being evacuated.
        to_shark.manta_storage_id = from_shark.manta_storage_id.clone();
        assert_eq!(
            validate_destination(&obj, &from_shark, &to_shark),
            Some(ObjectSkippedReason::ObjectAlreadyOnDestShark),
            "Attempt to evacuate an object back to its source."
        );

        // Test evacuating an object to a shark it is already on.
        to_shark.manta_storage_id = obj
            .sharks
            .iter()
            .find(|s| s.manta_storage_id != from_shark.manta_storage_id)
            .expect(
                "Should be able to find a shark that this object is not \
                 already on",
            )
            .manta_storage_id
            .clone();

        assert_eq!(
            validate_destination(&obj, &from_shark, &to_shark),
            Some(ObjectSkippedReason::ObjectAlreadyOnDestShark),
            "Attempt to evacuate an object back to its source."
        );
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

        // These tests are run locally and don't go out over the network so any properly formatted
        // host/domain name is valid here.
        let job_action = EvacuateJob::new(
            String::from("1.stor.fakedomain.us"),
            "region.fakedomain.us",
            "full_test.db",
        );

        // Create the database table
        assert!(job_action.create_table().is_ok());

        let job_action = Arc::new(job_action);

        let mut test_objects = vec![];

        let mut g = StdThreadGen::new(10);
        for _ in 0..2 {
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
                        // TODO
                        etag: String::from("Fake_etag"),
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
