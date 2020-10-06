/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2020 Joyent, Inc.
 */

use crate::metrics::{
    metrics_error_inc, metrics_gauge_dec, metrics_gauge_inc, metrics_gauge_set,
    metrics_object_inc_by, metrics_skip_inc, metrics_skip_inc_by,
    ACTION_EVACUATE, MD_THREAD_GAUGE,
};
use rebalancer::common::{
    self, AssignmentPayload, ObjectId, ObjectSkippedReason, Task, TaskStatus,
};
use rebalancer::error::{
    CrossbeamError, Error, InternalError, InternalErrorCode,
};
use rebalancer::libagent::{
    AgentAssignmentState, Assignment as AgentAssignment,
};
use rebalancer::util::{MAX_HTTP_STATUS_CODE, MIN_HTTP_STATUS_CODE};

use crate::config::{Config, MAX_TUNABLE_MD_UPDATE_THREADS};
use crate::jobs::{
    assignment_cache_usage, Assignment, AssignmentCacheEntry, AssignmentId,
    AssignmentState, JobUpdateMessage, StorageId,
};
use crate::moray_client;
use crate::pg_db;
use crate::storinfo::{self as mod_storinfo, SharkSource, StorageNode};

use std::collections::hash_map::Entry::{Occupied, Vacant};
use std::collections::HashMap;
use std::convert::TryFrom;
use std::error::Error as _Error;
use std::io::Write;
use std::str::FromStr;
use std::string::ToString;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::thread;
use std::time::Duration;

use crossbeam_channel as crossbeam;
use crossbeam_channel::TryRecvError;
use crossbeam_deque::{Injector, Steal};
use libmanta::moray::{MantaObject, MantaObjectShark};
use moray::client::MorayClient;
use moray::objects::{
    BatchPutOp, BatchRequest, Etag, MethodOptions as ObjectMethodOptions,
};
use quickcheck::{Arbitrary, Gen};
use quickcheck_helpers::random::string as random_string;
use rand::seq::SliceRandom;
use reqwest;
use serde::{self, Deserialize, Serialize};
use serde_json::Value;
use strum::IntoEnumIterator;
use threadpool::ThreadPool;
use uuid::Uuid;

type EvacuateObjectValue = Value;
type MorayClientHash = HashMap<u32, MorayClient>;

// --- Diesel Stuff, TODO This should be refactored --- //

const EVACUATE_OBJECTS_DB: &str = "evacuateobjects";

use diesel::deserialize::{self, FromSql};
use diesel::pg::{Pg, PgConnection, PgValue};
use diesel::prelude::*;
use diesel::result::Error::DatabaseError;
use diesel::result::{DatabaseErrorInformation, DatabaseErrorKind};
use diesel::serialize::{self, IsNull, Output, ToSql};
use diesel::sql_types;
use sharkspotter::SharkspotterMessage;
use std::thread::JoinHandle;

// Note: The ordering of the fields in this table must match the ordering of
// the fields in 'struct EvacuateObject'
table! {
    use diesel::sql_types::{Text, Integer, Nullable, Jsonb};
    evacuateobjects (id) {
        id -> Text,
        assignment_id -> Text,
        object -> Jsonb,
        shard -> Integer,
        dest_shark -> Text,
        etag -> Text,
        status -> Text,
        skipped_reason -> Nullable<Text>,
        error -> Nullable<Text>,
    }
}

table! {
    use diesel::sql_types::{Integer, Jsonb};
    config {
        id -> Integer,
        from_shark -> Jsonb,
    }
}

table! {
    use diesel::sql_types::{Text, Array, Integer};
    duplicates(id) {
        id -> Text,
        key -> Text,
        shards -> Array<Integer>,
    }
}

#[derive(Insertable, Queryable, Identifiable)]
#[table_name = "evacuateobjects"]
struct UpdateEvacuateObject<'a> {
    id: &'a str,
}

#[derive(Clone, Debug, Insertable, AsChangeset, Queryable, Serialize)]
#[table_name = "config"]
pub struct EvacuateJobDbConfig {
    id: i32,
    pub from_shark: Value,
}

#[derive(Clone, Debug, Insertable, AsChangeset, Queryable, Serialize)]
#[table_name = "duplicates"]
pub struct Duplicate {
    id: String,
    key: String,
    shards: Vec<i32>,
}

// The fields in this struct are a subset of those found in
// libmanta::MantaObject.  Unfortunately the schema for Manta Objects in
// the "manta" moray bucket is not consistent.  However each entry should
// have at least these fields and more.  The fields below are the minimum
// necessary to rebalance an object.  If we don't have them we can't safely
// rebalance this object.
#[derive(Deserialize)]
struct MantaObjectEssential {
    pub key: String,
    pub owner: String,

    #[serde(alias = "contentLength", default)]
    pub content_length: u64,

    #[serde(alias = "contentMD5", default)]
    pub content_md5: String,

    #[serde(alias = "objectId", default)]
    pub object_id: String,

    #[serde(default)]
    pub etag: String,

    #[serde(default)]
    pub sharks: Vec<MantaObjectShark>,
}

#[derive(
    Display,
    EnumString,
    EnumVariantNames,
    EnumIter,
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

impl Arbitrary for EvacuateObjectStatus {
    fn arbitrary<G: Gen>(g: &mut G) -> EvacuateObjectStatus {
        let i: usize = g.next_u32() as usize % Self::iter().count();
        Self::iter().nth(i).unwrap()
    }
}

impl ToSql<sql_types::Text, Pg> for EvacuateObjectStatus {
    fn to_sql<W: Write>(&self, out: &mut Output<W, Pg>) -> serialize::Result {
        let s = self.to_string();
        out.write_all(s.as_bytes())?;
        Ok(IsNull::No)
    }
}

impl FromSql<sql_types::Text, Pg> for EvacuateObjectStatus {
    fn from_sql(bytes: Option<PgValue<'_>>) -> deserialize::Result<Self> {
        let t: PgValue = not_none!(bytes);
        let t_str = String::from_utf8_lossy(t.as_bytes());
        Self::from_str(&t_str).map_err(std::convert::Into::into)
    }
}

#[derive(
    Display,
    EnumString,
    EnumVariantNames,
    EnumIter,
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
    BadMorayObject,
    BadMantaObject,
    BadShardNumber,
    DuplicateShark,
    InternalError,
    MetadataUpdateFailed,
    MissingSharks,
    BadContentLength,
}

impl Arbitrary for EvacuateObjectError {
    fn arbitrary<G: Gen>(g: &mut G) -> EvacuateObjectError {
        let i: usize = g.next_u32() as usize % Self::iter().count();
        Self::iter().nth(i).unwrap()
    }
}

// We implement EvacuateObjectError From rebalancer::Error so that we can
// easily map internal errors that occur during metadata update to the
// associated per object errors.  With this in place when we want to mark an
// object in the local database as "Error" we can simply call `.into()` on
// the internal error object.
impl From<Error> for EvacuateObjectError {
    /// Map a rebalancer error to the associated EvacuateObject Error.
    fn from(error: Error) -> Self {
        match error {
            Error::Internal(e) => match e.code {
                InternalErrorCode::BadMantaObject => {
                    EvacuateObjectError::BadMantaObject
                }
                InternalErrorCode::DuplicateShark => {
                    EvacuateObjectError::DuplicateShark
                }
                InternalErrorCode::BadMorayClient => {
                    EvacuateObjectError::BadMorayClient
                }
                InternalErrorCode::MetadataUpdateFailure => {
                    EvacuateObjectError::MetadataUpdateFailed
                }
                _ => EvacuateObjectError::InternalError,
            },
            _ => EvacuateObjectError::InternalError,
        }
    }
}

// Evacuate Object Error
impl ToSql<sql_types::Text, Pg> for EvacuateObjectError {
    fn to_sql<W: Write>(&self, out: &mut Output<W, Pg>) -> serialize::Result {
        let s = self.to_string();
        out.write_all(s.as_bytes())?;
        Ok(IsNull::No)
    }
}

impl FromSql<sql_types::Text, Pg> for EvacuateObjectError {
    fn from_sql(bytes: Option<PgValue<'_>>) -> deserialize::Result<Self> {
        let t: PgValue = not_none!(bytes);
        let t_str = String::from_utf8_lossy(t.as_bytes());
        Self::from_str(&t_str).map_err(std::convert::Into::into)
    }
}

enum MetadataClientOption<'a> {
    Client(&'a mut MorayClient),
    Hash(&'a mut MorayClientHash),
}

fn create_table_common(
    conn: &PgConnection,
    table_name: &str,
    create_query: &str,
) -> Result<usize, Error> {
    // TODO: check if table exists first and if so issue warning.  We may
    // need to handle this a bit more gracefully in the future for
    // restarting jobs.
    let drop_query = format!("DROP TABLE {}", table_name);

    if let Err(e) = conn.execute(&drop_query) {
        debug!("Table doesn't exist: {}", e);
    }

    conn.execute(&create_query).map_err(Error::from)
}

fn create_config_table(conn: &PgConnection) -> Result<usize, Error> {
    let create_query = "CREATE TABLE config(
        id Integer PRIMARY KEY,
        from_shark Jsonb
    );";

    create_table_common(conn, "config", create_query)
}

fn create_duplicate_table(conn: &PgConnection) -> Result<usize, Error> {
    let create_query = "CREATE TABLE duplicates(
        id TEXT PRIMARY KEY,
        key TEXT,
        shards Int[]
    );";

    create_table_common(conn, "duplicates", create_query)
}

// We only want to store a single configuration entry for the evacaute job.
// The reason we store it here instead of adding it on as a json blob to the
// rebalancer database's jobs table is because this keeps all the
// information for the evacuate job in a single location.  Doing so makes
// backing up the database after completion much easier.
fn update_evacuate_config_impl(
    conn: &PgConnection,
    from_shark: &MantaObjectShark,
) -> Result<usize, Error> {
    use self::config::dsl::{config as config_table, id as config_id};

    let from_shark_value =
        serde_json::to_value(from_shark).expect("MantaObjectShark to Value");
    let value = EvacuateJobDbConfig {
        id: 1,
        from_shark: from_shark_value,
    };

    let updated_records = diesel::insert_into(config_table)
        .values(&value)
        .on_conflict(config_id)
        .do_update()
        .set(&value)
        .execute(conn)
        .map_err(Error::from)?;

    if updated_records != 1 {
        let msg = format!(
            "Error updating evacuate job configuration.  Expected 1 record \
             to be updated, found {}",
            updated_records
        );

        error!("{}", msg);
        return Err(
            InternalError::new(Some(InternalErrorCode::DbQuery), msg).into()
        );
    };

    Ok(updated_records)
}

fn build_skipped_strings() -> Vec<String> {
    let mut skipped_strings: Vec<String> = vec![];

    for reason in ObjectSkippedReason::iter() {
        match reason {
            ObjectSkippedReason::HTTPStatusCode(_) => continue,
            _ => {
                skipped_strings.push(reason.to_string());
            }
        }
    }

    for code in MIN_HTTP_STATUS_CODE..MAX_HTTP_STATUS_CODE {
        let reason = format!(
            "{{{}:{}}}",
            // This value doesn't matter.  The to_string() method only
            // returns the variant name.
            ObjectSkippedReason::HTTPStatusCode(0).to_string(),
            code
        );

        skipped_strings.push(reason);
    }

    skipped_strings
}

pub fn create_evacuateobjects_table(
    conn: &PgConnection,
) -> Result<usize, Error> {
    let status_strings = EvacuateObjectStatus::variants();
    let error_strings = EvacuateObjectError::variants();
    let skipped_strings = build_skipped_strings();

    let status_check = format!("'{}'", status_strings.join("', '"));
    let error_check = format!("'{}'", error_strings.join("', '"));
    let skipped_check = format!("'{}'", skipped_strings.join("', '"));

    let create_query = format!(
        "
            CREATE TABLE evacuateobjects(
                id TEXT PRIMARY KEY,
                assignment_id TEXT,
                object Jsonb,
                shard Integer,
                dest_shark TEXT,
                etag TEXT,
                status TEXT CHECK(status IN ({})) NOT NULL,
                skipped_reason TEXT CHECK(skipped_reason IN ({})),
                error TEXT CHECK(error IN ({}))
            );",
        status_check, skipped_check, error_check
    );

    create_table_common(conn, EVACUATE_OBJECTS_DB, &create_query)?;

    conn.execute(
        "CREATE INDEX assignment_id on evacuateobjects (assignment_id);",
    )
    .map_err(Error::from)
}
// --- END Diesel Stuff --- //

struct FiniMsg;

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
    AsExpression,
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

    pub object: Value, // The MantaObject being rebalanced
    pub shard: i32,    // shard number of metadata object record
    pub dest_shark: String,
    pub etag: String, // Moray object "_etag"
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

impl Arbitrary for EvacuateObject {
    fn arbitrary<G: Gen>(g: &mut G) -> EvacuateObject {
        let manta_object = MantaObject::arbitrary(g);
        let manta_value = serde_json::to_value(manta_object.clone()).unwrap();
        let status = EvacuateObjectStatus::arbitrary(g);
        let mut skipped_reason = None;
        let mut error = None;
        let shard = g.next_u32() as i32 % 100;
        let dest_shark = random_string(g, 100);

        match status {
            EvacuateObjectStatus::Skipped => {
                skipped_reason = Some(ObjectSkippedReason::arbitrary(g));
            }
            EvacuateObjectStatus::Error => {
                error = Some(EvacuateObjectError::arbitrary(g));
            }
            _ => (),
        }

        EvacuateObject {
            id: Uuid::new_v4().to_string(),
            assignment_id: Uuid::new_v4().to_string(),
            object: manta_value,
            shard,
            etag: manta_object.etag, // This is a different etag
            dest_shark,
            status,
            skipped_reason,
            error,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum DestSharkStatus {
    Init,
    Assigned,
    Ready,
    Unavailable,
}

#[derive(Clone, Debug)]
pub struct EvacuateDestShark {
    pub shark: StorageNode,
    pub status: DestSharkStatus,
    pub assigned_mb: u64,
}

///
/// ```
/// use serde_json::json;
/// use manager::jobs::evacuate::EvacuateJobUpdateMessage;
///
/// let payload = json!({
///     "action": "set_metadata_threads",
///     "params": 30
/// });
///
/// let deserialized: EvacuateJobUpdateMessage = serde_json::from_value(payload).unwrap();
/// let EvacuateJobUpdateMessage::SetMetadataThreads(thr_count) = deserialized;
/// assert_eq!(thr_count, 30);
/// ```
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "action", content = "params", rename_all = "snake_case")]
pub enum EvacuateJobUpdateMessage {
    SetMetadataThreads(usize),
}

impl EvacuateJobUpdateMessage {
    pub fn validate(&self) -> Result<(), String> {
        #[allow(clippy::single_match)]
        match self {
            EvacuateJobUpdateMessage::SetMetadataThreads(num_threads) => {
                if *num_threads < 1 {
                    return Err(String::from(
                        "Cannot set metadata update threads below 1",
                    ));
                }

                // This is completely arbitrary, but intended to prevent the
                // rebalancer from hammering the metadata tier due to a fat
                // finger.  It is still possible to set this number higher
                // but only at the start of a job. See MANTA-5284.
                if *num_threads > MAX_TUNABLE_MD_UPDATE_THREADS {
                    return Err(format!(
                        "Cannot set metadata update threads \
                         above {}",
                        MAX_TUNABLE_MD_UPDATE_THREADS
                    ));
                }
            }
        }
        Ok(())
    }
}

enum DyanmicWorkerMsg {
    Data(AssignmentCacheEntry),
    Stop,
}

pub enum EvacuateJobType {
    Initial,
    Retry(String),
}

/// Evacuate a given shark
pub struct EvacuateJob {
    pub config: Config,

    /// Hash of destination sharks that may change during the job execution.
    pub dest_shark_hash: RwLock<HashMap<StorageId, EvacuateDestShark>>,

    /// Hash of in progress assignments.
    pub assignments: RwLock<HashMap<AssignmentId, AssignmentCacheEntry>>,

    /// The shark to evacuate.
    pub from_shark: MantaObjectShark,

    // TODO: remove this?
    /// The minimum available space for a shark to be considered a destination.
    pub min_avail_mb: Option<u64>,

    pub conn: Mutex<PgConnection>,

    pub post_client: reqwest::Client,

    pub get_client: reqwest::Client,

    pub bytes_transferred: AtomicU64,

    pub db_name: String,

    pub evac_type: EvacuateJobType,

    // Timer starts when first object is found and stops at the end of the job.
    pub object_movement_start_time: Mutex<Option<std::time::Instant>>,

    pub update_rx: Option<crossbeam_channel::Receiver<JobUpdateMessage>>,

    /// TESTING ONLY
    pub max_objects: Option<u32>,
}

impl TryFrom<SharkspotterMessage> for EvacuateObject {
    type Error = EvacuateObject;

    fn try_from(ss_msg: SharkspotterMessage) -> Result<Self, Self::Error> {
        let id = common::get_objectId_from_value(&ss_msg.manta_value)
            .expect("object ID from manta object Value");

        let mut eobj = EvacuateObject {
            id,
            object: ss_msg.manta_value,
            etag: ss_msg.etag,
            ..Default::default()
        };

        // TODO: build a test for this
        if ss_msg.shard > std::i32::MAX as u32 {
            error!("Found shard number over int32 max for: {}", eobj.id);

            eobj.status = EvacuateObjectStatus::Error;
            eobj.error = Some(EvacuateObjectError::BadShardNumber);
            return Err(eobj);
        }

        eobj.shard = ss_msg.shard as i32;

        Ok(eobj)
    }
}

impl EvacuateJob {
    /// Create a new EvacuateJob instance.
    /// As part of this initialization also create a new PgConnection.
    pub fn new(
        storage_id: String,
        config: &Config,
        db_name: &str,
        update_rx: Option<crossbeam_channel::Receiver<JobUpdateMessage>>,
        max_objects: Option<u32>,
    ) -> Result<Self, Error> {
        let mut job = Self::new_common(storage_id, config, db_name, update_rx)?;

        job.max_objects = max_objects;

        Ok(job)
    }

    pub fn retry(
        storage_id: String,
        config: &Config,
        db_name: &str,
        update_rx: Option<crossbeam_channel::Receiver<JobUpdateMessage>>,
        retry_uuid: &str,
    ) -> Result<Self, Error> {
        let mut job = Self::new_common(storage_id, config, db_name, update_rx)?;

        job.evac_type = EvacuateJobType::Retry(retry_uuid.to_string());
        job.max_objects = None;

        Ok(job)
    }

    fn new_common(
        storage_id: String,
        config: &Config,
        db_name: &str,
        update_rx: Option<crossbeam_channel::Receiver<JobUpdateMessage>>,
    ) -> Result<Self, Error> {
        let mut from_shark = MantaObjectShark::default();
        let conn = match pg_db::create_and_connect_db(db_name) {
            Ok(c) => c,
            Err(e) => {
                println!("Error creating Evacuate Job: {}", e);
                std::process::exit(1);
            }
        };

        create_evacuateobjects_table(&conn)?;
        create_config_table(&conn)?;
        create_duplicate_table(&conn)?;

        from_shark.manta_storage_id = storage_id;

        update_evacuate_config_impl(&conn, &from_shark)?;

        Ok(Self {
            config: config.to_owned(),
            min_avail_mb: Some(1000), // TODO: config
            dest_shark_hash: RwLock::new(HashMap::new()),
            assignments: RwLock::new(HashMap::new()),
            from_shark,
            conn: Mutex::new(conn),
            max_objects: Some(10),
            post_client: reqwest::Client::new(),
            get_client: reqwest::Client::new(),
            update_rx,
            evac_type: EvacuateJobType::Initial,
            db_name: db_name.to_string(),
            bytes_transferred: AtomicU64::new(0),
            object_movement_start_time: Mutex::new(None),
        })
    }

    pub fn create_tables(&self) -> Result<usize, Error> {
        let conn = self.conn.lock().expect("DB conn lock");
        create_evacuateobjects_table(&*conn)?;
        create_config_table(&*conn)
    }

    // This is not purely a validation function.  We do set the from_shark field
    // in here so that it has the complete manta_storage_id and datacenter.
    fn validate(&mut self) -> Result<(), Error> {
        let from_shark = moray_client::get_manta_object_shark(
            &self.from_shark.manta_storage_id,
            &self.config.domain_name,
        )?;

        self.from_shark = from_shark;

        Ok(())
    }

    fn update_evacuate_config(&self) -> Result<usize, Error> {
        let locked_conn = self.conn.lock().expect("DB conn lock");

        update_evacuate_config_impl(&locked_conn, &self.from_shark)
    }

    pub fn run(mut self) -> Result<(), Error> {
        self.validate()?;
        self.update_evacuate_config()?;

        let mut ret = Ok(());

        // job_action will be shared between threads so create an Arc for it.
        let job_action = Arc::new(self);

        // get what the evacuate job needs from the config structure
        let domain = &job_action.config.domain_name;
        let min_shard = job_action.config.min_shard_num();
        let max_shard = job_action.config.max_shard_num();

        // TODO: How big should each channel be?
        // Set up channels for thread to communicate.
        let (obj_tx, obj_rx): (
            crossbeam::Sender<EvacuateObject>,
            crossbeam::Receiver<EvacuateObject>,
        );
        let (full_assignment_tx, full_assignment_rx) = crossbeam::bounded(5);
        let (md_update_tx, md_update_rx) = crossbeam::bounded(5);
        let (checker_fini_tx, checker_fini_rx) = crossbeam::bounded(1);

        // TODO: lock evacuating server to readonly
        // TODO: add thread barriers MANTA-4457

        // Note that we use md_read_chunk_size for retry jobs both here in the
        // channel and in the limit for local_db_generator()'s local db query.
        // This way we can queue up objects that we read from the local db into
        // the channel while we querying for the next chunk from the local db.
        // The local db access is pretty slow when the job has on the order
        // of 50 million objects, so we want to make sure the
        // local_db_generator() is not waiting to put objects into the
        // channel while it could be running the next chunk query on the local
        // db.
        let obj_generator_thread = match &job_action.evac_type {
            EvacuateJobType::Initial => {
                let channel = crossbeam::bounded(100);
                obj_tx = channel.0;
                obj_rx = channel.1;
                start_sharkspotter(
                    obj_tx,
                    domain.as_str(),
                    Arc::clone(&job_action),
                    min_shard,
                    max_shard,
                )?
            }
            EvacuateJobType::Retry(retry_uuid) => {
                // start local db generator
                let channel = crossbeam::bounded(
                    job_action.config.options.md_read_chunk_size,
                );
                obj_tx = channel.0;
                obj_rx = channel.1;
                start_local_db_generator(
                    Arc::clone(&job_action),
                    obj_tx,
                    retry_uuid,
                )?
            }
        };

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

        // start storinfo thread which will periodically update the list of
        // available sharks.
        let mut storinfo = mod_storinfo::Storinfo::new(domain)?;
        storinfo.start().map_err(Error::from)?;
        let storinfo = Arc::new(storinfo);

        let assignment_manager = start_assignment_manager(
            full_assignment_tx,
            checker_fini_tx,
            obj_rx,
            Arc::clone(&job_action),
            Arc::clone(&storinfo),
        )?;

        // At this point the rebalance job is running and we are blocked at
        // the assignment_manager thread join.

        // TODO: should we "expect()" all these joins?
        match assignment_manager.join().expect("Assignment Manager") {
            Ok(()) => (),
            Err(e) => {
                if let Error::Internal(err) = e {
                    if err.code == InternalErrorCode::StorinfoError {
                        error!(
                            "Encountered empty storinfo on startup, exiting \
                             safely"
                        );
                        set_run_error(&mut ret, err);
                    } else {
                        panic!("Error {}", err);
                    }
                } else {
                    panic!("Error {}", e);
                }
            }
        }

        storinfo.fini();

        obj_generator_thread
            .join()
            .expect("Sharkspotter Thread")
            .unwrap_or_else(|e| {
                error!("Error joining sharkspotter: {}\n", e);
                set_run_error(&mut ret, e);
            });

        post_thread
            .join()
            .expect("Post Thread")
            .unwrap_or_else(|e| {
                error!("Error joining post thread: {}\n", e);
                set_run_error(&mut ret, e);
            });

        assignment_checker_thread
            .join()
            .expect("Checker Thread")
            .unwrap_or_else(|e| {
                error!("Error joining assignment checker thread: {}\n", e);
                set_run_error(&mut ret, e);
            });

        metadata_update_thread
            .join()
            .expect("MD Update Thread")
            .unwrap_or_else(|e| {
                error!("Error joining metadata update thread: {}\n", e);
                set_run_error(&mut ret, e);
            });

        info!(
            "Evacuate Job transferred {} bytes",
            job_action.bytes_transferred.load(Ordering::SeqCst)
        );

        ret
    }

    fn mark_objects_complete(&self, completed_objects: Vec<EvacuateObject>) {
        let mut obj_ids = vec![];

        for eobj in completed_objects.into_iter() {
            eobj.object
                .get("contentLength")
                .and_then(|cl| {
                    if let Some(bytes) = cl.as_u64() {
                        // TODO: metrics
                        self.bytes_transferred
                            .fetch_add(bytes, Ordering::SeqCst);
                    } else {
                        warn!("Could not get bytes as number from {}", cl);
                    }
                    Some(())
                })
                .or_else(|| {
                    warn!("Could not find contentLength for {}", eobj.id);
                    None
                });

            obj_ids.push(eobj.id);
        }

        debug!("Updated Objects: {:?}", obj_ids);
        metrics_object_inc_by(Some(ACTION_EVACUATE), obj_ids.len());
        self.mark_many_objects(obj_ids, EvacuateObjectStatus::Complete);
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
                info!("Set assignment state to {:?} for {}", a.state, a.id);
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

    #[allow(clippy::ptr_arg)]
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

        self.remove_assignment_from_cache(assign_id);
    }

    // Removes assignment cache entry from cache.  The cache entry is
    // implicitly dropped when the function returns.
    fn remove_assignment_from_cache(&self, assignment_id: &str) {
        let mut assignments =
            self.assignments.write().expect("assignments write");

        trace!(
            "Assignment Cache size: {} bytes",
            std::mem::size_of::<AssignmentCacheEntry>() * assignments.len()
        );

        let entry = assignments.remove(assignment_id);
        if entry.is_none() {
            warn!(
                "Attempt to remove assignment not in cache: {}",
                assignment_id
            );
        }

        debug_assert!(
            entry.is_some(),
            format!("Remove assignment not in hash: {}", assignment_id)
        );

        let initial_size = assignment_cache_usage(&assignments);
        assignments.shrink_to_fit();
        trace!(
            "Assignment cache reduced by {}",
            initial_size - assignment_cache_usage(&assignments)
        );
    }

    fn skip_object(
        &self,
        eobj: &mut EvacuateObject,
        reason: ObjectSkippedReason,
    ) {
        info!("Skipping object {}: {}.", &eobj.id, reason);
        metrics_skip_inc(Some(&reason.to_string()));

        eobj.status = EvacuateObjectStatus::Skipped;
        eobj.skipped_reason = Some(reason);
        self.insert_into_db(&eobj);
    }

    // This generates a new Assignment and sets the max_size with
    // get_shark_available_mb() which takes into account the outstanding
    // assignments for this shark.
    fn new_assignment(&self, shark: StorageNode) -> Result<Assignment, Error> {
        let mut assignment = Assignment::new(shark);

        assignment.max_size = self
            .get_shark_available_mb(&assignment.dest_shark.manta_storage_id)?;

        Ok(assignment)
    }

    // Source of truth for available storage space on a shark.
    #[allow(clippy::ptr_arg)]
    fn get_shark_available_mb(&self, shark: &StorageId) -> Result<u64, Error> {
        let dest_shark_hash = self
            .dest_shark_hash
            .read()
            .expect("get_shark_available_mb read lock");

        dest_shark_hash
            .get(shark)
            .ok_or_else(|| {
                let msg =
                    format!("Could not find dest_shark ({}) in hash", shark);
                InternalError::new(Some(InternalErrorCode::SharkNotFound), msg)
            })
            .and_then(|dest_shark| {
                Ok(_calculate_available_mb(
                    dest_shark,
                    self.config.max_fill_percentage,
                ))
            })
            .map_err(Error::from)
    }

    /// Iterate over a new set of storage nodes and update our destination
    /// shark hash accordingly.
    fn update_dest_sharks(&self, new_sharks: &[StorageNode]) {
        let mut dest_shark_hash = self
            .dest_shark_hash
            .write()
            .expect("update dest_shark_hash write lock");

        for sn in new_sharks.iter() {
            if let Some(dest_shark) =
                dest_shark_hash.get_mut(sn.manta_storage_id.as_str())
            {
                // This is the only place that can move a dest shark from the
                // Unavailable state to the Ready state.
                if dest_shark.status == DestSharkStatus::Unavailable {
                    dest_shark.status = DestSharkStatus::Ready;
                }

                // Update the available_mb as reported by storinfo.  We use
                // this along with dest_shark.assigned_mb to calculate the
                // available storage space in get_shark_available_mb().
                // Otherwise we should not modify available_mb directly.
                dest_shark.shark.available_mb = sn.available_mb;
            } else {
                // create new dest shark and add it to the hash
                let new_shark = EvacuateDestShark {
                    shark: sn.to_owned(),
                    status: DestSharkStatus::Init,
                    assigned_mb: 0,
                };
                debug!("Adding new destination shark {:?} ", new_shark);
                dest_shark_hash.insert(sn.manta_storage_id.clone(), new_shark);
            }
        }

        // Walk the list of our destination sharks, if it doesn't exist in
        // new_sharks Vec then mark it as unavailable.  We don't remove them
        // from the hash because they may re-appear in a subsequent query to
        // storinfo.  In that event we want to make sure that we are properly
        // tracking the available space as seen in get_shark_available_mb().
        for (sn_id, val) in dest_shark_hash.iter_mut() {
            if !new_sharks.iter().any(|s| &s.manta_storage_id == sn_id) {
                val.status = DestSharkStatus::Unavailable;
            }
        }
    }

    // Check for a new storinfo snapshot.
    // If the storinfo finds some sharks then update our hash.  If the
    // storinfo doesn't have any new sharks for us, then we will use
    // whatever is in the hash already.  If the list in our hash is also
    // empty then we sleep and retry.
    // Note that receiving back `None` from `storinfo.choose()` simply means
    // there is no update from the last time we asked so we should use the
    // existing value.
    // TODO: make retry delay configurable
    fn get_shark_list<S>(
        &self,
        storinfo: Arc<S>,
        algo: &mod_storinfo::DefaultChooseAlgorithm,
        retries: u16,
    ) -> Result<Vec<EvacuateDestShark>, Error>
    where
        S: SharkSource + 'static,
    {
        let mut shark_list: Vec<EvacuateDestShark> = vec![];
        let mut tries = 0;
        let shark_list_retry_delay = std::time::Duration::from_millis(500);

        trace!("Getting new shark list");
        while tries < retries {
            if let Some(valid_sharks) =
                storinfo.choose(&mod_storinfo::ChooseAlgorithm::Default(algo))
            {
                self.update_dest_sharks(&valid_sharks);
            }

            // Turn the shark hash into a list, and filter out any
            // unavailable sharks.
            shark_list = self
                .dest_shark_hash
                .read()
                .expect("dest_shark_hash read lock")
                .values()
                .filter(|v| v.status != DestSharkStatus::Unavailable)
                .map(|v| v.to_owned())
                .collect();

            if shark_list.is_empty() {
                warn!(
                    "Received empty list of sharks, will retry in {}ms",
                    shark_list_retry_delay.as_millis()
                );
                thread::sleep(shark_list_retry_delay);
            } else {
                break;
            }
            tries += 1;
        }

        if shark_list.is_empty() {
            Err(InternalError::new(
                Some(InternalErrorCode::StorinfoError),
                "No valid sharks available.",
            )
            .into())
        } else {
            // Rust sort methods sort from lowest to highest order.  We want
            // the sharks with the most available_mb at the beginning of the
            // list so we reverse the sort.
            shark_list.sort_by_key(|s| s.shark.available_mb);
            shark_list.as_mut_slice().reverse();
            Ok(shark_list)
        }
    }

    // TODO: Consider doing batched inserts: MANTA-4464.
    fn insert_into_db(&self, obj: &EvacuateObject) -> usize {
        use self::evacuateobjects::dsl::*;

        let locked_conn = self.conn.lock().expect("DB conn lock");

        match diesel::insert_into(evacuateobjects)
            .values(obj)
            .execute(&*locked_conn)
        {
            Ok(num_records) => num_records,
            Err(DatabaseError(DatabaseErrorKind::UniqueViolation, _)) => {
                self.insert_duplicate_with_existing(obj, &*locked_conn);
                1
            }
            Err(e) => {
                let msg = format!("Error inserting object into DB: {}", e);
                error!("{}", msg);
                panic!(msg);
            }
        }
    }

    fn insert_duplicate_with_existing(
        &self,
        eobj: &EvacuateObject,
        conn: &PgConnection,
    ) {
        use self::evacuateobjects::dsl::{evacuateobjects, id as db_id};

        let manta_object: MantaObjectEssential =
            serde_json::from_value(eobj.object.clone())
                .expect("manta object essential");
        let object_id = eobj.id.to_owned();
        let new_shard = eobj.shard;
        let key = manta_object.key;

        let existing_entry: EvacuateObject = evacuateobjects
            .filter(db_id.eq(&object_id))
            .load::<EvacuateObject>(conn)
            .expect("getting filtered objects")
            .first()
            .expect("empty existing entries")
            .to_owned();

        let existing_shard = existing_entry.shard;

        let duplicate = Duplicate {
            id: object_id,
            key,
            shards: vec![new_shard, existing_shard],
        };

        self.insert_duplicate_object(duplicate, new_shard);
    }
    // We want to concatenate the shards array with the new shard, but if
    // this is the first time we have inserted this object then we want to
    // make sure we include the existing shard (which should have been added
    // to the duplicate.shards array passed to this function).  If the insert
    // fails due to conflict that means we already have that shard number in
    // the array.  In that case we only want to insert the new_shard number.
    fn insert_duplicate_object(&self, duplicate: Duplicate, new_shard: i32) {
        let db_name = &self.db_name;
        let new_shard_array = vec![new_shard];
        let connect_string = format!(
            "host=localhost user=postgres password=postgres dbname={}",
            db_name
        );

        let mut client = pg::Client::connect(&connect_string, pg::NoTls)
            .expect("PG Connection error");

        info!("Inserting duplicate object: {:#?}", duplicate);

        client
            .execute(
                "INSERT INTO duplicates (id, key, shards) \
           VALUES ($1, $2, $3) \
           ON CONFLICT (id)\
           DO UPDATE SET shards = duplicates.shards || $4;
           ",
                &[
                    &duplicate.id,
                    &duplicate.key,
                    &duplicate.shards,
                    &new_shard_array,
                ],
            )
            .expect("Upsert error");
    }

    // This should only be called when an assignment fails to update.  There
    // cannot be duplicate entries in a single assignment because it is a
    // hashmap keyed by the object id.
    fn handle_duplicate_assignment(
        &self,
        assignment: &mut Assignment,
        vec_objs: &mut Vec<EvacuateObject>,
        info: Box<dyn DatabaseErrorInformation + Send + Sync>,
        conn: &PgConnection,
    ) {
        let details = info.details().expect("info missing details");
        let mut count = 0;
        let mut object: Option<EvacuateObject> = None;

        assignment.tasks.retain(|t, _| !details.contains(t));

        vec_objs.retain(|o| {
            if details.contains(o.id.as_str()) {
                object = Some(o.clone());
                count += 1;
                false
            } else {
                true
            }
        });

        assert_eq!(
            count, 1,
            "Did not find the right number of duplicate objects"
        );

        let eobj = object.expect("Some object");
        self.insert_duplicate_with_existing(&eobj, conn);
    }

    #[allow(clippy::ptr_arg)]
    fn insert_assignment_into_db(
        &self,
        assignment: &mut Assignment,
        vec_objs: &[EvacuateObject],
    ) -> Result<usize, Error> {
        use self::evacuateobjects::dsl::*;

        let assign_id = assignment.id.clone();
        let mut obj_list = vec_objs.to_owned();

        debug!(
            "Inserting {} objects for assignment ({}) into db",
            obj_list.len(),
            assign_id
        );

        let locked_conn = self.conn.lock().expect("DB conn lock");

        let mut ret = diesel::insert_into(evacuateobjects)
            .values(obj_list.clone())
            .execute(&*locked_conn);

        while ret.is_err() {
            match ret {
                Err(DatabaseError(
                    DatabaseErrorKind::UniqueViolation,
                    info,
                )) => {
                    info!(
                        "Encountered duplicate object in {}, \
                         assignment_count {}, obj_list_count: {}: {:#?}",
                        assign_id,
                        assignment.tasks.len(),
                        obj_list.len(),
                        info.details()
                    );
                    self.handle_duplicate_assignment(
                        assignment,
                        &mut obj_list,
                        info,
                        &*locked_conn,
                    )
                }
                Ok(_) => (), // unreachable?
                Err(e) => {
                    let msg = format!("Error inserting object into DB: {}", e);
                    error!("{}", msg);
                    panic!(msg);
                }
            }

            info!(
                "After duplicate handler {}, assignment_count: {}, \
                 obj_list_count: {}",
                assign_id,
                assignment.tasks.len(),
                obj_list.len()
            );

            ret = diesel::insert_into(evacuateobjects)
                .values(obj_list.clone())
                .execute(&*locked_conn);
        }

        let num_records = ret.expect("num_records");

        debug!(
            "inserted {} objects for assignment {} into db",
            num_records, assign_id
        );

        assert_eq!(num_records, obj_list.len());

        Ok(num_records)
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
            "Marking objects in assignment ({}) as skipped: {:?}",
            assignment_uuid, reason
        );
        // TODO: consider checking record count to ensure update success
        let skipped_count = diesel::update(evacuateobjects)
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
            });

        debug!(
            "Marked {} objects in assignment ({}) as skipped: {:?}",
            skipped_count, assignment_uuid, reason
        );
        metrics_skip_inc_by(Some(&reason.to_string()), skipped_count);
        skipped_count
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

        let update_cnt = diesel::update(evacuateobjects)
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
            });

        debug!(
            "Marked {} objects in assignment ({}) as error: {:?}",
            update_cnt, assignment_uuid, err
        );

        // TODO: We may need to remove this assignment from the cache

        update_cnt
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
            // Since we are bulk updating objects in the database by the same
            // reason, we can just as easily do the same thing with our metrics.
            // This is because all tasks in the vector are being skipped for
            // the same reason.
            let vec_len = vec_obj_ids.len();
            metrics_skip_inc_by(Some(&reason.to_string()), vec_len);

            let rows_updated = diesel::update(evacuateobjects)
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

            // Ensure that the number of rows affected by the update is in fact
            // equal to the number of entries in the vector.
            assert_eq!(
                rows_updated, vec_len,
                "Attempted to update {} rows, but only updated {}",
                vec_len, rows_updated
            );
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

        metrics_error_inc(Some(&err.to_string()));

        let locked_conn = self.conn.lock().expect("db conn lock");

        debug!("Updating object {} as error: {:?}", object_id, err);

        let update_cnt = diesel::update(evacuateobjects)
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
            });

        assert_eq!(update_cnt, 1);
        update_cnt
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
        let ret = diesel::update(evacuateobjects)
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
            });

        debug!(
            "Marked {} objects for assignment ({}) as {:?}",
            ret, id, to_status
        );

        ret
    }

    #[allow(clippy::ptr_arg)]
    fn mark_dest_shark<F>(
        &self,
        dest_shark: &StorageId,
        status: DestSharkStatus,
        post_fn: Option<F>,
    ) where
        F: Fn(&mut EvacuateDestShark) -> (),
    {
        if let Some(shark) = self
            .dest_shark_hash
            .write()
            .expect("dest_shark_hash write")
            .get_mut(dest_shark)
        {
            debug!("Updating shark '{}' to {:?} state", dest_shark, status,);
            shark.status = status;
            if let Some(func) = post_fn {
                func(shark)
            }
        } else {
            warn!("Could not find shark: '{}'", dest_shark);
        }
    }

    #[allow(clippy::ptr_arg)]
    fn mark_dest_shark_assigned(&self, dest_shark: &StorageId, size: u64) {
        trace!("Marking shark {} assigned with {}MB", dest_shark, size);
        self.mark_dest_shark(
            dest_shark,
            DestSharkStatus::Assigned,
            Some(|shark: &mut EvacuateDestShark| {
                if let Some(assigned_mb) = shark.assigned_mb.checked_add(size) {
                    shark.assigned_mb = assigned_mb;
                } else {
                    warn!(
                        "Detected overflow on assigned_mb, setting to \
                         u64::MAX"
                    );
                    shark.assigned_mb = std::u64::MAX;
                }
            }),
        )
    }

    #[allow(clippy::ptr_arg)]
    fn mark_dest_shark_ready(&self, dest_shark: &StorageId, size: u64) {
        trace!("Marking shark {} ready with {}MB", dest_shark, size);
        self.mark_dest_shark(
            dest_shark,
            DestSharkStatus::Ready,
            Some(|shark: &mut EvacuateDestShark| {
                if let Some(assigned_mb) = shark.assigned_mb.checked_sub(size) {
                    shark.assigned_mb = assigned_mb;
                } else {
                    warn!("Detected underflow on assigned_mb, setting to 0");
                    shark.assigned_mb = 0;
                }
            }),
        )
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
///    EvacauteJob's assignment cache.
/// 4. Implicitly drop (free) the Assignment on return.
fn assignment_post_success(
    job_action: &EvacuateJob,
    mut assignment: Assignment,
) {
    trace!(
        "Post of assignment ({}) with size {} succeeded",
        assignment.id,
        assignment.total_size
    );

    assignment.state = AssignmentState::Assigned;
    let mut assignments = job_action
        .assignments
        .write()
        .expect("Assignments hash write lock");

    // '.into()' converts the Assignment to its cache entry type and insert
    // that into the cache.  Since this function takes ownership of the
    // Assignment, the Assignment passed in is implicitly dropped (freed)
    // when this function returns.
    assignments.insert(assignment.id.clone(), assignment.into());
}

// This is one of two places where we mark the dest shark ready and decrease
// its assigned_mb counter.  We do this because we increased it in
// _channel_send_assignment(), and need to decrease it now due to the failure.
// See the note in _channel_send_assignment() that explains why we don't do
// simply increase the assigned_mb counter on a successful post in the
// post() function below.
fn assignment_post_fail(
    job_action: &EvacuateJob,
    assignment: &Assignment,
    reason: ObjectSkippedReason,
    assignment_state: AssignmentState,
) {
    // TODO: Should we blacklist this destination shark?

    warn!(
        "Post of assignment ({}) with size {} failed: {}",
        assignment.id, assignment.total_size, reason
    );

    job_action.mark_dest_shark_ready(
        &assignment.dest_shark.manta_storage_id,
        assignment.total_size,
    );

    job_action.skip_assignment(&assignment.id, reason, assignment_state);
}

impl PostAssignment for EvacuateJob {
    fn post(&self, assignment: Assignment) -> Result<(), Error> {
        let payload = AssignmentPayload {
            id: assignment.id.clone(),
            tasks: assignment.tasks.values().map(|t| t.to_owned()).collect(),
        };

        let agent_uri = format!(
            "http://{}:7878/assignments",
            assignment.dest_shark.manta_storage_id
        );

        trace!("Sending {:#?} to {}", payload, agent_uri);
        let res = match self.post_client.post(&agent_uri).json(&payload).send()
        {
            Ok(r) => r,
            Err(e) => {
                assignment_post_fail(
                    self,
                    &assignment,
                    ObjectSkippedReason::DestinationUnreachable,
                    AssignmentState::AgentUnavailable,
                );
                return Err(e.into());
            }
        };

        if !res.status().is_success() {
            assignment_post_fail(
                self,
                &assignment,
                ObjectSkippedReason::AssignmentRejected,
                AssignmentState::Rejected,
            );

            let err = format!(
                "Error posting assignment {} to {} ({})",
                payload.id,
                assignment.dest_shark.manta_storage_id,
                res.status()
            );

            return Err(InternalError::new(None, err).into());
        }

        debug!("Post of {} was successful", payload.id);
        assignment_post_success(self, assignment);
        Ok(())
    }
}

impl GetAssignment for EvacuateJob {
    fn get(
        &self,
        ace: &AssignmentCacheEntry,
    ) -> Result<AgentAssignment, Error> {
        let uri = format!(
            "http://{}:7878/assignments/{}",
            ace.dest_shark.manta_storage_id, ace.id
        );

        debug!("Getting Assignment: {:?}", uri);
        match self.get_client.get(&uri).send() {
            Ok(mut resp) => {
                if !resp.status().is_success() {
                    self.skip_assignment(
                        &ace.id,
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
                debug!("Assignment Get Response: {:#?}", resp);
                resp.json::<AgentAssignment>().map_err(Error::from)
            }
            Err(e) => {
                self.skip_assignment(
                    &ace.id,
                    ObjectSkippedReason::NetworkError,
                    AssignmentState::AgentUnavailable,
                );

                Err(e.into())
            }
        }
    }
}

impl UpdateMetadata for EvacuateJob {
    fn update_object_shark(
        &self,
        mut object: Value,
        new_shark: &StorageNode,
    ) -> Result<Value, Error> {
        let old_shark = &self.from_shark;
        let mut shark_found = false;

        // Get the sharks in the form of Vec<MantaObjectShark> to make it
        // easy to manipulate.
        let mut sharks = match common::get_sharks_from_value(&object) {
            Ok(s) => s,
            Err(e) => {
                let msg = format!(
                    "Could not get sharks from Manta Object {:#?}: {}",
                    object, e
                );
                return Err(InternalError::new(
                    Some(InternalErrorCode::SharkNotFound),
                    msg,
                )
                .into());
            }
        };

        // replace shark value
        for shark in sharks.iter_mut() {
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
                    return Err(InternalError::new(
                        Some(InternalErrorCode::DuplicateShark),
                        msg,
                    )
                    .into());
                }
            }
        }

        // Convert the Vec<MantaObjectShark> into a serde Value
        let sharks_value = serde_json::to_value(sharks)?;

        // update the manta object Value with the new sharks Value
        match object.get_mut("sharks") {
            Some(sharks) => {
                *sharks = sharks_value;
            }
            None => {
                let msg =
                    format!("Missing sharks in manta object {:#?}", object);

                error!("{}", &msg);
                return Err(InternalError::new(
                    Some(InternalErrorCode::BadMantaObject),
                    msg,
                )
                .into());
            }
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
        let ace = match assignments.get_mut(uuid) {
            Some(a) => a,
            None => {
                let msg = format!(
                    "Error getting assignment.  Couldn't find \
                     assignment {} in {} assignments.",
                    uuid,
                    assignments.len()
                );

                error!("{}", &msg);

                // TODO: This should never happen, should we panic here?

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
        match ace.state {
            AssignmentState::Assigned => (),
            _ => {
                warn!(
                    "Assignment in unexpected state '{:?}', skipping",
                    ace.state
                );
                // TODO: this should never happen but should we panic?
                // If we create more threads to check for assignments or
                // process them this may be possible.
                panic!("Assignment in wrong state {:?}", ace);
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
                    &ace.id,
                    EvacuateObjectStatus::PostProcessing,
                );
                ace.state = AssignmentState::AgentComplete;
            }

            AgentAssignmentState::Complete(Some(failed_tasks)) => {
                let objects = self.load_assignment_objects(
                    &ace.id,
                    EvacuateObjectStatus::Assigned,
                );

                info!(
                    "Assignment {} resulted in {} failed tasks.",
                    &ace.id,
                    failed_tasks.len()
                );
                trace!("{:#?}", &failed_tasks);

                // failed_tasks: Vec<Task>
                // objects: Vec<EvacuateObject>
                //
                // We iterate over the object IDs, and filter out those that are
                // in the failed_tasks Vec.
                let successful_tasks: Vec<ObjectId> = objects
                    .iter()
                    .map(|obj| obj.id.clone())
                    .filter(|obj_id| {
                        !failed_tasks.iter().any(|ft| &ft.object_id == obj_id)
                    })
                    .collect();

                self.mark_many_task_objects_skipped(failed_tasks);
                self.mark_many_objects(
                    successful_tasks,
                    EvacuateObjectStatus::PostProcessing,
                );

                ace.state = AssignmentState::AgentComplete;
            }
        }
        self.mark_dest_shark_ready(
            &ace.dest_shark.manta_storage_id,
            ace.total_size,
        );

        Ok(())
    }
}

fn _insert_bad_moray_object(
    job_action: &EvacuateJob,
    object: Value,
    id: ObjectId,
) {
    error!("Moray value missing etag {:#?}", object);
    let eobj = EvacuateObject {
        id,
        object,
        status: EvacuateObjectStatus::Error,
        error: Some(EvacuateObjectError::BadMorayObject),
        ..Default::default()
    };

    job_action.insert_into_db(&eobj);
}

fn _insert_bad_manta_object(
    job_action: &EvacuateJob,
    object: Value,
    id: ObjectId,
) {
    error!("Moray value missing etag {:#?}", object);
    let eobj = EvacuateObject {
        id,
        object,
        status: EvacuateObjectStatus::Error,
        error: Some(EvacuateObjectError::BadMantaObject),
        ..Default::default()
    };

    job_action.insert_into_db(&eobj);
}

//
// The amount that rebalancer can use is:
// (total * max_fill_percent) - used_mb - assigned_mb
//
// Where:
//  total = available_mb / (1 - percent_used)
//  used_mb = total * percent_used
//
// Given:
//  available_mb = 900
//  assigned_mb = 300
//  percent_used = 10
//  max_fill_percent = 80
//
// Then:
//  total_mb = 1000     [900 / (1 - .1)]
//  used_mb = 100       [1000 * .1]
//  max_fill_mb = 800   [1000 * .8]
//  remaining = 400     [800 - 100 - 300]
//
fn _calculate_available_mb(
    dest_shark: &EvacuateDestShark,
    max_fill_percentage: u32,
) -> u64 {
    let available_mb = dest_shark.shark.available_mb;
    let assigned_mb = dest_shark.assigned_mb;
    let max_percent = max_fill_percentage as f64 / 100.0;
    let percent_used = dest_shark.shark.percent_used as f64 / 100.0;

    let total_mb = available_mb as f64 / (1.0 - percent_used);
    let max_fill_mb = (total_mb * max_percent).floor() as u64;
    let used_mb = total_mb - available_mb as f64;

    if percent_used > max_percent {
        warn!(
            "percent used {} exceeds maximum utilization percentage {}, \
             reporting shark available MB as 0",
            percent_used, max_percent
        );
        return 0;
    }

    assert!(percent_used <= 1.0);
    assert!(percent_used >= 0.0);
    assert!(max_percent <= 1.0);
    assert!(max_percent >= 0.0);

    let max_remaining = max_fill_mb
        .checked_sub(used_mb.floor() as u64)
        .unwrap_or_else(|| {
            let msg = format!(
                "used_mb({}) > max_fill_mb({}) for max_fill({}): {:#?}",
                used_mb, max_fill_mb, max_percent, dest_shark
            );
            panic!(msg);
        });

    // If (max_remaining - assigned_mb) < 0 (i.e. assigned_mb > max_remaining),
    // we've already exceeded our max fill quota, return 0.
    if let Some(avail_mb) = max_remaining.checked_sub(assigned_mb) {
        avail_mb
    } else {
        warn!(
            "Detected underflow, returning 0 available_mb for {}",
            dest_shark.shark.manta_storage_id
        );
        0
    }
}

// Query the previous Job's database for skips and errors, and send them to
// the assignment_manager thread.  Note that we do synchronous chunk queries
// here with the limit controlled by the md_read_chunk_size tunable.
// We would utilize asynchronous queries here, but an attempt at bringing
// tokio_postgres crate into rebalancer revealed that we would have to
// upgraded many other crates, many of which have changed their interfaces
// across minor and major versions.  Unfortunately, this retry job work is
// urgent so that work could not be done in time, hence the synchronous chunks.
// Note also that the channel size should be equal to the chunk size.  This
// will allow this thread to queue up objects in the channel while it starts
// the next query.
// The query is really slow because we are looking for objects in a table
// with around 50 million entries.  If this isn't sufficient we could
// consider creating an index on this table for this use case.  We would have
// to consider the trade off if any of inserts.
fn local_db_generator(
    job_action: Arc<EvacuateJob>,
    obj_tx: crossbeam::Sender<EvacuateObject>,
    retry_uuid: &str,
) -> Result<(), Error> {
    use self::evacuateobjects::dsl::{evacuateobjects, id as obj_id, status};

    let limit = job_action.config.options.md_read_chunk_size as i64;
    let mut offset = 0;
    let mut found_objects = 0;
    let mut retry_count = 0;
    let conn = match pg_db::connect_db(retry_uuid) {
        Ok(c) => c,
        Err(e) => {
            println!("Error creating Evacuate Job: {}", e);
            std::process::exit(1);
        }
    };

    loop {
        debug!("retry limit: {} | offset: {}", limit, offset);
        let retry_objs: Vec<EvacuateObject> = evacuateobjects
            .filter(status.eq_any(vec![
                EvacuateObjectStatus::Skipped,
                EvacuateObjectStatus::Error,
            ]))
            .order(obj_id)
            .limit(limit)
            .offset(offset)
            .load::<EvacuateObject>(&conn)
            .expect("getting filtered objects");

        if retry_objs.is_empty() {
            info!(
                "Done scanning previous job {}.  Found Object Count: {}.  \
                 Retry Count: {}",
                retry_uuid, found_objects, retry_count
            );
            break;
        }

        info!("retrying {} objects", retry_objs.len());
        info!(
            "DEBUG: retry_obj len size is: {}",
            retry_objs.len() * std::mem::size_of::<EvacuateObject>()
        );
        info!(
            "DEBUG: retry_obj capacity size is: {}",
            retry_objs.capacity() * std::mem::size_of::<EvacuateObject>()
        );

        found_objects = retry_objs.len();

        // We do not want to include objects that bailed on bad shard number
        // because that is the only case where we couldn't properly transform
        // a sharkspotter message to an EvacuateObject instance.
        for obj in retry_objs {
            if let Some(reason) = obj.error {
                if reason == EvacuateObjectError::BadShardNumber {
                    warn!("Skipping bad shard number object {}", obj.id);
                    continue;
                }
            }
            retry_count += 1;

            // We don't modify the metadata in the database, so what this
            // object has for a sharks array should be the same as what it
            // was when we first found it.

            if let Err(e) = obj_tx.send(obj) {
                warn!("local db generator exiting early: {}", e);
                return Err(InternalError::new(
                    Some(InternalErrorCode::Crossbeam),
                    CrossbeamError::from(e).description(),
                )
                .into());
            }
        }

        offset += limit;
    }

    Ok(())
}

fn start_local_db_generator(
    job_action: Arc<EvacuateJob>,
    obj_tx: crossbeam::Sender<EvacuateObject>,
    retry_uuid: &str,
) -> Result<thread::JoinHandle<Result<(), Error>>, Error> {
    let db_name = retry_uuid.to_string();
    thread::Builder::new()
        .name(String::from("local_generator"))
        .spawn(move || local_db_generator(job_action, obj_tx, &db_name))
        .map_err(Error::from)
}

/// Start the sharkspotter thread and feed the objects into the assignment
/// thread.  If the assignment thread (the rx side of the channel) exits
/// prematurely the sender.send() method will return a SenderError and that
/// needs to be handled properly.
fn start_sharkspotter(
    obj_tx: crossbeam::Sender<EvacuateObject>,
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
        sharks: vec![shark.to_string()],
        chunk_size: job_action.config.options.md_read_chunk_size as u64,
        direct_db: true,
        max_threads: job_action.config.options.max_md_read_threads,
        ..Default::default()
    };

    debug!("Starting sharkspotter thread: {:?}", &config);

    let log = slog_scope::logger();

    let (ss_trans_tx, ss_trans_rx) = crossbeam_channel::bounded(10);
    thread::Builder::new()
        .name(String::from("sharkspotter"))
        .spawn(move || {
            let ss_trans_handle: JoinHandle<Result<(), Error>> =
                thread::Builder::new()
                    .name("sharkspotter_translator".to_string())
                    .spawn(move || {
                        while let Ok(ss_msg) = ss_trans_rx.recv() {
                            let eo: EvacuateObject =
                                match EvacuateObject::try_from(ss_msg) {
                                    Ok(o) => o,
                                    Err(e) => {
                                        job_action.insert_into_db(&e);
                                        continue;
                                    }
                                };

                            if let Err(e) = obj_tx.send(eo) {
                                warn!(
                                    "Could not send evacuate object.  Receive \
                                     side of channel exited prematurely.  Is \
                                     max_objects set? {}",
                                    e
                                );

                                break;
                            }
                        }

                        info!("Sharkspotter translator thread exiting");
                        Ok(())
                    })
                    .expect("Start sharkspotter translator thread");
            sharkspotter::run_multithreaded(&config, log, ss_trans_tx)
                .map_err(Error::from)?;

            ss_trans_handle
                .join()
                .expect("sharkspotter translator join")
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
///

#[derive(Clone)]
enum AssignmentMsg {
    Flush, // send all assignments to the Post thread, but keep running
    Stop,  // send all assignments to the Post thread, and stop running
    Data(Box<EvacuateObject>), // Add EvacuateObject to active assignment
}

struct SharkHashEntry {
    handle: thread::JoinHandle<Result<(), Error>>,
    tx: crossbeam::Sender<AssignmentMsg>,
    shark: StorageNode,
}

fn _msg_all_assignment_threads(
    shark_hash: &mut HashMap<StorageId, SharkHashEntry>,
    command: AssignmentMsg,
) {
    let mut remove_keys = vec![];

    shark_hash.iter().for_each(|(shark_id, she)| {
        if let Err(e) = she.tx.send(command.clone()) {
            error!("Error stopping shark ({}) assignments: {}", shark_id, e);
            // TODO: join?
            // This is a little funky.  We cant send
            // anything to the thread, but we need it to
            // shutdown...
            remove_keys.push(shark_id.clone());
        }
    });

    for k in remove_keys {
        shark_hash.remove(&k);
    }
}
fn _stop_shark_assignment_threads(
    shark_hash: &mut HashMap<StorageId, SharkHashEntry>,
) {
    _msg_all_assignment_threads(shark_hash, AssignmentMsg::Stop);
}

fn _flush_shark_assignment_threads(
    shark_hash: &mut HashMap<StorageId, SharkHashEntry>,
) {
    _msg_all_assignment_threads(shark_hash, AssignmentMsg::Flush);
}

// The drain here is really taking ownership of the shark_hash and then
// allowing it to go out of scope and be dropped, thus reclaiming memory.
fn _join_drain_shark_assignment_threads(
    shark_hash: HashMap<StorageId, SharkHashEntry>,
) {
    shark_hash.into_iter().for_each(|(shark_id, she)| {
        if let Err(e) = she
            .handle
            .join()
            .expect("Error joining shark assignment thread")
        {
            error!(
                "Shark assignment thread ({}) encountered an error: {}",
                shark_id, e
            );
        }
    });
}

// Takes ownership of the shark_hash, sends all associated threads the stop
// command, and eventually drops the shark_hash, reclaiming memory.
fn _stop_join_drain_assignment_threads(
    mut shark_hash: HashMap<StorageId, SharkHashEntry>,
) {
    _msg_all_assignment_threads(&mut shark_hash, AssignmentMsg::Stop);
    _join_drain_shark_assignment_threads(shark_hash);
}

fn _stop_join_some_assignment_threads(
    shark_hash: &mut HashMap<StorageId, SharkHashEntry>,
    shark_id_list: Vec<StorageId>,
) {
    for key in shark_id_list.iter() {
        if let Some(ent) = shark_hash.remove(key) {
            trace!("Stopping {:?} thread", ent.handle.thread().name());
            if let Err(e) = ent.tx.send(AssignmentMsg::Stop) {
                error!("Error sending Stop command to {}: {}", key, e);
            }
        }
    }
    for key in shark_id_list.iter() {
        if let Some(ent) = shark_hash.remove(key) {
            if let Err(e) = ent
                .handle
                .join()
                .expect("Error joining shark assignment thread")
            {
                error!(
                    "Shark assignment thread ({}) encountered an error: {}",
                    key, e
                );
            }
        }
    }
}

fn assignment_manager_impl<S>(
    full_assignment_tx: crossbeam::Sender<Assignment>,
    checker_fini_tx: crossbeam_channel::Sender<FiniMsg>,
    obj_rx: crossbeam_channel::Receiver<EvacuateObject>,
    job_action: Arc<EvacuateJob>,
    storinfo: Arc<S>,
) -> impl Fn() -> Result<(), Error>
where
    S: SharkSource + 'static,
{
    move || {
        let mut done = false;
        let mut object_count = 0;
        let max_objects = job_action.max_objects;
        let max_sharks = job_action.config.options.max_sharks;
        let max_tasks_per_assignment =
            job_action.config.options.max_tasks_per_assignment;

        let algo = mod_storinfo::DefaultChooseAlgorithm {
            min_avail_mb: job_action.min_avail_mb,
            blacklist: vec![],
        };

        let mut shark_hash: HashMap<StorageId, SharkHashEntry> = HashMap::new();

        while !done {
            // TODO: MANTA-4519
            // get a fresh shark list
            let mut shark_list =
                job_action.get_shark_list(Arc::clone(&storinfo), &algo, 3)?;

            // TODO: file ticket, tunable number of sharks which implies
            // number of threads.
            shark_list.truncate(max_sharks);

            // We've already truncated the list to only include the sharks
            // with the most available_mb.  Shuffling here ensures
            // that of those sharks our assignments are more evenly spread
            // out among them.  It is possible that a single shark could
            // have significantly more space available than every other
            // shark.  In such a case, if we dont shuffle, the evacuate
            // job could take much longer as every object would go to a
            // single shark.
            let mut rng = rand::thread_rng();
            shark_list.as_mut_slice().shuffle(&mut rng);

            let shark_list: Vec<StorageNode> =
                shark_list.into_iter().map(|s| s.shark).collect();

            // For any active sharks that are not in the list remove them
            // from the hash, send the stop command, join the associated
            // threads.
            let mut remove_keys = vec![];
            for (key, _) in shark_hash.iter() {
                if shark_list.iter().any(|s| &s.manta_storage_id == key) {
                    continue;
                }
                remove_keys.push(key.clone());
            }

            // Send the stop command which flushes any outstanding
            // assignments, then join the threads
            _stop_join_some_assignment_threads(&mut shark_hash, remove_keys);

            // Create and add any shark threads that are in the list but
            // not in the hash.
            for shark in shark_list.iter() {
                if shark_hash.contains_key(&shark.manta_storage_id) {
                    continue;
                }

                let (tx, rx) = crossbeam_channel::bounded(5);
                let builder = thread::Builder::new();
                let handle = builder
                    .name(format!(
                        "shark_assignment_generator({})",
                        shark.manta_storage_id
                    ))
                    .spawn(shark_assignment_generator(
                        Arc::clone(&job_action),
                        shark.clone(),
                        rx,
                        full_assignment_tx.clone(),
                    ))?;

                shark_hash.insert(
                    shark.manta_storage_id.clone(),
                    SharkHashEntry {
                        handle,
                        tx,
                        shark: shark.clone(),
                    },
                );
            }

            // for some max number of iterations get an object from shark
            // spotter, then:
            // - iteration over the hash:
            //      * find first shark that is a vaild destination
            //      * send object to that shark's thread
            // end loop
            for _ in 0..max_tasks_per_assignment * max_sharks {
                // Get an object
                if let Some(max) = max_objects {
                    if object_count >= max {
                        info!(
                            "Hit max objects count ({}).  Sending last \
                             assignments and exiting",
                            max
                        );

                        let mut start_time = job_action
                            .object_movement_start_time
                            .lock()
                            .unwrap();

                        if let Some(st) = start_time.take() {
                            info!(
                                "Evacuate Job object movement time: {} seconds",
                                st.elapsed().as_secs()
                            );
                        }

                        done = true;
                        break;
                    }
                }

                let mut eobj = match obj_rx.recv() {
                    Ok(obj) => {
                        if object_count == 0 {
                            *job_action
                                .object_movement_start_time
                                .lock()
                                .unwrap() = Some(std::time::Instant::now());
                        }

                        trace!("Received object {:#?}", &obj);
                        object_count += 1;

                        obj
                    }
                    Err(e) => {
                        warn!("Didn't receive object. {}\n", e);
                        info!("Sending last assignments");
                        done = true;
                        break;
                    }
                };

                // Iterate over the list of sharks and get the first
                // valid one.
                let mut last_reason = ObjectSkippedReason::AgentBusy;
                let shark_list_entry: Option<&StorageNode> =
                    shark_list.iter().find(|shark| {
                        if let Some(reason) = validate_destination(
                            &eobj.object,
                            &job_action.from_shark,
                            &shark,
                        ) {
                            trace!("shark is not valid because: {}", reason);
                            last_reason = reason;
                            return false;
                        }
                        true
                    });

                // Get the associated shark_hash_entry which holds the
                // send side of the shark_assignment_generator channel.
                let shark_hash_entry = match shark_list_entry {
                    Some(shark) => shark_hash
                        .get(&shark.manta_storage_id)
                        .expect("shark not found in hash"),
                    None => {
                        warn!("No sharks available");
                        job_action.skip_object(&mut eobj, last_reason);
                        continue;
                    }
                };

                let shark_id = shark_hash_entry.shark.manta_storage_id.clone();

                debug!("Sending {} to {}", eobj.id, shark_id);

                // Send the evacuate object to the
                // shark_assignment_generator.
                if let Err(e) = shark_hash_entry
                    .tx
                    .send(AssignmentMsg::Data(Box::new(eobj.clone())))
                {
                    error!(
                        "Error sending object to shark ({}) \
                         generator thread: {}",
                        &shark_id,
                        CrossbeamError::from(e)
                    );

                    job_action.skip_object(
                        &mut eobj,
                        ObjectSkippedReason::AssignmentError,
                    );

                    // We can't send anything to the thread, but we need to
                    // join it.
                    _stop_join_some_assignment_threads(
                        &mut shark_hash,
                        vec![shark_id],
                    );
                    break;
                }
            }

            // This is a conditional check.  If a given assignment is older
            // than MAX_ASSIGNMENT_AGE, then we will post it.  But we
            // don't want to unconditionally post assignments here
            // because we have seen that results in many single
            // object/task assignments.
            _flush_shark_assignment_threads(&mut shark_hash);
        }

        // Flush all the threads first so that while we are joining they
        // are all flushing.
        info!("Shutting down all assignment threads");
        _stop_join_drain_assignment_threads(shark_hash);

        info!("Manager: Shutting down assignment checker");
        checker_fini_tx.send(FiniMsg).expect("Fini Msg");
        Ok(())
    }
}

fn start_assignment_manager<S>(
    full_assignment_tx: crossbeam::Sender<Assignment>,
    checker_fini_tx: crossbeam_channel::Sender<FiniMsg>,
    obj_rx: crossbeam_channel::Receiver<EvacuateObject>,
    job_action: Arc<EvacuateJob>,
    storinfo: Arc<S>,
) -> Result<thread::JoinHandle<Result<(), Error>>, Error>
where
    S: SharkSource + 'static,
{
    thread::Builder::new()
        .name(String::from("assignment_manager"))
        .spawn(assignment_manager_impl(
            full_assignment_tx,
            checker_fini_tx,
            obj_rx,
            job_action,
            storinfo,
        ))
        .map_err(Error::from)
}

// Insert the assignment into the assignment cache then send it to the post
// thread.
fn _channel_send_assignment(
    job_action: Arc<EvacuateJob>,
    full_assignment_tx: &crossbeam_channel::Sender<Assignment>,
    assignment: Assignment,
) -> Result<(), Error> {
    if !assignment.tasks.is_empty() {
        // Insert the Assignment into the hash of assignments so
        // that the assignment checker thread knows to wait for
        // it to be posted and to check for it later on.
        let assignment_uuid = assignment.id.clone();
        let assignment_size = assignment.total_size;
        let dest_shark = assignment.dest_shark.manta_storage_id.clone();

        job_action
            .assignments
            .write()
            .expect("assignments write lock")
            .insert(assignment_uuid.clone(), assignment.clone().into());

        info!(
            "Sending {}MB Assignment: {}",
            assignment_size, assignment_uuid
        );

        // This might not be the best place to mark an assignment as
        // posted because it is possible that the post will fail.  It
        // would be better if we marked the dest shark as assigned in
        // assignment_post_success().  But that is it's own thread, and
        // each per shark assignment generator thread needs to know the
        // most up to date available_mb for its associated shark.  So
        // this blocking call is our best choice.  If we err on the
        // available_mb, it is better to err on the side of lower available
        // MB than higher, which is possible on a post error.  The
        // way we account for that is we call mark_dest_shark_ready() on post
        // failure, thus decreasing the assigned_mb and increasing the
        // available_mb for this shark.  Note that we also call
        // mark_dest_shark_ready() on a channel send failure.
        job_action.mark_dest_shark_assigned(&dest_shark, assignment_size);

        full_assignment_tx.send(assignment).map_err(|e| {
            error!("Error sending assignment to be posted: {}", e);

            job_action.mark_assignment_error(
                &assignment_uuid,
                EvacuateObjectError::InternalError,
            );

            job_action.mark_dest_shark_ready(&dest_shark, assignment_size);

            InternalError::new(
                Some(InternalErrorCode::Crossbeam),
                CrossbeamError::from(e).description(),
            )
        })?;
    }

    Ok(())
}

enum AssignmentAddObjectError {
    BadMantaObject,
    DestinationInsufficentSpace,
    SouceIsEvacShark,
    DuplicateObject,
}

// return True of False if we should flush the assignment or not
fn add_object_to_assignment(
    job_action: &EvacuateJob,
    mut eobj: EvacuateObject,
    shark: &StorageNode,
    assignment: &mut Assignment,
    available_space: &mut u64,
    from_shark_host: &str,
) -> Result<EvacuateObject, AssignmentAddObjectError> {
    eobj.dest_shark = shark.manta_storage_id.clone();

    let manta_object: MantaObjectEssential =
        match serde_json::from_value(eobj.object.clone()) {
            Ok(mo) => mo,
            Err(e) => {
                error!(
                    "Unable to get essential values from \
                     manta object: {} ({})",
                    eobj.id, e
                );
                job_action.mark_object_error(
                    &eobj.id,
                    EvacuateObjectError::BadMantaObject,
                );
                return Err(AssignmentAddObjectError::BadMantaObject);
            }
        };

    let source = manta_object
        .sharks
        .iter()
        .find(|s| s.manta_storage_id != from_shark_host);

    let source = match source {
        Some(src) => src,
        None => {
            // The only shark we could find was the one that
            // is being evacuated.
            job_action
                .skip_object(&mut eobj, ObjectSkippedReason::SourceIsEvacShark);

            return Err(AssignmentAddObjectError::SouceIsEvacShark);
        }
    };

    // Make sure there is enough space for this object on the
    // shark.
    let content_mb = manta_object.content_length / (1024 * 1024);
    if content_mb > *available_space {
        job_action.skip_object(
            &mut eobj,
            ObjectSkippedReason::DestinationInsufficientSpace,
        );

        return Err(AssignmentAddObjectError::DestinationInsufficentSpace);
    }

    if assignment
        .tasks
        .insert(
            manta_object.object_id.to_owned(),
            Task {
                object_id: manta_object.object_id.to_owned(),
                owner: manta_object.owner.to_owned(),
                md5sum: manta_object.content_md5.to_owned(),
                source: source.to_owned(),
                status: TaskStatus::Pending,
            },
        )
        .is_some()
    {
        // We have encountered a duplicate.  We have no way to know which one
        // is the correct one but we don't want to lose objects here by
        // overwriting one that is already in the assignment.
        // So call insert_duplicate_object() on this one as we aren't sure if
        // this one is misplaced or if the resident object is misplaced.
        //
        // Unfortunately we don't know the shard number of this task, so the
        // table will end up with one missing shard number.  The deleter will
        // have to handle this.  Fortunately, this only happens when two of
        // the same objects are added to the same assignment.  Since
        // assignment size is on the order of a few hundred and there are 50
        // million objects per shark, this should be fairly rare.  Adding
        // shard number to task may require a change to the agents as well.
        // We could insert each evacuate object as we receive it, but we
        // currently group them in assignments for performance reasons.

        debug!(
            "Found duplicate in assignment{}: {}",
            assignment.id, manta_object.object_id
        );

        let duplicate = Duplicate {
            id: manta_object.object_id,
            key: manta_object.key,
            shards: vec![eobj.shard],
        };

        job_action.insert_duplicate_object(duplicate, eobj.shard);

        return Err(AssignmentAddObjectError::DuplicateObject);
    }

    // We don't checked_sub() here because if we do underflow we want to
    // panic.  We've already assured that available_space >= content_mb above.
    *available_space -= content_mb;
    assignment.total_size += content_mb;

    trace!(
        "{}: Available space: {} | Tasks: {}",
        assignment.id,
        &available_space,
        &assignment.tasks.len()
    );

    eobj.status = EvacuateObjectStatus::Assigned;
    eobj.assignment_id = assignment.id.clone();

    Ok(eobj)
}

// This function handles:
// - inserting all assignment tasks into the database
// - sending the assignment to the post thread to be sent to the agents
// - getting a new assignment
// - resetting the eobj_vec
// - updating the available_space to the appropriate value
fn flush_assignment(
    job_action: &Arc<EvacuateJob>,
    assignment: &mut Assignment,
    eobj_vec: &mut Vec<EvacuateObject>,
    shark: &StorageNode,
    full_assignment_tx: &crossbeam::Sender<Assignment>,
) -> Result<u64, Error> {
    // This function modifies both assignment and eobj_vec
    job_action.insert_assignment_into_db(assignment, &eobj_vec)?;

    // This function calls mark_dest_shark_ready() which in turn
    // updates the assigned_mb counter for this shark.
    _channel_send_assignment(
        Arc::clone(job_action),
        full_assignment_tx,
        assignment.clone(),
    )?;

    *eobj_vec = Vec::new();

    // See above.  assigned_mb counter is up to date, so when we
    // create a new assignment with this method it will
    // set assignment.max_size to the most up to date value.
    *assignment = job_action.new_assignment(shark.to_owned())?;

    // See comment at top of shark_assignment_generator for why we divide by 2.
    let available_space = assignment.max_size / 2;
    debug!("Setting available_space: {}MB", available_space);

    Ok(available_space)
}

fn shark_assignment_generator(
    job_action: Arc<EvacuateJob>,
    shark: StorageNode, // TODO: reference?
    assign_msg_rx: crossbeam::Receiver<AssignmentMsg>,
    full_assignment_tx: crossbeam::Sender<Assignment>,
) -> impl Fn() -> Result<(), Error> {
    move || {
        let max_tasks = job_action.config.options.max_tasks_per_assignment;
        let max_age = job_action.config.options.max_assignment_age;
        let from_shark_host = job_action.from_shark.manta_storage_id.clone();
        let mut stop = false;
        let mut flush = false;
        let mut eobj_vec = vec![];
        let mut assignment = job_action.new_assignment(shark.clone())?;
        let mut assignment_birth_time = std::time::Instant::now();

        // max_size is set appropriately in `new_assignment()`.  We divide by 2
        // here to account for the possibility that other objects are written to
        // the shark while we are generating the assignment.
        let mut available_space = assignment.max_size / 2;

        debug!("Starting with available_space: {}MB", available_space);

        // TODO: get any objects from the DB that were previously supposed to
        // be assigned to this shark but the shark was busy at that time.

        while !stop {
            // An assignment message is either:
            // Stop
            // Flush
            // Data(EvacuateObject)
            let assign_msg = match assign_msg_rx.recv() {
                Ok(msg) => msg,
                Err(_) => {
                    if assignment.tasks.is_empty() {
                        break;
                    }

                    // This shouldn't happen because before the assignment
                    // manager exits it should send each shark thread the
                    // Stop message.  But in the event that it does exit
                    // without doing this and we have an active assignment,
                    // this will clean that up before exiting.

                    // This function modifies both assignment and eobj_vec.
                    job_action.insert_assignment_into_db(
                        &mut assignment,
                        &eobj_vec,
                    )?;

                    _channel_send_assignment(
                        Arc::clone(&job_action),
                        &full_assignment_tx,
                        assignment,
                    )?;

                    break;
                }
            };

            match assign_msg {
                AssignmentMsg::Stop => {
                    debug!("Received Stop");
                    stop = true;
                }
                AssignmentMsg::Flush => {
                    let assignment_len = assignment.tasks.len();
                    if assignment_len > 0
                        && assignment_birth_time.elapsed().as_secs() > max_age
                    {
                        debug!(
                            "Timeout reached, flushing {} task assignment",
                            assignment_len
                        );
                        flush = true;
                    }
                }

                AssignmentMsg::Data(data) => {
                    match add_object_to_assignment(
                        &job_action,
                        *data,
                        &shark,
                        &mut assignment,
                        &mut available_space,
                        &from_shark_host,
                    ) {
                        Ok(eobj) => eobj_vec.push(eobj),
                        Err(e) => match e {
                            AssignmentAddObjectError::DuplicateObject |
                            AssignmentAddObjectError::BadMantaObject |
                            AssignmentAddObjectError::SouceIsEvacShark => {
                                // We either skipped or errored on an object,
                                // but it was the object's fault so we should
                                // continue trying to add other objects to this
                                // assignment, for this shark.

                                continue;
                            }
                            AssignmentAddObjectError::DestinationInsufficentSpace => {
                                // TODO: This can be improved via MANTA-5306
                                // We have hit out maximum for this assignment.
                                // add_object_to_assignment() added the object
                                // that would have overflowed the available
                                // space as a skipped object to the DB.

                                available_space = flush_assignment(
                                    &job_action,
                                    &mut assignment,
                                    &mut eobj_vec,
                                    &shark,
                                    &full_assignment_tx,
                                )?;

                                continue;
                            }
                        }
                    }

                    // If this is the first assignment to be added, start the
                    // clock.  We don't care about the age of 0 task
                    // assignments.
                    if assignment.tasks.len() == 1 {
                        assignment_birth_time = std::time::Instant::now();
                    }
                }
            } // End Assignment Message match block

            // Post this assignment and create a new one if:
            //  * There are any tasks in the assignment AND:
            //      * We were told to flush or stop
            //        OR
            //      * We have reached the maximum number of tasks per assignment
            if !assignment.tasks.is_empty() && flush
                || stop
                || assignment.tasks.len() >= max_tasks
            {
                flush = false;

                available_space = flush_assignment(
                    &job_action,
                    &mut assignment,
                    &mut eobj_vec,
                    &shark,
                    &full_assignment_tx,
                )?;
            }
        } // End while !stop (get next message/object)

        Ok(())
    }
}

fn validate_destination(
    mobj_value: &Value,
    evac_shark: &MantaObjectShark,
    dest_shark: &StorageNode,
) -> Option<ObjectSkippedReason> {
    let sharks = common::get_sharks_from_value(mobj_value);

    debug_assert!(sharks.is_ok(), "could not get sharks from evacuate object");

    let sharks = match sharks {
        Ok(s) => s,
        Err(e) => {
            error!("{}", e);
            #[allow(clippy::assertions_on_constants)]
            return Some(ObjectSkippedReason::SourceOtherError);
        }
    };

    trace!(
        "Validating {:#?} as dest_shark\nFor object on {:#?}\n being \
         removed from {:#?}",
        dest_shark,
        sharks,
        evac_shark
    );

    let obj_on_dest = sharks
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
        && sharks.iter().any(|s| s.datacenter == dest_shark.datacenter)
    {
        return Some(ObjectSkippedReason::ObjectAlreadyInDatacenter);
    }
    None
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
                    trace!(
                        "posting {} task assignment: {:#?}",
                        assignment.tasks.len(),
                        &assignment
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
        object: Value,
        new_shark: &StorageNode,
    ) -> Result<Value, Error>;
}

// XXX: async / surf candidate
/// Structures implementing this trait are able to get assignments from an
/// agent.
trait GetAssignment: Sync + Send {
    fn get(&self, ace: &AssignmentCacheEntry)
        -> Result<AgentAssignment, Error>;
}

fn assignment_get<T>(
    job_action: Arc<T>,
    ace: &AssignmentCacheEntry,
) -> Result<AgentAssignment, Error>
where
    T: GetAssignment,
{
    job_action.get(ace)
}

fn _checker_should_run(checker_fini_rx: &crossbeam::Receiver<FiniMsg>) -> bool {
    match checker_fini_rx.try_recv() {
        Ok(_) => {
            info!("Assignment Checker thread received shutdown message.");
            false
        }
        Err(e) => match e {
            TryRecvError::Disconnected => {
                warn!(
                    "checker fini channel disconnected before sending message. \
                    Shutting down."
                );
                false
            }
            TryRecvError::Empty => {
                trace!("No shutdown message, keep Checker running");
                true
            }
        },
    }
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
    md_update_tx: crossbeam::Sender<AssignmentCacheEntry>,
) -> Result<thread::JoinHandle<Result<(), Error>>, Error> {
    thread::Builder::new()
        .name(String::from("Assignment Checker"))
        .spawn(move || {
            let mut run = true;
            loop {
                let mut found_assignment_count = 0;
                if run {
                    run = _checker_should_run(&checker_fini_rx);
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
                    assignments.values().any(|ace| {
                        ace.state == AssignmentState::Assigned
                            || ace.state == AssignmentState::Init
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

                for ace in assignments.values() {
                    if ace.state != AssignmentState::Assigned {
                        trace!("Skipping unassigned assignment {:?}", ace);
                        continue;
                    }

                    debug!(
                        "Assignment Checker, checking: {} | {:?}",
                        ace.id, ace.state
                    );

                    // TODO: Async/await candidate
                    let ag_assignment =
                        match assignment_get(Arc::clone(&job_action), &ace) {
                            Ok(a) => a,
                            Err(e) => {
                                // If necessary the assignment and its associated
                                // objects are marked as skipped in the get()
                                // method.
                                error!("Could not get assignment: {}", e);
                                continue;
                            }
                        };

                    debug!(
                        "Got Assignment: {} {:?}",
                        ag_assignment.uuid, ag_assignment.stats
                    );
                    // If agent assignment is complete, process it and pass
                    // it to the metadata update broker.  Otherwise, continue
                    // to next assignment.
                    match ag_assignment.stats.state {
                        AgentAssignmentState::Complete(_) => {
                            // We don't want to shut this thread down simply
                            // because we have issues handling one assignment.
                            // The process() function should mark the
                            // associated objects appropriately.
                            debug!(
                                "Processing Assignment: {:?}",
                                ag_assignment
                            );
                            job_action.process(ag_assignment).unwrap_or_else(
                                |e| {
                                    error!("Error Processing Assignment {}", e);
                                },
                            );
                        }
                        _ => continue,
                    }

                    found_assignment_count += 1;

                    match md_update_tx.send(ace.to_owned()) {
                        Ok(()) => (),
                        Err(e) => {
                            job_action.mark_assignment_error(
                                &ace.id,
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

                // TODO: MANTA-5106
                if found_assignment_count == 0 {
                    trace!("Found 0 completed assignments, sleeping for 500ms");
                    thread::sleep(Duration::from_millis(500));
                    continue;
                }

                trace!(
                    "Found {} completed assignments",
                    found_assignment_count
                );
            }
            Ok(())
        })
        .map_err(Error::from)
}

enum UpdateWorkerMsg {
    Data(AssignmentCacheEntry),
    Stop,
}

fn metadata_update_worker_static(
    job_action: Arc<EvacuateJob>,
    static_update_rx: crossbeam::Receiver<UpdateWorkerMsg>,
) -> impl Fn() {
    move || {
        // For each worker we create a hash of moray clients indexed by shard.
        // If the worker exits then the clients and the associated
        // connections are dropped.  This avoids having to place locks around
        // the shard connections.  It also allows us to manage our max
        // number of per-shard connections by simply tuning the number of
        // metadata update worker threads.
        let mut client_hash: HashMap<u32, MorayClient> = HashMap::new();

        debug!(
            "Started metadata update worker: {:?}",
            thread::current().id()
        );

        loop {
            let now = std::time::Instant::now();
            let ace = match static_update_rx.recv() {
                Ok(msg) => match msg {
                    UpdateWorkerMsg::Data(d) => d,
                    UpdateWorkerMsg::Stop => {
                        debug!(
                            "metadata update worker {:?} received STOP, \
                             exiting",
                            thread::current().id()
                        );
                        break;
                    }
                },
                Err(e) => {
                    error!("Error receiving UpdateWorkerMessage: {}", e);
                    break;
                }
            };

            // Make it easier to track how busy our metadata update threads are.
            trace!(
                "Waited {}ms to get an assignment",
                now.elapsed().as_millis()
            );

            let id = ace.id.clone();
            trace!("Assignment Metadata Update Start: {}", id);
            metadata_update_assignment(&job_action, ace, &mut client_hash);
            trace!("Assignment Metadata Update Complete: {}", id);
        }
    }
}

// This worker continues to run as long as the queue has entries for it to
// work on.  If, when the worker attempts to "steal" from the queue, the
// queue is empty the worker exits.
fn metadata_update_worker_dynamic(
    job_action: Arc<EvacuateJob>,
    queue_front: Arc<Injector<DyanmicWorkerMsg>>,
) -> impl Fn() {
    move || {
        // For each worker we create a hash of moray clients indexed by shard.
        // If the worker exits then the clients and the associated
        // connections are dropped.  This avoids having to place locks around
        // the shard connections.  It also allows us to manage our max
        // number of per-shard connections by simply tuning the number of
        // metadata update worker threads.
        let mut client_hash: HashMap<u32, MorayClient> = HashMap::new();

        metrics_gauge_inc(MD_THREAD_GAUGE);

        debug!(
            "Started metadata update worker: {:?}",
            thread::current().id()
        );

        // If the queue is empty or we receive a Stop message, then break out
        // of the loop and return.
        //
        // If we get a retry error then, retry.
        //
        // Otherwise get the assignment cache entry and process it.
        loop {
            let ace = match queue_front.steal() {
                Steal::Success(dwm) => match dwm {
                    DyanmicWorkerMsg::Data(a) => a,
                    DyanmicWorkerMsg::Stop => {
                        debug!("Received stop message, exiting");
                        break;
                    }
                },
                Steal::Retry => continue,
                Steal::Empty => break,
            };
            metadata_update_assignment(&job_action, ace, &mut client_hash);
        }

        debug!("Exiting metadata update worker.");
        metrics_gauge_dec(MD_THREAD_GAUGE);
    }
}

// If a client for this shard does not exist in the hash this function will
// create one and put it in the hash, and return it as an &mut.
fn get_client_from_hash<'a>(
    job_action: &Arc<EvacuateJob>,
    client_hash: &'a mut HashMap<u32, MorayClient>,
    shard: u32,
) -> Result<&'a mut MorayClient, Error> {
    // We can't use or_insert_with() here because in the event
    // that client creation fails we want to handle that error.
    match client_hash.entry(shard) {
        Occupied(entry) => Ok(entry.into_mut()),
        Vacant(entry) => {
            debug!("Client for shard {} does not exist, creating.", shard);
            let client = match moray_client::create_client(
                shard,
                &job_action.config.domain_name,
            ) {
                Ok(client) => client,
                Err(e) => {
                    let msg = format!(
                        "Failed to get Moray Client for shard {}: {}",
                        shard, e
                    );
                    return Err(InternalError::new(
                        Some(InternalErrorCode::BadMorayClient),
                        msg,
                    )
                    .into());
                }
            };
            Ok(entry.insert(client))
        }
    }
}

// Called when we are not using batched updates or a batched update fails and
// we want to update each object one by one.
fn metadata_update_one(
    job_action: &Arc<EvacuateJob>,
    client: MetadataClientOption,
    object: &EvacuateObjectValue,
    etag: &str,
    shard: u32,
) -> Result<(), Error> {
    let mclient = match client {
        MetadataClientOption::Client(c) => c,
        MetadataClientOption::Hash(client_hash) => {
            get_client_from_hash(job_action, client_hash, shard)?
        }
    };

    let now = std::time::Instant::now();
    let ret = moray_client::put_object(mclient, object, etag)
        .map_err(|e| {
            InternalError::new(
                Some(InternalErrorCode::MetadataUpdateFailure),
                e.description(),
            )
        })
        .map_err(Error::from);

    if ret.is_err() {
        error!(
            "Failed to update 1 object in {}us",
            now.elapsed().as_micros()
        );
    } else {
        let md_update_time = now.elapsed().as_micros();
        debug!("Updated 1 object in {}us", md_update_time);
    }

    ret
}

// Attempt to update all objects in this assignment in per shard batches.
// So given an assignment with objects in two different shards, we will make
// two calls to moray to update all the objects for this assignment in their
// respective shards.
//
// If a batch fails this function falls back to updating each object
// individually for that batch.
fn metadata_update_batch(
    job_action: &Arc<EvacuateJob>,
    client_hash: &mut HashMap<u32, MorayClient>,
    batched_reqs: HashMap<u32, Vec<BatchRequest>>,
) -> Vec<ObjectId> {
    let mut marked_error = vec![];
    for (shard, requests) in batched_reqs.into_iter() {
        let num_reqs = requests.len();
        info!(
            "Updating {} objects for shard {} in a single batch",
            num_reqs, shard
        );
        let mclient = match get_client_from_hash(job_action, client_hash, shard)
        {
            Ok(c) => c,
            Err(e) => {
                // If we can't get the client for these objects there's
                // nothing we can do.  Mark them all as error.
                // TODO: want mark_many_objects_error()
                error!("Could not get client for batch update: {}", e);
                let eobj_err: EvacuateObjectError = e.into();
                for r in requests.iter() {
                    let br = match r {
                        BatchRequest::Put(br) => br,
                        _ => panic!("Unexpected Batch Request"),
                    };

                    let id = common::get_objectId_from_value(&br.value)
                        .expect("Object Id missing");

                    job_action.mark_object_error(&id, eobj_err.clone());
                    marked_error.push(id);
                }
                continue;
            }
        };

        // If we fail the batch, step through the objects and attempt to
        // update each one individually. For each object that fails to
        // update mark it as error, and add it to the marked_error Vec to
        // be trimmed from our list of successful updates later.
        let now = std::time::Instant::now();
        if let Err(e) =
            mclient.batch(&requests, &ObjectMethodOptions::default(), |_| {
                // elapsed() gives us a u128, but unfortunately AtomicU128 is
                // nightly only.
                let md_update_time = now.elapsed().as_micros();

                info!(
                    "Batch updated {} objects in {}us",
                    num_reqs, md_update_time
                );
                Ok(())
            })
        {
            error!("Batch update failed, retrying individually: {}", e);
            retry_batch_update(
                job_action,
                requests,
                shard,
                mclient,
                &mut marked_error,
            );
        }
    }
    marked_error
}

fn retry_batch_update(
    job_action: &Arc<EvacuateJob>,
    requests: Vec<BatchRequest>,
    shard: u32,
    client: &mut MorayClient,
    marked_error: &mut Vec<ObjectId>,
) {
    for r in requests.into_iter() {
        let br: BatchPutOp = match r {
            BatchRequest::Put(br) => br,
            _ => panic!("Unexpected Batch Request"),
        };

        let etag = br
            .options
            .etag
            .specified_value()
            .expect("etag should be specified");

        let o = br.value;

        if let Err(muo_err) = metadata_update_one(
            job_action,
            MetadataClientOption::Client(client),
            &o,
            &etag,
            shard,
        ) {
            let id = common::get_objectId_from_value(&o)
                .expect("cannot get objectId");

            error!("Error updating object: {}\n{:?}", muo_err, o);
            job_action.mark_object_error(&id, muo_err.into());
            marked_error.push(id);

            continue;
        }
    }
}

fn batch_add_putobj(
    batched_reqs: &mut HashMap<u32, Vec<BatchRequest>>,
    object: Value,
    shard: u32,
    etag: String,
) -> Result<(), Error> {
    let key = common::get_key_from_object_value(&object)?;
    let mut options = ObjectMethodOptions::default();

    options.etag = Etag::Specified(etag);

    let put_req = BatchPutOp {
        bucket: "manta".to_string(),
        options,
        key,
        value: object,
    };

    batched_reqs
        .entry(shard)
        .and_modify(|ent| ent.push(BatchRequest::Put(put_req.clone())))
        .or_insert_with(|| vec![BatchRequest::Put(put_req)]);

    Ok(())
}

fn metadata_update_assignment(
    job_action: &Arc<EvacuateJob>,
    ace: AssignmentCacheEntry,
    client_hash: &mut HashMap<u32, MorayClient>,
) {
    info!("Updating metadata for assignment: {}", ace.id);

    // There is one moray client per shard, so when we collect the requests
    // into a batch we need to know which moray client this is going to based
    // on the shard number.
    let mut batched_reqs: HashMap<u32, Vec<BatchRequest>> = HashMap::new();
    let mut updated_objects = vec![];
    let dest_shark = &ace.dest_shark;
    let objects = job_action
        .load_assignment_objects(&ace.id, EvacuateObjectStatus::PostProcessing);

    trace!("Updating metadata for {} objects", objects.len());

    client_hash.shrink_to_fit();

    for eobj in objects {
        let etag = eobj.etag.clone();
        let mobj = eobj.object.clone();

        // Unfortunately sqlite only accepts signed integers.  So we
        // have to do the conversion here and cross our fingers that
        // we don't have more than 2.1 billion shards.
        // We do check this value coming in from sharkspotter as well.
        if eobj.shard < 0 {
            job_action.mark_object_error(
                &eobj.id,
                EvacuateObjectError::BadShardNumber,
            );

            // TODO: panic for now, but for release we should
            // continue to next object.
            panic!("Cannot have a negative shard {:#?}", eobj);
        }

        let shard = eobj.shard as u32;

        // This function updates the manta object with the new
        // sharks, and then returns the updated Manta metadata object.
        match job_action.update_object_shark(mobj, dest_shark) {
            Ok(o) => {
                if job_action.config.options.use_batched_updates {
                    if let Err(e) =
                        batch_add_putobj(&mut batched_reqs, o, shard, etag)
                    {
                        error!(
                            "Could not add put object operation to batch ({}): \
                            {}",
                            &eobj.id, e
                        );
                        job_action.mark_object_error(&eobj.id, e.into());
                        continue;
                    }
                } else if let Err(e) = metadata_update_one(
                    job_action,
                    MetadataClientOption::Hash(client_hash),
                    &o,
                    &etag,
                    shard,
                ) {
                    error!(
                        "Error updating object:\n{:#?}\nwith dest_shark \
                         {:?}\n({}).",
                        o, dest_shark, e
                    );
                    job_action.mark_object_error(&eobj.id, e.into());
                    continue;
                }
            }

            Err(e) => {
                error!(
                    "MD Update worker: Error updating \n\n{:#?}, with \
                     dest_shark {:?}\n\n({}).",
                    &eobj.object, dest_shark, e
                );
                job_action.mark_object_error(&eobj.id, e.into());
            }
        };

        updated_objects.push(eobj);
    }

    if job_action.config.options.use_batched_updates {
        let marked_error =
            metadata_update_batch(job_action, client_hash, batched_reqs);

        // Remove any of the objects that we had to mark as "Error" from the list
        // of updated objects.
        updated_objects.retain(|o| !marked_error.contains(&o.id));
    }

    info!("Assignment Complete: {}", &ace.id);

    job_action.remove_assignment_from_cache(&ace.id);
    job_action.mark_objects_complete(updated_objects);
    // TODO: check for DB insert error
}

fn update_dynamic_metadata_threads(
    pool: &mut ThreadPool,
    queue_back: &Arc<Injector<DyanmicWorkerMsg>>,
    max_thread_count: &mut usize,
    msg: JobUpdateMessage,
) {
    // Currently there is only one valid message here so we
    // can use this irrefutable pattern.
    // If we add any additional messages we have to use match
    // statements.
    let JobUpdateMessage::Evacuate(eum) = msg;
    let EvacuateJobUpdateMessage::SetMetadataThreads(new_worker_count) = eum;
    let difference: i32 = new_worker_count as i32 - *max_thread_count as i32;

    info!(
        "Updating metadata update thread count from {} to {}.",
        *max_thread_count, new_worker_count
    );

    // If the difference is negative then we need to
    // reduce our running thread count, so inject
    // the appropriate number of Stop messages to
    // tell active threads to exit.
    // Otherwise the logic below will handle spinning up
    // more worker threads if they are needed.
    for _ in difference..0 {
        queue_back.push(DyanmicWorkerMsg::Stop);
    }
    *max_thread_count = new_worker_count;
    pool.set_num_threads(*max_thread_count);

    info!(
        "Max number of metadata update threads set to: {}",
        max_thread_count
    );
}

/// This thread runs until EvacuateJob Completion.
/// When it receives a completed Assignment it will enqueue it into a work queue
/// and then possibly starts worker thread to do the work.  The worker thread
/// comes from a pool with a tunable size.  If the max number of worker threads
/// are already running the completed Assignment stays in the queue to be picked
/// up by the next available and running worker thread.  Worker threads will
/// exit if the queue is empty or if they receive a Stop message when they
/// finish their current work and check the queue for the next Assignment.
///
/// The number of threads can be adjusted while the job is running.  See
/// EvacuateJobUpdateMessage.
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
// This implementation was changed as a workaround for MANTA-4886.  We should
// be able to revert this change once that is fixed.
// Also, this is not the cleanest implementation, but I'd rather retain as much
// of of the current layout as possible, so that if future changes are made they
// don't interfere too much with the previous (preferred) one when we
// re-implement it.
fn metadata_update_broker_dynamic(
    job_action: Arc<EvacuateJob>,
    md_update_rx: crossbeam::Receiver<AssignmentCacheEntry>,
) -> Result<thread::JoinHandle<Result<(), Error>>, Error> {
    let mut max_thread_count =
        job_action.config.options.max_metadata_update_threads;
    let mut pool = ThreadPool::with_name(
        "Dyn_MD_Update".into(),
        job_action.config.options.max_metadata_update_threads,
    );
    let queue = Arc::new(Injector::<DyanmicWorkerMsg>::new());
    let update_rx = match &job_action.update_rx {
        Some(urx) => urx.clone(),
        None => panic!(
            "Missing update_rx channel for job with dynamic update threads"
        ),
    };

    thread::Builder::new()
        .name(String::from("Metadata Update broker"))
        .spawn(move || {
            loop {
                if let Ok(msg) = update_rx.try_recv() {
                    debug!("Received metadata update message: {:#?}", msg);
                    update_dynamic_metadata_threads(
                        &mut pool,
                        &queue,
                        &mut max_thread_count,
                        msg,
                    );
                }
                let ace = match md_update_rx.recv() {
                    Ok(ace) => ace,
                    Err(e) => {
                        // If the queue is empty and there are no active or
                        // queued threads, kick one off to drain the queue.
                        if !queue.is_empty()
                            && pool.active_count() == 0
                            && pool.queued_count() == 0
                        {
                            let worker = metadata_update_worker_dynamic(
                                Arc::clone(&job_action),
                                Arc::clone(&queue),
                            );

                            pool.execute(worker);
                        }

                        warn!(
                            "Could not receive metadata from assignment \
                             checker thread: {}",
                            e
                        );

                        break;
                    }
                };

                queue.push(DyanmicWorkerMsg::Data(ace));

                // If all the pools threads are devoted to workers there's
                // really no reason to queue up a new worker.
                let total_jobs = pool.active_count() + pool.queued_count();
                trace!("Total dynamic metadata update threads: {}", total_jobs);
                if total_jobs >= pool.max_count() {
                    trace!(
                        "Total threads ({}) exceeds max thread count for pool \
                         ({}) not starting new thread",
                        total_jobs,
                        pool.max_count()
                    );
                    continue;
                }

                // XXX: async/await candidate?
                let worker = metadata_update_worker_dynamic(
                    Arc::clone(&job_action),
                    Arc::clone(&queue),
                );

                pool.execute(worker);
            }
            pool.join();
            metrics_gauge_set(MD_THREAD_GAUGE, 0);
            Ok(())
        })
        .map_err(Error::from)
}

// Note how we create a separate channel here.  We could simply increase
// the capacity of the "static_tx/rx" channel in the EvacuateJob::run()
// method but it is cleaner to keep that function generic and put the
// logic for between static and dynamic metadata update threads in the
// start_metadata_update_broker.
// Instead of using a bounded queue with condvars here we instead use the
// crossbeam bounded channel as our queue for a much cleaner implementation.
// The bounded channel will block on both send(if full) and receive(if empty).
fn _update_broker_static(
    job_action: Arc<EvacuateJob>,
    md_update_rx: crossbeam::Receiver<AssignmentCacheEntry>,
) -> Result<(), Error> {
    let num_threads = job_action.config.options.max_metadata_update_threads;
    let queue_depth = job_action.config.options.static_queue_depth;
    let (static_tx, static_rx) = crossbeam_channel::bounded(queue_depth);
    let mut thread_handles = vec![];

    // Spin up static threads
    for i in 0..num_threads {
        let th_rx = static_rx.clone();
        let handle = thread::Builder::new()
            .name(format!("MD Update [{}]", i))
            .spawn(metadata_update_worker_static(
                Arc::clone(&job_action),
                th_rx,
            ))
            .expect("create static MD update thread");

        thread_handles.push(handle);
    }

    metrics_gauge_set(MD_THREAD_GAUGE, num_threads);

    loop {
        let ace = match md_update_rx.recv() {
            Ok(ace) => ace,
            Err(e) => {
                for _ in 0..num_threads {
                    if let Err(e) = static_tx.send(UpdateWorkerMsg::Stop) {
                        error!(
                            "Error sending stop to static metadata update \
                             worker thread: {}",
                            e
                        );
                    }
                }
                warn!(
                    "MD Update: cannot receive metadata from assignment \
                     checker thread: {}",
                    e
                );
                break;
            }
        };

        let msg = UpdateWorkerMsg::Data(ace);
        if let Err(e) = static_tx.send(msg) {
            error!(
                "Error sending assignment cache entry to static metadata \
                 update worker thread: {}",
                e
            );
        }
    }

    for handle in thread_handles.into_iter() {
        handle.join().expect("join worker handle");
    }

    metrics_gauge_set(MD_THREAD_GAUGE, 0);
    Ok(())
}

fn metadata_update_broker_static(
    job_action: Arc<EvacuateJob>,
    md_update_rx: crossbeam::Receiver<AssignmentCacheEntry>,
) -> Result<thread::JoinHandle<Result<(), Error>>, Error> {
    thread::Builder::new()
        .name(String::from("Metadata Update broker"))
        .spawn(move || _update_broker_static(job_action, md_update_rx))
        .map_err(Error::from)
}

fn start_metadata_update_broker(
    job_action: Arc<EvacuateJob>,
    md_update_rx: crossbeam::Receiver<AssignmentCacheEntry>,
) -> Result<thread::JoinHandle<Result<(), Error>>, Error> {
    if job_action.config.options.use_static_md_update_threads {
        metadata_update_broker_static(job_action, md_update_rx)
    } else {
        metadata_update_broker_dynamic(job_action, md_update_rx)
    }
}

// In the future we may want to return a vector of errors.  But since our
// architecture is such that all the various threads are connected by
// channels, an error in one channel cascades to other channels.  So, it is
// most likely the case that the first error is the most (and only) valuable
// one to the user.  At any rate, all errors are still logged, but the job
// error is only for the job DB.
fn set_run_error<E>(current_error: &mut Result<(), Error>, err: E)
where
    E: Into<Error>,
{
    if current_error.is_ok() {
        *current_error = Err(err.into());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::metrics_init;
    use crate::storinfo::ChooseAlgorithm;
    use lazy_static::lazy_static;
    use quickcheck::{Arbitrary, StdThreadGen};
    use quickcheck_helpers::random::string as random_string;
    use rand::Rng;
    use rebalancer::common::ObjectSkippedReason;
    use rebalancer::libagent::{router as agent_router, AgentAssignmentStats};
    use rebalancer::metrics::MetricsMap;
    use rebalancer::util;
    use reqwest::Client;

    lazy_static! {
        static ref INITIALIZED: Mutex<bool> = Mutex::new(false);
    }

    #[derive(Deserialize, Serialize)]
    struct TestMorayObject {
        _value: String,
        _etag: String,
    }

    impl TestMorayObject {
        fn new(manta_object: String, etag: String) -> Self {
            Self {
                _value: manta_object,
                _etag: etag,
            }
        }
    }

    fn unit_test_init() {
        let mut init = INITIALIZED.lock().unwrap();
        if *init {
            return;
        }

        metrics_init(rebalancer::metrics::ConfigMetrics::default());

        thread::spawn(move || {
            let _guard = util::init_global_logger(None);
            let addr = format!("{}:{}", "0.0.0.0", 7878);

            // The reason that we call gotham::start() to start the agent as
            // opposed to something like TestServer::new() in this case is
            // because TestServer will automatically pick a port for us based on
            // what is available at the time, most likely assuring us that
            // whatever port the agent ends up starting on, it will be something
            // other than 7878.  Currently, the wiring to pass a port all the
            // way down to the threads that contact the agent does not exist.
            // If or when it does, we can use gotham's TestServer as opposed to
            // explicitly calling gotham::start().  Finally, we pass `None' to
            // the router function for the agent because it does not run a
            // metrics server during manager testing.
            gotham::start(addr, agent_router(process_task_always_pass, None));
        });

        *init = true;
    }

    // This function is supplied to the agent when we start it.  This is what
    // it will use to process all tasks received in an assignment.  This
    // particular function does not actually process the task.  Instead, it
    // declares the task as having been _successfully_ processed.  This will
    // generally be used when testing the "happy path" of most evacuation job
    // functionality.
    fn process_task_always_pass(
        task: &mut Task,
        _client: &Client,
        _metrics: &Option<MetricsMap>,
    ) {
        task.set_status(TaskStatus::Complete);
    }

    // TODO: need some more reasonable ranges here.  We will panic in the
    // event that we hit objects above 1000 zetabytes
    fn generate_storage_node(local: bool) -> StorageNode {
        // Init rand
        let mut rng = rand::thread_rng();
        let mut g = StdThreadGen::new(100);

        // calculate random fields
        let timestamp: u64 = rng.gen();
        let filesystem = random_string(&mut g, rng.gen_range(1, 20));
        let datacenter = random_string(&mut g, rng.gen_range(1, 20));
        let manta_storage_id = match local {
            true => String::from("localhost"),
            false => format!("{}.stor.joyent.us", rng.gen_range(1, 100)),
        };

        // We set available_mb to a max of u64::MAX/100 because we calculate
        // the total storage space by available_mb / (1 - percent_used), and
        // if available_mb gets randomly set to something very close to
        // std::u64::MAX, we don't want to divide by a (1 - percent_used)
        // that is less than 1 (i.e. percent_used == 0), or else we will
        // overflow u64.  In the code we will panic if this happens, which is
        // the right thing to do because std::u64::MAX MB works out to be
        // > 18,000 Zetabytes... on a single storage node.
        let available_mb: u64 = rng.gen_range(0, std::u64::MAX / 100);
        let percent_used: u8 = rng.gen_range(0, 100);

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

    fn configure_test_job_common(mut job_action: EvacuateJob) -> EvacuateJob {
        // Manually set the datacenter so that our destination validation
        // allows for same DC transfer.  This is normally done in
        // EvacuateJob::validate(), but that calls into moray to get the DC
        // name.
        job_action.from_shark.datacenter = "dc1".into();
        assert!(job_action.create_tables().is_ok());

        job_action
    }

    fn create_test_retry_job(retry_uuid: &str) -> EvacuateJob {
        let mut config = Config::default();
        let from_shark = String::from("1.stor.domain");

        config.max_fill_percentage = 100;

        let (_, update_rx) = crossbeam_channel::unbounded();
        let job_action = EvacuateJob::retry(
            from_shark,
            &config,
            &Uuid::new_v4().to_string(),
            Some(update_rx),
            retry_uuid,
        )
        .expect("initialize retry job");

        configure_test_job_common(job_action)
    }

    fn create_test_evacuate_job(max_objects: usize) -> EvacuateJob {
        let mut config = Config::default();
        let from_shark = String::from("1.stor.domain");

        config.max_fill_percentage = 100;

        let (_, update_rx) = crossbeam_channel::unbounded();
        let job_action = EvacuateJob::new(
            from_shark,
            &config,
            &Uuid::new_v4().to_string(),
            Some(update_rx),
            Some(max_objects as u32),
        )
        .expect("initialize evacuate job");

        configure_test_job_common(job_action)
    }

    fn start_test_obj_generator_thread(
        obj_tx: crossbeam_channel::Sender<EvacuateObject>,
        test_objects: Vec<MantaObject>,
        from_shark: String,
    ) -> thread::JoinHandle<Result<(), Error>> {
        thread::Builder::new()
            .name(String::from("test object generator thread"))
            .spawn(move || {
                for o in test_objects.into_iter() {
                    let shard = 1;
                    let etag = String::from("Fake_etag");
                    let mobj_value =
                        serde_json::to_string(&o).expect("mobj value");

                    let moray_value = serde_json::to_value(
                        TestMorayObject::new(mobj_value, etag),
                    )
                    .expect("moray value");

                    let manta_value =
                        sharkspotter::manta_obj_from_moray_obj(&moray_value)
                            .expect("manta value");

                    let etag =
                        sharkspotter::etag_from_moray_value(&moray_value)
                            .expect("etag from moray value");

                    let ssobj = SharkspotterMessage {
                        manta_value,
                        etag,
                        shark: from_shark.clone(),
                        shard,
                    };

                    let eobj = EvacuateObject::try_from(ssobj)
                        .expect("evac obj from sharkspotter obj");

                    match obj_tx.send(eobj) {
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
                Ok(())
            })
            .expect("failed to build object generator thread")
    }

    #[derive(Default)]
    struct EmptyStorinfo {}
    impl SharkSource for EmptyStorinfo {
        fn choose(&self, _algo: &ChooseAlgorithm) -> Option<Vec<StorageNode>> {
            None
        }
    }

    #[test]
    fn calculate_available_mb_test() {
        unit_test_init();
        let mut max_fill_percentage = 80;
        let mut storage_node = StorageNode::default();

        // --- Test exact calculation --- //
        storage_node.percent_used = 10;
        storage_node.available_mb = 900;

        let evac_dest_shark = EvacuateDestShark {
            shark: storage_node.clone(),
            status: DestSharkStatus::Ready,
            assigned_mb: 100,
        };

        // Total MB is 1000, 100(10%) is used, and 100 is assigned, so there
        // should be a total of 800 MB available.  But our max fill is 80% (or
        // 800MB), so if we consider 200 MB used, of the 800MB available, we
        // should only see 600MB available for us.
        assert_eq!(
            _calculate_available_mb(&evac_dest_shark, max_fill_percentage),
            600
        );

        // --- Test Max fill percentage < percent used returns 0 --- //
        storage_node.percent_used = 90;
        storage_node.available_mb = 100;

        let evac_dest_shark = EvacuateDestShark {
            shark: storage_node.clone(),
            status: DestSharkStatus::Ready,
            assigned_mb: 100,
        };

        // Max fill is 80 and percent_used is 90, so this should return 0
        // because we are already over our high water mark.
        assert_eq!(
            _calculate_available_mb(&evac_dest_shark, max_fill_percentage),
            0
        );

        // --- Test max_remaining < assigned_mb should return 0 --- //
        // Used = 80 MB
        // Total = 100 MB
        storage_node.percent_used = 80;
        storage_node.available_mb = 20; // Total is 100MB

        // Max fill MB = 90 MB
        // max_remaining = max_fill_mb - used_mb = 90 - 80 = 10 MB
        max_fill_percentage = 90;

        // setting assigned_mb > 10 would result in max_remaining <
        // assigned_mb and should return 0
        let evac_dest_shark = EvacuateDestShark {
            shark: storage_node.clone(),
            status: DestSharkStatus::Ready,
            assigned_mb: 11,
        };

        assert_eq!(
            _calculate_available_mb(&evac_dest_shark, max_fill_percentage),
            0
        );

        // setting assigned_mb to 9 should leave us with 1MB to spare.
        let evac_dest_shark = EvacuateDestShark {
            shark: storage_node.clone(),
            status: DestSharkStatus::Ready,
            assigned_mb: 9,
        };

        assert_eq!(
            _calculate_available_mb(&evac_dest_shark, max_fill_percentage),
            1
        );

        // --- Test 0 available_mb should result in a return value of 0 --- //
        storage_node.percent_used = 100;
        storage_node.available_mb = 0;
        let evac_dest_shark = EvacuateDestShark {
            shark: storage_node.clone(),
            status: DestSharkStatus::Ready,
            assigned_mb: 9,
        };

        // set this to 100 so we don't trip over percent_used > max_fill
        max_fill_percentage = 100;

        assert_eq!(
            _calculate_available_mb(&evac_dest_shark, max_fill_percentage),
            0
        );

        // --- Test storinfo giving us incorrect values is still safe --- //
        storage_node.percent_used = 100;
        storage_node.available_mb = 100;
        let evac_dest_shark = EvacuateDestShark {
            shark: storage_node.clone(),
            status: DestSharkStatus::Ready,
            assigned_mb: 9,
        };

        // set this to 100 so we don't trip over percent_used > max_fill
        max_fill_percentage = 100;

        assert_eq!(
            _calculate_available_mb(&evac_dest_shark, max_fill_percentage),
            0
        );
    }

    #[test]
    fn available_mb() {
        unit_test_init();

        let num_objects = 100;
        let mut g = StdThreadGen::new(10);
        let mut test_objects = HashMap::new();

        struct MockStorinfo;

        impl MockStorinfo {
            fn new() -> Self {
                MockStorinfo {}
            }
        }

        impl SharkSource for MockStorinfo {
            fn choose(&self, _: &ChooseAlgorithm) -> Option<Vec<StorageNode>> {
                let mut sharks = vec![];

                // First create two storage nodes where we will put the objects.
                for i in 1..3 {
                    let mut sn = StorageNode::default();
                    sn.manta_storage_id = format!("{}.stor.domain", i);
                    sn.datacenter = "dc1".into();
                    sharks.push(sn);
                }

                // Create a destination storage node that we will use to
                // measure our available_mb calculations.  Since there are
                // only 3 storage nodes and all objects will be on 1.stor and
                // 2.stor this one should be the only possible destination.
                let mut dest_node = StorageNode::default();
                dest_node.available_mb = 200;
                dest_node.percent_used = 80;
                dest_node.datacenter = "dc1".into();
                dest_node.manta_storage_id = "3.stor.domain".into();

                sharks.push(dest_node);

                debug!("Choosing sharks: {:#?}", &sharks);
                Some(sharks)
            }
        }

        // Storinfo
        let storinfo = MockStorinfo::new();
        let storinfo = Arc::new(storinfo);

        // Create the job
        let mut job_action = create_test_evacuate_job(num_objects);
        job_action.config.max_fill_percentage = 90;
        let job_action = Arc::new(job_action);

        // Channels
        let (full_assignment_tx, full_assignment_rx) = crossbeam::bounded(5);
        let (obj_tx, obj_rx) = crossbeam::bounded(5);
        let (checker_fini_tx, _checker_fini_rx) = crossbeam::bounded(1);

        // Generate 100 x 1MiB objects for a total of 100MiB on sharks 1 and 2.
        for _ in 0..num_objects {
            let mut mobj = MantaObject::arbitrary(&mut g);
            let mut sharks = vec![];

            mobj.content_length = 1024 * 1024; // 1MiB
            for i in 1..3 {
                let shark = MantaObjectShark {
                    datacenter: String::from("dc1"), //todo
                    manta_storage_id: format!("{}.stor.domain", i),
                };
                sharks.push(shark);
            }
            mobj.sharks = sharks;

            test_objects.insert(mobj.object_id.clone(), mobj);
        }

        // Manager Thread
        let manager_thread = start_assignment_manager(
            full_assignment_tx,
            checker_fini_tx,
            obj_rx,
            Arc::clone(&job_action),
            Arc::clone(&storinfo),
        )
        .expect("manager_thread");

        // Generator Thread
        let generator_thread_handle = start_test_obj_generator_thread(
            obj_tx,
            test_objects
                .clone()
                .values()
                .map(|v| v.to_owned())
                .collect(),
            job_action.from_shark.manta_storage_id.clone(),
        );

        // Verification Thread
        let builder = thread::Builder::new();
        let verif_job_action = Arc::clone(&job_action);
        let verification_thread = builder
            .name(String::from("verification thread"))
            .spawn(move || {
                let mut object_count = 0;
                while let Ok(assignment) = full_assignment_rx.recv() {
                    assert_eq!(
                        assignment.dest_shark.manta_storage_id,
                        "3.stor.domain".to_string()
                    );

                    assert_eq!(assignment.dest_shark.available_mb, 200);

                    object_count += assignment.tasks.len();
                }

                let available_mb = verif_job_action
                    .get_shark_available_mb(&"3.stor.domain".to_string())
                    .expect("get_shark_available_mb");

                // We should have used all the available_mb, but because of
                // MANTA-5306 we skip one every time we hit our max.  Which
                // should happen 4 times with 100 objects.
                assert_eq!(available_mb, 4);

                {
                    use self::evacuateobjects::dsl::{evacuateobjects, status};

                    let locked_conn = verif_job_action.conn.lock().expect(
                        "DB \
                         conn",
                    );

                    let skipped_objs: Vec<EvacuateObject> = evacuateobjects
                        .filter(status.eq(EvacuateObjectStatus::Skipped))
                        .load::<EvacuateObject>(&*locked_conn)
                        .expect("getting filtered objects");

                    assert_eq!(skipped_objs.len() as u64, available_mb);
                    assert_eq!(num_objects - object_count, skipped_objs.len());

                    let all_objects: Vec<EvacuateObject> = evacuateobjects
                        .load::<EvacuateObject>(&*locked_conn)
                        .expect("getting filtered objects");

                    assert_eq!(all_objects.len(), num_objects);
                }
            })
            .expect("verification thread result");

        // Join Threads
        generator_thread_handle
            .join()
            .expect("generator thread handle")
            .expect("generator thread result");
        manager_thread
            .join()
            .expect("manager_thread")
            .expect("manager thread result");
        verification_thread
            .join()
            .expect("verification thread handle");

        // Now rerun the job, except this time simulate failure for every
        // assignment.
        let mut job_action = create_test_evacuate_job(num_objects);
        job_action.config.max_fill_percentage = 90;
        let job_action = Arc::new(job_action);

        // Channels
        let (full_assignment_tx, full_assignment_rx) = crossbeam::bounded(5);
        let (obj_tx, obj_rx) = crossbeam::bounded(5);
        let (checker_fini_tx, _checker_fini_rx) = crossbeam::bounded(1);

        // Manager Thread
        let manager_thread = start_assignment_manager(
            full_assignment_tx,
            checker_fini_tx,
            obj_rx,
            Arc::clone(&job_action),
            Arc::clone(&storinfo),
        )
        .expect("manager_thread");

        // Give the manager thread a second to populate its shark hash
        thread::sleep(std::time::Duration::from_secs(1));

        // Calculate the available_mb before we simulate the rebalance of any
        // objects.
        let pre_available_mb = job_action
            .get_shark_available_mb(&"3.stor.domain".to_string())
            .expect("get_shark_available_mb");

        // Max fill: 90%
        // available_mb: 200MB
        // percent used: 80%
        // Total: 200/0.8 = 1000
        // Total Fill: 1000 * 0.9 = 900
        // Used MB: 1000 - 200 = 800
        // pre_available_mb = 900 - 800 = 100
        assert_eq!(pre_available_mb, 100);

        // Generator Thread
        let generator_thread_handle = start_test_obj_generator_thread(
            obj_tx,
            test_objects
                .clone()
                .values()
                .map(|v| v.to_owned())
                .collect(),
            job_action.from_shark.manta_storage_id.clone(),
        );

        // Verification Thread
        let builder = thread::Builder::new();
        let verif_job_action = Arc::clone(&job_action);
        let verification_thread = builder
            .name(String::from("verification thread"))
            .spawn(move || {
                let mut object_count = 0;
                while let Ok(assignment) = full_assignment_rx.recv() {
                    assert_eq!(
                        assignment.dest_shark.manta_storage_id,
                        "3.stor.domain".to_string()
                    );

                    assert_eq!(assignment.dest_shark.available_mb, 200);

                    assignment_post_fail(
                        &verif_job_action,
                        &assignment,
                        ObjectSkippedReason::NetworkError,
                        AssignmentState::Assigned,
                    );

                    object_count += assignment.tasks.len();
                }

                let available_mb = verif_job_action
                    .get_shark_available_mb(&"3.stor.domain".to_string())
                    .expect("get_shark_available_mb");

                assert_eq!(available_mb, pre_available_mb);
                assert_eq!(object_count, num_objects);

                use self::evacuateobjects::dsl::{evacuateobjects, status};

                let locked_conn = verif_job_action.conn.lock().expect(
                    "DB \
                     conn",
                );

                let skipped_objs: Vec<EvacuateObject> = evacuateobjects
                    .filter(status.eq(EvacuateObjectStatus::Skipped))
                    .load::<EvacuateObject>(&*locked_conn)
                    .expect("getting filtered objects");

                assert_eq!(skipped_objs.len(), num_objects);
            })
            .expect("verification thread result");

        // Join Threads
        generator_thread_handle
            .join()
            .expect("generator thread handle")
            .expect("generator thread result");
        manager_thread
            .join()
            .expect("manager_thread")
            .expect("manager thread result");
        verification_thread
            .join()
            .expect("verification thread handle");
    }

    #[test]
    fn no_skip() {
        use super::*;
        use rand::Rng;

        unit_test_init();

        let num_objects = 1000;
        let mut g = StdThreadGen::new(10);
        let mut rng = rand::thread_rng();
        let mut test_objects = HashMap::new();

        struct NoSkipStorinfo;
        impl NoSkipStorinfo {
            fn new() -> Self {
                NoSkipStorinfo {}
            }
        }

        impl SharkSource for NoSkipStorinfo {
            fn choose(&self, _: &ChooseAlgorithm) -> Option<Vec<StorageNode>> {
                let mut g = StdThreadGen::new(100);
                let mut sharks = vec![];
                for i in 1..10 {
                    let mut shark: StorageNode = generate_storage_node(true);

                    shark.manta_storage_id = format!("{}.stor.domain", i);
                    if i % 3 == 0 {
                        let str_len = rand::thread_rng().gen_range(1, 50);
                        shark.datacenter = random_string(&mut g, str_len);
                    } else {
                        shark.datacenter = String::from("dc1");
                    }

                    sharks.push(shark);
                }
                Some(sharks)
            }
        }

        let job_action = Arc::new(create_test_evacuate_job(num_objects));

        let storinfo = NoSkipStorinfo::new();
        let storinfo = Arc::new(storinfo);

        let (full_assignment_tx, full_assignment_rx) = crossbeam::bounded(5);
        let (obj_tx, obj_rx) = crossbeam::bounded(5);
        let (checker_fini_tx, checker_fini_rx) = crossbeam::bounded(1);
        let (md_update_tx, _) = crossbeam::bounded(1);

        for _ in 0..num_objects {
            let mut mobj = MantaObject::arbitrary(&mut g);
            let mut sharks = vec![];

            // first pass: 1 or 2
            // second pass: 3 or 4
            for i in 0..2 {
                let shark_num = rng.gen_range(1 + i * 2, 3 + i * 2);

                let shark = MantaObjectShark {
                    datacenter: String::from("dc1"), //todo
                    manta_storage_id: format!("{}.stor.domain", shark_num),
                };
                sharks.push(shark);
            }
            mobj.sharks = sharks;

            test_objects.insert(mobj.object_id.clone(), mobj);
        }

        let assignment_checker_thread = start_assignment_checker(
            Arc::clone(&job_action),
            checker_fini_rx,
            md_update_tx,
        )
        .expect("start assignment checker thread");

        let test_objects_copy = test_objects.clone();
        let manager_thread = start_assignment_manager(
            full_assignment_tx,
            checker_fini_tx,
            obj_rx,
            Arc::clone(&job_action),
            Arc::clone(&storinfo),
        )
        .expect("start assignment manager");

        let generator_thread_handle = start_test_obj_generator_thread(
            obj_tx,
            test_objects_copy.values().map(|v| v.to_owned()).collect(),
            job_action.from_shark.manta_storage_id.clone(),
        );

        let verification_objects = test_objects.clone();
        let builder = thread::Builder::new();
        let verif_job_action = Arc::clone(&job_action);
        let verification_thread = builder
            .name(String::from("verification thread"))
            .spawn(move || {
                debug!("Starting verification thread");
                let mut assignment_count = 0;
                let mut task_count = 0;
                while let Ok(assignment) = full_assignment_rx.recv() {
                    let dest_shark = assignment.dest_shark.manta_storage_id;

                    assignment_count += 1;
                    task_count += assignment.tasks.len();

                    for (tid, _) in assignment.tasks {
                        let obj = verification_objects
                            .get(&tid)
                            .expect("missing object");

                        println!(
                            "Checking that {:#?} is not in {:#?}",
                            dest_shark, obj.sharks
                        );

                        assert_eq!(
                            obj.sharks.iter().any(|shark| {
                                shark.manta_storage_id == dest_shark
                            }),
                            false
                        );
                    }

                    println!("Task COUNT {}", task_count);
                }

                use self::evacuateobjects::dsl::{
                    evacuateobjects, skipped_reason, status,
                };
                let locked_conn =
                    verif_job_action.conn.lock().expect("DB conn");

                let skipped: Vec<EvacuateObject> = evacuateobjects
                    .filter(status.eq(EvacuateObjectStatus::Skipped))
                    .load::<EvacuateObject>(&*locked_conn)
                    .expect("getting skipped objects");

                let insufficient_space: Vec<EvacuateObject> =
                    evacuateobjects
                        .filter(skipped_reason.eq(
                            ObjectSkippedReason::DestinationInsufficientSpace,
                        ))
                        .load::<EvacuateObject>(&*locked_conn)
                        .expect("getting skipped objects");

                let skip_count = skipped.len();
                let insufficient_space_skips = insufficient_space.len();

                // Because we are using random sizes for objects and storage
                // node available MB we will hit many destination
                // insufficient space skips.  In production, that shouldn't
                // happen that often as we should rarely be in a situation
                // where all of our destination storage nodes are so close
                // to full.
                println!("Num Objects: {}", num_objects);
                println!("Assignment Count: {}", assignment_count);
                println!("Task Count: {}", task_count);
                println!("Total skips: {}", skip_count);
                println!(
                    "Insufficient space skips: {}",
                    insufficient_space_skips
                );
                println!(
                    "Other Skip Count: {} (this should be a fairly small \
                     percentage of Num Objects)",
                    skip_count - insufficient_space_skips
                );
                assert_eq!(task_count + skip_count, num_objects);
            })
            .expect("verification thread");

        verification_thread.join().expect("verification join");
        generator_thread_handle
            .join()
            .expect("obj_gen thr join")
            .expect("obj_gen thr result");

        // mark all assignments as post processed so that the checker thread
        // can exit.
        let mut locked_assignments =
            job_action.assignments.write().expect("lock assignments");
        locked_assignments.iter_mut().for_each(|(_, a)| {
            a.state = AssignmentState::PostProcessed;
        });
        drop(locked_assignments);

        assignment_checker_thread
            .join()
            .expect("checker thread join")
            .expect("checker thread error");
        manager_thread
            .join()
            .expect("manager thread join")
            .expect("manager thread error");
    }

    #[test]
    fn assignment_processing_test() {
        unit_test_init();

        let num_objects = 100;
        let mut g = StdThreadGen::new(10);
        let mut eobjs = vec![];

        // Evacuate Job Action
        let job_action = create_test_evacuate_job(num_objects);

        // New Assignment
        let mut assignment = Assignment::new(StorageNode::arbitrary(&mut g));
        let uuid = assignment.id.clone();

        // Create some EvacuateObjects
        for _ in 0..num_objects {
            let mobj = MantaObject::arbitrary(&mut g);
            let mobj_value = serde_json::to_value(mobj).expect("mobj_value");
            let shard = 1;
            let etag = random_string(&mut g, 10);
            let id =
                common::get_objectId_from_value(&mobj_value).expect("objectId");

            let mut eobj = EvacuateObject {
                id,
                object: mobj_value,
                shard,
                etag,
                ..Default::default()
            };

            eobj.assignment_id = uuid.clone();
            eobjs.push(eobj);
        }

        // Put the EvacuateObject's into the DB so that the process function
        // can look them up later.
        job_action
            .insert_assignment_into_db(&mut assignment, &eobjs)
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
            task.object_id = common::get_objectId_from_value(&eobjs[i].object)
                .expect("object id for task");

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
        assignments.insert(uuid.clone(), assignment.clone().into());

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
    fn empty_storinfo_test() {
        unit_test_init();
        let storinfo = Arc::new(EmptyStorinfo {});
        let (full_assignment_tx, _) = crossbeam::bounded(5);
        let (checker_fini_tx, _) = crossbeam::bounded(1);
        let (_, obj_rx) = crossbeam::bounded(5);

        let job_action = Arc::new(create_test_evacuate_job(99));

        let assignment_manager_handle = match start_assignment_manager(
            full_assignment_tx,
            checker_fini_tx,
            obj_rx,
            Arc::clone(&job_action),
            Arc::clone(&storinfo),
        ) {
            Ok(h) => h,
            Err(e) => {
                assert!(false, "Could not start assignment manager {}", e);
                return;
            }
        };

        let ret = assignment_manager_handle
            .join()
            .expect("assignment manager handle");

        assert_eq!(ret.is_err(), true);

        match ret.unwrap_err() {
            Error::Internal(e) => {
                assert_eq!(e.code, InternalErrorCode::StorinfoError);
            }
            _ => {
                assert_eq!(1, 0, "Incorrect Error Code");
            }
        }
    }

    #[test]
    fn skip_object_test() {
        // TODO: add test that includes skipped objects
        unit_test_init();
    }

    #[test]
    fn duplicate_object_id_test() {
        // TODO: add test that includes duplicate object IDs
        unit_test_init();
    }

    #[test]
    fn validate_destination_test() {
        unit_test_init();
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
        let obj_value = serde_json::to_value(obj.clone()).expect("obj value");
        assert!(
            validate_destination(&obj_value, &from_shark, &to_shark).is_none(),
            "Failed to evacuate to another shark in the same data center."
        );

        // Test compromising fault domain.
        to_shark.datacenter = obj.sharks[1].datacenter.clone();
        let obj_value = serde_json::to_value(obj.clone()).expect("obj value");
        assert_eq!(
            validate_destination(&obj_value, &from_shark, &to_shark),
            Some(ObjectSkippedReason::ObjectAlreadyInDatacenter),
            "Attempt to place more than one object in the same data center."
        );

        // Test evacuating an object to the mako being evacuated.
        to_shark.manta_storage_id = from_shark.manta_storage_id.clone();
        let obj_value = serde_json::to_value(obj.clone()).expect("obj value");
        assert_eq!(
            validate_destination(&obj_value, &from_shark, &to_shark),
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

        let obj_value = serde_json::to_value(obj.clone()).expect("obj value");
        assert_eq!(
            validate_destination(&obj_value, &from_shark, &to_shark),
            Some(ObjectSkippedReason::ObjectAlreadyOnDestShark),
            "Attempt to evacuate an object back to its source."
        );
    }

    fn run_full_test(
        test_objects: Vec<MantaObject>,
        md_update_th: Option<
            fn(
                Arc<EvacuateJob>,
                crossbeam::Receiver<AssignmentCacheEntry>,
            )
                -> Result<thread::JoinHandle<Result<(), Error>>, Error>,
        >,
        job_action: EvacuateJob,
    ) {
        unit_test_init();

        struct MockStorinfo;

        impl MockStorinfo {
            fn new() -> Self {
                MockStorinfo {}
            }
        }

        impl SharkSource for MockStorinfo {
            fn choose(&self, _: &ChooseAlgorithm) -> Option<Vec<StorageNode>> {
                let mut rng = rand::thread_rng();
                let random = rng.gen_range(0, 10);

                if random == 0 {
                    return None;
                }

                Some(generate_sharks(random, true))
            }
        }

        let now = std::time::Instant::now();

        // Storinfo
        let storinfo = MockStorinfo::new();
        let storinfo = Arc::new(storinfo);

        // Channels
        let (full_assignment_tx, full_assignment_rx) = crossbeam::bounded(5);
        let (obj_tx, obj_rx) = crossbeam::bounded(5);
        let (md_update_tx, md_update_rx) = crossbeam::bounded(5);
        let (checker_fini_tx, checker_fini_rx) = crossbeam::bounded(1);

        // Job Action
        let job_action = Arc::new(job_action);

        // Threads
        let obj_generator_th = match &job_action.evac_type {
            EvacuateJobType::Initial => start_test_obj_generator_thread(
                obj_tx,
                test_objects,
                job_action.from_shark.manta_storage_id.clone(),
            ),
            EvacuateJobType::Retry(retry_uuid) => {
                // start local db generator
                start_local_db_generator(
                    Arc::clone(&job_action),
                    obj_tx,
                    retry_uuid,
                )
                .expect("local db generator")
            }
        };

        let metadata_update_thread = match md_update_th {
            Some(md_func) => md_func(Arc::clone(&job_action), md_update_rx)
                .expect("start metadata update thread"),
            None => start_metadata_update_broker(
                Arc::clone(&job_action),
                md_update_rx,
            )
            .expect("start metadata updater thread"),
        };

        let assignment_checker_thread = start_assignment_checker(
            Arc::clone(&job_action),
            checker_fini_rx,
            md_update_tx,
        )
        .expect("start assignment checker thread");

        let assign_post_thread =
            start_assignment_post(full_assignment_rx, Arc::clone(&job_action))
                .expect("assignment post thread");

        let manager_thread = start_assignment_manager(
            full_assignment_tx,
            checker_fini_tx,
            obj_rx,
            Arc::clone(&job_action),
            Arc::clone(&storinfo),
        )
        .expect("start assignment manager");

        obj_generator_th
            .join()
            .expect("object generator thread")
            .expect("object generator thread result");

        match manager_thread
            .join()
            .expect("test assignment manager thread")
        {
            Ok(()) => (),
            Err(e) => {
                if let Error::Internal(err) = e {
                    if err.code == InternalErrorCode::StorinfoError {
                        error!(
                            "Encountered empty storinfo on startup, exiting \
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
    }

    #[test]
    fn full_test() {
        unit_test_init();

        let num_objects: usize = 100;
        let mut test_objects = vec![];
        let mut g = StdThreadGen::new(10);

        for _ in 0..num_objects {
            let mobj = MantaObject::arbitrary(&mut g);
            test_objects.push(mobj);
        }

        let job_action = create_test_evacuate_job(num_objects);
        let job_id = job_action.db_name.clone();
        run_full_test(test_objects, None, job_action);
        info!("Completed full test: {}", job_id);
    }

    fn validate_duplicate_table(job_id: &str, expected_shards: usize) {
        use diesel::sql_query;
        use diesel::sql_types::Integer;
        #[derive(QueryableByName, Debug)]
        struct ShardCount {
            #[sql_type = "Integer"]
            array_length: i32,
        }

        let conn = match pg_db::connect_db(job_id) {
            Ok(c) => c,
            Err(e) => {
                println!("Error creating Evacuate Job: {}", e);
                std::process::exit(1);
            }
        };

        let shard_count = sql_query(
            "SELECT ARRAY_LENGTH(duplicates.shards, 1) from duplicates",
        )
        .load::<ShardCount>(&conn)
        .expect("array length query");
        assert_eq!(shard_count.len(), 1);

        let array_length = shard_count.first().expect("").array_length;
        assert_eq!(array_length as usize, expected_shards);
    }

    fn get_error_count(job_id: &str, error: &str) -> i64 {
        use crate::jobs::evacuate::evacuateobjects::dsl::{
            error as error_field, evacuateobjects,
        };
        use diesel::dsl::*;

        let conn = match pg_db::connect_db(job_id) {
            Ok(c) => c,
            Err(e) => {
                println!("Error creating Evacuate Job: {}", e);
                std::process::exit(1);
            }
        };

        evacuateobjects
            .filter(error_field.eq(error))
            .select(count(error_field))
            .first(&conn)
            .expect("error count")
    }

    #[test]
    fn test_duplicate_handler() {
        unit_test_init();

        let num_objects: usize = 100;
        let mut test_objects = vec![];
        let mut g = StdThreadGen::new(10);
        let mobj = MantaObject::arbitrary(&mut g);

        for _ in 0..num_objects {
            test_objects.push(mobj.clone());
        }

        let job_action = create_test_evacuate_job(num_objects);
        let job_id = job_action.db_name.clone();
        run_full_test(test_objects, None, job_action);
        info!("Completed duplicate handler test: {}", job_id);

        // Now get the length of the shards array in our single entry, and
        // confirm that it is num_objects - 1.
        // We expect the length to be 1 less than num_objects because this
        // will exercise the add_object_to_assignment() function, which
        // checks for duplicates in the same assignment.  This function
        // currently can't get the shard number of both the previously added
        // object and the duplicate.
        validate_duplicate_table(&job_id, num_objects - 1);
    }

    #[test]
    fn test_duplicate_handler_small_assignment() {
        unit_test_init();

        let num_objects: usize = 100;
        let mut test_objects = vec![];
        let mut g = StdThreadGen::new(10);
        let mobj = MantaObject::arbitrary(&mut g);

        for _ in 0..num_objects {
            test_objects.push(mobj.clone());
        }

        let mut job_action = create_test_evacuate_job(num_objects);
        let job_id = job_action.db_name.clone();
        job_action.config.options.max_tasks_per_assignment = 1;

        run_full_test(test_objects, None, job_action);
        info!("Completed duplicate handler test: {}", job_id);

        // We expect the length to be exactly num_objects because this
        // will skip the duplicate check in the add_object_to_assignment()
        // function.  Instead it will hit the handle_duplicate_assignment()
        // function when it tries to insert an assignment with a duplicate
        // object id.
        validate_duplicate_table(&job_id, num_objects);
    }

    #[test]
    fn test_retry_job() {
        use crate::jobs::status::{get_job_status, JobStatusResults};
        use crate::jobs::JobActionDbEntry;
        unit_test_init();

        let num_objects: usize = 5000;
        let mut test_objects = vec![];
        let mut g = StdThreadGen::new(10);

        for _ in 0..num_objects {
            let mobj = MantaObject::arbitrary(&mut g);
            test_objects.push(mobj);
        }

        // Create and run a job that will skip all objects
        let job_action = create_test_evacuate_job(num_objects);
        let initial_uuid = job_action.db_name.clone();
        run_full_test(test_objects, Some(skip_all), job_action);

        // Create and run a retry job that should find all skipped objects
        let retry_job_action = create_test_retry_job(&initial_uuid);
        let retry_job_uuid = retry_job_action.db_name.clone();
        run_full_test(vec![], None, retry_job_action);

        debug!("initial job: {}", initial_uuid);
        debug!("retry job: {}", retry_job_uuid);

        // Confirm that we processed "num_objects" objects and that they are
        // all in the Error state (see below on why).
        let job_status_results = get_job_status(
            &Uuid::from_str(&retry_job_uuid).expect("retry job uuid"),
            &JobActionDbEntry::Evacuate,
        )
        .expect("job status");

        let JobStatusResults::Evacuate(results) = job_status_results;
        assert_eq!(results.get("Total"), Some(&(num_objects as i64)));

        // Almost all objects will be errors due to bad_moray_client.  But
        // because we are using random objects we may find that shark
        // assignment validation fails in which case the object will be skipped.
        let error_count =
            results.get("Error").expect("error results").to_owned();
        let skip_count =
            results.get("Skipped").expect("skip results").to_owned();
        assert_eq!(error_count + skip_count, num_objects as i64);

        // Confirm that all of the objects failed because they couldn't get a
        // good moray client, which is expected since this test is run locally.
        let bad_moray_client_count =
            get_error_count(&retry_job_uuid, "bad_moray_client");

        assert_eq!(bad_moray_client_count, error_count);
    }

    fn skip_all(
        job_action: Arc<EvacuateJob>,
        md_update_rx: crossbeam::Receiver<AssignmentCacheEntry>,
    ) -> Result<thread::JoinHandle<Result<(), Error>>, Error> {
        thread::Builder::new()
            .name("skip_all".to_string())
            .spawn(move || {
                while let Ok(ace) = md_update_rx.recv() {
                    info!("RECEIVED ASSIGNMENT: {}", ace.id);
                    job_action.mark_assignment_skipped(
                        &ace.id,
                        ObjectSkippedReason::NetworkError,
                    );
                }
                Ok(())
            })
            .map_err(Error::from)
    }
}
