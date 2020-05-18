/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2020 Joyent, Inc.
 */

use crate::metrics::{
    metrics_error_inc, metrics_object_inc_by, metrics_skip_inc,
    metrics_skip_inc_by, ACTION_EVACUATE,
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

use crate::config::{Config, ConfigOptions};
use crate::jobs::{
    assignment_cache_usage, Assignment, AssignmentCacheEntry, AssignmentId,
    AssignmentState, StorageId,
};
use crate::moray_client;
use crate::pg_db;
use crate::storinfo::{self as mod_storinfo, SharkSource, StorageNode};

use std::collections::hash_map::Entry::{Occupied, Vacant};
use std::collections::HashMap;
use std::error::Error as _Error;
use std::fs::{self, File};
use std::io::{BufRead, BufReader, ErrorKind, Write};
use std::path::PathBuf;
use std::str::FromStr;
use std::string::ToString;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::thread;
use std::time::Duration;

use crossbeam_channel as crossbeam;
use crossbeam_channel::TryRecvError;
use crossbeam_deque::{Injector, Steal};
use lazy_static::lazy_static;
use libmanta::moray::{MantaObject, MantaObjectShark};
use moray::client::MorayClient;
use quickcheck::{Arbitrary, Gen};
use quickcheck_helpers::random::string as random_string;
use rand::seq::SliceRandom;
use regex::Regex;
use reqwest;
use serde::{self, Deserialize, Serialize};
use serde_json::Value;
use strum::IntoEnumIterator;
use threadpool::ThreadPool;
use uuid::Uuid;

lazy_static! {
    static ref OBJ_FILE_RE: Regex =
        Regex::new(r#"^shard_(?P<shard>\d+)\.objs$"#).expect("compile regex");
    static ref SHARK_DIR_RE: Regex =
        Regex::new(r#"^\d+\.stor$"#).expect("compile regex");
}

// --- Diesel Stuff, TODO This should be refactored --- //

use diesel::deserialize::{self, FromSql};
use diesel::pg::{Pg, PgConnection, PgValue};
use diesel::prelude::*;
use diesel::serialize::{self, IsNull, Output, ToSql};
use diesel::sql_types;
use std::path::Path;

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

#[derive(Insertable, Queryable, Identifiable)]
#[table_name = "evacuateobjects"]
struct UpdateEvacuateObject<'a> {
    id: &'a str,
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

impl From<Error> for EvacuateObjectError {
    fn from(error: Error) -> Self {
        if let Error::Internal(int_err) = error {
            match int_err.code {
                InternalErrorCode::BadShardNumber => {
                    EvacuateObjectError::BadShardNumber
                }
                InternalErrorCode::BadMorayObject => {
                    EvacuateObjectError::BadMorayObject
                }
                InternalErrorCode::BadMantaObject => {
                    EvacuateObjectError::BadMantaObject
                }
                InternalErrorCode::DuplicateShark => {
                    EvacuateObjectError::DuplicateShark
                }
                InternalErrorCode::SharkNotFound => {
                    EvacuateObjectError::MissingSharks
                }
                _ => EvacuateObjectError::InternalError,
            }
        } else {
            EvacuateObjectError::InternalError
        }
    }
}

impl Arbitrary for EvacuateObjectError {
    fn arbitrary<G: Gen>(g: &mut G) -> EvacuateObjectError {
        let i: usize = g.next_u32() as usize % Self::iter().count();
        Self::iter().nth(i).unwrap()
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

pub fn create_evacuateobjects_table(
    conn: &PgConnection,
) -> Result<usize, Error> {
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

    let status_check = format!("'{}'", status_strings.join("', '"));
    let error_check = format!("'{}'", error_strings.join("', '"));
    let skipped_check = format!("'{}'", skipped_strings.join("', '"));

    // TODO: check if table exists first and if so issue warning.  We may
    // need to handle this a bit more gracefully in the future for
    // restarting jobs...
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
    conn.execute(&create_query).map_err(Error::from)?;

    conn.execute(
        "CREATE INDEX assignment_id on evacuateobjects (assignment_id);",
    )
    .map_err(Error::from)
}
// --- END Diesel Stuff --- //

struct FiniMsg;

trait ObjectGenerator {
    fn generate(self) -> Result<thread::JoinHandle<Result<(), Error>>, Error>;
}

struct SharkSpotterGenerator {
    job_action: Arc<EvacuateJob>,
    min_shard: u32,
    max_shard: u32,
    domain: String,
    obj_tx: crossbeam_channel::Sender<EvacuateObject>,
}

struct FileGenerator {
    job_action: Arc<EvacuateJob>,
    directory: String,
    min_shard: u32,
    max_shard: u32,
    obj_tx: crossbeam_channel::Sender<EvacuateObject>,
}

use failure::Error as Failure;
use failure::Fail;

#[derive(Debug, Fail)]
enum FileGeneratorError {
    #[fail(
        display = "Filename ({})should be of the format <num>_shard.objs",
        filename
    )]
    ImproperlyFormattedFilename { filename: String },
    #[fail(
        display = "Filename ({})should be of the format <num>_shard.objs",
        dirname
    )]
    ImproperlyFormattedDirname { dirname: String },
    #[fail(display = "Input directory should not contain sub-directories")]
    NestedDirectory,
    #[fail(display = "Input path must end in a filename and not '..'")]
    InvalidFileName,
    #[fail(display = "Input path must start with a directory")]
    MissingDirectory,
    #[fail(display = "Directory must be named as a storage node")]
    InvalidDirName,
}

impl FileGenerator {
    /// Validate that the input directory is of the form:
    /// <shark name>/
    ///     <per_shard object file>
    ///     <per_shard object file>
    ///     <per_shard object file>
    ///     ...
    fn validate_directory(&self) -> Result<(), Failure> {
        let dir = Path::new(self.directory.as_str());
        if !dir.is_dir() {
            return Err(FileGeneratorError::MissingDirectory.into());
        }

        let dirname = match dir.file_name() {
            Some(d) => d.to_string_lossy().to_string(),
            None => {
                return Err(FileGeneratorError::InvalidDirName.into());
            }
        };

        if !SHARK_DIR_RE.is_match(&dirname) {
            return Err(FileGeneratorError::ImproperlyFormattedDirname {
                dirname,
            }
            .into());
        }

        for entry in fs::read_dir(dir)? {
            let entry = entry?.path();
            if entry.is_dir() {
                return Err(FileGeneratorError::NestedDirectory.into());
            } else if entry.is_file() {
                let filename = match entry.file_name() {
                    Some(f) => f.to_string_lossy().to_string(),
                    None => {
                        return Err(FileGeneratorError::InvalidFileName.into());
                    }
                };

                // Does this filename match the format.
                if !OBJ_FILE_RE.is_match(&filename) {
                    return Err(
                        FileGeneratorError::ImproperlyFormattedFilename {
                            filename,
                        }
                        .into(),
                    );
                }
            }
        }
        Ok(())
    }

    fn walk_shards(&self) -> Result<(), Error> {
        let dir = Path::new(self.directory.as_str());
        for shard in fs::read_dir(dir)? {
            let shard = shard?.path();
            self.walk_objects(shard)?
        }
        Ok(())
    }

    ///
    /// Walk the objects in a single file named "shard_<shard_num>.objs"
    ////
    fn walk_objects(&self, shard: PathBuf) -> Result<(), Error> {
        let captures = shard
            .to_str()
            .ok_or_else(|| {
                InternalError::new(None, "failed to convert filename to string")
            })
            .and_then(|file_str| {
                OBJ_FILE_RE.captures(file_str).ok_or_else(|| {
                    InternalError::new(None, "failed to capture shard number")
                })
            })
            .map_err(Error::from)?;

        let shard_num = captures["shard"].parse::<u32>()?;

        // TODO: Check for min & max shard?

        let reader = BufReader::new(File::open(shard)?);
        for obj in reader.lines() {
            let moray_object = serde_json::to_value(obj?)?;
            let eobj = match EvacuateObject::from_moray_object(
                moray_object,
                shard_num,
            ) {
                Ok(obj) => obj,
                Err((partial_obj, e)) => {
                    match partial_obj {
                        Some(po) => {
                            let id = common::get_objectId_from_value(&po)?;

                            _insert_bad_object(
                                &self.job_action,
                                po,
                                id,
                                e.into(),
                            );
                            // We were able to record the issue with this
                            // object, so there is no data loss.  Keep
                            // reading in objects.
                            continue;
                        }
                        None => {
                            error!("Error converting moray object: {}", e);
                            return Err(e);
                        }
                    }
                }
            };

            let id = eobj.id.clone();
            self.obj_tx
                .send(eobj)
                .map_err(CrossbeamError::from)
                .map_err(|e| {
                    error!("Error sending object: {}", e);
                    self.job_action.mark_object_error(
                        &id,
                        EvacuateObjectError::InternalError,
                    );
                    InternalError::new(
                        Some(InternalErrorCode::Crossbeam),
                        e.description(),
                    )
                })
                .map_err(Error::from)?;
        }
        Ok(())
    }
}

impl ObjectGenerator for FileGenerator {
    fn generate(self) -> Result<thread::JoinHandle<Result<(), Error>>, Error> {
        thread::Builder::new()
            .name(String::from("file_gen"))
            .spawn(move || {
                if let Err(e) = self.validate_directory() {
                    return Err(InternalError::new(
                        Some(InternalErrorCode::InvalidJobAction),
                        e.to_string(),
                    )
                    .into());
                }
                self.walk_shards()
            })
            .map_err(Error::from)
    }
}

impl ObjectGenerator for SharkSpotterGenerator {
    fn generate(self) -> Result<thread::JoinHandle<Result<(), Error>>, Error> {
        let job_action = self.job_action;
        let max_objects = job_action.max_objects;
        let shark = &job_action.from_shark.manta_storage_id;
        let obj_tx = self.obj_tx;
        let config = sharkspotter::config::Config {
            domain: self.domain,
            min_shard: self.min_shard,
            max_shard: self.max_shard,
            shark: String::from(shark.as_str()),
            ..Default::default()
        };

        debug!("Starting sharkspotter generator: {:?}", &config);

        let log = slog_scope::logger();

        thread::Builder::new()
            .name(String::from("sharkspotter_gen"))
            .spawn(move || {
                let mut count = 0;
                sharkspotter::run(config, log, move |moray_object, shard| {
                    trace!(
                        "Sharkspotter discovered object: {:#?}",
                        &moray_object
                    );
                    count += 1;
                    // while testing, limit the number of objects processed for now
                    if let Some(max) = max_objects {
                        if count > max {
                            return Err(std::io::Error::new(
                                ErrorKind::Interrupted,
                                "Max Objects Limit Reached",
                            ));
                        }
                    }

                    // If the call to from_moray_object() fails, we will
                    // return here.  If we fail to even get a partial evacuate
                    // object then we should return error here and stop
                    // scanning.  If we get enough of an object that we can at
                    // least record the error in our persistent store then
                    // return Ok(()) and keep scanning.
                    let eobj = match EvacuateObject::from_moray_object(
                        moray_object,
                        shard,
                    ) {
                        Ok(obj) => obj,
                        Err((partial_obj, e)) => match partial_obj {
                            Some(po) => {
                                let id = common::get_objectId_from_value(&po)
                                    .map_err(|e| {
                                    std::io::Error::new(
                                        ErrorKind::Other,
                                        e.description(),
                                    )
                                })?;

                                _insert_bad_object(
                                    &job_action,
                                    po,
                                    id,
                                    e.into(),
                                );
                                return Ok(());
                            }
                            None => {
                                error!("Error converting moray object: {}", e);
                                return Err(std::io::Error::new(
                                    ErrorKind::Other,
                                    e,
                                ));
                            }
                        },
                    };

                    let id = eobj.id.clone();
                    obj_tx.send(eobj).map_err(CrossbeamError::from).map_err(
                        |e| {
                            error!("Sharkspotter: Error sending object: {}", e);
                            job_action.mark_object_error(
                                &id,
                                EvacuateObjectError::InternalError,
                            );
                            // TODO crossbeam error
                            std::io::Error::new(
                                ErrorKind::Other,
                                e.description(),
                            )
                        },
                    )
                })
                .map_err(|e| {
                    if e.kind() == ErrorKind::Interrupted {
                        if let Some(max) = max_objects {
                            if count > max {
                                return InternalError::new(
                                    Some(InternalErrorCode::MaxObjectsLimit),
                                    "Max Objects Limit Reached",
                                )
                                .into();
                            }
                        }
                    }
                    e.into()
                })
            })
            .map_err(Error::from)
    }
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

impl EvacuateObject {
    fn from_parts(
        manta_object: Value,
        id: ObjectId,
        etag: String,
        shard: i32,
    ) -> Self {
        Self {
            assignment_id: String::new(),
            id,
            object: manta_object,
            shard,
            etag,
            ..Default::default()
        }
    }

    // This function converts a moray object to an evacuate object.
    // On error it returns a tuple of (Option<MantaObject>, Error>).  The
    // reason for this is so that on return callers can insert the bad manta
    // object into the local DB so that we track the data.  If the object is
    // missing it means that the input moray_object was so malformed that we
    // couldn't even get the corresponding manta object from it.  If the
    // return value is Err(None, Error), the caller should consider this a
    // critical failure and consider failing the evacuate job as data loss
    // is likely.
    fn from_moray_object(
        moray_object: Value,
        shard: u32,
    ) -> Result<Self, (Option<Value>, Error)> {
        // For now we consider a poorly formatted manta object or a
        // missing objectId to be critical failures.  We cannot
        // insert an evacuate object without an objectId, so we can't record
        // an error here.  If we keep scanning at this point we risk losing
        // data, so we must let the caller know that this is a critical failure.
        let manta_object =
            match sharkspotter::manta_obj_from_moray_obj(&moray_object) {
                Ok(o) => o,
                Err(e) => {
                    return Err((
                        None,
                        InternalError::new(
                            Some(InternalErrorCode::BadMantaObject),
                            e,
                        )
                        .into(),
                    ))
                }
            };

        // A missing objectID is also considered to be a critical failure.
        let id = common::get_objectId_from_value(&manta_object)
            .map_err(|e| (None, e))?;

        // From here on we have enough information to record an error in our
        // persistent store.

        // If we fail to get the etag then we can't rebalance this
        // object, but we return Ok(()) because we want to keep
        // scanning for other objects.
        let etag = match moray_object.get("_etag") {
            Some(tag) => match serde_json::to_string(tag) {
                Ok(t) => t.replace("\"", ""),
                Err(e) => {
                    let err_msg =
                        format!("Cannot convert etag to string: {}", e);
                    error!("{}", err_msg);
                    return Err((
                        Some(manta_object),
                        InternalError::new(
                            Some(InternalErrorCode::BadMorayObject),
                            err_msg,
                        )
                        .into(),
                    ));
                }
            },
            None => {
                return Err((
                    Some(manta_object),
                    InternalError::new(
                        Some(InternalErrorCode::BadMorayObject),
                        "moray object is missing _etag",
                    )
                    .into(),
                ));
            }
        };

        // TODO: build a test for this
        if shard > std::i32::MAX as u32 {
            let err_msg =
                format!("Found shard number over int32 max for: {}", id);
            error!("{}", err_msg);

            return Err((
                Some(manta_object),
                InternalError::new(
                    Some(InternalErrorCode::BadShardNumber),
                    err_msg,
                )
                .into(),
            ));
        }

        Ok(Self::from_parts(manta_object, id, etag, shard as i32))
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum DestSharkStatus {
    Init,
    Assigned,
    Ready,
}

///
/// Default souce is SharkSpotter.  To specify a file source add the
/// following to the params section of your evacuate job payload:
///
/// ```json
/// "source": {
///     "type": "file",
///     "options": "/1.stor"
/// }
/// ```
///
/// To use the default sharkspotter source, simply leave this block out, or
/// explicitly specify it like so:
///
/// ```json
/// "source": {
///     "type": "sharkspotter"
/// }
/// ```
///
#[derive(Deserialize, Serialize)]
#[serde(tag = "type", content = "options")]
#[serde(rename_all = "lowercase")]
pub enum ObjectSource {
    SharkSpotter,
    File(String),
}

impl Default for ObjectSource {
    fn default() -> ObjectSource {
        ObjectSource::SharkSpotter
    }
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
    pub assignments: RwLock<HashMap<AssignmentId, AssignmentCacheEntry>>,

    /// The shark to evacuate.
    pub from_shark: MantaObjectShark,

    /// The minimum available space for a shark to be considered a destination.
    pub min_avail_mb: Option<u64>,

    // TODO: Maximum total size of objects to include in a single assignment.

    // TODO: max number of shark threads
    pub conn: Mutex<PgConnection>,

    /// domain_name of manta deployment
    pub domain_name: String,

    pub object_source: ObjectSource,

    /// Tunable options
    pub options: ConfigOptions,

    pub post_client: reqwest::Client,

    pub get_client: reqwest::Client,

    pub bytes_transferred: AtomicU64,

    /// TESTING ONLY
    pub max_objects: Option<u32>,
}

impl EvacuateJob {
    /// Create a new EvacuateJob instance.
    /// As part of this initialization also create a new PgConnection.
    pub fn new(
        storage_id: String,
        domain_name: &str,
        db_name: &str,
        object_source: ObjectSource,
        options: ConfigOptions,
        max_objects: Option<u32>,
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
        from_shark.manta_storage_id = storage_id;

        Ok(Self {
            min_avail_mb: Some(1000), // TODO: config
            dest_shark_list: RwLock::new(HashMap::new()),
            assignments: RwLock::new(HashMap::new()),
            from_shark,
            conn: Mutex::new(conn),
            domain_name: domain_name.to_string(),
            object_source,
            options,
            max_objects,
            post_client: reqwest::Client::new(),
            get_client: reqwest::Client::new(),
            bytes_transferred: AtomicU64::new(0),
        })
    }

    pub fn create_table(&self) -> Result<usize, Error> {
        let conn = self.conn.lock().expect("DB conn lock");
        create_evacuateobjects_table(&*conn)
    }

    // This is not purely a validation function.  We do set the from_shark field
    // in here so that it has the complete manta_storage_id and datacenter.
    fn validate(&mut self) -> Result<(), Error> {
        let from_shark = moray_client::get_manta_object_shark(
            &self.from_shark.manta_storage_id,
            &self.domain_name,
        )?;

        self.from_shark = from_shark;

        Ok(())
    }

    pub fn run(mut self, config: &Config) -> Result<(), Error> {
        self.validate()?;

        let mut ret = Ok(());

        // job_action will be shared between threads so create an Arc for it.
        let job_action = Arc::new(self);
        let domain = &config.domain_name;

        // TODO: How big should each channel be?
        // Set up channels for thread to communicate.
        let (obj_tx, obj_rx) = crossbeam::bounded(5);
        let (full_assignment_tx, full_assignment_rx) = crossbeam::bounded(5);
        let (md_update_tx, md_update_rx) = crossbeam::bounded(5);
        let (checker_fini_tx, checker_fini_rx) = crossbeam::bounded(1);

        // TODO: lock evacuating server to readonly
        // TODO: add thread barriers MANTA-4457

        let generator_thread =
            start_generator_thread(Arc::clone(&job_action), config, obj_tx)?;

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

        generator_thread
            .join()
            .expect("Generator Thread")
            .unwrap_or_else(|e| {
                error!("Error joining Generator Thread: {}\n", e);
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

        debug!(
            "Evacuate Job transferred {} bytes",
            job_action.bytes_transferred.load(Ordering::SeqCst)
        );
        ret
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
    ) -> Result<(), Error> {
        info!("Skipping object {}: {}.", &eobj.id, reason);
        metrics_skip_inc(Some(&reason.to_string()));

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
    ) -> Result<Vec<StorageNode>, Error>
    where
        S: SharkSource + 'static,
    {
        let mut shark_list: Vec<StorageNode> = vec![];
        let mut tries = 0;
        let shark_list_retry_delay = std::time::Duration::from_millis(500);

        trace!("Getting new shark list");
        while tries < retries {
            if let Some(valid_sharks) =
                storinfo.choose(&mod_storinfo::ChooseAlgorithm::Default(algo))
            {
                self.update_dest_sharks(&valid_sharks);
            }

            shark_list = self
                .dest_shark_list
                .read()
                .expect("dest_shark_list read lock")
                .values()
                .map(|v| v.shark.to_owned())
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
            shark_list.sort_by_key(|s| s.available_mb);
            shark_list.as_mut_slice().reverse();
            Ok(shark_list)
        }
    }

    // TODO: Consider doing batched inserts: MANTA-4464.
    fn insert_into_db(&self, obj: &EvacuateObject) -> Result<usize, Error> {
        use self::evacuateobjects::dsl::*;

        let locked_conn = self.conn.lock().expect("DB conn lock");

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

        Ok(ret)
    }

    #[allow(clippy::ptr_arg)]
    fn insert_assignment_into_db(
        &self,
        assign_id: &AssignmentId,
        vec_objs: &[EvacuateObject],
    ) -> Result<usize, Error> {
        use self::evacuateobjects::dsl::*;

        debug!(
            "Inserting {} objects for assignment ({}) into db",
            vec_objs.len(),
            assign_id
        );

        let locked_conn = self.conn.lock().expect("DB conn lock");
        let ret = diesel::insert_into(evacuateobjects)
            .values(vec_objs)
            .execute(&*locked_conn)
            .unwrap_or_else(|e| {
                let msg = format!("Error inserting object into DB: {}", e);
                error!("{}", msg);
                panic!(msg);
            });

        debug!(
            "inserted {} objects for assignment {} into db",
            ret, assign_id
        );

        assert_eq!(ret, vec_objs.len());

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
            "Marking {} objects in assignment ({}) as error: {:?}",
            update_cnt, assignment_uuid, err
        );

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
    fn mark_dest_shark(&self, dest_shark: &StorageId, status: DestSharkStatus) {
        if let Some(shark) = self
            .dest_shark_list
            .write()
            .expect("dest_shark_list write")
            .get_mut(dest_shark)
        {
            debug!("Updating shark '{}' to {:?} state", dest_shark, status,);
            shark.status = status;
        } else {
            warn!("Could not find shark: '{}'", dest_shark);
        }
    }

    #[allow(clippy::ptr_arg)]
    fn mark_dest_shark_assigned(&self, dest_shark: &StorageId) {
        self.mark_dest_shark(dest_shark, DestSharkStatus::Assigned)
    }

    #[allow(clippy::ptr_arg)]
    fn mark_dest_shark_ready(&self, dest_shark: &StorageId) {
        self.mark_dest_shark(dest_shark, DestSharkStatus::Ready)
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
/// 3. Update shark available_mb.
/// 4. Implicitly drop (free) the Assignment on return.
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
            // received an updated list of sharks from the storinfo service and
            // as a result removed this one from it's active hash.  Regardless
            // the assignment has been posted and is actively running on the
            // shark's rebalancer agent at this point.
            warn!(
                "Could not find destination shark ({}) to update available \
                 MB.",
                &assignment.dest_shark.manta_storage_id
            );
        }
    }

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
    /// This function only updates the local database in the event that an
    /// error is encountered.  This allows the caller to easily batch database
    /// updates for successful objects.
    fn update_object_shark(
        &self,
        mut object: Value,
        new_shark: &StorageNode,
        etag: String,
        mclient: &mut MorayClient,
    ) -> Result<Value, Error> {
        let old_shark = &self.from_shark;
        let mut shark_found = false;

        // Get the sharks in the form of Vec<MantaObjectShark> to make it
        // easy to manipulate.
        let mut sharks = common::get_sharks_from_value(&object)?;
        let id = common::get_objectId_from_value(&object)?;

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
                    error!("{}", &msg);
                    self.mark_object_error(
                        &id,
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
                self.mark_object_error(
                    &id,
                    EvacuateObjectError::BadMantaObject,
                );
                return Err(InternalError::new(
                    Some(InternalErrorCode::BadMantaObject),
                    msg,
                )
                .into());
            }
        }

        if let Err(e) = moray_client::put_object(mclient, &object, &etag) {
            self.mark_object_error(
                &id,
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
                info!(
                    "Assignment {} resulted in {} failed tasks.",
                    &ace.id,
                    failed_tasks.len()
                );
                trace!("{:#?}", &failed_tasks);

                let objects = self.load_assignment_objects(
                    &ace.id,
                    EvacuateObjectStatus::Assigned,
                );

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

        Ok(())
    }
}

fn _insert_bad_object(
    job_action: &EvacuateJob,
    object: Value,
    id: ObjectId,
    error_code: EvacuateObjectError,
) {
    let eobj = EvacuateObject {
        id,
        object,
        status: EvacuateObjectStatus::Error,
        error: Some(error_code),
        ..Default::default()
    };

    job_action
        .insert_into_db(&eobj)
        .expect("Error inserting bad EvacuateObject into DB");
}

fn start_generator_thread(
    job_action: Arc<EvacuateJob>,
    config: &Config,
    obj_tx: crossbeam::Sender<EvacuateObject>,
) -> Result<thread::JoinHandle<Result<(), Error>>, Error> {
    let min_shard = config.min_shard_num();
    let max_shard = config.max_shard_num();

    match &job_action.object_source {
        ObjectSource::File(directory) => {
            let fgen = FileGenerator {
                job_action: Arc::clone(&job_action), // XXX Used?
                directory: directory.to_owned(),
                min_shard,
                max_shard,
                obj_tx,
            };
            fgen.generate()
        }
        ObjectSource::SharkSpotter => {
            let domain = &config.domain_name;

            let shark_gen = SharkSpotterGenerator {
                domain: domain.clone(),
                job_action: Arc::clone(&job_action),
                min_shard,
                max_shard,
                obj_tx,
            };
            shark_gen.generate()
        }
    }
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
            error!(
                "Error stopping shark ({}) assignments: {}",
                shark_id,
                CrossbeamError::from(e)
            );
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
        .spawn(move || {
            let mut done = false;
            let max_sharks = job_action.options.max_sharks;
            let max_tasks_per_assignment =
                job_action.options.max_tasks_per_assignment;

            let algo = mod_storinfo::DefaultChooseAlgorithm {
                min_avail_mb: job_action.min_avail_mb,
                blacklist: vec![],
            };

            let mut shark_hash: HashMap<StorageId, SharkHashEntry> =
                HashMap::new();

            while !done {
                // TODO: MANTA-4519
                // get a fresh shark list
                let mut shark_list = job_action.get_shark_list(
                    Arc::clone(&storinfo),
                    &algo,
                    3,
                )?;

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
                _stop_join_some_assignment_threads(
                    &mut shark_hash,
                    remove_keys,
                );

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
                    let mut eobj = match obj_rx.recv() {
                        Ok(obj) => {
                            trace!("Received object {:#?}", &obj);
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
                            job_action.skip_object(&mut eobj, last_reason)?;
                            continue;
                        }
                    };

                    let shark_id =
                        shark_hash_entry.shark.manta_storage_id.clone();

                    // Send the evacuate object to the
                    // shark_assignment_generator.
                    if let Err(e) = shark_hash_entry
                        .tx
                        .send(AssignmentMsg::Data(Box::new(eobj)))
                    {
                        error!(
                            "Error sending object to shark ({}) \
                             generator thread: {}",
                            &shark_id,
                            CrossbeamError::from(e)
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
            _stop_join_drain_assignment_threads(shark_hash);

            info!("Manager: Shutting down assignment checker");
            checker_fini_tx.send(FiniMsg).expect("Fini Msg");
            Ok(())
        })
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

        job_action
            .assignments
            .write()
            .expect("assignments write lock")
            .insert(assignment_uuid.clone(), assignment.clone().into());

        info!("Sending Assignment: {}", assignment_uuid);
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

    Ok(())
}

fn shark_assignment_generator(
    job_action: Arc<EvacuateJob>,
    shark: StorageNode, // TODO: reference?
    assign_msg_rx: crossbeam::Receiver<AssignmentMsg>,
    full_assignment_tx: crossbeam::Sender<Assignment>,
) -> impl Fn() -> Result<(), Error> {
    move || {
        let max_tasks = job_action.options.max_tasks_per_assignment;
        let max_age = job_action.options.max_assignment_age;
        let from_shark_host = job_action.from_shark.manta_storage_id.clone();
        let mut assignment = Assignment::new(shark.clone());
        let mut assignment_birth_time = std::time::Instant::now();
        let mut flush = false;
        let mut stop = false;
        let mut eobj_vec = vec![];

        // There are two places where we "manage" the available space on a
        // shark.  One is in assignment_post_success() where the dest shark
        // entry in the EvacuateJobAction's hash is updated.  The other is here.
        // If this thread is still running when the shark enters the Ready
        // state again (when there are no active assignments on it), but
        // before the storinfo updates the amount of available space (or before
        // storinfo is re-queried for an updated) then we want to have some
        // idea of what we are working with.
        let mut available_space = assignment.max_size;

        // TODO: get any objects from the DB that were previously supposed to
        // be assigned to this shark but the shark was busy at that time.
        while !stop {
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
                    job_action
                        .mark_dest_shark_assigned(&shark.manta_storage_id);

                    job_action
                        .insert_assignment_into_db(&assignment.id, &eobj_vec)?;

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
                    let mut eobj = *data;
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
                                continue;
                            }
                        };

                    let source = match manta_object
                        .sharks
                        .iter()
                        .find(|s| s.manta_storage_id != from_shark_host)
                    {
                        Some(src) => src,
                        None => {
                            // The only shark we could find was the one that
                            // is being evacuated.
                            job_action.skip_object(
                                &mut eobj,
                                ObjectSkippedReason::SourceIsEvacShark,
                            )?;
                            continue;
                        }
                    };

                    // Make sure there is enough space for this object on the
                    // shark.
                    let content_mb =
                        manta_object.content_length / (1024 * 1024);
                    if content_mb > available_space {
                        job_action.skip_object(
                            &mut eobj,
                            ObjectSkippedReason::DestinationInsufficientSpace,
                        )?;

                        break;
                    }

                    assignment.tasks.insert(
                        manta_object.object_id.to_owned(),
                        Task {
                            object_id: manta_object.object_id.to_owned(),
                            owner: manta_object.owner.to_owned(),
                            md5sum: manta_object.content_md5.to_owned(),
                            source: source.to_owned(),
                            status: TaskStatus::Pending,
                        },
                    );

                    // If this is the first assignment to be added, start the
                    // clock.  We don't care about the age of 0 task
                    // assignments.
                    if assignment.tasks.len() == 1 {
                        assignment_birth_time = std::time::Instant::now();
                    }

                    assignment.total_size += content_mb;
                    available_space -= content_mb;

                    trace!(
                        "{}: Available space: {} | Tasks: {}",
                        assignment.id,
                        &available_space,
                        &assignment.tasks.len()
                    );

                    eobj.status = EvacuateObjectStatus::Assigned;
                    eobj.assignment_id = assignment.id.clone();
                    eobj_vec.push(eobj.clone());
                }
            }

            // Post this assignment and create a new one if:
            //  * There are any tasks in the assignment AND:
            //      * We were told to flush or stop
            //      OR
            //      * We have reached the maximum number of tasks per assignment
            if !assignment.tasks.is_empty() && flush
                || stop
                || assignment.tasks.len() >= max_tasks
            {
                let assignment_size = assignment.total_size;

                flush = false;

                job_action.mark_dest_shark_assigned(&shark.manta_storage_id);

                job_action
                    .insert_assignment_into_db(&assignment.id, &eobj_vec)?;

                _channel_send_assignment(
                    Arc::clone(&job_action),
                    &full_assignment_tx,
                    assignment,
                )?;

                // Re-set the available space to half of what is remaining.
                // See comment above where we initialize 'available_space'
                // for details.
                available_space = (shark.available_mb - assignment_size) / 2;
                assignment = Assignment::new(shark.clone());
                assignment.max_size = available_space;

                eobj_vec = Vec::new();
            }
        }
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
        object: Value,
        new_shark: &StorageNode,
        etag: String,
        mclient: &mut MorayClient,
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

                    found_assignment_count += 1;

                    // Mark the shark associated with this assignment as Ready
                    job_action.mark_dest_shark_ready(
                        &ace.dest_shark.manta_storage_id,
                    );

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
                    trace!("Found 0 completed assignments, sleeping for 100ms");
                    thread::sleep(Duration::from_millis(100));
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

fn reconnect_and_reupdate(
    job_action: &EvacuateJob,
    mobj: Value,
    shard: u32,
    dest_shark: &StorageNode,
    etag: String,
) -> Option<(Value, MorayClient)> {
    let mobj_clone = mobj.clone();
    let mut rclient =
        match moray_client::create_client(shard, &job_action.domain_name) {
            Ok(client) => client,
            Err(e) => {
                error!(
                "(on retry) MD Update Worker: failed to get moray client for \
                 shard number {}. Cannot update metadata for {:#?}\n{}",
                shard, mobj, e
            );
                return None;
            }
        };

    let robj = match job_action.update_object_shark(
        mobj,
        dest_shark,
        etag,
        &mut rclient,
    ) {
        Ok(robj) => robj,
        Err(e) => {
            error!(
                "(on retry) MD Update worker: Error \
                 updating \n\n{:#?}, with dest_shark \
                 {:?}\n\n{}",
                &mobj_clone, dest_shark, e
            );
            return None;
        }
    };

    Some((robj, rclient))
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
    queue_front: Arc<Injector<AssignmentCacheEntry>>,
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
            let ace = match queue_front.steal() {
                Steal::Success(a) => a,
                Steal::Retry => continue,
                Steal::Empty => break,
            };
            metadata_update_assignment(&job_action, ace, &mut client_hash);
        }
    }
}

fn metadata_update_assignment(
    job_action: &Arc<EvacuateJob>,
    ace: AssignmentCacheEntry,
    client_hash: &mut HashMap<u32, MorayClient>,
) {
    info!("Updating metadata for assignment: {}", ace.id);

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
        trace!("Getting client for shard {}", shard);

        // We can't use or_insert_with() here because in the event
        // that client creation fails we want to handle that error.
        let mclient = match client_hash.entry(shard) {
            Occupied(entry) => entry.into_mut(),
            Vacant(entry) => {
                debug!("Client for shard {} does not exist, creating.", shard);
                let client = match moray_client::create_client(
                    shard,
                    &job_action.domain_name,
                ) {
                    Ok(client) => client,
                    Err(e) => {
                        job_action.mark_object_error(
                            &eobj.id,
                            EvacuateObjectError::BadMorayClient,
                        );
                        error!(
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
        match job_action.update_object_shark(mobj, dest_shark, etag, mclient) {
            Ok(o) => o,
            Err(e) => {
                warn!(
                    "MD Update worker: Error updating \n\n{:#?}, with \
                     dest_shark {:?}\n\n({}).  Retrying...",
                    &eobj.object, dest_shark, e
                );

                // In testing we have seen this fail only due to
                // connection issues with rust-cueball / rust-fast.
                // There also does not seem to be a good way to check
                // if the connection is in a good state before trying
                // this reconnect and re-update.  So for now we
                // blindly try to reconnect and re-update the object.
                // MANTA-4630 (MANTA-5158)

                let rmobj = eobj.object.clone();
                let retag = eobj.etag.clone();
                match reconnect_and_reupdate(
                    &job_action,
                    rmobj,
                    shard,
                    dest_shark,
                    retag,
                ) {
                    Some((robj, rclient)) => {
                        // Implicitly drop the client returned
                        // from the hash which is probably bad
                        client_hash.insert(shard, rclient);

                        // Initially the object was marked as error.
                        // Returning the retried object here will
                        // allow it to be added to the 'updated_objects'
                        // Vec and later marked 'Complete' in the
                        // database.
                        robj
                    }
                    None => {
                        continue;
                    }
                }
            }
        };

        eobj.object
            .get("contentLength")
            .and_then(|cl| {
                if let Some(bytes) = cl.as_u64() {
                    // TODO: metrics
                    job_action
                        .bytes_transferred
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

        updated_objects.push(eobj.id.clone());
    }

    debug!("Updated Objects: {:?}", updated_objects);

    // TODO: Should the assignment be entered into the DB for a
    // persistent log?
    // Mark this assignment in the event that we do dump core on the
    // next instruction below, or if we do want to save off the
    // information from the assignment in the DB.
    match job_action
        .set_assignment_state(&ace.id, AssignmentState::PostProcessed)
    {
        Ok(()) => (),
        Err(e) => panic!("{}", e),
    }

    info!("Assignment Complete: {}", &ace.id);

    job_action.remove_assignment_from_cache(&ace.id);

    metrics_object_inc_by(Some(ACTION_EVACUATE), updated_objects.len());

    // TODO: check for DB insert error
    job_action
        .mark_many_objects(updated_objects, EvacuateObjectStatus::Complete);
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
    let pool = ThreadPool::new(job_action.options.max_metadata_update_threads);
    let queue = Arc::new(Injector::<AssignmentCacheEntry>::new());
    let queue_back = Arc::clone(&queue);

    thread::Builder::new()
        .name(String::from("Metadata Update broker"))
        .spawn(move || {
            loop {
                let ace = match md_update_rx.recv() {
                    Ok(ace) => ace,
                    Err(e) => {
                        // If the queue is empty and there are no active or
                        // queued threads, kick one off to drain the queue.
                        if !queue_back.is_empty()
                            && pool.active_count() == 0
                            && pool.queued_count() == 0
                        {
                            let worker = metadata_update_worker_dynamic(
                                Arc::clone(&job_action),
                                Arc::clone(&queue),
                            );

                            pool.execute(worker);
                        }
                        error!(
                            "MD Update: Error receiving metadata from \
                             assignment checker thread: {}",
                            e
                        );
                        break;
                    }
                };

                queue_back.push(ace);

                // If all the pools threads are devoted to workers there's
                // really no reason to queue up a new worker.
                let total_jobs = pool.active_count() + pool.queued_count();
                if total_jobs >= pool.max_count() {
                    trace!(
                        "Reached max thread count for pool not starting \
                         new thread"
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
    let num_threads = job_action.options.max_metadata_update_threads;
    let queue_depth = job_action.options.static_queue_depth;
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
    if job_action.options.use_static_md_update_threads {
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
    use rebalancer::libagent::{router as agent_router, AgentAssignmentStats};
    use rebalancer::util;
    use reqwest::Client;

    lazy_static! {
        static ref INITIALIZED: Mutex<bool> = Mutex::new(false);
    }

    fn unit_test_init() {
        let mut init = INITIALIZED.lock().unwrap();
        if *init {
            return;
        }

        metrics_init(rebalancer::metrics::ConfigMetrics::default());

        thread::spawn(move || {
            let _guard = util::init_global_logger();
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
    fn process_task_always_pass(task: &mut Task, _client: &Client) {
        task.set_status(TaskStatus::Complete);
    }

    fn generate_storage_node(local: bool) -> StorageNode {
        let mut rng = rand::thread_rng();
        let mut g = StdThreadGen::new(100);
        let available_mb: u64 = rng.gen();
        let percent_used: u8 = rng.gen_range(0, 101);
        let filesystem = random_string(&mut g, rng.gen_range(1, 20));
        let datacenter = random_string(&mut g, rng.gen_range(1, 20));
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

    #[derive(Default)]
    struct EmptyStorinfo {}
    impl SharkSource for EmptyStorinfo {
        fn choose(&self, _algo: &ChooseAlgorithm) -> Option<Vec<StorageNode>> {
            None
        }
    }

    #[test]
    fn no_skip() {
        use super::*;
        use rand::Rng;

        unit_test_init();
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
                        shark.datacenter = String::from("foo");
                    }

                    sharks.push(shark);
                }
                Some(sharks)
            }
        }

        let from_shark = String::from("1.stor.domain");
        let job_action = EvacuateJob::new(
            from_shark,
            "fakedomain.us",
            &Uuid::new_v4().to_string(),
            ObjectSource::default(),
            ConfigOptions::default(),
            Some(100),
        )
        .expect("initialize evacuate job");
        assert!(job_action.create_table().is_ok());

        let job_action = Arc::new(job_action);
        let storinfo = NoSkipStorinfo::new();
        let storinfo = Arc::new(storinfo);

        let (full_assignment_tx, full_assignment_rx) = crossbeam::bounded(5);
        let (obj_tx, obj_rx) = crossbeam::bounded(5);
        let (checker_fini_tx, checker_fini_rx) = crossbeam::bounded(1);
        let (md_update_tx, _) = crossbeam::bounded(1);

        let mut test_objects = HashMap::new();

        let mut g = StdThreadGen::new(10);

        let mut rng = rand::thread_rng();

        let num_objects = 1000;
        for _ in 0..num_objects {
            let mut mobj = MantaObject::arbitrary(&mut g);
            let mut sharks = vec![];

            // first pass: 1 or 2
            // second pass: 3 or 4
            for i in 0..2 {
                let shark_num = rng.gen_range(1 + i * 2, 3 + i * 2);

                let shark = MantaObjectShark {
                    datacenter: String::from("foo"), //todo
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

        let builder = thread::Builder::new();
        let obj_generator_th = builder
            .name(String::from("no skip object_generator_test"))
            .spawn(move || {
                for (id, o) in test_objects_copy.into_iter() {
                    let shard = 1;
                    let etag = String::from("Fake_etag");
                    let mobj_value =
                        serde_json::to_value(o.clone()).expect("mobj value");

                    let eobj = EvacuateObject::from_parts(
                        mobj_value,
                        id.clone(),
                        etag,
                        shard,
                    );
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
            })
            .expect("failed to build object generator thread");

        let verification_objects = test_objects.clone();
        let builder = thread::Builder::new();
        let verif_job_action = Arc::clone(&job_action);
        let verification_thread = builder
            .name(String::from("verification thread"))
            .spawn(move || {
                debug!("Starting verification thread");
                let mut assignment_count = 0;
                let mut task_count = 0;
                loop {
                    match full_assignment_rx.recv() {
                        Ok(assignment) => {
                            assignment_count += 1;
                            task_count += assignment.tasks.len();
                            let dest_shark =
                                assignment.dest_shark.manta_storage_id;
                            for (tid, _) in assignment.tasks {
                                match verification_objects.get(&tid) {
                                    Some(obj) => {
                                        println!(
                                            "Checking that {:#?} is not in \
                                             {:#?}",
                                            dest_shark, obj.sharks
                                        );

                                        assert_eq!(
                                            obj.sharks.iter().any(|shark| {
                                                shark.manta_storage_id
                                                    == dest_shark
                                            }),
                                            false
                                        );
                                    }
                                    None => {
                                        assert!(false, "test error");
                                    }
                                }
                            }
                            verif_job_action.mark_dest_shark_ready(&dest_shark);
                            println!("Task COUNT {}", task_count);
                        }
                        Err(_) => {
                            info!(
                                "Verification Thread: Channel closed, exiting."
                            );
                            break;
                        }
                    }
                }

                use self::evacuateobjects::dsl::{evacuateobjects, status};
                let locked_conn =
                    verif_job_action.conn.lock().expect("DB conn");
                let records: Vec<EvacuateObject> = evacuateobjects
                    .filter(status.eq(EvacuateObjectStatus::Skipped))
                    .load::<EvacuateObject>(&*locked_conn)
                    .expect("getting skipped objects");
                let skip_count = records.len();

                println!("Assignment Count: {}", assignment_count);
                println!("Task Count: {}", task_count);
                println!(
                    "Skip Count: {} (this should be a fairly small \
                     percentage of Num Objects)",
                    skip_count
                );
                println!("Num Objects: {}", num_objects);
                assert_eq!(task_count + skip_count, num_objects);
            })
            .expect("verification thread");

        verification_thread.join().expect("verification join TODO");
        obj_generator_th.join().expect("obj_gen thr join TODO");

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
        let mut g = StdThreadGen::new(10);
        let job_action = EvacuateJob::new(
            String::new(),
            "fakedomain.us",
            &Uuid::new_v4().to_string(),
            ObjectSource::default(),
            ConfigOptions::default(),
            None,
        )
        .expect("initialize evacuate job");

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
            let mobj_value = serde_json::to_value(mobj).expect("mobj_value");
            let shard = 1;
            let etag = random_string(&mut g, 10);
            let id =
                common::get_objectId_from_value(&mobj_value).expect("objectId");

            let mut eobj =
                EvacuateObject::from_parts(mobj_value, id, etag, shard);

            eobj.assignment_id = uuid.clone();
            eobjs.push(eobj);
        }

        // Put the EvacuateObject's into the DB so that the process function
        // can look them up later.
        job_action
            .insert_assignment_into_db(&uuid, &eobjs)
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

        // These tests are run locally and don't go out over the network so any properly formatted
        // host/domain name is valid here.
        let job_action = EvacuateJob::new(
            String::new(),
            "fakedomain.us",
            &Uuid::new_v4().to_string(),
            ObjectSource::default(),
            ConfigOptions::default(),
            None,
        )
        .expect("initialize evacuate job");
        let job_action = Arc::new(job_action);

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

    #[test]
    fn full_test() {
        unit_test_init();
        let now = std::time::Instant::now();
        let storinfo = MockStorinfo::new();
        let storinfo = Arc::new(storinfo);

        let (full_assignment_tx, full_assignment_rx) = crossbeam::bounded(5);
        let (obj_tx, obj_rx) = crossbeam::bounded(5);
        let (md_update_tx, md_update_rx) = crossbeam::bounded(5);
        let (checker_fini_tx, checker_fini_rx) = crossbeam::bounded(1);

        // These tests are run locally and don't go out over the network so any properly formatted
        // host/domain name is valid here.
        let job_action = EvacuateJob::new(
            String::new(),
            "region.fakedomain.us",
            &Uuid::new_v4().to_string(),
            ObjectSource::default(),
            ConfigOptions::default(),
            None,
        )
        .expect("initialize evacuate job");

        // Create the database table
        assert!(job_action.create_table().is_ok());

        let job_action = Arc::new(job_action);
        let mut test_objects = vec![];
        let mut g = StdThreadGen::new(10);
        let num_objects = 100;

        for _ in 0..num_objects {
            let mobj = MantaObject::arbitrary(&mut g);
            test_objects.push(mobj);
        }

        let test_objects_copy = test_objects.clone();

        let builder = thread::Builder::new();
        let obj_generator_th = builder
            .name(String::from("object_generator_test"))
            .spawn(move || {
                for o in test_objects_copy.into_iter() {
                    let shard = 1;
                    let etag = String::from("Fake_etag");
                    let mobj_value =
                        serde_json::to_value(o.clone()).expect("mobj value");

                    let id = o.object_id;
                    let eobj = EvacuateObject::from_parts(
                        mobj_value,
                        id.clone(),
                        etag,
                        shard,
                    );
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

        let manager_thread = start_assignment_manager(
            full_assignment_tx,
            checker_fini_tx,
            obj_rx,
            Arc::clone(&job_action),
            Arc::clone(&storinfo),
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
}
