/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2019, Joyent, Inc.
 */

pub mod evacuate;

use crate::config::Config;
use crate::error::Error;
use crate::picker::StorageNode;

use std::collections::HashMap;
use std::fmt;
use std::io::Write;
use std::str::FromStr;

use diesel::backend;
use diesel::deserialize::{self, FromSql};
use diesel::serialize::{self, IsNull, Output, ToSql};
use diesel::sql_types;
use diesel::sqlite::Sqlite;
use evacuate::EvacuateJob;
use libmanta::moray::MantaObjectShark;
use md5::{Digest, Md5};
use quickcheck::{Arbitrary, Gen};
use quickcheck_helpers::random::string as random_string;
use serde::{Deserialize, Serialize};
use strum::IntoEnumIterator;
use uuid::Uuid;

pub type StorageId = String; // Hostname
pub type AssignmentId = String; // UUID
pub type ObjectId = String; // UUID
pub type HttpStatusCode = u16;

pub struct Job {
    id: Uuid,
    action: JobAction,
    state: JobState,
    config: Config,
}

#[derive(Debug, Clone)]
pub enum JobState {
    Init,
    Setup,
    Running,
    Stopped,
    Complete,
    Failed,
}

pub enum JobAction {
    Evacuate(Box<EvacuateJob>),
    None,
}

impl fmt::Debug for Job {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let action_str = match &self.action {
            JobAction::Evacuate(ej) => format!(
                "EvacuateJob: {{ dest_shark_list: {:#?}, \
                 assignments: {:#?}, \
                 from_shark: {:#?}, \
                 min_avail_mb: {:#?}, \
                 max_tasks_per_assignment: {:#?}, \
                 }}",
                ej.dest_shark_list,
                ej.assignments,
                ej.from_shark,
                ej.min_avail_mb,
                ej.max_tasks_per_assignment
            ),
            _ => String::new(),
        };

        write!(
            f,
            "Job {{ id: {}, action: {}, state: {:#?}, config: {:#?} }}",
            self.id, action_str, self.state, self.config
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssignmentPayload {
    id: String,
    tasks: Vec<Task>,
}

impl From<AssignmentPayload> for (String, Vec<Task>) {
    fn from(p: AssignmentPayload) -> (String, Vec<Task>) {
        let AssignmentPayload { id, tasks } = p;
        (id, tasks)
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
enum AssignmentState {
    Init,             // Assignment is in the process of being created.
    Assigned,         // Assignment has been submitted to the Agent.
    Rejected,         // Agent has rejected the Assignment.
    AgentUnavailable, // Could not connect to agent.
    AgentComplete,    // Agent as completed its work, and the JobAction is now
    // post processing the Assignment.
    PostProcessed, // The Assignment has completed all necessary work.
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Assignment {
    id: String,
    dest_shark: StorageNode,
    tasks: HashMap<ObjectId, Task>,
    max_size: u64,
    total_size: u64,
    state: AssignmentState,
}

impl Assignment {
    fn new(dest_shark: StorageNode) -> Self {
        let max_size = dest_shark.available_mb / 2;

        Self {
            id: Uuid::new_v4().to_string(),
            dest_shark,
            max_size,
            total_size: 0,
            tasks: HashMap::new(),
            state: AssignmentState::Init,
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Task {
    pub object_id: String, // or Uuid
    pub owner: String,     // or Uuid
    pub md5sum: String,
    pub source: MantaObjectShark,

    #[serde(default = "TaskStatus::default")]
    pub status: TaskStatus,
}

impl Task {
    pub fn set_status(&mut self, status: TaskStatus) {
        self.status = status;
    }
}

impl Arbitrary for Task {
    fn arbitrary<G: Gen>(g: &mut G) -> Task {
        let len: usize = (g.next_u32() % 20) as usize;
        let mut hasher = Md5::new();
        hasher.input(random_string(g, len).as_bytes());
        let md5checksum = hasher.result();
        let md5sum = base64::encode(&md5checksum);

        Task {
            object_id: Uuid::new_v4().to_string(),
            owner: Uuid::new_v4().to_string(),
            md5sum,
            source: MantaObjectShark::arbitrary(g),
            status: TaskStatus::arbitrary(g),
        }
    }
}

// Note: if you change or add any of the fields here be sure to update the
// Arbitrary implementation.
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
pub enum TaskStatus {
    Pending,
    Running,
    Complete,
    Failed(ObjectSkippedReason),
}

impl Default for TaskStatus {
    fn default() -> Self {
        TaskStatus::Pending
    }
}

impl Arbitrary for TaskStatus {
    fn arbitrary<G: Gen>(g: &mut G) -> TaskStatus {
        let i = g.next_u32() % 4;
        match i {
            0 => TaskStatus::Pending,
            1 => TaskStatus::Running,
            2 => TaskStatus::Complete,
            3 => TaskStatus::Failed(Arbitrary::arbitrary(g)),
            _ => panic!(),
        }
    }
}

//
// There are many reasons why an object may fail on a rebalance job.  This
// enum of reasons is meant to be shared between both the agent and the
// zone, and also apply to all future job actions.  We could build out
// reasons for each job and then build translators for each agent
// error/reason to each job action reason, but it would be nice to avoid
// having to do that.  The trade off is that some of these reasons may be unique
// to a specific job action. It's a trade off and we may decide in the future to
// split these off but for now it seems to make the most sense going forward to
// put them all in the same spot.
//
#[derive(
    AsExpression,
    Clone,
    Copy,
    Debug,
    Deserialize,
    Display,
    EnumString,
    EnumVariantNames,
    EnumIter,
    Eq,
    FromSqlRow,
    Hash,
    PartialEq,
    Serialize,
)]
#[strum(serialize_all = "snake_case")]
#[sql_type = "sql_types::Text"]
pub enum ObjectSkippedReason {
    // Agent encountered a local filesystem error
    AgentFSError,

    // The specified agent does not have that assignment
    AgentAssignmentNoEnt,

    // Internal Assignment Error
    AssignmentError,

    // A mismatch of assignment data between the agent and the zone
    AssignmentMismatch,

    // The assignment was rejected by the agent.
    AssignmentRejected,

    // Not enough space on destination SN
    DestinationInsufficientSpace,

    // Destination agent was not reachable
    DestinationUnreachable,

    // MD5 Mismatch between the file on disk and the metadata.
    MD5Mismatch,

    // Catchall for unspecified network errors.
    NetworkError,

    // The object is already on the proposed destination shark, using it as a
    // destination for rebalance would reduce the durability by 1.
    ObjectAlreadyOnDestShark,

    // The object is already in the proposed destination datacenter, using it
    // as a destination for rebalance would reduce the failure domain.
    ObjectAlreadyInDatacenter,

    // Encountered some other http error (not 400 or 500) while attempting to
    // contact the source of the object.
    SourceOtherError,

    // The only source available is the shark that is being evacuated.
    SourceIsEvacShark,

    HTTPStatusCode(HttpStatusCode),
}

impl ObjectSkippedReason {
    // The "Strum" crate already provides a "to_string()" method which we
    // want to use here.  This is for handling the special case of variants
    // with values/fields.
    pub fn into_string(self) -> String {
        match self {
            ObjectSkippedReason::HTTPStatusCode(sc) => {
                format!("{{{}:{}}}", self, sc)
            }
            _ => self.to_string(),
        }
    }
}

impl Arbitrary for ObjectSkippedReason {
    fn arbitrary<G: Gen>(g: &mut G) -> ObjectSkippedReason {
        let i: usize = g.next_u32() as usize % Self::iter().count();
        // XXX
        // This is down right absurd.  Need to figure out why I cant use
        // gen_range() even though I have the rand::Rng trait in scope.
        let status_code: u16 = (g.next_u32() as u16 % 500) + 100;
        let reason = Self::iter().nth(i).unwrap();
        match reason {
            ObjectSkippedReason::HTTPStatusCode(_) => {
                ObjectSkippedReason::HTTPStatusCode(status_code)
            }
            _ => reason,
        }
    }
}

impl ToSql<sql_types::Text, Sqlite> for ObjectSkippedReason {
    fn to_sql<W: Write>(
        &self,
        out: &mut Output<W, Sqlite>,
    ) -> serialize::Result {
        let sr = self.into_string();
        out.write_all(sr.as_bytes())?;

        Ok(IsNull::No)
    }
}

impl FromSql<sql_types::Text, Sqlite> for ObjectSkippedReason {
    fn from_sql(
        bytes: Option<backend::RawValue<Sqlite>>,
    ) -> deserialize::Result<Self> {
        let t = not_none!(bytes).read_text();
        let ts: String = t.to_string();
        if ts.starts_with('{') && ts.ends_with('}') {
            // "{skipped_reason:status_code}"
            let matches: &[_] = &['{', '}'];

            // trim_matches: "skipped_reason:status_code"
            // split().collect(): ["skipped_reason", "status_code"]
            let sr_sc: Vec<&str> =
                ts.trim_matches(matches).split(':').collect();
            assert_eq!(sr_sc.len(), 2);

            // ["skipped_reason", "status_code"]
            let reason = ObjectSkippedReason::from_str(&sr_sc[0])?;
            match reason {
                ObjectSkippedReason::HTTPStatusCode(_) => {
                    Ok(ObjectSkippedReason::HTTPStatusCode(sr_sc[1].parse()?))
                }
                _ => {
                    panic!("variant with value not found");
                }
            }
        } else {
            Self::from_str(t).map_err(std::convert::Into::into)
        }
    }
}

impl Job {
    pub fn new(config: Config) -> Self {
        Job {
            config,
            ..Default::default()
        }
    }

    pub fn set_id(&mut self, id: Uuid) {
        self.id = id;
    }

    pub fn get_id(&self) -> Uuid {
        self.id
    }

    pub fn add_action(&mut self, action: JobAction) {
        self.action = action;
    }

    // The goal here is to eventually have a run method for all JobActions.
    pub fn run(self) -> Result<(), Error> {
        debug!("Starting job {:#?}", &self);
        println!("Starting Job: {}", &self.id);
        match self.action {
            JobAction::Evacuate(job_action) => job_action.run(&self.config),
            _ => Ok(()),
        }
    }
}

impl Default for JobAction {
    fn default() -> Self {
        JobAction::None
    }
}

impl Default for JobState {
    fn default() -> Self {
        JobState::Init
    }
}

impl Default for Job {
    fn default() -> Self {
        Self {
            action: JobAction::default(),
            state: JobState::default(),
            id: Uuid::new_v4(),
            config: Config::default(),
        }
    }
}
