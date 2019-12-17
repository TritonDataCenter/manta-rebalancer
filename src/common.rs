/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2019, Joyent, Inc.
 */

#[cfg(feature = "postgres")]
use std::io::Write;

#[cfg(feature = "postgres")]
use std::str::FromStr;

use libmanta::moray::MantaObjectShark;
use md5::{Digest, Md5};
use quickcheck::{Arbitrary, Gen};
use quickcheck_helpers::random::string as random_string;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[cfg(feature = "postgres")]
use diesel::deserialize::{self, FromSql};

#[cfg(feature = "postgres")]
use diesel::pg::{Pg, PgValue};

#[cfg(feature = "postgres")]
use diesel::serialize::{self, IsNull, Output, ToSql};

use diesel::sql_types;

use strum::IntoEnumIterator;

pub type HttpStatusCode = u16;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssignmentPayload {
    pub id: String,
    pub tasks: Vec<Task>,
}

impl From<AssignmentPayload> for (String, Vec<Task>) {
    fn from(p: AssignmentPayload) -> (String, Vec<Task>) {
        let AssignmentPayload { id, tasks } = p;
        (id, tasks)
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

    // The agent is busy and cant accept assignments at this time.
    AgentBusy,

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

#[cfg(feature = "postgres")]
fn _osr_from_sql(ts: String) -> deserialize::Result<ObjectSkippedReason> {
    if ts.starts_with('{') && ts.ends_with('}') {
        // Start with:
        //      "{skipped_reason:status_code}"
        let matches: &[_] = &['{', '}'];

        // trim_matches:
        //      "skipped_reason:status_code"
        //
        // split().collect():
        //      ["skipped_reason", "status_code"]
        let sr_sc: Vec<&str> = ts.trim_matches(matches).split(':').collect();
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
        ObjectSkippedReason::from_str(&ts).map_err(std::convert::Into::into)
    }
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

#[cfg(feature = "postgres")]
impl ToSql<sql_types::Text, Pg> for ObjectSkippedReason {
    fn to_sql<W: Write>(&self, out: &mut Output<W, Pg>) -> serialize::Result {
        let sr = self.into_string();
        out.write_all(sr.as_bytes())?;

        Ok(IsNull::No)
    }
}

#[cfg(feature = "postgres")]
impl FromSql<sql_types::Text, Pg> for ObjectSkippedReason {
    fn from_sql(bytes: Option<PgValue<'_>>) -> deserialize::Result<Self> {
        let t: PgValue = not_none!(bytes);
        let t_str = String::from_utf8_lossy(t.as_bytes());
        let ts: String = t_str.to_string();
        _osr_from_sql(ts)
    }
}
