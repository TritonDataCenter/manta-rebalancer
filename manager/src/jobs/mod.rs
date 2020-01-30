/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2020 Joyent, Inc.
 */

pub mod evacuate;
pub mod status;

use crate::config::Config;
use crate::storinfo::StorageNode;
use crate::pg_db::connect_or_create_db;
use rebalancer::common::{ObjectId, Task};
use rebalancer::error::Error;

use std::collections::HashMap;
use std::fmt;

use diesel::deserialize::{self, FromSql};
use diesel::pg::{Pg, PgConnection, PgValue};
use diesel::prelude::*;
use diesel::serialize::{self, IsNull, Output, ToSql};
use diesel::sql_types;
use diesel::Expression;
use evacuate::EvacuateJob;
use serde::{Deserialize, Serialize};
use std::io::Write;
use std::str::FromStr;

use uuid::Uuid;

pub type StorageId = String; // Hostname
pub type AssignmentId = String; // UUID
pub type HttpStatusCode = u16;


pub struct Job {
    id: Uuid,
    action: JobAction,
    state: JobState,
    config: Config,
}

// https://github.com/diesel-rs/diesel/issues/860
#[derive(Insertable, Queryable, Identifiable, AsChangeset, AsExpression,
PartialEq)]
#[table_name = "jobs"]
struct JobDbEntry {
    id: String,
    action: String,
    state: String,

}
/*
    action: JobActionDbEntry,
    state: JobState,
}
*/

table! {
    use diesel::sql_types::Text;
    jobs (id) {
        id -> Text,
        action -> Text,
        state -> Text,
    }
}

#[derive(Display, EnumString, EnumVariantNames, Debug, FromSqlRow,
AsExpression,)]
#[strum(serialize_all = "snake_case")]
//#[sql_type = "sql_types::Text"]
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

impl JobAction {
    fn to_db_entry(&self) -> JobActionDbEntry {
        match self {
            JobAction::Evacuate(_) => JobActionDbEntry::Evacuate,
            _ => JobActionDbEntry::None,
        }
    }
}

#[derive(Debug, Display, EnumString)]
#[strum(serialize_all = "snake_case")]
//#[sql_type = "sql_types::Text"]
enum JobActionDbEntry {
    Evacuate,
    None
}

impl ToSql<sql_types::Text, Pg> for JobActionDbEntry {
    fn to_sql<W: Write>(&self, out: &mut Output<W, Pg>) -> serialize::Result {
        let action = self.to_string();
        out.write_all(action.as_bytes())?;
        Ok(IsNull::No)
    }
}

impl FromSql<sql_types::Text, Pg> for JobActionDbEntry {
    fn from_sql(bytes: Option<PgValue<'_>>) -> deserialize::Result<Self> {
        let t: PgValue = not_none!(bytes);
        let t_str = String::from_utf8_lossy(t.as_bytes());
        Self::from_str(&t_str).map_err(std::convert::Into::into)
    }
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


impl Job {
    pub fn new(config: Config) -> Result<Self, Error> {
        let conn = match connect_or_create_db("rebalancer") {
            Ok(conn) => conn,
            Err(e) => {
                return Err(e.into());
            }
        };

        let job = Job {
            config,
            ..Default::default()
        };

        Ok(job)


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
