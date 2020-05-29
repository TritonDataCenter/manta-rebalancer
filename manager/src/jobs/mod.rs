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
use crate::pg_db::{connect_or_create_db, REBALANCER_DB};
use crate::storinfo::StorageNode;
use evacuate::{EvacuateJob, EvacuateJobUpdateMessage};
use rebalancer::common::{ObjectId, Task};
use rebalancer::error::{Error, InternalError, InternalErrorCode};

use std::collections::HashMap;
use std::fmt;
use std::io::Write;
use std::str::FromStr;

use diesel::deserialize::{self, FromSql};
use diesel::pg::{Pg, PgValue};
use diesel::prelude::*;
use diesel::serialize::{self, IsNull, Output, ToSql};
use diesel::sql_types;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub type StorageId = String; // Hostname
pub type AssignmentId = String; // UUID
pub type HttpStatusCode = u16;

/// The JobPayload is an enum with variants of JobActions.  A properly
/// formatted JobPayload submitted from the client in JSON form looks like:
///
/// ```json
/// {
///     "action": <job action (String)>,
///     "params": { <job action specific params > }
/// }
/// ```
#[derive(Serialize, Deserialize)]
#[serde(tag = "action", content = "params")]
#[serde(rename_all = "lowercase")]
pub enum JobPayload {
    Evacuate(EvacuateJobPayload),
}

#[derive(Serialize, Deserialize, Default)]
pub struct EvacuateJobPayload {
    pub from_shark: String,
    pub max_objects: Option<u32>,
}

#[derive(Debug)]
pub enum JobUpdateMessage {
    Evacuate(EvacuateJobUpdateMessage),
}

pub struct Job {
    id: Uuid,
    action: JobAction,
    state: JobState,
    config: Config,
    pub update_tx: Option<crossbeam_channel::Sender<JobUpdateMessage>>,
}

// JobBuilder allows us to build a job before commiting its configuration and
// calling it's `run()` method.  This also allows us to create job actions
// internally to the class(module) instead of exposing them to the rest of
// the crate.
pub struct JobBuilder {
    id: Uuid,
    action: Option<JobAction>,
    state: JobState,
    config: Config,
    update_tx: Option<crossbeam_channel::Sender<JobUpdateMessage>>,
}

impl JobBuilder {
    pub fn new(config: Config) -> Self {
        JobBuilder {
            config,
            ..Default::default()
        }
    }

    // Create the configuration for an evacuate job action and add it to this
    // job's action field.
    pub fn evacuate(
        mut self,
        from_shark: String,
        domain_name: &str,
        max_objects: Option<u32>,
    ) -> JobBuilder {
        // A better approach here would be to create a thread in each job
        // that would listen for the job update messages and then based on
        // the message action would send the appropriate messages to the
        // appropriate threads specific to that job and the update action.
        // In the future if we expand this feature, our current
        // implementation does not preclude this approach.  But that adds
        // complexity (which increases risk) where it is really not needed.
        // Instead, for now, we only create the channel if the job supports the
        // one update action implemented.  An update_tx of 'None' signifies
        // to the main.rs(server) that this job does not support dynamic
        // configuration updates, and will return an error to the user if an
        // update is attempted on this job.
        let (tx, rx) = if self.config.options.use_static_md_update_threads {
            (None, None)
        } else {
            let (tx, rx) = crossbeam_channel::unbounded();
            (Some(tx), Some(rx))
        };

        match EvacuateJob::new(
            from_shark,
            domain_name,
            &self.id.to_string(),
            self.config.options,
            rx,
            max_objects,
        ) {
            Ok(j) => {
                let action = JobAction::Evacuate(Box::new(j));
                self.action = Some(action);
                self.update_tx = tx;
            }
            Err(e) => {
                error!("Failed to initialize evacuate job: {}", e);
                self.state = JobState::Failed;
            }
        }

        self
    }

    // * commit the configuration
    // * set the job state to JobSate::Init
    // * insert the job into "rebalancer" database in the "jobs" table
    pub fn commit(self) -> Result<Job, Error> {
        if self.state != JobState::Init {
            let msg = format!(
                "Attempted to commit job in {} state.  Must be \
                 in init state.",
                self.state
            );
            return Err(InternalError::new(
                Some(InternalErrorCode::JobBuilderError),
                msg,
            )
            .into());
        }

        let action = match self.action {
            Some(a) => a,
            None => {
                return Err(InternalError::new(
                    Some(InternalErrorCode::JobBuilderError),
                    "A job action must be specified",
                )
                .into());
            }
        };

        /*
        let update_tx = match self.update_tx {
            Some(tx) => tx,
            None => {
                return Err(InternalError::new(
                    Some(InternalErrorCode::JobBuilderError),
                    "Missing job update channel",
                )
                .into());
            }
        };
         */

        let job = Job {
            id: self.id,
            action,
            state: JobState::Setup,
            config: self.config,
            update_tx: self.update_tx,
        };

        job.insert_into_db()?;

        Ok(job)
    }
}

// A rust version of the "jobs" database schema.  We call the various
// "to_db_entry()" methods for these fields to convert a Job object into one
// of these that can be inserted into the DB.  For the JobActionDbEntry we
// simply take the variant name of the JobAction.  There is no need to store
// the entire Job Action configuration in the DB, nor would it be valuable
// because the Job Action state is constantly changing as the job runs.
//
// This approach does not seem ideal, but it is along the lines of what the
// crate developers recommend.
// https://github.com/diesel-rs/diesel/issues/860
#[derive(
    AsChangeset,
    AsExpression,
    Debug,
    Deserialize,
    Identifiable,
    Insertable,
    PartialEq,
    Queryable,
    Serialize,
)]
#[table_name = "jobs"]
pub struct JobDbEntry {
    pub id: String,
    pub action: JobActionDbEntry,
    pub state: JobState,
}

table! {
    use diesel::sql_types::Text;
    jobs (id) {
        id -> Text,
        action -> Text,
        state -> Text,
    }
}

#[sql_type = "sql_types::Text"]
#[derive(
    AsExpression,
    Clone,
    Debug,
    Deserialize,
    Display,
    EnumString,
    EnumVariantNames,
    FromSqlRow,
    PartialEq,
    Serialize,
)]
#[strum(serialize_all = "snake_case")]
pub enum JobState {
    Init,
    Setup,
    Running,
    Stopped,
    Complete,
    Failed,
}

impl ToSql<sql_types::Text, Pg> for JobState {
    fn to_sql<W: Write>(&self, out: &mut Output<W, Pg>) -> serialize::Result {
        let action = self.to_string();
        out.write_all(action.as_bytes())?;
        Ok(IsNull::No)
    }
}

impl FromSql<sql_types::Text, Pg> for JobState {
    fn from_sql(bytes: Option<PgValue<'_>>) -> deserialize::Result<Self> {
        let t: PgValue = not_none!(bytes);
        let t_str = String::from_utf8_lossy(t.as_bytes());
        Self::from_str(&t_str).map_err(std::convert::Into::into)
    }
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

#[sql_type = "sql_types::Text"]
#[derive(
    AsExpression,
    Debug,
    Deserialize,
    Display,
    EnumString,
    EnumVariantNames,
    FromSqlRow,
    PartialEq,
    Serialize,
)]
#[strum(serialize_all = "snake_case")]
pub enum JobActionDbEntry {
    Evacuate,
    None,
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
                 }}",
                ej.dest_shark_list,
                ej.assignments,
                ej.from_shark,
                ej.min_avail_mb,
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
    AgentComplete,    // Agent has completed its work, and the JobAction is now
    // post processing the Assignment.
    PostProcessed, // The Assignment has completed all necessary work.
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Assignment {
    id: AssignmentId,
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

pub type AssignmentCache = HashMap<AssignmentId, AssignmentCacheEntry>;

#[derive(Clone, Debug)]
pub struct AssignmentCacheEntry {
    id: AssignmentId,
    dest_shark: StorageNode,
    state: AssignmentState,
}

impl From<Assignment> for AssignmentCacheEntry {
    fn from(assignment: Assignment) -> AssignmentCacheEntry {
        AssignmentCacheEntry {
            id: assignment.id,
            dest_shark: assignment.dest_shark,
            state: assignment.state,
        }
    }
}

pub fn assignment_cache_usage(assignments: &AssignmentCache) -> usize {
    assignments.capacity()
        * (std::mem::size_of::<Assignment>()
            + std::mem::size_of::<AssignmentId>())
}

impl Job {
    pub fn get_id(&self) -> Uuid {
        self.id
    }

    pub fn add_action(&mut self, action: JobAction) {
        self.action = action;
    }

    pub fn run(mut self) -> Result<(), Error> {
        let job_id = self.id.to_string();

        self.update_state(JobState::Running)?;
        debug!("Starting job {:#?}", &self);
        info!("Starting Job: {}", &job_id);
        let now = std::time::Instant::now();

        let result = match self.action {
            JobAction::Evacuate(job_action) => {
                match job_action.run(&self.config) {
                    Ok(()) => {
                        info!(
                            "Job {} completed in {} seconds",
                            &job_id,
                            now.elapsed().as_secs(),
                        );
                        Ok(())
                    }
                    Err(e) => match &e {
                        // This dance is only intended to support the
                        // evacuate object limit which will eventually be
                        // removed.
                        Error::Internal(err) => match err.code {
                            InternalErrorCode::MaxObjectsLimit => {
                                info!(
                                    "Job {} completed in {} seconds",
                                    &job_id,
                                    now.elapsed().as_secs(),
                                );
                                Ok(())
                            }
                            _ => {
                                error!(
                                    "Job {} failed in {} seconds: {}",
                                    &job_id,
                                    now.elapsed().as_secs(),
                                    err
                                );
                                Err(e)
                            }
                        },
                        _ => {
                            error!(
                                "Job {} failed in {} seconds: {}",
                                &job_id,
                                now.elapsed().as_secs(),
                                e
                            );
                            Err(e)
                        }
                    },
                }
            }
            _ => Ok(()),
        };

        let ret = match result {
            Ok(()) => {
                self.state = JobState::Complete;
                Ok(())
            }
            Err(e) => {
                self.state = JobState::Failed;
                Err(e)
            }
        };

        update_job_db_state(job_id, &self.state)?;
        ret
    }

    fn to_db_entry(&self) -> JobDbEntry {
        JobDbEntry {
            id: self.id.to_string(),
            action: self.action.to_db_entry(),
            state: self.state.clone(),
        }
    }

    fn insert_into_db(&self) -> Result<usize, Error> {
        use self::jobs::dsl::*;

        let db_ent = self.to_db_entry();
        let conn = match connect_or_create_db(REBALANCER_DB) {
            Ok(conn) => conn,
            Err(e) => {
                return Err(e);
            }
        };

        diesel::insert_into(jobs)
            .values(&db_ent)
            .execute(&conn)
            .map_err(Error::from)
    }

    fn update_state(&mut self, to_state: JobState) -> Result<usize, Error> {
        let result = update_job_db_state(self.id.to_string(), &to_state);
        self.state = to_state;
        result
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

impl Default for JobBuilder {
    fn default() -> Self {
        Self {
            id: Uuid::new_v4(),
            action: None,
            state: JobState::default(),
            config: Config::default(),
            update_tx: None,
        }
    }
}

fn update_job_db_state(
    job_id: String,
    to_state: &JobState,
) -> Result<usize, Error> {
    use self::jobs::dsl::*;

    let conn = match connect_or_create_db(REBALANCER_DB) {
        Ok(conn) => conn,
        Err(e) => {
            return Err(e);
        }
    };

    diesel::update(jobs)
        .filter(id.eq(job_id))
        .set(state.eq(to_state))
        .execute(&conn)
        .map_err(Error::from)
}

pub fn create_job_database() -> Result<(), Error> {
    let conn = connect_or_create_db(REBALANCER_DB)?;

    let action_strings = JobActionDbEntry::variants();
    let state_strings = JobState::variants();

    let action_check = format!("'{}'", action_strings.join("', '"));
    let state_check = format!("'{}'", state_strings.join("', '"));

    let create_query = format!(
        "
            CREATE TABLE IF NOT EXISTS jobs(
                id TEXT PRIMARY KEY,
                action TEXT CHECK(action IN ({})) NOT NULL,
                state TEXT CHECK(state IN ({})) NOT NULL
            );
        ",
        action_check, state_check,
    );

    conn.execute(&create_query).map(|_| {}).map_err(Error::from)
}

#[cfg(test)]
mod test {
    use super::*;
    use rebalancer::util;

    #[test]
    fn basic() {
        let _guard = util::init_global_logger();
        let config = Config::parse_config(&Some("src/config.json".to_string()))
            .expect("parse config");

        let builder = JobBuilder::new(config);
        assert_eq!(builder.state, JobState::Init);

        let from_shark = String::from("1.stor.domain");
        let builder = builder.evacuate(from_shark, "fakedomain.us", Some(1));
        assert_eq!(builder.state, JobState::Init);

        let job = builder.commit().expect("failed to create job");
        assert_eq!(job.state, JobState::Setup);

        // We expect an error here because every parameter above is fake
        assert!(job.run().is_err());
    }
}
