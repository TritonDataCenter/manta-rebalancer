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

use evacuate::EvacuateJob;
use libmanta::moray::MantaObjectShark;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub type StorageId = String; // Hostname
pub type AssignmentId = String; // UUID
pub type ObjectId = String; // UUID

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
    Init,          // Assignment is in the process of being created.
    Assigned,      // Assignment has been submitted to the Agent.
    Rejected,      // Agent has rejected the Assignment.
    AgentComplete, // Agent as completed its work, and the JobAction is now
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

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
pub enum TaskStatus {
    Pending,
    Running,
    Complete,
    Failed(String),
}

impl Default for TaskStatus {
    fn default() -> Self {
        TaskStatus::Pending
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

impl Job {
    pub fn new(action: JobAction, config: Config) -> Self {
        Job {
            action,
            config,
            ..Default::default()
        }
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
