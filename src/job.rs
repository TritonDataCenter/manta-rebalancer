// Copyright 2019 Joyent, Inc.


use crate::config::Config;
use crate::error::{CrossbeamError, Error, InternalError, InternalErrorCode};
use crate::picker as mod_picker;
use crate::picker::{Picker, StorageNode};
use crossbeam_channel as crossbeam;
use libmanta::moray::{MantaObject, MantaObjectShark};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::error::Error as _Error;
use std::io::ErrorKind;
use std::sync::{Arc, RwLock};
use std::thread;
use uuid::Uuid;

pub type StorageId = String;

#[derive(Debug)]
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

#[derive(Debug)]
pub enum JobAction {
    Evacuate(EvacuateJob),
    None,
}

#[derive(Debug, Clone, PartialEq)]
enum EvacuateObjectStatus {
    Unprocessed,    // Default state
    Processing,     // Object has been included in an assignment and that
                    // assignment has been submitted to a remora agent.
    Skipped,        // Could not find a shark to put this object in. TODO: Why?
     // TODO: Failed,   // Failed to Evacuate Object ???
     // TODO: Retrying, // Retrying a failed evacuate attempt
}

#[derive(Debug, Clone)]
struct EvacuateObject {
    id: String,
    object: MantaObject,
    status: EvacuateObjectStatus,
}

impl EvacuateObject {
    fn new(object: MantaObject) -> Self {
        Self {
            id: object.object_id.to_owned(),
            object,
            status: EvacuateObjectStatus::Unprocessed,
        }
    }
}

impl Default for EvacuateObject {
    fn default() -> Self {
        let object = MantaObject::default();
        Self {
            id: String::from(""),
            object,
            status: EvacuateObjectStatus::Unprocessed,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
enum DestSharkStatus {
    Init,
    Assigned,
    Ready,
}

#[derive(Clone, Debug)]
struct EvacuateDestShark {
    shark: StorageNode,
    status: DestSharkStatus,
}

#[derive(Debug)]
pub struct EvacuateJob {
    dest_shark_list: RwLock<HashMap<StorageId, EvacuateDestShark>>,
    objects: Vec<EvacuateObject>, // TODO: remove in favor of diesel w/ sqlite
    assignments: RwLock<Vec<Assignment>>,
    from_shark: MantaObjectShark,
    min_avail_mb: Option<u64>,
    max_tasks_per_assignment: Option<u32>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
enum AssignmentState {
    Init,
    Assigned,
    Rejected,
    Complete,
    PostProcessed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Assignment {
    id: String,
    dest_shark: StorageId,
    tasks: Vec<Task>,
    max_size: u64,
    total_size: u64,
    state: AssignmentState,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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

#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum TaskStatus {
    Pending,
    Running,
    Complete,
    Failed(String),
}

impl TaskStatus {
    pub fn default() -> Self {
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

    pub fn run(self) -> Result<(), Error> {
        match &self.action {
            JobAction::Evacuate(_) => run_evacuate_job(self),
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

impl EvacuateJob {
    pub fn new(from_shark: String) -> Self {
        Self {
            min_avail_mb: Some(1000),
            max_tasks_per_assignment: Some(100),
            dest_shark_list: RwLock::new(HashMap::new()),
            objects: vec![],
            assignments: RwLock::new(vec![]),
            from_shark: MantaObjectShark {
                manta_storage_id: from_shark,
                ..Default::default()
            },
        }
    }

    /// If a shark is in the Assigned state then it is busy.
    fn shark_busy(&self, shark: &StorageNode) -> bool {
        if let Some(eds) = self
            .dest_shark_list
            .read()
            .unwrap()
            .get(shark.manta_storage_id.as_str())
        {
            debug!(
                "shark '{}' status: {:?}",
                shark.manta_storage_id, eds.status
            );
            return eds.status == DestSharkStatus::Assigned;
        }

        false
    }

    fn post_assignment(&self, mut assignment: Assignment) -> Result<(), Error> {
        // TODO agent: Send assignment to agent
        // TODO sqlite: put assignment into persistent store?

        assignment.state = AssignmentState::Assigned;
        let mut assignments = self.assignments.write().unwrap();
        assignments.push(assignment);
        Ok(())
    }

    /// checks all assigned assignments for their current status from the
    /// agent returning number of assignments that changed state
    fn check_assignments(&self) -> Result<u32, Error> {
        debug!("checking assignments");
        let mut updated_shark_count = 0;
        let mut assignments = self.assignments.write().unwrap();
        for assignment in assignments.iter_mut() {
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
}

// TODO: bring this into impl EvacuateJob
fn run_evacuate_job(job: Job) -> Result<(), Error> {
    debug!("Running evacuate job: {:?}", &job);
    let domain = &job.config.domain_name;

    let job_action = match job.action {
        JobAction::Evacuate(action) => Arc::new(action),
        _ => {
            return Err(InternalError::new(
                Some(InternalErrorCode::InvalidJobAction),
                "Evacuate received invalid Job action",
            )
            .into());
        }
    };

    println!("Starting Evacuate Job: {}", &job.id);

    let min_shard = job.config.min_shard_num();
    let max_shard = job.config.max_shard_num();

    // Steps:
    // TODO lock evacuating server to readonly

    let (obj_tx, obj_rx) = crossbeam::bounded(5);
    let (empty_assignment_tx, empty_assignment_rx) = crossbeam::bounded(5);
    let (full_assignment_tx, full_assignment_rx) = crossbeam::bounded(5);

    let sharkspotter_handle = start_sharkspotter(
        obj_tx,
        domain.as_str(),
        Arc::clone(&job_action),
        min_shard,
        max_shard,
    )?;

    let assignment_generator = start_assignment_generator(
        obj_rx,
        empty_assignment_rx,
        full_assignment_tx,
        Arc::clone(&job_action),
    )?;

    let mut picker = mod_picker::Picker::new();
    picker.start().map_err(Error::from)?;

    let picker = Arc::new(picker);

    let assignment_manager = start_assignment_manager(
        empty_assignment_tx,
        full_assignment_rx,
        Arc::clone(&job_action),
        Arc::clone(&picker),
    )?;

    assignment_manager
        .join()
        .unwrap()
        .expect("Error joining assignment generator thread");

    picker.fini();

    sharkspotter_handle
        .join()
        .unwrap()
        .unwrap_or_else(|e| {
            error!("Error joining sharkspotter handle: {}\n", e);
            std::process::exit(1);
    });

    assignment_generator
        .join()
        .unwrap()
        .expect("Error joining assignment generator thread");

    Ok(())
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

    thread::Builder::new()
        .name(String::from("sharkspotter"))
        .spawn(move || {
        let mut count = 0;
        sharkspotter::run(&config, move |obj, _shard| {
            // while testing limit the number of objects processed for now
            count += 1;
            if count > 2000 {
                return Err(std::io::Error::new(
                    ErrorKind::Other,
                    "Just stop already",
                ));
            }

            // TODO:
            // - add shard number
            sender.send(obj).map_err(CrossbeamError::from).map_err(|e| {
                std::io::Error::new(ErrorKind::Other, e.description())
            })
        })
        .map_err(Error::from)
    }).map_err(Error::from)
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
fn start_assignment_manager(
    empty_assignment_tx: crossbeam::Sender<Assignment>,
    full_assignment_rx: crossbeam::Receiver<Assignment>,
    job_action: Arc<EvacuateJob>,
    picker: Arc<Picker>,
) -> Result<thread::JoinHandle<Result<(), Error>>, Error> {
    let builder = thread::Builder::new();

    builder
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
                        None => valid_sharks,
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
                    for (_key, value) in
                        job_action.dest_shark_list.read().unwrap().iter()
                    {
                        shark_list.push(value.shark.to_owned());
                    }

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
                    id: String::from(""),
                    dest_shark: cur_shark.manta_storage_id.clone(),
                    max_size: cur_shark.available_mb / 2,
                    total_size: 0,
                    tasks: vec![],
                    state: AssignmentState::Init,
                };

                match empty_assignment_tx.send(assignment) {
                    Ok(()) => {
                        // TODO: this should be it's own thread, and the
                        // generator should send the assignment to that thread.
                        // See step 3 in the block comment at the top of this
                        // function.
                        while let Ok(new_assignment) =
                            full_assignment_rx.try_recv()
                        {
                            // TODO: update shark with space avail?  Might not be needed
                            // since we take a picker snapshot before we start adding to
                            // a shark

                            info!(
                                "Posting Assignment: {}\n",
                                serde_json::to_string(&new_assignment).unwrap()
                            );
                            job_action.post_assignment(new_assignment)?;
                        }
                    }
                    Err(e) => {
                        // TODO: this thread should probably spawn
                        // assignment_generator thread(s) so that it can maintain
                        // the generator threads.
                        error!("{}", CrossbeamError::from(e));
                        break;
                    }
                }
            }
            Ok(())
        })
        .map_err(Error::from)
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
    let mut mobj: EvacuateObject = EvacuateObject::default();
    let max_tasks = job_action.max_tasks_per_assignment;
    let from_shark_host = job_action.from_shark.manta_storage_id.clone();

    let builder = thread::Builder::new();
    builder
        .name(String::from("assignment_generator"))
        .spawn(move || {
            'assignment: loop {
                let mut assignment = match empty_assignment_rx.recv() {
                    Ok(assignment) => assignment,
                    Err(_) => break 'assignment,
                };

                let mut available_space = assignment.max_size;
                let mut rx_obj_count = 0;

                debug!("Max tasks: {:?}", &max_tasks);
                'task: while {
                    let mut cont = true;

                    // If we have exceeded the per shark number of tasks then
                    // move on.  If max tasks is not specified then we continue
                    // to fill'er up.
                    if let Some(max_tasks) = max_tasks {
                        if assignment.tasks.len() > (max_tasks - 1) as usize {
                            cont = false;
                        } else {
                            cont = true;
                        }
                    }
                    cont
                } {
                    // Only get a new object if this current one wasn't skipped
                    // from the last iteration.
                    if mobj.status != EvacuateObjectStatus::Skipped {
                        mobj = match obj_rx.recv() {
                            Ok(obj) => {
                                rx_obj_count += 1;
                                trace!("Received object {}", &rx_obj_count);
                                EvacuateObject::new(obj)
                            }
                            Err(e) => {
                                error!("Error receiving object {}\n", e);

                                // There are no new objects left, post the
                                // current assignment and break out of the
                                // assignment loop.
                                if !assignment.tasks.is_empty() {
                                    // TODO: map error
                                    match full_assignment_tx
                                        .send(assignment)
                                        .map_err(CrossbeamError::from)
                                    {
                                        Ok(()) => (),
                                        Err(err) => {
                                            return Err(InternalError::new(
                                            Some(InternalErrorCode::Crossbeam),
                                            err.description()).into());
                                        }
                                    }
                                }
                                break 'assignment;
                            }
                        };
                    }

                    let content_mb = mobj.object.content_length / (1024 * 1024);
                    if content_mb > available_space {
                        mobj.status = EvacuateObjectStatus::Skipped;
                        info!(
                            "Skipping object, need: {}, available: {} | {:?}\n",
                            content_mb, available_space, mobj
                        );

                        // TODO sqlite: Push object onto persistent store
                        break 'task;
                    }

                    let obj = &mobj.object;

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
                        // mobj.status = EvacuateObjectStatus::Skipped;
                        // job_action.objects.push(mobj);
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
                            // TODO sqlite: put skipped object in persistent store.
                            // mobj.status = EvacuateObjectStatus::Skipped;
                            // job_action.objects.push(mobj);
                            continue;
                        }
                    };

                    // Add the task to the assignment.
                    // This creates a task copying the relevant fields of the
                    // object.
                    assignment.tasks.push(Task {
                        object_id: obj.object_id.to_owned(),
                        owner: obj.owner.to_owned(),
                        md5sum: obj.content_md5.to_owned(),
                        source: source.to_owned(),
                        status: TaskStatus::Pending,
                    });

                    available_space -= content_mb;

                    trace!(
                        "Available space: {} | Tasks: {}",
                        &available_space,
                        &assignment.tasks.len()
                    );
                    mobj.status = EvacuateObjectStatus::Processing;

                    // TODO sqlite: create_or_update object to persistent store
                }

                info!(
                    "sending assignment back to manager thread: {:?}",
                    &assignment
                );

                full_assignment_tx
                    .send(assignment)
                    .map_err(|e| {
                        error!(
                            "Error sending assignment back to manager: {}", e
                        );

                        InternalError::new(
                            Some(InternalErrorCode::Crossbeam),
                            CrossbeamError::from(e).description(),
                        )
                    })?;
            }

            Ok(())
        })
        .map_err(Error::from)
}
