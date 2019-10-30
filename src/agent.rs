/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2019, Joyent, Inc.
 */
use std::collections::{HashMap, HashSet};
use std::ffi::OsString;
use std::fs;
use std::fs::File;
use std::path::Path;
use std::sync::{mpsc, Arc, Mutex, RwLock};

use futures::future;
use futures::future::*;
use futures::stream::*;

use gotham::handler::{Handler, HandlerFuture, IntoHandlerError, NewHandler};
use gotham::helpers::http::response::{create_empty_response, create_response};
use gotham::router::{builder::*, Router};
use gotham::state::{FromState, State};
use gotham_derive::{StateData, StaticResponseExtender};

use base64;
use hyper::{Body, Chunk, Method};
use libmanta::moray::MantaObjectShark;
use md5::{Digest, Md5};

use crate::jobs::{AssignmentPayload, ObjectSkippedReason, Task, TaskStatus};

use reqwest::StatusCode;
use rusqlite;
use serde_derive::{Deserialize, Serialize};
use threadpool::ThreadPool;
use uuid::Uuid;
use walkdir::WalkDir;

type Assignments = HashMap<String, Arc<RwLock<Assignment>>>;

static REBALANCER_SCHEDULED_DIR: &str = "/var/tmp/rebalancer/scheduled";
static REBALANCER_FINISHED_DIR: &str = "/var/tmp/rebalancer/completed";

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum AgentAssignmentState {
    Scheduled,                   // Haven't even started it yet
    Running,                     // Currently processing it
    Complete(Option<Vec<Task>>), // Done.  Include any failed tasks
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AgentAssignmentStats {
    pub state: AgentAssignmentState,
    pub failed: usize,
    pub complete: usize,
    pub total: usize,
}

impl AgentAssignmentStats {
    pub fn new(total: usize) -> AgentAssignmentStats {
        AgentAssignmentStats {
            state: AgentAssignmentState::Scheduled,
            failed: 0,
            complete: 0,
            total,
        }
    }
}

#[derive(Clone, Debug, Deserialize, StateData, StaticResponseExtender)]
struct PathExtractor {
    #[serde(rename = "*")]
    parts: Vec<String>,
}

#[derive(Clone, Debug)]
pub struct Agent {
    assignments: Arc<Mutex<Assignments>>,
    quiescing: Arc<Mutex<HashSet<String>>>,
    tx: Arc<Mutex<mpsc::Sender<String>>>,
}

impl Agent {
    pub fn new(tx: Arc<Mutex<mpsc::Sender<String>>>) -> Agent {
        let assignments = Arc::new(Mutex::new(Assignments::new()));
        let quiescing = Arc::new(Mutex::new(HashSet::new()));
        Agent {
            assignments,
            quiescing,
            tx,
        }
    }

    pub fn run(addr: &'static str) {
        info!("Listening for requests at {}", addr);
        gotham::start(addr, router());
    }

    // Given an assignment uuid, check for its presence in both the "scheduled"
    // and "completed" directory.  If found in either, or the assignment is in
    // the process of being saved to disk, return true, otherwise false.
    fn assignment_exists(&self, uuid: &str) -> bool {
        let scheduled = format!("{}/{}", REBALANCER_SCHEDULED_DIR, &uuid);
        let finished = format!("{}/{}", REBALANCER_FINISHED_DIR, &uuid);

        if Path::new(&scheduled).exists() || Path::new(&finished).exists() {
            info!("Assignment {} has already been received.", uuid);
            return true;
        }

        let q = &mut self.quiescing.lock().unwrap();
        match q.get(uuid) {
            // An assignment by this uuid is currently in the process of being
            // saved to disk.  Return.
            Some(_) => {
                info!("Assignment {} is already quiescing.", uuid);
                return true;
            }
            _ => {
                // No known assignment by this uuid exists anywhere.  Add it to
                // the list of assignments that have been received but have yet
                // to be written out to disk.
                q.insert(uuid.to_owned());
                return false;
            }
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Assignment {
    pub uuid: String,
    pub stats: AgentAssignmentStats,

    #[serde(skip_serializing, skip_deserializing, default)]
    pub tasks: Vec<Task>,
}

impl Assignment {
    fn new(v: Vec<Task>, uuid: &str) -> Assignment {
        Assignment {
            uuid: uuid.to_string(),
            stats: AgentAssignmentStats::new(v.len()),
            tasks: v,
        }
    }
}

// Inform the work threads that of an assignment that needs to be processed.
// Whenever a worker is available, they will receive the UUID of the assignment
// from the receiving end of the channel and will then attempt to load it from
// disk in to memory for processing.
fn assignment_signal(agent: &Agent, uuid: &str) {
    let tx = agent.tx.lock().unwrap();
    tx.send(uuid.to_string()).unwrap();
}

// Given a uuid of an assignment (presumably on disk) and access to the
// HashMap, locate the assignment, load it in to memory and store it in the
// HashMap.  Currently, this should really only be called by a worker thread
// with the intention of immediately processing whatever it gets.
fn load_saved_assignment(
    assignments: &Arc<Mutex<Assignments>>,
    uuid: &str,
) -> Result<(), String> {
    match assignment_recall(format!("{}/{}", REBALANCER_SCHEDULED_DIR, &uuid)) {
        Ok(a) => {
            let mut work = assignments.lock().unwrap();
            work.insert(uuid.to_string(), a);
            Ok(())
        }
        Err(e) => {
            // We need to take some kind of remedial action here.  We have a
            // database file that (for one reason or another) we are unable to
            // load.  Rather than bring down the house by calling panic, it
            // is better to log the error and move on.  It may also be desirable
            // to quarantine problematic database files so that they can be
            // examined later, but not rediscovered by the agent.
            Err(format!("Error loading database: {}", e))
        }
    }
}

// Locate all saved assignments on disk and signal their presence to our pool
// of workers.  To be clear, this does not explicitly load assignments in to
// memory -- it merely notifies the thread pool of their existence.  Ultimately,
// worker(s) will load assignemnts in to memory right before processing them.
// This ensures that our memory footprint remains relateively low even if we
// experience a major backlog of assignments.
fn discover_saved_assignments(agent: &Agent) {
    for entry in WalkDir::new(REBALANCER_SCHEDULED_DIR)
        .min_depth(1)
        .follow_links(false)
        .into_iter()
        .filter_map(|e| e.ok())
    {
        let uuid = entry.file_name().to_string_lossy();
        debug!("Discovered unfinished assignment: {}", uuid);
        assignment_signal(&agent, &uuid);
    }
}

fn assignment_save(
    uuid: &str,
    path: &str,
    assignment: Arc<RwLock<Assignment>>,
) {
    let mut conn =
        match rusqlite::Connection::open(format!("{}/{}", path, uuid)) {
            Ok(conn) => conn,
            Err(e) => panic!("DB error opening {}/{}: {}", path, uuid, e),
        };

    let assn = assignment.read().unwrap();
    let tasklist = &assn.tasks;
    let stats = &assn.stats;

    // Create a transaction.  All database operations within this function
    // will be part of this transaction.  This includes the creation of both
    // the `tasks' and the `stats' table and the insertion of data in to each.
    let transaction = conn.transaction().unwrap();

    // Create the table for our tasks.
    match transaction.execute(
        "create table if not exists tasks (
        object_id text primary key not null unique,
        owner text not null,
        md5sum text not null,
        datacenter text not null,
        manta_storage_id text not null,
        status text not null
	)",
        rusqlite::params![],
    ) {
        Ok(_) => (),
        Err(e) => panic!("Database creation error: {}", e),
    }

    // Create the table for our stats.
    match transaction.execute(
        "create table if not exists stats (stats text not null)",
        rusqlite::params![],
    ) {
        Ok(_) => (),
        Err(e) => panic!("Database creation error: {}", e),
    }

    // Populate the task table with the tasks in this assignment.
    for task in tasklist.iter() {
        match transaction.execute(
            "INSERT INTO tasks
            (object_id, owner, md5sum, datacenter, manta_storage_id, status)
            values (?1, ?2, ?3, ?4, ?5, ?6)",
            rusqlite::params![
                task.object_id,
                task.owner,
                task.md5sum,
                task.source.datacenter,
                task.source.manta_storage_id,
                serde_json::to_vec(&task.status).unwrap()
            ],
        ) {
            Ok(_) => (),
            Err(e) => panic!("Task insertion error: {}", e),
        };
    }

    // Populate the stats table with our stats
    match transaction.execute(
        "INSERT INTO stats values (?1)",
        rusqlite::params![serde_json::to_vec(&stats).unwrap()],
    ) {
        Ok(_) => (),
        Err(e) => panic!("Task insertion error: {}", e),
    };

    // Finally, kick off the transaction as a whole.  Up until this point,
    // nothing has been committed to the database.  If this does not complete
    // successfully, we likely have a systemic problem that retrying or
    // "handling" will not mitigate.
    if let Err(e) = transaction.commit() {
        panic!("Transaction error on uuid: {}: {}", uuid, e);
    }
}

// Given the path of a particular assignment, extract its contents from
// persistent storage.  All assignements on disk are stored in separate
// files named after their uuid.  The format is an sqlite database.  We
// construct a vector of tasks based on the contents of the only table
// in the file called `tasks'.
fn assignment_recall<S: Into<String>>(
    path: S,
) -> Result<Arc<RwLock<Assignment>>, String> {
    let path = path.into();
    let file_path = Path::new(&path);

    if !file_path.exists() {
        return Err(format!("File does not exist: {}", path));
    }

    // The uuid is obtained from the file name.  All files stored to disk will
    // always be named after the assignment uuid.  This will never change,
    // however if it does, then the uuid of the assignment must be stored
    // somewhere within the database.
    let uuid = OsString::from(file_path.file_stem().unwrap())
        .into_string()
        .unwrap();

    let conn = match rusqlite::Connection::open(path) {
        Ok(conn) => conn,
        Err(e) => return Err(format!("DB error {}", e)),
    };

    let mut stmt = match conn.prepare(
        "SELECT object_id, owner, md5sum,
	   datacenter, manta_storage_id, status FROM tasks",
    ) {
        Ok(s) => s,
        Err(e) => return Err(format!("Query creation error: {}", e)),
    };

    // Load the table in to memory.  We will iterate through our results
    // and populate a vector of tasks based on the contents returned by
    // the query.
    let task_iter = match stmt.query_map(rusqlite::params![], |row| {
        let source = MantaObjectShark {
            datacenter: row.get(3)?,
            manta_storage_id: row.get(4)?,
        };

        let data: Vec<u8> = row.get(5)?;
        let s = String::from_utf8(data).unwrap();
        let status: TaskStatus = serde_json::from_str(&s).unwrap();

        let t = Task {
            object_id: row.get(0)?,
            owner: row.get(1)?,
            md5sum: row.get(2)?,
            source,
            status,
        };
        Ok(t)
    }) {
        Ok(iter) => iter,
        Err(e) => return Err(format!("Query execution error: {}", e)),
    };

    let mut tasks = Vec::new();

    // Populate our vector with the rows obtained from the database.  Each
    // row represents a task.
    for i in task_iter {
        tasks.push(i.unwrap());
    }

    stmt = match conn.prepare("SELECT stats FROM stats") {
        Ok(s) => s,
        Err(e) => return Err(format!("Query creation error: {}", e)),
    };

    let stats_iter = match stmt.query_map(rusqlite::params![], |row| {
        let data: Vec<u8> = row.get(0)?;
        let s = String::from_utf8(data).unwrap();
        let stats: AgentAssignmentStats = serde_json::from_str(&s).unwrap();
        Ok(stats)
    }) {
        Ok(iter) => iter,
        Err(e) => return Err(format!("Query execution error: {}", e)),
    };

    let mut stats = Vec::new();

    for i in stats_iter {
        stats.push(i.unwrap());
    }

    let mut assignment = Assignment::new(tasks, &uuid);
    assignment.stats = stats[0].clone();

    Ok(Arc::new(RwLock::new(assignment)))
}

// Take our current assignment that we have just finished processing and flush
// out the contents (with updated status for each task) out to a new database
// file in /var/tmp/rebalancer.  Next, delete the original file from
// /manta/rebalancer so that we do not process it again on restart of the agent.
// Finally, remove the assignment from our HashMap.
fn assignment_complete(assignments: Arc<Mutex<Assignments>>, uuid: String) {
    let assn = assignment_get(&assignments, &uuid).unwrap();

    assignment_save(&uuid, REBALANCER_FINISHED_DIR, assn);
    let src = format!("{}/{}", REBALANCER_SCHEDULED_DIR, uuid);

    match fs::remove_file(&src) {
        Ok(_) => (),
        Err(e) => panic!(format!("Error removing file: {}", e)),
    };

    // Remove it from our HashMap of assignments.
    let mut hm = assignments.lock().unwrap();
    hm.remove(&uuid);
}

fn post(agent: Agent, mut state: State) -> Box<HandlerFuture> {
    let f = Body::take_from(&mut state)
        .concat2()
        .then(move |full_body| match full_body {
            Ok(valid_body) => {
                // Ceremony for parsing the information needed to create an
                // an assignent out of the message body.
                let (uuid, v) = match validate_assignment(&valid_body) {
                    Ok(uv) => uv,
                    Err(_e) => {
                        let res = create_empty_response(
                            &state,
                            StatusCode::BAD_REQUEST,
                        );
                        return future::ok((state, res));
                    }
                };

                // Ensure that an asignment with this uuid is not already
                // currently in flight.  If there is one, do not allow this
                // assignment to proceed.
                if agent.assignment_exists(&uuid) {
                    let res =
                        create_empty_response(&state, StatusCode::CONFLICT);
                    return future::ok((state, res));
                }

                let assignment =
                    Arc::new(RwLock::new(Assignment::new(v, &uuid)));

                info!("Received assignment {}.", &uuid);
                debug!("Received assignment: {:#?}", &assignment);

                // Before we even process the assignment, save it to persistent
                // storage.
                assignment_save(
                    &uuid,
                    REBALANCER_SCHEDULED_DIR,
                    assignment.clone(),
                );

                // Assignment has been saved.  Remove its id from the the table.
                agent.quiescing.lock().unwrap().remove(&uuid);

                // Create a response containing our newly initialized stats.
                // This serves as confirmation to the client that we recieved
                // their request correctly and are working on it.
                let res = create_response(
                    &state,
                    StatusCode::OK,
                    mime::APPLICATION_JSON,
                    serde_json::to_vec(&uuid)
                        .expect("serialized assignment id"),
                );

                // Signal the workers that there is a new assignent ready for
                // processing.
                assignment_signal(&agent, &uuid);
                future::ok((state, res))
            }
            Err(e) => future::err((state, e.into_handler_error())),
        });
    Box::new(f)
}

fn empty_response(state: State, code: StatusCode) -> Box<HandlerFuture> {
    let res = create_empty_response(&state, code);
    Box::new(future::ok((state, res)))
}

// First check to see if the assignment in question is located in memory.  If
// it is, then just return it to the caller, otherwise, go to disk and look
// through both scheduled and completed assignments for a match.
fn get_assignment_impl(
    agent: &Agent,
    uuid: &str,
) -> Option<Arc<RwLock<Assignment>>> {
    // Check in memory.
    if let Some(assignment) = assignment_get(&agent.assignments, &uuid) {
        return Some(assignment);
    }

    // Check completed assignments on disk.
    if let Ok(assignment) =
        assignment_recall(format!("{}/{}", REBALANCER_FINISHED_DIR, &uuid))
    {
        return Some(assignment);
    }

    // Check scheduled assignments on disk.
    if let Ok(assignment) =
        assignment_recall(format!("{}/{}", REBALANCER_SCHEDULED_DIR, &uuid))
    {
        return Some(assignment);
    }

    // No assignment of the supplied uuid was found.
    None
}

fn get_assignment(agent: Agent, state: State, id: &str) -> Box<HandlerFuture> {
    // If the uuid supplied by the client does not represent a valid UUID,
    // return a response indicating that they sent a bad request.
    let uuid = match Uuid::parse_str(id) {
        Ok(_u) => id,
        Err(e) => {
            let msg = format!("Invalid uuid: {}", e);
            let res = create_response(
                &state,
                StatusCode::BAD_REQUEST,
                mime::TEXT_PLAIN,
                serde_json::to_vec(&msg).expect("serialized message"),
            );
            return Box::new(future::ok((state, res)));
        }
    };

    let res = match get_assignment_impl(&agent, uuid) {
        Some(a) => {
            let assignment = a.read().unwrap();
            create_response(
                &state,
                StatusCode::OK,
                mime::APPLICATION_JSON,
                serde_json::to_vec(&*assignment).expect("serialized task"),
            )
        }
        None => create_empty_response(&state, StatusCode::NOT_FOUND),
    };

    Box::new(future::ok((state, res)))
}

// This function extracts the message body of a POST request.  The message body
// is a serialized json object continaing two things: the uuid of the
// assignment itself and a Vec<Task>.  These two items comprise the payload
// and are required to process an assignment.  As soon as it has been
// deserialized, this function will split it up in to the various pieces
// needed by the agent to get the process started.  The pieces of the payload
// go separate ways after this point, so they are separated out here in a tuple
// to save the caller from the monotony of accessing each (private) member of
// the structure by hand.
fn validate_assignment(body: &Chunk) -> Result<(String, Vec<Task>), String> {
    let payload: AssignmentPayload =
        match serde_json::from_slice(&body.to_vec()) {
            Ok(p) => p,
            Err(e) => {
                return Err(format!("Failed to deserialize payload: {}", e))
            }
        };

    Ok(<(String, Vec<Task>)>::from(payload))
}

impl Handler for Agent {
    fn handle(self, state: State) -> Box<HandlerFuture> {
        let method = Method::borrow_from(&state);

        // If we are here, then the method must either be
        // POST or GET.  It can not be anything else.
        match method.as_str() {
            "POST" => post(self, state),
            "GET" => {
                let path = PathExtractor::borrow_from(&state).clone();

                // If we received a GET request, there must only be one
                // part of a path specified (i.e. the uuid of the assignment)
                // otherwise, return a 404 to the client.
                if path.parts.len() != 1 {
                    return empty_response(state, StatusCode::NOT_FOUND);
                }

                get_assignment(self, state, &path.parts[0])
            }
            _ => empty_response(state, StatusCode::METHOD_NOT_ALLOWED),
        }
    }
}

impl NewHandler for Agent {
    type Instance = Self;

    fn new_handler(&self) -> gotham::error::Result<Self::Instance> {
        Ok(self.clone())
    }
}

// Used to construct the full path of an object on a storage
// node given the owner id and object id.
fn manta_file_path(owner: &str, object: &str) -> String {
    let path = format!("/manta/{}/{}", owner, object);
    path.to_string()
}

fn file_create(owner: &str, object: &str) -> File {
    let parent_dir = format!("/manta/{}", owner);
    let object_path = manta_file_path(owner, object);

    match fs::create_dir_all(parent_dir) {
        Err(e) => panic!("Error creating directory {}", e),
        Ok(_) => true,
    };

    match File::create(&object_path) {
        Err(e) => panic!("Error creating file {}", e),
        Ok(file) => file,
    }
}

fn verify_file_md5(file_path: &str, csum: &str) -> bool {
    let mut file = match fs::File::open(&file_path) {
        Err(e) => panic!("Error opening file {}", e),
        Ok(file) => file,
    };

    let mut hasher = Md5::new();
    match std::io::copy(&mut file, &mut hasher) {
        Ok(_) => (),
        Err(e) => {
            error!("Error hashing {}", e);
            return false;
        }
    };

    let result_ascii = base64::encode(&hasher.result());
    result_ascii == csum
}

// TODO: Make this return an actual result.
fn download(
    uri: &str,
    owner: &str,
    object: &str,
    csum: &str,
) -> Result<(), ObjectSkippedReason> {
    let file_path = manta_file_path(owner, object);

    let mut response = match reqwest::get(uri) {
        Ok(resp) => resp,
        Err(e) => {
            error!("Request failed: {}", &e);
            return Err(ObjectSkippedReason::NetworkError);
        }
    };

    let status = response.status();
    let msg = format!("Download response for {} is {}", uri, status);
    if status != reqwest::StatusCode::OK {
        error!("{}", msg);
        return Err(ObjectSkippedReason::HTTPStatusCode(status.into()));
    }

    trace!("{}", msg);

    let mut file = file_create(owner, object);

    match std::io::copy(&mut response, &mut file) {
        Ok(_) => (),
        Err(e) => {
            error!("Failed to complete object download: {}:{}", uri, e);
            return Err(ObjectSkippedReason::AgentFSError);
        }
    };

    if verify_file_md5(&file_path, csum) {
        Ok(())
    } else {
        Err(ObjectSkippedReason::MD5Mismatch)
    }
}

#[cfg(feature = "always_pass")]
fn process_task(task: &mut Task) {
    task.set_status(TaskStatus::Complete);
    return;
}

#[cfg(not(feature = "always_pass"))]
fn process_task(task: &mut Task) {
    let file_path = manta_file_path(&task.owner, &task.object_id);
    let path = Path::new(&file_path);

    // If the file exists and the checksum matches, then
    // short-circuit this operation and return.  There is
    // no need to download anything.  Mark the task as
    // complete and move on.
    if path.exists() && verify_file_md5(&file_path, &task.md5sum) {
        task.set_status(TaskStatus::Complete);
        info!(
            "Checksum passed -- no need to download: {}/{}",
            &task.owner, &task.object_id
        );
        return;
    }

    // Put it all together.  The format of the url is:
    // http://<storage id>/<owner id>/<object id>
    let url = format!(
        "http://{}/{}/{}",
        &task.source.manta_storage_id, &task.owner, &task.object_id
    );

    // Reach out to the storage node to download
    // the object.
    let status =
        match download(&url, &task.owner, &task.object_id, &task.md5sum) {
            Ok(_) => TaskStatus::Complete,
            Err(e) => TaskStatus::Failed(e),
        };

    task.set_status(status);
}

// Searches our HashMap of assignments.  This is not to be confused with the
// function that searches our on-disk database for assignments if they are
// no longer in memory.  See `assignment_recall()' for the function that queries
// persistent storage for assignment information.
fn assignment_get(
    assignments: &Arc<Mutex<Assignments>>,
    uuid: &str,
) -> Option<Arc<RwLock<Assignment>>> {
    let work = assignments.lock().unwrap();
    match work.get(uuid) {
        Some(assignment) => Some(Arc::clone(&assignment)),
        None => None,
    }
}

// Invoked by the worker thread, this function receives a caller-supplied
// HashMap and a uuid that it uses as a key to find the assignment within
// it.  Once it has obtained it, it processes each task in the assignment
// sequentially.  It is important to take note that this function takes the
// assignment and runs it to completion.  While being processed, no other
// worker can modify the assignment (whether that be task information within
// it, its associated stats, or its position in the HashMap).  Upon completing
// the processing of the assignment, the exact same thread removes it from
// the HashMap allowing the process to reclaim the memory it occupied.  If,
// for some reason in the future, we allow other threads to modify an assignment
// while it is in flight (i.e. while we are currently processing it), we must
// make considerations here among (probably) other places too.  In the current
// implementation however, we have the assurance that the thread invoking this
// function is the only one that will ever access this assignment with the
// intention of modifying it and further, the same thread invoking this function
// is the only one that will clean up the assignment when we have finished
// processing it, by calling `assignment_complete()'.
fn process_assignment(assignments: Arc<Mutex<Assignments>>, uuid: String) {
    // If we are unsuccessful in loading the assignment from disk, there is
    // nothing left to do here, other than return.
    if let Err(e) = load_saved_assignment(&assignments, &uuid) {
        error!("Unable to load assignment {} from disk: {}", &uuid, e);
        return;
    }

    let assignment = assignment_get(&assignments, &uuid).unwrap();
    let len = assignment.read().unwrap().tasks.len();
    let mut failures = Vec::new();

    assignment.write().unwrap().stats.state = AgentAssignmentState::Running;

    info!("Begin processing assignment {}.", &uuid);

    for i in 0..len {
        let assn = assignment.clone();

        // Obtain a copy of the current task from our task list.  We
        // will update the state information of the task and write it
        // back in to the vector.  We want to retain ownership of the
        // write-lock on the vector for as little as possible, so we
        // perform this operation inside of a new scope -- as soon as
        // it ends, the lock will be dropped along with the reference
        // to the task list which is why we obtain a copy of the task
        // as opposed to a reference to it.
        let mut t = {
            let tmp = &mut assn.write().unwrap().tasks;
            tmp[i].set_status(TaskStatus::Running);
            tmp[i].clone()
        };

        // Process it.
        process_task(&mut t);

        // Grab the write lock on the assignment.  It will only be held until
        // the end of the loop (which is not for very long).
        let tmp = &mut assn.write().unwrap();

        // Update our stats.
        match t.status {
            TaskStatus::Pending => (),
            TaskStatus::Running => (),
            TaskStatus::Complete => tmp.stats.complete += 1,
            TaskStatus::Failed(_) => {
                tmp.stats.complete += 1;
                tmp.stats.failed += 1;
                failures.push(t.clone());
            }
        }

        // Update the task in the vector.
        tmp.tasks[i] = t;
    }

    let failed = if failures.is_empty() {
        None
    } else {
        Some(failures)
    };

    assignment.write().unwrap().stats.state =
        AgentAssignmentState::Complete(failed);

    info!("Finished processing assignment {}.", &uuid);
    assignment_complete(assignments, uuid);
}

/// Create a `Router`
fn router() -> Router {
    build_simple_router(|route| {
        let (w, r): (mpsc::Sender<String>, mpsc::Receiver<String>) =
            mpsc::channel();
        let tx = Arc::new(Mutex::new(w));
        let rx = Arc::new(Mutex::new(r));
        let agent = Agent::new(tx.clone());
        let pool = ThreadPool::new(1);

        create_dir(REBALANCER_SCHEDULED_DIR);
        create_dir(REBALANCER_FINISHED_DIR);

        for _ in 0..1 {
            let rx = Arc::clone(&rx);
            let assignments = Arc::clone(&agent.assignments);
            pool.execute(move || loop {
                let uuid = match rx.lock().unwrap().recv() {
                    Ok(r) => r,
                    Err(e) => {
                        error!("Channel read error: {}", e);
                        return;
                    }
                };
                process_assignment(Arc::clone(&assignments), uuid);
            });
        }

        discover_saved_assignments(&agent);

        route
            .get("/assignments/*")
            .with_path_extractor::<PathExtractor>()
            .to_new_handler(agent.clone());

        route.post("assignments").to_new_handler(agent.clone());
    })
}

fn create_dir(dirname: &str) {
    if let Err(e) = fs::create_dir_all(dirname) {
        panic!("Error creating directory {}", e);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util;
    use gotham::handler::assets::FileOptions;
    use gotham::router::builder::{
        build_simple_router, DefineSingleRoute, DrawRoutes,
    };
    use gotham::test::TestServer;
    use std::{mem, thread, time};

    static MANTA_SRC_DIR: &str = "/var/tmp/rebalancer/src";

    // Very basic web server used to serve out files upon request.  This is
    // intended to be a replacement for a normal storage node in Manta (for
    // non-mako-specific testing.  That is, it is a means for testing basic
    // agent functionality.  It runs on port 80, as a normal web server would
    // and it treats GET requests in a similar manner that mako would, routing
    // GET requests based on account id and object id.  In the context of
    // testing, it is expected that the account id is "rebalancer", therefore,
    // requests for objects should look like:
    // GET /rebalancer/<object>.  To test with a wide variety of accounts, use
    // a real storage node.
    fn simple_server() {
        let addr = "127.0.0.1:80";
        let router = build_simple_router(|route| {
            // You can add a `to_dir` or `to_file` route simply using a
            // `String` or `str` as above, or a `Path` or `PathBuf` to accept
            // default options.
            // route.get("/").to_file("assets/doc.html");
            // Or you can customize options for comressed file handling, cache
            // control headers etc by building a `FileOptions` instance.
            route.get("/rebalancer/*").to_dir(
                FileOptions::new(MANTA_SRC_DIR)
                    .with_cache_control("no-cache")
                    .with_gzip(true)
                    .build(),
            );
        });

        thread::spawn(move || gotham::start(addr, router));
    }

    // This is a wrapper for `send_assignment_impl()'.  Under most circumstances
    // this is the function that a test will call and in doing so, it is with
    // the expectation that we will receive a status code of 200 (OK) from the
    // server and also that we have no interest in setting the uuid of the
    // assignment to anything specific.  For test cases that (for example) use
    // the same assignment uuid more than once and/or expect various status
    // codes other than 200 from the server, `send_assignment_impl()' can be
    // called directly, allowing the caller to supply those expectations in the
    // form of two additional arguments: the uuid of the assignment and the
    // desired http status code returned from the server.
    fn send_assignment(
        test_server: &TestServer,
        assignment: Arc<RwLock<Assignment>>,
    ) -> String {
        // Generate a uuid to accompany the assignment that we are about to
        // send to the agent.
        let uuid = Uuid::new_v4().to_hyphenated().to_string();
        send_assignment_impl(test_server, assignment, &uuid, StatusCode::OK);
        uuid
    }

    // Utility that actually forms the request, sends it off to the test
    // server and verifies that it was received as intended.  Upon success,
    // return the uuid of the assignment which we will use to monitor progress.
    fn send_assignment_impl(
        test_server: &TestServer,
        assignment: Arc<RwLock<Assignment>>,
        id: &str,
        status: StatusCode,
    ) {
        let tasks = &assignment.read().unwrap().tasks;
        let uuid = id.to_string();
        let obj: (String, Vec<Task>) = (uuid.clone(), tasks.to_vec());

        // Finally, serialize the entire HashMap before stuffing it in the
        // message body.
        let body: Vec<u8> =
            serde_json::to_vec(&obj).expect("Serialized payload");

        let response = test_server
            .client()
            .post(
                "http://localhost/assignments",
                hyper::Body::from(body),
                mime::APPLICATION_JSON,
            )
            .perform()
            .unwrap();

        // Fail immediately if the status code returned to us from the server
        // is not what we expect.
        assert_eq!(response.status(), status);

        // If we are here, then we received the status code from the server
        // that we expected.  That is, things are proceeding how we hoped they
        // would.  If we are expecting a status code of anything other than
        // StatusCode::OK, then the test ends here as a success.  There is no
        // need to parse the message body or monitor progress later on as this
        // assignment is not being processed by the agent.
        if status != StatusCode::OK {
            return;
        }

        let body = response.read_body().unwrap();
        let data = String::from_utf8(body.to_vec()).unwrap();
        let resp_uuid: String = match serde_json::from_str(&data) {
            Ok(s) => s,
            Err(e) => panic!(format!("Error: {}", e)),
        };

        info!("Response: {:?}", resp_uuid);

        // Perhaps it is overkill, but check to ensure that the uuid given
        // back to us matches what we actually sent.
        assert_eq!(uuid, resp_uuid);
    }

    // Given a path of an assignment on disk, load it in to memory.  If for
    // some reason this does not succeed, panic, causing the test case that
    // invoked it to fail.
    fn load_assignment<S: Into<String>>(path: S) -> Arc<RwLock<Assignment>> {
        let assignment = match assignment_recall(path) {
            Ok(a) => a,
            Err(e) => panic!(format!("Unable to load assignment: {}", e)),
        };

        assignment
    }

    // Send a request to get the latest information on an assignment.  This
    // information is used by the test automation to determine how far along
    // the agent is in processing the assignment.  During testing, this will
    // likely be called repeatedly for a particular assignment until it is
    // observed that the number of tasks completed is equal to the total number
    // of tasks in the assignment.
    fn get_progress(test_server: &TestServer, uuid: &str) -> Assignment {
        let url = format!("http://localhost/assignments/{}", uuid);
        let response = test_server.client().get(url).perform().unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = response.read_body().unwrap();
        let data = String::from_utf8(body.to_vec()).unwrap();
        let assignment: Assignment = match serde_json::from_str(&data) {
            Ok(a) => a,
            Err(e) => panic!(format!("Failed to deserialize: {}", e)),
        };

        assignment
    }

    // Poll the server indefinitely on the status of a given assignment until
    // it is complete.  Currently, it's not clear if there should be an
    // expectation on how long an assignment should take to complete, especially
    // in a test scenario.  If the agent is inoperable due to being wedged, a
    // request will timeout causing a panic for a given test case anyway.  For
    // that reason, it is probably reasonable to allow this function to loop
    // indefinitely with the assumption that the agent is not hung.
    fn monitor_progress(test_server: &TestServer, uuid: &str) -> Assignment {
        loop {
            let assignment = get_progress(test_server, uuid);
            thread::sleep(time::Duration::from_secs(10));

            // If we have finished processing all tasks, return the assignment
            // to the caller.
            if mem::discriminant(&assignment.stats.state)
                == mem::discriminant(&AgentAssignmentState::Complete(None))
            {
                return assignment;
            }
            thread::sleep(time::Duration::from_secs(10));
        }
    }

    // Given the uuid of a single assignment, monitor its progress until
    // it is complete.  Ensure that all tasks in the assignment have the
    // expected status at the end.
    fn monitor_assignment(
        test_server: &TestServer,
        uuid: &str,
        expected: TaskStatus,
    ) {
        // Wait for the assignment to complete.
        let assignment = monitor_progress(test_server, &uuid);
        let stats = &assignment.stats;

        if let AgentAssignmentState::Complete(opt) = &stats.state {
            match opt {
                None => {
                    if expected != TaskStatus::Complete {
                        panic!("Assignment succeeded when it should not.");
                    }
                }
                Some(tasks) => {
                    for t in tasks.iter() {
                        assert_eq!(t.status, expected);
                    }
                }
            }
        }
    }

    // Utility function which is very frequently used to set up both the test
    // server (the agent) as well as a simple web server (which acts as a
    // storage node).  It only takes one argument which is the path of an
    // assignment to load.  Functions called from unit_test_init() that do not
    // succeed will panic, so there is no need to return anything other than the
    // the test server and the newly loaded assignment.
    fn unit_test_init(path: &str) -> (TestServer, Arc<RwLock<Assignment>>) {
        simple_server();
        let test_server: TestServer = TestServer::new(router()).unwrap();
        let assignment = load_assignment(path);
        (test_server, assignment)
    }

    // Test name:    Download
    // Description:  Download a healthy file from a storage node that the agent
    //               does not already have.
    // Expected:     The operation should be a success.  Specifically,
    //               TaskStatus for any/all tasks as part of this assignment
    //               should appear as "Complete".
    #[test]
    fn download() {
        let _guard = util::init_global_logger();
        let (test_server, assignment) = unit_test_init("test/agent/areacodes");
        let uuid = send_assignment(&test_server, assignment);
        monitor_assignment(&test_server, &uuid, TaskStatus::Complete);
    }

    // Test name:    Replace healthy
    // Description:  First, download a known healthy file that the agent (may or
    //               may not already have).  After successful completion of the
    //               first download, repeat the process a second time with the
    //               exact same assignment information.
    // Expected:     TaskStatus for all tasks in the assignment should appear
    //               as "Complete".
    #[test]
    fn replace_healthy() {
        let _guard = util::init_global_logger();
        // Download a file once.
        let (test_server, assignment) = unit_test_init("test/agent/areacodes");
        let uuid = send_assignment(&test_server, Arc::clone(&assignment));
        monitor_assignment(&test_server, &uuid, TaskStatus::Complete);

        // Send the exact same assignment again.  Note: Even though the contents
        // of this assignment are identical to the previous one, it will be
        // assigned a different uuid so that the agent does not automatically
        // reject it.  The utility function `send_assignment()' generates a
        // new random uuid on the callers behalf each time that it is called,
        // which is why we have assurance that there will not be a uuid
        // collision, resulting in a rejection.
        let uuid = send_assignment(&test_server, assignment);
        monitor_assignment(&test_server, &uuid, TaskStatus::Complete);
    }

    // Test name:   Client Error.
    // Description: Attempt to download an object from a storage node where
    //              the object does not reside will cause a client error.
    // Expected:    TaskStatus for all tasks in the assignment should appear
    //              as "Failed(HTTPStatusCode(NotFound))".
    #[test]
    fn object_not_found() {
        let _guard = util::init_global_logger();
        let (test_server, assignment) = unit_test_init("test/agent/areacodes");

        // Rename the object id to something that we know is not on the storage
        // server.  In this case, a file by the name of "abc".
        assignment.write().unwrap().tasks[0].object_id = "abc".to_string();
        let uuid = send_assignment(&test_server, assignment);
        monitor_assignment(
            &test_server,
            &uuid,
            TaskStatus::Failed(ObjectSkippedReason::HTTPStatusCode(
                reqwest::StatusCode::NOT_FOUND.into(),
            )),
        );
    }

    // Test name:   Fail to repair a damaged file due to checksum failure.
    // Description: Download a file in order to replace a known damaged copy.
    //              Upon completion of the download, the checksum of the file
    //              should fail.  It is important to note that the purpose of
    //              this test is not to specifcally test the correctness of
    //              the mechanism used to calculate the md5 hash, but rather to
    //              verify that in a situation where the calculated hash does
    //              not match the expected value, such an event is made known
    //              to us in the records of failed tasks supplied to us by the
    //              assignment when we ask for it at its completion.
    // Expected:    TaskStatus for all tasks in the assignment should appear
    //              as Failed("MD5Mismatch").
    #[test]
    fn failed_checksum() {
        let _guard = util::init_global_logger();
        let (test_server, assignment) = unit_test_init("test/agent/areacodes");

        // Scribble on the checksum information for the object.  This ensures
        // that it will fail at the end, even though the agent calculates it
        // correctly.
        assignment.write().unwrap().tasks[0].md5sum = "abc".to_string();
        let uuid = send_assignment(&test_server, assignment);
        monitor_assignment(
            &test_server,
            &uuid,
            TaskStatus::Failed(ObjectSkippedReason::MD5Mismatch),
        );
    }

    // Test name:   Duplicate assignment
    // Description: First, successfully process an assignment.  Upon completion
    //              reissue the exact same assignment (including the uuid) to
    //              the agent.  Any time that an agent receives an assignment
    //              uuid that it knows it has already received -- regardless of
    //              the state of that assignment (i.e. complete or not) -- it
    //              the request should be rejected.
    // Expected:    When we send the assignment for the second time, the server
    //              should return a response of 409 (CONFLICT).
    #[test]
    fn duplicate_assignment() {
        let _guard = util::init_global_logger();
        // Download a file once.
        let (test_server, assignment) = unit_test_init("test/agent/areacodes");
        let uuid = send_assignment(&test_server, Arc::clone(&assignment));
        monitor_assignment(&test_server, &uuid, TaskStatus::Complete);

        // Send the exact same assignment again and send it, although this time,
        // we will reuse our first uuid. We expect to receive a status code of
        // StatusCode::CONFLICT (409) from the server this time.
        send_assignment_impl(
            &test_server,
            assignment,
            &uuid,
            StatusCode::CONFLICT,
        );
    }
}
