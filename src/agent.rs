use std::collections::HashMap;
use std::fs;
use std::fs::File;
//use std::io::Error;
use std::net::*;
use std::path::Path;
use std::sync::{mpsc, Arc, Mutex, RwLock};

use futures::future;
use futures::future::*;
use futures::stream::*;

//use gotham::extractor::QueryStringExtractor;
use gotham::handler::{Handler, HandlerFuture, IntoHandlerError, NewHandler};
use gotham::helpers::http::response::{create_empty_response, create_response};
use gotham::router::{builder::*, Router};
use gotham::state::{FromState, State};
use gotham_derive::{StateData, StaticResponseExtender};

use base64;
use hyper::{Body, Chunk, Method, Uri};
use libmanta::moray::MantaObjectShark;
use md5::{Digest, Md5};

use crate::job::Task;
use crate::job::TaskStatus;

use reqwest::StatusCode;
use rusqlite;
use serde::ser::SerializeMap;
use serde_derive::Deserialize;
use threadpool::ThreadPool;
use trust_dns_resolver::Resolver;
use uuid::Uuid;
use walkdir::WalkDir;

type Assignments = HashMap<String, Arc<RwLock<Vec<Task>>>>;

static REMORA_SCHEDULED_DIR: &str = "/manta/remora";
static REMORA_FINISHED_DIR: &str = "/var/tmp/remora";

#[derive(Deserialize, StateData, StaticResponseExtender)]
struct QueryStringExtractor {
    uuid: String,
}

#[derive(Clone, Debug)]
pub struct Agent {
    assignments: Arc<Mutex<Assignments>>,
    tx: Arc<Mutex<mpsc::Sender<String>>>,
}

impl Agent {
    pub fn new(tx: Arc<Mutex<mpsc::Sender<String>>>) -> Agent {
        let assignments = Arc::new(Mutex::new(Assignments::new()));
        Agent {
            assignments,
            tx,
        }
    }
}

impl serde::ser::Serialize for Agent {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::ser::Serializer,
    {
        let work = self.assignments.lock().unwrap();
        let mut map = serializer.serialize_map(Some(work.len()))?;

        for (k, v) in &*work {
            let _tasks = v.read().unwrap();
            map.serialize_entry(&k.to_string(), &**v)?;
        }
        map.end()
    }
}

fn load_saved_assignments(agent: &Agent) {
    for entry in WalkDir::new(REMORA_SCHEDULED_DIR)
        .min_depth(1)
        .follow_links(false)
        .into_iter()
        .filter_map(|e| e.ok())
    {
        let uuid = entry.file_name().to_string_lossy();
        println!("{}/{}", REMORA_SCHEDULED_DIR, uuid);
        match assignment_recall(format!("{}/{}", REMORA_SCHEDULED_DIR, &uuid)) {
            Ok(v) => assignment_add(&agent, v, &uuid),
            Err(e) => panic!(format!("Error loading database: {}", e)),
        }
    }
}

fn assignment_save(uuid: &str, path: &str, tasks: Arc<RwLock<Vec<Task>>>) {
    let conn = match rusqlite::Connection::open(format!("{}/{}", path, uuid)) {
        Ok(conn) => conn,
        Err(e) => panic!("DB error {}", e),
    };

    let tasklist = tasks.read().unwrap();

    let result = conn.execute(
        "create table if not exists tasks (
		object_id text primary key not null unique,
		owner text not null,
		md5sum text not null,
		datacenter text not null,
		manta_storage_id text not null,
		status text not null
	    )",
        rusqlite::params![],
    );

    match result {
        Ok(_) => (),
        Err(e) => panic!("Database creation error: {}", e),
    };

    for task in &*tasklist {
        match conn.execute(
            "INSERT INTO tasks
			(object_id, owner, md5sum, datacenter, manta_storage_id,
			status)
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
}

// Given a uuid of a particular assignment, extract its contents from
// persistent storage.  All assignements on disk are stored in separate
// files named after their uuid.  The format is an sqlite database.  We
// construct a vector of tasks based on the contents of the only table
// in the file called `tasks'.
fn assignment_recall(path: String) -> Result<Arc<RwLock<Vec<Task>>>, String> {
    if !Path::new(&path).exists() {
        return Err(format!("File does not exist: {}", path));
    }

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

    Ok(Arc::new(RwLock::new(tasks)))
}

// Take our current assignment that we have just finished processing and flush
// out the contents (with updated status for each task) out to a new database
// file in /var/tmp/remora.  Next, delete the original file from /manta/remora
// so that we do not process it again on restart of the agent.  Finally, remove
// the assignment from our HashMap.
fn assignment_complete(assignments: Arc<Mutex<Assignments>>, uuid: String) {
    let tasks = assignment_get(&assignments, &uuid).unwrap();

    assignment_save(&uuid, REMORA_FINISHED_DIR, tasks);
    let src = format!("{}/{}", REMORA_SCHEDULED_DIR, uuid);

    match fs::remove_file(&src) {
        Ok(_) => (),
        Err(e) => panic!(format!("Error removing file: {}", e)),
    };

    // Remove it from our HashMap of assignments.
    let mut hm = assignments.lock().unwrap();
    hm.remove(&uuid);
}

// Take a given assignment (i.e. a vector of Task objects) and add it to
// the our HashMap which contains all outstanding work to be processed.  Then
// signal the uuid of that assignment to our workers.  The first available
// worker will begin procesing it.  There are basically two ways that this
// function can get called:
//
// 1. At the start of the remora agent when we are loading incomplete
//    assignments from disk.
// 2. When we receive an assignment over the network from the remora zone.
fn assignment_add(
    agent: &Agent,
    assignment: Arc<RwLock<Vec<Task>>>,
    uuidstr: &str,
) {
    let mut work = agent.assignments.lock().unwrap();

    work.insert(uuidstr.to_string(), assignment);

    let tx = agent.tx.lock().unwrap();
    tx.send(uuidstr.to_string()).unwrap();
}

fn post(agent: Agent, mut state: State) -> Box<HandlerFuture> {
    let f = Body::take_from(&mut state)
        .concat2()
        .then(move |full_body| match full_body {
            Ok(valid_body) => {
                let v = Arc::new(RwLock::new(
                    validate_assignment(&valid_body).unwrap(),
                ));

                let uuid = Uuid::new_v4().to_hyphenated().to_string();
                assignment_save(&uuid, REMORA_SCHEDULED_DIR, v.clone());

                let res = create_response(
                    &state,
                    StatusCode::OK,
                    mime::APPLICATION_JSON,
                    serde_json::to_vec(&uuid)
                        .expect("serialized assignment id"),
                );

                assignment_add(&agent, v, &uuid);
                future::ok((state, res))
            }
            Err(e) => future::err((state, e.into_handler_error())),
        });
    Box::new(f)
}

fn get(agent: Agent, state: State) -> Box<HandlerFuture> {
    // Note, it is not necessary to explicitly obtain the lock on our
    // hashmap when iterating through assignment/task information  because
    // the underlying serialization procedure (Agent serialization)
    // acquires it.
    let res = {
        create_response(
            &state,
            StatusCode::OK,
            mime::APPLICATION_JSON,
            serde_json::to_vec(&agent).expect("serialized assignments"),
        )
    };
    Box::new(future::ok((state, res)))
}

// First check to see if the assignment in question is located in memory.  If
// it is, then just return it to the caller, otherwise, go to disk and look
// through assignments that have already been full processed.
fn get_specific_impl(
    agent: &Agent,
    uuid: &str,
) -> Option<Arc<RwLock<Vec<Task>>>> {
    match assignment_get(&agent.assignments, &uuid) {
        Some(assignment) => Some(assignment),
        None => {
            // If it was not found in memory, then we should check
            // our records of assignments that have already been
            // completed.
            match assignment_recall(format!(
                "{}/{}",
                REMORA_FINISHED_DIR, &uuid
            )) {
                Ok(assignment) => Some(assignment),
                Err(e) => {
                    println!("Assignment recall: {}", e);
                    None
                }
            }
        }
    }
}

fn get_specific(agent: Agent, mut state: State) -> Box<HandlerFuture> {
    let query_param = QueryStringExtractor::take_from(&mut state);
    let uuid = query_param.uuid;

    let res = match get_specific_impl(&agent, &uuid) {
        Some(assignment) => {
            let tasks = assignment.read().unwrap();
            create_response(
                &state,
                StatusCode::OK,
                mime::APPLICATION_JSON,
                serde_json::to_vec(&**tasks).expect("serialized assignments"),
            )
        }
        None => create_empty_response(&state, StatusCode::NOT_FOUND),
    };

    Box::new(future::ok((state, res)))
}

fn validate_assignment(body: &Chunk) -> Result<Vec<Task>, String> {
    let data = String::from_utf8(body.to_vec()).unwrap();

    let v = match serde_json::from_str(&data) {
        Ok(v) => v,
        Err(e) => return Err(format!("Failed to deserialize: {}", e)),
    };

    let assignment: Vec<Task> = serde_json::from_value(v).unwrap();
    Ok(assignment.clone())
}

impl Handler for Agent {
    fn handle(self, state: State) -> Box<HandlerFuture> {
        let method = Method::borrow_from(&state);
        let path = Uri::borrow_from(&state).path();

        // If we are here, then the method must either be
        // POST or GET.  It can not be anything else.
        match method.as_str() {
            "POST" => post(self, state),
            _ => {
                if path == "/assignment" {
                    get_specific(self, state)
                } else {
                    get(self, state)
                }
            }
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
            println!("Error hashing {}", e);
            return false;
        }
    };

    let result_ascii = base64::encode(&hasher.result().to_ascii_lowercase());
    result_ascii == csum
}

// TODO: Make this return an actual result.
fn download(
    uri: &str,
    owner: &str,
    object: &str,
    csum: &str,
) -> Result<(), String> {
    let file_path = manta_file_path(owner, object);

    let mut response = match reqwest::get(uri) {
        Ok(resp) => resp,
        Err(e) => {
            println!("request failed!");
            return Err(format!("Network: {}", e));
        }
    };

    // If the response status code is anything other than 200 (Ok), flag
    // failure and return.
    if response.status() != reqwest::StatusCode::OK {
        return Err(format!(
            "Failed request with status: {}",
            response.status()
        ));
    }

    let mut file = file_create(owner, object);

    match std::io::copy(&mut response, &mut file) {
        Ok(_) => (),
        Err(e) => {
            println!("Failed to complete object download: {}:{}", uri, e);
            return Err(format!("Streaming: {}", e));
        }
    };

    if verify_file_md5(&file_path, csum) {
        Ok(())
    } else {
        Err("Checksum".to_string())
    }
}

fn name_to_address(name: &str) -> Result<String, String> {
    let resolver = Resolver::from_system_conf().unwrap();
    let response = match resolver.lookup_ip(name) {
        Ok(resp) => resp,
        Err(e) => return Err(format!("DNS lookup {}", e)),
    };
    let shark_ip: Vec<IpAddr> = response.iter().collect();
    Ok(shark_ip[0].to_string())
}

fn process_task(task: &mut Task) {
    let file_path = manta_file_path(&task.owner, &task.object_id);
    let path = Path::new(&file_path);

    // If the file exists and the checksum matches, then
    // short-circuit this operation and return.  There is
    // no need to download anything.  Mark the task as
    // complete and move on.
    if path.exists() && verify_file_md5(&file_path, &task.md5sum) {
        task.set_status(TaskStatus::Complete);
        println!("Checksum passed -- no need to download.");
        return;
    }

    // Resolve the storage id of the shark to an ip address.
    // This will be part of the url that we generate to
    // download the object.
    let shark_ip = match name_to_address(&task.source.manta_storage_id) {
        Ok(addr) => addr,
        Err(e) => {
            task.set_status(TaskStatus::Failed(e));
            println!("DNS lookup error");
            return;
        }
    };

    // Put it all together.  The format of the url is:
    // http://<shark ip>/<owner id>/<object id>
    let url = format!(
        "http://{}/{}/{}",
        shark_ip.to_string(),
        &task.owner,
        &task.object_id
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
) -> Option<Arc<RwLock<Vec<Task>>>> {
    let work = assignments.lock().unwrap();
    match work.get(uuid) {
        Some(assignment) => Some(assignment.clone()),
        None => None,
    }
}

fn process_assignment(assn: Arc<Mutex<Assignments>>, uuid: String) {
    let tasks = assignment_get(&assn, &uuid).unwrap();
    let len = tasks.read().unwrap().len();

    for i in 0..len {
        // Obtain a copy of the current task from our task list.  We
        // will update the state information of the task and write it
        // back in to the vector.  We want to retain ownership of the
        // write-lock on the vector for as little as possible, so we
        // perform this operation inside of a new scope -- as soon as
        // it ends, the lock will be dropped along with the reference
        // to the task list which is why we obtain a copy of the task
        // as opposed to a reference to it.
        let mut t = {
            let mut tmp = tasks.write().unwrap();
            tmp[i].set_status(TaskStatus::Running);
            tmp[i].clone()
        };

        // Process it.
        process_task(&mut t);

        // Update the task in the vector.
        let mut tmp = tasks.write().unwrap();
        tmp[i] = t;
    }

    assignment_complete(assn, uuid);
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

        for _ in 0..1 {
            let rx = rx.clone();
            let assignments = agent.assignments.clone();
            pool.execute(move || loop {
                let uuid = rx.lock().unwrap().recv().unwrap();
                process_assignment(assignments.clone(), uuid);
            });
        }

        load_saved_assignments(&agent);

        route
            .get("assignment")
            .with_query_string_extractor::<QueryStringExtractor>()
            .to_new_handler(agent.clone());

        route.get("assignments").to_new_handler(agent.clone());

        route.post("assignments").to_new_handler(agent.clone());
    })
}

fn create_dir(dirname: &str) {
    if let Err(e) = fs::create_dir_all(dirname) {
        panic!("Error creating directory {}", e);
    }
}

impl Agent {
    pub fn run(addr: &'static str) {
        create_dir(REMORA_SCHEDULED_DIR);
        create_dir(REMORA_FINISHED_DIR);

        println!("Listening for requests at {}", addr);
        gotham::start(addr, router());
    }
}
