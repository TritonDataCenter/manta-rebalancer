/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2020, Joyent, Inc.
 */

#[macro_use]
extern crate gotham_derive;

#[macro_use]
extern crate serde_derive;

#[macro_use]
extern crate rebalancer;

use std::collections::HashMap;
use std::string::ToString;
use manager::config::{self, Config};
use manager::jobs::{self, JobAction};

use clap::{App, Arg, ArgMatches};
use crossbeam_channel;
use futures::{future, Future, Stream};
use gotham::handler::{Handler, HandlerFuture, IntoHandlerError, NewHandler};
use gotham::helpers::http::response::create_response;
use gotham::router::builder::{
    build_simple_router, DefineSingleRoute, DrawRoutes,
};
use gotham::router::Router;
use gotham::state::{FromState, State};
use hyper::{Body, Response, StatusCode};
use libmanta::moray::MantaObjectShark;
use manager::jobs::evacuate::EvacuateJob;
use manager::jobs::status::StatusError;
use rebalancer::util;
use threadpool::ThreadPool;
use uuid::Uuid;

static THREAD_COUNT: usize = 1;

#[derive(Deserialize, StateData, StaticResponseExtender)]
struct GetJobParams {
    uuid: String,
}

#[derive(Debug, Deserialize, Serialize)]
struct JobStatus {
    status: HashMap<String, usize>,
}

#[derive(Debug, Deserialize, Serialize)]
struct JobList {
    jobs: Vec<String>,
}

fn bad_request(state: &State, msg: String) -> Response<Body> {
    warn!("{}", msg);
    create_response(state, StatusCode::BAD_REQUEST, mime::APPLICATION_JSON, msg)
}

fn invalid_server_error(state: &State, msg: String) -> Response<Body> {
    error!("{}", msg);
    create_response(
        state,
        StatusCode::INTERNAL_SERVER_ERROR,
        mime::APPLICATION_JSON,
        msg,
    )
}

type GetJobFuture =
    Box<dyn Future<Item = HashMap<String, usize>, Error = StatusError> + Send>;

fn get_status(uuid: Uuid) -> GetJobFuture {
    Box::new(match jobs::status::get_status(uuid) {
        Ok(status) => future::ok(status),
        Err(e) => future::err(e),
    })
}

fn get_job(mut state: State) -> Box<HandlerFuture> {
    info!("Get Job Request");
    let get_job_params = GetJobParams::take_from(&mut state);
    let uuid = match Uuid::parse_str(&get_job_params.uuid) {
        Ok(id) => id,
        Err(e) => {
            let msg = format!("Invalid UUID: {}", e);
            let ret = bad_request(&state, msg);
            return Box::new(future::ok((state, ret)));
        }
    };

    Box::new(get_status(uuid).then(move |result| {
        match result {
            Ok(job_status) => {
                let ret = match serde_json::to_string(&job_status) {
                    Ok(status) => create_response(
                        &state,
                        StatusCode::OK,
                        mime::APPLICATION_JSON,
                        status,
                    ),
                    Err(e) => {
                        let msg = format!("Error Getting Job Status: {}", e);
                        invalid_server_error(&state, msg)
                    }
                };
                future::ok((state, ret))
            }
            Err(e) => {
                let ret: Response<Body>;
                error!("Get Status error: {:?}", e);
                match e {
                    StatusError::DBExists => {
                        ret = bad_request(
                            &state,
                            format!("Could not find job UUID: {}", uuid),
                        );
                    }
                    // TODO: We want to eventually have a master DB that list
                    // all the Jobs and their states.  For now we simply
                    // create a new DB for each job.  The trouble is, while
                    // the Job is still initializing its table may not have
                    // been created yet as the tables are unique to the Job
                    // Action.
                    StatusError::LookupError | StatusError::Unknown => {
                        ret = invalid_server_error(
                            &state,
                            String::from(
                                "Internal Lookup Error.  Job may be \
                                 in the Init state.\n",
                            ),
                        );
                    }
                }
                future::ok((state, ret))
            }
        }
    }))
}

type JobListFuture =
    Box<dyn Future<Item = Vec<String>, Error = StatusError> + Send>;

fn get_job_list() -> JobListFuture {
    Box::new(match jobs::status::list_jobs() {
        Ok(list) => future::ok(list),
        Err(e) => future::err(e),
    })
}

fn list_jobs(state: State) -> Box<HandlerFuture> {
    info!("List Jobs Request");
    let job_list_future = get_job_list();
    Box::new(job_list_future.then(move |result| match result {
        Ok(list) => {
            let jobs = match serde_json::to_string(&list) {
                Ok(j) => j,
                Err(e) => {
                    let msg = format!("Error Getting Job List: {}", e);
                    let ret = invalid_server_error(&state, msg);
                    return future::ok((state, ret));
                }
            };
            let res = create_response(
                &state,
                StatusCode::OK,
                mime::APPLICATION_JSON,
                jobs,
            );
            future::ok((state, res))
        }
        Err(e) => {
            let msg = format!("Error Getting Job List: {:#?}", e);
            let ret = invalid_server_error(&state, msg);
            future::ok((state, ret))
        }
    }))
}

#[derive(Serialize, Deserialize, Default)]
struct JobPayload {
    action: JobActionPayload,
}

#[derive(Serialize, Deserialize)]
enum JobActionPayload {
    Evacuate(EvacuateJobPayload),
}

impl Default for JobActionPayload {
    fn default() -> Self {
        JobActionPayload::Evacuate(EvacuateJobPayload::default())
    }
}

#[derive(Serialize, Deserialize, Default)]
struct EvacuateJobPayload {
    from_shark: MantaObjectShark,
    domain_name: Option<String>,
    max_objects: Option<u32>,
}

#[derive(Clone)]
struct JobCreateHandler {
    tx: crossbeam_channel::Sender<jobs::Job>,
    config: Config,
}

impl NewHandler for JobCreateHandler {
    type Instance = Self;

    fn new_handler(&self) -> gotham::error::Result<Self::Instance> {
        Ok(self.clone())
    }
}

impl Handler for JobCreateHandler {
    fn handle(self, mut state: State) -> Box<HandlerFuture> {
        info!("Post Job Request");
        let mut job = jobs::Job::new(self.config.clone());
        let job_uuid = job.get_id().to_string();

        let f =
            Body::take_from(&mut state).concat2().then(
                move |body| match body {
                    Ok(valid_body) => {
                        match serde_json::from_slice::<JobPayload>(
                            &valid_body.into_bytes(),
                        ) {
                            Ok(jp) => future::ok(jp),
                            Err(e) => future::err(e.into_handler_error()),
                        }
                    }
                    Err(e) => future::err(e.into_handler_error()),
                },
            );

        let payload = match f.wait() {
            Ok(p) => p,
            Err(e) => {
                return Box::new(future::err((state, e)));
            }
        };

        let ret = match payload.action {
            JobActionPayload::Evacuate(evac_payload) => {
                let domain_name = evac_payload
                    .domain_name
                    .unwrap_or_else(|| self.config.domain_name.clone());
                let max_objects = match evac_payload.max_objects {
                    Some(val) => {
                        if val == 0 {
                            None
                        } else {
                            Some(val)
                        }
                    }
                    None => {
                        Some(10) // Default
                    }
                };

                let job_action =
                    JobAction::Evacuate(Box::new(EvacuateJob::new(
                        evac_payload.from_shark.clone(),
                        &domain_name,
                        &job_uuid,
                        max_objects,
                    )));

                job.add_action(job_action);

                if let Err(e) = self.tx.send(job) {
                    panic!("Tx error: {}", e);
                }

                let uuid_response = format!("{}\n", job_uuid);
                create_response(
                    &state,
                    StatusCode::OK,
                    mime::APPLICATION_JSON,
                    uuid_response,
                )
            }
        };

        Box::new(future::ok((state, ret)))
    }
}

fn router(config: Config) -> Router {
    let (tx, rx) = crossbeam_channel::bounded(5);
    let job_create_handler = JobCreateHandler { tx, config };

    let pool = ThreadPool::new(THREAD_COUNT);
    for _ in 0..THREAD_COUNT {
        let thread_rx = rx.clone();
        pool.execute(move || loop {
            let job = match thread_rx.recv() {
                Ok(j) => j,
                Err(_) => {
                    // TODO Check error
                    return;
                }
            };
            // This blocks until the job is complete.  If the user want's to
            // see the status of the job, they can issue a request to:
            //      /jobs/<job uuid>
            if let Err(e) = job.run() {
                warn!("Error running job: {}", e);
            }
        });
    }

    build_simple_router(|route| {
        route
            .get("/jobs/:uuid")
            .with_path_extractor::<GetJobParams>()
            .to(get_job);
        route.get("/jobs").to(list_jobs);
        route
            .post("/jobs")
            .to_new_handler(job_create_handler.clone());
    })
}

fn main() {
    let _guard = util::init_global_logger();
    let addr = "0.0.0.0:8888";

    let matches: ArgMatches = App::new("rebalancer")
        .version("0.1.0")
        .about("Rebalancer")
        .arg(
            Arg::with_name("config_file")
                .short("c")
                .long("config_file")
                .takes_value(true)
                .value_name("CONFIG_FILE")
                .help("Specify the location of the config file"),
        )
        .get_matches();

    let config_file = matches.value_of("config_file");

    let config = config::Config::parse_config(config_file)
        .map_err(|e| {
            //xxxremora::error!("Error parsing config: {}", e);
            error!("Error parsing config: {}", e);
            std::process::exit(1);
        })
        .unwrap();

    gotham::start(addr, router(config))
}

#[cfg(test)]
mod tests {
    use super::*;
    use gotham::test::TestServer;
    use lazy_static::lazy_static;
    use std::sync::Mutex;
    use std::thread;

    lazy_static! {
        static ref INITIALIZED: Mutex<bool> = Mutex::new(false);
    }

    fn unit_test_init() {
        let mut init = INITIALIZED.lock().unwrap();
        if *init {
            return;
        }

        thread::spawn(move || {
            let _guard = util::init_global_logger();
            loop {
                std::thread::sleep(std::time::Duration::from_millis(1))
            }
        });

        *init = true;
    }

    #[test]
    fn basic() {
        unit_test_init();
        let config = Config::parse_config(Some("src/config.json")).unwrap();

        // Create a Job manually so that we know one exists regardless of the
        // ability of this API to create one, or the order in which tests are
        // run.
        let new_job = manager::jobs::Job::new(config.clone());
        let job_id = new_job.get_id().to_string();
        let job_action = manager::jobs::evacuate::EvacuateJob::new(
            MantaObjectShark {
                manta_storage_id: String::from("fake_storage_id"),
                datacenter: String::from("fake_datacenter"),
            },
            "fake.joyent.us",
            &job_id,
            None,
        );

        job_action.create_table().unwrap();

        let test_server = TestServer::new(router(config)).unwrap();

        let response = test_server
            .client()
            .get("http://localhost:8888/jobs")
            .perform()
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let jobs_ret = response.read_body().unwrap();
        let job_list: Vec<String> = serde_json::from_slice(&jobs_ret).unwrap();

        assert!(job_list.len() > 0);
        println!("Return value: {:#?}", job_list);

        let get_job_uri = format!("http://localhost:8888/jobs/{}", job_id);
        let response = test_server.client().get(get_job_uri).perform().unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let ret = response.read_utf8_body().unwrap();
        let pretty_response: HashMap<String, usize> =
            serde_json::from_str(&ret).expect("job status hash");
        println!("{:#?}", pretty_response);
    }

    #[test]
    fn post_test() {
        unit_test_init();
        let config = Config::parse_config(Some("src/config.json")).unwrap();
        let test_server = TestServer::new(router(config)).unwrap();
        let action_payload = EvacuateJobPayload {
            domain_name: Some(String::from("fake.joyent.us")),
            from_shark: MantaObjectShark {
                manta_storage_id: String::from("fake_storage_id"),
                datacenter: String::from("fake_datacenter"),
            },
            max_objects: Some(10),
        };

        let job_payload = JobPayload {
            action: JobActionPayload::Evacuate(action_payload),
        };

        let payload = serde_json::to_string(&job_payload).unwrap();

        let response = test_server
            .client()
            .post(
                "http://localhost:8888/jobs",
                payload,
                mime::APPLICATION_JSON,
            )
            .perform()
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let ret = response.read_utf8_body().unwrap();
        assert!(Uuid::parse_str(&ret).is_ok());

        println!("{:#?}", ret);
    }
}
