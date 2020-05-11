/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2020 Joyent, Inc.
 */

#[macro_use]
extern crate failure;

#[macro_use]
extern crate gotham_derive;

#[macro_use]
extern crate serde_derive;

#[macro_use]
extern crate rebalancer;

// JEmallocator drastically improves our memory footprint
use jemallocator::Jemalloc;
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

use manager::config::Config;
use manager::jobs::status::StatusError;
use manager::jobs::{self, evacuate::ObjectSource, JobBuilder, JobDbEntry,
                    JobPayload};
use manager::metrics::{metrics_init, metrics_request_inc};
use rebalancer::util;

use std::collections::HashMap;
use std::error::Error;
use std::sync::{Arc, Mutex};

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
    Box<dyn Future<Item = HashMap<String, i64>, Error = StatusError> + Send>;

fn get_status(uuid: Uuid) -> GetJobFuture {
    Box::new(match jobs::status::get_status(uuid) {
        Ok(status) => future::ok(status),
        Err(e) => future::err(e),
    })
}

fn get_job(mut state: State) -> Box<HandlerFuture> {
    metrics_request_inc(Some("get_job"));
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
    Box<dyn Future<Item = Vec<JobDbEntry>, Error = StatusError> + Send>;

fn get_job_list() -> JobListFuture {
    Box::new(match jobs::status::list_jobs() {
        Ok(list) => future::ok(list),
        Err(e) => future::err(e),
    })
}

fn list_jobs(state: State) -> Box<HandlerFuture> {
    metrics_request_inc(Some("list_jobs"));
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

#[derive(Clone)]
struct JobCreateHandler {
    tx: crossbeam_channel::Sender<jobs::Job>,
    config: Arc<Mutex<Config>>,
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

        let config = self.config.lock().expect("config lock").clone();
        let domain_name = config.domain_name.clone();

        // If snaplinks are still in play then we immediately return failure.
        if config.snaplink_cleanup_required {
            let error = invalid_server_error(
                &state,
                String::from("Snaplink Cleanup Required"),
            );
            return Box::new(future::ok((state, error)));
        }

        let job_builder = JobBuilder::new(config);
        let f =
            Body::take_from(&mut state).concat2().then(
                move |body| match body {
                    Ok(valid_body) => {
                        match serde_json::from_slice::<JobPayload>(
                            &valid_body.into_bytes(),
                        ) {
                            Ok(jp) => future::ok(jp),
                            Err(e) => {
                                error!("Error deserializing: {}", &e);
                                future::err(e.into_handler_error())
                            }
                        }
                    }
                    Err(e) => {
                        error!("Body parse error: {}", &e);
                        future::err(e.into_handler_error())
                    }
                },
            );

        let payload: JobPayload = match f.wait() {
            Ok(p) => p,
            Err(e) => {
                error!("Payload error: {}", &e);
                return Box::new(future::err((state, e)));
            }
        };

        let ret = match payload {
            JobPayload::Evacuate(evac_payload) => {
                metrics_request_inc(Some("evacuate"));

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

                let job = match job_builder
                    .evacuate(
                        evac_payload.from_shark,
                        &domain_name,
                        evac_payload.source,
                        max_objects,
                    )
                    .commit()
                {
                    Ok(j) => j,
                    Err(e) => {
                        let error = invalid_server_error(
                            &state,
                            String::from(e.description()),
                        );
                        return Box::new(future::ok((state, error)));
                    }
                };

                let job_uuid = job.get_id();
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

fn router(config: Arc<Mutex<Config>>) -> Router {
    let (tx, rx) = crossbeam_channel::bounded(5);
    let job_create_handler = JobCreateHandler { tx, config };

    // Start the metrics server.
    metrics_init(rebalancer::metrics::ConfigMetrics::default());

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
            // This blocks until the job is complete.  If the user wants to
            // see the status of the job, they can issue a request to:
            //      /jobs/<job uuid>
            if let Err(e) = job.run() {
                warn!("Error running job: {}", e);
            }
        });
    }

    let rtr = build_simple_router(|route| {
        route
            .get("/jobs/:uuid")
            .with_path_extractor::<GetJobParams>()
            .to(get_job);
        route.get("/jobs").to(list_jobs);
        route
            .post("/jobs")
            .to_new_handler(job_create_handler.clone());
    });

    info!("Rebalancer Online");

    rtr
}

fn main() {
    let _guard = util::init_global_logger();

    info!("Initializing...");

    if let Err(e) = jobs::create_job_database() {
        error!("Error creating Jobs database: {}", e);
        return;
    }

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

    let config_file = matches.value_of("config_file").map(|s| s.to_string());
    let config = Arc::new(Mutex::new(
        Config::parse_config(&config_file)
            .map_err(|e| {
                error!("Error parsing config: {}", e);
                std::process::exit(1);
            })
            .unwrap(),
    ));

    let addr = format!(
        "0.0.0.0:{}",
        config.lock().expect("lock config").listen_port
    );

    let config_watcher_handle =
        Config::start_config_watcher(Arc::clone(&config), config_file);

    gotham::start_with_num_threads(addr, router(Arc::clone(&config)), 1);

    config_watcher_handle.join().expect("join config watcher");
}

#[cfg(test)]
mod tests {
    use super::*;
    use gotham::test::TestServer;
    use lazy_static::lazy_static;
    use manager::jobs::{EvacuateJobPayload, JobPayload};
    use rebalancer::error::{Error, InternalError};
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

        *init = true;

        thread::spawn(move || {
            let _guard = util::init_global_logger();
            loop {
                std::thread::sleep(std::time::Duration::from_millis(1))
            }
        });
    }

    fn get_job_list(
        test_server: &TestServer,
    ) -> Result<Vec<JobDbEntry>, Error> {
        let response = test_server
            .client()
            .get("http://localhost:8888/jobs")
            .perform()
            .expect("Get Job List");

        if !response.status().is_success() {
            let msg = format!("client get error: {}", response.status());
            return Err(InternalError::new(None, msg).into());
        }

        let jobs_ret = response.read_body().expect("response body");
        serde_json::from_slice(&jobs_ret).map_err(Error::from)
    }

    fn job_list_contains(jobs: &Vec<JobDbEntry>, id: &str) -> bool {
        jobs.iter().any(|j| j.id.to_string() == id)
    }

    fn test_server_init() -> (Arc<Mutex<Config>>, TestServer) {
        let config = Mutex::new(
            Config::parse_config(&Some("src/config.json".to_string()))
                .expect("config"),
        );
        let config = Arc::new(config);
        let test_server =
            TestServer::new(router(Arc::clone(&config))).expect("test server");
        (config, test_server)
    }

    #[test]
    fn basic() {
        unit_test_init();
        let (config, test_server) = test_server_init();

        // Create a Job manually so that we know one exists regardless of the
        // ability of this API to create one, or the order in which tests are
        // run.
        let config = config.lock().expect("lock config").clone();
        let job_builder = JobBuilder::new(config);
        let job = job_builder
            .evacuate(
                String::from("fake_storage_id"),
                "fake.joyent.us",
                ObjectSource::default(),
                None,
            )
            .commit()
            .expect("Failed to create job");
        let job_id = job.get_id().to_string();
        let job_list = get_job_list(&test_server).expect("get job list");

        assert!(job_list.len() > 0);
        assert!(job_list_contains(&job_list, &job_id));

        let get_job_uri = format!("http://localhost:8888/jobs/{}", job_id);
        let response = test_server
            .client()
            .get(get_job_uri)
            .perform()
            .expect("get job status response");

        assert_eq!(response.status(), StatusCode::OK);

        let ret = response.read_utf8_body().expect("response body");
        let pretty_response: HashMap<String, usize> =
            serde_json::from_str(&ret).expect("job status hash");
        println!("{:#?}", pretty_response);
    }

    #[test]
    fn post_test() {
        unit_test_init();
        let (_, test_server) = test_server_init();
        let job_payload = JobPayload::Evacuate(EvacuateJobPayload {
            from_shark: String::from("fake_storage_id"),
            max_objects: Some(10),
            source: ObjectSource::default(),
        });
        let payload = serde_json::to_string(&job_payload)
            .expect("serde serialize payload");
        let response = test_server
            .client()
            .post(
                "http://localhost:8888/jobs",
                payload,
                mime::APPLICATION_JSON,
            )
            .perform()
            .expect("client post");

        assert_eq!(response.status(), StatusCode::OK);

        let ret = response.read_utf8_body().expect("response body");
        let ret = ret.trim_end();
        assert!(Uuid::parse_str(ret).is_ok());

        println!("{:#?}", ret);
    }
}
