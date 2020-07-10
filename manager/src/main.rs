/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2020 Joyent, Inc.
 */

#[macro_use]
extern crate gotham_derive;

#[macro_use]
extern crate serde_derive;

#[macro_use]
extern crate rebalancer;

mod gotham_json_util;

// JEmallocator drastically improves our memory footprint
use jemallocator::Jemalloc;
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

use manager::config::Config;
use manager::jobs::status::{JobStatus, StatusError};
use manager::jobs::{
    self, JobActionDbEntry, JobBuilder, JobDbEntry, JobPayload, JobState,
    JobUpdateMessage,
};
use manager::metrics::{metrics_init, metrics_request_inc};
use manager::pg_db::{connect_db, REBALANCER_DB};
use rebalancer::util;

use std::collections::HashMap;
use std::error::Error;
use std::str::FromStr;
use std::sync::{Arc, Mutex};

use crate::gotham_json_util::JsonBody;
use clap::{App, Arg, ArgMatches};
use crossbeam_channel;
use diesel::query_dsl::{QueryDsl, RunQueryDsl};
use diesel::PgConnection;
use futures::{future, Future};
use gotham::handler::{Handler, HandlerFuture, NewHandler};
use gotham::helpers::http::response::create_response;
use gotham::middleware::Middleware;
use gotham::pipeline::new_pipeline;
use gotham::pipeline::set::{finalize_pipeline_set, new_pipeline_set};
use gotham::router::builder::{build_router, DefineSingleRoute, DrawRoutes};
use gotham::router::Router;
use gotham::state::{FromState, State};
use hyper::{Body, Response, StatusCode};
use lazy_static::lazy_static;
use manager::jobs::evacuate::EvacuateJobUpdateMessage;
use threadpool::ThreadPool;
use uuid::Uuid;

static THREAD_COUNT: usize = 1;

lazy_static! {
    static ref UPDATE_CHANS: Mutex<HashMap<Uuid, crossbeam_channel::Sender<JobUpdateMessage>>> =
        Mutex::new(HashMap::new());
}

#[derive(Deserialize, StateData, StaticResponseExtender)]
struct GetJobParams {
    uuid: String,
}

#[derive(Debug, Deserialize, Serialize)]
struct JobList {
    jobs: Vec<String>,
}

#[derive(Deserialize, StateData, StaticResponseExtender)]
struct UpdateJobParams {
    uuid: String,
}

fn add_update_channel(
    uuid: Uuid,
    update_tx: crossbeam_channel::Sender<JobUpdateMessage>,
) {
    let mut update_chans =
        UPDATE_CHANS.lock().expect("lock update chans hashmap");

    // This should only happen if we have duplicate UUIDs, so panic is
    // appropriate.
    if update_chans.insert(uuid, update_tx).is_some() {
        let msg = format!("update_tx for {} already exists", uuid.to_string());
        error!("{}", msg);
        panic!(msg);
    }
}

fn remove_update_channel(uuid: Uuid) {
    let mut update_chans =
        UPDATE_CHANS.lock().expect("lock update chans hashmap");

    if update_chans.remove(&uuid).is_none() {
        warn!(
            "attempt to remove update_tx for {} that  doesn't exist",
            uuid.to_string()
        );
    }
}

fn get_update_channel(
    uuid: Uuid,
) -> Result<crossbeam_channel::Sender<JobUpdateMessage>, String> {
    let update_chans = UPDATE_CHANS.lock().expect("lock update chans hashmap");
    let chan = update_chans.get(&uuid).ok_or_else(|| {
        format!(
            "Job ({}) does not support dynamic configuration updates",
            uuid
        )
    })?;

    Ok(chan.clone())
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
    Box<dyn Future<Item = JobStatus, Error = StatusError> + Send>;

fn get_job_status(uuid: Uuid) -> GetJobFuture {
    Box::new(match jobs::status::get_job(uuid) {
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

    Box::new(get_job_status(uuid).then(move |result| {
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

fn update_job(mut state: State) -> (State, Response<Body>) {
    use crate::jobs::jobs::dsl::jobs as jobs_db;

    let db_conn = DBConnMiddlewareData::take_from(&mut state).db_conn;
    let update_job_params = UpdateJobParams::take_from(&mut state);
    let uuid =
        Uuid::from_str(update_job_params.uuid.as_str()).expect("uuid from str");
    let tx: crossbeam_channel::Sender<JobUpdateMessage>;

    let job_db_entry: JobDbEntry = match jobs_db
        .find(update_job_params.uuid.as_str())
        .first(&db_conn)
    {
        Ok(jdbe) => jdbe,
        Err(_) => {
            let msg = format!("Could not find job {}", uuid);
            let res = bad_request(&state, msg);
            return (state, res);
        }
    };

    if job_db_entry.state != JobState::Running {
        let res = bad_request(
            &state,
            "Attempt to update job that is not running".into(),
        );
        return (state, res);
    }

    tx = match get_update_channel(uuid) {
        Ok(t) => t,
        Err(e) => {
            let res = bad_request(&state, e);
            return (state, res);
        }
    };

    #[allow(clippy::single_match)]
    let update_message = match job_db_entry.action {
        JobActionDbEntry::Evacuate => {
            let evac_msg =
                match state.json_body::<EvacuateJobUpdateMessage>().wait() {
                    Ok(p) => p,
                    Err(e) => {
                        let msg = format!(
                            "Could not parse Evacuate Update \
                             Message: {}",
                            e
                        );
                        warn!("{}", msg);
                        let res = create_response(
                            &state,
                            StatusCode::UNPROCESSABLE_ENTITY,
                            mime::APPLICATION_JSON,
                            msg,
                        );
                        return (state, res);
                    }
                };

            if let Err(e) = evac_msg.validate() {
                let res = bad_request(&state, e);
                return (state, res);
            }

            JobUpdateMessage::Evacuate(evac_msg)
        }
        _ => {
            let res = bad_request(&state, "payload action mismatch".into());
            return (state, res);
        }
    };

    // Send update message down channel
    if let Err(e) = tx.send(update_message) {
        let res = invalid_server_error(
            &state,
            format!("could not communicate with job: {}", e),
        );

        return (state, res);
    }

    let res =
        create_response(&state, StatusCode::OK, mime::APPLICATION_JSON, "");

    (state, res)
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

        // If snaplinks are still in play then we immediately return failure.
        if config.snaplink_cleanup_required {
            let error = invalid_server_error(
                &state,
                String::from("Snaplink Cleanup Required"),
            );
            return Box::new(future::ok((state, error)));
        }

        let job_builder = JobBuilder::new(config);
        let payload = match state.json_body::<JobPayload>().wait() {
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
                    .evacuate(evac_payload.from_shark, max_objects)
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
                let uuid_response = format!("{}\n", &job_uuid);

                if let Some(update_tx) = &job.update_tx {
                    add_update_channel(job_uuid, update_tx.clone());
                }

                if let Err(e) = self.tx.send(job) {
                    panic!("Tx error: {}", e);
                }

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

#[derive(NewMiddleware, Copy, Clone)]
struct NoopMiddleware;

impl Middleware for NoopMiddleware {
    fn call<Chain>(self, state: State, chain: Chain) -> Box<HandlerFuture>
    where
        Chain: FnOnce(State) -> Box<HandlerFuture> + Send + 'static,
    {
        chain(state)
    }
}

#[derive(NewMiddleware, Copy, Clone)]
struct DBConnMiddleware;

#[derive(StateData)]
struct DBConnMiddlewareData {
    db_conn: PgConnection,
}

impl Middleware for DBConnMiddleware {
    fn call<Chain>(self, mut state: State, chain: Chain) -> Box<HandlerFuture>
    where
        Chain: FnOnce(State) -> Box<HandlerFuture> + Send + 'static,
    {
        let db_conn = match connect_db(REBALANCER_DB) {
            Ok(dbc) => dbc,
            Err(e) => {
                let msg = format!("Could not connect to rebalancer DB: {}", e);
                let res = invalid_server_error(&state, msg);
                return Box::new(future::ok((state, res)));
            }
        };

        state.put(DBConnMiddlewareData { db_conn });
        chain(state)
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
                Err(e) => {
                    error!("Error receiving job message: {}", e);
                    return;
                }
            };
            let job_id = job.get_id();

            // This blocks until the job is complete.  If the user wants to
            // see the status of the job, they can issue a request to:
            //      /jobs/<job uuid>
            if let Err(e) = job.run() {
                warn!("Error running job: {}", e);
            }

            remove_update_channel(job_id);
        });
    }

    let ps_builder = new_pipeline_set();
    let (ps_builder, noop) =
        ps_builder.add(new_pipeline().add(NoopMiddleware).build());

    let (ps_builder, db_conn) =
        ps_builder.add(new_pipeline().add(DBConnMiddleware).build());

    let ps = finalize_pipeline_set(ps_builder);

    let noop_pipeline = (noop, ());
    let db_conn_pipeline = (db_conn, ());

    // By default we use the NoopPipeline.  If a given route needs a specific
    // pipeline, then specify it via `.with_pipeline_chain()`
    let rtr = build_router(noop_pipeline, ps, |route| {
        route.with_pipeline_chain(db_conn_pipeline, |route| {
            route
                .put("/jobs/:uuid")
                .with_path_extractor::<UpdateJobParams>()
                .to(update_job);
        });
        route
            .post("/jobs")
            .to_new_handler(job_create_handler.clone());
        route
            .get("/jobs/:uuid")
            .with_path_extractor::<GetJobParams>()
            .to(get_job);
        route.get("/jobs").to(list_jobs);
    });

    info!("Rebalancer Online");

    rtr
}

fn main() {
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
    let config = Config::parse_config(&config_file).unwrap_or_else(|e| {
        println!("Error parsing config file: {}", e);
        std::process::exit(1);
    });

    let _guard = util::init_global_logger(Some(config.log_level));

    let config = Arc::new(Mutex::new(config));

    info!("Initializing...");

    if let Err(e) = jobs::create_job_database() {
        error!("Error creating Jobs database: {}", e);
        return;
    }

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
    use gotham::test::{TestResponse, TestServer};
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
            let _guard = util::init_global_logger(None);
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

    fn put_update(
        test_server: &TestServer,
        uuid: &Uuid,
        update_msg: &EvacuateJobUpdateMessage,
    ) -> TestResponse {
        let put_url =
            format!("http://localhost:8888/jobs/{}", uuid.to_string());
        let update_msg = serde_json::to_string(update_msg).unwrap();
        test_server
            .client()
            .put(put_url, update_msg, mime::APPLICATION_JSON)
            .perform()
            .expect("put update")
    }

    fn create_job(test_server: &TestServer, job_payload: JobPayload) -> String {
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

        ret.to_string()
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
            .evacuate(String::from("fake_storage_id"), None)
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
        let pretty_response: JobStatus =
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
        });

        let job_id = create_job(&test_server, job_payload);
        println!("{}", job_id);
    }

    #[test]
    fn job_dynamic_update() {
        unit_test_init();
        let update_msg = EvacuateJobUpdateMessage::SetMetadataThreads(1);
        let (tx, _) = crossbeam_channel::unbounded();
        let uuid = Uuid::new_v4();
        let (config, test_server) = test_server_init();

        let config = { config.lock().unwrap().clone() };
        assert!(!config.options.use_static_md_update_threads);

        add_update_channel(uuid, tx);
        assert!(get_update_channel(uuid).is_ok());

        // We just manually put the channel in the UPDATE_CHANS hash, so
        // didn't actually create a job so we don't expect the job lookup to
        // succeed.
        let expected_body = format!("Could not find job {}", uuid);
        let res = put_update(&test_server, &uuid, &update_msg);
        let res_body = res.read_utf8_body().unwrap();

        assert_eq!(expected_body, res_body);

        remove_update_channel(uuid);
        assert!(get_update_channel(uuid).is_err());

        // Now we actually start a job and wait for it to fail, then assert
        // that we get the correct error message.  We could try to race with
        // the job creation and see if we get a 200 back, but that is error
        // prone.  Some testing will need to be done with this deployed in an
        // actual environment.
        let job_payload = JobPayload::Evacuate(EvacuateJobPayload {
            from_shark: String::from("fake_storage_id"),
            max_objects: Some(10),
        });
        let job_id = create_job(&test_server, job_payload);
        let mut count = 0;

        // This job will eventually fail, unless this is being run in a
        // legitimate manta deployment.  But all other tests assume they are
        // being run outside of manta.
        loop {
            if count > 50 {
                assert!(false, "Job stalled");
            }

            let job_list = get_job_list(&test_server).expect("get job list");
            let job = job_list
                .into_iter()
                .find(|j| j.id == job_id)
                .expect("missing job");

            if job.state != JobState::Failed {
                thread::sleep(std::time::Duration::from_millis(500));
                count += 1;
                continue;
            }
            break;
        }

        let expected_body =
            format!("Attempt to update job that is not running");

        let job_uuid = Uuid::parse_str(&job_id).unwrap();
        let res = put_update(&test_server, &job_uuid, &update_msg);
        let res_body = res.read_utf8_body().unwrap();

        assert_eq!(res_body, expected_body);
    }
}
