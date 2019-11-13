#[macro_use]
extern crate gotham_derive;

#[macro_use]
extern crate serde_derive;

use remora::{info, log_impl, util, warn};
use std::collections::HashMap;
use std::string::ToString;

use remora::config::{self, Config};
use remora::jobs::{self, JobAction};

use crossbeam_channel;
use futures::{future, Future, Stream};
use gotham::handler::{Handler, HandlerFuture, IntoHandlerError, NewHandler};
use gotham::helpers::http::response::{create_empty_response, create_response};
use gotham::router::builder::{
    build_simple_router, DefineSingleRoute, DrawRoutes,
};
use gotham::router::Router;
use gotham::state::{FromState, State};
use hyper::{Body, Response, StatusCode};
use libmanta::moray::MantaObjectShark;
use remora::jobs::evacuate::EvacuateJob;
use remora::jobs::status::StatusError;
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
    remora::error!("{}", msg);
    create_response(
        state,
        StatusCode::INTERNAL_SERVER_ERROR,
        mime::APPLICATION_JSON,
        msg,
    )
}

fn get_job(mut state: State) -> (State, Response<Body>) {
    info!("Get Job Request");
    let get_job_params = GetJobParams::take_from(&mut state);
    let uuid = match Uuid::parse_str(&get_job_params.uuid) {
        Ok(id) => id,
        Err(e) => {
            let msg = format!("Invalid UUID: {}", e);
            let ret = bad_request(&state, msg);
            return (state, ret);
        }
    };

    let status = match jobs::status::get_status(uuid) {
        Ok(s) => s,
        Err(e) => {
            let ret: Response<Body>;
            match e {
                StatusError::DBExists => {
                    ret = bad_request(
                        &state,
                        format!("Could not find job UUID: {}", uuid),
                    );
                }
                StatusError::LookupError => {
                    ret = invalid_server_error(
                        &state,
                        String::from("Job Database Error"),
                    );
                }
            }
            return (state, ret);
        }
    };

    let response = match serde_json::to_string(&status) {
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

    (state, response)
}

fn list_jobs(state: State) -> (State, Response<Body>) {
    info!("List Jobs Request");
    let response = match jobs::status::list_jobs() {
        Ok(list) => {
            let jobs = match serde_json::to_string(&list) {
                Ok(j) => j,
                Err(e) => {
                    let msg = format!("Error Getting Job List: {}", e);
                    let ret = invalid_server_error(&state, msg);
                    return (state, ret);
                }
            };
            create_response(
                &state,
                StatusCode::OK,
                mime::APPLICATION_JSON,
                jobs,
            )
        }
        Err(e) => {
            let msg = format!("Error Getting Job List: {}", e);
            let ret = invalid_server_error(&state, msg);
            return (state, ret);
        }
    };

    (state, response)
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
    max_objects: Option<String>,
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
                    Some(str_val) => {
                        // TODO
                        let val: u32 = match str_val.parse() {
                            Ok(v) => v,
                            Err(_) => {
                                let ret = create_empty_response(
                                    &state,
                                    StatusCode::UNPROCESSABLE_ENTITY,
                                );
                                return Box::new(future::ok((state, ret)));
                            }
                        };

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

                let from_shark = evac_payload.from_shark.clone();
                let job_action =
                    JobAction::Evacuate(Box::new(EvacuateJob::new(
                        from_shark,
                        &domain_name,
                        &job_uuid,
                        max_objects,
                    )));

                job.add_action(job_action);

                if let Err(e) = self.tx.send(job) {
                    panic!("Tx error: {}", e);
                }

                create_response(
                    &state,
                    StatusCode::OK,
                    mime::APPLICATION_JSON,
                    job_uuid,
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

    let config = config::Config::parse_config(None)
        .map_err(|e| {
            remora::error!("Error parsing config: {}", e);
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

    lazy_static! {
        static ref INITIALIZED: Mutex<bool> = Mutex::new(false);
    }

    fn unit_test_init() {
        let mut init = INITIALIZED.lock().unwrap();
        if *init {
            return;
        }

        let _guard = util::init_global_logger();

        *init = true;
    }

    #[test]
    fn basic() {
        unit_test_init();
        let config = Config::parse_config(Some("src/config.json")).unwrap();
        let test_server = TestServer::new(router(config)).unwrap();

        let response = test_server
            .client()
            .get("http://localhost:8888/jobs")
            .perform()
            .unwrap();

        let jobs_ret = response.read_body().unwrap();
        let job_list: JobList = serde_json::from_slice(&jobs_ret).unwrap();

        println!("{:#?}", job_list);

        let get_this_job = job_list.jobs[0].clone();
        let get_job_uri =
            format!("http://localhost:8888/jobs/{}", get_this_job);

        let response = test_server.client().get(get_job_uri).perform().unwrap();

        let ret = response.read_body().unwrap();
        let pretty_response: JobStatus = serde_json::from_slice(&ret).unwrap();
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
            max_objects: None,
        };

        let job_payload = JobPayload {
            action: JobActionPayload::Evacuate(action_payload),
        };

        let payload = serde_json::to_string(&job_payload).unwrap();

        println!("===========================");
        println!("{}", payload);
        println!("===========================");
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
        println!("{:#?}", ret);
    }
}
