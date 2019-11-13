#[macro_use]
extern crate gotham_derive;

#[macro_use]
extern crate serde_derive;

use std::collections::HashMap;

use remora::config;
use remora::jobs::{self, JobAction};
use remora::moray_client;
use remora::util;

use crossbeam_channel;
use futures::{future, Future, Stream};
use gotham::handler::{
    Handler, HandlerFuture, IntoHandlerError, IntoResponse, NewHandler,
};
use gotham::helpers::http::response::create_response;
use gotham::router::builder::{
    build_simple_router, DefineSingleRoute, DrawRoutes,
};
use gotham::router::Router;
use gotham::state::{FromState, State};
use hyper::{Body, Response, StatusCode};
use libmanta::moray::MantaObjectShark;
use remora::jobs::evacuate::EvacuateJob;
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

impl IntoResponse for JobStatus {
    fn into_response(self, state: &State) -> Response<Body> {
        create_response(
            &state,
            StatusCode::OK,
            mime::APPLICATION_JSON,
            serde_json::to_string(&self).expect("serialized Job Status"),
        )
    }
}

fn get_job(mut state: State) -> (State, JobStatus) {
    let get_job_params = GetJobParams::take_from(&mut state);
    let uuid = Uuid::parse_str(&get_job_params.uuid).expect("valid uuid");
    let status = jobs::status::get_status(uuid).expect("valid job");

    let response = JobStatus { status };

    (state, response)
}

impl IntoResponse for JobList {
    fn into_response(self, state: &State) -> Response<Body> {
        create_response(
            &state,
            StatusCode::OK,
            mime::APPLICATION_JSON,
            serde_json::to_string(&self).expect("serialized Job Status"),
        )
    }
}

fn list_jobs(state: State) -> (State, JobList) {
    let response = JobList {
        jobs: jobs::status::list_jobs().unwrap(),
    };

    (state, response)
}

/*
fn create_job(mut state: State) -> (State, String) {
    let request = Body::take_from(&mut state);


    config::Config::parse_config(None)

    (state, Uuid::new_v4().to_string())

}
*/

// TODO this needs to change to be more accomodating for future jobs.
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
}

impl NewHandler for JobCreateHandler {
    type Instance = Self;

    fn new_handler(&self) -> gotham::error::Result<Self::Instance> {
        Ok(self.clone())
    }
}

/*
impl Handler for JobCreateHandler {
    fn handle(self, mut state: State) -> Box<HandlerFuture> {
        // TODO:  config parsing should be done when the process starts
        let config = config::Config::parse_config(None).expect("Config parse");
        let mut job = jobs::Job::new(config.clone());
        let job_uuid = job.get_id().to_string();

        let f = Body::take_from(&mut state)
            .concat2()
            .then(move |body| match body {
                Ok(valid_body) => {
                    let payload:JobPayload = serde_json::from_slice(&valid_body
                        .into_bytes()).expect("Bad Payload");

                    match payload.action {
                        JobActionPayload::Evacuate(evac_payload) => {
                            let domain_name = evac_payload.domain_name
                                .unwrap_or(config.domain_name.clone());
                            let max_objects = match evac_payload.max_objects {
                                Some(str_val) => {

                                    // TODO
                                    let val: u32 = str_val.parse().expect
                                    ("max obj parse");

                                    if val == 0 {
                                        None
                                    } else {
                                        Some(val)
                                    }
                                },
                                None => {
                                    Some(10) // Default
                                }
                            };

                            // TODO: We are temporarily faking a shark for
                            // testing
                            let shark = MantaObjectShark {
                                manta_storage_id: evac_payload
                                    .from_shark.clone(),
                                datacenter: String::from
                                    ("Somefakedc")

                            };
                            /*
                            let shark moray_client::get_manta_object_shark (
                                &evac_payload.from_shark,
                                &domain_name).expect("get shark");
                            */

                            let job_action = JobAction::Evacuate(
                                Box::new(EvacuateJob::new(

                                    shark,
                                    &domain_name,
                                    &job_uuid,
                                    max_objects,
                                )));

                            job.add_action(job_action);
                            self.tx.send(job).expect("Send job to threadpool");
                            let res = create_response(
                                &state,
                                StatusCode::OK,
                                mime::APPLICATION_JSON,
                                job_uuid
                            );
                            future::ok((state, res))
                        }
                    }
                },
                Err(e) => future::err((state, e.into_handler_error())),
            });
        Box::new(f)
    }
}
*/

impl Handler for JobCreateHandler {
    fn handle(self, mut state: State) -> Box<HandlerFuture> {
        // TODO:  config parsing should be done when the process starts
        let config = config::Config::parse_config(None).expect("Config parse");
        let mut job = jobs::Job::new(config.clone());
        let job_uuid = job.get_id().to_string();

        let f =
            Body::take_from(&mut state).concat2().then(
                move |body| match body {
                    Ok(valid_body) => {
                        let payload: JobPayload =
                            serde_json::from_slice(&valid_body.into_bytes())
                                .expect("Bad Payload");
                        future::ok(payload)
                    }
                    Err(e) => future::err(e.into_handler_error()),
                },
            );

        let payload = f.wait().expect("Bad payload");
        let ret = match payload.action {
            JobActionPayload::Evacuate(evac_payload) => {
                let domain_name = evac_payload
                    .domain_name
                    .unwrap_or(config.domain_name.clone());
                let max_objects = match evac_payload.max_objects {
                    Some(str_val) => {
                        // TODO
                        let val: u32 = str_val.parse().expect("max obj parse");

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

                // TODO: We are temporarily faking a shark for
                // testing
                let shark = evac_payload.from_shark;
                /*
                let shark = MantaObjectShark {
                    manta_storage_id: evac_payload.from_shark.clone(),
                    datacenter: String::from("Somefakedc"),
                };
                */
                /*
                let shark moray_client::get_manta_object_shark (
                    &evac_payload.from_shark,
                    &domain_name).expect("get shark");
                */

                let job_action =
                    JobAction::Evacuate(Box::new(EvacuateJob::new(
                        shark,
                        &domain_name,
                        &job_uuid,
                        max_objects,
                    )));

                job.add_action(job_action);
                self.tx.send(job).expect("Send job to threadpool");
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

fn router() -> Router {
    let (tx, rx) = crossbeam_channel::bounded(5);
    let job_create_handler = JobCreateHandler { tx };

    //job_thread_pool(rx);
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
            job.run().expect("bad job run");
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
    let addr = "0.0.0.0:8888";

    gotham::start(addr, router())
}

#[cfg(test)]
mod tests {
    use super::*;
    use gotham::test::TestServer;

    #[test]
    fn basic() {
        let test_server = TestServer::new(router()).unwrap();

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
        let _guard = util::init_global_logger();
        let test_server = TestServer::new(router()).unwrap();
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

        if response.status() != StatusCode::OK {
            println!("CHECK ME");
            return;
        }
        let ret = response.read_utf8_body().unwrap();
        println!("{:#?}", ret);
    }
}
