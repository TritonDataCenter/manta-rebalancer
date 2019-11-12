#[macro_use]
extern crate gotham_derive;

#[macro_use]
extern crate serde_derive;

use std::collections::HashMap;

use remora::jobs;

use gotham::handler::IntoResponse;
use gotham::helpers::http::response::create_response;
use gotham::router::Router;
use gotham::router::builder::{build_simple_router, DefineSingleRoute, DrawRoutes};
use gotham::state::{State, FromState};
use hyper::{Body, Response, StatusCode};
use uuid::Uuid;

#[derive(Deserialize, StateData, StaticResponseExtender)]
struct GetJobParams {
    uuid: String,
}

#[derive(Debug, Deserialize, Serialize)]
struct JobStatus {
    status: HashMap<String, usize>
}

#[derive(Debug, Deserialize, Serialize)]
struct JobList {
    jobs: Vec<String>
}

impl IntoResponse for JobStatus {
    fn into_response(self, state: &State) -> Response<Body> {
        create_response(
            &state,
            StatusCode::OK,
            mime::APPLICATION_JSON,
            serde_json::to_string(&self)
                .expect("serialized Job Status")
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
            serde_json::to_string(&self)
                .expect("serialized Job Status")
        )
    }
}

fn list_jobs(state: State) -> (State, JobList) {
    let response = JobList {
        jobs: jobs::status::list_jobs().unwrap()
    };

    (state, response)
}

fn create_job(mut state: State) -> (State, String) {
    let request = Body::take_from(&mut state);

    // TODO

    (state, Uuid::new_v4().to_string())

}

fn router() -> Router {
    build_simple_router(|route| {
        route.get("/jobs/:uuid")
            .with_path_extractor::<GetJobParams>()
            .to(get_job);
        route.get("/jobs")
            .to(list_jobs);
        route.post("/jobs")
            .to(create_job);
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
        let get_job_uri = format!("http://localhost:8888/jobs/{}",
                                  get_this_job);

        let response = test_server
            .client()
            .get(get_job_uri)
            .perform()
            .unwrap();


        let ret = response.read_body().unwrap();
        let pretty_response: JobStatus = serde_json::from_slice(&ret).unwrap();
        println!("{:#?}", pretty_response);
    }
}
