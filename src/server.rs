
#[macro_use]
extern crate gotham_derive;

#[macro_use]
extern crate serde_derive;

use std::collections::HashMap;

use remora::jobs::status;

use gotham::router::Router;
use gotham::router::builder::{build_simple_router, DefineSingleRoute, DrawRoutes};
use gotham::state::{State, FromState};

use hyper::{Body, Response, StatusCode};
use gotham::helpers::http::response::create_response;

#[derive(Deserialize, StateData, StaticResponseExtender)]
struct GetJobParams {
    uuid: String,
}

struct JobStatus {
    status: HashMap<String, String>
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
fn get_job(mut state: State) -> (State, Response<Body>) {
    let get_job_params = GetJobParams::take_from(&mut state);
    let body = format!("\"ret\": \"{}\"", get_job_params.uuid);

    let response = JobStatus {

    }

    (state, response)
}
fn router() -> Router {
    build_simple_router(|route| {
        route.get("/jobs/:uuid")
            .with_path_extractor::<GetJobParams>()
            .to(get_job)
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
            .get("http://localhost:8888/jobs/foo")
            .perform()
            .unwrap();

        println!("{:?}", response.read_utf8_body().unwrap());
    }
}
