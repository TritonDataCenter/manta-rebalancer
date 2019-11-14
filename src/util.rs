/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2019, Joyent, Inc.
 */

use std::io;
use std::sync::Mutex;
use std::thread;

use clap::{crate_name, crate_version};
use slog::{o, Drain, Logger};

pub fn create_bunyan_logger<W>(io: W) -> Logger
where
    W: io::Write + std::marker::Send + 'static,
{
    Logger::root(
        Mutex::new(slog_bunyan::with_name(crate_name!(), io).build()).fuse(),
        o!("build-id" => crate_version!()),
    )
}

pub fn init_global_logger() -> slog_scope::GlobalLoggerGuard {
    let log = create_bunyan_logger(std::io::stdout());
    slog_scope::set_global_logger(log)
}

pub fn get_thread_name() -> String {
    let cn = crate_name!().to_owned();

    if thread::current().name().is_none() {
        return cn;
    }

    // Prepend the crate name to the thread name.
    format!("{} {}", &cn, thread::current().name().unwrap())
}

#[macro_export]
macro_rules! log_impl(
    ($lvl:expr, $($args:tt)+) => {
        let m = format!($($args)+);
        let stmt = format!("{}: {}", $crate::util::get_thread_name(), m);
        slog::slog_log!(slog_scope::logger(), $lvl, "", "{}", stmt)
    };
);

#[macro_export]
macro_rules! info(
    ($($args:tt)*) => {
        log_impl!(slog::Level::Info, $($args)*)
    };
);

#[macro_export]
macro_rules! error(
    ($($args:tt)*) => {
        log_impl!(slog::Level::Error, $($args)*)
    };
);

#[macro_export]
macro_rules! warn(
    ($($args:tt)*) => {
        log_impl!(slog::Level::Warning, $($args)*)
    };
);

#[macro_export]
macro_rules! trace(
    ($($args:tt)*) => {
        log_impl!(slog::Level::Trace, $($args)*)
    };
);

#[macro_export]
macro_rules! debug(
    ($($args:tt)*) => {
        log_impl!(slog::Level::Debug, $($args)*)
    };
);

#[macro_export]
macro_rules! crit(
    ($($args:tt)*) => {
        log_impl!(slog::Level::Critical, $($args)*)
    };
);

pub fn shard_host2num(shard_host: &str) -> u32 {
    let shard_split: Vec<&str> = shard_host.split('.').collect();
    shard_split[0].parse().unwrap()
}

#[cfg(test)]
pub mod test {
    use crate::agent::Assignment;
    use crate::jobs::Task;
    use gotham::test::TestServer;
    use reqwest::StatusCode;

    // Utility that actually forms the request, sends it off to the test
    // server and verifies that it was received as intended.  Upon success,
    // return the uuid of the assignment which we will use to monitor progress.
    pub fn send_assignment_impl(
        tasks: &Vec<Task>,
        id: &str,
        test_server: &TestServer,
        status: StatusCode,
    ) {
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

    // Send a request to get the latest information on an assignment.  This
    // information is used by the test automation to determine how far along
    // the agent is in processing the assignment.  During testing, this will
    // likely be called repeatedly for a particular assignment until it is
    // observed that the number of tasks completed is equal to the total number
    // of tasks in the assignment.
    pub fn get_progress(uuid: &str, test_server: &TestServer) -> Assignment {
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
}
