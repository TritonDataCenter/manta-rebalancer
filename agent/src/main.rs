/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2020 Joyent, Inc.
 */

extern crate rebalancer;

use std::env;

use rebalancer::libagent::Agent;
use rebalancer::util;

fn usage() {
    util::print_version();
    println!("Usage:");
    println!(" The options are:");
    println!("  -h, --help       Display this information");
    println!("  -V, --version    Display the program's version number");
}

fn main() {
    let _guard = util::init_global_logger();
    let args: Vec<String> = env::args().collect();
    let len = args.len();

    #[allow(clippy::never_loop)]
    for i in 1..len {
        match args[i].as_ref() {
            "-h" | "--help" => {
                usage();
                return;
            }
            "-V" | "--version" => {
                util::print_version();
                return;
            }
            _ => {
                println!("{:?}: illegal option -- {:?}", args[0], args[i]);
                usage();
                return;
            }
        }
    }

    // We should only be using 0.0.0.0 (INADDR_ANY) temporarily.  In production
    // we will be supply an ip address that is obtained from the config file
    // that we process which will dictate the network on which to listen.  It is
    // worth mentioning that this will likely also be the case for the agent
    // port.
    Agent::run("0.0.0.0:7878");
}

#[cfg(test)]
pub mod agenttests {
    use gotham::handler::assets::FileOptions;
    use gotham::router::builder::{
        build_simple_router, DefineSingleRoute, DrawRoutes,
    };
    use gotham::test::TestServer;
    use joyent_rust_utils::file::calculate_md5;
    use lazy_static::lazy_static;
    use libmanta::moray::MantaObjectShark;
    use rebalancer::agent_test_util::{get_progress, send_assignment_impl};
    use rebalancer::common::{ObjectSkippedReason, Task, TaskStatus};
    use rebalancer::libagent::{
        process_task, router, AgentAssignmentState, Assignment,
    };
    use rebalancer::util;
    use reqwest::StatusCode;
    use std::path::Path;
    use std::sync::Mutex;
    use std::{mem, thread, time};
    use uuid::Uuid;
    use walkdir::WalkDir;

    static MANTA_SRC_DIR: &str = "testfiles";

    lazy_static! {
        static ref INITIALIZED: Mutex<bool> = Mutex::new(false);
        static ref TEST_SERVER: Mutex<TestServer> =
            Mutex::new(TestServer::new(router(process_task)).unwrap());
    }

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
    fn unit_test_init() {
        let mut init = INITIALIZED.lock().unwrap();
        if *init {
            return;
        }

        let addr = "127.0.0.1:8080";
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

        thread::spawn(move || {
            let _guard = util::init_global_logger();
            gotham::start(addr, router);
        });
        *init = true;
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
    fn send_assignment(tasks: &Vec<Task>) -> String {
        // Generate a uuid to accompany the assignment that we are about to
        // send to the agent.
        let uuid = Uuid::new_v4().to_hyphenated().to_string();
        send_assignment_impl(
            tasks,
            &uuid,
            &TEST_SERVER.lock().unwrap(),
            StatusCode::OK,
        );
        uuid
    }

    // Poll the server indefinitely on the status of a given assignment until
    // it is complete.  Currently, it's not clear if there should be an
    // expectation on how long an assignment should take to complete, especially
    // in a test scenario.  If the agent is inoperable due to being wedged, a
    // request will timeout causing a panic for a given test case anyway.  For
    // that reason, it is probably reasonable to allow this function to loop
    // indefinitely with the assumption that the agent is not hung.
    fn monitor_progress(uuid: &str) -> Assignment {
        loop {
            let assignment = get_progress(uuid, &TEST_SERVER.lock().unwrap());
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
    fn monitor_assignment(uuid: &str, expected: TaskStatus) {
        // Wait for the assignment to complete.
        let assignment = monitor_progress(&uuid);
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

    fn create_assignment(path: &str) -> Vec<Task> {
        let mut tasks = Vec::new();

        for entry in WalkDir::new(path)
            .min_depth(1)
            .follow_links(false)
            .into_iter()
            .filter_map(|e| e.ok())
        {
            let p = entry.path();
            let task = object_to_task(&p);
            println!("{:#?}", &task);
            tasks.push(task);
        }

        tasks
    }

    fn object_to_task(path: &Path) -> Task {
        Task {
            object_id: path.file_name().unwrap().to_str().unwrap().to_owned(),
            owner: "rebalancer".to_owned(),
            md5sum: calculate_md5(path.to_str().unwrap()),
            source: MantaObjectShark {
                datacenter: "dc".to_owned(),
                manta_storage_id: "localhost:8080".to_owned(),
            },
            status: TaskStatus::Pending,
        }
    }

    // Test name:    Download
    // Description:  Download a healthy file from a storage node that the agent
    //               does not already have.
    // Expected:     The operation should be a success.  Specifically,
    //               TaskStatus for any/all tasks as part of this assignment
    //               should appear as "Complete".
    #[test]
    fn download() {
        unit_test_init();
        let assignment = create_assignment(MANTA_SRC_DIR);
        let uuid = send_assignment(&assignment);
        monitor_assignment(&uuid, TaskStatus::Complete);
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
        // Download a file once.
        unit_test_init();
        let assignment = create_assignment(MANTA_SRC_DIR);
        let uuid = send_assignment(&assignment);
        monitor_assignment(&uuid, TaskStatus::Complete);

        // Send the exact same assignment again.  Note: Even though the contents
        // of this assignment are identical to the previous one, it will be
        // assigned a different uuid so that the agent does not automatically
        // reject it.  The utility function `send_assignment()' generates a
        // new random uuid on the callers behalf each time that it is called,
        // which is why we have assurance that there will not be a uuid
        // collision, resulting in a rejection.
        let uuid = send_assignment(&assignment);
        monitor_assignment(&uuid, TaskStatus::Complete);
    }

    // Test name:   Client Error.
    // Description: Attempt to download an object from a storage node where
    //              the object does not reside will cause a client error.
    // Expected:    TaskStatus for all tasks in the assignment should appear
    //              as "Failed(HTTPStatusCode(NotFound))".
    #[test]
    fn object_not_found() {
        unit_test_init();
        let mut assignment = create_assignment(MANTA_SRC_DIR);

        // Rename the object id to something that we know is not on the storage
        // server.  In this case, a file by the name of "abc".
        assignment[0].object_id = "abc".to_string();
        let uuid = send_assignment(&assignment);
        monitor_assignment(
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
        unit_test_init();
        let mut assignment = create_assignment(MANTA_SRC_DIR);

        // Scribble on the checksum information for the object.  This ensures
        // that it will fail at the end, even though the agent calculates it
        // correctly.
        assignment[0].md5sum = "abc".to_string();
        let uuid = send_assignment(&assignment);
        monitor_assignment(
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
        // Download a file once.
        unit_test_init();
        let assignment = create_assignment(MANTA_SRC_DIR);
        let uuid = send_assignment(&assignment);
        monitor_assignment(&uuid, TaskStatus::Complete);

        // Send the exact same assignment again and send it, although this time,
        // we will reuse our first uuid. We expect to receive a status code of
        // StatusCode::CONFLICT (409) from the server this time.
        send_assignment_impl(
            &assignment,
            &uuid,
            &TEST_SERVER.lock().unwrap(),
            StatusCode::CONFLICT,
        );
    }
}
