/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2020 Joyent, Inc.
 */

use clap::{App, AppSettings, Arg, ArgMatches};
use manager::jobs::evacuate::ObjectSource;
use manager::jobs::{EvacuateJobPayload, JobPayload};
use reqwest;
use serde_json::Value;
use std::result::Result;

pub static JOBS_URL: &str = "http://localhost/jobs";

// Common function used in order to get a list of jobs, or get specific job
// information.  The contents of the response are evaluated and printed by
// the caller.
fn get_common(url: &str) -> Result<(), String> {
    let mut response = match reqwest::get(url) {
        Ok(resp) => resp,
        Err(e) => return Err(format!("Request failed: {}", &e)),
    };

    // Flag failure if we get a status code of anything other than 200.
    if !response.status().is_success() {
        return Err(format!("Failed to post job: {}", response.status()));
    }

    let v: Value = match response.json() {
        Ok(v) => v,
        Err(e) => return Err(format!("Failed to parse response body: {}", &e)),
    };

    let result = match serde_json::to_string_pretty(&v) {
        Ok(s) => s,
        Err(e) => return Err(format!("Failed to deserialize: {}", &e)),
    };

    println!("{}", result);
    Ok(())
}

// Given a spcific job id, send a request to the manager for more detailed
// information.
fn job_get(matches: &ArgMatches) -> Result<(), String> {
    let uuid = matches.value_of("uuid").unwrap();
    let url = format!("{}/{}", JOBS_URL, uuid);

    get_common(&url)
}

fn job_create(matches: &ArgMatches) -> Result<(), String> {
    match matches.subcommand() {
        ("evacuate", Some(evac_matches)) => job_create_evacuate(evac_matches),
        _ => unreachable!(),
    }
}

// Post an evacuate job to the manager.  In the future, there may be more than
// one kind of job that is supported, at which point, we should probably break
// out the evacate-specific logic in to another subroutine that this function
// calls.
fn job_create_evacuate(matches: &ArgMatches) -> Result<(), String> {
    // Get the storage id from the args.  Clap ensures that this argument is
    // supplied to us before we even reach this point.
    let shark = matches.value_of("shark").unwrap();

    // Max objects is an optional argument.
    let max_objects = match matches.value_of("max_objects") {
        None => None,
        Some(m) => match m.parse::<u64>() {
            Ok(n) => Some(n),
            Err(e) => {
                return Err(format!(
                    "Numeric value required for max_objects: {}",
                    e
                ));
            }
        },
    };

    let source = match matches.value_of("file_source") {
        None => ObjectSource::default(),
        Some(path) => ObjectSource::File(path.to_string()),
    };

    // Form the payload of the request.
    let job_payload = JobPayload::Evacuate(EvacuateJobPayload {
        from_shark: shark.to_owned(),
        source,
        max_objects,
    });

    // Serialize it.
    let payload: String =
        serde_json::to_string(&job_payload).expect("Serialize job payload");

    // Send the request.
    let client = reqwest::Client::new();
    let mut response = match client.post(JOBS_URL).body(payload).send() {
        Ok(resp) => resp,
        Err(e) => return Err(format!("Failed to post job: {}", &e)),
    };

    // Flag failure if we get a status code of anything other than 200.
    if !response.status().is_success() {
        return Err(format!("Server response: {}", response.status()));
    }

    // Parse out the job uuid from the response payload.
    let job_uuid = match response.text() {
        Ok(j) => j,
        Err(e) => return Err(format!("Failed to parse response: {}", e)),
    };

    println!("{}", job_uuid);
    Ok(())
}

// The `job' subcommand currently requires one of three different primary
// arguments.  While there are other arguments that might accompany the
// ones listed below, those are parsed separately depending on which of
// the pimary arguments are supplied.
fn process_subcmd_job(job_matches: &ArgMatches) -> Result<(), String> {
    match job_matches.subcommand() {
        ("get", Some(get_matches)) => job_get(get_matches),
        ("list", Some(_)) => get_common(JOBS_URL),
        ("create", Some(create_matches)) => job_create(create_matches),
        _ => unreachable!(),
    }
}

fn main() -> Result<(), String> {
    let evacuate_subcommand = App::new("evacuate")
        .about("Create an evacuate job")
        .arg(
            Arg::with_name("shark")
                .short("s")
                .long("shark")
                .takes_value(true)
                .required(true)
                .help("Specifies a shark on which to run a job"),
        )
        .arg(
            Arg::with_name("file_source")
                .short("F")
                .long("file_source")
                .takes_value(true)
                .help("Specifies a shark on which to run a job"),
        )
        .arg(
            Arg::with_name("max_objects")
                .short("m")
                .long("max_objects")
                .takes_value(true)
                .help("Maximum number of objects allowed in the job"),
        );

    let matches = App::new("rebalancer-adm")
        .setting(AppSettings::SubcommandRequiredElseHelp)
        .version("0.1.0")
        .about("Rebalancer client utility")
        .subcommand(
            App::new("job")
                .setting(AppSettings::SubcommandRequiredElseHelp)
                .about("Job operations")
                // Get subcommand
                .subcommand(
                    App::new("get")
                        .about("Get information on a specific job")
                        .arg(
                            Arg::with_name("uuid")
                                .takes_value(true)
                                .required(true)
                                .help("Uuid of a job"),
                        ),
                )
                // List subcommand
                .subcommand(
                    App::new("list").about("List all known rebalancer jobs"),
                )
                // Create subcommand
                .subcommand(
                    App::new("create")
                        .about("Create a rebalancer job")
                        .setting(AppSettings::SubcommandRequiredElseHelp)
                        // Create evacuate job
                        .subcommand(evacuate_subcommand),
                ),
        )
        .get_matches();

    match matches.subcommand() {
        ("job", Some(job_matches)) => process_subcmd_job(job_matches),
        _ => unreachable!(),
    }
}

#[cfg(test)]
mod rebalancer_adm_tests {
    use assert_cli;
    use indoc::indoc;

    #[test]
    fn no_params() {
        let version = env!("CARGO_PKG_VERSION");
        let usage = indoc!(
            "
            Rebalancer client utility

            USAGE:
                rebalancer-adm <SUBCOMMAND>

            FLAGS:
                -h, --help       Prints help information
                -V, --version    Prints version information

            SUBCOMMANDS:
                help    Prints this message or the help of the given \
                subcommand(s)
                job     Job operations
            "
        );

        assert_cli::Assert::cargo_binary("rebalancer-adm")
            .fails()
            .and()
            .stderr()
            .contains(version)
            .and()
            .stderr()
            .contains(usage)
            .unwrap();
    }

    #[test]
    fn job_list_extra_params() {
        let err_msg = indoc!(
            "
            error: Found argument 'extra' which wasn't expected, or isn't \
            valid in this context

            USAGE:
                rebalancer-adm job list
            "
        );

        assert_cli::Assert::cargo_binary("rebalancer-adm")
            .with_args(&["job", "list", "extra"])
            .fails()
            .and()
            .stderr()
            .contains(err_msg)
            .unwrap();
    }

    #[test]
    fn job_get_no_params() {
        let err_msg = indoc!(
            "
            error: The following required arguments were not provided:
                <uuid>

            USAGE:
                rebalancer-adm job get <uuid>
            "
        );

        assert_cli::Assert::cargo_binary("rebalancer-adm")
            .with_args(&["job", "get"])
            .fails()
            .and()
            .stderr()
            .contains(err_msg)
            .unwrap();
    }

    #[test]
    fn job_create_no_params() {
        let err_msg = indoc!(
            "
            Create a rebalancer job

            USAGE:
                rebalancer-adm job create <SUBCOMMAND>

            FLAGS:
                -h, --help       Prints help information
                -V, --version    Prints version information

            SUBCOMMANDS:
                evacuate    Create an evacuate job
                help        Prints this message or the help of the given \
                subcommand(s)
            "
        );

        assert_cli::Assert::cargo_binary("rebalancer-adm")
            .with_args(&["job", "create"])
            .fails()
            .and()
            .stderr()
            .contains(err_msg)
            .unwrap();
    }

    #[test]
    fn job_evacuate_no_params() {
        let err_msg = indoc!(
            "
            error: The following required arguments were not provided:
                --shark <shark>

            USAGE:
                rebalancer-adm job create evacuate [OPTIONS] --shark <shark>
            "
        );

        assert_cli::Assert::cargo_binary("rebalancer-adm")
            .with_args(&["job", "create", "evacuate"])
            .fails()
            .and()
            .stderr()
            .contains(err_msg)
            .unwrap();
    }
}
