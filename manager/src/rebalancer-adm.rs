/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2020 Joyent, Inc.
 */

use std::result::Result;
use manager::jobs::{EvacuateJobPayload, JobPayload};
use serde_json::Value::{self, Object};
use tabular::{Row, Table};

#[macro_use]
extern crate rebalancer;

use clap::{App, Arg, ArgMatches};
use reqwest;

pub static JOBS_URL: &str = "http://localhost/jobs";

// Display the result.  This function takes a Vector of Values and prints its
// contents either in tabular format (the current default) or in JSON, depending
// on what the user specified in their arguments.
fn display_result(values: Vec<Value>, matches: &ArgMatches) {
    // The user specified JSON output.  Print it and return.
    if matches.is_present("json") {
        match serde_json::to_string_pretty(&values) {
            Ok(v) => println!("{}", v),
            Err(e) => println!("Failed to serialize value: {}", &e),
        };
        return;
    }

    // Get the first value from our vector.  We examine the keys of the first
    // object.  This information will tell us how many cells to have per row.
    let entry = &values[0];
    let mut header = Row::new();
    let mut fmtstr = "".to_owned();

    if let Object(map) = entry {
        for (key, _val) in map {
            // Use the key name in the map to be the column label for the table.
            header.add_cell(key);

            // Push another argument on to the format string. {:<} means that
            // the value in the cell will be left-justified.
            fmtstr.push_str("{:<}   ");
        }
    }

    // Create the table.  The first row is our header of the table.
    let mut table = Table::new(&fmtstr);
    table.add_row(header);

    // Now iterate through our vector of values.  Each value is a type of map,
    // where it contains a list of keys and values.  The values of a single
    // entry form a single row in the table.  The loop below populates each
    // cell in the row, one at a time.
    for v in values.iter() {
        if let Object(map) = v {
            let mut row = Row::new();

            // Iterate through our value like a map.
            for (_key, val) in map {
                let mut element = format!("{}", val);
                element.retain(|x| !['"'].contains(&x));
                row.add_cell(&element);
            }

            // Add the row to the table now that it is fully populated.
            table.add_row(row);
        }
    }
    print!("{}", table);
}

// Common function used in order to get a list of jobs, or get specific job
// information.  The contents of the response are evaluated and printed by
// the caller.
fn manager_get_common(url: &str) -> Result<String, String> {
    let mut response = match reqwest::get(url) {
        Ok(resp) => resp,
        Err(e) => return Err(format!("Request failed: {}", &e)),
    };

    // Flag failure if we get a status code of anything other than 200.
    if !response.status().is_success() {
        return Err(format!("Error creating job: {}", response.status()));
    }

    match response.text() {
        Ok(p) => Ok(p),
        Err(e) => return Err(format!("Failed to parse response body: {}", &e)),
    }
}

// Send a request to the manager asking for the uuids of all known jobs.
fn job_list(matches: &ArgMatches) -> Result<(), String> {
    let payload = match manager_get_common(JOBS_URL) {
        Ok(p) => p,
        Err(e) => return Err(e),
    };

    let result = match serde_json::from_str(&payload) {
        Ok(v) => v,
        Err(e) => {
            return Err(format!("Failed to deserialize response: {}", &e))
        }
    };

    display_result(result, matches);
    Ok(())
}

// Given a spcific job id, send a request to the manager for more detailed
// information.
fn job_get(matches: &ArgMatches) -> Result<(), String> {
    let uuid = matches.value_of("uuid").unwrap();
    let url = format!("{}/{}", JOBS_URL, uuid);

    let payload = match manager_get_common(&url) {
        Ok(p) => p,
        Err(e) => return Err(e),
    };

    let result = match serde_json::from_str(&payload) {
        Ok(v) => {
            let mut vec = Vec::new();
            vec.push(v);
            vec
        }
        Err(e) => {
            return Err(format!("Failed to deserialize response: {}", &e))
        }
    };

    display_result(result, matches);
    Ok(())
}

// Post an evacuate job to the manager.  In the future, there may be more than
// one kind of job that is supported, at which point, we should probably break
// out the evacate-specific logic in to another subroutine that this function
// calls.
fn job_post(matches: &ArgMatches) -> Result<(), String> {
    // Get the storage id from the args.  Clap ensures that this argument is
    // supplied to us before we even reach this point.
    let shark = matches.value_of("shark").unwrap();

    // Max objects is an optional argument.
    let max_objects = match matches.value_of("max_objects") {
        None => None,
        Some(m) => match m.parse::<u32>() {
            Ok(n) => Some(n),
            Err(e) => {
                return Err(format!(
                    "Numeric value required for max_objects: {}",
                    e
                ));
            }
        },
    };

    // Form the payload of the request.
    let job_payload = JobPayload::Evacuate(EvacuateJobPayload {
        from_shark: shark.to_owned(),
        max_objects: max_objects,
    });

    // Serialize it.
    let payload: String =
        serde_json::to_string(&job_payload).expect("Serialize job payload");

    // Send the request.
    let client = reqwest::Client::new();
    let mut response = match client.post(JOBS_URL).body(payload).send() {
        Ok(resp) => resp,
        Err(e) => return Err(format!("Job post failed: {}", &e)),
    };

    // Flag failure if we get a status code of anything other than 200.
    if !response.status().is_success() {
        return Err(format!("Error creating job: {}", response.status()));
    }

    // Parse out the job uuid from the response payload.
    let job_uuid = match response.text() {
        Ok(j) => j,
        Err(e) => return Err(format!("Error: {}", e)),
    };

    println!("{}", job_uuid);
    Ok(())
}

// The `job' subcommand currently requires one of three different primary
// arguments.  While there are other arguments that might accompany the
// ones listed below, those are parsed separately depending on which of
// the pimary arguments are supplied.
fn process_subcmd_job(matches: &ArgMatches) -> Result<(), String> {
    if matches.is_present("list") {
        return job_list(matches);
    } else if matches.is_present("get") {
        return job_get(matches);
    } else if matches.is_present("evacuate") {
        return job_post(matches);
    } else {
        return Err("Invalid sub-command".to_string());
    }
}

fn main() {
    let matches = App::new("rebalancer-adm")
        .version("0.1.0")
        .about("Rebalancer client utility")
        .subcommand(
            App::new("job")
                .about("Job operations")
                .arg(
                    Arg::with_name("list")
                        .short("l")
                        .long("list")
                        .help("List uuids and status of all known jobs"),
                )
                .arg(
                    Arg::with_name("uuid")
                        .short("u")
                        .long("uuid")
                        .takes_value(true)
                        .help("List details of a specific job"),
                )
                .arg(
                    Arg::with_name("get")
                        .short("g")
                        .long("get")
                        .help("List details of a specific job")
                        .requires("uuid")
                        .conflicts_with("list"),
                )
                .arg(
                    Arg::with_name("shark")
                        .short("s")
                        .long("shark")
                        .takes_value(true)
                        .help("Specifies a shark on which to run a job"),
                )
                .arg(
                    Arg::with_name("max_objects")
                        .short("m")
                        .long("max_objects")
                        .help("Maximum number of objects allowed in the job"),
                )
                .arg(
                    Arg::with_name("evacuate")
                        .short("e")
                        .long("evacuate")
                        .help("Create an evacuate job")
                        .requires("shark")
                        .conflicts_with("uuid")
                        .conflicts_with("get")
                        .conflicts_with("list"),
                )
                .arg(
                    Arg::with_name("json")
                        .short("j")
                        .long("json")
                        .help("Prints information in json format"),
                ),
        )
        .get_matches();

    let mut result = Ok(());

    if let Some(ref matches) = matches.subcommand_matches("job") {
        result = process_subcmd_job(matches);
    }

    std::process::exit(match result {
        Ok(_) => 0,
        Err(e) => {
            error!("{}", e);
            1
        }
    });
}
