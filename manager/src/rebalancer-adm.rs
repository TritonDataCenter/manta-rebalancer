/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2020 Joyent, Inc.
 */

use std::result::Result;
//use manager::config::{Command, SubCommand};
//use manager::jobs::status::{self, StatusError};
use manager::jobs::{EvacuateJobPayload, JobPayload};
use serde_json::Value::{self, Object};
//use serde_json::Value::Object;
use tabular::{Row, Table};
//use rebalancer::util;
#[macro_use]
extern crate rebalancer;

use clap::{App, Arg, ArgMatches};
use reqwest;

pub static JOBS_URL: &str = "http://localhost/jobs";

fn display_result(values: Vec<Value>, matches: &ArgMatches) {
    if matches.is_present("json") {
        match serde_json::to_string_pretty(&values) {
            Ok(v) => println!("{}", v),
            Err(e) => println!("Failed to serialize value: {}", &e),
        };
        return;
    }

    let entry = &values[0];
    let mut header = Row::new();
    let mut fmtstr = "".to_owned();

    if let Object(map) = entry {
        for (key, _val) in map {
            header.add_cell(key);
            fmtstr.push_str("{:<}   ");
        }
    }

    let mut table = Table::new(&fmtstr);
    table.add_row(header);

    for v in values.iter() {
        if let Object(map) = v {
            let mut row = Row::new();
            for (_key, val) in map {
                let mut element = format!("{}", val);
                element.retain(|x| !['"'].contains(&x));
                row.add_cell(&element);
            }
            table.add_row(row);
        }
    }
    print!("{}", table);
}

fn manager_get_common(url: &str) -> Result<String, String> {
    let mut response = match reqwest::get(url) {
        Ok(resp) => resp,
        Err(e) => return Err(format!("Request failed: {}", &e)),
    };

    match response.text() {
        Ok(p) => Ok(p),
        Err(e) => return Err(format!("Failed to parse response body: {}", &e)),
    }
}

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

fn job_post(matches: &ArgMatches) -> Result<(), String> {
    let shark = matches.value_of("shark").unwrap();
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

    let job_payload = JobPayload::Evacuate(EvacuateJobPayload {
        from_shark: shark.to_owned(),
        max_objects: max_objects,
    });

    let payload: String =
        serde_json::to_string(&job_payload).expect("Serialize job payload");

    let client = reqwest::Client::new();
    let mut response = match client.post(JOBS_URL).body(payload).send() {
        Ok(resp) => resp,
        Err(e) => return Err(format!("Job post failed: {}", &e)),
    };

    if !response.status().is_success() {
        return Err(format!("Error creating job: {}", response.status()));
    }

    let job_uuid = match response.text() {
        Ok(j) => j,
        Err(e) => return Err(format!("Error: {}", e)),
    };

    println!("{}", job_uuid);
    Ok(())
}

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
