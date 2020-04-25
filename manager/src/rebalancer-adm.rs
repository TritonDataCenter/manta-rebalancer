/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2020 Joyent, Inc.
 */

//use manager::config::{Command, SubCommand};
//use manager::jobs::status::{self, StatusError};
use serde_json::Value::{self, Object};
//use serde_json::Value::Object;
use tabular::{Row, Table};
//use rebalancer::util;
#[macro_use]
extern crate rebalancer;

use clap::{App, Arg, ArgMatches};
use reqwest::{self, StatusCode};

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

fn manager_get_common(url: &str) -> std::result::Result<String, String> {
    let mut response = match reqwest::get(url) {
        Ok(resp) => resp,
        Err(e) => {
            let msg = format!("Request failed: {}", &e);
            error!("{}", &msg);
            return Err(msg);
        }
    };

    match response.text() {
        Ok(p) => Ok(p),
        Err(e) => {
            let msg = format!("Failed to parse response body: {}", &e);
            error!("{}", &msg);
            return Err(msg);
        }
    }
}

fn job_list(matches: &ArgMatches) {
    let payload = match manager_get_common(JOBS_URL) {
        Ok(p) => p,
        Err(e) => return,
    };

    let result = match serde_json::from_str(&payload) {
        Ok(v) => v,
        Err(e) => {
            let msg = format!("Failed to deserialize response: {}", &e);
            error!("{}", &msg);
            return;
        }
    };

    display_result(result, matches);
}

fn job_get(matches: &ArgMatches) {
    let uuid = match matches.value_of("uuid") {
        Some(u) => u,
        None => return,
    };

    let url = format!("{}/{}", JOBS_URL, uuid);

    let payload = match manager_get_common(&url) {
        Ok(p) => p,
        Err(e) => return, // Err(e);
    };

    let result = match serde_json::from_str(&payload) {
        Ok(v) => {
            let mut vec = Vec::new();
            vec.push(v);
            vec
        }
        Err(e) => {
            let msg = format!("Failed to deserialize response: {}", &e);
            error!("{}", &msg);
            return; // Err(msg)
        }
    };

    display_result(result, matches);
}

fn process_subcmd_job(matches: &ArgMatches) {
    if matches.is_present("list") {
        job_list(matches);
    } else if matches.is_present("get") {
        job_get(matches);
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
                    Arg::with_name("json")
                        .short("j")
                        .help("Prints information in json format"),
                ),
        )
        .get_matches();

    if let Some(ref matches) = matches.subcommand_matches("job") {
        process_subcmd_job(matches);
    }
}
