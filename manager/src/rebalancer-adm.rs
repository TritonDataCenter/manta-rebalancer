/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2020 Joyent, Inc.
 */

use manager::config::{Command, SubCommand};
use manager::jobs::status::{self, StatusError};
use rebalancer::error::Error;
use rebalancer::util;

fn main() -> Result<(), Error> {
    let _guard = util::init_global_logger();
    let command = Command::new().unwrap_or_else(|e| {
        eprintln!("Error parsing args: {}", e);
        std::process::exit(1);
    });

    match command.subcommand {
        SubCommand::Server => Ok(()),
        SubCommand::DoJob(_job) => {
            println!("This function is currently not allowed from the CLI. \
                Please contact the rebalancer-manager via its REST API on port \
                8888 to create a job.");
            std::process::exit(1);
        }
        SubCommand::Status(uuid) => match status::get_status(uuid) {
            Ok(status_report) => {
                for (status, count) in status_report.iter() {
                    println!("{}: {}", status, count);
                }
                Ok(())
            }
            Err(e) => {
                match e {
                    StatusError::DBExists => {
                        println!("Could not find Job UUID {}", uuid);
                    }
                    StatusError::LookupError | StatusError::Unknown => {
                        println!("Internal Lookup Error");
                    }
                }
                std::process::exit(1);
            }
        },
        SubCommand::JobList => match status::list_jobs() {
            Ok(list) => {
                let width = 20;
                println!(
                    "{:<width$}{:<width$}{:<width$}",
                    "JOB",
                    "ACTION",
                    "STATE",
                    width = width
                );
                for job in list {
                    println!(
                        "{:<width$}{:<width$}{:<width$}",
                        job.id,
                        job.action,
                        job.state,
                        width = width,
                    );
                }
                Ok(())
            }
            Err(_) => {
                println!("Internal Job List Error");
                std::process::exit(1);
            }
        },
    }
}
