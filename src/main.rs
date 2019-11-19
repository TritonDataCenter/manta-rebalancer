/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2019, Joyent, Inc.
 */

use remora::agent::Agent;
use remora::config::{Command, SubCommand};
use remora::error::Error;
use remora::jobs::status::{self, StatusError};
use remora::util;

fn main() -> Result<(), Error> {
    let _guard = util::init_global_logger();
    let command = Command::new().unwrap_or_else(|e| {
        eprintln!("Error parsing args: {}", e);
        std::process::exit(1);
    });

    match command.subcommand {
        SubCommand::Server => Ok(()),
        SubCommand::DoJob(job) => job.run(),
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
                for job in list {
                    println!("{}", job);
                }
                Ok(())
            }
            Err(_) => {
                println!("Internal Job List Error");
                std::process::exit(1);
            }
        },
        SubCommand::Agent => {
            // We should only be using 0.0.0.0 (INADDR_ANY) temporarily.  In
            // production we will be supply an ip address that is obtained from
            // the config file that we process which will dictate the network
            // on which to listen.  It is worth mentioning that this will likely
            // also be the case for the agent port.
            Agent::run("0.0.0.0:7878");
            Ok(())
        }
    }
}
