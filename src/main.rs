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

fn main() -> Result<(), Error> {
    pretty_env_logger::init();
    let command = Command::new().unwrap_or_else(|e| {
        eprintln!("Error parsing args: {}", e);
        std::process::exit(1);
    });

    match command.subcommand {
        SubCommand::Server => Ok(()),
        SubCommand::DoJob(job) => job.run(),
        SubCommand::Agent => {
            Agent::run("127.0.0.1:7878");
            Ok(())
        }
    }
}
