/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2019, Joyent, Inc.
 */
use std::env;

use remora::agent::Agent;
use remora::config;
use remora::util;

fn usage() {
    config::print_version();
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
                config::print_version();
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
