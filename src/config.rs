/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2019, Joyent, Inc.
 */

extern crate clap;

use clap::{App, AppSettings, Arg, ArgMatches, SubCommand as ClapSubCommand};
use serde::Deserialize;
use std::fs::File;
use std::io::{BufReader, Error};
use std::process;

use crate::jobs::{evacuate::EvacuateJob, Job, JobAction};
use crate::moray_client;
use crate::util;

#[derive(Deserialize, Default, Debug, Clone)]
pub struct Shard {
    pub host: String,
}

#[derive(Deserialize, Default, Debug, Clone)]
pub struct Config {
    pub sapi_url: String,
    pub domain_name: String,
    pub shards: Vec<Shard>,
    pub database_url: String,
}

impl Config {
    // TODO: there's a bug here that 1 will always be the min shard number
    pub fn min_shard_num(&self) -> u32 {
        self.shards.iter().fold(1, |res, elem| {
            let shard_num = util::shard_host2num(elem.host.as_str());

            if res < shard_num {
                return res;
            }

            shard_num
        })
    }

    pub fn max_shard_num(&self) -> u32 {
        self.shards.iter().fold(1, |res, elem| {
            let shard_num = util::shard_host2num(&elem.host);

            if res > shard_num {
                return res;
            }

            shard_num
        })
    }
}

pub enum SubCommand {
    Server, // Start the server
    Agent,
    DoJob(Box<Job>),
}

pub struct Command {
    pub config: Config,
    pub subcommand: SubCommand,
}

impl Command {
    pub fn parse_config(config_path: &str) -> Result<Config, Error> {
        let file = File::open(config_path)?;
        let reader = BufReader::new(file);
        let config: Config = serde_json::from_reader(reader)?;

        Ok(config)
    }
    pub fn new() -> Result<Command, Error> {
        let mut subcommand = SubCommand::Server;

        let matches: ArgMatches = App::new("remora")
            .version("0.1.0")
            .author("Rui Loura <rui@joyent.com>")
            .about("Remora")
            .setting(AppSettings::ArgRequiredElseHelp)
            .arg(
                Arg::with_name("server")
                    .short("s")
                    .long("server")
                    .help("Run in server mode"),
            )
            .arg(
                Arg::with_name("agent")
                    .short("a")
                    .long("agent")
                    .help("Run in agent mode"),
            )
            .arg(
                Arg::with_name("config_file")
                    .short("c")
                    .long("config_file")
                    .takes_value(true)
                    .value_name("CONFIG_FILE")
                    .required(true)
                    .help("Specify the location of the config file")
            )
            .subcommand(
                // TODO: server subcommand
                ClapSubCommand::with_name("job")
                    .about("Job Management")
                    .version("0.1.0")
                    .setting(AppSettings::ArgRequiredElseHelp)
                    // TODO:
                    // remora job create [options]
                    // remora job get <uuid>
                    // remora job list
                    .subcommand(
                        ClapSubCommand::with_name("create")
                            .about("Create a rebalancer Job")
                            .version("0.1.0")
                            .setting(AppSettings::ArgRequiredElseHelp)
                            .subcommand(
                                ClapSubCommand::with_name("evacuate")
                                    .about("run an evacuate job")
                                    .version("0.1.0")
                                    .setting(AppSettings::ArgRequiredElseHelp)
                                    .arg(
                                        Arg::with_name("from_shark")
                                            .short("s")
                                            .takes_value(true)
                                            .value_name("SHARK")
                                            .help("shark to evacuate"),
                                    )
                                    .arg(
                                        Arg::with_name("to_shark")
                                            .short("d")
                                            .takes_value(true)
                                            .value_name("SHARK")
                                            //TODO: .multiple(true)
                                            .help("shark to move object to (destination)"),
                                    )
                                    // TODO: use rust-sharkspotter
                                    .arg(
                                        Arg::with_name("object")
                                            .short("o")
                                            .takes_value(true)
                                            .multiple(true)
                                            .value_name("object_uuid")
                                            .help("individually specify object uuids"),
                                    )
                                    // TODO: Default to config file value.
                                    // In SAPI manta application metadata:
                                    // "DOMAIN_NAME"
                                    .arg(
                                        Arg::with_name("domain")
                                            .short("D")
                                            .takes_value(true)
                                            .value_name("DOMAIN_NAME")
                                            .help("Domain of Manta Deployment")
                                            .required(false)
                                    )
                                    .arg(
                                        Arg::with_name("max_objects")
                                            .short("X")
                                            .takes_value(true)
                                            .value_name("MAX_OBJECTS")
                                            .help("Limit the number of \
                                            objects evacuated.  0 for \
                                            unlimited.  Default: 10.  TESTING \
                                            ONLY.")
                                            .required(false)
                                    ),

                            ),
                    ),
            )
            .get_matches();

        let config_file = matches
            .value_of("config_file")
            .expect("Missing config file name");
        let config = Command::parse_config(config_file)?;

        if matches.is_present("server") {
            subcommand = SubCommand::Server;
        }

        if matches.is_present("agent") {
            subcommand = SubCommand::Agent;
        }

        // TODO: There must be a better way.  YAML perhaps?
        if let Some(sub_matches) = matches.subcommand_matches("job") {
            if let Some(job_matches) = sub_matches.subcommand_matches("create")
            {
                if let Some(create_matches) =
                    job_matches.subcommand_matches("evacuate")
                {
                    subcommand =
                        job_subcommand_handler(create_matches, config.clone());
                }
            }
        }

        Ok(Command { config, subcommand })
    }
}

// TODO:
// This should really be removed in favor of the following:
// 1. Command::new() handling override of domain_name from config file
// 2. Job::new() taking all args necessary to create new Job Action (e.g.
// EvacuateJob)
fn job_subcommand_handler(matches: &ArgMatches, config: Config) -> SubCommand {
    let shark_id = matches.value_of("from_shark").unwrap_or("").to_string();
    let domain_name = matches.value_of("domain").unwrap_or(&config.domain_name);
    let max: u32 = matches
        .value_of("max_objects")
        .unwrap_or("10")
        .parse()
        .unwrap();

    let max_objects = if max == 0 { None } else { Some(max) };

    let shark_id = format!("{}.{}", shark_id, domain_name);

    let from_shark =
        moray_client::get_manta_object_shark(&shark_id, domain_name)
            .expect("Error looking up manta shark");
    // TODO: This should probably be based on the Job UUID and not the pid as
    // we plan to have a server mode that will generate multiple jobs in a
    // single process.
    let db_url = format!("{}.{}", &config.database_url, process::id());
    let job_action = JobAction::Evacuate(Box::new(EvacuateJob::new(
        from_shark,
        domain_name,
        &db_url,
        max_objects,
    )));
    let job = Job::new(job_action, config);

    SubCommand::DoJob(Box::new(job))
}
