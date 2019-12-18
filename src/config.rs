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
use std::io::BufReader;

use crate::error::Error;
use crate::jobs::{evacuate::EvacuateJob, Job, JobAction};
use crate::moray_client;
use crate::util;
use uuid::Uuid;

static DEFAULT_CONFIG_PATH: &str = "/var/tmp/config.json";

#[derive(Deserialize, Default, Debug, Clone)]
pub struct Shard {
    pub host: String,
}

#[derive(Deserialize, Default, Debug, Clone)]
pub struct Config {
    pub domain_name: String,
    pub shards: Vec<Shard>,
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

    pub fn parse_config(config_path: Option<&str>) -> Result<Config, Error> {
        let config_path = config_path.unwrap_or(DEFAULT_CONFIG_PATH);
        let file = File::open(config_path)?;
        let reader = BufReader::new(file);
        let config: Config = serde_json::from_reader(reader)?;

        Ok(config)
    }
}

pub enum SubCommand {
    Server, // Start the server
    DoJob(Box<Job>),
    Status(Uuid),
    JobList,
}

pub struct Command {
    pub config: Config,
    pub subcommand: SubCommand,
}

impl Command {
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
                    .subcommand(
                        ClapSubCommand::with_name("list")
                            .about("Get list of rebalancer jobs")
                            .version("0.1.0")
                    )
                    .subcommand(
                        ClapSubCommand::with_name("status")
                            .about("Get the status of a rebalancer job")
                            .version("0.1.0")
                            .setting(AppSettings::ArgRequiredElseHelp)
                            .arg(Arg::with_name("JOB_ID")
                                .help("UUID of job")
                                .required(true)
                                .index(1)
                            )
                    )
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
        let config = Config::parse_config(Some(config_file))?;

        if matches.is_present("server") {
            subcommand = SubCommand::Server;
        }

        // TODO: There must be a better way.  YAML perhaps?
        if let Some(sub_matches) = matches.subcommand_matches("job") {
            // Job
            if let Some(create_matches) =
                sub_matches.subcommand_matches("create")
            {
                // Job Create
                if let Some(evacuate_matches) =
                    create_matches.subcommand_matches("evacuate")
                {
                    subcommand = job_create_subcommand_handler(
                        evacuate_matches,
                        config.clone(),
                    )?;
                }
            } else if let Some(status_matches) =
                sub_matches.subcommand_matches("status")
            {
                let uuid: Uuid =
                    Uuid::parse_str(status_matches.value_of("JOB_ID").unwrap())
                        .unwrap_or_else(|e| {
                            println!(
                                "Error parsing Job ID: {}\nJOB_ID must \
                                 be a valid v4 UUID",
                                e
                            );
                            std::process::exit(1);
                        });
                subcommand = SubCommand::Status(uuid);
            } else if sub_matches.subcommand_matches("list").is_some() {
                subcommand = SubCommand::JobList;
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
fn job_create_subcommand_handler(
    matches: &ArgMatches,
    config: Config,
) -> Result<SubCommand, Error> {
    let shark_id = matches.value_of("from_shark").unwrap_or("").to_string();
    let domain_name = matches
        .value_of("domain")
        .unwrap_or(&config.domain_name)
        .to_owned();
    let max: u32 = matches.value_of("max_objects").unwrap_or("10").parse()?;

    let max_objects = if max == 0 { None } else { Some(max) };
    let shark_id = format!("{}.{}", shark_id, domain_name);
    let from_shark =
        moray_client::get_manta_object_shark(&shark_id, &domain_name)
            .map_err(Error::from)?;

    let mut job = Job::new(config);
    let job_action = JobAction::Evacuate(Box::new(EvacuateJob::new(
        from_shark,
        &domain_name,
        &job.get_id().to_string(),
        max_objects,
    )));
    job.add_action(job_action);

    Ok(SubCommand::DoJob(Box::new(job)))
}
