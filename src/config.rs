// Copyright 2019 Joyent, Inc.

extern crate clap;

use clap::{App, AppSettings, Arg, ArgMatches, SubCommand as ClapSubCommand};
use serde::Deserialize;
use std::fs::File;
use std::io::{BufReader, Error};

use crate::job::{EvacuateJob, Job, JobAction};
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
}

impl Config {
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
#[derive(Debug)]
pub enum SubCommand {
    Server, // Start the server
    Agent,
    DoJob(Box<Job>),
}

#[derive(Debug)]
pub struct Command {
    pub config: Config,
    pub subcommand: SubCommand,
}

impl Command {
    pub fn parse_config() -> Result<Config, Error> {
        let file = File::open("./target/debug/config.json")?;
        let reader = BufReader::new(file);
        println!("parsing config");
        let config: Config = serde_json::from_reader(reader)?;

        Ok(config)
    }
    pub fn new() -> Result<Command, Error> {
        let mut subcommand = SubCommand::Server;
        let config = Command::parse_config()?;

        let domain_name = String::from(config.domain_name.as_str());

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
                                    // TODO: use picker
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
                                            .default_value(domain_name
                                                .as_str())
                                    ),

                            ),
                    ),
            )
            .get_matches();

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

fn job_subcommand_handler(matches: &ArgMatches, config: Config) -> SubCommand {
    let shark_id = matches.value_of("from_shark").unwrap_or("").to_string();
    let domain_name = matches.value_of("domain").unwrap_or(&config.domain_name);

    let from_shark = format!("{}.{}", shark_id, domain_name);
    let job_action = JobAction::Evacuate(EvacuateJob::new(from_shark));
    let job = Job::new(job_action, config);

    SubCommand::DoJob(Box::new(job))
}
