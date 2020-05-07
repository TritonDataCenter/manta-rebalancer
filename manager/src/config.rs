/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2020 Joyent, Inc.
 */

extern crate clap;

use std::fs::File;
use std::io::BufReader;
use std::sync::{Arc, Barrier, Mutex};

use clap::{App, AppSettings, Arg, ArgMatches, SubCommand as ClapSubCommand};
use crossbeam_channel::TrySendError;
use serde::Deserialize;
use signal_hook::{self, iterator::Signals};
use uuid::Uuid;

use crate::jobs::{Job, JobBuilder};
use rebalancer::error::Error;
use rebalancer::util;
use std::thread;
use std::thread::JoinHandle;

static DEFAULT_CONFIG_PATH: &str = "/opt/smartdc/rebalancer/config.json";

// The maximum number of tasks we will send in a single assignment to the agent.
static DEFAULT_MAX_TASKS_PER_ASSIGNMENT: usize = 50;

// The maximum number of threads that will be used for metadata updates.
// Each thread has its own hash of moray clients.
static DEFAULT_MAX_METADATA_UPDATE_THREADS: usize = 2;

// The maximum number of sharks we will use as destinations for things like
// evacuate job.  This is the top 5 of an ordered list which could mean a
// different set of sharks each time we get a snapshot from the storinfo zone.
static DEFAULT_MAX_SHARKS: usize = 5;

// The number of elements the bounded metadata update queue will be set to.
// For evacuate jobs this represents the number of assignments that can be in
// the post processing state waiting for a metadata update thread to become
// available.
static DEFAULT_STATIC_QUEUE_DEPTH: usize = 10;

// The maximum amount of time in seconds that an assignment should remain in
// memory before it is posted to an agent.  This is not a hard and fast rule.
// This will only be checked synchronously every time we gather another set of
// destination sharks.
static DEFAULT_MAX_ASSIGNMENT_AGE: u64 = 600;

#[derive(Deserialize, Default, Debug, Clone)]
pub struct Shard {
    pub host: String,
}

// Until we can determine a reasonable set of defaults and limits these
// tunables are intentionally not exposed in the documentation.
#[derive(Deserialize, Debug, Clone, Copy)]
#[serde(default)]
pub struct ConfigOptions {
    pub max_tasks_per_assignment: usize,
    pub max_metadata_update_threads: usize,
    pub max_sharks: usize,
    pub use_static_md_update_threads: bool,
    pub static_queue_depth: usize,
    pub max_assignment_age: u64,
}

impl Default for ConfigOptions {
    fn default() -> ConfigOptions {
        ConfigOptions {
            max_tasks_per_assignment: DEFAULT_MAX_TASKS_PER_ASSIGNMENT,
            max_metadata_update_threads: DEFAULT_MAX_METADATA_UPDATE_THREADS,
            max_sharks: DEFAULT_MAX_SHARKS,
            use_static_md_update_threads: true,
            static_queue_depth: DEFAULT_STATIC_QUEUE_DEPTH,
            max_assignment_age: DEFAULT_MAX_ASSIGNMENT_AGE,
        }
    }
}

#[derive(Deserialize, Default, Debug, Clone)]
pub struct Config {
    pub domain_name: String,

    pub shards: Vec<Shard>,

    #[serde(default)]
    pub snaplink_cleanup_required: bool,

    #[serde(default)]
    pub options: ConfigOptions,

    #[serde(default = "Config::default_port")]
    pub listen_port: u16,
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

    fn default_port() -> u16 {
        80
    }

    pub fn parse_config(config_path: &Option<String>) -> Result<Config, Error> {
        let config_path = config_path
            .to_owned()
            .unwrap_or_else(|| DEFAULT_CONFIG_PATH.to_string());
        let file = File::open(config_path)?;
        let reader = BufReader::new(file);
        let config: Config = serde_json::from_reader(reader)?;

        Ok(config)
    }

    fn config_updater(
        config_update_rx: crossbeam_channel::Receiver<()>,
        update_config: Arc<Mutex<Config>>,
        config_file: Option<String>,
    ) -> JoinHandle<()> {
        thread::Builder::new()
            .name(String::from("config updater"))
            .spawn(move || loop {
                match config_update_rx.recv() {
                    Ok(()) => {
                        let new_config =
                            match Config::parse_config(&config_file) {
                                Ok(c) => c,
                                Err(e) => {
                                    error!(
                                        "Error parsing config after signal \
                                         received. Not updating: {}",
                                        e
                                    );
                                    continue;
                                }
                            };
                        let mut slcr = update_config
                            .lock()
                            .expect("Lock snaplink_cleanup_required");

                        *slcr = new_config;
                        debug!("Configuration has been updated: {:#?}", *slcr);
                    }
                    Err(e) => {
                        warn!(
                            "Channel has been disconnected, exiting \
                             thread: {}",
                            e
                        );
                        return;
                    }
                }
            })
            .expect("Start config updater")
    }

    // Run a thread that listens for the SIGUSR1 signal which config-agent
    // should be sending us via SMF when the config file is updated.  When a
    // signal is trapped it simply sends an empty message to the updater thread
    // which handles updating the configuration state in memory.  We don't want
    // to block or take any locks here because the signal is asynchronous.
    fn config_update_signal_handler(
        config_update_tx: crossbeam_channel::Sender<()>,
        update_barrier: Arc<Barrier>,
    ) -> JoinHandle<()> {
        thread::Builder::new()
            .name(String::from("config update signal handler"))
            .spawn(move || {
                _config_update_signal_handler(config_update_tx, update_barrier)
            })
            .expect("Start Config Update Signal Handler")
    }

    // This thread spawns two other threads.  One of them handles the SIGUSR1
    // signal and in turn notifies the other that the config file needs to be
    // re-parsed.  This function returns a JoinHandle that will only join
    // after both of the other threads have completed.
    pub fn start_config_watcher(
        config: Arc<Mutex<Config>>,
        config_file: Option<String>,
    ) -> JoinHandle<()> {
        thread::Builder::new()
            .name("config watcher".to_string())
            .spawn(move || {
                let (update_tx, update_rx) = crossbeam_channel::bounded(1);
                let barrier = Arc::new(Barrier::new(2));
                let update_barrier = Arc::clone(&barrier);
                let sig_handler_handle = Config::config_update_signal_handler(
                    update_tx,
                    update_barrier,
                );
                barrier.wait();

                let update_config = Arc::clone(&config);
                let config_updater_handle = Config::config_updater(
                    update_rx,
                    update_config,
                    config_file,
                );

                config_updater_handle.join().expect("join config updater");
                sig_handler_handle.join().expect("join signal handler");
            })
            .expect("start config watcher")
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
            .expect("Missing config file name")
            .to_string();
        let config = Config::parse_config(&Some(config_file))?;

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
    let job = JobBuilder::new(config)
        .evacuate(shark_id, &domain_name, max_objects)
        .commit()?;

    Ok(SubCommand::DoJob(Box::new(job)))
}

fn _config_update_signal_handler(
    config_update_tx: crossbeam_channel::Sender<()>,
    update_barrier: Arc<Barrier>,
) {
    let signals =
        Signals::new(&[signal_hook::SIGUSR1]).expect("register signals");

    update_barrier.wait();

    for signal in signals.forever() {
        trace!("Signal Received: {}", signal);
        match signal {
            signal_hook::SIGUSR1 => {
                // If there is already a message in the buffer
                // (i.e. TrySendError::Full), then the updater
                // thread will be doing an update anyway so no
                // sense in clogging things up further.
                match config_update_tx.try_send(()) {
                    Err(TrySendError::Disconnected(_)) => {
                        warn!("config_update listener is closed");
                        break;
                    }
                    Ok(()) | Err(TrySendError::Full(_)) => {
                        continue;
                    }
                }
            }
            _ => unreachable!(), // Ignore other signals
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use lazy_static::lazy_static;
    use libc;
    use mustache::{Data, MapBuilder};
    use std::fs::File;
    use std::io::{BufRead, BufReader, Write};

    static TEST_CONFIG_FILE: &str = "config.test.json";

    lazy_static! {
        static ref INITIALIZED: Mutex<bool> = Mutex::new(false);
        pub static ref TEMPLATE_PATH: String = format!(
            "{}/{}",
            env!("CARGO_MANIFEST_DIR"),
            "../sapi_manifests/rebalancer/template"
        );
    }

    fn unit_test_init() {
        let mut init = INITIALIZED.lock().unwrap();
        if *init {
            return;
        }

        *init = true;

        thread::spawn(move || {
            let _guard = util::init_global_logger();
            loop {
                // Loop around ::park() in the event of spurious wake ups.
                std::thread::park();
            }
        });
    }

    fn write_config_file(buf: &[u8]) -> Config {
        File::create(TEST_CONFIG_FILE)
            .and_then(|mut f| f.write_all(buf))
            .map_err(Error::from)
            .and_then(|_| {
                Config::parse_config(&Some(TEST_CONFIG_FILE.to_string()))
            })
            .expect("file write")
    }

    // Update our test config file with new variables
    fn update_test_config_with_vars(vars: &Data) -> Config {
        let template_str = std::fs::read_to_string(TEMPLATE_PATH.to_string())
            .expect("template string");

        println!("{}", template_str);

        let config_data = mustache::compile_str(&template_str)
            .and_then(|t| t.render_data_to_string(vars))
            .expect("render template");

        println!("{}", &config_data);
        write_config_file(config_data.as_bytes())
    }

    // Initialize a test configuration file by parsing and rendering the
    // same configuration template used in production.
    fn config_init() -> Config {
        assert!(*INITIALIZED.lock().unwrap());

        std::fs::remove_file(TEST_CONFIG_FILE).unwrap_or(());

        let vars = MapBuilder::new()
            .insert_str("DOMAIN_NAME", "fake.joyent.us")
            .insert_bool("SNAPLINK_CLEANUP_REQUIRED", true)
            .insert_vec("INDEX_MORAY_SHARDS", |builder| {
                builder.push_map(|bld| {
                    bld.insert_str("host", "1.fake.joyent.us")
                        .insert_bool("last", true)
                })
            })
            .build();

        update_test_config_with_vars(&vars)
    }

    fn config_fini() {
        std::fs::remove_file(TEST_CONFIG_FILE)
            .expect("attempt to delete missing file")
    }

    #[test]
    fn config_basic_test() {
        unit_test_init();
        let config = config_init();

        // The template does not have a listen_port entry, so it should
        // default to 80.
        assert_eq!(config.listen_port, 80);

        File::open(TEST_CONFIG_FILE)
            .and_then(|f| {
                for line in BufReader::new(f).lines() {
                    let l = line.expect("line reader");
                    println!("{}", l);

                    assert!(!l.contains("options"));
                    assert!(!l.contains("max_tasks_per_assignment"));
                    assert!(!l.contains("max_metadata_update_threads"));
                    assert!(!l.contains("max_sharks"));
                }
                Ok(())
            })
            .expect("config_basic_test");

        config_fini();
    }

    #[test]
    fn config_options_test() {
        unit_test_init();

        let file_contents = r#"{
                "options": {
                    "max_tasks_per_assignment": 1111,
                    "max_metadata_update_threads": 2222,
                    "max_sharks": 3333
                },
                "domain_name": "perf1.scloud.host",
                "shards": [
                    {
                        "host": "1.moray.perf1.scloud.host"
                    }
                ]
            }
        "#;

        std::fs::remove_file(TEST_CONFIG_FILE).unwrap_or(());
        let config = write_config_file(file_contents.as_bytes());

        assert_eq!(config.options.max_tasks_per_assignment, 1111);
        assert_eq!(config.options.max_metadata_update_threads, 2222);
        assert_eq!(config.options.max_sharks, 3333);

        config_fini();
    }

    #[test]
    fn missing_snaplink_cleanup_required() {
        unit_test_init();
        std::fs::remove_file(TEST_CONFIG_FILE).unwrap_or(());

        let vars = MapBuilder::new()
            .insert_str("DOMAIN_NAME", "fake.joyent.us")
            .insert_vec("INDEX_MORAY_SHARDS", |builder| {
                builder.push_map(|bld| {
                    bld.insert_str("host", "1.fake.joyent.us")
                        .insert_bool("last", true)
                })
            })
            .build();

        let config = update_test_config_with_vars(&vars);

        assert_eq!(config.snaplink_cleanup_required, false);
        config_fini();
    }

    #[test]
    // 1. Create a config (both file and in memory).
    // 2. Start the config watcher.
    // 3. Update the config file we created in step 1.
    // 4. Send a signal to the config watcher (what config-agent would do in
    //    production).
    // 5. Confirm that our in memory config reflects that changes from step 3.
    fn signal_handler_config_update() {
        unit_test_init();
        println!("{}", env!("CARGO_MANIFEST_DIR"));

        // Generate a config with snaplink_cleanup_required=true.
        let config = Arc::new(Mutex::new(config_init()));

        assert!(
            config
                .lock()
                .expect("config lock")
                .snaplink_cleanup_required
        );

        let update_config = Arc::clone(&config);

        // Start the config watcher.
        let _watcher_handle = Config::start_config_watcher(
            update_config,
            Some(TEST_CONFIG_FILE.to_string()),
        );

        // Change SNAPLINK_CLEANUP_REQUIRED to false
        let vars = MapBuilder::new()
            .insert_str("DOMAIN_NAME", "fake.joyent.us")
            .insert_bool("SNAPLINK_CLEANUP_REQUIRED", false)
            .insert_vec("INDEX_MORAY_SHARDS", |builder| {
                builder.push_map(|bld| {
                    bld.insert_str("host", "1.fake.joyent.us")
                        .insert_bool("last", true)
                })
            })
            .build();
        let _ = update_test_config_with_vars(&vars);

        // Send a signal letting the watcher know that we've updated the
        // config file and it needs to re-parse and update our in memory state.
        unsafe { libc::raise(signal_hook::SIGUSR1) };
        thread::sleep(std::time::Duration::from_secs(2));

        // Assert that our in memory config's snaplink_cleanup_required field
        // has changed to false.
        let check_config = config.lock().expect("config lock");
        assert_eq!(check_config.snaplink_cleanup_required, false);

        config_fini();
    }
}
