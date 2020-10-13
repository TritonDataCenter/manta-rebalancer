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

use crossbeam_channel::TrySendError;
use serde::{de, de::Error as de_Error, Deserialize, Deserializer};
use signal_hook::{self, iterator::Signals};

use rebalancer::error::Error;
use rebalancer::util;
use slog::Level;
use std::thread;
use std::thread::JoinHandle;

static DEFAULT_CONFIG_PATH: &str = "/opt/smartdc/rebalancer/config.json";

// TODO: Determine max and min values for each (MANTA-5284)

// The maximum number of tasks we will send in a single assignment to the agent.
static DEFAULT_MAX_TASKS_PER_ASSIGNMENT: usize = 50;

// The maximum number of threads that will be used for metadata updates.
// Each thread has its own hash of moray clients.
static DEFAULT_MAX_METADATA_UPDATE_THREADS: usize = 10;

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

// The chunk size used when scanning the metadata tier or during a retry when
// reading from the local database.
static DEFAULT_METADATA_READ_CHUNK_SIZE: usize = 10000;

// Default maximum number of per-shard threads that we will use to scan the
// metadata tier.
static DEFAULT_MAX_METADATA_READ_THREADS: usize = 10;

pub const MAX_TUNABLE_MD_UPDATE_THREADS: usize = 250;

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
    pub use_batched_updates: bool,
    pub md_read_chunk_size: usize,
    pub max_md_read_threads: usize,
}

impl Default for ConfigOptions {
    fn default() -> ConfigOptions {
        ConfigOptions {
            max_tasks_per_assignment: DEFAULT_MAX_TASKS_PER_ASSIGNMENT,
            max_metadata_update_threads: DEFAULT_MAX_METADATA_UPDATE_THREADS,
            max_sharks: DEFAULT_MAX_SHARKS,
            use_static_md_update_threads: false,
            static_queue_depth: DEFAULT_STATIC_QUEUE_DEPTH,
            max_assignment_age: DEFAULT_MAX_ASSIGNMENT_AGE,
            use_batched_updates: true,
            md_read_chunk_size: DEFAULT_METADATA_READ_CHUNK_SIZE,
            max_md_read_threads: DEFAULT_MAX_METADATA_READ_THREADS,
        }
    }
}

#[derive(Deserialize, Debug, Clone)]
pub struct Config {
    pub domain_name: String,

    /// The `parse_config()` method sorts these shards by shard number.  If
    /// this struct is created ad-hoc without parsing the config file directly
    /// the `min_shard_num()` and `max_shard_num()` functions will not return
    /// accurate values, so this field must remain private.
    shards: Vec<Shard>,

    #[serde(default)]
    pub snaplink_cleanup_required: bool,

    #[serde(default)]
    pub options: ConfigOptions,

    #[serde(default = "Config::default_port")]
    pub listen_port: u16,

    #[serde(default = "Config::default_max_fill_percentage")]
    pub max_fill_percentage: u32,

    #[serde(
        deserialize_with = "log_level_deserialize",
        default = "Config::default_log_level"
    )]
    pub log_level: Level,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            domain_name: String::new(),
            shards: vec![],
            snaplink_cleanup_required: false,
            options: ConfigOptions::default(),
            listen_port: 80,
            max_fill_percentage: 100,
            log_level: Level::Debug,
        }
    }
}

fn log_level_deserialize<'de, D>(deserializer: D) -> Result<Level, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?.to_lowercase();
    match s.as_str() {
        "critical" | "crit" => Ok(Level::Critical),
        "error" => Ok(Level::Error),
        "warning" | "warn" => Ok(Level::Warning),
        "info" => Ok(Level::Info),
        "debug" => Ok(Level::Debug),
        "trace" => Ok(Level::Trace),
        _ => Err(D::Error::invalid_value(
            de::Unexpected::Str(s.as_str()),
            &"slog Level string",
        )),
    }
}

impl Config {
    /// This method assumes the `shards: Vec<Shard>` is sorted.
    pub fn min_shard_num(&self) -> u32 {
        util::shard_host2num(self.shards.first().expect("first").host.as_str())
    }

    /// This method assumes the `shards: Vec<Shard>` is sorted.
    pub fn max_shard_num(&self) -> u32 {
        util::shard_host2num(self.shards.last().expect("last").host.as_str())
    }

    fn default_port() -> u16 {
        80
    }

    fn default_max_fill_percentage() -> u32 {
        100
    }

    fn default_log_level() -> Level {
        Level::Debug
    }

    pub fn parse_config(config_path: &Option<String>) -> Result<Config, Error> {
        let config_path = config_path
            .to_owned()
            .unwrap_or_else(|| DEFAULT_CONFIG_PATH.to_string());
        let file = File::open(config_path)?;
        let reader = BufReader::new(file);
        let mut config: Config = serde_json::from_reader(reader)?;

        // Both min_shard_num() and max_shard_num() depend on this vector
        // being sorted.  Do not change or remove this line without making a
        // complementary change to those two functions.
        config
            .shards
            .sort_by_key(|s| util::shard_host2num(s.host.as_str()));

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
                        let mut config_lock =
                            update_config.lock().expect("Lock update_config");

                        *config_lock = new_config;
                        debug!(
                            "Configuration has been updated: {:#?}",
                            *config_lock
                        );
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
    use crate::test_util::{
        self, config_fini, update_test_config_with_vars, write_config_file,
        TEST_CONFIG_FILE,
    };
    use lazy_static::lazy_static;
    use libc;
    use mustache::MapBuilder;
    use std::fs::File;
    use std::io::Read;

    lazy_static! {
        static ref INITIALIZED: Mutex<bool> = Mutex::new(false);
    }

    fn unit_test_init() {
        let mut init = INITIALIZED.lock().unwrap();
        if *init {
            return;
        }

        *init = true;

        thread::spawn(move || {
            let _guard = util::init_global_logger(None);
            loop {
                // Loop around ::park() in the event of spurious wake ups.
                std::thread::park();
            }
        });
    }

    // Initialize a test configuration file by parsing and rendering the
    // same configuration template used in production.
    fn config_init() -> Config {
        assert!(*INITIALIZED.lock().unwrap());
        test_util::config_init()
    }

    #[test]
    fn min_max_shards() {
        unit_test_init();
        std::fs::remove_file(TEST_CONFIG_FILE).unwrap_or(());

        let vars = MapBuilder::new()
            .insert_str("DOMAIN_NAME", "fake.joyent.us")
            .insert_bool("SNAPLINK_CLEANUP_REQUIRED", true)
            .insert_vec("INDEX_MORAY_SHARDS", |builder| {
                builder.push_map(|bld| {
                    bld.insert_str("host", "3.fake.joyent.us")
                        .insert_bool("last", true)
                })
            })
            .build();
        let config = update_test_config_with_vars(&vars);

        assert_eq!(config.min_shard_num(), 3);
        assert_eq!(config.max_shard_num(), 3);

        let vars = MapBuilder::new()
            .insert_str("DOMAIN_NAME", "fake.joyent.us")
            .insert_bool("SNAPLINK_CLEANUP_REQUIRED", true)
            .insert_vec("INDEX_MORAY_SHARDS", |builder| {
                builder
                    .push_map(|bld| bld.insert_str("host", "99.fake.joyent.us"))
                    .push_map(|bld| {
                        bld.insert_str("host", "1000.fake.joyent.us")
                    })
                    .push_map(|bld| {
                        bld.insert_str("host", "200.fake.joyent.us")
                    })
                    .push_map(|bld| bld.insert_str("host", "2.fake.joyent.us"))
                    .push_map(|bld| {
                        bld.insert_str("host", "100.fake.joyent.us")
                            .insert_bool("last", true)
                    })
            })
            .build();

        let config = update_test_config_with_vars(&vars);

        assert_eq!(config.min_shard_num(), 2);
        assert_eq!(config.max_shard_num(), 1000);

        config_fini();
    }

    #[test]
    fn config_basic_test() {
        unit_test_init();
        let config = config_init();

        // The template does not have a listen_port entry, so it should
        // default to 80.
        assert_eq!(config.listen_port, 80);

        File::open(TEST_CONFIG_FILE)
            .and_then(|mut f| {
                let mut config_file = String::new();

                f.read_to_string(&mut config_file).expect("config file");

                assert!(config_file.contains("options"));
                assert!(config_file.contains("max_tasks_per_assignment"));
                assert!(config_file.contains("max_metadata_update_threads"));
                assert!(config_file.contains("max_sharks"));
                assert!(config_file.contains("use_static_md_update_threads"));
                assert!(config_file.contains("static_queue_depth"));
                assert!(config_file.contains("max_assignment_age"));

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
        assert_eq!(config.options.use_static_md_update_threads, false);
        assert_eq!(
            config.options.static_queue_depth,
            DEFAULT_STATIC_QUEUE_DEPTH
        );
        assert_eq!(
            config.options.max_assignment_age,
            DEFAULT_MAX_ASSIGNMENT_AGE
        );

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
