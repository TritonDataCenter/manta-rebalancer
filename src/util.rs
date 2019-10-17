/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2019, Joyent, Inc.
 */

use std::io;
use std::sync::Mutex;
use std::thread;

use clap::{crate_name, crate_version};
use slog::{o, Drain, Logger};

pub fn create_bunyan_logger<W>(io: W) -> Logger
where
    W: io::Write + std::marker::Send + 'static,
{
    Logger::root(
        Mutex::new(
            slog_bunyan::with_name(crate_name!(), io)
            .build()
        )
        .fuse(),
        o!("build-id" => crate_version!())
    )
}

pub fn init_global_logger() -> slog_scope::GlobalLoggerGuard
{
    let log = create_bunyan_logger(std::io::stdout());
    slog_scope::set_global_logger(log)
}

pub fn get_thread_name() -> String
{
    let cn = crate_name!().to_owned();

    if thread::current().name().is_none() {
        return cn;
    }

    // Prepend the crate name to the thread name.
    format!("{} {}", &cn, thread::current().name().unwrap())
}

#[macro_export]
macro_rules! log(
    ($lvl:expr, $($args:tt)+) => {
        let m = format!($($args)+);
        let stmt = format!("{}: {}", $crate::util::get_thread_name(), m);
        slog::slog_log!(slog_scope::logger(), $lvl, "", "{}", stmt)
    };
);

#[cfg(test)]
use rand::{distributions::Alphanumeric, Rng};

pub fn shard_host2num(shard_host: &str) -> u32 {
    let shard_split: Vec<&str> = shard_host.split('.').collect();
    shard_split[0].parse().unwrap()
}

// Used in test
#[cfg(test)]
pub fn random_string(len: usize) -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(len)
        .collect()
}
