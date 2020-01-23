/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2020, Joyent, Inc.
 */

use std::io;
use std::sync::Mutex;
use std::thread;

use clap::{crate_name, crate_version};
use slog::{o, Drain, Logger};

pub static MIN_HTTP_STATUS_CODE: u16 = 100;
pub static MAX_HTTP_STATUS_CODE: u16 = 600;

pub fn create_bunyan_logger<W>(io: W) -> Logger
where
    W: io::Write + std::marker::Send + 'static,
{
    Logger::root(
        Mutex::new(slog_bunyan::with_name(crate_name!(), io).build()).fuse(),
        o!("build-id" => crate_version!()),
    )
}

pub fn init_global_logger() -> slog_scope::GlobalLoggerGuard {
    let log = create_bunyan_logger(std::io::stdout());
    slog_scope::set_global_logger(log)
}

// If no name just print the crate name with the thread ID like so:
//      "<crate_name>[ThreadId(<tid>)]: <log msg>"
// Otherwise add the thread name as well.
//      "<crate_name>[ThreadId(<tid>):<thread name>]: <log msg>"
pub fn get_thread_name() -> String {
    let output = format!(
        "{}[{:?}",
        crate_name!().to_owned(),
        thread::current().id()
    );

    match thread::current().name() {
        Some(thn) => {
            format!("{}:{}]", output, thn)
        },
        None => {
            format!("{}]", output)
        }
    }
}

#[macro_export]
macro_rules! log_impl(
    ($lvl:expr, $($args:tt)+) => {
        let m = format!($($args)+);
        let stmt = format!("{}: {}", $crate::util::get_thread_name(), m);
        slog::slog_log!(slog_scope::logger(), $lvl, "", "{}", stmt)
    };
);

#[macro_export]
macro_rules! info(
    ($($args:tt)*) => {
        log_impl!(slog::Level::Info, $($args)*)
    };
);

#[macro_export]
macro_rules! error(
    ($($args:tt)*) => {
        log_impl!(slog::Level::Error, $($args)*)
    };
);

#[macro_export]
macro_rules! warn(
    ($($args:tt)*) => {
        log_impl!(slog::Level::Warning, $($args)*)
    };
);

#[macro_export]
macro_rules! trace(
    ($($args:tt)*) => {
        log_impl!(slog::Level::Trace, $($args)*)
    };
);

#[macro_export]
macro_rules! debug(
    ($($args:tt)*) => {
        log_impl!(slog::Level::Debug, $($args)*)
    };
);

#[macro_export]
macro_rules! crit(
    ($($args:tt)*) => {
        log_impl!(slog::Level::Critical, $($args)*)
    };
);

pub fn shard_host2num(shard_host: &str) -> u32 {
    let shard_split: Vec<&str> = shard_host.split('.').collect();
    shard_split[0].parse().unwrap()
}

pub fn print_version() {
    let version = env!("CARGO_PKG_VERSION");
    let name = env!("CARGO_PKG_NAME");
    println!("{} {}", name, version);
}
