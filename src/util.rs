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
use diesel::pg::PgConnection;
use diesel::prelude::*;

use slog::{o, Drain, Logger};

static DB_URL: &str = "postgres://postgres:postgres@";
pub fn connect_db(db_name: &str) -> PgConnection {
    let connect_url = format!("{}/{}", DB_URL, db_name);
    PgConnection::establish(&connect_url)
        .unwrap_or_else(|_| panic!("Error connecting to {}", DB_URL))
}

pub fn create_db(db_name: &str) {
    let create_query = format!("CREATE DATABASE \"{}\"", db_name);
    let conn = PgConnection::establish(&DB_URL).unwrap_or_else(|_| {
        panic!(
            "Cannot create DB {}.  Error connecting to {}",
            db_name, DB_URL
        )
    });

    conn.execute(&create_query)
        .expect("Error creating Database");
}

pub fn create_and_connect_db(db_name: &str) -> PgConnection {
    create_db(db_name);
    connect_db(db_name)
}

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

pub fn get_thread_name() -> String {
    let cn = crate_name!().to_owned();

    if thread::current().name().is_none() {
        return cn;
    }

    // Prepend the crate name to the thread name.
    format!("{} {}", &cn, thread::current().name().unwrap())
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
