/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2020 Joyent, Inc.
 */

/// Manta Object Rebalancer

#[macro_use]
extern crate diesel;

#[macro_use]
extern crate strum_macros;

#[macro_use]
extern crate rebalancer;

pub mod config;
pub mod jobs;
pub mod metrics;
pub mod moray_client;
pub mod pg_db;
pub mod storinfo;
mod pagination;
