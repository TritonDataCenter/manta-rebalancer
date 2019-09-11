/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2019, Joyent, Inc.
 */

use crc::crc32;
use unescape::unescape;

#[cfg(test)]
use rand::{distributions::Alphanumeric, Rng};

pub fn shard_host2num(shard_host: &str) -> u32 {
    let shard_split: Vec<&str> = shard_host.split('.').collect();
    shard_split[0].parse().unwrap()
}

pub fn crc_hex_str(data: &str) -> Option<String> {
    let str_obj = match unescape(data) {
        Some(o) => o,
        None => {
            return None;
        }
    };
    let crc_input = str_obj.trim_matches('"').as_bytes();
    let etag = crc32::checksum_ieee(crc_input);
    Some(format!("{:X}", etag))
}

// Used in test
#[cfg(test)]
pub fn random_string(len: usize) -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(len)
        .collect()
}
