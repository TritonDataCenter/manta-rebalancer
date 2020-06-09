/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2020 Joyent, Inc.
 */

use crate::config::Config;
use rebalancer::error::Error;

use lazy_static::lazy_static;
use mustache::{Data, MapBuilder};
use std::fs::File;
use std::io::Write;

pub static TEST_CONFIG_FILE: &str = "config.test.json";

lazy_static! {
    pub static ref TEMPLATE_PATH: String = format!(
        "{}/{}",
        env!("CARGO_MANIFEST_DIR"),
        "../sapi_manifests/rebalancer/template"
    );
}

pub fn write_config_file(buf: &[u8]) -> Config {
    File::create(TEST_CONFIG_FILE)
        .and_then(|mut f| f.write_all(buf))
        .map_err(Error::from)
        .and_then(|_| Config::parse_config(&Some(TEST_CONFIG_FILE.to_string())))
        .expect("file write")
}

// Update our test config file with new variables
pub fn update_test_config_with_vars(vars: &Data) -> Config {
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
pub fn config_init() -> Config {
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

pub fn config_fini() {
    std::fs::remove_file(TEST_CONFIG_FILE)
        .expect("attempt to delete missing file")
}
