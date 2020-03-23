/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2020 Joyent, Inc.
 */

use quickcheck::{Arbitrary, Gen};
use quickcheck_helpers::random::string as random_string;
use rebalancer::error::Error;
use reqwest::{self, StatusCode, Client};
use serde::{Deserialize, Serialize};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use std::thread::JoinHandle;
use std::{thread, time};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct StorageNode {
    #[serde(alias = "availableMB")]
    pub available_mb: u64,

    #[serde(alias = "percentUsed")]
    pub percent_used: u8,
    pub filesystem: String,
    pub datacenter: String,
    pub manta_storage_id: String,
    pub timestamp: u64, // TODO: can this be deserialized as a datetime type?
}

impl Arbitrary for StorageNode {
    fn arbitrary<G: Gen>(g: &mut G) -> StorageNode {
        let len: usize = (g.next_u32() % 20) as usize;
        StorageNode {
            available_mb: g.next_u64(),
            percent_used: (g.next_u32() % 100) as u8,
            filesystem: random_string(g, len),
            datacenter: random_string(g, len),
            manta_storage_id: format!(
                "{}.{}.{}",
                random_string(g, len),
                random_string(g, len),
                random_string(g, len),
            ),
            timestamp: g.next_u64(),
        }
    }
}

pub struct Storinfo {
    sharks: Arc<Mutex<Option<Vec<StorageNode>>>>,
    handle: Mutex<Option<JoinHandle<()>>>,
    running: Arc<AtomicBool>,
    host: String,
}

///
/// The algorithms available for choosing sharks.
///
///  * Default:
///     Provide a list of storage nodes that have at least a <minimum
///     available capacity> and are not in a <blacklist of datacenters>
pub enum ChooseAlgorithm<'a> {
    Default(&'a DefaultChooseAlgorithm),
}

#[derive(Default)]
pub struct DefaultChooseAlgorithm {
    pub blacklist: Vec<String>,
    pub min_avail_mb: Option<u64>,
}

impl<'a> ChooseAlgorithm<'a> {
    fn choose(&self, sharks: &[StorageNode]) -> Vec<StorageNode> {
        match self {
            ChooseAlgorithm::Default(algo) => algo.method(sharks),
        }
    }
}

impl DefaultChooseAlgorithm {
    fn method(&self, sharks: &[StorageNode]) -> Vec<StorageNode> {
        let mut ret: Vec<StorageNode> = vec![];

        // If the min_avail_mb is specified and the sharks available space is less
        // than min_avail_mb skip it.
        for s in sharks.iter() {
            if let Some(min_avail_mb) = self.min_avail_mb {
                if self.blacklist.contains(&s.datacenter)
                    || s.available_mb < min_avail_mb
                {
                    continue;
                }
            }
            ret.push(s.to_owned())
        }

        ret
    }
}

impl Storinfo {
    pub fn new(domain: &str) -> Result<Self, Error> {
        let storinfo_domain_name = format!("storinfo.{}", domain);
        Ok(Storinfo {
            running: Arc::new(AtomicBool::new(true)),
            handle: Mutex::new(None),
            sharks: Arc::new(Mutex::new(Some(vec![]))),
            host: storinfo_domain_name,
        })
    }

    /// Populate the storinfo's sharks field, and start the storinfo updater thread.
    pub fn start(&mut self) -> Result<(), Error> {
        let client = Client::new();
        let mut locked_sharks = self.sharks.lock().unwrap();
        // TODO: MANTA-4961, don't start job if picker cannot be reached.
        *locked_sharks = Some(fetch_sharks(&client, &self.host));

        let handle = Self::updater(
            self.host.clone(),
            Arc::clone(&self.sharks),
            Arc::clone(&self.running),
        );
        let mut locked_handle = self.handle.lock().unwrap();
        *locked_handle = Some(handle);
        Ok(())
    }

    pub fn fini(&self) {
        self.running.swap(false, Ordering::Relaxed);

        if let Some(handle) = self.handle.lock().unwrap().take() {
            handle.join().expect("failed to stop updater thread");
        } else {
            warn!("Updater thread not started");
        }
    }

    /// Get the the Vec<sharks> from the storinfo service.
    pub fn get_sharks(&self) -> Option<Vec<StorageNode>> {
        self.sharks.lock().unwrap().take()
    }

    fn updater(
        host: String,
        sharks: Arc<Mutex<Option<Vec<StorageNode>>>>,
        running: Arc<AtomicBool>,
    ) -> JoinHandle<()> {
        let updater_sharks = Arc::clone(&sharks);
        let keep_running = Arc::clone(&running);

        thread::spawn(move || {
            let sleep_time = time::Duration::from_secs(10);
            let client = Client::new();
            while keep_running.load(Ordering::Relaxed) {
                thread::sleep(sleep_time);

                let mut new_sharks = fetch_sharks(&client, &host);
                new_sharks.sort_by(|a, b| a.available_mb.cmp(&b.available_mb));

                let mut old_sharks = updater_sharks.lock().unwrap();
                *old_sharks = Some(new_sharks);
                info!("Sharks updated, sleeping for {:?}", sleep_time);
            }
        })
    }
}

// TODO: MANTA-4519
impl SharkSource for Storinfo {
    /// Choose the sharks based on the specified algorithm
    fn choose(&self, algo: &ChooseAlgorithm) -> Option<Vec<StorageNode>> {
        match self.get_sharks() {
            Some(s) => Some(algo.choose(&s)),
            None => None,
        }
    }
}

pub trait SharkSource: Sync + Send {
    fn choose(&self, algo: &ChooseAlgorithm) -> Option<Vec<StorageNode>>;
}

fn fetch_sharks(client: &Client, host: &str) -> Vec<StorageNode> {
    let mut new_sharks = vec![];
    let mut done = false;
    let mut after_id = String::new();
    let base_url = format!("http://{}/storagenodes", host);
    let limit = 100;

    while !done {
        let url = format!("{}?limit={}&after_id={}", base_url, limit, after_id);
        let mut response = match client.get(&url).send() {
            Ok(r) => r,
            Err(e) => {
                error!(
                    "Error requesting list of sharks from storinfo \
                     service: {}",
                    e
                );
                return vec![];
            }
        };

        trace!("Got picker response: {:#?}", response);

        // Storinfo, or our connection to it, is sick.  So instead of
        // breaking out of the loop and possibly returning partial results we
        // return an empty Vec.
        if response.status() != StatusCode::OK {
            error!("Could not contact storinfo service {:#?}", response);
            return vec![];
        }

        let result: Vec<StorageNode> =
            response.json().expect("picker response format");

        // .last() returns None on an empty Vec, so we can just break out of the
        // loop if that is the case.
        match result.last() {
            Some(r) => after_id = r.manta_storage_id.clone(),
            None => break,
        }

        if result.len() < limit {
            done = true;
        }

        new_sharks.extend(result);
    }

    debug!("storinfo updated with new sharks: {:?}", new_sharks);
    new_sharks
}
