/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2019, Joyent, Inc.
 */

use crate::error::Error;
use quickcheck::{Arbitrary, Gen};
use quickcheck_helpers::random::string as random_string;
use reqwest;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
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

#[derive(Default)]
pub struct Picker {
    sharks: Arc<Mutex<Option<Vec<StorageNode>>>>,
    handle: Mutex<Option<JoinHandle<()>>>,
    running: Arc<AtomicBool>,
}

///
/// The algorithms available for choosing sharks.
///
///  * Default:
///     Provide a list of storage nodes that have at least a <minimum
///     available capacity> and are not in a <blacklist of datacenters>
pub enum PickerAlgorithm<'a> {
    Default(&'a DefaultPickerAlgorithm),
}

#[derive(Default)]
pub struct DefaultPickerAlgorithm {
    pub blacklist: Vec<String>,
    pub min_avail_mb: Option<u64>,
}

impl<'a> PickerAlgorithm<'a> {
    fn choose(&self, sharks: &[StorageNode]) -> Vec<StorageNode> {
        match self {
            PickerAlgorithm::Default(algo) => algo.method(sharks),
        }
    }
}

impl DefaultPickerAlgorithm {
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

impl Picker {
    pub fn new() -> Self {
        Picker {
            running: Arc::new(AtomicBool::new(true)),
            handle: Mutex::new(None),
            sharks: Arc::new(Mutex::new(Some(vec![]))),
        }
    }

    /// Populate the picker's sharks field, and start the picker updater thread.
    pub fn start(&mut self) -> Result<(), Error> {
        let mut locked_sharks = self.sharks.lock().unwrap();
        *locked_sharks = Some(fetch_sharks());

        let handle =
            Self::updater(Arc::clone(&self.sharks), Arc::clone(&self.running));
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

    /// Get the the Vec<sharks> from the picker.
    pub fn get_sharks(&self) -> Option<Vec<StorageNode>> {
        self.sharks.lock().unwrap().take()
    }

    fn updater(
        sharks: Arc<Mutex<Option<Vec<StorageNode>>>>,
        running: Arc<AtomicBool>,
    ) -> JoinHandle<()> {
        let updater_sharks = Arc::clone(&sharks);
        let keep_running = Arc::clone(&running);

        thread::spawn(move || {
            let sleep_time = time::Duration::from_secs(10);
            while keep_running.load(Ordering::Relaxed) {
                thread::sleep(sleep_time);

                let mut new_sharks = fetch_sharks();
                new_sharks.sort_by(|a, b| a.available_mb.cmp(&b.available_mb));

                let mut old_sharks = updater_sharks.lock().unwrap();
                *old_sharks = Some(new_sharks);
                info!("Sharks updated, sleeping for {:?}", sleep_time);
            }
        })
    }
}

// TODO: MANTA-4519
impl SharkSource for Picker {
    /// Choose the sharks based on the specified algorithm
    fn choose(&self, algo: &PickerAlgorithm) -> Option<Vec<StorageNode>> {
        match self.get_sharks() {
            Some(s) => Some(algo.choose(&s)),
            None => None,
        }
    }
}

pub trait SharkSource: Sync + Send {
    fn choose(&self, algo: &PickerAlgorithm) -> Option<Vec<StorageNode>>;
}

// Use our prototype picker zone for now.  Might change this to a shard 1 moray
// client in the future.
fn fetch_sharks() -> Vec<StorageNode> {
    // TODO: should find picker in DNS
    let mut ret = reqwest::get("http://10.77.77.43/poll").unwrap();
    let result = ret.json::<HashMap<String, Vec<StorageNode>>>().unwrap();
    let mut new_sharks = vec![];

    for (dc, sharks) in result.clone() {
        let s = sharks.clone();
        new_sharks.extend(s);

        for shark in sharks {
            info!(
                "{}: {} ({}%)",
                dc, shark.manta_storage_id, shark.percent_used
            );
        }
    }
    debug!("picker updated with new sharks: {:?}", new_sharks);
    new_sharks
}
