/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2020 Joyent, Inc.
 */
use lazy_static::lazy_static;
use rebalancer::metrics;
use rebalancer::metrics::{
    counter_vec_inc, MetricsMap, ERROR_COUNT, OBJECT_COUNT, REQUEST_COUNT,
};
use std::sync::atomic::AtomicBool;
use std::sync::Mutex;
use std::thread;

// This is absoutely ridiculous.  Apparently, clippy will complain about the
// fact that a bool does not need to be wrapped inside of a mutex because we
// can use AtomicBool instead.  In our case however, it is not just access to
// the variable itself that requires serialization.  The lock that was once
// attached to the bool was also used to serialize access to the entire metrics
// initialization routine and therefore, was necessary.  Evidentally, clippy
// is not aware of _how_ we are using such structures, only that we do, and
// for that reason continues to recommend using the AtomicBool which simply
// will not achieve the stated objective(s).  For that reason, we resort to a
// cheap trick of wrapping the bool inside of a custom data structure so that
// clippy will not try to recommend an AtomicMetricsInit or the like.  For more
// information, see:
// https://rust-lang.github.io/rust-clippy/master/index.html#mutex_atomic
struct MetricsInit {
    pub init: bool,
}

impl MetricsInit {
    pub fn new() -> MetricsInit {
        MetricsInit { init: false }
    }
}

lazy_static! {
    static ref METRICS: Mutex<Option<MetricsMap>> = Mutex::new(None);
    static ref METRICS_INIT: Mutex<MetricsInit> =
        Mutex::new(MetricsInit::new());
}

// There will likely be several other strings of this nature defined as the
// rebalancer manager functionality is extended.
pub static ACTION_EVACUATE: &str = "evacuate";

// This method may come in handy if it is necessary to add more metrics to
// our collector.
pub fn manager_metrics_get() -> &'static Mutex<Option<MetricsMap>> {
    &METRICS
}

pub fn manager_metrics_init(cfg: metrics::ConfigMetrics) {
    let mut metrics_init = METRICS_INIT.lock().unwrap();

    if metrics_init.init {
        return;
    }

    let mut metrics = METRICS.lock().unwrap();
    *metrics = Some(metrics::register_metrics(&cfg));

    // Spawn a thread which runs our metrics server.
    let ms = thread::Builder::new()
        .name(String::from("Rebalancer Manager Metrics"))
        .spawn(move || {
            metrics::start_server(&cfg.host, cfg.port, &slog_scope::logger())
        });

    assert!(ms.is_ok());
    metrics_init.init = true;
}

//
// The following utility functions exist to ensure that a caller can not
// inadvertently provide an erroneous key to the MetricsMap when updating
// counts of some event that has occurred.
//

// Errors broken down by error type.
pub fn manager_metrics_error_inc(reason: Option<&str>) {
    manager_metrics_vec_inc(ERROR_COUNT, reason);
}

// Requests broken down by request type.
pub fn manager_metrics_request_inc(request: Option<&str>) {
    manager_metrics_vec_inc(REQUEST_COUNT, request);
}

// Objects processed, classified by action type.
pub fn manager_metrics_object_inc(action: Option<&str>) {
    manager_metrics_vec_inc(OBJECT_COUNT, action);
}

// Private method that directly accesses the metrics structure.
fn manager_metrics_vec_inc(key: &str, bucket: Option<&str>) {
    let metrics = METRICS.lock().unwrap().clone();
    counter_vec_inc(&metrics.unwrap(), key, bucket);
}
