/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2020 Joyent, Inc.
 */
use lazy_static::lazy_static;
use prometheus::{
    opts, register_counter_vec, register_gauge, register_histogram_vec,
};
use rebalancer::metrics::{
    self, counter_vec_inc_by, gauge_dec, gauge_inc, gauge_set,
    histogram_vec_observe, Metrics, MetricsMap, ERROR_COUNT, OBJECT_COUNT,
    REQUEST_COUNT,
};
use std::collections::HashMap;
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

// This is a metric label that is specific to the rebalancer manager, hence
// why it is defined here instead of where the common labels are.
pub static SKIP_COUNT: &str = "skip_count";

// Gauge for tracking the current number of active metadata update threads.
pub static MD_THREAD_GAUGE: &str = "md_thread_gauge";

// Histogram for tracking latencies of object-read operations.
pub static OBJECT_READ_TIMES: &str = "object_read_times";

// This method may come in handy if it is necessary to add more metrics to
// our collector.
pub fn metrics_get() -> &'static Mutex<Option<MetricsMap>> {
    &METRICS
}

pub fn metrics_init(cfg: metrics::ConfigMetrics) {
    let mut metrics_init = METRICS_INIT.lock().expect("metrics init lock");

    if metrics_init.init {
        return;
    }

    // Create our baseline metrics.
    let mut metrics = metrics::register_metrics(&cfg);

    let labels: HashMap<String, String> = metrics::get_const_labels()
        .lock()
        .expect("metrics const labels")
        .clone()
        .unwrap_or_else(HashMap::new);

    // Now create and register additional metrics exclusively used by the
    // rebalancer manger.
    let skip_counter = register_counter_vec!(
        opts!(SKIP_COUNT, "Objects skipped.").const_labels(labels.clone()),
        &["reason"]
    )
    .expect("failed to register skip_count counter");

    metrics.insert(SKIP_COUNT, Metrics::MetricsCounterVec(skip_counter));

    let md_thread_gauge = register_gauge!(opts!(
        MD_THREAD_GAUGE,
        "Number of currently active metadata threads."
    )
    .const_labels(labels.clone()))
    .expect("failed to register metadata thread gauge");

    metrics.insert(MD_THREAD_GAUGE, Metrics::MetricsGauge(md_thread_gauge));

    // Metric for collecting object read times, broken down by shard.
    let object_read_times = register_histogram_vec!(
        histogram_opts!(OBJECT_READ_TIMES, "Object read times.")
            .const_labels(labels),
        &["shard"]
    )
    .expect("Failed to register object read times histogram.");

    metrics.insert(
        OBJECT_READ_TIMES,
        Metrics::MetricsHistogramVec(object_read_times),
    );

    // Take the fully formed set of metrics and store it globally.
    let mut global_metrics = METRICS.lock().unwrap();
    *global_metrics = Some(metrics);

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

// Objects skipped broken down by reason.
pub fn metrics_skip_inc(reason: Option<&str>) {
    metrics_vec_inc_by(SKIP_COUNT, reason, 1);
}

// Errors broken down by error type.
pub fn metrics_error_inc(reason: Option<&str>) {
    metrics_vec_inc_by(ERROR_COUNT, reason, 1);
}

// Requests broken down by request type.
pub fn metrics_request_inc(request: Option<&str>) {
    metrics_vec_inc_by(REQUEST_COUNT, request, 1);
}

// Objects processed, classified by action type.
pub fn metrics_object_inc(action: Option<&str>) {
    metrics_vec_inc_by(OBJECT_COUNT, action, 1);
}

// Objects skipped, classified by reason.  This is used when we increment the
// value by more than one at a time.
pub fn metrics_skip_inc_by(reason: Option<&str>, val: usize) {
    metrics_vec_inc_by(SKIP_COUNT, reason, val);
}

// Objects processed, classified by action type.  This is used when we increment
// the value by more than one at a time.
pub fn metrics_object_inc_by(action: Option<&str>, val: usize) {
    metrics_vec_inc_by(OBJECT_COUNT, action, val);
}

// Private method that directly accesses the metrics structure.
fn metrics_vec_inc_by(key: &str, bucket: Option<&str>, val: usize) {
    let metrics = METRICS.lock().unwrap().clone();
    counter_vec_inc_by(&metrics.expect("metrics"), key, bucket, val);
}

pub fn metrics_gauge_dec(key: &str) {
    let metrics = METRICS.lock().unwrap().clone();
    gauge_dec(&metrics.expect("metrics"), key);
}

pub fn metrics_gauge_inc(key: &str) {
    let metrics = METRICS.lock().unwrap().clone();
    gauge_inc(&metrics.expect("metrics"), key);
}

pub fn metrics_gauge_set(key: &str, val: usize) {
    let metrics = METRICS.lock().unwrap().clone();
    gauge_set(&metrics.expect("metrics"), key, val);
}

pub fn metrics_histogram_vec_observe(key: &str, bucket: &str, val: u128) {
    let num = val as f64;
    let metrics = METRICS.lock().unwrap().clone();
    histogram_vec_observe(&metrics.expect("metrics"), key, bucket, num);
}
