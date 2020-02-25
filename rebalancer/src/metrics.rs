// Copyright 2020 Joyent, Inc.

use std::collections::HashMap;
use std::net::SocketAddr;

use gethostname::gethostname;
use hyper::header::{HeaderValue, CONTENT_TYPE};
use hyper::rt::{self, Future};
use hyper::server::Server;
use hyper::service::service_fn_ok;
use hyper::Body;
use hyper::StatusCode;
use hyper::{Request, Response};
use prometheus::{
    opts, register_counter, register_counter_vec, Counter, CounterVec, Encoder,
    TextEncoder,
};
use serde_derive::Deserialize;
use slog::{error, info, Logger};

#[derive(Clone, Deserialize)]
pub struct ConfigMetrics {
    /// Rebalancer metrics server address
    pub host: String,
    /// Rebalancer metrics server port
    pub port: u16,
    pub datacenter: String,
    pub service: String,
    pub server: String,
}

impl Default for ConfigMetrics {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".into(),
            port: 8878,
            datacenter: "development".into(),
            service: "1.rebalancer.localhost".into(),
            server: "127.0.0.1".into(),
        }
    }
}

// This enum exists so that we can take various prometheus counter types as the
// same data type.  This is necessary so that we can store all metrics that we
// create in the same hash map regardless of the type of counter.  Note, not
// all prometheus counters are enumerated below.  Add them as needed.
#[derive(Clone)]
pub enum Metrics {
    MetricsCounter(Counter),
    MetricsCounterVec(CounterVec),
}

pub fn counter_inc<S: ::std::hash::BuildHasher>(
    metrics: HashMap<String, Metrics, S>,
    key: &str,
) {
    match metrics.get(key) {
        Some(metric) => {
            if let Metrics::MetricsCounter(c) = metric {
                c.inc();
            }
        }
        None => error!(slog_scope::logger(), "Invalid metric: {}", key),
    }
}

pub fn counter_vec_inc<S: ::std::hash::BuildHasher>(
    metrics: HashMap<String, Metrics, S>,
    key: &str,
    bucket: &str,
) {
    match metrics.get(key) {
        Some(metric) => {
            if let Metrics::MetricsCounterVec(c) = metric {
                c.with_label_values(&[bucket]).inc();
            }
        }
        None => error!(slog_scope::logger(), "Invalid metric: {}", key),
    }
}

// Given the service configuration information, create (i.e. register) the
// desired metrics with prometheus.
pub fn register_metrics(labels: &ConfigMetrics) -> HashMap<String, Metrics> {
    let hostname = gethostname()
        .into_string()
        .unwrap_or_else(|_| String::from("unknown"));

    let mut metrics = HashMap::new();

    // Convert our ConfigAgentMetrics structure to a HashMap since that is what
    // Prometheus requires when creating a new metric with labels.  It is a
    // Manta-wide convention to require the (below) labels at a minimum as a
    // part of all metrics.  Other labels can be added, but these are required.
    let mut const_labels = HashMap::new();
    const_labels.insert("service".to_string(), labels.service.clone());
    const_labels.insert("server".to_string(), labels.server.clone());
    const_labels.insert("datacenter".to_string(), labels.datacenter.clone());
    const_labels.insert("zonename".to_string(), hostname.clone());

    // The request counter maintains a list of requests received, broken down
    // by the type of request (e.g. op=GET, op=POST).
    let request_counter = register_counter_vec!(
        opts!(
            "incoming_request_count",
            "Total number of requests handled."
        )
        .const_labels(const_labels.clone()),
        &["op"]
    )
    .expect("failed to register incoming_request_count counter");

    metrics.insert(
        "request_count".to_string(),
        Metrics::MetricsCounterVec(request_counter.clone()),
    );

    // The object counter maintains a count of the total number of objects that
    // have been processed (whether successfully or not).
    let object_counter = register_counter!(opts!(
        "object_count",
        "Total number of objects processed."
    )
    .const_labels(const_labels.clone()))
    .expect("failed to register object_count counter");

    metrics.insert(
        "object_count".to_string(),
        Metrics::MetricsCounter(object_counter.clone()),
    );

    // The error counter maintains a list of errors encountered, broken down by
    // the type of error observed.  Note that in order to avoid a polynomial
    // explosion of buckets here, one should have a reasonable idea of the
    // different kinds of possible errors that a given application could
    // encounter and in the event that there are too many possibilities, only
    // track certain error types and maintain the rest in a generic bucket.
    let error_counter = register_counter_vec!(
        opts!("error_count", "Errors encountered.")
            .const_labels(const_labels.clone()),
        &["error"]
    )
    .expect("failed to register error_count counter");

    metrics.insert(
        "error_count".to_string(),
        Metrics::MetricsCounterVec(error_counter.clone()),
    );

    metrics
}

// Start the metrics server on the address and port specified by the caller.
pub fn start_server(address: &str, port: u16, log: &Logger) {
    let addr = [&address, ":", &port.to_string()]
        .concat()
        .parse::<SocketAddr>()
        .unwrap();

    let log_clone = log.clone();

    let server = Server::bind(&addr)
        .serve(move || {
            service_fn_ok(move |_: Request<Body>| {
                // Gather all metrics from the default registry.
                let metric_families = prometheus::gather();
                let mut buffer = vec![];

                // Convert the MetricFamily message into text format and store
                // the result in `buffer' which will be in the payload of the
                // reponse to a request for metrics.
                let encoder = TextEncoder::new();
                encoder.encode(&metric_families, &mut buffer).unwrap();
                let content_type =
                    encoder.format_type().parse::<HeaderValue>().unwrap();

                // Send the response.
                Response::builder()
                    .header(CONTENT_TYPE, content_type)
                    .status(StatusCode::OK)
                    .body(Body::from(buffer))
                    .unwrap()
            })
        })
        .map_err(
            move |e| error!(log_clone, "metrics server error"; "error" => %e),
        );

    info!(log, "listening"; "address" => addr);

    rt::run(server);
}
