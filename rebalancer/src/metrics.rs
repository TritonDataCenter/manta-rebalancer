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
use slog::{error, info, Logger};
use serde_derive::Deserialize;

#[derive(Clone, Deserialize)]
pub struct MetricLabels {
    /// Rebalancer metrics server address
    pub host: String,
    /// Rebalancer metrics server port
    pub port: u16,
    pub datacenter: String,
    pub service: String,
    pub server: String,
}

impl Default for MetricLabels {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".into(),
            port: 3020,
            datacenter: "development".into(),
            service: "1.rebalancer.localhost".into(),
            server: "127.0.0.1".into(),
        }
    }
}

#[derive(Clone)]
pub struct RegisteredMetrics {
    pub request_count: CounterVec,
    pub object_count: Counter,
    pub error_count: CounterVec,
}

impl RegisteredMetrics {
    fn new(
        request_count: CounterVec,
        object_count: Counter,
        error_count: CounterVec,
    ) -> Self {
        RegisteredMetrics {
            request_count,
            object_count,
            error_count,
        }
    }
}

pub fn register_metrics(labels: &MetricLabels) -> RegisteredMetrics {
    let hostname = gethostname()
        .into_string()
        .unwrap_or_else(|_| String::from("unknown"));

    // Convert our MetricLabels structure to a HashMap since that is what
    // Prometheus requires when creating a new metric with labels.
    let mut const_labels = HashMap::new();
    const_labels.insert("service".to_string(), labels.service.clone());
    const_labels.insert("server".to_string(), labels.server.clone());
    const_labels.insert("datacenter".to_string(), labels.datacenter.clone());
    const_labels.insert("zonename".to_string(), hostname.clone());

    let request_counter = register_counter_vec!(opts!(
        "incoming_request_count",
        "Total number of requests handled."
    ).const_labels(const_labels.clone()), &["op"])
    .expect("failed to register incoming_request_count counter");

    let object_counter = register_counter!(opts!(
        "object_count",
        "Total number of objects processed."
    ).const_labels(const_labels.clone()))
    .expect("failed to register object_count counter");

    let error_counter = register_counter_vec!(opts!(
        "error_count",
        "Errors encountered."
    ).const_labels(const_labels.clone()), &["error"])
    .expect("failed to register error_count counter");

    RegisteredMetrics::new(
        request_counter,
        object_counter,
        error_counter
    )
}

pub fn start_server(
    address: &str,
    port: u16,
    log: &Logger,
) {
    let addr = [&address, ":", &port.to_string()]
        .concat()
        .parse::<SocketAddr>()
        .unwrap();

    let log_clone = log.clone();

    let server = Server::bind(&addr)
        .serve(move || {
            service_fn_ok(move |_: Request<Body>| {
                let metric_families = prometheus::gather();
                let mut buffer = vec![];
                let encoder = TextEncoder::new();
                encoder.encode(&metric_families, &mut buffer).unwrap();

                let content_type =
                    encoder.format_type().parse::<HeaderValue>().unwrap();

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
