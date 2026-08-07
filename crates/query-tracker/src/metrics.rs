// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use std::{
    convert::Infallible,
    net::SocketAddr,
    sync::atomic::{AtomicBool, Ordering},
};

use http_body_util::Full;
use hyper::{
    Request, Response,
    body::{Bytes, Incoming},
    service::service_fn,
};
use hyper_util::{rt::TokioIo, server::conn::auto};
use prometheus::{IntGauge, Registry, TextEncoder};
use tokio::net::TcpListener;
use tracing::{error, info};

static READY: AtomicBool = AtomicBool::new(false);

pub fn set_ready(ready: bool) {
    READY.store(ready, Ordering::Relaxed);
}

lazy_static::lazy_static! {
    static ref METRICS_REGISTRY: Registry = Registry::new();

    /// Current number of indexes present on the `snapshot_accounts` table.
    pub static ref SNAPSHOT_ACCOUNTS_INDEXES: IntGauge = {
        let gauge = IntGauge::new(
            "query_tracker_snapshot_accounts_indexes_total",
            "Current number of indexes on the snapshot_accounts table",
        )
        .expect("failed to create snapshot_accounts indexes gauge");
        METRICS_REGISTRY
            .register(Box::new(gauge.clone()))
            .expect("failed to register snapshot_accounts indexes gauge");
        gauge
    };
}

fn metrics_handler() -> Result<Response<Full<Bytes>>, Infallible> {
    let metrics = TextEncoder::new()
        .encode_to_string(&METRICS_REGISTRY.gather())
        .unwrap_or_else(|error| {
            error!("could not encode custom metrics: {error}");
            String::new()
        });

    Ok(Response::builder()
        .header("content-type", "text/plain")
        .body(Full::new(Bytes::from(metrics)))
        .unwrap())
}

fn health_handler(ready: bool) -> Result<Response<Full<Bytes>>, Infallible> {
    Ok(Response::builder()
        .status(if ready { 200 } else { 503 })
        .body(Full::new(Bytes::from(if ready {
            "OK"
        } else {
            "Not Ready"
        })))
        .unwrap())
}

async fn handle_metrics_request(
    req: Request<Incoming>,
) -> Result<Response<Full<Bytes>>, Infallible> {
    match req.uri().path() {
        "/metrics" => metrics_handler(),
        "/health" => health_handler(READY.load(Ordering::Relaxed)),
        _ => Ok(Response::builder()
            .status(404)
            .body(Full::new(Bytes::from("Not Found")))
            .unwrap()),
    }
}

#[cfg(test)]
mod tests {
    use hyper::StatusCode;

    use super::health_handler;

    #[test]
    fn health_is_ready_only_after_rpc_server_starts() {
        assert_eq!(health_handler(true).unwrap().status(), StatusCode::OK);
        assert_eq!(
            health_handler(false).unwrap().status(),
            StatusCode::SERVICE_UNAVAILABLE
        );
    }
}

pub async fn serve_metrics(addr: SocketAddr) {
    let listener = match TcpListener::bind(addr).await {
        Ok(l) => {
            info!("Prometheus server started at http://{}/metrics", addr);
            l
        }
        Err(e) => {
            error!("Failed to bind metrics server to {}: {}", addr, e);
            return;
        }
    };

    loop {
        let (stream, _) = match listener.accept().await {
            Ok(pair) => pair,
            Err(e) => {
                error!("Prometheus accept failed: {e:?}");
                continue;
            }
        };

        let io = TokioIo::new(stream);
        let service = service_fn(move |req: Request<Incoming>| handle_metrics_request(req));

        tokio::spawn(async move {
            let builder = auto::Builder::new(hyper_util::rt::TokioExecutor::new());
            let conn = builder.serve_connection(io, service);
            if let Err(e) = conn.await {
                error!("Prometheus connection failed: {e:?}");
            }
        });
    }
}
