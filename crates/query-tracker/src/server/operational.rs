// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! Operational endpoints — `GET /metrics` and `GET /health`.
//!
//! Served on the dedicated metrics port so scraping is isolated from the ingest
//! API. The metric definitions live in `modules::metrics`; this only exposes
//! them.

use crate::modules::metrics;
use http_body_util::Full;
use hyper::body::{Bytes, Incoming};
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto;
use std::convert::Infallible;
use std::net::SocketAddr;
use tokio::net::TcpListener;
use tracing::{error, info};

pub async fn serve(addr: SocketAddr) {
    metrics::init();
    let listener = match TcpListener::bind(addr).await {
        Ok(l) => {
            info!(target: "query_tracker_metrics", "metrics server listening on http://{addr}/metrics");
            l
        }
        Err(e) => {
            error!(target: "query_tracker_metrics", "failed to bind metrics server to {addr}: {e}");
            return;
        }
    };

    loop {
        let (stream, _) = match listener.accept().await {
            Ok(pair) => pair,
            Err(e) => {
                error!(target: "query_tracker_metrics", "accept failed: {e:?}");
                continue;
            }
        };
        let io = TokioIo::new(stream);
        tokio::spawn(async move {
            let service = service_fn(handle);
            let builder = auto::Builder::new(TokioExecutor::new());
            if let Err(e) = builder.serve_connection(io, service).await {
                error!(target: "query_tracker_metrics", "connection error: {e:?}");
            }
        });
    }
}

async fn handle(req: Request<Incoming>) -> Result<Response<Full<Bytes>>, Infallible> {
    let resp = match req.uri().path() {
        "/metrics" => Response::builder()
            .header("content-type", "text/plain")
            .body(Full::new(Bytes::from(metrics::encode())))
            .unwrap(),
        "/health" => Response::builder()
            .status(StatusCode::OK)
            .body(Full::new(Bytes::from("OK")))
            .unwrap(),
        _ => Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Full::new(Bytes::from("Not Found")))
            .unwrap(),
    };
    Ok(resp)
}
