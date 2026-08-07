// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use std::convert::Infallible;

use http_body_util::Full;
use hyper::{Response, StatusCode, body::Bytes};

use crate::modules::health::ServiceHealth;

pub(crate) fn handle(health: &ServiceHealth) -> Result<Response<Full<Bytes>>, Infallible> {
    Ok(response(health.is_healthy()))
}

fn response(healthy: bool) -> Response<Full<Bytes>> {
    Response::builder()
        .status(if healthy {
            StatusCode::OK
        } else {
            StatusCode::SERVICE_UNAVAILABLE
        })
        .body(Full::new(Bytes::from_static(if healthy {
            b"ok"
        } else {
            b"unhealthy"
        })))
        .unwrap()
}

#[cfg(test)]
mod tests {
    use hyper::StatusCode;

    use super::response;

    #[test]
    fn ready_only_when_service_is_healthy() {
        assert_eq!(response(true).status(), StatusCode::OK);
        assert_eq!(response(false).status(), StatusCode::SERVICE_UNAVAILABLE);
    }
}
