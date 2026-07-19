// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! Operational endpoints — introspection and ops, one file per endpoint
//! (filename = endpoint path). Served on the same port as the functional
//! endpoints (see `server::serve`).
//!
//! - `debug_candidates`    — `GET /debug/candidates`    (ranked creation queue).
//! - `debug_created`       — `GET /debug/created`       (active indexes).
//! - `debug_discrepancies` — `GET /debug/discrepancies` (demand/supply divergence).
//! - `metrics`             — `GET /metrics`             (Prometheus text).
//! - `health`              — `GET /health`              (liveness probe).
//!
//! The `debug_*` endpoints render the same row shape, so the shared JSON view
//! and error helpers live here.

pub mod debug_candidates;
pub mod debug_created;
pub mod debug_discrepancies;
pub mod health;
pub mod metrics;

use crate::modules::store::patterns::PatternRow;
use crate::server::text;
use http_body_util::Full;
use hyper::body::Bytes;
use hyper::{Response, StatusCode};
use serde_json::json as jval;
use tracing::error;

/// JSON view of a created pattern's demand vs. supply, shared by the
/// `debug_created` and `debug_discrepancies` endpoints.
fn created_view(r: &PatternRow) -> serde_json::Value {
    jval!({
        "pattern_id": r.pattern_id,
        "index": r.human_name,
        "demand_count": r.demand_count,
        "demand_since_create": (r.demand_count - r.demand_at_create).max(0),
        "idx_scan": r.last_idx_scan,
        "index_bytes": r.index_bytes,
        "variety_estimate": r.variety_estimate,
        "discrepancy_state": r.discrepancy_state,
        "discrepancy_ratio": r.discrepancy_ratio,
    })
}

/// Log a debug-endpoint DB failure (with target) and return a 500.
fn db_error(what: &str, e: sea_orm::DbErr) -> Response<Full<Bytes>> {
    error!(target: "query_tracker_server", "debug /{what} query failed: {e:?}");
    text(StatusCode::INTERNAL_SERVER_ERROR, "database error")
}
