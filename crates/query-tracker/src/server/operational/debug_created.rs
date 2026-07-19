// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! `GET /debug/created` — currently active auto-created indexes.
//!
//! Read-only JSON view of every `created` pattern with its demand (API) vs.
//! supply (Postgres `idx_scan`) figures, so operators can see what is built and
//! how it is being used.

use super::{created_view, db_error};
use crate::server::{AppState, json};
use http_body_util::Full;
use hyper::body::Bytes;
use hyper::{Response, StatusCode};
use serde_json::json as jval;
use std::sync::Arc;

pub async fn handle(state: &Arc<AppState>) -> Response<Full<Bytes>> {
    match state.store.list_created().await {
        Ok(rows) => json(
            StatusCode::OK,
            &jval!({ "created": rows.iter().map(created_view).collect::<Vec<_>>() }),
        ),
        Err(e) => db_error("created", e),
    }
}
