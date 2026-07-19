// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! `GET /debug/discrepancies` — created indexes where demand and supply disagree.
//!
//! Read-only JSON view restricted to `created` patterns that carry a
//! discrepancy state (e.g. demand ≫ supply, or the reverse), for quickly
//! spotting indexes the planner is ignoring or that are no longer demanded.

use super::{created_view, db_error};
use crate::server::{AppState, json};
use http_body_util::Full;
use hyper::body::Bytes;
use hyper::{Response, StatusCode};
use serde_json::json as jval;
use std::sync::Arc;

pub async fn handle(state: &Arc<AppState>) -> Response<Full<Bytes>> {
    match state.store.list_created().await {
        Ok(rows) => {
            let items: Vec<_> = rows
                .iter()
                .filter(|r| r.discrepancy_state.is_some())
                .map(created_view)
                .collect();
            json(StatusCode::OK, &jval!({ "discrepancies": items }))
        }
        Err(e) => db_error("discrepancies", e),
    }
}
