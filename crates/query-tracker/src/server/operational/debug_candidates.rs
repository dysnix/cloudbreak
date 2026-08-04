// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! `GET /debug/candidates` — the ranked creation queue.
//!
//! Read-only JSON view of the candidates in the exact order the creation loop
//! would pick them, each annotated with its priority score under the active
//! [`PriorityMode`](cloudbreak_core::PriorityMode), so operators can see *why*
//! an index will (or will not) be built without querying Postgres by hand.

use super::db_error;
use crate::server::{AppState, json};
use http_body_util::Full;
use hyper::body::Bytes;
use hyper::{Response, StatusCode};
use serde_json::json as jval;
use std::sync::Arc;

/// How many candidates the debug view returns; just a readable page size.
const DEBUG_CANDIDATE_LIMIT: u64 = 100;

pub async fn handle(state: &Arc<AppState>) -> Response<Full<Bytes>> {
    let cfg = &state.config;
    let rows = match state
        .store
        .top_candidates_scored(
            cfg.priority_mode,
            cfg.without_index_compensation_factor,
            cfg.index_generation_threshold,
            cfg.cost_eligibility_threshold_us,
            DEBUG_CANDIDATE_LIMIT,
        )
        .await
    {
        Ok(r) => r,
        Err(e) => return db_error("candidates", e),
    };

    let items: Vec<_> = rows
        .iter()
        .map(|(r, score)| {
            jval!({
                "pattern_id": r.pattern_id,
                "index": r.human_name,
                "score": score,
                "demand_count": r.demand_count,
                "total_cost_us": r.total_cost_us,
                "failed_count": r.failed_count,
                "variety_estimate": r.variety_estimate,
                "example_request": r.example_request,
            })
        })
        .collect();

    json(
        StatusCode::OK,
        &jval!({ "priority_mode": format!("{:?}", cfg.priority_mode), "candidates": items }),
    )
}
