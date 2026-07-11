// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! `GET /debug/*` — human-friendly introspection of tracker state.
//!
//! Mirrors the API's debug endpoints in spirit: read-only JSON views of the
//! decision-making state so operators can see *why* an index will (or will not)
//! be built or evicted, without querying Postgres by hand.
//!
//! - `/debug/candidates`   — the ranked creation queue (same order the creation
//!   loop uses), each with its priority score.
//! - `/debug/created`      — currently active indexes with demand vs. supply.
//! - `/debug/discrepancies`— created indexes where demand and supply disagree.

use super::{AppState, json, text};
use crate::modules::prioritization;
use crate::modules::store::patterns::PatternRow;
use http_body_util::Full;
use hyper::body::Bytes;
use hyper::{Response, StatusCode};
use serde_json::json as jval;
use std::sync::Arc;
use tracing::error;

/// How many candidates the debug view returns (matches nothing operationally;
/// just a readable page size).
const DEBUG_CANDIDATE_LIMIT: u64 = 100;

pub async fn candidates(state: &Arc<AppState>) -> Response<Full<Bytes>> {
    let cfg = &state.config;
    let rows = match state
        .store
        .top_candidates(
            cfg.priority_mode,
            cfg.cost_weight,
            cfg.failure_weight,
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
        .map(|r| {
            jval!({
                "pattern_id": r.pattern_id,
                "index": r.human_name,
                "score": prioritization::score(
                    cfg.priority_mode, cfg.cost_weight, cfg.failure_weight,
                    r.demand_count, r.total_cost_us, r.failed_count,
                ),
                "demand_count": r.demand_count,
                "total_cost_us": r.total_cost_us,
                "failed_count": r.failed_count,
                "variety_estimate": r.variety_estimate,
            })
        })
        .collect();

    json(
        StatusCode::OK,
        &jval!({ "priority_mode": format!("{:?}", cfg.priority_mode), "candidates": items }),
    )
}

pub async fn created(state: &Arc<AppState>) -> Response<Full<Bytes>> {
    match state.store.list_created().await {
        Ok(rows) => json(StatusCode::OK, &jval!({ "created": rows.iter().map(created_view).collect::<Vec<_>>() })),
        Err(e) => db_error("created", e),
    }
}

pub async fn discrepancies(state: &Arc<AppState>) -> Response<Full<Bytes>> {
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

fn db_error(what: &str, e: sea_orm::DbErr) -> Response<Full<Bytes>> {
    error!(target: "query_tracker_server", "debug /{what} query failed: {e:?}");
    text(StatusCode::INTERNAL_SERVER_ERROR, "database error")
}
