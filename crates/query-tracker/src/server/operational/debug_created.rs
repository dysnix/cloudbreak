// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! `GET /debug/created` — currently active auto-created indexes.
//!
//! Read-only JSON view of every `created` pattern with its demand (API) vs.
//! supply (`compensated_idx_scan`) figures, on-disk size (`index_mb`), latency
//! with/without the index and their `latency_gain`, and the latest
//! `explain_state` verdict — so operators can see what is built and how it is
//! being used.
//!
//! ## Query parameters
//!
//! - `order` — `created_at` (default, newest first), `index_mb`, `demand_count`,
//!   `idx_scan` (compensated), `avg_cost_with_index_ms`, `latency_gain`,
//!   `variety_estimate`, and `score` (only with `filter=eviction_candidates`,
//!   the default there, ascending = least useful first).
//! - `dir` — `asc` | `desc` (default `desc`, except `score` defaults to `asc`).
//! - `limit` — max rows (default: all).
//! - `example`, `pattern_id`, `verbose` — include the heavier fields.
//!
//! Examples:
//! - `GET /debug/created`
//! - `GET /debug/created?order=index_mb&limit=10`
//! - `GET /debug/created?order=variety_estimate&dir=desc`
//!
//! ## Eviction candidates (`filter=eviction_candidates`)
//!
//! Restricts the view to the created indexes the eviction pass would
//! *consider*: those past the idle + age-grace gates (`index-min-idle` /
//! `index-min-age-grace`), each annotated with its `eviction_score` (the inverse
//! of the creation ranking) and ordered least-useful-first by default
//! (`order=score`, ascending).
//!
//! This is the **eligible queue, not a guarantee**: the actual drop additionally
//! depends on the table being above the fill target (`eviction-fill-threshold`)
//! at runtime — eviction only trims the buffer band back down to the target — so
//! an index listed here may still be kept. Omit the filter for the full created
//! set.
//!
//! Examples:
//! - `GET /debug/created?filter=eviction_candidates` (the eviction queue)
//! - `GET /debug/created?filter=eviction_candidates&order=index_mb&verbose=true`

use super::{
    DebugQuery, Dir, avg_ms, bad_request, bytes_to_mib, compensated_idx_scan, created_view,
    db_error, envelope, latency_gain, order_and_limit, round2,
};
use crate::modules::store::patterns::PatternRow;
use crate::server::{AppState, json};
use http_body_util::Full;
use hyper::body::Bytes;
use hyper::{Response, StatusCode};
use serde_json::{Value, json as jval};
use std::sync::Arc;

/// A created row plus its eviction `score` when the eviction-candidates filter
/// is active (`None` for the full listing).
type Row = (PatternRow, Option<f64>);

pub async fn handle(state: &Arc<AppState>, query: Option<&str>) -> Response<Full<Bytes>> {
    let q = match DebugQuery::parse(query) {
        Ok(q) => q,
        Err(e) => return bad_request(e),
    };

    let eviction = match q.filter.as_deref() {
        None => false,
        Some("eviction_candidates") => true,
        Some(other) => {
            return bad_request(format!(
                "invalid filter '{other}' (created: eviction_candidates)"
            ));
        }
    };

    let cfg = &state.config;
    let rows: Vec<Row> = if eviction {
        match state
            .store
            .eviction_candidates(
                cfg.priority_mode,
                cfg.without_index_compensation_factor,
                cfg.index_min_idle.as_secs() as i64,
                cfg.index_min_age_grace.as_secs() as i64,
            )
            .await
        {
            Ok(v) => v.into_iter().map(|(r, s)| (r, Some(s))).collect(),
            Err(e) => return db_error("created", e),
        }
    } else {
        match state.store.list_created().await {
            Ok(v) => v.into_iter().map(|r| (r, None)).collect(),
            Err(e) => return db_error("created", e),
        }
    };

    let order = q
        .order
        .as_deref()
        .unwrap_or(if eviction { "score" } else { "created_at" });
    let key: fn(&Row) -> f64 = match order {
        "created_at" => |x| x.0.created_at_epoch.unwrap_or(0.0),
        "index_mb" => |x| bytes_to_mib(x.0.index_bytes),
        "demand_count" => |x| x.0.demand_count as f64,
        "idx_scan" => |x| compensated_idx_scan(x.0.last_idx_scan) as f64,
        "avg_cost_with_index_ms" => {
            |x| avg_ms(x.0.cost_with_index_us, x.0.cost_with_index_count).unwrap_or(0.0)
        }
        "latency_gain" => |x| latency_gain(&x.0).unwrap_or(0.0),
        "variety_estimate" => |x| x.0.variety_estimate as f64,
        "score" if eviction => |x| x.1.unwrap_or(0.0),
        other => {
            return bad_request(format!(
                "invalid order '{other}' (created: created_at, index_mb, demand_count, idx_scan, \
                 avg_cost_with_index_ms, latency_gain, variety_estimate; score with \
                 filter=eviction_candidates)"
            ));
        }
    };
    // Eviction score ascends (least useful first); everything else descends.
    let dir = q.dir.unwrap_or(if order == "score" {
        Dir::Asc
    } else {
        Dir::Desc
    });

    let (total, rows) = order_and_limit(rows, key, dir, q.limit);
    let items: Vec<Value> = rows
        .iter()
        .map(|(r, score)| {
            let mut view = created_view(r, &q);
            if let Some(s) = score {
                view.as_object_mut()
                    .expect("created_view built an object")
                    .insert("eviction_score".into(), jval!(round2(*s)));
            }
            view
        })
        .collect();

    json(StatusCode::OK, &envelope("created", total, q.limit, items))
}
