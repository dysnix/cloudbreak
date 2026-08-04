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
//!
//! ## EXPLAIN-verdict filters (`filter=explain_*`)
//!
//! Narrow the full created set by the latest `explain_state` verdict — the
//! detailed, per-index source of truth behind the `query_tracker_explain_state`
//! gauge and the EXPLAIN pass's summary log. Rows with no verdict yet
//! (`explain_state = null`, e.g. `explain-enabled` off) are excluded.
//!
//! - `filter=explain_none` — planner would use the index on **neither** table.
//! - `filter=explain_partial` — used on exactly **one** table (`accounts_table`
//!   xor `snapshot_accounts_table`).
//! - `filter=explain_incomplete` — `none` **or** partial: everything not fully
//!   used on both tables (the go-to detailed view).
//!
//! Examples:
//! - `GET /debug/created?filter=explain_incomplete&verbose=true`
//! - `GET /debug/created?filter=explain_none&order=idx_scan` (unused yet scanned first)

use super::{
    DebugQuery, Dir, avg_ms, bad_request, bytes_to_mib, compensated_idx_scan, created_view,
    db_error, envelope, latency_gain, order_and_limit, round2,
};
use crate::modules::store::patterns::{PatternRow, explain_state};
use crate::server::{AppState, json};
use http_body_util::Full;
use hyper::body::Bytes;
use hyper::{Response, StatusCode};
use serde_json::{Value, json as jval};
use std::sync::Arc;

/// A created row plus its eviction `score` when the eviction-candidates filter
/// is active (`None` for the full listing).
type Row = (PatternRow, Option<f64>);

/// The `?filter=` row-subset selector for `/debug/created`.
enum CreatedFilter {
    /// Every `created` pattern (no filter).
    All,
    /// Only eviction-eligible rows, each carrying its `eviction_score`.
    Eviction,
    /// EXPLAIN verdict `none` — planner would use the index on neither table.
    ExplainNone,
    /// EXPLAIN verdict on exactly one table (`accounts_table` xor
    /// `snapshot_accounts_table`).
    ExplainPartial,
    /// `none` **or** partial — every index not fully used on both tables (rows
    /// with no verdict yet, i.e. `explain_state = null`, are excluded).
    ExplainIncomplete,
}

impl CreatedFilter {
    /// Drop rows that don't match an EXPLAIN-verdict filter, in place. A no-op
    /// for [`All`](Self::All)/[`Eviction`](Self::Eviction), whose row set is
    /// already decided by the query above.
    fn retain_explain(&self, rows: &mut Vec<Row>) {
        let keep = match self {
            CreatedFilter::ExplainNone => |s: Option<&str>| s == Some(explain_state::NONE),
            CreatedFilter::ExplainPartial => |s: Option<&str>| {
                matches!(s, Some(explain_state::ACCOUNTS | explain_state::SNAPSHOT))
            },
            CreatedFilter::ExplainIncomplete => |s: Option<&str>| {
                matches!(
                    s,
                    Some(explain_state::NONE | explain_state::ACCOUNTS | explain_state::SNAPSHOT)
                )
            },
            CreatedFilter::All | CreatedFilter::Eviction => return,
        };
        rows.retain(|(r, _)| keep(r.explain_state.as_deref()));
    }
}

pub async fn handle(state: &Arc<AppState>, query: Option<&str>) -> Response<Full<Bytes>> {
    let q = match DebugQuery::parse(query) {
        Ok(q) => q,
        Err(e) => return bad_request(e),
    };

    let filter = match q.filter.as_deref() {
        None => CreatedFilter::All,
        Some("eviction_candidates") => CreatedFilter::Eviction,
        Some("explain_none") => CreatedFilter::ExplainNone,
        Some("explain_partial") => CreatedFilter::ExplainPartial,
        Some("explain_incomplete") => CreatedFilter::ExplainIncomplete,
        Some(other) => {
            return bad_request(format!(
                "invalid filter '{other}' (created: eviction_candidates, explain_none, \
                 explain_partial, explain_incomplete)"
            ));
        }
    };
    let eviction = matches!(filter, CreatedFilter::Eviction);

    let cfg = &state.config;
    let mut rows: Vec<Row> = if eviction {
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

    // EXPLAIN-verdict filters narrow the full created set to the indexes whose
    // last probe found them not fully used — the detailed per-index source of
    // truth behind the `query_tracker_explain_state` gauge / summary log.
    filter.retain_explain(&mut rows);

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
