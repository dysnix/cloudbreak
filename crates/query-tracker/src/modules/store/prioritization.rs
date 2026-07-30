// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! Prioritization — the *single* criterion shared by creation and eviction.
//!
//! There is no separate priority queue. Patterns live in `index_patterns` and
//! are ranked on read by translating the configured [`PriorityMode`] into a SQL
//! score *expression* ([`score_expr`]). Creation sorts that expression
//! **descending** (build the highest first); eviction sorts the *same*
//! expression **ascending** (drop the lowest first). So "what we most want to
//! build" and "what we least mind dropping" are, by construction, two ends of
//! one ordering.
//!
//! Most modes rank on lifetime totals. [`PriorityMode::Weighted`] instead ranks
//! on **windowed** activity (the per-window counts maintained by the score roll
//! task, see `store::Store::roll_scores`), so decisions track *current*
//! throughput rather than all-time popularity. Until a pattern has been rolled
//! once its `*_rate` columns are `NULL`; the expression then falls back to the
//! running total so a fresh pattern is still ranked within its first window.
//!
//! `Weighted` also folds in the measured **latency gain** — how much faster the
//! pattern is served *with* the index than without (see [`gain_expr`]) — as a
//! multiplier on `avg_cost`. Unlike the windowed counts this uses lifetime
//! averages (stable ratios), and stays neutral until the pattern has served
//! requests both with and without the index.

use cloudbreak_core::PriorityMode;

/// Index scans Postgres records per served request. A
/// `getProgramAccounts` is a `UNION ALL` over the `accounts` and
/// `snapshot_accounts` tables, so a single request scans **both** indexes of the
/// pair → ~2 `idx_scan` increments per request. Supply is therefore divided by
/// this before being compared to demand (here and in `stats::discrepancy`).
pub const SCANS_PER_REQUEST: i64 = 2;

/// SQL ranking expression for `mode` (higher = higher priority). Creation
/// appends `DESC`, eviction appends `ASC`; the expression itself is direction-
/// agnostic so both stay in lockstep. Weights in [`PriorityMode::Weighted`] are
/// numeric and embedded directly; they never come from untrusted input.
pub fn score_expr(mode: PriorityMode) -> String {
    match mode {
        PriorityMode::Frequency => "demand_count".to_string(),
        PriorityMode::Cost => "total_cost_us".to_string(),
        PriorityMode::CostPerHit => avg_cost_expr(),
        PriorityMode::Weighted {
            demand_weight,
            supply_weight,
            failure_weight,
            latency_weight,
            ..
        } => {
            // Per-window quantities, bootstrapped to the running total until the
            // first roll materializes a real window delta.
            let demand = "COALESCE(demand_rate, demand_count)";
            let supply = "COALESCE(supply_rate, last_idx_scan)";
            let failed = "COALESCE(failed_rate, failed_count)";
            let spr = SCANS_PER_REQUEST as f64;
            // `avg_cost` scaled by the latency gain (neutral = 1). The `+ 1`
            // baseline keeps the volume factor non-zero, so a zero-activity
            // pattern ranks by `avg_cost * gain` alone (and idle eviction
            // candidates never all tie at zero).
            format!(
                "({avg} * (1 + {lw} * LN({gain}))) * \
                 (1 + {dw} * {demand} + {sw} * ({supply}::float8 / {spr}) + {fw} * {failed})",
                avg = avg_cost_expr(),
                lw = latency_weight,
                gain = gain_expr(),
                dw = demand_weight,
                sw = supply_weight,
                fw = failure_weight,
            )
        }
    }
}

/// Average cost per request in microseconds; a ratio, so it is identical whether
/// measured over a window or over all time.
fn avg_cost_expr() -> String {
    "total_cost_us::float8 / GREATEST(demand_count, 1)".to_string()
}

/// Ratio of the without-index average cost to the with-index average — how many
/// times faster the pattern is served with the index (`> 1` helps, `< 1` hurts).
/// Returns `1` (neutral) until the pattern has served requests both with and
/// without the index, so a ratio can actually be formed. Averages are floored at
/// 1µs so the ratio (and the `LN` taken of it in [`score_expr`]) stays finite
/// regardless of how cheap a side became.
pub fn gain_expr() -> String {
    "CASE WHEN cost_with_index_count > 0 AND cost_without_index_count > 0 \
          THEN GREATEST(cost_without_index_us::float8 / GREATEST(cost_without_index_count, 1), 1) \
             / GREATEST(cost_with_index_us::float8 / GREATEST(cost_with_index_count, 1), 1) \
          ELSE 1 END"
        .to_string()
}
