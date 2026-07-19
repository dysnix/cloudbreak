// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! Prioritization — *which* candidate the tracker builds next.
//!
//! There is no separate priority queue anymore. Candidates live in
//! `index_patterns` and are ranked on read by translating the configured
//! [`PriorityMode`] into an `ORDER BY`. Because the score is derived from the
//! stored demand columns at query time, changing the mode (or its weights)
//! takes effect on the very next creation pass with no data migration.
//!
//! [`score`] reproduces the same ranking in Rust purely so the debug endpoint
//! can show operators the number each candidate is being ranked by.

use cloudbreak_core::PriorityMode;

/// SQL `ORDER BY` expression (highest priority first) for the given mode.
/// Weights are numeric and embedded directly; they never come from untrusted
/// input.
pub fn order_by_clause(mode: PriorityMode, cost_weight: f64, failure_weight: f64) -> String {
    match mode {
        PriorityMode::Frequency => "demand_count DESC".to_string(),
        PriorityMode::Cost => "total_cost_us DESC".to_string(),
        PriorityMode::CostPerHit => {
            "(total_cost_us::float8 / GREATEST(demand_count, 1)) DESC".to_string()
        }
        PriorityMode::Weighted => format!(
            "(demand_count::float8 + {cost_weight} * total_cost_us::float8 + {failure_weight} * failed_count::float8) DESC"
        ),
    }
}

/// Used in `/debug/candidates` endpoint to show the exact number driving the
/// ranking. This is **display-only**: the real ordering is done in SQL by
/// [`order_by_clause`] (the score is never materialized or stored), and this
/// function must mirror it mode-for-mode so the debug view matches what the
/// creation loop will actually pick.
pub fn score(
    mode: PriorityMode,
    cost_weight: f64,
    failure_weight: f64,
    demand_count: i64,
    total_cost_us: i64,
    failed_count: i64,
) -> f64 {
    match mode {
        PriorityMode::Frequency => demand_count as f64,
        PriorityMode::Cost => total_cost_us as f64,
        PriorityMode::CostPerHit => total_cost_us as f64 / (demand_count.max(1) as f64),
        PriorityMode::Weighted => {
            demand_count as f64
                + cost_weight * total_cost_us as f64
                + failure_weight * failed_count as f64
        }
    }
}
