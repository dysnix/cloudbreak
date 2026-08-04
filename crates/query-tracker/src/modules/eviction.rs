// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! Eviction — reclaiming index budget, carefully.
//!
//! One pass per `index-eviction-interval` that, in order:
//!
//! 0. **Latency regression guard** (when `index-regression-guard` is not `off`).
//!    Demand-side only, so it runs before the supply refresh and ignores the
//!    fill threshold: any created index measured *slower* than the pattern was
//!    without it (by `index-regression-ratio`, once both sides have
//!    `index-latency-min-samples`) is either warned about or dropped and marked
//!    `rejected`. A rejected pattern is not rebuilt until fresh without-index
//!    samples show the index would help again (see `creation`).
//! 1. **Refreshes supply.** Reads `idx_scan`/size per auto-index from Postgres
//!    (summed across partition leaves), folds it into each pattern's row and the
//!    per-index metrics. Skipped entirely if `track_counts` is off (stats
//!    frozen), since stale `idx_scan` must never drive a drop.
//! 2. **Flags discrepancies** (see `stats::discrepancy`) — records the
//!    demand-vs-supply verdict for observability only. It does not gate drops: a
//!    starved-but-demanded index is safe simply because step 3 requires *both*
//!    demand and scans to be idle, so a still-demanded index is never a drop
//!    candidate.
//! 3. **Drops** — only when the capped table is past `eviction-fill-threshold`
//!    — the idlest pairs (no demand *and* no scans for `index-min-idle`, older
//!    than `index-min-age-grace`). Below the fill line nothing is dropped. Once
//!    the count reaches `max-auto-indexes`, index creation is paused until
//!    eviction makes room again; all candidates are kept queued in
//!    `index_patterns` with no loss of data, trading a little index budget for
//!    stability.
//!
//! ## Value guard — never trade down
//!
//! An eviction only pays off if the slot it frees gets refilled with something
//! *more valuable*. So each drop is checked against the pending creation it
//! would make room for, both scored by the same [`prioritization::score_expr`]:
//! the least-useful eviction candidate is paired with the top creation
//! candidate, the 2nd-least with the 2nd, and so on. A pair is only dropped when
//! `evict_score < create_score`.
//!
//! Because eviction candidates ascend in score and creation candidates descend,
//! the gap `evict − create` only widens with rank — so the *first* pair that
//! fails the check means no later pair can pass, and we stop the pass entirely.
//! Running out of creation candidates stops it too: with nothing more valuable
//! waiting, freeing another slot would only shed value. A consequence is that
//! the table may sit above the fill target when there is nothing worth swapping
//! in — deliberately, since evicting for its own sake destroys value.
//!
//! The comparison inherits the priority mode's notion of "value": lifetime-total
//! modes weigh an idle index by its *all-time* worth (so a once-hot index
//! resists eviction), while [`Weighted`](cloudbreak_core::PriorityMode::Weighted)
//! weighs **recent windowed** activity, so decisions track current throughput.
//!
//! Drops are gated on indexer backpressure and run with a bounded `lock_timeout`
//! plus a configurable retry; a drop that cannot take its lock is logged and
//! left for the next pass rather than blocking ingest.

use crate::modules::store::patterns::status;
use crate::modules::store::{prioritization, Store};
use crate::modules::{CAP_TABLE, INDEX_TABLES, creation, indexer_backpressure};
use crate::stats::discrepancy::{self, DiscrepancyState};
use crate::stats::metrics;
use cloudbreak_core::{IndexRegressionGuard, QueryTrackerConfig};
use cloudbreak_core::modules::index_identity::IndexIdentity;
use sea_orm::{ConnectionTrait, DbErr, Statement, TransactionTrait};
use std::collections::HashMap;
use tracing::{error, info, warn};

#[tracing::instrument(name = "query_tracker_eviction", skip_all)]
pub async fn run(store: Store, config: QueryTrackerConfig) {
    info!(
        target: "query_tracker_eviction",
        "eviction task started (interval: {:?}, min-idle: {:?}, min-age-grace: {:?}, fill-threshold: {})",
        config.index_eviction_interval, config.index_min_idle, config.index_min_age_grace,
        config.eviction_fill_threshold
    );

    loop {
        tokio::time::sleep(config.index_eviction_interval).await;
        if let Err(e) = run_pass(&store, &config).await {
            error!(target: "query_tracker_eviction", "eviction pass failed: {e:?}");
        }
    }
}

async fn run_pass(store: &Store, config: &QueryTrackerConfig) -> Result<(), DbErr> {
    // Latency regression guard first. It compares demand-side with/without-index
    // cost, so it needs neither Postgres stats nor the fill threshold — a
    // harmful index is dropped even when the table has room.
    if config.index_regression_guard != IndexRegressionGuard::Off {
        run_regression_guard(store, config).await?;
    }

    if !store.track_counts_enabled().await? {
        warn!(
            target: "query_tracker_eviction",
            "track_counts is off; idx_scan is frozen — skipping pass to avoid dropping in-use indexes"
        );
        return Ok(());
    }

    let supply = supply_by_pattern(store).await?;
    refresh_supply_and_discrepancy(store, config, &supply).await?;
    refresh_aggregate_metrics(store).await;

    // Dropping requires a cap to define "full"; without one we never evict.
    let Some(max) = config.max_auto_indexes else {
        return Ok(());
    };
    let current = store.count_table_indexes(CAP_TABLE).await?;
    metrics::SNAPSHOT_ACCOUNTS_INDEXES.set(current);
    let fill = current as f64 / max as f64;
    if fill < config.eviction_fill_threshold {
        return Ok(());
    }

    let min_idle = config.index_min_idle.as_secs() as i64;
    let min_age = config.index_min_age_grace.as_secs() as i64;
    // Least-useful-first, each with its score (the inverse of the creation
    // ranking), so we drop straight from the top until back at target.
    let candidates = store
        .eviction_candidates(
            config.priority_mode,
            config.without_index_compensation_factor,
            min_idle,
            min_age,
        )
        .await?;
    if candidates.is_empty() {
        return Ok(());
    }

    // Value guard: the score each freed slot would be refilled with, highest
    // first. Paired rank-for-rank against `candidates` below; we never drop an
    // index worth as much as the best pending creation it would displace.
    let creation_scores = pending_creation_scores(store, config, candidates.len() as u64).await?;

    let target = (config.eviction_fill_threshold * max as f64).floor() as i64;
    let mut count = current;
    let mut evicted = 0usize;
    let mut reclaimed: i64 = 0;

    for (rank, (row, evict_score)) in candidates.iter().enumerate() {
        if count <= target {
            break;
        }
        // Only drop when a strictly more valuable creation is waiting for the
        // slot. Scores ascend here and descend in `creation_scores`, so the
        // first failing (or missing) counterpart means every later one fails
        // too — stop the whole pass rather than trade value away.
        match creation_scores.get(rank) {
            Some(create_score) if *evict_score < *create_score => {}
            Some(create_score) => {
                info!(
                    target: "query_tracker_eviction",
                    "stopping eviction at '{}': its score {evict_score:.3} is not below the best \
                     pending creation for this slot ({create_score:.3}); remaining candidates rank higher",
                    row.human_name
                );
                break;
            }
            None => {
                info!(
                    target: "query_tracker_eviction",
                    "stopping eviction at '{}': no pending creation left to justify freeing another slot",
                    row.human_name
                );
                break;
            }
        }
        if indexer_backpressure::is_under_pressure(
            &config.indexer_metrics,
            config.indexer_metrics_threshold,
        )
        .await
        {
            info!(target: "query_tracker_eviction", "indexer under pressure; pausing evictions");
            break;
        }
        let identity = match row.identity() {
            Ok(i) => i,
            Err(e) => {
                error!(target: "query_tracker_eviction", "skipping eviction candidate: {e}");
                continue;
            }
        };
        match drop_pair(store, &identity, config).await {
            Ok(()) => {
                if let Err(e) = store.mark_evicted(&row.pattern_id).await {
                    error!(target: "query_tracker_eviction", "failed to mark evicted: {e:?}");
                }
                metrics::INDEX_EVICTED_TOTAL.inc();
                clear_index_metrics(&row.human_name);
                reclaimed += row.index_bytes;
                evicted += 1;
                count -= 1;
            }
            Err(e) => {
                warn!(
                    target: "query_tracker_eviction",
                    "failed to evict '{}': {e}; will retry next pass", identity.human_name()
                );
            }
        }
    }

    if evicted > 0 {
        info!(
            target: "query_tracker_eviction",
            "evicted {evicted} idle index pair(s), reclaiming ~{reclaimed} bytes"
        );
    }
    Ok(())
}

/// Latency regression guard: find created indexes that measure **slower** than
/// the pattern was without them and, per `index-regression-guard`, either warn
/// or drop-and-`reject` them. Drops honour backpressure and the same
/// `lock_timeout`/retry path as idle eviction; a rejected pattern is not rebuilt
/// until [`Store::promote_recovered_rejections`] sees fresh contrary evidence.
async fn run_regression_guard(store: &Store, config: &QueryTrackerConfig) -> Result<(), DbErr> {
    let rows = store
        .regression_candidates(
            config.index_min_age_grace.as_secs() as i64,
            config.index_regression_ratio,
            config.without_index_compensation_factor,
        )
        .await?;

    // Current count of created indexes in the regressed state (before any drops
    // this pass move them to `rejected`).
    metrics::REGRESSED_INDEXES.set(rows.len() as i64);

    for row in &rows {
        let avg_with = avg_us(row.cost_with_index_us, row.cost_with_index_count);
        let avg_without_raw = avg_us(row.cost_without_index_us, row.cost_without_index_count);
        let avg_without = avg_without_raw * config.without_index_compensation_factor;

        match config.index_regression_guard {
            IndexRegressionGuard::Warn => {
                warn!(
                    target: "query_tracker_regression",
                    "index '{}' is slower WITH the index than without: with~{avg_with:.0}us vs \
                     without~{avg_without:.0}us (raw~{avg_without_raw:.0}us ×{:.2} compensation; \
                     >{:.2}x; {}/{} with/without requests); warn mode, keeping it",
                    row.human_name, config.without_index_compensation_factor,
                    config.index_regression_ratio,
                    row.cost_with_index_count, row.cost_without_index_count
                );
            }
            IndexRegressionGuard::Evict => {
                if indexer_backpressure::is_under_pressure(
                    &config.indexer_metrics,
                    config.indexer_metrics_threshold,
                )
                .await
                {
                    info!(target: "query_tracker_regression", "indexer under pressure; deferring regression drops");
                    break;
                }
                let identity = match row.identity() {
                    Ok(i) => i,
                    Err(e) => {
                        error!(target: "query_tracker_regression", "skipping regressed pattern: {e}");
                        continue;
                    }
                };
                match drop_pair(store, &identity, config).await {
                    Ok(()) => {
                        if let Err(e) = store.mark_rejected(&row.pattern_id).await {
                            error!(target: "query_tracker_regression", "failed to mark rejected: {e:?}");
                        }
                        metrics::INDEX_EVICTED_TOTAL.inc();
                        clear_index_metrics(&row.human_name);
                        warn!(
                            target: "query_tracker_regression",
                            "dropped regressed index '{}' (with~{avg_with:.0}us > without~{avg_without:.0}us); \
                             marked rejected — will not rebuild until it is slower without it",
                            row.human_name
                        );
                    }
                    Err(e) => warn!(
                        target: "query_tracker_regression",
                        "failed to drop regressed index '{}': {e}; will retry next pass",
                        identity.human_name()
                    ),
                }
            }
            IndexRegressionGuard::Off => {}
        }
    }
    Ok(())
}

/// Average microseconds per request for a `(sum_us, count)` bucket.
fn avg_us(sum_us: i64, count: i64) -> f64 {
    sum_us as f64 / count.max(1) as f64
}

/// Scores of the patterns creation would build next, highest first — the value
/// each freed slot would be refilled with. Mirrors the creation loop's own
/// gates (demand threshold, cost eligibility, program allow/deny) so the guard
/// compares against what would *actually* be created, not merely what exists.
async fn pending_creation_scores(
    store: &Store,
    config: &QueryTrackerConfig,
    limit: u64,
) -> Result<Vec<f64>, DbErr> {
    let scored = store
        .top_candidates_scored(
            config.priority_mode,
            config.without_index_compensation_factor,
            config.index_generation_threshold,
            config.cost_eligibility_threshold_us,
            limit,
        )
        .await?;
    let mut scores = Vec::with_capacity(scored.len());
    for (row, score) in scored {
        match row.identity() {
            Ok(identity) if creation::program_allowed(identity.program, config) => {
                scores.push(score);
            }
            // Not program-allowed: would never be built, so it is not value we
            // could gain — skip without consuming a pairing slot.
            Ok(_) => {}
            Err(e) => {
                error!(target: "query_tracker_eviction", "skipping creation candidate in value guard: {e}");
            }
        }
    }
    Ok(scores)
}

/// Aggregate the raw `(index_name, idx_scan, bytes)` rows into per-`pattern_id`
/// supply by stripping the table prefix and summing the pair.
async fn supply_by_pattern(store: &Store) -> Result<HashMap<String, (i64, i64)>, DbErr> {
    let mut by_pattern: HashMap<String, (i64, i64)> = HashMap::new();
    for (name, idx_scan, bytes) in store.read_auto_index_supply().await? {
        let Some(pattern_id) = strip_index_prefix(&name) else {
            continue;
        };
        let entry = by_pattern.entry(pattern_id).or_default();
        entry.0 += idx_scan;
        entry.1 += bytes;
    }
    Ok(by_pattern)
}

/// Fold supply into each created pattern, update per-index metrics, and evaluate
/// discrepancies.
async fn refresh_supply_and_discrepancy(
    store: &Store,
    config: &QueryTrackerConfig,
    supply: &HashMap<String, (i64, i64)>,
) -> Result<(), DbErr> {
    for row in store.list_created().await? {
        let (idx_scan, bytes) = supply.get(&row.pattern_id).copied().unwrap_or((0, 0));
        store
            .update_supply(&row.pattern_id, idx_scan, bytes)
            .await?;

        metrics::INDEX_IDX_SCAN
            .with_label_values(&[&row.human_name])
            .set(idx_scan);
        metrics::INDEX_DEMAND
            .with_label_values(&[&row.human_name])
            .set(row.demand_count);
        metrics::INDEX_VARIETY
            .with_label_values(&[&row.human_name])
            .set(row.variety_estimate);

        if config.discrepancy_enabled {
            let demand_since_create = (row.demand_count - row.demand_at_create).max(0);
            // A served request scans both tables of the pair, so raw idx_scan is
            // ~2x the request count; normalize before comparing to demand.
            let supply = idx_scan / prioritization::SCANS_PER_REQUEST;
            let (state, ratio) = discrepancy::evaluate(
                demand_since_create,
                supply,
                config.discrepancy_delta,
                config.index_generation_threshold as i64,
            );
            store
                .set_discrepancy(&row.pattern_id, state.as_stored(), ratio)
                .await?;
            if state == DiscrepancyState::Starved {
                warn!(
                    target: "query_tracker_discrepancy",
                    "index '{}' looks STARVED: demand~{demand_since_create} but supply~{supply} \
                     (idx_scan={idx_scan}, ratio {:.2}); Postgres may be ignoring it — \
                     check ANALYZE/planner, not evicting",
                    row.human_name, ratio.unwrap_or(0.0)
                );
            }
        }
    }
    Ok(())
}

async fn refresh_aggregate_metrics(store: &Store) {
    match store.counts().await {
        Ok(c) => {
            metrics::PATTERNS
                .with_label_values(&[status::CREATED])
                .set(c.created);
            metrics::PATTERNS
                .with_label_values(&[status::CANDIDATE])
                .set(c.candidate);
            metrics::PATTERNS
                .with_label_values(&[status::EVICTED])
                .set(c.evicted);
            metrics::PATTERNS
                .with_label_values(&[status::REJECTED])
                .set(c.rejected);
            metrics::DISCREPANT_INDEXES.set(c.discrepant);
        }
        Err(e) => error!(target: "query_tracker_eviction", "failed to refresh metrics: {e:?}"),
    }
}

/// Drop both sides of an index pair, honouring `lock_timeout` and retries.
async fn drop_pair(
    store: &Store,
    identity: &IndexIdentity,
    config: &QueryTrackerConfig,
) -> Result<(), String> {
    for table in INDEX_TABLES {
        drop_one(store, &identity.pg_index_name(table), config).await?;
    }
    Ok(())
}

async fn drop_one(
    store: &Store,
    index_name: &str,
    config: &QueryTrackerConfig,
) -> Result<(), String> {
    if !is_safe_index_identifier(index_name) {
        return Err(format!(
            "refusing to drop unexpected index name '{index_name}'"
        ));
    }
    let backend = store.db().get_database_backend();
    let lock_ms = config.drop_lock_timeout.as_millis();

    let mut attempt = 0u32;
    loop {
        let result: Result<(), DbErr> = async {
            let txn = store.db().begin().await?;
            txn.execute(Statement::from_string(
                backend,
                format!("SET LOCAL lock_timeout = '{lock_ms}ms'"),
            ))
            .await?;
            txn.execute(Statement::from_string(
                backend,
                format!("DROP INDEX IF EXISTS {index_name}"),
            ))
            .await?;
            txn.commit().await
        }
        .await;

        match result {
            Ok(()) => return Ok(()),
            Err(e) => {
                let msg = e.to_string();
                let is_lock_timeout = msg.to_lowercase().contains("lock timeout")
                    || msg.to_lowercase().contains("55p03");
                if is_lock_timeout && attempt < config.drop_retries {
                    attempt += 1;
                    warn!(
                        target: "query_tracker_eviction",
                        "lock timeout dropping '{index_name}'; retry {attempt}/{}",
                        config.drop_retries
                    );
                    continue;
                }
                if is_lock_timeout {
                    warn!(
                        target: "query_tracker_eviction",
                        "gave up dropping '{index_name}' after lock timeout (lock_timeout={lock_ms}ms); \
                         index left in place for next pass"
                    );
                }
                return Err(msg);
            }
        }
    }
}

fn clear_index_metrics(human_name: &str) {
    let _ = metrics::INDEX_IDX_SCAN.remove_label_values(&[human_name]);
    let _ = metrics::INDEX_DEMAND.remove_label_values(&[human_name]);
    let _ = metrics::INDEX_VARIETY.remove_label_values(&[human_name]);
}

/// `idx_accounts_<id>` / `idx_snapshot_accounts_<id>` -> `<id>`.
fn strip_index_prefix(name: &str) -> Option<String> {
    name.strip_prefix("idx_snapshot_accounts_")
        .or_else(|| name.strip_prefix("idx_accounts_"))
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
}

fn is_safe_index_identifier(name: &str) -> bool {
    !name.is_empty()
        && name
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strips_both_prefixes() {
        assert_eq!(
            strip_index_prefix("idx_accounts_abc123").as_deref(),
            Some("abc123")
        );
        assert_eq!(
            strip_index_prefix("idx_snapshot_accounts_abc123").as_deref(),
            Some("abc123")
        );
        assert_eq!(strip_index_prefix("pg_toast_index"), None);
        assert_eq!(strip_index_prefix("idx_accounts_"), None);
    }

    #[test]
    fn rejects_unsafe_names() {
        assert!(is_safe_index_identifier("idx_accounts_abc123"));
        assert!(!is_safe_index_identifier("idx_accounts_abc; DROP TABLE"));
        assert!(!is_safe_index_identifier(""));
    }
}
