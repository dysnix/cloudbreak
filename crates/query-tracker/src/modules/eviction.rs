// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! Eviction — reclaiming index budget, carefully.
//!
//! One pass per `index-eviction-interval` that, in order:
//!
//! 1. **Refreshes supply.** Reads `idx_scan`/size per auto-index from Postgres
//!    (summed across partition leaves), folds it into each pattern's row and the
//!    per-index metrics. Skipped entirely if `track_counts` is off (stats
//!    frozen), since stale `idx_scan` must never drive a drop.
//! 2. **Flags discrepancies** (see `discrepancy`) so a starved-but-demanded
//!    index is surfaced, not dropped.
//! 3. **Drops** — only when the capped table is past `eviction-fill-threshold`
//!    — the idlest pairs (no demand *and* no scans for `index-min-idle`, older
//!    than `index-min-age-grace`). Below the fill line nothing is dropped:
//!    creation simply slows, trading a little index budget for stability.
//!
//! Drops are gated on indexer backpressure and run with a bounded `lock_timeout`
//! plus a configurable retry; a drop that cannot take its lock is logged and
//! left for the next pass rather than blocking ingest.

use crate::modules::backpressure;
use crate::modules::discrepancy::{self, DiscrepancyState};
use crate::modules::identity::{CAP_TABLE, INDEX_TABLES, IndexIdentity};
use crate::modules::metrics;
use crate::modules::store::Store;
use cloudbreak_core::QueryTrackerConfig;
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
    let mut candidates = store.eviction_candidates(min_idle, min_age).await?;
    if candidates.is_empty() {
        return Ok(());
    }
    // Drop the least-useful first: fewest scans, then least demand.
    candidates.sort_by_key(|r| (r.last_idx_scan, r.demand_count));

    let target = (config.eviction_fill_threshold * max as f64).floor() as i64;
    let mut count = current;
    let mut evicted = 0usize;
    let mut reclaimed: i64 = 0;

    for row in &candidates {
        if count <= target {
            break;
        }
        if backpressure::is_under_pressure(&config.indexer_metrics, config.indexer_metrics_threshold)
            .await
        {
            info!(target: "query_tracker_eviction", "indexer under pressure; pausing evictions");
            break;
        }
        let Some(identity) = row.identity() else {
            continue;
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
        store.update_supply(&row.pattern_id, idx_scan, bytes).await?;

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
            let (state, ratio) = discrepancy::evaluate(
                demand_since_create,
                idx_scan,
                config.discrepancy_delta,
                config.index_generation_threshold as i64,
            );
            store
                .set_discrepancy(&row.pattern_id, state.as_stored(), ratio)
                .await?;
            if state == DiscrepancyState::Starved {
                warn!(
                    target: "query_tracker_discrepancy",
                    "index '{}' looks STARVED: demand~{demand_since_create} but idx_scan={idx_scan} \
                     (ratio {:.2}); Postgres may be ignoring it — check ANALYZE/planner, not evicting",
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
            metrics::PATTERNS_TOTAL.set(c.total);
            metrics::CREATED_INDEXES.set(c.created);
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

async fn drop_one(store: &Store, index_name: &str, config: &QueryTrackerConfig) -> Result<(), String> {
    if !is_safe_index_identifier(index_name) {
        return Err(format!("refusing to drop unexpected index name '{index_name}'"));
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
        assert_eq!(strip_index_prefix("idx_accounts_abc123").as_deref(), Some("abc123"));
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
