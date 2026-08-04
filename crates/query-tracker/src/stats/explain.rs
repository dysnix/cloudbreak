// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! EXPLAIN sampling — the optional third signal.
//!
//! Demand (API) says the workload *wants* an index; supply (`idx_scan`) says
//! Postgres *used* it. Neither cleanly answers "would the planner pick this
//! index for a matching query *right now*?" — which is what distinguishes "no
//! traffic arrived" from "planner refuses to use it". This opt-in pass answers
//! exactly that by running `EXPLAIN` (plan only, **never** ANALYZE, so nothing
//! executes) on a synthetic probe query shaped like the index's partial
//! predicate, and checking whether the plan references the index.
//!
//! Because `accounts` / `snapshot_accounts` are partitioned, the physical index
//! is a partitioned *parent* (never scanned directly, so it never appears in a
//! plan) plus an auto-named child index on each partition — and those child
//! names are what `EXPLAIN` prints. The check therefore matches the plan against
//! every member of the index's partition tree (see `Store::index_member_names`),
//! not the parent name, which alone would look "unused" for every index even
//! while its leaves are being scanned.
//!
//! The probe uses the row's stored `example_request` (a real request that maps to
//! the identity) so the memcmp constants are realistic values Postgres has
//! statistics for, it falls back to zeros per column when no
//! example is available. Each index pair is probed on **both** physical tables
//! (`accounts` and `snapshot_accounts`), one log line per table.
//!
//! It is diagnostic only — it never creates or drops anything. Per pass it:
//! logs a warning (corroborating `discrepancy`, with the compensated `idx_scan`
//! for context) when the planner would not use an index; persists the combined
//! verdict to `index_patterns.explain_state` (`none` / `accounts_table` /
//! `snapshot_accounts_table` / `both`) for the debug endpoints; and refreshes
//! the `query_tracker_explain_state` gauge with this pass's per-state counts.
//! Off unless `explain-enabled = true`.

use crate::modules::INDEX_TABLES;
use crate::modules::store::Store;
use crate::modules::store::patterns::explain_state;
use crate::modules::store::prioritization::SCANS_PER_REQUEST;
use crate::stats::metrics;
use cloudbreak_core::QueryTrackerConfig;
use cloudbreak_core::modules::index_identity::IndexIdentity;
use cloudbreak_core::modules::rpc_filter_type::{RpcFilterType, RpcProgramAccountsConfig};
use sea_orm::{ConnectionTrait, DbErr, Statement};
use std::collections::HashMap;
use tracing::{error, info, warn};

#[tracing::instrument(name = "query_tracker_explain", skip_all)]
pub async fn run(store: Store, config: QueryTrackerConfig) {
    info!(
        target: "query_tracker_explain",
        "EXPLAIN sampling task started (interval: {:?})", config.explain_interval
    );
    loop {
        tokio::time::sleep(config.explain_interval).await;
        if let Err(e) = run_pass(&store).await {
            error!(target: "query_tracker_explain", "EXPLAIN pass failed: {e:?}");
        }
    }
}

async fn run_pass(store: &Store) -> Result<(), DbErr> {
    // Count each verdict so the last-pass gauge reflects only this run. Seed all
    // four buckets at zero so a state that vanished this pass reports 0 rather
    // than keeping a stale value.
    let mut counts: HashMap<&'static str, i64> = [
        (explain_state::NONE, 0),
        (explain_state::ACCOUNTS, 0),
        (explain_state::SNAPSHOT, 0),
        (explain_state::BOTH, 0),
    ]
    .into_iter()
    .collect();

    for row in store.list_created().await? {
        let identity = match row.identity() {
            Ok(i) => i,
            Err(e) => {
                error!(target: "query_tracker_explain", "skipping created pattern: {e}");
                continue;
            }
        };
        // Real memcmp values from the stored example (empty ⇒ fall back to zeros).
        let example = example_memcmps(row.example_request.as_ref());
        // Compensated supply (raw idx_scan is ~2x per request; a GPA scans both
        // tables of the pair), shown in the warn so "planner says unused but it
        // is clearly being scanned" is obvious inline.
        let compensated_idx_scan = row.last_idx_scan / SCANS_PER_REQUEST;
        // The index pair spans both tables; probe each, report per table, and
        // fold the two verdicts into one persisted state (`accounts` = table 0,
        // `snapshot_accounts` = table 1).
        let mut used = [false; INDEX_TABLES.len()];
        for (i, table) in INDEX_TABLES.iter().enumerate() {
            let index_name = identity.pg_index_name(table);
            match planner_would_use(store, &identity, &index_name, table, &example).await {
                Ok(true) => used[i] = true,
                Ok(false) => warn!(
                    target: "query_tracker_explain",
                    "planner would NOT use '{}' on {table} for its own probe query — likely stale \
                     stats or predicate mismatch (compensated_idx_scan~{compensated_idx_scan})",
                    row.human_name
                ),
                Err(e) => error!(
                    target: "query_tracker_explain",
                    "EXPLAIN probe failed for '{}' on {table}: {e:?}", row.human_name
                ),
            }
        }
        // INDEX_TABLES is [accounts, snapshot_accounts]; persist the combined
        // verdict so the debug endpoints can surface it.
        let state = match (used[0], used[1]) {
            (false, false) => explain_state::NONE,
            (true, false) => explain_state::ACCOUNTS,
            (false, true) => explain_state::SNAPSHOT,
            (true, true) => explain_state::BOTH,
        };
        *counts.entry(state).or_insert(0) += 1;
        if let Err(e) = store.set_explain_state(&row.pattern_id, state).await {
            error!(
                target: "query_tracker_explain",
                "failed to persist explain_state for '{}': {e:?}", row.human_name
            );
        }
    }

    // Publish the last-pass snapshot (all four buckets, including zeros).
    for (state, count) in &counts {
        metrics::EXPLAIN_STATE
            .with_label_values(&[state])
            .set(*count);
    }
    Ok(())
}

/// Run `EXPLAIN` on a synthetic query matching the index's partial predicate on
/// `table` and return whether the resulting plan references `index_name`.
///
/// On a partitioned table the plan never names the partitioned parent index
/// (`index_name`) — it scans the partitions and prints their auto-named child
/// indexes — so we match the plan against every physical member of the index
/// (parent + partition leaves) as resolved by [`Store::index_member_names`].
/// Matching the parent name alone would report "not used" for every index even
/// while its leaves are being scanned.
async fn planner_would_use(
    store: &Store,
    identity: &IndexIdentity,
    index_name: &str,
    table: &str,
    example: &HashMap<u64, Vec<u8>>,
) -> Result<bool, DbErr> {
    let sql = format!("EXPLAIN {}", probe_query(identity, table, example));
    let rows = store
        .db()
        .query_all(Statement::from_string(
            store.db().get_database_backend(),
            sql,
        ))
        .await?;

    let mut plan = String::new();
    for row in &rows {
        if let Ok(line) = row.try_get::<String>("", "QUERY PLAN") {
            plan.push_str(&line);
            plan.push('\n');
        }
    }

    let members = store.index_member_names(index_name).await?;
    Ok(members.iter().any(|name| plan.contains(name.as_str())))
}

/// A representative query for `identity` on `table`: filters on `owner` (+
/// optional datasize) and equality on each memcmp column. Uses the real value
/// from `example` when one is present for that offset (and its length matches),
/// otherwise a zero-filled constant of the right length. Shaped to match the
/// partial index predicate.
fn probe_query(identity: &IndexIdentity, table: &str, example: &HashMap<u64, Vec<u8>>) -> String {
    let mut clauses = vec![format!(
        "owner = '\\x{}'::bytea",
        hex::encode(identity.program.to_bytes())
    )];
    if let Some(size) = identity.filter.datasize {
        clauses.push(format!("length(data) = {size}"));
    }
    for (offset, length) in &identity.filter.offsets_lengths {
        let value_hex = match example.get(offset) {
            Some(bytes) if bytes.len() == *length as usize => hex::encode(bytes),
            _ => "00".repeat(*length as usize),
        };
        clauses.push(format!(
            "substring(data, {}, {length}) = '\\x{value_hex}'::bytea",
            offset + 1
        ));
    }
    format!("SELECT 1 FROM {table} WHERE {}", clauses.join(" AND "))
}

/// Extract `offset → value` for each memcmp filter in the stored example
/// request, so [`probe_query`] can use real constants. Best-effort: an absent or
/// unparseable example yields an empty map (probe falls back to zeros).
fn example_memcmps(example: Option<&serde_json::Value>) -> HashMap<u64, Vec<u8>> {
    let mut map = HashMap::new();
    let Some(value) = example else {
        return map;
    };
    let Ok(config) = serde_json::from_value::<RpcProgramAccountsConfig>(value.clone()) else {
        return map;
    };
    let Some(filters) = config.filters else {
        return map;
    };
    for filter in filters {
        if let RpcFilterType::Memcmp(memcmp) = filter
            && let Some(bytes) = memcmp.bytes()
        {
            map.insert(memcmp.offset() as u64, bytes.to_vec());
        }
    }
    map
}
