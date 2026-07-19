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
//! It is purely diagnostic: it only emits a log warning (corroborating
//! `discrepancy`) when the planner would not use an index — it writes nothing to
//! the DB, updates no metrics, and never creates or drops anything. Off unless
//! `explain-enabled = true`.

use crate::modules::store::Store;
use crate::modules::CAP_TABLE;
use cloudbreak_core::modules::index_identity::IndexIdentity;
use cloudbreak_core::QueryTrackerConfig;
use sea_orm::{ConnectionTrait, DbErr, Statement};
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
    for row in store.list_created().await? {
        let identity = match row.identity() {
            Ok(i) => i,
            Err(e) => {
                error!(target: "query_tracker_explain", "skipping created pattern: {e}");
                continue;
            }
        };
        let index_name = identity.pg_index_name(CAP_TABLE);
        match planner_would_use(store, &identity, &index_name).await {
            Ok(true) => {}
            Ok(false) => warn!(
                target: "query_tracker_explain",
                "planner would NOT use '{}' for its own probe query — likely stale stats or \
                 predicate mismatch (starved)", row.human_name
            ),
            Err(e) => error!(
                target: "query_tracker_explain",
                "EXPLAIN probe failed for '{}': {e:?}", row.human_name
            ),
        }
    }
    Ok(())
}

/// Run `EXPLAIN` on a synthetic query matching the index's partial predicate and
/// return whether the resulting plan references `index_name`.
async fn planner_would_use(
    store: &Store,
    identity: &IndexIdentity,
    index_name: &str,
) -> Result<bool, DbErr> {
    let sql = format!("EXPLAIN {}", probe_query(identity));
    let rows = store
        .db()
        .query_all(Statement::from_string(store.db().get_database_backend(), sql))
        .await?;

    let mut plan = String::new();
    for row in &rows {
        if let Ok(line) = row.try_get::<String>("", "QUERY PLAN") {
            plan.push_str(&line);
            plan.push('\n');
        }
    }
    Ok(plan.contains(index_name))
}

/// A representative query for `identity`: filters on `owner` (+ optional
/// datasize) and equality on each memcmp column, using zero-filled synthetic
/// constants of the right length. Shaped to match the partial index predicate.
fn probe_query(identity: &IndexIdentity) -> String {
    let mut clauses = vec![format!(
        "owner = '\\x{}'::bytea",
        hex::encode(identity.program.to_bytes())
    )];
    if let Some(size) = identity.filter.datasize {
        clauses.push(format!("length(data) = {size}"));
    }
    for (offset, length) in &identity.filter.offsets_lengths {
        let zeros = "00".repeat(*length as usize);
        clauses.push(format!(
            "substring(data, {}, {length}) = '\\x{zeros}'::bytea",
            offset + 1
        ));
    }
    format!(
        "SELECT 1 FROM {CAP_TABLE} WHERE {}",
        clauses.join(" AND ")
    )
}
