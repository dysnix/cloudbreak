// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! Creation — turning demand into indexes.
//!
//! A single polling loop that, each tick: checks indexer backpressure, checks
//! the index cap, asks the [`Store`] for the highest-priority candidate the
//! configured [`PriorityMode`](cloudbreak_core::PriorityMode) allows, and — if it passes the program
//! allow/deny list — builds the index **pair** (`accounts` +
//! `snapshot_accounts`).
//!
//! There is no in-memory queue: candidates live in the DB and are ranked on
//! read, so nothing is lost across restarts. When the count reaches
//! `max-auto-indexes`, creation is *paused* until eviction makes room again —
//! candidates are kept queued in `index_patterns` with no loss of data, never
//! dropped. Physical `CREATE INDEX IF NOT EXISTS` plus a persisted `created`
//! status means we never re-issue an index we already have.

use crate::modules::indexer_backpressure;
use crate::modules::store::{Store, patterns::PatternRow};
use crate::modules::{CAP_TABLE, INDEX_TABLES};
use crate::stats::metrics;
use cloudbreak_core::{IndexRegressionGuard, QueryTrackerConfig};
use cloudbreak_core::modules::index_identity::IndexIdentity;
use sea_orm::{ConnectionTrait, Statement};
use solana_pubkey::Pubkey;
use tracing::{error, info, warn};

/// How many top candidates to fetch per tick before applying the program
/// allow/deny filter in Rust (keeps program scoping out of SQL).
const CANDIDATE_FETCH_LIMIT: u64 = 16;

/// Whether `program` is allowed by the include/exclude lists.
pub fn program_allowed(program: Pubkey, config: &QueryTrackerConfig) -> bool {
    if config.included_programs.is_empty() {
        !config.excluded_programs.iter().any(|p| p.0 == program)
    } else {
        config.included_programs.iter().any(|p| p.0 == program)
    }
}

#[tracing::instrument(name = "query_tracker_creation", skip_all)]
pub async fn run(store: Store, config: QueryTrackerConfig) {
    info!(
        target: "query_tracker_creation",
        "creation loop started (delay: {:?}, priority: {:?}, threshold: {}, max-auto-indexes: {:?})",
        config.index_creation_delay, config.priority_mode, config.index_generation_threshold,
        config.max_auto_indexes
    );

    loop {
        tokio::time::sleep(config.index_creation_delay).await;

        // Recover `rejected` patterns (dropped by the latency guard) that have
        // since proven slower without the index — they become candidates again.
        if config.index_regression_guard != IndexRegressionGuard::Off {
            match store
                .promote_recovered_rejections(
                    config.index_regression_retry_delay.as_secs() as i64,
                    config.index_regression_ratio,
                )
                .await
            {
                Ok(n) if n > 0 => info!(
                    target: "query_tracker_creation",
                    "recovered {n} rejected pattern(s) to candidate (now slower without the index)"
                ),
                Ok(_) => {}
                Err(e) => error!(
                    target: "query_tracker_creation",
                    "failed to recover rejected patterns: {e:?}"
                ),
            }
        }

        if indexer_backpressure::is_under_pressure(
            &config.indexer_metrics,
            config.indexer_metrics_threshold,
        )
        .await
        {
            continue;
        }

        // Cap check: a full table just defers creation; candidates persist.
        if let Some(max) = config.max_auto_indexes {
            match store.count_table_indexes(CAP_TABLE).await {
                Ok(current) => {
                    metrics::SNAPSHOT_ACCOUNTS_INDEXES.set(current);
                    if current as usize >= max {
                        warn!(
                            target: "query_tracker_creation",
                            "auto-index cap reached ({current}/{max}); deferring creation"
                        );
                        continue;
                    }
                }
                Err(e) => {
                    error!(target: "query_tracker_creation", "failed to count indexes: {e:?}");
                    continue;
                }
            }
        }

        let candidates = match store
            .top_candidates(
                config.priority_mode,
                config.index_generation_threshold,
                config.cost_eligibility_threshold_us,
                CANDIDATE_FETCH_LIMIT,
            )
            .await
        {
            Ok(c) => c,
            Err(e) => {
                error!(target: "query_tracker_creation", "failed to fetch candidates: {e:?}");
                continue;
            }
        };

        // Pick the highest-priority candidate that passes program scoping and
        // parses back to a valid identity.
        let chosen = candidates.into_iter().find_map(|row| {
            let identity = match row.identity() {
                Ok(i) => i,
                Err(e) => {
                    error!(target: "query_tracker_creation", "skipping candidate: {e}");
                    return None;
                }
            };
            program_allowed(identity.program, &config).then_some((row, identity))
        });

        let Some((row, identity)) = chosen else {
            continue;
        };

        create_pair(&store, &row, &identity).await;
    }
}

/// Build the index pair for one identity, marking the pattern created only if
/// both sides succeed.
async fn create_pair(store: &Store, row: &PatternRow, identity: &IndexIdentity) {
    for table in INDEX_TABLES {
        if let Err(e) = create_one(store, identity, table).await {
            error!(
                target: "query_tracker_creation",
                "failed to create index '{}' on {table}: {e:?}",
                identity.pg_index_name(table)
            );
            metrics::INDEX_CREATE_FAILURES_TOTAL.inc();
            if let Err(e) = store.mark_create_failed(&row.pattern_id, &e).await {
                error!(target: "query_tracker_creation", "failed to record create failure: {e:?}");
            }
            return;
        }
    }

    if let Err(e) = store.mark_created(&row.pattern_id).await {
        error!(target: "query_tracker_creation", "failed to mark pattern created: {e:?}");
        return;
    }

    metrics::INDEX_CREATED_TOTAL.inc();
    info!(
        target: "query_tracker_creation",
        "created index pair for '{}' (demand: {}, variety~{})",
        identity.human_name(), row.demand_count, row.variety_estimate
    );
}

/// Execute a single `CREATE INDEX IF NOT EXISTS`. Returns the error string on
/// failure so it can be persisted.
async fn create_one(store: &Store, identity: &IndexIdentity, table: &str) -> Result<(), String> {
    let sql = identity.create_index_sql(table);
    let db = store.db();
    let stmt = Statement::from_string(db.get_database_backend(), sql);
    db.execute(stmt)
        .await
        .map(|_| ())
        .map_err(|e| e.to_string())
}
