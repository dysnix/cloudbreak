// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! Store — the only place that speaks SQL.
//!
//! Everything the tracker knows lives in the `index_patterns` table; this
//! module owns the connection and exposes typed operations over it (record
//! demand, pick candidates, update supply, evict, flag discrepancies, read
//! counts). No SQL or column name leaks past this boundary, which keeps the
//! pipeline modules (`ingest`, `creation`, `eviction`, ...) readable and makes
//! the schema easy to evolve.
//!
//! The tracker is a single service, so the per-identity variety sketch
//! ([`VarietySketch`]) is simply loaded, folded, and written back inside a row
//! transaction — no cross-process HLL merge is needed.

pub mod patterns;

use crate::modules::identity::IndexIdentity;
use crate::modules::variety::VarietySketch;
use cloudbreak_core::PriorityMode;
use patterns::{PatternRow, offsets_from_json, offsets_to_json, status};
use sea_orm::{
    ConnectionTrait, DatabaseConnection, DbErr, QueryResult, Statement, TransactionTrait, Value,
};

/// Columns selected into a [`PatternRow`]; kept in one place so every read
/// stays in sync with [`row_to_pattern`].
const PATTERN_COLUMNS: &str = "pattern_id, program, human_name, offsets_lengths, datasize, \
     demand_count, demand_at_create, total_cost_us, failed_count, variety_estimate, status, \
     last_idx_scan, index_bytes, discrepancy_state, discrepancy_ratio";

/// Aggregate counts for metrics.
#[derive(Debug, Clone, Copy, Default)]
pub struct StoreCounts {
    pub total: i64,
    pub created: i64,
    pub discrepant: i64,
}

/// Typed access to the `index_patterns` table.
#[derive(Clone)]
pub struct Store {
    db: DatabaseConnection,
}

impl Store {
    pub fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }

    /// Escape hatch for the few catalog reads that are not about
    /// `index_patterns` (pg_stat / pg_class), used by `creation`/`eviction`.
    pub fn db(&self) -> &DatabaseConnection {
        &self.db
    }

    // ---- ingest -----------------------------------------------------------

    /// Fold one aggregated observation into the pattern's row: bump demand,
    /// cost and failure counters, refresh `last_demand_at`, resurrect an evicted
    /// pattern back to `candidate`, and merge value fingerprints into the
    /// variety sketch. Runs in a transaction so the sketch read-modify-write is
    /// consistent under concurrent `/track` requests.
    pub async fn record_demand(
        &self,
        identity: &IndexIdentity,
        count: u32,
        cost_us: u64,
        failed: u32,
        fingerprints: &[u64],
    ) -> Result<(), DbErr> {
        let backend = self.db.get_database_backend();
        let pattern_id = identity.pattern_id();

        let txn = self.db.begin().await?;

        let insert = Statement::from_sql_and_values(
            backend,
            "INSERT INTO index_patterns \
               (pattern_id, program, human_name, offsets_lengths, datasize, \
                demand_count, total_cost_us, failed_count, last_demand_at) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, now()) \
             ON CONFLICT (pattern_id) DO UPDATE SET \
               demand_count = index_patterns.demand_count + EXCLUDED.demand_count, \
               total_cost_us = index_patterns.total_cost_us + EXCLUDED.total_cost_us, \
               failed_count = index_patterns.failed_count + EXCLUDED.failed_count, \
               last_demand_at = now(), \
               status = CASE WHEN index_patterns.status = 'evicted' \
                             THEN 'candidate' ELSE index_patterns.status END \
             RETURNING variety_hll",
            [
                pattern_id.clone().into(),
                identity.program.to_bytes().to_vec().into(),
                identity.human_name().into(),
                Value::Json(Some(Box::new(offsets_to_json(
                    &identity.filter.offsets_lengths,
                )))),
                identity.filter.datasize.map(|d| d as i64).into(),
                (count as i64).into(),
                (cost_us as i64).into(),
                (failed as i64).into(),
            ],
        );

        let existing_hll: Option<Vec<u8>> = txn
            .query_one(insert)
            .await?
            .and_then(|row| row.try_get::<Option<Vec<u8>>>("", "variety_hll").ok())
            .flatten();

        if !fingerprints.is_empty() {
            let mut sketch = VarietySketch::from_bytes(existing_hll.as_deref());
            sketch.insert_many(fingerprints);
            let hll_bytes = sketch.to_bytes();
            let estimate = sketch.estimate() as i64;
            txn.execute(Statement::from_sql_and_values(
                backend,
                "UPDATE index_patterns SET variety_hll = $2, variety_estimate = $3 \
                 WHERE pattern_id = $1",
                [pattern_id.into(), hll_bytes.into(), estimate.into()],
            ))
            .await?;
        }

        txn.commit().await
    }

    // ---- creation ---------------------------------------------------------

    /// Highest-priority creation candidates, ranked per [`PriorityMode`].
    /// Numeric parameters are embedded as literals (they are never
    /// user-controlled), which keeps the dynamic ORDER BY / cost gate simple.
    pub async fn top_candidates(
        &self,
        mode: PriorityMode,
        cost_weight: f64,
        failure_weight: f64,
        threshold: u32,
        cost_eligibility_us: Option<u64>,
        limit: u64,
    ) -> Result<Vec<PatternRow>, DbErr> {
        let order_by = crate::modules::prioritization::order_by_clause(mode, cost_weight, failure_weight);
        let cost_gate = match cost_eligibility_us {
            Some(us) => format!(
                " AND (total_cost_us::float8 / GREATEST(demand_count, 1)) >= {us}"
            ),
            None => String::new(),
        };
        let sql = format!(
            "SELECT {PATTERN_COLUMNS} FROM index_patterns \
             WHERE status = '{candidate}' AND demand_count >= {threshold}{cost_gate} \
             ORDER BY {order_by} LIMIT {limit}",
            candidate = status::CANDIDATE,
        );
        let rows = self
            .db
            .query_all(Statement::from_string(self.db.get_database_backend(), sql))
            .await?;
        rows.iter().map(row_to_pattern).collect()
    }

    /// Mark a pattern as created and (re)anchor its supply clock to now.
    pub async fn mark_created(&self, pattern_id: &str) -> Result<(), DbErr> {
        self.db
            .execute(Statement::from_sql_and_values(
                self.db.get_database_backend(),
                "UPDATE index_patterns SET status = 'created', created_at = now(), \
                   last_seen_used = now(), last_idx_scan = 0, last_create_error = NULL, \
                   evicted_at = NULL, demand_at_create = demand_count \
                 WHERE pattern_id = $1",
                [pattern_id.into()],
            ))
            .await
            .map(|_| ())
    }

    /// Record a failed creation attempt (keeps the pattern a candidate so it is
    /// retried on a later pass).
    pub async fn mark_create_failed(&self, pattern_id: &str, error: &str) -> Result<(), DbErr> {
        self.db
            .execute(Statement::from_sql_and_values(
                self.db.get_database_backend(),
                "UPDATE index_patterns SET create_attempts = create_attempts + 1, \
                   last_create_error = $2 WHERE pattern_id = $1",
                [pattern_id.into(), error.into()],
            ))
            .await
            .map(|_| ())
    }

    // ---- supply / eviction ------------------------------------------------

    /// All patterns currently in the `created` state.
    pub async fn list_created(&self) -> Result<Vec<PatternRow>, DbErr> {
        let sql = format!(
            "SELECT {PATTERN_COLUMNS} FROM index_patterns WHERE status = '{}'",
            status::CREATED
        );
        let rows = self
            .db
            .query_all(Statement::from_string(self.db.get_database_backend(), sql))
            .await?;
        rows.iter().map(row_to_pattern).collect()
    }

    /// Update a pattern's supply columns; bump `last_seen_used` only when the
    /// scan counter actually moved (a change is the only reliable "was used"
    /// signal, and it survives stat resets — a reset just restarts the clock).
    pub async fn update_supply(
        &self,
        pattern_id: &str,
        idx_scan: i64,
        bytes: i64,
    ) -> Result<(), DbErr> {
        self.db
            .execute(Statement::from_sql_and_values(
                self.db.get_database_backend(),
                "UPDATE index_patterns SET \
                   index_bytes = $2, \
                   last_seen_used = CASE WHEN $3 <> last_idx_scan OR last_seen_used IS NULL \
                                        THEN now() ELSE last_seen_used END, \
                   last_idx_scan = $3 \
                 WHERE pattern_id = $1",
                [pattern_id.into(), bytes.into(), idx_scan.into()],
            ))
            .await
            .map(|_| ())
    }

    /// Created patterns idle by **both** supply and demand for longer than
    /// `min_idle_secs`, and older than `min_age_secs`. Requiring *both* signals
    /// to be quiet is what avoids the drop→slow→rebuild churn loop: a pattern
    /// Postgres is ignoring but the API is still demanding is not idle.
    pub async fn eviction_candidates(
        &self,
        min_idle_secs: i64,
        min_age_secs: i64,
    ) -> Result<Vec<PatternRow>, DbErr> {
        let sql = format!(
            "SELECT {PATTERN_COLUMNS} FROM index_patterns \
             WHERE status = '{created}' \
               AND EXTRACT(EPOCH FROM (now() - COALESCE(created_at, first_seen_at))) > $1 \
               AND EXTRACT(EPOCH FROM (now() - COALESCE(last_seen_used, created_at, first_seen_at))) > $2 \
               AND EXTRACT(EPOCH FROM (now() - COALESCE(last_demand_at, first_seen_at))) > $2",
            created = status::CREATED,
        );
        let rows = self
            .db
            .query_all(Statement::from_sql_and_values(
                self.db.get_database_backend(),
                sql,
                [min_age_secs.into(), min_idle_secs.into()],
            ))
            .await?;
        rows.iter().map(row_to_pattern).collect()
    }

    /// Mark a pattern as evicted after its index pair has been dropped.
    pub async fn mark_evicted(&self, pattern_id: &str) -> Result<(), DbErr> {
        self.db
            .execute(Statement::from_sql_and_values(
                self.db.get_database_backend(),
                "UPDATE index_patterns SET status = 'evicted', evicted_at = now() \
                 WHERE pattern_id = $1",
                [pattern_id.into()],
            ))
            .await
            .map(|_| ())
    }

    /// Record (or clear) the discrepancy verdict for a pattern.
    pub async fn set_discrepancy(
        &self,
        pattern_id: &str,
        state: Option<&str>,
        ratio: Option<f64>,
    ) -> Result<(), DbErr> {
        self.db
            .execute(Statement::from_sql_and_values(
                self.db.get_database_backend(),
                "UPDATE index_patterns SET discrepancy_state = $2, discrepancy_ratio = $3, \
                   discrepancy_at = CASE WHEN $2 IS NOT NULL THEN now() ELSE discrepancy_at END \
                 WHERE pattern_id = $1",
                [
                    pattern_id.into(),
                    state.map(|s| s.to_string()).into(),
                    ratio.into(),
                ],
            ))
            .await
            .map(|_| ())
    }

    // ---- catalog reads ----------------------------------------------------

    /// `(index_name, summed idx_scan, summed bytes)` for every auto-index on the
    /// `accounts` / `snapshot_accounts` tables. Sums across partition leaves via
    /// `pg_partition_tree`; a plain index yields no tree rows so the LEFT JOIN +
    /// COALESCE falls back to the index's own OID.
    pub async fn read_auto_index_supply(&self) -> Result<Vec<(String, i64, i64)>, DbErr> {
        let rows = self
            .db
            .query_all(Statement::from_string(
                self.db.get_database_backend(),
                "SELECT c.relname AS index_name, \
                    COALESCE(SUM(s.idx_scan), 0)::bigint AS idx_scan, \
                    COALESCE(SUM(pg_relation_size(COALESCE(t.relid, c.oid))), 0)::bigint AS bytes \
                 FROM pg_class c \
                 JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = 'public' \
                 LEFT JOIN LATERAL pg_partition_tree(c.oid) t ON true \
                 LEFT JOIN pg_stat_user_indexes s ON s.indexrelid = COALESCE(t.relid, c.oid) \
                 WHERE c.relkind IN ('i', 'I') \
                   AND (c.relname LIKE 'idx_accounts\\_%' OR c.relname LIKE 'idx_snapshot_accounts\\_%') \
                 GROUP BY c.relname"
                    .to_string(),
            ))
            .await?;
        rows.iter()
            .map(|row| {
                Ok((
                    row.try_get::<String>("", "index_name")?,
                    row.try_get::<i64>("", "idx_scan")?,
                    row.try_get::<i64>("", "bytes")?,
                ))
            })
            .collect()
    }

    /// Number of indexes on `table` (used for the cap / fill ratio).
    pub async fn count_table_indexes(&self, table: &str) -> Result<i64, DbErr> {
        self.db
            .query_one(Statement::from_sql_and_values(
                self.db.get_database_backend(),
                "SELECT COUNT(*)::bigint AS n FROM pg_indexes \
                 WHERE schemaname = 'public' AND tablename = $1",
                [table.into()],
            ))
            .await?
            .map(|row| row.try_get::<i64>("", "n"))
            .transpose()
            .map(|n| n.unwrap_or(0))
    }

    /// Whether Postgres is currently accumulating statistics. When off,
    /// `idx_scan` is frozen and the eviction pass must be skipped.
    pub async fn track_counts_enabled(&self) -> Result<bool, DbErr> {
        Ok(self
            .db
            .query_one(Statement::from_string(
                self.db.get_database_backend(),
                "SELECT current_setting('track_counts') = 'on' AS enabled".to_string(),
            ))
            .await?
            .map(|row| row.try_get::<bool>("", "enabled"))
            .transpose()?
            .unwrap_or(false))
    }

    /// Aggregate counts for metrics.
    pub async fn counts(&self) -> Result<StoreCounts, DbErr> {
        let row = self
            .db
            .query_one(Statement::from_string(
                self.db.get_database_backend(),
                "SELECT COUNT(*)::bigint AS total, \
                   COUNT(*) FILTER (WHERE status = 'created')::bigint AS created, \
                   COUNT(*) FILTER (WHERE status = 'created' AND discrepancy_state IS NOT NULL)::bigint AS discrepant \
                 FROM index_patterns"
                    .to_string(),
            ))
            .await?;
        match row {
            Some(row) => Ok(StoreCounts {
                total: row.try_get("", "total")?,
                created: row.try_get("", "created")?,
                discrepant: row.try_get("", "discrepant")?,
            }),
            None => Ok(StoreCounts::default()),
        }
    }
}

/// Decode one query row into a [`PatternRow`]. The single point that must match
/// [`PATTERN_COLUMNS`].
fn row_to_pattern(row: &QueryResult) -> Result<PatternRow, DbErr> {
    Ok(PatternRow {
        pattern_id: row.try_get("", "pattern_id")?,
        program: row.try_get("", "program")?,
        human_name: row.try_get("", "human_name")?,
        offsets_lengths: offsets_from_json(&row.try_get::<serde_json::Value>("", "offsets_lengths")?),
        datasize: row.try_get("", "datasize")?,
        demand_count: row.try_get("", "demand_count")?,
        demand_at_create: row.try_get("", "demand_at_create")?,
        total_cost_us: row.try_get("", "total_cost_us")?,
        failed_count: row.try_get("", "failed_count")?,
        variety_estimate: row.try_get("", "variety_estimate")?,
        status: row.try_get("", "status")?,
        last_idx_scan: row.try_get("", "last_idx_scan")?,
        index_bytes: row.try_get("", "index_bytes")?,
        discrepancy_state: row.try_get("", "discrepancy_state")?,
        discrepancy_ratio: row.try_get("", "discrepancy_ratio")?,
    })
}
