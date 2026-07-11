// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! `index_patterns` — the single durable table backing the reworked query
//! tracker.
//!
//! It supersedes `auto_index_usage`: instead of only remembering which indexes
//! were created and when they were last used, one row per [`IndexIdentity`]
//! (keyed by its deterministic `pattern_id`) now holds **everything** the
//! tracker needs to make decisions, so the service is fully restartable and
//! never loses history:
//!
//! - identity: `program`, `offsets_lengths` (JSONB), `datasize`, `human_name`
//! - demand (API side): `demand_count`, `demand_at_create`, `total_cost_us`,
//!   `failed_count`, `variety_hll` / `variety_estimate`, `first_seen_at`,
//!   `last_demand_at`
//! - lifecycle: `status`, `created_at`, `evicted_at`, `create_attempts`,
//!   `last_create_error`
//! - supply (Postgres side): `last_idx_scan`, `last_seen_used`, `index_bytes`
//! - discrepancy: `discrepancy_state`, `discrepancy_ratio`, `discrepancy_at`
//!
//! `auto_index_usage` is intentionally left in place (unused) rather than
//! dropped here; physical indexes created by the old naming scheme keep their
//! names and are simply no longer tracked.

use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

const CREATE_SQL: &str = "\
CREATE TABLE IF NOT EXISTS index_patterns (
    pattern_id        TEXT PRIMARY KEY,
    program           BYTEA NOT NULL,
    human_name        TEXT NOT NULL,
    offsets_lengths   JSONB NOT NULL DEFAULT '[]'::jsonb,
    datasize          BIGINT,

    demand_count      BIGINT NOT NULL DEFAULT 0,
    demand_at_create  BIGINT NOT NULL DEFAULT 0,
    total_cost_us     BIGINT NOT NULL DEFAULT 0,
    failed_count      BIGINT NOT NULL DEFAULT 0,
    variety_hll       BYTEA,
    variety_estimate  BIGINT NOT NULL DEFAULT 0,
    first_seen_at     TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_demand_at    TIMESTAMPTZ,

    status            TEXT NOT NULL DEFAULT 'candidate',
    created_at        TIMESTAMPTZ,
    evicted_at        TIMESTAMPTZ,
    create_attempts   INTEGER NOT NULL DEFAULT 0,
    last_create_error TEXT,

    last_idx_scan     BIGINT NOT NULL DEFAULT 0,
    last_seen_used    TIMESTAMPTZ,
    index_bytes       BIGINT NOT NULL DEFAULT 0,

    discrepancy_state TEXT,
    discrepancy_ratio DOUBLE PRECISION,
    discrepancy_at    TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_index_patterns_status ON index_patterns (status);
";

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .get_connection()
            .execute_unprepared(CREATE_SQL)
            .await?;
        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .get_connection()
            .execute_unprepared("DROP TABLE IF EXISTS index_patterns;")
            .await?;
        Ok(())
    }
}
