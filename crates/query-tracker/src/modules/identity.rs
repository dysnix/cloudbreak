// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! Identity — *what* the tracker builds, and where.
//!
//! The canonical [`IndexIdentity`] (parsing, deterministic `pattern_id`,
//! `<=63`-byte physical names, `CREATE INDEX` SQL, value fingerprints) lives in
//! `cloudbreak-core` so the API client and the tracker agree on the exact same
//! key. This module re-exports it and adds the tracker-only detail that every
//! auto-index is built as a **pair** across the live and snapshot tables.

pub use cloudbreak_core::modules::index_identity::{
    IndexIdentity, ParsedIndexFilter, PG_MAX_IDENTIFIER_LEN,
};

/// The two tables an auto-index is always created on (and dropped from)
/// together. Keeping them paired means the planner has the index available on
/// whichever table a GPA query targets.
pub const INDEX_TABLES: [&str; 2] = ["accounts", "snapshot_accounts"];

/// The table whose index count is used for the `max-auto-indexes` cap and the
/// eviction fill ratio (the snapshot table carries the full working set).
pub const CAP_TABLE: &str = "snapshot_accounts";
