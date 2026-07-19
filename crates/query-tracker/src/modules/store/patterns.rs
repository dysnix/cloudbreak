// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! `index_patterns` row type and (de)serialization helpers.
//!
//! [`PatternRow`] is the in-memory view of a row, decoupled from the raw SQL so
//! the rest of the tracker never touches column names or JSON encoding.

use crate::error::QueryTrackerError;
use cloudbreak_core::modules::index_identity::IndexIdentity;
use solana_pubkey::Pubkey;

/// Lifecycle status of a pattern. Stored as text for legibility in the DB.
pub mod status {
    /// Demand seen, not yet built (eligible for creation).
    pub const CANDIDATE: &str = "candidate";
    /// Physical index pair exists.
    pub const CREATED: &str = "created";
    /// Previously created, dropped by eviction (demand may resurrect it).
    pub const EVICTED: &str = "evicted";
}

/// In-memory view of an `index_patterns` row (only the columns consumers need).
#[derive(Debug, Clone)]
pub struct PatternRow {
    pub pattern_id: String,
    pub program: Vec<u8>,
    pub human_name: String,
    pub offsets_lengths: Vec<(u64, u64)>,
    pub datasize: Option<i64>,
    pub demand_count: i64,
    pub demand_at_create: i64,
    pub total_cost_us: i64,
    pub failed_count: i64,
    pub variety_estimate: i64,
    pub status: String,
    pub last_idx_scan: i64,
    pub index_bytes: i64,
    pub discrepancy_state: Option<String>,
    pub discrepancy_ratio: Option<f64>,
}

impl PatternRow {
    /// Reconstruct the [`IndexIdentity`] this row represents.
    ///
    /// Returns an error if the row whose stored `program`bytes are not a valid pubkey
    pub fn identity(&self) -> Result<IndexIdentity, QueryTrackerError> {
        let program = Pubkey::try_from(self.program.as_slice()).map_err(|e| {
            QueryTrackerError::Internal(format!(
                "pattern {} ({}) has invalid program bytes ({} bytes): {e:?}",
                self.pattern_id,
                self.human_name,
                self.program.len(),
            ))
        })?;
        Ok(IndexIdentity::from_parts(
            program,
            self.offsets_lengths.clone(),
            self.datasize.map(|d| d as u64),
        ))
    }
}

/// Encode `(offset, length)` pairs as a JSON array of two-element arrays for the
/// `offsets_lengths` JSONB column.
pub fn offsets_to_json(offsets_lengths: &[(u64, u64)]) -> serde_json::Value {
    serde_json::Value::Array(
        offsets_lengths
            .iter()
            .map(|(o, l)| serde_json::json!([o, l]))
            .collect(),
    )
}

/// Decode the `offsets_lengths` JSONB column back into pairs. Malformed entries
/// are skipped rather than failing the whole read.
pub fn offsets_from_json(value: &serde_json::Value) -> Vec<(u64, u64)> {
    let Some(arr) = value.as_array() else {
        return Vec::new();
    };
    arr.iter()
        .filter_map(|pair| {
            let p = pair.as_array()?;
            let o = p.first()?.as_u64()?;
            let l = p.get(1)?.as_u64()?;
            Some((o, l))
        })
        .collect()
}
