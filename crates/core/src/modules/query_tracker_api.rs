// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! The HTTP contract between the API (`query_tracker_client`) and the query
//! tracker service. Kept here, in `core`, so both sides share exactly one set
//! of typed request/response structs instead of duplicating wire shapes.
//!
//! There is a single ingest endpoint — `POST /track` — that always takes a
//! batch. A single query is just a batch of length one, so the legacy
//! "track one query" case needs no separate route.
//!
//! Values that do not define an index (commitment, encoding, filter *values*,
//! ...) are intentionally not modelled here beyond what [`TrackObservation`]
//! carries: the client sends a representative `config` (so the tracker can
//! reconstruct the `IndexIdentity`) plus the aggregated demand and a bounded
//! set of `value_fingerprints` used only for variety estimation.

use crate::modules::rpc_filter_type::RpcProgramAccountsConfig;
use serde::{Deserialize, Serialize};

/// Path of the ingest endpoint on the tracker's main HTTP server.
pub const TRACK_PATH: &str = "/track";

/// One aggregated observation for a single `IndexIdentity`, covering all the
/// requests the API saw for that identity during a flush interval.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrackObservation {
    /// Program id (base58). Combined with `config` this reconstructs the
    /// `IndexIdentity` on the tracker side.
    pub program: String,
    /// A representative request config for this identity. Only its
    /// index-defining shape (memcmp offsets/lengths, datasize) is used; the
    /// concrete filter values are irrelevant to identity.
    #[serde(default)]
    pub config: Option<RpcProgramAccountsConfig>,
    /// Number of requests observed for this identity.
    pub count: u32,
    /// Sum of observed DB cost in microseconds across those requests. For
    /// failed/timed-out requests the client contributes the timeout budget as a
    /// cost estimate.
    #[serde(default)]
    pub total_cost_us: u64,
    /// How many of `count` failed or timed out (a demand signal for patterns
    /// that currently cannot be served without an index).
    #[serde(default)]
    pub failed_count: u32,
    /// Bounded set of distinct fingerprints of the memcmp *values* seen this
    /// interval (see `IndexIdentity::value_fingerprint`). Fed into the tracker's
    /// per-identity variety sketch; may be empty (e.g. datasize-only indexes).
    #[serde(default)]
    pub value_fingerprints: Vec<u64>,
}

/// A batch of observations. `POST /track` body.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrackBatch {
    pub observations: Vec<TrackObservation>,
}

/// `POST /track` response: how many observations were applied vs. skipped
/// (not indexable / unparsable program).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrackResponse {
    pub accepted: usize,
    pub skipped: usize,
}
