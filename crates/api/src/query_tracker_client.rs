// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! API-side query tracker client.
//!
//! Every `getProgramAccounts` request is folded into an in-memory buffer keyed
//! by [`IndexIdentity`] — the exact same key the tracker uses — so many raw
//! queries that would build one index collapse into a single buffered entry.
//! The buffer is periodically flushed to the tracker over plain HTTP using the
//! shared [`TrackBatch`] type.
//!
//! Design points (see the query tracker rework):
//! - **No data loss.** A flush that fails (tracker down, 5xx, transport error)
//!   re-buffers its batch instead of dropping it, so demand survives outages.
//! - **Bounded.** At most `max-buffered-identities` distinct identities are
//!   held; once full, *new* identities are dropped (counted) while already
//!   buffered ones keep aggregating — memory can never grow without limit.
//! - **Batched.** Large buffers are flushed in `max-batch-size` chunks so a
//!   single POST body stays bounded. A single query is just a batch of one.
//! - **Variety.** Per identity we keep a bounded set of distinct memcmp-value
//!   fingerprints; the tracker folds them into its HyperLogLog. We deliberately
//!   collapse/aggregate every other dimension (commitment, encoding, data
//!   slice, filter values) — only the index-defining shape and value-variety
//!   survive.
//! - **Failures count.** Failed/timed-out queries are recorded too (with the
//!   timeout budget as a cost estimate); a pattern that currently cannot be
//!   served without an index is exactly one worth indexing.

use cloudbreak_core::modules::index_identity::IndexIdentity;
use cloudbreak_core::modules::query_tracker_api::{TRACK_PATH, TrackBatch, TrackObservation};
use cloudbreak_core::modules::rpc_filter_type::RpcProgramAccountsConfig;
use reqwest::Client;
use solana_pubkey::Pubkey;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tracing::{debug, info, warn};

const DEFAULT_FLUSH_INTERVAL: Duration = Duration::from_secs(60);
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(5);
/// Cap on distinct value fingerprints kept per identity per flush interval.
/// Bounds memory; the tracker's HLL accumulates variety across intervals.
const MAX_FINGERPRINTS_PER_IDENTITY: usize = 256;

/// One buffered identity: the aggregated demand for a single [`IndexIdentity`].
struct BufferedIdentity {
    program: Pubkey,
    /// A representative config; only its index-defining shape matters.
    config: Option<RpcProgramAccountsConfig>,
    count: u32,
    total_cost_us: u64,
    failed_count: u32,
    fingerprints: HashSet<u64>,
}

impl BufferedIdentity {
    fn new(program: Pubkey, config: Option<RpcProgramAccountsConfig>) -> Self {
        Self {
            program,
            config,
            count: 0,
            total_cost_us: 0,
            failed_count: 0,
            fingerprints: HashSet::new(),
        }
    }

    fn to_observation(&self) -> TrackObservation {
        TrackObservation {
            program: self.program.to_string(),
            config: self.config.clone(),
            count: self.count,
            total_cost_us: self.total_cost_us,
            failed_count: self.failed_count,
            value_fingerprints: self.fingerprints.iter().copied().collect(),
        }
    }

    /// Fold another buffered entry for the same identity into this one.
    fn merge(&mut self, other: BufferedIdentity) {
        self.count = self.count.saturating_add(other.count);
        self.total_cost_us = self.total_cost_us.saturating_add(other.total_cost_us);
        self.failed_count = self.failed_count.saturating_add(other.failed_count);
        for fp in other.fingerprints {
            if self.fingerprints.len() >= MAX_FINGERPRINTS_PER_IDENTITY {
                break;
            }
            self.fingerprints.insert(fp);
        }
    }
}

#[derive(Clone)]
pub struct QueryTrackerClient {
    buffer: Arc<Mutex<HashMap<String, BufferedIdentity>>>,
    max_buffered: usize,
    dropped: Arc<AtomicU64>,
}

impl QueryTrackerClient {
    pub fn new(
        endpoint: &str,
        timeout: Option<Duration>,
        flush_interval: Option<Duration>,
        max_buffered_identities: usize,
        max_batch_size: usize,
    ) -> Self {
        if endpoint.is_empty() || endpoint == "http://" {
            panic!(
                "Query tracker client is enabled but no valid endpoint is configured. \
                 Queries will not be tracked."
            );
        }

        let client = Self {
            buffer: Arc::new(Mutex::new(HashMap::new())),
            max_buffered: max_buffered_identities.max(1),
            dropped: Arc::new(AtomicU64::new(0)),
        };

        client.spawn_flush_task(
            endpoint.trim_end_matches('/').to_string(),
            timeout.unwrap_or(DEFAULT_TIMEOUT),
            flush_interval.unwrap_or(DEFAULT_FLUSH_INTERVAL),
            max_batch_size.max(1),
        );

        client
    }

    /// Buffer one observed query. Cheap and in-memory: parses the identity,
    /// aggregates demand, records failure, and folds the value fingerprint into
    /// the identity's set. Non-indexable queries are ignored.
    pub fn buffer_query(
        &self,
        program: Pubkey,
        config: Option<RpcProgramAccountsConfig>,
        observed_cost_us: u64,
        failed: bool,
    ) {
        let Some(identity) = IndexIdentity::parse(program, config.as_ref()) else {
            return;
        };
        let key = identity.pattern_id();
        let fingerprint = IndexIdentity::value_fingerprint(config.as_ref());

        let mut buf = match self.buffer.lock() {
            Ok(b) => b,
            Err(e) => {
                warn!(target: "query_tracker_client", "failed to lock query buffer: {e}");
                return;
            }
        };

        if !buf.contains_key(&key) && buf.len() >= self.max_buffered {
            self.dropped.fetch_add(1, Ordering::Relaxed);
            return;
        }

        let entry = buf
            .entry(key)
            .or_insert_with(|| BufferedIdentity::new(program, config));
        entry.count = entry.count.saturating_add(1);
        entry.total_cost_us = entry.total_cost_us.saturating_add(observed_cost_us);
        if failed {
            entry.failed_count = entry.failed_count.saturating_add(1);
        }
        if let Some(fp) = fingerprint
            && entry.fingerprints.len() < MAX_FINGERPRINTS_PER_IDENTITY
        {
            entry.fingerprints.insert(fp);
        }
    }

    fn spawn_flush_task(
        &self,
        endpoint: String,
        timeout: Duration,
        interval: Duration,
        max_batch_size: usize,
    ) {
        let buffer = self.buffer.clone();
        let dropped = self.dropped.clone();
        let url = format!("{endpoint}{TRACK_PATH}");

        let http = Client::builder()
            .timeout(timeout)
            .build()
            .expect("failed to build query tracker HTTP client");

        info!(
            target: "query_tracker_client",
            "query tracker flush task started (endpoint: {url}, interval: {})",
            humantime::format_duration(interval)
        );

        tokio::spawn(async move {
            loop {
                tokio::time::sleep(interval).await;

                let dropped_since = dropped.swap(0, Ordering::Relaxed);
                if dropped_since > 0 {
                    warn!(
                        target: "query_tracker_client",
                        "query buffer full: dropped {dropped_since} new identities since last flush \
                         (consider raising max-buffered-identities)"
                    );
                }

                let mut entries: Vec<(String, BufferedIdentity)> = match buffer.lock() {
                    Ok(mut buf) => buf.drain().collect(),
                    Err(e) => {
                        warn!(target: "query_tracker_client", "failed to lock buffer for flush: {e}");
                        continue;
                    }
                };
                if entries.is_empty() {
                    continue;
                }

                let mut requeue: Vec<(String, BufferedIdentity)> = Vec::new();
                let total = entries.len();
                let mut sent = 0usize;

                while !entries.is_empty() {
                    let take = max_batch_size.min(entries.len());
                    let chunk: Vec<(String, BufferedIdentity)> =
                        entries.drain(..take).collect();
                    let batch = TrackBatch {
                        observations: chunk.iter().map(|(_, e)| e.to_observation()).collect(),
                    };

                    if send_batch(&http, &url, &batch).await {
                        sent += chunk.len();
                    } else {
                        requeue.extend(chunk);
                    }
                }

                if sent > 0 {
                    debug!(target: "query_tracker_client", "flushed {sent}/{total} identities to {url}");
                }
                if !requeue.is_empty() {
                    warn!(
                        target: "query_tracker_client",
                        "re-buffering {} identities after failed flush to {url}", requeue.len()
                    );
                    requeue_entries(&buffer, requeue);
                }
            }
        });
    }
}

/// POST one batch. Returns `true` on a 2xx response, `false` otherwise (so the
/// caller re-buffers rather than losing the data).
async fn send_batch(http: &Client, url: &str, batch: &TrackBatch) -> bool {
    match http.post(url).json(batch).send().await {
        Ok(resp) if resp.status().is_success() => true,
        Ok(resp) => {
            warn!(target: "query_tracker_client", "tracker returned {} when flushing", resp.status());
            false
        }
        Err(e) => {
            warn!(target: "query_tracker_client", "failed to flush to {url}: {e}");
            false
        }
    }
}

/// Merge un-sent entries back into the buffer, folding into any new demand that
/// accumulated during the flush.
fn requeue_entries(
    buffer: &Arc<Mutex<HashMap<String, BufferedIdentity>>>,
    entries: Vec<(String, BufferedIdentity)>,
) {
    let Ok(mut buf) = buffer.lock() else {
        warn!(target: "query_tracker_client", "failed to lock buffer to requeue; dropping retry batch");
        return;
    };
    for (key, entry) in entries {
        match buf.get_mut(&key) {
            Some(existing) => existing.merge(entry),
            None => {
                buf.insert(key, entry);
            }
        }
    }
}
