// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! Per-client-IP egress bandwidth tracking.
//!
//! Mechanism (one 100 ms slotting engine feeding two views):
//!  - Every response frame's byte length is attributed, as it is written to the
//!    wire, to the current 100 ms window for its client IP via [`record`]. This
//!    is a single cheap atomic add on the hot path.
//!  - A background sampler ([`spawn_sampler`]) rotates the window every 100 ms:
//!    it snapshots+resets each client's accumulator, updates a per-scrape
//!    running max, and observes the window throughput into a histogram.
//!  - **A — gauge** `cloudbreak_api_client_ip_peak_bytes_per_second`: the peak
//!    100 ms-window throughput since the last scrape. Refreshed + reset at
//!    scrape time by [`refresh_gauges`].
//!  - **B — histogram** `cloudbreak_api_client_ip_throughput_bytes_per_second`:
//!    distribution of 100 ms-window throughput samples.
//!
//! Cardinality is capped at [`MAX_CLIENT_IPS`] distinct IPs; further IPs are
//! bucketed under [`OTHER_LABEL`].

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use lazy_static::lazy_static;
use prometheus::{GaugeVec, HistogramOpts, HistogramVec, Opts, Registry};

/// Length of each throughput window. The precision knob: smaller catches
/// shorter bursts. `bytes_in_slot * SLOT_HZ = bytes/sec`.
const SLOT_MS: u64 = 100;
const SLOT_HZ: f64 = 1000.0 / SLOT_MS as f64;

/// Max distinct client-IP label values before overflow is bucketed under
/// [`OTHER_LABEL`], keeping series count bounded.
const MAX_CLIENT_IPS: usize = 100;
const OTHER_LABEL: &str = "overflowed-ips";

/// Per-client accumulator. `current` is the still-open window's byte total;
/// `running_max` is the largest closed-window byte total since the last gauge
/// refresh (drives the peak gauge).
struct ClientBw {
    current: AtomicU64,
    running_max: AtomicU64,
}

impl ClientBw {
    fn new() -> Self {
        Self {
            current: AtomicU64::new(0),
            running_max: AtomicU64::new(0),
        }
    }
}

/// Histogram buckets in **bytes/sec**.
/// (1 Gbit/s = 0.125 GB/s = 1.25e8 bytes/sec.)
fn throughput_buckets() -> Vec<f64> {
    vec![
        1.25e7,  // 0.1 Gbit/s
        2.5e8,   // 2
        5.0e8,   // 4
        7.5e8,   // 6
        1.0e9,   // 8
        1.125e9, // 9
        1.25e9,  // 10
        2.5e9,   // 20
        3.125e9, // 25
        5.0e9,   // 40
        6.25e9,  // 50
    ]
}

lazy_static! {
    static ref STATE: RwLock<HashMap<String, Arc<ClientBw>>> = RwLock::new(HashMap::new());

    /// View A: peak per-client-IP egress throughput (bytes/sec) over any 100 ms
    /// window since the last scrape.
    pub static ref CLIENT_IP_PEAK_BYTES_PER_SEC: GaugeVec = GaugeVec::new(
        Opts::new(
            "cloudbreak_api_client_ip_peak_bytes_per_second",
            "Peak per-client-IP egress throughput in bytes/sec over any 100ms window since the last scrape."
        ),
        &["client_ip"],
    )
    .unwrap();

    /// View B: distribution of per-client-IP egress throughput (bytes/sec),
    /// sampled over 100 ms windows.
    pub static ref CLIENT_IP_THROUGHPUT_BYTES_PER_SEC: HistogramVec = HistogramVec::new(
        HistogramOpts::new(
            "cloudbreak_api_client_ip_throughput_bytes_per_second",
            "Distribution of per-client-IP egress throughput in bytes/sec, sampled over 100ms windows."
        )
        .buckets(throughput_buckets()),
        &["client_ip"],
    )
    .unwrap();
}

/// Register the bandwidth collectors and start the background sampler. Called
/// once from `setup_metrics` (inside the tokio runtime). Registration and
/// sampler startup are idempotent-safe to call once.
pub fn register(registry: &Registry) {
    registry
        .register(Box::new(CLIENT_IP_PEAK_BYTES_PER_SEC.clone()))
        .expect("client-ip peak gauge can't be registered");
    registry
        .register(Box::new(CLIENT_IP_THROUGHPUT_BYTES_PER_SEC.clone()))
        .expect("client-ip throughput histogram can't be registered");
    spawn_sampler();
}

/// The single hot-path call: attribute `bytes` of egress to `client_ip`'s
/// currently-open 100 ms window. Cheap: a read-lock + atomic add on the fast
/// path; a write-lock only when first seeing an IP (or routing overflow to
/// [`OTHER_LABEL`]).
pub fn record(client_ip: &str, bytes: u64) {
    if bytes == 0 {
        return;
    }

    // Fast path: entry already exists.
    {
        let map = STATE.read().unwrap();
        if let Some(client) = map.get(client_ip) {
            client.current.fetch_add(bytes, Ordering::Relaxed);
            return;
        }
    }

    // Slow path: create the entry (or bucket into "other" when at capacity).
    let mut map = STATE.write().unwrap();
    // Re-check under the write lock in case another thread inserted it.
    if let Some(client) = map.get(client_ip) {
        client.current.fetch_add(bytes, Ordering::Relaxed);
        return;
    }
    let key = if map.len() < MAX_CLIENT_IPS {
        client_ip.to_string()
    } else {
        OTHER_LABEL.to_string()
    };
    let client = map.entry(key).or_insert_with(|| Arc::new(ClientBw::new()));
    client.current.fetch_add(bytes, Ordering::Relaxed);
}

/// Close the current 100 ms window for every tracked client: snapshot+reset the
/// accumulator, fold it into the per-scrape running max (A), and record the
/// window's throughput into the histogram (B).
fn sample_once() {
    // Snapshot the Arcs under a short read lock, then work lock-free.
    let clients: Vec<(String, Arc<ClientBw>)> = {
        let map = STATE.read().unwrap();
        map.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
    };

    for (ip, client) in clients {
        let closed = client.current.swap(0, Ordering::Relaxed);
        if closed == 0 {
            continue;
        }
        client.running_max.fetch_max(closed, Ordering::Relaxed);
        let bytes_per_sec = closed as f64 * SLOT_HZ;
        CLIENT_IP_THROUGHPUT_BYTES_PER_SEC
            .with_label_values(&[&ip])
            .observe(bytes_per_sec);
    }
}

/// Spawn the 100 ms sampler task. Safe to call once from `setup_metrics`.
pub fn spawn_sampler() {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_millis(SLOT_MS));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            ticker.tick().await;
            sample_once();
        }
    });
}

/// Publish the peak gauge (A) at scrape time and reset the per-scrape running
/// max. Called from the `/metrics` handler.
pub fn refresh_gauges() {
    let clients: Vec<(String, Arc<ClientBw>)> = {
        let map = STATE.read().unwrap();
        map.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
    };

    for (ip, client) in clients {
        let peak_slot_bytes = client.running_max.swap(0, Ordering::Relaxed);
        CLIENT_IP_PEAK_BYTES_PER_SEC
            .with_label_values(&[&ip])
            .set(peak_slot_bytes as f64 * SLOT_HZ);
    }
}
