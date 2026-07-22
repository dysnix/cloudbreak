// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! Per-client-IP egress bandwidth tracking.
//!
//! Mechanism (one 1 s slotting engine feeding two views):
//!  - Every response frame's byte length is attributed, as it is written to the
//!    wire, to the current 1 s window for its client IP via [`record`]. This is
//!    a single cheap atomic add on the hot path.
//!  - A background sampler ([`spawn_sampler`]) closes the window every second:
//!    it snapshots+resets each client's accumulator (yielding a directly
//!    measured bytes/sec), feeds it into both views, and there is no ×10
//!    extrapolation.
//!  - **A — gauge** `cloudbreak_api_client_ip_peak_bytes_per_second`: the max
//!    1 s-window throughput over the trailing [`WINDOW_SECS`] seconds. Set by
//!    the sampler once per second and simply read at scrape time — never reset
//!    on read, so any number of `/metrics` consumers is harmless. Idle clients
//!    decay to 0 once their peak ages out of the window.
//!  - **B — histogram** `cloudbreak_api_client_ip_throughput_bytes_per_second`:
//!    distribution of 1 s-window throughput samples (active windows only).
//!
//! Cardinality is capped at [`MAX_CLIENT_IPS`] distinct IPs; further IPs are
//! bucketed under [`OTHER_LABEL`].

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use lazy_static::lazy_static;
use prometheus::{GaugeVec, HistogramOpts, HistogramVec, Opts, Registry};

/// Length of each throughput window. Bytes accumulated in one slot is already a
/// bytes/sec rate, so no extrapolation is applied to either view.
const SLOT_MS: u64 = 1000;

/// Trailing window (in 1 s slots) over which the peak gauge reports its max.
/// "Max seen in the last minute": as long as the Prometheus scrape interval is
/// `<= WINDOW_SECS`, every 1 s window's peak stays published long enough to be
/// scraped at least once, so no spike is lost.
const WINDOW_SECS: usize = 60;

/// Max distinct client-IP label values before overflow is bucketed under
/// [`OTHER_LABEL`], keeping series count bounded.
const MAX_CLIENT_IPS: usize = 100;
const OTHER_LABEL: &str = "overflowed-ips";

/// Per-client accumulator. `current` is the still-open 1 s window's byte total
/// (touched on the hot path). `window` is a ring of the last [`WINDOW_SECS`]
/// closed 1 s totals, touched only by the single sampler task; its max drives
/// the peak gauge.
struct ClientBw {
    current: AtomicU64,
    window: Mutex<PeakWindow>,
}

impl ClientBw {
    fn new() -> Self {
        Self {
            current: AtomicU64::new(0),
            window: Mutex::new(PeakWindow::new()),
        }
    }
}

/// Fixed-size ring of the last [`WINDOW_SECS`] closed 1 s byte totals. Only the
/// sampler task accesses it, so the mutex is effectively uncontended.
struct PeakWindow {
    slots: [u64; WINDOW_SECS],
    idx: usize,
}

impl PeakWindow {
    fn new() -> Self {
        Self {
            slots: [0; WINDOW_SECS],
            idx: 0,
        }
    }

    /// Push the newest closed-window total (which may be 0 for an idle second so
    /// that stale peaks age out) and return the max over the trailing window.
    fn push_and_max(&mut self, closed: u64) -> u64 {
        self.slots[self.idx] = closed;
        self.idx = (self.idx + 1) % WINDOW_SECS;
        self.slots.iter().copied().max().unwrap_or(0)
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

    /// View A: peak per-client-IP egress throughput (bytes/sec) over any 1 s
    /// window in the trailing `WINDOW_SECS`.
    pub static ref CLIENT_IP_PEAK_BYTES_PER_SEC: GaugeVec = GaugeVec::new(
        Opts::new(
            "cloudbreak_api_client_ip_peak_bytes_per_second",
            "Peak per-client-IP egress throughput in bytes/sec over any 1s window in the trailing 60s."
        ),
        &["client_ip"],
    )
    .unwrap();

    /// View B: distribution of per-client-IP egress throughput (bytes/sec),
    /// sampled over 1 s windows.
    pub static ref CLIENT_IP_THROUGHPUT_BYTES_PER_SEC: HistogramVec = HistogramVec::new(
        HistogramOpts::new(
            "cloudbreak_api_client_ip_throughput_bytes_per_second",
            "Distribution of per-client-IP egress throughput in bytes/sec, sampled over 1s windows."
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
/// currently-open 1 s window. Cheap: a read-lock + atomic add on the fast path;
/// a write-lock only when first seeing an IP (or routing overflow to
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

/// Close the current 1 s window for every tracked client: snapshot+reset the
/// accumulator, fold the closed total into the trailing-window peak and publish
/// it to the gauge (A), and — for active windows — record the throughput into
/// the histogram (B).
fn sample_once() {
    // Snapshot the Arcs under a short read lock, then work lock-free.
    let clients: Vec<(String, Arc<ClientBw>)> = {
        let map = STATE.read().unwrap();
        map.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
    };

    for (ip, client) in clients {
        let closed = client.current.swap(0, Ordering::Relaxed);

        // View B: only observe active windows so idle seconds don't drag the
        // distribution toward 0.
        if closed > 0 {
            CLIENT_IP_THROUGHPUT_BYTES_PER_SEC
                .with_label_values(&[&ip])
                .observe(closed as f64);
        }

        // View A: push the closed total (including 0 for idle seconds, so stale
        // peaks age out of the trailing window) and publish the rolling max.
        let peak = client.window.lock().unwrap().push_and_max(closed);
        CLIENT_IP_PEAK_BYTES_PER_SEC
            .with_label_values(&[&ip])
            .set(peak as f64);
    }
}

/// Spawn the 1 s sampler task. Safe to call once from `setup_metrics`.
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
