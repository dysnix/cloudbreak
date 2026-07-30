// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! Bandwidth metering for the benchmark.
//!
//! A single shared [`BwMeter`] does two independent jobs:
//!  1. **Pacing (behavioural).** When `target_gbits` is set, it enforces the cap
//!     via a zero-burst token bucket paced entirely off the *estimated* response
//!     size (the VictoriaLogs `bytes` field). The spawner calls
//!     [`BwMeter::acquire_for`] with each dispatched request's estimate; the
//!     bucket charges that estimate up front and blocks the next dispatch until
//!     the debt has refilled. Because the charge happens at dispatch (not on
//!     receipt), the gate throttles *before* the bytes land, which flattens the
//!     sub-second spikes a receipt-driven limiter can't catch. No burst budget
//!     ever accrues, so dispatch is smoothed to the cap rate.
//!  2. **Stats (informational only).** Two running totals are kept and printed
//!     side by side in the summary: the *estimated* bytes (what pacing used) and
//!     the *real* bytes actually received off the wire ([`BwMeter::record`]).
//!     The real total never influences pacing — it's there so you can see how
//!     far the estimate deviated from reality.
//!
//! The cap only applies to requests that carry an estimate (VictoriaLogs);
//! requests without one (`json_file`, mismatch dir) are never charged and run
//! uncapped.

use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Zero-burst token bucket over *bytes*. `tokens` is clamped at 0 on refill so
/// no budget ever accrues while idle (no bursting); it only goes negative as
/// dispatches charge their estimate, and the spawner waits the debt out.
struct TokenBucket {
    /// Byte budget; never positive (clamped to 0), negative means in debt.
    tokens: f64,
    /// Refill rate in bytes per second (`target_gbits * 1e9 / 8`).
    refill_per_sec: f64,
    last: Instant,
}

impl TokenBucket {
    fn refill(&mut self) {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last).as_secs_f64();
        // Clamp at 0: no burst budget ever accrues.
        self.tokens = (self.tokens + elapsed * self.refill_per_sec).min(0.0);
        self.last = now;
    }
}

pub struct BwMeter {
    /// Sum of *estimated* response bytes for dispatched requests (pacing basis
    /// and the "estimated" summary figure).
    est_bytes: AtomicU64,
    /// Sum of *actual* response bytes received off the wire (the "real" summary
    /// figure only — never affects pacing).
    actual_bytes: AtomicU64,
    /// `None` disables pacing — the meter then only accumulates the two totals.
    limiter: Option<Mutex<TokenBucket>>,
}

impl BwMeter {
    /// `target_gbits` = optional cap in Gbit/s. When set, pacing is enabled with
    /// zero burst.
    pub fn new(target_gbits: Option<f64>) -> Self {
        let limiter = target_gbits.filter(|g| *g > 0.0).map(|gbits| {
            Mutex::new(TokenBucket {
                tokens: 0.0,
                refill_per_sec: gbits * 1e9 / 8.0,
                last: Instant::now(),
            })
        });
        Self {
            est_bytes: AtomicU64::new(0),
            actual_bytes: AtomicU64::new(0),
            limiter,
        }
    }

    /// Called once per dispatched request. Accumulates the estimate total and,
    /// when both a cap and an estimate are present, paces: blocks while the
    /// bucket is in debt, then charges `est`. Requests without an estimate
    /// (`None`) are never charged and never blocked — the cap is VLogs-only.
    pub async fn acquire_for(&self, est: Option<u64>) {
        if let Some(est) = est {
            self.est_bytes.fetch_add(est, Ordering::Relaxed);
        }
        let (Some(bucket), Some(est)) = (&self.limiter, est) else {
            return;
        };
        loop {
            let wait_secs = {
                let mut tb = bucket.lock().unwrap();
                tb.refill();
                if tb.tokens >= 0.0 {
                    tb.tokens -= est as f64;
                    return;
                }
                -tb.tokens / tb.refill_per_sec
            };
            // Clamp so we always make forward progress even for tiny debts.
            tokio::time::sleep(Duration::from_secs_f64(wait_secs.clamp(0.001, 5.0))).await;
        }
    }

    /// Account `bytes` actually received from rpc1. Feeds the "real" stat only;
    /// never touches the token bucket.
    pub fn record(&self, bytes: u64) {
        self.actual_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Total *estimated* rpc1 bytes for dispatched requests (pacing basis).
    pub fn est_bytes(&self) -> u64 {
        self.est_bytes.load(Ordering::Relaxed)
    }

    /// Total *actual* rpc1 bytes received off the wire.
    pub fn actual_bytes(&self) -> u64 {
        self.actual_bytes.load(Ordering::Relaxed)
    }
}
