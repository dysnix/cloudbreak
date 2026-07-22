// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! Bandwidth metering for the benchmark.
//!
//! A single shared [`BwMeter`] does two jobs:
//!  1. Accumulates the total rpc1 bytes actually received off the wire, used to
//!     print the average Gbit/s in the summary (works regardless of any limit).
//!  2. Optionally enforces a `target_gbits` cap via a debt-based token bucket:
//!     every received byte is subtracted from the bucket, and the spawner calls
//!     [`BwMeter::acquire`] before dispatching each request, sleeping while the
//!     bucket is in debt. This throttles effective RPS below `target_rps`
//!     exactly when bandwidth — not request rate — is the binding constraint.

use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Debt-based token bucket over *bytes*. `tokens` may go negative; the spawner
/// waits out the debt before dispatching more work.
struct TokenBucket {
    /// Available byte budget; negative means we've overspent and must wait.
    tokens: f64,
    /// Burst ceiling (bytes) the bucket refills up to.
    capacity: f64,
    /// Refill rate in bytes per second (`target_gbits * 1e9 / 8`).
    refill_per_sec: f64,
    last: Instant,
}

impl TokenBucket {
    fn refill(&mut self) {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last).as_secs_f64();
        self.tokens = (self.tokens + elapsed * self.refill_per_sec).min(self.capacity);
        self.last = now;
    }
}

pub struct BwMeter {
    /// Total rpc1 bytes received off the wire (for the summary average).
    total_bytes: AtomicU64,
    /// `None` disables throttling — the meter then only totals bytes.
    limiter: Option<Mutex<TokenBucket>>,
}

impl BwMeter {
    /// `target_gbits` = optional cap in Gbit/s; `burst_secs` sizes the burst
    /// capacity (how many seconds' worth of budget can accrue while idle).
    pub fn new(target_gbits: Option<f64>, burst_secs: f64) -> Self {
        let limiter = target_gbits
            .filter(|g| *g > 0.0)
            .map(|gbits| {
                let refill_per_sec = gbits * 1e9 / 8.0;
                let capacity = refill_per_sec * burst_secs;
                Mutex::new(TokenBucket {
                    tokens: capacity,
                    capacity,
                    refill_per_sec,
                    last: Instant::now(),
                })
            });
        Self {
            total_bytes: AtomicU64::new(0),
            limiter,
        }
    }

    /// Account `bytes` actually received from rpc1. Always totals; also charges
    /// the token bucket when a limit is configured.
    pub fn record(&self, bytes: u64) {
        self.total_bytes.fetch_add(bytes, Ordering::Relaxed);
        if let Some(bucket) = &self.limiter {
            let mut tb = bucket.lock().unwrap();
            tb.refill();
            tb.tokens -= bytes as f64;
        }
    }

    /// Block until there is positive byte budget. No-op when no limit is set.
    pub async fn acquire(&self) {
        let Some(bucket) = &self.limiter else {
            return;
        };
        loop {
            let wait_secs = {
                let mut tb = bucket.lock().unwrap();
                tb.refill();
                if tb.tokens > 0.0 {
                    return;
                }
                -tb.tokens / tb.refill_per_sec
            };
            // Clamp so we always make forward progress even for tiny debts.
            tokio::time::sleep(Duration::from_secs_f64(wait_secs.clamp(0.001, 5.0))).await;
        }
    }

    pub fn total_bytes(&self) -> u64 {
        self.total_bytes.load(Ordering::Relaxed)
    }
}
