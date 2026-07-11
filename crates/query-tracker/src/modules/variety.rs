// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! Variety tracking — how many *distinct* filter values a single index serves.
//!
//! ## What part of the process this covers
//!
//! An [`IndexIdentity`](cloudbreak_core::modules::index_identity::IndexIdentity)
//! deliberately collapses everything that does not define an index: commitment,
//! encoding, data slice, and crucially the memcmp *values*. So one index answers
//! many different queries that share the same `(program, offsets/lengths,
//! datasize)` shape but pass different bytes. This module keeps a cheap estimate
//! of *how many distinct value-sets* an index is serving, so the tracker can
//! tell "one query hammered a million times" apart from "a million distinct
//! queries collapsing onto one index" — useful for prioritization and eviction.
//!
//! ## What variety is (and isn't) preserved
//!
//! We track the number of distinct **memcmp value fingerprints** per identity
//! (see `IndexIdentity::value_fingerprint`). We do **not** keep the values
//! themselves, nor per-value counts, nor variety across the non-indexing
//! dimensions (commitment/encoding/etc.) — those are aggregated away entirely.
//!
//! ## Why a sketch (and why our own, tiny one)
//!
//! Keeping the exact set of values would be unbounded. A HyperLogLog gives a
//! fixed-size (`M` bytes) approximate distinct count with ~1.6% error, which is
//! all we need. The implementation below is intentionally minimal and fully
//! self-contained: no external crate, deterministic across restarts, and it
//! serializes to a flat byte array (the registers) that lives in the
//! `index_patterns.variety_hll` column. Inputs are already-hashed, uniformly
//! distributed `u64` fingerprints, so the sketch feeds on their bits directly
//! without an internal hash step.

/// log2 of the number of registers. `P = 12` → 4096 registers → 4 KiB per
/// sketch and a standard error of ~1.04/sqrt(2^P) ≈ 1.6%.
const P: u32 = 12;
/// Number of registers.
const M: usize = 1 << P;

/// Fixed-size HyperLogLog over pre-hashed `u64` fingerprints.
#[derive(Clone)]
pub struct VarietySketch {
    registers: Vec<u8>,
}

impl Default for VarietySketch {
    fn default() -> Self {
        Self::new()
    }
}

impl VarietySketch {
    pub fn new() -> Self {
        Self {
            registers: vec![0u8; M],
        }
    }

    /// Rebuild a sketch from its stored register bytes. Returns a fresh empty
    /// sketch if the stored blob is absent or the wrong length (e.g. a
    /// precision change), so a corrupt/legacy blob degrades gracefully rather
    /// than erroring.
    pub fn from_bytes(bytes: Option<&[u8]>) -> Self {
        match bytes {
            Some(b) if b.len() == M => Self {
                registers: b.to_vec(),
            },
            _ => Self::new(),
        }
    }

    /// Serialized form for storage: just the register bytes.
    pub fn to_bytes(&self) -> Vec<u8> {
        self.registers.clone()
    }

    /// Fold one fingerprint into the sketch.
    pub fn insert(&mut self, hash: u64) {
        let idx = (hash >> (64 - P)) as usize;
        let w = hash & ((1u64 << (64 - P)) - 1);
        // `w` has at least `P` leading zeros (its top P bits are the index).
        let rank = (w.leading_zeros() - P + 1) as u8;
        if rank > self.registers[idx] {
            self.registers[idx] = rank;
        }
    }

    /// Fold in every fingerprint in `hashes`.
    pub fn insert_many(&mut self, hashes: &[u64]) {
        for &h in hashes {
            self.insert(h);
        }
    }

    /// Approximate number of distinct fingerprints observed so far.
    pub fn estimate(&self) -> u64 {
        let m = M as f64;
        let alpha = 0.7213 / (1.0 + 1.079 / m);

        let mut sum = 0.0f64;
        let mut zeros = 0usize;
        for &r in &self.registers {
            sum += 2f64.powi(-(r as i32));
            if r == 0 {
                zeros += 1;
            }
        }

        let raw = alpha * m * m / sum;

        // Small-range correction (linear counting) when many registers are empty.
        let estimate = if raw <= 2.5 * m && zeros > 0 {
            m * (m / zeros as f64).ln()
        } else {
            raw
        };

        estimate.round() as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_is_zero() {
        assert_eq!(VarietySketch::new().estimate(), 0);
    }

    #[test]
    fn counts_distinct_within_error() {
        // Feed 10_000 well-spread fingerprints; expect ~10k within a few %.
        let mut s = VarietySketch::new();
        for i in 0..10_000u64 {
            // splitmix64 to spread the bits like real blake3 output would.
            let mut z = i.wrapping_add(0x9E37_79B9_7F4A_7C15);
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            z ^= z >> 31;
            s.insert(z);
        }
        let est = s.estimate();
        let err = (est as f64 - 10_000.0).abs() / 10_000.0;
        assert!(err < 0.10, "estimate {est} too far from 10000 (err {err})");
    }

    #[test]
    fn duplicates_do_not_inflate() {
        let mut s = VarietySketch::new();
        for _ in 0..5_000 {
            s.insert(0xDEAD_BEEF_CAFE_1234);
        }
        assert_eq!(s.estimate(), 1);
    }

    #[test]
    fn roundtrips_through_bytes() {
        let mut s = VarietySketch::new();
        s.insert_many(&[1, 2, 3, 4, 5]);
        let restored = VarietySketch::from_bytes(Some(&s.to_bytes()));
        assert_eq!(s.estimate(), restored.estimate());
        // Wrong length degrades to empty.
        assert_eq!(VarietySketch::from_bytes(Some(&[0u8; 3])).estimate(), 0);
        assert_eq!(VarietySketch::from_bytes(None).estimate(), 0);
    }
}
