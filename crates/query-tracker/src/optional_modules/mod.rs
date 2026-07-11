// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! Opt-in extras that are off by default. They only *observe* (emit logs and
//! metrics); they never create or drop indexes. Each is a self-contained module
//! spawned by `lib::run` only when its config flag is set.
//!
//! - `explain`: periodic `EXPLAIN` sampling — a third, planner-level signal on
//!   top of demand (API) and supply (`idx_scan`).

pub mod explain;
