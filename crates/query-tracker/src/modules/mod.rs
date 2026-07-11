// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! Core pipeline of the query tracker. Reading the module list is meant to
//! convey how the service works, end to end:
//!
//! ```text
//! identity      what we build (shared IndexIdentity + the accounts/snapshot pair)
//! store         the only SQL: index_patterns CRUD, candidates, supply, discrepancy
//! variety       cheap distinct-value (HLL) estimate per index
//! prioritization PriorityMode -> ORDER BY (score derived on read)
//! ingest        demand in: a batch of observations -> store
//! backpressure  gate DDL on indexer load
//! creation      demand -> CREATE INDEX pair -> mark created
//! eviction      refresh supply, flag discrepancies, drop idle pairs past fill %
//! discrepancy   demand-vs-supply verdict used by eviction
//! metrics       prometheus registry + gauges/counters
//! ```

pub mod backpressure;
pub mod creation;
pub mod discrepancy;
pub mod eviction;
pub mod identity;
pub mod ingest;
pub mod metrics;
pub mod prioritization;
pub mod store;
pub mod variety;
