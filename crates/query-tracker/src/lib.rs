// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! Query tracker service.
//!
//! Persistence-first, demand-driven auto-indexer. All decision state lives in
//! the `index_patterns` table (see the migration), so the service is fully
//! restartable and never loses history. The crate is organised as:
//!
//! - `server`          — HTTP edge: ingest (`/track`), debug, metrics/health.
//! - `modules`         — the core pipeline (identity → store → ingest →
//!   prioritization → creation/eviction/discrepancy → metrics).
//! - `optional_modules`— opt-in observers (EXPLAIN sampling).
//!
//! `run` wires them together: it loads config, connects the DB, starts the two
//! HTTP servers, and spawns the background loops enabled by config.

pub mod error;
pub mod modules;
pub mod optional_modules;
pub mod server;

pub use error::{QueryTrackerError, QueryTrackerResult};

use crate::modules::store::Store;
use crate::server::AppState;
use cloudbreak_core::{QueryTrackerServiceConfig, TryLoadConfig};
use sea_orm::{ConnectOptions, Database};
use std::sync::Arc;
use tracing::info;

pub async fn run(config_path: &str) -> cloudbreak_core::Result<()> {
    let config = QueryTrackerServiceConfig::try_load(config_path)?;
    let server_addr = config.server_addr();
    let metrics_addr = config.metrics_addr();

    modules::metrics::init();
    tokio::spawn(async move { server::operational::serve(metrics_addr).await });

    let database = Database::connect(ConnectOptions::from(config.database.clone())).await?;
    let store = Store::new(database);
    let qt = config.query_tracker.clone();

    info!(
        target: "query_tracker",
        "query tracker starting (create-indexes: {}, priority: {:?}, threshold: {}, eviction: {}, explain: {})",
        qt.create_database_indexes, qt.priority_mode, qt.index_generation_threshold,
        qt.index_eviction_enabled, qt.explain_enabled
    );

    if qt.create_database_indexes {
        let (store, cfg) = (store.clone(), qt.clone());
        tokio::spawn(async move { modules::creation::run(store, cfg).await });
    } else {
        info!(target: "query_tracker", "index creation disabled; recording demand only");
    }

    if qt.index_eviction_enabled {
        let (store, cfg) = (store.clone(), qt.clone());
        tokio::spawn(async move { modules::eviction::run(store, cfg).await });
    }

    if qt.explain_enabled {
        let (store, cfg) = (store.clone(), qt.clone());
        tokio::spawn(async move { optional_modules::explain::run(store, cfg).await });
    }

    let state = Arc::new(AppState {
        store,
        config: qt,
    });
    tokio::spawn(async move { server::serve(server_addr, state).await });

    info!(target: "query_tracker", "query tracker running on http://{server_addr}. Press Ctrl+C to stop.");
    tokio::signal::ctrl_c().await?;
    info!(target: "query_tracker", "shutdown signal received; stopping query tracker");

    Ok(())
}
