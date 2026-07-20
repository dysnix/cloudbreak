// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use std::sync::{Arc, Mutex};
use cloudbreak_core::{
    IndexConfig, SnapshotConfig,
    modules::{account_owner_map::AccountOwnerMap, supply_tracker::SupplyTracker},
};

use crate::metrics;

/// Only on `FinishedAndCleanedUp` state we mark the serviceas healthy, but on `Started` state
///  we don't execute the `process_snapshot_if_needed` function again
#[derive(PartialEq, Clone, Copy)]
pub enum SnapshotProcessingState {
    NotStarted,
    Started,
    Finished,
    FinishedAndCleanedUp,
}

/// Only tries to process the snapshot if we set the `snapshot` config section on `IndexConfig`
/// Loads the snapshot on startup and marks the SnapshotState as `Started`.
/// On finished, marks the SnapshotState as `Finished` and cleans up the stored accounts.
pub async fn process_snapshot_if_needed(
    config: IndexConfig,
    slot: u64,
    snapshot_processing_state: Arc<Mutex<SnapshotProcessingState>>,
    finalize_slot_buffer_size: Arc<Mutex<usize>>,
    accounts_owner_map: AccountOwnerMap,
    supply_tracker: SupplyTracker,
) {
    let snapshot_config = match config.snapshot {
        Some(snapshot_config) => snapshot_config,
        None => return,
    };

    {
        let snapshot_processing_state = snapshot_processing_state
            .lock()
            .expect("Failed to lock snapshot_processing_state");
        match *snapshot_processing_state {
            SnapshotProcessingState::NotStarted => (),
            SnapshotProcessingState::Started
            | SnapshotProcessingState::Finished
            | SnapshotProcessingState::FinishedAndCleanedUp => {
                tracing::debug!("Skipping snapshot processing - not a startup");
                return;
            }
        };
    }

    let snapshot_processing_state_clone = snapshot_processing_state.clone();

    tokio::spawn(async move {
        let _guard = metrics::TokioTaskCounterGuard::new("snapshot_processing");

        let handle = cloudbreak_snapshot::run(
            SnapshotConfig {
                accounts_file_concurency: snapshot_config.accounts_file_concurency,
                database: config.database,
                tracker_endpoint: snapshot_config.tracker_endpoint,
                metrics: config.metrics,
                programs: config.programs,
                pg_indexes: snapshot_config.pg_indexes,
            },
            Some(slot),
            Some(metrics::METRICS_REGISTRY.clone()),
            Some(finalize_slot_buffer_size.clone()),
            accounts_owner_map,
            supply_tracker,
        )
        .await;

        if let Err(e) = handle {
            tracing::error!("Failed to process snapshot: {:?}", e);
            panic!("Failed to process snapshot: {:?}", e);
        }

        *snapshot_processing_state_clone
            .lock()
            .expect("Failed to lock snapshot_processing_state") = SnapshotProcessingState::Finished;
    });

    *snapshot_processing_state
        .lock()
        .expect("Failed to lock snapshot_processing_state") = SnapshotProcessingState::Started;
}
