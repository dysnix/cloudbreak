// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use cloudbreak_core::{IndexConfig, SnapshotConfig, modules::account_owner_map::AccountOwnerMap};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use crate::metrics;
use crate::modules::finalize_slot::UpdatedAccountsDuringStartup;

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
///
/// When there is no `snapshot` config section there is nothing to bootstrap, so startup is
/// already effectively finished: we clear the `Startup` unhealthy reason once (via
/// [`UpdatedAccountsDuringStartup::finish_startup_without_snapshot`]) so a no-snapshot node
/// reports healthy and its slot-dependent reads are not gated forever.
pub async fn process_snapshot_if_needed(
    config: IndexConfig,
    slot: u64,
    updated_accounts_during_startup: &UpdatedAccountsDuringStartup,
    finalize_slot_buffer_size: Arc<Mutex<usize>>,
    accounts_owner_map: AccountOwnerMap,
) {
    let snapshot_config = match config.snapshot {
        Some(snapshot_config) => snapshot_config,
        None => {
            updated_accounts_during_startup
                .finish_startup_without_snapshot()
                .await;
            return;
        }
    };

    let snapshot_processing_state = &updated_accounts_during_startup.snapshot_processing_state;

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

        let run_config = SnapshotConfig {
            accounts_file_concurency: snapshot_config.accounts_file_concurency,
            database: config.database,
            tracker_endpoint: snapshot_config.tracker_endpoint,
            metrics: config.metrics,
            programs: config.programs,
            pg_indexes: snapshot_config.pg_indexes,
        };

        let mut attempt = 1_u64;
        loop {
            match cloudbreak_snapshot::run(
                run_config.clone(),
                Some(slot),
                Some(metrics::METRICS_REGISTRY.clone()),
                Some(finalize_slot_buffer_size.clone()),
                accounts_owner_map.clone(),
            )
            .await
            {
                Ok(()) => break,
                Err(error) => {
                    tracing::error!(
                        attempt,
                        ?error,
                        "Failed to process snapshot; retrying in 10 seconds"
                    );
                    attempt += 1;
                    tokio::time::sleep(Duration::from_secs(10)).await;
                }
            }
        }

        *snapshot_processing_state_clone
            .lock()
            .expect("Failed to lock snapshot_processing_state") = SnapshotProcessingState::Finished;
    });

    *snapshot_processing_state
        .lock()
        .expect("Failed to lock snapshot_processing_state") = SnapshotProcessingState::Started;
}
