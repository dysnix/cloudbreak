// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use cloudbreak_core::{Result, SnapshotConfig, modules::account_owner_map::AccountOwnerMap};
use sea_orm::{ConnectOptions, Database, DatabaseConnection};
use solana_accounts_db::accounts_file::AccountsFile;
use std::{
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};
use tokio::time::Instant;
use tokio::{sync::mpsc::Sender, task::JoinSet};
use yellowstone_grpc_proto::geyser::{
    SubscribeUpdateAccount, SubscribeUpdateAccountInfo, SubscribeUpdateBlock,
};

use crate::{
    db_queries::SnapshotAccountVersion,
    sidecar::{AccountFileData, SnapshotData, SnapshotType},
};

pub mod accountsdb_helpers;
pub mod bootstrap;
mod db_queries;
pub mod lt_hash;
pub mod metrics;
pub mod sidecar;
pub mod stake_data;

pub use db_queries::persist_epoch_stakes;

const DB_ACCOUNTS_BATCH_SIZE: usize = 200;

/// Download and save into postgres the snapshot data for the received slot (getting all snapshots files
///  needed until data to that slot is available)
/// If slot is not provided it will just download the latest available full and incremental snapshots
///
/// Safety Note: This function should be run in a separate thread to avoid blocking the main thread
pub async fn run(
    config: SnapshotConfig,
    received_slot: Option<u64>,
    metrics_registry: Option<prometheus::Registry>,
    buffer_size: Option<Arc<Mutex<usize>>>,
    accounts_owner_map: AccountOwnerMap,
) -> Result<bootstrap::BootstrapRun> {
    run_with_prepared(
        config,
        received_slot,
        metrics_registry,
        buffer_size,
        accounts_owner_map,
        None,
    )
    .await
}

pub async fn run_with_prepared(
    config: SnapshotConfig,
    received_slot: Option<u64>,
    metrics_registry: Option<prometheus::Registry>,
    buffer_size: Option<Arc<Mutex<usize>>>,
    accounts_owner_map: AccountOwnerMap,
    prepared_run: Option<bootstrap::BootstrapRun>,
) -> Result<bootstrap::BootstrapRun> {
    let start_time = Instant::now();

    let database = Database::connect(ConnectOptions::from(config.database.clone())).await?;

    metrics::register_metrics(metrics_registry)?;

    let target_slot = received_slot.unwrap_or(0);
    let mut prepared_run = prepared_run;
    loop {
        let run = match prepared_run.take() {
            Some(run) => run,
            None => bootstrap::prepare_run(&database, target_slot).await?,
        };
        let result = run_prepared_bootstrap(
            &database,
            &config,
            received_slot,
            buffer_size.clone(),
            accounts_owner_map.clone(),
            run.clone(),
        )
        .await;
        match result {
            Ok(()) => {
                tracing::info!(
                    run_id = run.id,
                    "Snapshot bootstrap reached live reconciliation in {} secs",
                    start_time.elapsed().as_secs_f64()
                );
                return Ok(run);
            }
            Err(error) => {
                let unavailable = error.chain().any(|cause| {
                    cause
                        .downcast_ref::<sidecar::SnapshotDownloadError>()
                        .is_some_and(sidecar::SnapshotDownloadError::is_unrecoverable)
                        || cause
                            .downcast_ref::<bootstrap::SnapshotArtifactUnrecoverable>()
                            .is_some()
                });
                if unavailable {
                    tracing::warn!(
                        run_id = run.id,
                        ?error,
                        "Exact snapshot artifacts are unavailable; abandoning durable run"
                    );
                    bootstrap::abandon_run(&database, run.id, "exact_archive_unavailable").await?;
                    continue;
                }
                return Err(error);
            }
        }
    }
}

async fn run_prepared_bootstrap(
    database: &DatabaseConnection,
    config: &SnapshotConfig,
    received_slot: Option<u64>,
    buffer_size: Option<Arc<Mutex<usize>>>,
    accounts_owner_map: AccountOwnerMap,
    run: bootstrap::BootstrapRun,
) -> Result<()> {
    let (snapshot_pair, full_size, incremental_size) =
        if let Some(pair) = bootstrap::load_pair(database, run.id).await? {
            pair
        } else {
            let pair = sidecar::get_snapshot_data(
                &config.tracker_endpoint.endpoint,
                received_slot,
                true,
                false,
            )
            .await?;
            bootstrap::persist_pair(database, run.id, &pair).await?;
            (pair, None, None)
        };

    tracing::info!(run_id = run.id, "Snapshot data: {:?}", snapshot_pair);
    bootstrap::set_phase(database, run.id, bootstrap::PHASE_DOWNLOAD).await?;

    let full_complete =
        bootstrap::archive_ingestion_complete(database, run.id, SnapshotType::Full).await?;
    let full_database = database.clone();
    let full_endpoint = snapshot_pair.downloading_endpoint.clone();
    let full_snapshot = snapshot_pair.full_snapshot.clone();
    let full_config = config.clone();
    let full_owner_map = accounts_owner_map.clone();
    let full = async move {
        if full_complete {
            bootstrap::mark_archive_files_skipped(&full_database, run.id, SnapshotType::Full)
                .await?;
            tracing::info!(
                run_id = run.id,
                "Full snapshot ingestion is already checkpointed"
            );
            Ok(())
        } else {
            process_snapshot_archive(SnapshotArchiveJob {
                database: full_database,
                run_id: run.id,
                sidecar_endpoint: full_endpoint,
                snapshot_data: full_snapshot,
                snapshot_type: SnapshotType::Full,
                recorded_size: full_size,
                config: full_config,
                accounts_owner_map: full_owner_map,
            })
            .await
        }
    };
    if let Some(incremental) = snapshot_pair.incremental_snapshot.clone() {
        let incremental_complete =
            bootstrap::archive_ingestion_complete(database, run.id, SnapshotType::Incremental)
                .await?;
        let incremental_database = database.clone();
        let incremental_endpoint = snapshot_pair.downloading_endpoint.clone();
        let incremental_config = config.clone();
        let incremental_owner_map = accounts_owner_map;
        let incremental = async move {
            if incremental_complete {
                bootstrap::mark_archive_files_skipped(
                    &incremental_database,
                    run.id,
                    SnapshotType::Incremental,
                )
                .await?;
                tracing::info!(
                    run_id = run.id,
                    "Incremental snapshot ingestion is already checkpointed"
                );
                Ok(())
            } else {
                process_snapshot_archive(SnapshotArchiveJob {
                    database: incremental_database,
                    run_id: run.id,
                    sidecar_endpoint: incremental_endpoint,
                    snapshot_data: incremental,
                    snapshot_type: SnapshotType::Incremental,
                    recorded_size: incremental_size,
                    config: incremental_config,
                    accounts_owner_map: incremental_owner_map,
                })
                .await
            }
        };
        let (full_result, incremental_result) = tokio::join!(full, incremental);
        full_result?;
        incremental_result?;
    } else {
        full.await?;
    }
    bootstrap::update_metrics(database, &run).await?;

    if let Some(buffer_size) = buffer_size {
        bootstrap::set_phase(database, run.id, bootstrap::PHASE_CLUSTERING).await?;
        db_queries::cluster_snapshot_accounts_table_resumable(
            database,
            buffer_size,
            config.database.partition_clustering_threshold,
            run.id,
        )
        .await?;
    }
    bootstrap::set_phase(database, run.id, bootstrap::PHASE_DEDUP_PREPARATION).await?;
    db_queries::prepare_deduplication_resumable(database, run.id).await?;
    bootstrap::set_phase(database, run.id, bootstrap::PHASE_DUPLICATE_CLEANUP).await?;
    db_queries::cleanup_duplicates_resumable(database, run.id).await?;
    bootstrap::set_phase(database, run.id, bootstrap::PHASE_CLOSED_CLEANUP).await?;
    db_queries::cleanup_closed_resumable(database, run.id).await?;
    bootstrap::set_phase(database, run.id, bootstrap::PHASE_INDEX_CREATION).await?;
    db_queries::create_database_indexes_resumable(database, &config.pg_indexes, run.id).await?;
    bootstrap::set_phase(database, run.id, bootstrap::PHASE_LIVE_RECONCILIATION).await?;
    Ok(())
}

struct SnapshotArchiveJob {
    database: DatabaseConnection,
    run_id: i64,
    sidecar_endpoint: String,
    snapshot_data: SnapshotData,
    snapshot_type: SnapshotType,
    recorded_size: Option<u64>,
    config: SnapshotConfig,
    accounts_owner_map: AccountOwnerMap,
}

async fn process_snapshot_archive(job: SnapshotArchiveJob) -> Result<()> {
    let SnapshotArchiveJob {
        database,
        run_id,
        sidecar_endpoint,
        snapshot_data,
        snapshot_type,
        recorded_size,
        config,
        accounts_owner_map,
    } = job;
    let base_dir = sidecar::snapshot_base_dir(snapshot_data.slot);
    let archive_size = sidecar::download_snapshot_file_resumable(
        &sidecar_endpoint,
        snapshot_data.clone(),
        snapshot_type,
        &base_dir,
        recorded_size,
    )
    .await?;
    bootstrap::record_archive_download(&database, run_id, snapshot_type, archive_size).await?;
    bootstrap::set_phase(&database, run_id, bootstrap::PHASE_EXTRACTION).await?;

    let account_files =
        prepare_extracted_snapshot(&database, run_id, snapshot_type, &snapshot_data, &base_dir)
            .await?;
    bootstrap::set_phase(&database, run_id, bootstrap::PHASE_INGESTION).await?;
    process_account_files(
        &database,
        run_id,
        snapshot_type,
        account_files,
        config,
        accounts_owner_map,
    )
    .await
}

async fn prepare_extracted_snapshot(
    database: &DatabaseConnection,
    run_id: i64,
    snapshot_type: SnapshotType,
    snapshot_data: &SnapshotData,
    base_dir: &Path,
) -> Result<Vec<AccountFileData>> {
    let checkpoints = bootstrap::account_file_checkpoints(database, run_id, snapshot_type).await?;
    let accounts_dir = base_dir.join("uncompressed_snapshot/accounts");
    if !checkpoints.is_empty() && bootstrap::validate_extracted_files(&accounts_dir, &checkpoints) {
        let mut files = Vec::new();
        for checkpoint in checkpoints {
            let path = accounts_dir.join(&checkpoint.file_name);
            if checkpoint.completed {
                bootstrap::mark_account_file_skipped(
                    database,
                    run_id,
                    snapshot_type,
                    checkpoint.account_slot,
                    checkpoint.write_version,
                )
                .await?;
                if path.exists() {
                    std::fs::remove_file(path)?;
                }
                continue;
            }
            files.push(AccountFileData {
                path,
                size: checkpoint.current_len as usize,
                slot: checkpoint.account_slot as u64,
                write_version: checkpoint.write_version as u64,
            });
        }
        tracing::info!(
            run_id,
            archive_type = bootstrap::archive_type(snapshot_type),
            pending = files.len(),
            "Reusing validated extracted snapshot files"
        );
        return Ok(files);
    }

    let staging_dir = base_dir.join(format!(
        "extracting-{run_id}-{}",
        bootstrap::archive_type(snapshot_type)
    ));
    if staging_dir.exists() {
        std::fs::remove_dir_all(&staging_dir)?;
    }
    std::fs::create_dir_all(&staging_dir)?;
    let archive_path = base_dir.join(&snapshot_data.file_name);
    let unpacked = match sidecar::unpack_compressed_snapshot(
        archive_path.clone(),
        &staging_dir,
        snapshot_data.slot,
    ) {
        Ok(unpacked) => unpacked,
        Err(error) => {
            tracing::warn!(
                run_id,
                archive = %archive_path.display(),
                ?error,
                "Snapshot archive could not be extracted; removing the local copy so the exact archive is downloaded again"
            );
            let _ = std::fs::remove_file(&archive_path);
            let _ = std::fs::remove_dir_all(&staging_dir);
            let failures =
                bootstrap::record_archive_validation_failure(database, run_id, snapshot_type)
                    .await?;
            if failures >= 2 {
                return Err(bootstrap::SnapshotArtifactUnrecoverable.into());
            }
            return Err(error);
        }
    };
    db_queries::persist_epoch_stakes(database, &unpacked.stake_data).await?;

    let replacement = staging_dir.join("uncompressed_snapshot");
    let destination = base_dir.join("uncompressed_snapshot");
    let backup = base_dir.join(format!(
        "uncompressed_snapshot.replaced-{run_id}-{}",
        bootstrap::archive_type(snapshot_type)
    ));
    if backup.exists() {
        std::fs::remove_dir_all(&backup)?;
    }
    if destination.exists() {
        std::fs::rename(&destination, &backup)?;
    }
    if let Err(error) = std::fs::rename(&replacement, &destination) {
        if backup.exists() && !destination.exists() {
            let _ = std::fs::rename(&backup, &destination);
        }
        return Err(error.into());
    }
    if backup.exists() {
        std::fs::remove_dir_all(&backup)?;
    }
    std::fs::remove_dir_all(&staging_dir)?;

    let files = unpacked
        .account_files
        .into_iter()
        .map(|file| AccountFileData {
            path: destination.join(
                file.path
                    .file_name()
                    .expect("account file must have a name"),
            ),
            ..file
        })
        .collect::<Vec<_>>();
    bootstrap::persist_manifest(database, run_id, snapshot_type, &files).await?;
    let checkpoints = bootstrap::account_file_checkpoints(database, run_id, snapshot_type).await?;
    let completed = checkpoints
        .iter()
        .filter(|checkpoint| checkpoint.completed)
        .map(|checkpoint| checkpoint.file_name.as_str())
        .collect::<std::collections::HashSet<_>>();
    for checkpoint in &checkpoints {
        if checkpoint.completed {
            let path = accounts_dir.join(&checkpoint.file_name);
            if path.exists() {
                std::fs::remove_file(path)?;
            }
        }
    }
    Ok(files
        .into_iter()
        .filter(|file| {
            file.path
                .file_name()
                .and_then(|name| name.to_str())
                .is_none_or(|name| !completed.contains(name))
        })
        .collect())
}

/// Processes one account file atomically with its durable completion checkpoint.
async fn process_account_files(
    database: &DatabaseConnection,
    run_id: i64,
    snapshot_type: SnapshotType,
    solana_snapshot: Vec<AccountFileData>,
    config: SnapshotConfig,
    accounts_owner_map: AccountOwnerMap,
) -> Result<()> {
    let start_time = Instant::now();
    let total_accounts_files_opening_time_micros = Arc::new(Mutex::new(0));

    let mut account_file_workers: JoinSet<Result<()>> = JoinSet::new();
    let accounts_file_concurency = config.accounts_file_concurency.unwrap_or(32);
    let programs_include = config
        .programs
        .include
        .iter()
        .map(|p| p.0)
        .collect::<Vec<_>>();
    let programs_exclude = config
        .programs
        .exclude
        .iter()
        .map(|p| p.0)
        .collect::<Vec<_>>();

    let total_accounts_files_count = solana_snapshot.len();
    let accounts_files_processed = Arc::new(Mutex::new(0));
    let mut last_log_time = Instant::now();

    let accounts_count = Arc::new(Mutex::new(0));

    for AccountFileData {
        path,
        size: current_len,
        slot: account_file_slot,
        write_version,
    } in solana_snapshot
    {
        let accounts_count = accounts_count.clone();
        let programs_include = programs_include.clone();
        let programs_exclude = programs_exclude.clone();
        let database = database.clone();

        let percentage_processed =
            *accounts_files_processed.lock().unwrap() * 100 / total_accounts_files_count;

        metrics::PROCESSED_SNAPSHOT_ITEMS
            .with_label_values(&["accounts_files_total"])
            .inc();
        metrics::PROCESSED_SNAPSHOT_ITEMS
            .with_label_values(&["accounts_files_percentage"])
            .set(percentage_processed as f64);
        metrics::PROCESSED_SNAPSHOT_ITEMS
            .with_label_values(&["accounts_total"])
            .set(*accounts_count.lock().unwrap() as f64);

        *accounts_files_processed.lock().unwrap() += 1;

        if last_log_time.elapsed().as_secs() > 30 {
            tracing::info!(target: "processed_snapshot_items", "Accounts files processed: {}% - Accounts total: {}", percentage_processed, *accounts_count.lock().unwrap());
            last_log_time = Instant::now();
        }

        let total_accounts_files_opening_time_micros =
            total_accounts_files_opening_time_micros.clone();

        let accounts_owner_map = accounts_owner_map.clone();

        account_file_workers.spawn(async move {
            let working_path = path.with_file_name(format!(
                ".{}.cloudbreak-working-{run_id}",
                path.file_name()
                    .and_then(|name| name.to_str())
                    .unwrap_or("appendvec")
            ));
            if working_path.exists() {
                std::fs::remove_file(&working_path)?;
            }
            std::fs::hard_link(&path, &working_path)?;
            let start_time = Instant::now();
            let accounts = AccountsFile::new_for_startup(
                working_path,
                current_len,
                solana_accounts_db::accounts_file::StorageAccess::default(),
            )?;

            let elapsed = start_time.elapsed().as_micros();
            *total_accounts_files_opening_time_micros
                .lock()
                .expect("Failed to lock total_accounts_files_opening_time_micros") += elapsed;

            let mut all_accounts_chunks = Vec::new();
            let mut current_accounts_chunk = Vec::new();
            let mut snapshot_account_versions = Vec::new();

            // Collect all account offsets first
            let mut offsets = Vec::new();
            accounts.scan_accounts_without_data(|offset, _| {
                offsets.push(offset);
            })?;

            // Fetch full account data for each offset
            for offset in offsets {
                accounts.get_stored_account_callback(offset, |account| {
                    // Regardless of the owners being excluded or included, we add the snapshot account version to
                    // the list, we need all accounts there for later deduplication
                    snapshot_account_versions.push(SnapshotAccountVersion {
                        pubkey: account.pubkey().to_bytes().to_vec(),
                        slot: account_file_slot,
                        owner: account.owner.to_bytes().to_vec(),
                    });

                    if !programs_include.is_empty() {
                        if !programs_include.contains(account.owner) {
                            return;
                        }
                    } else if programs_exclude.contains(account.owner) {
                        return;
                    }

                    let pubkey = account.pubkey.to_bytes().to_vec();
                    let owner = account.owner.to_bytes().to_vec();

                    // Add non closed accounts to the accounts owner map (if enabled)
                    if account.lamports > 0 {
                        accounts_owner_map.upsert_account(&pubkey, &owner, account_file_slot);
                    }

                    let account_update = SubscribeUpdateAccount {
                        account: Some(SubscribeUpdateAccountInfo {
                            pubkey,
                            lamports: account.lamports,
                            owner,
                            executable: account.executable,
                            rent_epoch: account.rent_epoch,
                            data: account.data.to_vec(),
                            write_version,
                            txn_signature: None,
                        }),
                        slot: account_file_slot,
                        is_startup: true,
                    };

                    current_accounts_chunk.push(account_update);

                    if current_accounts_chunk.len() >= DB_ACCOUNTS_BATCH_SIZE {
                        all_accounts_chunks.push(std::mem::take(&mut current_accounts_chunk));
                    }

                    *accounts_count.lock().unwrap() += 1;
                });
            }
            if !current_accounts_chunk.is_empty() {
                all_accounts_chunks.push(current_accounts_chunk);
            }

            for chunk in all_accounts_chunks {
                if chunk.is_empty() {
                    tracing::warn!(
                        "chunk is empty for slot: {} and write version: {}",
                        account_file_slot,
                        write_version
                    );
                    continue;
                }

                db_queries::upsert_accounts_batched(&database, chunk).await?;
            }

            drop(accounts);
            bootstrap::commit_account_file(
                &database,
                run_id,
                snapshot_type,
                account_file_slot,
                write_version,
                snapshot_account_versions,
            )
            .await?;
            std::fs::remove_file(&path)?;

            Ok(())
        });

        if account_file_workers.len() >= accounts_file_concurency {
            account_file_workers
                .join_next()
                .await
                .expect("not expected empty account_file_workers")??;
        }
    }

    while let Some(res) = account_file_workers.join_next().await {
        res??;
    }

    let elapsed = start_time.elapsed().as_secs_f64();
    tracing::info!(target: "total_snapshot_accounts", "Snapshot processed! - Accounts count: {} in {} seconds", accounts_count.lock().unwrap(), elapsed);
    tracing::info!(target: "total_snapshot_accounts", "Total accounts files opening time: {} seconds", *total_accounts_files_opening_time_micros.lock().unwrap() / 1_000_000);

    let run = bootstrap::BootstrapRun {
        id: run_id,
        target_slot: 0,
        covered_slot: None,
        phase: bootstrap::PHASE_INGESTION.to_string(),
        source: "snapshot".to_string(),
        resumed_count: 0,
    };
    bootstrap::update_metrics(database, &run).await?;
    Ok(())
}

/// Version of the `process_downloaded_snapshot` function that only processes the slots that are in the gaps list
pub async fn process_downloaded_snapshot_with_gap_filling(
    snapshot_slot: u64,
    incremental_snapshot_file_name: String,
    base_dir: PathBuf,
    config: SnapshotConfig,
    gaps_list: Vec<u64>,
    block_sender: Sender<SubscribeUpdateBlock>,
) -> Result<()> {
    let start_time = Instant::now();

    let path = base_dir.join(&incremental_snapshot_file_name);
    let sidecar::UnpackedSnapshot {
        account_files: solana_snapshot,
        stake_data: _,
    } = sidecar::unpack_compressed_snapshot(path, &base_dir, snapshot_slot)?;
    let mut account_file_workers: JoinSet<Result<()>> = JoinSet::new();
    let accounts_file_concurency = config.accounts_file_concurency.unwrap_or(32);
    let programs_include = config
        .programs
        .include
        .iter()
        .map(|p| p.0)
        .collect::<Vec<_>>();
    let programs_exclude = config
        .programs
        .exclude
        .iter()
        .map(|p| p.0)
        .collect::<Vec<_>>();

    let total_accounts_files_count = solana_snapshot.len();
    let accounts_files_processed = Arc::new(Mutex::new(0));
    let mut last_log_time = Instant::now();

    let accounts_count = Arc::new(Mutex::new(0));

    for AccountFileData {
        path,
        size: current_len,
        slot: account_file_slot,
        write_version,
    } in solana_snapshot
    {
        if !gaps_list.contains(&account_file_slot) {
            continue;
        }

        let accounts_count = accounts_count.clone();
        let programs_include = programs_include.clone();
        let programs_exclude = programs_exclude.clone();

        let percentage_processed =
            *accounts_files_processed.lock().unwrap() * 100 / total_accounts_files_count;

        metrics::PROCESSED_SNAPSHOT_ITEMS
            .with_label_values(&["accounts_files_total"])
            .inc();
        metrics::PROCESSED_SNAPSHOT_ITEMS
            .with_label_values(&["accounts_files_percentage"])
            .set(percentage_processed as f64);
        metrics::PROCESSED_SNAPSHOT_ITEMS
            .with_label_values(&["accounts_total"])
            .set(*accounts_count.lock().unwrap() as f64);

        *accounts_files_processed.lock().unwrap() += 1;

        if last_log_time.elapsed().as_secs() > 30 {
            tracing::info!(target: "processed_snapshot_items", "Accounts files processed: {}% - Accounts total: {}", percentage_processed, *accounts_count.lock().unwrap());
            last_log_time = Instant::now();
        }

        let block_sender = block_sender.clone();
        account_file_workers.spawn(async move {
            let accounts = AccountsFile::new_for_startup(
                path,
                current_len,
                solana_accounts_db::accounts_file::StorageAccess::default(),
            )?;

            let mut accounts_for_slot = Vec::new();

            // Collect all account offsets first
            let mut offsets = Vec::new();
            accounts.scan_accounts_without_data(|offset, _| {
                offsets.push(offset);
            })?;

            // Fetch full account data for each offset
            for offset in offsets {
                accounts.get_stored_account_callback(offset, |account| {
                    if !programs_include.is_empty() {
                        // We always include accounts being closed, they are needed for later cleanup of older versions of the accounts
                        if !programs_include.contains(account.owner) && account.lamports > 0 {
                            return;
                        }
                    } else if programs_exclude.contains(account.owner) {
                        return;
                    }

                    let account_update = SubscribeUpdateAccountInfo {
                        pubkey: account.pubkey.to_bytes().to_vec(),
                        lamports: account.lamports,
                        owner: account.owner.to_bytes().to_vec(),
                        executable: account.executable,
                        rent_epoch: account.rent_epoch,
                        data: account.data.to_vec(),
                        write_version,
                        txn_signature: None,
                    };

                    accounts_for_slot.push(account_update);

                    *accounts_count.lock().unwrap() += 1;
                });
            }

            let accounts_for_slot_len = accounts_for_slot.len();

            block_sender
                .send(SubscribeUpdateBlock {
                    slot: account_file_slot,
                    accounts: accounts_for_slot,
                    block_height: None,
                    block_time: None,
                    blockhash: String::new(),
                    rewards: None,
                    parent_slot: 0,
                    parent_blockhash: String::new(),
                    executed_transaction_count: 0,
                    transactions: Vec::new(),
                    updated_account_count: accounts_for_slot_len as u64,
                    entries_count: 0,
                    entries: Vec::new(),
                })
                .await?;

            Ok(())
        });

        if account_file_workers.len() >= accounts_file_concurency {
            account_file_workers
                .join_next()
                .await
                .expect("not expected empty account_file_workers")??;
        }
    }

    while let Some(res) = account_file_workers.join_next().await {
        res??;
    }

    let elapsed = start_time.elapsed().as_secs_f64();
    tracing::info!(target: "total_snapshot_accounts", "Snapshot processed! - Accounts count: {} in {} seconds", accounts_count.lock().unwrap(), elapsed);

    Ok(())
}

#[cfg(test)]
mod continuation_tests {
    use super::*;

    #[test]
    fn appendvec_drop_removes_only_the_working_hard_link() {
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("1.1");
        let working = dir.path().join(".1.1.cloudbreak-working-1");
        std::fs::write(&source, [0_u8; 1024]).unwrap();
        std::fs::hard_link(&source, &working).unwrap();

        let accounts = AccountsFile::new_for_startup(
            &working,
            0,
            solana_accounts_db::accounts_file::StorageAccess::File,
        )
        .unwrap();
        drop(accounts);

        assert!(source.exists());
        assert!(!working.exists());
    }
}
