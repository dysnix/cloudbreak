// SPDX-License-Identifier: AGPL-3.0-only

use std::{collections::HashSet, path::Path};

use cloudbreak_core::SnapshotPgIndexesConfig;
use sea_orm::{
    ConnectionTrait, DatabaseBackend, DatabaseConnection, FromQueryResult, Statement,
    TransactionTrait, Value,
};

use crate::{
    db_queries::SnapshotAccountVersion,
    metrics,
    sidecar::{AccountFileData, SnapshotData, SnapshotPair, SnapshotType},
};

const PHASE_PAIR_SELECTION: &str = "pair_selection";
pub const PHASE_DOWNLOAD: &str = "download";
pub const PHASE_EXTRACTION: &str = "extraction";
pub const PHASE_INGESTION: &str = "ingestion";
pub const PHASE_CLUSTERING: &str = "clustering";
pub const PHASE_DEDUP_PREPARATION: &str = "deduplication_preparation";
pub const PHASE_DUPLICATE_CLEANUP: &str = "duplicate_cleanup";
pub const PHASE_CLOSED_CLEANUP: &str = "closed_account_cleanup";
pub const PHASE_INDEX_CREATION: &str = "index_creation";
pub const PHASE_LIVE_RECONCILIATION: &str = "live_update_reconciliation";
pub const PHASE_READY: &str = "ready";

#[derive(Debug, thiserror::Error)]
#[error("the exact snapshot archive repeatedly failed validation")]
pub struct SnapshotArtifactUnrecoverable;

#[derive(Debug, Clone, FromQueryResult)]
pub struct BootstrapRun {
    pub id: i64,
    pub target_slot: i64,
    pub covered_slot: Option<i64>,
    pub phase: String,
    pub source: String,
    pub resumed_count: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartupBootstrapState {
    Ready { replay_slot: u64 },
    NeedsBootstrap { replay_slot: Option<u64> },
}

#[derive(Debug, Clone, FromQueryResult)]
struct ArchiveRow {
    archive_type: String,
    file_name: String,
    slot: i64,
    base_slot: Option<i64>,
    downloading_endpoint: String,
    download_url: Option<String>,
    archive_size: Option<i64>,
}

#[derive(Debug, Clone, FromQueryResult)]
pub struct AccountFileCheckpoint {
    pub archive_type: String,
    pub file_name: String,
    pub account_slot: i64,
    pub write_version: i64,
    pub current_len: i64,
    pub disk_size: i64,
    pub account_count: Option<i64>,
    pub completed: bool,
}

fn statement(sql: impl Into<String>, values: Vec<Value>) -> Statement {
    Statement::from_sql_and_values(DatabaseBackend::Postgres, sql, values)
}

pub fn archive_type(snapshot_type: SnapshotType) -> &'static str {
    match snapshot_type {
        SnapshotType::Full => "full",
        SnapshotType::Incremental => "incremental",
    }
}

fn required_index_names(cfg: &SnapshotPgIndexesConfig) -> Vec<&'static str> {
    let mut indexes = Vec::new();
    if cfg.idx_snapshot_accounts_pubkey {
        indexes.push("idx_snapshot_accounts_pubkey");
    }
    if cfg.idx_snapshot_accounts_token_mint {
        indexes.push("idx_snapshot_accounts_token_mint");
    }
    if cfg.idx_snapshot_accounts_token_owner {
        indexes.push("idx_snapshot_accounts_token_owner");
    }
    if cfg.idx_snapshot_accounts_pubkey_slot {
        indexes.push("idx_snapshot_accounts_pubkey_slot");
    }
    if cfg.idx_snapshot_accounts_token_delegate {
        indexes.push("idx_snapshot_accounts_token_delegate");
    }
    indexes
}

async fn latest_run(db: &DatabaseConnection) -> Result<Option<BootstrapRun>, anyhow::Error> {
    Ok(BootstrapRun::find_by_statement(Statement::from_string(
        DatabaseBackend::Postgres,
        "SELECT id, target_slot, covered_slot, phase, source, resumed_count \
         FROM snapshot_bootstrap_runs ORDER BY id DESC LIMIT 1",
    ))
    .one(db)
    .await?)
}

pub async fn inspect_or_adopt_ready_database(
    db: &DatabaseConnection,
    cfg: &SnapshotPgIndexesConfig,
) -> Result<StartupBootstrapState, anyhow::Error> {
    if let Some(run) = latest_run(db).await? {
        update_metrics(db, &run).await?;
        if run.phase == PHASE_READY {
            return Ok(StartupBootstrapState::Ready {
                replay_slot: persisted_confirmed_tip(db)
                    .await?
                    .unwrap_or(run.covered_slot.unwrap_or(run.target_slot) as u64),
            });
        }
        return Ok(StartupBootstrapState::NeedsBootstrap {
            replay_slot: persisted_confirmed_tip(db).await?,
        });
    }

    let snapshot_has_rows: bool = db
        .query_one(Statement::from_string(
            DatabaseBackend::Postgres,
            "SELECT EXISTS (SELECT 1 FROM snapshot_accounts LIMIT 1) AS present",
        ))
        .await?
        .ok_or_else(|| anyhow::anyhow!("snapshot table existence query returned no row"))?
        .try_get("", "present")?;
    let confirmed_tip = persisted_confirmed_tip(db).await?;
    let finalized_tip = persisted_finalized_tip(db).await?;
    let required_indexes = required_index_names(cfg);
    let present_indexes = existing_indexes(db).await?;
    let indexes_ready = required_indexes
        .iter()
        .all(|index| present_indexes.contains(*index));

    if let (true, Some(replay_slot), Some(_)) = (
        snapshot_has_rows && indexes_ready,
        confirmed_tip,
        finalized_tip,
    ) {
        let row = BootstrapRun::find_by_statement(statement(
            "INSERT INTO snapshot_bootstrap_runs \
             (target_slot, covered_slot, phase, source, completed_at) \
             VALUES ($1, $2, 'ready', 'adopted_existing', CURRENT_TIMESTAMP) \
             RETURNING id, target_slot, covered_slot, phase, source, resumed_count",
            vec![
                Value::BigInt(Some(replay_slot as i64)),
                Value::BigInt(Some(replay_slot as i64)),
            ],
        ))
        .one(db)
        .await?
        .ok_or_else(|| anyhow::anyhow!("failed to return adopted bootstrap run"))?;
        tracing::info!(
            run_id = row.id,
            replay_slot,
            "Adopted existing ready Cloudbreak database"
        );
        update_metrics(db, &row).await?;
        return Ok(StartupBootstrapState::Ready { replay_slot });
    }

    Ok(StartupBootstrapState::NeedsBootstrap {
        replay_slot: confirmed_tip,
    })
}

async fn persisted_tip(
    db: &DatabaseConnection,
    commitment: i32,
) -> Result<Option<u64>, anyhow::Error> {
    let row = db
        .query_one(statement(
            "SELECT MAX(slot) AS slot FROM slots WHERE commitment = $1",
            vec![Value::Int(Some(commitment))],
        ))
        .await?;
    let slot: Option<i64> = row.and_then(|row| row.try_get("", "slot").ok()).flatten();
    Ok(slot.map(|slot| slot as u64))
}

pub async fn persisted_confirmed_tip(
    db: &DatabaseConnection,
) -> Result<Option<u64>, anyhow::Error> {
    persisted_tip(db, 1).await
}

async fn persisted_finalized_tip(db: &DatabaseConnection) -> Result<Option<u64>, anyhow::Error> {
    persisted_tip(db, 2).await
}

async fn existing_indexes(db: &DatabaseConnection) -> Result<HashSet<String>, anyhow::Error> {
    let rows = db
        .query_all(Statement::from_string(
            DatabaseBackend::Postgres,
            "SELECT indexname FROM pg_indexes WHERE schemaname = current_schema() \
             AND tablename = 'snapshot_accounts'",
        ))
        .await?;
    rows.into_iter()
        .map(|row| Ok(row.try_get("", "indexname")?))
        .collect()
}

pub async fn prepare_run(
    db: &DatabaseConnection,
    target_slot: u64,
) -> Result<BootstrapRun, anyhow::Error> {
    if let Some(mut run) = BootstrapRun::find_by_statement(Statement::from_string(
        DatabaseBackend::Postgres,
        "SELECT id, target_slot, covered_slot, phase, source, resumed_count \
         FROM snapshot_bootstrap_runs WHERE phase NOT IN ('ready', 'abandoned') \
         ORDER BY id DESC LIMIT 1",
    ))
    .one(db)
    .await?
    {
        if checkpoints_incompatible_with_unlogged_tables(db, run.id).await? {
            abandon_run(db, run.id, "unlogged_relations_truncated").await?;
        } else {
            db.execute(statement(
                "UPDATE snapshot_bootstrap_runs SET resumed_count = resumed_count + 1, \
                 updated_at = CURRENT_TIMESTAMP WHERE id = $1",
                vec![Value::BigInt(Some(run.id))],
            ))
            .await?;
            db.execute(statement(
                "UPDATE snapshot_bootstrap_account_files SET skipped_on_resume = false WHERE run_id = $1",
                vec![Value::BigInt(Some(run.id))],
            ))
            .await?;
            run.resumed_count += 1;
            metrics::BOOTSTRAP_RESUMED_RUNS_TOTAL.inc();
            tracing::info!(run_id = run.id, phase = %run.phase, "Resuming durable snapshot bootstrap");
            update_metrics(db, &run).await?;
            return Ok(run);
        }
    }

    reset_snapshot_work(db).await?;
    let run = BootstrapRun::find_by_statement(statement(
        "INSERT INTO snapshot_bootstrap_runs (target_slot, phase) VALUES ($1, $2) \
         RETURNING id, target_slot, covered_slot, phase, source, resumed_count",
        vec![
            Value::BigInt(Some(target_slot as i64)),
            Value::String(Some(Box::new(PHASE_PAIR_SELECTION.to_string()))),
        ],
    ))
    .one(db)
    .await?
    .ok_or_else(|| anyhow::anyhow!("failed to return new bootstrap run"))?;
    update_metrics(db, &run).await?;
    Ok(run)
}

async fn checkpoints_incompatible_with_unlogged_tables(
    db: &DatabaseConnection,
    run_id: i64,
) -> Result<bool, anyhow::Error> {
    let completed: i64 = db
        .query_one(statement(
            "SELECT COUNT(*) AS count FROM snapshot_bootstrap_account_files \
             WHERE run_id = $1 AND completed_at IS NOT NULL",
            vec![Value::BigInt(Some(run_id))],
        ))
        .await?
        .ok_or_else(|| anyhow::anyhow!("completed checkpoint count returned no row"))?
        .try_get("", "count")?;
    if completed == 0 {
        return Ok(false);
    }

    let mut any_unlogged_rows = false;
    for table in ["temp_snapshot_account_versions", "snapshot_accounts"] {
        let sql = format!("SELECT EXISTS (SELECT 1 FROM {table} LIMIT 1) AS present");
        let present: bool = db
            .query_one(Statement::from_string(DatabaseBackend::Postgres, sql))
            .await?
            .ok_or_else(|| anyhow::anyhow!("relation check returned no row"))?
            .try_get("", "present")?;
        any_unlogged_rows |= present;
    }
    Ok(!any_unlogged_rows)
}

pub async fn abandon_run(
    db: &DatabaseConnection,
    run_id: i64,
    reason: &str,
) -> Result<(), anyhow::Error> {
    let artifact_slots = db
        .query_all(statement(
            "SELECT DISTINCT slot FROM snapshot_bootstrap_archives WHERE run_id = $1",
            vec![Value::BigInt(Some(run_id))],
        ))
        .await?
        .into_iter()
        .map(|row| row.try_get::<i64>("", "slot"))
        .collect::<Result<Vec<_>, _>>()?;
    db.execute(statement(
        "UPDATE snapshot_bootstrap_runs SET phase = 'abandoned', abandon_reason = $2, \
         updated_at = CURRENT_TIMESTAMP, completed_at = CURRENT_TIMESTAMP WHERE id = $1",
        vec![
            Value::BigInt(Some(run_id)),
            Value::String(Some(Box::new(reason.to_string()))),
        ],
    ))
    .await?;
    metrics::BOOTSTRAP_DISCARDED_RUNS_TOTAL
        .with_label_values(&[reason])
        .inc();
    reset_snapshot_work(db).await?;
    for slot in artifact_slots {
        let path = crate::sidecar::snapshot_base_dir(slot as u64);
        if path.exists() {
            std::fs::remove_dir_all(&path)?;
        }
    }
    Ok(())
}

async fn reset_snapshot_work(db: &DatabaseConnection) -> Result<(), anyhow::Error> {
    db.execute_unprepared(
        r#"
        DROP TABLE IF EXISTS accounts_to_delete;
        DROP TABLE IF EXISTS temp_snapshot_account_versions;
        DROP INDEX IF EXISTS idx_snapshot_accounts_pubkey;
        DROP INDEX IF EXISTS idx_snapshot_accounts_token_mint;
        DROP INDEX IF EXISTS idx_snapshot_accounts_token_owner;
        DROP INDEX IF EXISTS idx_snapshot_accounts_pubkey_slot;
        DROP INDEX IF EXISTS idx_snapshot_accounts_token_delegate;
        TRUNCATE TABLE snapshot_accounts;
        CREATE UNLOGGED TABLE temp_snapshot_account_versions (
            pubkey BYTEA NOT NULL,
            slot BIGINT NOT NULL,
            owner BYTEA NOT NULL
        );
        "#,
    )
    .await?;
    Ok(())
}

pub async fn load_pair(
    db: &DatabaseConnection,
    run_id: i64,
) -> Result<Option<(SnapshotPair, Option<u64>, Option<u64>)>, anyhow::Error> {
    let rows = ArchiveRow::find_by_statement(statement(
        "SELECT archive_type, file_name, slot, base_slot, downloading_endpoint, download_url, archive_size \
         FROM snapshot_bootstrap_archives WHERE run_id = $1 ORDER BY archive_type",
        vec![Value::BigInt(Some(run_id))],
    ))
    .all(db)
    .await?;
    if rows.is_empty() {
        return Ok(None);
    }

    let endpoint = rows[0].downloading_endpoint.clone();
    let mut full = None;
    let mut incremental = None;
    let mut full_size = None;
    let mut incremental_size = None;
    for row in rows {
        let snapshot_type = if row.archive_type == "full" {
            SnapshotType::Full
        } else {
            SnapshotType::Incremental
        };
        let data = SnapshotData {
            file_name: row.file_name,
            base_slot: row.base_slot.map(|slot| slot as u64),
            slot: row.slot as u64,
            snapshot_type,
            download_url: row.download_url,
        };
        match snapshot_type {
            SnapshotType::Full => {
                full_size = row.archive_size.map(|size| size as u64);
                full = Some(data);
            }
            SnapshotType::Incremental => {
                incremental_size = row.archive_size.map(|size| size as u64);
                incremental = Some(data);
            }
        }
    }
    let full_snapshot =
        full.ok_or_else(|| anyhow::anyhow!("persisted pair has no full archive"))?;
    Ok(Some((
        SnapshotPair {
            full_snapshot,
            incremental_snapshot: incremental,
            downloading_endpoint: endpoint,
        },
        full_size,
        incremental_size,
    )))
}

pub async fn persist_pair(
    db: &DatabaseConnection,
    run_id: i64,
    pair: &SnapshotPair,
) -> Result<(), anyhow::Error> {
    let txn = db.begin().await?;
    persist_archive(
        &txn,
        run_id,
        &pair.downloading_endpoint,
        &pair.full_snapshot,
    )
    .await?;
    if let Some(incremental) = &pair.incremental_snapshot {
        persist_archive(&txn, run_id, &pair.downloading_endpoint, incremental).await?;
    }
    let covered_slot = pair
        .incremental_snapshot
        .as_ref()
        .map(|snapshot| snapshot.slot)
        .unwrap_or(pair.full_snapshot.slot);
    txn.execute(statement(
        "UPDATE snapshot_bootstrap_runs SET covered_slot = $2, phase = $3, \
         updated_at = CURRENT_TIMESTAMP WHERE id = $1",
        vec![
            Value::BigInt(Some(run_id)),
            Value::BigInt(Some(covered_slot as i64)),
            Value::String(Some(Box::new(PHASE_DOWNLOAD.to_string()))),
        ],
    ))
    .await?;
    txn.commit().await?;
    Ok(())
}

async fn persist_archive<C: ConnectionTrait>(
    db: &C,
    run_id: i64,
    endpoint: &str,
    snapshot: &SnapshotData,
) -> Result<(), anyhow::Error> {
    db.execute(statement(
        "INSERT INTO snapshot_bootstrap_archives \
         (run_id, archive_type, file_name, slot, base_slot, downloading_endpoint, download_url) \
         VALUES ($1, $2, $3, $4, $5, $6, $7)",
        vec![
            Value::BigInt(Some(run_id)),
            Value::String(Some(Box::new(
                archive_type(snapshot.snapshot_type).to_string(),
            ))),
            Value::String(Some(Box::new(snapshot.file_name.clone()))),
            Value::BigInt(Some(snapshot.slot as i64)),
            Value::BigInt(snapshot.base_slot.map(|slot| slot as i64)),
            Value::String(Some(Box::new(endpoint.to_string()))),
            Value::String(snapshot.download_url.clone().map(Box::new)),
        ],
    ))
    .await?;
    Ok(())
}

pub async fn record_archive_download(
    db: &DatabaseConnection,
    run_id: i64,
    snapshot_type: SnapshotType,
    size: u64,
) -> Result<(), anyhow::Error> {
    db.execute(statement(
        "UPDATE snapshot_bootstrap_archives SET archive_size = $3, downloaded_at = CURRENT_TIMESTAMP \
         WHERE run_id = $1 AND archive_type = $2",
        vec![
            Value::BigInt(Some(run_id)),
            Value::String(Some(Box::new(archive_type(snapshot_type).to_string()))),
            Value::BigInt(Some(size as i64)),
        ],
    ))
    .await?;
    Ok(())
}

pub async fn record_archive_validation_failure(
    db: &DatabaseConnection,
    run_id: i64,
    snapshot_type: SnapshotType,
) -> Result<i32, anyhow::Error> {
    let row = db
        .query_one(statement(
            "UPDATE snapshot_bootstrap_archives SET validation_failures = validation_failures + 1 \
             WHERE run_id = $1 AND archive_type = $2 RETURNING validation_failures",
            vec![
                Value::BigInt(Some(run_id)),
                Value::String(Some(Box::new(archive_type(snapshot_type).to_string()))),
            ],
        ))
        .await?
        .ok_or_else(|| anyhow::anyhow!("archive validation failure had no persisted archive"))?;
    Ok(row.try_get("", "validation_failures")?)
}

pub async fn set_phase(
    db: &DatabaseConnection,
    run_id: i64,
    phase: &str,
) -> Result<(), anyhow::Error> {
    let result = db
        .execute(statement(
            "UPDATE snapshot_bootstrap_runs SET phase = $2, updated_at = CURRENT_TIMESTAMP \
         WHERE id = $1 AND array_position(ARRAY[\
           'pair_selection', 'download', 'extraction', 'ingestion', 'clustering', \
           'deduplication_preparation', 'duplicate_cleanup', 'closed_account_cleanup', \
           'index_creation', 'live_update_reconciliation', 'ready'\
         ]::text[], phase) <= array_position(ARRAY[\
           'pair_selection', 'download', 'extraction', 'ingestion', 'clustering', \
           'deduplication_preparation', 'duplicate_cleanup', 'closed_account_cleanup', \
           'index_creation', 'live_update_reconciliation', 'ready'\
         ]::text[], $2)",
            vec![
                Value::BigInt(Some(run_id)),
                Value::String(Some(Box::new(phase.to_string()))),
            ],
        ))
        .await?;
    if result.rows_affected() > 0 {
        metrics::set_bootstrap_phase(run_id, phase, "snapshot");
    }
    Ok(())
}

pub async fn persist_manifest(
    db: &DatabaseConnection,
    run_id: i64,
    snapshot_type: SnapshotType,
    files: &[AccountFileData],
) -> Result<(), anyhow::Error> {
    let txn = db.begin().await?;
    for file in files {
        let file_name = file
            .path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| anyhow::anyhow!("invalid account file path {}", file.path.display()))?;
        let disk_size = std::fs::metadata(&file.path)?.len();
        txn.execute(statement(
            "INSERT INTO snapshot_bootstrap_account_files \
             (run_id, archive_type, file_name, account_slot, write_version, current_len, disk_size) \
             VALUES ($1, $2, $3, $4, $5, $6, $7) ON CONFLICT DO NOTHING",
            vec![
                Value::BigInt(Some(run_id)),
                Value::String(Some(Box::new(archive_type(snapshot_type).to_string()))),
                Value::String(Some(Box::new(file_name.to_string()))),
                Value::BigInt(Some(file.slot as i64)),
                Value::BigInt(Some(file.write_version as i64)),
                Value::BigInt(Some(file.size as i64)),
                Value::BigInt(Some(disk_size as i64)),
            ],
        ))
        .await?;
    }
    txn.execute(statement(
        "UPDATE snapshot_bootstrap_archives SET extracted_at = CURRENT_TIMESTAMP \
         WHERE run_id = $1 AND archive_type = $2",
        vec![
            Value::BigInt(Some(run_id)),
            Value::String(Some(Box::new(archive_type(snapshot_type).to_string()))),
        ],
    ))
    .await?;
    txn.commit().await?;
    Ok(())
}

pub async fn account_file_checkpoints(
    db: &DatabaseConnection,
    run_id: i64,
    snapshot_type: SnapshotType,
) -> Result<Vec<AccountFileCheckpoint>, anyhow::Error> {
    Ok(AccountFileCheckpoint::find_by_statement(statement(
        "SELECT archive_type, file_name, account_slot, write_version, current_len, disk_size, \
         account_count, completed_at IS NOT NULL AS completed \
         FROM snapshot_bootstrap_account_files WHERE run_id = $1 AND archive_type = $2 \
         ORDER BY account_slot, write_version",
        vec![
            Value::BigInt(Some(run_id)),
            Value::String(Some(Box::new(archive_type(snapshot_type).to_string()))),
        ],
    ))
    .all(db)
    .await?)
}

pub async fn archive_ingestion_complete(
    db: &DatabaseConnection,
    run_id: i64,
    snapshot_type: SnapshotType,
) -> Result<bool, anyhow::Error> {
    let row = db
        .query_one(statement(
            "SELECT COUNT(*) AS total, COUNT(*) FILTER (WHERE completed_at IS NULL) AS pending \
             FROM snapshot_bootstrap_account_files WHERE run_id = $1 AND archive_type = $2",
            vec![
                Value::BigInt(Some(run_id)),
                Value::String(Some(Box::new(archive_type(snapshot_type).to_string()))),
            ],
        ))
        .await?
        .ok_or_else(|| anyhow::anyhow!("archive completion query returned no row"))?;
    let total: i64 = row.try_get("", "total")?;
    let pending: i64 = row.try_get("", "pending")?;
    Ok(total > 0 && pending == 0)
}

pub async fn mark_archive_files_skipped(
    db: &DatabaseConnection,
    run_id: i64,
    snapshot_type: SnapshotType,
) -> Result<(), anyhow::Error> {
    db.execute(statement(
        "UPDATE snapshot_bootstrap_account_files SET skipped_on_resume = true \
         WHERE run_id = $1 AND archive_type = $2 AND completed_at IS NOT NULL",
        vec![
            Value::BigInt(Some(run_id)),
            Value::String(Some(Box::new(archive_type(snapshot_type).to_string()))),
        ],
    ))
    .await?;
    Ok(())
}

pub async fn mark_account_file_skipped(
    db: &DatabaseConnection,
    run_id: i64,
    snapshot_type: SnapshotType,
    account_slot: i64,
    write_version: i64,
) -> Result<(), anyhow::Error> {
    db.execute(statement(
        "UPDATE snapshot_bootstrap_account_files SET skipped_on_resume = true \
         WHERE run_id = $1 AND archive_type = $2 AND account_slot = $3 AND write_version = $4",
        vec![
            Value::BigInt(Some(run_id)),
            Value::String(Some(Box::new(archive_type(snapshot_type).to_string()))),
            Value::BigInt(Some(account_slot)),
            Value::BigInt(Some(write_version)),
        ],
    ))
    .await?;
    Ok(())
}

pub fn validate_extracted_files(
    accounts_dir: &Path,
    checkpoints: &[AccountFileCheckpoint],
) -> bool {
    checkpoints.iter().filter(|row| !row.completed).all(|row| {
        std::fs::metadata(accounts_dir.join(&row.file_name))
            .map(|metadata| metadata.len() == row.disk_size as u64)
            .unwrap_or(false)
    })
}

pub async fn commit_account_file(
    db: &DatabaseConnection,
    run_id: i64,
    snapshot_type: SnapshotType,
    account_slot: u64,
    write_version: u64,
    versions: Vec<SnapshotAccountVersion>,
) -> Result<(), anyhow::Error> {
    let txn = db.begin().await?;
    for chunk in
        versions.chunks(crate::db_queries::INSERT_SNAPSHOT_ACCOUNT_VERSIONS_TEMP_TABLE_BATCH_SIZE)
    {
        crate::db_queries::insert_into_temp_snapshot_account_versions_connection(&txn, chunk)
            .await?;
    }
    let checkpoint = txn
        .execute(statement(
            "UPDATE snapshot_bootstrap_account_files SET account_count = $5, \
         completed_at = CURRENT_TIMESTAMP WHERE run_id = $1 AND archive_type = $2 \
         AND account_slot = $3 AND write_version = $4",
            vec![
                Value::BigInt(Some(run_id)),
                Value::String(Some(Box::new(archive_type(snapshot_type).to_string()))),
                Value::BigInt(Some(account_slot as i64)),
                Value::BigInt(Some(write_version as i64)),
                Value::BigInt(Some(versions.len() as i64)),
            ],
        ))
        .await?;
    if checkpoint.rows_affected() != 1 {
        return Err(anyhow::anyhow!(
            "account file checkpoint was missing for {}/{}/{account_slot}/{write_version}",
            run_id,
            archive_type(snapshot_type)
        ));
    }
    txn.commit().await?;
    Ok(())
}

pub async fn step_complete(
    db: &DatabaseConnection,
    run_id: i64,
    phase: &str,
    item: &str,
) -> Result<bool, anyhow::Error> {
    let exists: bool = db
        .query_one(statement(
            "SELECT EXISTS (SELECT 1 FROM snapshot_bootstrap_postprocessing \
             WHERE run_id = $1 AND phase = $2 AND item = $3) AS present",
            vec![
                Value::BigInt(Some(run_id)),
                Value::String(Some(Box::new(phase.to_string()))),
                Value::String(Some(Box::new(item.to_string()))),
            ],
        ))
        .await?
        .ok_or_else(|| anyhow::anyhow!("postprocessing checkpoint query returned no row"))?
        .try_get("", "present")?;
    Ok(exists)
}

pub async fn mark_step_complete(
    db: &DatabaseConnection,
    run_id: i64,
    phase: &str,
    item: &str,
) -> Result<(), anyhow::Error> {
    db.execute(statement(
        "INSERT INTO snapshot_bootstrap_postprocessing (run_id, phase, item) \
         VALUES ($1, $2, $3) ON CONFLICT DO NOTHING",
        vec![
            Value::BigInt(Some(run_id)),
            Value::String(Some(Box::new(phase.to_string()))),
            Value::String(Some(Box::new(item.to_string()))),
        ],
    ))
    .await?;
    Ok(())
}

pub async fn persist_startup_updated_accounts(
    db: &DatabaseConnection,
    slot: u64,
    pubkeys: &[Vec<u8>],
) -> Result<bool, anyhow::Error> {
    let txn = db.begin().await?;
    let run_id = txn
        .query_one(Statement::from_string(
            DatabaseBackend::Postgres,
            "SELECT id FROM snapshot_bootstrap_runs WHERE phase NOT IN ('ready', 'abandoned') \
             ORDER BY id DESC LIMIT 1 FOR UPDATE",
        ))
        .await?
        .map(|row| row.try_get::<i64>("", "id"))
        .transpose()?;
    let Some(run_id) = run_id else {
        txn.commit().await?;
        return Ok(false);
    };
    for chunk in pubkeys.chunks(1_000) {
        let values = chunk
            .iter()
            .cloned()
            .map(|pubkey| Value::Bytes(Some(Box::new(pubkey))))
            .collect();
        txn.execute(statement(
            "INSERT INTO snapshot_bootstrap_updated_accounts (run_id, pubkey, latest_slot) \
             SELECT $1, pubkey, $2 FROM UNNEST($3::bytea[]) AS pubkey \
             ON CONFLICT (run_id, pubkey) DO UPDATE SET latest_slot = \
             GREATEST(snapshot_bootstrap_updated_accounts.latest_slot, EXCLUDED.latest_slot)",
            vec![
                Value::BigInt(Some(run_id)),
                Value::BigInt(Some(slot as i64)),
                Value::Array(sea_orm::sea_query::ArrayType::Bytes, Some(Box::new(values))),
            ],
        ))
        .await?;
    }
    txn.commit().await?;
    Ok(true)
}

pub async fn reconcile_updated_accounts_and_mark_ready(
    db: &DatabaseConnection,
    run_id: i64,
) -> Result<(), anyhow::Error> {
    set_phase(db, run_id, PHASE_LIVE_RECONCILIATION).await?;
    loop {
        let txn = db.begin().await?;
        txn.execute(statement(
            "SELECT pg_advisory_xact_lock($1)",
            vec![Value::BigInt(Some(run_id))],
        ))
        .await?;
        txn.query_one(statement(
            "SELECT id FROM snapshot_bootstrap_runs WHERE id = $1 FOR UPDATE",
            vec![Value::BigInt(Some(run_id))],
        ))
        .await?
        .ok_or_else(|| {
            anyhow::anyhow!("bootstrap run {run_id} disappeared during reconciliation")
        })?;
        let rows = txn
            .query_all(statement(
                "SELECT pubkey, latest_slot FROM snapshot_bootstrap_updated_accounts \
                 WHERE run_id = $1 ORDER BY pubkey LIMIT 1000 FOR UPDATE SKIP LOCKED",
                vec![Value::BigInt(Some(run_id))],
            ))
            .await?;
        if rows.is_empty() {
            txn.execute(statement(
                "UPDATE snapshot_bootstrap_runs SET phase = 'ready', updated_at = CURRENT_TIMESTAMP, \
                 completed_at = CURRENT_TIMESTAMP WHERE id = $1",
                vec![Value::BigInt(Some(run_id))],
            ))
            .await?;
            txn.commit().await?;
            metrics::set_bootstrap_phase(run_id, PHASE_READY, "snapshot");
            metrics::BOOTSTRAP_PENDING_LIVE_UPDATES.set(0.0);
            cleanup_ready_artifacts(db, run_id).await;
            return Ok(());
        }
        let pubkeys: Vec<Value> = rows
            .iter()
            .map(|row| row.try_get::<Vec<u8>>("", "pubkey"))
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .map(|pubkey| Value::Bytes(Some(Box::new(pubkey))))
            .collect();
        txn.execute(statement(
            "DELETE FROM snapshot_accounts WHERE pubkey = ANY($1::bytea[])",
            vec![Value::Array(
                sea_orm::sea_query::ArrayType::Bytes,
                Some(Box::new(pubkeys.clone())),
            )],
        ))
        .await?;
        txn.execute(statement(
            "DELETE FROM snapshot_bootstrap_updated_accounts WHERE run_id = $1 \
             AND pubkey = ANY($2::bytea[])",
            vec![
                Value::BigInt(Some(run_id)),
                Value::Array(
                    sea_orm::sea_query::ArrayType::Bytes,
                    Some(Box::new(pubkeys)),
                ),
            ],
        ))
        .await?;
        txn.commit().await?;
        update_pending_live_metric(db, run_id).await?;
    }
}

async fn cleanup_ready_artifacts(db: &DatabaseConnection, run_id: i64) {
    let slots = match db
        .query_all(statement(
            "SELECT DISTINCT slot FROM snapshot_bootstrap_archives WHERE run_id = $1",
            vec![Value::BigInt(Some(run_id))],
        ))
        .await
    {
        Ok(rows) => rows,
        Err(error) => {
            tracing::warn!(
                run_id,
                ?error,
                "Failed to list ready bootstrap artifacts for cleanup"
            );
            return;
        }
    };
    for row in slots {
        let Ok(slot) = row.try_get::<i64>("", "slot") else {
            continue;
        };
        let path = crate::sidecar::snapshot_base_dir(slot as u64);
        if let Err(error) = std::fs::remove_dir_all(&path)
            && error.kind() != std::io::ErrorKind::NotFound
        {
            tracing::warn!(run_id, path = %path.display(), ?error, "Failed to remove ready bootstrap artifact directory");
        }
    }
}

pub async fn update_metrics(
    db: &DatabaseConnection,
    run: &BootstrapRun,
) -> Result<(), anyhow::Error> {
    let current = BootstrapRun::find_by_statement(statement(
        "SELECT id, target_slot, covered_slot, phase, source, resumed_count \
         FROM snapshot_bootstrap_runs WHERE id = $1",
        vec![Value::BigInt(Some(run.id))],
    ))
    .one(db)
    .await?
    .ok_or_else(|| anyhow::anyhow!("bootstrap run {} disappeared", run.id))?;
    metrics::set_bootstrap_phase(current.id, &current.phase, &current.source);
    for kind in ["full", "incremental"] {
        let row = db
            .query_one(statement(
            "SELECT COUNT(*) AS total, COUNT(*) FILTER (WHERE completed_at IS NOT NULL) AS completed, \
                 COUNT(*) FILTER (WHERE skipped_on_resume) AS skipped \
                 FROM snapshot_bootstrap_account_files WHERE run_id = $1 AND archive_type = $2",
                vec![
                    Value::BigInt(Some(run.id)),
                    Value::String(Some(Box::new(kind.to_string()))),
                ],
            ))
            .await?
            .ok_or_else(|| anyhow::anyhow!("bootstrap file metric query returned no row"))?;
        let total: i64 = row.try_get("", "total")?;
        let completed: i64 = row.try_get("", "completed")?;
        let skipped: i64 = row.try_get("", "skipped")?;
        metrics::BOOTSTRAP_FILES_TOTAL
            .with_label_values(&[kind])
            .set(total as f64);
        metrics::BOOTSTRAP_FILES_COMPLETED
            .with_label_values(&[kind])
            .set(completed as f64);
        metrics::BOOTSTRAP_FILES_SKIPPED
            .with_label_values(&[kind])
            .set(skipped as f64);
    }
    let resumed: i64 = db
        .query_one(Statement::from_string(
            DatabaseBackend::Postgres,
            "SELECT COALESCE(SUM(resumed_count), 0)::BIGINT AS count FROM snapshot_bootstrap_runs",
        ))
        .await?
        .ok_or_else(|| anyhow::anyhow!("bootstrap resume metric query returned no row"))?
        .try_get("", "count")?;
    let observed_resumes = metrics::BOOTSTRAP_RESUMED_RUNS_TOTAL.get();
    if resumed as f64 > observed_resumes {
        metrics::BOOTSTRAP_RESUMED_RUNS_TOTAL.inc_by(resumed as f64 - observed_resumes);
    }
    let discarded = db
        .query_all(Statement::from_string(
            DatabaseBackend::Postgres,
            "SELECT abandon_reason, COUNT(*) AS count FROM snapshot_bootstrap_runs \
             WHERE phase = 'abandoned' GROUP BY abandon_reason",
        ))
        .await?;
    for row in discarded {
        let reason: Option<String> = row.try_get("", "abandon_reason")?;
        let reason = reason.unwrap_or_else(|| "unknown".to_string());
        let count: i64 = row.try_get("", "count")?;
        let counter = metrics::BOOTSTRAP_DISCARDED_RUNS_TOTAL.with_label_values(&[&reason]);
        let observed = counter.get();
        if count as f64 > observed {
            counter.inc_by(count as f64 - observed);
        }
    }
    update_pending_live_metric(db, run.id).await?;
    Ok(())
}

async fn update_pending_live_metric(
    db: &DatabaseConnection,
    run_id: i64,
) -> Result<(), anyhow::Error> {
    let pending: i64 = db
        .query_one(statement(
            "SELECT COUNT(*) AS count FROM snapshot_bootstrap_updated_accounts WHERE run_id = $1",
            vec![Value::BigInt(Some(run_id))],
        ))
        .await?
        .ok_or_else(|| anyhow::anyhow!("pending live update metric query returned no row"))?
        .try_get("", "count")?;
    metrics::BOOTSTRAP_PENDING_LIVE_UPDATES.set(pending as f64);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use sea_orm::{ConnectOptions, Database};
    use tempfile::tempdir;

    #[test]
    fn validates_only_pending_extracted_files() {
        let dir = tempdir().unwrap();
        std::fs::write(dir.path().join("1.1"), [0_u8; 8]).unwrap();
        let checkpoints = vec![
            AccountFileCheckpoint {
                archive_type: "full".into(),
                file_name: "1.1".into(),
                account_slot: 1,
                write_version: 1,
                current_len: 8,
                disk_size: 8,
                account_count: None,
                completed: false,
            },
            AccountFileCheckpoint {
                archive_type: "full".into(),
                file_name: "2.2".into(),
                account_slot: 2,
                write_version: 2,
                current_len: 8,
                disk_size: 8,
                account_count: Some(1),
                completed: true,
            },
        ];
        assert!(validate_extracted_files(dir.path(), &checkpoints));
        std::fs::write(dir.path().join("1.1"), [0_u8; 7]).unwrap();
        assert!(!validate_extracted_files(dir.path(), &checkpoints));
    }

    #[tokio::test]
    async fn durable_checkpoint_reconciliation_truncation_and_adoption() {
        let Ok(database_url) = std::env::var("CLOUDBREAK_TEST_DATABASE_URL") else {
            return;
        };
        let db = Database::connect(ConnectOptions::new(database_url))
            .await
            .unwrap();
        db.execute_unprepared(
            r#"
            DELETE FROM snapshot_bootstrap_runs;
            TRUNCATE snapshot_accounts;
            DELETE FROM slots;
            "#,
        )
        .await
        .unwrap();

        let run = prepare_run(&db, 100).await.unwrap();
        let pair = SnapshotPair {
            full_snapshot: SnapshotData {
                file_name: "snapshot-90-hash.tar.zst".into(),
                base_slot: None,
                slot: 90,
                snapshot_type: SnapshotType::Full,
                download_url: Some("https://snapshot.invalid/full".into()),
            },
            incremental_snapshot: Some(SnapshotData {
                file_name: "incremental-snapshot-90-100-hash.tar.zst".into(),
                base_slot: Some(90),
                slot: 100,
                snapshot_type: SnapshotType::Incremental,
                download_url: Some("https://snapshot.invalid/incremental".into()),
            }),
            downloading_endpoint: "https://snapshot.invalid".into(),
        };
        persist_pair(&db, run.id, &pair).await.unwrap();
        let (persisted_pair, _, _) = load_pair(&db, run.id).await.unwrap().unwrap();
        assert_eq!(persisted_pair.full_snapshot.slot, 90);
        assert_eq!(persisted_pair.incremental_snapshot.unwrap().slot, 100);
        assert_eq!(
            record_archive_validation_failure(&db, run.id, SnapshotType::Full)
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            record_archive_validation_failure(&db, run.id, SnapshotType::Full)
                .await
                .unwrap(),
            2
        );
        let dir = tempdir().unwrap();
        let account_path = dir.path().join("10.1");
        std::fs::write(&account_path, [0_u8; 8]).unwrap();
        persist_manifest(
            &db,
            run.id,
            SnapshotType::Full,
            &[AccountFileData {
                path: account_path,
                size: 8,
                slot: 10,
                write_version: 1,
            }],
        )
        .await
        .unwrap();
        let pubkey = vec![1_u8; 32];
        let owner = vec![2_u8; 32];
        insert_snapshot_fixture(&db, &pubkey, &owner, 10).await;
        commit_account_file(
            &db,
            run.id,
            SnapshotType::Full,
            10,
            1,
            vec![SnapshotAccountVersion {
                pubkey: pubkey.clone(),
                slot: 10,
                owner,
            }],
        )
        .await
        .unwrap();

        let resumed = prepare_run(&db, 200).await.unwrap();
        assert_eq!(resumed.id, run.id);
        assert_eq!(resumed.target_slot, 100);
        assert_eq!(resumed.resumed_count, 1);

        let buffer_size = std::sync::Arc::new(std::sync::Mutex::new(0));
        crate::db_queries::cluster_snapshot_accounts_table_resumable(
            &db,
            buffer_size.clone(),
            None,
            run.id,
        )
        .await
        .unwrap();
        crate::db_queries::cluster_snapshot_accounts_table_resumable(
            &db,
            buffer_size,
            None,
            run.id,
        )
        .await
        .unwrap();
        crate::db_queries::prepare_deduplication_resumable(&db, run.id)
            .await
            .unwrap();
        crate::db_queries::prepare_deduplication_resumable(&db, run.id)
            .await
            .unwrap();
        crate::db_queries::cleanup_duplicates_resumable(&db, run.id)
            .await
            .unwrap();
        crate::db_queries::cleanup_duplicates_resumable(&db, run.id)
            .await
            .unwrap();
        crate::db_queries::cleanup_closed_resumable(&db, run.id)
            .await
            .unwrap();
        crate::db_queries::cleanup_closed_resumable(&db, run.id)
            .await
            .unwrap();
        crate::db_queries::create_database_indexes_resumable(
            &db,
            &SnapshotPgIndexesConfig::default(),
            run.id,
        )
        .await
        .unwrap();
        db.execute_unprepared("DROP INDEX idx_snapshot_accounts_pubkey_slot")
            .await
            .unwrap();
        crate::db_queries::create_database_indexes_resumable(
            &db,
            &SnapshotPgIndexesConfig::default(),
            run.id,
        )
        .await
        .unwrap();
        assert_eq!(
            scalar_i64(
                &db,
                "SELECT COUNT(*) FROM pg_indexes WHERE indexname = 'idx_snapshot_accounts_pubkey_slot'"
            )
            .await,
            1
        );

        set_phase(&db, run.id, PHASE_LIVE_RECONCILIATION)
            .await
            .unwrap();
        assert!(
            persist_startup_updated_accounts(&db, 101, std::slice::from_ref(&pubkey))
                .await
                .unwrap()
        );
        reconcile_updated_accounts_and_mark_ready(&db, run.id)
            .await
            .unwrap();
        assert!(
            !persist_startup_updated_accounts(&db, 102, std::slice::from_ref(&pubkey))
                .await
                .unwrap()
        );
        assert_eq!(
            scalar_i64(&db, "SELECT COUNT(*) FROM snapshot_accounts").await,
            0
        );
        assert_eq!(
            scalar_string(
                &db,
                "SELECT phase FROM snapshot_bootstrap_runs ORDER BY id DESC LIMIT 1"
            )
            .await,
            "ready"
        );

        let crash_run = prepare_run(&db, 300).await.unwrap();
        let crash_path = dir.path().join("20.2");
        std::fs::write(&crash_path, [0_u8; 8]).unwrap();
        persist_manifest(
            &db,
            crash_run.id,
            SnapshotType::Full,
            &[AccountFileData {
                path: crash_path,
                size: 8,
                slot: 20,
                write_version: 2,
            }],
        )
        .await
        .unwrap();
        insert_snapshot_fixture(&db, &pubkey, &[3_u8; 32], 20).await;
        commit_account_file(
            &db,
            crash_run.id,
            SnapshotType::Full,
            20,
            2,
            vec![SnapshotAccountVersion {
                pubkey: pubkey.clone(),
                slot: 20,
                owner: vec![3_u8; 32],
            }],
        )
        .await
        .unwrap();
        db.execute_unprepared(
            "TRUNCATE temp_snapshot_account_versions; TRUNCATE snapshot_accounts",
        )
        .await
        .unwrap();
        let replacement = prepare_run(&db, 301).await.unwrap();
        assert_ne!(replacement.id, crash_run.id);
        assert_eq!(
            scalar_string(
                &db,
                &format!(
                    "SELECT phase FROM snapshot_bootstrap_runs WHERE id = {}",
                    crash_run.id
                )
            )
            .await,
            "abandoned"
        );

        db.execute_unprepared(
            r#"
            UPDATE snapshot_bootstrap_runs SET phase = 'abandoned', completed_at = CURRENT_TIMESTAMP
              WHERE phase NOT IN ('ready', 'abandoned');
            DELETE FROM snapshot_bootstrap_runs;
            TRUNCATE snapshot_accounts;
            DELETE FROM slots;
            DROP INDEX IF EXISTS idx_snapshot_accounts_pubkey_slot;
            DROP INDEX IF EXISTS idx_snapshot_accounts_token_mint;
            DROP INDEX IF EXISTS idx_snapshot_accounts_token_owner;
            DROP INDEX IF EXISTS idx_snapshot_accounts_token_delegate;
            "#,
        )
        .await
        .unwrap();
        insert_snapshot_fixture(&db, &pubkey, &[4_u8; 32], 30).await;
        db.execute_unprepared(
            r#"
            INSERT INTO slots (slot, commitment, block_time, health)
            VALUES (400, 1, 0, true), (399, 2, 0, true);
            CREATE INDEX idx_snapshot_accounts_pubkey_slot ON snapshot_accounts (pubkey, slot DESC);
            CREATE INDEX idx_snapshot_accounts_token_mint ON snapshot_accounts (token_mint);
            CREATE INDEX idx_snapshot_accounts_token_owner ON snapshot_accounts (token_owner);
            CREATE INDEX idx_snapshot_accounts_token_delegate ON snapshot_accounts (slot);
            "#,
        )
        .await
        .unwrap();
        assert_eq!(
            inspect_or_adopt_ready_database(&db, &SnapshotPgIndexesConfig::default())
                .await
                .unwrap(),
            StartupBootstrapState::Ready { replay_slot: 400 }
        );
        assert_eq!(
            scalar_string(
                &db,
                "SELECT source FROM snapshot_bootstrap_runs ORDER BY id DESC LIMIT 1"
            )
            .await,
            "adopted_existing"
        );
    }

    async fn insert_snapshot_fixture(
        db: &DatabaseConnection,
        pubkey: &[u8],
        owner: &[u8],
        slot: i64,
    ) {
        db.execute(statement(
            "INSERT INTO snapshot_accounts \
             (pubkey, owner, lamports, slot, executable, rent_epoch, data, write_version) \
             VALUES ($1, $2, 1, $3, false, 0, '\\x'::bytea, 1) ON CONFLICT DO NOTHING",
            vec![
                Value::Bytes(Some(Box::new(pubkey.to_vec()))),
                Value::Bytes(Some(Box::new(owner.to_vec()))),
                Value::BigInt(Some(slot)),
            ],
        ))
        .await
        .unwrap();
    }

    async fn scalar_i64(db: &DatabaseConnection, sql: &str) -> i64 {
        db.query_one(Statement::from_string(DatabaseBackend::Postgres, sql))
            .await
            .unwrap()
            .unwrap()
            .try_get_by_index(0)
            .unwrap()
    }

    async fn scalar_string(db: &DatabaseConnection, sql: &str) -> String {
        db.query_one(Statement::from_string(DatabaseBackend::Postgres, sql))
            .await
            .unwrap()
            .unwrap()
            .try_get_by_index(0)
            .unwrap()
    }
}
