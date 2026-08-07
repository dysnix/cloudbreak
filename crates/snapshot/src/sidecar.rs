// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use bincode::Options;
use cloudbreak_core::Result;
use futures::StreamExt;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tar::Archive;
use tokio::time::{Instant, sleep};
use tokio::{fs::File, io::AsyncWriteExt};
use zstd::Decoder;

use crate::accountsdb_helpers::{
    AccountsDbFields, DeserializableVersionedBank, ExtraFields, MAX_STREAM_SIZE,
    SerializableAccountStorageEntry,
};

#[derive(Debug, Clone)]
pub struct SnapshotData {
    pub file_name: String,
    pub base_slot: Option<u64>,
    pub slot: u64,
    pub snapshot_type: SnapshotType,
    /// If there is a download url for the file, it would be preferred over the sidecar pair endpoint.
    pub download_url: Option<String>,
}

#[derive(Debug, Clone)]
pub struct SnapshotPair {
    pub full_snapshot: SnapshotData,
    /// It will be None if the snapshot pair doesn't contain an incremental snapshot
    pub incremental_snapshot: Option<SnapshotData>,
    /// The Sidecar endpoint from which to download the snapshot
    pub downloading_endpoint: String,
}

impl SnapshotPair {
    pub fn parse(json_value: &serde_json::Value) -> Result<Self> {
        // Cloudbreak has historically consumed a flat response. solana-cluster-manager's
        // `/v1/snapshots` endpoint wraps metadata in `info` while keeping `target` on the outer
        // item, so accept both shapes.
        let snapshot_info = match json_value.get("info") {
            Some(info) if info.is_object() => info,
            Some(_) => return Err(anyhow::anyhow!("snapshot info is not an object")),
            None => json_value,
        };

        let sidecar_endpoint = json_value
            .get("target")
            .or_else(|| snapshot_info.get("target"))
            .and_then(|value| value.as_str())
            .ok_or(anyhow::anyhow!("sidecar endpoint not found"))?;

        let files = snapshot_info
            .get("files")
            .and_then(|files| files.as_array())
            .ok_or(anyhow::anyhow!("snapshot files not found"))?;

        let mut full_snapshot_file = None;
        let mut incremental_snapshot_file = None;

        for file in files {
            let snapshot_file = Self::parse_file(file)?;
            match snapshot_file.snapshot_type {
                SnapshotType::Full => {
                    if full_snapshot_file.replace(snapshot_file).is_some() {
                        return Err(anyhow::anyhow!(
                            "more than one full snapshot file found in pair"
                        ));
                    }
                }
                SnapshotType::Incremental => {
                    if incremental_snapshot_file.replace(snapshot_file).is_some() {
                        return Err(anyhow::anyhow!(
                            "more than one incremental snapshot file found in pair"
                        ));
                    }
                }
            }
        }

        let full_snapshot_file =
            full_snapshot_file.ok_or(anyhow::anyhow!("full snapshot file not found in pair"))?;

        // The current tracker omits pair-level `base_slot`. Derive it from the incremental file,
        // or from the full snapshot itself when the pair contains only a full snapshot.
        let slot = optional_u64(snapshot_info, "slot")?.unwrap_or_else(|| {
            incremental_snapshot_file
                .as_ref()
                .map(|snapshot| snapshot.slot)
                .unwrap_or(full_snapshot_file.slot)
        });
        let base_slot = optional_u64(snapshot_info, "base_slot")?
            .filter(|base_slot| *base_slot != 0)
            .or_else(|| {
                incremental_snapshot_file
                    .as_ref()
                    .and_then(|snapshot| snapshot.base_slot)
            })
            .unwrap_or(full_snapshot_file.slot);

        // Check that files slots are correct
        let snapshot_pair = SnapshotPair {
            full_snapshot: full_snapshot_file,
            incremental_snapshot: incremental_snapshot_file,
            downloading_endpoint: sidecar_endpoint.to_string(),
        };
        snapshot_pair.check_files_slots(slot, base_slot)?;

        Ok(snapshot_pair)
    }

    fn parse_file(file: &serde_json::Value) -> Result<SnapshotData> {
        let file_name = file
            .get("file_name")
            .ok_or(anyhow::anyhow!("file_name not found"))?
            .as_str()
            .ok_or(anyhow::anyhow!("file_name not found"))?
            .to_string();
        let slot = file
            .get("slot")
            .ok_or(anyhow::anyhow!("slot not found"))?
            .as_u64()
            .ok_or(anyhow::anyhow!("slot not found"))?;
        let base_slot = file.get("base_slot").and_then(|v| v.as_u64());
        let download_url = file
            .get("download_url")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        let snapshot_type = if file_name.contains("incremental") {
            if base_slot.is_none() {
                return Err(anyhow::anyhow!(
                    "base slot not found for incremental snapshot"
                ));
            }

            SnapshotType::Incremental
        } else {
            SnapshotType::Full
        };

        Ok(SnapshotData {
            file_name,
            base_slot,
            slot,
            snapshot_type,
            download_url,
        })
    }

    /// It will check that each file slot data matches the root json item slot data
    fn check_files_slots(&self, slot: u64, base_slot: u64) -> Result<()> {
        let is_full_correct = self.full_snapshot.slot == base_slot;

        let is_incremental_correct = if let Some(incremental_snapshot) = &self.incremental_snapshot
        {
            incremental_snapshot.slot == slot
                && incremental_snapshot.base_slot.ok_or(anyhow::anyhow!(
                    "base slot not found for incremental snapshot"
                ))? == base_slot
        } else {
            true
        };

        if !is_full_correct || !is_incremental_correct {
            return Err(anyhow::anyhow!("files slots do not match"));
        }

        Ok(())
    }

    /// Checks if the target slot is covered by the snapshot pair.
    /// If there is an incremental snapshot, it will also check that full and incremental base slots match.
    pub fn check_target_slot(&self, target_slot: u64) -> Result<bool> {
        let mut snapshot_covered_slot = self.full_snapshot.slot;

        if let Some(incremental_snapshot) = &self.incremental_snapshot {
            let incremental_base_slot = incremental_snapshot.base_slot.ok_or(anyhow::anyhow!(
                "base slot not found for incremental snapshot"
            ))?;

            if incremental_base_slot != self.full_snapshot.slot {
                return Err(anyhow::anyhow!(
                    "incremental snapshot base slot does not match full snapshot slot"
                ));
            }
            snapshot_covered_slot = incremental_snapshot.slot;
        }

        let is_covered = snapshot_covered_slot >= target_slot;

        Ok(is_covered)
    }
}

fn optional_u64(json_value: &serde_json::Value, key: &str) -> Result<Option<u64>> {
    match json_value.get(key) {
        Some(value) => value
            .as_u64()
            .map(Some)
            .ok_or_else(|| anyhow::anyhow!("{key} is not an unsigned integer")),
        None => Ok(None),
    }
}

fn resolve_sidecar_endpoint(sidecar_endpoint: &str, tracker_endpoint: &str) -> Result<String> {
    if sidecar_endpoint.contains("://") {
        reqwest::Url::parse(sidecar_endpoint)?;
        return Ok(sidecar_endpoint.to_string());
    }

    let tracker_url = reqwest::Url::parse(tracker_endpoint)?;
    let sidecar_endpoint = sidecar_endpoint.trim_start_matches("//");
    let resolved_endpoint = format!("{}://{}", tracker_url.scheme(), sidecar_endpoint);

    // Validate the result here so a bad tracker target is reported before snapshot downloading.
    reqwest::Url::parse(&resolved_endpoint)?;

    Ok(resolved_endpoint)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parses_current_nested_tracker_response() {
        let value = json!({
            "target": "mainnet-snapshot-cluster-sidecar-0:13080",
            "inverse_slot": 18446744073271936604u64,
            "info": {
                "slot": 437615011,
                "hash": "incremental-hash",
                "files": [
                    {
                        "file_name": "incremental-snapshot-437595677-437615011-incremental-hash.tar.zst",
                        "slot": 437615011,
                        "base_slot": 437595677
                    },
                    {
                        "file_name": "snapshot-437595677-full-hash.tar.zst",
                        "slot": 437595677
                    }
                ]
            }
        });

        let pair = SnapshotPair::parse(&value).expect("current tracker response should parse");

        assert_eq!(
            pair.downloading_endpoint,
            "mainnet-snapshot-cluster-sidecar-0:13080"
        );
        assert_eq!(pair.full_snapshot.slot, 437595677);
        let incremental = pair
            .incremental_snapshot
            .expect("incremental snapshot should be present");
        assert_eq!(incremental.slot, 437615011);
        assert_eq!(incremental.base_slot, Some(437595677));
    }

    #[test]
    fn parses_legacy_flat_tracker_response() {
        let value = json!({
            "slot": 200,
            "base_slot": 100,
            "target": "http://snapshot-sidecar:13080",
            "files": [
                {
                    "file_name": "incremental-snapshot-100-200-hash.tar.zst",
                    "slot": 200,
                    "base_slot": 100
                },
                {
                    "file_name": "snapshot-100-hash.tar.zst",
                    "slot": 100
                }
            ]
        });

        let pair = SnapshotPair::parse(&value).expect("legacy tracker response should parse");

        assert_eq!(pair.downloading_endpoint, "http://snapshot-sidecar:13080");
        assert_eq!(pair.full_snapshot.slot, 100);
        assert_eq!(pair.incremental_snapshot.unwrap().slot, 200);
    }

    #[test]
    fn derives_base_slot_when_flat_tracker_uses_zero_sentinel() {
        let value = json!({
            "slot": 437769435,
            "base_slot": 0,
            "target": "http://mainnet-snapshot-cluster-sidecar-0:13080",
            "files": [
                {
                    "file_name": "incremental-snapshot-437745774-437769435-incremental-hash.tar.zst",
                    "slot": 437769435,
                    "base_slot": 437745774
                },
                {
                    "file_name": "snapshot-437745774-full-hash.tar.zst",
                    "slot": 437745774
                }
            ]
        });

        let pair = SnapshotPair::parse(&value).expect("zero pair base slot should be derived");

        assert_eq!(pair.full_snapshot.slot, 437745774);
        let incremental = pair.incremental_snapshot.unwrap();
        assert_eq!(incremental.slot, 437769435);
        assert_eq!(incremental.base_slot, Some(437745774));
    }

    #[test]
    fn derives_slot_and_base_slot_for_full_only_pair() {
        let value = json!({
            "target": "snapshot-sidecar:13080",
            "info": {
                "files": [{
                    "file_name": "snapshot-300-hash.tar.zst",
                    "slot": 300
                }]
            }
        });

        let pair = SnapshotPair::parse(&value).expect("full-only pair should parse");

        assert_eq!(pair.full_snapshot.slot, 300);
        assert!(pair.incremental_snapshot.is_none());
        assert!(pair.check_target_slot(300).unwrap());
        assert!(!pair.check_target_slot(301).unwrap());
    }

    #[test]
    fn rejects_missing_files_without_panicking() {
        let error = SnapshotPair::parse(&json!({
            "target": "snapshot-sidecar:13080",
            "info": { "slot": 300 }
        }))
        .expect_err("missing files should be rejected");

        assert!(error.to_string().contains("snapshot files not found"));
    }

    #[test]
    fn resolves_scheme_less_sidecar_endpoint_from_tracker_scheme() {
        assert_eq!(
            resolve_sidecar_endpoint(
                "mainnet-snapshot-cluster-sidecar-0:13080",
                "http://mainnet-cloudbreak-snapshot-tracker:8458"
            )
            .unwrap(),
            "http://mainnet-snapshot-cluster-sidecar-0:13080"
        );
        assert_eq!(
            resolve_sidecar_endpoint(
                "https://snapshot.example.com",
                "http://mainnet-cloudbreak-snapshot-tracker:8458"
            )
            .unwrap(),
            "https://snapshot.example.com"
        );
    }
}

const RETRY_WAIT_SECS: Duration = Duration::from_secs(10);

/// Minimum interval between "no covering snapshot" warnings while polling the tracker.
const NO_COVERAGE_LOG_INTERVAL: Duration = Duration::from_secs(30);

/// Base directory where a snapshot for `slot` is downloaded and unpacked (e.g. `./snapshot_123`).
pub fn snapshot_base_dir(slot: u64) -> PathBuf {
    PathBuf::from(format!("./snapshot_{}", slot))
}

/// Like [`snapshot_base_dir`] but suffixed with a millisecond timestamp (e.g.
/// `./snapshot_123_1700000000000`). Used by self-healing gap fills so concurrent/sequential
/// downloads for the same slot never collide on disk.
pub fn snapshot_base_dir_timestamped(slot: u64) -> PathBuf {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    PathBuf::from(format!("./snapshot_{}_{}", slot, timestamp))
}

/// Returns what are the correct snapshots to be downloaded based on the received slot and sidecar available snapshots
/// If target_slot is not provided, it will return the latest available full and incremental snapshot pair
///
/// It will block until the snapshots required are available
///
/// `force_returned_incremental` will only return a pair that contains also an incremental snapshot
pub async fn get_snapshot_data(
    tracker_endpoint: &str,
    target_slot: Option<u64>,
    save_to_file: bool,
    force_returned_incremental: bool,
) -> Result<SnapshotPair> {
    let client = reqwest::Client::new();
    let mut last_no_coverage_log: Option<Instant> = None;

    loop {
        let response = client
            .get(format!("{}/v1/snapshots", tracker_endpoint))
            .send()
            .await?
            .error_for_status()?;

        let json_value: serde_json::Value = response.json().await?;

        if save_to_file {
            let timestamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis();

            let file_path = format!("./tracker_responses/{}.json", timestamp);

            if let Some(parent) = Path::new(&file_path).parent() {
                tokio::fs::create_dir_all(parent).await?;
            }

            let mut file = File::create(&file_path).await?;
            let pretty_json = serde_json::to_string_pretty(&json_value)?;

            file.write_all(pretty_json.as_bytes()).await?;
        }

        // Highest slot covered by any pair offered by the tracker (incremental slot if present,
        // full slot otherwise), used to log how far behind the tracker is when nothing covers
        // the target slot.
        let mut highest_available_slot: Option<u64> = None;

        let snapshots = json_value
            .as_array()
            .ok_or(anyhow::anyhow!("snapshot tracker response is not an array"))?;

        for snapshot in snapshots {
            let mut snapshot_pair = match SnapshotPair::parse(snapshot) {
                Ok(snapshot_pair) => snapshot_pair,
                Err(error) => {
                    tracing::warn!(
                        target: "get_snapshot_data",
                        ?error,
                        "Skipping malformed snapshot tracker entry"
                    );
                    continue;
                }
            };

            snapshot_pair.downloading_endpoint = match resolve_sidecar_endpoint(
                &snapshot_pair.downloading_endpoint,
                tracker_endpoint,
            ) {
                Ok(endpoint) => endpoint,
                Err(error) => {
                    tracing::warn!(
                        target: "get_snapshot_data",
                        ?error,
                        endpoint = %snapshot_pair.downloading_endpoint,
                        "Skipping snapshot tracker entry with an invalid sidecar endpoint"
                    );
                    continue;
                }
            };

            let pair_covered_slot = snapshot_pair
                .incremental_snapshot
                .as_ref()
                .map(|incremental| incremental.slot)
                .unwrap_or(snapshot_pair.full_snapshot.slot);
            highest_available_slot = Some(
                highest_available_slot
                    .map_or(pair_covered_slot, |slot| slot.max(pair_covered_slot)),
            );

            let is_covered = if let Some(target_slot) = target_slot {
                snapshot_pair.check_target_slot(target_slot)?
            } else {
                true
            };

            // If incremental is required, we need to check that the snapshot pair contains an incremental snapshot
            let is_incremental_flag_satisfied = if force_returned_incremental {
                snapshot_pair.incremental_snapshot.is_some()
            } else {
                true
            };

            if is_covered && is_incremental_flag_satisfied {
                return Ok(snapshot_pair);
            }
        }

        let should_log =
            last_no_coverage_log.is_none_or(|last| last.elapsed() >= NO_COVERAGE_LOG_INTERVAL);
        if should_log {
            tracing::warn!(
                target: "get_snapshot_data",
                "No covering snapshot available from tracker yet - highest available slot: {:?} - target slot: {:?} - retrying every {}s",
                highest_available_slot,
                target_slot,
                RETRY_WAIT_SECS.as_secs()
            );
            last_no_coverage_log = Some(Instant::now());
        }

        sleep(RETRY_WAIT_SECS).await;
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotType {
    Full,
    Incremental,
}

/// Downloads the snapshot file from the sidecar
///
/// It will prefer the download url if it is available, otherwise it will use the sidecar endpoint.
pub async fn download_snapshot_file(
    sidecar_endpoint: &str,
    snapshot_data: SnapshotData,
    snapshot_type: SnapshotType,
    base_dir: &Path,
) -> Result<()> {
    download_snapshot_file_resumable(
        sidecar_endpoint,
        snapshot_data,
        snapshot_type,
        base_dir,
        None,
    )
    .await?;
    Ok(())
}

#[derive(Debug, thiserror::Error)]
pub enum SnapshotDownloadError {
    #[error("exact snapshot archive is unavailable: HTTP {0}")]
    Unavailable(reqwest::StatusCode),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

impl SnapshotDownloadError {
    pub fn is_unrecoverable(&self) -> bool {
        matches!(
            self,
            Self::Unavailable(status)
                if *status == reqwest::StatusCode::NOT_FOUND
                    || *status == reqwest::StatusCode::GONE
        )
    }
}

/// Downloads an immutable snapshot archive through a partial file and atomically renames it.
/// A complete local archive is reused without contacting the source when its recorded size is
/// available and matches.
pub async fn download_snapshot_file_resumable(
    sidecar_endpoint: &str,
    snapshot_data: SnapshotData,
    snapshot_type: SnapshotType,
    base_dir: &Path,
    recorded_size: Option<u64>,
) -> std::result::Result<u64, SnapshotDownloadError> {
    let url = if let Some(download_url) = snapshot_data.download_url {
        download_url
    } else {
        format!(
            "{}/v1/snapshot/{}",
            sidecar_endpoint, snapshot_data.file_name
        )
    };

    let file_path = base_dir.join(&snapshot_data.file_name);
    if let Some(size) = recorded_size
        && tokio::fs::metadata(&file_path)
            .await
            .map(|metadata| metadata.len() == size)
            .unwrap_or(false)
    {
        tracing::info!(
            target: "download_snapshot_file",
            file = %snapshot_data.file_name,
            size,
            "Reusing complete local snapshot archive"
        );
        return Ok(size);
    }

    let client = reqwest::Client::new();
    let start_time = tokio::time::Instant::now();
    let response = client
        .get(url)
        .send()
        .await
        .map_err(|error| SnapshotDownloadError::Other(error.into()))?;

    if !response.status().is_success() {
        let status = response.status();
        if status == reqwest::StatusCode::NOT_FOUND || status == reqwest::StatusCode::GONE {
            return Err(SnapshotDownloadError::Unavailable(status));
        }
        return Err(SnapshotDownloadError::Other(anyhow::anyhow!(
            "Failed to download file: HTTP {status}"
        )));
    }

    let total_size = response.content_length().unwrap_or(0);
    tracing::info!(
        target: "download_snapshot_file",
        "Starting to download file {} of size: {} MB ({:?}) from endpoint: {}",
        snapshot_data.file_name,
        total_size / 1024 / 1024,
        snapshot_type,
        sidecar_endpoint,
    );

    // Create the directory if it doesn't exist
    if let Some(parent) = file_path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(|error| SnapshotDownloadError::Other(error.into()))?;
    }

    if total_size > 0
        && tokio::fs::metadata(&file_path)
            .await
            .map(|metadata| metadata.len() == total_size)
            .unwrap_or(false)
    {
        tracing::info!(
            target: "download_snapshot_file",
            file = %snapshot_data.file_name,
            size = total_size,
            "Reusing complete local snapshot archive"
        );
        return Ok(total_size);
    }

    let partial_path = file_path.with_extension(format!(
        "{}.part",
        file_path
            .extension()
            .and_then(|extension| extension.to_str())
            .unwrap_or("archive")
    ));
    let mut file = File::create(&partial_path)
        .await
        .map_err(|error| SnapshotDownloadError::Other(error.into()))?;
    let mut stream = response.bytes_stream();
    let mut downloaded = 0u64;
    let mut last_log_time = tokio::time::Instant::now();

    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|error| SnapshotDownloadError::Other(error.into()))?;
        file.write_all(&chunk)
            .await
            .map_err(|error| SnapshotDownloadError::Other(error.into()))?;
        downloaded += chunk.len() as u64;

        if total_size > 0 && last_log_time.elapsed().as_secs() > 10 {
            let progress = (downloaded as f64 / total_size as f64) * 100.0;
            tracing::info!(
                target: "snapshot_download_progress",
                "Progress: {:.1}% ({}/{}) - {} seconds - {:.1} MB/s",
                progress,
                downloaded / 1024 / 1024,
                total_size / 1024 / 1024,
                start_time.elapsed().as_secs_f64(),
                downloaded as f64 / 1024.0 / 1024.0 / start_time.elapsed().as_secs_f64()
            );
            last_log_time = tokio::time::Instant::now();
        }
    }

    file.flush()
        .await
        .map_err(|error| SnapshotDownloadError::Other(error.into()))?;
    file.sync_all()
        .await
        .map_err(|error| SnapshotDownloadError::Other(error.into()))?;
    drop(file);
    if total_size > 0 && downloaded != total_size {
        return Err(SnapshotDownloadError::Other(anyhow::anyhow!(
            "snapshot archive size mismatch: downloaded {downloaded}, expected {total_size}"
        )));
    }
    tokio::fs::rename(&partial_path, &file_path)
        .await
        .map_err(|error| SnapshotDownloadError::Other(error.into()))?;
    tracing::info!(
        target: "download_snapshot_file",
        "File {} downloaded successfully in {} secs ({:?})",
        snapshot_data.file_name,
        start_time.elapsed().as_secs_f64(),
        snapshot_type
    );

    Ok(downloaded)
}

pub struct UnpackedSnapshot {
    pub account_files: Vec<AccountFileData>,
    pub stake_data: crate::stake_data::SnapshotStakeData,
}

pub fn unpack_compressed_snapshot<P: Into<PathBuf>>(
    path: P,
    base_dir: &Path,
    slot: u64,
) -> Result<UnpackedSnapshot> {
    let start_time = Instant::now();
    let path_buf: PathBuf = path.into();

    let temp_dir = base_dir.join("uncompressed_snapshot");

    let file = std::fs::File::open(path_buf)?;

    let decoder = Decoder::new(file)?;

    let mut archive = Archive::new(decoder);
    archive.unpack(temp_dir.clone())?;

    let elapsed = start_time.elapsed().as_secs_f64();
    tracing::info!(target: "unpack_compressed_snapshot", "Unpacked compressed snapshot in {} seconds", elapsed);

    let version_path = temp_dir.join("version");
    let _version = std::fs::read_to_string(version_path)?.trim().to_string();

    // Deserializing the snapshot metadata file
    let snapshots_dir = temp_dir.join("snapshots");
    let snapshot_file_name = format!("{}/{}", slot, slot);
    let snapshot_file = std::fs::File::open(snapshots_dir.join(snapshot_file_name))?;

    let mut snapshot_stream = std::io::BufReader::new(snapshot_file);

    let bank_fields: DeserializableVersionedBank = bincode::options()
        .with_limit(MAX_STREAM_SIZE)
        .with_fixint_encoding()
        .allow_trailing_bytes()
        .deserialize_from(&mut snapshot_stream)?;

    let elapsed = start_time.elapsed().as_secs_f64() - elapsed;
    tracing::info!(target: "unpack_compressed_snapshot", "Deserialized DeserializableVersionedBank in {} seconds", elapsed);

    let accounts_db_fields: AccountsDbFields<SerializableAccountStorageEntry> = bincode::options()
        .with_limit(MAX_STREAM_SIZE)
        .with_fixint_encoding()
        .allow_trailing_bytes()
        .deserialize_from(&mut snapshot_stream)
        .unwrap();

    let elapsed = start_time.elapsed().as_secs_f64() - elapsed;
    tracing::info!(target: "unpack_compressed_snapshot", "Deserialized AccountsDbFields Vec in {} seconds", elapsed);

    let extra_fields: ExtraFields = bincode::options()
        .with_limit(MAX_STREAM_SIZE)
        .with_fixint_encoding()
        .allow_trailing_bytes()
        .deserialize_from(&mut snapshot_stream)?;

    let stake_data =
        crate::stake_data::extract_stake_data(&bank_fields, &extra_fields.versioned_epoch_stakes);
    tracing::info!(
        target: "unpack_compressed_snapshot",
        "Extracted stake data: epoch={}, voters={}, in_epoch_set={}",
        stake_data.epoch,
        stake_data.voters.len(),
        stake_data.voters.iter().filter(|v| v.in_epoch_set).count(),
    );

    let AccountsDbFields(accounts_metadata, _, accountsdb_fields_slot, ..) = accounts_db_fields;

    assert_eq!(slot, accountsdb_fields_slot);
    assert_eq!(slot, bank_fields.slot);

    // Deserializing the accounts directory files
    let accounts_dir = temp_dir.join("accounts");

    let mut account_file_data = Vec::new();

    for entry in std::fs::read_dir(accounts_dir)?.filter_map(|entry| entry.ok()) {
        let path = entry.path();
        let file_size = std::fs::metadata(&path)?.len() as usize;
        let file_name = entry.file_name().to_string_lossy().to_string();

        let (slot_str, id_str) = file_name
            .split_once('.')
            .ok_or(anyhow::anyhow!("Invalid file name: {}", file_name))?;
        let slot = slot_str.parse::<u64>()?;
        let id = id_str.parse::<u64>()?;

        let accounts_metadata = match accounts_metadata.get(&slot) {
            Some(accounts_metadata) => accounts_metadata,
            None => {
                tracing::error!(
                    "accounts_metadata not found for slot: {} - file_size: {} - write_version: {}",
                    slot,
                    file_size,
                    id
                );
                account_file_data.push(return_default_account_file_data(path, slot, file_size, id));
                continue;
            }
        };

        let mut size = None;
        for account in accounts_metadata {
            if account.id as u64 == id {
                size = Some(account.accounts_current_len);
                break;
            }
        }
        let size = match size {
            Some(size) => size,
            None => {
                tracing::error!(
                    "size not found for write version: {} and slot: {} - file_size: {} - accounts_metadata: {:?}",
                    id,
                    slot,
                    file_size,
                    accounts_metadata
                );
                account_file_data.push(return_default_account_file_data(path, slot, file_size, id));
                continue;
            }
        };

        if size != file_size {
            tracing::warn!("size mismatch for id: {} and slot: {}", id, slot);
        }

        account_file_data.push(AccountFileData {
            path,
            size,
            slot,
            write_version: id,
        });
    }

    let elapsed = start_time.elapsed().as_secs_f64() - elapsed;
    tracing::info!(target: "unpack_compressed_snapshot", "Deserialized accounts directory metadata in {} seconds", elapsed);

    let elapsed = start_time.elapsed().as_secs_f64();
    tracing::info!(target: "unpack_compressed_snapshot", "Total unpacking time: {} seconds", elapsed);

    Ok(UnpackedSnapshot {
        account_files: account_file_data,
        stake_data,
    })
}

pub struct AccountFileData {
    pub path: PathBuf,
    pub size: usize,
    pub slot: u64,
    pub write_version: u64,
}

/// If for some reason we don't file the account file we are looking for (or the write version doesn't match)
///  we use the file name for getting the write version and the file size for the default account file data
fn return_default_account_file_data(
    path: PathBuf,
    slot: u64,
    file_size: usize,
    write_version: u64,
) -> AccountFileData {
    AccountFileData {
        path,
        size: file_size,
        slot,
        write_version,
    }
}
