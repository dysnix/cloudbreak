// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use prometheus::{
    Counter, CounterVec, Error, Gauge, GaugeVec, Histogram, HistogramOpts, Opts, Registry,
    core::Collector,
};

lazy_static::lazy_static! {
    pub static ref DB_SNAPSHOT_ERRORS: Counter = Counter::new(
        "cloudbreak_db_snapshot_errors",
        "Number of DB snapshot errors"
    )
    .expect("Failed to create Snapshot DB errors counter");

    pub static ref SNAPSHOT_BATCH_INSERT_TIME: Histogram = Histogram::with_opts(
        HistogramOpts::new(
            "cloudbreak_snapshot_batch_insert_time",
            "Time taken to insert a batch of snapshot accounts"
        )
    )
    .expect("Failed to create block size histogram");

    pub static ref PROCESSED_SNAPSHOT_ITEMS: GaugeVec = GaugeVec::new(
        Opts::new("cloudbreak_processed_snapshot_items", "Number of processed snapshot items"),
        &["type"]
    )
    .expect("Failed to create processed snapshot items counter");

    pub static ref SNAPSHOT_CLEAN_UP_DUPLICATED_ACCOUNTS_BATCH_TIME: Histogram = Histogram::with_opts(
        HistogramOpts::new(
            "cloudbreak_snapshot_clean_up_duplicated_accounts_batch_time",
            "Time taken to clean up a batch of duplicated accounts"
        )
    )
    .expect("Failed to create snapshot clean up duplicated accounts batch time histogram");

    pub static ref SNAPSHOT_ACCOUNTS_BUFFER_SIZE: GaugeVec = GaugeVec::new(
        Opts::new("cloudbreak_snapshot_accounts_buffer_size", "Size of the buffer for snapshot accounts"),
        &["type"]
    )
    .expect("Failed to create snapshot accounts buffer size gauge");

    pub static ref SNAPSHOT_CLEAN_UP_DUPLICATED_ACCOUNTS_BATCH_ROWS_AFFECTED: CounterVec = CounterVec::new(
        Opts::new("cloudbreak_snapshot_clean_up_duplicated_accounts_batch_rows_affected", "Number of rows affected by a batch of duplicated accounts"),
        &["type"]
    )
    .expect("Failed to create snapshot clean up duplicated accounts batch rows affected counter");

    pub static ref BOOTSTRAP_PHASE: GaugeVec = GaugeVec::new(
        Opts::new("cloudbreak_bootstrap_phase", "Current durable bootstrap phase (one active series has value 1)"),
        &["run_id", "phase", "source"]
    ).expect("Failed to create bootstrap phase gauge");

    pub static ref BOOTSTRAP_FILES_TOTAL: GaugeVec = GaugeVec::new(
        Opts::new("cloudbreak_bootstrap_account_files_total", "Account files in the durable bootstrap manifest"),
        &["archive_type"]
    ).expect("Failed to create bootstrap file total gauge");

    pub static ref BOOTSTRAP_FILES_COMPLETED: GaugeVec = GaugeVec::new(
        Opts::new("cloudbreak_bootstrap_account_files_completed", "Account files committed to PostgreSQL"),
        &["archive_type"]
    ).expect("Failed to create bootstrap completed file gauge");

    pub static ref BOOTSTRAP_FILES_SKIPPED: GaugeVec = GaugeVec::new(
        Opts::new("cloudbreak_bootstrap_account_files_skipped", "Completed account files skipped by the current resumed run"),
        &["archive_type"]
    ).expect("Failed to create bootstrap skipped file gauge");

    pub static ref BOOTSTRAP_RESUMED_RUNS_TOTAL: Counter = Counter::new(
        "cloudbreak_bootstrap_resumed_runs_total", "Number of bootstrap process starts that resumed a durable run"
    ).expect("Failed to create bootstrap resumed run counter");

    pub static ref BOOTSTRAP_DISCARDED_RUNS_TOTAL: CounterVec = CounterVec::new(
        Opts::new("cloudbreak_bootstrap_discarded_runs_total", "Durable bootstrap runs discarded as unrecoverable"),
        &["reason"]
    ).expect("Failed to create bootstrap discarded run counter");

    pub static ref BOOTSTRAP_PENDING_LIVE_UPDATES: Gauge = Gauge::new(
        "cloudbreak_bootstrap_pending_live_update_reconciliations", "Live-updated pubkeys still pending snapshot-table reconciliation"
    ).expect("Failed to create pending live update gauge");

    pub static ref BOOTSTRAP_POSTPROCESS_ITEMS: GaugeVec = GaugeVec::new(
        Opts::new("cloudbreak_bootstrap_postprocessing_items", "Durable post-processing item totals and completions"),
        &["phase", "state"]
    ).expect("Failed to create bootstrap postprocessing gauge");
}

pub fn increment_db_snapshot_errors() {
    DB_SNAPSHOT_ERRORS.inc();
}

pub fn record_snapshot_batch_insert_time(elapsed: f64) {
    SNAPSHOT_BATCH_INSERT_TIME.observe(elapsed);
}

pub fn register_metrics(metrics_registry: Option<Registry>) -> Result<(), anyhow::Error> {
    let metrics_registry = match metrics_registry {
        Some(metrics_registry) => metrics_registry,
        None => return Ok(()),
    };

    register_collector(&metrics_registry, Box::new(DB_SNAPSHOT_ERRORS.clone()))?;
    register_collector(
        &metrics_registry,
        Box::new(SNAPSHOT_BATCH_INSERT_TIME.clone()),
    )?;
    register_collector(
        &metrics_registry,
        Box::new(PROCESSED_SNAPSHOT_ITEMS.clone()),
    )?;
    register_collector(
        &metrics_registry,
        Box::new(SNAPSHOT_CLEAN_UP_DUPLICATED_ACCOUNTS_BATCH_TIME.clone()),
    )?;
    register_collector(
        &metrics_registry,
        Box::new(SNAPSHOT_CLEAN_UP_DUPLICATED_ACCOUNTS_BATCH_ROWS_AFFECTED.clone()),
    )?;
    register_collector(
        &metrics_registry,
        Box::new(SNAPSHOT_ACCOUNTS_BUFFER_SIZE.clone()),
    )?;
    register_collector(&metrics_registry, Box::new(BOOTSTRAP_PHASE.clone()))?;
    register_collector(&metrics_registry, Box::new(BOOTSTRAP_FILES_TOTAL.clone()))?;
    register_collector(
        &metrics_registry,
        Box::new(BOOTSTRAP_FILES_COMPLETED.clone()),
    )?;
    register_collector(&metrics_registry, Box::new(BOOTSTRAP_FILES_SKIPPED.clone()))?;
    register_collector(
        &metrics_registry,
        Box::new(BOOTSTRAP_RESUMED_RUNS_TOTAL.clone()),
    )?;
    register_collector(
        &metrics_registry,
        Box::new(BOOTSTRAP_DISCARDED_RUNS_TOTAL.clone()),
    )?;
    register_collector(
        &metrics_registry,
        Box::new(BOOTSTRAP_PENDING_LIVE_UPDATES.clone()),
    )?;
    register_collector(
        &metrics_registry,
        Box::new(BOOTSTRAP_POSTPROCESS_ITEMS.clone()),
    )?;

    Ok(())
}

pub fn set_bootstrap_phase(run_id: i64, phase: &str, source: &str) {
    BOOTSTRAP_PHASE.reset();
    let run_id = run_id.to_string();
    BOOTSTRAP_PHASE
        .with_label_values(&[run_id.as_str(), phase, source])
        .set(1.0);
}

fn register_collector(
    registry: &Registry,
    collector: Box<dyn Collector>,
) -> Result<(), anyhow::Error> {
    match registry.register(collector) {
        Ok(()) | Err(Error::AlreadyReg) => Ok(()),
        Err(error) => Err(error.into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_metrics_registration_is_idempotent() {
        let registry = Registry::new();

        register_metrics(Some(registry.clone())).unwrap();
        register_metrics(Some(registry)).unwrap();
    }
}
