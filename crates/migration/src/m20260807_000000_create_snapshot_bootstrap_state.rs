use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                CREATE TABLE snapshot_bootstrap_runs (
                    id BIGSERIAL PRIMARY KEY,
                    target_slot BIGINT NOT NULL,
                    covered_slot BIGINT,
                    phase TEXT NOT NULL,
                    source TEXT NOT NULL DEFAULT 'snapshot',
                    resumed_count BIGINT NOT NULL DEFAULT 0,
                    abandon_reason TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    completed_at TIMESTAMPTZ,
                    CONSTRAINT snapshot_bootstrap_runs_phase_check CHECK (phase IN (
                        'pair_selection', 'download', 'extraction', 'ingestion', 'clustering',
                        'deduplication_preparation', 'duplicate_cleanup', 'closed_account_cleanup',
                        'index_creation', 'live_update_reconciliation', 'ready', 'abandoned'
                    ))
                );

                CREATE UNIQUE INDEX snapshot_bootstrap_one_active_run
                    ON snapshot_bootstrap_runs ((true))
                    WHERE phase NOT IN ('ready', 'abandoned');

                CREATE TABLE snapshot_bootstrap_archives (
                    run_id BIGINT NOT NULL REFERENCES snapshot_bootstrap_runs(id) ON DELETE CASCADE,
                    archive_type TEXT NOT NULL,
                    file_name TEXT NOT NULL,
                    slot BIGINT NOT NULL,
                    base_slot BIGINT,
                    downloading_endpoint TEXT NOT NULL,
                    download_url TEXT,
                    archive_size BIGINT,
                    validation_failures INTEGER NOT NULL DEFAULT 0,
                    downloaded_at TIMESTAMPTZ,
                    extracted_at TIMESTAMPTZ,
                    PRIMARY KEY (run_id, archive_type),
                    CONSTRAINT snapshot_bootstrap_archives_type_check
                        CHECK (archive_type IN ('full', 'incremental'))
                );

                CREATE TABLE snapshot_bootstrap_account_files (
                    run_id BIGINT NOT NULL REFERENCES snapshot_bootstrap_runs(id) ON DELETE CASCADE,
                    archive_type TEXT NOT NULL,
                    file_name TEXT NOT NULL,
                    account_slot BIGINT NOT NULL,
                    write_version BIGINT NOT NULL,
                    current_len BIGINT NOT NULL,
                    disk_size BIGINT NOT NULL,
                    account_count BIGINT,
                    skipped_on_resume BOOLEAN NOT NULL DEFAULT FALSE,
                    completed_at TIMESTAMPTZ,
                    PRIMARY KEY (run_id, archive_type, account_slot, write_version),
                    CONSTRAINT snapshot_bootstrap_account_files_type_check
                        CHECK (archive_type IN ('full', 'incremental'))
                );

                CREATE INDEX snapshot_bootstrap_account_files_pending
                    ON snapshot_bootstrap_account_files (run_id, archive_type)
                    WHERE completed_at IS NULL;

                CREATE TABLE snapshot_bootstrap_postprocessing (
                    run_id BIGINT NOT NULL REFERENCES snapshot_bootstrap_runs(id) ON DELETE CASCADE,
                    phase TEXT NOT NULL,
                    item TEXT NOT NULL,
                    completed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (run_id, phase, item)
                );

                CREATE TABLE snapshot_bootstrap_updated_accounts (
                    run_id BIGINT NOT NULL REFERENCES snapshot_bootstrap_runs(id) ON DELETE CASCADE,
                    pubkey BYTEA NOT NULL,
                    latest_slot BIGINT NOT NULL,
                    PRIMARY KEY (run_id, pubkey)
                );

                CREATE INDEX snapshot_bootstrap_updated_accounts_run
                    ON snapshot_bootstrap_updated_accounts (run_id);
                "#,
            )
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                DROP TABLE IF EXISTS snapshot_bootstrap_updated_accounts;
                DROP TABLE IF EXISTS snapshot_bootstrap_postprocessing;
                DROP TABLE IF EXISTS snapshot_bootstrap_account_files;
                DROP TABLE IF EXISTS snapshot_bootstrap_archives;
                DROP TABLE IF EXISTS snapshot_bootstrap_runs;
                "#,
            )
            .await?;

        Ok(())
    }
}
