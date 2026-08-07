// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

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
                CREATE UNLOGGED TABLE account_lookup (
                    pubkey BYTEA NOT NULL,
                    commitment INTEGER NOT NULL,
                    present BOOLEAN NOT NULL,
                    owner BYTEA NOT NULL,
                    lamports BIGINT NOT NULL,
                    account_slot BIGINT NOT NULL,
                    executable BOOLEAN NOT NULL,
                    rent_epoch NUMERIC(20, 0) NOT NULL,
                    data BYTEA NOT NULL,
                    write_version BIGINT NOT NULL,
                    updated_on TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (pubkey, commitment),
                    CONSTRAINT account_lookup_commitment_check CHECK (commitment IN (1, 2))
                );
                "#,
            )
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(
                Table::drop()
                    .table(AccountLookup::Table)
                    .if_exists()
                    .to_owned(),
            )
            .await?;
        Ok(())
    }
}

#[derive(Iden)]
enum AccountLookup {
    Table,
}
