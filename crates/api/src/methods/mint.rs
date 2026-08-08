// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use crate::error::RpcError;
use crate::http::CloudbreakRpcState;
use crate::methods::get_multiple_accounts::fetch_accounts_without_mint;
use crate::methods::is_token_program;
use cloudbreak_core::modules::rpc_filter_type::{RpcFilterType, RpcProgramAccountsConfig};
use sea_orm::sqlx::Row;
use sea_orm::{DatabaseConnection, sqlx};
use solana_commitment_config::CommitmentLevel;
use solana_pubkey::Pubkey;
use std::time::Duration;
use tokio::time::{Instant, timeout};

/// Check if there is at least one memcmp that allows to use tokenowner or tokenmint DB indexes or Error otherwise
/// Returns a boolean indicating if the filters match for a mint filter
pub fn check_filters_are_valid_for_token_query(
    program: Pubkey,
    config: RpcProgramAccountsConfig,
) -> Result<Option<Vec<u8>>, RpcError> {
    if !is_token_program(&program) {
        return Err(RpcError::InternalError);
    }

    let filters = config.filters.as_ref().ok_or(RpcError::InvalidParams)?;

    let mut valid_filters = false;
    let mut mint_pubkey = None;

    for filter in filters {
        if let RpcFilterType::Memcmp(memcmp) = filter {
            let offset = memcmp.offset();
            let bytes = memcmp.bytes().ok_or(RpcError::InvalidParams)?;

            let is_token_filter = offset == 32 && bytes.len() == 32;
            let is_mint_filter = offset == 0 && bytes.len() == 32;

            if is_mint_filter {
                mint_pubkey = Some(bytes.to_vec());
            }

            if is_token_filter || is_mint_filter {
                valid_filters = true;
                break;
            }
        }
    }

    if !valid_filters {
        return Err(RpcError::InvalidParams);
    }

    Ok(mint_pubkey)
}

/// gets mint data from the database (it adds the ability to merge mint data for jsonParsed
///  encoding for gPA tokenmint queries)
#[tracing::instrument(name = "mint_data", skip_all)]
pub async fn get_mint(
    token_program: Pubkey,
    mint: Vec<u8>,
    slot: u64,
    db: &DatabaseConnection,
    queries_timeout: Duration,
) -> Option<Vec<u8>> {
    let start_time = Instant::now();
    let pool = db.get_postgres_connection_pool();
    let db_slot = i64::try_from(slot).ok()?;

    let rows = timeout(queries_timeout, async {
        sqlx::query(include_str!("../db/getMintDataWithProgram.sql"))
            .bind(mint)
            .bind(db_slot)
            .bind(token_program.to_bytes().to_vec())
            .fetch_all(pool)
            .await
            .map_err(|e| {
                tracing::error!("Database query error: {}", e);
                RpcError::InternalError
            })
            .ok()
    })
    .await
    .unwrap_or_else(|elapsed| {
        tracing::error!("Database query error: {}", elapsed);
        None
    })?;

    let row = rows.first()?;

    let mint_data: Vec<u8> = row.get::<Vec<u8>, _>("data");

    tracing::debug!(
        target: "gpa_mint_data",
        "Mint filter query duration: {:?} microseconds - mint data length: {}",
        start_time.elapsed().as_micros(),
        mint_data.len()
    );

    Some(mint_data)
}

/// Fetch a live token mint from only the two token-program partitions. If the
/// account is not there, use the generic maintained lookup so callers preserve
/// the distinction between a missing account and a non-token account.
pub async fn fetch_token_mint_account(
    state: &CloudbreakRpcState,
    mint: &Pubkey,
    slot: u64,
    commitment: CommitmentLevel,
) -> Result<Vec<sqlx::postgres::PgRow>, sqlx::Error> {
    let db_slot = i64::try_from(slot).map_err(|error| {
        sqlx::Error::Protocol(format!("slot does not fit in PostgreSQL bigint: {error}"))
    })?;
    let mint_bytes = mint.to_bytes().to_vec();
    let rows = sqlx::query(include_str!("../db/getTokenMintAccount.sql"))
        .bind(&mint_bytes)
        .bind(db_slot)
        .fetch_all(state.database.get_postgres_connection_pool())
        .await?;

    if rows.is_empty() {
        fetch_accounts_without_mint(state, &[mint_bytes], slot, commitment).await
    } else {
        Ok(rows)
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn token_mint_lookup_exposes_owner_predicates_to_partition_planner() {
        let sql = include_str!("../db/getTokenMintAccount.sql");

        assert_eq!(sql.matches("(accounts.owner =").count(), 1);
        assert_eq!(sql.matches("(snapshot_accounts.owner =").count(), 1);
        assert!(sql.contains("accounts.pubkey = $1"));
        assert!(sql.contains("snapshot_accounts.pubkey = $1"));
    }
}
