// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use cloudbreak_core::{
    AccountSelectorConfig,
    account_lookup::{self as lookup_helpers, CONFIRMED_COMMITMENT, FINALIZED_COMMITMENT},
};
use cloudbreak_entity::account_lookup;
use rust_decimal::prelude::ToPrimitive;
use sea_orm::ActiveValue::{NotSet, Set};
use sea_orm::sqlx::Row;
use sea_orm::sqlx::{self};
use solana_account::AccountSharedData;
use solana_account_decoder::{UiAccountEncoding, UiDataSliceConfig, encode_ui_account};
use solana_account_decoder_client_types::UiAccount;
use solana_commitment_config::CommitmentLevel;
use solana_pubkey::Pubkey;
use solana_rpc_client_api::config::RpcAccountInfoConfig;
use solana_rpc_client_api::response::{Response as RpcResponse, RpcResponseContext};
use tokio::time::timeout;
use tracing::Instrument;

use crate::error::RpcError;
use crate::http::CloudbreakRpcState;
use crate::methods::token::{check_account_data_len_for_encoding, parse_additional_mint_data};
use crate::methods::{is_token_program, resolve_commitment};
use crate::metrics;

const GMA_BLOCKING_ENCODING_MIN_BYTES: usize = 64 * 1024;

struct AccountForEncoding {
    owner: Pubkey,
    lamports: u64,
    executable: bool,
    rent_epoch: u64,
    data: Arc<Vec<u8>>,
    mint_data: Vec<u8>,
}

fn should_offload_encoding(encoding: UiAccountEncoding, account_data_bytes: usize) -> bool {
    encoding == UiAccountEncoding::Base64Zstd
        || account_data_bytes >= GMA_BLOCKING_ENCODING_MIN_BYTES
}

fn encode_accounts(
    parsed_pubkeys: Vec<Pubkey>,
    row_by_pubkey: HashMap<Pubkey, AccountForEncoding>,
    indexer_filter: Arc<AccountSelectorConfig>,
    encoding: UiAccountEncoding,
    data_slice: Option<UiDataSliceConfig>,
    block_time: i64,
) -> Result<Vec<Option<UiAccount>>, RpcError> {
    let mut result = Vec::with_capacity(parsed_pubkeys.len());

    for pubkey in &parsed_pubkeys {
        let Some(row) = row_by_pubkey.get(pubkey) else {
            result.push(None);
            continue;
        };

        if !indexer_filter.is_program_selected(&row.owner) {
            tracing::error!(
                target: "gma_indexer_filter",
                pubkey = %pubkey,
                owner = %row.owner,
                "getMultipleAccounts: skipping account because owner is excluded by the current indexer filter"
            );
            result.push(None);
            continue;
        }

        let additional_mint_data = if encoding == UiAccountEncoding::JsonParsed
            && is_token_program(&row.owner)
            && row.data.len() >= 32
        {
            let mint_pubkey =
                Pubkey::try_from(&row.data[..32]).map_err(|_| RpcError::InternalError)?;
            parse_additional_mint_data(&mint_pubkey, &row.mint_data, block_time)
        } else {
            None
        };

        check_account_data_len_for_encoding(encoding, data_slice, row.data.len(), pubkey)?;

        let account_shared_data = AccountSharedData::create_from_existing_shared_data(
            row.lamports,
            Arc::clone(&row.data),
            row.owner,
            row.executable,
            row.rent_epoch,
        );
        result.push(Some(encode_ui_account(
            pubkey,
            &account_shared_data,
            encoding,
            additional_mint_data,
            data_slice,
        )));
    }

    Ok(result)
}

pub(crate) async fn fetch_accounts_without_mint(
    state: &CloudbreakRpcState,
    pubkey_bytes: &[Vec<u8>],
    latest_slot: u64,
    commitment: CommitmentLevel,
) -> Result<Vec<sqlx::postgres::PgRow>, sqlx::Error> {
    let pool = state.database.get_postgres_connection_pool();
    let db_slot = i64::try_from(latest_slot).map_err(|error| {
        sqlx::Error::Protocol(format!("slot does not fit in PostgreSQL bigint: {error}"))
    })?;
    let commitment_code = match commitment {
        CommitmentLevel::Confirmed => CONFIRMED_COMMITMENT,
        CommitmentLevel::Finalized => FINALIZED_COMMITMENT,
        CommitmentLevel::Processed => CONFIRMED_COMMITMENT,
    };
    let commitment_label = match commitment_code {
        CONFIRMED_COMMITMENT => "confirmed",
        FINALIZED_COMMITMENT => "finalized",
        _ => unreachable!("unsupported account lookup commitment"),
    };

    let lookup_result = sqlx::query(include_str!("../db/getMultipleAccountsLookup.sql"))
        .bind(pubkey_bytes)
        .bind(commitment_code)
        .bind(db_slot)
        .fetch_all(pool)
        .await;
    // During a rolling upgrade the API can briefly start before the indexer's
    // migration init container has created the lookup table. Keep serving from
    // the canonical tables and begin caching automatically once it exists.
    let mut rows = match lookup_result {
        Ok(rows) => rows,
        Err(sqlx::Error::Database(error)) if error.code().as_deref() == Some("42P01") => {
            tracing::warn!("account lookup table is not available yet; using canonical tables");
            Vec::new()
        }
        Err(error) => return Err(error),
    };
    let cached_pubkeys = rows
        .iter()
        .map(|row| row.get::<Vec<u8>, _>("pubkey"))
        .collect::<HashSet<_>>();
    let missing_pubkeys = pubkey_bytes
        .iter()
        .filter(|pubkey| !cached_pubkeys.contains(*pubkey))
        .cloned()
        .collect::<HashSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();

    metrics::CLOUDBREAK_ACCOUNT_LOOKUP_KEYS_TOTAL
        .with_label_values(&[commitment_label, "hit"])
        .inc_by(cached_pubkeys.len() as u64);
    metrics::CLOUDBREAK_ACCOUNT_LOOKUP_KEYS_TOTAL
        .with_label_values(&[commitment_label, "miss"])
        .inc_by(missing_pubkeys.len() as u64);

    if missing_pubkeys.is_empty() {
        return Ok(rows);
    }

    let fallback_rows = sqlx::query(include_str!("../db/getMultipleAccounts.sql"))
        .bind(&missing_pubkeys)
        .bind(db_slot)
        .fetch_all(pool)
        .await?;
    let returned_pubkeys = fallback_rows
        .iter()
        .map(|row| row.get::<Vec<u8>, _>("pubkey"))
        .collect::<HashSet<_>>();
    let mut lookup_rows = fallback_rows
        .iter()
        .map(|row| account_lookup::ActiveModel {
            pubkey: Set(row.get("pubkey")),
            commitment: Set(commitment_code),
            present: Set(true),
            owner: Set(row.get("owner")),
            lamports: Set(row.get("lamports")),
            account_slot: Set(row.get("slot")),
            executable: Set(row.get("executable")),
            rent_epoch: Set(row.get("rent_epoch")),
            data: Set(row.get("data")),
            write_version: Set(row.get("write_version")),
            updated_on: NotSet,
        })
        .collect::<Vec<_>>();
    lookup_rows.extend(
        missing_pubkeys
            .iter()
            .filter(|pubkey| !returned_pubkeys.contains(*pubkey))
            .cloned()
            .map(|pubkey| lookup_helpers::tombstone(pubkey, commitment_code, db_slot)),
    );

    if let Some(permit) = state.try_account_lookup_fill_permit() {
        metrics::CLOUDBREAK_ACCOUNT_LOOKUP_FILLS_TOTAL
            .with_label_values(&["scheduled"])
            .inc();
        let database = state.database.clone();
        tokio::spawn(async move {
            let _permit = permit;
            match lookup_helpers::upsert(&database, lookup_rows).await {
                Ok(()) => metrics::CLOUDBREAK_ACCOUNT_LOOKUP_FILLS_TOTAL
                    .with_label_values(&["success"])
                    .inc(),
                Err(error) => {
                    metrics::CLOUDBREAK_ACCOUNT_LOOKUP_FILLS_TOTAL
                        .with_label_values(&["error"])
                        .inc();
                    tracing::warn!(?error, "failed to populate account lookup cache");
                }
            }
        });
    } else {
        metrics::CLOUDBREAK_ACCOUNT_LOOKUP_FILLS_TOTAL
            .with_label_values(&["saturated"])
            .inc();
    }
    rows.extend(fallback_rows);
    Ok(rows)
}

#[tracing::instrument(name = "gma_rpc", skip_all, fields(num_pubkeys = pubkeys.len()))]
pub async fn get_multiple_accounts(
    state: &CloudbreakRpcState,
    pubkeys: Vec<String>,
    config: Option<RpcAccountInfoConfig>,
) -> Result<RpcResponse<Vec<Option<UiAccount>>>, RpcError> {
    let _guard = metrics::InFlightRequestGuard::new("gma");

    let max_multiple_accounts = state.max_multiple_accounts;
    if pubkeys.len() > max_multiple_accounts {
        return Err(RpcError::InvalidParamsWithMessage(format!(
            "Too many inputs provided; max {max_multiple_accounts}"
        )));
    }

    let config = config.unwrap_or_default();

    // Validate all pubkeys up-front. Any failure fails the whole call
    let parsed_pubkeys: Vec<Pubkey> = pubkeys
        .iter()
        .map(|pk| {
            pk.parse::<Pubkey>()
                .map_err(|_| RpcError::PubkeyValidationError(pk.clone()))
        })
        .collect::<Result<Vec<_>, _>>()?;

    let commitment = config
        .commitment
        .map(|commitment_config| {
            resolve_commitment(commitment_config.commitment, state.processed_commitment)
        })
        .transpose()?
        .unwrap_or(CommitmentLevel::Finalized);

    let (latest_slot, block_time) = state.latest_slot_and_block_time(commitment).await?;

    if let Some(min_context_slot) = config.min_context_slot
        && latest_slot < min_context_slot
    {
        return Err(RpcError::RpcSlotBehindMinContextSlot {
            rpc_slot: latest_slot,
        });
    }

    // Short-circuit for an empty input list, return `value: []` without touching the DB.
    if parsed_pubkeys.is_empty() {
        return Ok(RpcResponse {
            context: RpcResponseContext {
                slot: latest_slot,
                api_version: None,
            },
            value: vec![],
        });
    }

    let encoding = config.encoding.unwrap_or(UiAccountEncoding::Base64);
    let data_slice = config.data_slice;
    let with_mint = encoding == UiAccountEncoding::JsonParsed;

    let sql_template = if with_mint {
        include_str!("../db/getMultipleAccountsWithMintData.sql")
    } else {
        include_str!("../db/getMultipleAccounts.sql")
    };

    let pubkey_bytes: Vec<Vec<u8>> = parsed_pubkeys
        .iter()
        .map(|pubkey| pubkey.to_bytes().to_vec())
        .collect();
    // Keep the query text stable so SQLx can reuse its per-connection prepared-statement cache.
    // The tracing span still records query latency without injecting a unique traceparent comment.
    tracing::debug!(target: "gma_sql", "## sql: {}", sql_template);

    let pool = state.database.get_postgres_connection_pool();
    let rows = timeout(state.queries_timeout, async {
        let span = tracing::info_span!("gma_db");
        async {
            if with_mint {
                return sqlx::query(sql_template)
                    .bind(&pubkey_bytes)
                    .bind(i64::try_from(latest_slot).map_err(|error| {
                        sqlx::Error::Protocol(format!(
                            "slot does not fit in PostgreSQL bigint: {error}"
                        ))
                    })?)
                    .fetch_all(pool)
                    .await;
            }
            fetch_accounts_without_mint(state, &pubkey_bytes, latest_slot, commitment).await
        }
        .instrument(span)
        .await
    })
    .await
    .map_err(|_elapsed| {
        tracing::error!("getMultipleAccounts query timed out");
        RpcError::InternalError
    })?
    .map_err(|e| {
        tracing::error!("Database query error: {}", e);
        RpcError::InternalError
    })?;

    // Decode each SQL row once and retain account data behind an Arc. This
    // avoids copying every payload again when AccountSharedData is created and
    // lets repeated request pubkeys share the same backing allocation.
    let mut row_by_pubkey = HashMap::with_capacity(rows.len());
    let mut account_data_bytes = 0usize;
    for row in rows {
        let pubkey_bytes: Vec<u8> = row.get("pubkey");
        let row_pubkey = Pubkey::try_from(pubkey_bytes.as_slice()).map_err(|_| {
            tracing::error!("getMultipleAccounts: invalid pubkey bytes returned by DB");
            RpcError::InternalError
        })?;
        let present = row.try_get::<bool, _>("present").unwrap_or(true);
        let lamports = row.get::<i64, _>("lamports");
        if !present || lamports <= 0 {
            continue;
        }

        let owner_bytes: Vec<u8> = row.get("owner");
        let owner =
            Pubkey::try_from(owner_bytes.as_slice()).map_err(|_| RpcError::InternalError)?;
        let executable: bool = row.get("executable");
        let rent_epoch = row
            .get::<rust_decimal::Decimal, _>("rent_epoch")
            .to_u64()
            .unwrap_or(0);
        let data: Vec<u8> = row.get("data");
        account_data_bytes = account_data_bytes.saturating_add(data.len());
        row_by_pubkey.insert(
            row_pubkey,
            AccountForEncoding {
                owner,
                lamports: lamports as u64,
                executable,
                rent_epoch,
                data: Arc::new(data),
                mint_data: row.try_get("mint_data").ok().unwrap_or_default(),
            },
        );
    }

    let indexer_filter = Arc::clone(&state.indexer_filter);
    let encode = move || {
        encode_accounts(
            parsed_pubkeys,
            row_by_pubkey,
            indexer_filter,
            encoding,
            data_slice,
            block_time,
        )
    };
    let result = if should_offload_encoding(encoding, account_data_bytes) {
        tokio::task::spawn_blocking(encode)
            .await
            .map_err(|error| {
                tracing::error!(?error, "getMultipleAccounts encoding task failed");
                RpcError::InternalError
            })??
    } else {
        encode()?
    };

    Ok(RpcResponse {
        context: RpcResponseContext {
            slot: latest_slot,
            api_version: None,
        },
        value: result,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn offloads_compressed_or_large_account_encoding() {
        assert!(should_offload_encoding(UiAccountEncoding::Base64Zstd, 1));
        assert!(should_offload_encoding(
            UiAccountEncoding::Base64,
            GMA_BLOCKING_ENCODING_MIN_BYTES
        ));
        assert!(!should_offload_encoding(
            UiAccountEncoding::Base64,
            GMA_BLOCKING_ENCODING_MIN_BYTES - 1
        ));
    }
}
