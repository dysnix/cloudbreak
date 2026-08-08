// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use sea_orm::sqlx::Row;
use solana_commitment_config::CommitmentLevel;
use solana_pubkey::Pubkey;
use solana_rpc_client_api::config::RpcContextConfig;
use solana_rpc_client_api::response::{Response as RpcResponse, RpcResponseContext};
use tokio::time::timeout;
use tracing::Instrument;

use crate::error::RpcError;
use crate::http::CloudbreakRpcState;
use crate::methods::get_multiple_accounts::fetch_accounts_without_mint;
use crate::methods::resolve_commitment;
use crate::metrics;

#[tracing::instrument(name = "get_balance_rpc", skip_all, fields(pubkey = %pubkey))]
pub async fn get_balance(
    state: &CloudbreakRpcState,
    pubkey: String,
    config: Option<RpcContextConfig>,
) -> Result<RpcResponse<u64>, RpcError> {
    let _guard = metrics::InFlightRequestGuard::new("getBalance");

    let config = config.unwrap_or_default();

    let pubkey: Pubkey = pubkey
        .parse()
        .map_err(|_| RpcError::PubkeyValidationError(pubkey.clone()))?;

    let commitment = config
        .commitment
        .map(|commitment_config| {
            resolve_commitment(commitment_config.commitment, state.processed_commitment)
        })
        .transpose()?
        .unwrap_or(CommitmentLevel::Finalized);

    let (context_slot, _) = state.latest_slot_and_block_time(commitment).await?;

    if let Some(min_context_slot) = config.min_context_slot
        && context_slot < min_context_slot
    {
        return Err(RpcError::RpcSlotBehindMinContextSlot {
            rpc_slot: context_slot,
        });
    }

    let pubkey_bytes = vec![pubkey.to_bytes().to_vec()];
    let rows = timeout(state.queries_timeout, async {
        let span = tracing::info_span!("get_balance_db");
        fetch_accounts_without_mint(state, &pubkey_bytes, context_slot, commitment)
            .instrument(span)
            .await
    })
    .await
    .map_err(|_elapsed| {
        tracing::error!("getBalance query timed out");
        RpcError::InternalError
    })?
    .map_err(|e| {
        tracing::error!("Database query error: {}", e);
        RpcError::InternalError
    })?;

    let Some(row) = rows.first() else {
        return Ok(RpcResponse {
            context: RpcResponseContext {
                slot: context_slot,
                api_version: None,
            },
            value: 0,
        });
    };

    let present = row.try_get::<bool, _>("present").unwrap_or(true);
    let lamports = row.get::<i64, _>("lamports");
    if !present || lamports <= 0 {
        return Ok(RpcResponse {
            context: RpcResponseContext {
                slot: context_slot,
                api_version: None,
            },
            value: 0,
        });
    }

    let owner_bytes: Vec<u8> = row.get("owner");

    let owner = Pubkey::try_from(owner_bytes.as_slice()).map_err(|_| RpcError::InternalError)?;

    if !state.indexer_filter.is_program_selected(&owner) {
        return Err(RpcError::AccountOwnerExcluded {
            pubkey: pubkey.to_string(),
            owner: owner.to_string(),
        });
    }

    Ok(RpcResponse {
        context: RpcResponseContext {
            slot: context_slot,
            api_version: None,
        },
        value: lamports as u64,
    })
}
