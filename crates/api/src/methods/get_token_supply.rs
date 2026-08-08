// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use sea_orm::sqlx::Row;
use solana_account_decoder::parse_token::token_amount_to_ui_amount_v3;
use solana_account_decoder_client_types::token::UiTokenAmount;
use solana_commitment_config::{CommitmentConfig, CommitmentLevel};
use solana_pubkey::Pubkey;
use solana_rpc_client_api::response::{Response as RpcResponse, RpcResponseContext};
use spl_token_2022::extension::StateWithExtensions;
use spl_token_2022::state::Mint;
use tokio::time::timeout;
use tracing::Instrument;

use crate::error::RpcError;
use crate::http::CloudbreakRpcState;
use crate::methods::get_multiple_accounts::fetch_accounts_without_mint;
use crate::methods::token::parse_additional_mint_data;
use crate::methods::{is_token_program, resolve_commitment};
use crate::metrics;

#[tracing::instrument(name = "get_token_supply_rpc", skip_all, fields(pubkey = %mint))]
pub async fn get_token_supply(
    state: &CloudbreakRpcState,
    mint: String,
    commitment: Option<CommitmentConfig>,
) -> Result<RpcResponse<UiTokenAmount>, RpcError> {
    let _guard = metrics::InFlightRequestGuard::new("getTokenSupply");

    let pubkey: Pubkey = mint
        .parse()
        .map_err(|_| RpcError::PubkeyValidationError(mint.clone()))?;

    let commitment = commitment
        .map(|commitment_config| {
            resolve_commitment(commitment_config.commitment, state.processed_commitment)
        })
        .transpose()?
        .unwrap_or(CommitmentLevel::Finalized);

    let (latest_slot, block_time) = state.latest_slot_and_block_time(commitment).await?;

    let pubkey_bytes = vec![pubkey.to_bytes().to_vec()];
    let rows = timeout(state.queries_timeout, async {
        let span = tracing::info_span!("get_token_supply_db");
        fetch_accounts_without_mint(state, &pubkey_bytes, latest_slot, commitment)
            .instrument(span)
            .await
    })
    .await
    .map_err(|_elapsed| {
        tracing::error!("getTokenSupply query timed out");
        RpcError::InternalError
    })?
    .map_err(|e| {
        tracing::error!("Database query error: {}", e);
        RpcError::InternalError
    })?;

    let Some(row) = rows.first() else {
        // Account not in DB (or its latest version was closed)
        return Err(RpcError::AccountNotFound {
            pubkey: pubkey.to_string(),
        });
    };

    if !row.try_get::<bool, _>("present").unwrap_or(true) || row.get::<i64, _>("lamports") <= 0 {
        return Err(RpcError::AccountNotFound {
            pubkey: pubkey.to_string(),
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

    if !is_token_program(&owner) {
        return Err(RpcError::NotATokenAccount {
            pubkey: pubkey.to_string(),
        });
    }

    let data: Vec<u8> = row.get("data");

    let mint_state =
        StateWithExtensions::<Mint>::unpack(&data).map_err(|_| RpcError::MintDataNotFound {
            mint: pubkey.to_string(),
        })?;

    let supply = mint_state.base.supply;
    let additional_mint_data = parse_additional_mint_data(&pubkey, &data, block_time);
    let additional_data = additional_mint_data
        .as_ref()
        .and_then(|d| d.spl_token_additional_data.as_ref())
        .ok_or_else(|| RpcError::MintDataNotFound {
            mint: pubkey.to_string(),
        })?;

    let ui_token_amount = token_amount_to_ui_amount_v3(supply, additional_data);

    Ok(RpcResponse {
        context: RpcResponseContext {
            slot: latest_slot,
            api_version: None,
        },
        value: ui_token_amount,
    })
}
