// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use sea_orm::sqlx::{self, Row};
use solana_account_decoder::parse_token::token_amount_to_ui_amount_v3;
use solana_account_decoder_client_types::token::UiTokenAmount;
use solana_commitment_config::{CommitmentConfig, CommitmentLevel};
use solana_pubkey::Pubkey;
use solana_rpc_client_api::response::{Response as RpcResponse, RpcResponseContext};
use tokio::time::timeout;
use tracing::Instrument;

use crate::error::RpcError;
use crate::http::CloudbreakRpcState;
use crate::methods::get_multiple_accounts::fetch_accounts_without_mint;
use crate::methods::token::parse_additional_mint_data;
use crate::methods::{is_token_program, resolve_commitment};
use crate::metrics;

#[tracing::instrument(name = "get_token_account_balance_rpc", skip_all, fields(pubkey = %pubkey))]
pub async fn get_token_account_balance(
    state: &CloudbreakRpcState,
    pubkey: String,
    commitment: Option<CommitmentConfig>,
) -> Result<RpcResponse<UiTokenAmount>, RpcError> {
    let _guard = metrics::InFlightRequestGuard::new("getTokenAccountBalance");

    let pubkey: Pubkey = pubkey
        .parse()
        .map_err(|_| RpcError::PubkeyValidationError(pubkey.clone()))?;

    let commitment = commitment
        .map(|commitment_config| {
            resolve_commitment(commitment_config.commitment, state.processed_commitment)
        })
        .transpose()?
        .unwrap_or(CommitmentLevel::Finalized);

    let (latest_slot, block_time) = state.latest_slot_and_block_time(commitment).await?;

    let pubkey_bytes = vec![pubkey.to_bytes().to_vec()];
    let (rows, mint_rows) = timeout(state.queries_timeout, async {
        let span = tracing::info_span!("get_token_account_balance_db");
        async {
            let rows =
                fetch_accounts_without_mint(state, &pubkey_bytes, latest_slot, commitment).await?;
            let mint_pubkey = rows.first().and_then(|row| {
                let present = row.try_get::<bool, _>("present").unwrap_or(true);
                let lamports = row.get::<i64, _>("lamports");
                let owner_bytes: Vec<u8> = row.get("owner");
                let owner = Pubkey::try_from(owner_bytes.as_slice()).ok()?;
                let data: Vec<u8> = row.get("data");
                (present && lamports > 0 && is_token_program(&owner) && data.len() >= 32)
                    .then(|| Pubkey::try_from(&data[..32]).ok())
                    .flatten()
            });
            let mint_rows = match mint_pubkey {
                Some(mint_pubkey) if mint_pubkey != spl_token_interface::native_mint::id() => {
                    fetch_accounts_without_mint(
                        state,
                        &[mint_pubkey.to_bytes().to_vec()],
                        latest_slot,
                        commitment,
                    )
                    .await?
                }
                _ => Vec::new(),
            };
            Ok::<_, sqlx::Error>((rows, mint_rows))
        }
        .instrument(span)
        .await
    })
    .await
    .map_err(|_elapsed| {
        tracing::error!("getTokenAccountBalance query timed out");
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

    let present = row.try_get::<bool, _>("present").unwrap_or(true);
    if !present || row.get::<i64, _>("lamports") <= 0 {
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
    let (mint_pubkey, amount) = parse_token_account_fields(&data).ok_or_else(|| {
        tracing::error!(
            "getTokenAccountBalance: token account data is only {} bytes for pubkey {}",
            data.len(),
            pubkey
        );
        RpcError::InternalError
    })?;

    // Pass mint_data (or empty) unconditionally so the WSOL native_mint short-circuit
    // can hardcode decimals=9 even when the mint account itself isn't in our DB —
    // same trick we use in gAI / gTABO.
    let mint_data = mint_rows
        .first()
        .filter(|row| {
            let present = row.try_get::<bool, _>("present").unwrap_or(true);
            let lamports = row.get::<i64, _>("lamports");
            let owner_bytes: Vec<u8> = row.get("owner");
            let owner = Pubkey::try_from(owner_bytes.as_slice()).ok();
            present && lamports > 0 && owner.as_ref().is_some_and(is_token_program)
        })
        .map(|row| row.get::<Vec<u8>, _>("data"))
        .unwrap_or_default();
    let additional_mint_data = parse_additional_mint_data(&mint_pubkey, &mint_data, block_time);

    let additional_data = additional_mint_data
        .as_ref()
        .and_then(|d| d.spl_token_additional_data.as_ref())
        .ok_or_else(|| RpcError::MintDataNotFound {
            mint: mint_pubkey.to_string(),
        })?;

    let ui_token_amount = token_amount_to_ui_amount_v3(amount, additional_data);

    Ok(RpcResponse {
        context: RpcResponseContext {
            slot: latest_slot,
            api_version: None,
        },
        value: ui_token_amount,
    })
}

fn parse_token_account_fields(data: &[u8]) -> Option<(Pubkey, u64)> {
    let mint = Pubkey::try_from(data.get(..32)?).ok()?;
    let amount = u64::from_le_bytes(data.get(64..72)?.try_into().ok()?);
    Some((mint, amount))
}

#[cfg(test)]
mod tests {
    use super::parse_token_account_fields;

    #[test]
    fn token_account_fields_require_mint_and_amount_bytes() {
        let mut data = vec![0_u8; 72];
        data[..32].copy_from_slice(&[7_u8; 32]);
        data[64..72].copy_from_slice(&42_u64.to_le_bytes());

        let (mint, amount) = parse_token_account_fields(&data).expect("valid token account");
        assert_eq!(mint.to_bytes(), [7_u8; 32]);
        assert_eq!(amount, 42);
        assert_eq!(parse_token_account_fields(&data[..71]), None);
    }
}
