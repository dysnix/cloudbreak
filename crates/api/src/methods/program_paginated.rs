// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use std::time::Instant;

use cloudbreak_core::modules::rpc_filter_type::{
    RpcProgramAccountsConfig, account_matches_value_cmps, has_value_cmp,
};
use sea_orm::sqlx::{self, Row};
use serde::{Deserialize, Serialize};
use solana_account_decoder::UiAccountEncoding;
use solana_pubkey::Pubkey;
use solana_rpc_client_api::response::{
    Response as RpcResponse, RpcKeyedAccount, RpcResponseContext,
};
use tokio::time::timeout;

use crate::error::RpcError;
use crate::http::{CloudbreakApiResponse, CloudbreakRpcState};
use crate::methods::program;
use crate::methods::token;
use crate::methods::{SqlDataSliceFilter, is_token_program, resolve_commitment};
use crate::{db_query, metrics};

const DEFAULT_PAGE_LIMIT: usize = 1_000;
const MAX_PAGE_LIMIT: usize = 10_000;
const FILTERED_SCAN_MULTIPLIER: usize = 50;
const MAX_SCAN_LIMIT: usize = 100_000;

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RpcProgramAccountsPaginatedConfig {
    #[serde(flatten)]
    pub program_config: RpcProgramAccountsConfig,
    pub limit: Option<usize>,
    pub pagination_key: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PaginatedProgramAccounts {
    pub accounts: Vec<RpcKeyedAccount>,
    pub pagination_key: Option<String>,
}

pub type PaginatedProgramAccountsResponse = CloudbreakApiResponse<PaginatedProgramAccounts>;

pub async fn get_program_accounts_paginated(
    state: &CloudbreakRpcState,
    program: String,
    config: Option<RpcProgramAccountsPaginatedConfig>,
) -> Result<PaginatedProgramAccountsResponse, RpcError> {
    let _guard = metrics::InFlightRequestGuard::new("gpa_paginated");
    let config = config.unwrap_or_default();
    let limit = validate_limit(config.limit)?;
    let cursor = decode_pagination_key(config.pagination_key.as_deref())?;
    let program = program
        .parse::<Pubkey>()
        .map_err(|_| RpcError::InvalidParams)?;

    if !state.indexer_filter.is_program_selected(&program) {
        return Err(RpcError::KeyExcludedFromSecondaryIndex {
            key: program.to_string(),
        });
    }

    if let Some(filters) = &config.program_config.filters {
        for filter in filters {
            filter
                .verify()
                .map_err(|e| RpcError::InvalidParamsWithMessage(format!("Invalid param: {e}")))?;
        }
    }

    let commitment = config
        .program_config
        .account_config
        .commitment
        .map(|commitment| resolve_commitment(commitment.commitment, state.processed_commitment))
        .transpose()?
        .unwrap_or(solana_commitment_config::CommitmentLevel::Finalized);
    let (latest_slot, block_time) = state.latest_slot_and_block_time(commitment).await?;

    if let Some(min_context_slot) = config.program_config.account_config.min_context_slot
        && latest_slot < min_context_slot
    {
        return Err(RpcError::RpcSlotBehindMinContextSlot {
            rpc_slot: latest_slot,
        });
    }

    let filters = config.program_config.filters.clone().unwrap_or_default();
    let sql_filters = filters
        .iter()
        .filter_map(|filter| {
            SqlDataSliceFilter::new(filter, "latest", is_token_program(&program)).to_string()
        })
        .map(|filter| format!("AND {filter}"))
        .collect::<Vec<_>>()
        .join("\n        ");
    let scan_limit = scan_limit(limit, !filters.is_empty());
    let fetch_limit = limit + 1;
    let sql = load_sql(
        program,
        latest_slot,
        cursor.as_ref(),
        scan_limit,
        fetch_limit,
        &sql_filters,
    );

    tracing::debug!(target: "gpa_paginated_sql", "## sql: {}", sql);

    let query_started = Instant::now();
    let pool = state.database.get_postgres_connection_pool();
    let sql = db_query::add_trace_traceparent_to_query(&sql);
    let rows = timeout(state.queries_timeout, sqlx::raw_sql(&sql).fetch_all(pool))
        .await
        .map_err(|_| {
            tracing::error!("Paginated getProgramAccounts database query timed out");
            RpcError::InternalError
        })?
        .map_err(|error| {
            tracing::error!("Paginated getProgramAccounts database query error: {error}");
            RpcError::InternalError
        })?;

    let mut data_rows = Vec::with_capacity(rows.len().saturating_sub(1));
    let mut candidate_count = 0usize;
    let mut scan_end = None;
    for row in rows {
        if row.get::<bool, _>("is_metadata") {
            candidate_count = row.get::<i64, _>("candidate_count").max(0) as usize;
            scan_end = row
                .try_get::<Option<Vec<u8>>, _>("scan_end")
                .ok()
                .flatten()
                .and_then(|bytes| Pubkey::try_from(bytes.as_slice()).ok());
        } else {
            data_rows.push(row);
        }
    }

    let raw_row_count = data_rows.len();
    let apply_value_cmp = has_value_cmp(&filters);
    let encoding = config
        .program_config
        .account_config
        .encoding
        .unwrap_or(UiAccountEncoding::Binary);
    let data_slice = config.program_config.account_config.data_slice;
    let additional_mint_data = token_mint_additional_data(
        state,
        program,
        latest_slot,
        block_time,
        encoding,
        &config.program_config,
    )
    .await;

    let mut accounts = Vec::with_capacity(limit.min(raw_row_count));
    let mut response_bytes = 0u64;
    let encode_span = tracing::info_span!("gpa_paginated_encode");
    let mut last_consumed = None;
    let mut stopped_at_page_limit = false;

    for row in data_rows {
        let pubkey = Pubkey::try_from(row.get::<Vec<u8>, _>(0).as_slice())
            .map_err(|_| RpcError::InternalError)?;
        let matches_value_cmp =
            !apply_value_cmp || account_matches_value_cmps(&filters, &row.get::<Vec<u8>, _>(6));

        if matches_value_cmp && accounts.len() == limit {
            stopped_at_page_limit = true;
            break;
        }

        last_consumed = Some(pubkey);
        if !matches_value_cmp {
            continue;
        }

        let encoded = program::process_row(
            row,
            encoding,
            data_slice,
            &mut response_bytes,
            &encode_span,
            additional_mint_data,
        )?;
        accounts.push(encoded.account);
    }

    let pagination_key = if stopped_at_page_limit || raw_row_count == fetch_limit {
        last_consumed.map(|pubkey| pubkey.to_string())
    } else if candidate_count == scan_limit {
        scan_end.map(|pubkey| pubkey.to_string())
    } else {
        None
    };

    tracing::debug!(
        target: "gpa_paginated",
        accounts = accounts.len(),
        candidate_count,
        scan_limit,
        response_bytes,
        elapsed_ms = query_started.elapsed().as_millis(),
        "served paginated getProgramAccounts page"
    );

    let page = PaginatedProgramAccounts {
        accounts,
        pagination_key,
    };
    if config.program_config.with_context.unwrap_or(false) {
        Ok(CloudbreakApiResponse::ResponseWithContext(RpcResponse {
            context: RpcResponseContext::new(latest_slot),
            value: page,
        }))
    } else {
        Ok(CloudbreakApiResponse::Response(page))
    }
}

fn validate_limit(limit: Option<usize>) -> Result<usize, RpcError> {
    let limit = limit.unwrap_or(DEFAULT_PAGE_LIMIT);
    if !(1..=MAX_PAGE_LIMIT).contains(&limit) {
        return Err(RpcError::InvalidParamsWithMessage(format!(
            "limit must be between 1 and {MAX_PAGE_LIMIT}"
        )));
    }
    Ok(limit)
}

fn decode_pagination_key(key: Option<&str>) -> Result<Option<Pubkey>, RpcError> {
    key.map(|key| {
        key.parse::<Pubkey>()
            .map_err(|_| RpcError::InvalidParamsWithMessage("invalid paginationKey".to_string()))
    })
    .transpose()
}

fn scan_limit(page_limit: usize, filtered: bool) -> usize {
    if !filtered {
        return page_limit + 1;
    }

    page_limit
        .saturating_mul(FILTERED_SCAN_MULTIPLIER)
        .max(page_limit + 1)
        .min(MAX_SCAN_LIMIT)
}

fn load_sql(
    program: Pubkey,
    slot: u64,
    cursor: Option<&Pubkey>,
    scan_limit: usize,
    fetch_limit: usize,
    filters: &str,
) -> String {
    let program = format!("'\\x{}'::bytea", hex::encode(program.to_bytes()));
    let cursor = cursor
        .map(|cursor| format!("'\\x{}'::bytea", hex::encode(cursor.to_bytes())))
        .unwrap_or_else(|| "'\\x'::bytea".to_string());

    include_str!("../db/getProgramAccountsPaginated.sql")
        .replace("-- {filters}", filters)
        .replace("$1", &program)
        .replace("$2", &slot.to_string())
        .replace("$3", &cursor)
        .replace("$4", &scan_limit.to_string())
        .replace("$5", &fetch_limit.to_string())
}

async fn token_mint_additional_data(
    state: &CloudbreakRpcState,
    program: Pubkey,
    slot: u64,
    block_time: i64,
    encoding: UiAccountEncoding,
    config: &RpcProgramAccountsConfig,
) -> Option<solana_account_decoder::parse_account_data::AccountAdditionalDataV3> {
    if !is_token_program(&program) || encoding != UiAccountEncoding::JsonParsed {
        return None;
    }

    let mint =
        crate::methods::mint::check_filters_are_valid_for_token_query(program, config.clone())
            .ok()
            .flatten()?;
    let mint_pubkey = Pubkey::try_from(mint.as_slice()).ok()?;
    crate::methods::mint::get_mint(program, mint, slot, &state.database, state.queries_timeout)
        .await
        .and_then(|data| token::parse_additional_mint_data(&mint_pubkey, &data, block_time))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn paginated_config_uses_flat_wire_fields() {
        let config: RpcProgramAccountsPaginatedConfig = serde_json::from_value(serde_json::json!({
            "commitment": "confirmed",
            "encoding": "base64",
            "limit": 500,
            "paginationKey": "11111111111111111111111111111111",
            "filters": [{"dataSize": 165}]
        }))
        .unwrap();

        assert_eq!(config.limit, Some(500));
        assert_eq!(
            config.pagination_key.as_deref(),
            Some("11111111111111111111111111111111")
        );
        assert_eq!(config.program_config.filters.unwrap().len(), 1);
    }

    #[test]
    fn limit_validation_matches_public_contract() {
        assert_eq!(validate_limit(None).unwrap(), 1_000);
        assert_eq!(validate_limit(Some(1)).unwrap(), 1);
        assert_eq!(validate_limit(Some(10_000)).unwrap(), 10_000);
        assert!(validate_limit(Some(0)).is_err());
        assert!(validate_limit(Some(10_001)).is_err());
    }

    #[test]
    fn filtered_pages_scan_a_bounded_window() {
        assert_eq!(scan_limit(1_000, false), 1_001);
        assert_eq!(scan_limit(1_000, true), 50_000);
        assert_eq!(scan_limit(10_000, true), 100_000);
    }

    #[test]
    fn pagination_key_is_a_pubkey() {
        assert!(decode_pagination_key(Some("11111111111111111111111111111111")).is_ok());
        assert!(decode_pagination_key(Some("not-a-pubkey")).is_err());
    }

    #[test]
    fn response_uses_camel_case_pagination_key() {
        let value = serde_json::to_value(PaginatedProgramAccounts {
            accounts: vec![],
            pagination_key: Some("11111111111111111111111111111111".to_string()),
        })
        .unwrap();

        assert_eq!(
            value,
            serde_json::json!({
                "accounts": [],
                "paginationKey": "11111111111111111111111111111111"
            })
        );
    }

    #[test]
    fn generated_sql_keyset_scans_before_filtering() {
        let program = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
            .parse::<Pubkey>()
            .unwrap();
        let sql = load_sql(
            program,
            42,
            None,
            50_000,
            1_001,
            "AND latest.token_mint = '\\x01'::bytea",
        );

        assert!(sql.contains("pubkey > '\\x'::bytea"));
        assert!(sql.contains("LIMIT 50000"));
        assert!(sql.contains("LIMIT 1001"));
        assert!(sql.contains("AND latest.token_mint = '\\x01'::bytea"));
        assert!(!sql.contains("$1"));
    }
}
