-- SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

-- getMultipleAccounts (jsonParsed variant). Same set-based latest-account
-- selection as getMultipleAccounts.sql, plus a deduplicated mint LEFT JOIN so
-- we look up each distinct mint exactly once.
--
-- $1 = bytea[] input pubkeys (order doesn't matter)
-- $2 = bigint bound on slot from commitment

WITH all_account_versions AS NOT MATERIALIZED (
    SELECT
        accounts.pubkey,
        accounts.owner,
        accounts.lamports,
        accounts.slot,
        accounts.executable,
        accounts.rent_epoch,
        accounts.data,
        accounts.token_mint
    FROM accounts
    WHERE
        accounts.pubkey = ANY($1::bytea[])
        AND accounts.slot <= $2::bigint
    UNION ALL
    SELECT
        snapshot_accounts.pubkey,
        snapshot_accounts.owner,
        snapshot_accounts.lamports,
        snapshot_accounts.slot,
        snapshot_accounts.executable,
        snapshot_accounts.rent_epoch,
        snapshot_accounts.data,
        snapshot_accounts.token_mint
    FROM snapshot_accounts
    WHERE
        snapshot_accounts.pubkey = ANY($1::bytea[])
        AND snapshot_accounts.slot <= $2::bigint
),

latest_account AS (
    SELECT DISTINCT ON (pubkey)
        pubkey,
        owner,
        lamports,
        slot,
        executable,
        rent_epoch,
        data,
        token_mint
    FROM all_account_versions
    ORDER BY pubkey ASC, slot DESC
),

-- Distinct mints needed for jsonParsed enrichment only for live token accounts.
needed_mints AS (
    SELECT DISTINCT token_mint AS mint_pubkey
    FROM latest_account
    WHERE
        lamports > 0
        AND token_mint IS NOT NULL
        AND (
            owner = '\x06ddf6e1d765a193d9cbe146ceeb79ac1cb485ed5f5b37913a8cf5857eff00a9'::bytea -- TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA -- noqa: LT05
            OR owner = '\x06ddf6e1ee758fde18425dbce46ccddab61afc4d83b90d27febdf928d8a18bfc'::bytea -- TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb -- noqa: LT05
        )
),

all_mint_versions AS NOT MATERIALIZED (
    SELECT
        accounts.pubkey,
        accounts.data,
        accounts.slot,
        accounts.lamports
    FROM accounts
    INNER JOIN needed_mints ON accounts.pubkey = needed_mints.mint_pubkey
    WHERE accounts.slot <= $2::bigint
    UNION ALL
    SELECT
        snapshot_accounts.pubkey,
        snapshot_accounts.data,
        snapshot_accounts.slot,
        snapshot_accounts.lamports
    FROM snapshot_accounts
    INNER JOIN needed_mints
        ON snapshot_accounts.pubkey = needed_mints.mint_pubkey
    WHERE snapshot_accounts.slot <= $2::bigint
),

mint AS (
    -- Latest version per mint, then drop closed ones
    SELECT
        pubkey,
        mint_data
    FROM (
        SELECT DISTINCT ON (pubkey)
            pubkey,
            data AS mint_data,
            lamports
        FROM all_mint_versions
        ORDER BY pubkey ASC, slot DESC
    ) AS latest_per_pubkey
    WHERE lamports > 0
)

SELECT
    latest_account.pubkey,
    latest_account.owner,
    latest_account.lamports,
    latest_account.slot,
    latest_account.executable,
    latest_account.rent_epoch,
    latest_account.data,
    mint.mint_data
FROM latest_account
LEFT JOIN mint ON latest_account.token_mint = mint.pubkey
WHERE latest_account.lamports > 0;
