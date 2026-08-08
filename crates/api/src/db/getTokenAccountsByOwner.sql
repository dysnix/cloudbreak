-- SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

WITH
program_accounts AS (
    SELECT
        accounts.pubkey,
        accounts.owner,
        accounts.lamports,
        accounts.slot,
        accounts.executable,
        accounts.rent_epoch,
        accounts.data,
        accounts.token_mint,
        accounts.token_owner
    FROM accounts
    -- We could also directly query the `slots` table here in the subquery
    -- to get the slot for the commitment rather than using the 
    -- `slot_for_commitment` CTE(TODO!: test performance difference)
    WHERE
        accounts.owner = $1
        AND accounts.slot <= $2
    -- {accounts_filters}
    UNION ALL
    SELECT
        snapshot_accounts.pubkey,
        snapshot_accounts.owner,
        snapshot_accounts.lamports,
        snapshot_accounts.slot,
        snapshot_accounts.executable,
        snapshot_accounts.rent_epoch,
        snapshot_accounts.data,
        snapshot_accounts.token_mint,
        snapshot_accounts.token_owner
    FROM snapshot_accounts
    WHERE
        snapshot_accounts.owner = $1
        AND snapshot_accounts.slot <= $2
-- {snapshot_filters}
),

-- Select the latest version directly. Filtering closed accounts happens only
-- after this selection so an older live version can never reappear after a
-- newer tombstone.
deduplicated_program_accounts AS (
    SELECT
        latest.pubkey,
        latest.owner,
        latest.lamports,
        latest.slot,
        latest.executable,
        latest.rent_epoch,
        latest.data,
        latest.token_mint,
        latest.token_owner
    FROM (
        SELECT DISTINCT ON (program_accounts.pubkey)
            program_accounts.pubkey,
            program_accounts.owner,
            program_accounts.lamports,
            program_accounts.slot,
            program_accounts.executable,
            program_accounts.rent_epoch,
            program_accounts.data,
            program_accounts.token_mint,
            program_accounts.token_owner
        FROM program_accounts
        ORDER BY program_accounts.pubkey, program_accounts.slot DESC
    ) AS latest
    WHERE latest.lamports > 0
),

-- Get unique mints we need to look up
needed_mints AS (
    SELECT DISTINCT token_mint
    FROM deduplicated_program_accounts
    WHERE token_mint IS NOT NULL
),

-- Fetch each needed mint once. The former all_mint_versions CTE was consumed
-- twice (to find MAX(slot), then to fetch the row), which made PostgreSQL
-- vastly overestimate this part of the query and enable expensive JIT
-- compilation even for owners with only a few token accounts.
mints AS NOT MATERIALIZED (
    SELECT
        needed_mints.token_mint AS pubkey,
        latest_mint.mint_data
    FROM needed_mints
    LEFT JOIN LATERAL (
        SELECT mint_versions.data AS mint_data
        FROM (
            SELECT
                accounts.data,
                accounts.slot
            FROM accounts
            WHERE
                accounts.owner = $1
                AND accounts.pubkey = needed_mints.token_mint
                AND accounts.slot <= $2
            UNION ALL
            SELECT
                snapshot_accounts.data,
                snapshot_accounts.slot
            FROM snapshot_accounts
            WHERE
                snapshot_accounts.owner = $1
                AND snapshot_accounts.pubkey = needed_mints.token_mint
                AND snapshot_accounts.slot <= $2
        ) AS mint_versions
        ORDER BY mint_versions.slot DESC
        LIMIT 1
    ) AS latest_mint ON TRUE
)

SELECT
    deduplicated_program_accounts.pubkey,
    deduplicated_program_accounts.owner,
    deduplicated_program_accounts.lamports,
    deduplicated_program_accounts.slot,
    deduplicated_program_accounts.executable,
    deduplicated_program_accounts.rent_epoch,
    deduplicated_program_accounts.data,
    deduplicated_program_accounts.token_mint,
    mints.mint_data
FROM deduplicated_program_accounts
LEFT JOIN mints ON deduplicated_program_accounts.token_mint = mints.pubkey;
