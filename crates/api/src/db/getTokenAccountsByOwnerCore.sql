-- SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

-- Direct jsonParsed getTokenAccountsByOwner account scan.
-- Mint metadata is intentionally fetched in one account_lookup-backed batch by
-- the handler instead of one LATERAL lookup per distinct mint.
WITH program_accounts AS (
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
)

SELECT
    latest.pubkey,
    latest.owner,
    latest.lamports,
    latest.slot,
    latest.executable,
    latest.rent_epoch,
    latest.data,
    latest.token_mint
FROM (
    SELECT DISTINCT ON (program_accounts.pubkey)
        program_accounts.pubkey,
        program_accounts.owner,
        program_accounts.lamports,
        program_accounts.slot,
        program_accounts.executable,
        program_accounts.rent_epoch,
        program_accounts.data,
        program_accounts.token_mint
    FROM program_accounts
    ORDER BY program_accounts.pubkey, program_accounts.slot DESC
) AS latest
WHERE latest.lamports > 0;
