-- SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

-- getMultipleAccounts (raw / non-jsonParsed variant).
--
-- $1 = bytea[] input pubkeys (order doesn't matter)
-- $2 = bigint bound on slot derived from the requested commitment

WITH all_account_versions AS NOT MATERIALIZED (
    SELECT
        accounts.pubkey,
        accounts.owner,
        accounts.lamports,
        accounts.slot,
        accounts.executable,
        accounts.rent_epoch,
        accounts.data,
        accounts.write_version
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
        snapshot_accounts.write_version
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
        write_version
    FROM all_account_versions
    ORDER BY pubkey ASC, slot DESC
)

SELECT
    pubkey,
    owner,
    lamports,
    slot,
    executable,
    rent_epoch,
    data,
    write_version,
    TRUE AS present
FROM latest_account
WHERE lamports > 0;
