-- SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

WITH all_versions AS NOT MATERIALIZED (
    SELECT pubkey, owner, lamports, slot, executable, rent_epoch, data, write_version
    FROM accounts
    WHERE pubkey = ANY($1::bytea[]) AND slot <= $2::bigint
    UNION ALL
    SELECT pubkey, owner, lamports, slot, executable, rent_epoch, data, write_version
    FROM snapshot_accounts
    WHERE pubkey = ANY($1::bytea[]) AND slot <= $2::bigint
),
latest AS (
    SELECT DISTINCT ON (pubkey)
        pubkey, owner, lamports, slot, executable, rent_epoch, data, write_version
    FROM all_versions
    ORDER BY pubkey, slot DESC, write_version DESC
)
INSERT INTO account_lookup (
    pubkey, commitment, present, owner, lamports, account_slot,
    executable, rent_epoch, data, write_version
)
SELECT
    pubkey, $3::integer, lamports > 0, owner, lamports, slot,
    executable, rent_epoch, data, write_version
FROM latest
ON CONFLICT (pubkey, commitment) DO UPDATE SET
    present = EXCLUDED.present,
    owner = EXCLUDED.owner,
    lamports = EXCLUDED.lamports,
    account_slot = EXCLUDED.account_slot,
    executable = EXCLUDED.executable,
    rent_epoch = EXCLUDED.rent_epoch,
    data = EXCLUDED.data,
    write_version = EXCLUDED.write_version,
    updated_on = CURRENT_TIMESTAMP
WHERE
    EXCLUDED.account_slot > account_lookup.account_slot
    OR (
        EXCLUDED.account_slot = account_lookup.account_slot
        AND EXCLUDED.write_version >= account_lookup.write_version
    );
