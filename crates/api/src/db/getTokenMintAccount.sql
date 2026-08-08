-- SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

-- Fast path for a mint account whose owner must be one of the two token
-- programs. The explicit owner predicates prune every non-token partition.
-- $1 = mint pubkey (bytea)
-- $2 = bigint slot bound

WITH all_versions AS NOT MATERIALIZED (
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
        (accounts.owner = '\x06ddf6e1d765a193d9cbe146ceeb79ac1cb485ed5f5b37913a8cf5857eff00a9'::bytea -- noqa: LT05
         OR accounts.owner = '\x06ddf6e1ee758fde18425dbce46ccddab61afc4d83b90d27febdf928d8a18bfc'::bytea) -- noqa: LT05
        AND accounts.pubkey = $1
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
        (snapshot_accounts.owner = '\x06ddf6e1d765a193d9cbe146ceeb79ac1cb485ed5f5b37913a8cf5857eff00a9'::bytea -- noqa: LT05
         OR snapshot_accounts.owner = '\x06ddf6e1ee758fde18425dbce46ccddab61afc4d83b90d27febdf928d8a18bfc'::bytea) -- noqa: LT05
        AND snapshot_accounts.pubkey = $1
        AND snapshot_accounts.slot <= $2::bigint
),

latest_account AS (
    SELECT
        pubkey,
        owner,
        lamports,
        slot,
        executable,
        rent_epoch,
        data,
        write_version
    FROM all_versions
    ORDER BY slot DESC
    LIMIT 1
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
