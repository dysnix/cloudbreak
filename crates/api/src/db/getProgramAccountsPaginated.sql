-- SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

-- Pagination is intentionally driven by the existing (pubkey, slot DESC)
-- indexes instead of a filter-specific index.  Each request scans a bounded
-- pubkey window, resolves the latest account version in that window, and only
-- then applies the GPA filters.  This avoids sorting the complete matching set
-- before returning the first page.
WITH
live_keys AS MATERIALIZED (
    SELECT DISTINCT ON (pubkey)
        pubkey
    FROM accounts
    WHERE
        owner = $1
        AND slot <= $2
        AND pubkey > $3
    ORDER BY pubkey ASC, slot DESC
    LIMIT $4
),
snapshot_keys AS MATERIALIZED (
    SELECT DISTINCT ON (pubkey)
        pubkey
    FROM snapshot_accounts
    WHERE
        owner = $1
        AND slot <= $2
        AND pubkey > $3
    ORDER BY pubkey ASC, slot DESC
    LIMIT $4
),
candidate_keys AS MATERIALIZED (
    SELECT pubkey
    FROM (
        SELECT pubkey FROM live_keys
        UNION ALL
        SELECT pubkey FROM snapshot_keys
    ) AS keys
    GROUP BY pubkey
    ORDER BY pubkey ASC
    LIMIT $4
),
matching AS MATERIALIZED (
    SELECT
        candidate_keys.pubkey,
        latest.owner,
        latest.lamports,
        latest.slot,
        latest.executable,
        latest.rent_epoch,
        latest.data,
        latest.token_mint
    FROM candidate_keys
    CROSS JOIN LATERAL (
        SELECT
            versions.owner,
            versions.lamports,
            versions.slot,
            versions.executable,
            versions.rent_epoch,
            versions.data,
            versions.token_mint,
            versions.token_owner
        FROM (
            SELECT
                owner,
                lamports,
                slot,
                executable,
                rent_epoch,
                data,
                token_mint,
                token_owner
            FROM accounts
            WHERE
                owner = $1
                AND pubkey = candidate_keys.pubkey
                AND slot <= $2
            UNION ALL
            SELECT
                owner,
                lamports,
                slot,
                executable,
                rent_epoch,
                data,
                token_mint,
                token_owner
            FROM snapshot_accounts
            WHERE
                owner = $1
                AND pubkey = candidate_keys.pubkey
                AND slot <= $2
        ) AS versions
        ORDER BY versions.slot DESC
        LIMIT 1
    ) AS latest
    WHERE
        latest.lamports > 0
        -- {filters}
    ORDER BY candidate_keys.pubkey ASC
    LIMIT $5
)

SELECT
    matching.pubkey,
    matching.owner,
    matching.lamports,
    matching.slot,
    matching.executable,
    matching.rent_epoch,
    matching.data,
    matching.token_mint,
    FALSE AS is_metadata,
    NULL::bytea AS scan_end,
    NULL::bigint AS candidate_count
FROM matching

UNION ALL

SELECT
    NULL::bytea AS pubkey,
    NULL::bytea AS owner,
    NULL::bigint AS lamports,
    NULL::bigint AS slot,
    NULL::boolean AS executable,
    NULL::numeric AS rent_epoch,
    NULL::bytea AS data,
    NULL::bytea AS token_mint,
    TRUE AS is_metadata,
    (SELECT pubkey FROM candidate_keys ORDER BY pubkey DESC LIMIT 1) AS scan_end,
    COUNT(*)::bigint AS candidate_count
FROM candidate_keys

ORDER BY is_metadata ASC, pubkey ASC NULLS LAST;
