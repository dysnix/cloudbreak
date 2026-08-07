-- SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

SELECT
    pubkey,
    owner,
    lamports,
    account_slot AS slot,
    executable,
    rent_epoch,
    data,
    write_version,
    present
FROM account_lookup
WHERE
    pubkey = ANY($1::bytea[])
    AND commitment = $2::integer
    AND account_slot <= $3::bigint
    AND write_version >= 0;
