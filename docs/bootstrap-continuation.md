# Durable snapshot bootstrap continuation

Cloudbreak persists snapshot bootstrap control state in PostgreSQL and keeps immutable snapshot
archives plus unprocessed extracted AppendVec files in the configured snapshot working directory.
An indexer restart resumes the newest active run and never switches that run to a newer tracker
pair.

## Recovery boundaries

- Archive downloads use a `.part` file followed by an atomic rename. A complete archive is reused
  when its recorded size matches.
- The extracted account-file manifest is stored before ingestion. Each AppendVec is read through a
  temporary hard link, so `AccountsFile` can remove its working file without removing the reusable
  extracted source.
- Snapshot-account inserts are idempotent. An account file's temporary version rows and completion
  checkpoint commit in one PostgreSQL transaction; the extracted source is deleted only afterward.
- Clustering, deduplication preparation, duplicate cleanup, closed-account cleanup, and each index
  have durable completion checkpoints. Catalog checks repair a missing or invalid checkpointed
  index.
- Pubkeys updated or closed by Yellowstone during bootstrap are persisted and removed from
  `snapshot_accounts` before the run becomes `ready`.

If pending extracted files are missing or have the wrong recorded size, Cloudbreak re-extracts the
same persisted archive pair through a staging tree. HTTP 404 or 410 for an exact archive marks the
run abandoned and starts a new run from the current tracker pair. A locally invalid archive is
removed and downloaded again; a second validation failure abandons the exact pair instead of
crash-looping forever.

The account tables and ingestion work tables remain `UNLOGGED`. If PostgreSQL crash recovery
truncates them while durable file checkpoints exist, Cloudbreak abandons that incompatible run and
starts cleanly. A clean PostgreSQL restart retains those relations and resumes normally.

## Startup and readiness

On the first upgrade, a database with snapshot rows, confirmed/finalized chain tips, and every
configured snapshot index is recorded as an adopted `ready` run. Later restarts skip snapshot
loading and seed Yellowstone `from_slot` from the persisted confirmed tip. The existing
out-of-range fallback remains active when the provider no longer retains that replay slot.

API and indexer readiness stay false until the durable run reaches `ready`, including completion of
live-update reconciliation.

## Metrics

- `cloudbreak_bootstrap_phase{run_id,phase,source}`
- `cloudbreak_bootstrap_account_files_total{archive_type}`
- `cloudbreak_bootstrap_account_files_completed{archive_type}`
- `cloudbreak_bootstrap_account_files_skipped{archive_type}`
- `cloudbreak_bootstrap_resumed_runs_total`
- `cloudbreak_bootstrap_discarded_runs_total{reason}`
- `cloudbreak_bootstrap_pending_live_update_reconciliations`
- `cloudbreak_bootstrap_postprocessing_items{phase,state}`

The `phase` series has exactly one value of `1` in a process. Post-processing `state` is `total` or
`completed`.
