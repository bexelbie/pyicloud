# photos sync: support incremental (delta) sync using /changes/zone API

## Problem

`photos sync` currently performs a full library enumeration on every run. For large libraries (50K–100K+ assets), this takes minutes even when nothing has changed, and makes frequent scheduled syncs impractical.

The underlying transport layer (`CloudKitContainerClient.iter_changes()`) already supports Apple's `/changes/zone` endpoint which returns only assets modified since a given sync token, but the sync pipeline doesn't use it — `_iter_sync_assets()` always does a full query.

## Proposed solution

Use the existing `iter_changes()` transport to build an incremental sync path:

1. After a successful full sync, persist the sync token in the state DB
2. On subsequent runs, call `/changes/zone` with the stored token — process only the changed assets
3. Advance the token only after a fully successful cycle (no partial advances on failure)
4. Fall back to full enumeration transparently when delta sync fails (invalid token, zone errors, etc.)

User-facing controls:
- `--sync-mode auto|full|incremental` (default: `auto`)
- `--full-scan` one-time override flag

## Key design decisions

- **Cross-page record buffering**: `/changes/zone` returns records in change-log order, not paired order. CPLMaster and CPLAsset for the same photo can arrive on different pages. A buffer accumulates unpaired records and emits events as pairs complete.
- **Token safety**: Token only advances on complete success. Invalid tokens (BAD_REQUEST) trigger automatic fallback. Config changes (different `--album`, `--recent`, etc.) produce separate state files so stale tokens can't skip needed assets.
- **No secondary fetches**: `/changes/zone` returns full CKRecord objects with all fields (download URLs, sizes, etc.) — same as full enumeration. No additional API calls needed.

## Evidence

This approach is proven by [kei](https://github.com/rhoopr/kei) (Rust iCloud Photos client) which uses the same API pattern with cross-page buffering and token safety rules.

I have a working implementation on [`bexelbie/pyicloud#feature/delta-sync`](https://github.com/bexelbie/pyicloud/tree/feature/delta-sync) tested against a real ~95K asset library — delta runs complete in 2–4 seconds vs 40+ seconds for full enumeration when no changes exist, and correctly detect and download newly added assets.
