# FIX-B-MC2 — time-travel schema-at-snapshot second-level memo — SUMMARY

> Deviation 3/5 of the P6 deviation→fix batch. Single-task loop: design → design red-team (`wf_903bf4e9-3a4`)
> → implement → impl-verify (`wf_67804f35-d5e`) → build+UT (RED→GREEN) → commit. Design + adjudication detail
> in `FIX-B-MC2-SCHEMA-AT-MEMO-design.md`.

## Problem

Time-travel reads (`FOR VERSION/TIME AS OF`, `@tag`, `@branch`) resolved the schema AS OF the pinned
schemaId by calling `catalogOps.schemaAt(table, schemaId)` (a paimon `schemaManager().schema(id)`
schema-file read) on **every query**, pinning the result only to the per-statement
`PluginDrivenMvccSnapshot`. Repeated time-travel to the same snapshot re-read the schema file. Legacy
served it from the shared catalog-level `PaimonExternalMetaCache` keyed by `(NameMapping, schemaId)` (repeat
= hit); the SPI cutover (CACHE-P1) dropped that second-level cache. NIT / perf-only; the user elected to fix.

## Root cause

`PaimonConnector.getMetadata()` returns a **fresh** `PaimonConnectorMetadata` per query, so nothing on the
metadata persists the at-snapshot schema across queries. The legacy cache lived on the long-lived
catalog-level metacache; the cutover had no equivalent on the time-travel path.

## Fix

A connector-side, immutable, bounded second-level memo of the `schemaAt` read:

- **New `PaimonSchemaAtMemo`** (package-private): a plain `ConcurrentHashMap<MemoKey, PaimonSchemaSnapshot>`
  (module convention; lock-free reads) with a best-effort size bound (clear-on-overflow). `getOrLoad` does
  `get → (miss) loader.get() OUTSIDE any lock → putIfAbsent`; a concurrent same-key double-load is harmless
  (immutable identical value) and equals the pre-fix per-query load; a loader exception is never cached.
- **`MemoKey`** = the handle's extracted identity `(databaseName, tableName, sysTableName, branchName,
  schemaId)`, mirroring `PaimonTableHandle.equals/hashCode`. Stored as extracted fields (NOT a retained
  handle) so the memo does not pin the handle's loaded paimon `Table` in memory.
- **`PaimonConnector`** owns the memo (one per catalog, long-lived; rebuilt → empty on REFRESH CATALOG via
  `onClose` `connector=null`) and injects it into each per-query metadata via a new **package-private**
  4-arg ctor. The public 3-arg ctor delegates with a fresh per-instance memo, so the ~15 existing
  construction sites are unchanged.
- **`PaimonConnectorMetadata.getTableSchema(session, handle, snapshot)`** — the only changed path, and only
  its `schemaId >= 0` branch (the `< 0` latest fallback is untouched): `resolveTable` runs **once** (outside
  the loader, so a branch handle's `getTable` reload stays at most one per query = pre-fix), then
  `schemaAtMemo.getOrLoad(handle, schemaId, () -> catalogOps.schemaAt(table, schemaId))`, then
  `buildTableSchema` rebuilds the `ConnectorTableSchema` fresh from the live table.

**Key red-team correction (MAJOR):** the original design memoized the built `ConnectorTableSchema`, which
embeds the **live** `coreOptions()` — not keyed by schemaId → could serve stale PROPERTIES after an
external `ALTER…SET` without REFRESH. Switched to memoizing the raw `PaimonSchemaSnapshot` (a pure function
of `(table-identity, schemaId)` — the actual `schemaAt` I/O target); `ConnectorTableSchema` is rebuilt per
query so `coreOptions`/`properties` stay live. The single behavioral delta vs pre-fix is therefore "the
`schemaAt` read is skipped on a repeat"; everything else is byte-identical.

## No-regression (the hard constraint — red-team-verified)

1. The cached value is a pure function of the key → no stale read, no TTL; the only invalidation is REFRESH
   (connector rebuild → fresh memo). Live coreOptions are NOT cached.
2. The latest path (schemaId<0) never builds a key; the 2-arg latest schema stays cached by the bridge.
3. Worst case = pre-fix: miss = pre-fix load + O(1) put; hit = pre-fix minus the `schemaAt` read; overflow/
   concurrent-double-load = a re-read. The memo never does more work than before on any path.

## Tests (RED→GREEN verified by separate mutation runs)

- `PaimonConnectorMetadataMvccTest` (+3): `getTableSchemaAtSnapshotIsMemoizedAcrossQueries` (two metadata
  sharing one memo → second query reads NO `schemaAt`), `...MemoIsKeyedBySchemaId`, `...MemoIsKeyedByBranch`
  (asserts the branch actually read AND its columns differ from base).
- `PaimonSchemaAtMemoTest` (new, 3): `sameKeyLoadsOnce`, `sysTableNameDistinguishesKey` (Rule-9 guard for the
  sysName key component), `overflowEvictsAndReReadsNeverStale` (bound degrades to a re-read, never stale).
- **RED proof:** RED-1 (memo disabled) → both memo tests fail; RED-2 (key drops schemaId/branch/sys) → all
  3 key tests fail; RED-3 (bound disabled) → overflow test fails. Each control test stayed green.

## Result

Full paimon module: **293 tests, 0 failures, 0 errors, 1 skipped** (gated live test); checkstyle clean;
`tools/check-connector-imports.sh` exit 0; BUILD SUCCESS. No fe-core / SPI / BE change. **e2e gated
(`enablePaimonTest=false`) — NOT run.**

HEAD after commit: see git log. Next deviation: **A1** (plugin split proportional weight), then **B-R2-be**.
