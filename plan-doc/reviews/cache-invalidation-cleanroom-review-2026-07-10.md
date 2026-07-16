# Cache-invalidation fixes — clean-room adversarial review (2026-07-10)

> Adversarial review of the three connector-cache invalidation fixes landed this round
> (`3b66982fedf` D1 fe-core hook, `7b8fed012be` D1b paimon memo, `982db925659` D2 per-partition keying).
> Run `wf_fe6ddef4-777`: 4 blind dimension readers (D1 / D1b / D2 / completeness) → each finding
> adversarially verified by 3 independent lenses (correctness / concurrency-or-parity / does-it-reproduce),
> confirmed at ≥2/3. Totals: **8 raised, 4 CONFIRMED (all 3/3), 2 weak (1/3), 2 dropped.**
>
> **Verdict: the fixes are net improvements but INCOMPLETE. 3 live gaps + 1 dormant race remain; must be
> fixed before this phase is called done.** None is a regression that makes things worse than pre-fix (before,
> nothing was invalidated anywhere); each is an incomplete realization of the stated fix.

## Confirmed findings + fix approach

### R1 — DROP/CREATE invalidation is coordinator-only; followers/observers stay stale (MEDIUM, LIVE)
`PluginDrivenExternalCatalog` invalidates the connector cache only on the FE that RUNS the DDL. Followers/
observers replay via `ExternalCatalog.replayDropTable`/`replayCreateTable`/`replayDropDb` (the PluginDriven
branch, metadataOps==null) which touch only the FE name cache — never `connector.invalidate*`. So a
follower that had queried paimon/iceberg `d.t` keeps its `latestSnapshotCache[(d,t)]` pinned to the dropped
table's snapshot until the 24h access-TTL.
- **Precedent (this is normally done):** `RefreshManager.replayRefreshTable → refreshTableInternal:254-257`
  DOES call `connector.invalidateTable` on replay; and `PluginDrivenExternalCatalog` already added a
  `replayTruncateTable` override that routes through `refreshTableInternal` for exactly this reason.
- **Fix:** override `replayDropTable`/`replayCreateTable`/`replayDropDb` in `PluginDrivenExternalCatalog`
  (or the shared replay seam) to also call `connector.invalidateTable`/`invalidateDb` after the base
  bookkeeping, mirroring `replayTruncateTable`. Needs the remote names on the replay path (resolve like the
  coordinator does, or persist them in the log).

### R2 — iceberg/paimon do not override `invalidateDb` → DROP DATABASE + REFRESH DATABASE are no-ops (MEDIUM, LIVE) [= confirmed #2 + #4, same root]
Only `HiveConnector` overrides `invalidateDb`; `IcebergConnector`/`PaimonConnector` inherit the SPI no-op
default (`Connector.java:324`). So:
- The new `dropDb → connector.invalidateDb(d)` hook (D1) is INERT for iceberg/paimon (the dropDb comment's
  "drops every table in this db" is false for them). `DROP DATABASE d FORCE` cascades table drops INSIDE the
  connector, bypassing per-table `invalidateTable`, so the cascaded tables' `latestSnapshotCache`/
  `PaimonSchemaAtMemo` entries survive.
- **Pre-existing (independent of this round):** `RefreshManager.refreshDbInternal:126 → invalidateDb` — so
  `REFRESH DATABASE d` on iceberg/paimon has ALWAYS been a silent no-op for their snapshot/schema caches.
- Also: hive's `forEachBuiltSibling(sibling.invalidateDb)` is a no-op against the iceberg/hudi siblings
  post-flip for the same reason.
- **Fix:** add `invalidateDb(db)` overrides to `IcebergConnector` (db-scoped removeIf over
  `latestSnapshotCache` keys whose namespace==db) and `PaimonConnector` (`latestSnapshotCache` +
  `PaimonSchemaAtMemo`, db-scoped). Requires a db-scoped invalidate on `IcebergLatestSnapshotCache` /
  `PaimonLatestSnapshotCache` / `PaimonSchemaAtMemo` (mirror the existing per-table `matches`). Fixes the
  D1 dropDb hook AND the pre-existing REFRESH DATABASE gap in one go.

### R3 — `getPartitions` raw `put` bypasses the invalidateGeneration guard (MEDIUM, DORMANT)
The rewritten `getPartitions` uses `partitionsCache.put(...)` directly (`CachingHmsClient.java:182`), which
has no generation check. The pre-commit code used `partitionsCache.get(key, loader)` → `getWithManualLoad`,
which captures `invalidateGeneration` before the load and drops the put if a `flush` raced it
(`MetaCacheEntry.java:240-256`). So a `REFRESH TABLE` (`flush`) racing an in-flight cold-cache fetch now
re-caches the pre-refresh partitions, which survive until TTL/next REFRESH. `getTable`/`listPartitionNames`/
`getTableColumnStatistics` still use the guarded path — only `getPartitions` lost it.
- Currently dormant (hive not flipped; `flush` not yet wired to REFRESH for a live hms catalog).
- **Fix (keep bulk RPC + per-partition keying + divergence-safety):** add a generation-guarded put to the
  connector copy of `MetaCacheEntry` — `long invalidationGeneration()` + `putIfNotInvalidatedSince(gen, key,
  value)` (put under the key's stripe lock, then `removeLoadedValue` if the generation moved). In
  `getPartitions`, capture the generation BEFORE the delegate RPC, then guard each per-partition put with it;
  keep adding the delegate results to the result list directly (preserves the misparse→never-drop safety).
  This is an ADDITIVE framework method (no behavior/byte change for paimon/iceberg; same Caffeine version).

### R4 — RENAME TABLE never invalidates the connector cache (weak 1/3, but strong reasoning; LIVE)
`renameTable → afterExternalRename` (`:~649/1007`) runs unregister+resetMetaCacheNames+constraint+editlog and
NEVER calls `connector.invalidate*`, and does NOT route through `refreshTableInternal` (unlike truncate /
column-ALTER). So an iceberg/paimon atomic table-swap (`RENAME t→t_arch; RENAME t_new→t`) leaves the stale
`latestSnapshotCache[(db,t)]` pinned → the recreated `t` reads the OLD snapshot. Same drop+recreate class
D1 targets; the design doc even cites Trino's `renameTable` self-invalidation as the parity model, then
FIX 1 hooks only dropTable/createTable/dropDb. **Fix:** invalidate the source (and target) name's connector
cache in the rename path (source on the coordinator + replay; and `replayRenameTable` for followers). Verify
whether `replaceTable`/CTAS-overwrite has the same gap.

## Weak / not-fixing
- **W1 (paimon memo TOCTOU, LOW, 1/3):** a lock-free invalidate-vs-in-flight-load window can memoize an
  OLD schema for a reused schemaId if a concurrent drop+recreate lands between the loader read and the
  `putIfAbsent`. NOT introduced/worsened by this round (inherent to every cache here incl. the Caffeine
  snapshot cache); the commit message's unconditional "immutability ⇒ never stale" is the only overclaim.
  No fix required; the 3 primary axes (CHM keySet removeIf safety, `matches` over/under-match + null-safety,
  remote-name identity parity) are all correct. If ever hardened: computeIfAbsent-style load-under-key or an
  invalidate-generation counter.

## Next-session task (fix before Phase 2)
Order: **R2** (fixes 2 confirmed + a pre-existing live bug; connector-local) → **R1** (fe-core replay) →
**R4** (rename; same class as R1) → **R3** (dormant; framework method). Each its own commit + tests + build
(paimon uses `install`, see HANDOFF). Re-run a targeted adversarial pass on R1/R2/R4 (live paths) after.
