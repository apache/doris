# FIX-B-R2-be — memoize the schema-evolution dict's per-schema reads — SUMMARY

> Deviation 5/5 (LAST) of the P6 deviation→fix batch. Single-task loop: design → design red-team
> (`wf_222e1abd-655`) → implement → impl-verify (agent `a00f6071f82920bda`) → build+UT (RED→GREEN) → commit.
> **User decision: Option A — memoize the reads, keep the eager superset emission.** Detail:
> `FIX-B-R2-BE-SCHEMA-DICT-MEMO-design.md`.

## Problem & why narrowing was rejected

`buildSchemaEvolutionParam` builds BE's `history_schema_info` dict by reading **every** committed schema
(`schemaManager.listAllIds()` + `schemaManager.schema(id).fields()`) on **every scan**, even when the
query's files touch one schema. Legacy added entries lazily per referenced file `schema_id`.

The task-list's "narrow to referenced ids" is **architecturally infeasible connector-only and BE-crash-prone**:
`getScanPlanProvider()` returns a NEW provider per call (so planScan's split schema_ids can't reach the dict
build); the dict is built lazily and often BEFORE splits are planned; the referenced set is per-scan; the
generic bridge can't collect `paimon.schema_id`; re-planning in props is forbidden (new I/O); and an
under-covering set hard-crashes BE (CI 969249). The user chose **memoization** instead.

## Fix (Option A)

Keep `listAllIds()` and the dict emission **byte-identical** (full superset → always covers any file's
schema_id → **zero BE-crash risk**); only the per-schema-id field READ is served from a connector-level
immutable memo. **Reuse the existing B-MC2 `PaimonSchemaAtMemo`** — it already caches exactly this fact
(`(handle, schemaId) → schema fields`, write-once, cleared on REFRESH):

- `PaimonScanPlanProvider`: new **package-private** 4-arg ctor `(props, catalogOps, context, schemaAtMemo)`;
  the public 2/3-arg ctors delegate with a **fresh** memo (so ~25 existing construction sites are
  behavior-identical — first build = direct read = pre-fix). `buildSchemaEvolutionParam` now takes the
  `PaimonTableHandle` and wraps the `listAllIds` loop read in `schemaAtMemo.getOrLoad(handle, schemaId, …)`;
  the loader keeps the **DIRECT** read (`schemaManager.schema(id)` → `PaimonSchemaSnapshot`, NOT
  `catalogOps.schemaAt`, so the real-table + fake-catalogOps tests are unaffected). The `-1` current entry
  is NOT memoized (live read).
- `PaimonConnector.getScanPlanProvider()`: injects the SAME per-catalog `schemaAtMemo` that `getMetadata`
  uses, so the dict reads are memoized across scans and shared with the B-MC2 time-travel path.

**Consistency (same key → same value across both features), validated by design red-team + impl-verify:**
the cached fact is the **write-once committed `schema-<id>` file**, identical regardless of which `Table`
instance's `schemaManager` reads it (B-MC2's unpinned `resolveTable` vs this fix's snapshot-pinned
`resolveScanTable`); `MemoKey` excludes `scanOptions` and mirrors `PaimonTableHandle` identity exactly; and
B-MC2 NEVER writes a `$ro`/sys key (sys handles short-circuit before the at-snapshot memo write). No
corruption case found.

## No-regression / safety

Emission unchanged → zero new BE-crash surface. Miss = today's read + O(1) put (the full snapshot adds no
I/O — `partitionKeys()/primaryKeys()` are O(1) on the already-read `TableSchema`); hit = the read is skipped;
overflow/concurrent-double-load = a re-read. Loader exceptions propagate uncached (fail-loud preserved).
Order-independent (unlike narrowing).

## Tests (+5 `PaimonScanPlanProviderTest`, RED→GREEN; red-team's 6 findings folded in)

- `schemaEvolutionDictPopulatesSharedMemo` (memo populated with K entries), `…ReadsFromMemoOnHit` (sentinel
  pre-seed surfaces in the dict → proves a cache HIT, the MAJOR test-gap fix), `…ByteIdenticalWithMemo`
  (memo path == non-memo baseline → emission unchanged), `…SkippedUnderForceJniLeavesMemoEmpty` (force-jni /
  `force_jni_scanner` gate off the dict + memo), `getScanPlanProviderInjectsSharedSchemaMemo` (connector
  injects the shared memo — pins the wiring so the fix can't silently no-op).
- **RED proof:** memo-bypass → the populate + sentinel-HIT tests fail; drop the `getScanPlanProvider`
  injection → the wiring test fails; the byte-identical + force-jni guards correctly stay green.

## Result

Full paimon module **303 tests, 0 failures, 1 skipped** (298 + 5), checkstyle 0, import-check 0, clean
rebuild BUILD SUCCESS. Connector-only (no fe-core / SPI / BE change). e2e gated (`enablePaimonTest=false`)
— NOT run. **This was the LAST of the 5 P6 deviation→fixes** (A3 / A2 / B-MC2 / A1 / B-R2-be all done).
