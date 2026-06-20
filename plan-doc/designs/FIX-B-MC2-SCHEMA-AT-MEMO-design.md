# FIX-B-MC2 — time-travel schema-at-snapshot second-level memo (NO PERF REGRESSION)

> Source: `task-list-P6-deviation-fixes.md` §B-MC2 + `reviews/P6-paimon-fullpath-cleanroom-2026-06-18.md` §MC2.
> Single-task loop: design → **design red-team (DONE, `wf_903bf4e9-3a4`)** → implement → impl-verify → build+UT.
> Hard constraint: **NO performance regression** — the no-regression argument below MUST hold and WAS
> red-team-verified. **This doc reflects the post-red-team revisions** (see §Red-team adjudication at the end).

## Problem

Time-travel reads (`FOR VERSION/TIME AS OF`, `@tag`, `@branch`) resolve the schema AS OF the pinned
schemaId through `PaimonConnectorMetadata.getTableSchema(session, handle, snapshot)` →
`catalogOps.schemaAt(table, snapshot.getSchemaId())` (`PaimonConnectorMetadata.java:221-222`). That
`schemaAt` is a paimon `schemaManager().schema(schemaId)` read (a schema-file round-trip,
`PaimonCatalogOps.java:321-327`). It runs on **every query** and the result is pinned only to the
per-statement `PluginDrivenMvccSnapshot` (`PluginDrivenMvccExternalTable.java:260-262`). Repeated
time-travel to the same snapshot re-reads the schema file each time.

Legacy served this from the shared catalog-level `PaimonExternalMetaCache` keyed by
`(NameMapping, schemaId)` — a repeat time-travel to the same schemaId was a cache **hit**. The SPI
cutover (review tag CACHE-P1) dropped that second-level cache; only the **latest** schema stays cached
(via the bridge's generic schema cache — see "Latest path untouched" below).

Severity: **NIT** (CACHE-P1). Diagnostic/perf only, no correctness impact — but the user elected to fix
it (restore the legacy hit) rather than accept-as-deviation.

## Root cause

`PaimonConnector.getMetadata(session)` returns a **fresh** `new PaimonConnectorMetadata(...)` on every
call (`PaimonConnector.java:94-97`), and fe-core calls `getMetadata(session)` **once per query**
(`PluginDrivenMvccExternalTable.java:122,218` and every other call site). So the metadata object is a
per-query throwaway: nothing on it persists the at-snapshot schema across queries. The legacy cache lived
on the long-lived catalog-level metacache; the cutover has no equivalent on the time-travel path.

**Consequence for the fix's home (critical):** a memo stored as an *instance field of
`PaimonConnectorMetadata`* would give **zero** cross-query benefit (it would die with the per-query
metadata object after its single `schemaAt`). The memo's **storage must live on the long-lived
`PaimonConnector`** (one per catalog), and be *injected into* the per-query metadata so the at-snapshot
resolve can consult/populate it. (`PaimonTableResolver` is a stateless static utility — `final class`,
private ctor — so it is NOT a home for cross-query state.)

## Design

**Connector-side, immutable, bounded memo of the `schemaAt` read. Bridge UNCHANGED. SPI UNCHANGED.**

### What is memoized (post-red-team): the raw `PaimonSchemaSnapshot`, NOT the built `ConnectorTableSchema`

The red-team (MAJOR, REAL) showed the built `ConnectorTableSchema` is **not** a pure function of the
schemaId key: `buildTableSchema` sources its `properties` from the **live** table —
`schemaProps.putAll(((DataTable) table).coreOptions().toMap())` (`PaimonConnectorMetadata.java:251-252`),
where `table = resolveTable(handle)` is the LATEST table. Caching the built schema would freeze the live
`coreOptions` under a schemaId key and could serve stale PROPERTIES after an external `ALTER…SET OPTION`
without REFRESH (D-046 SHOW CREATE TABLE PROPERTIES channel) — a violation of "never return stale data
than today".

So we memoize the **`PaimonCatalogOps.PaimonSchemaSnapshot`** (fields + partitionKeys + primaryKeys —
the exact output of the `schemaAt` schema-file read), which **IS** a pure function of
`(table-identity, schemaId)` (a committed schemaId's schema content is write-once). `ConnectorTableSchema`
is rebuilt fresh per query from the live `resolveTable` table, so `coreOptions`/`properties` stay LIVE =
byte-identical to today. The ONLY behavioral delta vs today is: **the `schemaAt` schema-file read is
skipped on a repeat**. `resolveTable` and `buildTableSchema` still run every query (unchanged).

### Components

1. **New tiny class `PaimonSchemaAtMemo`** (package-private, in `fe-connector-paimon`):
   - Storage = plain **`ConcurrentHashMap<MemoKey, PaimonCatalogOps.PaimonSchemaSnapshot>`** (matches the
     module convention — the only long-lived caches in fe-connector-paimon are plain `ConcurrentHashMap`,
     `PaimonConnector.java:81-82`; lock-free reads; no new dependency).
   - **`MemoKey`** = immutable holder of the handle's **extracted identity** `(databaseName, tableName,
     sysTableName, branchName, schemaId)` built in `new MemoKey(PaimonTableHandle handle, long schemaId)`.
     It mirrors `PaimonTableHandle.equals/hashCode`, which key on exactly
     `(databaseName, tableName, sysTableName, branchName)` (`PaimonTableHandle.java:233-240`, `scanOptions`
     correctly excluded from identity). **Extracted fields, NOT a retained handle reference** — deliberate:
     a `PaimonTableHandle` carries its loaded paimon `Table` (`setPaimonTable`), so retaining the handle as
     a map key would pin that `Table` (catalog/schema/IO refs) in the cache for its lifetime. Extracting the
     four `String`/`long` identity fields avoids that memory pin at the cost of a documented sync-point with
     the handle's identity (a comment in `MemoKey` points to `PaimonTableHandle:233-240`). This is a
     deliberate deviation from the red-team's "delegate to handle.equals" suggestion, which did not account
     for `Table`-pinning. Branch is load-bearing (same schemaId on different branches = different schema;
     `branchName` is live on the pinned handle via `applySnapshot→withBranch`). `sysName` is a forward-compat
     guard (sys tables don't reach this path today, but a key collision would be a correctness bug).
   - **Bound (best-effort, honors task-list "bounded (maxSize)"):** before a `put`, if `size() >= MAX`,
     `clear()` the map. Crude but correct: values are immutable, so a flush only causes re-reads
     (= today), never a stale/wrong value. The keyspace is `(table, branch, schemaId)` — naturally tiny —
     so this safety valve effectively never fires; `MAX` is a generous constant (proposed `10000`). The
     map is also freed wholesale on REFRESH (connector rebuild).
   - `PaimonSchemaSnapshot getOrLoad(PaimonTableHandle handle, long schemaId, Supplier<PaimonSchemaSnapshot> loader)`:
     ```
     MemoKey k = new MemoKey(handle, schemaId);
     PaimonSchemaSnapshot hit = cache.get(k);          // lock-free
     if (hit != null) return hit;
     PaimonSchemaSnapshot loaded = loader.get();        // the schemaAt I/O — OUTSIDE any lock
     if (cache.size() >= MAX) {                          // best-effort bound
         cache.clear();
     }
     PaimonSchemaSnapshot prev = cache.putIfAbsent(k, loaded);
     return prev != null ? prev : loaded;               // canonicalize on a concurrent race
     ```
     The loader runs without any lock (no I/O-under-lock, no `computeIfAbsent`). A concurrent same-key
     miss may double-load (rare) — harmless: the value is immutable and identical (same schemaId), and a
     double-load equals today's two independent per-query loads, never worse. A loader exception
     propagates BEFORE any `put`, so failures are never negative-cached.

2. **`PaimonConnector`**: add `private final PaimonSchemaAtMemo schemaAtMemo = new PaimonSchemaAtMemo(MAX);`
   and pass it into the metadata: `new PaimonConnectorMetadata(catalogOps, properties, context, schemaAtMemo)`.
   The connector is one-per-catalog and set to `null` on `onClose()`
   (`PluginDrivenExternalCatalog.java:557`), which REFRESH CATALOG triggers via
   `resetToUninitialized()→onClose()`; the next access rebuilds the connector → **fresh empty memo**. So
   REFRESH is the (only needed) invalidation.

3. **`PaimonConnectorMetadata`**:
   - New **package-private** 4-arg ctor `(catalogOps, properties, context, schemaAtMemo)` storing the memo.
     Package-private (not public) keeps the connector surface minimal (Rule 3); the cross-query-hit test
     lives in the same package `org.apache.doris.connector.paimon` and constructs both metadata instances
     through it with a shared `PaimonSchemaAtMemo`.
   - Keep the existing **public** 3-arg ctor delegating to the 4-arg with a **fresh per-instance**
     `new PaimonSchemaAtMemo(MAX)`. This keeps all ~15 existing test construction sites compiling
     unchanged; their single-resolve behavior is identical (first call is always a miss → load).
   - In `getTableSchema(session, handle, snapshot)` (the at-snapshot overload), the **only** change is the
     `schemaId >= 0` branch (the `< 0` latest fallback at line 216-217 is untouched). **`resolveTable` is
     called ONCE, outside the loader**, so the branch-handle `getTable` reload happens at most once per
     query = today:
     ```
     PaimonTableHandle paimonHandle = (PaimonTableHandle) handle;
     long schemaId = snapshot.getSchemaId();
     Table table = resolveTable(paimonHandle);                       // once — keeps branch getTable == today
     PaimonCatalogOps.PaimonSchemaSnapshot schema =
             schemaAtMemo.getOrLoad(paimonHandle, schemaId, () -> catalogOps.schemaAt(table, schemaId));
     return buildTableSchema(paimonHandle.getTableName(), table,
             schema.fields(), schema.partitionKeys(), schema.primaryKeys());
     ```

## NO-regression argument (must hold + WAS red-team-verified)

1. **The memoized value is a pure function of the key → no stale read, no TTL.** We cache only
   `PaimonSchemaSnapshot` (fields/partitionKeys/primaryKeys of a *committed* schemaId), which is write-once
   in paimon (rollback/ALTER mint *new* ids; a re-pointed tag resolves a *new* schemaId at resolve-time →
   a different key, never a stale hit). The live-bound `coreOptions`/`properties` are NOT cached — they are
   rebuilt per query from the live table, so they stay current (the red-team MAJOR that killed the
   `ConnectorTableSchema`-memo). The only invalidation is REFRESH CATALOG (connector rebuild → fresh memo).
2. **Latest path untouched.** The memo is consulted **only** on the `schemaId >= 0` at-snapshot branch.
   `schemaId < 0` (latest / system tables) still delegates to `getTableSchema(session, handle)` (line
   216-217), cached cross-query by the bridge's generic schema cache (`getSchemaCacheValue →
   getLatestSchemaCacheValue → super`). The 2-arg latest `getTableSchema` (called from
   `PluginDrivenExternalTable:172`) is untouched — no double-caching.
3. **Worst case = current.** On a miss: `resolveTable` + `schemaAt` + `buildTableSchema` (= today) + an
   O(1) hash put. On a hit: `resolveTable` + `buildTableSchema` (= today) with the `schemaAt` read
   **skipped** (strictly faster, = legacy). On overflow/eviction or a concurrent double-load: a re-read =
   today. The memo NEVER does more work than today on any path.

## Implementation Plan

- New file `fe-connector-paimon/.../PaimonSchemaAtMemo.java` (ASF header, javadoc, `ConcurrentHashMap` +
  `MemoKey` + `getOrLoad` + `size()` test accessor).
- `PaimonConnector.java`: add the `schemaAtMemo` field; pass it in `getMetadata`.
- `PaimonConnectorMetadata.java`: package-private 4-arg ctor + public 3-arg delegate; memo-wrap the
  `schemaId>=0` branch of the 3-arg `getTableSchema` (resolveTable once, outside the loader).
- No SPI/bridge/BE change. `tools/check-connector-imports.sh` stays exit 0 (only `java.util.concurrent.*`
  + `java.util.function.Supplier` + existing connector/paimon imports added; verified the allowlist does
  not match these).

## Risk Analysis

- **Thread-safety:** `ConcurrentHashMap` (lock-free reads); loader runs outside any lock; immutable value
  → safe publication via the concurrent map. No iteration. The size-guard `clear()` is best-effort under
  concurrency (worst case a few extra re-reads — never a correctness issue).
- **Stale properties:** eliminated by memoizing `PaimonSchemaSnapshot` (not the live-bound built schema).
- **Key correctness:** delegates to `PaimonTableHandle.equals/hashCode` (db+table+sysName+branch) — no
  re-listed second identity site, no cross-branch/cross-sys collision. schemaId<0 never builds a key.
- **Ctor blast radius:** only the connector-internal ctor changes; the SPI `ConnectorMetadata` interface
  is untouched; the public 3-arg ctor is retained → no test/site churn.
- **Memory:** best-effort bounded by `MAX`; per-catalog; freed on REFRESH/close.
- **Checkstyle:** new file needs license header + class/field javadoc; runs in `validate` phase.

## Test Plan

### Unit Tests (`PaimonConnectorMetadataMvccTest` new tests, RED→GREEN verified by separate runs)

Drive via the recording seam; count underlying `schemaAt` reads through `ops.log` ("schemaAt:N"). Use a
**shared memo across two metadata instances** (each its own `RecordingPaimonCatalogOps`) to model two
queries (each query = a fresh `getMetadata` in production, sharing the connector-owned memo). The 4-arg
package-private ctor makes this construction compilable in-package.

1. **Cross-query hit (non-branch):** ops1 + ops2 share ONE `PaimonSchemaAtMemo`, both configured with the
   same `schemaAt`. Resolve the same `(handle, schemaId=2)` on metadata1 then metadata2. Assert ops1.log
   contains `schemaAt:2` exactly once and **ops2.log contains NO `schemaAt`** (the second resolve hit the
   memo — the primary RED→GREEN signal). Both results are value-equal. **MUTATION (RED):** remove the memo
   → ops2 also reads → ops2.log gains `schemaAt:2`.
2. **Different schemaId → reads again:** shared memo, resolve schemaId=2 then schemaId=3 → distinct keys →
   ops sees both `schemaAt:2` and `schemaAt:3`.
3. **Different branch, same schemaId → reads again (branch-in-key guard):** two-ops-shared-memo; ops1 =
   base handle, ops2 = `withBranch("b1")` handle with `ops2.branchTable` carrying `bid/bdt` (mirroring the
   existing branch test at `PaimonConnectorMetadataMvccTest.java:963-993`), both at schemaId=2. Assert
   BOTH: **(a)** the BRANCH ops2.log contains `schemaAt:2` (the branch resolve actually read — was NOT a
   cross-branch memo hit) AND **(b)** the branch-handle result columns equal `[bid,bdt]` and differ from
   the base-handle result columns `[id]` (the branch returned the branch schema, not a stale base value
   cached under a branch-blind key). **MUTATION (RED):** drop the branch component from the key → branch
   resolve hits → (a) and (b) both go RED.
4. **Latest path unaffected:** schemaId<0 resolve does not consult the memo (no `schemaAt`); the existing
   `getTableSchemaWithNegativeSchemaIdFallsBackToLatest` already pins this.
5. **Existing exact-equality at-snapshot + branch tests** keep passing unchanged (single resolve = miss =
   identical result; per-instance memo via the 3-arg ctor).

### Micro-tests (`PaimonSchemaAtMemoTest`)

- **schemaId-keyed dedup:** two `getOrLoad` calls for the same `(handle, schemaId)` with a counting
  `Supplier` → loader invoked ONCE.
- **sysName-distinguishing (Rule 9 guard for the sysName key component):** two handles equal in
  `(db, table, branch, schemaId)` but differing in `sysTableName` (one via
  `PaimonTableHandle.forSystemTable`, mirroring the test `sysHandle` helper) → two distinct loads (a
  sysName-blind key would yield one).
- **bound/eviction (honors "bounded"):** put `MAX+1` distinct keys, assert the map stays bounded and an
  evicted key re-loads on next access (proves eviction degrades to a re-read, never a stale value) —
  validates the no-regression "worst case = current" claim directly.

### E2E

Gated (`enablePaimonTest=false`) — **NOT run**; note as gated in the summary. The UT cross-query-hit test
is the authoritative proof; a live e2e would only observe a latency delta, not a correctness change.

## Files

- NEW `fe/fe-connector/fe-connector-paimon/.../PaimonSchemaAtMemo.java`
- `fe/fe-connector/fe-connector-paimon/.../PaimonConnector.java`
- `fe/fe-connector/fe-connector-paimon/.../PaimonConnectorMetadata.java`
- `fe/fe-connector/fe-connector-paimon/.../test/.../PaimonConnectorMetadataMvccTest.java` (new tests)
- NEW `fe/fe-connector/fe-connector-paimon/.../test/.../PaimonSchemaAtMemoTest.java`

## Red-team adjudication (`wf_903bf4e9-3a4`, 4 lenses → per-finding verify)

All four lensVerdicts judged the design **structurally sound** on lifecycle, no-regression, tests, and
scope. 6 actionable findings adopted (incorporated above):

- **MAJOR (REAL):** built-`ConnectorTableSchema` memo serves stale live `coreOptions` → **switched the
  memoized value to `PaimonSchemaSnapshot`** (pure function of the key); rebuild `ConnectorTableSchema`
  per query so options stay live.
- **MINOR (REAL):** hand-rolled 5-field `Key` duplicates `PaimonTableHandle` identity → **`MemoKey(handle,
  schemaId)` delegating to `handle.equals/hashCode`** (drift-proof, ~40 fewer lines).
- **NIT (PARTIAL):** LRU/synchronizedMap over-engineered vs module convention → **plain `ConcurrentHashMap`
  + best-effort clear-on-overflow bound**; NOT `computeIfAbsent` (loader I/O must stay off any lock).
- **MAJOR→test (PARTIAL):** branch Test 3 underspecified → **hardened to assert the branch actually read +
  columns differ**.
- **MINOR (REAL):** no test guards the `sysName` key component → **added the sysName-distinguishing
  micro-test** (kept sysName in the key).
- **NIT (PARTIAL):** 4-arg ctor visibility unspecified → **pinned package-private**.

Refuted/optional (no change): negative-caching of loader failures (pseudocode already correct);
`assertSame`-on-hit (the `ops.log` no-schemaAt assertion is the real discriminator, and `assertSame` would
wrongly couple to instance-memoization — incompatible with the `PaimonSchemaSnapshot` rebuild); Test-4
memo-not-touched extension (already optional/covered); import-rule safe (confirmed).
