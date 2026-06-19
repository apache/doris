# Task List — P6 deviation → code fixes (A1/A2/A3 + B-R2-be/B-MC2)

> Five P6-review findings the user elected to **fix** (rather than accept-as-deviation).
> Source: `reviews/P6-paimon-fullpath-cleanroom-2026-06-18.md` (§read findings, §cache findings) + the analysis
> in this conversation. The rest of P6-DEVIATIONS stays accept-as-deviation (see `task-list-P6-fixes.md`).
>
> **Process each ONE at a time** (AGENT-PLAYBOOK single-task loop): design → design red-team → implement →
> impl verify → build+UT → commit → summary → check off. Each is independent; suggested order below is by
> increasing blast radius. **B-items carry a hard constraint: NO performance regression** — the design MUST
> reproduce the no-regression argument recorded here and the red-team MUST verify it.
>
> Build/verify reminders (memory `doris-build-verify-gotchas`): paimon module needs
> `-am package -Dassembly.skipAssembly=true` (HiveConf in shade jar); checkstyle runs in `validate`;
> `tools/check-connector-imports.sh` must stay exit 0; e2e is gated (`enablePaimonTest=false`).

## Suggested order (independent; smallest blast radius first)

A3 → A2 → B-MC2 → A1 → B-R2-be   (✅ A3 / ✅ A2 / ✅ B-MC2 done; next = **A1**, then B-R2-be)

---

## A3 — JNI split `self_split_weight` omitted when weight is 0  (NIT / profile-parity)

- [x] **A3** — DONE. Gate changed from `selfSplitWeight > 0` to `paimonSplit != null` in
  `PaimonScanRange` ctor (emit-iff-JNI = legacy `PaimonScanNode.setPaimonParams:274` parity); new
  `PaimonScanRangeSelfSplitWeightTest` (3, RED→GREEN verified by separate runs); 283/0/0/1skip,
  checkstyle 0, import-check 0; design red-team 0-actionable, impl-verify APPROVE. See
  `designs/FIX-A3-SELF-SPLIT-WEIGHT-{design,summary}.md`.
- **Finding:** report §R1 (be). The connector gates `paimon.self_split_weight` emission on `selfSplitWeight > 0`,
  so a JNI split whose computed weight is exactly 0 (non-DataSplit sys split with `rowCount()==0`, or a DataSplit
  with total fileSize 0) leaves the prop unset → BE reads `-1` instead of `0`. Legacy emitted it unconditionally.
- **Impact:** profile-only — feeds only BE `_max_time_split_weight_counter`; never rows/counts/predicates/schema.
- **Fix:** drop the `> 0` gate so the weight (incl. 0) is always emitted; `PaimonScanRange.populateRangeParams:194-197`
  already only reads the prop on the JNI branch, so native splits are unaffected. (Find the `if (selfSplitWeight > 0)`
  guard at the prop-build site in `PaimonScanPlanProvider` / `PaimonScanRange` builder.)
- **Files:** `fe-connector-paimon/.../PaimonScanPlanProvider.java` (or the split-range builder that writes
  `paimon.self_split_weight`).
- **Test intent:** a JNI split with weight 0 emits `paimon.self_split_weight=0` (RED before: prop absent).

---

## A2 — EXPLAIN drops legacy `predicatesFromPaimon:` line  (MINOR / missing-port)

- [x] **A2** — DONE. `appendExplainInfo` deserializes the already-present `paimon.predicate` prop (the
  exact pushed `List<Predicate>`) and renders the legacy block between `paimonNativeReadSplits=` and the
  VERBOSE `PaimonSplitStats` (legacy order `PaimonScanNode:657-671`); chosen over re-converting (filter
  not in the seam, provider re-instantiated per call). 4 new `PaimonScanExplainTest` (RED→GREEN by
  separate runs: 3 fail unfixed → 0); 287/0/0/1skip, checkstyle 0, import-check 0; design red-team
  0-actionable, impl-verify APPROVE. See `designs/FIX-A2-PREDICATES-FROM-PAIMON-{design,summary}.md`.
- **Finding:** report §R2 (scan). Legacy `PaimonScanNode:660-668` listed the converted Paimon `Predicate` objects
  actually pushed to the SDK (or ` NONE`). The SPI `appendExplainInfo:1117` emits only generic `PREDICATES:` +
  `paimonNativeReadSplits=` + VERBOSE `PaimonSplitStats`, so a silently-dropped LTZ/FLOAT/CAST conjunct is no longer
  observable. Diagnostic-only; no correctness/perf impact; no regression test asserts the line.
- **Fix:** in the connector's `appendExplainInfo`, re-run `PaimonPredicateConverter` over the pushed filter and render
  a `predicatesFromPaimon:` block (or ` NONE`). Byte-parity with legacy `Predicate.toString()` is NOT reconstructible —
  aim for semantic equivalence. Keep it connector-side (do NOT add source-specific code to the generic node).
- **Files:** `fe-connector-paimon/.../PaimonScanPlanProvider.java` (`appendExplainInfo`), `PaimonPredicateConverter`.
- **Test intent:** EXPLAIN of a paimon scan with a pushable predicate shows `predicatesFromPaimon:` with the converted
  predicate; with only non-pushable (LTZ/FLOAT/CAST) it shows the dropped state. Pin via a connector UT on the
  rendered string (no live e2e needed).

---

## A1 — Plugin splits get uniform split weight (legacy = proportional)  (MINOR / regression)

- [ ] **A1**
- **Finding:** report §R1 (scan). `PluginDrivenSplit` ctor (`PluginDrivenSplit.java:39-48`) forwards
  path/start/length/fileSize/modTime/hosts/partitionValues to `FileSplit` but **never sets `selfSplitWeight` /
  `targetSplitSize`**, so `FileSplit.getSplitWeight()` hits the null branch → `SplitWeight.standard()` (uniform).
  Legacy `PaimonScanNode:499` set `targetSplitSize` on all splits, so `FederationBackendPolicy` distributed by
  proportional (fileSize-sum) weight. **FE-side BE-assignment only** — no rows/route/BE-read/result change.
- **Already computed (just not threaded to FE FileSplit):** connector `computeSplitWeight():885` (fileSize-sum or
  rowCount), `resolveTargetSplitSize():845`, `PaimonScanRange.getSelfSplitWeight():169`. These reach BE thrift (JNI
  `paimon.self_split_weight`) but not the FE `FileSplit` scheduling fields.
- **Fix:** add `getSelfSplitWeight()` / `getTargetSplitSize()` to the SPI `ConnectorScanRange` (interface in
  fe-connector-api), populate them from the connector's already-computed values, and set `FileSplit.selfSplitWeight` /
  `targetSplitSize` in the `PluginDrivenSplit` ctor. **Generic, connector-agnostic** (other connectors return their
  own weights / 0). Confirm the SPI default keeps non-weighting connectors at `SplitWeight.standard()` (no regression
  for them).
- **Files:** `fe-connector-api/.../ConnectorScanRange.java` (new getters + default), `fe-core/.../PluginDrivenSplit.java`
  (ctor), `fe-connector-paimon/.../PaimonScanRange.java` (wire the computed weight/targetSize through).
- **Test intent:** a `PluginDrivenSplit` built from a weighted `ConnectorScanRange` yields proportional
  `getSplitWeight()` (RED before: `standard()`); a connector that returns no weight stays `standard()`.
- **Note:** adjacent to A3 (both about split weight) but distinct — A1 is FE scheduling, A3 is BE profile. Can be
  done together or separately.

---

## B-MC2 — time-travel schema re-resolved per query (no second-level cache)  (NIT / CACHE-P1) — **NO PERF REGRESSION**

- [x] **B-MC2** — DONE `10284edbf88`. New connector-side `PaimonSchemaAtMemo`
  (`ConcurrentHashMap<MemoKey, PaimonSchemaSnapshot>`, loader-outside-lock + best-effort clear-on-overflow
  bound), owned by the long-lived per-catalog `PaimonConnector` (rebuilt→empty on REFRESH) and injected
  into each per-query metadata via a new **package-private 4-arg ctor** (public 3-arg delegates with a fresh
  per-instance memo → ~15 sites unchanged). `getTableSchema(snapshot)` schemaId>=0 branch: `resolveTable`
  ONCE outside the loader, memo wraps ONLY the `schemaAt` read, `ConnectorTableSchema` rebuilt fresh per
  query. **Design red-team MAJOR adopted:** memoize the raw `PaimonSchemaSnapshot` (pure function of the
  key), NOT the built `ConnectorTableSchema` (which embeds live `coreOptions` → stale-properties risk).
  `MemoKey` = extracted handle identity (db,table,sysName,branch)+schemaId (no `Table` pinning — a
  deviation from the red-team's "delegate to handle.equals" to avoid retaining the loaded paimon Table).
  +3 `PaimonConnectorMetadataMvccTest` + new `PaimonSchemaAtMemoTest` (3), each RED-verified by a separate
  mutation run (RED-1 memo-disabled / RED-2 key-drops-fields / RED-3 bound-disabled). 293/0/0/1skip,
  checkstyle 0, import-check 0; design red-team `wf_903bf4e9-3a4`, impl-verify `wf_67804f35-d5e`
  (2× COMMIT_AS_IS + 1× FIX_THEN_COMMIT = only the verifier's own scratch file; production code clean).
  e2e gated, NOT run. See `designs/FIX-B-MC2-SCHEMA-AT-MEMO-{design,summary}.md`.
- **Finding:** report §MC2. `PluginDrivenMvccExternalTable.loadSnapshot:259-262` resolves schema-at-snapshot via
  `metadata.getTableSchema(pinnedHandle, snapshot)` (a `schemaAt` round-trip) **every query** and pins it to the
  per-statement `PluginDrivenMvccSnapshot` only. Repeated time-travel to the same snapshot re-reads it; legacy served
  it from the shared `(NameMapping, schemaId)` cache (repeat = hit). Latest reads are unaffected (cached via super).
- **Fix (connector-side immutable memo — bridge UNCHANGED):** add a bounded
  `Map<(Identifier, schemaId) → ConnectorTableSchema>` memo inside the connector's schema-at-snapshot resolve
  (`PaimonConnectorMetadata.getTableSchema` for the pinned case / `PaimonTableResolver`). Check memo first; on miss do
  the `schemaAt` read and populate. Keyed by `snapshot.schemaId()`.
- **NO-regression argument (MUST hold + be red-team-verified):**
  1. **Immutable keys** — a committed paimon schemaId's schema never changes → no TTL/invalidation needed, no stale
     read. (Cleared only on REFRESH CATALOG = connector rebuild.)
  2. **Latest path untouched** — MC2 is the time-travel branch only; latest still flows
     `getSchemaCacheValue:361 → getLatestSchemaCacheValue → super` (generic cache). No change there.
  3. **Worst case = current** — bound the memo (maxSize); immutable values mean an evicted entry just re-reads
     (= today's behavior), never slower. Hit = faster (= legacy).
- **Files:** `fe-connector-paimon/.../PaimonConnectorMetadata.java` (or `PaimonTableResolver.java`).
- **Test intent:** two time-travel resolves at the same schemaId trigger ONE underlying `schemaAt` read (second is a
  memo hit); a different schemaId reads again; latest-path resolves are unaffected. Drive via the recording
  catalog-ops seam (count underlying reads).
- **Design note:** this locally re-introduces what CACHE-P1 dropped, but scoped to time-travel + immutable + bounded —
  the report explicitly sanctioned it ("reintroduce a schemaId-keyed memo without touching the bridge").

---

## B-R2-be — `history_schema_info` eager superset → narrow to referenced schema_ids  (NIT / intentional) — **NO PERF REGRESSION**

- [ ] **B-R2-be**
- **Finding:** report §R2 (be). `buildSchemaEvolutionParam:1214-1232` emits the `-1` (current, from requested columns —
  cheap, keep) PLUS **one entry per `schemaManager.listAllIds()`** — reading every committed schema file even for a
  single-schema query. Legacy added entries lazily, one per distinct file `schema_id` a split referenced, + `-1`.
- **Fix (narrow to referenced ids = legacy behavior):** build the historical entries only for the **distinct file
  `schema_id`s of the planned splits** (∪ `{-1}`), instead of `listAllIds()`. Those ids are already enumerated at
  `planScan:483` (`.schemaId(file.schemaId())`) — collect them into a `Set<Long>` (zero new I/O; files already in
  memory).
- **NO-regression argument (MUST hold + be red-team-verified):** let N = total committed schemas, K = distinct schema
  ids in the query's files. K ≤ N always (single-schema query K=1 vs N; all-schema query K=N = same as today). Reads
  are **always ≤ current, never more**. Collecting ids is a CPU set-op over already-loaded files. No new I/O.
- **CORRECTNESS guard (critical):** BE looks up each split by its emitted per-file `schema_id` and **fails loud**
  (`"miss table/file schema info"`, `table_schema_change_helper.h`) if absent. The narrowed set
  `{all planned files' file.schemaId()} ∪ {-1}` is **exactly** the set BE references (`:483` emits those) — neither
  under- nor over-covers. The `-1`/current entry (built from requested columns, not a schema read) stays as-is.
- **KEY implementation caveat = call order:** the dict is built in `getScanNodeProperties`
  (`PluginDrivenScanNode.getOrLoadPropertiesResult:1034`), but the schema_id set comes from `planScan`. The design
  MUST confirm the lifecycle order; if props are built before splits, **thread the planned-split schema_id set into the
  dict build, or defer the dict build until after `planScan`** — do NOT re-enumerate splits inside the props build
  (that would be a NEW I/O cost = a regression).
- **Optional complementary (also no-regression, order-independent):** memoize `schemaManager.schema(id)→fields` by
  schemaId in the connector (committed schemas immutable → no TTL) so the narrowed reads are also cached across scans
  (legacy did this via `PaimonExternalMetaCache`). Combine with the narrowing or ship separately.
- **Files:** `fe-connector-paimon/.../PaimonScanPlanProvider.java` (`buildSchemaEvolutionParam`, `planScan` to collect
  ids, the props-build threading); possibly the SPI/bridge if the schema_id set must cross from planScan to props.
- **Test intent:** a query touching files of only schema id X emits a dict with entries `{-1, X}` (NOT all committed
  ids); a query spanning ids {X,Y} emits `{-1, X, Y}`; every emitted per-split `schema_id` is present in the dict
  (BE-fail-loud guard). RED before: dict contains all `listAllIds()`.
