# HMS Cutover — Write-Chain Decomposition (design §4.5)

**Date:** 2026-07-08 · **Branch:** `catalog-spi-11-hive` · **HEAD:** `75a5d25d746` (W5 done)
**Scope:** decompose the *remaining* pre-flip write-chain items of the HMS-catalog cutover into ordered,
independently-committable **dormant** sub-steps. Planning only — no edits in this doc.

All line numbers below were re-verified against HEAD `75a5d25d746`. Where the authoritative design doc
(`hms-cutover-retype-design-2026-07-07.md` §4.5) disagrees with HEAD, HEAD wins.

---

## 0. Scope and what is already DONE

### 0.1 The cutover model (context)
An HMS external catalog (Hive + Iceberg + Hudi tables) is migrating from fe-core's built-in implementation to a
plugin "connector". A flipped `hms` catalog is served by the **HIVE plugin as a GATEWAY**; iceberg-on-HMS tables
delegate to a sibling `IcebergConnector`. The **single flip line** is adding `"hms"` to
`CatalogFactory.SPI_READY_TYPES` (CatalogFactory.java:49-50 — today `{"jdbc","es","trino-connector","max_compute",
"paimon","iceberg"}`, **no `"hms"`**). That flip is **NOT part of this step**.

**"Dormant" is not one thing.** Three distinct risk classes appear below; the ordered plan (§2) keeps them separate:

- **Class A — strictly unreachable pre-flip.** New `switch` cases keyed on `getType()=="hms"`; no
  `PluginDrivenExternalCatalog` has `getType()=="hms"` until the flip, so the code is literally never entered.
  *(Items 5, 5b.)*
- **Class B — live code, value-identical.** Edits a live method but the resolved value / bytes are provably
  unchanged for every catalog. *(Item 4.)*
- **Class C — live-exercised, behavior-preserving via a no-op SPI default.** The new fe-core wiring runs **today**
  on already-flipped live connectors (es/jdbc/paimon/max_compute/iceberg), relying on a permissive/no-op SPI
  default to be inert. Not byte-unchanged; carries live-connector regression surface and **requires a
  live-connector no-regression test**, not merely a dormant hive unit test. *(Items 1, 2a.)*

### 0.2 The delegation spine — already landed and dormant
The read + write + procedure + DDL **delegation** spine is complete on HEAD and dormant:

- **Read** (scan) delegation via `HiveConnector.getScanPlanProvider(handle)` (per-handle gateway routing to the
  iceberg sibling for foreign handles).
- **Write** delegation: the plain-hive write path is already class-forked
  (`UnboundTableSinkCreator`/`BindSink`/`PhysicalPlanTranslator`); at the flip it falls through to the live
  `UnboundConnectorTableSink → PluginDrivenTableSink → PluginDrivenInsertExecutor` driven by the dormant
  `HiveWritePlanProvider`/`HiveConnectorTransaction`.
- **Procedure + write-admission per-handle** delegation (W1–W5, commits `d1df7df`/`5a48c`/`8aee2`/`75a5d`):
  `HiveConnectorMetadata.siblingMetadata(...)` forwards foreign (iceberg-on-HMS) handles to the sibling; hive
  handles get hive semantics. **This plumbing is present at HEAD** and is a prerequisite that item 2a leans on.

### 0.3 The five §4.5 write-chain items (this decomposition)
The §4.5 item set is **complete** and matches the design doc verbatim (`hms-cutover-retype-design-2026-07-07.md`:
78-83). No §4.5 item was missed. This doc covers all five, **plus a sixth (item 5b)** that verifying item 5
surfaces as a firm dormant sub-step (not the "soft follow-up" earlier recon framed it as):

| # | Item | Class | Remaining pre-flip work? |
|---|------|-------|--------------------------|
| 1 | Read-ACID query-finish commit wiring | C (live, no-op default) | **Yes** — new SPI + fe-core wiring |
| 2a | Reject explicit-partition-spec INSERT | C (live, no-op default) | **Yes** — new SPI + wiring + override |
| 2b | Reject LZO read-only | — | **No dormant work** — done in connector; only a flip-time delete |
| 3 | Delete `Env.hiveTransactionMgr` | — | **No** — flip/delete-time only |
| 4 | Relocate `BIND_BROKER_NAME` | B (value-identical) | **Yes** — constant refactor |
| 5 | `pluginCatalogTypeToEngine` `"hms"→ENGINE_HIVE` | A (unreachable) | **Yes** — one switch case |
| 5b | `PluginDrivenExternalTable` engine-display `"hms"` cases | A (unreachable) | **Yes** — two switch cases (merge into 5) |

---

## 1. Items in detail

### Item 1 — READ-ACID query-finish commit wiring — **dormant-committable-now (Class C)**

**HEAD state (unwired — CONFIRMED).**
- Plugin **opens** but never **releases**: `fe-connector-hive/.../HiveScanPlanProvider.java:175-177`
  `planAcidScan` does `HiveReadTransaction txn = new HiveReadTransaction(session.getQueryId(), session.getUser(),
  ...); readTxnManager.register(txn);`. `register()` (HiveReadTransactionManager.java:47-50) calls `txn.begin()`
  (opens the metastore txn) + `txnMap.put(txn.getQueryId(), txn)`.
- The matching release **exists but has zero callers**: `HiveReadTransactionManager.java:52-61`
  `public void deregister(String queryId){ HiveReadTransaction txn = txnMap.remove(queryId); if(txn!=null){ try
  { txn.commit(); } catch (RuntimeException e) { LOG.warn(...); } } }` — `commit()` → `hmsClient.commitTxn`,
  releasing the shared read lock. **Note:** commit failure is *logged and swallowed*, not propagated (verified
  HEAD :55-59) — the release is best-effort, **not** "fail-loud".
- The SPI has **no** release method: `ConnectorScanPlanProvider` (fe-connector-api) — no
  `releaseReadTransaction`. Default-no-op precedent already exists there (`classifyColumn`, `getDeleteFiles`).
- fe-core query-finish spine **already present and generic**: `QueryFinishCallbackRegistry` +
  `QeProcessor.registerQueryFinishCallback` (QeProcessor.java:46 / QeProcessorImpl.java:216-217) invoked inside
  `unregisterQuery` via `runAndClear(DebugUtil.printId(queryId))` (QeProcessorImpl.java:212). **Nothing on the
  scan side registers a callback today.** No new fe-core lifecycle infra is needed.
- Legacy model to mirror: `HiveScanNode.java:138-149`
  `Env.getCurrentHiveTransactionMgr().register(hiveTransaction); ... QeProcessorImpl.INSTANCE
  .registerQueryFinishCallback(txnQueryId, () -> Env.getCurrentHiveTransactionMgr().deregister(txnQueryId));`.

**Touch points (three changes).**
1. **NEW SPI** — `ConnectorScanPlanProvider` (fe-connector-api): add
   `default void releaseReadTransaction(String queryId) { }` (no-op default, matching the existing
   `classifyColumn`/`getDeleteFiles` default-no-op pattern).
2. **Connector impl** — `HiveScanPlanProvider`:
   `@Override public void releaseReadTransaction(String queryId){ readTxnManager.deregister(queryId); }`.
   `readTxnManager` (HiveScanPlanProvider.java:86) is the *same* per-connector instance
   `HiveConnector.java:79/104` injects into every provider, so `register` (planAcidScan) and `deregister` route
   to one manager.
3. **fe-core wiring** — `PluginDrivenScanNode.getSplits()`: after `resolveScanProvider()` returns non-null
   (i.e. after the null-guard at ~:911-915, and it must **dominate** `planScan` at ~:965 — register **before**
   the `requiredPartitions`-empty short-circuit ~:938), register **unconditionally**:
   `String queryId = connectorSession.getQueryId(); QeProcessorImpl.INSTANCE.registerQueryFinishCallback(queryId,
   () -> onPluginClassLoader(scanProvider, () -> { scanProvider.releaseReadTransaction(queryId); return null;
   }));`. Reuse the existing private-static `onPluginClassLoader` helper (PluginDrivenScanNode.java:516-524) for
   the TCCL pin; add an import of `org.apache.doris.qe.QeProcessorImpl`.

**Why the single `getSplits` site is leak-proof.** Hive can *never* enter connector batch mode:
`HiveScanPlanProvider` overrides neither `supportsBatchScan` (defaults `false`) nor
`planScanForPartitionBatch`, so the batch entry point (`PluginDrivenScanNode.startSplit →
planScanForPartitionBatch`) can never open a hive read txn. `register(txn)` is reachable **only** via
`getSplits → planScan`. Register **before** `planScan` so a `planScan` that opens the metastore txn and then
throws still has its release callback in place (`deregister` on an absent `queryId` is a safe no-op).

**queryId identity invariant (single string, no conversion).**
`ConnectorSessionBuilder.java:57` (`org.apache.doris.connector`, **not** `datasource`) sets
`b.queryId = ctx.queryId() != null ? DebugUtil.printId(ctx.queryId()) : ""`. So
`session.getQueryId()` == `DebugUtil.printId(...)` == the `runAndClear` key (QeProcessorImpl.java:212) == the
`txnMap` key (`txn.getQueryId()`). One string serves the registry key and the manager key.

**Traps.**
- **T1 (TCCL — load-bearing).** The callback runs on the `StmtExecutor` thread (unregisterQuery callers
  StmtExecutor.java:1035/2188), whose TCCL is the fe-core `app` loader, **not** the plugin loader.
  `deregister → txn.commit → hmsClient.commitTxn` dispatches into plugin/hive-metastore/thrift code that
  resolves classes by name via TCCL (documented split-brain hazard). The callback **must** wrap
  `releaseReadTransaction` in `onPluginClassLoader(scanProvider,...)`; omitting the pin risks
  ClassCast/NoClassDef at commit time on a live hive query.
- **T2 (live-but-inert on already-flipped connectors — Class C).** The fe-core registration is
  **unconditional**, so it runs **today** for every live plugin scan (es/jdbc/paimon/max_compute/iceberg). For
  them the callback pins TCCL and calls the no-op default `releaseReadTransaction` — functionally inert but
  genuinely exercised. This is why item 1 is behavior-preserving yet **not strictly dormant**: the default
  no-op must never throw; per-query cost is one extra `CopyOnWriteArrayList` entry. **Do NOT** gate it with any
  `if(hive)`/`instanceof`/`isTransactional` branch (IRON RULE).
- **T3 (connector-agnostic gate).** `HiveConnector.getScanPlanProvider(handle)` (HiveConnector.java:120-124)
  returns the **iceberg sibling** provider for a foreign handle, whose default `releaseReadTransaction` is a
  no-op — no hive read txn is opened for iceberg-on-HMS, so nothing to release.
- **T4 (pre-existing multi-ACID-table parity limitation — do NOT fix here).** `HiveReadTransactionManager` keys
  `txnMap` by `queryId`; a query joining two full-ACID hive tables runs `planAcidScan` twice with the same
  `session.getQueryId()`, so the 2nd `register()` overwrites the 1st and that 1st metastore txn leaks. Legacy
  `HiveTransactionMgr` has identical keying — the plugin faithfully preserves parity. Out of scope for item 1.
- **T5 (id scoping).** The release must key on `session.getQueryId()` (== `DebugUtil.printId`). A future
  refactor passing a raw `TUniqueId` string would make `runAndClear` never match (silent lock leak) or
  `deregister` miss the `txnMap` key. Keep the single-string invariant.

**Proving test (two, both provable now).**
1. **Connector** (fe-connector-hive, mirror `HiveReadTransactionTest` with its fake `HmsClient`): drive
   `planAcidScan → register` (assert `openTxn` fired, shared lock acquired), call
   `HiveScanPlanProvider.releaseReadTransaction(queryId)`, assert `commitTxn(txnId)` fired **exactly once** and a
   second call is a no-op (idempotent `txnMap.remove`). Encodes WHY: an unreleased read txn leaks the metastore
   shared lock. **Do not** assert an exception surfaces on commit failure — HEAD swallows+logs it (T-above).
2. **fe-core** (mirror `PluginDrivenScanNode…SelectionTest`/`MvccPinTest` Mockito style): stub
   `resolveScanProvider()` to a Mockito `ConnectorScanPlanProvider`, run `getSplits()`, capture the `Runnable`
   passed to a mocked `QeProcessorImpl.registerQueryFinishCallback` (verify registered with
   `session.getQueryId()` and **before** `planScan`), invoke it, verify `releaseReadTransaction(queryId)` is
   called with TCCL pinned to the provider's classloader. `QueryFinishCallbackRegistryTest` already proves
   `runAndClear` fires+clears per-query, so registry semantics need no new test. **Add**: a live-connector
   no-regression check (paimon/iceberg scan still green) since the registration is unconditional.

**Deps / risk.** Independent of items 2/4/5. **Coupled to item 3** as the two halves of the read cutover:
item 1 wires the NEW plugin-side release; item 3 DELETES the legacy `Env.hiveTransactionMgr` + legacy
`HiveScanNode`. Item 1 is a **hard pre-flip prerequisite**: without it, flipped ACID reads register a metastore
txn (HiveScanPlanProvider.java:177) that is never committed → **leaked shared read lock**. No ordering
constraint vs the flip other than "item 1 before flip".

---

### Item 2a — reject explicit-partition-spec INSERT — **dormant-committable-now (Class C)**

**HEAD state (NOT ported — this is the real remaining pre-check).**
`BindSink.java:667-669` (inside `bindHiveTableSink`, the legacy `UnboundHiveTableSink` path):
`if (!sink.getPartitions().isEmpty()) { throw new AnalysisException("Not support insert with partition spec in
hive catalog."); }`. `sink.getPartitions()` (`UnboundBaseExternalTableSink:69`) is the **dynamic partition-NAME
list** from `PARTITION(p1,p2)` — a *different* field from the static `PARTITION(col='val')` spec
(`staticPartitionKeyValues`).

On the flip, `CatalogFactory` makes the hms catalog a `PluginDrivenExternalCatalog`, so
`UnboundTableSinkCreator` builds `UnboundConnectorTableSink` and binding goes through **`bindConnectorTableSink`**
(BindSink.java:758-850). Verified at HEAD: `bindConnectorTableSink` reads `sink.getStaticPartitionKeyValues()`
(:768) and calls `checkConnectorStaticPartitions(...)` (:778) but **NEVER reads `sink.getPartitions()`** — so
`INSERT INTO hive PARTITION(p1,p2)` would be **silently accepted** post-flip (a fail-loud regression).
`HiveConnectorMetadata.validateStaticPartitionColumns` is a permissive no-op covering only the static `col=val`
form, not this name-list form.

**Touch points — mirror the existing `checkConnectorStaticPartitions`/`validateStaticPartitionColumns` seam**
(BindSink.java:727-756 + `ConnectorWriteOps.validateStaticPartitionColumns` :71-74):
1. **NEW neutral SPI** on `ConnectorWriteOps`, e.g.
   `default void validateWritePartitionNames(ConnectorSession session, ConnectorTableHandle handle,
   List<String> partitionNames) { /* permissive default no-op */ }` (connector owns the message).
2. **fe-core wiring** in `bindConnectorTableSink`: after resolving `table`, guard
   `if (!sink.getPartitions().isEmpty())`, resolve the handle (reuse the `checkConnectorStaticPartitions`
   pattern / `PluginDrivenExternalTable.resolveConnectorTableHandle`), call the SPI, and
   `catch (DorisConnectorException e) { throw new AnalysisException(e.getMessage(), e); }`. **The handle
   round-trip + SPI call happen only inside the guard** — so plain `INSERT INTO hive SELECT` (empty
   `getPartitions()`) is fully byte-unchanged for every live connector; only an `INSERT … PARTITION(names)`
   pays the extra round-trip.
3. **Connector site** `HiveConnectorMetadata`: override — foreign (non-`HiveTableHandle`) →
   `siblingMetadata(session).validateWritePartitionNames(...)`; hive handle + non-empty names →
   `throw new DorisConnectorException("Not support insert with partition spec in hive catalog.")` (exact legacy
   message, **no trailing period difference** — see T-below); empty names → return silently.
4. **flip/delete-time:** delete BindSink.java:667-669 (with the rest of `bindHiveTableSink`).

**Traps.**
- **T1 (key on the right field).** Must key the reject on `sink.getPartitions()` (dynamic NAME list) **only** —
  NOT `staticPartitionKeyValues`. Hive static-partition INSERT `PARTITION(dt='x')` is legal plain-hive and must
  stay permissive; conflating the two newly rejects legal writes.
- **T2 (don't key on "table is partitioned").** A normal dynamic-partition hive write uses **no** PARTITION
  clause (`getPartitions()` empty), so keying on partitioning would reject **all** writes to partitioned hive
  tables. Plain `INSERT … SELECT` → guard false → no reject (parity preserved).
- **T3 (delegate foreign handles).** The hive override must delegate a foreign (iceberg-on-HMS) handle to the
  sibling (permissive), else iceberg-on-HMS `PARTITION(...)` writes get rejected where a standalone
  `type=iceberg` catalog accepts them — a heterogeneous-vs-standalone divergence.
- **T4 (early-return on empty names).** Return silently on empty names so the sibling-test invariant
  "`validate*` for a hive handle returns silently" (`HiveConnectorMetadataSiblingDelegationTest`) still holds.
- **T5 (do NOT inline a generic reject).** `bindConnectorTableSink` is **live** for iceberg/max_compute/paimon,
  which today silently ignore `PARTITION(p1,p2)`. A generic throw inlined in fe-core would be a **live behavior
  change** (not dormant) and would draft the message in the engine (violates connector-agnostic +
  message-in-connector). The permissive-default neutral SPI keeps live connectors' *empty-partition* path
  byte-unchanged and the hive reject dormant. **Class C caveat:** for a live connector that *does* pass a
  non-empty name-list, the new path runs a real `getTableHandle` round-trip + no-op SPI (behavior-preserving,
  not byte-unchanged) — the proving test must confirm those connectors still silently accept.

**Message-text constraint (hard).** The pre-existing e2e
`regression-test/suites/external_table_p0/hive/ddl/test_hive_write_type.groovy:250` asserts on `INSERT …
partition(pt1,pt2)`: `exception "errCode = 2, detailMessage = Not support insert with partition spec in hive
catalog"`. This LIVE test runs on the legacy path today and against the flipped **connector** path post-flip, so
it is the byte-exact parity cross-check: `HiveConnectorMetadata`'s `DorisConnectorException.getMessage()` →
`AnalysisException` **must** reproduce `Not support insert with partition spec in hive catalog` verbatim. (The
legacy fe-core literal has a trailing `.`; the regression matches on a substring without it, so either is fine —
but keep the connector message byte-identical to the legacy literal to be safe.)

**Proving test.**
- **Connector** (dormant, provable now): extend `HiveConnectorMetadataSiblingDelegationTest` — hive handle +
  non-empty names → throws exactly the legacy message; hive handle + empty list → no-op, never consults the
  sibling; foreign handle → forwards to sibling. Add `validateWritePartitionNames` to `EXPECTED_WRITE_METHODS`
  (the Rule-9 completeness lock).
- **fe-core**: a `bindConnectorTableSink` test asserting a flipped-hms hive `INSERT … PARTITION(p1,p2)` surfaces
  `AnalysisException` carrying the legacy message, AND that plain `INSERT INTO hive SELECT` and hive static
  `PARTITION(dt='x')` are NOT rejected. Encode WHY (fail-loud parity), not just that a throw occurs.
- **Live-connector no-regression** (Class C): iceberg/paimon INSERT still green (the guard + round-trip must not
  reject their partition writes).

**Deps / risk.** Depends on the sibling-delegation plumbing already landed (W1–W5, present at HEAD).
Independent of items 1/3/4/5. Land the SPI+override+wiring as one dormant commit **before** the flip; the delete
of BindSink.java:667-669 happens at/after the flip.

---

### Item 2b — reject LZO read-only — **DONE in connector; only a flip-time delete remains**

**HEAD state (three loci; the definitive guard is NOT the BindSink fast-fail).**
1. **Legacy fast-fail** (best-effort, table-SD only): `BindSink.java:671-681` — throws
   `"INSERT INTO is not supported for LZO Hive tables (input format: …). LZO tables are read-only in Doris."`.
   Its own comment (BindSink.java:673-676) says it is *best-effort* and the **definitive** guard lives
   elsewhere.
2. **Legacy definitive guard** (`BaseExternalTableDataSink.getTFileFormatType(String)` :78-89) — rejects LZO
   before any other format match. Sole callers are `HiveTableSink` (table SD, ~:126) and every existing
   partition SD (~:223). `PluginDrivenTableSink` extends the same base but does **not** call
   `getTFileFormatType(String)`.
3. **Connector port** (`HiveSinkHelper.getTFileFormatType` :83-101, **byte-identical message**), wired on the
   write-admission path at `HiveWritePlanProvider.buildSink:195` (table SD) **and** `buildExistingPartitions:262`
   (every existing partition SD).

**Classification.** No remaining port work — 2b is done in the connector. The connector port is **PARITY**, not
an upgrade: legacy **already** rejected per-partition LZO via `BaseExternalTableDataSink.getTFileFormatType`
(called for the table SD and every partition SD, per its comment). Earlier recon's "intentionally broader /
documented upgrade" framing was **wrong** — it conflated "the BindSink fast-fail" (table-SD only) with "legacy
behavior" (which includes the definitive per-partition guard). Coverage of plain-hive is unchanged.

**Remaining touch point (flip/delete-time only).** Delete scope is broader than earlier recon stated — the whole
legacy sink chain dies at flip and includes **all three** loci:
- `BindSink.java:671-681` (the fast-fail block — inside `bindHiveTableSink`, dies with the whole method);
- `HiveTableSink` itself (constructed live at `PhysicalPlanTranslator.java:569
  `new HiveTableSink((HMSExternalTable)...)`);
- `BaseExternalTableDataSink.getTFileFormatType(String)` becomes **dead** once `HiveTableSink` is deleted (no
  other caller). The flip author decides whether to also remove the now-orphaned protected method.

**Traps.**
- The substring-`"lzo"` match is case-insensitive and applied **before** the text/orc/parquet match
  (`LzoTextInputFormat` contains `"text"`); the ordering at `HiveSinkHelper` :84-96 (and
  `BaseExternalTableDataSink` :86-96) is load-bearing — reordering it after the text match would misclassify
  LZO as `FORMAT_CSV_PLAIN` and let a permanently-invisible write through.
- The reject now fires at **plan** time (`planWrite`) rather than analysis time (`BindSink`) — a slightly later
  but still pre-BE fail point; matches the design's "push into the write path".

**Proving test.** Already dormant-proven by `HiveWritePlanProviderTest.planWriteRejectsLzoInputFormat` (asserts
`planWrite` throws `DorisConnectorException` naming LZO for a table-SD LZO input format). A per-partition-LZO
variant (partition SD LZO, table SD plain) would additionally lock the broadened wiring at
`HiveWritePlanProvider:262` — worth adding, not blocking.

**Deps / risk.** Independent of all other items. The DELETE is part of the flip/delete unit (§3) and must not
precede the flip.

---

### Item 3 — delete `Env.hiveTransactionMgr` + accessors — **flip/delete-time only**

**HEAD state.** fe-core `Env.java:568` `private HiveTransactionMgr hiveTransactionMgr;`; `Env.java:865`
`this.hiveTransactionMgr = new HiveTransactionMgr();`; `Env.java:1097-1103`
`getHiveTransactionMgr()` / `static getCurrentHiveTransactionMgr()`. The **only live consumer** is legacy fe-core
`HiveScanNode.java:141/147/319`. `HiveScanNode` is still **live** pre-flip: `PhysicalPlanTranslator.java:814`'s
`table instanceof PluginDrivenExternalTable` arm is missed by a plain `HMSExternalTable`, falling to the switch
`case HIVE: new HiveScanNode(...)` (~:834-835). The plugin already holds a complete port
(`HiveConnector.readTxnManager`, `HiveReadTransactionManager`, "plugin-owned and dormant until the read
cutover", HiveReadTransactionManager.java:31-35). One test reference: `OlapInsertExecutorTest.java:264/270`
mocks the static `getCurrentHiveTransactionMgr`.

**Classification / touch points (DELETE-only, at the read cutover — same change that deletes legacy
`HiveScanNode`).** No SPI to add, no dormant relocation (the relocation already shipped as
`HiveReadTransactionManager`):
1. `Env.java:568` delete field; 2. `Env.java:865` delete ctor init; 3. `Env.java:1097-1099` delete
`getHiveTransactionMgr()`; 4. `Env.java:1101-1103` delete `getCurrentHiveTransactionMgr()`; 5. delete orphaned
fe-core `datasource/hive/HiveTransactionMgr.java` + `HiveTransaction.java` (superseded by the plugin ports);
6. update `OlapInsertExecutorTest.java:264/270` to drop the mock (or the test stops compiling). **No connector
site** — `HiveConnector.readTxnManager` is already wired; **item 1** is what activates it.

**Traps.**
- Deleting the Env field/accessors while legacy `HiveScanNode` still exists = compile break at
  HiveScanNode.java:141/147/319 **and** live ACID hive reads lose their read-lock + write-id snapshot
  registration → incorrect ACID snapshots + leaked HMS read locks. Must be **strictly co-sequenced** with
  `HiveScanNode` deletion.
- If the flip lands before **item 1** wires the plugin-side release, flipped ACID reads register a
  `HiveReadTransaction` that is never committed → **permanent metastore read-lock leak**. So item 1 gates the
  flip; item 3 gates on the flip.
- Forgetting `OlapInsertExecutorTest` update = test compile break (silent "skipped test" risk).

**Proving test.** No dormant unit test can prove deletion in isolation (the field is live today). Proven
post-cutover by: (i) fe-core compiles after `HiveScanNode` + Env manager removal; (ii) existing
`HiveReadTransactionTest` locks `register→begin`/`deregister→commit`; (iii) an e2e ACID-hive read regression on a
flipped hms catalog asserting the HMS read lock is released at query finish (per MEMORY: iceberg/hive-on-HMS new
capabilities each need e2e); (iv) `OlapInsertExecutorTest` updated and green.

**Deps / risk.** Purely delete-time; **last** in the whole write-chain. Gated behind item 1 (before flip) and
the flip itself. Independent of items 4/5 code-wise.

---

### Item 4 — relocate `BIND_BROKER_NAME` → `BrokerProperties.BIND_BROKER_NAME_KEY` — **dormant-committable-now (Class B)**

**HEAD state.** `HMSExternalCatalog.java:60` `public static final String BIND_BROKER_NAME = "broker.name";`.
`BrokerProperties.java:58` `private static final String BIND_BROKER_NAME_KEY = "broker.name";` (**private**; also
used by `guessIsMe` :64-65 via `equalsIgnoreCase`). The two literals are **identical** (`"broker.name"`). The
generic base reads it: `ExternalCatalog.java:1319-1320`
`public String bindBrokerName(){ return catalogProperty.getProperties().get(HMSExternalCatalog.BIND_BROKER_NAME);
}` — a fe-core **base-class → HMS-subclass** constant dependency (import at ExternalCatalog.java:44). Repo-wide,
`HMSExternalCatalog.BIND_BROKER_NAME` has **exactly one** external reference (ExternalCatalog.java:1320) plus its
definition; `HMSExternalCatalog` appears in `ExternalCatalog.java` **only** at the import (:44) and :1320.
`bindBrokerName()` itself has multiple callers, all unaffected because the resolved key is unchanged
(HiveScanNode.java:125/210/239, DefaultConnectorContext.java:318, HiveTableSink.java:161; mocked in
HiveScanNodeTest.java).

**Touch points (fe-core only, no connector/SPI surface).**
1. `BrokerProperties.java:58` `private → public` on `BIND_BROKER_NAME_KEY`.
2. `ExternalCatalog.java:1320` `HMSExternalCatalog.BIND_BROKER_NAME → BrokerProperties.BIND_BROKER_NAME_KEY`,
   keeping the exact `catalogProperty.getProperties().get(...)` shape.
3. `ExternalCatalog.java:44` remove the now-unused `import …hive.HMSExternalCatalog;`, add
   `import …datasource.property.storage.BrokerProperties;` (same fe-core module).
4. `HMSExternalCatalog.java:60` delete the constant.

This also satisfies an iron-rule cleanup: the generic `ExternalCatalog` base no longer imports/depends on the
HMS subclass.

**Traps.**
- **Preserve exact lookup semantics.** `bindBrokerName()` uses case-**sensitive** `Map.get("broker.name")`,
  whereas `guessIsMe` uses `equalsIgnoreCase`. **Do NOT harmonize** `bindBrokerName` to case-insensitive while
  relocating — that would silently change the resolved broker name for every catalog that set a differently-cased
  key. Keep `.get(BIND_BROKER_NAME_KEY)` byte-identical to `.get("broker.name")`.
- Because both constants are literally `"broker.name"`, the resolved value is provably unchanged for hms **and**
  every non-hms catalog (absent → null).
- Don't delete the HMS copy without the single-reference grep (done) — an outside reference would compile-break.
- Making the key public must not shadow the `@ConnectorProperty(names={"broker.name"})` literal at
  BrokerProperties.java:38 — separate, both remain `"broker.name"`.

**Proving test.** Parity unit test on the **real** method (HiveScanNodeTest *mocks* `bindBrokerName` returning
`""`, so it will NOT catch a regression): (i) `{"broker.name":"b1"}` → `bindBrokerName()=="b1"`; (ii) absent →
`null`; (iii) assert the single-source constant equals `"broker.name"` post-removal.

**Deps / risk.** Fully independent; committable **now**, no flip gating. Lowest risk (Class B, value-identical).
Safe to land first.

---

### Item 5 — `pluginCatalogTypeToEngine` add `case "hms" → ENGINE_HIVE` — **dormant-committable-now (Class A)**

**HEAD state.** `CreateTableInfo.java:935-946` (`org.apache.doris.nereids.trees.plans.commands.info`):
`switch(catalog.getType()){ case "max_compute": return ENGINE_MAXCOMPUTE; case "paimon": return ENGINE_PAIMON;
case "iceberg": return ENGINE_ICEBERG; default: return null; }` — **no `"hms"`**, so hms → `null`.
`ENGINE_HIVE = "hive"` exists (CreateTableInfo.java:121). Flip mechanism confirmed: pre-flip `"hms"` →
`HMSExternalCatalog` (CatalogFactory :133-134); with `"hms"` in `SPI_READY_TYPES` → `PluginDrivenExternalCatalog`
(CatalogFactory :110) with `getType()=="hms"`. Consumers:
- `paddingEngineName` (CreateTableInfo.java:911-922): pre-flip hms hits :913 `instanceof HMSExternalCatalog →
  ENGINE_HIVE`; **post-flip** it misses :913, reaches :915-916 `instanceof PluginDrivenExternalCatalog &&
  pluginCatalogTypeToEngine(...) != null` which is **false** (null) → falls to :921 `throw "Current catalog does
  not support create table"`.
- `checkEngineWithCatalog` (:386-395) mirrors the same pattern.

**Touch point (fe-core only, single line).** Add to the switch:
`case "hms": return ENGINE_HIVE;`. Post-flip this makes (i) `paddingEngineName` :915-919 pad `engine=hive` for a
no-ENGINE CREATE on a flipped hms catalog (mirrors legacy :913-914); (ii) `checkEngineWithCatalog` :388-393
reject a non-hive explicit ENGINE with "This catalog can only use `hive` engine." (mirrors legacy :386-387).

**Traps.**
- Must return `ENGINE_HIVE` (`"hive"`), never `"hms"`/`"maxcompute"`.
- **Non-interference verified** with the iceberg-only arm `getEffectiveIcebergFormatVersion` (~:1163) which
  gates on `ENGINE_ICEBERG.equals(pluginCatalogTypeToEngine(...))` — `hms → ENGINE_HIVE != ENGINE_ICEBERG`, so
  hms does NOT trigger iceberg row-lineage/format-version logic (correct: an iceberg-on-HMS table created via
  the hms gateway is still a hive-engine CREATE — legacy hms catalogs always use hive engine).
- **Dormancy verified (Class A):** no `PluginDrivenExternalCatalog` has `getType()=="hms"` until `"hms"` enters
  `SPI_READY_TYPES`, so the new case is unreachable pre-flip; plain `HMSExternalCatalog` continues through the
  `instanceof` arms (:913 / :386) unchanged.

**IRON-RULE note (tension, not a new violation).** `pluginCatalogTypeToEngine` (and items 5b's switches) switch
on `PluginDrivenExternalCatalog.getType()` — a source-type switch in fe-core, in tension with "no
`switch(dlaType)`/engine-name checks". This is a **pre-existing, maintainer-sanctioned** pattern
(max_compute/paimon/iceberg already committed; javadoc at CreateTableInfo.java:926-933 documents the sync
invariant), so extending it with `"hms"` **conforms** (Rule 11) and is NOT a new violation. Cleaner long-term:
push engine-name behind a connector-provided SPI so fe-core stops switching on `getType()` — out of scope here.

**Proving test.** Dormant unit test in `CreateTableInfoEngineCatalogTest` (mocks
`PluginDrivenExternalCatalog.getType()`, reflectively invokes private `paddingEngineName`/`checkEngineWithCatalog`):
`registerPluginCatalog("hms_ctl","hms")` then assert (i) no-ENGINE CREATE pads `engineName=="hive"`; (ii) CTAS via
`validateCreateTableAsSelect` pads `"hive"`; (iii) `checkEngineWithCatalog` throws for explicit `ENGINE!=hive`;
(iv) passes for `ENGINE=hive`. Proves the switch entry without an actual flip (`getType` mocked to `"hms"`).

**Deps / risk.** Fully independent; committable now, live only at the flip. **Merge with item 5b** (below).

---

### Item 5b — `PluginDrivenExternalTable` engine-display `"hms"` cases — **dormant-committable-now (Class A) — MERGE INTO ITEM 5**

**Why this exists.** Verifying item 5 surfaces a firm dormant sub-step the original 5-item set omitted, and the
`pluginCatalogTypeToEngine` javadoc **mandates** it: CreateTableInfo.java:930-931 says the switch "must stay in
sync" with `PluginDrivenExternalTable.getEngine()/getEngineTableTypeName()`. `hms` IS a CREATE-TABLE-capable
full-adopter, so the sync obligation is triggered.

**HEAD state.** `PluginDrivenExternalTable.java:988-1021` `getEngine()` and :1023-1043
`getEngineTableTypeName()` switch on `PluginDrivenExternalCatalog.getType()` with cases
jdbc/es/iceberg/trino-connector/max_compute/paimon and **NO `"hms"`** → default `super.getEngine()` (`"Plugin"`)
and `TableType.PLUGIN_EXTERNAL_TABLE.name()`. A flipped hms table therefore **displays engine `"Plugin"`** in
`SHOW TABLE STATUS` / `information_schema.tables`, where legacy `HMSExternalTable` shows **`"hms"`** — a
user-visible regression at flip.

**Correct parity values (NOT `"hive"`).** Legacy DISPLAY engine for hms tables is **`"hms"`** — verified
`TableIf.java:270-271` `case HMS_EXTERNAL_TABLE: return "hms";`. This is **distinct** from the CREATE-TABLE
engine (item 5 → `"hive"`). So item 5b must add:
- `getEngine()`: `case "hms": return TableType.HMS_EXTERNAL_TABLE.toEngineName();` (== `"hms"`).
- `getEngineTableTypeName()`: `case "hms": return TableType.HMS_EXTERNAL_TABLE.name();` (== `"HMS_EXTERNAL_TABLE"`).

Returning `ENGINE_HIVE` here would be a *different* regression (legacy hms tables never displayed `"hive"`).

**Classification / scope.** Class A (unreachable until a PluginDriven catalog has `getType()=="hms"` — same
dormancy as item 5). **Display-only, NOT a functional break** — verified: `getEngineTableTypeName` feeds only
SHOW output (Env.java:4292/4657); `getEngine()` has **no** write/sink/route/dml callers (grep-confirmed). But it
is a firm dormant-committable sub-step, not a "soft follow-up".

**Why merge into item 5's commit.** Splitting opens a window where CREATE resolves `engine=hive` but display
shows `"Plugin"` — an internally inconsistent flip. Land item 5 (CREATE engine) + item 5b (display engine) as
**one commit**.

**Proving test.** Extend the item-5 test harness (or the existing engine-display test for
`PluginDrivenExternalTable`): mock `getType()=="hms"`, assert `getEngine()=="hms"` and
`getEngineTableTypeName()=="HMS_EXTERNAL_TABLE"` (mirrors the jdbc/es/iceberg cases).

**Deps / risk.** Independent of items 1/2a/3/4. Coupled to item 5 (merge). Low risk (display-only, unreachable).

---

## 2. Ordered dormant sub-steps (WC codes — internal only)

All four WC commits land **before** the single `SPI_READY_TYPES` flip line. WC1–WC3 are independent leaves with
**no ordering constraint among themselves**; WC4 (item 1) is the one **hard pre-flip prerequisite**. Recommended
order is risk-ascending (land the safest first):

| Code | Item(s) | Class | What lands | Deps | Risk |
|------|---------|-------|-----------|------|------|
| **WC1** | 4 | B (value-identical) | Relocate `BIND_BROKER_NAME` → `BrokerProperties.BIND_BROKER_NAME_KEY`; make key public; drop HMS copy; fix `ExternalCatalog` import | none | **Lowest.** Live method but resolved value provably identical. Only risk = case-sensitivity harmonization (T-item4). |
| **WC2** | 5 **+** 5b | A (unreachable) | `pluginCatalogTypeToEngine` `case "hms"→ENGINE_HIVE`; `PluginDrivenExternalTable.getEngine()` `case "hms"→"hms"`; `getEngineTableTypeName()` `case "hms"→"HMS_EXTERNAL_TABLE"` — **one commit** | none | **Very low.** Strictly unreachable pre-flip. Risk = wrong return value (hive vs hms confusion) — CREATE=hive, DISPLAY=hms. |
| **WC3** | 2a | C (live, no-op default) | New `ConnectorWriteOps.validateWritePartitionNames` (permissive default); `bindConnectorTableSink` guarded wiring; `HiveConnectorMetadata` override (hive→reject, foreign→sibling) | sibling plumbing W1–W5 (present) | **Medium.** New guard runs on live iceberg/mc/paimon **only when `PARTITION(names)` present**; empty case byte-unchanged. Needs live-connector no-regression test. Message must match e2e verbatim. |
| **WC4** | 1 | C (live, no-op default) | New `ConnectorScanPlanProvider.releaseReadTransaction` (no-op default); `HiveScanPlanProvider` override; `PluginDrivenScanNode.getSplits` **unconditional** callback registration with TCCL pin | none (coupled to item 3 at delete-time) | **Highest of the four.** Registration runs on **every** live plugin scan (es/jdbc/paimon/mc/iceberg) calling the no-op default. TCCL pin is load-bearing (T1). **Hard prerequisite for a safe flip.** |

**Then:** the single flip line — add `"hms"` to `CatalogFactory.SPI_READY_TYPES:50` (**not** a WC step).

**Ordering rationale.**
- WC1/WC2/WC3 have no code overlap and no flip-gating beyond "before the flip"; any interleaving is safe. Listed
  risk-ascending.
- WC4 must merge **before the flip** or flipped ACID reads leak the metastore shared read lock (item 1 T-above /
  item 3 trap-b). It has no ordering constraint vs WC1–WC3.
- Class-C commits (WC3, WC4) each **require a live-connector no-regression test** (paimon/iceberg scan + insert
  still green), because their fe-core wiring executes today on live connectors relying on no-op SPI defaults.
  Class-A/B commits (WC1, WC2) are provable with dormant unit tests alone.

---

## 3. Residuals deferred to the flip / delete phase

These are **NOT** dormant-committable and must **NOT** precede the flip. The flip flips
`HMSExternalTable → PluginDrivenExternalTable`, so `PhysicalPlanTranslator` selects `PluginDrivenScanNode` over
legacy `HiveScanNode` and `UnboundTableSinkCreator` routes writes through `UnboundConnectorTableSink`; the whole
legacy scan/sink chain then goes dead and is deletable.

**R1 — Delete `bindHiveTableSink` (ONE deletion, holds BOTH legacy write pre-checks).** `bindHiveTableSink`
(BindSink.java:660) contains **both** the item-2a partition-spec reject (:667-669) **and** the item-2b legacy LZO
fast-fail (:671-681). They are a **single** flip-time delete (remove the whole method), not two — earlier
"two separate deletes / cyclic unit" framing was imprecise.

**R2 — Delete legacy `HiveScanNode` + item 3.** Co-sequenced as one change: delete `HiveScanNode`
(sole live caller of `Env.getCurrentHiveTransactionMgr` via PhysicalPlanTranslator.java:834 `case HIVE`) together
with `Env.hiveTransactionMgr` field + accessors (Env.java:568/865/1097-1103), the orphaned fe-core
`HiveTransactionMgr.java`/`HiveTransaction.java`, and the `OlapInsertExecutorTest` mock update. **Gated behind
WC4** (item 1 must have wired the plugin-side release before the flip).

**R3 — Delete `HiveTableSink` + orphaned LZO guard.** `HiveTableSink` (constructed at
PhysicalPlanTranslator.java:569) dies at the flip; `BaseExternalTableDataSink.getTFileFormatType(String)`
(:78-89) becomes dead once `HiveTableSink` is its last caller. Flip author decides whether to remove the
now-orphaned protected method.

**R4 — The flip line itself.** Add `"hms"` to `CatalogFactory.SPI_READY_TYPES` (§4.5's single flip; explicitly
out of every WC/residual sub-step).

---

## 4. Known unknowns / out-of-scope flags (honest boundaries)

- **Write-side TCCL pin (design §4.4 item W6) is NOT closed by item 1.** Item 1 handles only the **read**-side
  query-finish TCCL pin. The analogous **write**-side pin is open: `IcebergWritePlanProvider.planWrite`
  (~:152) builds its `TDataSink` on a fe-core thread under the app loader, and `executeAuthenticated` is only
  confirmed deep at ~:672 — whether sink-construction itself is pinned is flagged **OPEN** in
  `hms-write-delegation-decomposition-2026-07-08.md:30` (W6: "flip-time, NOT dormant-testable, VERIFY first").
  This is §4.4 scope (not one of the five §4.5 items), so its absence from this decomposition is by design — but
  a reader consolidating "remaining pre-flip write items" should know item 1's read-side pin does **not** close
  the write-side TCCL story.
- **Multi-ACID-table read-txn leak (item 1 T4).** Pre-existing in both legacy `HiveTransactionMgr` and the
  plugin `HiveReadTransactionManager` (both key `txnMap` by `queryId`). Deliberately **out of scope** for item 1
  — fixing it inside WC4 would diverge from legacy parity.
- **`deregister` swallows commit failure.** Not "fail-loud" (HiveReadTransactionManager.java:55-59 logs and
  swallows `RuntimeException`; `QueryFinishCallbackRegistry` further isolates per-callback exceptions). Any
  item-1 test must assert *exactly-once `commitTxn`* and *idempotent second call*, **not** an exception on
  commit failure (that behavior does not exist at HEAD).

---

## 5. References (all HEAD `75a5d25d746`, branch `catalog-spi-11-hive`)

**Design / prior recon.**
- `plan-doc/tasks/hms-cutover-retype-design-2026-07-07.md` §4.5 (:77-83), §5.1 (phase ordering).
- `plan-doc/tasks/hms-write-delegation-decomposition-2026-07-08.md` (W1–W6; W6 write-side pin OPEN at :30).
- `plan-doc/tasks/hms-cutover-sibling-connector-decomposition-2026-07-08.md` (sibling W1–W5 plumbing).

**Item 1 (read-ACID).** `fe-connector-hive/.../HiveScanPlanProvider.java:86,124-127,175-177`;
`.../HiveReadTransactionManager.java:47-61`; `.../HiveReadTransaction.java` (begin/commit);
`.../HiveConnector.java:79,104,120-124`; `fe-connector-api ConnectorScanPlanProvider.java` (default-no-op
precedent `classifyColumn`/`getDeleteFiles`); `fe-core .../datasource/PluginDrivenScanNode.java:516-524
(onPluginClassLoader), ~911-915/938/965 (getSplits)`; `.../qe/QeProcessorImpl.java:212,216-217`;
`.../qe/QeProcessor.java:46`; `.../qe/QueryFinishCallbackRegistry.java`; `.../connector/ConnectorSessionBuilder.java:57`;
legacy `.../planner/HiveScanNode.java:138-149,319`. Tests: `HiveReadTransactionTest`,
`QueryFinishCallbackRegistryTest`, `PluginDrivenScanNode…SelectionTest`/`MvccPinTest`.

**Item 2a/2b (write pre-checks).** `fe-core .../nereids/rules/analysis/BindSink.java:660-681 (bindHiveTableSink:
2a :667-669, 2b :671-681), 727-756 (checkConnectorStaticPartitions), 758-850 (bindConnectorTableSink: :768/778)`;
`.../nereids/trees/plans/commands/UnboundBaseExternalTableSink.java:69`;
`.../nereids/trees/plans/commands/UnboundTableSinkCreator.java:60-63/93-97/128-132`;
`.../planner/BaseExternalTableDataSink.java:78-89`; `.../planner/HiveTableSink.java (~126/223)`;
`.../translator/PhysicalPlanTranslator.java:569`; `fe-connector-hive HiveSinkHelper.java:83-101`;
`HiveWritePlanProvider.java:195,262`; `HiveConnectorMetadata.java (validateStaticPartitionColumns ~:1503-1510,
siblingMetadata)`; `fe-connector-api ConnectorWriteOps.java:71-74`. Tests:
`HiveWritePlanProviderTest.planWriteRejectsLzoInputFormat`, `HiveConnectorMetadataSiblingDelegationTest`
(EXPECTED_WRITE_METHODS lock), e2e `regression-test/suites/external_table_p0/hive/ddl/test_hive_write_type.groovy:250`.

**Item 3 (Env txn mgr).** `fe-core .../catalog/Env.java:568,865,1097-1103`;
`.../datasource/hive/HiveTransactionMgr.java`, `HiveTransaction.java`; `.../planner/HiveScanNode.java:141,147,319`;
`.../translator/PhysicalPlanTranslator.java:814,834-835`; `HiveReadTransactionManager.java:31-35`;
test `OlapInsertExecutorTest.java:264,270`.

**Item 4 (broker name).** `fe-core .../datasource/hive/HMSExternalCatalog.java:60`;
`.../datasource/property/storage/BrokerProperties.java:38,58,64-65`;
`.../datasource/ExternalCatalog.java:44,1319-1320`.

**Item 5 / 5b (engine map + display).** `fe-core .../nereids/trees/plans/commands/info/CreateTableInfo.java:121
(ENGINE_HIVE), 386-395 (checkEngineWithCatalog), 911-922 (paddingEngineName), 926-946 (pluginCatalogTypeToEngine
+ sync javadoc), ~1163 (getEffectiveIcebergFormatVersion)`;
`.../datasource/PluginDrivenExternalTable.java:988-1021 (getEngine), 1023-1043 (getEngineTableTypeName)`;
`.../catalog/TableIf.java:240-278 (toEngineName; HMS_EXTERNAL_TABLE→"hms" :270-271)`; `Env.java:4292,4657` (SHOW
consumers). Flip: `.../datasource/CatalogFactory.java:49-50 (SPI_READY_TYPES), 98,110,133-134`. Test:
`CreateTableInfoEngineCatalogTest`.