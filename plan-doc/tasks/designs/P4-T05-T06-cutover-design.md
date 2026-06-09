# P4-T05 / P4-T06 — MaxCompute Cutover Design (Batch C)

> Design-first. **✅ SIGNED OFF 2026-06-06** (DECISION-1 = A flag · DECISION-2 = two commits, flip last · DECISION-3 = binding in cutover — see §5). No code touched in this design session; implementation = next fresh session(s), T05 then T06.
> Anchors below were **re-verified against current code** (2026-06-06, branch `catalog-spi-05`) — recon line numbers from HANDOFF were corrected where they had drifted.
> Inputs: [P4 plan](../P4-maxcompute-migration.md) · [P4-T03 design](./P4-T03-write-txn-design.md) · [P4-T04 design](./P4-T04-write-plan-design.md) · [write RFC](./connector-write-spi-rfc.md) · [HANDOFF](../../HANDOFF.md).

---

## 0. Scope & status

Batch C = the **only live cutover** in the maxcompute migration. After the flip, a `max_compute` catalog deserializes to `PluginDrivenExternalCatalog` / `PluginDrivenExternalTable`, and read / write / DDL / partition / show all route through the SPI.

| Task | Nature | Gate | Commit |
|---|---|---|---|
| **P4-T05** | Mechanical wiring (GSON image-compat + engine-name cases) | 🔒 still closed (dormant) | `[P4-T05]` |
| **P4-T06** | Live cutover: dormant→live write wiring + flip + R-004 | 🔓 **live** | `[P4-T06]` (flip as the *last, smallest* commit — see §4.5) |

**Two SPI additions** (both default-preserving, zero impact on jdbc/es/trino): `ConnectorSession.setCurrentTransaction(...)` and `ConnectorWriteOps.usesConnectorTransaction()` (DECISION-1). Log under E11 / decisions-log at impl time.

---

## 1. Background — current state (verified, file:line)

### 1.1 The flip points (T05/T06 mechanical)
- `GsonUtils` (`fe-core/.../persist/gson/GsonUtils.java`): migrated connectors use `registerCompatibleSubtype` — catalogs at **:405-412** (es/jdbc/trino), tables at **:478-483**. **MaxCompute still uses legacy `registerSubtype`: catalog `:397`, table `:472`** (← real edit sites; HANDOFF's ~405/~478 pointed at the compat *block*, not the MC lines). Must **atomically replace** (RuntimeTypeAdapterFactory throws duplicate-label IAE if both forms coexist — P2-T03 precedent).
- `PluginDrivenExternalTable` (`fe-core/.../datasource/PluginDrivenExternalTable.java`): `getEngine()` switch `:196-215` (cases jdbc/es/trino-connector), `getEngineTableTypeName()` `:218-231`. Need `case "max_compute"` in both, returning `TableType.MAX_COMPUTE_EXTERNAL_TABLE.toEngineName()` / `.name()`.
- `PluginDrivenExternalCatalog.legacyLogTypeToCatalogType()` `:347-354`: only special-cases `TRINO_CONNECTOR → "trino-connector"`; **default branch `logType.name().toLowerCase(Locale.ROOT)` already yields `"max_compute"`** ⇒ **NO new case needed** (simpler than HANDOFF implied).
- `CatalogFactory` (`fe-core/.../datasource/CatalogFactory.java`): `SPI_READY_TYPES` at **:52** = `{"jdbc","es","trino-connector"}`; legacy MC switch `case "max_compute"` at **:146-149**. Flip = add `"max_compute"` to `:52`, delete `:146-149`.
- Image-compat enums to **KEEP**: `TableIf.TableType.MAX_COMPUTE_EXTERNAL_TABLE` (`:220`), `InitCatalogLog.Type.MAX_COMPUTE` (`:41`).

### 1.2 The write lifecycle (verified order)
`InsertIntoTableCommand.initPlan` (`:261-360`): **(1) translate** (builds `PluginDrivenTableSink` + its own `connectorSession` via `catalog.buildConnectorSession()` — `PhysicalPlanTranslator.visitPhysicalConnectorTableSink:645-701`, session built `:658`) → **(2) `beginTransaction()`** (`:354`) → **(3) `finalizeSink()`** (`:355-356`) → later `executeSingleInsert` → `beforeExec` → coordinator → `onComplete`(commit) (`AbstractInsertExecutor:251-272`, `BaseExternalTableInsertExecutor.onComplete:92-126`).

**Critical constraint:** the sink's `connectorSession` is built at step 1 (before the txn exists), and `PluginDrivenTableSink.planWrite(connectorSession, …)` (`PluginDrivenTableSink:222`) — i.e. **T04 Approach A, locked** — reads `session.getCurrentTransaction()` (`MaxComputeWritePlanProvider:197`, **fail-loud if absent** `:199`) at step 3. So the connectorTx must be **created (step 2) and bound onto the sink's session before step 3's `bindDataSink`**.

### 1.3 The dormant→live gaps (verified)
| # | Gap (verified) | file:line |
|---|---|---|
| G1 | `ConnectorSession` has `getCurrentTransaction()` default `Optional.empty()`, **no setter**; `ConnectorSessionImpl` has no txn field | `ConnectorSession:75-78`; `ConnectorSessionImpl:32-56` |
| G2 | Live executor is `PluginDrivenInsertExecutor`, built for the **JDBC insert-handle model**: `getWriteConfig`(:97) + `beginInsert`(:101) + `finishInsert`(:109) — **all throwing-default for MC** (D-4) | `PluginDrivenInsertExecutor:70-104`; `MaxComputeConnectorMetadata:241,247,264` |
| G3 | `PluginDrivenTransactionManager.begin(connectorTx)` (W4, `:71-77`) stores only in its **local map — does NOT `putTxnById`** in `GlobalExternalTransactionInfoMgr` | `PluginDrivenTransactionManager:71-77` vs legacy `AbstractExternalTransactionManager.begin:42-48` |
| G4 | `UnboundConnectorTableSink` carries **no static-partition spec** (only `UnboundMaxComputeTableSink` does) | `UnboundTableSinkCreator:66-110` |
| G5 | `InsertIntoTableCommand:598` builds an **empty** `PluginDrivenInsertCommandContext`; `InsertOverwriteTableCommand:407-418` sets overwrite+staticSpec on **legacy** `MCInsertCommandContext` only | `InsertIntoTableCommand:564-598`; `InsertOverwriteTableCommand:407-418` |

The BE→FE block-alloc callback `FrontendServiceImpl.getMaxComputeBlockIdRange:3680-3719` already looks the txn up by `getTxnById(txnId)` (`:3694`) and dispatches on `supportsWriteBlockAllocation()` (`:3696`) — generic (W3/W6). It will throw "Can't find txn" unless **G3** is fixed (the connectorTx must be globally registered). Same registry is used to feed `addCommitData` back from BE.

---

## 2. The cutover in one picture

```
TRANSLATE ──> PluginDrivenTableSink{ connectorSession (no txn yet) }
   │
beginTransaction()  [executor]                                  ┌─ G1 setCurrentTransaction (SPI+impl)
   ├─ usesConnectorTransaction()? ── yes (MC) ──┐               ├─ G2 executor restructure
   │                                            │               ├─ G3 global txn registration
   │   connectorTx = writeOps.beginTransaction(execSession)     │
   │   txnId = pluginTxnMgr.begin(connectorTx)  ── G3 registers ┘
   │
finalizeSink()  [executor]
   ├─ sink.getConnectorSession().setCurrentTransaction(connectorTx)   ← G1
   └─ super.finalizeSink → bindDataSink → planWrite(sinkSession)      ← T04 Approach A reads txn, setWriteSession, stamps txn_id
                                                                       (creates ODPS write session here)
BE exec ──> block-alloc RPC ──> FrontendServiceImpl.getMaxComputeBlockIdRange
            ──> getTxnById(txnId) [needs G3] ──> connectorTx.allocateWriteBlockRange
        ──> commit fragments fed back ──> getTxnById ──> connectorTx.addCommitData

onComplete ──> transactionManager.commit(txnId) ──> connectorTx.commit()  (aggregate WriterCommitMessage → ODPS session.commit)

INSERT [OVERWRITE] [PARTITION(..)] ── G4 UnboundConnectorTableSink carries static spec
                                   └─ G5 fill PluginDrivenInsertCommandContext{overwrite, staticPartitionSpec}
                                        (consumed by PluginDrivenTableSink.bindViaWritePlanProvider:212-224)
```

---

## 3. P4-T05 — mechanical wiring (dormant, gate closed)

Pure image-compat / engine-name plumbing; **no behavior change** while gate is closed. Mirrors P2 trino batch-B.

1. `GsonUtils`: replace `registerSubtype(MaxComputeExternalCatalog…)` `:397` → `registerCompatibleSubtype(PluginDrivenExternalCatalog.class, "MaxComputeExternalCatalog")` (move into the `:405-412` block); same for table `:472` → `registerCompatibleSubtype(PluginDrivenExternalTable.class, "MaxComputeExternalTable")` (into `:478-483`). Atomic replace.
2. `PluginDrivenExternalTable.getEngine()` `:196-215` + `getEngineTableTypeName()` `:218-231`: add `case "max_compute"`.
3. `legacyLogTypeToCatalogType`: **no change** (default branch covers it — verified §1.1). Add a code comment noting MAX_COMPUTE relies on the default, to prevent a future "add a redundant case" churn.

Gate: compile + checkstyle + import-gate (fe-core only). Commit `[P4-T05]`. Still dormant — `max_compute` not yet in `SPI_READY_TYPES`, so live catalogs remain legacy.

> ⚠️ Intermediate-state caveat (P2 batch-B precedent): after the atomic GSON replace but **before** the flip, a freshly-created MC catalog cannot round-trip (compat subtype registered, but factory still legacy). Do not deploy between T05 and T06; land them close together.

### 3.4 Implementation notes (T05 landed 2026-06-06 — gate-green, pending commit)
- **DB registration folded in (correction to §3.1 / §8 step 1).** The ordered TODO listed only catalog `:397` + table `:472`, but the **database** `:452` (`MaxComputeExternalDatabase`) was still a plain `registerSubtype`. Left un-migrated it throws `ClassCastException` post-flip: `MaxComputeExternalDatabase.buildTableInternal:44` casts `extCatalog` to `MaxComputeExternalCatalog`, but a replayed catalog is now `PluginDrivenExternalCatalog`. es/jdbc/trino migrated catalog+**db**+table together (their legacy DB classes are deleted). T05 therefore migrated **all three** GSON registrations to `registerCompatibleSubtype` + removed the 3 now-unused `maxcompute.*` imports. Verified safe: `InitDatabaseLog.Type.MAX_COMPUTE` has no replay-dispatch use (self-ref only); `dbLogType` is not `@SerializedName` → handled identically to the shipped es/jdbc/trino DBs.
- **Adversarial verification fan-out (4 read-only agents) — 2 alarms adjudicated as non-issues:**
  - *`getMetaCacheEngine()` → "default" not "maxcompute"* = **false positive.** The plugin path loads schema via the connector (`PluginDrivenExternalTable.initSchema`) under the "default" bucket — exactly as shipped es/jdbc/trino tables (which never overrode it). `MaxComputeExternalMetaCache` is referenced only by legacy `MaxComputeExternalTable:71,122` (Batch-D dead code); partitions come from the connector (P4-T02). No override needed.
  - *`getMysqlType()` → "BASE TABLE" not null* = **consistent with accepted precedent.** Migrated ES tables already went null→"BASE TABLE" (`ES_EXTERNAL_TABLE` is absent from `TableType.toMysqlType`) and shipped with no override. MC matching is the same accepted change.
  - *dormancy ("a new MC catalog can't serialize in the T05↔flip window")* = the **already-documented** intermediate-state caveat above. The agent's suggested fix (keep `registerSubtype` too) is **wrong** — coexistence throws the duplicate-label IAE the atomic replace exists to avoid. No action.
- **Test:** `PluginDrivenExternalTableEngineTest` extended with 2 `max_compute` cases (engine = null; type name = `MAX_COMPUTE_EXTERNAL_TABLE`) — 9/9 green. Matches the file's existing Mockito helper (the §7 "no mockito" guidance is for new T06 files).
- **Gate (fe-core):** compile BUILD SUCCESS · checkstyle 0 · import-gate 0 · UT 9-0-0 (real BUILD/MVN_EXIT/CS_EXIT verified).

---

## 4. P4-T06 — live cutover

### 4.1 Dormant→live write wiring (the hard part — all dormant-safe, additive)

**W-a (G1) — bind a txn into the session.** `ConnectorSession`: add `default void setCurrentTransaction(ConnectorTransaction txn) { throw … }` (or no-op default + override). `ConnectorSessionImpl`: add a `volatile ConnectorTransaction currentTransaction` field + `setCurrentTransaction` + `@Override getCurrentTransaction()`. `PluginDrivenTableSink`: add `getConnectorSession()` getter (field exists `:114`, no getter today).

**W-b (DECISION-1) — capability signal.** Add `ConnectorWriteOps.usesConnectorTransaction()` default `false`; `MaxComputeConnectorMetadata` overrides `true`. The executor routes on this **before** touching any throwing-default write method. (Alternatives weighed in §5.)

**W-c (G2) — `PluginDrivenInsertExecutor` restructure** (mirrors legacy `MCInsertExecutor`, which returns `TransactionType.MAXCOMPUTE` `:81-82` and pulls the txn from the manager):
- Extract connector/session/writeOps setup into a helper; call it at the **start of `beginTransaction()`** (currently built in `beforeExec:71-76`).
- `beginTransaction()`:
  - txn-model: `connectorTx = writeOps.beginTransaction(execSession)` (`MaxComputeConnectorMetadata:264` → `new MaxComputeConnectorTransaction(session.allocateTransactionId())`); `txnId = ((PluginDrivenTransactionManager) transactionManager).begin(connectorTx)`.
  - else: `super.beginTransaction()` (unchanged `:87-89`).
- `finalizeSink()` (override): if `connectorTx != null && sink instanceof PluginDrivenTableSink`, `((PluginDrivenTableSink) sink).getConnectorSession().setCurrentTransaction(connectorTx)` **before** `super.finalizeSink(...)`.
- `beforeExec()` (override): `if (connectorTx != null) return;` (write session already created by `planWrite`; no `getWriteConfig`/`beginInsert`). JDBC path unchanged. `doBeforeCommit`/`onFail` already guard on `insertHandle != null` (`:108`,`:140`) → null for MC ⇒ correctly skipped.
- `transactionType()`: txn-model → `TransactionType.MAXCOMPUTE` (enum value exists; profiling-only, low-risk — note it's MC-specific in a generic executor, acceptable while MC is the sole txn-model adopter).

Two `ConnectorSession` instances exist (executor's, built for id-alloc; sink's, which planWrite reads) — the **txn is shared by reference** via W-a, so this is correct; a future simplification could unify them, out of scope here.

**W-d (G3) — global registration.** `PluginDrivenTransactionManager.begin(connectorTx)` `:71-77`: also `Env.getCurrentEnv().getGlobalExternalTransactionInfoMgr().putTxnById(txnId, theWrappedTxn)` (mirror `AbstractExternalTransactionManager.begin:42-48`). Verify `commit`/`rollback` (`:80-…`) `removeTxnById` (add if missing — legacy removes at `AbstractExternalTransactionManager:54`). Without this, both the block-alloc RPC and the BE commit-data feedback throw "Can't find txn."

### 4.2 Binding-time context: overwrite + static partition (G4+G5)

> ⚠️ **INCOMPLETE — corrected by P0-3 / FIX-BIND-STATIC-PARTITION ([D-030], 2026-06-07).** G4/G5 below
> only wired the static spec into `UnboundConnectorTableSink` and `PluginDrivenInsertCommandContext`
> (for the BE write-plan). They did **NOT** mirror the legacy **bind-time** handling in
> `BindSink.bindConnectorTableSink`: (a) excluding the static partition columns from the bound columns,
> and (b) projecting the child to **full-schema** order. So the "faithful generic mirror" claim was
> false — the very INSERT-PARTITION regression DECISION-3 promised to prevent was live (no-column-list
> static INSERT threw at bind; reordered/partial explicit lists silently mis-mapped columns). P0-3
> completes the mirror (gated by capability `SINK_REQUIRE_FULL_SCHEMA_ORDER`). See
> `reviews/P4-T06e-FIX-BIND-STATIC-PARTITION-review-rounds.md`.

Required so **INSERT OVERWRITE** and **INSERT … PARTITION(col=val)** keep working post-cutover (else a user-visible regression at the flip). Faithful generic mirror of the legacy MC path:
- **G4**: `UnboundConnectorTableSink` — add `staticPartitionKeyValues` (+ ctor variant), mirroring `UnboundMaxComputeTableSink`. `UnboundTableSinkCreator:66-110`: pass static partitions to the connector unbound sink for plugin-driven tables.
- **G5**: fill `PluginDrivenInsertCommandContext` (already has `staticPartitionSpec`+getter/setter from T04; `overwrite` inherited from `BaseExternalTableInsertCommandContext:24`):
  - `InsertIntoTableCommand` ~`:567-598`: mirror the MC branch `:564-581` — extract static spec from the unbound sink, `setStaticPartitionSpec(...)` on the (no-longer-empty) `PluginDrivenInsertCommandContext`.
  - `InsertOverwriteTableCommand` ~`:407-418`: add a plugin-driven branch — `setOverwrite(true)` + `setStaticPartitionSpec(...)` on `PluginDrivenInsertCommandContext`.
- Consumed by `PluginDrivenTableSink.bindViaWritePlanProvider:212-224` (reads `isOverwrite()` `:217` + `getStaticPartitionSpec()` `:218`).

### 4.3 The flip
- `CatalogFactory`: add `"max_compute"` to `SPI_READY_TYPES` (`:52`); delete `case "max_compute"` `:146-149` + now-unused import.
- This is the live switch. Keep it the **last** commit (§4.5).

### 4.4 R-004 — ODPS-SDK-under-plugin-classloader defensive test
Risk (risks.md R-004): "classloader 隔离打破 SDK 单例." Plugin isolation = `ConnectorPluginManager` + `ChildFirstClassLoader`, parent-first prefixes `org.apache.doris.connector.*` / `org.apache.doris.filesystem.*`. No in-repo harness loads a plugin **under its isolated classloader** (`FakeConnectorPluginTest` loads via the test classpath — does NOT exercise isolation). No ODPS endpoint/creds in the repo.

**Two separable concerns** — split the test accordingly:
1. **Isolation correctness (no creds, CI-runnable):** load the connector under a plugin-style classloader and instantiate the ODPS client (`MCConnectorClientFactory`, needs `mc.endpoint`/`mc.default.project`/auth) — assert **no `NoClassDefFoundError` / `ClassCastException` / SDK-singleton poisoning** when class-loading the ODPS SDK in isolation. This is the part that actually addresses R-004's "broken singleton" risk and can run without a live endpoint.
2. **Live connectivity (creds, user-run):** one trivial metadata call (e.g. `odps.projects().get(project).reload()` or `tables().exists`) against a real endpoint. **I author it; user runs it** (per sign-off; mirrors P0-T24/25). Credentials via env vars / system properties — never committed.

Cutover is declared complete only after the user reports (2) green; (1) lands as a normal connector UT.

### 4.5 Commit granularity
All of §4.1+§4.2 is **additive / dormant-safe** (only reachable once `max_compute` is in `SPI_READY_TYPES`). Recommended ordering inside T06: land write-wiring + binding-context + R-004-isolation-UT **first** (dormant), then the **flip** (§4.3) as the final, smallest, highest-signal commit. (DECISION-2: is this two commits `[P4-T06a]`/`[P4-T06b]`, or one `[P4-T06]`?)

---

## 5. Decisions (✅ all signed off 2026-06-06)

**DECISION-1 ✅ = (A)** `ConnectorWriteOps.usesConnectorTransaction()` flag. — capability signal for the txn-write model (W-b):
- **(A) `ConnectorWriteOps.usesConnectorTransaction()` flag, default false — CHOSEN.** Matches the SPI's existing capability style (`supportsInsert/Delete/Merge`, `supportsWriteBlockAllocation`); explicit; one default method; zero coupling; lets the executor branch *before* any throwing-default call.
- (B) Route on `connector.getWritePlanProvider() != null`. Zero new SPI, but couples "has a write-plan provider" with "uses a connector transaction" — loose; breaks for a future planWrite-but-autocommit connector.
- (C) Un-throw `getWriteConfig` for MC + add `ConnectorWriteType.MAXCOMPUTE` (or reuse `CUSTOM`); route on write-type. Reuses one SPI method conceptually, but reverses D-4, adds enum churn, and forces `getWriteConfig` to be called earlier. More moving parts (Rule 2 disfavors).

**DECISION-2 ✅ = two commits, flip last** (§4.5): `[P4-T06a]` = wiring/binding/R-004 isolation UT (dormant); `[P4-T06b]` = the SPI_READY_TYPES flip + delete CatalogFactory case. Flip isolated = easiest to review/revert.

**DECISION-3 ✅ = in the cutover (T06)** (§4.2): static-partition + overwrite binding lands with the cutover, avoiding an INSERT-OVERWRITE-PARTITION regression at the flip.

---

## 6. Risk analysis

| Risk | Mitigation |
|---|---|
| Flip breaks read/DDL/partition parity | Batch A+B already at parity (gate-green); flip only changes dispatch. Manual smoke per acceptance list. |
| Txn not registered → block-alloc / commit-feedback throw | W-d (G3) — mirror legacy `putTxnById`; UT asserts registration. |
| `planWrite` fail-loud if txn absent on sink session | W-a binding in `finalizeSink` before `bindDataSink`; UT for the executor ordering. |
| INSERT OVERWRITE / static partition regression | §4.2 (DECISION-3 = in cutover). |
| Intermediate (post-GSON, pre-flip) un-deployable state | Land T05+T06 close; don't deploy between (P2 precedent, §3). |
| R-004 SDK-singleton breakage under isolation | §4.4 part 1 (no-creds UT) + part 2 (user-run live). |
| MCInsertExecutor still reachable (double path) | OQ-1 — Batch D verifies it becomes dead code; cutover routes plugin-driven MC to `PluginDrivenInsertExecutor`. |

---

## 7. Test plan

**Unit (connector + fe-core, JUnit5 hand-doubles, no mockito):**
- `ConnectorSessionImpl` setCurrentTransaction/getCurrentTransaction round-trip.
- `PluginDrivenTransactionManager.begin(connectorTx)` registers in `GlobalExternalTransactionInfoMgr` (getTxnById returns it; commit/rollback removes).
- Executor ordering: txn-model `beginTransaction` creates+registers; `finalizeSink` binds onto sink session before `planWrite`; `beforeExec` skips `beginInsert`. (Fake connector with `usesConnectorTransaction()=true`.)
- Binding-context: INSERT OVERWRITE → `PluginDrivenInsertCommandContext.isOverwrite()==true`; PARTITION(col=val) → `getStaticPartitionSpec()` populated.
- R-004 part 1 (classloader-isolation, no creds).
- (Carries the P4-T10 write-txn golden / TBinaryProtocol round-trip already planned.)

**User-run / e2e:** R-004 part 2 (live ODPS connectivity). Manual smoke after flip: SELECT, CREATE/DROP TABLE+DB, SHOW PARTITIONS / partitions+partition_values TVF, INSERT, INSERT OVERWRITE [PARTITION]. (regression-test suite under `external_table_p2/maxcompute/` exists but needs a cluster+creds — same DV-003 constraint; defer/flag, do not silently skip.)

---

## 8. Ordered TODO

**P4-T05 (dormant):**
1. `GsonUtils:397/:472` atomic compat replace.
2. `PluginDrivenExternalTable` getEngine/getEngineTableTypeName `case "max_compute"`; comment on legacyLogTypeToCatalogType default.
3. Gate (fe-core): compile + checkstyle + import-gate (real BUILD/MVN_EXIT/CS_EXIT). Commit `[P4-T05]`.

**P4-T06 (live):**
4. W-a: `ConnectorSession.setCurrentTransaction` + `ConnectorSessionImpl` field/override + `PluginDrivenTableSink.getConnectorSession`.
5. W-b: `ConnectorWriteOps.usesConnectorTransaction()` + MC override (per DECISION-1).
6. W-c: `PluginDrivenInsertExecutor` restructure.
7. W-d: `PluginDrivenTransactionManager.begin(connectorTx)` global register + commit/rollback deregister.
8. §4.2: `UnboundConnectorTableSink` static spec + `InsertInto`/`InsertOverwrite` fill `PluginDrivenInsertCommandContext` (per DECISION-3).
9. R-004 part-1 UT; author R-004 part-2 (user-run).
10. UTs (§7). Gate `-pl :fe-connector-maxcompute,:fe-connector-api,:fe-core -am` compile + checkstyle + import-gate.
11. **Flip:** `CatalogFactory` SPI_READY_TYPES + delete case (`[P4-T06b]` or final part of `[P4-T06]`, per DECISION-2).
12. doc-sync (5 steps) + decisions-log (DECISION-1/2/3, the 2 SPI additions → E11).

---

## 9. Open questions / boundaries
- **Don't** re-open T03/T04 decisions (Approach A locked; planWrite reads `getCurrentTransaction`). This design wires *to* it.
- `transactionType()` for a generic txn-model executor returning `MAXCOMPUTE` is profiling-only and MC-is-sole-adopter-correct; revisit when a 2nd txn-model connector arrives.
- Batch D (post-cutover) still owns: exhaustive reverse-ref re-grep, deleting `datasource/maxcompute/`, verifying `MCInsertExecutor` dead (OQ-1).
