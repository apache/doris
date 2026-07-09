# Hudi `@incr` Incremental Read — connector step design (FE-only, no BE change)

Authoritative, code-grounded design for the incremental-read no-regression gap closure (the highest-risk
Group-C item). HEAD = branch `catalog-spi-11-hive`. Two recon workflows back this doc (all HEAD-verified):
- `w2ififdgn` / `wf_4b001028-a0c` — BE row-filtering verdict (the §5.1 landmine), port inventory.
- `wp3k0gcvx` / `wf_05d58d84-143` — FE-only neutral-SPI feasibility verdict.
Full recon output: `/tmp/claude-1000/.../tasks/w2ififdgn.output` and `.../wp3k0gcvx.output` (this session).

---

## 0. The problem (why incremental is the hard one)

A COW update **rewrites the whole base file**, copying unchanged older-commit rows forward. So selecting the
base files touched in a `(begin, end]` commit window is **not enough** — those files also contain
out-of-window rows. Correct incremental read needs a **row-level** filter `_hoodie_commit_time > begin AND
<= end`.

Legacy delivered that row filter with a **source-specific fe-core injection**:
- `CheckPolicy.java:83-88` — an analysis rule — wraps a `LogicalFilter` carrying
  `LogicalHudiScan.generateIncrementalExpression()` (`LogicalHudiScan.java:129-148`), gated on
  `relation instanceof LogicalHudiScan && getTable() instanceof HMSExternalTable`.
- The filter references the `_hoodie_commit_time` **meta column** (legacy hudi exposes all 5 `_hoodie_*` meta
  columns as visible), which materializes it into the scan tuple; BE reads it and applies the conjunct via its
  normal scan-conjunct machinery; a projection above drops it.

Post-flip, a hudi-on-HMS table is a generic `PluginDrivenExternalTable` → `LogicalFileScan` (`BindRelation.java:654`),
which matches **neither** `instanceof LogicalHudiScan` **nor** `HMSExternalTable`. So the legacy injection is
structurally dead, and the **iron rule forbids** re-adding a `dlaType==HUDI` / `PluginDrivenExternalTable`
arm to `CheckPolicy`/fe-core.

### §5.1 verdict — where row correctness can live (both recons, cross-verified by hand)

BE does **not** window-filter on its own, on any path:
- Native reader (`be/src/format/table/hudi_reader.cpp:29-83`, `format_v2/table/hudi_reader.cpp`) reads the whole
  base file; zero commit-time logic. `THudiFileDesc` (`PlanNodes.thrift:409-422`) has only a scalar
  `instant_time`, no begin/end window.
- JNI reader forwards every `hoodie.*` scan property to the Java scanner, but
  `HadoopHudiJniScanner.java:124-131` **drops** them (only lifts `hadoop_conf.*`), builds one
  `HoodieRealtimeFileSplit` at `instant_time`, and returns all rows.
- **BUT** both BE paths **do** apply pushed scan conjuncts (native: `table_reader.h:303-313` expression filters;
  JNI: `finalize_jni_block` `VExprContext::filter_block`, `format_v2/jni/hudi_jni_reader.cpp:159-162`).

⇒ Row correctness = **a `_hoodie_commit_time` conjunct must reach the scan node**. There are exactly two ways:
- **(A) BE-synthesized** — new BE code reads the window from a carrier and synthesizes+applies the filter. Rejected:
  touches C++/JNI in two reader trees + a thrift/protocol change; higher blast radius; **user does not want BE changes**.
- **(B) FE-only neutral SPI (CHOSEN)** — the connector supplies the predicate through a **neutral, connector-agnostic**
  SPI; a generic fe-core analysis rule injects it as a `LogicalFilter` on a **hidden** `_hoodie_commit_time` column
  it exposes. BE applies it with **zero BE change** (it already applies scan conjuncts). This is the "re-home
  source-specific fe-core logic into a neutral SPI" pattern used throughout this migration, and mirrors Trino's
  unenforced/residual-predicate model.

**Feasibility of (B): CONFIRMED (recon `wp3k0gcvx`).** The only non-obvious constraint: inject at the **logical
(analysis) layer**, NOT the physical scan node. A scan node cannot mint a slot for an unprojected column
(`generateTupleDesc` only loops `scan.getOutput()`, `PhysicalPlanTranslator.java:896-898` — no `addSlot`); but an
analysis-time `LogicalFilter` referencing a hidden column **does** force materialization (legacy proof). The window
is available at analysis time — `@incr` bounds ride `scanParams` on the plugin `LogicalFileScan` at bind time
(`BindRelation.java:654`), and the MVCC snapshot is resolved during analysis (`StatementContext.loadSnapshots`,
retrieved later by `PluginDrivenScanNode.pinMvccSnapshot`, `:755-759`).

---

## 1. The neutral SPI + fe-core mechanism (the row-correctness core)

### 1.1 Expose the 5 `_hoodie_*` meta columns as VISIBLE (connector-side, generic) — SIGNED D-C3-1
Per D-C3-1, the connector exposes all 5 hudi meta columns (`_hoodie_commit_time`, `_hoodie_commit_seqno`,
`_hoodie_record_key`, `_hoodie_partition_path`, `_hoodie_file_name`) as **visible** STRING columns in the hudi
schema (port legacy `getTableAvroSchema(true)` / `HMSExternalTable.initHudiSchema`). Being visible, they are in
the plugin scan output (`getFullSchema()` → `LogicalFileScan.computePluginDrivenOutput():214-224`) and bindable by
name — so `_hoodie_commit_time` is materializable by the injected filter (§1.4) with **no** hidden-column path
needed. `SELECT *`/`DESCRIBE` now match legacy. (The `ConnectorColumn.invisible()` /
`ConnectorColumnConverter.java:81-82` hidden path — iceberg v3 row-lineage's mechanism — is available but NOT
used here given D-C3-1.)

✅ **SIGNED D-C3-1 (visibility parity, user 2026-07-09) = (ii) all 5 `_hoodie_*` meta columns VISIBLE = full
legacy `SELECT *`/`DESCRIBE` parity** (the no-regression bar is legacy `HudiScanNode`). The connector's hudi
schema must expose `_hoodie_commit_time`, `_hoodie_commit_seqno`, `_hoodie_record_key`,
`_hoodie_partition_path`, `_hoodie_file_name` as visible columns (port legacy `HMSExternalTable.initHudiSchema`
meta-column set / `getTableAvroSchema(true)` — DV-008 gap-2). This also makes `_hoodie_commit_time` naturally
materializable for the incremental filter (no separate hidden-column path needed) and restores legacy
`SELECT _hoodie_commit_time`. Changes the current (dormant) connector's `SELECT *` to add these 5 columns — no
live regression (hms not yet in `SPI_READY_TYPES`). Because the columns are visible, §1.3/§1.4's slot binding
resolves them the same way; the `invisible()`/hidden-column path is NOT needed.

### 1.2 Neutral SPI: connector supplies a synthetic scan predicate
Add a neutral default to `ConnectorMetadata` (parent-first, default = none; fe-core NEVER discriminates by source):

```
// Connector may require an extra scan-level predicate (e.g. a CDC/incremental commit-time window) that the
// engine must apply. Default = empty. iceberg/paimon/jdbc/... inherit empty → plans byte-identical.
default List<ConnectorExpression> getSyntheticScanPredicates(
        ConnectorSession session, ConnectorTableHandle handle, TableScanParams scanParams) {
    return List.of();
}
```
- Returns **`ConnectorExpression`** (the existing pushdown expression type — `ConnectorColumnRef` + `ConnectorLiteral.ofString`
  + `ConnectorComparison{GT,LE}` + `ConnectorAnd` already express `_hoodie_commit_time > 'begin' AND <= 'end'`,
  recon-confirmed). This keeps the SPI **general and Trino-aligned** (a connector residual predicate), not a
  hudi-shaped struct.
- Hudi connector resolves `(begin, end]` from `scanParams` (resolving an omitted `endTime` via its own
  metaClient, exactly as legacy `withScanParams` does — inside `HudiMetaClientExecutor.execute` for TCCL/auth),
  and returns the range `ConnectorExpression` over `_hoodie_commit_time`. Non-incremental scans → empty.
- **Simpler fallback if the reverse converter proves heavy:** a `SyntheticScanPredicate{colName, lowerExclusive,
  upperInclusive}` struct + fe-core builds native Nereids exprs directly (no converter). Recorded as fallback;
  primary is the general `ConnectorExpression` form the user asked for.

### 1.3 fe-core: reverse `ConnectorExpression → Nereids Expression` (bounded, new)
No reverse converter exists (only forward `Expr/Nereids → ConnectorExpression`:
`ExprToConnectorExpressionConverter`, `NereidsToConnectorExpressionConverter`). Build a bounded reverse for the
shape {ColumnRef, StringLiteral, GT/LE/…comparisons, And/Or} → Nereids `SlotReference`/`StringLiteral`/
`GreaterThan`/`LessThanEqual`/`And`, binding `ConnectorColumnRef.name` → the scan output's `SlotReference` by name
(the scan's `getLogicalProperties().getOutput()`). ~150–250 LOC, connector-agnostic, reusable.

### 1.4 fe-core: neutral analysis rule (the injection locus)
A neutral rule (either a new analysis rule on `LogicalFileScan`, or a neutral hook **replacing** the
`instanceof LogicalHudiScan` branch at `CheckPolicy.java:83-88`) that, for any plugin `LogicalFileScan` with
`scanParams`:
1. calls `connector.getSyntheticScanPredicates(session, handle, scanParams)`;
2. converts each `ConnectorExpression` → bound Nereids `Expression` (§1.3), resolving column refs against the
   scan output; **no-op if the named slot is absent** (legacy `timeField==null` short-circuit,
   `LogicalHudiScan.java:140-142`);
3. wraps a `LogicalFilter` over the returned conjuncts.

iceberg/paimon return empty → **no filter added, plan byte-identical**. This is the iron-rule guarantee: fe-core
calls the SPI unconditionally; only hudi answers. Keep both bounds **string-typed** (`StringLiteral`) — a numeric
coercion would silently change lexicographic instant comparison.

**Dormancy:** pre-flip, legacy hudi still binds to `LogicalHudiScan` and the OLD `CheckPolicy` branch serves it;
the new neutral SPI returns empty for every live plugin connector (iceberg/paimon), so the rule adds nothing.
Do NOT delete the old `CheckPolicy:83-88` branch until the flip (it is live for legacy hudi).

### 1.5 Flip-time (NOT now)
Remove the incremental/time-travel `throw` in `visitPhysicalHudiScan` (`PhysicalPlanTranslator.java:909-912`) — it
is dead for the plugin path anyway — and delete the legacy `CheckPolicy:83-88` hudi branch + `LogicalHudiScan` +
`datasource/hudi/source/*` alongside the rest of the legacy hudi deletion.

---

## 2. Connector-side incremental read (the split-selection port)

Independent of the row-filter mechanism; this is the "which files" half, all in `fe-connector-hudi`, dormant.

### 2.1 `resolveTimeTravel(INCREMENTAL)` + `applySnapshot` (extend HD-C2 spine)
- `resolveTimeTravel` currently returns `Optional.empty()` for `INCREMENTAL` (`HudiConnectorMetadata.java:303-304`).
  Add the `INCREMENTAL` case: parse `spec.getIncrementalParams()` (begin/end), resolve the window against the
  completed timeline **inside `HudiMetaClientExecutor.execute`** (TCCL-pin+auth), and return a
  `ConnectorMvccSnapshot` that (a) pins schema/freshness at **LATEST** (mirror paimon; empty table → −1) and
  (b) carries begin/end + a mode marker via new internal property keys (mirror `HUDI_QUERY_INSTANT_PROPERTY`,
  `:85-89`), e.g. `HUDI_BEGIN_INSTANT_PROPERTY` / `HUDI_END_INSTANT_PROPERTY` / `HUDI_INCREMENTAL_MODE`.
  **Never return empty for a valid window** — `PluginDrivenMvccExternalTable.loadSnapshot` fail-loud
  `notFoundMessage` has no INCREMENTAL arm (`:348-350`), so empty → wrong-domain error.
- `applySnapshot` reads those properties and stamps `beginInstant`/`endInstant`(+mode) onto `HudiTableHandle`
  via `toBuilder()` (preserve `prunedPartitionPaths`, as HD-C2 did). Absent → handle unchanged (snapshot path
  byte-identical).

### 2.2 Port the IncrementalRelation family into the connector
| Legacy (`fe-core datasource/hudi/source/`) | Action | Re-home |
|---|---|---|
| `IncrementalRelation` (interface) | Port as connector-internal interface | keep shape (`collectSplits`/`collectFileSlices`/`getStartTs`/`getEndTs`/`getHoodieParams`) |
| `COWIncrementalRelation` (`:74-240`) | Port | `LocationPath`, `TableFormatType.HUDI`, `HudiPartitionUtils.parsePartitionValues`, `spi.Split`/`HudiSplit`→`HudiScanRange` |
| `MORIncrementalRelation` (`:64-205`) | Port; **fix** the `:92` latent bug (`LATEST_TIME.equals(latestTime)` should test `endTimestamp`; COW does it right at `:98`) | `spi.Split` |
| `EmptyIncrementalRelation` (`:29-71`) | Port; short-circuit to empty split list (its `incr.operation`/`includeStartTime` keys are inert now — BE isn't the row filter) | — |
| `LogicalHudiScan.withScanParams` driver (`:232-281`) | **Re-implement** (HMSExternalTable-coupled) inside `resolveTimeTravel` using the connector's own metaClient/storage plumbing (HD-C1/C2) | — |
| `HudiScanNode.getIncrementalSplits` gating (`:381-403,468-484,556-568`) | **Re-implement** in `planScan` onto existing `collectCowSplits`/`collectMorSplits` | — |
| `CheckPolicy`/`generateIncrementalExpression` row filter | **Do NOT port to fe-core** — becomes the neutral SPI of §1 | — |

### 2.3 `planScan` incremental branch
When the handle carries a window: branch to incremental split enumeration — COW base files over the range
(native OK), MOR merged file-slices at `endTs` (JNI, `THudiFileDesc.instant_time=endTs`). Then:
- **RO-as-RT quirk (single connector locus):** a COW `_ro` table with serde `hoodie.query.as.ro.table=true`
  (name endsWith `_ro`) → treat as MOR for incremental. Legacy duplicates this in two places
  (`LogicalHudiScan.java:251-260` + `HudiScanNode.java:186-199`); collapse to ONE check in
  `HudiScanPlanProvider` split-selection.
- **`fallbackFullTableScan` degrade:** check `shouldFullTableScan()` **before** calling the ported
  `collectSplits`/`collectFileSlices` (which throw when `fullTableScan`); on true, fall through to the normal
  latest-snapshot partition scan (not error).
- **Scan-node properties:** with the FE-filter approach, BE ignores hoodie incremental params, so **do NOT** emit
  `hoodie.datasource.read.begin/end.instanttime` (the connector selects files; fe-core injects the row filter).
  This is simpler than the BE-carrier design.
- `@incr` lists **LATEST** partitions + **LATEST** schema (do NOT pin schema for incremental).

---

## 3. Iron-rule & correctness landmines
1. **No hudi branch in fe-core.** The neutral rule keys on the SPI returning empty for non-hudi; the only real
   relocation is `CheckPolicy:83-88` → neutral SPI (flip-time). Column exposure via `invisible()` is already clean.
2. **Empty-window / slot-absent over-read.** SPI returns empty for non-incremental scans; the rule no-ops if the
   named slot is absent (legacy `:140-142`).
3. **String typing.** Keep bounds `StringLiteral`; lexicographic instant compare (Hudi instants are fixed-width sortable).
4. **MOR JNI required_fields.** The hidden `_hoodie_commit_time` materializes as a REGULAR file slot (via the
   filter reference) → flows through `FileQueryScanNode` required-slots automatically. Assert in MOR e2e.
5. **Materialize-then-discard.** Projection above the scan drops the hidden column when unselected (matches legacy).
6. **TCCL wrapping.** Any new metaClient/timeline touch from `resolveTimeTravel`/the SPI runs on the metadata
   thread → wrap in `HudiMetaClientExecutor.execute`. `planScan` is already pinned.
7. **MOR `endTs` bug** (`MORIncrementalRelation.java:92`) — fix on port.

---

## 4. Proposed dormant-commit decomposition (each independently committable + same-loader unit test)
- **INC-1 — handle pin spine + `resolveTimeTravel(INCREMENTAL)` + `applySnapshot`** (connector, dormant). Window
  resolution → LATEST-pinned snapshot carrying begin/end; handle stamped. *Test:* non-empty snapshot with window
  props; handle carries window + preserves pruning; never empty for a valid window.
- **INC-2 — port IncrementalRelation family** (connector, dormant). COW/MOR/Empty + interface; re-home helpers;
  fix `MOR:92`. *Test:* start/end resolution, commit-range selection, Empty path, throw-on-fullTableScan.
- **INC-3 — incremental `planScan`** (connector, dormant). COW/MOR/RO-as-RT/fallback branch; no hoodie params
  emitted. *Test:* split set + fallback degrades to snapshot path (not throw) + RO-as-RT routes MOR.
- **INC-4 — neutral synthetic-predicate SPI + fe-core rule + reverse converter + hidden `_hoodie_commit_time`**
  (fe-core + connector, dormant-in-effect). *Test:* SPI empty for iceberg/paimon (plan unchanged); hudi returns
  range expr; rule wraps `LogicalFilter`; converter builds bound Nereids exprs; hidden column not in `SELECT *`.
- **INC-5 — flip (live)** — remove the `visitPhysicalHudiScan` incremental throw; delete legacy `CheckPolicy`
  hudi branch + legacy hudi source. *Closing e2e:* §5 below.

## 5. Flip-time e2e (the literal correctness gate; per memory `hms-iceberg-delegation-needs-e2e`)
COW + MOR tables, each ≥3 commits; `SELECT ... incr('beginTime'=c1,'endTime'=c2)` returns EXACTLY the rows written
in `(c1, c2]` — **including the linchpin case where a base file touched in the window also carries forward
older-commit rows** (proves the `_hoodie_commit_time` filter, not just file selection). Plus: RO-as-RT table,
`endTime='latest'` sentinel, empty-timeline (Empty relation), fallback-full-table-scan (archived instant). Assert
byte-parity vs the legacy HMS hudi catalog on identical data.

## 6. Decisions
- **D-C3-1 (visibility parity) — SIGNED (user 2026-07-09) = all 5 `_hoodie_*` meta columns VISIBLE (legacy parity).** (§1.1)
- **SPI return shape** — general `ConnectorExpression` (recommended, Trino-aligned) vs simple string-bounds struct
  (cheaper fallback). Implementer's call unless the reverse converter proves heavier than ~250 LOC. (§1.2)
- **Overall architecture — SIGNED (user 2026-07-09) = FE-only neutral SPI, NO BE change** (user requirement;
  recon `wp3k0gcvx` confirmed feasible + simpler/safer than BE). (§0/§1)
