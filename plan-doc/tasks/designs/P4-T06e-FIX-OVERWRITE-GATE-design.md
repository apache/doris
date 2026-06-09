# FIX-OVERWRITE-GATE (P4-T06e) — design

> 7th cutover-fix. Scope: fe-core only. Single-gate change. Surgical (Rule 3).
> Source: clean-room re-review NG-1 (`plan-doc/reviews/P4-maxcompute-full-rereview-2026-06-07.md`,
> §NG-1 / §C domain-6 / §E). High confidence; live e2e is the real truth-gate.

## Problem

After the MaxCompute SPI cutover, a MaxCompute table is a `PluginDrivenExternalTable`
(`TableType.PLUGIN_EXTERNAL_TABLE`). `INSERT OVERWRITE` into such a table is rejected before any
write work begins, by the gate in `InsertOverwriteTableCommand`.

Current gate (`InsertOverwriteTableCommand.java:315-323`):

```java
private boolean allowInsertOverwrite(TableIf targetTable) {
    if (targetTable instanceof OlapTable || targetTable instanceof RemoteDorisExternalTable) {
        return true;
    } else {
        return targetTable instanceof HMSExternalTable
                || targetTable instanceof IcebergExternalTable
                || targetTable instanceof MaxComputeExternalTable;
    }
}
```

Caller (`InsertOverwriteTableCommand.java:142-148`):

```java
// check allow insert overwrite
if (!allowInsertOverwrite(targetTableIf)) {
    String errMsg = "insert into overwrite only support OLAP/Remote OLAP and HMS/ICEBERG table."
            + " But current table type is " + targetTableIf.getType();
    LOG.error(errMsg);
    throw new AnalysisException(errMsg);
}
```

`PluginDrivenExternalTable` matches none of the listed types, so `run()` throws:

```
AnalysisException: insert into overwrite only support OLAP/Remote OLAP and HMS/ICEBERG table.
But current table type is PLUGIN_EXTERNAL_TABLE
```

(`targetTableIf.getType()` for a `PluginDrivenExternalTable` is `PLUGIN_EXTERNAL_TABLE`, set in its
ctor `PluginDrivenExternalTable.java:71` — verified.)

`cutover↔legacy`: legacy `MaxComputeExternalTable` matched the last `instanceof` and passed the gate,
so `INSERT OVERWRITE` executed. Post-cutover the same command throws before reaching the (fully
wired) write machinery.

## Root Cause

`allowInsertOverwrite` is a pure `instanceof` allow-list of *legacy* table classes. The cutover
replaced the concrete `MaxComputeExternalTable` with the generic `PluginDrivenExternalTable`, but
this gate was never extended to recognise the generic SPI table type. The lower OVERWRITE machinery
*was* extended (it already has a `UnboundConnectorTableSink` branch — see below), so this is a
classic "dispatch only half-wired": the entry gate rejects what the body already supports.

### The lower machinery is already complete (one-gate change confirmed)

Once the gate passes, the path is fully wired for the plugin/connector case:

1. `run()` (`:157-160`) calls `InsertUtils.normalizePlan(...)`. For a `PluginDrivenExternalCatalog`,
   the parsed `INSERT OVERWRITE` plan is an `UnboundConnectorTableSink`
   (`UnboundTableSinkCreator.java:68-69, :108-110, :149-151` all map
   `curCatalog instanceof PluginDrivenExternalCatalog` → `UnboundConnectorTableSink`;
   `InsertUtils.normalizePlan` handles it at `InsertUtils.java:609-610`).
2. The non-OLAP branch at `run()` `:215-218` sets `partitionNames = []` (FE does not create temp
   partitions for external tables), and the flow enters `insertIntoPartitions(...)` via `:241-279`.
3. `insertIntoPartitions` (`:345-444`) dispatches on the sink type. The
   `UnboundConnectorTableSink` branch (`:420-440`) rebuilds the sink, creates a
   `PluginDrivenInsertCommandContext`, sets `overwrite=true`, and copies the static-partition spec
   from `sink.getStaticPartitionKeyValues()`. This is the genuine OVERWRITE wiring — it just is
   never reached today.

So the fix is a single gate edit. No change to the body, the sink, the context, or the translator.

## Design

### The change

Add a `PluginDrivenExternalTable` branch to `allowInsertOverwrite`:

```java
private boolean allowInsertOverwrite(TableIf targetTable) {
    if (targetTable instanceof OlapTable || targetTable instanceof RemoteDorisExternalTable) {
        return true;
    } else {
        return targetTable instanceof HMSExternalTable
                || targetTable instanceof IcebergExternalTable
                || targetTable instanceof MaxComputeExternalTable
                || targetTable instanceof PluginDrivenExternalTable;
    }
}
```

### Predicate choice — `instanceof PluginDrivenExternalTable` (Rule 7, Rule 2)

The re-review (§NG-1 处置) phrased the predicate as "key on the SPI generic type; whether OVERWRITE
is supported is decided by whether downstream produces an `UnboundConnectorTableSink`." Examining the
actual code, those two phrasings collapse to the *same* predicate:

- `UnboundTableSinkCreator` produces an `UnboundConnectorTableSink` **iff**
  `curCatalog instanceof PluginDrivenExternalCatalog` (`:68`, `:108`, `:149`) — there is **no**
  capability flag or table-level toggle in that decision.
- A `PluginDrivenExternalTable` always belongs to a `PluginDrivenExternalCatalog` (its ctor and all
  metadata accessors cast `catalog` to `PluginDrivenExternalCatalog`).
- Therefore "table is `PluginDrivenExternalTable`" ⇔ "downstream produces `UnboundConnectorTableSink`"
  ⇔ "the `:420-440` OVERWRITE branch will fire". The `instanceof` is the faithful, minimal encoding
  of the report's "downstream produces UnboundConnectorTableSink" criterion.

**Alternative considered — capability-gated** (`ConnectorCapability.SUPPORTS_INSERT`):
`ConnectorCapability` already exists and has `SUPPORTS_INSERT` (`ConnectorCapability.java:30`), and
`PluginDrivenExternalTable.supportsParallelWrite()` (`:78-85`) shows the established pattern for
reading capabilities. We could gate the branch on
`((PluginDrivenExternalCatalog) catalog).getConnector().getCapabilities().contains(SUPPORTS_INSERT)`.

Rejected for this fix, because:
1. **It would not match the current contract.** No other downstream component (the sink creator, the
   BindSink connector branch, `InsertUtils`) consults `SUPPORTS_INSERT` before producing/binding a
   connector sink. Gating *only* the OVERWRITE gate on the capability would make OVERWRITE stricter
   than plain INSERT, which is inconsistent and surprising. A capability check, if wanted, belongs in
   the sink-creation layer (`UnboundTableSinkCreator`) so that INSERT and OVERWRITE share it — that
   is a separate, broader change, out of scope for a regression fix.
2. **Rule 2 (simplicity) / Rule 11 (match conventions).** Every other arm of `allowInsertOverwrite`
   is a bare `instanceof` (OlapTable / RemoteDoris / HMS / Iceberg / MaxCompute — `:316-321`); none
   gates on a capability or write-support flag. A bare `instanceof PluginDrivenExternalTable` matches
   the surrounding style exactly. If the underlying connector genuinely cannot write, the failure
   surfaces deterministically deeper in the write path (BE / connector sink), exactly as it would for
   plain INSERT today — the gate is not the right place to pre-empt that.
3. **The report's literal criterion is the `UnboundConnectorTableSink`, not a capability** — and that
   is what `instanceof PluginDrivenExternalTable` encodes (see above equivalence).

**Recommendation:** `instanceof PluginDrivenExternalTable`. This is the simplest predicate that is
*correct against the actual downstream dispatch* and consistent with both the existing arms of this
method and the FIX-PART-GATES decision① principle ("key on the SPI type, do not over-broaden"). Note
the contrast with FIX-PART-GATES decision①: there the override was *shared* by jdbc/es/trino/MC, so
an unconditional `true` would have flipped non-MC behavior, and the predicate had to be narrowed
(`!getPartitionColumns().isEmpty()`). Here the predicate already *is* type-scoped — `instanceof
PluginDrivenExternalTable` only fires for plugin tables — and the downstream is uniformly wired for
all of them, so no further narrowing is warranted or beneficial.

### Shared-override / blast-radius note (jdbc/es/trino)

`allowInsertOverwrite` is **not** an override shared across table classes — it is a private method of
`InsertOverwriteTableCommand` keyed on `instanceof`. Adding the branch only changes behavior for
tables that are `PluginDrivenExternalTable` (jdbc, es, trino-connector, max_compute after cutover).
For jdbc/es/trino this *enables* the OVERWRITE entry gate where it was previously rejected — but the
downstream is identical for all of them: they all flow through the same `UnboundConnectorTableSink` →
`PluginDrivenInsertCommandContext(overwrite=true)` branch (`:420-440`). If a given connector cannot
actually perform an overwrite, it fails at the connector/BE write layer with a connector-specific
error, exactly as a plain INSERT into a write-incapable connector does today. The gate is not the
behavioral firewall for "can this connector write" — the connector itself is. This is the same
semantics legacy had: legacy gated only on `instanceof MaxComputeExternalTable` because MC was the
only connector-style table; the generic replacement is `instanceof PluginDrivenExternalTable`.

## Implementation Plan

**File:** `fe/fe-core/src/main/java/org/apache/doris/nereids/trees/plans/commands/insert/InsertOverwriteTableCommand.java`

**Method:** `allowInsertOverwrite(TableIf)` (`:315-323`).

**Edit** — append one `instanceof` arm to the `else` return (`:319-321`):

```java
        return targetTable instanceof HMSExternalTable
                || targetTable instanceof IcebergExternalTable
                || targetTable instanceof MaxComputeExternalTable
                || targetTable instanceof PluginDrivenExternalTable;
```

**Import to add:**
`import org.apache.doris.datasource.PluginDrivenExternalTable;`

**Import placement / checkstyle (CustomImportOrder: doris → third-party → java; UnusedImports;
LineLength 120):** the new import is in the `org.apache.doris.datasource.*` block. The existing block
(`:29-33`) is:

```
import org.apache.doris.datasource.doris.RemoteDorisExternalTable;
import org.apache.doris.datasource.doris.RemoteOlapTable;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.datasource.iceberg.IcebergExternalTable;
import org.apache.doris.datasource.maxcompute.MaxComputeExternalTable;
```

`org.apache.doris.datasource.PluginDrivenExternalTable` (package `datasource`, no sub-package) sorts
*before* `org.apache.doris.datasource.doris.RemoteDorisExternalTable` lexicographically (`.P` <
`.doris` — uppercase ASCII before lowercase). Insert it as the **first** line of that block, i.e.
immediately before `:29`. (Confirm against `checkstyle:check`; if the project's import comparator is
case-insensitive the line may instead sort after the `maxcompute` line — let checkstyle dictate the
exact slot.)

No other edits. The branch body and all downstream wiring are unchanged.

## Risk Analysis

- **Blast radius — non-MC plugin connectors (jdbc/es/trino):** This change lets jdbc/es/trino-backed
  `PluginDrivenExternalTable`s pass the OVERWRITE *entry* gate. Pre-cutover those were legacy
  `JdbcExternalTable` / `EsExternalTable` / `TrinoConnectorExternalTable`, which were **never** in
  `allowInsertOverwrite` — so for them this is a *new* code path being opened, not a parity
  restoration. Mitigation/justification: the downstream is uniform (all plugin catalogs produce
  `UnboundConnectorTableSink`), and a connector that cannot overwrite fails deterministically at the
  bind/BE layer with a connector-specific error — the same place plain INSERT fails for a
  write-incapable connector. The gate is intentionally not the per-connector write-capability check.
  If product wants OVERWRITE locked down per-connector, that belongs in `UnboundTableSinkCreator`
  (shared by INSERT + OVERWRITE), not here — flagged, out of scope.
- **Batch-D red-line interaction (🔴):** Batch-D plans to delete the legacy
  `instanceof MaxComputeExternalTable` arm (`:321`) from this method
  (`P4-batchD-maxcompute-removal-design.md` §2, file row for `InsertOverwriteTableCommand.java`).
  That deletion is safe **only after** this fix adds the `PluginDrivenExternalTable` arm — otherwise
  the gate loses *all* coverage for MaxCompute tables (legacy class is gone post-cutover anyway, and
  the generic arm would not yet exist) and `INSERT OVERWRITE` breaks permanently. Ordering: this fix
  must land *before* the Batch-D delete-branch edit for `:321`. **Doc-sync flag below.**
- **What can still fail at BE/live (real truth-gate):** This fix only proves the FE entry gate is
  passable. The actual OVERWRITE execution (static-partition spec honored, partition replace
  semantics, affected-rows, MC `INSERT OVERWRITE` vs `INSERT INTO ... OVERWRITE` mapping) is BE +
  connector + ODPS, and per the re-review (§NG-1 note, §E#5/#6) the truth-gate is **live e2e against
  real ODPS, which CI skips**. The re-review also flags adjacent write blockers (NG-2/NG-4 dynamic
  partition GATHER/local-sort, NG-3 static-partition bind) that this fix does *not* address — a green
  gate here does not imply a green end-to-end OVERWRITE until those are fixed and run live.
  This fix is necessary-but-not-sufficient for working OVERWRITE; it removes the first (FE) blocker.

## Test Plan

### Unit Tests

**Location:** new test class
`fe/fe-core/src/test/java/org/apache/doris/nereids/trees/plans/commands/insert/InsertOverwriteTableCommandTest.java`
(no existing unit test targets `allowInsertOverwrite` — verified; this is the package home of the
command under test).

`allowInsertOverwrite(TableIf)` is `private` and field-independent (it inspects only its argument),
so invoke it directly via the project's established private-method test helper
`org.apache.doris.common.jmockit.Deencapsulation.invoke(instance, "allowInsertOverwrite", table)`
(pattern: `transaction/TableStreamOffsetTransactionTest.java:112`). Construct the command with any
minimal `LogicalPlan` (the ctor only requires a non-null `logicalQuery`; `allowInsertOverwrite` never
touches it — use a mock/stub `LogicalPlan` or a trivial unbound sink). Build the
`PluginDrivenExternalTable` with the mock-catalog pattern from
`PluginDrivenExternalTablePartitionTest` (`TestablePluginCatalog("max_compute", ...)` + a
`PluginDrivenExternalTable` anonymous subclass that no-ops `makeSureInitialized()`), so no Doris env
is required.

**Test 1 — `allowInsertOverwriteAcceptsPluginDrivenTable` (the Rule-9 red-before / green-after test):**
- Arrange: a `PluginDrivenExternalTable` backed by a `max_compute` `TestablePluginCatalog`.
- Act: `boolean allowed = Deencapsulation.invoke(cmd, "allowInsertOverwrite", table);`
- Assert: `assertTrue(allowed, "INSERT OVERWRITE into a cutover MaxCompute (PluginDrivenExternalTable) must pass the gate; legacy MaxComputeExternalTable did")`.
- **Why it encodes intent (Rule 9):** it asserts the *business* invariant "cutover MaxCompute tables
  retain INSERT OVERWRITE support that legacy had" — not merely "method returns a boolean".
- **Mutation / red-before proof:** remove the new `|| targetTable instanceof PluginDrivenExternalTable`
  arm (i.e. revert the production change) → `allowInsertOverwrite` returns `false` for a
  `PluginDrivenExternalTable` → this assertion goes **red**. With the arm present it is green. This is
  the loop's red-before/green-after gate.

**Test 2 (optional parity guard) — `allowInsertOverwriteStillRejectsUnsupportedType`:**
- Arrange: a `TableIf` that is none of the allow-listed types (e.g. a mock `TableIf`, or any
  internal table type not in the list).
- Assert: `assertFalse(...)`.
- **Why:** pins that the new arm did not accidentally broaden to "all external tables" — a mutation
  that replaced the targeted `instanceof` with an unconditional `true` would make this red. Keeps the
  predicate honest (Rule 9 — the test can fail if the gate logic is loosened).

**Optional integration-style assertion (only if cheap):** if a `run()`-level test can be stood up
that asserts the *exact pre-fix exception message*
(`"...But current table type is PLUGIN_EXTERNAL_TABLE"`) is **no longer thrown** for a plugin table,
it documents the user-visible symptom. This is heavier (needs more of `run()`'s collaborators) and is
not required — Test 1 already gives the deterministic red-before/green-after gate. Prefer Test 1 +
Test 2 for the loop.

**Out of scope for this loop (state explicitly):** end-to-end `INSERT OVERWRITE` execution against
real ODPS (`external_table_p2/maxcompute/*`). Per §Risk, that is the real truth-gate but requires
live credentials and is CI-skipped; it is not part of this fix's unit-test loop.

---

# Round 2 revision (2026-06-07) — narrow predicate via SPI capability (user decision = Option A)

**Why revised:** round-1 clean-room adversarial review (`w5ke8sjaq`) confirmed (2/2) that the bare
`instanceof PluginDrivenExternalTable` predicate also admits **JDBC** (which is `PluginDrivenExternalTable`
post-cutover, `supportsInsert()=true` but `getWriteConfig` never propagates the overwrite flag) →
`INSERT OVERWRITE` **silently degrades to a plain INSERT (data loss)**. Before this fix JDBC overwrite
failed *loud* (rejected at the gate); the bare predicate makes the silent-loss path newly reachable —
a regression this fix introduces, forbidden by Rule 12. ES/Trino (`supportsInsert()=false`) are not a
data bug (they already fail loud downstream) but are also newly admitted then fail with a *generic*
"does not support INSERT" message. The original design consciously deferred this ("the gate is not the
per-connector write firewall"); the review evidence + Rule 12 overrule that deferral. See
`plan-doc/reviews/P4-T06e-FIX-OVERWRITE-GATE-review-rounds.md` Round 1.

**Decision (user, 2026-06-07): Option A — add an SPI capability.** Generic, SPI-aligned, fail-loud at
the gate for non-overwrite connectors, future connectors opt-in.

**Changes:**
1. `ConnectorWriteOps.java` — add `default boolean supportsInsertOverwrite() { return false; }` right
   after `supportsInsert()` (capability-query group). Default false = connectors that support plain
   INSERT but not overwrite stay rejected, so callers fail loud instead of silently appending.
2. `MaxComputeConnectorMetadata.java` — `@Override public boolean supportsInsertOverwrite() { return true; }`
   (MaxCompute genuinely honors overwrite: `MaxComputeWritePlanProvider:167` `builder.overwrite(true)`).
3. `InsertOverwriteTableCommand.java` — narrow the new arm to
   `targetTable instanceof PluginDrivenExternalTable && pluginConnectorSupportsInsertOverwrite((PluginDrivenExternalTable) targetTable)`,
   helper queries the connector capability via the established access pattern
   (`catalog.getConnector().getMetadata(catalog.buildConnectorSession()).supportsInsertOverwrite()`,
   mirroring `PhysicalPlanTranslator:657-686`). Extra import: `PluginDrivenExternalCatalog` (no
   Connector/ConnectorMetadata/ConnectorSession imports — method-chained). Short-circuit `&&` means the
   connector is only touched for PluginDriven tables (OlapTable etc. return early).
4. Error message (round-1 finding #3) — update the reject message so it is no longer misleading
   (it omitted MaxCompute/plugin types).
5. Test (round-1 finding #4) — replace the tautological `mock(TableIf.class)`-only negative with a
   concrete capability-gated suite: (a) overwrite-capable PluginDriven → allowed; (b) **non-overwrite-capable
   PluginDriven (JDBC-like, `supportsInsertOverwrite()=false`) → rejected** (the regression guard;
   mutation: drop `&& supportsInsertOverwrite` → returns true → red); (c) unsupported `TableIf` → rejected.

**Blast radius after revision:** JDBC/ES/Trino now rejected AT the gate with a clear message (matches
legacy product behavior — none were ever in the overwrite allow-list), zero silent data loss; MaxCompute
restored to parity. The pre-existing JDBC `getWriteConfig` overwrite-flag gap is left for a separate
ticket (now unreachable for overwrite, so no live regression).

---

# Outcome (2026-06-07) — DONE, 2 rounds

Round-1 fix (bare `instanceof`) failed adversarial review (clean-room `w5ke8sjaq`): introduced a JDBC
silent overwrite→plain-INSERT data-loss path (Rule 12). Round-2 fix (Option A, SPI capability
`supportsInsertOverwrite()`) converged: round-2 review `wo81wbi7x` returned **0 surviving findings**, all
4 round-1 findings closed, test non-vacuous, no historical contradiction. fe-core + 2 connector modules
compile, UT 3/3, mutation-verified (revert→regression-guard test reds). See
`plan-doc/reviews/P4-T06e-FIX-OVERWRITE-GATE-review-rounds.md` for the full round log.
**Truth-gate remaining:** live `INSERT OVERWRITE` e2e against real ODPS (CI-skipped) + the adjacent
write blockers P0-2/P0-3.
