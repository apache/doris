# FIX-WRITE-DISTRIBUTION (P4-T06e, P0-2) — design

> 8th cutover-fix. Scope: fe-core (planner sink + plugin table) + fe-connector-api (1 enum
> value) + fe-connector-maxcompute (1 capability override). Surgical (Rule 3).
> Source: clean-room re-review NG-2 / NG-4 (= F17 / F18 / F43)
> (`plan-doc/reviews/P4-maxcompute-full-rereview-2026-06-07.md`, §A.NG-2/NG-4, §C domain-2/6, §E#2/#4).
> High confidence; **live e2e against real ODPS is the real truth-gate** (CI-skipped).

## Problem

After the MaxCompute SPI cutover, a MaxCompute write goes through the generic
`PhysicalConnectorTableSink` instead of legacy `PhysicalMaxComputeTableSink`. The generic sink's
`getRequirePhysicalProperties()` collapses *all* write distribution to a single boolean:

```java
// PhysicalConnectorTableSink.java:114-121 (current)
@Override
public PhysicalProperties getRequirePhysicalProperties() {
    if (targetTable instanceof PluginDrivenExternalTable
            && ((PluginDrivenExternalTable) targetTable).supportsParallelWrite()) {
        return PhysicalProperties.SINK_RANDOM_PARTITIONED;
    }
    return PhysicalProperties.GATHER;
}
```

`MaxComputeDorisConnector` declares **no** capabilities (`getCapabilities()` inherits the empty
default — verified `MaxComputeDorisConnector.java`, no override), so `supportsParallelWrite()` is
**false** → every MaxCompute write falls to **GATHER** (single writer). This produces two
regressions versus legacy:

- **NG-2 (blocker, F17):** a **dynamic-partition** INSERT loses the **hash-by-partition + mandatory
  local-sort** that legacy `PhysicalMaxComputeTableSink.getRequirePhysicalProperties():111-155`
  enforced. The MaxCompute Storage API streams partition writers and **closes the previous partition
  writer the moment it sees a different partition value**; un-grouped (unsorted) multi-partition rows
  trigger BE `"writer has been closed"` errors. (Legacy comment at `:144-147` documents exactly this.)
- **NG-4 (major, F18):** **non-partitioned / all-static** MaxCompute writes degrade from
  `SINK_RANDOM_PARTITIONED` (multiple parallel writers, legacy) to **GATHER** (single writer) →
  write-throughput regression.

Legacy `PhysicalMaxComputeTableSink.getRequirePhysicalProperties()` is a clean 3-branch:

| case | legacy output |
|---|---|
| has partition cols **and** a partition col present in `cols` (dynamic) | `DistributionSpecHiveTableSinkHashPartitioned(partitionExprIds)` + `MustLocalSortOrderSpec(partitionOrderKeys)` |
| has partition cols, none present in `cols` (all static) | `SINK_RANDOM_PARTITIONED` |
| no partition cols | `SINK_RANDOM_PARTITIONED` |

The generic sink reproduces **none** of branch-1's hash+local-sort and reaches RANDOM only behind a
capability MaxCompute never declares.

## Root Cause

`PhysicalConnectorTableSink` was cloned from JDBC/ES write semantics (single transactional writer,
no partitions). It models exactly one knob — `supportsParallelWrite()` → RANDOM-vs-GATHER — and has
**no channel** for a connector to declare the MaxCompute-style requirement *"dynamic-partition writes
must be hash-distributed and locally sorted by partition columns."* The legacy logic lived in the
MaxCompute-specific `PhysicalMaxComputeTableSink`, which the cutover stopped instantiating; the
distribution/sort knowledge was never ported into the generic sink or surfaced through the SPI. This
is the write-path face of the recurring "half-wired dispatch" (re-review §C domain-6): the read/DDL
dispatch was generalized, the write *distribution* was not.

## Design

### Two orthogonal connector signals → two capabilities

The legacy 3-branch needs exactly two connector-declared facts, which I map to **`ConnectorCapability`
enum** values read through `connector.getCapabilities()` — the *same* mechanism the sibling
`supportsParallelWrite()` already uses (read in this very method), so both reads are uniform and
require no `ConnectorSession`/metadata construction in the planner property-derivation hot path:

1. **`SUPPORTS_PARALLEL_WRITE`** (already exists, `ConnectorCapability.java:51`) — "multiple
   concurrent writers are safe." Drives the non-partition / all-static → `SINK_RANDOM_PARTITIONED`
   branch. **MaxCompute must now declare it** (fixes NG-4). Read via the existing
   `PluginDrivenExternalTable.supportsParallelWrite()`.
2. **`SINK_REQUIRE_PARTITION_LOCAL_SORT`** (NEW enum value) — "dynamic-partition writes must be
   hash-distributed and locally sorted by partition columns" (the MaxCompute Storage-API streaming
   constraint). Drives branch-1. **MaxCompute declares it** (fixes NG-2). Read via a new
   `PluginDrivenExternalTable.requirePartitionLocalSortOnWrite()`.

Default for the new capability is **absent/false** → no behavior change for any other connector
(jdbc/es/trino: neither capability → still GATHER), mirroring the FIX-OVERWRITE-GATE
default-false-opt-in philosophy. The two capabilities are intended to be declared **together** by a
partition-writing connector (hash distribution is inherently parallel); the sink does not force that
pairing (branch-1 keys only on the local-sort capability, faithful to legacy's unconditional
dynamic→hash+sort), but the design note records the intended pairing.

### The fe-core sink logic — **critical correction vs legacy: index by `cols`, not full-schema**

The generic sink's `getRequirePhysicalProperties()` reproduces the legacy 3-branch, **but the
partition-column → child-output index mapping MUST differ from legacy.** This is the single most
important correctness point of this fix:

- Legacy `bindMaxComputeTableSink` (`BindSink.java:904-906`) projects the child to **full-schema**
  order (`getOutputProjectByCoercion(table.getFullSchema(), ...)`), so legacy
  `PhysicalMaxComputeTableSink` can index `child().getOutput().get(fullSchemaIdx)`.
- The generic `bindConnectorTableSink` (`BindSink.java:949-950`) projects the child to **`bindColumns`**
  order (`getOutputProjectByCoercion(bindColumns, ...)`), where `bindColumns == boundSink.getCols()`,
  and enforces `cols.size() == child.getOutput().size()` (`:941`). So for the generic sink
  `child().getOutput().get(i)` corresponds to **`cols.get(i)`**, NOT to `fullSchema.get(i)`.

Therefore the generic sink finds each partition column by its index **in `cols`** and reads the
aligned child output slot at the same index:

```java
@Override
public PhysicalProperties getRequirePhysicalProperties() {
    if (!(targetTable instanceof PluginDrivenExternalTable)) {
        return PhysicalProperties.GATHER;
    }
    PluginDrivenExternalTable table = (PluginDrivenExternalTable) targetTable;

    // Branch 1 — dynamic-partition write that the connector requires to be hash-distributed and
    // locally sorted by partition columns (MaxCompute Storage API streams partition writers and
    // errors on unsorted multi-partition data — mirrors legacy PhysicalMaxComputeTableSink).
    if (table.requirePartitionLocalSortOnWrite()) {
        Set<String> partitionNames = table.getPartitionColumns().stream()
                .map(Column::getName).collect(Collectors.toSet());
        if (!partitionNames.isEmpty()) {
            // Index by cols (== child output alignment for the connector sink), NOT full schema.
            List<Integer> partitionColIdx = new ArrayList<>();
            for (int i = 0; i < cols.size(); i++) {
                if (partitionNames.contains(cols.get(i).getName())) {
                    partitionColIdx.add(i);
                }
            }
            if (!partitionColIdx.isEmpty()) {  // a partition col present in cols == dynamic write
                List<ExprId> exprIds = partitionColIdx.stream()
                        .map(idx -> child().getOutput().get(idx).getExprId())
                        .collect(Collectors.toList());
                DistributionSpecHiveTableSinkHashPartitioned shuffleInfo =
                        new DistributionSpecHiveTableSinkHashPartitioned();
                shuffleInfo.setOutputColExprIds(exprIds);
                List<OrderKey> orderKeys = partitionColIdx.stream()
                        .map(idx -> new OrderKey(child().getOutput().get(idx), true, false))
                        .collect(Collectors.toList());
                return new PhysicalProperties(shuffleInfo)
                        .withOrderSpec(new MustLocalSortOrderSpec(orderKeys));
            }
            // partition cols exist but none in cols == all-static: fall through.
        }
    }

    // Branch 2/3 — non-partition or all-static: parallel writers if the connector supports it.
    if (table.supportsParallelWrite()) {
        return PhysicalProperties.SINK_RANDOM_PARTITIONED;
    }
    return PhysicalProperties.GATHER;
}
```

Result mapping:

| table / write shape | caps declared | output | legacy parity |
|---|---|---|---|
| MaxCompute, dynamic partition | both | hash(part) + local-sort(part) | ✅ = legacy branch-1 |
| MaxCompute, all-static partition | both | `SINK_RANDOM_PARTITIONED` | ✅ = legacy branch-2 |
| MaxCompute, non-partitioned | both | `SINK_RANDOM_PARTITIONED` | ✅ = legacy branch-3 |
| jdbc / es / trino | none | `GATHER` | ✅ unchanged |

### Why no change is needed in `RequestPropertyDeriver`

`RequestPropertyDeriver.visitPhysicalConnectorTableSink():212-227` already routes correctly:
`GATHER → GATHER`; else (with `enableStrictConsistencyDml`, default **true** —
`SessionVariable.java:1566`) `→ getRequirePhysicalProperties()` to children. So once
`getRequirePhysicalProperties()` returns hash+local-sort, the deriver enforces it (inserts the
shuffle + local sort) exactly as it does for legacy `visitPhysicalMaxComputeTableSink():180-188`. The
non-strict (`enable_strict_consistency_dml=false`) path pushes `ANY` for **both** legacy MC and the
generic connector sink — i.e. it drops the requirement identically in legacy and cutover, so it is a
pre-existing parity, not a regression introduced here. (A user who turns off strict-consistency DML
loses local-sort on dynamic partitions in legacy too; default-on covers the common case.)

### Known minor divergence — `ShuffleKeyPruner` (documented, not fixed here)

`ShuffleKeyPruner.visitPhysicalConnectorTableSink():286-295` lacks the non-strict short-circuit that
`visitPhysicalMaxComputeTableSink():272-283` has. In the **default strict** mode both compute
`childAllowShuffleKeyPrune = required.equals(ANY)` → `false` for a dynamic-partition write → **identical
behavior**. They diverge **only** when `enable_strict_consistency_dml=false`: legacy prunes shuffle keys
(`true`), generic does not (`required` is hash+sort ≠ `ANY` → `false`). The generic path therefore
prunes **less** (more conservative) — a missed optimization, never a correctness issue, and it is
**pre-existing** (the generic branch already differs; this fix does not introduce it). Recorded as a
minor deviation; aligning it would touch the shared connector branch for jdbc/es and is out of scope.

### Coupling with P0-3 (FIX-BIND-STATIC-PARTITION) — correct either way, fully exercised only after P0-3

The dynamic/static detection reads `cols` and relies on the contract *"static partition columns are
excluded from `cols`"* — the same contract legacy `getRequirePhysicalProperties()` relies on (legacy
`bindMaxComputeTableSink:876-879` excludes them). The generic `bindConnectorTableSink` does **not** yet
exclude them (that is the P0-3 bug, NG-3). Consequences, both **safe**:

- `INSERT INTO mc PARTITION(p='x') SELECT <non-partition cols>` (no column list, all-static): today
  this **fails at bind** (`cols` includes `p`, child output excludes it → `:941` count mismatch
  throws) — so `getRequirePhysicalProperties()` is **never reached**. After P0-3, `cols` excludes the
  static `p` → branch falls through to `SINK_RANDOM_PARTITIONED`. ✅ either way.
- `INSERT INTO mc PARTITION(p) SELECT ... , p_val` (dynamic): `p` is in `cols` → branch-1
  hash+local-sort. ✅ today and after P0-3.
- Mixed `PARTITION(p1='x', p2) SELECT ...`: after P0-3, `cols` excludes static `p1`, includes dynamic
  `p2` → hash+sort by `p2` only. Legacy hashes+sorts by `{p1,p2}` but `p1` is a projected constant, so
  `{p2}` ≡ `{p1,p2}` for grouping. ✅ functionally equivalent.

So **this fix is correct regardless of P0-3 ordering**; it is merely not *exercised* for the all-static
no-column-list shape until P0-3 lands. Documented; no ordering constraint imposed on P0-3.

> **Forward-pointer (from the P0-2 clean-room review, 2026-06-07 — survivors F2/F4/F5 all
> `known-degradation`, `matchesDesignIntent=true`, 0 must-fix):** when **P0-3 / FIX-BIND-STATIC-PARTITION**
> lands, add a Rule-9 integration regression that `INSERT INTO mc PARTITION(p='x') SELECT <non-partition
> cols>` (no column list) **binds without throwing** AND `getRequirePhysicalProperties()` then returns
> `SINK_RANDOM_PARTITIONED` (the all-static branch fully exercised end-to-end). Until then, T2
> (`allStaticPartitionWriteUsesRandomPartitioned`) unit-tests that branch over a cols-already-stripped
> input (reachable today only via the explicit-column-list static form — see the test's Javadoc).
> **Batch-D red-line:** do not delete legacy `PhysicalMaxComputeTableSink` (sole logical copy) until
> *both* this fix and P0-3 have landed, else all-static parity is lost before it is end-to-end exercised.

### Alternatives considered

- **(B) Derive implicitly — no new capability** (`supportsParallelWrite() && hasPartitionCols &&
  dynamic → hash+local-sort`). Simpler (Rule 2), but forces the MaxCompute Storage-API local-sort
  policy on **every** future parallel-write partitioned connector, even ones that buffer per-partition
  and don't need it (an unnecessary sort cost). Rejected: conflates two orthogonal facts; the
  re-review §A.NG-2 处置 and the HANDOFF explicitly call for a *connector-declared* "distribution+sort"
  hook, not an implicit universal default.
- **(C) Method on `ConnectorWriteOps`** (`requirePartitionLocalSortOnWrite()`), mirroring
  FIX-OVERWRITE-GATE's `supportsInsertOverwrite()`. Works, but reading it from the sink needs a
  `ConnectorSession` + `getMetadata(...)` round-trip inside property derivation, whereas the sibling
  `supportsParallelWrite()` read in the same method uses the cheaper `getCapabilities()` set. Rejected
  for inconsistency + hot-path cost; the capability is a static connector property, which is exactly
  what `ConnectorCapability` is for.
- **(A, chosen) New `ConnectorCapability` enum value.** Consistent with the sibling read, cheap,
  opt-in, matches the HANDOFF guidance. The enum already carries planner-distribution semantics
  (`SUPPORTS_PARALLEL_WRITE`'s own doc-comment describes GATHER-vs-parallel), so a sibling
  distribution capability fits.

## Implementation Plan

**File 1 — `fe/fe-connector/fe-connector-api/.../ConnectorCapability.java`**
Append a new enum value after `SUPPORTS_PARALLEL_WRITE` (`:51`):

```java
    /**
     * Indicates the connector requires dynamic-partition writes to be hash-distributed by
     * partition columns and locally sorted by them before reaching the sink.
     *
     * <p>Streaming partition writers (e.g. MaxCompute Storage API) close the previous partition
     * writer when a new partition value appears; un-grouped rows cause "writer has been closed"
     * errors. A connector declaring this is expected to also declare {@link #SUPPORTS_PARALLEL_WRITE}.</p>
     */
    SINK_REQUIRE_PARTITION_LOCAL_SORT
```

**File 2 — `fe/fe-connector/fe-connector-maxcompute/.../MaxComputeDorisConnector.java`**
Add `getCapabilities()` override (currently absent → empty set):

```java
@Override
public Set<ConnectorCapability> getCapabilities() {
    return EnumSet.of(ConnectorCapability.SUPPORTS_PARALLEL_WRITE,
            ConnectorCapability.SINK_REQUIRE_PARTITION_LOCAL_SORT);
}
```
Imports: `org.apache.doris.connector.api.ConnectorCapability`, `java.util.EnumSet`, `java.util.Set`.

**File 3 — `fe/fe-core/.../datasource/PluginDrivenExternalTable.java`**
Add a sibling to `supportsParallelWrite()` (`:78-85`):

```java
public boolean requirePartitionLocalSortOnWrite() {
    if (!(catalog instanceof PluginDrivenExternalCatalog)) {
        return false;
    }
    Connector connector = ((PluginDrivenExternalCatalog) catalog).getConnector();
    return connector != null
            && connector.getCapabilities().contains(ConnectorCapability.SINK_REQUIRE_PARTITION_LOCAL_SORT);
}
```
(No new import — `ConnectorCapability` already imported `:26`.)

**File 4 — `fe/fe-core/.../physical/PhysicalConnectorTableSink.java`**
Replace `getRequirePhysicalProperties()` (`:114-121`) with the 3-branch (cols-indexed) logic above.
New imports (mirror `PhysicalMaxComputeTableSink`'s import block):
`DistributionSpecHiveTableSinkHashPartitioned`, `MustLocalSortOrderSpec`, `OrderKey`, `ExprId`,
`java.util.ArrayList`, `java.util.Set`, `java.util.stream.Collectors`. Update the method Javadoc to
describe all three branches.

**No change** to `RequestPropertyDeriver`, `PhysicalPlanTranslator`, `BindSink`, or the BE/thrift sink.

## Risk Analysis

- **Blast radius of declaring `SUPPORTS_PARALLEL_WRITE` for MaxCompute:** the capability has exactly
  **two** readers in the tree — `PluginDrivenExternalTable.supportsParallelWrite()` and
  `PhysicalConnectorTableSink:117` (verified by grep). The new capability has one reader (the new table
  method). So flipping both **only** affects `getRequirePhysicalProperties()` and its two consumers
  (`RequestPropertyDeriver`, `ShuffleKeyPruner`), both analyzed above. No DDL/read/transaction path
  reads these capabilities. Other connectors are untouched (they declare neither).
- **Index-by-cols correctness** is the highest-risk element (a verbatim copy of legacy that indexed by
  full-schema would be wrong/out-of-bounds for the connector sink). Covered by the design note above
  and pinned by the UT (dynamic-partition exprIds must equal the *cols-position* child slots).
- **`enable_strict_consistency_dml=false`** path drops the requirement (pushes ANY) — **parity with
  legacy**, not a new regression. Documented.
- **Batch-D red-line (🔴):** `PhysicalMaxComputeTableSink` is the **sole** logical copy of this
  hash+local-sort logic. Batch-D must not delete it until this fix lands the equivalent in the generic
  sink + MaxCompute capability declaration. Ordering: this fix **before** the Batch-D delete of
  `PhysicalMaxComputeTableSink`. Doc-sync flag below.
- **Truth-gate remaining (live e2e):** unit tests prove `getRequirePhysicalProperties()` returns the
  right spec; they do **not** prove BE actually avoids "writer has been closed" end-to-end. Per
  re-review §E#6 that requires **live INSERT across multiple dynamic partitions against real ODPS**
  (CI-skipped). This fix is necessary-but-not-sufficient until run live alongside P0-3.

## Test Plan

### Unit Tests

**Location:** new `fe/fe-core/src/test/java/org/apache/doris/nereids/trees/plans/physical/PhysicalConnectorTableSinkTest.java`.

`getRequirePhysicalProperties()` reads protected-final fields `targetTable`, `cols` and calls
`child()`. Construct the sink with the project's established pattern (memory
`catalog-spi-fe-core-test-infra`): `Mockito.mock(PhysicalConnectorTableSink.class, CALLS_REAL_METHODS)`
to skip the ctor, `Deencapsulation.setField(sink, "targetTable"/"cols", ...)` to inject finals, and
`Mockito.doReturn(childPlan).when(sink).child()` (childPlan = a mock `Plan` whose `getOutput()`
returns hand-built `SlotReference`s aligned 1:1 with `cols`). The `PluginDrivenExternalTable` is built
with the `TestablePluginCatalog` + `tableWithCacheValue` pattern from
`PluginDrivenExternalTablePartitionTest`, and its mock `Connector.getCapabilities()` is stubbed per
case. No Doris env needed.

- **T1 `dynamicPartitionWriteRequiresHashAndLocalSort` (the Rule-9 red-before/green-after gate):**
  partitioned table (`part` ∈ schema), caps = {PARALLEL_WRITE, REQUIRE_PARTITION_LOCAL_SORT}, `cols`
  **includes** `part`. Assert result distribution is `DistributionSpecHiveTableSinkHashPartitioned`
  whose `getOutputColExprIds()` equals the ExprId of the **cols-position** child slot for `part`, AND
  the order spec is `MustLocalSortOrderSpec` over that same slot. **Why it encodes intent (Rule 9):**
  asserts the business invariant "dynamic-partition MaxCompute writes are grouped per partition so the
  Storage API does not hit 'writer has been closed'." **Mutation:** revert
  `getRequirePhysicalProperties()` to the old `supportsParallelWrite? RANDOM : GATHER` → result is
  `SINK_RANDOM_PARTITIONED` (no order spec) → red. Also: an index-by-full-schema mutation maps to the
  wrong/out-of-range slot → red.
- **T2 `allStaticPartitionWriteUsesRandomPartitioned`:** partitioned table, both caps, `cols`
  **excludes** all partition cols → assert `SINK_RANDOM_PARTITIONED` (no order spec). Pins the
  static-vs-dynamic detection (mutation dropping the `partitionColIdx.isEmpty()` fall-through would
  red).
- **T3 `nonPartitionedWriteUsesRandomWhenParallel`:** no partition cols, both caps → assert
  `SINK_RANDOM_PARTITIONED`. (NG-4 parity for non-partitioned tables.)
- **T4 `nonParallelConnectorGathers`:** table with **no** capabilities (jdbc-like) → assert `GATHER`.
  Guards that the change did not broaden parallel/sort behavior to capability-less connectors.

### E2E Tests

Out of scope for this loop (per the round process: compile+UT, no e2e). The real truth-gate —
`INSERT` across **multiple dynamic partitions** against real ODPS asserting no `"writer has been
closed"` + parallel throughput on non-partitioned writes — requires live ODPS credentials and is
CI-skipped. Recorded as the remaining live gate (alongside P0-3 / FIX-OVERWRITE-GATE).

## Doc-sync (with or after this fix)

- **Batch-D red-line** (`P4-batchD-maxcompute-removal-design.md`): the delete of
  `PhysicalMaxComputeTableSink` must be ordered **after** this fix (sole logical copy of write
  distribution). Confirm the "zero survivor" claim accounts for the new generic-sink + capability path.
- **decisions-log / deviations-log:** register the new `ConnectorCapability.SINK_REQUIRE_PARTITION_LOCAL_SORT`
  + MaxCompute capability set; register the `ShuffleKeyPruner` non-strict minor deviation; register the
  `enable_strict_consistency_dml=false` parity note.
- **task-list-P4-rereview.md:** flip P0-2 progress + append the review-rounds cumulative conclusion.
