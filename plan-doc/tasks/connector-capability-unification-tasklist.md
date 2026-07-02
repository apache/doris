# Connector Capability Unification — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.
>
> **Design source:** `plan-doc/tasks/designs/connector-capability-unification-design.md` (confirmed with user). This plan re-derives the concrete edit set from CURRENT code — the design's line numbers and capability inventory were ~3 days stale (see "Drift reconciliation" below). **Trust this plan's control-flow/method anchors, not the design's line numbers.**

**Goal:** Make a connector's write capabilities declared in exactly one place — the write plan provider (the SPI seam that implements them) — and delete the parallel `ConnectorCapability` flag layer and `ConnectorWriteOps` boolean layer that duplicated them.

**Architecture:** Write capabilities move onto `ConnectorWritePlanProvider` as **argless, connector-level** default methods (`supportedOperations()`, `supportsWriteBranch()`, `requiresParallelWrite()`, `requiresFullSchemaWriteOrder()`, `requiresPartitionLocalSort()`, `requiresMaterializeStaticPartitionValues()`). `Connector` gains null-safe delegators so the engine never checks `getWritePlanProvider() != null` directly. The 5 `ConnectorWriteOps` write-capability booleans and 14 `ConnectorCapability` enum values (12 dead + `SUPPORTS_INSERT` + `SUPPORTS_TIME_TRAVEL`) are deleted; 4 sink-trait enum values move to the provider. 8 `ConnectorCapability` values that are genuine static planning switches (with live readers) remain.

**Tech Stack:** Java 8, Maven (multi-module reactor under `fe/`), JUnit 5, Mockito (fe-core tests only; connector tests use real fakes — no Mockito).

## Global Constraints

- **Connectors must not import fe-core.** Verify with `bash tools/check-connector-imports.sh`. All new SPI types live in `fe-connector-api` (`org.apache.doris.connector.api.*`).
- **Connector tests have no Mockito** — use real `InMemoryCatalog` / recording fakes. **fe-core tests use Mockito** (`CALLS_REAL_METHODS` + `Deencapsulation.setField` + stubbing `getConnector`/`getMetadata`/`buildConnectorSession`). Mockito `anyString()` does not match null.
- **Maven build/test:** `mvn -f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl :<artifact> -am -DfailIfNoTests=false -Dmaven.build.cache.enabled=false test -Dtest=<Class>` (always absolute `-f`; cwd is reset between calls). Read the surefire XML `tests=`/`failures=` or grep `BUILD SUCCESS`; a piped `$?` is the pipe tail. fe-core build can exceed the 120s tool timeout → raise timeout to ~590000ms or run in background.
- **Checkstyle:** `mvn -f .../fe/pom.xml -pl :<artifact> checkstyle:check` — **never add `-am`** (drags `fe-common`'s pre-existing errors into a false red).
- **Each task = one independent commit** (project convention). Commit message trailer:
  `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>` + `Claude-Session: …`.
- **`git add` is path-whitelisted — never `git add -A`** (working tree has scratch dirs + a plaintext-key regression conf that must not be committed).
- **Mutation check (Rule 9/12):** after a behavior-gating change, flip the guard (single-string anchor, count==1) and confirm the relevant test fails (maven rc != 0); restore.

---

## Drift reconciliation (design vs. current code — read before starting)

The design (2026-06-29) predates two capabilities added on 2026-07-01. Re-derived from current `ConnectorCapability.java` (26 values, not 24):

**KEEP as enum — 8 static planning switches, each with a live production reader:**

| Value | Reader (file) |
|---|---|
| `SUPPORTS_MVCC_SNAPSHOT` | `PluginDrivenExternalDatabase` |
| `SUPPORTS_VIEW` | `PluginDrivenExternalTable`, `PluginDrivenExternalCatalog` |
| `SUPPORTS_SHOW_CREATE_DDL` | `PluginDrivenExternalTable` |
| `SUPPORTS_PARTITION_STATS` | `ShowPartitionsCommand` |
| `SUPPORTS_COLUMN_AUTO_ANALYZE` | `PluginDrivenExternalTable` |
| `SUPPORTS_TOPN_LAZY_MATERIALIZE` | `PluginDrivenExternalTable` |
| `SUPPORTS_PASSTHROUGH_QUERY` | `QueryTableValueFunction` |
| `SUPPORTS_NESTED_COLUMN_PRUNE` | `PluginDrivenExternalTable` *(design missed this one — it is a genuine keeper)* |

**MOVE to the provider — 4 sink-trait flags (delete enum value, add provider method):**

| Enum value (delete) | New provider method | Current reader (rewire) |
|---|---|---|
| `SUPPORTS_PARALLEL_WRITE` | `requiresParallelWrite()` | `PluginDrivenExternalTable.supportsParallelWrite()` |
| `SINK_REQUIRE_FULL_SCHEMA_ORDER` | `requiresFullSchemaWriteOrder()` | `PluginDrivenExternalTable.requiresFullSchemaWriteOrder()` |
| `SINK_REQUIRE_PARTITION_LOCAL_SORT` | `requiresPartitionLocalSort()` | `PluginDrivenExternalTable.requirePartitionLocalSortOnWrite()` |
| `SINK_MATERIALIZE_STATIC_PARTITION_VALUES` | `requiresMaterializeStaticPartitionValues()` | `PluginDrivenExternalTable.materializeStaticPartitionValues()` *(design missed this one — user confirmed moving it)* |

**DELETE outright — 14 values:**
- 12 dead (zero readers): `SUPPORTS_FILTER_PUSHDOWN`, `SUPPORTS_PROJECTION_PUSHDOWN`, `SUPPORTS_LIMIT_PUSHDOWN`, `SUPPORTS_PARTITION_PRUNING`, `SUPPORTS_DELETE`, `SUPPORTS_UPDATE`, `SUPPORTS_MERGE`, `SUPPORTS_CREATE_TABLE`, `SUPPORTS_STATISTICS`, `SUPPORTS_METASTORE_EVENTS`, `SUPPORTS_VENDED_CREDENTIALS`, `SUPPORTS_ACID_TRANSACTIONS`.
  - `SUPPORTS_FILTER_PUSHDOWN` has ONE test-only placeholder use (`PluginDrivenMvccTableFactoryTest:66`) → repoint to `SUPPORTS_VIEW`.
  - `SUPPORTS_STATISTICS` has one self-file javadoc `{@link}` (inside `SUPPORTS_PARTITION_STATS` doc) → remove the link.
- `SUPPORTS_INSERT` — no reader; produced only by `JdbcDorisConnector`. Derived from `supportedOperations().contains(INSERT)`.
- `SUPPORTS_TIME_TRAVEL` — **no production reader** (verified; real time-travel gate is `SUPPORTS_MVCC_SNAPSHOT`). Cross-checked the flip task list/review: it is a dead-by-name placeholder, **not** reserved for post-flip wiring → safe to delete. Produced by `IcebergConnector` + `PaimonConnector`; asserted in 2 tests.

**`ConnectorWriteOps` booleans — delete 5, keep 3:**
- Delete: `supportsInsert`, `supportsInsertOverwrite`, `supportsDelete`, `supportsMerge`, `supportsWriteBranch`.
- Keep: `validateRowLevelDmlMode`, `validateStaticPartitionColumns` (dynamic, connector-authored messages), `beginTransaction` (transaction factory).

**Per-connector reality (verified — drives Task 2 declarations):**

| Connector | write provider | `supportedOperations()` | branch | sink-traits → true | keep in `getCapabilities()` |
|---|---|---|---|---|---|
| Iceberg | `IcebergWritePlanProvider` | `{INSERT,OVERWRITE,DELETE,MERGE,REWRITE}` | yes | parallelWrite, fullSchemaOrder, materializeStaticPartition | MVCC, COLUMN_AUTO_ANALYZE, TOPN_LAZY, SHOW_CREATE_DDL, VIEW, NESTED_COLUMN_PRUNE |
| MaxCompute | `MaxComputeWritePlanProvider` | `{INSERT,OVERWRITE}` | no | parallelWrite, fullSchemaOrder, partitionLocalSort | (none → remove `getCapabilities()` override) |
| JDBC | `JdbcWritePlanProvider` | `{INSERT}` (default, no override) | no | none | PASSTHROUGH_QUERY |
| Paimon | null | — | — | — | MVCC, PARTITION_STATS, COLUMN_AUTO_ANALYZE, SHOW_CREATE_DDL |
| ES | null | — | — | — | (none) |
| Trino | null | — | — | — | (none) |

> Iceberg `REWRITE` reaches `planWrite` via the procedure path (`IcebergProcedureOps.planRewrite` → N per-group INSERT-SELECT writes); it is not a `PhysicalPlanTranslator` sink gate. It is listed in `supportedOperations()` only to keep the set the single complete source of truth. UPDATE is synthesized upstream as a MERGE plan, so it never reaches a translator gate as `WriteOperation.UPDATE`; only `validateRowLevelDmlMode` (retained) sees UPDATE.

---

## File structure

**SPI (fe-connector-api):**
- `write/ConnectorWritePlanProvider.java` — +6 default methods (Task 1)
- `Connector.java` — +6 null-safe delegators (Task 1)
- `ConnectorWriteOps.java` — −5 boolean methods (Task 4)
- `ConnectorCapability.java` — −18 values (12 dead +2 derived +4 moved), keep 8 (Task 5)
- `ConnectorContractValidator.java` — **new** (Task 6)

**Connectors:**
- iceberg: `IcebergWritePlanProvider.java` (+overrides, Task 2), `IcebergConnectorMetadata.java` (−4 overrides, Task 4), `IcebergConnector.java` (trim `getCapabilities`, Task 5)
- maxcompute: `MaxComputeWritePlanProvider.java` (+overrides, Task 2), `MaxComputeConnectorMetadata.java` (−2 overrides, Task 4), `MaxComputeDorisConnector.java` (remove `getCapabilities` override, Task 5)
- jdbc: `JdbcConnectorMetadata.java` (−1 override, Task 4), `JdbcDorisConnector.java` (drop `SUPPORTS_INSERT`, Task 5)
- paimon: `PaimonConnector.java` (drop `SUPPORTS_TIME_TRAVEL`, Task 5)

**fe-core consumers (Task 3):**
- `nereids/glue/translator/PhysicalPlanTranslator.java` (2 gates)
- `nereids/trees/plans/commands/insert/InsertOverwriteTableCommand.java` (2 methods)
- `nereids/trees/plans/commands/insert/InsertIntoTableCommand.java` (1 method)
- `datasource/iceberg/IcebergNereidsUtils.java` (1 method)
- `nereids/trees/plans/commands/IcebergRowLevelDmlTransform.java` (1 method)
- `datasource/PluginDrivenExternalTable.java` (4 sink-trait accessors)
- `connector/ConnectorPluginManager.java` (validator hook, Task 6)

---

## Task 1: Additive SPI — provider methods + Connector delegators

**Files:**
- Modify: `fe/fe-connector/fe-connector-api/src/main/java/org/apache/doris/connector/api/write/ConnectorWritePlanProvider.java`
- Modify: `fe/fe-connector/fe-connector-api/src/main/java/org/apache/doris/connector/api/Connector.java`
- Test: `fe/fe-core/src/test/java/org/apache/doris/connector/fake/FakeConnectorPluginTest.java` (or a focused new `ConnectorWriteDelegationTest` if the fake plugin is a cleaner harness)

**Interfaces produced (later tasks depend on these exact signatures):**
- `ConnectorWritePlanProvider`: `Set<WriteOperation> supportedOperations()` (default `EnumSet.of(INSERT)`); `boolean supportsWriteBranch()`, `requiresParallelWrite()`, `requiresFullSchemaWriteOrder()`, `requiresPartitionLocalSort()`, `requiresMaterializeStaticPartitionValues()` (all default `false`).
- `Connector`: `Set<WriteOperation> supportedWriteOperations()`; `boolean supportsWriteBranch()`, `requiresParallelWrite()`, `requiresFullSchemaWriteOrder()`, `requiresPartitionLocalSort()`, `requiresMaterializeStaticPartitionValues()` (null-safe delegators).

- [ ] **Step 1: Add the 6 default methods to `ConnectorWritePlanProvider`.** Add imports `java.util.EnumSet`, `java.util.Set`, `org.apache.doris.connector.api.handle.WriteOperation`. Append inside the interface (after `getSyntheticWriteColumns`):

```java
    /**
     * The write operations this provider can plan, in one place — the single source of truth for a
     * connector's write capability. Replaces the removed {@code ConnectorWriteOps} boolean methods and
     * the {@code SUPPORTS_INSERT} capability. Default: INSERT only (any write provider can at least
     * append). A connector overrides this to add OVERWRITE / DELETE / MERGE / REWRITE. Connector-level
     * (does not vary per table); per-table mode constraints stay in
     * {@link org.apache.doris.connector.api.ConnectorWriteOps#validateRowLevelDmlMode}.
     */
    default Set<WriteOperation> supportedOperations() {
        return EnumSet.of(WriteOperation.INSERT);
    }

    /** Whether this connector can write into a named table branch ({@code INSERT INTO t@branch(name)}). Default: no. */
    default boolean supportsWriteBranch() {
        return false;
    }

    /**
     * Whether the connector supports multiple concurrent writers (parallel sink instances). Connectors that
     * do not declare this get GATHER (single-writer) distribution. Relocated from
     * {@code ConnectorCapability.SUPPORTS_PARALLEL_WRITE}. Default: no.
     */
    default boolean requiresParallelWrite() {
        return false;
    }

    /**
     * Whether the connector maps write data columns positionally against the full table schema (so the sink
     * must project rows to full-schema order with unmentioned columns filled). Relocated from
     * {@code ConnectorCapability.SINK_REQUIRE_FULL_SCHEMA_ORDER}. Default: no.
     */
    default boolean requiresFullSchemaWriteOrder() {
        return false;
    }

    /**
     * Whether dynamic-partition writes must be hash-distributed by partition columns and locally sorted by
     * them before the sink (e.g. MaxCompute Storage API). Relocated from
     * {@code ConnectorCapability.SINK_REQUIRE_PARTITION_LOCAL_SORT}. A connector declaring this must also
     * declare {@link #requiresParallelWrite()} and {@link #requiresFullSchemaWriteOrder()}. Default: no.
     */
    default boolean requiresPartitionLocalSort() {
        return false;
    }

    /**
     * Whether the connector's data files physically retain partition columns, so a static-partition write
     * must materialize the PARTITION-clause literal into the data column instead of NULL-filling it (e.g.
     * Iceberg). Relocated from {@code ConnectorCapability.SINK_MATERIALIZE_STATIC_PARTITION_VALUES}. Default: no.
     */
    default boolean requiresMaterializeStaticPartitionValues() {
        return false;
    }
```

- [ ] **Step 2: Add the 6 null-safe delegators to `Connector`.** Add imports `java.util.EnumSet`, `org.apache.doris.connector.api.handle.WriteOperation` (`Set` and `ConnectorWritePlanProvider` are already visible; confirm and add if missing). Append after `getWritePlanProvider()`:

```java
    /**
     * The write operations the engine may perform on this connector — the single admission source. Reads the
     * write provider's {@link ConnectorWritePlanProvider#supportedOperations()}; no provider ⇒ empty set ⇒ all
     * writes rejected. The engine consults this instead of {@code getWritePlanProvider() != null}.
     */
    default Set<WriteOperation> supportedWriteOperations() {
        ConnectorWritePlanProvider p = getWritePlanProvider();
        return p == null ? EnumSet.noneOf(WriteOperation.class) : p.supportedOperations();
    }

    /** Null-safe view of {@link ConnectorWritePlanProvider#supportsWriteBranch()}. No provider ⇒ false. */
    default boolean supportsWriteBranch() {
        ConnectorWritePlanProvider p = getWritePlanProvider();
        return p != null && p.supportsWriteBranch();
    }

    /** Null-safe view of {@link ConnectorWritePlanProvider#requiresParallelWrite()}. No provider ⇒ false. */
    default boolean requiresParallelWrite() {
        ConnectorWritePlanProvider p = getWritePlanProvider();
        return p != null && p.requiresParallelWrite();
    }

    /** Null-safe view of {@link ConnectorWritePlanProvider#requiresFullSchemaWriteOrder()}. No provider ⇒ false. */
    default boolean requiresFullSchemaWriteOrder() {
        ConnectorWritePlanProvider p = getWritePlanProvider();
        return p != null && p.requiresFullSchemaWriteOrder();
    }

    /** Null-safe view of {@link ConnectorWritePlanProvider#requiresPartitionLocalSort()}. No provider ⇒ false. */
    default boolean requiresPartitionLocalSort() {
        ConnectorWritePlanProvider p = getWritePlanProvider();
        return p != null && p.requiresPartitionLocalSort();
    }

    /** Null-safe view of {@link ConnectorWritePlanProvider#requiresMaterializeStaticPartitionValues()}. No provider ⇒ false. */
    default boolean requiresMaterializeStaticPartitionValues() {
        ConnectorWritePlanProvider p = getWritePlanProvider();
        return p != null && p.requiresMaterializeStaticPartitionValues();
    }
```

- [ ] **Step 3: Write a delegation unit test.** In a fe-core test (Mockito allowed), assert: a `Connector` with `getWritePlanProvider()==null` returns `supportedWriteOperations().isEmpty()` and all `requiresXxx()`/`supportsWriteBranch()` false; a connector whose provider overrides `supportedOperations()`→`{INSERT,OVERWRITE}` and `requiresParallelWrite()`→true is reflected through the delegators. Use `Mockito.mock(Connector.class, CALLS_REAL_METHODS)` + stub `getWritePlanProvider()`.

```java
@Test
void delegatorsReflectProviderAndAreNullSafe() {
    Connector noWrite = mock(Connector.class, CALLS_REAL_METHODS);
    when(noWrite.getWritePlanProvider()).thenReturn(null);
    assertTrue(noWrite.supportedWriteOperations().isEmpty());
    assertFalse(noWrite.supportsWriteBranch());
    assertFalse(noWrite.requiresParallelWrite());

    ConnectorWritePlanProvider prov = new ConnectorWritePlanProvider() {
        public ConnectorSinkPlan planWrite(ConnectorSession s, ConnectorWriteHandle h) { return null; }
        public Set<WriteOperation> supportedOperations() {
            return EnumSet.of(WriteOperation.INSERT, WriteOperation.OVERWRITE);
        }
        public boolean requiresParallelWrite() { return true; }
    };
    Connector w = mock(Connector.class, CALLS_REAL_METHODS);
    when(w.getWritePlanProvider()).thenReturn(prov);
    assertEquals(EnumSet.of(WriteOperation.INSERT, WriteOperation.OVERWRITE), w.supportedWriteOperations());
    assertTrue(w.requiresParallelWrite());
    assertFalse(w.requiresPartitionLocalSort());
}
```

- [ ] **Step 4: Build + test.** `mvn -f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl :fe-connector-api -am -Dmaven.build.cache.enabled=false install -DskipTests` then run the test class in `:fe-core`. Expected: compile SUCCESS, test PASS. Checkstyle: `mvn -f .../fe/pom.xml -pl :fe-connector-api checkstyle:check`.
- [ ] **Step 5: Commit** (`git add` the 3 files by path):
  `[refactor](connector) 写能力单一来源(1/6)：ConnectorWritePlanProvider 补 supportedOperations/sink-trait 默认方法 + Connector null-safe 委派`

---

## Task 2: Connector provider declarations (iceberg + maxcompute)

**Files:**
- Modify: `fe/fe-connector/fe-connector-iceberg/src/main/java/org/apache/doris/connector/iceberg/IcebergWritePlanProvider.java`
- Modify: `fe/fe-connector/fe-connector-maxcompute/src/main/java/org/apache/doris/connector/maxcompute/MaxComputeWritePlanProvider.java`
- Test: `IcebergWritePlanProviderTest` (iceberg module), `MaxComputeWritePlanProviderTest` (maxcompute module) — new or existing.

**Interfaces consumed:** the 6 provider defaults from Task 1.

> JDBC needs no change — its provider inherits `supportedOperations() = {INSERT}` and all sink-traits false, which is exactly correct.

- [ ] **Step 1: Override in `IcebergWritePlanProvider`.** Add imports `java.util.EnumSet`, `java.util.Set`, `org.apache.doris.connector.api.handle.WriteOperation`. Add:

```java
    @Override
    public Set<WriteOperation> supportedOperations() {
        return EnumSet.of(WriteOperation.INSERT, WriteOperation.OVERWRITE,
                WriteOperation.DELETE, WriteOperation.MERGE, WriteOperation.REWRITE);
    }

    @Override
    public boolean supportsWriteBranch() {
        return true;
    }

    @Override
    public boolean requiresParallelWrite() {
        return true;
    }

    @Override
    public boolean requiresFullSchemaWriteOrder() {
        return true;
    }

    @Override
    public boolean requiresMaterializeStaticPartitionValues() {
        return true;
    }
```

- [ ] **Step 2: Override in `MaxComputeWritePlanProvider`.** Add the same imports. Add:

```java
    @Override
    public Set<WriteOperation> supportedOperations() {
        return EnumSet.of(WriteOperation.INSERT, WriteOperation.OVERWRITE);
    }

    @Override
    public boolean requiresParallelWrite() {
        return true;
    }

    @Override
    public boolean requiresFullSchemaWriteOrder() {
        return true;
    }

    @Override
    public boolean requiresPartitionLocalSort() {
        return true;
    }
```

- [ ] **Step 3: Provider unit tests.** In each connector's module (no Mockito — construct the real provider):

```java
// iceberg
@Test void declaresFullWriteOperationSet() {
    IcebergWritePlanProvider p = /* construct as existing tests do */;
    assertEquals(EnumSet.of(WriteOperation.INSERT, WriteOperation.OVERWRITE,
            WriteOperation.DELETE, WriteOperation.MERGE, WriteOperation.REWRITE), p.supportedOperations());
    assertTrue(p.supportsWriteBranch());
    assertTrue(p.requiresParallelWrite());
    assertTrue(p.requiresFullSchemaWriteOrder());
    assertTrue(p.requiresMaterializeStaticPartitionValues());
    assertFalse(p.requiresPartitionLocalSort()); // iceberg does NOT require partition-local sort
}
// maxcompute
@Test void declaresInsertOverwriteAndSinkTraits() {
    MaxComputeWritePlanProvider p = /* construct as existing tests do */;
    assertEquals(EnumSet.of(WriteOperation.INSERT, WriteOperation.OVERWRITE), p.supportedOperations());
    assertTrue(p.requiresParallelWrite());
    assertTrue(p.requiresFullSchemaWriteOrder());
    assertTrue(p.requiresPartitionLocalSort());
    assertFalse(p.supportsWriteBranch());
    assertFalse(p.requiresMaterializeStaticPartitionValues());
}
```

> If constructing the provider standalone is awkward (needs a connector context), assert through `connector.getWritePlanProvider()` instead — follow the existing `*WritePlanProviderTest` construction pattern in that module.

- [ ] **Step 4: Build + test** each module (`-pl :fe-connector-iceberg -am`, `-pl :fe-connector-maxcompute -am`, both need `package` for maxcompute if HiveConf shade applies — iceberg is fine with `test`). Checkstyle each (no `-am`).
- [ ] **Step 5: Commit:**
  `[refactor](connector) 写能力单一来源(2/6)：iceberg/maxcompute 写 provider 声明 supportedOperations + sink-trait`

---

## Task 3: Rewrite fe-core consumer sites + migrate behavioral test mocks

**Files (modify):**
- `PhysicalPlanTranslator.java` (2 gates), `InsertOverwriteTableCommand.java` (2 methods), `InsertIntoTableCommand.java` (1 method), `IcebergNereidsUtils.java` (1 method), `IcebergRowLevelDmlTransform.java` (1 method), `PluginDrivenExternalTable.java` (4 accessors)
- Tests (modify mocks): `IcebergNereidsUtilsTest.java`, `IcebergRowLevelDmlTransformTest.java`, `InsertIntoTableCommandTest.java`, `InsertOverwriteTableCommandTest.java`

**Interfaces consumed:** `Connector` delegators from Task 1; the iceberg/maxcompute declarations from Task 2 (so behavior is unchanged for those connectors).

- [ ] **Step 1: INSERT gate — `PhysicalPlanTranslator.visitPhysicalConnectorTableSink`.** Replace the provider-null guard (currently ~:732-737). Add import `org.apache.doris.connector.api.handle.WriteOperation` (and `java.util.Set` if needed).

  Before:
```java
ConnectorWritePlanProvider writePlanProvider = connector.getWritePlanProvider();
if (writePlanProvider == null) {
    throw new AnalysisException(
            "Connector '" + catalog.getName() + "' (type: " + catalog.getType()
                    + ") does not support INSERT operations");
}
```
  After:
```java
if (!connector.supportedWriteOperations().contains(WriteOperation.INSERT)) {
    throw new AnalysisException(
            "Connector '" + catalog.getName() + "' (type: " + catalog.getType()
                    + ") does not support INSERT operations");
}
ConnectorWritePlanProvider writePlanProvider = connector.getWritePlanProvider();
```
  (`writePlanProvider` is guaranteed non-null once the set contains INSERT, since a non-empty set implies a non-null provider.)

- [ ] **Step 2: Row-level DML gate — `PhysicalPlanTranslator.buildPluginRowLevelDmlSink`** (currently ~:684-689).

  Before:
```java
ConnectorWritePlanProvider writePlanProvider = connector.getWritePlanProvider();
if (writePlanProvider == null) {
    throw new AnalysisException(
            "Connector '" + catalog.getName() + "' (type: " + catalog.getType()
                    + ") does not support row-level DML operations");
}
```
  After:
```java
Set<WriteOperation> writeOps = connector.supportedWriteOperations();
if (!(writeOps.contains(WriteOperation.DELETE) || writeOps.contains(WriteOperation.MERGE))) {
    throw new AnalysisException(
            "Connector '" + catalog.getName() + "' (type: " + catalog.getType()
                    + ") does not support row-level DML operations");
}
ConnectorWritePlanProvider writePlanProvider = connector.getWritePlanProvider();
```

- [ ] **Step 3: `InsertOverwriteTableCommand.pluginConnectorSupportsInsertOverwrite`** (~:338-342). Add import `WriteOperation`.

  Before: `return catalog.getConnector().getMetadata(catalog.buildConnectorSession()).supportsInsertOverwrite();`
  After: `return catalog.getConnector().supportedWriteOperations().contains(WriteOperation.OVERWRITE);`

- [ ] **Step 4: `InsertOverwriteTableCommand.pluginConnectorSupportsWriteBranch`** (~:350-358).

  Before: `return catalog.getConnector().getMetadata(catalog.buildConnectorSession()).supportsWriteBranch();`
  After: `return catalog.getConnector().supportsWriteBranch();`

- [ ] **Step 5: `InsertIntoTableCommand.connectorSupportsWriteBranch`** (~:807-814).

  Before: `return catalog.getConnector().getMetadata(catalog.buildConnectorSession()).supportsWriteBranch();`
  After: `return catalog.getConnector().supportsWriteBranch();`

- [ ] **Step 6: `IcebergNereidsUtils.pluginConnectorSupportsRowLevelDml`** (~:157-167). Add import `WriteOperation`, `java.util.Set`. Keep the null-connector short-circuit; drop the metadata fetch.

  Before (lines 165-166): `ConnectorMetadata metadata = connector.getMetadata(catalog.buildConnectorSession());`
  `return metadata.supportsDelete() || metadata.supportsMerge();`
  After:
```java
Set<WriteOperation> ops = connector.supportedWriteOperations();
return ops.contains(WriteOperation.DELETE) || ops.contains(WriteOperation.MERGE);
```
  (Remove the now-unused `ConnectorMetadata metadata = ...` line. If `ConnectorMetadata` import becomes unused, remove it.)

- [ ] **Step 7: `IcebergRowLevelDmlTransform.pluginConnectorSupportsRowLevelDml`** (~:97-101). Add import `WriteOperation`, `java.util.Set`.

  Before:
```java
ConnectorMetadata metadata = catalog.getConnector().getMetadata(catalog.buildConnectorSession());
return metadata.supportsDelete() || metadata.supportsMerge();
```
  After:
```java
Set<WriteOperation> ops = catalog.getConnector().supportedWriteOperations();
return ops.contains(WriteOperation.DELETE) || ops.contains(WriteOperation.MERGE);
```
  (Leave `checkPluginMode`/`toWriteOperation`/`validateRowLevelDmlMode` untouched — that per-op mode check is retained.)

- [ ] **Step 8: `PluginDrivenExternalTable` — 4 sink-trait accessors.** Swap each `getCapabilities().contains(...)` for the provider-backed delegator. The `catalog instanceof PluginDrivenExternalCatalog` guard and `connector != null &&` stay.
  - `supportsParallelWrite()` (~:106-113): `... && connector.requiresParallelWrite();`
  - `requirePartitionLocalSortOnWrite()` (~:197-204): `... && connector.requiresPartitionLocalSort();`
  - `requiresFullSchemaWriteOrder()` (~:212-219): `... && connector.requiresFullSchemaWriteOrder();`
  - `materializeStaticPartitionValues()` (~:228-236): `... && connector.requiresMaterializeStaticPartitionValues();`
  (The `import ...ConnectorCapability;` stays for now — the other accessors still use it; it is removed in Task 5 if it becomes unused.)

- [ ] **Step 9: Migrate behavioral test mocks** (these stub the OLD boolean and assert downstream behavior — repoint them at the new path so they still exercise real coverage; do NOT delete):
  - `IcebergNereidsUtilsTest` (~:1019-1022) and `IcebergRowLevelDmlTransformTest` (~:95-121): currently stub `metadata.supportsDelete()/supportsMerge()`. Production now calls `connector.supportedWriteOperations()`. Repoint: stub `connector.getWritePlanProvider()` to return a provider whose `supportedOperations()` yields `{DELETE}` / `{MERGE}` / `{}` for the respective positive/negative cases (or stub `connector.supportedWriteOperations()` directly since it's a `CALLS_REAL_METHODS` default — stub the underlying `getWritePlanProvider()`). Assert the same downstream selection/transform behavior.
  - `InsertIntoTableCommandTest` (~:182-195): stubbed `supportsWriteBranch()` on metadata → repoint to stub `connector.getWritePlanProvider().supportsWriteBranch()` (or `connector.supportsWriteBranch()`). Keep the `@branch` gating assertion.
  - `InsertOverwriteTableCommandTest` (~:60-145): stubbed `supportsInsertOverwrite()` (:73) and `supportsWriteBranch()` (:91) → repoint to `supportedWriteOperations()` containing/omitting `OVERWRITE` and `supportsWriteBranch()`. Keep the mutation-guard comments at :111/:145 pointing at the new gate expressions.

- [ ] **Step 10: Build + test.** `-pl :fe-core -am` build; run the 4 migrated test classes + `IcebergConnectorMetadataTest` (still green — booleans still exist). Expected PASS. **Mutation check:** flip `contains(WriteOperation.INSERT)` → `contains(WriteOperation.OVERWRITE)` in the INSERT gate; confirm an INSERT-admission test fails; restore.
- [ ] **Step 11: Commit:**
  `[refactor](connector) 写能力单一来源(3/6)：准入点改读 Connector 委派 + 迁移行为门测试 mock`

---

## Task 4: Delete the 5 `ConnectorWriteOps` booleans + overrides + fix self-assertion tests

**Files (modify):**
- `ConnectorWriteOps.java` (−5 methods)
- `IcebergConnectorMetadata.java` (−4 overrides: supportsDelete/supportsMerge/supportsInsertOverwrite/supportsWriteBranch), `JdbcConnectorMetadata.java` (−supportsInsert), `MaxComputeConnectorMetadata.java` (−supportsInsert/−supportsInsertOverwrite)
- Tests: `FakeConnectorPluginTest.java`, `EsScanPlanProviderTest.java`, `JdbcDorisConnectorTest.java`, `IcebergConnectorMetadataTest.java`

**Precondition:** Task 3 removed all production readers of these 5 booleans; only tests reference them now.

- [ ] **Step 1: Delete the 5 default methods** from `ConnectorWriteOps` (`supportsInsert`, `supportsInsertOverwrite`, `supportsDelete`, `supportsMerge`, `supportsWriteBranch`). Keep `validateRowLevelDmlMode`, `validateStaticPartitionColumns`, `beginTransaction`. Update the class javadoc if it enumerates the removed methods.
- [ ] **Step 2: Delete the connector overrides** — iceberg metadata (4), jdbc metadata (1), maxcompute metadata (2). Remove any now-unused imports.
- [ ] **Step 3: Fix self-assertion (tautology) tests** — these assert a connector's own boolean about itself (Rule 9 violation), and no longer compile:
  - `FakeConnectorPluginTest.writeOpsCapabilitiesDefaultToFalse()` (~:163-165) → **delete the method** (the delegation coverage now lives in Task 1's delegator test).
  - `EsScanPlanProviderTest.testEsMetadataDoesNotSupportWrite()` (~:149-153) → **repurpose** to a behavior assertion: `assertTrue(esConnector.supportedWriteOperations().isEmpty());` (ES declares no write ops).
  - `JdbcDorisConnectorTest.testJdbcMetadataSupportsInsert()` (~:158-162) → **repurpose**: `assertEquals(EnumSet.of(WriteOperation.INSERT), jdbcConnector.supportedWriteOperations());` and `assertFalse(jdbcConnector.supportsWriteBranch());`.
  - `IcebergConnectorMetadataTest` (~:121-122, :318, :331) → **repurpose** to provider-level assertions (or delete if Task 2's `IcebergWritePlanProviderTest` already covers them): iceberg provider `supportedOperations()` contains DELETE/MERGE/OVERWRITE and `supportsWriteBranch()` true. Prefer deleting the redundant ones and keeping a single provider-level assertion to avoid duplication.
- [ ] **Step 4: Build + test** `-pl :fe-connector-api -am install -DskipTests`, then the 4 connector/fe-core test classes. Run `bash tools/check-connector-imports.sh` (expect only the known HMS false-positive). Checkstyle the touched modules.
- [ ] **Step 5: Commit:**
  `[refactor](connector) 写能力单一来源(4/6)：删除 ConnectorWriteOps 5 个写布尔 + 连接器覆写 + 同义反复测试`

---

## Task 5: Delete/move `ConnectorCapability` values + trim `getCapabilities()` + fix enum tests

**Files (modify):**
- `ConnectorCapability.java` (26 → 8 values)
- `IcebergConnector.java`, `MaxComputeDorisConnector.java`, `JdbcDorisConnector.java`, `PaimonConnector.java` (trim `getCapabilities()`)
- Tests: `IcebergConnectorTest.java`, `PaimonConnectorMetadataMvccTest.java`, `PluginDrivenMvccTableFactoryTest.java`

**Precondition:** Task 3 rewired the 4 sink-trait readers to the provider; no code reads the 18 removed enum values except the connector `getCapabilities()` producers (fixed here) and tests (fixed here).

- [ ] **Step 1: Reduce `ConnectorCapability` to 8 values.** Keep only: `SUPPORTS_MVCC_SNAPSHOT`, `SUPPORTS_PARTITION_STATS`, `SUPPORTS_COLUMN_AUTO_ANALYZE`, `SUPPORTS_TOPN_LAZY_MATERIALIZE`, `SUPPORTS_SHOW_CREATE_DDL`, `SUPPORTS_VIEW`, `SUPPORTS_NESTED_COLUMN_PRUNE`, `SUPPORTS_PASSTHROUGH_QUERY`. Delete the other 18 (12 dead + `SUPPORTS_INSERT` + `SUPPORTS_TIME_TRAVEL` + 4 moved sink-traits) and their javadoc. In the `SUPPORTS_PARTITION_STATS` javadoc, remove the `{@link #SUPPORTS_STATISTICS}` reference (delete the "distinct from ... table-level statistics" clause or rephrase without the dead link). Update the enum's class-level javadoc to reflect it is the escape-hatch static-switch layer (sink traits + write ops now live on the write provider).
- [ ] **Step 2: Trim `getCapabilities()` producers:**
  - `IcebergConnector` (~:311-319): keep `SUPPORTS_MVCC_SNAPSHOT, SUPPORTS_COLUMN_AUTO_ANALYZE, SUPPORTS_TOPN_LAZY_MATERIALIZE, SUPPORTS_SHOW_CREATE_DDL, SUPPORTS_VIEW, SUPPORTS_NESTED_COLUMN_PRUNE`. Remove `SUPPORTS_TIME_TRAVEL, SINK_REQUIRE_FULL_SCHEMA_ORDER, SINK_MATERIALIZE_STATIC_PARTITION_VALUES, SUPPORTS_PARALLEL_WRITE`. Update the surrounding javadoc (drop the TIME_TRAVEL mention at ~:264).
  - `MaxComputeDorisConnector` (~:186-192): all three (`SUPPORTS_PARALLEL_WRITE, SINK_REQUIRE_PARTITION_LOCAL_SORT, SINK_REQUIRE_FULL_SCHEMA_ORDER`) moved → **remove the `getCapabilities()` override entirely** (inherits the empty default). Remove now-unused imports.
  - `JdbcDorisConnector` (~:100-101): remove `SUPPORTS_INSERT` → `getCapabilities()` returns just `SUPPORTS_PASSTHROUGH_QUERY`.
  - `PaimonConnector` (~:194-213): remove `SUPPORTS_TIME_TRAVEL`; keep `SUPPORTS_MVCC_SNAPSHOT, SUPPORTS_PARTITION_STATS, SUPPORTS_COLUMN_AUTO_ANALYZE, SUPPORTS_SHOW_CREATE_DDL`.
- [ ] **Step 3: Fix enum tests:**
  - `IcebergConnectorTest.declaresMvccAndTimeTravelCapabilities()` (~:109-110): delete the `SUPPORTS_TIME_TRAVEL` assertion; keep the `SUPPORTS_MVCC_SNAPSHOT` one. Rename the method to `declaresMvccSnapshotCapability()`.
  - `PaimonConnectorMetadataMvccTest.connectorDeclaresMvccAndTimeTravelCapabilities()` (~:1160-1163): delete the `SUPPORTS_TIME_TRAVEL` assertion; keep `SUPPORTS_MVCC_SNAPSHOT`. Rename to drop "TimeTravel".
  - `PluginDrivenMvccTableFactoryTest` (~:66): the placeholder `catalogWithCapabilities(ConnectorCapability.SUPPORTS_FILTER_PUSHDOWN)` uses a now-deleted value → change to `ConnectorCapability.SUPPORTS_VIEW` (any surviving non-MVCC value; the test only needs "some non-MVCC capability").
- [ ] **Step 4: Grep-verify no dangling references.** `grep -rn "SUPPORTS_TIME_TRAVEL\|SUPPORTS_INSERT\|SUPPORTS_PARALLEL_WRITE\|SINK_REQUIRE_FULL_SCHEMA_ORDER\|SINK_REQUIRE_PARTITION_LOCAL_SORT\|SINK_MATERIALIZE_STATIC_PARTITION_VALUES\|SUPPORTS_FILTER_PUSHDOWN\|SUPPORTS_STATISTICS" fe/ --include=*.java` → expect zero hits after this task.
- [ ] **Step 5: Build + test** all touched connector modules + `:fe-core -am`. Run `IcebergConnectorTest`, `PaimonConnectorMetadataMvccTest`, `PluginDrivenMvccTableFactoryTest`, and a full `:fe-connector-iceberg` module test (watch for the known pre-existing flaky field-id classes — isolate-run to confirm). Checkstyle each module (no `-am`). `bash tools/check-connector-imports.sh`.
- [ ] **Step 6: Commit:**
  `[refactor](connector) 写能力单一来源(5/6)：ConnectorCapability 删 18 死/派生/迁移值(26→8) + 各连接器 getCapabilities 收口`

---

## Task 6: `ConnectorContractValidator` + registration hook + contract & gate tests

**Files:**
- Create: `fe/fe-connector/fe-connector-api/src/main/java/org/apache/doris/connector/api/ConnectorContractValidator.java`
- Modify: `fe/fe-core/src/main/java/org/apache/doris/connector/ConnectorPluginManager.java` (invoke at `createConnector`)
- Test: new `ConnectorContractTest` (parameterized over the 6 connectors), new admission gate tests in fe-core.

**Interfaces consumed:** the argless `Connector` delegators (Task 1).

- [ ] **Step 1: Write the validator** (pure structural invariants — callable at registration because the methods are argless):

```java
package org.apache.doris.connector.api;

import org.apache.doris.connector.api.handle.WriteOperation;

import java.util.Set;

/**
 * Fails loud at connector registration if a connector's declared write capabilities are internally
 * inconsistent. The invariants are structural (no table handle needed) and mirror the doc contracts the
 * removed {@code ConnectorCapability} javadoc stated only in prose.
 */
public final class ConnectorContractValidator {

    private ConnectorContractValidator() {}

    /** @throws IllegalStateException if any write-capability invariant is violated. */
    public static void validate(Connector connector, String catalogType) {
        Set<WriteOperation> ops = connector.supportedWriteOperations();
        // #2 branch-write implies plain INSERT is supported (branch is an INSERT modifier).
        if (connector.supportsWriteBranch() && !ops.contains(WriteOperation.INSERT)) {
            throw new IllegalStateException("Connector '" + catalogType
                    + "' declares supportsWriteBranch but its supportedOperations lacks INSERT");
        }
        // #3 partition-local-sort implies parallel write AND full-schema write order.
        if (connector.requiresPartitionLocalSort()
                && !(connector.requiresParallelWrite() && connector.requiresFullSchemaWriteOrder())) {
            throw new IllegalStateException("Connector '" + catalogType
                    + "' declares requiresPartitionLocalSort without requiresParallelWrite"
                    + " AND requiresFullSchemaWriteOrder");
        }
    }
}
```

- [ ] **Step 2: Invoke at registration.** In `ConnectorPluginManager.createConnector` (~:126-144), capture `provider.create(...)` into a local, validate, return:

  Before: `return provider.create(properties, context);`
  After:
```java
Connector connector = provider.create(properties, context);
ConnectorContractValidator.validate(connector, catalogType);
return connector;
```

- [ ] **Step 3: Parameterized contract test** (`ConnectorContractTest`, in whichever module can see all 6 connectors — likely fe-core with the connectors on the test classpath; else one test per connector module). Encodes invariant #1 as the per-connector expected set (the pragmatic "declaration = implementation" check — a naive `planWrite`-per-op probe is unsafe without real handles):

```java
static Stream<Arguments> connectors() {
    return Stream.of(
        arguments("iceberg", EnumSet.of(INSERT, OVERWRITE, DELETE, MERGE, REWRITE), true,  true,  false, true,  true),
        arguments("maxcompute", EnumSet.of(INSERT, OVERWRITE),                      false, true,  true,  true,  false),
        arguments("jdbc",     EnumSet.of(INSERT),                                    false, false, false, false, false),
        arguments("es",       EnumSet.noneOf(WriteOperation.class),                 false, false, false, false, false),
        arguments("trino",    EnumSet.noneOf(WriteOperation.class),                 false, false, false, false, false),
        arguments("paimon",   EnumSet.noneOf(WriteOperation.class),                 false, false, false, false, false));
    // columns: type, expectedOps, branch, parallel, localSort, fullSchema, materializeStatic
}

@ParameterizedTest @MethodSource("connectors")
void declaredWriteCapabilitiesMatchAndAreSelfConsistent(String type, Set<WriteOperation> ops,
        boolean branch, boolean parallel, boolean localSort, boolean fullSchema, boolean materialize) {
    Connector c = /* build the connector of `type` via ConnectorPluginManager or its provider */;
    assertEquals(ops, c.supportedWriteOperations());
    assertEquals(branch, c.supportsWriteBranch());
    assertEquals(parallel, c.requiresParallelWrite());
    assertEquals(localSort, c.requiresPartitionLocalSort());
    assertEquals(fullSchema, c.requiresFullSchemaWriteOrder());
    assertEquals(materialize, c.requiresMaterializeStaticPartitionValues());
    ConnectorContractValidator.validate(c, type); // structural invariants must hold
}
```

  Plus explicit negative tests for the validator (Rule 9 — assert the invariant bites):
```java
@Test void validatorRejectsBranchWithoutInsert() { /* fake connector: branch=true, ops={} → expect IllegalStateException */ }
@Test void validatorRejectsLocalSortWithoutParallelAndFullSchema() { /* localSort=true, parallel=false → expect throw */ }
```

- [ ] **Step 4: Admission gate + granularity tests** (fe-core, Mockito): a fake plugin connector declaring `{INSERT}` passes `visitPhysicalConnectorTableSink`; one declaring `{}` (null provider) is rejected with `does not support INSERT operations`. A connector declaring only `{INSERT}` has OVERWRITE rejected (`InsertOverwriteTableCommand`) and row-level DML rejected (`does not support row-level DML operations`), with the two messages distinct. These are the behavior-gate tests that go red if the admission logic regresses.
- [ ] **Step 5: Build + test.** `-pl :fe-core -am` + connector modules. **Mutation check:** break invariant #2 in the validator (drop `!`); confirm `validatorRejectsBranchWithoutInsert` fails; restore. Checkstyle. `check-connector-imports.sh`.
- [ ] **Step 6: Commit:**
  `[refactor](connector) 写能力单一来源(6/6)：ConnectorContractValidator + 注册期 fail-loud + 六连接器契约/准入门测试`

---

## Test strategy (Rule 9 — test intent, not literal values)

- **Deleted** (tautology / self-referential): `FakeConnectorPluginTest.writeOpsCapabilitiesDefaultToFalse`, the `supportsInsert()` self-asserts, the two `SUPPORTS_TIME_TRAVEL` self-asserts, redundant iceberg metadata self-asserts.
- **Repurposed to behavior**: ES/JDBC/iceberg capability tests now assert the observable `supportedWriteOperations()` / provider set, which change if the connector's real write plan changes.
- **Migrated (kept coverage)**: the 4 mock-and-assert-downstream tests repoint their stubs at the new delegator path — they still fail if the routing/gating logic regresses.
- **New behavior gates**: INSERT/OVERWRITE/row-level-DML admission over fake connectors; a business-logic regression turns them red.
- **New contract**: parameterized per-connector expected sets + structural invariants + validator negative tests.

## Self-review (done against the design)

- **Coverage:** design G1 (single-source, symmetric add) → Tasks 1-2 + contract test; G2 (real logic reads the capability seam) → Task 3; G3 (delete dead declarations + tautology tests) → Tasks 4/5 + test strategy; G4 (granular ops) → `supportedOperations` set + Task 6 granularity test. §A→T1/T2, §B→T5, §C→T3, §E→T6.
- **Deviations from design (all user-confirmed or drift-driven, noted inline):** (1) all new methods **argless** (design showed `(session, tableHandle)`) — user-confirmed, avoids a handle-resolution failure path; (2) `SINK_MATERIALIZE_STATIC_PARTITION_VALUES` moved to provider (new 4th sink-trait) — user-confirmed; (3) `SUPPORTS_NESTED_COLUMN_PRUNE` kept as an 8th enum value (design listed 7) — it has a live reader; (4) `validateStaticPartitionColumns` retained (design didn't list it) — same category as `validateRowLevelDmlMode`.
- **Type consistency:** provider method `supportedOperations()` vs. Connector delegator `supportedWriteOperations()` (deliberately distinct names — provider-local vs. engine-facing); sink-trait names identical on provider and delegator (pure delegation). Verified against every call site.
- **Sequencing / flip safety:** additive (T1/T2) → consumer rewire (T3) → deletions (T4/T5) → validator (T6); the build is green and independently committable at every task. This series is **decoupled from the iceberg flip** and must **not** be bundled into the flip's atomic push; pre-cutover iceberg is inert on these generic admission points (it still uses the legacy sink). Land it as its own commit series.

## ⚠️ Not covered here (raise before/after execution)
- Whether to run this series **now** (flip still stabilizing) or after the flip's second sign-off. Changes are safe either way (inert for pre-cutover iceberg); the only rule is do not co-push with the flip. Confirm timing with the user at execution.
