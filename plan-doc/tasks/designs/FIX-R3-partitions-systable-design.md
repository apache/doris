# FIX-R3 — `table$partitions` system table for flipped hive (`partition_values` TVF)

**Failing suite**: `external_table_p0/hive/test_hive_partition_values_tvf`
**Scope**: fe-connector-api (SPI, +1 default method) · fe-connector-hive (`HiveConnectorMetadata`) · fe-core (`PluginDrivenExternalTable.getSupportedSysTables`). **No BE change, no Nereids change.**

## Root cause (HEAD-verified, recon `wf_05998eee-e1d`)

A flipped plain-hive table is a `PluginDrivenExternalTable`. Its `getSupportedSysTables()`
(`PluginDrivenExternalTable.java:948-972`) discovers sys-table names from the connector
(`metadata.listSupportedSysTables`) and **wraps every name in a `PluginDrivenSysTable`** — a
`NativeSysTable` (native connector scan path). But `HiveConnectorMetadata.listSupportedSysTables`
(`HiveConnectorMetadata.java:1348-1354`) returns **`emptyList()`** for a real `HiveTableHandle`
("Hive exposes no system tables"). So:

- `getSupportedSysTables()` → empty map → `findSysTable("t$partitions")` → `Optional.empty`.
- `select … t$partitions` → `RelationUtil.validateForQuery` false → **`"Unknown sys table 't$partitions'"`**.
- `desc t$partitions` → `DescribeCommand.resolveForDescribe` empty → **`"sys table not found: partitions"`**.

Legacy `HMSExternalTable.getSupportedSysTables()` (`HMSExternalTable.java:1239-1251`) returned, for
`dlaType==HIVE`, `PartitionsSysTable.HIVE_SUPPORTED_SYS_TABLES = {"partitions": PartitionsSysTable.INSTANCE}`
— a **`TvfSysTable`** that routes `t$partitions` to the fe-core `partition_values` TVF
(`PartitionsSysTable.java:38,52,59`). That TVF and its BE→FE metadata fetch are **already fully
plugin-aware** (`PartitionValuesTableValuedFunction.analyzeAndGetTable` accepts
`PluginDrivenExternalTable` at :144-148; `MetadataGenerator.partitionValuesMetadataResultForPluginTable`
at :2131). The **only** gap is the `$partitions` suffix → sys-table → TVF *bridge*.

### Why fe-core cannot decide native-vs-TVF by name

The bare name `"partitions"` is **overloaded**:

| Source | `partitions` sys table | Served by |
|---|---|---|
| hive | TVF | fe-core `partition_values` TVF (`PartitionsSysTable`, a `TvfSysTable`) |
| iceberg (`MetadataTableType.PARTITIONS`) | native | `IcebergSysTableJniScanner` (native metadata scan) |
| paimon (`SystemTableLoader.SYSTEM_TABLES`) | native | native metadata scan |

Iceberg **and** paimon both report `"partitions"` (recon thread A, javap-confirmed on the SDK jars).
So a fe-core `if (name.equals("partitions")) → TVF` mapping would **misroute iceberg/paimon
`t$partitions` to the hive TVF** and break them. The native-vs-TVF decision is inherently
**connector-specific** and must be declared by the connector.

Nor can fe-core **infer** the kind from `getSysTableHandle` returning empty: the kind is needed at
*discovery* time in `getSupportedSysTables` (before any handle is fetched), `getSysTableHandle` is
eager and auth-bearing (paimon loads the SDK `Table`; calling it per name at discovery would force an
auth/metadata round-trip), and a native connector may legitimately omit a handle for some names
(iceberg omits `position_deletes`) — so empty ≠ TVF. The kind must be an explicit connector signal.

The connector cannot build the fe-core `TvfSysTable` itself — `createFunction`/`createFunctionRef`
return Nereids/fe-core types (`TableValuedFunction`, `TableValuedFunctionRefInfo`) that connectors
must not import. So the connector can only **declare the kind**; fe-core maps kind→concrete SysTable.

## Design — connector-declared kind, `supports*`-style opt-in

Mirrors the established connector opt-in convention in this migration (`supportsTableSample()`,
`supportsBatchScan()`, `adjustFileCompressType()` — all default-off hooks a connector overrides).
Iron-rule clean: fe-core keys on a **generic capability**, never a source name / `instanceof HMSExternal*`.

### 1. SPI (`ConnectorTableOps`, fe-connector-api) — +1 default method

```java
/**
 * Whether the named system table of {@code baseTableHandle} is served by the generic
 * {@code partition_values} table-valued function (fe-core's {@code PartitionsSysTable}), rather
 * than a native connector scan. Default {@code false} (native, the {@code getSysTableHandle} path).
 *
 * <p>A connector whose partitioned tables expose their partition rows through the generic
 * partition-values TVF (hive) overrides this to return {@code true} for that sys-table name; the
 * connector need NOT return a handle from {@link #getSysTableHandle} for such a name (the TVF path
 * does not consult it). {@code sysName} is the bare name (no {@code "$"}).</p>
 */
default boolean isPartitionValuesSysTable(ConnectorSession session,
        ConnectorTableHandle baseTableHandle, String sysName) {
    return false;
}
```

### 2. fe-core `PluginDrivenExternalTable.getSupportedSysTables()` — kind-aware wrap

The single wrap loop (`:968-970`) branches on the connector-declared kind:

```java
for (String sysName : names) {
    if (metadata.isPartitionValuesSysTable(session, handleOpt.get(), sysName)) {
        // Served by the generic partition_values TVF (fe-core PartitionsSysTable), not a native scan.
        // Key on the singleton's OWN name (== "partitions"): PartitionsSysTable strips its hard-wired
        // "$partitions" suffix in createFunction, so a map key that ever differed would crash
        // (StringIndexOutOfBounds). Same value as sysName for hive today; strictly safer. (F1)
        result.put(PartitionsSysTable.INSTANCE.getSysTableName(), PartitionsSysTable.INSTANCE);
    } else {
        result.put(sysName, new PluginDrivenSysTable(sysName));
    }
}
```

`PartitionsSysTable.INSTANCE` is a stateless singleton keyed `"partitions"` (its `getSysTableName()`),
matching the connector-returned key. `SysTableResolver` already routes any `TvfSysTable` to the TVF
path (`:58-62`) for SELECT / DESCRIBE / validate / SHOW-CREATE — **no new call site**.

### 3. fe-connector-hive `HiveConnectorMetadata`

- `listSupportedSysTables` hive branch: `emptyList()` → **`List.of("partitions")`**.
  **Unconditional** (every hive handle, partitioned or not) — matches legacy
  (`HMSExternalTable` returned the map for all `dlaType==HIVE`) and is **required** by the test:
  `select/desc non_partitioned_tbl$partitions` must reach the TVF ctor and throw
  `"… is not a partitioned table"` (`PartitionValuesTableValuedFunction:140/146`), not
  `"Unknown sys table"`. Gating on partitioned-only would surface the wrong error.
- New `isPartitionValuesSysTable` override — the foreign-handle guard is **load-bearing** (F2): a
  naive `return "partitions".equals(sysName)` without it would flip iceberg-on-HMS `t$partitions` to
  the hive TVF. Verbatim (mirrors `listSupportedSysTables:1349-1350` / `getSysTableHandle:1359-1361`):
  ```java
  @Override
  public boolean isPartitionValuesSysTable(ConnectorSession session,
          ConnectorTableHandle baseTableHandle, String sysName) {
      if (!(baseTableHandle instanceof HiveTableHandle)) {
          return siblingMetadata(session, baseTableHandle)
                  .isPartitionValuesSysTable(session, baseTableHandle, sysName);
      }
      return "partitions".equals(sysName);
  }
  ```
- `getSysTableHandle`: **unchanged** (stays `emptyList`/`empty` for hive; TVF path never consults it).

## Blast radius / non-regression

- **iceberg / paimon native catalogs**: don't override `isPartitionValuesSysTable` → default false →
  `t$partitions` stays native. Unchanged.
- **iceberg-on-HMS / hudi-on-HMS (sibling delegation)**: foreign handle → hive delegates
  `listSupportedSysTables` + `isPartitionValuesSysTable` to the sibling → sibling returns its native
  names + false → fe-core wraps native. `t$partitions` stays native. Unchanged.
- **Internal OLAP tables** (`pv_inner1$partitions`, test §12): default empty `getSupportedSysTables`
  → `"sys table not found"` / `"Unknown sys table"`. Untouched.
- **Direct `partition_values(...)` TVF** (test sql92/94/95): bypasses sys-table resolution entirely.
  Already worked; untouched.
- `PluginDrivenMvccExternalTable` (the subclass flipped hive actually instantiates) inherits
  `getSupportedSysTables` from the base — one fix covers both.

## Tests (TDD: RED → GREEN)

**fe-connector-api** — no test (trivial default; exercised via hive + fe-core).

**fe-connector-hive** (recording-fake, NO mockito):
- New `HiveConnectorMetadataSysTableTest`: hive handle →
  `listSupportedSysTables == ["partitions"]`; `isPartitionValuesSysTable(_, hive, "partitions") == true`,
  `== false` for `"snapshots"`/null; `getSysTableHandle(_, hive, "partitions")` empty.
- `HiveConnectorMetadataSiblingDelegationTest` (update, lockstep):
  - foreign-handle test: add `md.isPartitionValuesSysTable(null, foreignHandle, "partitions")` call;
    add `"isPartitionValuesSysTable"` to `EXPECTED_METHODS` (:462-469); add the recording override to
    `RecordingSiblingMetadata` (:630-638); assert the sibling's answer flows through.
  - hive-handle test (:192): flip `listSupportedSysTables(_, hive).isEmpty()` →
    `== ["partitions"]`; add `isPartitionValuesSysTable(_, hive, "partitions") == true`.

**fe-core** (mockito):
- `PluginDrivenSysTableTest`: add a case — `metadata.isPartitionValuesSysTable(session, baseHandle, "partitions")`
  stubbed true → `getSupportedSysTables().get("partitions")` is a `TvfSysTable` (`PartitionsSysTable`),
  and a native name (`"snapshots"`, stub false) stays a `PluginDrivenSysTable`. Assert `findSysTable`
  routes the TVF one through `SysTableResolver.resolveForPlan` → `forTvf`.

## Residual / e2e

- **e2e (user-run), required GREEN gate (F3)** — not optional residual:
  - `test_hive_partition_values_tvf` (all of sql01-113: `$partitions` suffix, desc, agg, view, join,
    non-partitioned error, all-types).
  - **iceberg-on-HMS + native iceberg/paimon `t$partitions` still return NATIVE rows** — this is the
    only proof of the F2 foreign-handle guard: the iceberg divert (`HiveConnectorMetadata:353`) is
    DORMANT at HEAD, so the recording-fake unit tests cannot exercise it. memory
    `hms-iceberg-delegation-needs-e2e`.
- `@Disabled HmsQueryCacheTest` references `PartitionsSysTable.HIVE_SUPPORTED_SYS_TABLES` (compiles,
  not run) — unaffected.
