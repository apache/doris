# P0 — Connector SPI 扩展 RFC v1

> 状态：草案 v1 · 日期 2026-05-24 · 阶段 P0 · 主计划 [`00-connector-migration-master-plan.md`](./00-connector-migration-master-plan.md)
> 评审人：FE 平台组、各 connector owner
> 范围：列出后续 6 个 connector 迁移所需的全部新增 SPI 类型 / 方法 / 默认行为，给出 Java 签名草稿与影响面分析。
> 不在范围：现有 SPI 的破坏性变更（D9 锁定 `apiVersion=1`）。

---

## 0. 摘要

| # | 扩展点 | 触发的迁移目标 | 入口包 | 影响阶段 |
|---|---|---|---|---|
| E1 | DDL Info / `ConnectorCreateTableRequest` | Hive、Iceberg、Paimon 的完整 CREATE TABLE | `connector.api.ddl` | P5/P6/P7 |
| E2 | Procedures / `ConnectorProcedureOps` | Iceberg 10 个 action | `connector.api.procedure` | P6 |
| E3 | Meta Invalidator / `ConnectorMetaInvalidator` | HMS event pipeline | `connector.spi` + `connector.api.events` | P7 |
| E4 | Transactions / `ConnectorTransaction` | Hive ACID、Iceberg、Paimon、MaxCompute | `connector.api`（扩展 `WriteOps`）| P5–P7 |
| E5 | MVCC Snapshot / `ConnectorMvccSnapshot` | Iceberg、Paimon | `connector.api.mvcc` | P5/P6 |
| E6 | Vended Credentials / `ConnectorCredentials` | Iceberg REST、Paimon REST、S3 Tables | `connector.api.scan` | P5/P6 |
| E7 | Sys Tables | Iceberg `$snapshots/$history/...`、Paimon | `connector.api`（扩展 `TableOps`）| P5/P6 |
| E8 | 列级 Statistics 写入 / `ConnectorColumnStatistics` | Hive ANALYZE | `connector.api.statistics`（扩展 `StatisticsOps`）| P7 |
| E9 | Delete / Merge sink 配置 | Iceberg DML | `connector.api.write`（扩展 `WriteConfig`/`WriteOps`）| P6 |
| E10 | Partition 列举 / `listPartitions` | MaxCompute、Paimon、Hive | `connector.api`（扩展 `TableOps`）| P4/P5/P7 |

**总体不变量**：

- 全部以 **default 方法**新增；现有 ES / JDBC 实现零修改。
- `ConnectorProvider.apiVersion()` 保持 `1`；`ConnectorPluginManager.CURRENT_API_VERSION` 保持 `1`。
- 任何新增类型不依赖 `org.apache.doris.{catalog,common,datasource,qe,analysis,nereids,planner}`。
- 与已有类型有命名冲突时，**复用旧的**（如 `ConnectorPartitionInfo` 复用、`ConnectorWriteConfig` 扩展而非重建）。

---

## 1. 目标与范围

**做什么**：把主计划 §2.1 的 10 项 SPI 缺口逐一展开到"可以发起 PR 的 Java 签名级别"。每项列：

1. 现状（旧实现锚点：fe-core 文件 + 关键调用方）。
2. 设计签名（接口 / 类草稿，含 javadoc 关键句）。
3. 默认行为（让旧 connector 零修改通过）。
4. fe-core 侧 converter / 适配（如有）。
5. 受影响连接器与验收标准。

**不做什么**：实现代码——本 RFC 只到接口和草稿层；实现在对应 Pn 阶段做。

---

## 2. 设计原则

### 2.1 向后兼容（核心）

- 每个新增方法都是 `default`，旧 connector 不实现也能编译通过。
- 默认行为分两类：
  - **能力声明类**（`supports*` / `listSysTableTypes`）→ 返回空 / false。
  - **必须实现才有意义类**（`createTable(request)` / `callProcedure`）→ `throw new DorisConnectorException("xxx not supported")`，由 fe-core 在调用前用对应 `ConnectorCapability` 判断。

### 2.2 包结构（在现有基础上微调）

```
fe-connector-api/src/main/java/org/apache/doris/connector/api/
├── (existing)                       Connector, ConnectorMetadata, ConnectorSession, ConnectorTableSchema, ...
├── ddl/                  [NEW]      ConnectorCreateTableRequest, ConnectorPartitionSpec, ConnectorBucketSpec
├── events/               [NEW]      ConnectorMetaInvalidator  ← 接口在 spi 包，类放 api 便于复用
├── mvcc/                 [NEW]      ConnectorMvccSnapshot
├── procedure/            [NEW]      ConnectorProcedureOps, ConnectorProcedureSpec, ConnectorProcedureArgument
├── statistics/           [NEW]      ConnectorColumnStatistics  ← ConnectorStatisticsOps 已在 api 包根
├── pushdown/             (existing)
├── scan/                 (existing) + [NEW] ConnectorCredentials
├── write/                (existing) + [NEW] ConnectorWriteType.DELETE / MERGE
└── handle/               (existing) + [NEW] ConnectorTransaction (replace placeholder)
```

`fe-connector-spi` 只新增一个 `ConnectorMetaInvalidator` 接口（放在 spi 包让 `ConnectorContext` 可以引用），其余都在 api。

### 2.3 命名一致性

- 接口名：`Connector*Ops` 表示一组操作（继承到 `ConnectorMetadata`），如 `ConnectorProcedureOps`。
- 值对象：`Connector*` 名词（如 `ConnectorCreateTableRequest`、`ConnectorMvccSnapshot`）。
- Handle：`Connector*Handle`（不可变标识 / opaque pointer）。

### 2.4 不在 SPI 暴露的东西

- Doris 内部类型：`Expr`、`Column`、`TableIf`、`CreateTableInfo`、`PartitionDesc` 等——fe-core 侧 converter 负责翻译。
- 任何 `org.apache.doris.thrift.*` 类只在 `ConnectorScanRange.populateRangeParams` / `ConnectorMetadata.buildTableDescriptor` / `ConnectorScanPlanProvider.populateScanLevelParams` 三个已有入口暴露，新增 SPI 不引入更多 thrift 依赖。

---

## 3. 扩展点速查矩阵

| 扩展 | 新增类型 | 新增 / 扩展的方法（节选） |
|---|---|---|
| E1 | `ConnectorCreateTableRequest`、`ConnectorPartitionSpec`、`ConnectorBucketSpec` | `ConnectorTableOps.createTable(session, request)` |
| E2 | `ConnectorProcedureOps`、`ConnectorProcedureSpec`、`ConnectorProcedureArgument` | `ConnectorMetadata extends ConnectorProcedureOps` |
| E3 | `ConnectorMetaInvalidator`（spi 包接口） | `ConnectorContext.getMetaInvalidator()` |
| E4 | `ConnectorTransaction`（继承自旧的 `ConnectorTransactionHandle`） | `ConnectorWriteOps.beginTransaction(session)`、`commit/rollback` |
| E5 | `ConnectorMvccSnapshot` | `ConnectorMetadata.beginQuerySnapshot / getSnapshotAt / getSnapshotById` |
| E6 | `ConnectorCredentials` | `ConnectorScanPlanProvider.getCredentialsForScans(session, handle, ranges) → Map<ConnectorScanRange, ConnectorCredentials>` |
| E7 | — | `ConnectorTableOps.listSysTableTypes(handle)` + 通过 `getTableHandle("tbl$snapshots")` 暴露 |
| E8 | `ConnectorColumnStatistics` | `ConnectorStatisticsOps.setColumnStatistics(...)` |
| E9 | `ConnectorWriteType.DELETE` / `MERGE_DELETE` / `MERGE_INSERT` 三个新枚举值 | `ConnectorWriteOps.getDeleteConfig / getMergeConfig` |
| E10 | — | `ConnectorTableOps.listPartitionNames` + `listPartitions(handle, filter)` |
| E11 | `ConnectorWritePlanProvider`、`ConnectorSinkPlan`、`ConnectorWriteHandle`（写包）| `Connector.getWritePlanProvider()`、`ConnectorTransaction.addCommitData / supportsWriteBlockAllocation / allocateWriteBlockRange / getUpdateCnt`（[D-022]；详见 §20 + 写 RFC）|

---

## 4. 扩展 E1：DDL Info / `ConnectorCreateTableRequest`

### 4.1 现状

- `IcebergMetadataOps.createTable(CreateTableInfo)` 直接吃 nereids 的 `CreateTableInfo`（含 `ColumnDefinition`、`PartitionTableInfo`、`DistributionDescriptor`、`engine`、`properties`）。
- `HiveMetadataOps.createTable(CreateTableInfo)` 同上。
- 现有 SPI 的 `ConnectorTableOps.createTable(session, ConnectorTableSchema, Map<String,String>)` **没有分区 / 分桶 / external / ifNotExists 概念**。

### 4.2 设计

```java
// connector.api.ddl.ConnectorCreateTableRequest
package org.apache.doris.connector.api.ddl;

public final class ConnectorCreateTableRequest {
    private final String dbName;
    private final String tableName;
    private final List<ConnectorColumn> columns;
    private final ConnectorPartitionSpec partitionSpec;  // nullable
    private final ConnectorBucketSpec bucketSpec;        // nullable
    private final String comment;
    private final Map<String, String> properties;
    private final boolean ifNotExists;
    private final boolean external;            // EXTERNAL TABLE
    // builder + getters omitted
}

public final class ConnectorPartitionSpec {
    public enum Style {
        IDENTITY,                    // Hive style: partition by col1, col2
        TRANSFORM,                   // Iceberg style: bucket(N, col) / truncate(N, col) / years(col) / ...
        LIST,                        // Doris style: PARTITION BY LIST
        RANGE,                       // Doris style: PARTITION BY RANGE
    }
    private final Style style;
    private final List<ConnectorPartitionField> fields;
    private final List<ConnectorPartitionValueDef> initialValues;  // for LIST/RANGE
}

public final class ConnectorPartitionField {
    private final String columnName;
    private final String transform;        // "identity" | "bucket" | "truncate" | "year" | "month" | "day" | "hour"
    private final List<Integer> transformArgs; // e.g., [16] for bucket(16, ...)
}

public final class ConnectorBucketSpec {
    private final List<String> columns;
    private final int numBuckets;
    private final String algorithm;        // "hive_hash" | "iceberg_bucket" | "doris_default"
}
```

### 4.3 在 `ConnectorTableOps` 新增

```java
public interface ConnectorTableOps {
    // ... existing ...

    /**
     * Creates a table with full DDL semantics (partition, bucket, external, IF NOT EXISTS).
     *
     * <p>Connectors should override this method when they support advanced CREATE TABLE
     * options. The default implementation degrades to the legacy
     * {@link #createTable(ConnectorSession, ConnectorTableSchema, Map)} for backward
     * compatibility, dropping partition / bucket / external info.</p>
     *
     * @throws DorisConnectorException if the connector cannot honor the request
     */
    default void createTable(ConnectorSession session,
            ConnectorCreateTableRequest request) {
        ConnectorTableSchema schema = new ConnectorTableSchema(
                request.getTableName(), request.getColumns(),
                null, request.getProperties());
        createTable(session, schema, request.getProperties());
    }
}
```

### 4.4 fe-core 侧 converter

新增 `fe/fe-core/src/main/java/org/apache/doris/connector/ddl/CreateTableInfoToConnectorRequestConverter.java`：

```java
public final class CreateTableInfoToConnectorRequestConverter {
    public static ConnectorCreateTableRequest convert(CreateTableInfo info,
                                                       String dbName) {
        return ConnectorCreateTableRequest.builder()
                .dbName(dbName)
                .tableName(info.getTableNameInfo().getTbl())
                .columns(convertColumns(info.getColumns()))
                .partitionSpec(convertPartition(info.getPartitionTableInfo()))
                .bucketSpec(convertBucket(info.getDistributionDesc()))
                .comment(info.getComment())
                .properties(info.getProperties())
                .ifNotExists(info.isIfNotExists())
                .external(info.isExternal())
                .build();
    }
    // ... convertColumns / convertPartition / convertBucket
}
```

`PluginDrivenExternalCatalog` 不需要改——CREATE TABLE 经由 `ExternalCatalog.createTable(...)` 入口，新加一段：

```java
public class PluginDrivenExternalCatalog extends ExternalCatalog {
    @Override
    public boolean createTable(CreateTableStmt stmt) throws UserException {
        ConnectorSession s = buildConnectorSession();
        ConnectorCreateTableRequest req = CreateTableInfoToConnectorRequestConverter
                .convert(stmt.getCreateTableInfo(), stmt.getDbName());
        connector.getMetadata(s).createTable(s, req);
        return true;
    }
}
```

### 4.5 影响的连接器

| 连接器 | 必须实现 | 备注 |
|---|---|---|
| Hive | 是 | 当前 fe-core 用 `CreateTableInfo` 直接构造 Hive table，要还原全部分区 / 分桶逻辑 |
| Iceberg | 是 | Iceberg transform spec 是最复杂的，已是 connector 化重点 |
| Paimon | 是 | bucket spec 必须 |
| JDBC | 不需要 | 已经在用旧 createTable，无 partition / bucket 需求 |
| ES | 不需要 | 不支持 CREATE TABLE |
| 其他（MaxCompute/Trino/Hudi）| 取决于是否支持 CREATE TABLE | MaxCompute 支持 partition；Trino-connector 透传；Hudi 不支持 |

### 4.6 验收标准

- `mvn -pl fe-connector-api compile` 通过。
- 一个测试 connector（在 `fe-connector-api/src/test`）只用旧 `createTable(session, schema, props)` 也能编译。
- fe-core `CreateTableInfoToConnectorRequestConverter` 单测覆盖 Hive 风格 / Iceberg transform / List partition 三种来源。

---

## 5. 扩展 E2：Procedures / `ConnectorProcedureOps`

### 5.1 现状

- `fe/fe-core/src/main/java/org/apache/doris/datasource/iceberg/action/BaseIcebergAction.java` 抽象基类。
- 10 个子类：`IcebergCherrypickSnapshotAction`、`IcebergExpireSnapshotsAction`、`IcebergFastForwardAction`、`IcebergPublishChangesAction`、`IcebergRewriteDataFilesAction`、`IcebergRewriteManifestsAction`、`IcebergRollbackToSnapshotAction`、`IcebergRollbackToTimestampAction`、`IcebergSetCurrentSnapshotAction`。
- `IcebergExecuteActionFactory` 按 procedure 名 dispatch。
- 入口：`CALL iceberg.system.rewrite_data_files(...)` 之类语法 → nereids `ExecuteCommand` → `ExternalCatalog.executeAction(...)`（当前是 `IcebergExternalCatalog` 实现）。

### 5.2 设计

```java
// connector.api.procedure.ConnectorProcedureOps
package org.apache.doris.connector.api.procedure;

public interface ConnectorProcedureOps {

    /**
     * Lists all procedures this connector exposes.
     *
     * <p><b>Lifecycle contract (U1)</b>: the returned set MUST be stable across
     * the connector instance's lifetime. fe-core may cache this list, and changes
     * in the external system (e.g., a server-side plugin install) will only be
     * visible after the catalog is dropped and re-created.</p>
     */
    default List<ConnectorProcedureSpec> listProcedures() {
        return Collections.emptyList();
    }

    /**
     * Executes a named procedure with bound arguments.
     *
     * <p>Argument values follow the {@link ConnectorType} system:
     * boxed primitives, {@link String}, {@link java.time.Instant}, {@link java.util.List}, {@link Map}.</p>
     *
     * @param session    connector session
     * @param procedureName fully qualified procedure name (e.g., "rewrite_data_files")
     * @param arguments  name → bound value
     * @return procedure-specific result map (e.g., "rewritten_data_files_count" → 42)
     * @throws DorisConnectorException if the procedure name is unknown or args are invalid
     */
    default Map<String, Object> callProcedure(ConnectorSession session,
            String procedureName, Map<String, Object> arguments) {
        throw new DorisConnectorException(
                "Procedure not supported: " + procedureName);
    }
}

public final class ConnectorProcedureSpec {
    private final String name;
    private final String description;
    private final List<ConnectorProcedureArgument> arguments;
    // builder + getters
}

public final class ConnectorProcedureArgument {
    private final String name;
    private final ConnectorType type;
    private final boolean required;
    private final Object defaultValue;     // boxed, may be null
    // builder + getters
}
```

### 5.3 在 `ConnectorMetadata` 加入 super interface

```java
public interface ConnectorMetadata extends
        ConnectorSchemaOps,
        ConnectorTableOps,
        ConnectorPushdownOps,
        ConnectorStatisticsOps,
        ConnectorWriteOps,
        ConnectorIdentifierOps,
        ConnectorProcedureOps,    // [NEW]
        Closeable { ... }
```

### 5.4 fe-core 侧适配

把 `ExecuteCommand`（nereids）改为：

```java
public class ExecuteCommand extends Command {
    public void run(ConnectContext ctx) {
        ExternalCatalog cat = ...;
        if (cat instanceof PluginDrivenExternalCatalog) {
            PluginDrivenExternalCatalog pdc = (PluginDrivenExternalCatalog) cat;
            ConnectorSession s = pdc.buildConnectorSession();
            Map<String, Object> result = pdc.getConnector()
                    .getMetadata(s)
                    .callProcedure(s, procedureName, argsMap);
            displayResult(result);
            return;
        }
        // legacy path (kept until P6 completes)
    }
}
```

`IcebergConnectorMetadata.callProcedure` 内部走原 `BaseIcebergAction` 的 10 个子类实现（搬到 connector 内）。

### 5.5 影响连接器

- Iceberg（必须，10 procedure）。
- Paimon（可选，未来可加 expire-snapshots 等）。
- 其他连接器：不实现。

### 5.6 验收标准

- 默认行为：未实现 procedure 的 connector 调用时抛清晰错误，不导致 NPE。
- `ConnectorProcedureSpec` 通过 `Connector.getMetadata(...).listProcedures()` 暴露，可被 `SHOW PROCEDURES FROM <catalog>` 列出（**附：** 是否需要这条 SQL 也加 SPI 入口？建议留到 P6 评估）。

---

## 6. 扩展 E3：Meta Invalidator / `ConnectorMetaInvalidator`

### 6.1 现状

- `fe/fe-core/src/main/java/org/apache/doris/datasource/hive/event/MetastoreEventsProcessor.java` 是后台线程。
- 21 个 `MetastoreEvent` 子类（`CreateTableEvent`、`AlterPartitionEvent`、`InsertEvent`...）封装 HMS `NotificationEvent`。
- 事件处理流：HMS API → `EventFactory` → 解析为 `MetastoreEvent` → `event.process()` → 调 fe-core `ExternalMetaCacheMgr.invalidateTableCache(...)`。

### 6.2 设计（D4：把 event 流程整体搬到 fe-connector-hms）

```java
// connector.spi.ConnectorMetaInvalidator  ← 放 spi 包，让 ConnectorContext 可引用
package org.apache.doris.connector.spi;

public interface ConnectorMetaInvalidator {

    ConnectorMetaInvalidator NOOP = new ConnectorMetaInvalidator() { };

    /** Invalidates the entire catalog's metadata caches. */
    default void invalidateAll() { }

    /** Invalidates cached metadata for one database. */
    default void invalidateDatabase(String dbName) { }

    /** Invalidates cached metadata for one table. */
    default void invalidateTable(String dbName, String tableName) { }

    /**
     * Invalidates cached partition info for one partition.
     * @param partitionValues partition column values in declared order (e.g., ["2024", "01"])
     */
    default void invalidatePartition(String dbName, String tableName,
            List<String> partitionValues) { }

    /** Invalidates cached statistics for one table (without dropping schema cache). */
    default void invalidateStatistics(String dbName, String tableName) { }
}
```

### 6.3 在 `ConnectorContext` 暴露

```java
public interface ConnectorContext {
    // ... existing ...

    /**
     * Returns the meta invalidator that the connector can call to notify
     * the engine of external metadata changes (e.g., from HMS notification events).
     */
    default ConnectorMetaInvalidator getMetaInvalidator() {
        return ConnectorMetaInvalidator.NOOP;
    }
}
```

### 6.4 fe-core 侧实现

`DefaultConnectorContext` 提供基于 `ExternalMetaCacheMgr` + 当前 catalogId 的实例：

```java
public class DefaultConnectorContext implements ConnectorContext {
    @Override
    public ConnectorMetaInvalidator getMetaInvalidator() {
        return new ExternalMetaCacheInvalidator(this.catalogId);
    }
}

// fe/fe-core/.../connector/ExternalMetaCacheInvalidator.java [NEW]
public class ExternalMetaCacheInvalidator implements ConnectorMetaInvalidator {
    private final long catalogId;
    public ExternalMetaCacheInvalidator(long catalogId) { this.catalogId = catalogId; }

    @Override
    public void invalidateTable(String dbName, String tableName) {
        Env.getCurrentEnv().getExtMetaCacheMgr()
                .invalidateTableCache(catalogId, dbName, tableName);
    }
    // ... other methods delegate to ExternalMetaCacheMgr
}
```

### 6.5 fe-connector-hms 侧迁移

整体 move：

```
mv fe/fe-core/src/main/java/org/apache/doris/datasource/hive/event/*
   fe/fe-connector/fe-connector-hms/src/main/java/org/apache/doris/connector/hms/events/
```

`MetastoreEventsProcessor` 的构造参数从 `HMSExternalCatalog` 改为 `(HmsClient, ConnectorMetaInvalidator)`。每个 event 类 `process()` 改为调 `invalidator.invalidateXxx`，而不是 fe-core 的 `ExternalMetaCacheMgr`。

启动：`HiveConnector` 在 `create(...)` 时启动一个 `MetastoreEventsProcessor` 后台线程；`close()` 时停掉。

### 6.6 影响

- 仅 Hive / HMS（其它 connector 不需要 event）。
- fe-core `ExternalMetaCacheMgr` API 表面不变；只是被调用方从 `MetastoreEventsProcessor` 变为 `ExternalMetaCacheInvalidator`。

### 6.7 验收标准

- `fe-connector-hms` 不再 import 任何 `org.apache.doris.datasource.*`。
- 现有的 HMS event 集成测试（如果有）继续通过。
- 在没有 event listener 的连接器上，`ConnectorContext.getMetaInvalidator()` 返回 NOOP，无任何后台线程开销。

---

## 7. 扩展 E4：Transactions / `ConnectorTransaction`

### 7.1 现状

- `fe/fe-core/.../transaction/TransactionManagerFactory.java` 按 catalog 类型 switch：
  - HMS → `HiveTransactionManager`（包 `HiveTransactionMgr`，包 `HMSTransaction`）
  - Iceberg → `IcebergTransactionManager`（包 `IcebergTransaction`）
  - PluginDriven → `PluginDrivenTransactionManager`（占位）
- 每个 `*Transaction` 类持有 commit/rollback 状态：snapshot id、staged files、partition adds 等。

### 7.2 设计

将占位的 `ConnectorTransactionHandle`（24 行的空接口）扩展为可用的 `ConnectorTransaction`：

```java
// connector.api.handle.ConnectorTransaction  ← 同包替换占位
package org.apache.doris.connector.api.handle;

public interface ConnectorTransaction extends ConnectorTransactionHandle, Closeable {

    /** Stable transaction ID assigned by the connector. */
    long getTransactionId();

    /**
     * Commits all pending operations bound to this transaction.
     *
     * @throws DorisConnectorException on conflict / IO failure / external system error
     */
    void commit();

    /**
     * Aborts all pending operations and releases resources.
     * Safe to call multiple times; subsequent calls are no-ops.
     */
    void rollback();

    /** Called by the engine after commit OR rollback to release connections etc. */
    @Override
    void close();
}
```

### 7.3 在 `ConnectorWriteOps` 扩展

```java
public interface ConnectorWriteOps {
    // ... existing beginInsert/finishInsert/abortInsert/beginDelete/... ...

    /**
     * Begins a new transaction scoped to a single SQL statement (auto-commit) or to
     * an explicit BEGIN..COMMIT block. The returned transaction is passed to subsequent
     * begin* / finish* / abort* calls via the same {@link ConnectorSession}.
     *
     * <p>Connectors that do not support multi-statement transactions can either:</p>
     * <ul>
     *   <li>Return a no-op transaction whose commit/rollback do nothing.</li>
     *   <li>Throw, in which case the engine treats every statement as auto-commit.</li>
     * </ul>
     */
    default ConnectorTransaction beginTransaction(ConnectorSession session) {
        throw new DorisConnectorException("Transactions not supported");
    }
}
```

### 7.4 fe-core 侧改造

`PluginDrivenTransactionManager` 改为通用：

```java
public class PluginDrivenTransactionManager implements TransactionManager {
    private final Map<Long, ConnectorTransaction> active = new ConcurrentHashMap<>();

    public ConnectorTransaction begin(Connector c, ConnectorSession s) {
        ConnectorTransaction tx = c.getMetadata(s).beginTransaction(s);
        active.put(tx.getTransactionId(), tx);
        return tx;
    }

    @Override
    public void commit(long txId) {
        ConnectorTransaction tx = active.remove(txId);
        if (tx != null) { tx.commit(); tx.close(); }
    }

    @Override
    public void rollback(long txId) {
        ConnectorTransaction tx = active.remove(txId);
        if (tx != null) { tx.rollback(); tx.close(); }
    }
}
```

`TransactionManagerFactory` 在 P7/P8 删除 HMS / Iceberg 分支，只留 PluginDriven 一种。

### 7.5 影响

- Hive、Iceberg、Paimon、MaxCompute（4 个有事务的连接器）。
- JDBC、ES、Trino-connector：返回 no-op transaction 或抛 unsupported。

### 7.6 与旧 `beginInsert` 的关系

旧 `beginInsert(session, handle, columns) -> ConnectorInsertHandle` 不变；新增的 `beginTransaction` 是"包含 begin/end 的更高阶事务"。连接器有两种用法：

1. **简单**：不实现 `beginTransaction`，每次 `beginInsert` 内部自管事务（适合 JDBC）。
2. **复杂**：实现 `beginTransaction`，`beginInsert` 内部把 work 挂到当前 `ConnectorSession` 关联的事务上（适合 Iceberg / Hive ACID）。

`ConnectorSession` 新增可选字段：

```java
public interface ConnectorSession {
    // ... existing ...
    default Optional<ConnectorTransaction> getCurrentTransaction() {
        return Optional.empty();
    }
}
```

fe-core 用 `ConnectorSessionImpl` 在事务期间填入。

### 7.7 验收标准

- 已有 JDBC 测试（auto-commit）继续通过。
- 新增一个 mock 事务 connector 测试 BEGIN/COMMIT 路径。

---

## 8. 扩展 E5：MVCC Snapshot / `ConnectorMvccSnapshot`

### 8.1 现状

- `fe/fe-core/.../iceberg/IcebergMvccSnapshot.java` 包装 Iceberg snapshot id + timestamp。
- `fe/fe-core/.../paimon/PaimonMvccSnapshot.java` 同上。
- 调用方：nereids `MvccSnapshot` 接口在分析阶段查询 snapshot；scan plan 使用 snapshot id。

### 8.2 设计

```java
// connector.api.mvcc.ConnectorMvccSnapshot
package org.apache.doris.connector.api.mvcc;

public final class ConnectorMvccSnapshot {
    private final long snapshotId;
    private final long timestampMillis;
    private final String description;
    private final Map<String, String> properties;     // connector-specific metadata
    // builder + getters
}
```

### 8.3 在 `ConnectorMetadata` 新增

```java
public interface ConnectorMetadata extends ... {

    /**
     * Returns the current snapshot at query begin time, used as the MVCC pin for
     * all subsequent reads of {@code handle}. Returning {@link Optional#empty()}
     * means the connector does not support MVCC and reads see whatever is current.
     */
    default Optional<ConnectorMvccSnapshot> beginQuerySnapshot(
            ConnectorSession session, ConnectorTableHandle handle) {
        return Optional.empty();
    }

    /** Returns the snapshot at the given wall-clock time, or empty if none. */
    default Optional<ConnectorMvccSnapshot> getSnapshotAt(
            ConnectorSession session, ConnectorTableHandle handle,
            long timestampMillis) {
        return Optional.empty();
    }

    /** Returns the snapshot with the given id, or empty if none. */
    default Optional<ConnectorMvccSnapshot> getSnapshotById(
            ConnectorSession session, ConnectorTableHandle handle,
            long snapshotId) {
        return Optional.empty();
    }
}
```

### 8.4 fe-core 侧

新增 `ConnectorMvccSnapshotAdapter` 实现 fe-core 的 `MvccSnapshot` 接口，包 `ConnectorMvccSnapshot`。`PluginDrivenExternalTable` 在 `getMvccSnapshot(...)` 中返回 adapter 实例。

### 8.5 影响

- Iceberg、Paimon 必须实现。
- Hudi 可选（incremental query 时序）。
- 其他 connector 默认返回 `Optional.empty()`，fe-core 退化到非 MVCC 读。

### 8.6 验收标准

- Iceberg / Paimon connector 实现后能传 snapshot id 到 BE。
- `SELECT * FROM tbl FOR VERSION AS OF 123` / `FOR TIMESTAMP AS OF '...'` 路径走通。

---

## 9. 扩展 E6：Vended Credentials / `ConnectorCredentials`

### 9.1 现状

- `fe/fe-core/.../iceberg/IcebergVendedCredentialsProvider.java`、`PaimonVendedCredentialsProvider.java` 在 fe-core 通过 `instanceof` 探测，再调 connector 的 REST catalog 拿 STS 凭证。
- 凭证传给 BE：嵌在 `TFileScanRangeParams.location_properties` 里。
- `ConnectorCapability.SUPPORTS_VENDED_CREDENTIALS` 已存在。

### 9.2 设计

```java
// connector.api.scan.ConnectorCredentials
package org.apache.doris.connector.api.scan;

public final class ConnectorCredentials {
    private final Map<String, String> credentials;     // e.g., aws_access_key / aws_secret_key / session_token
    private final long expiryEpochMillis;              // -1 = no expiry
    // builder + getters
}
```

### 9.3 在 `ConnectorScanPlanProvider` 新增

```java
public interface ConnectorScanPlanProvider {
    // ... existing ...

    /**
     * Returns short-lived credentials for a batch of scan ranges in a single call.
     *
     * <p>Batch semantics let the connector amortize STS / vending API calls:</p>
     * <ul>
     *   <li>One STS call for all ranges → return a {@link Map} that maps every range
     *       to the <em>same</em> {@link ConnectorCredentials} instance.</li>
     *   <li>Group ranges by location prefix (e.g., S3 bucket / prefix) → return a
     *       map where ranges in the same group share an instance.</li>
     *   <li>One credential per range → return a map with distinct instances per key.</li>
     * </ul>
     *
     * <p>The returned map's keys must be a subset of {@code scanRanges}; any range
     * not present in the map will scan without vended credentials (the engine falls
     * back to the catalog-level filesystem properties).</p>
     *
     * <p>Connectors that do not vend credentials should return
     * {@link Collections#emptyMap()} (the default).</p>
     *
     * @param session    current session
     * @param handle     the table being scanned
     * @param scanRanges all ranges produced by {@link #planScan} for this scan node
     * @return per-range credentials map (instances may be shared across keys)
     */
    default Map<ConnectorScanRange, ConnectorCredentials> getCredentialsForScans(
            ConnectorSession session,
            ConnectorTableHandle handle,
            List<ConnectorScanRange> scanRanges) {
        return Collections.emptyMap();
    }
}
```

### 9.4 fe-core 侧

`PluginDrivenScanNode` 在 `createScanRangeLocations()` 完成后、`setScanParams` 之前做一次批量调用并缓存结果：

```java
public class PluginDrivenScanNode extends FileQueryScanNode {
    // ... existing fields ...
    private Map<ConnectorScanRange, ConnectorCredentials> cachedCredentials;

    @Override
    public void createScanRangeLocations() throws UserException {
        super.createScanRangeLocations();

        if (connector.getCapabilities().contains(ConnectorCapability.SUPPORTS_VENDED_CREDENTIALS)) {
            List<ConnectorScanRange> ranges = collectScanRanges();   // already on hand from getSplits()
            cachedCredentials = scanProvider.getCredentialsForScans(
                    connectorSession, currentHandle, ranges);
        }
        // ... existing populateScanLevelParams etc.
    }

    @Override
    protected void setScanParams(TFileRangeDesc rangeDesc, Split split) {
        // ... existing tableFormatFileDesc construction ...
        if (cachedCredentials != null) {
            ConnectorScanRange range = ((PluginDrivenSplit) split).getConnectorScanRange();
            ConnectorCredentials c = cachedCredentials.get(range);   // null = no vended creds for this range
            if (c != null) {
                mergeIntoLocationProps(rangeDesc, c.getCredentials());
            }
        }
    }
}
```

**关键不变量**：
- `getCredentialsForScans` 在一个 scan node 生命周期内只被调用一次。
- 返回 map 的 value 可以共享实例 —— 单次 STS call、N 个 range 同一组凭证是常态而非例外。
- 返回 map 的 key 是输入 list 的子集 —— 缺失的 range 退化到 catalog-level FS properties，**不报错**。

### 9.5 影响

- Iceberg REST catalog、Paimon REST catalog、S3 Tables。
- 其他 connector 不实现。

### 9.6 验收标准

- Iceberg REST + S3 vended path 跑通查询。
- 凭证不出现在 EXPLAIN / SHOW CREATE 输出（mask test）。
- **STS 调用频次回归**：一个 scan node 不论 split 数量多少，对外只触发 1 次 STS 调用（除非连接器主动按 prefix 分组）。在 `IcebergConnectorMetadataTest` 或同级集成测试里加 mock STS 计数器断言。

---

## 10. 扩展 E7：Sys Tables

> ⚠️ **本节 §10.2/§10.3 的「`$`-后缀普通表 + 连接器 `getTableHandle` 内解析后缀 + `listSysTableSuffixes`」设计已被 D-039 / DV-023 取代（superseded 2026-06-10，P5-B4 实现时）。** 该设计**从未落地**；live fe-core 实际用 `SysTableResolver` + `NativeSysTable` + `TableIf.getSupportedSysTables/findSysTable`（iceberg + legacy-paimon 共用）。P5-B4 复用该 live 机制：连接器 SPI 加 `ConnectorTableOps.listSupportedSysTables` + `getSysTableHandle`（default no-op），fe-core 加通用 `PluginDrivenSysTable extends NativeSysTable` + `PluginDrivenSysExternalTable`（报 `PLUGIN_EXTERNAL_TABLE`，经 `SysTableResolver` 路由到 `PluginDrivenScanNode`）。§10.1 现状仍准确；下方 §10.2/§10.3 仅作历史设计追溯，**勿据其实现**。详见 [decisions-log D-039](./decisions-log.md) / [deviations-log DV-023](./deviations-log.md) / `tasks/P5-paimon-migration.md` §批次 B4。

### 10.1 现状

- `IcebergSysExternalTable.SysTableType` 枚举（`HISTORY`、`SNAPSHOTS`、`FILES`、`MANIFESTS`、`PARTITIONS`、`POSITION_DELETES`、`ALL_DATA_FILES`、`ALL_MANIFESTS`、`ENTRIES`）。
- `PaimonSysExternalTable` 类似。
- 引用方式：`SELECT * FROM iceberg_cat.db.tbl$snapshots`。

### 10.2 设计（**不引入新类型**，复用 `ConnectorTableHandle`）

把 sys-table 看作"特殊命名的普通表"。`ConnectorTableOps.getTableHandle(session, db, "tbl$snapshots")` 由 connector 内部解析 `$snapshots` 后缀，返回带 sys-type 标记的 handle（标记在 connector 内部，对 fe-core 透明）。

新增一个 listing 入口供 `SHOW TABLES` 选择性展示：

```java
public interface ConnectorTableOps {
    // ... existing ...

    /**
     * Lists the connector-specific system table suffixes available for a base table.
     * Returns the set of suffixes (without the leading "$"), e.g., ["snapshots", "history", "files"].
     * Default: empty (no sys tables).
     */
    default List<String> listSysTableSuffixes(ConnectorSession session,
            ConnectorTableHandle baseTableHandle) {
        return Collections.emptyList();
    }
}
```

`getTableSchema(session, sysHandle)` 返回的 schema 中 `tableFormatType = "ICEBERG_SYS"` / `"PAIMON_SYS"`，scan provider 走对应路径。

### 10.3 fe-core 侧

`PluginDrivenExternalDatabase.tableExists("tbl$snapshots")` 路由到 `connector.getMetadata(s).getTableHandle(s, db, "tbl$snapshots")`。

`information_schema.tables` 默认不展开 sys table（避免噪音）；用户显式 `SHOW TABLES LIKE '%$%'` 时才查 `listSysTableSuffixes`。

### 10.4 影响

- Iceberg、Paimon 实现 `listSysTableSuffixes` + `getTableHandle("$xxx")`。
- 其他 connector：默认空。

### 10.5 验收标准

- `SELECT * FROM cat.db.tbl$snapshots` 工作。
- `SHOW TABLES` 默认不返回 `tbl$snapshots`。

---

## 11. 扩展 E8：列级 Statistics 写入 / `ConnectorColumnStatistics`

### 11.1 现状

- `HMSExternalTable.createAnalysisTask(info) → ExternalAnalysisTask`。
- task 跑 `ANALYZE TABLE ... COMPUTE STATISTICS` 后调 `HiveMetadataOps.updateColumnStatistics(...)`。

### 11.2 设计

```java
// connector.api.statistics.ConnectorColumnStatistics
package org.apache.doris.connector.api.statistics;

/**
 * Per-column statistics for a connector table.
 *
 * <p><b>Type safety for {@code minValue} / {@code maxValue} (U6)</b>:
 * Values are stored as {@link Object} but MUST be one of the Java boxed types
 * listed below, matched to the column's {@link ConnectorType}. Connectors
 * reading a value that does not match the expected type MUST throw
 * {@link IllegalArgumentException}; fe-core translates this to a
 * user-visible {@code UserException}.</p>
 *
 * <table>
 *   <caption>Allowed Java types for min/max by ConnectorType family</caption>
 *   <tr><th>ConnectorType family</th><th>Java boxed type</th></tr>
 *   <tr><td>BOOLEAN</td><td>{@link Boolean}</td></tr>
 *   <tr><td>TINYINT / SMALLINT / INT</td><td>{@link Integer}</td></tr>
 *   <tr><td>BIGINT</td><td>{@link Long}</td></tr>
 *   <tr><td>LARGEINT / DECIMAL</td><td>{@link java.math.BigDecimal}</td></tr>
 *   <tr><td>FLOAT</td><td>{@link Float}</td></tr>
 *   <tr><td>DOUBLE</td><td>{@link Double}</td></tr>
 *   <tr><td>DATE</td><td>{@link java.time.LocalDate}</td></tr>
 *   <tr><td>DATETIME / TIMESTAMP</td><td>{@link java.time.Instant}</td></tr>
 *   <tr><td>CHAR / VARCHAR / STRING</td><td>{@link String}</td></tr>
 *   <tr><td>BINARY / VARBINARY</td><td>{@code byte[]}</td></tr>
 *   <tr><td>ARRAY / MAP / STRUCT</td><td>min/max NOT applicable — must be {@code null}</td></tr>
 * </table>
 */
public final class ConnectorColumnStatistics {
    private final long nullCount;            // -1 unknown
    private final long ndv;                  // num distinct values; -1 unknown
    private final Object minValue;           // boxed per type table above; null = no min
    private final Object maxValue;           // boxed per type table above; null = no max
    private final long avgRowSizeBytes;      // -1 unknown
    private final long maxRowSizeBytes;      // -1 unknown
    // builder + getters
}
```

### 11.3 在 `ConnectorStatisticsOps` 新增

```java
public interface ConnectorStatisticsOps {
    // ... existing getTableStatistics ...

    /** Returns per-column statistics, or empty map if unavailable. */
    default Map<String, ConnectorColumnStatistics> getColumnStatistics(
            ConnectorSession session, ConnectorTableHandle handle) {
        return Collections.emptyMap();
    }

    /**
     * Persists per-column statistics back to the external metastore.
     * Called by {@code ANALYZE TABLE} after FE computes statistics.
     */
    default void setColumnStatistics(ConnectorSession session,
            ConnectorTableHandle handle,
            Map<String, ConnectorColumnStatistics> columnStats) {
        throw new DorisConnectorException("setColumnStatistics not supported");
    }
}
```

### 11.4 fe-core 侧

`ExternalAnalysisTask.persist(...)` 检测 catalog 是否为 `PluginDrivenExternalCatalog` —— 是则调 `connector.getMetadata(s).setColumnStatistics(s, handle, statsMap)`。

### 11.5 影响

- 主要 Hive（HMS column stats）。
- Iceberg / Paimon 可选（snapshot summary 已包含部分统计）。

### 11.6 验收标准

- `ANALYZE TABLE hive_cat.db.tbl COMPUTE STATISTICS FOR ALL COLUMNS` 后，HMS 中能查到 column stats。

---

## 12. 扩展 E9：Delete / Merge Sink 配置

### 12.1 现状

- `fe/fe-core/.../planner/IcebergDeleteSink.java`、`IcebergMergeSink.java`、`IcebergTableSink.java` 是 nereids physical sink 的实现。
- 它们 import `IcebergExternalTable`、`IcebergMetadataOps`，跟 fe-core 强耦合。
- Iceberg `DELETE FROM` / `MERGE` 走 `IcebergDeleteCommand` / `IcebergMergeCommand` 命令类。

### 12.2 设计

扩展 `ConnectorWriteType` 枚举：

```java
public enum ConnectorWriteType {
    FILE_WRITE,
    JDBC_WRITE,
    REMOTE_OLAP_WRITE,
    CUSTOM,
    FILE_DELETE,                  // [NEW] Iceberg position-delete or equality-delete files
    FILE_MERGE,                   // [NEW] row-level merge (insert + delete)
}
```

在 `ConnectorWriteOps` 新增：

```java
public interface ConnectorWriteOps {
    // ... existing getWriteConfig ...

    /**
     * Returns the configuration for a DELETE operation. Connector tells BE how to
     * write delete files (position-delete vs equality-delete vs MOR).
     */
    default ConnectorWriteConfig getDeleteConfig(ConnectorSession session,
            ConnectorTableHandle handle, List<ConnectorColumn> filterColumns) {
        throw new DorisConnectorException("Delete not supported");
    }

    /**
     * Returns the configuration for a MERGE (combined insert+delete) operation.
     */
    default ConnectorWriteConfig getMergeConfig(ConnectorSession session,
            ConnectorTableHandle handle,
            List<ConnectorColumn> insertColumns,
            List<ConnectorColumn> deleteFilterColumns) {
        throw new DorisConnectorException("Merge not supported");
    }
}
```

### 12.3 fe-core 侧

P6.3 中：

- 删除 `IcebergDeleteSink` / `IcebergMergeSink` / `IcebergTableSink`，统一改为 `PhysicalConnectorTableSink`（已存在）。
- `PhysicalConnectorTableSink` 根据 `ConnectorWriteType` 构造对应 `TDataSink`：
  - `FILE_WRITE` → `THiveTableSink` / `TIcebergTableSink`（或新统一的 `TConnectorFileSink`）
  - `FILE_DELETE` → `TIcebergDeleteSink`
  - `FILE_MERGE` → `TIcebergMergeSink`
- 这层 thrift 选择仍由 fe-core 做（thrift 类型是 wire 协议）；connector 只需要返回 `ConnectorWriteConfig`。

### 12.4 影响

- Iceberg（DELETE / MERGE / UPDATE）。
- Hive ACID（DELETE / UPDATE）—— P7.3。
- Paimon（MERGE-on-read）—— P5。

### 12.5 验收标准

- Iceberg `DELETE FROM t WHERE id < 100` 在 connector 模块化后输出与旧路径 bit-for-bit 一致的 delete file。
- `MERGE INTO target USING source ON ... WHEN MATCHED THEN DELETE WHEN NOT MATCHED THEN INSERT` 跑通。

---

## 13. 扩展 E10：Partition 列举 / `listPartitions`

### 13.1 现状

- `HMSExternalCatalog.listPartitionNames`、`MaxComputeExternalCatalog.listPartitionNames`、`PaimonExternalCatalog.listPartitions`。
- 调用方：`MetadataGenerator`（TVF 后端）、`PartitionsTableValuedFunction`、`ShowPartitionsCommand`、Nereids 分区裁剪（`HivePartitionPruner`）。

### 13.2 设计（**复用现有** `ConnectorPartitionInfo`）

```java
public interface ConnectorTableOps {
    // ... existing ...

    /**
     * Lists all partition display names (e.g., "year=2024/month=01").
     * Cheap; should avoid loading partition metadata.
     */
    default List<String> listPartitionNames(ConnectorSession session,
            ConnectorTableHandle handle) {
        return Collections.emptyList();
    }

    /**
     * Lists partitions matching the optional filter, with full metadata.
     * Expensive; should use partition pruning when possible.
     */
    default List<ConnectorPartitionInfo> listPartitions(ConnectorSession session,
            ConnectorTableHandle handle,
            Optional<ConnectorExpression> filter) {
        return Collections.emptyList();
    }

    /**
     * Lists distinct partition column value combinations.
     * Used by partition_values() TVF and column-distinct-value optimizations.
     */
    default List<List<String>> listPartitionValues(ConnectorSession session,
            ConnectorTableHandle handle,
            List<String> partitionColumns) {
        return Collections.emptyList();
    }
}
```

### 13.3 增强 `ConnectorPartitionInfo`（向后兼容追加字段）

当前已有：`partitionName`、`partitionValues`、`properties`。

追加只读字段（不破坏构造器签名 —— 用 builder 模式追加）：

```java
public final class ConnectorPartitionInfo {
    // existing fields ...
    private final long rowCount;          // -1 unknown
    private final long sizeBytes;         // -1 unknown
    private final long lastModifiedMillis; // -1 unknown

    // existing 3-arg constructor delegates to the new 6-arg constructor with -1/-1/-1
    public ConnectorPartitionInfo(String partitionName, Map<String, String> partitionValues,
            Map<String, String> properties) {
        this(partitionName, partitionValues, properties, -1, -1, -1);
    }

    public ConnectorPartitionInfo(String partitionName, Map<String, String> partitionValues,
            Map<String, String> properties, long rowCount, long sizeBytes, long lastModifiedMillis) {
        // ...
    }

    public long getRowCount() { return rowCount; }
    public long getSizeBytes() { return sizeBytes; }
    public long getLastModifiedMillis() { return lastModifiedMillis; }
}
```

### 13.4 影响

- Hive、Iceberg、Paimon、MaxCompute、Hudi（任何 partitioned 外部表）。
- 调用方收口：`MetadataGenerator`、`PartitionsTableValuedFunction`、`ShowPartitionsCommand` 三处改走 `PluginDrivenExternalCatalog.getConnector().getMetadata(...).listPartitions(...)`。

### 13.5 验收标准

- `SHOW PARTITIONS FROM cat.db.tbl` 输出 bit-for-bit 等同于旧路径。
- `partition_values('cat.db.tbl', 'col')` TVF 等价。
- 1000-partition Hive 表 `listPartitionNames` 性能不退化 5% 以上。

---

## 14. 实施顺序与里程碑

### 14.1 实施顺序

10 个扩展点不需要全部一次性进 mainline；可分阶段：

| 批次 | 扩展点 | 时机 | 阻塞的 P 阶段 |
|---|---|---|---|
| **批 0**（先行） | E3（MetaInvalidator）、E4（Transaction）、E5（MvccSnapshot）| P0 内必须完成 | 这三个是后续连接器实现 ConnectorMetadata 时的 baseline |
| **批 1** | E1（CreateTableRequest）、E10（listPartitions）| P0 末 / P1 初 | 阻塞 P3 hudi、P5 paimon |
| **批 2** | E6（Credentials）、E7（SysTables）、E9（Delete/Merge）| P5 之前 | 阻塞 P5 paimon、P6 iceberg |
| **批 3** | E2（Procedures）| P6 之前 | 阻塞 P6 iceberg actions |
| **批 4** | E8（Column Statistics）| P7 之前 | 阻塞 P7 Hive ANALYZE |

### 14.2 P0 里程碑（共计约 2 周）

```
W0 ─ Day 1-2  本 RFC 评审、调整签名
W0 ─ Day 3-5  实现批 0（E3/E4/E5）的接口 + javadoc + 默认行为
W1 ─ Day 1-3  实现批 1（E1/E10）的接口 + fe-core converter 草稿
W1 ─ Day 4-5  实现 fe-core 侧 ExternalMetaCacheInvalidator、PluginDrivenTransactionManager 通用版
W1 ─ Day 5    CI grep 守门脚本 tools/check-connector-imports.sh + maven enforcer 接入
```

### 14.3 批 2-4 在各 P 阶段开始时随主任务一起做

每个连接器迁移启动前 1-2 天，把该阶段需要的扩展点接口/默认实现写进 fe-connector-api，然后再开始迁移。

---

## 15. 测试策略

### 15.1 单元测试

- 每个新增类型都有等价 / 哈希 / 序列化（如适用）测试，放 `fe-connector-api/src/test/java/.../<package>/`。
- 默认方法行为测试：定义一个"什么都不实现"的 `BaseConnectorTest` mock connector，调每个 default 方法验证抛错/返回空一致。

### 15.2 fe-core 侧 converter 测试

- `CreateTableInfoToConnectorRequestConverter`：覆盖 Hive identity partition、Iceberg transform partition、List partition、Range partition 四种来源。
- `ExternalMetaCacheInvalidator`：mock `ExternalMetaCacheMgr`，验证每个 invalidate 方法都正确路由到对应 cache 方法。

### 15.3 集成回归

- ES、JDBC 这两个已迁连接器的 regression-test 子集必须全绿（证明现有 SPI 没被破坏）。
- 新增一个 `FakeConnectorPlugin` 在 `fe/fe-core/src/test/`，覆盖所有新增 default 行为路径。

### 15.4 grep 守门

```bash
# tools/check-connector-imports.sh
#!/bin/bash
set -e
FORBIDDEN='org\.apache\.doris\.(catalog|common|datasource|qe|analysis|nereids|planner)'
RESULT=$(grep -rEn "^import ${FORBIDDEN}\." fe/fe-connector/*/src/main/java \
        | grep -v 'org.apache.doris.thrift' \
        | grep -v 'org.apache.doris.connector' \
        | grep -v 'org.apache.doris.extension' \
        | grep -v 'org.apache.doris.filesystem' || true)
if [ -n "$RESULT" ]; then
    echo "FORBIDDEN IMPORTS in fe-connector modules:" >&2
    echo "$RESULT" >&2
    exit 1
fi
```

挂到 maven enforcer plugin 的 `pre-compile` 阶段。

---

## 16. 风险与未决问题

### 16.1 风险

| ID | 风险 | 缓解 |
|---|---|---|
| Q1 | `ConnectorProcedureSpec.arguments` 用 `Object` 装载值类型不安全 | 限定允许的类型枚举：`String/Long/Double/Boolean/Instant/List/Map`；构造时校验 |
| Q2 | `ConnectorMetaInvalidator` 在异常路径被调用时可能 leak（线程未停）| `Connector.close()` 中要明确停止 listener thread |
| Q3 | `ConnectorTransaction.commit` 在跨多个 BE 分片场景下不是简单调用——需要 fe-core 先收集 commit info | 已在 `ConnectorWriteOps.finishInsert(handle, fragments)` 覆盖；`beginTransaction` 只负责开/关，不负责 commit 数据 |
| Q4 | `ConnectorMvccSnapshot.snapshotId` 是 long，但有的系统（Delta Lake 未来引入）用 string | 暂用 long；如未来需要再加 `String getSnapshotIdAsString()` |
| Q5 | E1 的 `ConnectorPartitionField.transform` 字符串编码不规范 | 在 RFC 附录列举允许的 transform 字符串集合（与 Iceberg 对齐：`identity / year / month / day / hour / bucket[N] / truncate[N]`）|
| Q6 | E9 的 thrift sink 选择仍在 fe-core，可能跟不上 connector 新增 sink 类型 | 在 `ConnectorWriteConfig.properties` 留 `"thrift_sink_type"` 自定义字段 + `CUSTOM` 走 generic sink 兜底 |

### 16.2 未决问题（✅ 2026-05-24 全部决议）

| ID | 问题 | 决议 |
|---|---|---|
| U1 | `ConnectorProcedureSpec.listProcedures` 是否在 connector 初始化时一次性返回，还是允许动态变化？ | ✅ **一次性**。Connector 生命周期内稳定；如外部系统的可用 procedure 集合变化，必须重新创建 catalog |
| U2 | `ConnectorMetaInvalidator` 是否要 `invalidateColumnStatistics(...)`？ | ✅ **暂不要**。column stats 失效一并挂在 `invalidateTable` 上，避免接口表面膨胀；后续如发现频繁单独失效再加 |
| U3 | `ConnectorTransaction.getTransactionId` 是连接器分配还是 fe-core 分配？ | ✅ **连接器分配**。连接器自己最清楚事务 ID 与外部系统的对应关系；fe-core 在 `PluginDrivenTransactionManager` 用 `Map<Long, ConnectorTransaction>` 索引即可 |
| U4 | `getCredentialsForScan` 是否要批量化？ | ✅ **是**。签名定为 `Map<ConnectorScanRange, ConnectorCredentials> getCredentialsForScans(session, handle, List<ConnectorScanRange>)`，由连接器自由决定 STS 调用粒度（共享实例 / 按 prefix 分组 / 1:1），fe-core 一个 scan node 一次调用 |
| U5 | sys-table 命名约定（`$snapshots` vs `\$snapshots` vs `[$snapshots]`）跨方言一致性？ | ✅ **统一 `$suffix`**。SPI 层固定该约定；如未来发现冲突（如某 SQL dialect 把 `$` 视为变量前缀），通过 catalog property `sys_table_separator` 提供别名机制，但不在本 RFC 范围 |
| U6 | `ConnectorColumnStatistics.minValue / maxValue` 用 `Object` 装载，类型安全如何保证？ | ✅ **javadoc 类型映射表 + 抛 `IllegalArgumentException`**。在 `ConnectorColumnStatistics` javadoc 中列出 `ConnectorType` ↔ Java 装箱类型映射（见 §11.2）；连接器读到不匹配类型时直接抛 `IllegalArgumentException`，由 fe-core 转成 `UserException` 返回客户端 |

---

## 17. 验收清单（出 P0 时勾选）

```
[ ] fe-connector-api 编译通过，新增类型 / 方法全部就位
[ ] fe-connector-spi 仅新增 ConnectorMetaInvalidator 接口，无其他改动
[ ] fe-core 侧 converter（CreateTableInfoToConnectorRequestConverter、ExternalMetaCacheInvalidator、ConnectorMvccSnapshotAdapter）就位
[ ] PluginDrivenTransactionManager 通用化（不再依赖任何具体连接器）
[ ] JDBC、ES 现有 regression-test 全绿
[ ] FakeConnectorPlugin 覆盖所有新增 default 行为
[ ] tools/check-connector-imports.sh 接入 maven enforcer
[x] 本 RFC 关闭未决问题 U1-U6，签名定稿  ← ✅ 2026-05-24 完成
[ ] plan-doc/00 §3.1 P0 任务全部勾选
```

---

## 18. 附录 A：所有新增 / 修改的文件清单

```
新增（fe-connector-api）：
  org/apache/doris/connector/api/ddl/ConnectorCreateTableRequest.java
  org/apache/doris/connector/api/ddl/ConnectorPartitionSpec.java
  org/apache/doris/connector/api/ddl/ConnectorPartitionField.java
  org/apache/doris/connector/api/ddl/ConnectorPartitionValueDef.java
  org/apache/doris/connector/api/ddl/ConnectorBucketSpec.java
  org/apache/doris/connector/api/procedure/ConnectorProcedureOps.java
  org/apache/doris/connector/api/procedure/ConnectorProcedureSpec.java
  org/apache/doris/connector/api/procedure/ConnectorProcedureArgument.java
  org/apache/doris/connector/api/mvcc/ConnectorMvccSnapshot.java
  org/apache/doris/connector/api/scan/ConnectorCredentials.java
  org/apache/doris/connector/api/statistics/ConnectorColumnStatistics.java

替换（fe-connector-api）：
  org/apache/doris/connector/api/handle/ConnectorTransaction.java
      （原 ConnectorTransactionHandle 保留为父接口；ConnectorTransaction 继承它）

修改（fe-connector-api，仅新增 default 方法）：
  ConnectorMetadata.java                ← extends ConnectorProcedureOps
  ConnectorTableOps.java                ← createTable(request) / listPartitions / listPartitionNames /
                                          listPartitionValues / listSysTableSuffixes
  ConnectorWriteOps.java                ← beginTransaction / getDeleteConfig / getMergeConfig
  ConnectorStatisticsOps.java           ← getColumnStatistics / setColumnStatistics
  ConnectorScanPlanProvider.java        ← getCredentialsForScan
  ConnectorSession.java                 ← getCurrentTransaction
  ConnectorWriteType.java               ← + FILE_DELETE, FILE_MERGE
  ConnectorPartitionInfo.java           ← + rowCount/sizeBytes/lastModifiedMillis (with backward-compat ctor)

新增（fe-connector-spi）：
  org/apache/doris/connector/spi/ConnectorMetaInvalidator.java

修改（fe-connector-spi）：
  ConnectorContext.java                 ← getMetaInvalidator()

新增（fe-core 桥接）：
  org/apache/doris/connector/ddl/CreateTableInfoToConnectorRequestConverter.java
  org/apache/doris/connector/ExternalMetaCacheInvalidator.java
  org/apache/doris/connector/ConnectorMvccSnapshotAdapter.java

修改（fe-core）：
  org/apache/doris/connector/DefaultConnectorContext.java        ← getMetaInvalidator override
  org/apache/doris/connector/ConnectorSessionImpl.java           ← currentTransaction field
  org/apache/doris/transaction/PluginDrivenTransactionManager.java ← 通用化
```

## 19. 附录 B：Allowed Transform 字符串（E1 用）

| 字符串 | 含义 | 来源风格 |
|---|---|---|
| `identity` | 原值分区 | Hive / Iceberg |
| `year` | 取年份 | Iceberg |
| `month` | 取年月 | Iceberg |
| `day` | 取年月日 | Iceberg |
| `hour` | 取年月日时 | Iceberg |
| `bucket` | 哈希分桶；`transformArgs = [N]` | Iceberg |
| `truncate` | 截断；`transformArgs = [W]` | Iceberg |
| `list` | 显式列表分区，初始值在 `initialValues` | Doris |
| `range` | 显式范围分区，初始值在 `initialValues` | Doris |

未列出的字符串视为 `CUSTOM`，由 connector 内部识别。

---

## 20. 扩展 E11：写/事务 SPI（写-plan-provider + ConnectorTransaction 写回调）

> 后补节（2026-06-06），置于附录后以避免重排既有节号。完整设计见 [写/事务 SPI RFC](./tasks/designs/connector-write-spi-rfc.md)（§5 API / §8 fe-core 改动 / §12 W1→W7）。决策见 [D-022](./decisions-log.md)（A/B1/C1/D/E）；W5 收口位置修正见 [DV-009](./deviations-log.md)。

把 fe-core 通用写编排（`Coordinator`/`LoadProcessor`/`FrontendServiceImpl`/`TransactionManager`）完全多态化，消除全部 `instanceof *Transaction` / concrete cast；定义连接器写/事务 SPI（maxcompute P4 / iceberg P6 / hive P7 实现，paimon P5 零 SPI 改动接入）。**保 BE 契约不变**。

**SPI 面（default-only，[D-009]）**：
- `ConnectorTransaction`（既有，+4 default）：`addCommitData(byte[])`（B1）、`supportsWriteBlockAllocation()` / `allocateWriteBlockRange(sid, count)`（C1）、`getUpdateCnt()`。fe-core `Transaction` 加同名 default；`PluginDrivenTransaction`（`PluginDrivenTransactionManager` 产）桥接委派（A）。
- `ConnectorSession.allocateTransactionId()`（P4-T03 新增 default 抛；fe-core `ConnectorSessionImpl` override 回 `Env.getNextId`）：为**无外部 id 的连接器**（如 maxcompute）提供引擎全局 txn id 分配器，连接器经它在 `beginTransaction` 分配，id 即 Doris `txn_id`（与 sink / `GlobalExternalTransactionInfoMgr` 一致）。细化 [D-015]/U3「连接器分配」，见 [D-024]。
- **P4-T06 翻闸新增（2，default-preserving，零 jdbc/es/trino 影响；[D-026] 预授、登记 2026-06-07）**：`ConnectorSession.setCurrentTransaction(ConnectorTransaction)`（default 抛；fe-core `ConnectorSessionImpl` 加 volatile 字段 + override `getCurrentTransaction`）——把 connectorTx 绑入 **sink 的** session 供 T04 `planWrite` 读 `getCurrentTransaction()`（解 dormant→live 的 G1）；`ConnectorWriteOps.usesConnectorTransaction()`（default false；`MaxComputeConnectorMetadata` override true）——executor 据此在调任何 throwing-default 写法前分流 txn-model（MC）vs JDBC insert-handle（[D-026] D-1）。
- `ConnectorWritePlanProvider.planWrite(session, handle) → ConnectorSinkPlan(TDataSink)`（E，仿 `ConnectorScanPlanProvider`）；`Connector.getWritePlanProvider()` default null。`ConnectorWriteHandle` = {tableHandle, columns, overwrite, writeContext}；`ConnectorSinkPlan` 包 opaque `TDataSink`。
- DML 覆盖 INSERT / DELETE / MERGE（D）；procedures defer（E2 / P6）。

**三处 seam**：B1 commit 载荷 opaque bytes（`TBinaryProtocol` 序列化，单点 `CommitDataSerializer`，连接器反序列化）；C1 maxcompute block-id 窄 callback；E 写-plan-provider 产 opaque `TDataSink`。

**W-phase 落地**（behind gate、零行为变更、golden 等价）：W1+W2（SPI 面 + `Transaction` 泛化）`be945476ba7`；W3+W6（解耦 3 热路径 + golden 测）`9ad2bbe40ec`；W4（`PluginDrivenTransaction` 委派）`759cc0874c8`；W5（`planWrite` layer 进 `visitPhysicalConnectorTableSink`，见 [DV-009]）`9ebe5e27fa4`；W7（本节 + [D-021]/[D-022]）。逐连接器 adopter（搬类 + impl 写 SPI + 翻闸）= P4 / P6 / P7。

---

## 21. 扩展 E13：存储 URI 归一化（`ConnectorContext.normalizeStorageUri`）

> 后补节（2026-06-11，P5-fix-FIX-URI-NORMALIZE）。findings B-7DF（native 数据文件）+ B-7DV（deletion vector）—— 见 [task-list #1](./task-list-P5-rereview2-fixes.md) / [设计](./tasks/designs/P5-fix-URI-NORMALIZE-design.md)。

**问题**：paimon 连接器把 native ORC/Parquet **数据文件路径**和 **deletion-vector 路径**裸传 BE，未做 scheme 归一化。paimon SDK 发的是 warehouse 原生 scheme（`oss://`/`cos://`/`obs://`/`s3a://`，或 OSS `bucket.endpoint` authority 形）；BE 文件工厂按 scheme 分派、S3 reader 只认 `s3://`。后果：S3-兼容（非 AWS）warehouse 上 native 数据文件读直接挂（B-7DF），或 DV 静默丢→被删行重现（B-7DV，merge-on-read 错行，更危险）。纯 `s3://`/`hdfs://` 不受影响；JNI 路不受影响（序列化 paimon `Table` 自带 `FileIO`）。

**根因**：legacy `PaimonScanNode` 两路径都经 **2-arg 归一化** `LocationPath.of(path, storagePropertiesMap)` → `StorageProperties.validateAndNormalizeUri()`（`PaimonScanNode.java:443` 数据文件 / `:296-297` DV）；翻闸丢了。连接器禁 import fe-core `LocationPath`/`StorageProperties`，故须经 SPI 缝。两路径机制不同：数据文件经 `PluginDrivenSplit.buildPath` 的**单-arg 非归一化** `LocationPath.of(pathStr)` → `FileQueryScanNode:568` 写 thrift；DV 由连接器在 `PaimonScanRange.populateRangeParams` **直接烤进 thrift**，fe-core 永不经手 → bridge-only 修不到 DV。故唯一统一缝 = 连接器侧 SPI 调用。

**SPI 面（default no-op，零它连接器影响）**：
- `ConnectorContext.normalizeStorageUri(String rawUri) → String`（`fe-connector-spi`）：default 返回原值（恒等），故 es/jdbc/maxcompute/trino 及任何已规范 URI 不受影响。
- fe-core `DefaultConnectorContext` override：`LocationPath.of(rawUri, storagePropertiesSupplier.get()).toStorageLocation().toString()`——复用引擎/legacy/iceberg 同一 `LocationPath` 归一化，单一真相源、无漂移。**fail-loud**（`StoragePropertiesException` 传播）：路径归一化不了宁可显式炸，不可静默送裸路（DV 错行）。null/blank 短路返回原值。
- `DefaultConnectorContext` 加 `Supplier<Map<StorageProperties.Type, StorageProperties>> storagePropertiesSupplier`（4-arg ctor；既有 2/3-arg ctor 委派空 map supplier——它们不被 paimon 用、该法仅 paimon 调）。`PluginDrivenExternalCatalog:150` 接线 `() -> catalogProperty.getStoragePropertiesMap()`（lazy，scan 时调，catalog 已初始化）。

**连接器侧**：`PaimonScanPlanProvider.buildNativeRange`（B7 抽出的可测 seam）对**数据文件路径 + DV 路径**各调 `normalizeUri()`（= `context != null ? context.normalizeStorageUri(raw) : raw`，null-guard 同 `vendStorageCredentials`，offline 单测保留裸路）。JNI 路 + `getScanNodeProperties` 不动。

**作用域/偏差**：归一化用 catalog **静态** `getStoragePropertiesMap()`，非 legacy 的 vended-overlay 版（`VendedCredentialsFactory`）——scheme 归一化与 vended 凭据正交（vended 改 `AWS_*` 键非 scheme），仅 *纯-vended-无静态存储配* REST catalog 的边角会缺 entry→fail-loud；该边角归凭据缝（#2 `FIX-STATIC-CREDS-BE` / `FIX-REST-VENDED`），见 [DV-025](./deviations-log.md)。

**测**：fe-core `DefaultConnectorContextNormalizeUriTest`（真 OSS map，oss://→s3://、s3:// 恒等、null/blank、空 map fail-loud）；连接器 `PaimonScanPlanProviderTest` 3 测（`buildNativeRange` 数据文件+DV 双归一化、无-DV 仅数据、无-context 裸路）。live-e2e（OSS warehouse + DV）CI-gated。
