# Connector 迁移总体计划（fe-core/datasource → fe-connector/*）

> 状态：草案 v1 · 撰写日期 2026-05-24 · 分支 `catalog-spi-2`
> 范围：把 `fe/fe-core/src/main/java/org/apache/doris/datasource/` 下所有"具体数据源"代码（hive/iceberg/paimon/hudi/trino/maxcompute/lakesoul/jdbc/es）解耦到 `fe/fe-connector/*` 下的插件模块；只把"通用基础设施"和"SPI 桥接层"留在 fe-core。
> 不在范围：BE 端 reader 实现、`fe-fs-spi` 文件系统插件化（已是独立工作流）、`extension-spi`。
>
> ---
>
> 📍 **当前推进状态、活跃 task、风险等动态信息见 [`PROGRESS.md`](./PROGRESS.md)**（本文件只放战略，不放进度）。
> 📚 **跟踪机制说明 / 文档索引**：[`README.md`](./README.md)
> 🤖 **Agent 协作规范**（context 管理 / subagent / handoff）：[`AGENT-PLAYBOOK.md`](./AGENT-PLAYBOOK.md)
> 📋 **决策日志（D-NNN）**：[`decisions-log.md`](./decisions-log.md) · **偏差日志（DV-NNN）**：[`deviations-log.md`](./deviations-log.md) · **风险登记（R-NNN）**：[`risks.md`](./risks.md)
> 📁 **阶段任务**：[`tasks/`](./tasks/) · **连接器跟踪**：[`connectors/`](./connectors/)

---

## 0. 阅后即明的现状（Recap）

| 维度 | 状态 |
|---|---|
| SPI/API 模块 | ✅ `fe-connector-api` + `fe-connector-spi` 已建立，依赖只含 `fe-thrift (provided)`、`fe-extension-spi` |
| 反向边界 | ✅ 干净。`fe-connector/**` 下 0 处对 `org.apache.doris.{catalog,common,datasource,qe,analysis,nereids,planner}` 的 import |
| 桥接层 | ✅ `PluginDrivenExternalCatalog / Database / Table / ScanNode / Split`、`ExprToConnectorExpressionConverter`、`ConnectorColumnConverter`、`DorisTypeVisitor`、`ConnectorPluginManager`、`ConnectorFactory`、`DefaultConnectorContext` 已就绪 |
| 已切到 SPI 路径 | ✅ `jdbc`、`es`（见 `CatalogFactory.SPI_READY_TYPES`） |
| 未切到 SPI 路径 | ⏳ `hms`、`iceberg`、`paimon`、`trino-connector`、`max_compute`、`hudi`（仍走 `switch-case`） |
| 旧/新重复代码 | ⚠️ `Jdbc*Client` 13 个方言（fe-core 旧 + fe-connector 新）、`PaimonPredicateConverter`、`McStructureHelper` |
| 反向耦合（要清理）| 96 处 `instanceof XExternal*` 散落在 34 个文件；其中 14 个在 `nereids/`、`planner/`、`alter/`、`tablefunction/` 等热区 |
| 测试 | jdbc=13 个、es=7 个；其余 6 个连接器模块 0 个 |

---

## 1. 总目标与终态

### 1.1 终态定义

**fe-core/datasource/ 留下什么**：

- `CatalogIf` / `CatalogMgr` / `CatalogFactory` / `CatalogProperty` / `CatalogLog` —— catalog 注册/调度
- `ExternalCatalog` / `ExternalDatabase` / `ExternalTable` / `ExternalView` —— 抽象基类
- `InternalCatalog`、`ExternalMetaCacheMgr`、`ExternalMetaIdMgr`、`ExternalRowCountCache`、`ExternalFunctionRules` —— 跨连接器共享设施
- `FederationBackendPolicy`、`FileSplit*`、`SplitGenerator`、`SplitAssignment`、`SplitSourceManager`、`NodeSelectionStrategy`、`FileCacheAdmissionManager` —— 通用 split/分发
- `FileScanNode` / `FileQueryScanNode` / `ExternalScanNode` —— 通用 scan 基类
- `PluginDrivenExternalCatalog/Database/Table/ScanNode/Split` —— SPI 桥
- `ExprToConnectorExpressionConverter`、`ConnectorColumnConverter`、`DorisTypeVisitor` —— Doris ↔ SPI 类型/表达式转换
- `metacache/`、`mvcc/`、`statistics/`、`property/`、`credentials/`、`connectivity/`、`operations/`、`systable/`、`infoschema/`、`test/`、`tvf/` —— 通用框架（**保留**；其中 `property/` 的连接器专属常量需要逐步搬走）
- `kafka/`、`kinesis/`、`odbc/`、`doris/`（Doris-to-Doris federation）—— 暂时保留，不在本计划主线（决策点 D7）

**fe-core/datasource/ 删除什么**：

- `hive/`、`iceberg/`、`paimon/`、`hudi/`、`maxcompute/`、`trinoconnector/`、`jdbc/`、`lakesoul/` 整个子目录
- `fe/fe-core/src/main/java/org/apache/doris/transaction/{Hive,Iceberg}TransactionManager.java`、`TransactionManagerFactory.java` 中的连接器分支
- `fe/fe-core/src/main/java/org/apache/doris/planner/Iceberg{DeleteSink,MergeSink,TableSink}.java` 等连接器专属 sink/scan-node
- `fe/fe-core/src/main/java/org/apache/doris/nereids/glue/translator/PhysicalPlanTranslator.java` 中 line 734–790 的 7 个 `instanceof` 分支 → 收口到 `PluginDrivenScanNode.create(...)`
- 散落在 `nereids/`、`planner/`、`alter/`、`tablefunction/`、`catalog/RefreshManager` 的 `instanceof XExternal*` —— 全部走 SPI 接口
- `SPI_READY_TYPES` 白名单本身

**fe-connector/ 终态**：每个连接器是一个**独立可装卸的 plugin zip**，部署到 `${doris_home}/plugins/connector/<name>/`（**单数**，`build.sh:1073`；勿与 trino-connector 自带插件的投放点 `plugins/trino_plugins/` 混淆），FE 启动通过 `connector_plugin_root` 加载。运行时 `fe-core` 对具体连接器名一无所知；用户安装/卸载连接器无需重启 FE（决策点 D8）。

### 1.2 三个不可妥协的不变量

1. **fe-connector → fe-core 单向依赖**：禁止任何 `import org.apache.doris.{catalog,common,datasource,qe,analysis,nereids,planner}`。允许 `org.apache.doris.thrift.*`（provided）和 `org.apache.doris.connector.*` / `org.apache.doris.extension.*` / `org.apache.doris.filesystem.*`。CI 必须有 grep 守门。
2. **Image / 元数据持久化向后兼容**：旧 FE image 中保存的 `IcebergExternalCatalog`、`PaimonExternalDatabase` 等 GSON 类型必须能被新 FE 反序列化并平滑迁移到 `PluginDrivenExternalCatalog`。范本是 `PluginDrivenExternalCatalog.gsonPostProcess()` 中对 ES/JDBC 的处理（已有），需推广到所有类型。
3. **用户可见行为不回归**：`SHOW CREATE CATALOG`、`SHOW TABLE STATUS`、`information_schema.tables`、`EXPLAIN` 输出、错误信息、catalog `type` 字段、`engine` 字段（`getEngine` / `getEngineTableTypeName`）需保留旧名字。已经在 `PluginDrivenExternalTable.getEngine()` 用 switch 兜底，迁移过程中维护这个 switch。

---

## 2. 现状审视：先解决的 SPI 设计缺口

迁移之前必须先把 SPI 补齐到能承载所有六个连接器，否则边迁边补会被反复打回。

### 2.1 必须新增的 SPI 能力

> 全部加在 `fe-connector-api` 下；保持 `default` 方法策略以让现有连接器零迁移成本。

| 能力 | 当前在哪 | 计划新增的 SPI |
|---|---|---|
| **DDL info** —— `CreateTableInfo`/`PartitionDesc`/`DistributionDesc` 等都是 nereids 类型，连接器看不到 | `IcebergMetadataOps.createTable(CreateTableInfo)`、`HiveMetadataOps.createTable(CreateTableInfo)` | 在 `ConnectorTableOps` 增加 `createTable(session, ConnectorCreateTableRequest)`，引入 `ConnectorCreateTableRequest`、`ConnectorPartitionSpec`、`ConnectorBucketSpec` 三个 POJO；fe-core 侧加 `CreateTableInfoToConnectorRequestConverter` |
| **Procedures / Actions** —— Iceberg 10 个 `IcebergXxxAction` 通过 `BaseIcebergAction` 调用 `IcebergMetadataOps.commit*` | `datasource/iceberg/action/*` | 新增 `ConnectorProcedureOps`（`listProcedures`、`callProcedure(name, args)`），fe-core 侧 `ExecuteActionCommand` 走通用 dispatch |
| **元数据失效事件**（HMS notification）—— 21 个 `MetastoreEvent` 类 | `datasource/hive/event/MetastoreEventsProcessor` 调用 fe-core 的 `ExternalMetaCacheMgr.invalidate*` | 选项 A：把 event 处理整体搬到 `fe-connector-hms`，通过 `ConnectorContext.getMetaInvalidator()`（新增）回调 fe-core。选项 B：只把"轮询 HMS 拿事件流"和"解析事件"放连接器，"分发失效"留 fe-core。**推荐 A**（决策点 D4） |
| **事务管理器** | `transaction/HiveTransactionManager`、`IcebergTransactionManager` | 新增 `ConnectorTransactionFactory`（已存在的 `PluginDrivenTransactionManager` 当骨架），把 `HiveTransactionMgr` 内部状态搬进连接器实现 |
| **MVCC 快照** | `IcebergMvccSnapshot`、`PaimonMvccSnapshot` | 新增 `ConnectorMvccSnapshot` 类型，`ConnectorMetadata.beginQuery(session) -> ConnectorMvccSnapshot`；fe-core 侧 `MvccSnapshot` 接口由连接器提供实现 |
| **Vended credentials** | `IcebergVendedCredentialsProvider`、`PaimonVendedCredentialsProvider` | `ConnectorCapability.SUPPORTS_VENDED_CREDENTIALS` 已存在；新增 `ConnectorCredentials getCredentialsForScan(session, ConnectorScanRange)` |
| **Sys-tables / metadata-tables** | `IcebergSysExternalTable`、`PaimonSysExternalTable` | 在 `ConnectorTableOps` 增加 `listSysTableTypes()` / `getSysTableSchema(...)` |
| **Statistics 写入**（`ANALYZE TABLE`）| `HMSExternalTable.createAnalysisTask` | 把 `ExternalAnalysisTask` 改为只调 `ConnectorStatisticsOps`；新增 `setColumnStatistics` 方法 |
| **写路径 sink 配置**（不是数据本身，BE 写）| `IcebergDeleteSink`、`IcebergMergeSink`、`IcebergTableSink` | `ConnectorWriteOps.getWriteConfig` 已存在；扩展为支持 `getDeleteConfig`、`getMergeConfig`；planner 用通用 `PhysicalConnectorTableSink` |
| **Partition 列举**（给 TVF / SHOW PARTITIONS 用）| `MaxComputeExternalCatalog`、`PaimonExternalCatalog`、`HMSExternalCatalog` 各自的 `listPartition*` | 新增 `ConnectorTableOps.listPartitions(session, handle, filter)` / `listPartitionValues(session, handle, columns)` |

### 2.2 推荐放弃 / 推迟的 SPI 演进

- **不要**为 ScanRange 引入更多多态——`PluginDrivenScanNode` extends `FileQueryScanNode` 的桥接策略已经验证可行（ES/JDBC）。
- **不要**抽象 `Resource` 兼容层——旧 resource-backed catalog 用 `gsonPostProcess` 回填 `type` 已足够。

### 2.3 SPI 改动的版本号管理

`ConnectorProvider.apiVersion()` 当前固定返回 1。每次 SPI **新增** default 方法不动版本号；每次 SPI **改签名 / 删方法**版本号 +1，`ConnectorPluginManager` 中 `CURRENT_API_VERSION` 同步 +1。本计划过程中只新增 default 方法，因此版本号保持 1。

---

## 3. 阶段划分（按风险与价值排序）

```
┌─────────────────────────────────────────────────────────────────────┐
│  P0: SPI 缺口补齐（不迁连接器）          ~2 周                       │
│  P1: 重复代码清理 + scan-node 收口      ~1 周                       │
│  P2: trino-connector 迁移               ~2 周    最小风险，先打通流程 │
│  P3: hudi 迁移（含 DLA 重构）           ~2 周                       │
│  P4: maxcompute 迁移                    ~2 周                       │
│  P5: paimon 迁移                        ~3 周                       │
│  P6: iceberg 迁移（含 7 catalog 变体）  ~5 周                       │
│  P7: hive (+HMS) 迁移（含 event 引擎）  ~6 周    最复杂，最后做      │
│  P8: 收尾——删 SPI_READY_TYPES、删旧类、删 instanceof  ~2 周        │
└─────────────────────────────────────────────────────────────────────┘
```

总长度估算 **~25 周**（不含 lakesoul / RemoteDoris 等遗留类型清理）。

### 3.1 阶段 P0：SPI 缺口补齐

**目标**：让 §2.1 表里所有缺口都有对应 SPI 类型/方法在 `fe-connector-api`，且至少一个连接器的现有实现已经能在 `default` 模式下正常工作。

**任务**：

1. 新增 SPI 类型：`ConnectorCreateTableRequest`、`ConnectorPartitionSpec`、`ConnectorBucketSpec`、`ConnectorProcedureOps`、`ConnectorMvccSnapshot`、`ConnectorCredentials`、`ConnectorMetaInvalidator`（在 SPI 包）。
2. 在 `ConnectorTableOps`、`ConnectorMetadata`、`ConnectorContext` 上新增对应 default 方法。
3. 在 `fe-core` 侧加 converter：`CreateTableInfoToConnectorRequestConverter`、`ConnectorRequestToCreateTableInfoConverter`（如果需要双向）。
4. 给 `PluginDrivenExternalCatalog` / `PluginDrivenExternalTable` 加上分发：CREATE TABLE / EXECUTE PROCEDURE / ANALYZE / SHOW PARTITIONS 都路由到 SPI。
5. 更新 `ConnectorPluginManager` 文档：列出"新 SPI 在 v1 中以 default 方法形式添加，连接器无需更新版本号"。
6. CI 守门：grep 脚本 `tools/check-connector-imports.sh` 在 `fe-connector/**/*.java` 中扫描禁用 import 列表，作为 maven 的 `enforcer` 步骤。

**完成判据**：
- `mvn -pl fe-connector verify` 全绿。
- JDBC、ES 仍正常（回归）。
- 一条 fake 连接器（在测试目录下）能在不实现新 SPI 的情况下编译并工作。

### 3.2 阶段 P1：重复清理 + scan-node 收口

**目标**：在迁连接器之前先把已经造成混乱的旧代码清掉，并把 `PhysicalPlanTranslator` 的 scan-node 分支从 7 个减到 1 个。

**任务**：

1. 删除 fe-core 旧的 `datasource/jdbc/client/Jdbc*Client.java` 13 个文件 + `JdbcFieldSchema.java`。删除前 grep `org.apache.doris.datasource.jdbc.client` 在 fe-core 内被谁引用——预期只有 `JdbcExternalCatalog` 等已经走 SPI 的代码会引用，需要它们也搬走或改路径。
2. 删除 fe-core 重复的 `PaimonPredicateConverter`、`McStructureHelper`，让 fe-core 通过 SPI 桥接（如果 fe-core 真的需要这两个工具，应该挪到通用工具包；但更可能它们是 leak，可直接删）。
3. **收口 `PhysicalPlanTranslator.visitPhysicalFileScan`**：把所有 `instanceof HMSExternalTable / IcebergExternalTable / TrinoConnectorExternalTable / MaxComputeExternalTable / LakeSoulExternalTable` 分支统一改为：
   - 若 `table instanceof PluginDrivenExternalTable` → 走 `PluginDrivenScanNode.create(...)`
   - 兜底（迁移期）保留老分支
   - 在每个连接器迁移完成时（P3–P7）删掉对应分支。
4. 把 `visitPhysicalHudiScan` 改为内部委托 `PluginDrivenScanNode` 处理增量场景（这里是 `getScanNodeProperties()` 的扩展）。
5. 把 `LogicalFileScan.computeOutput` 中的 `instanceof IcebergExternalTable` / `HMSExternalTable` 改成通过 `ConnectorMetadata.getTableSchema` 拿额外列（metadata column）。

**完成判据**：
- `PhysicalPlanTranslator` 不再 `import` 任何具体 `*ExternalTable` 类（除迁移期 fallback）。
- 全量回归 P0 通过。

### 3.3 阶段 P2：trino-connector 迁移（先开荒）

**为什么先做它**：

- fe-core 侧只有 6 个文件 + `source/`，且只有 2 处反向 `instanceof` 引用。
- fe-connector-trino 已经有 13 个文件，scan/predicate/plugin loader/services provider 都已搬好。
- 没有 transaction、没有 event、没有 ACID。
- 失败的爆炸半径最小，可以把整个 migration playbook 跑一遍。

**任务清单**（**这套清单就是后续每个连接器都要走的 playbook**）：

1. **代码层面**：
   - 把 `datasource/trinoconnector/TrinoConnectorExternalCatalog/Database/Table` 中尚未在 connector 模块中的逻辑搬过去（schema cache、plugin loader 关闭、property 校验）。
   - 在 `TrinoConnectorMetadata` 实现 `getTableSchema` / `listTableNames` / `getTableHandle` / `applyFilter` / `applyProjection`（多数已在）。
   - `TrinoScanPlanProvider` 已实现 `planScan` —— review 一遍 split 数量、Thrift desc 字段。
2. **桥接层面**：
   - `CatalogFactory.SPI_READY_TYPES` 加入 `trino-connector`。
   - `PhysicalPlanTranslator` 删除 `instanceof TrinoConnectorExternalTable` 分支。
   - `PluginDrivenExternalTable.getEngine() / getEngineTableTypeName()` 加 `trino-connector` 分支。
   - 检查 `TableIf.TableType.TRINO_CONNECTOR_EXTERNAL_TABLE` 是否仍需保留作为 GSON 兼容（**保留**，并在 `gsonPostProcess` 中迁移到 `PLUGIN_EXTERNAL_TABLE`）。
3. **持久化兼容**：
   - 在 `PluginDrivenExternalCatalog.gsonPostProcess` 中加 `trinoconnector → plugin` 的 logType 迁移（已有 ES/JDBC 范本）。
   - 在 `ExternalCatalog.registerCompatibleSubtype` 注册 `TrinoConnectorExternalCatalog` → `PluginDrivenExternalCatalog`。
4. **测试**：
   - 给 `fe-connector-trino` 加单元测试（mock Trino plugin），覆盖 schema 解析、predicate 转换、scan plan。
   - regression-test 里新增 `trino_connector_migration_compat` 测试：模拟旧 FE image 反序列化。
5. **打包**：
   - 验证 `mvn package -pl fe-connector-trino` 生成的 `plugin.zip` 内容、`lib/` 排除项是否完整。
   - 文档：在 `docs-next/` 添加 trino-connector 插件安装步骤。
6. **删除 fe-core 旧代码**（迁移完成的最后一步）：
   - `rm -rf fe/fe-core/src/main/java/org/apache/doris/datasource/trinoconnector/`
   - `CatalogFactory.java` 移除 `case "trino-connector": ...`
   - 删除 `import` 失败处全部走 SPI 改造。

**风险点**：Trino 插件加载（`TrinoPluginManager` 在连接器内部）要确认 classloader 隔离不会破坏 fe-core 现有 Trino 用法。如果有 BE 端共用 Trino 二进制的情况，需要复核。

### 3.4 阶段 P3：hudi 迁移

**特殊性**：hudi 没有自己的 `*ExternalCatalog`，它寄生在 HMS 上——表是 `HMSExternalTable.dlaType=HUDI`。

**任务**：

1. **重构 DLA 模型**：在 SPI 层显式建模"一个 HMS 表实际是 hudi 表"。两个选项：
   - **选项 A**（推荐）：在 `ConnectorTableSchema.tableFormatType` 上做约定值 `HUDI` / `ICEBERG` / `HIVE`，由 HMS 连接器探测后填充；Doris 侧 `PluginDrivenExternalTable` 根据这个值决定走 `PhysicalHudiScan` 还是 `PhysicalFileScan`。
   - **选项 B**：hudi 作为独立 catalog type，但 catalog 内部委托 HMS 连接器拿元数据。
   - 决策点 D5。
2. **迁移代码**：
   - `datasource/hudi/HudiUtils.java`、`HudiSchemaCacheKey/Value`、`HudiMvccSnapshot`、`HudiPartitionProcessor` 搬入 `fe-connector-hudi`。
   - `datasource/hudi/source/` 下的 `HudiScanNode` 删除，改为 `PluginDrivenScanNode` + `HudiScanPlanProvider`（已存在）补全 incremental relation 逻辑。
   - 4 个 `HoodieIncremental*Relation` 类是和 hudi-spark 库交互，必须在连接器模块里（已在 lib），review classpath。
3. **桥接**：`SPI_READY_TYPES` 加 hudi。但因为 hudi 不能独立 CREATE CATALOG（它依附 HMS），CatalogFactory 路由可能要特别处理：用户写 `type=hms`，由 HMS 连接器自行判断 dlaType 后用 hudi-specific 行为。
4. **测试**：用 hudi 测试集群跑读时序，确保 incremental query 不回归。

### 3.5 阶段 P4：maxcompute 迁移

**任务**：

1. 搬 `MCTransaction`、`MaxComputeExternalMetaCache`、`MaxComputeSchemaCacheValue` 到 `fe-connector-maxcompute`。
2. 删 fe-core 重复的 `McStructureHelper`（P1 已删，确认）。
3. `MaxComputeMetadataOps` 现有 fe-core 实现搬到连接器（连接器内已有 `MaxComputeConnectorMetadata` 骨架）。
4. 收口 `PhysicalPlanTranslator`、`ShowPartitionsCommand`、`PartitionsTableValuedFunction` 中对 `MaxComputeExternalCatalog/Table` 的 12 处 instanceof。
5. `SPI_READY_TYPES` 加 `max_compute`。
6. 删 `datasource/maxcompute/`。

### 3.6 阶段 P5：paimon 迁移

**复杂度跃升原因**：

- 6 个 catalog flavor（HMS/DLF/REST/File/Base/Factory）—— 在连接器内用工厂模式重组：`PaimonConnectorProvider.create()` 根据 properties 实例化 `Catalog`。
- `PaimonMvccSnapshot` —— 用 P0 新增的 `ConnectorMvccSnapshot` 类型承接。
- `PaimonVendedCredentialsProvider` —— 用 P0 新增的 vended credentials SPI 承接。
- `PaimonSysExternalTable` —— 用 P0 新增的 sys table SPI 承接。
- BE 通过 JNI 调用 paimon-reader，序列化 Paimon Table 通过 `ConnectorScanPlanProvider.getSerializedTable` 已有支持。

**任务**：

1. 完整 port `PaimonMetadataOps` → `PaimonConnectorMetadata`（注意 partitionStatistics、bucketing）。
2. Port 6 个 catalog flavor。
3. 实现 MVCC、Vended、Sys Tables 三套 SPI。
4. 删 fe-core `PaimonPredicateConverter` 重复（P1 已删，确认）。
5. 删 fe-core `datasource/paimon/`。
6. 清 10 处反向 `instanceof PaimonExternalCatalog/Table`。

### 3.7 阶段 P6：iceberg 迁移（最大）

**为什么排第二难**：

- 7 个 catalog flavor（HMS/Glue/Hadoop/Jdbc/REST/S3Tables/DLF）—— 但 Iceberg SDK 本身就抽象了 Catalog，连接器只要 dispatch property → 选实例化哪个 SDK Catalog。
- 10 个 `IcebergXxxAction`（`RewriteDataFiles`、`ExpireSnapshots`、`RollbackToSnapshot` 等）—— 用 P0 新增的 `ConnectorProcedureOps` 承接。
- `IcebergTransaction`（966 行）+ `IcebergMetadataOps`（1247 行）+ `IcebergUtils`（1718 行）+ `IcebergScanNode`（1228 行）= **5 千多行重戏**。
- `IcebergMvccSnapshot` + snapshot cache + manifest cache —— 用 `ConnectorMvccSnapshot` 承接，cache 由连接器内部管理（决策点 D6）。
- `IcebergSysExternalTable` + 元数据列（`IcebergMetadataColumn`、`IcebergRowId`）—— 用 sys table SPI。
- `dlf/`、`broker/`、`fileio/`、`helper/`、`profile/`、`rewrite/` 各子目录都要看清是引擎相关还是用户逻辑。
- nereids 写命令 `IcebergDeleteCommand` / `IcebergMergeCommand` / `IcebergUpdateCommand` 大量依赖 `IcebergExternalTable` —— 这些要改为通过 `ConnectorWriteOps.beginMerge`、`beginDelete`、`getDeleteConfig` 等 SPI 调用，且 planner 改用 `PhysicalConnectorTableSink`（已存在）。
- `planner/IcebergDeleteSink.java`、`IcebergMergeSink.java`、`IcebergTableSink.java` 要删除并由通用 sink 承接。

**任务分子阶段**：

- P6.1 元数据 only（catalog flavors + ConnectorMetadata）—— 2 周
- P6.2 scan path（ScanPlanProvider + MVCC + cache）—— 1 周
- P6.3 write path（commit/transaction + DML SPI + planner 改造）—— 1 周
- P6.4 actions（procedure SPI 接上 10 个 action）—— 0.5 周
- P6.5 sys tables + metadata columns —— 0.5 周
- P6.6 删除 fe-core `datasource/iceberg/` + 删 13 处反向 instanceof —— 0.5 周

**风险**：Iceberg 写路径与 nereids 优化器深度耦合（如 `IcebergConflictDetectionFilterUtils`）。建议在 P6.3 前先单独写一个**写路径方案 RFC**，请 PMC 评审。

### 3.8 阶段 P7：hive (+HMS) 迁移（最复杂）

**复杂度顶点的原因**：

- HMS 是 hive、hudi、iceberg-hms-flavor、paimon-hms-flavor **共同的元数据后端**。HMS 连接器必须在 P7 之前就稳定可用（事实上 P3/P5/P6 已经在用 `fe-connector-hms`）。
- 21 个 metastore event 类 + `MetastoreEventsProcessor` —— 用 P0 新增的 `ConnectorMetaInvalidator` 承接（决策点 D4）。
- `HMSTransaction`（1866 行）+ `HiveTransactionMgr` —— ACID 事务管理，**最难**，需要重写写路径。
- `HMSExternalTable`（1293 行）—— 处理 hive / hudi / iceberg 三种 dlaType 的分流逻辑。这部分要被 P3、P6.1 的 DLA 模型重构吸收。
- 31 处反向 `instanceof HMSExternalCatalog / HMSExternalTable`，分布在 `nereids/glue/translator`、`tablefunction/MetadataGenerator`、`AnalyzeTableCommand`、`ShowPartitionsCommand` 等热路径。

**任务分子阶段**：

- P7.1 把 `HiveMetadataOps` 全功能搬到 `HiveConnectorMetadata`（基础 DDL、partition、statistics）—— 2 周
- P7.2 event pipeline 整体搬到 `fe-connector-hms`，提供 `ConnectorMetaInvalidator` 回调 —— 1.5 周
- P7.3 HMSTransaction + HiveTransactionMgr 搬到 `fe-connector-hive`，ACID 写路径联调 —— 2 周
- P7.4 DLA 分流逻辑改造（让 `HMSExternalTable` 退化为可被 PluginDrivenExternalTable 承接）—— 0.5 周
- P7.5 删除 fe-core `datasource/hive/` + 31 处反向 instanceof —— 0.5 周

**风险**：
1. ACID 写路径（INSERT OVERWRITE、INSERT INTO partition）的事务一致性回归——必须有专门的 acid 集成测试。
2. HMS event 处理的性能：在连接器进程内做事件流处理 vs fe-core 内做有什么差异。
3. Kerberos UGI 上下文——`ConnectorContext.executeAuthenticated` 现已支持，但要逐条审查。

### 3.9 阶段 P8：收尾清理

**任务**：

1. 删除 `CatalogFactory.SPI_READY_TYPES` 白名单 —— 所有 catalog 类型都走 SPI 路径，未找到 provider 的（如 `lakesoul`）按 P8.x 决策处理（直接报错或在 `gsonPostProcess` 中迁移）。
2. 删除 `CatalogFactory.createCatalog` 中的 `switch-case` 兜底，仅保留 SPI 找不到时的明确错误信息。
3. 删除 `PluginDrivenExternalTable.getEngine()` / `getEngineTableTypeName()` 中的 switch —— 改为 `Connector` 暴露 `getEngineName()` 这一 SPI 方法。
4. 删除 fe-core 中尚存的 `TableType.{HMS,ICEBERG,PAIMON,HUDI,MAX_COMPUTE,TRINO_CONNECTOR,LAKESOUL,ES,JDBC}_EXTERNAL_TABLE` 枚举值（保留 `PLUGIN_EXTERNAL_TABLE`）。所有写到 image 的旧值在 `gsonPostProcess` 中自动 reroute。
5. 文档：在 `fe/fe-connector/README.md` 写明"如何新增一个 connector plugin"的步骤化指南。
6. CI 守门强化：除 §1.2 的 import 守门，新增"fe-core 不得 import 任何 `*Connector*` 实现包"的 grep。

---

## 4. 单连接器迁移 Playbook（可复制清单）

每个连接器迁移，依次走完这 13 步：

```
[ ] 1. 列出该连接器在 fe-core/datasource/<name>/ 下的所有类，按 §1.1 终态分类。
[ ] 2. 列出 fe-connector-<name>/ 已有类，对照差距。
[ ] 3. 列出反向 instanceof / cast 调用点（grep `instanceof Xxx | (Xxx)`）。
[ ] 4. 在 fe-connector-<name>/ 实现缺失的 ConnectorMetadata / ScanPlanProvider 方法。
[ ] 5. 实现 ConnectorProvider.validateProperties 并补 ConnectorProvider.preCreateValidation。
[ ] 6. 实现 META-INF/services 注册（多数已就绪）。
[ ] 7. CatalogFactory.SPI_READY_TYPES 加入该类型。
[ ] 8. PluginDrivenExternalCatalog.gsonPostProcess 加迁移分支（logType → PLUGIN）。
[ ] 9. ExternalCatalog.registerCompatibleSubtype 注册 GSON 兼容子类型。
[ ] 10. 替换所有反向 instanceof：planner / nereids / tablefunction / alter / catalog 各处。
[ ] 11. PhysicalPlanTranslator.visitPhysicalFileScan 删该连接器分支。
[ ] 12. 写 / 跑回归测试：单元（fe-connector-<name>/src/test）+ regression-test 中 image 兼容用例。
[ ] 13. 删除 fe-core/datasource/<name>/ 整个目录 + 所有未关联 import。
```

---

## 5. 决策点（✅ 2026-05-24 全部按推荐确认）

| ID | 决策内容 | 决议 |
|---|---|---|
| D1 | SPI 是否要支持 SQL 透传以外的远程 query（如 `query()` TVF）？ | ✅ 沿用已有 `SUPPORTS_PASSTHROUGH_QUERY` |
| D2 | `PluginDrivenScanNode` 是否长期保持 `extends FileQueryScanNode`？ | ✅ 是；JDBC/ES 用 `FORMAT_JNI` 兜底 |
| D3 | 旧 `*ExternalCatalog` 子类的命运？ | ✅ **全部删除**，不保留中间形态 |
| D4 | HMS event pipeline 放哪儿？ | ✅ **fe-connector-hms** 内，通过 `ConnectorMetaInvalidator` 回调 |
| D5 | hudi/iceberg 在 HMS 上的 DLA 模型？ | ✅ **选项 A**：用 `ConnectorTableSchema.tableFormatType` 区分 |
| D6 | Iceberg snapshot/manifest cache 放哪儿？ | ✅ **连接器内**，fe-core 不感知 |
| D7 | `kafka` / `kinesis` / `odbc` / `doris` 子目录是否在本计划范围？ | ✅ **否**，单独立项 |
| D8 | 生产环境是否允许 "built-in" 连接器（classpath 中带）？ | ✅ **否**，只测试用，生产强制目录式插件 |
| D9 | API 版本号何时 +1？ | ✅ 本计划范围内**永不 +1**，只新增 default 方法 |
| D10 | `LakeSoulExternalCatalog` 是否删除？ | ✅ 在 P8 删除剩余类 |
| D11 | `RemoteDorisExternalCatalog`（Doris-to-Doris）是否做成 connector？ | ✅ 长期做，**不在本计划主线** |
| D12 | 用户安装 connector 后是否要求重启 FE？ | ✅ 初版**强制重启** |

---

## 6. 风险登记册

| ID | 风险 | 影响 | 缓解 |
|---|---|---|---|
| R1 | Image 反序列化兼容性回归（用户从旧 FE 升级） | High | 每次迁移加 image 兼容测试；保留 `gsonPostProcess` 迁移分支至少 2 个大版本 |
| R2 | Hive ACID 写路径在重构后数据不一致 | High | P7.3 必须有独立 ACID 集成测试套件作为 gate |
| R3 | Iceberg Procedure SPI 抽象失败（10 个 action 行为不齐） | Med | 先看 Trino Iceberg connector 怎么做的，再定 SPI 形态 |
| R4 | classloader 隔离打破 SDK 单例（Iceberg、Paimon、Trino）| Med | `ClassLoadingPolicy` 中 `parent-first` 列表必须覆盖所有共享 SDK 接口 |
| R5 | nereids 优化器对 `IcebergExternalTable` 的特殊规则不能用通用 SPI 表达 | Med | 在 P6.3 之前单独评审写路径方案；考虑给 ConnectorMetadata 暴露 hint API |
| R6 | 性能回归：每次访问通过 SPI 的反射/桥接增加额外开销 | Low | benchmark：1k 个 catalog × `listTableNames`、`getSchema` 路径基准；接受 < 5% 损失 |
| R7 | 部分 jar 在 BE / FE 间共享，连接器化后 FE 侧无法访问 | Low | `plugin-zip.xml` 的 exclude 列表要包含 BE 侧 jar；逐个 review |
| R8 | 用户文档与新插件部署流程不同步 | Low | P2 开始就同步写文档；P8 时整理为一份完整 admin guide |

---

## 7. 交付物

1. 本计划 `plan-doc/00-connector-migration-master-plan.md`（v1）。
2. 每个连接器一份 `plan-doc/<phase>-<connector>-migration.md`，在进入对应阶段时撰写。
3. P0 输出：`plan-doc/01-spi-extensions-rfc.md` —— SPI 新增能力的详细设计。
4. P6 输出：`plan-doc/06-iceberg-write-path-rfc.md` —— Iceberg 写路径 SPI 化方案。
5. P7 输出：`plan-doc/07-hms-event-pipeline-rfc.md` —— HMS event pipeline 放置 RFC。
6. P8 输出：`fe/fe-connector/README.md` —— 用户/开发者最终使用手册。

---

## 8. 当前一周建议从哪开始

1. ✅ **已完成**：决策点 D1–D12 已于 2026-05-24 全部按推荐确认。
2. 🚧 **进行中**：P0 RFC —— 见 `plan-doc/01-spi-extensions-rfc.md`，列出 §2.1 表里所有新增类型/方法的具体 Java 签名和 javadoc 草稿。
3. **下一步**：P1 的重复清理 + scan-node 收口（无 SPI 风险、纯重构，可独立 PR）。
4. **再下一步**：P2 trino-connector 全流程，把 playbook 跑通后再大规模铺开。
