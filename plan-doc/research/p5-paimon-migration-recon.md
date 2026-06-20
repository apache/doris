# P5 paimon 迁移 — code-grounded recon

> 产出于 P5 启动（2026-06-09）。方法：14 路 subagent（5 区 fe-core 旧实现 + 新 SPI 面 + maxcompute 样板 + 连接器现状 → 5 区 old→new 设计 → 跨切面对抗复审）code-grounded 调研 + 主线 firsthand 核读 load-bearing 锚点（SPI_READY_TYPES / GSON 注册 / PluginDrivenExternalTable / ConnectorPartitionInfo）。
> 用途：research-design-workflow 的 research note；P5 scope fork 的事实底座。设计/批次/TODO 见 `tasks/P5-paimon-migration.md`。
> 范围：用户指定 5 功能区 —— ① 普通表读取 ② 系统表读取 ③ procedure ④ DDL ⑤ mtmv —— 旧框架实现 + 映射新 SPI + 对齐 maxcompute 接口一致性。

---

## 0. 头条结论（与 HANDOFF / 连接器档假设的偏差）

**paimon 是首个真正消费 E5(MVCC)/E6(vended)/E7(sys-tables) 的 adopter，且唯一带 MTMV 的 adopter —— 而 SPI 对 MTMV 完全无面（E10 缺）。maxcompute 这四块全无先例。**

1. **「6 catalog flavor 工厂重组」失真**：`fe-connector-paimon-{api,backend-filesystem,backend-hms,backend-rest,backend-aliyun-dlf}` 五个模块**只有 gitignore 的 `.flattened-pom.xml`，零 src / 零 pom / 未注册 Maven 模块 / 连接器不依赖它们**（`git ls-files` 实证）。当前连接器走**单 Catalog 模型**：`PaimonConnector.createCatalog`（`PaimonConnector.java:75-83`）把 `Options.fromMap(props)` 直接喂 paimon SDK 的 `CatalogFactory`，由 SDK 按 `paimon.catalog.type` 自分发——**无任何 Doris 侧 flavor 装配**（warehouse / HiveConf / StorageProperties / 每-flavor authenticator 全缺）。→ flavor 模型是 **scope fork**（见 §11 决策 D1）。
2. **MTMV 无 SPI 面（E10 缺，blocker）**：`PluginDrivenExternalTable`（`PluginDrivenExternalTable.java:62`）**不** implements `MTMVRelatedTableIf`/`MTMVBaseTableIf`/`MvccTable`（firsthand 核实）；MTMV 框架靠 `instanceof MTMVRelatedTableIf` 分发（`MTMVPartitionUtil.java:265/497/588`、`StatementContext.java:987/1003` 的 `MvccTable`）。故**翻闸后 SPI paimon 表对 MTMV 刷新与时间旅行 pin 完全隐形**——若按 maxcompute 样板直接翻闸即**静默功能回归**。legacy `PaimonExternalTable.java:74` 实现全部三接口。
3. **procedure = 零可迁**：fe-core `datasource/paimon/` **无 procedure/action 文件**；`CALL paimon.x` 现即抛 `AnalysisException`（`CallFunc.java:43`），`ALTER TABLE paimon EXECUTE` 现即抛 `DdlException`（`ExecuteActionFactory.java:61`）。唯 iceberg 有 EXECUTE actions（11 文件，FE 内嵌 SDK 跑），`expire_snapshots` 是 **iceberg** 词，非 paimon。→ P5 该区 = **doc-only no-op**（非「后续 port」）。
4. **读路径已近完工**：连接器 `PaimonScanPlanProvider` 已做 `ReadBuilder.withFilter/withProjection/newScan().plan().splits()` + native(ORC/Parquet)-vs-JNI 分类 + deletion file + `TPaimonFileDesc`，与 fe-core 字节级接近 → 普通表读取主要是**补缺 + 翻闸**，非 greenfield。
5. **翻闸 GSON blast radius 比 MC 大**：paimon 有 **7 处注册**（5 catalog `GsonUtils.java:390-396` + db `:450` + table `:471`，全 `registerSubtype`，firsthand 核实），MC 只迁 1 catalog 名。漏任一（尤其 db）→ replay `ClassCastException`（[[catalog-spi-gson-migrate-all-three]]）。
6. **重复 `PaimonPredicateConverter`** 确认：fe-core `source/PaimonPredicateConverter.java:43`（吃 fe-core `Expr`）vs 连接器 `PaimonPredicateConverter.java:57`（吃 `ConnectorExpression`）。连接器版有 **session-TZ bug**（时间戳走固定 UTC offset `:284`，无 session TZ）——[[catalog-spi-connector-session-tz-gotcha]] 同款，翻闸前须修。

---

## 1. 连接器现状（`fe-connector-paimon` = 10 文件 / metadata-read + scan 骨架）

唯一 git-tracked 模块（`fe/fe-connector/pom.xml:49` 注册），Phase-1 commit `5c325655b8b`(PR 62183) 建，**非** P5 专项。**0 测试**。

| 类 | 状态 | 锚点 / 备注 |
|---|---|---|
| `PaimonConnectorProvider` getType=`paimon` | ✅ | `:32`；META-INF/services 已注册；运行时永不分发（paimon 不在 SPI_READY_TYPES）|
| `PaimonConnector` | ✅ | `:43`；lazy double-checked 建 Catalog（`:64-83`，**stub**，无 flavor 装配）|
| `PaimonConnectorMetadata` | 🟡 read-only | `:51`；list db/table + getTableHandle/getTableSchema/getColumnHandles 实现；`getProperties` stub 返空（`:154`）；DDL/write/stats/**partition**/MVCC/sys-table/identifier 全落 SPI throwing/empty 默认 |
| `PaimonScanPlanProvider` | 🟡 | `:71`；planScan 真做 projection+predicate+native/JNI+deletion；缺 COUNT 下推（row_count 恒 -1）、cpp-reader encode、history-schema、6-arg requiredPartitions override；planScan 假设 `getPaimonTable` 非空**无 reload fallback** |
| `PaimonPredicateConverter` | 🟡 | `:57`；AND/OR/比较/IN/IS NULL/前缀 LIKE；FLOAT 返 null(`:262`)、CHAR null(`:274`)（与 legacy 故意不下推一致）、TIME 不支持、**时间戳固定 UTC(`:284`) 无 session TZ = bug** |
| `PaimonScanRange` | ✅ | `:51`；populateRangeParams(`:155-227`) 建 `TPaimonFileDesc`（JNI+native+deletion+count+columns-from-path）；getTableFormatType=`paimon`(`:130`)；row_count 分支在但 provider 不喂 |
| `PaimonTableHandle` | ✅ 但有坑 | `:31`；持 db/table/partition-keys/primary-keys + **transient Table(`:41/73`)**；序列化后 Table 丢、planScan 无 reload = **BLOCKER** |
| `PaimonColumnHandle` / `PaimonTypeMapping` / `PaimonConnectorProperties` | ✅ | TypeMapping 仅 paimon→ConnectorType（DDL 反向缺）；Properties 仅 catalog-type/warehouse/2 mapping flag（HMS/REST/DLF/JDBC/cred 键全缺）|

**5 个 backend 模块 = 空壳**（仅 gitignore `.flattened-pom.xml`，描述意图中的 `PaimonBackend`/`PaimonBackendFactory` ServiceLoader；aliyun-dlf 自述「M3 STUB」）。

---

## 2. 新 SPI 面 E1–E10 状态（paimon 视角）

| 扩展点 | 状态 | 锚点 / paimon 关系 |
|---|---|---|
| E1 CreateTableRequest | ✅ defined-and-consumed | `ddl/ConnectorCreateTableRequest.java:40` + PartitionSpec/BucketSpec；MC 已用；`CreateTableInfoToConnectorRequestConverter` 喂；paimon createTable 待实现 |
| E2 Procedures | ❌ **absent** | 无任何 procedure SPI；最近似 `ConnectorTableOps.executeStmt:114`（DML passthrough，**非** procedure registry）。paimon 无需（§3.3）|
| E3 Scan/Pushdown | ✅ defined-and-consumed | `scan/ConnectorScanPlanProvider.java:38`(planScan/getSerializedTable:278) + `ConnectorPushdownOps.java:35`；`PluginDrivenScanNode:474/660/679` 消费；paimon 已用 |
| E4 Write/Transaction | ✅ defined-and-consumed | `write/ConnectorWritePlanProvider.java:34` + `ConnectorWriteOps` + `ConnectorTransaction`；live consumer = MC。**paimon 写本 session 未列入用户 5 区**（legacy `PaimonMetadataOps` 仅 DDL，无 INSERT 写 session；merge-on-read 写是 E9，未来）|
| E5 MvccSnapshot | 🟡 **defined-no-consumer** | `mvcc/ConnectorMvccSnapshot.java:34`（final，仅 snapshotId:51/timestampMillis:56/desc/props）；`ConnectorMetadata.beginQuerySnapshot:60/getSnapshotAt:66/getSnapshotById:73` 默认空；`ConnectorMvccSnapshotAdapter` **仅自身文件引用**，3 方法仅测试调；`ConnectorScanRange` **无 snapshot 字段**（无 BE pin seam）。**paimon = 首个真消费者** |
| E6 VendedCredentials | ❌ **absent** | 仅 `ConnectorCapability.SUPPORTS_VENDED_CREDENTIALS` 枚举常量(`:38`)，零消费者。paimon REST flavor 首个需求方 |
| E7 SysTables | ❌ **absent** | 无 SysTable/MetadataTable SPI、无 fe-core bridge。仅 `listPartitions*` 分区内省。**greenfield**，paimon 首个 |
| E8 Statistics | 🟡 partial | 仅表级 `getTableStatistics` 默认空；无列级 |
| E9 Identifier | ✅ | `ConnectorIdentifierOps`，identity 默认，低风险 |
| **E10 MTMV** | ❌ **absent（blocker）** | fe-connector 树 **零 MTMV 符号**；`PluginDrivenExternalTable.java:62` 实现无 MTMV 接口（firsthand）；legacy `PaimonExternalTable.java:74` 实现全套。须**新增**（详见 §3.5 + §4）|

---

## 3. 五大功能区 fe-core 现状（code-grounded）

### 3.1 普通表读取

- **入口/分发**：`BindRelation`（`:467/539` PAIMON_EXTERNAL_TABLE → LogicalFileScan + `loadSnapshots`→`PaimonExternalTable.loadSnapshot`）→ `PhysicalPlanTranslator:781`（reverse-instanceof `PAIMON_EXTERNAL_TABLE` → new `PaimonScanNode`，并传 tableSnapshot/scanParams `:793-797`）。
- **核心类**：`PaimonScanNode`（`source/PaimonScanNode.java:78` extends `FileQueryScanNode`）—— split 生成（`getSplits:360`：DataSplit→native RawFiles / JNI 全序列化 Split / COUNT 合并行数）、谓词转换（`convertPredicate:189`）、per-split thrift（`setPaimonParams:253`）、`getPaimonSplitFromAPI:581`（真 SDK 调 `ReadBuilder.plan().splits()`）、`validateIncrementalReadParams:701`、`getProcessedTable:880`（增量读 `copy(incrReadParams)`）。`PaimonSource`（`source/PaimonSource.java:37`）解析 `org.apache.paimon.table.Table`（reverse-instanceof PaimonExternalTable/PaimonSysExternalTable）。
- **BE 交互**：JNI（`be-java-extensions/paimon-scanner` + BE C++ `paimon_jni_reader.cpp` **及** native `paimon_cpp_reader.cpp`/`paimon_predicate_converter.cpp`）。`serialized_table`/`paimon_split`/`paimon_predicate` 经 `PaimonUtil.encodeObjectToString`（InstantiationUtil + **URL-safe** Base64）。
- **缓存**：`PaimonExternalMetaCache` + `metacache/paimon/{PaimonTableLoader,PaimonLatestSnapshotProjectionLoader,PaimonPartitionInfoLoader}`。
- **⚠️ 序列化格式 Base64 之争（已证伪为非 blocker）**：连接器用 **standard** Base64（`PaimonScanPlanProvider.java:384`）vs fe-core URL-safe（`PaimonUtil.java:519`）；但 BE `PaimonUtils.deserialize` **先试 URL_DECODER 再 fallback STD_DECODER**（`PaimonUtils.java:42-47`）→ 两种都能反序列化。降级为「须 round-trip smoke 验 + pin paimon-core 版本」（InstantiationUtil 跨版本敏感）。

### 3.2 系统表读取

- **两层**：通用 `datasource/systable/`（`SysTable` 名解析末-`$` 切分 `:92-101`、`SysTableResolver:54` 分发 `NativeSysTable && ExternalTable`、`PaimonSysTable:45` extends `NativeSysTable`，`SUPPORTED_SYS_TABLES` 取自 SDK `SystemTableLoader.SYSTEM_TABLES:51`）**+** paimon 专有 `PaimonSysExternalTable`（`:65` extends ExternalTable，报 `PAIMON_EXTERNAL_TABLE:88`，`getSysPaimonTable` 走 **4-arg** `Identifier(db,tbl,"main",sysName):118`，`toThrift` 发 `HIVE_TABLE:180`，`fetchRowCount` plan splits `:200`）。
- **读路径**：`PaimonScanNode` 多处 sys 分支——非 DataSplit 强制 JNI、`shouldForceJniForSystemTable`（binlog/audit_log，`:523-536`）、`getProcessedTable` 拒 scan-params/time-travel（`:883-890`）。
- **关键迁移约束**：迁后 sys wrapper **必须 extends `PluginDrivenExternalTable`（报 PLUGIN_EXTERNAL_TABLE）** 才能走 `PluginDrivenScanNode`；若照抄报 `PAIMON_EXTERNAL_TABLE` → 路由到**即将删除**的 legacy `PaimonScanNode`（routing blocker）。binlog/audit_log 是 **DataTable** 却须强制 JNI → 须按 **sysName** 而非 split 类型判定。

### 3.3 procedure

- **零可迁**（firsthand）。`CALL` → `CallFunc.getFunc` 闭式 2-case（EXECUTE_STMT/FLUSH_AUDIT_LOG），default throw（`CallFunc.java:43`）。`ALTER ... EXECUTE` → `ExecuteActionFactory.createAction:50` reverse-instanceof，仅 `IcebergExternalTable` 分支，其余 throw（`:61`）。`ExecuteActionFactory` import fe-core 具体类（连接器禁 import），故未来 paimon procedure 须**新 SPI seam**。
- **两个假阳性**（须 doc 钉死防后人误挖）：① `CALL paimon.sys.migrate_table`（`test_hive_migrate_paimon.groovy:84-93`）= **Spark** 容器命令非 Doris；② `expire_snapshots` = **iceberg** action（`IcebergExpireSnapshotsAction`）非 paimon。

### 3.4 DDL（create/drop table & db + 6 flavor）

- **flavor 装配（hard part）**：`PaimonExternalCatalogFactory:29-47`（验 flavor、恒返 base catalog）；`AbstractPaimonProperties:37`（warehouse/options/ExecutionAuthenticator/S3 normalize）+ 5 子类 `Paimon{HMS,AliyunDLF,Rest,FileSystem,Jdbc}MetaStoreProperties` + `PaimonPropertiesFactory:22`（按 `paimon.catalog.type` 注册 dlf/filesystem/hms/rest/jdbc）。全 import fe-core `StorageProperties`/`HMSBaseProperties`/`HadoopExecutionAuthenticator`（**禁 import**）。**每-flavor authenticator**（HMS/HDFS-FS/JDBC=Hadoop doAs；DLF/Rest=no-op）包裹 createCatalog+每次 DDL——丢即 Kerberized DDL 炸（无离线测覆盖）。REST 忽略 storage(`Rest.java:78`)、DLF 强制 OSS(`DLF.java:90`)、JDBC 动态 DriverShim/URLClassLoader——非对称须复刻。
- **metadata-ops**：`PaimonMetadataOps:64-405`（createDb HMS-only-props gate `:103`、dropDb force enumerate-loop `:147`、createTable 双存在检查 + `toPaimonSchema:231`、`DorisToPaimonTypeVisitor:48`）。**ALTER 全 throw UnsupportedOperation（`:309-333`）= 本就不支持，out of scope**。
- **latent bug 须保留不修**：performCreateTable 用 remote 名查存在但 LOCAL 名建 Identifier（`Ops.java:190` vs `:223`）。
- **翻闸 fe-core 编辑点**：SPI_READY_TYPES 加 paimon(`CatalogFactory.java:52`)+删 built-in case(`:142`)；`CreateTableInfo.pluginCatalogTypeToEngine` 加 `case paimon→ENGINE_PAIMON`（`:937-944`，否则 no-ENGINE CREATE TABLE + engine 一致性断，instanceof PaimonExternalCatalog `:388/915` 翻闸后失火）；GSON 7 注册齐迁。

### 3.5 mtmv

- legacy `PaimonExternalTable.java:74` implements `MTMVRelatedTableIf`+`MTMVBaseTableIf`+`MvccTable`，方法：`loadSnapshot:308`(→`PaimonMvccSnapshot`)、`getTableSnapshot:271/284`(→`MTMVSnapshotIdSnapshot(snapshotId)`)、`getPartitionSnapshot:259`(→`MTMVTimestampSnapshot(lastFileCreationTime)`)、`getAndCopyPartitionItems:228`、`getPartitionType:233`(LIST/UNPARTITIONED)、`getPartitionColumnNames:241`、`isPartitionInvalid:254`、`isPartitionColumnAllowNull:298`(恒 true)、`beforeMTMVRefresh:223`(no-op)、`getNewestUpdateVersionOrTime:290`(**故意绕过 pin**)。
- **单-pin 不变式**：`loadSnapshot` 一次 pin `PaimonSnapshotCacheValue`(snapshotId+分区集+Table handle)，全 MTMV/schema 方法经 `getOrFetchSnapshotCacheValue:383` 复用同 pin。`StatementContext.loadSnapshots:987` 按 `MvccTableInfo` 存不透明 `MvccSnapshot`。
- **SPI 拆分难点**：`ConnectorMvccSnapshot`（final）只能载 snapshotId/timestamp，**载不动**分区 map + Table handle → 富 pin 须留 fe-core 侧。MTMV 类型（`PartitionItem`/`PartitionType`/`Column`/`MTMVSnapshotIf` 及其 GSON 子类）**无 SPI 等价物** → MTMV 必须留在 fe-core。
- **好消息**：表级 staleness=snapshotId 正好映 `ConnectorMvccSnapshot.getSnapshotId():51`（long，-1 哨兵对齐 `PaimonSnapshot.java:23`）；分区级 staleness=`ConnectorPartitionInfo.getLastModifiedMillis():90`（**已存在**，firsthand 核实，6-arg ctor `:53`）；分区枚举→PartitionItem 由 `PluginDrivenExternalTable.getNameToPartitionItems:246`（基类已做，复用即删 fe-core 重复 `PaimonUtil.generatePartitionInfo`）。
- **新风险 GAP-LISTPART-AT-SNAPSHOT**：`listPartitions` 与 snapshot 方法解耦——无法「按 pin 的 snapshotId 列分区」。若 `beginQuerySnapshot` pin 了 snapshotId 但 `listPartitions(latest)` 看到更新 snapshot → 刷新 staleness keying 错位（**最高 correctness 风险，静默**）。

---

## 4. 跨切面（翻闸风险，对抗复审已核）

| 关注 | 结论 / 行动 |
|---|---|
| **MTMV 无 SPI（E10）** | 翻闸**不**机械阻断（plain SELECT 经 `getPaimonTable(empty)` 取 latest 仍工作），但**静默回归**已有 paimon-MTMV-base + 时间旅行 pin。修法 = fe-core 新建 `PaimonPluginDrivenExternalTable extends PluginDrivenExternalTable` 实现三接口（拉 SPI-neutral 数据：snapshotId via E5、lastModifiedMillis via ConnectorPartitionInfo）。**翻闸前必须落地此桥 OR 显式 fail-loud + 文档化「暂不支持」**——禁静默读 latest。GSON table 子类须注册此 MTMV-capable 子类（非裸 PluginDrivenExternalTable）。|
| **GSON 7 注册原子齐迁** | 5 catalog 名 + db + table 全转 `registerCompatibleSubtype`→PluginDriven*；漏任一（尤其 db）→ replay `ClassCastException`。须加每个 flavor catalog 名的 replay 测。|
| **FE 分发缺口（部分已预接）** | **更正先验**：DROP TABLE/CREATE·DROP DB/CREATE TABLE 已在 `PluginDrivenExternalCatalog`(createTable:267/createDb:336/dropDb:377/dropTable:406) 通用 override；SHOW PARTITIONS(`ShowPartitionsCommand:206`)+partitions TVF(`MetadataGenerator:1349`) 已对 PLUGIN_EXTERNAL_TABLE 预接。**残留缺口 = 连接器侧 `listPartitions*` 未实现** + 确认 `isPartitionedTable`/`getPartitionColumns` 透出 paimon 分区列。|
| **classloader R-004** | `ConnectorPluginManager.java:62-63` parent-first → paimon SDK 单一共享实例（`SystemTableLoader` 静态、JDBC DriverManager 全局）。多 catalog 不同 flavor/版本共存依赖 SDK 容忍度（**开放，源码不可验**）。|
| **FE/BE 共享 jar R-007** | BE paimon-scanner **冻结不动**；序列化身份是契约。须 **pin 连接器 paimon-core == be-java-extensions/paimon-scanner + preload-extensions 的版本**（InstantiationUtil 跨版本静默破）。Base64 之争已证伪（§3.1）。删 legacy 后须验 paimon-core 在 FE classpath 恰一份（[[catalog-spi-be-java-ext-shared-classpath]] FE 类比）。|
| **snapshotId 类型 R-012** | `long` 端到端正确，-1 哨兵。连接器须返 `ConnectorMvccSnapshot(snapshotId=-1)` 而非 `Optional.empty()` 表示空表。|
| **session-TZ 谓词 bug** | 连接器 `PaimonPredicateConverter:284` 固定 UTC → 非 UTC session 时间戳谓词误推丢行。翻闸前修：读 `ConnectorSession.getTimeZone()` 惰性解析+降级（镜像 `MaxComputePredicateConverter:302`）。|

---

## 5. maxcompute 一致性约定（连接器须遵循，from full-adopter 样板）

> MC = 完整 full-adopter 范本，但 **sys-table/procedure/MVCC/MTMV/vended 全无**（fe-core datasource/maxcompute 已 0 文件）。paimon 在这四块**无 MC 先例**，须自定义 minimal default-no-op SPI。可镜像的是结构约定：

1. **仅依赖** fe-connector-api/spi + 数据源 SDK + `org.apache.doris.thrift.*`；**禁** import fe-core/fe-common/fe-catalog → 常量/工具**拷入**模块（MC 拷 MCProperties→MCConnectorProperties、MCUtils→MCConnectorClientFactory）。
2. fe-core 可调项经 `ConnectorSession.getSessionProperties()/getTimeZone()` **逐字节字符串键**读，带 legacy 默认 fallback；键须对齐 `ConnectorSessionBuilder` 注入。
3. Provider getType 字符串 == SPI_READY_TYPES 项 == ScanRange.getTableFormatType（单一真源）；META-INF/services 注册。
4. Handle/ColumnHandle = 不透明 Serializable 值（重 SDK 对象 transient + **反序列化 reload**）；ScanRange Serializable，getTableFormatType 选 BE thrift 分支。
5. BE thrift 全在连接器内建（populateRangeParams/buildTableDescriptor/sink），类型打标，**禁返 null**（BE static_cast）。
6. `validateProperties` 在 CREATE CATALOG fail-fast 抛 `IllegalArgumentException`。
7. 谓词 **exact-or-dropped**：不可转 → 降级 NO_PREDICATE（BE 复算）；`supportsCastPredicatePushdown()=false` 除非 CAST 语义等同。
8. 重远端 client/settings **建一次共享** scan+write（lazy double-checked）。
9. 远端 SDK 调用经接口抽出（如 `McStructureHelper`）+ ctor 可注入 → 离线 recording-fake 单测（**无 mockito / 无 fe-core**）；live 测 JUnit Assumptions 守 env。
10. DDL：连接器做远端，`PluginDrivenExternalCatalog` override 做 edit-log+cache invalidation；显式 honor IF NOT EXISTS/FORCE，fail-loud 不静默。
11. 翻闸 GSON 三注册（catalog+db+table）原子齐迁 + SPI_READY_TYPES 加类型。

**paimon 须 diverge 处（合理）**：① MTMV/MvccTable 经 fe-core 子类（非加到通用基类，保 MC/jdbc/es/trino 干净）；② 首个真 E5 消费者须 override beginQuerySnapshot 三方法 + 声明 SUPPORTS_MVCC_SNAPSHOT/TIME_TRAVEL；③ 首个 E7 消费者须定义 sys-table SPI hook（default-empty）。

---

## 6. 测试基线

- 连接器 `fe-connector-paimon`：**0 测试**（须建测试模块 + 注入式 SDK seam）。
- 旧框架 / sys-table / MTMV-over-paimon / 时间旅行 / deletion-vector native / 元数据 sys-table JNI 读：**recon 未定位到任何回归/e2e baseline** → 翻闸回归不可检，须 B0/B9 自建 before/after parity + FE→BE round-trip smoke。

---

## 7. 沿用坑（来自 HANDOFF / auto-memory）

- maven 绝对 `-f .../fe/pom.xml -pl :<art> -am -Dmaven.build.cache.enabled=false`；改连接器 `:fe-connector-paimon`、改 SPI `:fe-connector-api`、改 fe-core `:fe-core`。读真实 `Tests run:`/`BUILD`，勿信后台 echo exit（[[doris-build-verify-gotchas]]）。
- 连接器禁 import fe-core（import-gate `tools/check-connector-imports.sh`）；session 值经 session-property 透传（[[catalog-spi-connector-session-tz-gotcha]]）。
- 连接器测试模块无 mockito（纯 seam / child-first loader，[[catalog-spi-fe-core-test-infra]]）；checkstyle 含 test 源、test 阶段不跑（单独 `checkstyle:check`）。
- 删多模块 dep 须核传递依赖（[[catalog-spi-be-java-ext-shared-classpath]]：P4 删 odps 蒸发 BE commons-lang）；删 legacy 后验 paimon-core FE classpath 恰一份。
- clean-room 对抗复审偏好（[[clean-room-adversarial-review-pref]]）；本 recon 已用（14 agent + cross-cut critic 证伪了 Base64-blocker / FE-dispatch-全缺 / 6-flavor-工厂-已建 三个先验）。
