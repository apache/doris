# 删除批次方案 — hive/hudi/iceberg 遗留单元收尾删除（HEAD-verified，2026-07-13）

> **状态**：施工批次方案，**待用户 review**。本文档给「删除阶段」的可编译拓扑批次序 + 每批验证点 + 新揪出的连锁文件/测试 + 待用户决策。
> **依据**：本 session 一轮 15-agent HEAD 重核（`.claude/wf-p75-deletion-batch-verify.js`，run `wf_749d6834-e98`）——§4-B ACID 定论 + DELETE/EDIT 清单在**当前 HEAD**（`0f45c482e58`，已含 6 个委派 commit）逐文件重核 + 抽取遗留就绪度。**行号信 HEAD**。
> **上游文档**：施工蓝图 `P7.5-datasource-deletion-plan.md`（§1 删除集 / §2 EDIT 集 / §5 拓扑序）；合规委派 `datasource-deletion-extraction-reanalysis-2026-07-13.md`；进度清单 `datasource-deletion-tasklist.md`。

---

## 0. 一句话结论

委派（步 2a/2b）已全绿；删除阶段**唯一还欠的 fe-core 行为改动**是「分区有序值 fail-loud」（删旧回退），其余全是**机械切死臂 + 机械删死文件 + 联动测试**。§4-B ACID 隐患**已定论为可安全直切**（无需连接器改动）。

---

## 1. §4-B ACID 隐患 — 定论：**安全直切**（不需连接器改动）

**问题**：`FileQueryScanNode.splitToScanRange:465-468` 有一段死臂 `if (fileSplit instanceof HiveSplit) isACID = ...`。迁移后的 hive 走 `PluginDrivenSplit`（不是 `HiveSplit`），`isACID` 恒 false。担心：带分区的 **ACID 事务表**若连接器没填分区值，直切这段后会按路径把 `delta_xxx` 目录层误当分区值剥错列。

**HEAD 重核结论（三重独立保险，均已核实）**：
1. **连接器对分区表总是填分区值**：`fe-connector-hive` 唯一的 scan-range 构造点 `HiveScanRange.builder()...partitionValues(...)`（`newRangeBuilder`）取自 `convertPartitions`（HMS 分区列名 × 值），**ACID 分支 `planAcidScan` 复用同一份分区列表与 emit 路径**。分区值 Map 为空只可能是「分区表却零分区值」的畸形 HMS 元数据——合法 Hive 不产生。
2. **非空分区值 Map → `PluginDrivenSplit.getPartitionValues()` 非 null**：故 `splitToScanRange:470` 走 `normalizeColumnsFromPath` 分支，**从不进 isACID 路径解析**。
3. **`HiveScanRange.populateRangeParams:163-165` 无条件 `unset` columns-from-path**，随后仅「重建」被空值保护——即便出现畸形空分区值，被错剥的路径解析结果也在 BE 见到前被丢弃，**绝不会**把错误分区值发给 BE。

**处置**：Phase-1 item #1 = **直接切掉 `instanceof HiveSplit` 死臂 + import**（`FileQueryScanNode:465-468` + `import:39`），`boolean isACID=false`（`:464`）保留（`:472` 仍用）。**无需**新增 `ConnectorScanRange.isAcid()` 中立位。belt-and-suspenders：补一条「迁移-hive 分区 ACID 表读、分区列值正确」的 e2e。

> 对齐 Trino：分区值由连接器解析好交引擎，引擎不做路径字符串剥列——这正是委派后的状态。

---

## 2. 删除阶段还欠的**唯一 fe-core 行为改动**：分区有序值 fail-loud

**现状**：`PluginDrivenMvccExternalTable.toListPartitionItem:311-313` 仍保留旧解析回退
`!connectorValues.isEmpty() ? connectorValues : HiveUtil.toPartitionValues(partitionName)`。这是删 `HiveUtil` 前必须先拆的**最后一处 live 引用**；且 `listLatestPartitions:272-282` 的 try/catch 会**静默吞**分区（表被误报 UNPARTITIONED），所以不能只删回退不 fail-loud。

**已核实**：4 连接器**全部已填** `orderedPartitionValues`——hive `HiveConnectorMetadata:1110`、iceberg `IcebergPartitionUtils:545`、paimon `PaimonConnectorMetadata:1052/1085`、hudi `HudiConnectorMetadata:736`。

**处置**：删回退，`connectorValues` 空即 `Preconditions.checkState` 硬报错（分区表上强制连接器供值）。这是删除批次的**第一步**（Batch 1），配分区有序值 e2e（含非字符串列真 NULL 分区）。

其余抽取遗留（均已就绪）：
- **hive LZO 三方法** = 纯删（消费点全死，连接器早自带）；同批 co-delete `HiveUtilTest.java`。
- **hive 默认分区哨兵** = LIVE 侧已完（`FilePartitionUtils` 三处已改指中立常量），随 `HiveExternalMetaCache` 整删。
- **IcebergUtils 行血缘成员** = 已成 dead-only；`CreateTableInfo.validateIcebergRowLineageColumns(int)` 已 dead（唯一调用方是死 `IcebergMetadataOps:358`），随 iceberg 批删。

---

## 3. 批次序（可编译拓扑）——每批后 fe-core `test-compile` 必绿

> 核心原理：**先补行为改动 → 再切断所有 live 文件对删除集的编译引用（不删类）→ 再一次性删除自成闭环的死文件簇 + 联动测试**。切死臂时删除集的类仍在，故 test-compile 一路绿；删文件时 live 引用已清零，剩下的就是死簇内部互引，整簇原子删。

### Batch 1 — Phase-1 前置（行为改动，独立 commit + e2e）
- **1a fail-loud**：`PluginDrivenMvccExternalTable.toListPartitionItem` 删回退、强制连接器供值。
- **1b 事件管道 legacy 拆除**：`Env` 去 `MetastoreEventsProcessor` 全套面（field `410` / init `768` / getter `1046-1048` / `start():2089` / import `109`）；`ExternalMetaIdMgr:127-130` 切死 else 臂；改 `ExternalMetaIdMgrTest`（去 mock 的 `@Override getMetastoreEventsProcessor`）。
- **1c §4-B**：`FileQueryScanNode:465-468` 切 `instanceof HiveSplit` 死臂 + `import:39`。
- **验证**：test-compile 绿；分区有序值 e2e + ACID 读 e2e（用户自跑）。

### Batch 2 — 切断所有 live KEEP 文件对删除集的死臂/死方法/死 import（**不删任何类**）
> 分区块提交，每块后 test-compile 绿（删除集类仍存在）。行号见 §5 明细（本 session 已逐一重核）。

- **2a datasource/ 内**：`CatalogMgr`（两 `instanceof HMSExternalTable` 臂 `818-830/867-872` + 5 import）· `ExternalMetaCacheMgr`（hive/hudi/iceberg 注册 `292-294` + 访问器 `159-172` + 常量 `62-64` + import `24-26`）· `ExternalMetaCacheRouteResolver`（死臂 `66-71` + 常量 `36-38` + import `23`）· `CatalogConnectivityTestCoordinator`（Hive/Glue 两臂 `269-278` + import `22-23`）· `Env` 的 `hiveTransactionMgr` 全套面（field `572` / init `870` / getter `1106-1112` / import `108`）。
- **2b planner/statistics**：`PlanNode`（两 `instanceof IcebergScanNode` 臂 `949-975` + 死方法 `mergeIcebergAccessPathsWithId:1011-1033` + import `39`/`23`）· `BaseExternalTableDataSink`（`getTFileFormatType:78-103`/`getTFileCompressType:105-126`/抽象 `supportedFileFormatTypes:61` 随 `HiveTableSink` 死 → 连 `PluginDrivenTableSink` override `127-131` 一并清；**getBrokerAddresses:63-76 亦死，见决策**）· `RefreshManager`（`214-234` HMS 臂 + `312-318` else 臂 + import `32-34`）· `AnalysisManager`（`1492-1493` → `return false` + import `47`）· `StatisticsUtil`（`1007-1012` 臂 + 死方法 `getHiveRowCount:524-560`/`getRowCountFromParameters:562-578`/`getTotalSizeFromHMS:580-591` + import `58-59`）· `StatisticsAutoCollector`（**hudi-jar `VisibleForTesting` import `36` 换非-hudi 源，见决策**）· `MaterializeProbeVisitor`（`61` set 项 + `137-141` 臂 + import `24-25`）。
- **2c nereids binds/commands**：`BindRelation`（`case HMS_EXTERNAL_TABLE:755-781` 整删 + import `51/52/105`）· `BindSink`（rule `159-160` + `bindHiveTableSink:660-715` + 死 `bind:1029-1042` + import `44/45/46/54/85`；**注：LZO `:678` 在死方法内，随删无需 redirect——与旧蓝图 §5 口径不同**）· `CheckPolicy`（hudi 增量臂 `93-100` + import `23/38`）· `CreateTableInfo`（HMS 臂 `387-388`/`910-911` + import `52`；`validateIcebergRowLineageColumns(int):1138-1156` + import `53` **留到 Batch 3 随 IcebergMetadataOps 删**）· `PhysicalPlanTranslator`（scan 死臂 `830-859`/`933-948` + sink 访问器 `563-569` + import `62-66/138/198` + 死 `DirectoryLister` 面 `70-72/280` + **DistributionSpecHiveTableSink* 面 `3363-3374`+import `80/81`**）· `InsertIntoTableCommand`（`506-519` + import `38/73`）· `InsertOverwriteTableCommand`（`320`/`396-409`/`473-475` + import `33/43`）· `InsertUtils`（`289-294`/`605-606` + import `33/42`）· `LogicalFileScan`（`242-263` + import `26/42/43/44`）· `AnalyzeTableCommand`（`319` + import `36`）· `ShowCreateDatabaseCommand`（`86-94` + import `32`）· `ShowCreateTableCommand`（`156-160` + import `37/38`）· `ShowPartitionsCommand`（`154-157`/`202`/`222-224`/`361-405`/`426-427` + import `49`）· `UnboundTableSinkCreator`（`60-61`/`93-94`/`128-129` + import `26`）· **访问器默认方法**：`SinkVisitor`（visitUnbound/Logical/PhysicalHiveTableSink）· `RelationVisitor`（visitLogical/PhysicalHudiScan）· 具体 override `ShuffleKeyPruner`/`RequestPropertyDeriver`/`StatsCalculator`/`TurnOffPageCacheForInsertIntoSelect`· `AggregateStrategies:750`/`LogicalFileScanToPhysicalFileScan:34` 死三元/守卫复原 · `RuleSet`（去两死 impl-rule 注册 `74/75/201/227/248/273`）。
- **2d tablefunction**：`MetadataGenerator`（`469-472`/`1325-1348`/`2100-2126` + import `72/73`）· `PartitionsTableValuedFunction`（`172`/`191-199` + import `33/34`）· `PartitionValuesTableValuedFunction`（`117`/`138-143` + import `31/32`）。
- **已由委派处理（无需再动）**：`BindExpression`/`IcebergMergeCommand`/`IcebergUpdateCommand`（group-4 已改读 `reservedPassthrough`，无 IcebergUtils 引用）；`FilePartitionUtils`（group-3 已改）。
- **验证**：每块 test-compile 绿 + checkstyle 0（`UnusedImports` 必清）。

### Batch 3 — 原子删除死簇（Phase 4-5）
> 此时 live 引用已零，删除集 + 死兄弟 + 连锁 impl-rule/访问器默认方法 + 引用它们的测试**自成闭环**，整簇一次删（可拆 3a/3b 若能找到干净子簇，否则单 commit，仿 paimon/iceberg 删除范式）。

- **3a 主簇（三目录 + trap-tier + infra + 抽取本体）**：`datasource/hive`（29，含 `HiveUtil`/`HiveExternalMetaCache`）+ `hive/event`(21) + `hive/source`(2) + `datasource/hudi`(15) + `datasource/iceberg`(23，含 `IcebergUtils`) + `datasource/infra`(6) + trap-tier（`HiveInsertExecutor`/`Logical|PhysicalHiveTableSink`/`UnboundHiveTableSink`/`Logical|PhysicalHudiScan`/`HiveTableSink`/`HMSAnalysisTask`/`HiveTransactionManager`/`TransactionManagerFactory`）+ **连锁 impl-rule**（`LogicalHiveTableSinkToPhysicalHiveTableSink`/`LogicalHudiScanToPhysicalHudiScan`）+ `CreateTableInfo.validateIcebergRowLineageColumns(int)` 方法删 + 访问器默认方法删（`SinkVisitor`/`RelationVisitor`）+ 具体 visitor override 删。
- **3b 测试联动**（同 commit，否则 test-compile 断）：删 `MetastoreEventFactoryTest`/`HiveScanNodeTest`/`HiveTableSinkTest`/`HMSAnalysisTaskTest`/`HudiExternalMetaCacheTest`/`HudiUtilsTest`/`IcebergExternalMetaCacheTest`/`IcebergUtilsTest`/`IcebergCountPushDownTest`/`IcebergMetadataOpTest`/`IcebergSysTableResolverTest`/`HiveUtilTest`/`HMSExternalTableTest`/`TestHMSCachedClient`/`ThriftHMSCachedClientTest`/`HmsCommitTest`/…（完整名单见 recon）+ 删 3 个 `@Disabled`（`HmsCatalogTest`/`HmsQueryCacheTest`/`HiveDDLAndDMLPlanTest`）；改 `OlapInsertExecutorTest`（去 stub `getCurrentHiveTransactionMgr`）等桩测试。
- **可选清理**：`PlanType` 孤儿枚举 `LOGICAL_HIVE_TABLE_SINK`/`PHYSICAL_HUDI_SCAN`/`PHYSICAL_HIVE_TABLE_SINK`（见决策）。
- **验证**：test-compile 绿。

### Batch 4 — 守门（Phase 6）
- fe-core `mvn -pl fe-core -am test-compile` BUILD SUCCESS（含 test 源）· checkstyle 0 · `tools/check-connector-imports.sh` 净 · 回放单测绿（`HmsGsonCompatReplayTest`/`IcebergGsonCompatReplayTest`/`PaimonGsonCompatReplayTest`）· `ExternalMetaCacheRouteResolverTest`（改构造后）。
- 用户自跑：翻闸 hms 全量回归删前后逐位一致。

---

## 4. 本轮新揪出、旧蓝图未列的要点

1. **`HiveVersionUtil`（fe-core）不是死的**：唯一消费者是 fe-core 补丁版 `org/apache/hadoop/hive/metastore/HiveMetaStoreClient.java:357`，经**连接性测试链**（`CatalogConnectivityTestCoordinator`→`HiveHMSConnectivityTester`→`HMSBaseConnectivityTester:47`）活着。而连接性测试器本身在删除集里。→ 删掉测试器后 `HiveMetaStoreClient`（补丁版）失去该消费者。**待定**：P7.5 是否顺带删掉「孤儿化的 fe-core 补丁版 `HiveMetaStoreClient` + `HiveVersionUtil`」，还是保留？（详见 §5 决策）
2. **`HudiExternalMetaCache`/`IcebergExternalMetaCache` 非严格死**：FE 启动时 `ExternalMetaCacheMgr:293-294` 无条件 `new`（虽从不被有效查询）。删前必须先去注册+访问器（已列入 Batch 2a），行为保持。
3. **连锁文件（不在原 106 清单）**：2 个 impl-rule + `RuleSet` 注册；访问器默认方法（`SinkVisitor`/`RelationVisitor`）；具体 override（`ShuffleKeyPruner`/`RequestPropertyDeriver`/`StatsCalculator`/`TurnOffPageCacheForInsertIntoSelect`）；`AggregateStrategies`/`LogicalFileScanToPhysicalFileScan` 死臂；`DistributionSpecHiveTableSink*` 面。均须同批处理否则不编译。
4. **`StatisticsAutoCollector` 引 `org.apache.hudi.common.util.VisibleForTesting`**（hudi jar 注解）——须换非-hudi 源以让 fe-core 与 hudi jar 解耦（与后续 pom 裁剪相关）。
5. **`BaseExternalTableDataSink.getBrokerAddresses:63-76`** 亦随 `HiveTableSink` 死（额外发现，删前确认意图）。
6. **第二处哨兵泄漏**（`TablePartitionValues.HIVE_DEFAULT_PARTITION` ← 活 `MetadataGenerator:2166` `partition_values` TVF）**确认仍活**——**明确不在本轮**，单列后续。

---

## 5. 用户决策（2026-07-13 已拍板）

1. **总批次序** = ✅ 认可，按「Batch1 行为改动 → Batch2 切死臂 → Batch3 原子删死簇 → Batch4 守门」执行。
2. **§4-B** = ✅ 认可安全直切、不加连接器 ACID 位、补一条 ACID 读 e2e。
3. **`HiveVersionUtil` + fe-core 补丁版 `HiveMetaStoreClient`** = ✅ **两个都删**。用户确认 fe-connector-hms 已自带二者副本（`fe-connector-hms/.../HiveMetaStoreClient.java` + `fe-connector-hms/.../hive/HiveVersionUtil.java`），今后统一用连接器侧，fe-core 两份不再使用。fe-core 两份的全部引用者均在删除集内，删完为零引用孤儿，随 Batch 3 删。
4. **`getBrokerAddresses` / `PlanType` 孤儿枚举 / `StatisticsAutoCollector` 注解换源** = 随批清理（未反对，按推荐）。
5. **Batch 3 粒度** = ✅ 一个大删除提交（最终 squash）。
6. **PR 打包** = 拓扑多 commit → 最终 squash（沿 paimon/iceberg 范式）。

---

## 附：核验存档
- 工作流：`.claude/wf-p75-deletion-batch-verify.js`，run `wf_749d6834-e98`（15 agent；§4-B trace + 6 DELETE 组 + 4 EDIT 组 + 4 抽取遗留；ACID 对抗复核 + IcebergUtils 遗留复核为 resume 续跑）。
- 结构化结果：scratchpad `p75-verify-results.json` / `edit-inv.txt`（ephemeral）。
