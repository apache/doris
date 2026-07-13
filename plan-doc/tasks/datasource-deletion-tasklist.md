# ✅ 进度追踪 — 收尾删除 hive 家族遗留代码

> **用途**：本任务的**唯一进度清单**。完成一项即把 `[ ]` 勾成 `[x]`（每步随 commit 更新）。
> **怎么做**看设计文档，别在这里展开：施工蓝图 `P7.5-datasource-deletion-plan.md`；抽取合规方案
> `datasource-deletion-extraction-reanalysis-2026-07-13.md`；上下文 `../HANDOFF.md`。行号信 HEAD。

---

## 阶段 0 — 调研与设计
- [x] 全遗留目录逐文件清点（保留/删除/改）+ 施工蓝图（22-agent recon）
- [x] 揪出散落在计划/统计/事务里的遗留写-扫描链（trap-tier）
- [x] 识别三项删前置缺口（事件拆除 / ACID 分区列 / 测试联动）
- [x] 抽取四组的隔离合规重分析（14-agent recon + 对抗验证 + 人工复核）
- [x] 用户 review 通过 + 主计划拓扑序改写 + 交接更新 + 提交

## 阶段 1 — 删前置缺口（批次方案见 `datasource-deletion-batch-plan-2026-07-13.md`，用户 2026-07-13 review 通过）
- [x] **§4-B ACID 定论 = 安全直切**（15-agent 重核 + 对抗复核：连接器对分区表恒填分区值 + `HiveScanRange.populateRangeParams` 无条件重建 columns-from-path，三/四重独立保险）——**无需**连接器 `isAcid()` 中立位；直接切 `FileQueryScanNode` 死 `instanceof HiveSplit` 臂 + import（1c）。⚠ 欠 ACID 分区读 e2e（用户自跑）。
- [x] 分区有序值 fail-loud（`PluginDrivenMvccExternalTable.toListPartitionItem` 删 `HiveUtil.toPartitionValues` 回退，连接器空值在 try/catch **之外** `checkState` 硬报错；4 连接器已全接线）（1a）。⚠ 欠分区有序值 e2e（含非字符串列真 NULL 分区，用户自跑）。
- [x] 事件管道 legacy 拆除（`Env` 去 `MetastoreEventsProcessor` field/init/getter/`start()`/import；`ExternalMetaIdMgr` 切死 else 臂 127-130；`ExternalMetaIdMgrTest` 改测活路径 `MetastoreEventSyncDriver`）（1b）。
- 注：`Env.hiveTransactionMgr` 全套面 = 阶段 3 切臂（随 hive 死簇），非本批。`HiveVersionUtil` + fe-core 补丁版 `HiveMetaStoreClient` = 阶段 4 删（用户确认 fe-connector-hms 已自带副本，fe-core 两份不再使用）。

## 阶段 2a — 连接器侧委派（各带单测，最先做）
**分区名解析（连接器交已解析的有序值）** — commit `49254f1d429`
- [x] `ConnectorPartitionInfo` 加 `orderedPartitionValues` 字段 + getter + 构造重载（空默认，改 equals/hashCode/toString）+ 单测 9/9
- [x] hive 连接器填有序值（`HiveConnectorMetadata` 复用 `HiveWriteUtils.toPartitionValues`）
- [x] paimon 连接器填有序值（render 循环内收集，`install` 验证过）
- [x] iceberg 连接器填有序值（`raw.values`，`String.valueOf` 保 "null" 字节一致）
- [x] hudi 连接器填有序值（`partKeyNames` render 序，render/parse 互逆）

**hive 默认分区哨兵（查询路径经现有 SPI 委派）** — commit `feddf050190`（连接器+测试）+ `c05bb01798e`（fe-core）
- [x] `HiveScanRange.populateRangeParams` 加 `columnsFromPath{,Keys,IsNull}` 重置（镜像 `IcebergScanRange`，**窄** `HIVE_DEFAULT_PARTITION.equals`，非 `normalize()`）+ 单测 5/5 绿

**iceberg 行血缘（逐列中立标记 + 建表校验下沉）** — commit `1ca679e0820`(基建位) / `9fa915b290b`(连接器)
- [x] `ConnectorColumn` 加 `reservedPassthrough` 位 + `ConnectorColumnConverter` 跨界重贴进 `Column`（`Column` 加**非持久**字段=无 `@SerializedName`，仿 `isCompoundKey`；拷贝构造补行；不进 equals/hashCode）
- [x] iceberg 连接器给 v3 行血缘列设 `reservedPassthrough`（`IcebergConnectorMetadata:410-413` 链式 `.reservedPassthrough()`）
- [x] `IcebergConnectorMetadata.createTable` 吸收 v3 保留名拒绝（新 `IcebergSchemaBuilder.getEffectiveFormatVersion` 全优先级）+ 单测（请求级/目录 default/override + below-v3 allow）

## 阶段 2b — fe-core 消费者改委派（连接器侧全绿后）
- [x] `toListPartitionItem` 改 zip 连接器有序值（先带回退、暂不 fail-loud）— commit `49254f1d429`（fail-loud 留到删 `HiveUtil` 时）
- [x] `FilePartitionUtils` 三处改（import 换中立常量 / 加载路径换常量 / 查询路径删哨兵项只留 `value==null`）— commit `c05bb01798e`（fe-core test-compile 绿 0 checkstyle）
- [x] `BindExpression.isIcebergMergeMetaColumn` 改读 `reservedPassthrough`（从 `sink.getCols()` 派生保留名集合、大小写不敏感）— commit `3364966cdd4`
- [x] `CreateTableInfo` 删 iceberg v3 **engine gate** + 死私有 helper（校验已下沉连接器）— commit `3364966cdd4`。⚠ 公有 `validateIcebergRowLineageColumns(int)` **保留**（仅被**死** `IcebergMetadataOps:358` 引用，随其删除阶段一起删；同 IcebergUtils 成员的处置）
- [x] `IcebergMergeCommand` 7 处改（5 处 predicate→`isReservedPassthrough()`；342-343 名产出→按 schema 序遍历保留列 `getName()`）— commit `3364966cdd4`
- [x] `IcebergUpdateCommand:106` 改读保留标记 — commit `3364966cdd4`

## 阶段 3 — 切死臂（= Batch 2「切断死分支，不删类」；每块后 test-compile 必绿）
> **⚠ Batch-2/Batch-3 边界铁律（本 session 核出）**：只在 Batch 2 切「活方法里的死分支」（`instanceof HMSExternal*`/`case HMS_EXTERNAL_TABLE`/`DLAType` 死臂），切完顺清**仅**被该臂用的 import/local/私有方法。**不得**在 Batch 2 删「使用者含仍存在的待删类」的声明——如 `ExternalMetaCacheMgr.hive()/hudi()/iceberg()` 访问器+注册、`Env.hiveTransactionMgr` field/getter、`BaseExternalTableDataSink.getTFileFormatType/getTFileCompressType/supportedFileFormatTypes`、`StatisticsUtil.getHiveRowCount/getTotalSizeFromHMS`、`CreateTableInfo.validateIcebergRowLineageColumns(int)`、`PhysicalPlanTranslator` 的 sink-visitor 方法、`SinkVisitor`/`RelationVisitor` 默认方法——它们的使用者是待删类（如死 `HiveScanNode`/`HiveTableSink`）或 Batch 3 才删的类，**须随 Batch 3 与被删类原子一起移除**，否则 Batch 2 删会编译断。
- [x] datasource/ 内自包含死臂（`CatalogMgr` 两分区臂 / `ExternalMetaCacheRouteResolver` HMS 路由 / `CatalogConnectivityTestCoordinator` Hive/Glue 臂 / `RefreshManager` 两臂）— commit `1e75d5023e5`
- [x] `FileQueryScanNode` HiveSplit 臂 / `ExternalMetaIdMgr` 死 else 臂 / `Env` `MetastoreEventsProcessor` 面 — 已在阶段 1（Batch 1）完成
- [ ] **nereids 死臂（Batch 2 剩余，本 session 未做）**：`PlanNode`（两 `instanceof IcebergScanNode` 臂 + 死私有 `mergeIcebergAccessPathsWithId`，其唯一调用方=该两臂，可 Batch2 一并删）· `BindRelation`（`case HMS_EXTERNAL_TABLE`）· `BindSink`（hive-sink rule + `bindHiveTableSink` + 死 `bind` 重载）· `CheckPolicy`（hudi 增量臂）· `CreateTableInfo`（两 HMS 臂 387/910；`validateIcebergRowLineageColumns(int)` 留 Batch3）· `PhysicalPlanTranslator`（scan 死臂 830-859/933-948；sink-visitor 方法留 Batch3）· `InsertIntoTableCommand`/`InsertOverwriteTableCommand`/`InsertUtils`（HMS/UnboundHiveTableSink 臂）· `LogicalFileScan`· `AnalyzeTableCommand`· `ShowCreateDatabaseCommand`/`ShowCreateTableCommand`/`ShowPartitionsCommand`· `UnboundTableSinkCreator`· `MaterializeProbeVisitor`
- [ ] **statistics/tvf 死臂（Batch 2 剩余）**：`AnalysisManager`（1492→`return false`）· `StatisticsUtil`（仅切 instanceof 臂 1007-1012；死方法留 Batch3）· `StatisticsAutoCollector`（hudi-jar `VisibleForTesting` 换源，独立小改）· `MetadataGenerator`（三处 HMS 臂 469/1325/2100，死方法 dealHMSCatalog/forHmsTable 留 Batch3）· `PartitionsTableValuedFunction`· `PartitionValuesTableValuedFunction`
- [ ] 每块切完清 `UnusedImports`（checkstyle 会 fail build）+ test-compile 绿
> 精确 file:line 见 `datasource-deletion-batch-plan-2026-07-13.md §3 Batch2`（行号 = HEAD `0f45c482e58`，上列剩余文件未被 Batch1/2a 触及故仍有效；动手前仍 grep 复核）。

## 阶段 4 — 删 trap-tier + 循环单元
- [ ] trap-tier 文件（`HiveInsertExecutor`/`LogicalHiveTableSink`/`PhysicalHiveTableSink`/`UnboundHiveTableSink`/`LogicalHudiScan`/`PhysicalHudiScan`/`HiveTableSink`/`HMSAnalysisTask`/`HiveTransactionManager`/`TransactionManagerFactory`）
- [ ] hive 循环单元（顶层 29 + `event/` 21 + `source/` 2）含 hive LZO 三方法纯删
- [ ] hudi 循环单元（15）
- [ ] iceberg 循环单元（22 + `IcebergUtils`）
- [ ] `infra/`（6）

## 阶段 5 — 测试源联动
- [ ] 删 3 个 `@Disabled`（`HmsCatalogTest`/`HmsQueryCacheTest`/`HiveDDLAndDMLPlanTest`）+ `MetastoreEventFactoryTest`
- [ ] 改 `ExternalMetaIdMgrTest` / `TestHMSCachedClient` + 其余引用删除单元的测试逐个处置

## 阶段 6 — 守门
- [ ] fe-core `test-compile` BUILD SUCCESS（含 test 源）+ checkstyle 0
- [ ] import-gate 净（`tools/check-connector-imports.sh`）
- [ ] 靶向回放单测保绿（`HmsGsonCompatReplayTest`/`Iceberg`/`PaimonGsonCompatReplayTest` 等）
- [ ] ACID 分区列 e2e（若判定需连接器改）+ 分区有序值委派 e2e（paimon+iceberg+hive+hudi，含非字符串列真 NULL 分区）
- [ ] 用户自跑：翻闸 hms 全量回归删前后逐位一致

## 后续独立项（不并入本轮）
- [ ] 第二处哨兵泄漏 `TablePartitionValues.HIVE_DEFAULT_PARTITION` ← `MetadataGenerator:2166`（`partition_values` TVF）单列处理
- [ ] iceberg AWS 属性簇 + maven 依赖（`iceberg-aws`/`s3tables` 等）pom 裁剪（排在 iceberg 文件删除之后）
- [ ] jdbc `client|util` 的 streaming/CDC 子系统独立迁移
