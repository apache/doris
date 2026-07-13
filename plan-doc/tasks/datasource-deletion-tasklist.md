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

## 阶段 1 — 删前置缺口
- [ ] 迁移-hive ACID 分区列的连接器中立信号（`ConnectorScanRange` 加默认 false 的 `isAcid()` 位；`FileQueryScanNode` 死 `instanceof HiveSplit` 臂改读它）
- [ ] 事件管道 legacy 拆除的前置编辑（`Env` 去 `MetastoreEventsProcessor` 全套面 + `ExternalMetaIdMgr` 切死 else 臂）— 与阶段 3 切臂合并亦可

## 阶段 2a — 连接器侧委派（各带单测，最先做）
**分区名解析（连接器交已解析的有序值）** — commit `49254f1d429`
- [x] `ConnectorPartitionInfo` 加 `orderedPartitionValues` 字段 + getter + 构造重载（空默认，改 equals/hashCode/toString）+ 单测 9/9
- [x] hive 连接器填有序值（`HiveConnectorMetadata` 复用 `HiveWriteUtils.toPartitionValues`）
- [x] paimon 连接器填有序值（render 循环内收集，`install` 验证过）
- [x] iceberg 连接器填有序值（`raw.values`，`String.valueOf` 保 "null" 字节一致）
- [x] hudi 连接器填有序值（`partKeyNames` render 序，render/parse 互逆）

**hive 默认分区哨兵（查询路径经现有 SPI 委派）** — commit `feddf050190`（连接器+测试）+ `c05bb01798e`（fe-core）
- [x] `HiveScanRange.populateRangeParams` 加 `columnsFromPath{,Keys,IsNull}` 重置（镜像 `IcebergScanRange`，**窄** `HIVE_DEFAULT_PARTITION.equals`，非 `normalize()`）+ 单测 5/5 绿

**iceberg 行血缘（逐列中立标记 + 建表校验下沉）**
- [ ] `ConnectorColumn` 加 `reservedPassthrough` 位 + `ConnectorColumnConverter` 跨界重贴进 `Column`
- [ ] iceberg 连接器给 v3 行血缘列设 `reservedPassthrough`
- [ ] `IcebergConnectorMetadata.createTable` 吸收 v3 格式版本解析 + 保留名冲突拒绝 + 单测

## 阶段 2b — fe-core 消费者改委派（连接器侧全绿后）
- [x] `toListPartitionItem` 改 zip 连接器有序值（先带回退、暂不 fail-loud）— commit `49254f1d429`（fail-loud 留到删 `HiveUtil` 时）
- [x] `FilePartitionUtils` 三处改（import 换中立常量 / 加载路径换常量 / 查询路径删哨兵项只留 `value==null`）— commit `c05bb01798e`（fe-core test-compile 绿 0 checkstyle）
- [ ] `BindExpression.isIcebergMergeMetaColumn` 改读 `reservedPassthrough`（大小写不敏感）
- [ ] `CreateTableInfo` 删 iceberg v3 校验方法 + engine gate（校验已下沉连接器）
- [ ] `IcebergMergeCommand` 7 处改读保留标记 + 按标记枚举列名（176/201/260/336/342/343/369）
- [ ] `IcebergUpdateCommand:106` 改读保留标记

## 阶段 3 — 切死臂 + 清死面
- [ ] `datasource/` 内 6 处死臂（`CatalogMgr`/`ExternalMetaCacheMgr`/`ExternalMetaCacheRouteResolver`/`CatalogConnectivityTestCoordinator`/`FileQueryScanNode` HiveSplit 臂/`ExternalMetaIdMgr`）
- [ ] `Env` 两套面（`MetastoreEventsProcessor` + `hiveTransactionMgr`）
- [ ] `datasource/` 外死臂（`RefreshManager`/`PlanNode`/`BaseExternalTableDataSink`/`PhysicalPlanTranslator`/`BindRelation`/`BindSink`/`CheckPolicy`/`CreateTableInfo`/`Insert*`/`Show*`/统计族/`MetadataGenerator`+TVF）
- [ ] 切完清 `UnusedImports`（checkstyle 会 fail build）

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
