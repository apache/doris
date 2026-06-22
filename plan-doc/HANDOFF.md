# 🤝 Session Handoff

> 滚动文档：每次 session 结束**直接覆盖**（不保留历史；历史见 `git log plan-doc/HANDOFF.md`）。协作规范：[AGENT-PLAYBOOK.md](./AGENT-PLAYBOOK.md)
> **范围**：本文件 = catalog-spi **主线** handoff。metastore/storage 抽取子线**已彻底 CLOSED**（2026-06-22 收官，全部合入主线 #64446/#64653/#64655）——
> [`metastore-storage-refactor/`](./metastore-storage-refactor/) 文档仅作历史留存、**后续勿读**；需了解 metastore-spi 现状请直接读代码 `fe/fe-connector/fe-connector-metastore-spi/`。

---

# 🎯 下一个 session 的任务 — **P6.2-T07（MVCC / time-travel）**（**P6.2-T06 field-id 字典已绿，见下「✅ P6.2-T06 = DONE」；T01–T06 / P6.1〔T01–T10〕全绿**）

> **T07 scope**：MVCC time-travel——`IcebergConnectorMetadata.{resolveTimeTravel(5 kinds: SNAPSHOT_ID/TIMESTAMP/TAG/BRANCH/INCREMENTAL),applySnapshot,getTableSchema(@snapshot),beginQuerySnapshot}` + `IcebergTableHandle` scan-option 键（自包含移植 legacy `getSpecifiedSnapshot`/`IcebergTableQueryInfo`）+ timestamp TZ aliases（复用 T02 alias map）+ 接线 `PluginDrivenMvccExternalTable`/`applyMvccSnapshotPin`（通用已就绪）。**起步先读** legacy `IcebergScanNode.getSpecifiedSnapshot`(:1059) + paimon MVCC 模板（`PaimonConnectorMetadata.{resolveTimeTravel,applySnapshot,beginQuerySnapshot}` + `PaimonMvccSnapshot`）+ recon §MVCC 行。
>
> **⚠️ T07 须一并解决 T06 留下的 fail-loud 竞态（对抗复核 `wf_7109cc62-b6e` 确认,UT 不可见,T06 惰性）**：`buildColumnHandles`(`PluginDrivenScanNode:1048`) 在 `pinMvccSnapshot`(:1059) **之前**跑、`getScanNodeProperties`(:1063) 在 pin **之后**跑——time-travel 钉旧 snapshot 时,请求列可能在该 snapshot 已不存在 → `IcebergSchemaUtils.buildCurrentSchema` fail-loud 抛。**修法（T07 选一）**：①字典改从 `IcebergColumnHandle` 已携带的 field-id 构建（稳定的 Doris-slot id = legacy `Column.uniqueId` parity,不再按名 re-lookup);②`getColumnHandles` 在钉后的 handle 解析。详见设计文档 §7「No paimon-style latest() fallback」条。
>
> **🔴🔴 T06 暴露的 P6.6 翻闸阻塞项（用户签字本轮不修,跨 paimon,须翻闸前 holistic 修）**：**GLOBAL_ROWID（top-N 延迟物化合成列）在 SPI 路径被通用 `PluginDrivenScanNode` 归类为 REGULAR（非 SYNTHESIZED）→ T06 字典让 BE 走 field-id 路径 → 该列不在字典 → `iceberg_reader.cpp:181` `children_column_exists` DCHECK → 整 BE 崩**。修在共享 fe-core `PluginDrivenScanNode.classifyColumn`(GLOBAL_ROWID→SYNTHESIZED),但 `paimon_reader.cpp` 无 SYNTHESIZED-GLOBAL_ROWID 处理器 → 盲改可能破 paimon top-N,**须 paimon 影响分析 + 可能 BE 协同**。详见设计文档 §6。**P6.6 翻闸前必修,否则 iceberg top-N 查询挂 BE。**

> **工作分支 = `catalog-spi-10-iceberg`**（off `branch-catalog-spi` @ `e5959e1b53d`，PR base = `branch-catalog-spi`，squash 合并）。**翻闸全有或全无，P6.1–P6.5 切忌动 `SPI_READY_TYPES`**（翻闸只在 P6.6）。
>
> **✅ P6.1 = DONE（本 session 2026-06-22 收口 T10）**：P6.1 task 表 T01–T10 全绿（见 `P6-iceberg-migration.md:143-154`）。T10 经 redefine 吸收了「metastore 模块拆分（Phase A，行为不变）+ iceberg per-flavor 校验（Phase B，§4 逐字）」——validateProperties 接线只是 Phase B 的尾巴。**A-gate + B-gate 全绿；对抗 parity 复核 4 MATCH + 1 nit。**
> **下一步 = P6.2 实现（从 P6.2-T01 起）——不是 P6.6！** P6.6 翻闸是「全有或全无」（`CatalogFactory:104-113`），**须等 P6.1–P6.5 全部实现完**（scan/write/procedure/sys-table 都还没做）；现在翻闸会让所有 iceberg 查询走只有读元数据+校验的连接器→scan/write 全断。

---

# ✅ P6.2 recon = DONE（2026-06-22 本 session；recon-only，**未改一行产品代码**，仅文档）

> 本 session = recon session（playbook §7.3）：7 路并行结构化 recon（workflow `wf_a74302c7-194`）+ 主线直读 paimon vended 链/`ConnectorContext`/`ConnectorDeleteFile`/`ConnectorMetaInvalidator`。产出 = recon 笔记 + P6.2 逐 task 拆解 + 4 决策签字。**下一轮起实现（P6.2-T01）。**

- **recon 笔记**：[`research/p6.2-iceberg-scan-recon.md`](./research/p6.2-iceberg-scan-recon.md)（scan/MVCC/cache/vended/field-id old→new 映射 + 风险登记 + paimon 模板锚点）。
- **逐 task 拆解**：`tasks/P6-iceberg-migration.md` 末「P6.2 逐 task 拆解」（P6.2-T01..T11）。
- **🔑 用户裁定（4 项，全签字 2026-06-22）**：
  1. **D6 cache = 全连接器内部**（镜像 `PaimonLatestSnapshotCache` + schema-at-snapshot memo + manifest cache loader/value 也搬进连接器，path-keyed）。
  2. **field-id = 字符串属性**（`getScanNodeProperties` 用 iceberg Schema 构 `history_schema_info`，**不改 `ConnectorColumn`**；镜像 paimon `FIX-SCHEMA-EVOLUTION`）。
  3. **O2 vended = 复用既有 `ConnectorContext` 接缝，E6 取消**——code-grounded 证 paimon **无独立 `ConnectorCredentials` SPI**，用 `context.vendStorageCredentials(rawToken)` + `normalizeStorageUri(uri,token)`（`DefaultConnectorContext` 实现，引擎中立）。iceberg token 同类（raw cloud props）直接复用；连接器只写 iceberg SDK `extractVendedToken(table)`；仅 REST，DLF 凭据走 HiveConf（T07/T10 已接）。详见 recon §5。
  4. **节奏 = 本轮收尾 recon，下轮起实现**。
- **🔑 关键结论：P6.2 净 0 个新 SPI 接口、0 处 SPI 破坏**——scan（`ConnectorScanPlanProvider`）/MVCC（`ConnectorMvccSnapshot`/`PluginDrivenMvccExternalTable`）接缝就绪；delete equality 元数据编码进既有 `ConnectorDeleteFile.properties`；field-id/vended 走既有接缝；cache 失效用既有 `ConnectorMetaInvalidator`/`Connector.invalidate*`（默认方法已存在，paimon override 过）。
- **🔴 最高危**（UT 不可见，P6.6 docker 验）：**field-id 丢失**（schema 演化 BE SIGSEGV/DCHECK，CI #969249 类）+ **分区列双填**（必发 `path_partition_keys`，CI #968880 类）。
- **下一步（实现起点）**：P6.2-T01 = `IcebergScanPlanProvider` 骨架 + `IcebergScanRange` + 接线 + `ignorePartitionPruneShortCircuit()=true` + 测试基建扩。镜像 `PaimonScanPlanProvider`(1589)/`PaimonScanRange`(383)。**起步先**读 recon 笔记 + migration P6.2 块 + paimon 真实代码（大文件 subagent 总结）。

---

# ✅ P6.2-T01 = DONE（2026-06-22 本 commit，未 push）— scan provider 骨架（镜像 paimon）

> 方法：先 4 路并行 workflow（`wf_94a2e297-9b7`）抽 paimon 模板骨架形状（provider 构造器/字段、`PaimonScanRange` Builder/carrier、`PaimonConnector.getScanPlanProvider` 接线、测试 harness），再 TDD（RED=`cannot find symbol`→GREEN）。**全绿**：fe-connector-iceberg UT **117/0/0**（1 skip=env-gated `IcebergLiveConnectivityTest`；111→117 = +6）、checkstyle 0、import-gate 净、iceberg 仍**不在** `SPI_READY_TYPES`（零行为变更）、**无 pom 改**。

- **新建 `IcebergScanRange`（implements `ConnectorScanRange`）**：最小 `FILE_SCAN` carrier = path/start/length/fileSize/fileFormat + `Builder`（`new IcebergScanRange.Builder()`，镜像 paimon）。getters + `getTableFormatType()="iceberg"`（= `TableFormatType.ICEBERG` 值，BE 据此选 reader）+ `getProperties()=emptyMap`。**延迟到 T02..T09**：delete-file / JNI-split / schema-id / 分区 columns-from-path / COUNT / 类型化 `populateRangeParams`（现用 SPI 默认）。
- **新建 `IcebergScanPlanProvider`（implements `ConnectorScanPlanProvider`）**：字段 `properties`/`catalogOps`(`IcebergCatalogOps` seam)/`context`(nullable)；2-arg + 3-arg 构造器（镜像 paimon，wiring 稳定）。override `ignorePartitionPruneShortCircuit()=true`（iceberg 谓词驱动，不消费 `requiredPartitions`，genuine prune-to-zero 须 scan-all）。`planScan(4-arg)` 骨架 = `resolveTable(handle)`（经 seam，context 在场则包 `executeAuthenticated`，镜像 `IcebergConnectorMetadata`/paimon `resolveTable`）→ 返回 `Collections.emptyList()`。其余方法全走 SPI 默认。
- **`IcebergConnector.getScanPlanProvider()` 接线**：每调 new 一个 provider（镜像 `PaimonConnector`），用 **1-arg** `CatalogBackedIcebergCatalogOps(getOrCreateCatalog())`（scan 只 `loadTable`，listing-gating flags 无关）+ `context`。
- **测试**（无 Mockito，fail-loud fake）：`IcebergScanRangeTest`(2，builder/默认 contract)、`IcebergScanPlanProviderTest`(4：getScanRangeType==FILE_SCAN / ignorePartitionPruneShortCircuit==true / planScan 经 seam 解析表返回空 / planScan 在 auth context 内〔`authCount==1`〕)。复用既有 `RecordingIcebergCatalogOps`/`RecordingConnectorContext`/`FakeIcebergTable`；planScan 测试传 `null` session（骨架不读 session，T02 接）。
- **未直接单测**：`IcebergConnector.getScanPlanProvider()` 接线（需 `getOrCreateCatalog()` live catalog，同 paimon 不单测其 `getScanPlanProvider`）→ P6.6 e2e 验。
- **下一步 = P6.2-T02**：谓词下推（自包含移植 `convertToIcebergExpr`，不 import fe-core）+ `createTableScan`（filter add 顺序保真）+ `planFileScanTask` split 枚举（targetSplitSize/batch 阈值）。manifest-cache 集成留 T08。

---

# ✅ P6.2-T02 = DONE（2026-06-22 本 commit，未 push）— 谓词下推 + createTableScan + split 枚举

> 方法：6-reader recon workflow（`wf_a49c72b0-fb9`）抽 legacy `IcebergScanNode`/`IcebergUtils.convertToIcebergExpr`/paimon 模板/SPI 形态 → TDD → 3-reviewer 对抗 parity workflow（`wf_19375f44-74a`，每发现独立 skeptic verify）。设计文档 = `designs/P6-T02-iceberg-scan-pushdown-design.md`。**全绿**：fe-connector-iceberg UT **138/0/0**（1 skip=env-gated live；117→138 = +15 converter +6 scan/tz）、checkstyle 0、import-gate 净、iceberg 仍**不在** `SPI_READY_TYPES`、**无 pom 改**。

- **新建 `IcebergPredicateConverter`**（自包含，仅 import `connector.api.pushdown` + `org.apache.iceberg`）= 忠实移植 legacy `convertToIcebergExpr`：输入 `ConnectorExpression`（fe-core `ExprToConnectorExpressionConverter` 产）→ iceberg `Expression`。handled set 镜像 legacy（AND keep-arm / OR·NOT all-or-nothing / Comparison 7 ops〔NE→not(equal)、EQ_FOR_NULL+null→isNull〕/ IN·NOT-IN / bare bool→alwaysTrue·False），`extractIcebergLiteral` 逐字移植 `extractDorisLiteral` 类型矩阵（Java 值 × iceberg type，int32·vs·int64 / float·vs·double 经 `ConnectorType.getTypeName()` 区分，timestamp micros + `shouldAdjustToUTC`），`checkConversion` bind-test **折进递归**（关键：nested `(a AND b_bad) OR c` 须先降级再建 OR，否则带 unbindable leaf 进 OR=plan 崩）。metadata-col block + caseInsensitiveFindField。**测试 = 9×13 legacy 网格 oracle 逐 cell 复刻**（vs fe-core `IcebergPredicateTest`）。
- **`IcebergScanPlanProvider.planScan` 实装**：predicate→`table.newScan()`+per-conjunct `scan.filter`（镜像 createTableScan；**丢** fe-core-only `metricsReporter`〔profile drop〕+ `planWith`〔用 SDK 默认 worker pool〕）→ `splitFiles`（iceberg SDK `TableScanUtil.splitFiles`，**非** fe-core `FileSplitter`）→ 每 `FileScanTask` 出最小 `IcebergScanRange`(path/start/length/fileSize)。`determineTargetFileSplitSize` 移植 legacy（session 变量 `file_split_size`/`max_initial_file_split_size`/`max_file_split_size`/`max_initial_file_split_num`/`max_file_split_num`，键+默认与 SessionVariable·paimon 一致，经 `getSessionProperties()` 读；`ScanTaskUtil.contentSizeInBytes` 1.10.1 缺→用 `DataFile.fileSizeInBytes()`，data-file 等价）。
- **scan 测试用真 in-memory iceberg 表**（`InMemoryCatalog` + append `DataFile` 元数据，无 Parquet IO，全离线）：枚举计数 / 96MB split-tiling（contiguous 覆盖）/ 分区裁剪（`WHERE p=1` 排除 p=2 文件）/ unpushable→scan-all / 空表→0 range / seam·auth（T01 契约保留，改用真空表）。**`FakeIcebergTable` newScan 抛**故 scan 测试改 InMemoryCatalog。
- **🔴 对抗复核抓到 1 真 bug（UT 不可见，已修+测）**：`resolveSessionZone` 用裸 `ZoneId.of(tz)`，但 legacy 经 Doris `TimeUtils.timeZoneAliasMap` 解析（`CST`/`PRC`→`Asia/Shanghai`、`UTC`/`GMT`→`UTC`、+`ZoneId.SHORT_IDS`）；Doris session `time_zone` 不规范化存（`SET time_zone='CST'` 留 "CST"）→ 裸 `ZoneId.of("CST")` 抛→UTC 回退→timestamptz 字面量下推偏移 8h→**错裁文件/错结果**。修=连接器内自包含复刻 alias map + `ZoneId.of(tz, aliasMap)`。UT 不可见因网格无 `withZone()` 列；现补 `resolveSessionZoneHonorsDorisTimezoneAliases` + `timestamptzLiteralUsesSessionZone`。**其余复核全 MATCH / 文档化 deviation，0 refuted。**
- **deviation（UT 不可见，P6.6 docker 验，登记设计文档）**：profile/`planWith` drop；reversed `literal OP col`/col-col drop（legacy 反序有潜伏 bug，Nereids 已规范化故不可达，drop=安全过近似）；`ConnectorIsNull`/`Like`/`Between` drop（legacy `convertToIcebergExpr` 无此 case，IS NULL 仍经 EQ_FOR_NULL+null 下推）；LARGEINT 经 String 路；decimal→STRING·datetime→STRING 字面量串形 best-effort；batch mode 延后。
- **下一步 = P6.2-T03**（见顶部任务头）：typed range params + `populateRangeParams` + native·vs·JNI + `path_partition_keys` 必发。

---

# ✅ P6.2-T03 = DONE（2026-06-22 本 commit，未 push）— typed range-params + populateRangeParams + native fileFormat + path_partition_keys

> 方法：4 路并行 recon（legacy `setIcebergParams`/`createIcebergSplit`/paimon 模板/`ConnectorScanRange`+`PluginDrivenScanNode` SPI/thrift）→ 设计文档 `designs/P6-T03-iceberg-scan-rangeparams-design.md` → TDD（每件 RED→GREEN）→ 5-dim 对抗 parity workflow（`wf_cb4d65f0-775`，每发现独立 skeptic verify）。**全绿**：fe-connector-iceberg UT **164/0/0**（1 skip=env-gated live；138→164）、fe-core PluginDriven* **53/0/0**（无回归）+ 新 `PluginDrivenSplitPartitionValuesTest` 4/0、checkstyle 0、import-gate 净、iceberg 仍**不在** `SPI_READY_TYPES`、plugin-zip 无 fe-thrift（provided）。

- **新建 `IcebergPartitionUtils`**（自包含端口,仅 iceberg+guava+jackson,零 fe-core import）= legacy `IcebergUtils` 5 个分区助手（`getIdentityPartitionColumns`/`getIdentityPartitionInfoMap`/`getPartitionValues`/`getPartitionDataJson`/`serializePartitionValue`）。两处有意 delta：①时区参数用解析后的 `ZoneId`（非 legacy 裸 String+`ZoneId.of(tz)`，避非规范 session tz 崩,接 T02 alias 修）；②`partition_data_json` 用 iceberg 自带 Jackson `JsonUtil.mapper()`（非 fe-core `GsonUtils`,BE 重解析故值相同）。UT 12。
- **`IcebergScanRange`** 加 typed carriers（`formatVersion`/`partitionSpecId`/`partitionDataJson`/v3 `firstRowId`+`lastUpdatedSequenceNumber`/`partitionValues`）+ `populateRangeParams`→`TIcebergFileDesc`（镜像 `setIcebergParams`：format_version 恒发、original_file_path=raw path、spec_id/data_json 非空才发、v3 才发 row-lineage〔-1 fallback〕、v1 才发 content=DATA、`table_level_row_count=-1`、columns-from-path **unset-then-set** 排序+null→""+isNull,无 sentinel）+ `getPartitionValues`/`isPartitionBearing` override。UT 11。
- **`IcebergScanPlanProvider`** planScan 逐 `FileScanTask` 填 carriers（`buildRange`:formatVersion〔port T09〕/per-file `dataFile.format()`/分区 spec+json+ordered identity map/v3 asym guard）+ **`getScanNodeProperties`**（`path_partition_keys` 小写逗号 #968880 guard,仅分区表;`file_format_type=jni`）。UT 15。
- **pom**：iceberg 连接器加 `fe-thrift`（**provided**,镜像 paimon;构 `TIcebergFileDesc` 需,非传递故直接声明;不入 plugin-zip 实证）。
- **🔴 对抗复核（5-dim workflow）抓 2 真 bug（已修+测,源码逐链核实）+ delete_files 误报证伪**：
  1. **Bug1（low,fail-loud）**：非 orc/parquet 数据文件静默落 `FORMAT_JNI`（legacy `getFileFormatType` 抛 DdlException）→ `buildRange` 加 guard 抛 `IllegalStateException("Unsupported format name: %s for iceberg table.")`。UT avro→throw。
  2. **Bug2（medium,query-crash;用户裁定选项1）**：分区表某文件身份分区值为空（分区演化 transform→identity / 全 binary-identity）→ `getPartitionValues` 空 → 通用 `PluginDrivenSplit.buildPartitionValues` 折叠 empty→null → `FileQueryScanNode` 回退**路径解析** → iceberg 非 `key=value` 布局 → `FilePartitionUtils:152` **抛 UserException**（legacy 永传非空空列表故不抛,逐链核实）。**修=加 1 个非破坏 SPI 默认方法 `ConnectorScanRange.isPartitionBearing()`（默认 false,paimon 字节不变）+ iceberg `partitionSpecId!=null` 返 true + `buildPartitionValues` partition-bearing 空时返非空空列表**。fe-core UT 4 + iceberg UT 1;PluginDriven* 53 无回归。**⚠️ 这是 P6.2「0 新 SPI」的唯一例外（用户签字,为真 query-crash;非破坏默认方法）。**
  3. **delete_files 误报**：skeptic 证「v2+ 未发 delete_files(unset) 对无 delete 表 == 空列表,BE 安全」→ T03 留 unset,T04 实装（设计文档记录）。
- **deviation（UT 不可见,P6.6 docker 验,登记设计文档）**：typed carriers vs paimon string-props;per-file format vs legacy table-uniform（+jni node default per-range override,更正确）;Jackson vs Gson（值同）;ZoneId vs raw String tz;columns-from-path unset-then-set（非 paimon）;Bug1 异常型 `IllegalStateException` vs `DdlException`（连接器不能 import fe-core,消息字节同）。
- **下一步 = P6.2-T04**（见顶部任务头）：merge-on-read delete files。

---

# ✅ P6.2-T04 = DONE（2026-06-22，本 session 2 commit，未 push）— merge-on-read delete files + 数据路径归一化跟修

> 方法：主线 code-grounded recon（直读 legacy `setIcebergParams`/`getDeleteFileFilters`/`IcebergDeleteFileFilter` + 通用 `PluginDrivenScanNode` delete 接缝 + paimon `getDeleteFiles` 模板 + thrift + iceberg SDK delete-file builder API）→ 设计文档 `designs/P6-T04-iceberg-scan-deletefiles-design.md` → TDD → 4-dim 对抗 parity workflow（`wf_d530fdbf-2bf`，每发现独立 skeptic verify）。**全绿**：fe-connector-iceberg UT **177/0/0**（1 skip env-gated live；164→177）、checkstyle 0、import-gate 净、iceberg 仍**不在** `SPI_READY_TYPES`、**无 SPI/fe-core/pom 改**（plugin-zip 无 fe-thrift 不变）。

- **commit 1 `d249a4a9dea`（T04 delete files）**：
  - **`IcebergScanRange`** 加类型化 `Serializable` `DeleteFile` carrier（`positionDelete`(content=1)/`deletionVector`(PUFFIN,content=3,+content_offset/size)/`equalityDelete`(content=2,+field_ids) 3 工厂 + `toThrift()`；position bounds/format/field_ids/offset 仅非空才 set）+ `populateRangeParams` v1(content=DATA)/v2+(**恒 setDeleteFiles，无 delete 也发空 list**，legacy parity)分支。
  - **`IcebergScanPlanProvider`** `buildDeleteFiles(task.deletes())`→`convertDelete`（POSITION vs PUFFIN-DV 按 `format()` 分；bounds 经 `Conversions.fromByteBuffer(DELETE_FILE_POS)` + `-1` sentinel；equality field-ids 从 `DeleteFile.equalityFieldIds()`，**独立于 T06 字典故正确**）+ delete 路径经 `context.normalizeStorageUri` 归一化 + **`getDeleteFiles(TTableFormatFileDesc)` override**（VERBOSE EXPLAIN read-back，含 equality，verbatim port legacy :398-421）。
  - **关键设计选择（设计 §2）**：delete 元数据走**类型化 carrier + `populateRangeParams`**（与 T03 一致、通用节点保持源无关），**非** recon §2 原计划的 `ConnectorDeleteFile.properties`（通用节点不读 `ConnectorScanRange.getDeleteFiles()` 建 thrift，走它会把 iceberg 专码塞进通用节点违反规则）→ **recon §2 delete 行 + `ConnectorDeleteFile`/`ConnectorScanRange.getDeleteFiles()` SPI 默认对 iceberg 现已死，待清**。**0 新 SPI**。
  - **范围外**（不 port）：`deleteFilesByReferencedDataFile`/`Desc` maps（仅 `IcebergRewritableDeletePlanner` 写/rewrite 路用=P6.3+）；COUNT(T05)；field-id 数据字典(T06)；vended 2-arg normalize(T09)。
- **commit 2 `54ecac35e3d`（数据路径归一化跟修，用户裁定「现在顺手修+独立提交」）**：
  - **对抗 workflow 抓到 1 真 gap（medium/high-confidence/0 反驳）**：连接器**主数据文件路径**原样 raw 发 BE（`dataFile.path()`→`getPath()`→`PluginDrivenSplit` 1-arg `LocationPath.of` **不归一化**），而 legacy `createIcebergSplit:852` 用 2-arg `LocationPath.of(path, storagePropertiesMap)` 归一化、paimon 也连接器内归一化(FIX-URI-NORMALIZE)。对象存储(oss/cos/obs/s3a)warehouse 翻闸后 delete 路径→s3:// 但数据路径仍 oss://→BE S3 工厂打不开。**T04 delete 逻辑本身复核字节正确**（这是 T02/T03 数据路径遗留 gap，非 delete）。
  - **修**：`IcebergScanRange` 单一 `path` 拆为**归一化 `path`**（`getPath()`→BE 打开）+ **raw `originalPath`**（`original_file_path`，BE 据它匹配 position-delete，须保持 raw=legacy `setOriginalFilePath:304`）；`buildRange` 用同一 `context.normalizeStorageUri` 接缝归一化数据路径；`originalPath` 未设时回落 `path`（单-arg `.path(...)` 调用方不变）。新测试 `planScanNormalizesDataFilePathButKeepsOriginalFilePathRaw`（oss:// 数据文件→range path s3://、`original_file_path` 仍 oss://）。
- **deviation（UT 不可见，P6.6 docker 验）**：1-arg `normalizeStorageUri`(static map) vs T09 vended 2-arg（同主路径今状）；DV `contentOffset()`/`contentSizeInBytes()` auto-unbox 到 `long`（legacy parity，1.10.1 DV 必有两值）。
- **下一步 = P6.2-T05**（见顶部任务头）：COUNT 下推 + batch mode。

---

# ✅ P6.2-T05 = DONE（2026-06-22，本 session `2cc510608f6` 已 commit，未 push）— COUNT(*) 下推 + batch mode 延后

> 方法：code-grounded recon（直读 legacy `getCountFromSnapshot`/`setIcebergParams` count 分支/`doGetSplits` count 短路/`isBatchMode` + **通用 `PluginDrivenScanNode` 的 FIX-COUNT-PUSHDOWN 既有接缝** + paimon `planScanInternal`/`buildCountRange`/`PaimonScanRange` 模板）→ 用户裁定 batch mode（中文解释+AskUserQuestion）→ 设计文档 `designs/P6-T05-iceberg-scan-countpushdown-design.md` → TDD → 4-dim 对抗 parity workflow（`wf_e0afb564-38b`，每发现独立 skeptic verify）。**全绿**：fe-connector-iceberg UT **185/0/0**（1 skip env-gated；177→185 = +8）、checkstyle 0、import-gate 净、iceberg 仍**不在** `SPI_READY_TYPES`、**无 SPI/fe-core/pom 改**。

- **🔑 关键架构发现（纠正 HANDOFF 计划）**：通用 `PluginDrivenScanNode.getSplits`(:688-715) **已有** COUNT 下推机制（FIX-COUNT-PUSHDOWN）——算 `countPushdown=getPushDownAggNoGroupingOp()==COUNT` 经 SPI **7-参** `planScan` overload 传连接器；`ConnectorScanRange.getPushDownRowCount()`（默认 -1）是既有 SPI 载体，paimon 已实现。故 iceberg **镜像 paimon，非移植 legacy 的多 split 分发**。**净 0 新 SPI。**
- **`IcebergScanRange`**：加类型化 `pushDownRowCount` 载体（默认 -1）+ `getPushDownRowCount()` override；`populateRangeParams` 改发 `setTableLevelRowCount(pushDownRowCount)`（T03 恒发 -1 → 现默认 -1 保所有普通 range 字节不变，仅 count range 带真值）。
- **`IcebergScanPlanProvider`**：加 COUNT-aware 7-参 `planScan` override → `planScanInternal`；抽 `buildScan`；`getCountFromSnapshot(scan,session)`（忠实移植 4 分支 + empty(0)；**读 `scan.snapshot()`** 而非 legacy `getSpecifiedSnapshot()`/`currentSnapshot()`——非 time-travel 等价、MVCC-ready、随 scan 自动跟随）；`ignoreIcebergDanglingDelete`（session 键 `ignore_iceberg_dangling_delete`，默认 false）；`planCountPushdown` **塌缩成单 whole-file count range**（`scan.planFiles()` 非 `splitFiles`，丢 legacy >10000 并行多 split trim=纯性能差异，BE 求和等价）。不可下推（-1）**回落正常 tiled 扫描**。
- **🟡 batch mode = 延后（用户签字 2026-06-22，镜像 paimon）**：legacy 按 **manifest 文件计数**阈值触发；通用节点按 **分区计数**阈值（`supportsBatchScan` gate，默认 false）——轴不匹配，忠实移植需新「连接器自决 isBatchMode」SPI 接缝=违反「0 新 SPI」。iceberg **不 override** `supportsBatchScan` → 分区版 batch mode 对 iceberg 关。batch=扩展性优化非正确性，延后=超大扫描一次性物化（同 paimon/骨架）。
- **🔴 对抗复核（4-dim workflow，每发现 skeptic verify）= 0 正确性发现 + 1 doc-only nit（已修）**：3 核心维度（getCountFromSnapshot / count-range emission / plumbing-fallback）**全 0 发现 = 完全忠实**。1 nit（doc-only，零结果影响）：whole-file 理由误借 paimon 的 `splittable=!applyCountPushdown`（iceberg legacy 无此 flag——legacy `planFileScanTask`→`splitFiles` **会**字节切 count 文件取首 split byte-range）。代码 `scan.planFiles()` 正确（count 下 BE count reader 不读文件、start/length 无关、BE 求和 → 单 whole-file range 等价）；仅理由错→修 Javadoc + 设计 §1/§2（无代码逻辑改）。
- **deviation（UT 不可见，P6.6 docker 验）**：并行多 split trim drop（纯性能，BE 求和等价）；`scan.snapshot()` vs legacy（非 time-travel 等价）；空表 COUNT EXPLAIN 显 `(-1)` vs legacy `(0)`（EXPLAIN-only，BE 结果同为 0）；`summary.get(...)` null-unsafe（legacy parity，真实快照恒带 totals）。
- **测试**（真 InMemoryCatalog，无 Mockito）：`IcebergScanRangeTest` +2（载体默认 -1 / count 发 table_level_row_count）；`IcebergScanPlanProviderTest` +6（塌缩单 range 总数 60 / equality-delete→scan-all / position-delete net-out 带/不带 ignore flag / 空表→空 / countPushdown=false→正常多 range）。
- **下一步 = P6.2-T06**（见顶部任务头）：field-id 数据字典（最高危）。

---

# ✅ P6.2-T06 = DONE（2026-06-22，本 session，未 push）— field-id 数据字典 `history_schema_info`（P6.2 最高危）

> 方法：4-reader recon workflow（`wf_9da2a77c-df8`，legacy/BE/SDK/wiring 结构化）→ 设计文档 `designs/P6-T06-iceberg-scan-fieldid-design.md` → TDD → 4-dim 对抗 parity workflow（`wf_7109cc62-b6e`，每发现独立 skeptic verify）。**全绿**：fe-connector-iceberg UT **205/0/0**（1 skip env-gated；185→205 = +20）、checkstyle 0、import-gate 净、iceberg 仍**不在** `SPI_READY_TYPES`、**无 SPI/fe-core/pom 改**。

- **🔑 关键纠正（legacy-parity,纠 HANDOFF/recon 计划）**：HANDOFF 原写「历史枚举全 schema-id」是 **paimon-shaped,对 iceberg 错**。code-grounded 证：legacy iceberg 发**唯一一条 -1 entry**（`IcebergScanNode.createScanRangeLocations:452-471` 调 `initSchemaInfoFor{All,Pruned}Column(-1L)` **一次**,无 schema-id 循环）；**BE 从 parquet/orc 文件元数据直接读 file field-id**（`iceberg_reader.cpp:149 history_schema_info.front()`,`by_parquet_field_id` 读文件 field_id）按相等匹配单条 table-side -1 entry → iceberg field-id 是永久不变量,**单条足够**（≠ paimon `by_table_field_id` 需 per-schema-id 历史条目）。`TIcebergFileDesc` 无 schema_id。
- **新建 `IcebergSchemaUtils`**（自包含,镜像 `IcebergPartitionUtils` 风格,零 fe-core import）：`extractNameMapping`（port legacy + 递归嵌套,fail-soft）；`buildCurrentSchema`（单 -1 TSchema,**按请求列名 keyed**=CI #969249 修,`caseInsensitiveFindField`,缺列 fail-loud,空→全列回退）；`buildField`（递归 iceberg `Type`→`TField`,**每层都带 id/name/isOptional/name-mapping**=legacy `ExternalUtil` parity,**别于 paimon**〔paimon array/map 元素不带 id〕；scalar→`STRING` 占位符=BE 只用 type.type 当 nested-vs-scalar 判别;ARRAY/MAP/STRUCT 嵌套）；encode/apply（base64 TBinaryProtocol,fail-loud,catch LinkageError）。
- **新建 `IcebergColumnHandle`**（name+fieldId,镜像 `PaimonColumnHandle`,equals/hashCode by name）+ **`IcebergConnectorMetadata.getColumnHandles`**（auth-wrap loadTable,`schema.columns()`→小写`Locale.ROOT` name→handle）——**recon 抓的 wiring gap**：原先缺 getColumnHandles → 通用节点拿不到 pruned 列 → 字典会退化成全列（CI #969249 反模式）。
- **`IcebergScanPlanProvider`**：`getScanNodeProperties` **无条件**发 `iceberg.schema_evolution`（keyed off requested columns,legacy `createScanRangeLocations` 无条件）+ 新 `populateScanLevelParams` override 解码落 `TFileScanRangeParams`。
- **casing 钉死**：top-level 小写 `Locale.ROOT`（match `parseSchema`）；nested as-is（match `IcebergTypeMapping` nested `f.name()`）。**`is_optional` 恒 true**（legacy `ExternalUtil` 用 Doris `isAllowNull()`=parseSchema 强制 true;BE field-id 路径不读 is_optional,惰性,但保 parity)。
- **🔴 对抗复核（4-dim/11-agent workflow）= 5 驳回 + 2 确认**：
  1. **#1 GLOBAL_ROWID（high,确认,用户签字延后 P6.6）= 翻闸阻塞项**——见顶部任务头 🔴🔴 块 + 设计 §6。跨 paimon、须共享 fe-core 修,超 T06 scope。
  2. **#2 fail-loud 竞态（medium,确认,T06 惰性）**——MVCC pin 未接故 buildColumnHandles/getScanNodeProperties 读同一 current handle,fail-loud 不触发;T07 time-travel 须修。见顶部任务头 ⚠️ 块 + 设计 §7。
  3. 驳回 5 项（大小写一致性×3=单一来源 `Locale.ROOT` by-construction;空列回退安全=BE 读 tuple-descriptor 非字典;字段ID缺口误报）。
- **测试**（真 InMemoryCatalog,无 Mockito,断**解码后**字典 vs legacy 期望非类名）：`IcebergSchemaUtilsTest` 15 + `IcebergColumnHandleTest` 2 + metadata getColumnHandles 1 + provider 2。**注意 InMemoryCatalog.createTable 会重排 field-id**,测试从 `table.schema()` 取期望 id 非硬编码。
- **下一步 = P6.2-T07**（见顶部任务头）：MVCC time-travel（一并修 #2 fail-loud 竞态）。

## 🧭 用户裁定（T10 session，按序）
1. **iceberg CREATE-CATALOG 校验做全量 per-flavor**（非最小/非延迟）。无任何回归测试强制（115 套件无一断言建目录属性校验报错；paimon 同），纯取忠实度。
2. **校验逻辑下沉到共享 metastore 层**（不在连接器写 bespoke）。storage 半边**已被 fe-filesystem bind 时校验**（`S3FileSystemProperties.validate()` 等），连接器拿到的 `StorageProperties` 已校验过 → 本任务只做 **metastore 半边**。
3. **把 metastore 重构成 per-engine 模块、镜像 fe-filesystem**（`-spi` 只留扩展点+共享基类，impl 进 per-engine 模块）。理由：impl 塞在 `-spi` 既非严格 SPI 惯例、也与本仓库 fe-filesystem（`-spi` 纯接口、impl 在 `fe-filesystem-s3/-oss/...`）不一致。

## ⛓️ 关键架构洞察（写下来免重推）
- **classpath 隔离消解派发碰撞**：`bindForType("hms"/"rest"/...)` 是 ServiceLoader 首命中、token-only。paimon/iceberg 同 token 会撞。**但**各连接器 plugin-zip 只装本引擎的 metastore 模块 → 运行时 ServiceLoader 只见本引擎 provider → `bindForType(flavor)` 引擎内唯一解。**所以拆模块后无需改派发签名、无需 engine-qualified 参数**。（单测同 classpath 含两套时按引擎 scope 断言。）
- **HMS/DLF 是双引擎共享的**（iceberg `IcebergConnector:175/248` 用 `bindForType("hms"/"dlf")` 拿 `toHiveConfOverrides`/`toDlfCatalogConf` 装 conf）。拆模块后 iceberg classpath 不含 paimon impl → iceberg 须有**自己的** Hms/Dlf。共享的 conf + 连接规则抽进 `-spi` 的**引擎中立基类** `AbstractHmsMetaStoreProperties`/`AbstractDlfMetaStoreProperties`（`toHiveConfOverrides`/`toDlfCatalogConf` + `validateConnection`），paimon/iceberg 各自 extend → **零规则重复，paimon 报错/顺序字节不变**。
- **校验全是 prop-map 驱动**（REST 读 `iceberg.rest.*`、Glue `glue.*`、JDBC、HMS、DLF 全从 props）→ `validateProperties(Map)` 够用，不需 storage/context。
- **JDBC driver_class/url 规则是惰性**（initCatalog）→ 已被连接器现有 `preCreateValidation`+`maybeRegisterJdbcDriver` 覆盖，**不进 validateProperties**（只做 uri/catalog_name/warehouse 三条解析期必填）。
- **逐字 legacy 规则已对抗验证 `complete-and-exact`**（5-agent workflow `wf_8ae4353f-9a8`）→ 全部落在设计文档 **§4**（REST 10 条含急切/延迟触发序、Glue 4 条、JDBC 3 条、HMS 3 条、DLF 3 条 AK/SK/endpoint〔**iceberg 不要求 warehouse、不做 OSS 检查**，有别于共享 DLF impl 的 `requireWarehouse`+带"Paimon"字样的 `requireOssStorage`〕）。**实现时逐字照搬 §4 报错串。**

## ✅ T05 = DONE（2026-06-22，part3 `b0dbe91e1a5` 已 commit，未 push）

> 设计文档 = `plan-doc/tasks/designs/P6-T05-iceberg-catalog-assembly-design.md`。前序：part1 `562201deb9f`（metastore-spi `bindForType`），part2 `6f7b292b5c6`（factory common base + S3FileIO）。
> **T05 全部完成**：5-flavor 装配 + connector LIVE 接线 + DriverShim + 52 parity 测试。**验收全绿**：fe-connector-iceberg UT 75/0/0、checkstyle 0、import-gate 净、iceberg 仍**不在** `SPI_READY_TYPES`。

1. **5-agent code-grounded 复核**（legacy `AbstractIcebergProperties`+5 flavor 实体逐行）→ 精确 key 派生契约。**纠正 2 处过时 HANDOFF**：
   - **GLUE 是自包含的**（非旧 HANDOFF 说的「走 base chooseS3」）：legacy `appendS3Props` **无条件 plain put** 5 个 `s3.*`（空串也发，从 `S3Properties.of(origProps)` 专属 store，**忽略** storagePropertiesList），不走 base `toS3FileIOProperties`（那条才 blank-guard + assume-role）。连接器版从 `chosenS3`（fe-filesystem 单一真源 D-061）无条件发，`glue.*` 凭据从 raw props 读。
   - **HADOOP warehouse FE 不强制**（旧 HANDOFF 说「必填」错）：仅 JDBC 强制 warehouse；HADOOP 靠 HadoopCatalog 下游自己抛——**不要加 warehouse 检查**。
   - REST 凭据类型优先级（核 `IcebergAwsClientCredentialsProperties.getCredentialType`）：**EXPLICIT(AK&SK) > ASSUME_ROLE(roleArn) > PROVIDER_CHAIN**（连接器用 `hasStaticCredentials()`/`hasAssumeRole()` 对齐）。
2. **实现（纯静态，镜像 paimon）**：`IcebergCatalogFactory.{appendRestProperties,appendGlueProperties,appendJdbcProperties,buildCatalogProperties(编排),resolveCatalogName,buildHadoopConfiguration,assembleHiveConf}`；`IcebergConnector.createCatalog` flavor switch（HMS `bindForType`→HiveConf；GLUE conf=null；JDBC driver+catalog_name 位置参数；REST/HADOOP storage conf）+ TCCL-pin + `executeAuthenticated` + DriverShim + `preCreateValidation`。pom 加 `hadoop-mapreduce-client-core`+`commons-lang` **test-scope**（`new HiveConf()` 静态初始化需,运行时闭包已含,不入 plugin-zip,镜像 paimon）。
3. **对抗 parity review**（3-flavor 独立比对 legacy）= **0 confirmed bug**（2 个 minor 经独立 verify 均非真 bug：REST oauth2 token else 分支 `""` vs `null` 仅在校验拒绝的不可达态;JDBC driver dedup 是有意的 paimon-parity 重构,可观察结果一致）。

## 🟢 T05 已交付的可复用件（T06/T07 直接接）

- **`buildCatalogProperties(props,flavor,chosenS3)`**：编排器,handles base+impl+per-flavor+type 删除+jdbc catalog_name 删除。**s3tables/dlf 当前走 default 分支 = 仅 base+impl（占位）**——T06/T07 在此扩。
- **`IcebergConnector.createCatalog` switch 的 `default` 分支** = rest/hadoop/s3tables/dlf 共用 `buildHadoopConfiguration`；s3tables/dlf 的 bespoke 实例化（`new S3TablesCatalog().initialize` / `new DLFCatalog().setConf().initialize`）尚未接,现仍走 `CatalogUtil.buildIcebergCatalog`(impl-name)——**dlf 会 ClassNotFound（`...dlf.DLFCatalog` 未建），s3tables 可能能起但缺 region/s3 派生**。因 iceberg 不在 SPI_READY 故未被触发。
- **`MetaStoreProviders.bindForType("dlf",props,storageConf)`** 已就绪（part1）：T07 DLF 直接调 → `DlfMetaStoreProperties.toDlfCatalogConf()`,勿再做 metastore-spi recon。
- **`assembleHiveConf` / `buildHadoopConfiguration` / `firstNonBlank`**（public static）= T06/T07 复用。

## 🟡 T05 已记录的 deviation（UT 不可见,仅 P6.6 docker plugin-zip e2e 真验）

- **REST PROVIDER_CHAIN 非-DEFAULT** provider-class 跳过（连接器不能 import fe-core `AwsCredentialsProviderFactory.getV2ClassName`；DEFAULT=no-op=常见情形无缺口）。
- **GLUE/REST 的 `s3.*`** 取自 fe-filesystem 类型化 `S3CompatibleFileSystemProperties`（D-061）而非 legacy `S3Properties.of(origProps)`——若 glue 仅用 `glue.*` 别名供凭据（fe-filesystem 不读这些进 S3 store），s3.* FileIO 凭据可能缺（glue-client 凭据仍从 `glue.*` 读,不受影响）。
- **base 的 no-S3-store fallback region**（`getRegionFromProperties`）未移植（niche；连接器读类型化 storage）。

## ✅ T06 = DONE（2026-06-22，`386e183c091` 已 commit，未 push）

> **纠正 HANDOFF 过时计划**：HANDOFF 原写「2-arg `initialize(name,props)`」是错的——反编译 s3tables SDK 证实 2-arg 走 `DefaultS3TablesAwsClientFactory`，其 `applyClientCredentialConfigurations` **只认 `client.credentials-provider` 类名**，**静默丢 `s3.access-key-id`/`s3.secret-access-key`** → 静态凭据 s3tables 控制面会回退默认链鉴权失败。legacy 实走 **3-arg** `initialize(name,opts,client)` 手搓 client。**用户签字「全阶梯」凭据**（静态+assume-role STS+默认链）。

- **`IcebergCatalogFactory.buildS3TablesCatalogProperties`（纯）**：base（copy-all + warehouse=table-bucket ARN + manifest-cache）+ s3tables S3FileIO dialect。**不加 catalog-impl、不删 type**（legacy bespoke `initCatalog` 都不做；只有 CatalogUtil 路做；3-arg 仅校验 warehouse 非空）。
- **EXPLICIT-wins 凭据阶梯**（对抗 parity review 抓出的真 bug）：s3tables 用 `appendS3TablesFileIOProperties`（镜像 legacy `putS3FileIOCredentialProperties`）——**静态 AK/SK 在场则压制 assume-role 块**；这**有别于** rest/hadoop/jdbc 用的 `appendS3FileIOProperties`（=`toS3FileIOProperties`，恒发 assume-role-if-role）。两 helper 共享新私有 `putS3FileIODialect`。**rest/hadoop/jdbc 的 always-emit 不变**（与 legacy 一致，勿改）。
- **`IcebergConnector`**：s3tables 在 CatalogUtil flavor switch **之前**早分支到 `createS3TablesCatalog`；`buildS3TablesClient`（region + `buildAwsCredentialsProvider` + `s3tables.endpoint` override + `HttpClientProperties`）+ `new S3TablesCatalog().initialize(name,opts,client)`，复用 TCCL-pin + `executeAuthenticated`。`buildCatalogAuthenticated` 泛化成 `Callable<Catalog>`。**无 chosenS3 / region 空 → fail-loud**。**不调 setConf**（legacy 不调；HANDOFF「setConf」也是过时的）。
- **deviation（UT 不可见，仅 P6.6 docker plugin-zip e2e 验）**：非-DEFAULT `PROVIDER_CHAIN` provider mode 退化成 `DefaultCredentialsProvider`（连接器不能 import fe-core `AwsCredentialsProviderFactory.createV2`）；assume-role 的 STS base creds 同此退化。常见静态/实例角色不受影响。**与 T05 REST/glue PROVIDER_CHAIN gap 同族**。
- **验收全绿**：fe-connector-iceberg UT 81/0/0（IcebergCatalogFactoryTest +4 prop-map 含 EXPLICIT-wins 压制；新 IcebergConnectorTest 2 = 无 storage / region 空 fail-loud）、checkstyle 0、import-gate 净、iceberg 仍**不在** `SPI_READY_TYPES`。SDK 闭包 T04 已就位（s3tables/sts/auth），无 pom 改。

## ✅ T07 = DONE（2026-06-22，`16926486e8c` 已 commit，未 push）

> **纠正 HANDOFF 过时计划**：HANDOFF 列 `ProxyMetaStoreClient` 为「vendored」要 port——recon 实证它（198KB）+ `DataLakeConfig` 在 **hive-catalog-shade 未 relocate**（`com/aliyun/datalake/...`），且仅 `RetryingMetaStoreClient.getProxy(..., ProxyMetaStoreClient.class.getName())` **反射按名加载** → **不 port 2193 行源码**，`DLFClientPool` 直接 import（从 shade 解析，plugin-zip 已 ship `hive-catalog-shade-3.1.1.jar`）。

- **5 文件 port 进连接器** `org.apache.doris.connector.iceberg.dlf[.client]`：`HiveCompatibleCatalog`(DLF-only base)、`DLFTableOperations`、`DLFClientPool`、`DLFCachedClientPool` **逐字**（仅改 package；零 fe-core import；iceberg-hive 1.10.1 + relocated `shade.doris.hive...thrift` 均来自 hive-catalog-shade，连接器直连 iceberg 也 1.10.1 **无版本错位**）。
- **`DLFCatalog` 断 3 fe-core import**（`OSSProperties`/`CloudCredential`/`S3Util`，全在 `initializeFileIO`）：OSS 配置改读类型化 `S3CompatibleFileSystemProperties`（D-061，ctor 注入）；S3 client 内联 `buildOssS3Client` **逐行复刻** `S3Util.buildS3Client`（UrlConnection 30s + AwsS3V4 signer + 3-retry equal-jitter + **chunkedEncoding=false**〔OSS 必需〕+ path-style）；oss→s3.oss endpoint rewrite 抽成纯函数 `toS3CompatibleEndpoint`（单测）。
- **`IcebergCatalogFactory.buildDlfConfiguration`（纯）**：`toDlfCatalogConf()` 的 8 个 `dlf.catalog.*` 键（= `DataLakeConfig.CATALOG_*` 常量值，反编译核对逐一对上）+ legacy 的 `hive.metastore.type=dlf` + `type=hms`。
- **`IcebergConnector`**：dlf 在 CatalogUtil flavor switch **之前**早分支 → `createDlfCatalog`（conf 经 `bindForType("dlf",...).toDlfCatalogConf()`；base catalogProps；`new DLFCatalog(oss).setConf(conf).initialize(...)` 包在 TCCL-pin + executeAuthenticated）；无 OSS storage **fail-loud**。
- **deviation（UT 不可见，P6.6 docker 验）**：①ProxyMetaStoreClient 按名从 shade 加载；②非静态凭据回退 `DefaultCredentialsProvider`（legacy 是 5-provider 链；DLF 必有 OSS 凭据故不可达）；③`toDlfCatalogConf` 把 OSS storage config 叠进 metastore conf（benign 多键）。
- **对抗 parity review = 0 confirmed bug**（4 文件逐字、sever/conf/endpoint/creds/wiring 全对 legacy）。**未删 fe-core legacy DLF**（STILL-CONSUMED 至翻闸）。
- **验收全绿**：fe-connector-iceberg UT 85/0/0（+`DLFCatalogTest` 2 endpoint、+`buildDlfConfiguration` 1、+dlf fail-loud 1）、checkstyle 0、import-gate 净、plugin-zip 含 hive-catalog-shade、iceberg 仍**不在** `SPI_READY_TYPES`、**无 pom 改**。

## ✅ T09 = DONE（2026-06-22，未 push；4 读路径 gap + 字节级命名空间 split + auth 包裹）

> 设计文档 = `plan-doc/tasks/designs/P6-T09-iceberg-read-parity-design.md`。**5-reviewer 对抗 parity 复核 + 每发现二次 adversarial verify**（workflow）跑过：4 个原 gap 全 MATCH；额外查到 2 真差异（已修），1 误报（已证伪）。

- **G1 format-version**：`IcebergConnectorMetadata.getFormatVersion(table)` 逐字 port legacy `IcebergUtils.getFormatVersion`（`BaseTable→operations().current().formatVersion()`，else `format-version` prop parseInt，default 2）。删除 skeleton 的 `spec().specId()>=0?2:1`（恒 2）。
- **G2 column 构造**：name `toLowerCase(Locale.ROOT)`、isKey=true、nullable 恒 true（不读 `isOptional`）、`WITH_TIMEZONE` marker（源 TIMESTAMP+`shouldAdjustToUTC`，**独立于** mapping flag，顶层列）。`ConnectorColumnConverter` 已确认会把这些 flag 落到 `Column`。field-id/uniqueId 仍丢（`ConnectorColumn` 无载体，DESCRIBE 不需，scan 路 P6.2+）。
- **G3/G4 listing**：`CatalogBackedIcebergCatalogOps` 内部加嵌套命名空间递归（REST+`iceberg.rest.nested-namespace-enabled` flag，dotted）、view 过滤（减 `listViews`，gate `ViewCatalog && (!rest||iceberg.rest.view-enabled)`）、dotted-namespace split + `external_catalog.name` 追加。配置由 `IcebergConnector.getMetadata` 从 props 派生后线程进 seam。**seam 接口不变**（递归/过滤/split 全 INTERNAL）。
- **G2.1 命名空间 split 字节级**：改用 legacy 同款 Guava `Splitter.on('.').omitEmptyStrings().trimResults()`（plain Java `String.trim()` 漏 U+0020 以上的 Unicode 空白如 U+3000；NBSP U+00A0 不算差异——Guava whitespace() 不含它）。guava 33.2.1 compile-scope（hadoop 传递，已 bundle）。
- **G5 auth 包裹（用户裁定纳入 T09）**：`IcebergConnectorMetadata` ctor 增 `ConnectorContext`（由 `getMetadata` 线程），5 个读全包 `context.executeAuthenticated(...)`，逐方法镜像 legacy 异常语义（listDatabaseNames warn+rethrow；db/tableExists+loadTable rethrow；listTableNames `catch RuntimeException→rethrow` 再 wrap——iceberg `NoSuch*` 是 unchecked，`UGI.doAs` 不包，故无需 lambda 内 catch，**有别于** paimon 的 checked 异常处理）。seam 保持 auth-agnostic。**仅 Kerberized HMS/REST 翻闸后可见**（simple-auth `executeAuthenticated` 直通）。
- **误报已证伪**：`iceberg.format-version`/`iceberg.partition-spec` 合成键无 fe-core 读者且**不会**泄漏进 SHOW CREATE——`Env.java:4936-4939` 把 PluginDriven LOCATION+PROPERTIES 渲染 gate 在 `PAIMON_EXTERNAL_TABLE`，iceberg `getEngineTableTypeName()` 返回 `PLUGIN_EXTERNAL_TABLE` → 整块跳过。未来 iceberg SHOW-CREATE 渲染分支（P6.6）才需处理这些键。
- **测试**（无 Mockito，fail-loud fake）：新 `FakeIcebergCatalog`/`FakeIcebergViewCatalog`/`PlainIcebergCatalog`（iceberg `Catalog`/`SupportsNamespaces`/`ViewCatalog` 1.10.1 抽象集，注意 `Catalog`+`ViewCatalog` 的 `initialize` diamond 须 override）+ `CatalogBackedIcebergCatalogOpsTest`（13）；`IcebergConnectorMetadataTest`（19，含 column flag/format-version/lowercase/withTimeZone/auth `authCount`+`failAuth`）；`RecordingConnectorContext`（harness 早就备好 authCount/failAuth）。**TDD：每改 RED→GREEN，auth 包裹 post-hoc 拆一个 wrap 验非空 RED 再恢复。**
- **验收全绿**：fe-connector-iceberg UT **103/0/0**、checkstyle 0、import-gate 净、iceberg 仍**不在** `SPI_READY_TYPES`、**无 pom 改**（guava 已是 compile 依赖）。

## ✅ T10 Phase A = DONE（2026-06-22，本 HANDOFF commit，未 push）— metastore 模块拆分（filesystem 式），行为不变

> 设计文档 = `P6-T10-iceberg-validation-design.md` v2 §5/§10。**A-gate 全绿**：metastore-spi UT 4/0/0、**metastore-paimon UT 43/0/0**、**paimon 连接器 UT 318/0/0**（1 skip=env-gated `PaimonLiveConnectivityTest`）、iceberg 连接器 UT 103/0/0（reactor 健康，未被破）、checkstyle 0、import-gate 净。

- **新模块 `fe-connector-metastore-paimon`**（reactor 入 `fe-connector/pom.xml`，紧跟 metastore-spi）：5 个 `Paimon{Hms,Dlf,Rest,Jdbc,FileSystem}MetaStoreProperties` + 5 个 `Paimon…MetaStoreProvider` + `META-INF/services`（paimon FQN），包 `org.apache.doris.connector.metastore.paimon.{hms,dlf,rest,jdbc,fs}`。镜像 fe-filesystem per-backend。
- **`-spi` 瘦身**：删 5 impl + 5 provider + META-INF/services + 5 flavor 测试 + dispatch 测试；**新增 2 个引擎中立基类** `Abstract{Hms,Dlf}MetaStoreProperties`（字段 + `toHiveConfOverrides`/`toDlfCatalogConf` conf + 新 `validateConnection()`〔§4 共享连接规则〕）；保留框架（`MetaStoreProvider(s)`/`AbstractMetaStoreProperties`/`MetaStoreParseUtils`/`JdbcDriverSupport`）+ `MetaStoreParseUtilsTest`。
- **`validate()` 拆分（字节不变）**：`PaimonHms.validate()` = `requireWarehouse()` + `validateConnection()`；`PaimonDlf.validate()` = `requireWarehouse()` + `validateConnection()` + `requireOssStorage()`（**paimon-only，留连接器子类**，非基类）；Rest/Jdbc/Fs 整体搬（validate 不变）。触发序逐字保留（warehouse→…）。
- **paimon 连接器 pom**：`fe-connector-metastore-spi` 依赖 → `fe-connector-metastore-paimon`（-spi/-api 传递）；**plugin-zip.xml 无需改**（blanket dependencySet 自动收）。`unzip -l` 实证：lib/ 含 `fe-connector-metastore-{paimon,spi,api}-*.jar`；paimon jar 内 META-INF/services=5 paimon FQN + 10 类；spi jar **无** META-INF/services、带 2 基类、**无** 旧 impl 子包。
- **对抗字节复核**：3 wholesale impl（Rest/Jdbc/Fs）+ 5 provider normalized-diff vs 原始 = **仅 javadoc/注释措辞差异**（"Paimon X" vs "X"），可执行代码零变更；Hms/Dlf split 的行为 parity 由 16+7 测试（精确报错/序/conf）守。
- **关键架构事实（纠正过时 pom 注释）**：fe-core **不依赖** metastore 模块（旧 `-spi` pom 注释「compiled into fe-core」是**错的**，从 fe-filesystem-spi 误抄）；metastore jar 是 **child-first bundle 进连接器 plugin-zip**（blanket dependencySet）。ServiceLoader 用 `MetaStoreProvider.class.getClassLoader()`→plugin 内所有 metastore jar 同一 child loader，拆模块后发现照常。
- **Phase A↔B 的过渡态**：iceberg 连接器仍依赖 `-spi`（含基类+框架，**无 paimon provider**）。iceberg 的 `bindForType("hms"/"dlf")` 仅 live createCatalog 触发（非 UT、iceberg 未翻闸），故 Phase A 不破 iceberg UT；**Phase B 建 `-iceberg` 模块补 iceberg provider 后恢复**。
- **本地构建坑（非缺陷）**：非-clean 构建会留 `-spi/target/classes/META-INF/services` 旧 FQN → ServiceLoader 报 `Provider …spi.hms.HmsMetaStoreProvider not found`；**必 `clean`**（CI 本就 clean，无碍）。

## ✅ T10 Phase B = DONE（2026-06-22，本 HANDOFF commit，未 push）— iceberg per-flavor 校验特性

> 设计文档 §10 TODO 7-13。**B-gate 全绿**：metastore-iceberg UT **36/0/0**、iceberg 连接器 UT **111/0/0**（1 skip=env-gated `IcebergLiveConnectivityTest`）、checkstyle 0、import-gate 净、iceberg 仍**不在** `SPI_READY_TYPES`。**对抗 parity 复核（5-agent workflow `wf_a3aaa55a-79e`，逐 flavor vs legacy fe-core + §4）= 4 MATCH + 1 nit-deviation（REST locale，见下）。**

- **新模块 `fe-connector-metastore-iceberg`**（reactor 入 `fe-connector/pom.xml`，紧跟 `-paimon`）：包 `org.apache.doris.connector.metastore.iceberg.{hms,dlf,rest,jdbc,glue,noop}`。7 flavor + 7 provider + `META-INF/services`。镜像 `-paimon`。
- **flavor 实现**：`IcebergHms/Dlf` extend 共享基类 `Abstract{Hms,Dlf}MetaStoreProperties`，`validate()`=`validateConnection()`（**仅连接，无 warehouse/OSS**——与 paimon 的关键区别）；`IcebergRest/Jdbc/Glue` 逐字 §4（REST 10 条用 `ParamRules`〔fe-foundation,连接器可用〕near-verbatim port `IcebergRestProperties.buildRules` + 内联 `Security`/`AwsCredentialsProviderMode` 枚举检查；Glue 4 条 port `AWSGlueMetaStoreBaseProperties`；JDBC 3 条 uri/catalog_name/warehouse）；`IcebergNoOp`（hadoop/s3tables 共享，`validate()`=no-op，storage 上游校验）。**flavor token：rest/hms/glue/dlf/jdbc/hadoop/s3tables**（无 filesystem，hadoop=文件系统式；`IcebergExternalCatalog` 常量实证）。
- **连接器接线**：iceberg 连接器 pom 依赖 `-spi`→`-iceberg`（透传 `-spi`+`-api`）；`IcebergConnectorProvider.validateProperties` → `bindForType(resolveFlavor(props), props, {}).validate()`（`resolveFlavor` 缺失返回 null → `bindForType(null)` 抛，iceberg classpath 无 provider 认 null）。plugin-zip **无需改**（blanket dependencySet），`unzip -l` 实证含 metastore-iceberg、**不含** metastore-paimon。`bindForType("hms"/"dlf")` conf 经共享基类 == T05/T07（57 `IcebergCatalogFactoryTest` 全绿守）。
- **测试**：metastore-iceberg per-flavor（REST 12〔10 规则+2 fire-order〕、Glue 6、JDBC 4、HMS 5、DLF 3、dispatch 7）+ iceberg 连接器 `IcebergConnectorValidatePropertiesTest` 7（provider 入口 accept/reject）+ env-gated `IcebergLiveConnectivityTest`（镜像 Paimon，gate `ICEBERG_REST_URI`）。无 Mockito，WHY+MUTATION 注释。
- **对抗复核结论**：REST/Glue/JDBC/HMS/DLF 规则集/报错串/fire-order/guard 全核对 legacy。唯一 deviation=**REST `validateCredentialsProviderMode` 用 `Locale.ROOT` vs legacy 默认 locale `toUpperCase()`**（nit；ASCII 模式名字节相同；仅 Turkish-i 等非 ASCII locale 下 ROOT 更正确〔legacy 会误拒 `web-identity`〕；真实 ASCII 输入不可达）——**有意保留 ROOT**（更正确+checkstyle 安全），已在代码注释 + 此处记录。其余 4 flavor MATCH（nit 均 out-of-scope/inert：Glue 的 `S3Properties.of`〔s3.external_id〕=storage 上游；JDBC `isNullOrEmpty` vs `isBlank`=绑定已 trim 故 inert；HMS authType 默认 `""` vs `"none"`=对 3 规则零影响；DLF endpoint 抛 `IllegalArgumentException` vs legacy `StoragePropertiesException`=消息字节同、均 RuntimeException 同样 wrap、无代码分支 subtype；DLF proxyMode 字面量 vs 常量=值相同且 conf-side）。
- **deviation（UT 不可见，P6.6 docker 验）**：与 paimon 同族——HMS/DLF 的 conf/auth 包裹在连接器侧 T05/T07 已接（本 Phase 只加 validate）；iceberg 校验仅 simple-auth 直通可在 UT 见，Kerberized 须翻闸后 e2e。

## 🔴 关键认知（写下来免下次重踩）

- **T04 残留风险（UT/打包不可见，仅 P6.6 docker plugin-zip e2e 真闸可验）**：
  1. `hive-catalog-shade` **内含** iceberg 1.10.1 与连接器直接 `iceberg-core` 在 child-first loader 共存——版本相同→预期 byte-identical benign（fe-connector-hive 同款已上线），但**首次** direct-iceberg + shade 组合，未 live 验。
  2. **glue 显式-AK 凭据 provider 类 `com.amazonaws.glue.catalog.credentials.*` 来源未定**（不在 hive-catalog-shade / iceberg-aws；fe-core `aws-java-sdk-glue` v1 疑源未证）→ **T05 glue flavor wiring 时核**（不挡 T04 闭包）。
  3. `apache-client` 经 awssdk 传递 runtime 入闭包（paimon 同款故意 ship，无害）。
- **silent 读路径 bug（骨架，翻闸后才在 regression 暴露）—— T08 修 #1#1b#2；T09 修 #3#4 + 命名空间 split + auth 包裹**：
  3. ✅ **format-version（T09 DONE）**：已改读 table 真元数据（`getFormatVersion`），删 `spec().specId()>=0?2:1`。pin 测试已翻成 read-from-property。
  4. ✅ **column 构造 + listing（T09 DONE）**：nullable 恒 true、isKey 恒 true、小写化（`Locale.ROOT`）、`WITH_TIMEZONE` marker、nested-namespace 递归 + view 过滤、命名空间 dotted-split + external_catalog.name 全部落地。详见上「✅ T09 = DONE」。
- **Q2=B metastore-spi mini-recon + 改造 = ✅ DONE（part1 `562201deb9f`）**：新增 `MetaStoreProvider.supportsType(String)` + `MetaStoreProviders.bindForType(flavor,props,storageConf)`（5 provider 全转，paimon `supports(Map)` 字节不变）。HMS/DLF conf 复用 `HmsMetaStorePropertiesImpl.toHiveConfOverrides` + `DlfMetaStorePropertiesImpl.toDlfCatalogConf`（SDK-free）。**iceberg 只用 `toHiveConfOverrides()`/`toDlfCatalogConf()`，不调 paimon `validate()`**（paimon-ism: requireWarehouse for all flavors；iceberg HMS 不要求 warehouse）。无 glue/s3tables provider（glue/s3tables 在连接器 `IcebergCatalogFactory` 内直接装配，不走 metastore-spi）。
- **跨切面风险（带入 T05–T07 + P6.6 翻闸门）**：R-004 AWS-SDK `ExecutionAttribute` static 撞（已用 child-first 自包含 awssdk 闭包缓解，待 docker 验）；DLF `ProxyMetaStoreClient` 按类名反射加载须入 plugin-zip 闭包（hive-catalog-shade 已带，T07 决定 vendored-source vs shade-bundled）；hive-catalog-shade relocated thrift vs host（已 exclude fe-thrift/libthrift）；**field-id 丢失**（`ConnectorColumn` 无载体，P6.2+ scan 前须重引，否则同 paimon BE SIGSEGV/DCHECK 类 bug）。**这些 UT 不可见，仅 P6.6 docker plugin-zip e2e 可验**。

## 🟢 下一步（精确）—— **P6.1 ✅ DONE（T01–T10 全绿），当前 = P6.2（scan + MVCC + cache + vended）**

> **⚠️ 不是 P6.6！** P6 是 8 阶段串行（P6.1→P6.2→P6.3→P6.4→P6.5→P6.6 翻闸→P6.7 删 legacy→P6.8 回归，见 `P6-iceberg-migration.md:113`）。刚完成的 T10 是 **P6.1 的最后一个 task**。翻闸（P6.6）「全有或全无」，**须等 P6.2–P6.5（scan/write/procedure/sys-table）全部实现完**——现在翻闸会让 iceberg 全查询走只有读元数据+校验的连接器，scan/write 立刻全断。
> **起步先**：按 AGENT-PLAYBOOK §7.1/§7.2 做 P6.2 的 code-grounded recon + 逐 task 拆解（产出 P6.2 自己的 task 行）；re-read `P6-iceberg-migration.md` 的 P6.2 块（§阶段拆分行 + §old→new 映射 scan/cache/MVCC/vended 行 + §阶段内前置 #1 + §验收门 P6.2 + 开放决策 O2）。

- ✅ **P6.1 = DONE**：T01–T03（结构倒置 + 测试基建，`ae54a2174ff`）、T04（7-flavor pom 闭包，`1fd4d42c297`）、T05（5-flavor 装配 + DriverShim，`b0dbe91e1a5`）、T06（s3tables bespoke，`386e183c091`）、T07（DLF 子树 port，`16926486e8c`）、T08（type-mapping 读 parity，`d41fa4faf3e`）、T09（读路径列/format-version/listing/auth parity，`2caf9edc983`）、**T10（= metastore 模块拆分 Phase A `f67195fee64` + iceberg per-flavor 校验 Phase B `6cc4de3078f`）**。验收门（line 89）：7 flavor 装配 + ConnectorMetadata 读 parity + per-flavor CREATE 校验 全绿。
- **P6.2（当前任务）scope**（`P6-iceberg-migration.md` P6.2 块）：
  1. **scan 路径**：`source/`（7 文件，`IcebergScanNode` 1228）→ `IcebergScanPlanProvider` + `IcebergScanRange` + 通用 `PluginDrivenScanNode`（SPI 点 E3，已就绪）。
  2. **MVCC**：`IcebergMvccSnapshot` + `IcebergSnapshot` → `ConnectorMvccSnapshot` + 通用 `PluginDrivenMvccExternalTable`（E5/D-042 源无关，已就绪）。
  3. **cache**：`IcebergExternalMetaCache`(289) + `cache/`(2) + `IcebergSnapshotCacheValue` → 连接器内 cache（决策 D6，无 SPI）。
  4. **vended**：`IcebergVendedCredentialsProvider` → **新 `ConnectorCredentials` SPI（E6）**——**P6.2 起前在 `fe-connector-api` 新建**（§阶段内前置 #1）；核对 iceberg REST/DLF vended 与 paimon `isVendedCredentialsEnabled` 网关能否合面（开放决策 O2，影响 metastore 子线）。
  - **最高危**：**field-id 丢失**——`ConnectorColumn` 无 field-id 载体（T09 记录），scan 前必须重引，否则同 paimon BE SIGSEGV/DCHECK 类崩溃（见「🔴 关键认知」末条）。
  - **验收门（line 90）**：scan parity（谓词下推 / 分区裁剪行数 / native·JNI / position+equality delete / SELECT* 无谓词）vs `IcebergScanNode`；MVCC time-travel（AS OF / VERSION）；vended REST/DLF round-trip。**仍不碰 `SPI_READY_TYPES`，零行为变更。**
- **此后**：P6.3 写路径（先写 `06-iceberg-write-path-rfc.md` 过 PMC）→ P6.4 procedures（新 `ConnectorProcedureOps` E2）→ P6.5 sys-table + 元数据列 → **P6.6 才翻闸**（加 `SPI_READY_TYPES` + GSON compat + SHOW-CREATE 渲染：合成键 `iceberg.format-version`/`iceberg.partition-spec` 现被 `Env.java:4936` paimon-gate 挡住，翻闸渲染时才决定保留/剥离）→ P6.7 删 legacy → P6.8 docker 回归（届时才首验 T04–T10 全部 UT-不可见 deviation）。

> **历史风险（已缓解，docker 待 P6.6 验）**：R-A1 paimon 打包（Phase A 已搬完，A-gate plugin-zip 实证绿）；R-A2/R-A3 conf 基类抽取/iceberg 重连（既有 paimon/iceberg conf 测试守，全绿）。

---

# 📦 仓库 / 进度状态

- **工作分支 = `catalog-spi-10-iceberg`**（P6.1 起步前 HEAD = `e5959e1b53d` #64655 P3b）。branch off `branch-catalog-spi`，PR 时 squash 合并。**所有 commit 均未 push。**
  - **已 commit（旧 session）**：T01-T03 `ae54a2174ff`；T08 `d41fa4faf3e`（type-mapping read parity）；T04 `1fd4d42c297`（pom 7-flavor 闭包 + plugin-zip + fe/pom dM）。
  - **已 commit（T05 全套）**：`562201deb9f`（part1：metastore-spi `bindForType`/`supportsType` + 设计文档）；`6f7b292b5c6`（part2：factory `buildBaseCatalogProperties`/`appendS3FileIOProperties`/`chooseS3Compatible`）；`b0dbe91e1a5`（**part3：5-flavor appender + connector LIVE wiring + DriverShim + 52 parity 测试；UT 75/0/0、checkstyle 0、import-gate 净**）。
  - **已 commit（T06）**：`386e183c091`（**s3tables bespoke 3-arg：`buildS3TablesCatalogProperties` + EXPLICIT-wins `appendS3TablesFileIOProperties` + connector `createS3TablesCatalog`/`buildS3TablesClient`/`buildAwsCredentialsProvider` + `Callable<Catalog>` 泛化；UT 81/0/0、checkstyle 0、import-gate 净**）。
  - **已 commit（T07）**：`16926486e8c`（**DLF 子树 port 进连接器 `dlf/`〔5 文件，4 逐字 + DLFCatalog 断 3 fe-core import〕 + `IcebergCatalogFactory.buildDlfConfiguration` + connector `createDlfCatalog` 早分支；UT 85/0/0**）。
  - **已 commit（T09）**：`2caf9edc983`——读路径 parity：`IcebergConnectorMetadata`（列构造 + format-version + 5 读 auth 包裹）、`IcebergCatalogOps`（nested-namespace 递归 + view 过滤 + Guava 字节级 split）、`IcebergConnector.getMetadata`（线程 config + context）、`IcebergConnectorProperties`（+`REST_VIEW_ENABLED`/`EXTERNAL_CATALOG_NAME`）+ 5 新测试件 + 设计文档。UT 103/0/0。
  - **已 commit（T10 设计）**：`2043d1f07c2`——`P6-T10-iceberg-validation-design.md` **v2**（metastore 模块拆分 filesystem 式 + iceberg per-flavor 校验下沉；含 §4 逐字规则、§5 目标结构、§8 风险、§10 时序 TODO）。recon workflow `wf_8ae4353f-9a8`。
  - **已 commit（T10 Phase A）**：`f67195fee64`——metastore 模块拆分（行为不变）：新模块 `fe-connector-metastore-paimon`（5 impl + 5 provider + META-INF）、`-spi` 抽 `Abstract{Hms,Dlf}MetaStoreProperties` + 瘦身、reactor + paimon 连接器 pom 重连。A-gate 全绿（metastore UT 4+43、paimon 连接器 318、iceberg 103、plugin-zip 实证、checkstyle 0、import-gate 净）。详见上「✅ T10 Phase A = DONE」。
  - **已 commit（T10 Phase B = 本 HANDOFF commit）**：iceberg per-flavor 校验：新模块 `fe-connector-metastore-iceberg`（7 flavor + 7 provider + META-INF；hms/dlf extend 共享基类、rest/glue/jdbc §4 逐字、hadoop/s3tables no-op）、iceberg 连接器 pom 重连 `-spi`→`-iceberg` + `validateProperties` 接线 + env-gated live test。B-gate 全绿（metastore-iceberg UT 36、iceberg 连接器 UT 111〔1 skip〕、plugin-zip 隔离实证、checkstyle 0、import-gate 净、iceberg 仍不在 SPI_READY_TYPES）；对抗 parity 复核 4 MATCH + 1 nit。详见上「✅ T10 Phase B = DONE」。
  - **已 commit（P6.2 recon）**：`0df8bf20abd`——recon-only（未改产品码）：`research/p6.2-iceberg-scan-recon.md` + migration P6.2 逐 task 拆解 + 4 决策签字（E6 取消）。
  - **已 commit（P6.2-T01）**：`78b6f988bc4`——scan provider 骨架：`IcebergScanPlanProvider`+`IcebergScanRange`+`getScanPlanProvider` 接线+`ignorePartitionPruneShortCircuit=true`，planScan resolve 表→空。UT 117/0。
  - **已 commit（P6.2-T02）**：`90f5474fcf5`——谓词下推+createTableScan+split 枚举：新建 `IcebergPredicateConverter`（移植 `convertToIcebergExpr` 矩阵+checkConversion）、`planScan` 实装（`TableScanUtil.splitFiles`+`determineTargetFileSplitSize`）、scan 测试用 `InMemoryCatalog` 真表。对抗复核修 1 真 bug（session tz alias→timestamptz 下推偏移）。UT 138/0（1 skip）。详见上「✅ P6.2-T02 = DONE」。
  - **已 commit（P6.2-T03）**：`626dd933dcf`——typed range-params + `populateRangeParams`→`TIcebergFileDesc` + native fileFormat + `path_partition_keys`：新 `IcebergPartitionUtils`（port 分区助手，ZoneId/Jackson 两 delta）、`IcebergScanRange` typed carriers + populateRangeParams、`IcebergScanPlanProvider` 填 carriers + `getScanNodeProperties`、pom `fe-thrift`(provided)。5-dim 对抗 workflow 修 2 真 bug（Bug1 非 orc/parquet fail-loud；**Bug2 用户裁定加非破坏 SPI `isPartitionBearing()`——P6.2「0 新 SPI」唯一例外**）+ delete_files 误报证伪。iceberg UT 164/0（1 skip）+ fe-core PluginDriven* 53/0 无回归。详见上「✅ P6.2-T03 = DONE」。
  - **已 commit（P6.2-T04 = 本 HANDOFF commit，2 个）**：`d249a4a9dea`（**T04 delete files**：`IcebergScanRange` 类型化 `DeleteFile` carrier + `toThrift` + v1/v2 populateRangeParams 分支；`IcebergScanPlanProvider` `buildDeleteFiles`/`convertDelete`（position/DV/equality）+ delete 路径归一化 + `getDeleteFiles` EXPLAIN override；类型化 carrier 非 `ConnectorDeleteFile.properties`〔设计 §2，recon §2 超越〕；0 新 SPI。UT 164→176）+ `54ecac35e3d`（**数据路径归一化跟修**，对抗 workflow `wf_d530fdbf-2bf` 抓的 1 真 gap：拆 `IcebergScanRange.path`〔归一化〕/`originalPath`〔raw〕、`buildRange` 归一化主数据路径，镜像 paimon+legacy `createIcebergSplit:852`。UT 176→177）。checkstyle 0、import-gate 净、iceberg 仍不在 SPI_READY_TYPES、无 SPI/fe-core/pom 改。详见上「✅ P6.2-T04 = DONE」。
  - **已 commit（P6.2-T05）**：`2cc510608f6`——COUNT(*) 下推：`IcebergScanRange` 加 `pushDownRowCount` 载体 + `getPushDownRowCount` + `populateRangeParams` 发 `table_level_row_count`；`IcebergScanPlanProvider` 加 7-参 COUNT-aware `planScan` override + `getCountFromSnapshot`（读 `scan.snapshot()`）+ `ignoreIcebergDanglingDelete` + `planCountPushdown`（塌缩单 whole-file count range，镜像 paimon，丢 >10000 并行 trim）；batch mode 延后（用户签字）。设计文档 `designs/P6-T05-iceberg-scan-countpushdown-design.md`。对抗复核 `wf_e0afb564-38b` 4-dim：0 正确性发现 + 1 doc-only nit（splittable 误述）已修。UT 177→185（1 skip）、checkstyle 0、import-gate 净、iceberg 仍不在 SPI_READY_TYPES、**无 SPI/fe-core/pom 改**。详见上「✅ P6.2-T05 = DONE」。
  - **已 commit（P6.2-T06 = 本 HANDOFF commit）**：field-id 数据字典 `history_schema_info`：新 `IcebergSchemaUtils`（单 -1 entry + `extractNameMapping` + `buildCurrentSchema` keyed-off-requested-columns + `buildField` 每层带 id/name-mapping + encode/apply）+ 新 `IcebergColumnHandle` + `IcebergConnectorMetadata.getColumnHandles` + `IcebergScanPlanProvider`（`getScanNodeProperties` 发 `iceberg.schema_evolution` + `populateScanLevelParams`）+ 4 新/改测试 + 设计文档。**纠 HANDOFF 计划**：单 -1 entry 非全 schema-id（iceberg field-id 永久不变 + BE 从文件读 file field-id）。对抗复核 `wf_7109cc62-b6e` 5 驳回 + 2 确认（#1 GLOBAL_ROWID = P6.6 翻闸阻塞、用户签字延后；#2 fail-loud 竞态 = T06 惰性、T07 修）。UT 185→205（1 skip）、checkstyle 0、import-gate 净、iceberg 仍不在 SPI_READY_TYPES、**无 SPI/fe-core/pom 改**。详见上「✅ P6.2-T06 = DONE」。
  - **metastore 子线 = 已彻底 CLOSED**（8 文档加 CLOSED banner；后续勿读，见顶部范围注）。
- **stale cruft = 本 session 已清理**：删除 `fe-connector-{iceberg,paimon}-{api,backend-*}` 共 12 个目录（仅含 gitignored 生成物 `.flattened-pom.xml`，0 tracked、不在 reactor = 本地 `phase3-module-split` 旧实验遗留；untracked 故 git 无变更）。当前线用单 `fe-connector-iceberg` + flavor switch。
- **P0–P5 + P3 hybrid + P4 + P3b 全部已合入**（#63582/#63641/#64096/#64143/#64253/#64300/#64446/#64653/#64655）。iceberg **不在** `SPI_READY_TYPES`（`CatalogFactory:50` = {jdbc,es,trino-connector,max_compute,paimon}），仍走 switch-case（`:137 case "iceberg"`）。
- ⚠️ `regression-test/conf/regression-conf.groovy`（明文 Aliyun key）+ `*.bak` + scratch（`.audit-scratch/` `conf.cmy/` `META-INF/`）**严禁 `git add -A`**，commit 前 path-whitelist。

## 🗺️ 代码脚手架（iceberg）

- **连接器（终态归宿）**：`fe/fe-connector/fe-connector-iceberg/src/main/java/org/apache/doris/connector/iceberg/`（现：`IcebergConnector`/`Provider`/`ConnectorMetadata`/`ConnectorProperties`/`TableHandle`/`TypeMapping`/`CatalogFactory`/`CatalogOps` + **`dlf/`子树〔T07：`DLFCatalog`/`HiveCompatibleCatalog`/`DLFTableOperations`/`client/{DLFClientPool,DLFCachedClientPool}`〕**）。**pom 闭包（T04）已就绪**：7-flavor SDK + hive-catalog-shade + metastore-spi。
- **paimon 模板**（P6.1 镜像）：`fe/fe-connector/fe-connector-paimon/`（`PaimonCatalogFactory`/`PaimonCatalogOps` seam/`createCatalogFromContext` CL-pin+executeAuthenticated/测试 `RecordingPaimonCatalogOps`/`FakePaimonTable`）+ `fe-connector-paimon-hive-shade`（paimon 专建 thrift shade，**iceberg 不复用，改用 hive-catalog-shade**）。
- **legacy 对照（P6.1 读路径）**：fe-core `datasource/iceberg/`（`IcebergMetadataOps`1362 读半 / `IcebergExternalTable`535 读 / `IcebergUtils`1826 schema-type / `DorisTypeToIcebergType`134 / 7 flavor catalog + `HiveCompatibleCatalog`181 + `dlf/`4）+ `datasource/property/metastore/`（`AbstractIcebergProperties`285 + 7 flavor + factory，**STILL-CONSUMED 留 fe-core 至翻闸后**；T05/T07 移植 per-flavor 装配的源）。
- **metastore-spi（Q2=B 已扩 part1）**：`fe/fe-connector/fe-connector-metastore-spi/`（`MetaStoreProviders.bind`(paimon)+**新 `bindForType(flavor,…)`(iceberg)** + `MetaStoreProvider.supportsType` + `HmsMetaStorePropertiesImpl.toHiveConfOverrides`/`DlfMetaStorePropertiesImpl.toDlfCatalogConf`）。无 glue/s3tables provider（这两者在连接器内直接装配）。**T04 已加为 iceberg 连接器依赖。**

## ⚙️ 操作须知（复用）

- maven 绝对 `-f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl :<art> **-am** -Dmaven.build.cache.enabled=false -DfailIfNoTests=false`；验证读 surefire XML + `BUILD SUCCESS`。**漏 `-am`→`${revision}` 假错 / dependency:tree 找不到 reactor sibling**。**checkstyle 在 `validate` phase（编译前）跑**。连接器模块 art = `fe-connector-iceberg`。
- **plugin-zip 实查**（T04 起验闭包真相）：`mvn -pl :fe-connector-iceberg -am package -DskipTests` → `unzip -l target/doris-fe-connector-iceberg.zip | grep lib/`。
- 连接器禁 import fe-core：`bash tools/check-connector-imports.sh`（仅允许 `org.apache.doris.{thrift,connector,extension,filesystem}`）。
- 测试无 Mockito（fail-loud fake）；live-e2e CI-gated（docker），勿谎称跑过。
- cwd 跨 Bash 调用持久，`cd` 破相对路径 → 一律绝对路径。

## ⚠️ Commit 须知（任何 `git add` 前必读）

- **path-whitelist `git add`，严禁 `git add -A`**（scrub `regression-conf.groovy` 明文 key + `*.bak` + scratch `.audit-scratch/`·`conf.cmy/`·`META-INF/`）。
- message `[refactor](catalog) P6.1 iceberg: <subj>`（mirror #64653/#64655）+ 根因/解法/测试，末尾带
  `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>` +（本工作分支约定）`Claude-Session: …`（最终 squash 入上游时会被剥离）。
- PR base = `branch-catalog-spi`，squash 合并。历史 `catalog-spi-07-paimon` force-push 流程**已作废**。

## 🧠 给下一个 agent 的 meta

- **删除/parity 前必 grep 全调用方 + 区分 DEAD vs STILL-CONSUMED**（参 P5-T29 教训；metastore-props 是 STILL-CONSUMED）。
- **HANDOFF 的依赖名可能过时**——T04 实证「iceberg-hive-metastore」「aliyun DLF SDK」均不存在（在 hive-catalog-shade 内）。下次动 pom/依赖前**先 recon（grep repo + unzip 实证）再信 HANDOFF**。
- **Q2=B（用户主动选）已落地 part1** —— `bindForType`/`supportsType` 已加（`562201deb9f`）；后续 flavor（T07 DLF）直接调 `bindForType("dlf",…)`，勿再重做 metastore-spi recon。
- **REST signing-cred 的 PROVIDER_CHAIN 非-DEFAULT gap**（见「🟡 T05 剩余」REST 条）：实现 REST 时若要补该 niche 路，需新增一个 `ConnectorContext` seam 让 fe-core 解析 `AwsCredentialsProviderFactory.getV2ClassName`（连接器不能 import）；DEFAULT mode 无缺口，可先 NOTE 跳过。
- **大文件（`IcebergUtils`1826 等）用 subagent 总结**（playbook §3.1），勿主线整读。
