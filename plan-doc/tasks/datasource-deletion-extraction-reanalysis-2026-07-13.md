# 抽取前置重分析 — 隔离铁律下的合规处置（2026-07-13）

> **状态**：设计完成，**用户已 review 通过（2026-07-13）**。进入实现。
>
> **✅ 用户拍板（2026-07-13）**：① 认可本合规重分析取代原「搬中立家」方案；
> ② 组 4 保留列身份用**形态甲 = 逐列中立布尔位** `reservedPassthrough`（不走 table 级 list SPI）；
> ③ 组 4 建表校验**接受**移连接器侧带来的时机/异常类型/文案变化（不加前置钩子）；
> ④ 新揪出的第二处哨兵泄漏（`TablePartitionValues.HIVE_DEFAULT_PARTITION` / `partition_values` TVF）
> **单列紧邻后续**，不并入本轮；⑤ 组 1 用两并行列表、组 3 加载路径改指现有中立常量（均按推荐）。
> **背景**：原删除计划的「抽取 4 组活成员搬到中立家（fe-core util）」方案在两条新隔离铁律下**整体作废**
> （A = fe-core 源相关代码只出不进；B = 禁 deletion-scaffolding 式就近搬迁）。本文档逐消费者重定归属，
> 产出隔离合规方案。取代 `P7.5-datasource-deletion-plan.md §3` 的「处置」列，并据此修订 §5 步 2。
> **方法**：14-agent HEAD-grounded 重分析（`.claude/wf-p75-s3-reanalysis.js`，run `wf_6b4ce713-04f`，
> 6 路清点 + 4 路设计 + 4 路对抗验证），关键新事实经人工 clean-room 交叉核对。四组结论均 SOUND（对抗存活）。

---

## 结论总览

原「抽取搬中立家」的 4 组，重分析后**没有一组需要把源相关代码搬进 fe-core**。真实处置分三类：

| 组 | 内容 | 处置 | 新 SPI？ | 铁律 A/B |
|---|---|---|---|---|
| 1 | hive 分区名解析 `HiveUtil.toPartitionValues` 等 3 方法 | 2 纯删 + 1 经**连接器已产出的数据**委派 | 中立数据字段（值列表）| ✅✅ |
| 2 | hive LZO 输入格式判别 `isLzoInputFormat` 等 3 方法 | **纯删**（消费点全死，连接器早已自带）| 无 | ✅✅ |
| 3 | hive 默认分区哨兵 `__HIVE_DEFAULT_PARTITION__` | 查询路径经**现有** SPI 委派 + 加载路径改指现有中立常量 | 无（复用现有）| ✅✅ |
| 4 | iceberg 行血缘列 `_row_id` 等 6 成员 | 常量纯删 + 保留列身份经**中立能力位** + 建表校验经**现有**建表 SPI 委派 | 中立"保留列"位 | ✅✅ |

正面范式 = 对齐 Trino：分区值由连接器**解析好**再交引擎；hive 默认分区哨兵与隐藏/元数据列住在连接器，引擎从不字符串匹配它们。

---

## 组 1 — hive 分区名解析

**成员（在删除单元 `HiveUtil`）**：`toPartitionValues(String):173`、`toPartitionColNameAndValues(String):156`、
`convertToNamePartitionMap(...):204`。均按 hive 的 `key=value/...` 约定切分并用 hive 库 `FileUtils.unescapePathName` 反转义。

**活性核实**：
- `toPartitionColNameAndValues` 唯一消费者 `HMSAnalysisTask:108` = 死（legacy HMS 层）→ **纯删**。
- `convertToNamePartitionMap` 唯一消费者 `HMSTransaction:465` = 死；活等价物已内联在 `HiveConnectorTransaction:498-511` → **纯删**。
- `toPartitionValues` 唯一**活**消费者 = `PluginDrivenMvccExternalTable.toListPartitionItem:310`（paimon/iceberg/hive/hudi 实时经 `listLatestPartitions:263` 走它）。

**为何不能"搬中立 util"**：`toListPartitionItem` 收到连接器渲染好的名字串 `dt=2024-01-01`，再解析出值。任何 fe-core 副本都要么
重新 `import` hive 库 `FileUtils`（撞 A），要么复刻 hive 转义表（撞 B）。

**合规处置**：连接器**本就在同一循环里**算出了有序值列表（渲染名字 + 空值标志时），却把值列表丢弃、逼 fe-core 重解析。
在**已中立**的 `ConnectorPartitionInfo`（`fe-connector-api`）上加一个 `List<String> orderedPartitionValues`（渲染后的有序值，
与现有 `partitionValueNullFlags` 位对齐）。fe-core 直接 zip「连接器值 + 类型 + 空标志」，**零解析**，删掉 `HiveUtil`/`FileUtils` 依赖。
- 4 个连接器各补一行传已算好的值：hive `HiveConnectorMetadata:1108`、paimon `:1078`、iceberg `IcebergPartitionUtils:545`、hudi `HudiConnectorMetadata:734`。
- **不复用现有** `ConnectorPartitionInfo.getPartitionValues()`（Map）：paimon 在该 Map 存的是**未渲染的原始 spec**（DATE 存 epoch-int，
  `PaimonConnectorMetadata:1067`）且该 Map 是 paimon 自身扫描规划的输入（`:971` 在读它），改写会扰动 paimon 内部。故须独立的有序渲染值字段。

**对抗修正**：删掉 fe-core 的 `toPartitionValues` 后**无回退**——若某连接器未接线，`listLatestPartitions:277` 的 try/catch 会**静默吞掉**
分区（表被误报 UNPARTITIONED）。故须**fail-loud**（分区表上该字段为空即硬报错），并把 `HiveUtil` 删除**卡在 4 连接器全部接线 + 覆盖性 e2e**之后。

**待定**：① SPI 形态 = 两条并行列表（值 + 空标志，最小改动，**推荐**）vs 单个 (值,是否 null) 配对对象（防位错但改动大）。
② iceberg/hudi 现在不传空标志——确认它们的 LIST 分区路径确实从不渲染 NULL 哨兵，否则非字符串列（INT/DATE）的空分区丢弃 bug 会复现。

**工作量**：小-中（~0.5–1 天）+ 分区 e2e（paimon + iceberg identity/bucket/truncate + hive + hudi，含非字符串列真 NULL 分区）。

---

## 组 2 — hive LZO 输入格式判别 → 纯删

**成员**：`HiveUtil.isLzoInputFormat:129`、`isLzoDataFile:141`、`isSplittable:114`。

**活性核实（全死）**：
- `BindSink.bindHiveTableSink:678`：`UnboundHiveTableSink` 仅 `UnboundTableSinkCreator:61/94/129` 创建，全被 `instanceof HMSExternalCatalog`
  守卫（翻闸后真 catalog 是 `PluginDrivenExternalCatalog`→`UnboundConnectorTableSink`），故 bindHiveTableSink 永不触达。
- `BaseExternalTableDataSink.getTFileFormatType:86`：仅经死 `HiveTableSink` 触达（`PluginDrivenTableSink` 只 override `supportedFileFormatTypes()`，从不调它）。
- `HiveExternalMetaCache.loadFiles:395/398/405`：死 legacy 缓存，仅死 `HiveScanNode` 读。

**连接器早已自带全部 LZO 能力**：扫描判别/切分掩码/`.lzo` 数据文件过滤 = `HiveScanPlanProvider.isLzoInputFormat:551`/`isLzoDataFile:560`/`:138,248`；
写入拒绝 = `HiveSinkHelper.getTFileFormatType:84`（经 `HiveWritePlanProvider:195/262`）。**已有测试** `HiveScanPlanProviderLzoFilterTest`/`HiveTableFormatDetectionTest`。

**处置**：纯删三方法 + 其死消费点，随 trap-tier 批删。无新代码、无 SPI、无连接器改动。往任何 fe-core 通用节点内联 `contains("lzo")` 会撞 A 且无必要。

**对抗修正（文档口径）**：INSERT-LZO 拒绝的活归宿是 `HiveSinkHelper.getTFileFormatType:84`（原设计误写 `HiveConnectorMetadata:1800`，
该处无 LZO 逻辑）。唯一行为差 = 时机/异常类型（bind 期 `AnalysisException` → 写规划期 `DorisConnectorException`）；若有 e2e 断言 bind 期文案，改测试对齐连接器。

**排序注意**：`isSplittable:114` 引用 `HMSExternalTable.SUPPORTED_HIVE_FILE_FORMATS`，须与 `HMSExternalTable` **同批删**否则不编译。
顺带 `BaseExternalTableDataSink.getTFileCompressType:105`/抽象 `supportedFileFormatTypes():61` 删 `HiveTableSink` 后失去唯一调用方，同批清。

**工作量**：低（~0.5 天，纯机械删除 + 编译序核对）。

---

## 组 3 — hive 默认分区哨兵 `__HIVE_DEFAULT_PARTITION__`

**成员**：`HiveExternalMetaCache.HIVE_DEFAULT_PARTITION:114`。**两条消费路径分治**：

**(a) 查询扫描路径**（`FileQueryScanNode:473` → `FilePartitionUtils.normalizeColumnsFromPath:165`）= **活的 hive-only 铁律 A 泄漏**。
hive 是最后一个其 scan range **不**重置 `columnsFromPath*` 的连接器，故 fe-core 的 `:165` 字符串比较是它把哨兵转成 `columnsFromPathIsNull` 喂 BE 的地方。
- **处置**：经**现有** SPI `ConnectorScanRange.populateRangeParams`（iceberg/paimon 已 override）委派——给 `HiveScanRange.populateRangeParams`
  加同样的 `columnsFromPath{,Keys,IsNull}` 重置，用**窄** `HIVE_DEFAULT_PARTITION.equals` 派生 isNull（与 legacy 字节一致，**不可**用
  `ConnectorPartitionValues.normalize()`——它还会把 `\N` 也置空）。源相关逻辑落 `fe-connector-hive`；fe-core 随后**删掉** `normalizeColumnsFromPath:165` 的哨兵项（只留 `value==null`）。**无新 SPI**。

**(b) 加载路径**（`FileGroupInfo:269/316`、`NereidsFileGroupInfo:280/333` → `parseColumnsFromPathWithNullInfo:143`）= **无连接器**
（broker/stream load 解析用户给的 hive-布局 `key=value` 路径）。没有可委派对象，哨兵是**通用文件格式约定**（Doris broker load 多年独立于任何 catalog 就懂 hive 布局）。
- **处置**：保留比较，只把**常量引用**从被删的 hive 类改指**已存在**的中立常量 `ConnectorPartitionValues.HIVE_DEFAULT_PARTITION`（`fe-connector-api:26`，fe-core 已依赖 fe-connector-api）。**不新增任何 fe-core 声明**。

`FileQueryScanNode:471` 的路径解析回退分支 + `HudiScanNode:335` 死；`HMSAnalysisTask:115` trap-tier 死。

**fe-core 改动**（`FilePartitionUtils`，三处机械改）：import 换成中立常量；加载路径 `:143` 换常量来源；查询路径 `:165` 删哨兵项只留 `value==null`。

**对抗结论**：SOUND。攻击（"加载路径仍留 hive token"）在铁律字面上**失败**——A 禁的是**增加**源相关，不禁保留既有通用行为；且加载路径无连接器可委派，
强行委派会**弄坏**它。净 fe-core token **减少**（查询路径哨兵项移入 hive 插件）。

> **实现前置 trace（2026-07-13 已核，供施工）**：活查询路径 = `PluginDrivenScanNode extends FileQueryScanNode`，复用继承的
> `createScanRangeLocations:455` → `fileSplit.getPartitionValues()` 非 null 走 `normalizeColumnsFromPath(...)`(`:473`)（`PluginDrivenSplit.getPartitionValues:82`
> 由 `scanRange.getPartitionValues()` Map 转来）→ `createFileRangeDesc:552` 仅当 `columnsFromPathKeys`(=`pathPartitionKeys`)非空时 set
> `columnsFromPath/Keys/IsNull` → `setScanParams:1606` override → `populateRangeParams:1617`。iceberg/paimon 在 `populateRangeParams`
> **unset+重建** columnsFromPath（`IcebergScanRange:110-125`）覆盖掉 `:165` 的结果；**hive 未重置**→ 故 hive 是 `:165` 哨兵的最后消费者。
> **施工**：① `HiveScanRange.populateRangeParams` 加 `unsetColumnsFromPath{,Keys,IsNull}` + 从 `partitionValues` Map 重建（key/value/isNull，
> **窄** `HIVE_DEFAULT_PARTITION.equals(value)`→ value `""`+isNull `true`），镜像 iceberg；② `FilePartitionUtils.normalizeColumnsFromPath:165`
> 删哨兵项只留 `value==null`；③ `parseColumnsFromPathWithNullInfo:143`（加载路径）常量改指 `ConnectorPartitionValues.HIVE_DEFAULT_PARTITION` + import 换。
> **动手前仍须核**：`HiveScanRange.partitionValues` 由 hive scan plan provider 如何填（key=分区列名？null 是否存哨兵串？）、与 `transactional_hive`
> ACID 分支的交互、以及重建后与当前 `normalizeColumnsFromPath` 输出**逐字节一致**（含 columnsFromPathKeys 顺序，BE 按 key 名匹配）。加一条 hive 真 NULL 分区回归。

**⚠ 新揪出的第二处同形泄漏（原 §3 漏列）**：`TablePartitionValues.HIVE_DEFAULT_PARTITION:47`，被**活的** `MetadataGenerator:2166`
（`partition_values` TVF）字符串比较。只删 `HiveExternalMetaCache` 那份会留下这份同样的泄漏。**须单列一组**做自己的活性判定（trap-tier 死 vs 连接器空标志委派），否则"泄漏只是被搬家"。

**待定**：① 加载路径哲学 = 改指现有中立常量（**推荐**，最少代码）vs 新铸中立 `FileFormatConstants.DEFAULT_PARTITION_NAME`（弱于 B 且无收益）。
② 字节一致契约（用窄 `.equals` 不用 `normalize()`）。③ 第二处泄漏是否纳入本轮 / 单列后续。

**工作量**：小（~0.5 天）+ hive 真 NULL 分区回归 + broker-load 回归。

---

## 组 4 — iceberg 行血缘列（iceberg v3 规范 `_row_id` / `_last_updated_sequence_number`）

**成员（`IcebergUtils`）**：`ICEBERG_ROW_LINEAGE_MIN_VERSION=3:185`、`ICEBERG_ROW_ID_COL:186`、`ICEBERG_LAST_UPDATED_SEQUENCE_NUMBER_COL:187`、
`getEffectiveIcebergFormatVersion:197`、`isIcebergRowLineageColumn(Column):1825`、`isIcebergRowLineageColumn(String):1830`。**三分处置**：

**(1) 3 常量 + 两个 `isIcebergRowLineageColumn` → 纯删 fe-core**：连接器已有私有副本
（`IcebergConnectorMetadata:113-117`、`IcebergSchemaUtils:95-96`、`IcebergPredicateConverter:92`、`IcebergScanPlanProvider:158`）。

**(2) "保留列"身份 → 中立能力位**（给通用消费者 `BindExpression` + 两个 KEEP 的 iceberg 命令 `IcebergMergeCommand`/`IcebergUpdateCommand`）。
fe-core 不再字符串匹配 `_row_id`，改问连接器声明的"保留列"集合。**两种形态**（用户择一）：
- **形态甲（对抗推荐，更轻）**：per-column 中立布尔位。保留标记**本就跨 SPI**——`ConnectorColumnConverter:80-90` 已把
  `setIsVisible(false)` + 连接器保留 `uniqueId`（iceberg v3 `_row_id=2147483540`）重贴到 `Column`。在 `ConnectorColumn`→`Column` 旁加一个
  连接器无关布尔 `reservedPassthrough`（连同现有 invisible/uniqueId 重贴），消费者读 `column.isReservedPassthrough()`：既给成员判定，
  又给 `IcebergMergeCommand:342/343` 按 schema 序枚举名字所需的有序集——**无 metadata 往返、无 table 穿线**。
- **形态乙**：table 级 `List<String> getReservedColumnNames()`（`ConnectorMetadata` default 空，仅 iceberg override），
  经 `getMetadata(session)` 运行时往返。仿 `supportsTableSample`。

**(3) `getEffectiveIcebergFormatVersion`（`import` iceberg 库 `CatalogProperties/TableProperties`）+ `CreateTableInfo` 的 iceberg v3 建表校验
→ 经现有建表 SPI 委派**。iceberg 库类**不能**待在 fe-core 通用 classpath（且 B 禁把它捞进 fe-core util），故格式版本解析 + 保留名冲突拒绝
落 `IcebergConnectorMetadata.createTable`（经**已存在**的 `PluginDrivenExternalCatalog.createTable:396` → `ConnectorTableOps.createTable(session, request):203` seam）。
- 注：建表期**没有**转换后的 `Column`（用户在定义列），故此路只能走连接器 createTable 委派，**不能**用形态甲的 per-column 位——两机制**互补非二选一**。

`IcebergScanNode:833`（死 trap-tier 扫描节点）+ `IcebergMetadataOps:352/359`（仅死 `HMSExternalCatalog.getIcebergMetadataOps` 触达）= 随 legacy 包纯删。

**fe-core 消费者改动**：`BindExpression.isIcebergMergeMetaColumn:351-359`（保留引擎内建列检查，iceberg 项改读中立保留位）；
`CreateTableInfo:798-799,1145-1173`（删校验方法 + iceberg gate，改连接器侧校验）；`IcebergMergeCommand`（**7** 处：176/201/260/336/342/343/369，
取一次保留集、predicate 改成员测试、`:342/343` 字面改按保留集迭代）；`IcebergUpdateCommand:106`（改中立保留位测试）。

**对抗结论**：SOUND，但**原设计的 table 级 SPI 偏重**——对抗验证证明 per-column 保留标记本就跨 SPI（`ConnectorColumnConverter`），
故**形态甲 per-column 布尔位**更轻、复用现有机制、零往返，**推荐**。两个校正：字符串成员测试须**大小写不敏感**（原 `equalsIgnoreCase`，别退化成 `List.contains`）；
`IcebergMergeCommand` 是 7 处不是 3 处。

**待定**：① 保留列 SPI 形态（甲 per-column 位 **推荐** / 乙 table 级 list）。② 建表校验**时机/文案**：今日 `CreateTableInfo.validate()`
抛 `AnalysisException`（前置，"Cannot create Iceberg v3 table with reserved row lineage column: ..."）→ 移入连接器后变 `DorisConnectorException`（靠后）。
接受时机/异常类型变化，还是加一个可选 `validateCreateTable` 前置钩子保精确时机？③ **既存债，非本轮**：`IcebergMergeCommand`/`IcebergUpdateCommand`
仍是 fe-core 里 iceberg 命名的活类（经 capability gate 活，非 legacy instanceof）；`Column.ICEBERG_ROWID_COL:63` 是既存 rule-A 债
（Doris 合成 `__DORIS_` 隐藏列，~40 引用）——**不当作 sanctioned 家、不新铸 `Column._row_id`**。二者记为后续清理，不在本轮解决。
④ 测试重锚：钉这些字面的 fe-core 契约测试须移到连接器侧。

**工作量**：中（~1–2 天）+ 建表 v3 拒绝 + MERGE/UPDATE 行血缘透传的 iceberg e2e。

---

## 对 §5 拓扑序的影响（步 2 重写）

原「步 2 = 抽取 4 组活成员搬中立家」改为：

- **步 2a（连接器侧先行，独立小改 + 单测）**：① `ConnectorPartitionInfo` 加有序值字段 + 4 连接器填值（组 1）；
  ② `HiveScanRange.populateRangeParams` 加 columnsFromPath 重置（组 3-a）；③ iceberg 保留列能力位 + `IcebergConnectorMetadata.createTable`
  吸收 v3 校验（组 4-2/4-3）。
- **步 2b（fe-core 消费者改经 SPL/常量委派）**：`toListPartitionItem` 改 zip 连接器值（组 1）；`FilePartitionUtils` 三处改（组 3）；
  `BindExpression`/`CreateTableInfo`/`IcebergMergeCommand`/`IcebergUpdateCommand` 改读中立位/删校验（组 4）。
- 组 2（LZO）**无步 2**，直接并入步 4 的 trap-tier 机械删。
- 步 3/4（切死臂 + 删单元）**在步 2 全绿后**方可删 `HiveUtil`/`HiveExternalMetaCache`/`IcebergUtils`（组 1 须先确认 4 连接器全接线 + fail-loud）。

每步后 fe-core `test-compile` 必绿。连接器侧改动按 paimon 用 `install` 验证。

---

## 待用户决策汇总（review 时定）

1. **总方向**：认可这份"零 fe-core 源相关新增"的合规重分析取代原「搬中立家」方案？
2. **组 4 保留列 SPI 形态**：per-column 中立布尔位（推荐，轻）vs table 级 list SPI。
3. **组 4 建表校验**：接受校验移连接器侧带来的时机/异常类型/文案变化，还是加前置钩子保精确时机？
4. **新揪出的第二处哨兵泄漏**（`partition_values` TVF 路径）：纳入本轮，还是单列紧邻后续？
5. 组 1 SPI 形态（两列表 vs 配对对象）· 组 3 加载路径常量（改指现有中立常量 vs 新铸）——均有推荐，无异议即按推荐。
