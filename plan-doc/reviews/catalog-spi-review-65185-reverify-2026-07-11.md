# #65185 第三方评审 — 对当前分支 `catalog-spi-11-hive` 的逐条核实

> **输入**：`plan-doc/reviews/catalog-spi-review-65185.md`(第三方评审，基线 = `upstream-apache/branch-catalog-spi` tip `3ba75b7cf8a`，**仅含 P1–P6，无 P7/hive**)。
> **核实对象**：当前本地分支 `catalog-spi-11-hive`(HEAD `3b6985c7c88`),**HMS SPI 原子翻闸已完成**。
> **方法**:多 agent 工作流(3 recon + 34 finding-cluster 核实 + 对每条存活的高/中发现做对抗证伪),按 **符号/行为** 定位现码(报告行号已过时),交叉核对 `deviations-log`/`decisions-log` 避免把已验收偏差当新 bug。79 条结论,62 条存活为真/活跃。
> **纪律**:不动代码,仅核实分析。所有 file:line 均对 HEAD 核过。

---

## 0. 一句话结论

报告的核心前提**已过时**:它写于「hive 未迁移、hudi 休眠」的分支;当前分支 `"hms"` 已进 `SPI_READY_TYPES`(`CatalogFactory.java:55`),**plain-hive + iceberg-on-HMS + hudi-on-HMS 全部改走插件路径**(hudi 作为 hms 网关的兄弟连接器)。因此报告的每条结论必须重新落到「当前分支」这一坐标:

- **报告的「休眠 P7 地雷」中,hudi 的 4 个高危已被翻闸激活为真实回归**(silent 丢行 / JNI 崩溃),这是本次核实**最重要的产出**,比报告更严重。
- 翻闸配套的大量 P7 工作(hive 耦合缝、连接器缓存失效、事件同步、hive 写事务、HD-A4 表类型硬化)**已修掉若干报告条目**。
- 与连接器无关的 P0/P2/P4/P5/P6(iceberg/paimon/maxcompute)发现**基本仍然成立**。
- 一批被对抗证伪 / 属 parity / 已登记验收 的条目**在当前分支不构成需修问题**,单列一节说明(§6),以免误改。

**行动优先级**:先处理 §2 的 4 个 hudi 高危(翻闸即静默丢行/崩),再看 §3 的中危。

---

## 1. 严重程度总表(仅列「真实/活跃」的条目)

> 严重度取「对抗验证后的最终值」;`新激活` = 报告标休眠、翻闸后变活;`仍然` = 与连接器无关一直存在。

| # | ID | 现状 | 严重度 | 范围 | 一句话 |
|---|---|---|---|---|---|
| H1 | P3-hudi-unescape | 新激活 | 🔴高 | hudi-on-HMS | 分区名不 unescape,含 `:/%/空格/=` 的值静默剪掉→丢行 |
| H2 | P3-hudi-datetime | 新激活 | 🔴高 | hudi-on-HMS | datetime 分区谓词渲染成 ISO(`T`、省秒)永不等于 Hive 文本→整表剪到 0 行 |
| H3 | P3-hudi-storagepath | 新激活 | 🔴高 | hudi-on-HMS | 剪枝把 HMS hive-style 名当 Hudi 存储路径喂 fsView→非 hive-style 表带 filter 0 split |
| H4 | P3-hudi-avro-jni | 新激活 | 🔴高 | hudi-on-HMS | JNI 列名发原始大小写,BE 精确匹配 lowercase slot→每个 MOR/JNI split 崩 |
| M1 | P1-2 | 新激活 | 🟠中 | hive | `TABLESAMPLE` 在插件路径静默全表扫(不转发+无采样管线) |
| M2 | P4-batch-hive | 新激活 | 🟠中 | hive | 翻闸 hive 完全丢批量/异步 split(连接器未 opt-in 任何 batch flavor) |
| M3 | P4-1 | 仍然 | 🟠中 | maxcompute | batch 闸门 `!=NOT_PRUNED`→`!isPruned`,无谓词大分区表丢异步 batch |
| M4 | P4-2 | 仍然 | 🟠中 | maxcompute | 分区值缓存删除,每次规划一次全量 ODPS `listPartitions` |
| M5 | P6-3 | 仍然(iceberg-on-HMS 新激活) | 🟠中 | iceberg | `computeRowCount` 丢 equality-delete gate→MOR/CDC 表统计膨胀误导 CBO |
| M6 | P6-4 | 仍然 | 🟠中 | iceberg | s3tables 无显式凭证即硬失败,丢 EC2 instance-profile 默认链 |
| M7 | P6-5 | 仍然 | 🟠中 | iceberg | REST vended-cred region 别名收窄,丢 `AWS_REGION`/`*.signing-region`→"Unable to load region" |
| M8 | P6-2 | 仍然(hms 放大) | 🟠中(运营) | 全 SPI | 升级只换 `lib/` 不部署 `plugins/connector`→所有存量 hms/iceberg 目录首访抛异常 |
| L1 | P0-5 | 仍然 | 🟡低 | 门禁 | import-gate 三洞(漏 `import static`/漏 6 个包/只扫 main) |
| L2 | P1-3-sqlcache | 新激活 | 🟡低 | hive | 翻闸 hive 丢 SQL 结果缓存资格 + `COUNTER_QUERY_HIVE_TABLE` 停增 |
| L3 | P2-1 | 仍然 | 🟡低 | trino | 元数据方法开 Trino 事务从不 commit/close(与 master parity,量级放大) |
| L4 | P2-2 | 仍然 | 🟡低 | trino | `getInstance(pluginDir)` 首胜单例 vs per-catalog `trino.plugin.dir`(fail-loud) |
| L5 | P2-4 | 仍然 | 🟡低 | trino | `listTableNames` 丢 LinkedHashSet 去重 |
| L6 | P2-5 | 仍然 | 🟡低 | trino | guard 字段先于其余字段发布→并发首访瞬时 NPE(volatile 自愈) |
| L7 | P3b-2 | 仍然 | 🟡低 | kerberos | Kerberos 认证器每次 login/refresh 无条件 `UGI.setConfiguration`+强设 authorization(丢 first-writer guard);metastore 路 parity |
| L8 | P3b-4 | 仍然 | 🟡低 | kerberos | `doAs` 把 `InterruptedException`→`IOException` 不 restore interrupt |
| L9 | P4-4 | 仍然 | 🟡低 | maxcompute | 谓词下推全有全无(一个不可转 conjunct 丢整个 filter);perf-only |
| L10 | P5-1 | 仍然(hive 新激活) | 🟡低 | 全 SPI | EXPLAIN 节点名 `V{PAIMON,ICEBERG,HIVE}_SCAN_NODE`→`VPluginDrivenScanNode`;未登记 |
| L11 | P5-2 | 仍然 | 🟡低 | paimon | JNI/COUNT range 用表级 `file.format`(默认 parquet)非首文件后缀;默认 JNI reader 不消费故影响窄 |
| L12 | P5-3 | 仍然 | 🟡低 | paimon/iceberg | `selectedPartitionNum` 语义:SDK-distinct→Nereids-剪枝数;影响 EXPLAIN + `sql_block_rule` |
| L13 | P5-4 | 仍然 | 🟡低 | paimon | to-Paimon 建表丢嵌套 nullability(comment 半已登记 DV-035c) |
| L14 | P5-5 | 仍然 | 🟡低 | paimon | `ignore_split_type` session 变量插件路径静默 no-op |
| L15 | P5b-1 | 仍然 | 🟡低 | paimon | `PAIMON_SCAN_METRICS` 常量悬空 0 writer;FE 侧 paimon scan 剖面回归(已登记) |
| L16 | P6-1 | 部分已修 | 🟡低 | iceberg | 快照缓存 vs schema 缓存偏斜的结构隐患仍在,但触发已被同 TTL+原子失效大幅收窄 |
| L17 | P6-6 | 仍然 | 🟡低 | iceberg | 同表多版本自连接:schema 绑定 version-blind vs scan version-aware→字段 id 偏斜(窄触发,fail-loud) |
| L18 | P6-9 | 仍然 | 🟡低 | iceberg | 未知/v3 类型(TIMESTAMP_NANO/GEOMETRY…)静默降 UNSUPPORTED,legacy schema-load 抛 |
| L19 | MAGICKEY-collision | 仍然 | 🟡低 | 全 SPI | `partition_columns` 魔法键与源 TBLPROPERTY 撞名→非分区表被误当分区(窄) |
| L20 | P4-3-EQ | 待实测 | 🟡低 | maxcompute | EQ 发 `==`(SDK/legacy 是 `=`);ODPS 是否容忍需 live A/B |
| — | (已登记/验收的低危) | — | 🟡低 | — | P4-5(DV-018)、P4-6(DV-034)、P4-SHOWPART:limit(DV-021)、P4-SHOWPART:where、P6-7、P6-8、P6-10(DV-049③) — 见 §5 |
| — | (设计债 15 条) | — | ⚪ | — | 见 §4(含 **DUPLICATION:partition-prune** — 承载 H1–H3 的复制块,修复须一处落地) |

---

## 2. 🔴 高危(4 条,均由 HMS 翻闸新激活,均在 hudi-on-HMS 上)

> 共同背景:hudi 表寄生于 HMS 目录,翻闸后经 hive 网关 `createSiblingConnector("hudi")` → `HudiConnectorMetadata.applyFilter`(由 `PluginDrivenScanNode.convertPredicate:648` 实时调用)。报告标 CONFIRMED-but-dormant,现全部 **可达**。四者集中在同一段被复制的 EQ/IN 剪枝代码里(见 §4 DUPLICATION:partition-prune),修复应在共享 helper 一处落地。

### H1 — hudi 分区值不 unescape → 静默丢行

- **现证据**:`HudiConnectorMetadata.parsePartitionName:1054-1064` 用 `part.substring(eq+1)` 存值,**无 unescape**;`matchesPredicates:1067-1078` 直接 `allowedValues.contains(actualValue)` 字符串比。候选名来自 `hmsClient.listPartitionNames:247`(`ThriftHmsClient.listPartitionNames:214-218` 原样转发 HMS,返回 Hive 转义名如 `ts=2024-01-01 10%3A00%3A00`);谓词侧 `extractLiteralValue:1030-1036` 返回未转义值 `2024-01-01 10:00:00`。二者永不相等→`matched=0`,而 `matched(0) != all(N)` 使 `applyFilter:255` 的 bail 不触发→`prunedPartitionPaths` 空→`resolvePartitions:587` 返回空→扫 0 分区→**对真实存在的值返回 0 行**。
- **对比 legacy**:`HudiExternalMetaCache.loadPartitionNames:231` 对 HMS 名做 `FileUtils::unescapePathName`,再按 typed Nereids key 剪枝。故这是**回归**,非 parity。
- **P7 是否已修**:否。P7 的 unescape 修复落在**另一个**方法 `HudiScanPlanProvider.parsePartitionValues:723-737`(scan 侧值保真),`HudiPartitionValuesTest` 覆盖的是它;**剪枝决策用的 `parsePartitionName` 未改、未测**(`HudiPartitionPruningTest` 只用干净的 `year=2024`)。
- **触发面**:仅当分区值含 Hive 转义字符(时间戳、含空格/特殊字符的字符串);纯日期/整数不受影响。但触发时是**静默数据丢失**,无报错。
- **解决方案**:在 `parsePartitionName` 存值前 unescape(复用连接器已有的 `HudiScanPlanProvider.unescapePathName` 或 Hive `FileUtils.unescapePathName`),对齐 legacy。补 `HudiPartitionPruningTest`:HMS 名 `ts=2024-01-01 10%3A00%3A00` + 谓词 `ts='2024-01-01 10:00:00'` 断言命中而非剪掉。**首选在 §4 的共享 helper 里修一处**。

### H2 — hudi datetime 分区谓词 ISO 化 → 整表剪到 0 行

- **现证据**:`ExprToConnectorExpressionConverter.convertDateLiteral:309-322` 把非 DATE 的 datetime 字面量包成 `LocalDateTime`;`extractLiteralValue:1030-1036` 用 `String.valueOf(val)` = `LocalDateTime.toString()`,对 `2024-01-01 10:00:00` 产出 `2024-01-01T10:00`(ISO 用 `T` 分隔、**省略零秒**)。`matchesPredicates` 纯字符串比 HMS 值文本 `2024-01-01 10:00:00`(空格分隔、全秒)→**永不相等→每个分区被剪→0 行**。DATE 列安全(`LocalDate.toString()`=`2024-01-01` 匹配)。
- **对比 legacy**:hudi 分区走 Nereids typed 剪枝(`HudiExternalMetaCache:205` 喂 typed 列类型),不做字符串比,故不受影响。**回归**。
- **P7 是否已修**:否。
- **解决方案**:对时间类型不做字符串剪枝——`extractLiteralValue` 把 `LocalDateTime` 按 Hive 规范分区文本(空格分隔、全 `HH:mm:ss`)渲染;更对齐 legacy 的是让 `matchesPredicates` type-aware(两侧过列类型),或对时间列干脆退回 fe-core 的 typed Nereids 剪枝。补 DATETIME 分区列 + `= '2024-01-01 10:00:00'` 的剪枝测试。
- 注:此条的对抗证伪 agent 未完成(schema 重试超限),但机制确定、与 H1/H3 同源且二者已 CONFIRMED,判定保留 🔴高。

### H3 — hudi 把 HMS 名当存储路径喂 fsView → 非 hive-style 表带 filter 0 split

- **现证据**:`applyFilter:244-267` **无条件**把 `prunedPartitionPaths` 设为 `hmsClient.listPartitionNames` 的子集(hive-style 名 `year=2024/month=01`);`resolvePartitions:587-590` 原样返回,`collectCowSplits:394`/`collectMorSplits:429` 把它当**第一参**传给 `fsView.getLatestBaseFilesBeforeOrOn(partitionPath,...)`,而 FileSystemView 按 **Hudi 相对存储路径** 索引。对 `hive_style_partitioning=false`(Hudi 默认)的表,物理布局是 `2024/01` 而 HMS 名是 `year=2024/month=01`→fsView 找不到文件→0 split;**不带 filter 时** `resolvePartitions` 回退 `listAllPartitionPaths`(Hudi 元数据,正确)→有行。即报告的"带 filter 0 split、不带 filter 有行"。
- **对比 legacy**:默认 `use_hive_sync_partition=false` 从 Hudi 元数据 `getAllPartitionNames` 取相对路径;hive-sync 分支也经 `FSUtils.getRelativePartitionPath` 相对化(`HudiScanNode:410-411`)。**回归**。连接器**自己的**列举路 `collectPartitions:636-665` 是 `use_hive_sync` 感知的,但 `applyFilter` 绕过了它。`HudiScanPlanProvider:716-721` 的 javadoc 自称此坑"翻闸前已闭合"是**过时的**。
- **P7 是否已修**:否(HD-A3 只修了分区**值**保真 + 列举路)。
- **解决方案**:让 `applyFilter` 的候选分区源 `use_hive_sync_partition` 感知,复用 `collectPartitions`:`!useHiveSyncPartition` 时对 `listAllPartitionPaths`(相对路径 `2024/01`)剪枝;hive-sync 时用 `FSUtils.getRelativePartitionPath` 相对化 HMS LOCATION。补非 hive-style 表断言"带 filter 分区集 == 不带 filter"。

### H4 — hudi 混大小写 Avro 列名崩 JNI/MOR reader

- **现证据**:两处 lowercase 修的是**别的**路径——Doris 列 schema(`HudiConnectorMetadata.avroSchemaToColumns:905` `.toLowerCase(ROOT)`)和 native `history_schema_info` 字典(`HudiSchemaUtils.buildField:137`)。但 JNI reader 的列名列表**仍是原始大小写**:`HudiScanPlanProvider.planScan:180-181` `.map(Schema.Field::name)` 无 lowercase,经 `HudiScanRange:220` 塞进 `THudiFileDesc.column_names`。BE `HadoopHudiJniScanner.initRequiredColumnsAndTypes:212-229` 用**原始大小写** key 建 `hudiColNameToType`,再对每个 **lowercase** 的 `requiredField` 做精确 `containsKey`,不命中即 `throw IllegalArgumentException`——`Id/Name/Addr` 表下 lowercase `id` 不在 `{Id,Name,Addr}`→**每个 JNI split 抛**。
- **对比 legacy**:`HMSExternalTable:749` 把 Avro 名 lowercase 进 Column,`HudiScanNode:223` `map(Column::getName)` 发 lowercase 名,匹配。**回归**。该 PR 自己的 `HudiSchemaParityTest`(用 `Id/Name/Addr`)只断言 `avroSchemaToColumns` 和 native 字典,**从不断言 JNI 列名列表**,抓不到。
- **触发面**:混大小写列 **且** MOR-带-log 或 `force_jni`;纯 COW / 全小写不受影响。但触发时**每个 split 硬崩→整查询失败**。
- **解决方案**:`HudiScanPlanProvider.planScan:181` 把 `.map(Schema.Field::name)` 改为 `.map(f -> f.name().toLowerCase(Locale.ROOT))`(与 `:905`/`HudiSchemaUtils:137` 一致);列类型顺序不变仍对齐;Hive ObjectInspector 大小写不敏感,文件读仍解析。给 `HudiSchemaParityTest` 补 JNI 列名断言。

---

## 3. 🟠 中危(8 条)

### M1 — P1-2 `TABLESAMPLE` 在翻闸 hive 上静默全表扫(新激活)

- **现证据**:`PhysicalPlanTranslator.visitPhysicalFileScan:812` **先**匹配 `PluginDrivenExternalTable`,只 `setSelectedPartitions:820`,**不转发 `getTableSample()`**(唯一转发在 legacy hive 臂 `:837-840`,翻闸后死)。`PluginDrivenScanNode.getSplits:998-1019` 从 connector ranges 1:1 建 split,**零** tableSample 引用;`ConnectorScanPlanProvider.planScan:119` 也无采样参数。样本已由 `BindRelation:812-816` 带进 `LogicalFileScan`,故 `SELECT ... TABLESAMPLE(...)` **静默丢弃**(非报错)→返回全表基数。
- **对比**:翻闸前 hive 走 `HiveScanNode`(`:332,447-463`)会应用采样。无登记偏差(recon 确认 TABLESAMPLE 不在 deviations/decisions)。现有 `test_hive_tablesample_p0.groovy` 只断言 EXPLAIN 子串,抓不到。
- **解决方案**:(1)translator 插件臂在 `setSelectedPartitions` 后镜像 legacy:`if (fileScan.getTableSample().isPresent()) pluginScanNode.setTableSample(...)`;(2)在 `PluginDrivenScanNode.getSplits` 实现**通用、connector-agnostic** 的按 split 大小采样(仿 `HiveScanNode:448-458`,对通用 `PluginDrivenSplit` 大小操作,不按源分支)。强化 groovy 断言采样后基数。

### M2 — P4-batch-hive:翻闸 hive 完全丢批量/异步 split(新激活)

- **现证据**:翻闸后 hive 表全走 `PluginDrivenScanNode`。`computeBatchMode:1085-1113` 需连接器 opt-in:`streamingSplitEstimate`(仅 iceberg override)或 `supportsBatchScan`(仅 MaxCompute override)。`HiveScanPlanProvider` 二者都不 override(javadoc `:62` 自称"No batch/lazy split mode"),继承 `ConnectorScanPlanProvider.supportsBatchScan=false`(`:231`)→`shouldUseBatchMode` 恒 false→**同步 `getSplits`**,把所有选中分区的所有文件 split 一次性物化进 FE 堆。
- **对比 legacy**:`HiveScanNode.isBatchMode:283-294` 纯按 `prunedPartitions.size() >= num_partitions_in_batch_mode`(默认 1024)启用异步 `startSplit` streaming,**无 opt-in gate**。hive 是实际最大分区源→FE 堆/延迟影响潜在最大。fail-safe(结果正确)。D2 连接器缓存只恢复列举**性能**,不恢复异步/streaming split。
- **解决方案**:给 hive 连接器加 batch 通路——override `HiveScanPlanProvider.supportsBatchScan`(分区表 true)+ `planScanForPartitionBatch`,仿 `MaxComputeScanPlanProvider`,无需改 fe-core。或明确登记为验收偏差 + 大分区 e2e 真值门(**当前既未修也未登记**)。

### M3 — P4-1:MaxCompute batch 闸门 `!=NOT_PRUNED`→`!isPruned`(仍然)

- **现证据**:`PluginDrivenScanNode.shouldUseBatchMode:1136` 是 `if (selectedPartitions == null || !selectedPartitions.isPruned) return false;`;legacy `MaxComputeScanNode:227` 是 `!= NOT_PRUNED`。二者**不等价**:无谓词分区表 `ExternalTable.initSelectedPartitions:447` 返回 `SelectedPartitions(size, fullMap, isPruned=false)`——非 `NOT_PRUNED` 哨兵。legacy→batch;SPI→`!false`→返回 false→**无 batch**。`MaxComputeScanPlanProvider.supportsBatchScan:254` opt-in(`getFileNum()>0`),默认 `num_partitions_in_batch_mode=1024`。故 `SELECT col FROM odps_t`(≥1024 分区、无谓词)走同步全枚举,FE 规划延迟+堆压。行内 `:1129-1132` 注释自称 parity **是错的**(只覆盖非分区 `NOT_PRUNED` 情形);同作者在 `displayPartitionCounts:297` 却正确用 `!= NOT_PRUNED`。
- **范围澄清**:**仅 MaxCompute**(唯一 opt-in `supportsBatchScan`);iceberg 走 streaming flavor 不看 `isPruned`;hive 是**另一根因**(M2)。DV-019/D-035 只登记了 wiring/test-gap 并**误称 parity**,该语义分歧未登记。
- **解决方案**:`:1136` 改回 `selectedPartitions == SelectedPartitions.NOT_PRUNED`(非分区表仍 →false;无谓词分区表落到 `:1145` 的 size≥阈值 检查),对齐 legacy;补纯 helper 单测覆盖 `(isPruned=false, 非 NOT_PRUNED, size≥阈值)`;订正 `:1129-1132` 注释。

### M4 — P4-2:MaxCompute 分区值缓存删除(仍然)

- **现证据**:`PluginDrivenExternalTable.getNameToPartitionItems:780-781` 每次无条件 `metadata.listPartitions(...)`(注释 `:776-779` 自认"no FE-side partition-value cache");连接器 `MaxComputeConnectorMetadata.listPartitions:256-273`→`McStructureHelper.getPartitions:112-118` = 裸 ODPS SDK 调用(javadoc `:253-254` 自认"no connector-side cache")。FE、连接器两层**都无缓存**。`initSelectedPartitions:439-447` 每次规划调一次→**每查询一次全量 ODPS `getPartitions()`**。
- **对比 legacy**:`MaxComputeExternalMetaCache` 的 TTL Caffeine `partitionValuesEntry`,重复查询零 ODPS 往返。数万分区表每次规划多秒 + 限流风险。correctness 安全(BE 重过滤)。**hive/hms 不受此累**——P7 给 hive 加了连接器自有缓存(`CachingHmsClient`,commit `f742651990d`);唯 maxcompute 显式弃缓存。
- **解决方案**:在 maxcompute 连接器内加 TTL/容量 Caffeine(仿已落地的 `CachingHmsClient`/`HiveFileListingCache`),keyed `(project,db,table)`,`REFRESH` 失效——**不**在 fe-core 加二级缓存(违背 connector-owned 方向)。若延后,须在 deviations-log 正式登记(CACHE-P1 从未登记)。

### M5 — P6-3:iceberg `computeRowCount` 丢 equality-delete gate(仍然;iceberg-on-HMS 新激活)

- **现证据**:`IcebergConnectorMetadata.computeRowCount:631-646` 只读 `total-records`、`total-position-deletes`,无条件返回 `totalRecords - positionDeletes`,无 equality-delete gate(类只声明 `TOTAL_RECORDS`/`TOTAL_POSITION_DELETES`)。其 javadoc(`:624-629`)与单测 `equalityDeletesDoNotGateTableStatistics:195-210` 断言"legacy 不 gate"——**此前提为假**:legacy `IcebergUtils.getIcebergRowCount:1202`→`getCountFromSummary`,`:231-233` `if (!equalityDeletes.equals("0")) return UNKNOWN_ROW_COUNT`。COUNT(*) 下推副本 `IcebergScanPlanProvider.getCountFromSummary:1572-1574` **保留**了 gate→**不对称**:有 equality-delete 时 COUNT(*) 正确退让,但 `getTableStatistics` 报膨胀行数→误导 CBO。
- **翻闸影响**:type=iceberg 自 P6 已在 SPI(故"仍然");但翻闸让 **iceberg-on-HMS** 也走 `computeRowCount`(legacy HMS 路 `HMSExternalTable:547` 走带 gate 的 `getIcebergRowCount`)→对 iceberg-on-HMS 是**新激活**。非登记偏差。
- **解决方案**:加 `TOTAL_EQUALITY_DELETES` 常量,`computeRowCount` 在减法前 `null || !="0"→返回 -1(UNKNOWN)`,复现 legacy;订正 javadoc + 重写把回归锁死的单测(应断言 UNKNOWN)。

### M6 — P6-4:iceberg s3tables 无显式凭证即硬失败(仍然)

- **现证据**:`IcebergConnector.createS3TablesCatalog:735-739` `if (!chosenS3.isPresent()) throw ...`;`chosenS3` 来自 `S3FileSystemProvider.supports:64-74`,要求 `hasCredential(AK/SK||roleARN||credentials_provider_type) && hasLocation`(除非 explicit-S3)。EC2 instance-profile s3tables 目录(仅 `s3.region`+warehouse ARN,无静态凭证/无 marker)→`supports()=false`→无 S3 storage→`chosenS3` 空→**建表首访抛**。
- **对比 legacy**:`IcebergS3TablesMetaStoreProperties` 直接 `S3Properties.of(origProps)`(无凭证 gate)+ `createAwsCredentialsProvider(...,false)`→无静态凭证时返回 `DefaultCredentialsProvider`(instance-profile 默认链)→region+warehouse-only 可用。现 `supports()` 比报告**更松**(接受 `credentials_provider_type`),故可用 `s3.credentials_provider_type` 绕过,但纯默认链场景仍硬失败。**回归**,fail-loud,非登记。
- **解决方案**:`createS3TablesCatalog` 在 `chosenS3` 空但能从原始 props 解析出 region 时(复用 M7 拓宽的别名),用 `DefaultCredentialsProvider`+`Region.of(...)` 建 client 而非抛,镜像 legacy;仅当既无 storage 又无 region 才 fail-loud。

### M7 — P6-5:iceberg REST vended-cred region 别名收窄(仍然)

- **现证据**:`IcebergCatalogFactory.java:83` `S3_REGION_ALIASES = {s3.region, aws.region, region, client.region}`;`appendS3FileIO:187-194` 在 `chosenS3` 空的臂上以 `firstNonBlank(props, S3_REGION_ALIASES)` 为**唯一** region 源。REST vended-cred 目录(无静态 AK/SK→`chosenS3` 空)若 region 仅经 `AWS_REGION`/`iceberg.rest.signing-region`/`rest.signing-region` 提供→`firstNonBlank` 返回 null→`CLIENT_REGION` 不发→iceberg S3FileIO 落 `DefaultAwsRegionProviderChain`→**"Unable to load region"**。javadoc `:78-83` 自称"mirror legacy `getRegionFromProperties`"**不实**:legacy 扫所有 `@ConnectorProperty(isRegionField=true)` 别名(S3 就 10 个,含上述被丢的)。
- **解决方案**:拓宽 `S3_REGION_ALIASES` 对齐 legacy `getRegionFromProperties`(至少加 `AWS_REGION`/`iceberg.rest.signing-region`/`rest.signing-region`,以及 `REGION`/`glue.region`/`aws.glue.region`);fe-connector 不能 import fe-core,连接器侧复制别名列表(与 ENDPOINT 列表同模式)。订正 `:78-83` 注释。

### M8 — P6-2:升级只换 `lib/` 不部署 `plugins/connector`(仍然;hms 放大)

- **现证据**:`PluginDrivenExternalCatalog.java:135` `throw new RuntimeException("No ConnectorProvider found...")`——连接器插件未部署时首访即抛。`CatalogFactory` 的 degraded 分支(`:119-127`)**只护 replay/启动**(注册 null-connector 目录,启动不抛),外部目录懒初始化→抛在**首查**。built-in fallback switch(`:136-157`)已删 `case "hms"`/`case "iceberg"`。
- **翻闸放大**:报告 tip 时只 iceberg 暴露(hms 仍走 legacy `case "hms"`)。翻闸把 `"hms"` 入 `SPI_READY_TYPES` 并删 legacy fallback→**所有** type=hms 目录(plain-hive+iceberg-on-HMS+hudi-on-HMS)首查即抛,hms/hive 是实际主力外部目录类型→lib-only 升级几乎击穿全部外部目录访问。
- **性质**:对抗验证把它从"高"降为"设计/运营"——因为它是**有意的 fail-loud**(消息明确、启动有 per-catalog WARN、无静默/无数据损坏),正确升级(部署 `plugins/connector`)零问题。但 blast radius 因翻闸显著扩大,仍按 🟠中(运营)排。
- **解决方案**:主线是**发布/升级工具**必须把连接器 jar 部署到 `connector_plugin_root`(build.sh/部署步骤)+ 响亮 release note"post-cutover 不支持 lib-only 换包"。代码侧防御(可选):replay 结束后聚合输出一条 ERROR 枚举所有 degraded(connector==null)目录,让运维在启动而非首查时发现。**保留** first-access throw,**不**加 legacy fallback(那会倒退翻闸)。

---

## 4. ⚪ 设计债 / 架构趋势(真实但按现行规则非"须修 bug";逐条已核实存在)

> 铁律提醒:fe-core 有意 connector-agnostic + 不解析属性(用户拍板),故"SPI 表面长出连接器词汇"是**已知设计张力**,记为设计债。以下均已对 HEAD 核实为真。

| ID | 现状 | 要点(file:line) | 建议 |
|---|---|---|---|
| **DUPLICATION:partition-prune** | **新激活·承载 H1–H3** | `HiveConnectorMetadata:1995-2093` 与 `HudiConnectorMetadata:980-1078` 的 7 方法 EQ/IN 剪枝块**逐字节相同**(仅末尾 `}` 不同),现两份都在生产路径 | **最高优先 de-dup**:抽 `HmsStylePartitionPruner` 到共享模块,H1–H3 的 unescape/相对化修复一处落地,防"修一漏一"连接器专属分歧 |
| P4-D / 5.3 引擎名 switch | 仍然·翻闸增至 4 case | 三处 fe-core 字符串 switch 手动同步:`PluginDrivenExternalTable.getEngine:1182`、`getEngineTableTypeName:1220`、`CreateTableInfo.pluginCatalogTypeToEngine:927`;javadoc 自认"两个 switch 须同步" | 引 `Connector.getLegacyEngineName()`/`getLegacyTableTypeName()` SPI,连接器声明一次,删三处 switch;或共享静态表 |
| P6-S(a) SHOW CREATE 无脱敏 | 仍然(安全) | `Env.java:4897-4907` 插件 PROPERTIES 逐字 append,无 `DatasourcePrintableMap`/SENSITIVE_KEY masking,仅靠 capability gate;`ConnectorCapability:80-82` 自认 flag 是唯一防线 | ~5 行:该处改走 `new DatasourcePrintableMap<>(props," = ",true,true,hidePassword)`,把 javadoc 握手变纵深防御。**注**:当前无可达泄漏(hive 未声明该 cap,SHOW CREATE 只出注释;paimon/iceberg 只渲染表级 option) |
| P6-S(b) 敏感键硬编码 fe-core | 仍然(安全) | `DatasourcePrintableMap.SENSITIVE_KEY` 硬列 MC/iceberg-REST/glue/dlf 键(`:57-75`,"Keep in sync"注释) | 折进已有的 `registerSensitiveKeys(...)` SPI 聚合(filesystem 已用);至少把 iceberg REST 键移到连接器注册。**masking 当前完好**,翻闸后 hms 秘密不泄 |
| P6-D1 RowLevelDmlRegistry iceberg 形 | 仍然(潜伏) | `RowLevelDmlRegistry:37` 仅 `IcebergRowLevelDmlTransform`;gate 泛化(capability)但 body 硬绑 `__DORIS_ICEBERG_ROWID_COL__`。**hive 未触发**(仅 iceberg 声明 DELETE/MERGE;hive `WritePlanProvider:124` 只 INSERT/OVERWRITE) | 加 fail-loud 契约检查(非 iceberg 连接器声明 DELETE/MERGE 时拒绝),刷新过时 javadoc;第二个 row-level-DML 连接器落地前登记验收 |
| 4.2 stringly-typed 契约 | 仍然 | `ConnectorPartitionField.transform:37`(String,未知静默 CUSTOM)、`transformArgs:38`(仅 `List<Integer>`)、`ConnectorBucketSpec.algorithm:39`(free string) | 硬化时换 enum+CUSTOM(String)、拓宽 args;近期至少共享常量类 |
| 4.3 SPI 连接器化 | 仍然·翻闸轻微放大 | `supportsWriteBlockAllocation/allocateWriteBlockRange`(仅 MC)、`deriveStorageProperties`(仅 iceberg)、7 个 branch/tag/partition-field 方法(iceberg 形)、`cleanupEmptyManagedLocation`(iceberg 单消费者)。**订正报告**:后者的 `["data","metadata"]` 在**连接器侧**(`IcebergSchemaBuilder:71`),fe-core 只收参数,未硬编码 iceberg 目录 | 连接器专属能力收进 capability-discovered facet(如 `getProcedureOps()` 范式);登记 4.3 为验收架构张力 |
| P6-D2 millis/micros 命名 | 仍然 | `ConnectorMvccPartitionView.getNewestUpdateTimeMillis:113` 名说毫秒实微秒(iceberg `last_updated_at`);javadoc 已警示 | 廉价:重命名 `getNewestUpdateMarker()`/`...Raw()`,单实现者+单消费者,零行为变更 |
| P6-D3 raw-path 字符串契约 | 仍然 | `ConnectorMetadata.applyRewriteFileScope:204`/`applyTopnLazyMaterialization:226` 的相等/全 schema 契约只活在 javadoc,违反=**静默数据损坏**非报错;单消费者 iceberg | 引 `ConnectorFilePath` 不透明 token 令相等 by-construction;debug 断言全 schema 覆盖;或登记验收 |
| MAGICKEY:channel | 仍然(未被翻闸激活) | SHOW CREATE 子句经保留魔法键传**连接器预渲染的 Doris SQL 文本**(`IcebergConnectorMetadata:458-502` 渲 `` BUCKET(N,`c`) `` 等),fe-core 逐字 append 再按名 strip。**部分改进**:`show.*` 现是声明常量,但 `partition_columns`/`primary_keys` 仍裸字面量 | 换结构化 `ConnectorTableDdl`(transform+sort 作数据),fe-core 单一 altitude 渲染;至少把两裸键提为常量 |
| P6-D4:tccl 复制 | 仍然 | `TcclPinningConnectorContext` 仍 iceberg/paimon 两份(~73% 同);hive **未加第 3 份**但在 `HiveConnectorMetadata:798-805,832-845` **内联** open-code 同一 TCCL pin(第 3 种形态) | 抽 `Tccl.pin(loader,Runnable)` 到 spi/support,三处共用 |
| P5-D:hiveconf 复制 | 仍然 | `assembleHiveConf`/`buildHadoopConfiguration` 仍 iceberg/paimon 两份;hive 未加 3rd,但**连接器内** `buildHadoopConf` 三重复(`HiveConnector:621`/`HiveScanPlanProvider:444`/`HiveConnectorMetadata:967`) | 合并 hive 三处为一私有 helper;长期共享 `HadoopConfBuilder` |
| DUPLICATION:fakestub | 仍然·翻闸恶化 | `Fake*HmsClient` 测试桩从 ~3 增至 ~11 份(hive/hudi 各测各造) | 加共享 `AbstractFakeHmsClient` 测试夹具 |
| P2-3 preCreateValidation 急切 | 仍然 | `TrinoDorisConnector:78` CREATE CATALOG 时急切加载插件+构造连接器(legacy 惰性);replay 跳校验 | 视为有意 fail-fast,**登记 deviations-log**(当前未登记);或改 best-effort |
| P4-SHOWPART:admission | 仍然 | `ShowPartitionsCommand:202-206` admit 所有 `PluginDrivenExternalCatalog`;jdbc/es/trino 错误类别变化 | 无需改;已登记决策 D-028 |
| P5-6 convertAnd sound | 仍然 | `PaimonPredicateConverter:121-130` OR 下保留部分合取;**sound**(超集,BE 重过滤),甚至比 legacy 剪得多 | 无需修;可注释登记免复审再标 |
| P6-D4:flavor-provider | **部分已修** | flavor wrapper 复制经共享 `Abstract*MetaStoreProperties` 基类大幅收敛,余薄子类矩阵;hms 复用共享基类 | 无需动;此行 5.4 是**改善**非恶化 |
| P8 前置(5 项) | 仍然·翻闸增 | `SPI_READY_TYPES` 字符串门(`CatalogFactory:54`)、`getEngine` switch、data-cache allowlist(`FileQueryScanNode:112` 已含 "hms",**hive 数据缓存正常**)、`TableType.ICEBERG_EXTERNAL_TABLE`(8 处/4 文件,活+死混合)、`PlanNode.printNestedColumns instanceof IcebergScanNode`(见 §6 死码) | 均为 **P8 删除前置**,非 bug;P8 换连接器声明属性/按注册路由。**当前对 hive 均无错误行为** |

---

## 5. 🟡 已登记 / 验收偏差 / benign(真实但已决策或无害,列此避免误改)

| ID | 现状 | 要点 | 处置 |
|---|---|---|---|
| P4-5 | 仍然(hive 新激活) | `PluginDrivenInsertExecutor.doAfterCommit:167-175` 吞 post-commit refresh 失败,INSERT 报成功 | **DV-018 已签字**(数据已提交,报失败会诱发重复写);翻闸后 hive INSERT 继承同行为 |
| P4-6 | 仍然(hive 新激活) | `createDb/dropDb` 丢 errno 1007/1008(createTable 恢复了 1050) | **DV-034/DV-021 GAP3 已登记**;修法 = 仿 createTable 加 `ErrorReport.reportDdlException` |
| P4-SHOWPART:limit | 新激活 | 翻闸 hive SHOW PARTITIONS 丢远端 LIMIT 下推(全取-FE 排序-分页) | **DV-021 GAP9 已登记**;升序结果等价,仅多元数据 payload |
| P4-SHOWPART:where | 新激活 | 翻闸 hive SHOW PARTITIONS 静默接受非 PartitionName WHERE/ORDER(legacy 拒绝) | 已登记为 plugin-model parity(对齐 paimon/iceberg);比 legacy hive 更松 |
| P6-7 | 仍然 | `FOR TIME AS OF '<数字串>'` 读作 epoch millis(legacy 拒) | benign 超集,javadoc 已声明有意;可补登记 |
| P6-8 | 仍然 | 无 ctx `resolveSessionZone` 回退 UTC vs legacy FE 默认 tz | **无可达无 ctx 时间旅行路径**(时旅恒有 ConnectContext);latent |
| P6-10 | 仍然 | `$position_deletes` 丢定向错误文案,变通用 not-found | **DV-049③ 已签字**,reg-test 已同步;语义等价 |

---

## 6. ✅ 报告条目在当前分支**不成立 / 已修 / parity**(核实要点)

> 用户特别关注"哪些结论不适用于当前分支"。以下逐条为报告发现,但**当前分支不构成需修问题**——或被 P7 修掉,或被对抗证伪,或与 master parity/属死码。**不要据此改代码**。

| ID | 判定 | 为什么不成立(要点) |
|---|---|---|
| P0-1 | parity | 翻闸 hive 全支持 CREATE/DROP;它拒绝的 ALTER 在 legacy 与新路都无 catalog 名(仅措辞变);errno 另登记 DV-034 |
| P0-2 | parity(不可达) | `readBucketNum` 的 `translateToCatalogStyle().getBuckets()` 证明不抛→catch 是死防御码,silent-0 不可达 |
| P0-3 | parity(不可达) | `convertFields` else-drop 对 hive(identity)/iceberg(transform)分区不可达;grammar 只产 slot/transform |
| P0-4 | 不适用 | hive/iceberg/paimon 均 override 完整 `createTable(request)`,降级默认不可达 |
| P0-6(stats) | 不适用 | `invalidateStatistics` no-op **零生产调用**;事件走 `MetastoreEventSyncDriver`(中立 CatalogMgr/RefreshManager),不经 `ConnectorMetaInvalidator`;legacy 事件也从不失效统计 |
| P0-6(part) | parity(有意) | 活的分区事件路是 `HiveConnector.invalidatePartition`(整表 flush,pull-based 正确),报告引的是死方法;已登记 |
| P1-1 | 不适用 | 翻闸 hudi 绑 `LogicalFileScan`→`visitPhysicalFileScan:812` 建 `PluginDrivenScanNode` 并 `setSelectedPartitions:820`+`setDistributeExprLists:866`;`visitPhysicalHudiScan` 插件臂是**死码**,分区剪枝/bucket-shuffle 已 parity |
| P1-3(iceberg) | 不适用 | `PlanNode.printNestedColumns instanceof IcebergScanNode` 对翻闸 iceberg 是死码(已 `PluginDrivenScanNode:377` 注释 + FU-h10-deadcode 跟踪),纯 cosmetic,与 hive 无关 |
| **P3-hudi COW/MOR UNKNOWN** | **已修** | **HD-A4(`cf8710691cf`)**:`planScan:140` `isCow = metaClient.getTableType()==COPY_ON_WRITE` 从**权威** Hudi 配置取读路径;`detectHudiTableType` 的 UNKNOWN 启发式仅用于 per-split 覆盖的 node 默认 + 信息属性,不再选错读路径→陈旧行风险消除 |
| **P3b-1** | **证伪(parity)** | 已发布/长期基线本就是 `"hadoop"`+doAs(legacy fe-core `DFSFileSystem` 全经 `getHadoopAuthenticator().doAs`,默认 user "hadoop");master 的 process-user 只活在**未发布**的 fs-SPI 脚手架(#62023)且与 master 自身 metastore 侧不一致。#64655 consolidation 是**有意**统一。无升级回归 |
| **P3b-3** | **证伪(机制错)** | `UGI.setConfiguration`→`initialize` 本身是 `private static synchronized` on 单一 parent-loaded `UGI.class`(`org.apache.hadoop.` 在 FS 是 parent-first);外层 `synchronized(HadoopKerberosAuthenticator.class)` 冗余,拆成两 monitor 也不丢保护。连接器侧 `org.apache.hadoop.` 非 parent-first→各自隔离 UGI,无共享全局可损。双载 jar 真实但**无害** |
| P4-3-IN | 已修 | `convertIn:169-184` 方向正确(`col IN (...)`),legacy 反向 bug 已修;缺一条定向回归测试(建议补) |
| P4-3-EQ | 待实测 | 算子仍 `==`(`:145-146`);ODPS 是否容忍需 live A/B。建议直接对齐 `=` 消除不确定性(见 §L20) |
| P5-7 | parity | `getInitialValues()` 仅测试填充,`VALUES IN` 静默吞 = master 同款;死 API 面。新增 `hasExplicitPartitionValues` 仅 hive 消费,paimon 未 opt-in |
| P6-S:cap-enum | 不适用 | capability enum 破坏性重写是一次性迁移成本,已落地,无运行期缺陷 |
| ENGINE-SWITCH:i3-hive | 已修 | 三处 switch 翻闸都加了正确 `case "hms"`→SHOW TABLE STATUS 显 "hms" 非通用 "Plugin";I3 回归被规避。建议补一条断言锁 parity |

---

## 7. 建议的行动清单

**翻闸后、更大范围放量前(高)**
1. **hudi 4 高危(H1–H4)**:翻闸即在 hudi-on-HMS 上静默丢行/崩。修复应在 §4 `DUPLICATION:partition-prune` 抽出的共享剪枝 helper 一处落地(H1/H3)+ H2 时间类型不做字符串比 + H4 `planScan:181` lowercase;每条补能抓到的回归测试(现有 parity 测试写法抓不到)。
2. **hive/MC 批量丢失(M1/M2/M3)**:TABLESAMPLE 转发+通用采样;hive `supportsBatchScan` opt-in;MC `!=NOT_PRUNED` 复原。
3. **iceberg 统计/凭证(M5/M6/M7)**:equality-delete gate、s3tables 默认链、region 别名。
4. **升级坑(M8)**:发布工具部署 `plugins/connector` + release note + 启动聚合告警。

**贯穿性(建议开跟踪项,对齐 #65185)**
5. `Connector.getLegacyEngineName()` SPI 收口三处引擎名 switch(P4-D);EXPLAIN 节点名(P5-1)登记或按连接器声明 legacy 名。
6. `HmsStylePartitionPruner` / `Tccl.pin` / `HadoopConfBuilder` / `AbstractFakeHmsClient` 共享化(§4 复制项)。
7. SHOW CREATE 脱敏纵深(P6-S(a))+ 敏感键 SPI 聚合(P6-S(b))。
8. import-gate 三洞补齐(P0-5);MC 分区缓存(M4)。

**待实测**
9. P4-3-EQ(`==` vs `=`)live ODPS A/B;或直接改 `=`。
10. P5-2 option-unset ORC paimon-cpp e2e(默认 JNI reader 不受影响,窄)。

---

## 附录 A:方法与统计

- **工作流**:`wf_588a73bd-294`。3 recon(类定位图 / P7-翻闸修复台账 / 已登记偏差摘要)→ 34 finding-cluster 核实(按符号定位现码,交叉核对 deviations/decisions-log)→ 对每条存活的高/中发现派对抗证伪 agent(尽力 REFUTE:找 guard / master-parity / P7-fix / 不可达 / 登记偏差)。
- **规模**:60 agent,58 完成,2 个对抗 agent schema 重试超限(丢失 H2 与 P6-6 的对抗判定;二者由同源已 CONFIRMED 的姐妹发现佐证,判定保留)。
- **口径**:79 条结论。状态分布 — STILL_REAL 52 / NEWLY_ACTIVE 10 / NO_LONGER_APPLICABLE 6 / PARITY 5 / FIXED 3 / PARTIALLY_FIXED 2 / CANNOT_VERIFY 1。对抗判定 — CONFIRMED_REAL 10 / DOWNGRADED 9 / REFUTED 2。
- **原始逐条证据**(含每条 file:line、失败场景、对抗推理)落 scratchpad `findings_detail.md`;工作流返回值落 `journal.jsonl`。

## 附录 B:与报告 §10 优先级的对照

报告 §10 的 4 个"合 master 前高危"在当前分支的落点:
- P6-1(缓存偏斜崩 BE):**部分已修**(T07/T08 原子 pin + 同 TTL + 原子失效),对抗降为低危结构隐患(触发极窄);
- P6-2(升级坑):**仍然**,blast radius 因翻闸放大(M8);
- P4-1(batch-mode):**仍然**(M3),另 hive 有独立 M2;
- P6-S(SHOW CREATE 脱敏):**仍然**(设计/安全,§4),当前无可达泄漏。

报告 §10"P7 前必查(dormant)"的 hudi 4 高危 → **已翻闸激活为 §2 的 H1–H4**(报告的预警准确兑现);TABLESAMPLE→M1;CacheAnalyzer instanceof→L2;kerberos 锁分裂→**证伪**(§6 P3b-3)。
