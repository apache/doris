# Catalog SPI 抽象错配修复 — TASKLIST

> 逐条任务清单 + 状态板。配 [`HANDOFF.md`](HANDOFF.md) 使用。**动手前先读 HANDOFF 第 2 节（三条前置），再对照当前代码重侦察。**
>
> `file:line` 标注：`(HEAD)` = 本次 `aaab68ef474` 亲验；`(基线)` = 仅见于 `analysis-*`（分支 `catalog-spi-2-lvl-cache`），须重侦察。

## 状态图例

| 标记 | 含义 |
|---|---|
| ⬜ TODO | 待处理 |
| 🔄 DOING | 处理中 |
| ✅ DONE | 已完成（含验证 + commit） |
| ⚠️ VERIFY | 需先重侦察确认是否仍成立（疑已 STALE） |
| 🚫 WONTFIX | 有意设计 / 潜伏无复现 / 待签字，记录不动 |
| ☑️ CLOSED | STALE_FIXED 或 REFUTED，已闭环 |

---

## 状态板（28 条一览）

| # | 批次 | 核实 | 状态 | 一句话 |
|---|---|---|---|---|
| 2  | B1 | CONFIRMED | ⬜ | getColumnHandles 无 snapshot 参 → paimon time-travel+混合投影 → **BE crash** |
| 1  | B2 | PARTIAL | ⬜ | hudi decimal 分区谓词走 String.valueOf，未同步 hive 已修分支 → 潜在误剪 |
| 14 | B2 | PARTIAL | ⬜ | paimon `partition_values()` TVF 读 raw spec → DATE 显 epoch-day、null 显 `__DEFAULT_PARTITION__` |
| 21 | B3 | CONFIRMED | ⬜ | `ConnectorDeleteFile` 欠建模且零 caller = 死代码，删 |
| 23 | B3 | CONFIRMED | ⬜ | `ConnectorDomain`/`ConnectorRange` 死抽象（columnDomains 恒空），删 |
| 25 | B3 | CONFIRMED | ⬜ | `ConnectorMvccSnapshot` 独缺 equals/hashCode，补一致性 |
| 28 | B3 | STALE_FIXED | ⚠️ | detect-and-delegate 已修；仅剩 HiveConnector "Dormant" 过时注释（**疑已清理**） |
| 7  | B4 | CONFIRMED | ⬜ | `getWriteContext()` 错标 free-form bag，实为静态分区 spec |
| 8  | B4 | CONFIRMED | ⬜ | `ConnectorTransaction` 泄漏 odps/iceberg 专属方法（write-block 还双泄漏进 fe-core） |
| 9  | B4 | CONFIRMED | ⬜ | `ConnectorBucketSpec` "iceberg_bucket" 死文档 + 表级分布 vs per-field 类别错误 |
| 27 | B4 | PARTIAL | ⬜ | reader-type 3 份手搓副本 + 3 种线上编码（force_jni 回归已 REFUTED） |
| 10 | B5 | PARTIAL | 🚫 | iceberg writeDefault 未接 + Column 元数据默认恒 null（initialDefault 已正确下发） |
| 11 | B5 | PARTIAL | 🚫 | paimon CREATE CHAR/VARCHAR→MAX、DATETIME scale→micros（刻意 legacy parity） |
| 12 | B5 | PARTIAL | 🚫 | paimon 嵌套 struct comment 丢弃（DV-035 已签字；nullability 已 FIX-L13 修复） |
| 13 | B5 | PARTIAL | 🚫 | `initialValues` LIST/RANGE 恒空 vestigial 槽（有意分层，零消费者） |
| 16 | B5 | PARTIAL | 🚫 | `getNewestUpdateTimeMillis` 微秒/毫秒命名错配（CacheAnalyzer 安全抑制 SqlCache） |
| 19 | B5 | PARTIAL | 🚫 | 读路径嵌套 nullable/comment 不进 StructField（legacy parity，功能影响为零） |
| 3  | B6 | STALE_FIXED | ☑️ | paimon JNI/COUNT 已改 per-file 后缀 format + 回归测试 |
| 4  | B6 | STALE_FIXED | ☑️ | hive 分区值 KEY+VALUE 双 unescape 已修（#65473）+ 单测 |
| 5  | B6 | REFUTED | ☑️ | iceberg 行数三点全反：snapshot summary / 扣 position delete / equality gate -1 |
| 6  | B6 | STALE_FIXED | ☑️ | LZ4FRAME→LZ4BLOCK 经 `adjustFileCompressType` 能力位已恢复 + 单测 |
| 17 | B6 | REFUTED | ☑️ | branch 移动窗口被 `IcebergStatementScope` 每语句单次冻结堵死 |
| 15 | B6 | CONFIRMED | 🚫 | stats 无 snapshot，time-travel 下仅 CBO 估计偏斜不错结果（legacy 端口，同 #2 族） |
| 18 | B6 | PARTIAL | 🚫 | FOR TIME AS OF 数字串当 epoch，合法 datetime 恒不误判（有意对齐） |
| 20 | B6 | CONFIRMED | 🚫 | transform 参数只 `List<Integer>`，当前无非整型/小数参数 transform（YAGNI） |
| 22 | B6 | CONFIRMED | 🚫 | iceberg 谓词用 latest 非 pinned schema，有意 legacy parity，只丢下推不丢正确性 |
| 24 | B6 | PARTIAL | 🚫 | `getFreshnessValue` 一 long 两粒度，tagged-union 判别枚举始终在场 |
| 26 | B6 | CONFIRMED | 🚫 | iceberg residual 挂 scan-node 级非 per-range，legacy 对齐，收益薄 |

---

## B1 · 正确性 / 稳定性（排期）

### #2 — getColumnHandles 无 snapshot 参数 → paimon BE crash ⬜
- **核实**：CONFIRMED（高）｜文档 [B](analysis-B-schema-column-identity.md#2)
- **现象**：通用节点在 MVCC pin **之前**跑 `buildColumnHandles`，用未 pin 的 handle 拿 latest-keyed 的 `getColumnHandles` map；time-travel（`FOR VERSION AS OF`）+ 列被 RENAME 后，query slot 携旧名、latest map 只有新名 → slot 静默丢列。**真实触发 repro = 混合投影**（改名列与存活列同现）：paimon 侧 dict 的 -1 target 条目缺列 → BE `StructNode children.at()` `std::out_of_range` → **SIGABRT**。iceberg 已在连接器侧用 `hasSnapshotPin()` 全量重建 dict 自防，**paimon 未做**、更脆。纯单列改名投影被空集回退救活、不炸。
- **建议动作（需先定架构高度，择一）**：
  1. **框架级（推荐、覆盖全连接器）**：给 SPI `getColumnHandles` 增 snapshot/pinned-handle 形参，或把 pin 提到 `buildColumnHandles` 之前，使 handle 按 pinned（旧名）schema 建。→ **碰 SPI 签名 + fe-core 只出不进铁律，需用户拍板**。**可一并覆盖 #15**。
  2. **paimon 局部修（镜像 iceberg 先例）**：检测 `scanOptions` 非空（pin）时投影/dict 改从 pinned `table.rowType()` 全量重建、忽略 latest-keyed lossy columns；须同步修 `planScanInternal` 的 projected 序号（不能像 iceberg 只喂 dict）。
  3. **兜底**：`buildColumnHandles` slot 名 miss 时 fail-loud（抛错而非静默丢）。
- **必须**：补 paimon **混合投影 + FOR VERSION AS OF** e2e 回归（参 hudi HD-C5b / iceberg T07 的 pin 测试）。
- **目标代码（基线，须重侦察）**：`ConnectorTableOps.getColumnHandles`；`PluginDrivenScanNode.buildColumnHandles`（~1917-1934）；`PaimonScanPlanProvider`（投影 ~485-510 / dict ~1543-1635）；iceberg 模板 `IcebergScanPlanProvider`（pin 分支 ~1594-1618）。
- **风险 / 依赖**：碰铁律 A；方案 1 需定架构高度并交 review；与 #15 合并治理更合算。

---

## B2 · 活跃产错小修（小步低风险，建议先做）

### #1 — hudi decimal 分区谓词未同步 hive 修复 ⬜
- **核实**：PARTIAL（high）｜文档 [A](analysis-A-literal-predicate.md#1)
- **现象**：`ConnectorLiteral` 无 typed canonical 访问器，各连接器各自从 Java 值重建 canonical string。hive 已修（`stripTrailingZeros().toPlainString()`），**hudi 的 decimal 分支仍走 `String.valueOf`** → `WHERE d = 1` 传入 BigDecimal `1.0000` 得 `"1.0000"`，与存储分区值 `"1"` 失配 → 分区被误剪、丢行。datetime 分支 hudi 已镜像、不受影响。
- **建议动作**：把 hive 的 BigDecimal 分支镜像进 `HudiConnectorMetadata.extractLiteralValue`（与其已镜像的 LocalDateTime 分支并列）。小步低风险。
- **验证**：加 hudi decimal 分区剪枝单测/e2e（decimal 分区列 `WHERE d = 1` 命中存储 `"1"` 的分区、返回非空）。
- **目标代码**：`HudiConnectorMetadata.extractLiteralValue`（**HEAD `~:1063`**）。
- **架构备注**：根治分叉（在 `ConnectorLiteral` 加 canonical 渲染入口 / 边界透传 `DateLiteral` canonical 文本）属独立设计任务、碰铁律，不在本条范围。

### #14 — paimon partition_values() TVF 读 raw spec 产错 ⬜
- **核实**：PARTIAL（high，报告低估为"休眠"，实为**活跃产错**）｜文档 [C](analysis-C-partition.md#14)
- **现象**：paimon `collectPartitions` 同时产 rendered `orderedValues`（格式化日期+归一 null）与 raw `partition.spec()`（DATE=epoch-day `19723`、null=`__DEFAULT_PARTITION__`）。`ConnectorPartitionInfo.getPartitionValues()` 暴露 **raw**。活跃路径 `partition_values()` TVF → `getNameToPartitionValues` 读同一 raw map：DATE 经 `convertStringToDateV2` 把 epoch-day 当日期串解析 → 报错/错值；null 因 `__DEFAULT_PARTITION__` ≠ `__HIVE_DEFAULT_PARTITION__` → 渲染成字面量而非 SQL NULL。
- **建议动作（择一）**：
  1. 按名索引下游改用 `getOrderedPartitionValues()`（已 rendered + 归一）；
  2. 或在渲染点补 DATE 格式化 + null 归一到 `HIVE_DEFAULT_PARTITION`，使 raw 值与 TVF 下游口径对齐。
- **验证**：paimon DATE 分区 + null 分区跑 `partition_values()` TVF，断言 DATE 显 `2024-01-01`、null 显 SQL NULL。
- **目标代码**：`PluginDrivenExternalTable.getNameToPartitionValues`（**HEAD `:870`**）；消费点 `MetadataGenerator.java`（**HEAD `:2035`**）；raw 源 `ConnectorPartitionInfo.getPartitionValues` / paimon `collectPartitions`。
- **备注**：iceberg 也 override `listPartitions` 喂 `getNameToPartitionValues`，其 `ConnectorPartitionInfo` 值渲染另论（超 #14 范围，同族一并看）；休眠的 SPI `listPartitionValues` 本身零 fe-core 生产调用者，别只修它。

---

## B3 · 死代码 / 注释清理（无功能风险，建议其次）

### #21 — 删死代码 ConnectorDeleteFile ⬜
- **核实**：CONFIRMED（high）｜文档 [D](analysis-D-scan-split-file.md#21)
- **现象**：`ConnectorDeleteFile` 只建模 (path, format, recordCount, properties)，缺 content-type/序列号/bounds/field-id/DV offset；iceberg 完全绕开用内部 typed `IcebergScanRange.DeleteFile`。`ConnectorScanRange.getDeleteFiles()` 默认返 `emptyList()`，**全树无 override、无 caller** = 死代码。
- **建议动作**：删 `ConnectorDeleteFile` 类 + `ConnectorScanRange.getDeleteFiles()` 默认方法。
- **⚠️ 勿混淆**：另有同名 `ConnectorScanPlanProvider.getDeleteFiles(TTableFormatFileDesc) → List<String>`（EXPLAIN 回读删除文件路径，**生产在用**，iceberg/paimon 有 override）——**保留**。
- **验证**：编译 + 全量单测通过（删死代码零运行时风险）。
- **目标代码（HEAD 均在）**：`fe-connector-api/.../scan/ConnectorDeleteFile.java`；`ConnectorScanRange.java:149`（`default List<ConnectorDeleteFile> getDeleteFiles()`）。

### #23 — 删死抽象 ConnectorDomain / ConnectorRange ⬜
- **核实**：CONFIRMED（medium）｜文档 [A](analysis-A-literal-predicate.md#23)
- **现象**：`ConnectorDomain`+`ConnectorRange`（javadoc 自称 "fast partition pruning"）零构造、零读取；唯一生产侧 `ConnectorFilterConstraint.columnDomains` 恒被填 `emptyMap()`，三个消费者（hive/hudi/trino applyFilter）全部只读 `getExpression()`，无一调 `getColumnDomains()`。彻底死代码，误导后人（还埋 CHAR padding 坑）。
- **建议动作**：删 `ConnectorDomain` + `ConnectorRange` + `ConnectorFilterConstraint.columnDomains` 字段/构造参/`getColumnDomains()`/相关 javadoc。**保留** `ConnectorFilterConstraint` 其余（`getExpression()` 在用）。
- **验证**：编译 + 单测；确认 `ConnectorFilterConstraint` 单参构造/`getExpression` 路径不受影响。
- **目标代码（HEAD 均在）**：`.../pushdown/ConnectorDomain.java`、`ConnectorRange.java`；`ConnectorFilterConstraint.java`（`:32` javadoc、`:42` 字段、`:45-49` 构造、`:67-68` getter）。

### #25 — 补 ConnectorMvccSnapshot equals/hashCode ⬜
- **核实**：CONFIRMED（high，纯形态 smell，零运行时影响）｜文档 [E](analysis-E-stats-mvcc-timetravel.md#25)
- **现象**：MVCC/stats 值对象家族四兄弟（Partition/PartitionView/TimeTravelSpec/TableStatistics）都有 equals+hashCode+toString，**唯 `ConnectorMvccSnapshot` 独缺**。当前无任何 value-key 用法（不作 Map/Set key、不 distinct），故零影响。
- **建议动作**：补 equals/hashCode（+toString），覆盖 6 字段（snapshotId/timestampMillis/schemaId/lastModifiedFreshness/description/properties），与兄弟类同风格。已 import `java.util.Objects`，单文件、不碰调用方。
- **验证**：新增/复用逐字段单测；round-trip 相等性。
- **目标代码（HEAD）**：`fe-connector-api/.../mvcc/ConnectorMvccSnapshot.java`（已确认**无** equals/hashCode）。
- **可选**：鉴于零运行时影响，也可评估直接标 backlog 不修。

### #28 — 清理 HiveConnector 过时 "Dormant" 注释 ⚠️
- **核实**：STALE_FIXED（core detect-and-delegate 已修）｜文档 [G](analysis-G-reader-path-dispatch.md#28)
- **现象**：核心 finding（type=hms 进 SPI 后 iceberg/hudi 表被当裸 hive 读）已由 **#65473 原子修复**（hms 进 `SPI_READY_TYPES` 与 detect-and-delegate 同 commit 落地），预测 bug 从未出现。**仅剩注释漂移**：`HiveConnector.java` / `HiveConnectorMetadata.java` 里 "Dormant/Inert until hms enters SPI_READY_TYPES / never called for this connector" 已过时（功能已 live）。
- **⚠️ 先 verify**：本次 recon 中这些**具体注释在 `HiveConnector.java` 已 grep 不到**（`Inert until hms` / `never called for this connector` 零命中）——疑已被 `#65893` 清理。**动手前先确认是否还有残留**；若已无 → 直接标 ☑️ 记一句证据。
- **勿误删**：其它文件（`Env.java`、`MetastoreEventSyncDriver.java`、`CachingHmsClient.java`、若干测试、`IcebergRewriteDataFilesAction.java` 等）的 "Dormant" 注释是关于**别的休眠子系统**（event sync / caching client / P6.6 cutover），非本 finding 范围，**保留**。

---

## B4 · 架构收敛（碰 fe-core 铁律，独立设计任务，交 review）

> 这批"最小改=纯 javadoc/删死文档"（低风险，可当 B3 做），"彻底改=下沉窄接口/重命名/统一枚举"（碰铁律，须交 review）。TASKLIST 各给两档。**别顺手做彻底版。**

### #7 — getWriteContext() 错标 free-form bag ⬜
- **核实**：CONFIRMED（high，命名/文档 smell，无运行期 bug）｜文档 [F](analysis-F-write-txn-ddl.md#7)
- **现象**：`getWriteContext(): Map<String,String>` javadoc 承诺 "static partition spec, **write path, and other connector-defined keys**"，但唯一生产路径只塞静态分区 spec（`writeContext = ctx.getStaticPartitionSpec()`），承诺的 write path / other keys **永不存在**。误导有**两处** locus（方法级 + 类级 javadoc）。
- **建议动作**：
  - **最小（首选、surgical）**：收窄 `ConnectorWriteHandle` **两处** javadoc（方法级 + 类级），删 write path / other keys 虚假承诺。
  - **彻底**：重命名 `getStaticPartitionSpec()`，同步 3 个 plan provider（iceberg/hive/maxcompute）+ `PluginDrivenTableSink` + 单测 + `IcebergWriteContext.java` 注释。
- **目标代码（HEAD）**：`fe-connector-api/.../handle/ConnectorWriteHandle.java:48`（"Free-form write context"）+ 类级 javadoc；消费者见 grep（iceberg `IcebergWritePlanProvider.java:375/423-424`、hive `:161`、maxcompute `MaxComputeWritePlanProvider.java:100`）。

### #8 — ConnectorTransaction 私有方法泄漏（含 fe-core 双泄漏）⬜
- **核实**：CONFIRMED（high）｜文档 [F](analysis-F-write-txn-ddl.md#8)
- **现象**：通用事务接口带三个 source-specific 方法：`allocateWriteBlockRange`（odps write-session）、`registerRewriteSourceFiles` + `getRewriteAddedDataFilesCount`（iceberg-compaction），均 default throw。**write-block 还双泄漏**——fe-core `org.apache.doris.transaction.Transaction` 本身也带这对方法，`FrontendServiceImpl` 的静态类型正是 fe-core `Transaction`，`JdbcTransaction` 等白继承 odps 默认。**这是本条最该点名的一面（碰 fe-core 只出不进铁律）。**
- **建议动作**：下沉窄 opt-in 能力接口（与本仓库 `supports*()` 范式一致）：
  - `WriteBlockAllocatingTransaction`（`allocateWriteBlockRange`），MaxCompute 实现。**⚠️ 收敛必须在 fe-core `Transaction` 层（或其 `PluginDrivenTransaction` wrapper）做**——只在 connector-api 侧新增窄接口不够，fe-core `Transaction` 上的 odps 默认方法 + `JdbcTransaction` 白继承仍在，泄漏未消。
  - `RewriteCapableTransaction`（rewrite 两方法），Iceberg 实现；`ConnectorRewriteDriver` 先窄接口 `instanceof` 再调（把"不支持"从运行期 throw 提前为类型不匹配）。此对只在 connector-api 侧（未污染 fe-core）。
- **目标代码（HEAD）**：fe-core `transaction/Transaction.java:45/60`；`PluginDrivenTransactionManager.java:175-185`；`FrontendServiceImpl.java:3887/3892`；`ConnectorRewriteDriver.java:151/167`；connector-api `ConnectorTransaction`；impl `MaxComputeConnectorTransaction` / `IcebergConnectorTransaction`。
- **风险**：碰铁律；改事务契约需 clean-room review。txn-id 粒度本身正确、勿动。

### #9 — ConnectorBucketSpec 类别错误 + iceberg_bucket 死文档 ⬜
- **核实**：CONFIRMED（high，潜伏，非活 bug）｜文档 [F](analysis-F-write-txn-ddl.md#9)
- **现象**：`ConnectorBucketSpec` = 表级单 `numBuckets` + 字符串 `algorithm`（javadoc 列 `"iceberg_bucket"`），与 iceberg **per-field** `bucket(N, col)` transform 是类别错误。`"iceberg_bucket"` 全树仅 javadoc 1 命中 = **纯死文档**（零生产/消费）；iceberg CREATE 的 bucket 走 partition-spec transform（`IcebergSchemaBuilder.buildPartitionSpec` 的 `case "bucket"`），从不调 `getBucketSpec()`。
- **建议动作**：
  - **最小（首选）**：删/改 `ConnectorBucketSpec.java:31` 的 `"iceberg_bucket"` javadoc，注明本 SPI 只承载表级 hash/random，iceberg 走 `ConnectorPartitionField` transform 路径。
  - **可选**：iceberg.createTable 对非空 `getBucketSpec()` fail-loud（防 `DISTRIBUTED BY HASH` 被静默吞）——先确认 legacy iceberg 对 `DISTRIBUTED BY` 的原行为，避免回归。
- **目标代码（HEAD）**：`fe-connector-api/.../ddl/ConnectorBucketSpec.java:31`。

### #27 — reader-type 3 份手搓副本（纯架构观察）⬜
- **核实**：PARTIAL（high；架构观察成立，但唯一严重度支撑的 hudi `force_jni_scanner` 回归 **REFUTED**）｜文档 [G](analysis-G-reader-path-dispatch.md#27)
- **现象**：三连接器各写一套 reader-type 决策，一个概念 3 种线上编码（paimon typed `TPaimonReaderType` 枚举 / hudi 魔法串 `file_format_type="jni"` / iceberg serialized-split 约定）；`TIcebergFileDesc`/`THudiFileDesc` 无 reader_type 字段（thrift 不对称）。**force_jni 回归不成立**：SPI `HudiScanPlanProvider` 已完整 honor（读 session、COW 在 force_jni 下改走 JNI、烘焙 flag 进 split 供无 session 路径保持一致），有 `HudiForceJniTest` 锁定。
- **建议动作**：**默认不做**。若追求一致性 → 在 `fe-connector-api` 引统一 `ReaderType` 枚举、各 provider 返回。**碰"fe-core 只出不进 / 禁 scaffolding"铁律 + 跨连接器大重构**，收益有限（三种湖格式删除/读语义本质不同），应作独立设计任务交 review，**别挂在"回归"名下**。

---

## B5 · 可选增强（需产品签字，默认不动）🚫

> 均非缺陷：或有意 legacy parity、或边缘能力未接、或已签字接受的偏差。**每项动手前先向用户确认是否升级**（参本项目 hudi/paimon "完整对齐 vs 有意提升" 的签字先例）。升级必同步改被 mutation 单测钉死的断言。

| # | 一句话 | 若要做 | 文档 |
|---|---|---|---|
| 10 | iceberg writeDefault 未接、Column 元数据默认恒 null（initialDefault 已经 #65502 正确下发 BE，勿动） | `parseSchema` 用 `field.writeDefault()` 填 `ConnectorColumn.defaultValue`；仅补写默认/展示，非纠错 | [B](analysis-B-schema-column-identity.md#10) |
| 11 | paimon CREATE CHAR/VARCHAR→VarChar(MAX)、DATETIME scale→micros（decimal 正常）；mutation 单测钉死 | `PaimonTypeMapping` 改 `CharType(len)`/`VarCharType(len)`/`TimestampType(scale)`，同步改测试；打破 parity 须签字 | [B](analysis-B-schema-column-identity.md#11) |
| 12 | paimon 嵌套 struct **comment** 丢弃（DV-035 已签字；**nullability 已 FIX-L13 修复、不成立**） | DataField 改 4 参构造传 `getChildComment(i)`，改单测断言 | [B](analysis-B-schema-column-identity.md#12) |
| 13 | `initialValues` 对 LIST/RANGE 显式值恒空未 lower（有意分层、零消费者、Hive 靠 `hasExplicitPartitionValues` fail-loud） | 要么删 vestigial 槽消 SPI 错配，要么未来真需外表预建分区时补 lowering + 连接器消费 | [C](analysis-C-partition.md#13) |
| 16 | `getNewestUpdateTimeMillis` 名 millis 实 micros；CacheAnalyzer wall-clock 混算 → iceberg SqlCache **被安全抑制**（correctness-safe + master parity） | 重命名去单位语义（`getNewestUpdateMonotonicMarker`）；根因=CacheAnalyzer `latestPartitionTime` 双义。纯整洁化，删旧代码期不宜优先 | [E](analysis-E-stats-mvcc-timetravel.md#16) |
| 19 | 读路径嵌套 struct nullable/comment 不进 Doris `StructField`（legacy parity，查询正确性影响为零，仅 DESCRIBE 展示不一致） | 改连接器读映射用 4 参 `structOf` 采 iceberg `.isOptional()/.doc()`、paimon `.isNullable()/.description()` + `convertStructType` 4 参 StructField | [B](analysis-B-schema-column-identity.md#19) |

---

## B6 · 已闭环 / 不成立（仅记录，无需动）

> 逐条快速确认后勾掉即可。STALE_FIXED / REFUTED 已闭环（☑️）；CONFIRMED-但有意/潜伏（🚫）记录不动。

### 已闭环 ☑️（STALE_FIXED / REFUTED）
- **#3** STALE_FIXED — paimon JNI/COUNT 已改 per-file 后缀 format（`buildJniScanRange`/`buildCountRange` 走 `dataSplitFileFormat`），回归测试 `PaimonScanPlanProviderTest` 钉死（改回 default/`"jni"` 变红）。｜[D](analysis-D-scan-split-file.md#3)
- **#4** STALE_FIXED — hive `parsePartitionName` 已对 KEY+VALUE **双双** `unescapePathName`（#65473），比 legacy 更完整（连特殊字符列名也 unescape），单测 `parsePartitionNameUnescapesValues` 护。｜[C](analysis-C-partition.md#4)
- **#5** REFUTED — iceberg 行数三点全反：`computeRowCount` 取 `currentSnapshot().summary()`（snapshot 级）、扣 `total-position-deletes`（delete-aware）、`total-equality-deletes != "0"` 显式 gate 到 -1/UNKNOWN；COUNT 下推同 gate。忠实移植 legacy `getCountFromSummary`（#64648）。｜[E](analysis-E-stats-mvcc-timetravel.md#5)
- **#6** STALE_FIXED — 无压缩槽是**有意设计**（走能力位 `adjustFileCompressType`，保持通用节点 connector-agnostic）；LZ4FRAME→LZ4BLOCK 重映射经 `HiveScanPlanProvider.adjustFileCompressType` + `PluginDrivenScanNode.getFileCompressType` 完整恢复（#65473），多单测护。｜[D](analysis-D-scan-split-file.md#6)
- **#17** REFUTED — branch 移动机制属实，但 `IcebergStatementScope.sharedTable`（key 含 queryId）保证每语句对某表单次冻结加载，pin 与 scan 复用同一冻结 `Table`，`useRef` 恒解析回 pin 时的 `snapshotId`/`schemaId`，"plan→scan 间 head 移动"窗口不存在。｜[E](analysis-E-stats-mvcc-timetravel.md#17)

### 记录不动 🚫（CONFIRMED/PARTIAL 但有意 / 潜伏 / 无失败场景）
- **#15** CONFIRMED — stats 无 snapshot 参，time-travel 下 `computeRowCount` 恒取 `currentSnapshot`，与已 pin 的 schema 不对称 → CBO 基数估计偏斜；**但仅影响估计、不改结果正确性**，且是 legacy 忠实端口。**同 #2 族**，治理应与 schema/partition 快照粒度统一做（见 HANDOFF §4 依赖）。｜[E](analysis-E-stats-mvcc-timetravel.md#15)
- **#18** PARTIAL — 数字正则当 epoch 属实，但合法 datetime 字面量必含非数字字符（`-`/`:`/空格）恒不落 epoch 分支，无 date/epoch 交叠歧义；系跨连接器（paimon/iceberg）有意对齐的 benign superset。｜[E](analysis-E-stats-mvcc-timetravel.md#18)
- **#20** CONFIRMED — transform 参数只 `List<Integer>`（ALTER 侧甚至单 `Integer`），非整型被静默丢、小数被截断；但当前 iceberg/paimon/hive 全部分区 transform 无非整型/小数参数（bucket/truncate 均整型宽度），**今天无可复现错误**（YAGNI，勿提前抽象）。｜[C](analysis-C-partition.md#20)
- **#22** CONFIRMED — iceberg 谓词转换用 `table.schema()`（latest）非 pinned；time-travel 到改名前快照 + 谓词命中被改名列时该谓词 drop 到 BE residual（安全过近似，**不漏行/不多行**），仅丢文件级剪枝性能。注释锁死为有意 legacy parity，且与 dict-schema/slot-schema 字节一致 INVARIANT 协同（改动风险高、收益薄）。｜[B](analysis-B-schema-column-identity.md#22)
- **#24** PARTIAL — `getFreshnessValue` 一 long 承载 snapshot-id 或 epoch-millis，但由并列 `Freshness{SNAPSHOT_ID, LAST_MODIFIED}` 枚举原子标定（tagged-union），消费侧无条件先读判别位分派；iceberg 恒发 SNAPSHOT_ID、paimon 走 pin-timestamp，构造不出失败场景。｜[E](analysis-E-stats-mvcc-timetravel.md#24)
- **#26** CONFIRMED — iceberg per-file residual 仅用于 FE 剪枝，BE 收的是 scan-node 级整套下压 conjuncts（非 per-range 裁剪后 residual）；**结果完全正确**，仅对已满足分区谓词的文件多做恒真判断，开销可忽略，与 legacy 一致。｜[D](analysis-D-scan-split-file.md#26)
