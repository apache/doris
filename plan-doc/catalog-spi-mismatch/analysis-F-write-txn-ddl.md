# 类别 F — 写 / 事务 / DDL 粒度

**范围说明**：本类别聚焦 catalog SPI 迁移中「写 / 事务 / DDL」层的接口粒度与抽象一致性问题——即通用写句柄（`ConnectorWriteHandle`）、通用事务接口（`ConnectorTransaction`）、以及通用建表分布结构（`ConnectorBucketSpec`）上是否存在把某个连接器专属语义塞进通用 SPI、或文档承诺与实现不符的「抽象错配 / smell」。三条发现（#7、#8、#9）均为设计层 / 文档层问题（严重度 S），不涉及运行期正确性 bug。

**基线声明**：本核实基于当前工作树 branch `catalog-spi-2-lvl-cache`，逐条独立读当前代码 + 对抗复核（investigate 初查 + verify 对抗复核两阶段），并以 `verify.final_verdict` 与 `verify.corrections` 为准修正初查草稿，非轻信原 survey。所有结论均引 file:line。

## 总表

| # | 严重 | 原报告结论 | 核实结论 | 一句话 |
|---|------|-----------|---------|--------|
| 7 | 🟠 S | getWriteContext() 错标通用 bag，实际只承载 INSERT 静态分区 spec | **CONFIRMED**(high) | 通用 map + javadoc 承诺「write path/其它 key」是死承诺，唯一内容只有静态分区 spec |
| 8 | 🟠 S | ConnectorTransaction 泄漏 odps write-block + iceberg-compaction 专属方法 | **CONFIRMED**(high) | 三个 source-shaped 方法泄漏进通用事务接口；write-block 还**双重泄漏**到 fe-core `Transaction` |
| 9 | 🟠 S | ConnectorBucketSpec 表级分布 vs iceberg per-field bucket 类别错误，"iceberg_bucket" 无接线 | **CONFIRMED**(high) | "iceberg_bucket" 算法值是纯死文档，iceberg bucket 走 partition-spec transform 路径 |

---

## #7 getWriteContext() 错标通用 bag

**核实结论**：**CONFIRMED**，置信度 high。两阶段一致（verify.agrees=true），最终以 verify 为准——verify 完全认可 stage-1 的两个事实断言，仅补充了 stage-1 遗漏的第二处误导 locus 与修复完整性。属命名 / 文档层 smell（S），无运行期 bug。

**原报告主张**：`getWriteContext()` 在 SPI 上标注 / 命名为通用 free-form map，但实际只承载 INSERT/OVERWRITE 静态分区 spec；通用 bag 语义误导。

**核实过程**（读了当前以下 file:line）：

- SPI 声明：`ConnectorWriteHandle.getWriteContext()` 返回 `Map<String,String>`，javadoc 明写 "Free-form write context: static partition spec, write path, and other connector-defined keys"（fe-connector-api/.../handle/ConnectorWriteHandle.java:47-51）。verify 额外指出**同一文件的类级 javadoc**（ConnectorWriteHandle.java:31-34）也复刻了同样的过度承诺措辞 "...whether it is an OVERWRITE, and a free-form write context (static partition spec, write path, etc.)"——即误导有**两处** locus。
- 唯一生产路径：`PluginDrivenTableSink.bindDataSink` 中 writeContext 初始化为 `Collections.emptyMap()`（:164），唯一赋值 `writeContext = ctx.getStaticPartitionSpec()`（:169）；EXPLAIN 路径传 `Collections.emptyMap()`（:146）；字段 / 参数 / accessor 均命名通用 `writeContext`（:191/197/202/234-235）。确认唯一生产路径只塞静态分区 spec，不塞任何第二种 key。
- 上游来源：`PluginDrivenInsertCommandContext` 字段名为 `staticPartitionSpec`，类注释自称 "static partition spec — a generic col -> val map"（PluginDrivenInsertCommandContext.java:29-32）；`setStaticPartitionSpec` 的全部 caller（grep 整个 fe-core）仅 InsertOverwriteTableCommand.java:432 与 InsertIntoTableCommand.java:526 两处，均静态分区场景，无第三生产者。
- 全部消费者一律作静态分区值用：iceberg 仅在 OVERWRITE 且非空时 `setStaticPartitionValues(handle.getWriteContext())` 并塞进 IcebergWriteContext（IcebergWritePlanProvider.java:375、:420-421）；hive 传入 `HiveWriteContext.getStaticPartitionValues`（HiveWritePlanProvider.java:161 → HiveWriteContext.java:69）；maxcompute 局部变量直接命名 `staticPartitionSpec`（MaxComputeWritePlanProvider.java:100-101/133）。
- javadoc 承诺的 "write path" 从不经此 map：hive writePath 由 `createTempPath`/`normalizeStorageUri` 独立算（HiveWritePlanProvider.java:152-156），iceberg 输出路径走 `resolveLocationFields`（IcebergWritePlanProvider.java:409-416），均与 writeContext 无关。

**背景**：catalog SPI 把写请求收敛到 `ConnectorWriteHandle`。静态分区（`INSERT INTO t PARTITION(a=1)` / `INSERT OVERWRITE ... PARTITION`）本是明确的 `col -> val` 语义结构，但 SPI 没有给它专门 accessor（如 `getStaticPartitionSpec()`），而是塞进命名为通用容器的 `getWriteContext(): Map<String,String>`，并在 javadoc 里把承诺扩大为 "static partition spec, write path, and other connector-defined keys"。

**影响（具体示例）**：接口语义 / 文档错配，不产生错误结果，低危（S）。

- 示例：`INSERT OVERWRITE tbl PARTITION(dt='2026-07-21')`，map 里只会有 `{dt=2026-07-21}`，javadoc 承诺的 "write path" 一项永远不存在。
- 误导方向一：新连接器作者读 ConnectorWriteHandle.java:47-51（及类级 :31-34）的 javadoc，会以为可往 writeContext 放 write path 或自定义 key 并期待被填充 / 透传，但引擎侧（PluginDrivenTableSink.java:164-169）永远只写静态分区 spec，其它 key 恒空。
- 误导方向二：阅读 `handle.getWriteContext()`（如 MaxComputeWritePlanProvider.java:100）者若按字面 "free-form context" 理解，可能误写多余的防御 / 过滤逻辑。
- 文档与实现不一致会误导维护者，但不会让任何查询 / 写入产生错误数据。

**修复方案**（纯重命名 / 收窄语义的表层改动，不改行为）：

1. 最小改（首选，贴合 Rule 3 surgical）：仅修正 javadoc，把 ConnectorWriteHandle.java **:47-51 与 :31-34 两处**的描述都收窄为 "the INSERT/OVERWRITE static partition spec as a col -> val map"，删掉 write path / other keys 的虚假承诺。**注意 verify 指出 stage-1 只提 :47-51 会漏改类级 :31-34，两处必须同改否则误导仍在。**
2. 更彻底：把方法 / 字段重命名为 `getStaticPartitionSpec()`（与生产者 `PluginDrivenInsertCommandContext.getStaticPartitionSpec`、消费者本地变量 `staticPartitionSpec` 对齐），同步改 3 个 plan provider（Iceberg/Hive/MaxCompute）+ PluginDrivenTableSink + 相关单测；并同步 `IcebergWriteContext.java:36` 按 getWriteContext 命名的注释引用（stage-1 修复点未含，属遗漏但极次要）。

当前无功能风险，不阻塞；建议先取方案 1。

---

## #8 ConnectorTransaction 私有方法泄漏

**核实结论**：**CONFIRMED**，置信度 high。两阶段一致（verify.agrees=true），但**最终以 verify 为准并采纳其重要补充**：verify 认可 stage-1 全部事实断言，但指出 stage-1「问题仅限这三个方法、全在 ConnectorTransaction」不完整——odps write-block 语义实为**双接口泄漏**，同时污染了 fe-core 的 `org.apache.doris.transaction.Transaction`。

**原报告主张**：通用事务接口被两个连接器塑形：`allocateWriteBlockRange(writeSessionId, count)` 是纯 odps write-session 语义（其它连接器 default 返 false/throw），`registerRewriteSourceFiles(Set<String>)`/`getRewriteAddedDataFilesCount()` 是 iceberg-compaction 专属（default throw）；txn-id 粒度本身 OK。

**核实过程**（读了当前以下 file:line）：

- 通用 SPI 接口 `ConnectorTransaction`（fe-connector-api/.../handle/ConnectorTransaction.java）确实携带三个 source-specific 方法，均带 default：
  1. `supportsWriteBlockAllocation()` default false（:78-80）+ `allocateWriteBlockRange(String, long)` default `throw "write block allocation not supported"`（:94-96）；javadoc（:73-77）明写 "e.g. maxcompute"。
  2. `registerRewriteSourceFiles(Set<String>)` default throw（:142-144），javadoc（:131-141）标 "Compaction rewrite (rewrite_data_files)"。
  3. `getRewriteAddedDataFilesCount()` default throw（:154-156），javadoc（:146-153）标 compaction rewrite。
- 唯一 override（grep 全仓确认无第三个）：MaxComputeConnectorTransaction.java:128-130 返 true、:132-160 实现 block_id 分配（count>0 校验、writeSessionId equals 校验、CAS nextBlockId、maxBlockCount 上限——纯 odps write-session 语义）；IcebergConnectorTransaction.java:365-376 `registerRewriteSourceFiles`（派生 iceberg DataFile）、:1175 `getRewriteAddedDataFilesCount`。
- 调用侧：FrontendServiceImpl.java:3885-3893（getMaxComputeBlockIdRange RPC，先 `supportsWriteBlockAllocation()` 门控再 `allocateWriteBlockRange`）；ConnectorRewriteDriver.java:151/167（rewrite_data_files 驱动，静态类型 `connectorTx=ConnectorTransaction`）；PluginDrivenTransactionManager.java:175-185 委派 wrapper。
- **verify 额外核对（stage-1 未覆盖，关键）**：fe-core 的 `org.apache.doris.transaction.Transaction` 本身也带 `supportsWriteBlockAllocation()`（Transaction.java:45-47 default false）与 `allocateWriteBlockRange`（:60-62 default throw）。FrontendServiceImpl:3885 的静态类型正是该 fe-core `Transaction`，实现者为 `PluginDrivenTransactionManager$PluginDrivenTransaction`（委派 connectorTx）；`JdbcTransaction` 等其它实现者当前**白继承**这两个 odps 默认方法。iceberg 那对 rewrite 方法则**只**泄漏在 connector-api ConnectorTransaction，未污染 fe-core Transaction（ConnectorRewriteDriver 直接持 connectorTx 调用）。

**背景**：`ConnectorTransaction` 应只承载所有连接器都需要的事务契约（getTransactionId/commit/rollback/close/addCommitData/getUpdateCnt 等）。但它额外带了三个只服务单一连接器的方法，以 default 塞进通用接口。且写块分配这对方法还额外挂在 fe-core 通用接口 `Transaction` 上。

**影响（具体示例）**：设计层 SPI 抽象错配（abstraction leak），非运行期 correctness bug——各连接器要么 override、要么被能力位 / default 正确兜住。

- 示例 1（不出错但语义耦合）：对一张 hive 表跑 INSERT，其事务从不实现 allocateWriteBlockRange，BE 写路径也不会对 hive 发 block 分配回调（`supportsWriteBlockAllocation` 返 false）；即便误调也只抛 "write block allocation not supported"。行为正确，但通用接口凭空多了 odps 概念。
- 示例 2：对一张 iceberg 表跑 `CALL rewrite_data_files(...)`，ConnectorRewriteDriver.java:151/167 调这两个方法，仅 iceberg 能应答；若哪天让 paimon/hive 走同一 driver 而未 override，会在**运行期**撞 `UnsupportedOperationException` 而非编译期被拦——default-throw 把「未实现」从类型系统挪到运行期。
- 架构代价：(1) 每个新连接器作者面对通用接口时看到三个与自己无关的 odps/iceberg 专属方法；(2) fe-core 通用契约被两个具体连接器塑形，违背 connector-agnostic 目标；(3) 按本仓库铁律（MEMORY: fe-core-source-isolation-iron-rules，fe-core 源须 connector-agnostic），把 odps 专属的 `allocateWriteBlockRange` 放到 fe-core `Transaction` SPI 上，**性质上比放在插件侧 connector-api 更违规**——这是这条发现里更该点名的一面。

**修复方案**（把 source-specific 语义下沉到窄 opt-in 能力接口，与本仓库既有 `supports*()` 能力 opt-in 范式一致）：

- 写块分配：新增 `WriteBlockAllocatingTransaction`（含 `allocateWriteBlockRange`），MaxComputeConnectorTransaction 实现。**关键修正（采纳 verify）**：FrontendServiceImpl.java:3885 的调用点静态类型是 **fe-core `Transaction`**（非 ConnectorTransaction），故收敛必须在 **fe-core `Transaction` 这一层**（或其 `PluginDrivenTransaction` 委派 wrapper）一并做——不能只在 connector-api 侧新增窄接口，否则 fe-core `Transaction` 上的 odps 默认方法与 `JdbcTransaction` 等的白继承仍在，泄漏未消。
- 压实重写：新增 `RewriteCapableTransaction`（含 `registerRewriteSourceFiles` + `getRewriteAddedDataFilesCount`），IcebergConnectorTransaction 实现；ConnectorRewriteDriver.java:151/167 先窄接口 `instanceof` 断言再调，把「不支持」从运行期 throw 提前为类型不匹配。此对只需在 connector-api 侧处理（未污染 fe-core）。

txn-id 粒度本身正确（getTransactionId/commit/rollback/close/addCommitData 是真通用契约），default 兜底无 correctness 缺陷，javadoc 已文档化这些方法的连接器专属性（属「有意但已记录的泄漏」），下沉为窄接口是自然收敛方向，属整洁度改进而非缺陷修复。

---

## #9 ConnectorBucketSpec 表分布 vs per-field bucket

**核实结论**：**CONFIRMED**，置信度 high。两阶段一致（verify.agrees=true），最终以 verify 为准——verify 认可 stage-1 全部事实断言与影响评估，仅做极小行号精修（不影响 verdict）。属潜伏的文档 / API-shape 错配，非活跃正确性 bug。

**原报告主张**：`ConnectorBucketSpec` 表级单 numBuckets / 字符串算法（含 "iceberg_bucket" 值），与 iceberg per-field transform 是类别错误；iceberg CREATE 从 partition spec 读 bucket[N]，从不消费这个 "iceberg_bucket" 值 → 无正确接线。

**核实过程**（读了当前以下 file:line）：

- `ConnectorBucketSpec` 为表级单一模型：字段 `List<String> columns` / `int numBuckets` / `String algorithm` 在 **:37-39**（verify 精修：stage-1 引的 "35-48" 是整个类声明起止范围而非精确字段行）。javadoc 算法值块横跨 **:29-33**（hive_hash 在 :30、**iceberg_bucket 在 :31**、doris_default 在 :32）。无 per-field / per-transform 结构。
- `grep "iceberg_bucket" --include=*.java` 全 fe/ 仅 1 命中 = ConnectorBucketSpec.java:31 的 javadoc——**无生产者、无消费者，纯死文档**。
- 唯一 bucketSpec 生产者 `convertBucket`（CreateTableInfoToConnectorRequestConverter.java:203-214）只置 `algorithm = d.isHash() ? "doris_default" : "doris_random"`，永不产 "iceberg_bucket" 也不产 "hive_hash"。
- iceberg CREATE 路径 `IcebergConnectorMetadata.createTable`（fe-connector-iceberg/.../IcebergConnectorMetadata.java，当前代码）只消费 getColumns/getPartitionSpec/getSortOrder/getProperties，**从不调 `getBucketSpec()`**。bucket[N] 由 partition spec 的 per-field transform 生成：`IcebergSchemaBuilder.buildPartitionSpec` 对 `case "bucket"` 调 `builder.bucket(column, intArg(...))`（IcebergSchemaBuilder.java:159-204，核心行 175-177）——这才是真实可用的接线。
- `grep getBucketSpec/bucketSpec` 于 fe-connector-iceberg/：生产代码零引用（仅测试代码命中原生 iceberg PartitionSpec builder，与 SPI 无关）。
- 真实 bucketSpec 消费者只有 hive（HiveConnectorMetadata.java:1646，仅认 doris_random 拒绝、其余当 hash）与 maxcompute（MaxComputeConnectorMetadata.java:600-604，仅认 "doris_default"）。

**背景**：`ConnectorBucketSpec`（:37-39）采用 Doris 传统「表级分布」模型：一组 bucket 列 + 整型 numBuckets + 算法字符串。javadoc（:29-33）号称支持三种算法值，含 `"iceberg_bucket" — Iceberg bucket transform`。而 iceberg 的 bucketing 语义是「per-field transform」：`PARTITION BY bucket(N, col)`，每个分区字段各带自己的 transform 和 bucket 数，属于 partition spec 而非表级单一 numBuckets。二者是两种不同模型（类别错误）。

**影响（具体示例）**：今天不产生错误查询结果，属潜伏。

- 正常路径：iceberg 建 bucket 分区表（例 `CREATE TABLE t(...) PARTITION BY LIST (bucket(16, id)) ...`）完全正常——`convertTransformField` 转成 `ConnectorPartitionField(transform="bucket", args=[16])`，`buildPartitionSpec` 再转成 iceberg `builder.bucket(id, 16)`，建出的 partition spec 正确。
- 潜伏风险 1（静默吞用户意图）：若用户对 iceberg 外表写 `DISTRIBUTED BY HASH(col) BUCKETS N`（Doris 表级分布语法），convertBucket 会产出 `algorithm="doris_default"` 的 bucketSpec，而 iceberg.createTable 完全忽略 `getBucketSpec()` → 该子句被**静默丢弃**（既不报错也不生效）。因 iceberg 本不用表级 hash 分布，危害有限。
- 潜伏风险 2（文档陷阱）：javadoc（:31）声称 "iceberg_bucket" 是合法算法值，后续开发者若据此在 convertBucket 加 iceberg 分支发 "iceberg_bucket" 并期望 iceberg.createTable 去 `getBucketSpec()` 消费，会发现根本没有接线端——白写。这正是「类别错误」埋的雷。

**修复方案**（均轻量、非功能性）：

- 首选（消除误导，最贴合外科手术式改动）：删除或改写 ConnectorBucketSpec.java:31 那行 javadoc，去掉从不存在的 "iceberg_bucket" 算法值，明确注明本 SPI 只承载表级 hash/random 分布，iceberg 的 per-field bucket 走 `ConnectorPartitionField` transform 路径。
- 可选（防静默吞子句）：若担心风险 1，可在 iceberg.createTable 里对非空 `getBucketSpec()` fail-loud（抛 DorisConnectorException 提示改用 `PARTITION BY bucket(N,col)`），对齐 hive 连接器 fail-loud 既有范式（HiveConnectorMetadata.java:1646 附近）。但需先确认 legacy iceberg 建表对 `DISTRIBUTED BY` 的原有行为再决定，避免行为回归。

因不产生错误结果，建议按 S 级低优先处理，首选仅修文档。

---

## 本类别小结

**哪些真**：三条发现（#7、#8、#9）**全部 CONFIRMED（high）**，两阶段无分歧。它们都是真实存在的设计 / 文档层抽象错配。

**哪些伪 / 已修**：无——本类别无伪报、无已修条目；但三条**均为 S 级 smell，无一为运行期 correctness bug**：各连接器要么 override、要么被能力位 / default / partition-spec 正确接线兜住，当前查询与写入结果都正确，均不阻塞。

**共性根因**：通用 SPI 层「被具体连接器塑形」——把单一连接器的专属语义或不存在的承诺塞进本应 connector-agnostic 的通用接口 / 结构：
- #7 是把静态分区 spec 伪装成 free-form bag 并在 javadoc 承诺永不存在的 key；
- #8 是把 odps write-block、iceberg compaction 方法以 default 塞进通用事务接口（且 write-block 还双重泄漏到 fe-core `Transaction`）；
- #9 是把 iceberg per-field bucket 塞进表级分布模型的 javadoc 却无任何接线。
三者的共同修复方向一致：**要么收窄 / 删除误导性文档（#7、#9 首选），要么下沉到 opt-in 窄能力接口（#8），使通用 SPI 保持 connector-agnostic**——这与本仓库既有 `supports*()` 能力 opt-in 范式、以及 fe-core 源隔离铁律（MEMORY: fe-core-source-isolation-iron-rules、catalog-spi-plugindriven-no-source-specific-code）直接呼应。

**与其他类别的关联**：#8 中 write-block 泄漏到 fe-core `org.apache.doris.transaction.Transaction`，是本类别里唯一触碰 fe-core 源隔离铁律的一面，性质比纯插件侧泄漏更重，与「fe-core 通用契约不得被 source-specific 代码污染」的架构主线（其它类别的 PluginDrivenScanNode connector-agnostic 要求同源）相通；#7/#9 的「javadoc 承诺 vs 实际接线」错配则属纯文档层收敛，可在同一轮 SPI 清理中一并处理。
