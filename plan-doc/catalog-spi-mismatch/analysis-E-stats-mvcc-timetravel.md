# 类别 E — 统计 / MVCC / 时间旅行粒度

## 范围说明与基线声明

本类别覆盖 catalog SPI 迁移中与**统计信息（row count / stats）**、**MVCC 快照**、**时间旅行（FOR VERSION / FOR TIME AS OF）**、以及 MVCC 值对象**语义粒度/形态**相关的 7 条发现（#5、#15、#16、#17、#18、#24、#25）。核心议题：查询时点解析出的快照（snapshot/version）是否被一致地 pin 到 schema、stats、scan 各条路径上，以及承载 MVCC/新鲜度信息的 SPI 值类型是否语义清晰、单位正确。

**基线声明**：本核实基于当前工作树 branch `catalog-spi-2-lvl-cache`，逐条独立用 Read/Grep 读当前代码 + 对抗复核（verify），非轻信原 survey。所有结论均引 file:line；凡 verify 阶段与 investigate（stage-1）分歧处，均已注明最终以哪一阶段为准及理由。行号以当前工作树为准（个别注释/javadoc 偏移可能有 ±1~2 行漂移，不影响结论）。

## 总表

| # | 严重 | 原报告结论 | 核实结论 | 一句话 |
|---|------|-----------|---------|--------|
| 5 | 🔴 F | iceberg 行数用表级公式而非 snapshot·delete-aware，equality-delete 未扣除/gate | **REFUTED**（高） | 三点全部反了：行数取 currentSnapshot summary（snapshot 级）、已扣 position delete（delete-aware）、并对 equality delete 显式 gate 到 -1/UNKNOWN；方向性误读 |
| 15 | 🟠 S | getTableStatistics 无 snapshot 参数，time-travel 下 stats 偏斜 | **CONFIRMED**（高） | 属实：stats 恒取 currentSnapshot，与 schema 已 pin 不对称；但仅估计偏斜、不错结果，且是 legacy 忠实端口非迁移回归 |
| 16 | 🟠 S | getNewestUpdateTimeMillis 名说毫秒实微秒 | **PARTIAL**（高） | 单位/命名错配为事实，但非 S 级正确性 bug；stage-1 "零算术消费者"论据被 verify 推翻——CacheAnalyzer 确有 wall-clock 毫秒算术，只是后果为安全抑制 iceberg SqlCache |
| 17 | 🟠 F | MVCC branch ref 定点 vs 移动，plan→scan 间 head 移动致列错配/崩溃 | **REFUTED**（高） | 机制单点属实，但 IcebergStatementScope 保证每语句单次冻结加载，plan/scan 复用同一冻结 Table，移动窗口不存在 |
| 18 | 🟠 F | FOR TIME AS OF 数字串当 epoch，date/epoch 歧义 | **PARTIAL**（高） | 数字正则→当 epoch 属实，但合法 datetime 字面量必含非数字字符恒不误判；"歧义"定性夸大，且系跨连接器有意对齐设计 |
| 24 | 🟡 S | getFreshnessValue 一 long 两粒度，语义歧义 | **PARTIAL**（高） | 一 long 两粒度属实，但由并列 Freshness 判别枚举明确标定（tagged-union），消费侧无条件分派，构造不出失败场景 |
| 25 | 🟡 S | ConnectorMvccSnapshot 唯独缺 equals/hashCode | **CONFIRMED**（高） | 事实精确命中，纯形态/一致性 smell，零运行时影响；作为 backlog 一致性项即可 |

---

## #5 iceberg 行数丢 equality-delete gate

**核实结论**：**REFUTED**（高置信）。investigate 与 verify 完全一致（verify `agrees=true`、`corrections=none`），verify 独立 Read 复核两条行数路径全部关键位点，与 stage-1 引用逐条吻合。主张的三项事实断言全部不成立，方向完全反了。

**原报告主张**：iceberg 行数用表级公式而非 snapshot·delete-aware，equality-delete 未从行数扣除/gate。

**核实过程**：iceberg 行数在两条路径上，均基于 current snapshot 的 summary，且既 delete-aware、又对 equality-delete 显式 gate——与主张的每一项断言相反。
- 表级统计 `IcebergConnectorMetadata.computeRowCount`（IcebergConnectorMetadata.java:746-768）：首取 `table.currentSnapshot().summary()`（**snapshot 级，非表级静态公式**）；三个计数器 `total-equality-deletes` / `total-records` / `total-position-deletes` 任一为 null → 返回 -1（761-762，兼容 compaction/overwrite 快照可能省略计数器，避免旧代码 `Long.parseLong(null)` NPE）；`total-equality-deletes != "0"` → 返回 -1（764-765）；否则 `total-records - total-position-deletes`（767，**已扣除 position delete，delete-aware**）。`getTableStatistics`（715-734）再把 `rowCount<=0` 折叠成 `Optional.empty()`（对应 legacy "0 -> UNKNOWN" 语义，系统表直接 empty）。
- COUNT(*) 下推 `IcebergScanPlanProvider.getCountFromSummary`（IcebergScanPlanProvider.java:2295-2306）：共享同一 null-guard（2299）与 equality-delete gate——`equalityDeletes != "0"` → `return -1` 不下推（2303-2304）。另 `mayHaveEqualityDeletes` 类逻辑（1757-1759）亦按 `total-equality-deletes` 判断。
- 两处代码注释均明标为 legacy `IcebergUtils.getCountFromSummary` 的忠实移植（upstream 32a2651f66b，#64648），且本就是 delete-aware。

**详解（主张为何不成立）**：主张的三点逐条与代码相反——
- "行数用表级公式而非 snapshot·delete-aware"：错。`computeRowCount` 读的是 `currentSnapshot().summary()`（IcebergConnectorMetadata.java:747），是 snapshot 级；并从 `total-records` 扣除 `total-position-deletes`（767），是 delete-aware。
- "equality-delete 未从行数扣除/gate"：错，恰好相反。equality delete 在读时需重投影、summary 无法精确净出，所以代码**显式** gate 到 -1（computeRowCount:764-765；下推:2303-2304），position delete 则被扣除。

即"未 gate equality delete"恰是代码刻意避免的场景，主张方向完全反了，可能基于过时或误读的代码。

**具体反例验证主张站不住**：一张带 `total-equality-deletes="5"` 的 iceberg 表，`computeRowCount` 走到 764-765 直接返回 -1，`getTableStatistics` 返回 `Optional.empty()`，CBO 拿到 UNKNOWN 而**不会**用未扣 equality delete 的 `total-records` 错误行数；COUNT(*) 下推路径同样在 2303-2304 return -1 不下推。主张担忧的"用未扣 equality delete 的行数"场景在代码里根本不可达。

**结论**：REFUTED，高置信，无需修复。若报告本意是"equality-delete 情况下行数退化为 UNKNOWN（而非精确估）"，那是有意的保守设计（summary 无法精确净出 equality delete），并非缺陷。

---

## #15 getTableStatistics 无 snapshot 参数

**核实结论**：**CONFIRMED**（高置信）。investigate 与 verify 两阶段完全一致，无分歧。verify 独立复核全部关键位点，与 stage-1 引用逐条吻合，并补充了一条同源佐证（partition 枚举同样非快照 pin）。

**原报告主张**：`fetchRowCount` 解析新 handle 时不带 MvccSnapshot，iceberg `computeRowCount` 用 `table.currentSnapshot()`；`FOR VERSION AS OF <old>` 查询下优化器拿 latest 行数、scan 读 pinned old 快照 → stats/scan 粒度偏斜（仅估计，不错结果）；schema 已原子 pin、stats 没有。

**核实过程**：
- SPI 签名 `ConnectorStatisticsOps.getTableStatistics(ConnectorSession session, ConnectorTableHandle handle)`（ConnectorStatisticsOps.java:32-36）不含任何 MvccSnapshot/version 参数；同接口 getColumnStatistics/estimateDataSizeByListingFiles/listFileSizes 均无快照维度。
- fe-core 消费点 `PluginDrivenExternalTable.fetchRowCount()`（PluginDrivenExternalTable.java:1148-1160）：session 来自 `buildCrossStatementSession()`（1152，跨查询作用域、缓存性质，天然非 per-query），经 `resolveConnectorTableHandle`（1155）→ `getTableStatistics`（1160）。
- `resolveConnectorTableHandle`（PluginDrivenExternalTable.java:116-120）仅调 `metadata.getTableHandle(session, dbName, getRemoteName())`，不 thread 任何快照/版本。
- iceberg 实现 `IcebergConnectorMetadata.getTableStatistics`（IcebergConnectorMetadata.java:716-734）→ `computeRowCount(loadTable(session, iceHandle))`；`computeRowCount`（746-767）首行即 `Snapshot snapshot = table.currentSnapshot();`（747），返回 `total-records - total-position-deletes`（767，命中 equality-delete 时 gate 返回 -1）。该方法类注释（736-744）明写 faithful port of legacy `IcebergUtils.getIcebergRowCount`。
- paimon 同形：`getTableStatistics(session, handle)`（PaimonConnectorMetadata.java:1183-1194）→ `catalogOps.rowCount(resolveTable(paimonHandle))`，handle 无快照维度。
- 对照 schema 路径：`getPartitionColumns` / `getFullSchema` / `getNameToPartitionItems`（PluginDrivenExternalTable.java:632/689/807）均带 `Optional<MvccSnapshot>` 并经 `getSchemaCacheValue(snapshot)` pin——证实"schema pin、stats 不 pin"的不对称成立。
- verify 补充：`getNameToPartitionItems`（807-855）虽签名带 snapshot，内部仍走无快照的 `resolveConnectorTableHandle`（821）+ `listPartitions(..., Optional.empty())`（831），即分区枚举同样非快照 pin——说明这是一族既有局限（与 P6-1 stats-grain 同源），非 stats 单点特例。
- legacy 对照：master 分支 `IcebergUtils.getIcebergRowCount` 同样无条件 `icebergTable.currentSnapshot()`（无时间旅行感知），证实连接器是忠实端口，非迁移引入的回归。

**背景**：统计 SPI 的签名与 schema 系列 SPI 形成不对称——后者能把查询时点解析出的快照原子 pin 进 handle，而 fetchRowCount 经 `resolveConnectorTableHandle` 拿一个"裸"handle。因此行数天生是"表级、latest 快照"的口径，且 session 来自跨查询缓存作用域，更不可能携带某条查询的时间旅行时点。

**影响（具体示例）**：
输入：iceberg 表 `t` 有快照 S1（100 万行）后追加到快照 S2（300 万行，当前）。查询 `SELECT ... FROM t FOR VERSION AS OF <S1> JOIN big ON ...`。
- 扫描侧经 MVCC applySnapshot 正确 pin 到 S1，只读 100 万行数据文件；
- 但 CBO 走 fetchRowCount → getTableStatistics → computeRowCount（currentSnapshot=S2）= 300 万，优化器按 300 万做基数估计和 join reorder。
- 后果：stats 与真实扫描量偏斜 3 倍，可能选出次优的 join 顺序/分布方式/build-probe 侧。
- **关键限定**：这只影响执行计划的代价估计，**不改变结果正确性**——扫描输出仍是 S1 的 100 万行真实数据。反向（FOR VERSION AS OF 一个后续被 DELETE 压缩的更"重"历史快照）则会低估。paimon time-travel 上同构。

**修复方案（可选、非必须）**：与 P6-1 stats-grain 是同族问题。若要真正修复，给统计 SPI 增加快照维度——`getTableStatistics` 签名引入 `Optional<ConnectorMvccSnapshot>`（或让 fetchRowCount 侧把查询时点解析的快照 thread 进 handle），iceberg 侧改用 `table.snapshot(snapshotId)` 而非 currentSnapshot，paimon 类同。约束：(a) fetchRowCount 现走跨查询缓存 session，行数被 ExternalRowCountCache 缓存复用，做成 per-snapshot 需重设缓存键，成本不小；(b) 收益仅 CBO 估计精度、不涉正确性。综合看，保持现状（接受估计偏斜）是合理工程取舍，修复应与 schema/stats/partition 快照粒度统一治理时一并做，而非单点改。严重度 S/orange + "仅估计"标注公允甚至略偏保守。

---

## #16 getNewestUpdateTimeMillis 名说毫秒实微秒

**核实结论**：**PARTIAL**（高置信）。命名/单位错配为 CONFIRMED 事实，但非 S 级正确性缺陷（PARTIAL 成立）。**最终以 verify 为准**：verify `agrees=false`，因为 stage-1 的核心论据（"唯一消费者是 Dictionary"、"从不与任何 millis 量做算术"）**事实错误**——存在第二类消费者 CacheAnalyzer 确实把该微秒值与 wall-clock 毫秒混算。但该算术足迹的后果是良性的，故 severity 判 PARTIAL 仍成立。

**原报告主张**：`getNewestUpdateTimeMillis` 名字说 millis，但源（iceberg）定义是微秒，单位错配。

**核实过程**：
- SPI 定义 `ConnectorMvccPartitionView.getNewestUpdateTimeMillis()`（ConnectorMvccPartitionView.java:113），返回字段 `newestUpdateTimeMillis`（70），方法名带 "Millis" 后缀。javadoc（101-111）明写"单位由源定义、iceberg 为 MICROSECONDS、勿当 millis / 勿转换、只依赖单调性、与 master parity"。
- iceberg 实现：`IcebergPartitionUtils.java` 从 PARTITIONS 元数据表 `row.get(9)` 读 `last_updated_at`（769-770 行布局注释，806 `lastUpdateTime=row.get(9,Long)`，iceberg 微秒），`newestUpdateTimeMillis = max(lastUpdateTime)`（603-604），传入 view 构造器（605-606）。名义 millis、实际 micros，错配属实。
- range-view 透传：`PluginDrivenMvccExternalTable.java:815-823` 的 range-view 路径返回 `pin.getNewestUpdateTimeMillis()` 作为 `getNewestUpdateVersionOrTime()`。
- 消费者复核（verify 关键补充）：grep `getNewestUpdateVersionOrTime` 全部 caller，发现 stage-1 漏判的第二类消费者：
  - **Dictionary.java:271-296** `hasNewerSourceVersion()`——只把返回值 `tableVersionNow` 与自身此前存的 `srcVersion` 做 `<`/`>` 单调比较（282/287），从不换算、不与 millis 算术。（stage-1 已正确覆盖）
  - **CacheAnalyzer.java:489** `latestPartitionTime = getNewestUpdateVersionOrTime()`；:258 `now = Math.max(now, latestPartitionTime)`；:263 `(now - latestPartitionTime) >= cache_last_version_interval_second * 1000L`；:385-387 `nowtime() = System.currentTimeMillis()`。**微秒 token 确实进入了 wall-clock 毫秒算术**。（stage-1 漏判）
  - SqlCacheContext.java:200、NereidsSqlCacheManager.java:479——把该值当版本 token 做等值/不等比较，无单位算术。

**背景**：该值定位为 iceberg 分区新鲜度探针的载荷，javadoc 明确要求调用方"只依赖单调性、从不依赖绝对刻度"，并与 master `IcebergExternalTable.getNewestUpdateVersionOrTime = max(partition.lastUpdateTime)` 逐字对齐。绝大多数消费者（Dictionary、SqlCacheContext、NereidsSqlCacheManager）确实只做单调/等值比较。

**影响（具体示例）**：
- 单调探针路径（Dictionary）：一张按 DAY transform 分区的 iceberg 表，某次探针取到 `last_updated_at = 1_700_000_000_000_000`（微秒）；写入新数据后快照更新，下次取到更大微秒值；`hasNewerSourceVersion` 判 `新值 > srcVersion` → 触发字典刷新。无论单位 micros/millis 结果一致，无失效路径。
- **wall-clock 混算路径（CacheAnalyzer，verify 揭示的真实算术足迹）**：iceberg 微秒（~1.7e15）恒大于 epoch 毫秒（~1.7e12）。经 `now = Math.max(nowtime(), latestPartitionTime)` 把 `now` 抬到微秒刻度；:263 只对全局最大值持有者判 gate，`now == 该最大值` → 差值 ≈ 0 → "数据已静默 ≥ 30s"闸门**永不通过** → 触及该 iceberg 表的查询**实质上永不启用 SqlCache**。这是缓存**被安全抑制**（correctness-safe：不会误命中陈旧结果、不会误启用缓存），且与 master 行为一致（master 同样返回微秒）。因此**不构成正确性 bug**，只是 iceberg 表默默拿不到 SqlCache 的性能优化。

**为何仍判 PARTIAL 而非 S 级**：命名/单位错配是 CONFIRMED 的客观事实，但唯一真实的算术足迹（CacheAnalyzer）后果良性（安全抑制 + master parity），不存在"什么表/什么查询 → 错误结果"的可复现失效路径。故 severity 判 PARTIAL 成立。**须更正 stage-1 的错误表述**："零算术消费者 / 唯一消费者 Dictionary"是错的；正确表述为"存在一处 wall-clock 毫秒算术消费者（CacheAnalyzer），但其后果仅为 iceberg SqlCache 被安全抑制且与 master parity，故不构成正确性缺陷"。

**修复方案（低优先级，纯整洁化）**：若清理重命名，把方法/字段更名为不带单位语义的名字（如 `getNewestUpdateMonotonicMarker()`），同步 ConnectorMvccPartitionView.java、PluginDrivenMvccSnapshot.java:173、IcebergPartitionUtils.java:603 及相关测试。**根因值得一并标注**：CacheAnalyzer 端 `latestPartitionTime` 同时承载"毫秒时间戳"与"不透明版本 token"两义（把该 token 与 wall-clock `now` 混算），这一语义混淆才是真正的形态问题。属命名/语义整洁，不涉行为变更，删旧代码期不建议优先动。

---

## #17 MVCC branch ref 定点 vs 移动

**核实结论**：**REFUTED**（高置信）。investigate 与 verify 完全一致，verify 独立 Read/Grep 复核全部 load-bearing file:line，逐条吻合，仅补两点非纠正性说明。

**原报告主张**：`ConnectorMvccSnapshot` 是定点（snapshotId+schemaId），但 branch 是移动引用（head 会进）；applySnapshot 路由到 `scan.useRef(name)`，scan 跟 branch head（忽略 pin 的 snapshotId），而 schema/stats 定点；plan→scan 之间对 branch 的 schema-changing commit → BE 用旧 schemaId 读新 schema 数据 → 列错配/错行/崩溃；破了 pin 的 query 内一致版本契约。

**核实过程**：主张描述的每个单点机制在代码中逐一存在，但据以推导的 bug 前提（plan→scan 之间 branch head 会移动）被"每语句只加载一次表"的作用域堵死。
- pin 捕获 `IcebergConnectorMetadata.resolveRef`（IcebergConnectorMetadata.java:1787-1798）：对 BRANCH/TAG/VERSION_REF 按 REF NAME 定点——`snapshotId = ref.snapshotId()`（1794）、`schemaId = SnapshotUtil.schemaFor(table, refName).schemaId()`（1792）、ref 名塞进 REF_PROPERTY（1796），均来自传入的 table。
- `resolveTimeTravel`（1733-1735）对 BRANCH 走 `resolveRef(..., SnapshotRef::isBranch)`（1766），而 pin 捕获经 `loadTable(session, handle)`（625）→ `resolveTableForRead`（641-647）→ `IcebergStatementScope.sharedTable`。
- `applySnapshot`（1821-1834）把 snapshotId + ref + schemaId 一起 `withSnapshot` 到 handle（1833）。
- scan 侧 `IcebergScanPlanProvider.buildScan`（IcebergScanPlanProvider.java:1099-1124）：`if (handle.getRef()!=null) scan = scan.useRef(handle.getRef())`（1106）else `useSnapshot`（1108），在冻结的 `table` 上。
- schema 定点：dict schema `pinnedSchema`（1141-1150）按 `handle.getSchemaId()` 取 `table.schemas()`；slot schema `getTableSchema(...,snapshot)`（IcebergConnectorMetadata.java:448-468）按 `snapshot.getSchemaId()`。二者绑同一被捕获的 schemaId（经 applySnapshot withSnapshot 1833 回填），注释（1131-1139 / 460-464）强制两侧 fallback 字节一致（SIGABRT 防护不变式）。
- **决定性证据——三条路径读同一个每语句冻结的 Table 实例**：`IcebergStatementScope.sharedTable`（IcebergStatementScope.java:59-67）key = `"iceberg.table:"+catalogId+":"+db+":"+tbl+":"+queryId`，`session.getStatementScope().computeIfAbsent(key, loader)`——同一 queryId 下 pin 路径（resolveTableForRead 641-647）与 scan 路径（resolveTable，IcebergScanPlanProvider.java:2341-2358）key 完全相同，loader 至多跑一次，后到者复用同一 RAW Table 对象。类注释（31-47）明说 read metadata / scan planning / write 都解析同一张表。
- verify 补充：作用域不仅堵住"两次远端 reload"，还屏蔽跨查询 tableCache 在 pin 与 scan 之间刷新——loader 只为首个调用者运行（ScanProvider 2348-2357 / Metadata 643-646），后到者恒拿 scope 内已冻结对象，与 tableCache 当前内容无关。

**为何非问题**：主张的失败场景要成立，必须在同一条 SELECT 内发生两件事：(a) pin 阶段从 branch 读到 schemaId=S_old；(b) scan 阶段 useRef 又从远端读到已推进的新 head（schema=S_new 的新数据）。只有当 pin 与 scan 分别触发两次独立远端 loadTable 时，这个"中间 commit"窗口才存在。而 `IcebergStatementScope` 保证单条语句对某张表只加载一次并冻结，useRef 是在这份不可变的内存 TableMetadata 上解析 branch，永远解析回 pin 时捕获的同一个 `ref.snapshotId()`，其 schema 恰是被定点的 schemaId。

**具体反例验证主张站不住**：表 `db.t` 有 branch=b1（head=snap_100，schema=S5）。执行 `SELECT * FROM t FOR VERSION AS OF 'b1'`：
- resolveRef 在 statement scope 首次加载 Table@T0，得 snapshotId=100、schemaId=5、REF_PROPERTY=b1。
- 即便此刻别的 writer 向 b1 提交了 snap_200（schema 演进为 S6），
- buildScan 复用同一个 Table@T0（scope 命中，不再 reload），`useRef("b1")` 在 T0 的 refs 上解析 → 仍是 snap_100；dict/slot schema 仍是 S5。
- BE 拿到的是 (snap_100 数据 + S5 schema)，完全自洽，不会出现"用 S5 读 S6 数据"。
- 要读到 snap_200 只能是下一条 query（新 queryId → 新 scope → 新加载），那时 pin 与 scan 又各自基于 T1 自洽。跨 query 读到更新的 head 是 branch 的正确语义（移动引用本就该跟 head），不是 bug。

**其他说明**：
- `useRef` 而非 `useSnapshot(ref.snapshotId())` 纯为 legacy parity（注释 IcebergScanPlanProvider.java:1102-1103、IcebergConnectorMetadata.java:1785、IcebergTableHandle.java:34-38），在冻结元数据上二者等价，无副作用。
- 唯一"每次都加载"的分支：`sharedTable` 在 `session==null`（离线/直构造单测）或 `ConnectorStatementScope.NONE`（离线 planning）下每次 loadTable（IcebergStatementScope.java:43-46,60-62）。但那是无活跃语句的离线路径；生产 SELECT 持有真实 StatementContext scope，读侧走 computeIfAbsent 单次加载，无此窗口。

**结论**：主张把"branch 是移动引用 + useRef 跟 head"正确识别为机制，但错误假设 pin 与 scan 会二次 reload 从而暴露移动窗口；实际由 `IcebergStatementScope` 保证 query 内单次冻结加载，一致性契约被保证而非被破坏。评为 REFUTED，高置信。

---

## #18 FOR TIME AS OF 数字串当 epoch

**核实结论**：**PARTIAL**（高置信）。investigate 与 verify 一致：机械事实 CONFIRMED（数字正则 → 当 epoch），"date/epoch 歧义/bug"定性 REFUTED，整体 PARTIAL、严重度低。verify 补两点非纠正说明。

**原报告主张**：FOR TIME AS OF 用数字 regex 判定，数字串被当 epoch，产生 date/epoch 歧义。

**核实过程**：
- fe-core `PluginDrivenMvccExternalTable.toTimeTravelSpec`（PluginDrivenMvccExternalTable.java:430-431）：当 `snap.getType()==VersionType.TIME` 时 `return ConnectorTimeTravelSpec.timestamp(value, isDigital(value))`。TIME 分支无条件把 digital 标志透传给连接器，fe-core 不做 datetime 校验（source-agnostic 分派）。
- `isDigital`（452-453）= `DIGITAL_REGEX.matcher(value).matches()`，`DIGITAL_REGEX = Pattern.compile("\\d+")`（99，注释标 parity `PaimonUtil.isDigitalString`）——即纯数字串。
- SPI 契约 `ConnectorTimeTravelSpec`（ConnectorTimeTravelSpec.java:43-46,118-128）：digital=true 表示 value 已是 epoch-millis，false 表示待解析 datetime 串。
- iceberg `IcebergConnectorMetadata.parseTimestampMillis`（IcebergConnectorMetadata.java:1806-1812）：`if (spec.isDigital()) return Long.parseLong(getStringValue())`（当 epoch 毫秒），否则 `IcebergTimeUtils.datetimeToMillis(...)` 按 session 时区解析。结果喂 `SnapshotUtil.snapshotIdAsOfTime`（1749-1752）。javadoc（1800-1804）明写 legacy 一贯按 datetime 串解析、数字值本会失败，honoring digital 是 "benign superset"。
- `IcebergTimeUtils.java:49` `DATETIME_FORMAT = ofPattern("yyyy-MM-dd HH:mm:ss")`，datetimeToMillis（97-99）用它 `LocalDateTime.parse`——合法 datetime 字面量必含 `-`/`:`/空格，`isDigital` 对其恒 false，无 date/epoch 交叠歧义。
- paimon 对齐：`PaimonConnectorMetadata.java:694-695` 同样 `if (spec.isDigital()) return Long.parseLong(value)`，注释（672-673）标忠实复刻 legacy `PaimonUtil.getPaimonSnapshotByTimestamp`。
- verify 补充：当前工作树中 legacy iceberg 的 FOR TIME AS OF 解析代码已被删除（`fe-core/.../datasource/iceberg/` 下无 timeStringToLong/snapshotIdAsOfTime/VersionType.TIME 命中），故"legacy 把数字值当 datetime 解析会失败"这一对照现基于 IcebergConnectorMetadata.java:1800-1804 与 IcebergTimeUtils.java:47-53 的 byte-parity 注释佐证，而非现存 legacy 源码——不影响结论。

**为何"歧义"定性夸大（REFUTED 部分）**：合法 datetime 字面量（`2023-01-01 00:00:00`）恒含非数字字符，`isDigital` 永远为 false，永远走 datetime 分支，不会被误当 epoch。真实日期与 epoch 之间没有任何交叠歧义。唯一会被当 epoch 的是"裸整数串"，而裸整数在 legacy 下从来不是合法 FOR TIME AS OF 输入。

**影响（具体示例）**：
- 合理用法：`SELECT * FROM ice_tbl FOR TIME AS OF '1700000000000'` → digital=true → epoch 1700000000000ms（2023-11）→ `SnapshotUtil.snapshotIdAsOfTime`。这是使用者显式想按 epoch 取的合理用法。
- 误报级边角：`FOR TIME AS OF '20231001120000'`（用户以为是紧凑日期 yyyyMMddHHmmss）→ digital=true → 被 parseLong 当成 epoch 毫秒（约公元 2611 年）。verify 澄清机制：`SnapshotUtil.snapshotIdAsOfTime` 对晚于所有快照的时刻**静默返回最新快照**（仅当早于首个快照才抛异常 → 被 catch 成 empty，见 1720 注释），而非报错。注意：此紧凑格式在 Doris 下从来不是合法 datetime 输入，legacy iceberg 对它会 `datetimeToMillis` 解析失败抛异常；新连接器把它当 epoch 属于"更宽松地接受"而非在合法日期语义下静默误判——且 :1800-1804 明确记为有意的 benign superset。

**为何非缺陷 / 属既定设计**：digital → epoch 路径是跨连接器统一的既定契约：paimon:694-695 同样 parseLong 并标忠实复刻 legacy；`ConnectorTimeTravelSpec` javadoc（43-46,118-128）把"digital ⇒ epoch-millis 字面量"写进 SPI 契约；iceberg 侧有意镜像该语义并成文说明。

**结论**：机械事实为真（CONFIRMED 部分），但"date/epoch 歧义"危害定性不成立（REFUTED 部分）——合法日期串不会落入 epoch 分支，真正受影响的仅是本就非法的裸整数输入，且系有意对齐设计。整体 PARTIAL，实际严重度低。若仍要收紧，可在 fe-core 分派处对 TIME 类型不打 digital 标志（强制走 datetime 解析），或对紧凑数字串给出明确 "unsupported datetime format" 报错而非静默当 epoch；但这会破坏与 paimon 的既有对齐，需产品层拍板，不宜按 bug 直接改。

---

## #24 getFreshnessValue 一 long 两粒度

**核实结论**：**PARTIAL**（高置信）。investigate 与 verify 一致：字面观察正确（一 long 两种粒度），但由并列判别枚举完全化解歧义，非缺陷。verify 独立复核全部位点，并纠正 stage-1 analysis 草稿一处自相矛盾的括注（不影响 verdict）。

**原报告主张**：`getFreshnessValue` 一个 long 承载两种粒度——snapshot-id 或 millis（取决于源），语义歧义。

**核实过程**：
- `ConnectorMvccPartition.getFreshnessValue()`（ConnectorMvccPartition.java:49/82-84）确为单个 long，javadoc 明写"snapshot id or epoch-millis timestamp, per the view's freshness kind"。字面主张成立。
- 判别标签在同一对象图上：`ConnectorMvccPartitionView.Freshness` 枚举 `{SNAPSHOT_ID, LAST_MODIFIED}`（ConnectorMvccPartitionView.java:59-65），javadoc 各自绑定 `MTMVSnapshotIdSnapshot` / `MTMVTimestampSnapshot`；构造器（72-74）payload 与标签一次传入，原子绑定，不存在"给了值忘了给粒度"的窗口。
- 传递侧：`PluginDrivenMvccExternalTable.java:213-214` 读 `view.getFreshness()==SNAPSHOT_ID` 固化为 boolean `snapshotIdFreshness`；:229 存 per-partition freshnessValue；:232-233 一并进 pin（PluginDrivenMvccSnapshot.java:62/122/165-166）。
- 消费侧 `getPartitionSnapshot`（PluginDrivenMvccExternalTable.java:680-711）：先无条件查 `pin.isSnapshotIdFreshness()`（688）→ `MTMVSnapshotIdSnapshot(value)`（692）；否则按 `isLastModifiedFreshness`（hive on-demand probe，702-706）或 pin-timestamp（710，`MTMVTimestampSnapshot`）分派。无"拿到 value 不知粒度"的路径。
- 实际使用者收敛：iceberg 建 view 时两处（空表分支 557-558 + 正常分支 605-606，构造 partition 596）均**硬编码 `Freshness.SNAPSHOT_ID`**，恒发 snapshot-id、从不发 LAST_MODIFIED；paimon 不 override `getMvccPartitionView`（PaimonConnectorMetadata.java:105 / PaimonConnector.java:137），走默认 LIST/timestamp 路径，根本不经此 range-view long；hive override 仅委派给 sibling metastore（HiveConnectorMetadata.java:1399-1402）。

**为何不是 bug（PARTIAL）**：这个 long 是一个带标签联合（tagged union）的负载字段，判别标签始终在同一对象图上，消费侧无条件按标签分派，属标准 tagged-union 模式，不产生错误结果。构造不出"什么表/什么查询 → 什么错误结果"的例子：任何取用 freshnessValue 处都先读判别位。若强行让 iceberg 发错标签，那是连接器自身 bug，与"一个 long 两粒度"的 API 设计无关。当前 range-view 路径唯一实现者是 iceberg 且恒为 SNAPSHOT_ID，两粒度并存只是 API 面预留、实运行不共存。定性符合报告自评 🟡/S/潜伏。

**须更正 stage-1 一处笔误（不影响 verdict）**：stage-1 analysis 草稿写"snapshot-id 连接器（iceberg/paimon）永不落入 timestamp 分支"——**不准确**。paimon 恰是 timestamp 连接器：不 override range-view，`getPartitionSnapshot` 中落入 710 行 `MTMVTimestampSnapshot`（pin-timestamp 分支），即"落入 timestamp 分支"。正确表述：**iceberg 走 snapshot-id 分支、paimon 走 pin-timestamp 分支，二者均不触达 hive 的 on-demand probe（694-706，由 isLastModifiedFreshness 门控）；真正会读 timestamp 语义的正是 paimon**。源码 698-699 注释 "a snapshot-id connector (paimon/iceberg) NEVER reaches the probe" 中的 (paimon/iceberg) 是"非 hive"的宽松分组，非指 paimon 用 snapshot-id 语义。stage-1 的 code_reality 段本身已正确写明"paimon 走 LIST/timestamp 路径"，此仅为草稿括注与 code_reality 之间的自相矛盾。

**修复方案**：非必需。若要进一步消除 API 面表意负担，可把 payload 拆成语义命名的两个 getter，或引入小的值对象承载 (kind,value)；但属 taste 级重构，且会碰本项目"fe-core/SPI 只减不增、surgical"纪律，收益极薄，删旧代码期不建议动。

---

## #25 ConnectorMvccSnapshot 无 equals/hashCode

**核实结论**：**CONFIRMED**（高置信）——严格限定为主张自己声明的 minor/latent "shape smell"范畴，非功能缺陷。investigate 与 verify 一致，verify 仅指出 stage-1 若干行号轻微漂移及一点补充（四个兄弟类还都定义了 toString）。

**原报告主张**：4 个 MVCC/stats 兄弟类（Partition/View/TimeTravelSpec/TableStatistics）都定义了 equals+hashCode，唯 `ConnectorMvccSnapshot` 没有 → 不能按 (table,version) 做 value-key，shape smell。

**核实过程**：
- `ConnectorMvccSnapshot`（ConnectorMvccSnapshot.java:34-150）是 final class，含 6 字段（snapshotId/timestampMillis/description/schemaId/properties/lastModifiedFreshness）+ Builder，body 末尾直接是 `build()`（约 148），**没有** override equals/hashCode/toString（verify 通读全文 1-150 确认）。
- 四个兄弟类均有完整 equals+hashCode+toString：ConnectorMvccPartition（equals@88, hashCode@103, toString@108）、ConnectorMvccPartitionView（equals@118, hashCode@133, toString@138）、ConnectorTimeTravelSpec（equals@192, hashCode@207, toString@212）、ConnectorTableStatistics（equals@51, hashCode@63, toString@68）。
- 两个 fe-core 包装类也都无 equals/hashCode：`ConnectorMvccSnapshotAdapter.java:32-41`（仅持有 snapshot + getSnapshot()）、`PluginDrivenMvccSnapshot.java:54-141`（持有 connectorSnapshot 字段并 delegate getters）。
- 全仓 grep `ConnectorMvccSnapshot` 的 value-key 用法（`.equals`/`.hashCode`/`Set<`/`Map<` key/distinct/contains/HashSet）→ **零命中**。生产者 HudiConnectorMetadata:503（+ paimon 测试），消费者 PluginDrivenMvccExternalTable:380（`resolved.get()`），测试走逐字段 getter 断言。它只作为不透明 pin 按引用穿过查询生命周期（beginQuerySnapshot 产出 → 包进 adapter/PluginDrivenMvccSnapshot → BE 序列化边界 unwrap）。
- verify 行号更正（annotation/javadoc 偏移，非实质）：ConnectorTimeTravelSpec equals @192（stage-1 记 191-209）、ConnectorTableStatistics equals@51/hashCode@63（stage-1 记 50-65）。补充：四兄弟均定义 toString，真正 parity 目标是 3 方法（equals+hashCode+toString）而非 2；stage-1 已把 toString 标为可选，无实际 gap。

**背景**：MVCC/stats 值对象家族里，四个 immutable value 类都实现了逐字段 equals+hashCode，同族的 ConnectorMvccSnapshot 独缺，构成家族内一致性缺口（shape smell）。

**影响评估——零运行时影响**：当前没有任何真实输入/表/查询会因此产生错误结果。所有使用点均不对其做 value 比较：既不作 Map/Set key，也不 distinct/contains，两个包装类同样无 equals/hashCode。主张里"不能按 (table,version) 做 value-key"是**假设性**能力缺失而非被现有代码踩中的路径。假想示例：若将来有人用 `Set<ConnectorMvccSnapshot>` 对同一表同一 snapshotId 的两次 begin 结果去重、或写单测 `assertEquals(expectedSnapshot, actual)`，会退化为 identity 比较从而"永不相等"——但今天不存在这样的调用点，单测走逐字段 getter 断言。主张自评 🟡/S/潜伏，与代码现实吻合，无低估无夸大。

**修复方案（低优先级，纯一致性收敛）**：给 ConnectorMvccSnapshot 补一组 override，与兄弟类同风格：equals 覆盖 snapshotId/timestampMillis/schemaId/lastModifiedFreshness/description/properties 六字段（`==` 比三个 long/boolean，`Objects.equals` 比 description，`Map.equals` 比 properties），hashCode 用 `Objects.hash` 同集合，可顺带补 toString。该类已 import `java.util.Objects`，无需新增依赖，改动局限单文件、不碰任何调用方，符合 surgical 原则。鉴于零运行时影响，亦可选择不修、仅作为 backlog 一致性项跟踪。

---

## 本类别小结

**真问题（CONFIRMED，但均限定严重度）**：
- **#15**（stats 无 snapshot 参数）——唯一"真实但有边界"的问题：time-travel 下行数恒取 currentSnapshot 与 schema 已 pin 形成不对称，导致 CBO 基数估计偏斜；但**仅影响估计、不错结果**，且是 legacy `IcebergUtils.getIcebergRowCount` 的忠实端口、非迁移回归。
- **#25**（ConnectorMvccSnapshot 缺 equals/hashCode）——纯**形态/一致性 smell**，零运行时影响，backlog 项。

**伪问题 / 定性夸大（PARTIAL / REFUTED）**：
- **#5**（iceberg 行数丢 equality-delete gate）——**REFUTED**：主张的三项断言全部反了。行数取 `currentSnapshot().summary()`（snapshot 级）、扣除 position delete（delete-aware）、并对 equality delete 显式 gate 到 -1/UNKNOWN（computeRowCount:764-765、下推:2303-2304），两处均为 legacy `IcebergUtils.getCountFromSummary`（#64648）的忠实 delete-aware 移植；属方向性误读，无需修复。
- **#16**（微秒/毫秒命名错配）——事实成立但非 S 级；verify 纠正 stage-1"零算术消费者"的错误，真相是 CacheAnalyzer 确有 wall-clock 混算，但后果为**安全抑制** iceberg SqlCache（correctness-safe + master parity）。
- **#17**（branch ref 移动）——**REFUTED**：机制单点属实，但 `IcebergStatementScope` 每语句单次冻结加载令 plan/scan 复用同一冻结 Table，移动窗口不存在。
- **#18**（数字串当 epoch）——PARTIAL：合法 datetime 字面量必含非数字字符恒不误判，"歧义"夸大，且系跨连接器有意对齐设计。
- **#24**（一 long 两粒度）——PARTIAL：tagged-union，判别枚举始终在场，消费侧无条件分派，构造不出失败场景。

**共性根因**：
1. **快照 pin 的粒度不统一**（#15 核心，#16/#17/#18 相关）——catalog SPI 已把 schema 系列做成快照感知（`Optional<MvccSnapshot>`），但 stats 与 partition 枚举路径仍走无快照的 `resolveConnectorTableHandle` + currentSnapshot。这是一族既有局限（与 P6-1 stats-grain 同源），应统一治理而非单点改。
2. **legacy parity 优先于命名/单位纯净**（#16、#18）——多处刻意保持与 master 逐字对齐（微秒透传、digital→epoch benign superset），带来命名/单位表意负担，但均被消费侧的单调比较/判别位化解，不产生正确性问题。
3. **多条主张把"存在某机制"误当"存在某 bug"**（#17、#24、#18）——报告正确识别了移动引用、tagged-union payload、数字正则等机制，但错误假设它们会在运行时被踩中；实际由 statement scope（#17）、判别枚举（#24）、字面量格式约束（#18）精确堵死。

**与其他类别的关联**：#15 明确指向 **P6-1 stats-grain**（快照粒度族问题），#17/#18 依赖 **IcebergStatementScope / 每语句单次加载**（见项目 memory「iceberg 表解析/缓存作用域」与「session=user 目录无活跃跨查询缓存」），#16 触及 **query-cache（SqlCache）** 子系统的 wall-clock 语义。若后续统一治理"快照粒度"，应把 stats（#15）、partition 枚举（#15 verify 补充）、以及 #16 的新鲜度 token 语义一并纳入，而非逐条打补丁。
