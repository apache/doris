# Progress Log — fe-connector-iceberg 热路径重操作修复

> **Append-only** 日志：只追加、不覆盖（覆盖式状态在 HANDOFF/tasklist）。
> 每完成 / 阻塞 / 复核推翻 / 重大发现加一段：日期 · 任务 · commit · 结论 · 踩坑。

---

## 2026-07-17 — session 0：建任务空间

- 依据审计报告 `../reviews/perf-audit-fe-connector-iceberg-2026-07-17.md`（23 确认发现，P0/P1/P2 三层七簇）建立独立任务空间 `plan-doc/perf-hotpath-iceberg/`。
- 落地文件：`README.md`（导航 + 单项立项流程 + 约束铁律）、`tasklist.md`（23 发现 → PERF-NN 任务，按 §5 优先级排序 + 总览表）、`HANDOFF.md`（起点 = PERF-01）、`progress.md`（本文件）、`designs/`（per-task 设计/小结空目录）。
- 任务粒度：**一簇一任务**（用户 2026-07-17 拍板）—— 按审计 §1 总览表行边界，簇级并成一项、未成簇的独立发现各一项，共 **11 个任务**。全部 `⏳ 待启动`，零实现。
- 未动任何产品代码；审计报告仍为待 review 草案，约定每项立项前复核行号/乘数。
- **下一步**：见 HANDOFF —— 启动 PERF-01（簇1，per-planning-pass Table memo）。

---

## 2026-07-17 — session 1：PERF-01 复核 + 设计定稿（未动产品代码）

- **复核（subagent）**：确认簇1 loadTable 调用点/无 CachingCatalog/`IcebergLatestSnapshotCache` 只存 `(snapshotId,schemaId)`。修正审计草案两点：①`loadTable` **快照无关** → memo 键只需 `TableIdentifier`（非 `(id,snapshot)`）；②legacy `IcebergMetadataCache`（#60937 前）**本就跨查询缓存 Table 对象**（`LoadingCache<…,IcebergTableCacheValue>`），故"行为不变 = 跨查询缓存"（非审计的 per-planning-pass）。
- **红队对抗**：**驳回 Part B（convertPredicate 收窄）** —— `doFinalize`（`FileQueryScanNode:252-253`）先 convertPredicate 后 createScanRangeLocations（首次 build），convertPredicate 时属性缓存皆 null，清空是 no-op，消不掉重算；真正的重复是 dual-build（getSplits 路径 + 属性路径），Part A 覆盖。**BLOCKER**：跨查询缓存 gate 必须含 `vended-credentials`（非仅 session=user），否则 24h TTL 内命中过期 token → BE 403。
- **架构澄清**：`IcebergConnectorMetadata`/`IcebergScanPlanProvider`/`IcebergCatalogOps` 均**每调用 new、每查询多个、用完即弃**；仅 `IcebergConnector` + SDK `Catalog` 长生命周期。→ 跨调用共享只能挂 `IcebergConnector` 或 `handle`。
- **用户定稿设计（多轮对齐 Trino）**：**胖 handle（`IcebergTableHandle.transient resolvedTable`，查询内单实例、随查询计划自动回收、连接器侧零累积，= Trino per-transaction 胖 handle 的 Doris 对应物）+ 跨查询 `IcebergTableCache`（挂 `IcebergConnector`，仿 `IcebergLatestSnapshotCache`，读 helper 消费非 catalog-seam 装饰器→DDL 天然隔离，gate=`isUserSessionEnabled()||restVendedCredentialsEnabled()`）**。去重计数：规划期 1 次、全查询 1 次（跨查询开）/≤2（关）。全设计 + 度量守门见 `designs/FIX-PERF-01-table-memo-design.md`。
- **未动任何产品代码**。**下一步**：实现（TDD，先 `RecordingIcebergCatalogOps` 度量守门）。

---

## 2026-07-18 — session 2：PERF-01 实现 + 全绿（commit `484f0e0c125`）

- **实现按定稿落地（TDD，一个 `[perf]` commit）**：① `IcebergTableHandle` 加 `transient Table resolvedTable`（不序列化、不入 equals/hashCode，三个 `with*` 携带前行）；② 新增 `IcebergTableCache`（仿 `IcebergLatestSnapshotCache`，值 raw Table，manual-miss-load 原样透传异常）；③ `IcebergConnector` 按 gate 建 tableCache 或 null，传入 metadata/provider，三个 `invalidate*` 加跨查询失效；④ metadata 统一读 helper `resolveTableForRead`（胖 handle→cache→裸 loadTable，不开 auth scope/不包异常），`loadTable(handle)` 包 auth，`getMvccPartitionView`/`listPartitions`/`listPartitionNames` 经它并保住 `NoSuchTableException`→空降级；⑤ provider `resolveTable` 胖 handle 优先 + per-call `wrapTableForScan`。
- **关键校验（动码前）**：确认 `PluginDrivenScanNode.currentHandle` 每查询新建、`resolveConnectorTableHandle`→每次 fresh `getTableHandle`、不挂长生命周期 `ExternalTable` → 胖 handle memo 严格查询内、无跨查询泄漏（design 假设成立，非一厢情愿）。`MetaCacheEntry.loadAndTrack` 确认 `throw e`（RuntimeException 原样），故 `NoSuchTableException` 穿缓存不被包 → 降级不破。
- **度量守门（新）**：`IcebergScanPlanProviderTest.planningPassLoadsSameTableOnceViaFatHandle` —— `getColumnHandles`+`planScan` 穿同一 handle → 远端 loadTable 恰 1 次（修前 2）。另加 handle 携带/transient、`IcebergTableCacheTest`、凭证 gate + REFRESH 失效诸测。
- **回归修（1 个）**：`IcebergScanPlanProviderTest` 的 COUNT(*) 用例共享 `static final T1` handle，胖 handle 令其跨用例串味（空表用例污染后续 → 5 挂）→ 改 `planCount` 每次 fresh handle（镜像生产每查询新 handle）。**非生产 bug**，纯测试 artifact。
- **结果**：全 iceberg 模块 **932 pass / 0 fail / 1 skip**（`install -am`），checkstyle 绿。summary 见 `designs/FIX-PERF-01-table-memo-summary.md`。
- **构建坑记录**：本 worktree `${revision}` CI 版本 + 未 flatten 的已装 pom → `-pl iceberg` 单模块永远解析不到 `fe-connector:pom:${revision}`（`-Drevision=` 不透传到传递依赖 pom）；且 `-am test` 只到 test 相不产 hms-hive-shade 的 shade jar（缺 `HiveConf`）。**可靠跑法 = `mvn install -pl fe-connector/fe-connector-iceberg -am -Dtest=<iceberg 类列表> -DfailIfNoTests=false -Dmaven.build.cache.enabled=false`**（reactor 解析 revision + 到 install 相产 shade jar + 上游 0 匹配测试快速跳过）。
- **下一步**：见 HANDOFF —— PERF-02（分区视图跨查询缓存 + MTMV refresh pin，复用本任务的 `(table, snapshotId)` pin 与胖 handle 模式）。

---

## 2026-07-18 — session 2（续）：PERF-02 实现 + 全绿（commit `518d0599cbf`）

- **复核确认审计簇3**：分区表分析期 `materializeLatest → getMvccPartitionView/listPartitions → IcebergPartitionUtils.loadRawPartitions`（PARTITIONS 元数据表扫描，读该快照全部 manifest）跨查询零缓存；三消费方（MVCC 视图 / SHOW PARTITIONS / selectedPartitionNum）同源。**关键复核发现**：PERF-01 已消掉本簇 `loadTable` 那半，剩余 = PARTITIONS 扫描；且三方法内部才把 snapshotId 解析成具体值 → 缓存必须在 `IcebergPartitionUtils` 内按解析后 snapshotId 建键（非 metadata 层）。**vs master**：master 把 loadPartitionInfo 缓在 TTL'd tableEntry，故这是重构丢失的回退（非 eligible 分区表则是 selectedPartitionNum 特性引入的新增未缓存成本）。
- **实现（连接器侧，一个 `[perf]` commit）**：新增 `IcebergPartitionCache`（键 `(TableIdentifier, snapshotId)`、值 raw 分区列表、**无凭证 gate**——纯元数据）；`IcebergPartitionUtils` 三方法加 `(id, cache)` 参 + 旧签名重载、`loadRawPartitions` 拆 cache-aware + uncached、`IcebergRawPartition` 改包内可见；`IcebergConnector` 无条件构造 + 传入 + 三失效钩子 + test 访问器；`IcebergConnectorMetadata` ctor 加参 + 三处传 `(TableIdentifier, cache)`。
- **范围决定（用户认可）**：**MTMV refresh 级 pin 不做**——`ttl>0` 时 `IcebergLatestSnapshotCache` 已稳定一次 refresh 内 snapshotId，本缓存据此把 4~6 次重复坍缩成 1，无需改 fe-core；仅 `ttl=0` 无缓存目录残留既有枚举偏移边角，超范围。
- **度量守门（新）**：`IcebergPartitionUtilsTest.partitionScanIsCachedAcrossRepeatsAndConsumersAtSameSnapshot` —— 真分区表重复 `buildMvccPartitionView` + `listPartitions` 同快照 → `loadCountForTest()==1`。另 `IcebergPartitionCacheTest`（含 `ValidationException` 透传+不缓存）、连接器 gate/失效诸测。
- **结果**：全 iceberg 模块 **943 pass / 0 fail / 1 skip**，checkstyle 绿。summary 见 `designs/FIX-PERF-02-partition-view-cache-summary.md`。0 回归（无需改任何现有测试）。
- **下一步**：见 HANDOFF —— PERF-03（#64134 planFiles 兜底复活：`file_format_type` 无 write-format 时走整表 planFiles 反推格式 → memoize / 从枚举反推）。

---

## 2026-07-18 — session 3：PERF-03 实现 + 全绿（commit `0b96f2e6c78`）

- **复核确认审计 C2/C11（同一重操作两视角）**：`getScanNodeProperties:1350 → IcebergWriterHelper.getFileFormat → resolveFileFormatName → inferFileFormatFromDataFiles → table.newScan().planFiles()`（无过滤整表 manifest 扫描）。门槛=表属性无 `write-format` 且无 `write.format.default`。fe-core 侧 `cachedPropertiesResult` 已 node-memoize ⇒ 查询内 1 次，但跨查询零缓存 ⇒ 每查询/EXPLAIN 一遍整表扫描（大表数百 ms~秒 ×QPS）。
- **路线否决（复核推翻 HANDOFF 原倾向 b）**：HANDOFF 倾向"从 planScan 枚举反推格式"（消除而非缓存），复核**否决**——`FileQueryScanNode.createScanRangeLocations` **先** `getFileFormatType`（line 325 → 触发本重操作）**后** `getSplits/planScan`（line 422）：格式须先于数据扫描给出，planScan 尚未跑；且推断是无过滤、planScan 带谓词，谓词剪空所有文件时反推拿不到格式破 legacy parity；改时序须动 fe-core（违反"fe-core 源只出不进"铁律）。**正是 HANDOFF 预判的"两路径独立触发 → 退回 (a) memoize"**。
- **设计红队（独立 agent 对抗）**：7 项攻击（时序/键 soundness/无 gate/失败粘住/混格式&空表/失效完整/写路径不缓存）**全部未击穿**，判"根本可靠"。采纳两点：R1 loader 只能抛 unchecked（`close()` 的 `IOException` 包 `UncheckedIOException`）；R2 混格式表"格式变确定"如实记为可复现性变化非卖点。R3 加 avro 首文件重抛断言。
- **实现（连接器侧，一个 `[perf]` commit，零 fe-core 改动）**：新增 `IcebergFormatCache`（键 `(TableIdentifier, currentSnapshotId)`、值格式名 `String`、**无凭证 gate**）；`IcebergWriterHelper` 加 cache-aware `getFileFormat` 重载 + `resolveFileFormatName` 重载，拆 `inferFirstDataFileFormat`(透传异常)/`inferFileFormatFromDataFiles`(吞→委派)，orc/parquet 映射与其 unsupported throw 保持在 `getOrLoad` 之外；`IcebergScanPlanProvider` 加 nullable `formatCache` 字段 + 7 参 ctor（6 参委派 null）+ 改 `getScanNodeProperties` 调用点；`IcebergConnector` 无条件构造 + 传 provider + 三失效钩子 + `formatCacheForTest`。**写路径 7 个 `getFileFormat(Table)` 调用点不动**（留 PERF-07/C20）。
- **度量守门（新）**：`IcebergWriterHelperTest`（真 InMemoryCatalog 表 + 真数据文件）无格式属性表重复 `getFileFormat(table,id,cache)` → `loadCountForTest()==1`+`size()==1`+与无缓存 parity；属性表 `size()==0`；null cache 存活；avro 首文件推断一次但每次重抛。`IcebergFormatCacheTest`（8：TTL/关/失效/失败不缓存可重试）。`IcebergConnectorCacheTest`（+2：三目录无 gate + REFRESH 失效）。
- **结果**：全 iceberg 模块 **957 pass / 0 fail / 1 skip**（`install -am`），checkstyle 绿，0 回归（无需改任何现有测试）。summary 见 `designs/FIX-PERF-03-format-inference-cache-summary.md`。
- **踩坑/印证**：InMemoryCatalog `newAppend().commit()` 后同一 table 对象 `currentSnapshot()` 会刷新（度量守门测试 ORC+loadCount==1 通过即证），无需像 `dayPartitionedTable` 那样 reload；但 reload 仍是更稳的既有范式。
- **下一步**：见 HANDOFF —— PERF-04（IcebergManifestCache 两条旁路接回：C17 streaming 大表踢出 cache + C18 COUNT(*) 下推绕过 cache）。

---

## 2026-07-18 — session 3（续）：PERF-04 实现 + 全绿（commit `2e5f393779c`）

- **复核 + 冲突求证（关键）**：C17/C18 = manifest cache（opt-in 默认关）被大表流式 + COUNT(*) 下推两条路径绕过。**审计说"legacy batch 走 cache 是回退"，我核 legacy `6fef25709d3^` 确认但补全另一半**：legacy batch 走 cache 时**一次性物化整表任务列表**（无 OOM 保护，OOM 保护只在缓存关时的惰性分支）；迁移用"一律流式（OOM 安全、无缓存）"换掉了缓存收益。**审计建议的"缓存开→退回物化"会重引 legacy 就有的 OOM 风险**——否决"一行修复"。
- **决策上交用户（AskUserQuestion，三选项 A 恢复 legacy/B 维持现状/C 惰性+缓存兼得）**：用户选 **C**。C 可行性由代码证实：现 `planFileScanTaskWithManifestCache` 只在 Phase 2 攒 `List`，delete 索引(Phase 1)本就 eager（= SDK planFiles 也 eager 读 delete）→ 把 Phase 2 改惰性即得"缓存 + 不 OOM"。
- **设计红队（独立 agent 对抗，7 攻击）**：核心思路(惰性迭代器三处复用)判 sound；抓 **1 HIGH**（回退 `catch` 须 `Exception` 非 `IOException`——缓存把失败重抛成 RuntimeException）+ 3 MED（跨线程 stats 竞争→流式用无 stats 重载；COUNT 对比测试勿断言 path 相等因 `ParallelIterable` 乱序；保 `snapshot==null` 守卫 + 空表 COUNT 用例）+ 2 LOW（大表流式现在填充缓存=目的；COUNT+cache 少一份 profile=与现有 cache 路径一致）。全并入。
- **实现（连接器侧一个 `[perf]` commit）**：抽 `cacheBackedFileScanTasks()`（惰性 CloseableIterable）+ `manifestCacheGet`（stats/无 stats 重载分派）+ 内部类 `ManifestCacheFileScanTaskIterator`（扁平映射迭代器，schemaJson/specJson 提为循环不变量）；`planFileScanTaskWithManifestCache` 改物化它；`streamSplits`→`streamingFileScanTasks`（cache 开惰性+回退）；`planCountPushdown`→`countPushdownFileScanTasks`（cache 开取首文件+回退）+ 从 `planScanInternal` 透传 `filter`/`session`。
- **测试（`IcebergScanPlanProviderTest` +5）**：流式 cache parity vs SDK + 命中；分区剪枝 parity；跨多 data manifest 扁平映射；COUNT count-parity(60) + 惰性早停(`cache.size()<总 manifest`)；空表 null 快照 COUNT 守卫。回退路径未单测（`IcebergManifestCache` final 无 seam，与既有同步回退同样未测，逐字镜像）。
- **踩坑（CI 前本地捕获）**：① `java.util.Iterator` 漏 import（编译挂）；② 重构后同步 `planFileScanTaskWithManifestCache` 顶部无条件 `session.getQueryId()`，而既有空表用例传 **null session**（旧码 snapshot==null 早返回、从不碰 session）→ NPE；改 `statsQueryId = session != null ? getQueryId() : null`。
- **结果**：全 iceberg 模块 **962 pass / 0 fail / 0 error / 1 skip**，checkstyle 0 违规，0 回归（现有测试全绿）。summary 见 `designs/FIX-PERF-04-*-summary.md`。
- **下一步**：见 HANDOFF —— PERF-05（C9 information_schema.tables 每表 loadTable 只为取 comment）。

---

## 2026-07-18 — session 3（续）：PERF-05 实现 + 全绿（commit `aea3ebdd40e`）

- **复核缩小（关键）**：审计 C9 写于 PERF-01 之前。核当前代码：`getTableComment → loadTable → resolveTableForRead → IcebergTableCache`（PERF-01）→ **普通目录重复 information_schema 查询已命中缓存**（"BI 反复查反复慢"最痛点普通目录已解）。残余 = **凭证 gated 目录**（tableCache 因带凭证被 gate 为 null）次次重载。→ 上交用户决策（AskUserQuestion，A 补注释缓存 / B 判定基本修复）。
- **用户选 A（补注释缓存，仅特殊目录）**。设计：无 gate `IcebergCommentCache`（键 TableIdentifier、值 comment String），仅在 tableCache 关掉的凭证 gated 目录建。
- **设计红队（独立 agent，7 攻击）抓 1 HIGH（关键安全）**：原设计 gate=`tableCache==null` **过宽**——把 **session=user** 卷入。session=user 的授权在 per-user loadTable 调用本身；共享 comment 缓存会把用户 A 加载的 comment 发给"有合法 token+过 Doris 权限、但 REST 侧对该表无权"的用户 B（B 的 token 从未被校验）= **元数据泄漏，且相对现状回退**。→ 收窄 gate 为 **`restVendedCredentialsEnabled && !isUserSessionEnabled()`**（vended 单一静态身份共享安全；session=user 保持 live）。另 2 LOW（ttl≤0→disabled 映射 load-bearing；vended 新增陈旧窗口）并入。其余 6 攻击 SURVIVES。
- **实现（连接器侧一个 `[perf]` commit）**：新增 `IcebergCommentCache`（镜像 `IcebergTableCache` + `loadCountForTest`）；`IcebergConnector` 字段 + vended-only 构造 + getMetadata 传参 + 三 invalidate（null-guard）+ `commentCacheForTest`；`IcebergConnectorMetadata` 7 参 ctor + `getTableComment` 路由 + 抽 `loadTableComment`。
- **测试**：`IcebergCommentCacheTest`（8：TTL/关/失效/失败不缓存）；`IcebergConnectorMetadataTest` 度量守门（`loadCountForTest()==1` + `ops.log` 远端 load 计数）；`IcebergConnectorCacheTest`（+2：gate=plain null / vended非session 非null / session=user null / vended+session null；vended REFRESH 失效）。
- **踩坑（CI 前本地捕获）**：2 处主源行超 120（javadoc）→ checkstyle 挂 → 缩短文案（纯 docs，无行为变）。
- **结果**：全 iceberg 模块 **973 pass / 0 fail / 0 error / 1 skip**，checkstyle 0 违规，0 回归。summary 见 `designs/FIX-PERF-05-*-summary.md`。
- **新判据（可复用）**：**「缓存 gate 是授权决策，不止凭证泄漏决策」**——session=user 的授权发生在 load 调用里，缓存命中会绕过它，故即便缓存值不含凭证也不能对 session=user 共享；与「值含 FileIO/凭证才 gate」正交，二者叠加判定。
- **下一步**：见 HANDOFF —— PERF-06（C3 REST vended-credentials 每 data/delete file 重建 StorageProperties+Configuration）。

---

## 2026-07-18 — session 4：PERF-06 实现 + 全绿（commit `6294edf2833`）

- **复核（HEAD grep，行号已漂）**：确认贵活 `buildVendedStorageMap(token)` 纯 token 派生、与 per-file URI 无关（URI 只用于其后廉价 `LocationPath.of`）；token 每 scan 只提取一次且同一 Map 实例穿三路径（同步 data `planScanInternal`、流式 `streamSplits`、position_deletes）所有 `normalizeUri`（数据 1191/delete 1243/位置删除 935）。**关键生命周期事实**：`DefaultConnectorContext` 是 **per-catalog、跨查询（含并发）共享**（`PluginDrivenExternalCatalog:197`，已缓存 catalogFileSystem）——这决定了不能在其上做跨查询 memo。
- **路线上交用户（AskUserQuestion）**：(A) 连接器侧「准备一次、逐文件套用」新增 SPI normalizer seam vs (B) fe-core 框架内按 token 单条目 memo。讲清 B 在 per-catalog 共享对象上并发冲刷退化 + 凭证跨查询保留贴红线；Trino 架构参考=每 scan 建一次 FileSystem 复用（对齐 A）。**用户选 A**。
- **设计红队（独立 Explore agent，6 点核查）**：判 sound、**无 hard blocker**。确认①seam 完整（三路径全覆盖、`planSystemTableScan` 非 position_deletes 分支不 normalize）②parity 逐字（`buildVendedStorageMap` 纯函数 fail-soft、`storagePropertiesSupplier` scan 内稳定甚至更一致、空URI短路保序）③token 不变量④`TcclPinning` override 是拿到提升的必要条件且无需 TCCL pin⑤写路径 `getBackendFileType` O(1)/写、leg"在范围外正确⑥单线程套用无并发隐患。**采纳其加固**：`effective` 改**惰性 memo（首个非空 URI 才派生）**而非构造时 eager，消除"零文件/全空 URI 且 storage-map init 抛"的极窄异常时机分歧。另提示测试迁移 + `context==null` 用 `identity()` 兜底（均已计划）。
- **实现（跨 3 模块一个 `[perf]` commit）**：SPI `ConnectorContext.newStorageUriNormalizer` default（逐文件折回）；`DefaultConnectorContext` override（惰性 memo 匿名 `UnaryOperator`）；iceberg `TcclPinning` 透传；`IcebergScanPlanProvider` 新增 `newUriNormalizer` helper + 三处 scan-start 构造 + 六 seam 参数 token→normalizer + 删旧 `normalizeUri` helper。
- **测试**：连接器守门 `planScanDerivesUriNormalizerOncePerScanNotPerFile`（3 文件 scan → `newNormalizerCount==1` & `normalizeCount==3`）；`RecordingConnectorContext` +计数器 + override（仍逐 apply 折回 recording，现存断言零改）；迁移 8 处 convertDelete 签名（6 plain→`identity()`，2 归一化→`newUriNormalizer(token)` 保留 recording 断言）；fe-core `DefaultConnectorContextNormalizeUriTest` +4 parity（vended/static/空URI/坏路径 fail-loud + 一 normalizer 多 URI）。
- **构建坑（实证）**：iceberg 连接器**不依赖 fe-core**（SPI 解耦），故 `-pl iceberg -am` 反应堆里**无 fe-core**——fe-core override + 其测试须**单独** `mvn test -pl fe-core -am` 验（本任务首个动 fe-core 的 perf 项，后续动 fe-core 者都要两段验）。
- **结果**：iceberg 全模块 **974 pass / 0 fail / 1 skip**（+1 守门）；fe-core `DefaultConnectorContextNormalizeUriTest` **9→13 绿**；两处 BUILD SUCCESS + checkstyle 0 违规，0 回归。summary 见 `designs/FIX-PERF-06-*-summary.md`。
- **新判据（可复用）**：**「作用域即安全边界」**——同一份"含凭证派生"的缓存，跨查询（catalog 级对象）做=撞凭证/授权 gate；**scan 级**做=天然安全（token 恒定、无跨用户、结束即回收）。PERF-05 的 gate 是"跨查询缓存必须判授权"，PERF-06 是"把作用域收回 scan 就不必判"——两面。
- **下一步**：见 HANDOFF —— PERF-07（C20 写路径一条 DML 3~5 次 load 同表）。

---

## 2026-07-18 — session 5：PERF-07 实现（完整统一版）+ 全绿（commit `97bdcd6bdbe` fe-core + `ea7fd1f6e7a` iceberg）

- **开场校验（6 路侦察 workflow）**：把权威设计每处 `文件:行号` 锚点跟 HEAD 逐一核对——**几乎零漂移**（仅 `getTableSchema` @Override 签名上移 10 行、方法体 419-440 不变）；**定论唯一开放问题**——预编译 EXECUTE 复用同一 `StatementContext`，重置钩子应挂 `ExecuteCommand:90-91`（那两行既有 per-execution 重置旁），**不**挂通用 setter（`setConnectContext` 11 caller 无一在语句中途、但过宽）；作用域 key 含 queryId 为第二道防线。另修正设计两处笔误（`ConnectorSession` 实现实为 21 非 14，仅生产 `ConnectorSessionImpl` 改；删除面比设计列的多——扫描 4 + 写 2 个 ctor、两测试类调用点、两处 `{@link}` 悬空）。
- **用户拍板**：写路径口径矛盾（任务清单旧写"beginWrite 保持新载" vs 权威设计"取共享表"），上交 AskUserQuestion → **选完整统一版**（读写共享一次加载 + 拆胖句柄 + stash 下沉）。
- **实现（两个 `[perf]` commit）**：
  - **fe-core 基础**（`97bdcd6bdbe`）：`fe-connector-api` 加 `ConnectorStatementScope`(接口+NONE)+ `ConnectorSession.getStatementScope()` 默认；`fe-core` 加 `ConnectorStatementScopeImpl`(CHM) + `StatementContext` 懒建字段/同步访问器/`resetConnectorStatementScope`(不在 close/release 清、随 GC，镜像 snapshots) + `ConnectorSessionImpl/Builder` **构造期捕获**(from(ctx) 优先、回退 ConnectContext.get()、两级 null→NONE) + `ExecuteCommand` 一行重置。
  - **iceberg**（`ea7fd1f6e7a`）：`IcebergStatementScope` helper(`sharedTable` 键 catalogId:db:tbl:queryId + `rewritableDeleteSupply` 键 catalogId:queryId、null-session 兜底)；读 `resolveTableForRead`(+session)/扫描 `resolveTable`/写 `resolveTable`/`beginWrite` 四处走它；**拆 L0**(删 `resolvedTable` 字段/访问器/三 with* 携带 + 两 seam 读写)；`beginWrite` 取共享表(保留 openTransaction refresh)；扫描 accumulate + 写 drain 经作用域同键 map，**整删** `IcebergRewritableDeleteStash`(141 行)+ 测试(204 行)+ 六 ctor 参数；写侧 v3 DELETE/UPDATE/MERGE + NONE **fail-loud**。
- **关键取舍**：`beginWrite` 保留 refresh（`Transactions.newTransaction` 本就 refresh；保留更安全=新鲜 OCC 基底 + 消解「共享表比 pin 旧」跨缓存错位；读扫描显式钉快照读不可变数据、不受共享对象被 refresh 到 latest 影响）；写侧性能收益≈0，真交付=架构连贯 + 删单例/类。
- **测试**：iceberg 重写 3 个扫描 stash 测试 + 5 个写 stash 测试为作用域版 + 新增 fail-loud/度量守门/`IcebergStatementScopeTest`(键隔离)/`TestStatementScope` 助手；删 2 个 L0 handle 测试 + `IcebergRewritableDeleteStashTest`。fe-core 新增 `ConnectorStatementScopeTest`(NONE 不 memo/impl memo+隔离/StatementContext 懒建+reset)。
- **踩坑**：`planScan(null,...)` 空 session 测试 → `sharedTable`/`rewritableDeleteSupply` 加 null 兜底(等价 NONE)；删 L0 测试后 `IcebergTableHandleTest` 5 个 import 悬空(checkstyle 挂 test 源)→ 清理。
- **对抗复审**：6 视角多 agent(读路径/凭证隔离/快照OCC/删除供给/SPI捕获/L0删除)+ 逐条对抗核实 → **0 确认发现**（3 lens 首轮 stream timeout，单独重跑亦全 clean）。凭证隔离核实：读/扫描/写均 `newCatalogBackedOps(session)` 同用户 + 作用域每语句(不同 ConnectContext→不同 StatementContext→不同 map)；删除供给核实：扫描/写同 StatementContext scope 同 key、parity 逐字、fail-loud 恰覆盖消费 rewritableDeletes 的三 op。
- **结果**：iceberg **968 pass / 0 fail / 1 skip**；fe-core 两段验 `ConnectorStatementScopeTest` 3 + `ConnectorSessionImplTest` 17 绿；两处 BUILD SUCCESS + checkstyle 0 违规，0 回归。summary 见 `designs/FIX-PERF-07-unified-per-statement-table-owner-summary.md`。
- **新判据（可复用）**：**「唯一贯穿语句的对象是 `StatementContext`」**——连接器 session 一语句被重建 ~26 次、缓存挂它即死；跨读写共享须向上够到 StatementContext(经中性 SPI `getStatementScope()`)，**构造期捕获**(非实时读)才能让 off-thread 扫描泵够到(它们复用请求线程建的同一 session、无 ConnectContext thread-local)。
- **下一步**：见 HANDOFF —— PERF-08（C19/C21 维护路径逐单位重扫/无去重；改动小可先行）。

---

## 2026-07-18 — session 3：PERF-08 实现 + 全绿（commit `89cc39c8d88` C19 + `be0035eff62` C21）

- **开场复核（动码前，grep HEAD 校行号）**：PERF-07 后行号有漂——C19 driver STEP3 循环实在 `ConnectorRewriteDriver.java:143-145`（fe-core），iceberg 侧 `IcebergConnectorTransaction.registerRewriteSourceFiles:366`（每次 `useSnapshot(startingSnapshotId).planFiles()` 全表扫）；C21 `buildDeleteFileContentMap:271-293`。关键事实核实：**逐组注册的所有调用 pin 同一 `startingSnapshotId`**（beginWrite 捕获一次、共享事务恒定）、bin-pack 组**互斥**、四列结果计数来自组元数据非 `filesToDelete`；C21 map 只喂 `deleteWith` 分类回调。
- **设计红队（2 独立对抗 agent，clean-room）**：两条均判 `parity_preserved=true`、0 破坏性发现。采纳提醒：①C19 union 的 List 去重正确性**依赖组互斥**（iceberg 可证，SPI 未强制）——落地在 STEP3 注释显式记该不变量 + 补空组 no-op 单测；stale-plan 报错文案计数从「首个缺失组」变「并集」属 cosmetic 同结局。②C21 `visited` set **必须方法作用域**（放进 snapshot 内层则每轮清零、dedup 空转仍 parity 但零收益）——落地加注释锁定。
- **实现（两个独立 `[perf]` commit）**：
  - **C19 fe-core（`89cc39c8d88`）**：STEP3 逐组循环 → 抽包私有静态纯函数 `unionSourceFilePaths(groups)` 并集**一次**调 `registerRewriteSourceFiles`。iceberg 连接器零改动。铁律核对：仅把已有 driver 对已有中性 SPI（`Set<String>`）的调用从 G 次收敛为 1 次，无 source-specific 分支、无逻辑上移 fe-core → 合「fe-core 只减不增 + connector-agnostic」。
  - **C21 iceberg（`be0035eff62`）**：`buildDeleteFileContentMap` 加方法作用域 `visitedDeleteManifests`，`if (!add(manifest.path())) continue`；降包私有 + `@VisibleForTesting int lastDeleteManifestReadCount` 度量守门。
- **测试**：C19 `ConnectorRewriteDriverTest` +2（并集去重/空组空 plan）→ 5/5；分布式 STEP3 需真 BE（类注释既定），单测守到并集函数层，端到端留 flip rehearsal e2e。C21 `IcebergExpireSnapshotsActionTest` +1 + `ActionTestTables` 加 position/equality delete 播种 helper；构造「同一 delete manifest 被多 snapshot 前推共享」表，自检 sanity（perSnapshotReads>distinct，防空测）+ 断言读次数==distinct + position/equality 分类 parity → 9/9。
- **踩坑**：`ActionTestTables` 起初误加未命名的 `DeleteFile` import（builder 内联进 `addDeletes` 不具名）→ checkstyle UnusedImports；删 import + 删一个凑数的 anchor 方法（违简单律）。
- **结果**：fe-core BUILD SUCCESS + `ConnectorRewriteDriverTest` 5/5 + 0 checkstyle；iceberg BUILD SUCCESS + `IcebergExpireSnapshotsActionTest` 9/9 + 0 checkstyle。度量：C19 整表 `planFiles()` G+1→1；C21 delete manifest 远程读 O(S×M)→O(distinct M)。
- **下一步**：见 HANDOFF —— P2 剩余（PERF-09 fe-core streaming pump 微批 / PERF-10 WHERE conjunct memo / PERF-11 簇5 per-split 不变量+payload）。

---

## 2026-07-18 — session 3（续）：PERF-10 复核判低价值暂缓 + PERF-11 部分完成（commit `10b7d29423f`）

- **PERF-10（C8，WHERE conjunct 转换）复核后暂缓（🔬）**：按 findings.json #7 权威口径 + HEAD 核对——两个独立 verifier 均判 **Low**（纯 planning CPU 微秒~低毫秒、无远程 IO），且当初「每次转换旁跟一次昂贵 loadTable」已被 PERF-01/07 修掉，单独价值极低；干净不了（k≈7 中 4 次在 fe-core 通用节点 `buildRemainingFilter`、**带副作用** `filteredToOriginalIndex`，须双不变 + 缓存副作用）；连接器侧「安全小切片」（去 `getScanNodeProperties` 的 EXPLAIN-only `PUSHDOWN_PREDICATES_PROP` 白干）也做不干净——`ConnectorSession` 无 explain 信号（加=碰框架 SPI，用户不碰）、复用 `buildScan` 转换又因 `getScanNodeProperties` 早于 `buildScan` 顺序反了。**用户 2026-07-18 拍板跳过、改做高价值 PERF-11**。tasklist 标 🔬 记录复活条件（愿加中性 explain 信号则连接器侧可复活）。
- **PERF-11 部分完成（`10b7d29423f`，连接器侧一个 commit）= C12 + C15(a) + C13(plan)**：
  - **调研**：读 `buildRange`/`buildRangeForTask`(共用 choke point，eager+流式)/`IcebergScanRange.populateRangeParams`(全 immutable、只读)/`IcebergStreamingSplitSource`(单线程、每 source=每 scan)；确认 per-file 不变量 = partition JSON/identity/ordered 值/delete 载体/format/row-lineage/normalized path，per-slice = start/length/`selfSplitWeight`(=length 项 per-slice)。**C13 原证据 stale**（`IcebergRewritableDeleteStash` PERF-07 已删；现供给走 `IcebergStatementScope.rewritableDeleteSupply` 每语句 map，per-slice 幂等 put）。
  - **设计红队（3 视角 byte-parity 专攻）**：均 `parity_preserved=true`、0 破坏。关键确认：不同逻辑文件永不共享 `DataFile` 实例（SDK `ContentFileUtil.copy`+缓存 `dataFile.copy()` 逐条拷贝，后者带防复用注释）→ 身份键不 stale；`splitFiles`(`transformAndConcat`) 连续吐同文件 slice → 1 条目缓存 100% 命中、即便非连续也只降 perf；immutable+只读+换引用 → 共享/流式 look-ahead 安全。**3 落地纪律**：selfSplitWeight/start/length 必 per-slice；put 在文件首 slice(miss) 用新文件路径(覆盖末文件、防 v3 复活)；scratch 每 scan 新建非 provider/static。
  - **落地**：`PerFileScratch`(10 字段)+`computePerFileInvariants`(原逐 slice 逐字不变、末尾写 scratch)；`buildRange` 复用/compute + per-slice 组装、range 引用 scratch 不可变 `partitionValues`/`deleteCarriers` 实例；`buildRangeForTask` 动 buildRange 前捕获 `firstSliceOfFile`、put 仅首 slice；eager 局部 scratch、流式实例字段 scratch；count-pushdown 传 null；`@VisibleForTesting perFileInvariantComputeCount` 守门 + 2 测试(切分文件算 1 次/两文件算 2 次无跨文件串味)。
  - **明确不做**（tasklist 记）：**C15(b) 线路字节**（`populateRangeParams` 逐 range `toThrift()`+partition JSON 塞每个 `TFileRangeDesc`，减字节须 thrift 字典+BE reader 改=协议演进非回归修复）、**C13(wire) 二次转换**（同处线路侧、随 C15b）、**C14 fe-core 通用节点 hoist**（框架层、须双不变）。
  - **结果**：`IcebergScanPlanProviderTest` 105/105（+2）、`IcebergConnectorTransactionTest` 66/66、**全模块 1064 pass/1 skip/0 fail**、BUILD SUCCESS + 0 checkstyle。
- **下一步**：见 HANDOFF —— PERF-09（fe-core streaming pump 微批）或 PERF-11 remainder（C15b/C13-wire/C14）。
