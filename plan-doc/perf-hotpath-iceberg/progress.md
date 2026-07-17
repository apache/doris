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
