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
