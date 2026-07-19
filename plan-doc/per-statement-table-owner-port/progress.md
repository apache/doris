# Progress Log —— 每语句表加载归属者 · 移植到其它连接器

> **Append-only**：只追加、不覆盖（覆盖式状态在 HANDOFF/tasklist）。

---

## 2026-07-19 — session 0：建任务空间

- 用户拍板：把 iceberg 的 PERF-07「每语句表加载归属者」整体性改动移植到其它连接器；**本 session 只建跟踪文档空间，实际处理留下一个 session**。
- 读准 iceberg 蓝本（PERF-07 summary）：可复用地基 = 中性 `ConnectorStatementScope`(fe-connector-api) + `ConnectorSession.getStatementScope()` + `ConnectorStatementScopeImpl`/`StatementContext`/`ConnectorSessionImpl` 构造期捕获/`ExecuteCommand` 重置(fe-core)，**已随 PERF-07 落地**（commit `97bdcd6bdbe`）。→ 关键结论：**移植是纯连接器侧工作，无需再改 fe-core**（不再触铁律 A）。连接器侧范式 = `IcebergStatementScope` helper + 四处 `resolveTable*` 共享 + (可选)拆胖句柄 + (可选)下沉跨臂暂存 + (可选)fail-loud。
- 初摸候选（grep）：只有 iceberg 用了该 SPI；`loadTable`/`getTable` 触点 paimon 4 / hive 3 / hudi 1 / maxcompute·es·jdbc·trino 0。→ 候选 = paimon(高)/hive-hms(中-高)/hudi(中)；读-only 四家大概率排除，待复核。
- 落地文件：`README.md`（用途 + 已就位地基 + 连接器侧 5 步模板 + 候选表 + 单连接器立项流程 + 铁律）、`tasklist.md`（PORT-01~04 全待 recon）、`HANDOFF.md`（下一步 = 确认范围 + 逐候选 recon）、`progress.md`（本文件）、`designs/`（空）。
- **未动任何产品代码**。**下一步**：见 HANDOFF —— 下个 session 起步先与用户确认范围/顺序，再对第一个连接器 recon。
