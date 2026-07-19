# Progress Log —— 每语句表加载归属者 · 移植到其它连接器

> **Append-only**：只追加、不覆盖（覆盖式状态在 HANDOFF/tasklist）。

---

## 2026-07-19 — session 0：建任务空间

- 用户拍板：把 iceberg 的 PERF-07「每语句表加载归属者」整体性改动移植到其它连接器；**本 session 只建跟踪文档空间，实际处理留下一个 session**。
- 读准 iceberg 蓝本（PERF-07 summary）：可复用地基 = 中性 `ConnectorStatementScope`(fe-connector-api) + `ConnectorSession.getStatementScope()` + `ConnectorStatementScopeImpl`/`StatementContext`/`ConnectorSessionImpl` 构造期捕获/`ExecuteCommand` 重置(fe-core)，**已随 PERF-07 落地**（commit `97bdcd6bdbe`）。→ 关键结论：**移植是纯连接器侧工作，无需再改 fe-core**（不再触铁律 A）。连接器侧范式 = `IcebergStatementScope` helper + 四处 `resolveTable*` 共享 + (可选)拆胖句柄 + (可选)下沉跨臂暂存 + (可选)fail-loud。
- 初摸候选（grep）：只有 iceberg 用了该 SPI；`loadTable`/`getTable` 触点 paimon 4 / hive 3 / hudi 1 / maxcompute·es·jdbc·trino 0。→ 候选 = paimon(高)/hive-hms(中-高)/hudi(中)；读-only 四家大概率排除，待复核。
- 落地文件：`README.md`（用途 + 已就位地基 + 连接器侧 5 步模板 + 候选表 + 单连接器立项流程 + 铁律）、`tasklist.md`（PORT-01~04 全待 recon）、`HANDOFF.md`（下一步 = 确认范围 + 逐候选 recon）、`progress.md`（本文件）、`designs/`（空）。
- **未动任何产品代码**。**下一步**：见 HANDOFF —— 下个 session 起步先与用户确认范围/顺序，再对第一个连接器 recon。

## 2026-07-19 — session 1：逐连接器 recon + 架构统一性调研（结论：全部 🔬 + 定高度 L0）

- **逐连接器 recon（recon + 独立对抗复核，双签，多 agent workflow）**：结论 = **没有一个连接器现在值得移植**。
  - paimon → 🔬 **将来候选**：写未迁移（只读）、加载已被 transient 胖句柄 + SDK CachingCatalog 压到≈1；触发点=加行级 UPDATE/DELETE。
  - hive/hms → 🔬 不必做：仅 INSERT/OVERWRITE、跨查询 `CachingHmsClient.tableCache` 已兜（作用域超集）；网关只做 1 次探测加载后委派兄弟（兄弟自带作用域）。
  - hudi → 🔬 不必做：只读；唯一读侧成本=未缓存 metaClient 重建，形状不对/可变/鉴权错配。
  - maxcompute·es·jdbc·trino → 🔬 排除：均无行级 DML；trino 读侧重解析最贵=另立议题占位。
  - **核心洞察**：iceberg 独特在它是唯一迁移了行级写的连接器（行级写才有多臂重复加载 + 跨臂暂存）。
- **架构统一性专项调研（3 agent：placement / onboarding / Trino-altitude）**：
  - **统一接口已存在** = 中性 `getStatementScope()` + `ConnectorStatementScope`（新连接器天生继承）。
  - **Trino 模型不可直接移植**：Trino 靠"引擎自持每事务 metadata 生命周期"，Doris 连接器是共享单例、session 一条语句重建~26 次，唯一 span 宿主=`StatementContext`；现有 SPI 已是最可移植高度。
  - **高度分级 L0/L1/L2/L3**；**paimon-加写=L1 抽共享 helper 的正确触发点**（有 2 个真实用户）；共享 helper 落 `fe-connector-api`（不碰铁律 A）。
- **用户拍板**：先把结论记成**单独文档** → `designs/recon-findings-and-trino-refactor-groundwork.md`；**重构成 Trino 架构（L2/L3）留下个 session 专题讨论**（该文档 §7 已备预备材料）。
- **未动任何产品代码**（纯 plan-doc）。**下一步**：见 HANDOFF —— 下个 session = 讨论"重构成 Trino 架构"的可行性/分期/与铁律 A 的取舍。
