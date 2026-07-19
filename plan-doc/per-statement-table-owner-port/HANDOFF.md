# 🤝 Session Handoff —— 每语句表加载归属者 · 移植到其它连接器

> **滚动文档**：每 session 结束覆盖式更新，只留下一个 session 必须的上下文。
> 开场必读顺序、模板、铁律见 [`README.md`](./README.md)；进度见 [`tasklist.md`](./tasklist.md)。

---

# 🆕 本空间刚建立（2026-07-19），下一个 session = **起步：确认范围 + 逐候选 recon**

## 第一件事（**不是动码**）
1. 读 `README.md`（尤其"已就位地基"+"连接器侧移植模板 5 步"+"候选连接器表"）。
2. 读 iceberg 蓝本：`../perf-hotpath-iceberg/designs/FIX-PERF-07-unified-per-statement-table-owner-summary.md` + 代码 `fe-connector-iceberg/.../IcebergStatementScope.java`（唯一现成范本）。
3. **向用户复述范围并确认**：候选 = paimon(高) / hive-hms(中-高) / hudi(中)；maxcompute/es/jdbc/trino 大概率排除（0 metastore loadTable fan-out）。先定"做哪几个、从哪个起"。
4. 选定第一个连接器后，按 README「单连接器立项流程」step 1 = **recon**（数一条 DML 加载同表几次 / 现有缓存边界 / 有无胖句柄+跨臂暂存），产出现状图，再写设计交用户确认，再动码。

## 关键已知（省得重新发现）
- **地基已就位、勿再改**：`ConnectorStatementScope` + `ConnectorSession.getStatementScope()`（fe-connector-api）+ `ConnectorStatementScopeImpl` + `StatementContext` 懒建/重置 + `ConnectorSessionImpl` 构造期捕获 + `ExecuteCommand` 重置（fe-core）。**移植=纯连接器侧、免 fe-core 两段验**。
- **目前只有 iceberg 用了该 SPI**（`grep -rln getStatementScope fe/fe-connector/*/src/main/java` 只出 iceberg + api）。
- **初摸数据**（2026-07-19，`loadTable`/`getTable` 触点文件数）：paimon 4 / hive 3 / hudi 1 / maxcompute·es·jdbc·trino 0。写路径连接器：hive、iceberg、jdbc、maxcompute、hudi 有 `getWritePlanProvider`/写面；**paimon 未在连接器层 override `getWritePlanProvider`**（写路径结构待确认）。

## 起步须与用户确认的点
- **范围**：只做 catalog-backed 三家（paimon/hive/hudi），还是也看读-only 连接器？（建议：先只做多载明显的，读-only 复核后排除）。
- **顺序**：建议从 **paimon** 起（多载最像 iceberg、`fe-connector-cache` 框架副本已在），或从写路径最清晰的 **hive** 起。
- **hive 网关特殊性**：hms 网关按 handle 选 sibling provider（委派 iceberg/hudi），且 hms 休眠（未进 `SPI_READY_TYPES`）——设计时要专门处理"网关自身 vs 委派 sibling"的作用域归属。

## 铁律提醒
- 地基勿再改；移植只写连接器侧；暴露地基缺口先停手交 review。
- 作用域跨用户即泄漏——凡有 session=user/凭证语义的连接器，复核共享安全（iceberg 判据：授权在 load 调用里，缓存命中绕过它）。
- surgical：模板里"拆胖句柄""下沉暂存"仅当该连接器真有对应物才做。

---

# 🗂 遗留 / 关联
- iceberg 蓝本任务空间：`../perf-hotpath-iceberg/`（PERF-07 权威设计/小结）。
- 架构记忆：`iceberg-table-resolution-cache-scoping`。
- e2e 一律留各连接器进 `SPI_READY_TYPES` 切换阶段统一补（对齐 `hms-iceberg-delegation-needs-e2e`）。
