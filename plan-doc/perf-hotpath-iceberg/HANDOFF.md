# 🤝 Session Handoff — fe-connector-iceberg 热路径重操作修复

> **滚动文档**：每次 session 结束**覆盖式更新**，只保留下一个 session 必须的上下文；完成明细进 `git log` + `designs/` + `progress.md`。
> **范围** = 逐一修复审计报告 `../reviews/perf-audit-fe-connector-iceberg-2026-07-17.md` 的 23 条发现 → **11 个 `PERF-NN` 任务**，见 [`tasklist.md`](./tasklist.md)。
> 开场必读顺序、单项立项流程、约束铁律：[`README.md`](./README.md)。

---

# 🆕 下一个 session = **实现 PERF-01（设计已定稿，进入编码）**

## 现状（session 1，2026-07-17）

- PERF-01 **复核 + 红队 + 设计全部完成并定稿**，写入 [`designs/FIX-PERF-01-table-memo-design.md`](./designs/FIX-PERF-01-table-memo-design.md)。**未动任何产品代码**。
- 设计经用户多轮对齐 Trino 拍板，**最终形态**：
  - **① 胖 handle**：`IcebergTableHandle` 加 `transient Table resolvedTable`（不序列化）+ getter/setter；读表统一"胖 handle 优先"；`withSnapshot/withRewriteFileScope/withTopnLazyMaterialize`（`:182/198/207`）**携带前行**；sys 表 handle 不用。→ 查询内单实例、随查询计划自动回收、**连接器侧零每查询累积**。
  - **② 跨查询 `IcebergTableCache`**（新类，仿 `IcebergLatestSnapshotCache`，值=raw Table）：挂长生命周期 `IcebergConnector`，传进每个 fresh metadata + provider，**在读 helper 里消费**（非 catalog-seam 装饰器 → DDL 天然隔离）。**gate=`isUserSessionEnabled() || restVendedCredentialsEnabled()` 关闭**（红队 BLOCKER：vended token 过期会 403）。
  - **Part B（convertPredicate 收窄）已删**（红队证伪为 no-op）。
- 度量守门：`RecordingIcebergCatalogOps` 断言规划期对同一表 `loadTable` 远端=1；全查询 1（跨查询开）/≤2（关）。

## 下一个 session 第一件事（TDD，精确到动作）

1. 先读 `designs/FIX-PERF-01-table-memo-design.md`（§3 设计 / §4 实现计划 / §5 风险 / §6 度量守门）——这是权威 spec。
2. **先写测试（TDD）**：`RecordingIcebergCatalogOps`（test 已存在）计数守门——规划期对同一表 `loadTable` 远端次数 = 1（修前基线先记录）。
3. 按 §4 实现计划 5 步落地（Commit 1，一个 commit）：
   - `IcebergTableHandle`：transient `resolvedTable` + getter/setter + 3 个 `with*` 携带。
   - 新增 `IcebergTableCache.java`（仿 `IcebergLatestSnapshotCache`，值 `Table`）。
   - `IcebergConnector`：构造 `tableCache`、传入 metadata/provider、`invalidate*` 三钩子（`:523-553`）、gate。
   - `IcebergConnectorMetadata.loadTable(handle):540` 改"胖 handle 优先→跨查询缓存→remote→setResolvedTable"；`getColumnHandles`/`getTableStatistics`/`resolveTimeTravel`/`getMvccPartitionView`/`listPartitions`/`beginQuerySnapshot` 经它；sys 分支不动。
   - `IcebergScanPlanProvider.resolveTable:1981` 改"胖 handle 优先"+ per-call `wrapTableForScan`；构造加 `tableCache` 参。
4. 验证（parity + 减负）→ 独立 commit `[perf](catalog) fe-connector-iceberg: <subject> (PERF-01)` → 写 `designs/FIX-PERF-01-...-summary.md` → 更新 tasklist/progress/本 HANDOFF。

## ⚠️ 关键认知（别重踩）

- **gate 两半都要**：`isUserSessionEnabled() || restVendedCredentialsEnabled()`（后者独立于前者，红队 Attack 2 = BLOCKER）。关的只是跨查询层；胖 handle 层不 gate（查询内 token 新鲜）。
- **胖 handle 存 raw Table**；`resolveTable` 命中后仍 per-call `wrapTableForScan`（Kerberos FileIO 不冻进缓存）。
- **DDL/写/procedure 不碰 resolvedTable、不经读 helper**（走裸 ops 拿 fresh base）。
- **memo 放连接器/handle 侧，别碰 fe-core 源**（fe-core 只出不进铁律）。`IcebergTableCache` 挂 `IcebergConnector`，`resolvedTable` 挂 `IcebergTableHandle`——都在连接器侧。
- 携带 transient 的原因：`getColumnHandles` 在 pin 前 set，`pinMvccSnapshot` 用 `withSnapshot` 换 handle，不携带则 pin 后重解析。

---

# 🧰 构建/验证坑（承自主线 HANDOFF，直接复用）

1. **测试必加 `-Dmaven.build.cache.enabled=false`**（否则 surefire 被静默跳过，BUILD SUCCESS 是陈旧文件）。
2. **`mvn ... | tail` 后的 `$?` 是 `tail` 的** —— 读 `BUILD SUCCESS`/`BUILD FAILURE` 行。
3. **maven 用绝对 `-f <abs>/fe/pom.xml`**（cwd 跨调用持久，`cd` 破相对路径）。
4. **`regression-test/conf/regression-conf.groovy` 本就脏** —— 别 `git add -A`，精确 add。
5. **并发 session 共享同一 worktree** —— 动码前探测（`git log/status` + `pgrep maven` 看 `etime`）。
6. **本 worktree 的 `.git` 是文件** —— rebase 脚本用 `git rev-parse --git-path`。

---

# 🗂 开放问题

- 无。设计定稿，进入实现。
