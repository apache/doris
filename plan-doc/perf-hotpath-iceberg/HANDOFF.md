# 🤝 Session Handoff — fe-connector-iceberg 热路径重操作修复

> **滚动文档**：每次 session 结束**覆盖式更新**，只保留下一个 session 必须的上下文；完成明细进 `git log` + `designs/` + `progress.md`，别堆这里。
> **范围** = 逐一修复审计报告 `../reviews/perf-audit-fe-connector-iceberg-2026-07-17.md` 的 23 条发现（C1–C23 → **一簇一任务** 11 个 `PERF-NN` 任务，见 [`tasklist.md`](./tasklist.md)）。
> 开场必读顺序、单项立项流程、约束铁律：[`README.md`](./README.md)。

---

# 🆕 下一个 session = **启动 PERF-01（簇1，收益最大）**

## 现状（session 0，2026-07-17）

- 本任务空间**刚建好**：`README.md` / `tasklist.md` / `HANDOFF.md` / `progress.md` / `designs/` 就位。
- 粒度已定：**一簇一任务**（用户 2026-07-17 拍板）—— 23 发现归并为 **11 个任务**。
- **零任务已实现** —— 全部 11 项 `⏳ 待启动`。
- 审计报告是**待 review 的结论草案**（报告 §5 自称"供 review 讨论，非结论"）。因此**不照单硬修**：每项第一步是**按 findings.json 的调用链复核行号 + 乘数**（README §单项立项流程 step 2），复核站得住再设计实现。

## 下一个 session 第一件事（精确到动作）

1. 向用户复述并确认起点："上次建好了 iceberg 热路径修复任务空间，下一步启动 **PERF-01（簇1，per-planning-pass Table memo）**，对吗？"（或用户另指定某项，如想先做改动小的 **PERF-08**＝维护路径 C19/C21）。
2. 确认后，**先复核 PERF-01**（design subagent）：
   - `grep` 复核簇1 的 7 个 loadTable 调用点行号（报告簇1 表 / findings.json 里 C1 C4 C6 C10 C16 的 `heavy_op` + `multiplicity` 字段）：`IcebergCatalogOps.java` 的 `loadTable`（报告记 :340）、`IcebergConnectorMetadata.getColumnHandles`（:587）、`IcebergScanPlanProvider.getScanNodeProperties`（:1300/1311）+ `resolveTable`（:1981-1993）、`convertPredicate`（:795-798 无条件清 `cachedPropertiesResult`）、`streamingSplitEstimate`（:410）、`planScanInternal`（:562）。
   - 确认无 `CachingCatalog`（全仓 grep）、`IcebergLatestSnapshotCache` 确只存 `(snapshotId,schemaId)`。
   - 确认 `beginQuerySnapshot` 的 pin 可作 `(TableIdentifier, snapshotId)` memo key。
3. 复核站得住 → 写 `designs/FIX-PERF-01-table-memo-design.md`（含**度量方案**：用调用计数证明 loadTable 从 N→1），设计红队，再实现。

## ⚠️ 关键认知（别重踩）

- **PERF-01 必须最先做**：它的 `convertPredicate` 失效收窄是 PERF-03（簇2 消第二次计算）和 PERF-10（C8）的前置；它立住的 `(table, snapshotId)` memo 模式被 PERF-02/03 复用。顺序错了要返工。
- **这是性能修复 = 行为必须不变**：验收闸门是「相关 UT/e2e 全绿（parity）+ 证明减负（调用计数/时延）」，不是新增功能。每项尽量补一个**调用计数守门**（如断言规划期 loadTable=1），否则回归无法自动发现。
- **memo 放连接器侧，别碰 fe-core 源**（fe-core 只出不进铁律）。PERF-09（C5 微批）、PERF-11 内 C14（通用节点）虽改 fe-core 通用层（允许，因非 source-specific），但须证跨连接器 byte+cost 双不变、禁按源名分支。
- **审计草案可能被复核推翻**：若某条复核后乘数/路径不成立，tasklist 标 🔬 + 记 progress，**不硬凑修复**。

---

# 🧰 构建/验证坑（承自主线 HANDOFF，本空间直接复用，别再踩）

1. **maven build cache 会静默跳过 surefire** —— 日志 `Skipping plugin execution (cached): surefire:test` 时 **BUILD SUCCESS 是空的**（报告是陈旧文件）。**所有测试必须加 `-Dmaven.build.cache.enabled=false`**。
2. **`mvn ... | tail` 后的 `$?` 是 `tail` 的**，不是 maven 的 —— 重定向到文件再取 `$?`，或读 `BUILD SUCCESS`/`BUILD FAILURE` 行。
3. **maven 用绝对 `-f <abs>/fe/pom.xml`**（cwd 跨调用持久，`cd` 会破相对路径 / 触发权限弹窗）。
4. **`regression-test/conf/regression-conf.groovy` 工作区本就是脏的**（session 开始前即 `M`）—— **别顺手 `git add -A`**，按需精确 add。
5. **并发 session 共享同一 worktree** —— 动码前探测并发活动（`git log/status` + maven 进程 + 近 90s mtime）；`pgrep maven` 可能查到 1 天前的僵尸 until-loop，**看 `etime` 再判**，别误判成活跃 session 无谓停手。小步快提交缩短被 amend 卷走窗口。
6. **本 worktree 的 `.git` 是文件不是目录**（linked worktree）—— rebase/冲突脚本禁硬编码 `.git/rebase-merge`，一律 `git rev-parse --git-path`。

---

# 🗂 开放问题 / 待用户裁决（不是 TODO，是需要拍板的）

- **审计整体是否已被用户接受为立项依据**：目前按"每项立项前自行复核行号/乘数"处理。若用户已整体签字，可省去部分复核力度（但行号复核仍建议保留）。
- ~~任务粒度~~：已定 **一簇一任务**（2026-07-17），无遗留。
