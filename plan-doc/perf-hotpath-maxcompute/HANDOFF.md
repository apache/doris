# 🤝 Session Handoff — perf-hotpath-maxcompute

> 滚动文档：顶部「下一步」每 session 覆盖式更新；底部稳定区勿覆盖。
> 完成明细进 `git log` + 本空间 `progress.md`；回填伞形 `plan-doc/connector-cache-unification/`。

---

# 🆕 下一步（覆盖区）

## 当前状态（2026-07-24）

- **Round 1（PERF-MC01）= 每语句表句柄记忆化：✅ 完成，commit `58daadd10e0`**。全模块 120 测试绿、checkstyle 0 违规、两处变异验证（去 memo + db-blind key 均能令守门测试变红）、净室对抗复审（parity/staleness HOLD、concurrency 经 verify REFUTED、test-quality 补跨-db 用例后 HOLD）。
- **e2e 未跑**（需集群）：异构 + 独立 max_compute catalog 的 SELECT/分区裁剪/写路径解析计数，留标注。
- 设计权威：[`designs/round-1-handle-memo-design.md`](./designs/round-1-handle-memo-design.md)；日志见 [`progress.md`](./progress.md)。

## 之后

- **PERF-MC02**（doc-only，随手）：清 `beginTransaction` / `MaxComputeWritePlanProvider` 的过时 "dormant until cutover" 注释（写路径早已 live）。可并进伞形 WS-DOC 统一做。
- **PERF-MC03**（可选、低优先）：跨查询 ODPS `Table` 缓存——出现真实热点 profile 再上。
- 伞形层面：WS-MC 完成后，下一个 workstream = **WS-ES**（es 连接器 per-scan hoist + mapping 承载）。

---

# 🧱 稳定区（勿覆盖）

## 铁律（继承伞形）
1. **fe-core 源只出不进 + 禁 scaffolding 搬迁**：新缓存/memo 一律连接器侧。本轮 memo 在 `MaxComputeConnectorMetadata` 实例上，天然合规。
2. **authz 准入**：maxcompute 一 catalog 一套**静态凭证**、单一 ODPS 身份（无 per-user vending）→ 名字为 key 的 memo authz-安全。
3. **parity 只减不改**：性能修复必须行为不变；present 路径 memo、absent 路径逐字节等价今天（每次重探）。

## build / 验证坑（继承伞形）
1. maven 用绝对 `-f <abs>/fe/pom.xml`；`${revision}` 未 flatten → `-am`；测试用 `install` 或单模块 `test`（deps 已 install 后）。
2. 测试必加 `-Dmaven.build.cache.enabled=false`；别用 `-q`；grep `BUILD SUCCESS` + `Tests run:`。
3. 后台跑直接 `mvn … >> log 2>&1`（别 nohup&），读日志非通知 exit code。
4. checkstyle 绑 `validate` 阶段（`install`/`test` 都会跑），扫 test 源；主源 ≤120。

## 并发探测（动码前）
- 本 worktree `find fe -newermt '-120 sec'`（滤 `/target/`）+ `pgrep -a -f maven`；`/mnt/disk1/gq` 是**别的用户**的 worktree（独立 ~/.m2），不冲突。
- 提交只精确 `git add` 本任务文件，禁 `git add -A`（工作树有大量无关 untracked）。

---

# 🔗 关系
- **伞形**：[`plan-doc/connector-cache-unification/`](../connector-cache-unification/)（WS-MC 行）。
- **模板**：[`plan-doc/perf-hotpath-iceberg/`](../perf-hotpath-iceberg/) / [`perf-hotpath-hudi/`](../perf-hotpath-hudi/)。
- **问题类**：[`perf-heavy-op-hot-path-problem-class.md`](../perf-heavy-op-hot-path-problem-class.md)。
