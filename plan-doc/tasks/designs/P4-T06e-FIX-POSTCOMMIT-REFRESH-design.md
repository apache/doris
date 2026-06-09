# FIX-POSTCOMMIT-REFRESH 设计（P3-12 / NG-8 / F15=F21）

> 严重度：🟡 minor（regression=no）。处置：**无产线逻辑改动**——仅 Javadoc 泛化 + DV-018/D-034 登记。
> 用户拍板（2026-06-08）：**DV-018 + Javadoc 泛化**（不回退到 legacy 传播失败）。
> 来源：`plan-doc/reviews/P4-maxcompute-full-rereview-2026-06-07.md` §A NG-8。
> 实现 commit：`1f2e00d3696`（Javadoc + 本设计）；账本回填 commit 见下一 doc-sync commit。

## Problem

翻闸后 `PluginDrivenInsertExecutor.doAfterCommit()`（`:177-186`）用 try/catch 包 `super.doAfterCommit()`，
post-commit 缓存刷新失败时仅 log warning、INSERT 仍报成功；legacy `MCInsertExecutor` 不 override →
刷新异常向上传播 → INSERT 报 FAILED。这是**可观察的行为变更**且无书面登记，且现有 Javadoc（`:164-176`）
只为 JDBC_WRITE 路径辩护，没覆盖现在同一 executor 也走的 MC connector-transaction 路径。

## Root Cause

`super.doAfterCommit()` = `BaseExternalTableInsertExecutor.doAfterCommit()`（`:133-140`）
→ `RefreshManager.handleRefreshTable(...)`（`RefreshManager.java:125-156`），它做三步且**全部在提交之后、纯 FE 侧**：
1. 校验 catalog/db/table 存在（不在抛 `DdlException`）；
2. `refreshTableInternal(...)` 刷新 FE 本地 schema/row-count/分区缓存（`:152`）；
3. `logRefreshExternalTable(...)` 写一条 external-table refresh editlog（`:155`），通知 follower 失效缓存。

按生命周期序（`BaseExternalTableInsertExecutor:118-124`）：`doBeforeCommit → commit（远端数据持久）→ doAfterCommit`。
即 `handleRefreshTable` 跑时数据已落 ODPS / 远端、FE 无法回滚；它**从不触碰已提交的远端数据**，
只动 FE 缓存与 follower 通知。故刷新失败 ⇒ 报 FAILED ⇒ 用户/pipeline 重试 ⇒ **重复写**——
cutover 的「吞 + warn」反而更安全。

## Design

不改任何产线逻辑（swallow 行为本身正确、对 JDBC 与 MC 两路径同样安全）。仅两件事：

1. **Javadoc 泛化**（`PluginDrivenInsertExecutor.java:164-176`）：把 swallow 理由从「只讲 JDBC_WRITE」
   扩到覆盖 connector-transaction(MC) 路径，写明：
   - 两路径在 doAfterCommit 时数据均已持久（JDBC=BE 直提 / MC=transaction manager onComplete 提交）；
   - `super.doAfterCommit()` 只刷 FE 缓存 + 写 refresh editlog、不碰远端数据；
   - swallow 最坏只致**瞬时缓存 stale，自愈于下次 refresh/TTL**；
   - 显式注明本行为**有意分歧于 legacy MCInsertExecutor**，引用 DV-018。
2. **账本登记**：D-034（决策：接受更安全的 swallow、不回退）+ DV-018（偏差：行为分歧于 legacy，已登记）。

## Implementation Plan

- 编辑 `PluginDrivenInsertExecutor.java:164-176` 的 Javadoc（注释 only，行宽 ≤120）。
- 新增 `decisions-log.md` D-034、`deviations-log.md` DV-018（索引行 + 详细记录）。
- 更新 `task-list-P4-rereview.md` P3-12 行 → DONE；`HANDOFF.md` 同步。
- 守门：`-pl :fe-core checkstyle:check`（注释改动的唯一真实闸：行宽 / Javadoc 格式）+ `import-gate`。

## Risk Analysis

- **零产线逻辑风险**：仅改注释，字节码不变。
- **对抗性安全核查（已做）**：`handleRefreshTable` 写的 refresh editlog 只是 follower 缓存失效提示、
  非数据真相源（ODPS 才是）；master 在写 editlog（`:155`）前已先本地刷新（`:152`）。即便 editlog
  丢失，follower 最坏缓存暂 stale、到自身 TTL/下次 refresh 自愈，**无数据正确性损失、无主从分裂**。
- **唯一被否决的替代**：回退到 legacy 传播失败 → 重新引入重复写隐患（review 判定更不安全）。

## Test Plan

### Unit Tests

无新增 UT。注释 only，无可被 mutation pin 的产线逻辑变化（与 P3-9/P3-10 不同——本项不动逻辑）。
swallow 路径本身的覆盖现状：`doAfterCommit` 的 try/catch 由现有 executor 测路径间接覆盖；
异常吞行为的 offline 直测受同类 harness 缺位限制（连接器/外表 insert 无轻量 spy harness，见 [DV-015]）。

### E2E Tests

CI-skip（需真实 ODPS）。真值闸：在 MC INSERT 提交成功后人为令 refresh 失败（如并发 DROP CATALOG），
断言 INSERT 仍报 OK（非 FAILED）+ 日志含 stale-cache warning。归类于写路径 live e2e 套件，与
DV-013/DV-014 写真值闸一并 live 验。

## 关联

- 决策 [D-034]、偏差 [DV-018]
- 复审 [§A NG-8](../../reviews/P4-maxcompute-full-rereview-2026-06-07.md)
- 同类 harness 缺位 [DV-015]
