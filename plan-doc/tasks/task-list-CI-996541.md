# Task List — CI 996541 (Doris_External_Regression, PR 65474 @ `fa2fcf4b246`)

> **Issue doc** = [`ci-996541-failure-analysis.md`](./ci-996541-failure-analysis.md)（22-agent 递交 + 双 lens 对抗复核后的定稿）。
> **结论**：非集群故障（BE 单次启动、优雅退出、0 条 `F:` 级日志）。13 个失败（runner 口径；其中 1 个在 TeamCity 侧被 mute）收敛为 **5 个独立根因**。

## 用户签字（2026-07-15）

- **B3 → 走 C1**：修绑定层（`StatementContext.getSnapshot` 同表多版本不再回退 LATEST），**不**放宽 L17 guard。
  理由：skew 是本分支 `442a1081e6d` 自造的；上游单 key、有 pin 时从不返 empty，t1/t2 schema 同为 `{c1}` ⇒ 上游零 skew。guard 只是信使。
- **D → 走 b（整段注释）**：CREATE + `qt_test_10~14` 全部注释保留（**不删**）+ 一行 `logger.info` 说明后续重新支持；`.out` 同步删 5 个结果块。
  ⚠️ **已知代价（用户知情）**：丢掉 `dlf.secret_key`/`dlf.access_key` 的 `*XXX` 打码回归覆盖——而打码正是 `6c9b491dbcf` 标为「🔴 安全：脱敏不能随功能一起删（本轮最重要的发现）」的重点保留项。DLF 重新支持时整段恢复。
- **C 押后**：并发 session 正在同一战场（插件包瘦身 / 依赖 scope，`dece64b9ff5` / `ae82ffd2573`）活动，且 C 的产物级验证（重复类=0 / +916KB）需在瘦身后的新产物上重跑。

## 任务

- [ ] **T1 (A)** — iceberg partition spec evolution ⇒ 异构 arity 分区降级为 UNPARTITIONED（连接器侧）
  `IcebergPartitionUtils.java:659`，**名字列表**比较（非 arity 比较）。修 6 个用例。
- [ ] **T2 (B1)** — L17 guard 排除合成 row-id
  `PluginDrivenScanNode.java:936`。修 `test_iceberg_time_travel`、`iceberg_branch_complex_queries`。
- [ ] **T3 (B2)** — sys-table pin 按能力位门控
  `PluginDrivenScanNode.java:1044` `resolveSysTableSnapshotPin()`。修 `paimon_system_table`。
- [ ] **T4 (B3/C1)** — 绑定层同表多版本不再回退 LATEST
  `StatementContext.java:1015-1032`。修 `iceberg_query_tag_branch`。**最大改动，碰 Nereids 绑定层。**
- [ ] **T5 (D)** — `test_catalogs_tvf` DLF 整段注释 + `.out` + logger.info
- [ ] ~~**C**~~ — paimon `hive-serde` 打包（**押后**，等并发 session 收工）

## 施工须知

- **⚠️ 并发**：另一 session 在本工作树活动（`mvn dependency:tree` 运行中）。**path-whitelist `git add`，严禁 `git add -A`**；每条 fix 独立 commit，小步快提交。
- **⚠️ `dependencyManagement` first-match-wins，顺序承重**（`ae82ffd2573` 实证；挪到 netty-bom import 之后即静默失效）——C 施工时必读。
- **铁律**：fe-core 源只出不进（T2/T3/T4 碰 fe-core，均为修**本分支自己加的**代码/回归上游行为，非搬迁逻辑，不触发该律）；通用 SPI 层禁 source-specific 分支。
- maven：`mvn -o -f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl fe-core -am test-compile -Dmaven.build.cache.enabled=false`（**漏 `-am` → 假错 `${revision}`**）。

## 诚实前提（不得当成已验证）

每个 suite 都在**第一个** query 就 abort ⇒ 下游断言（如 `partition_evolution_ddl` 约 22 个 qt_）**在本分支从未执行过**。本轮修复只保证「不再 crash」，被解锁的下游断言状态**未知**，以真实 CI 为准。
