# 🤝 Session Handoff

> **滚动文档**：每次 session 结束**覆盖式更新**，**只保留下一个 session 必须的上下文**；完成的工作明细**不落这里**（在 `git log` + `tasks/` 设计文档里）。协作规范：[AGENT-PLAYBOOK.md](./AGENT-PLAYBOOK.md)。
> **范围** = 修 TeamCity **CI 997422**（Doris_External_Regression）的失败用例。

---

# 🆕 下一个 session = **重跑 CI 验证本轮 3 个修复**

> **本轮任务** = TeamCity `Doris_External_Regression` **#997422**（PR 65474 @ `6a450c9fa79`）
> **10 failed + 2 muted**（occurrence 口径 12）。
> **权威文档**：根因分析 = [`tasks/ci-997422-failure-analysis.md`](./tasks/ci-997422-failure-analysis.md)（18-agent recon + 3-lens 对抗复核 + 本人独立复核；每条证据带 file:line / 日志行号 / 实测数字）。

## 🔑 定性：**不是集群故障**，别去查宕机/OOM

BE 单次启动、优雅退出（`be.out` 仅退出时 LSAN leak summary，零 `SIGSEGV`/`SIGABRT`/`CHECK failed`）· `dmesg.txt` **无 OOM-killer**（失败时宿主机余 **19.03GB**）· 551 通过。
**12 个失败 = 4 个独立根因**（A+B / C / D / E），**A+B 是同一个 bug、占 9 个**。

## ✅ 上一轮（996541）的修复已被本轮 e2e 验证生效 —— **不是回炉**

`test_iceberg_time_travel`、`iceberg_branch_complex_queries`、`paimon_system_table`、`test_catalogs_tvf`、6 个 spec 演进用例**本轮全部未再出现**。本轮即上一版 HANDOFF 要求的 TODO 9（`4f8b35c2126` 的 e2e），**它验出了 `4f8b35c2126` 自己引入的回归**。

⚠️ **易混点**：`bd6fdf7009a` 修的是 `__DORIS_GLOBAL_ROWID_COL__`（topn lazy-mat 合成列）；本轮 A+B 是 `__DORIS_ICEBERG_ROWID_COL__`（iceberg 写路径合成列）。**两个不同的列、不同的 bug**，勿当同一个反复修。

---

## ✅ 已交付（3 个可修根因，各自独立 commit + 变异验证）

| commit | 根因 | 修的用例 | 守门 |
|---|---|---|---|
| `35cf72cce91` | **A+B** 计划路径丢连接器合成写列（`4f8b35c2126` 把 `LogicalFileScan:223` 改调**无人 override** 的 1-arg `getFullSchema(Optional<MvccSnapshot>)`） | **9 个** iceberg DML / hidden-column | 38/38；变异（删 1-arg override）红在 `expected:<3> but was:<2>` = CI 症状本身；相关 92/92；checkstyle 0 |
| `a4cba35725c` | **D** paimon shade 缺 `hive-serde` ⇒ `serdeConstants`（`9a10ece30c8` 删 hive-catalog-shade 触发 `c276e955683` 的潜伏洞） | `test_create_paimon_table` | 基线 jar serdeConstants=0 → 修后=1；**classload 冒烟**跑通 `<clinit>`；**plugin zip 端到端**加载成功且全 zip 恰好 1 份 |
| `6320389dc06` | **C** L17 guard 对 sys-table 是范畴错误（`270bd11f4da` 知情延期，**延期前提为假**） | `test_iceberg_position_deletes_sys_table` | 11/11；双变异（删排除→1 红；放宽到父类→5 红）；相关 62/62；checkstyle 0 |

**E（muted，`test_hdfs_parquet_group0`）= 有意不修**：上游 `51e44133b1d` 的 `mem_limit=35%` + 一个真含 2.000GiB 字符串列的上游 fixture（footer 实测 `total_uncompressed_size=2,147,483,749`=2³¹+101）⇒ PODArray 2GiB→4GiB 增长。`git merge-base --is-ancestor 51e44133b1d master` = **YES** ⇒ **master 同样复现**。无真 OOM、无泄漏、非过期用例。在本分支改那个 conf = 静默 revert 上游决定并掩盖真回归。**保持 mute + 记录理由**（理由全文见分析文档 E 节）。

---

# ⏭ 下一个 session 要做的

1. **重跑 CI（唯一真闸门）**。预期：A+B 的 9 个解开列数断言、C 解开 init、D 的 paimon HMS 恢复。
2. **⚠️ C 很可能需要第二轮**：去掉抛出只解开 init。真正绿还需 iceberg `$position_deletes` planner 认这个 pin（`doPlanPositionDeletesSystemTableScan` 读 `handle.hasSnapshotPin()` ← `IcebergConnectorMetadata.applySnapshot` 喂）—— **已 trace 未执行**。且该 suite `:562-568`（源表跨 ADD COLUMN 时间旅行）在本分支**从未跑过**。
3. **⚠️ A+B「修完就绿」未证**：v1 suite 原在 `:98` abort，其后 `:101` "row-id column must be populated" 与 v3 取值断言**在本分支从未执行过**。本改动只保证列回到输出。
4. **待用户裁决**：`scannedPartitionCount` 在 `$position_deletes` 上触发（= 旧 HANDOFF gap ③ 的后半、`PluginDrivenScanNode:1213`）—— 与 2026-07-13 `selectedPartitionNum` 签字冲突，**本轮故意未打包**，需先裁决。
5. **勿顺手修的潜伏洞**：A+B 的合成 row-id `uniqueId = -1`（`IcebergWritePlanProvider.buildRowIdColumn` 6-arg ctor；`ConnectorColumnConverter:89-91` 只回填 `>= 0`）⇒ 列回到输出后 L17 guard 退化成 name 匹配、无法在 pinned schema resolve。**今天不可达**（pinnedSchema 仅显式时间旅行非空，且无用例组合 show_hidden/DML + `@tag`/`@branch`/`FOR..AS OF`）。若要修，**按通用属性（schema-cache 来源）判，绝不按 iceberg 列名**。

---

# 🧰 构建/验证坑（本轮实测，下轮直接复用，别再踩）

1. **maven build cache 会静默跳过 surefire** —— 日志 `Skipping plugin execution (cached): surefire:test`，此时 **BUILD SUCCESS 是空的**（surefire 报告是上次的陈旧文件）。**所有测试必须加 `-Dmaven.build.cache.enabled=false`**。本轮第一次跑就中招（BUILD SUCCESS 但 0 测试真跑）。
2. **`mvn ... | tail` 后的 `$?` 是 `tail` 的**，不是 maven 的 —— 重定向到文件再取 `$?`，或读 `BUILD SUCCESS`/`BUILD FAILURE` 行。
3. **`surefire:test` 独立 goal 解析不了 `${revision}`** ⇒ 必须走 `test` 生命周期 + `-am`；上游模块无匹配测试时加 `-DfailIfNoTests=false`。
4. **`hive-serde` 闭包首次需联网**（`javax.servlet:servlet-api:2.4` 不在本地仓），`-o` 会失败。
5. **`-Dtest='org.apache.doris.datasource.**'` 全包 sweep > 10min**，会被 shell 超时砍；用具体类名清单。
6. **`regression-test/conf/regression-conf.groovy` 工作区本就是脏的**（session 开始前即 `M`）—— 三个 commit 均未包含它，**别顺手 `git add -A`**。
7. **`pgrep maven` 可能查到 1 天前的僵尸 until-loop**（本轮见 PID 843896，etime `1-01:58`，在轮询 Jul 15 就跑完的日志）—— **看 `etime` 再判定是否真并发**，别误判成活跃 session 而无谓停手。

---

# 🗄 被本次覆盖的旧上下文（catalog-spi 主线：删旧代码 / rebase / trino / QUIC 瘦身）

按用户 2026-07-15 指示，本文件已用 CI 任务上下文**完全覆盖**。**旧内容完整保存在 `8eb5463f769:plan-doc/HANDOFF.md`**（`git show 8eb5463f769:plan-doc/HANDOFF.md`）。其中**仍未结项、需要时去那里捞**的条目：
① 删除线 PR 收尾（拓扑多 commit → 最终 squash）+ 用户自跑翻闸 hms 全量回归；
② e2e 欠账矩阵（`tasks/hms-cutover-execution-plan-2026-07-10.md §4/§5`）+ 继承自上游的 `$position_deletes` e2e 翻闸门（**本轮 C 即其中一项，已修待验**）；
③ rebase 引入的 2 个集成缺口（`IcebergScanPlanProvider:1419` 丢 `enable.mapping.timestamp_tz`；`scannedPartitionCount` 对 `$position_deletes` 触发，语义待用户拍板 = 上面第 4 点）；
④ trino 改名 PR 收尾两笔（**需 release note**；BE 未跑全量构建 + fallback 无 e2e）；
⑤ 独立任务空间 `plan-doc/hive-catalog-shade-removal/`（**从它自己的 HANDOFF 进**）；
⑥ 并发 session 已结项的 QUIC 根治（`ae82ffd2573`）+ 插件包瘦身 Tier A（`dece64b9ff5`）明细。
