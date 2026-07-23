# 🤝 Session Handoff — 连接器缓存框架统一 (connector cache unification)

> **滚动文档**：顶部「下一步」区每 session 结束**覆盖式更新**，只保留下一个 session 必须的上下文；
> 底部「稳定区」（推进流程 / 铁律 / build 坑 / 并发探测）**勿随意覆盖**，是跨 session 常驻规则。
> 完成明细进 `git log` + 各兄弟空间的 `designs/` + 本空间 [`progress.md`](./progress.md)。
> **本空间是"伞形协调空间"**：调研结论在 [`00-research-report.md`](./00-research-report.md) + [`connectors/*.md`](./connectors/) + [`data/connector-audits.json`](./data/connector-audits.json)；
> 真正的逐连接器执行**各自另开兄弟空间**（`plan-doc/perf-hotpath-<connector>/`，镜像 `plan-doc/perf-hotpath-iceberg/` 布局），不并进本空间（报告 §9）。

---

## 📖 新 session 开场读序

```
1. Read 本 HANDOFF.md                          ← 你在这（下一步 + 稳定区规则）
2. Read tasklist.md                            ← workstream 追踪到哪了、决策签了没
3. 需要某条发现细节时才 Read 00-research-report.md 对应节 / connectors/<c>.md / connector-audits.json
   —— 别默认全读；报告很长，按需取。
4. 参考实现模板：plan-doc/perf-hotpath-iceberg/（README=流程 / HANDOFF / tasklist / designs）
   问题类：plan-doc/perf-heavy-op-hot-path-problem-class.md
```

---

# 🆕 下一步（覆盖区）

## 当前状态（2026-07-23）

- **调研已完成**：19-agent clean-room 对抗式编排，~2.4M token，0 error；7 连接器各经"审计→独立对抗验证"，**5 CONFIRMED / 2 ADJUSTED**（hudi、es，更正已折进报告）。
- **产品代码 0 改动，e2e 未跑。** 所有 `file:line` 是调研时（2026-07-22 HEAD）快照。
- **本 session 只搭了追踪空间**（HANDOFF + tasklist + progress），**未启动任何执行**、未做任何拍板。

## ⚠️ 下一个 session 第一件事 = 先拿用户对 4 个决策的签字（**别直接开工**）

报告 §8 有 **4 个必须由 owner 拍板的决策**，它们决定本轮打多大、按什么顺序、要不要现在就通用化门禁。
按本项目惯例（memory `ask-user-explain-in-chinese-first`）：**先用浅显完整中文向用户讲清每个决策的背景 + 各选项 + 我方建议**，拿到签字后再按签字范围启动。
4 个决策的追踪见 [`tasklist.md`](./tasklist.md) 的「待拍板决策」表；一句话摘要：

| 决策 | 问题 | 报告建议（默认） |
|---|---|---|
| **D1 范围** | 本轮只打 hudi，还是把 maxcompute `MC-1`、es `ES-F1/F2` 一并纳入? | hudi 单独立项（旗舰）+ mc/es 各一个小 PR；jdbc/paimon/trino P2 进 backlog |
| **D2 排序** | 先建共享底座泛化，还是 per-connector-first? | **per-connector-first**（泛型 table/handle 缓存目前只有 hudi 一个新消费者，出现第 2 个再上收，避免投机抽象 / Rule 2） |
| **D3 门禁通用化** | `check-authz-cache-sharding.sh` 现硬编码只盯 `IcebergConnector.java`，现在就改成"扫描任意声明 `SUPPORTS_USER_SESSION` 的连接器"吗? | 今天所有推荐的新缓存都 authz-安全，故 (b) 延后无当下风险；但 (a) 前瞻通用化更稳 —— 交用户选 |
| **D4 共享 helper** | 是否投入一个 fe-core 共享的"经 statement scope 解析表" helper? | 复核显示只有 iceberg（已有）+ hudi 刚需 → 让 hudi 在自己模块内做 memo，**不提炼进 fe-core**（碰铁律 A，Rule 2） |

## 拿到签字后：默认启动 **WS-HUDI（唯一 P1 真缺口，旗舰）**

- **为什么是 hudi**：它是所有 SPI 连接器里缓存最薄的一个——零连接器侧 cross-query 元数据缓存、零 per-statement memo，甚至没像 hive 那样包 `CachingHmsClient`（用裸 `ThriftHmsClient`），导致 `HoodieTableMetaClient` 每 planning pass 重建约 5–6 次、schema 重解析约 4 次。这是与"翻闸前 iceberg 那批被删缓存"最像的 loop-amplified 缺口。
- **启动动作**：新开 `plan-doc/perf-hotpath-hudi/`（镜像 `perf-hotpath-iceberg/` 布局：README/HANDOFF/tasklist/designs），把 `HD-P01..05` 拆成 `PERF-NN` 逐项立项。交付概要见 [`tasklist.md`](./tasklist.md) 的 WS-HUDI 详情。
- **⚠ 动码前先按 HEAD 重侦察**（memory `execution-blueprint-overestimates-recon-first`）：报告的 `file:line`、乘数、"5–6x/4x" 都是调研估算，**行号信 grep 不信文档**；hudi 三条 ADJUSTED 更正（authz-safety 依据、HD-P03 单 pass 计数、HD-P01 无条件下界 ~3–4x）务必读 [`data/connector-audits.json`](./data/connector-audits.json) 的 hudi `verify.corrections`。

---

# 🧱 稳定区（勿覆盖）

## 从调研到执行的推进流程（每个 workstream）

> 一次一个 workstream 串行推进；这些是**性能/结构修复 → 必须行为不变**，复核与"证明只减不改"是重点。对齐 `step-by-step-fix` skill 与参考空间 `perf-hotpath-iceberg/README.md` 的立项流程。

```
1. 拿 owner 签字（D1 决定本 workstream 是否在本轮范围内）。
2. 新开兄弟空间 plan-doc/perf-hotpath-<connector>/（镜像 perf-hotpath-iceberg 布局），
   把该连接器的发现（HD-P0x / MC-x / ES-Fx …）拆成 PERF-NN 逐项。
3. 【动码前必做】按 HEAD 重侦察：重新 grep 行号、重新确认 ①重操作真实成本 ②乘数 ③是否真无缓存兜住。
   调研是估算，复核可能缩小/推翻某条（如 hudi HD-P03 对过滤查询已去重到 ~1 次）。
4. 写设计文档 + 设计红队（clean-room 对抗，本项目偏好）。
5. 实现：最小改动、守铁律、连接器侧、匹配风格。
6. 验证：相关 UT/e2e 全绿（parity 禁回归）+ 补调用计数守门（如 metaClient build 从 N 降到 1）。
   e2e 需集群，本地不跑的留标注。
7. 独立 commit：[perf](catalog) fe-connector-<c>: <subject> (PERF-NN)。commit log 全英文。
8. 更新该兄弟空间 tasklist/HANDOFF；回本伞形空间更新 tasklist workstream 状态 + append progress.md。
```

## 🔒 铁律（违反即返工，动码前记牢）

1. **fe-core 源只出不进 + 禁 scaffolding 搬迁**（memory `fe-core-source-isolation-iron-rules`）：新缓存/memo 一律放**连接器侧**（`fe-connector-<c>` / handle / `fe-connector-cache` 工具箱），**不得**为省事把 table 缓存/属性解析/派生 helper 塞进 fe-core。hudi/mc/es 的所有推荐修复都在连接器侧，天然合规；**D4 若选"提炼进 fe-core"= 净增 fe-core SPI，碰铁律 A，须用户显式签字**（报告建议不提炼）。
2. **fe-core 通用节点保持 connector-agnostic**：不在 `PluginDrivenScanNode` 等通用 SPI 层写 source-specific 分支；通用热路径若要改须证 **byte + cost 对所有连接器双不变**。本轮推荐修复**都不碰** fe-core 通用节点（纯连接器侧），若某项需要碰，停手交 review。
3. **fe-core 不解析属性**（memory `catalog-spi-no-property-parsing-in-fecore`）：属性派生放插件侧组装 → BE thrift → 回传。
4. **新缓存准入必查 authz**（报告 §6）：给任一连接器加"名字为 key 的 cross-query 缓存"前，先确认该连接器**是否 vend per-user 凭证**。今天 7 个非-iceberg 连接器都是**单一 catalog 身份**（hudi/mc/es/jdbc/trino/paimon/hive 逐一核实），故名字为 key 的缓存 authz-安全、**无需 Layer-3 bypass**。**但这是"当前"安全非"结构"安全**：一旦某连接器未来接 per-user 凭证 vending（REST-OIDC 等），名字为 key 的缓存立刻变泄漏面，而 `check-authz-cache-sharding.sh` 现只盯 iceberg（见 D3）。
5. **freshness-敏感数据的 cross-query 缓存必 TTL + REFRESH 有界**：hudi 的 metaClient/分区缓存要有界；**es 的 shard routing 绝不能 cross-query 缓存**（ES rebalance/refresh 模型），保持 per-statement。
6. **反指别硬套**（报告 §7）：jdbc / trino / paimon / hive **不要**再加 iceberg-式重缓存（廉价透传 / 委托嵌入式引擎缓存 / SDK 已缓存 / 已 framework-aligned）；详见 [`tasklist.md`](./tasklist.md)「反指/不立项」。

## 🧰 build / 验证坑（继承自参考空间 + 本程序特有）

1. **maven 用绝对 `-f <abs>/fe/pom.xml`**（cwd 跨调用持久，`cd` 破相对路径）；`${revision}` 未 flatten → 必 `-am`；测试用 **`install`** 而非 `test`（`-am test` 不产 shade jar）。
2. **测试必加 `-Dmaven.build.cache.enabled=false`**（否则 surefire 静默跳过）；**别用 `mvn -q` 跑测试**（抑制 `BUILD SUCCESS`/`Tests run`）；grep 日志 `BUILD SUCCESS` + `Tests run:`。
3. **别 `nohup … &` 套 `run_in_background`**（maven 变孤儿假 exit 0）；后台跑直接 `mvn … >> log 2>&1`，**通知里 exit code 是 echo 的**，读日志 `BUILD SUCCESS` 行。
4. **动 fe-core 者两段验**（若某项不得已碰 fe-core）：连接器模块 `-pl <c> -am` 反应堆不含 fe-core；fe-core 改动须另 `mvn test -pl fe-core -am -Dtest=… -f <abs>/fe/pom.xml`。
5. **⚠ hudi 特有**：`fe-connector-hudi` 目前**零 `fe-connector-cache` 依赖**（pom 没引工具箱，也没包 `CachingHmsClient`）——WS-HUDI 第一步要**改 pom 引入工具箱**，这是 hudi 独有的前置（hive/paimon/mc 已引）。
6. **checkstyle 主源 ≤120 且扫 test 源**；import 序 `SAME_PACKAGE→THIRD_PARTY→STANDARD_JAVA`，组内字母序组间空行。

## 🔎 并发探测（动码前）

- 本 worktree：`find fe -newermt '-120 sec'`（滤 `/target/`）+ `pgrep -a -f maven`；`/mnt/disk1/yy/git/doris` 是另一 worktree，不冲突（memory `concurrent-sessions-shared-worktree-hazard`）。
- 本 worktree 的 `.git` 是**文件**不是目录（linked worktree）——rebase 脚本用 `git rev-parse --git-path`（memory `worktree-gitdir-is-a-file`）。
- 提交只精确 `git add` 本任务文件：工作树有大量无关 untracked（`.claude/`、`docker/`、`regression-conf.groovy` 本就脏），**禁 `git add -A`**。

---

# 🔗 关系

- **参考实现（模板）**：[`plan-doc/perf-hotpath-iceberg/`](../perf-hotpath-iceberg/)（iceberg 那批被删缓存的逐条修复，`PERF-01..11`）+ [`perf-heavy-op-hot-path-problem-class.md`](../perf-heavy-op-hot-path-problem-class.md)（本调研套用的"热路径重操作放大"问题类）。
- **主线**：`plan-doc/HANDOFF.md`（catalog-spi 主线）——**并行但不混流**。
- **协作规范 / context 预算 / subagent 纪律**：沿用 [`../AGENT-PLAYBOOK.md`](../AGENT-PLAYBOOK.md)。
- **交接纪律**（memory `handoff-discipline-per-phase`）：每轮完成即更新 HANDOFF/tasklist；每轮起步先读 HANDOFF 再对照 HEAD 真实代码 review（计划/行号可能过时）。context 用量过 30% 就找干净节点覆写 HANDOFF 并通知用户开新 session（memory `session-handoff-at-30pct-context`）。
