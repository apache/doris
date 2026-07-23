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

## 当前状态（2026-07-23，设计定稿、owner 已确认）

- **调研 + 设计均完成；owner 4 决策已签字 + 设计后二次确认**（[`tasklist.md`](./tasklist.md) §0）。
- **设计定稿** → [`designs/foundation-design-FINAL.md`](./designs/foundation-design-FINAL.md)（配 `foundation-design-draft.md` + `review-1..3.md`）。11-agent 只读设计调研 + 3 路对抗评审，全程逐符号核对 HEAD。
- **底座 PR-0/1/2 已落地（下方「进行中」为准）；连接器消费 PR-3~7 + e2e 未跑。** 设计里 `file:line` 是 2026-07-23 HEAD 侦察快照，动码前须按 HEAD 重侦察。
- **核心结论**：走"先建底座"，但**底座几乎现成** → 整套 A/B/C + hudi/mc/es 消费 **全程 0 行 fe-core 改动**（D4 原"提炼进 fe-core"被侦察推翻：per-statement memo 底座已在 `fe-connector-api`，owner 二次确认不动 fe-core；iceberg 本轮就改挂）。

## ⚠️ 进行中：PR-0 + PR-1 + PR-2 已完成（2026-07-23/24）→ 下一步 PR-3；动每个文件前先按 HEAD 重侦察

设计已 owner 确认，已开工。**PR-0 完成**（预编译执行连接器作用域 reset 回归守门 + 外表可达性查实 + FINAL 设计机制描述更正；见 [`progress.md`](./progress.md) "2026-07-23 (3)"）。**PR-1 完成**（通用缓存封装升格为 `ConnectorMetadataCache`，纯重命名 + 构造器加 `entryName`，66 分区视图缓存测试证零变化；commit `a804145faaa`，见 "2026-07-23 (4)"）。**PR-2 完成**（语句作用域通用 helper `ConnectorStatementScopes.resolveInStatement` 放 `fe-connector-api`=**0 行 fe-core**，iceberg `sharedTable` 改委派 byte-identical；8+7 单测 + iceberg 全模块 1133 测试 0 失败 + 4 路对抗净室复审 PARITY_HOLDS + Rule-9 变异验证；commit `ae8c925074d`，见 "2026-07-24"）。**下一步 = PR-3**（iceberg 5 手写缓存收敛到 `ConnectorMetadataCache`，pre-resolved `CacheSpec` + 钉死 legacy entry 名 + `invalidateDb` parity 测试 + PR-1 推迟的 6 处 ttl≤0 映射去重；见下表）。

**但设计里行号/乘数是 2026-07-23 快照，动每个文件前按符号重 grep 确认**（memory `execution-blueprint-overestimates-recon-first`；已累计侦察更正——PR-0 机制"新 context"实为"复用+显式 reset"、PR-1"删兼容子类"实为无子类可删、PR-2 `rewritableDeleteSupply` 须留 iceberg 私有非搬进 helper）。PR-3 侦察须重确认：iceberg 5 缓存全独立 `final class` 建于 `MetaCacheEntry`、entry 名 hyphen（`iceberg-table` 等非 `iceberg.table`）、`formatCache` 挂 `IcebergScanPlanProvider` 非 metadata 对象、6 处 `ttl≤0→CACHE_TTL_DISABLE_CACHE` 映射（`IcebergComment/Format/LatestSnapshot/Partition/Table` + `PaimonLatestSnapshotCache`）、`invalidateDb` 现走 `Namespace.of(db)+id.namespace().equals`。

## 实施路线（8 个独立 PR，foundation-first；详见 FINAL 设计 §Sequencing / §Risk register / §Verification plan）

| PR | 主题 | 关键约束 / 守门 |
|---|---|---|
| **PR-0** ✅（2026-07-23 完成） | 验预编译 `EXECUTE` 重执行时 statement scope 是否每次刷新 | 已加 `ConnectorStatementScopeTest.executeCommandResetsConnectorScopePerExecution`（驱动 `ExecuteCommand.run()`、变异验证仅该测试变红）；**机制更正**：不是"新 context"而是复用 context + `ExecuteCommand.java:95` 显式 reset；**外表可达性已查实**（走普通 `executor.execute()` 路，非 OLAP 短路） |
| **PR-1** ✅（2026-07-23 完成，commit `a804145faaa`） | 升格 `ConnectorPartitionViewCache[V]`→`ConnectorMetadataCache[V]`；`PartitionViewCacheKey`→`ConnectorTableKey`；构造器加 `entryName` 参数；iceberg/hive/paimon 改挂；修陈旧 javadoc。**更正**：无"旧类可删"（是重命名非删除）；**收窄**：ttl 去重 + 预解析 CacheSpec 构造器推迟到 PR-3（避免二次翻动 iceberg 手写缓存） | ✅ `install -pl cache,hive,iceberg,paimon -am` BUILD SUCCESS；66 分区视图缓存测试 0 失败，条目名/配置项/键逐字节不变 |
| **PR-2** ✅（2026-07-24 完成，commit `ae8c925074d`） 语句作用域 helper（`fe-connector-api`）+ iceberg 改挂 | 加 `ConnectorStatementScopes.resolveInStatement` + namespace 注册表（`ICEBERG_TABLE`）；iceberg 私有包装改委派（key 逐字节不变，`rewritableDeleteSupply` 留私有） | ✅ **fe-core 0 行**；8+7 单测 + iceberg 全模块 1133 测试 0 失败 + 4 路对抗净室 PARITY_HOLDS + Rule-9 变异（queryId→sessionId 恰红 2 byte-key 测试） |
| **PR-3** iceberg 5 缓存收敛到通用封装 | 用 pre-resolved `CacheSpec` 构造器、**钉死旧 entry 名**（`iceberg-table` 等，防 dashboard/`*ForTest` 断） | **`invalidateDb` parity 测试**（Namespace-equals→String-equals，须证 Doris iceberg 命名空间单层等价）；连接器侧保留凭证置空 |
| **PR-4** 旗舰 hudi | metaClient+schema 每语句 1 次；HMS 走 `CachingHmsClient`；`(表,已完成instant)` 跨查询分区缓存；per-scan hoist | **记忆"不可关闭投影"非原始 metaClient**（scope 末关所有 AutoCloseable）；instant **每语句重取最新已完成**、只缓存分区列表；pom 加 `fe-connector-cache` + **验 Caffeine≥2.9.3 自带**；**e2e 必需**（异构+独立 hudi-on-HMS + 并发提交分区列举） |
| **PR-5** maxcompute（小） | `getTableHandle` 每语句 memo，消冗余远程 `exists()`（14–17→1） | 连接器侧、fe-core 0 行；仅 in-statement `buildConnectorSession` 路径 |
| **PR-6** es（小） | `EsMetadataState` per-scan hoist（2→1）；raw mapping 挂语句作用域态 | **分片路由必须每语句、拆 `fetch()`**、绝不跨查询缓存 |
| **PR-7** 门禁通用化（脚本） | 模块内扫"新建缓存"构造点 + 断言凭证置空；hive 网关纳入 + fail-loud 守卫；零声明者硬失败 | 独立、随时；扩 self-test fixtures |

- **PR-1 + PR-2 是底座**，解锁 PR-3/4/5/6；**PR-7 独立**；**PR-0 阻塞 PR-2**。
- **连接器 PR（4/5/6）用兄弟空间**（`plan-doc/perf-hotpath-<c>/`，镜像 iceberg 布局）逐项立项；**底座 PR（1/2/3/7）跨连接器**，进本伞形空间 `designs/`。
- **本轮全程守铁律 A（fe-core 0 行）**——实施中若发现某步"不得不碰 fe-core"，**停手交 review**（大概率又是侦察能推翻的伪需求）。

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
