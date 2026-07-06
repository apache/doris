# 🤝 Session Handoff

> 滚动文档：每次 session 结束**覆盖式更新**（不累积历史；历史见 `git log plan-doc/HANDOFF.md`）。协作规范：[AGENT-PLAYBOOK.md](./AGENT-PLAYBOOK.md)
> **范围** = catalog-spi **主线** handoff。metastore/storage 抽取子线**已 CLOSED**（合入 #64446/#64653/#64655）——`metastore-storage-refactor/` 仅历史留存、勿读；metastore-spi 现状直接读代码 `fe/fe-connector/fe-connector-metastore-spi/`。

---

# 🎯 当前状态（2026-07-06）= **P7.3 原子批 INC-1..INC-5 的 FINAL 原子 feature commit 已落地 = `0b19506acfe`（40 文件：22 新 + 18 改，+7456/−34）。写链 + 读链全部完成、全绿、且已提交。工作树不再持有本批未提交功能码（原子批 WIP 阶段结束）。下一步 = cutover（P7.4/P7.5，另起原子批）：翻闸 `SPI_READY_TYPES` + fe-core 写链 retype + 读侧放锁接线 + 摘 legacy `HiveTransactionMgr` + 删 `datasource/hive`（守跨连接器删序）+ 跑 ACID 集成测试套件（R-002 项目最大风险）。**

> **本轮做了什么（FINAL：一次原子 feature commit，已落地）**：按白名单 path-whitelist `git add`（api 3 + hms 11 + hive 22 + fe-core 4 = 40；`git add <module>/src` 仅收该模块 src 下 wanted 文件，`target/` 已 gitignore）→ 暂存后 `git diff --cached` 自检 = 正好 **40/40（22 A + 18 M）**，无一文件落在 4 白名单区域外、无 `.bak`/`regression-conf` 明文 key/`.audit-scratch`/`conf.cmy`/`META-INF`/`docker`/`.claude`/`plan-doc` 混入 → commit `0b19506acfe`（message 沿用 `[feat](catalog) P7.3 hive …` 范式 + 三段模块摘要 + 验证数据 + `Refs: apache/doris#65185` + `Co-Authored-By`/`Claude-Session` 尾）。**核实**：`FakeConnectorContext`/`HmsPartitionInfo` 是既有已跟踪共享类（被既有 hudi/hive 测试引用），非本批新增（HANDOFF 旧叙述误记为"新"）→ 本批自洽、单独 checkout 白名单集即可编译。

> **本轮验证（独立重跑、不信自述、提交前 fresh 编译杜绝 stale `.class`）**：`fe-connector-hive` `clean test` **104 测试 0 失败 0 错误 0 跳过 BUILD SUCCESS**；`fe-connector-hms` 49、`fe-connector-api` 55 全绿；`fe-core` 从零编译 4560 源 + 受影响 2 测试类（`ConnectorContractValidatorTest` 7 + `PhysicalConnectorTableSinkTest` 9 = **16** 0 失败 0 跳过）+ checkstyle 0；连接器 3 模块 checkstyle 0；`check-connector-imports.sh` 净（仅命中 `HiveVersionUtil` 已知误报）。

> **✅ INC-1..INC-5 已全部提交进 `0b19506acfe`**（不再是 uncommitted 工作树 WIP；原子批载体阶段结束）。逐增量移植记录留档备查：INC-3=`tasks/P7.3-INC-3-portmap.md`、INC-4=`tasks/P7.3-INC-4-portmap.md`、INC-5=`tasks/P7.3-INC-5-portmap.md`；各 INC 的文件清单见 `git show --stat 0b19506acfe`。

> **权威实现依据**（信 HEAD 控制流，不信行号）：
> - `tasks/P7.3-hive-write-txn-implementation-design.md`（§2 决策 D1–D12、§4 签名、§5 移植细节、§6 构建顺序）。
> - **`tasks/P7.3-INC-3-portmap.md`（595 行）**：逐行移植地图 + §7 12 处 GAP + **§9 已核实的实现前检查**（GAP-11/12 结论、D6 MPU map 重载确认、FS 构建/鉴权/iceberg 模板签名全确认）。INC-4/INC-5 也可续用其读侧 §5.4 + 测试计划 §6。
> - 移植源 = HEAD `fe/fe-core/.../datasource/hive/HMSTransaction.java`；模板 = `fe-connector-iceberg/.../IcebergConnectorTransaction.java`+`IcebergWriteContext.java`。

---

# 🚀 下个 session 任务 = **cutover（P7.4/P7.5，另起原子批）：把 dormant 的 hive 插件真正接上线**

> **① FINAL —— ✅ 已落地**（commit `0b19506acfe`）。INC-1..INC-5 全部提交 + 全绿 + 已过净室对抗复审（INC-5：1 CONFIRMED 已修 + 1 自查已修 + 测试补强）。**不再回炒任何增量、勿重写/勿再复审已提交代码。**

> **② cutover（P7.4/P7.5，最硬风险 R-002）** —— 参照 P5 paimon(#64446/#64653) + P6 iceberg(#64688) 翻闸+删 legacy 范式：
> - **翻闸**：把 `HIVE` 加入 `SPI_READY_TYPES`（当前 hive 天然 dormant——编译+单测但零线上路由，翻闸后才真正走插件路径）。
> - **写链 retype**：fe-core 写链改走通用 `ConnectorWritePlanProvider` 路径（DEC-1 seam 已就位、默认关）。
> - **读侧接线**：把 `Env.getCurrentHiveTransactionMgr()` 换成插件 `HiveReadTransactionManager`；把 `QueryFinishCallbackRegistry` 接到 `deregister`（放读锁 / 提交读事务）。
> - **摘 legacy + 删除**：摘 legacy `HiveTransactionMgr`；删 `datasource/hive`（+hudi + 23 个 HMS-iceberg 支撑类，**守跨连接器删序**：`datasource/hive/` 删不掉直到 `HudiUtils`/`HudiScanNode`/`IcebergHMSSource`/`HMSAnalysisTask`/`StatisticsUtil.getIcebergColumnStats` 等全 retype 到 generic——见文末背景段）。
> - **硬门 = 跑 ACID 集成测试套件**（R-002 项目最大风险，含 INC-5 注留为 cutover 门的 insert-only 标记门禁的 `planScan` 级回归——需 live 读/写路径 + txn + FS 管线，**勿静默跳过**，Rule 12）。
> - **范围内**：full-ACID **读** + insert-only **读**（INC-5 已落地）；full-ACID **写**继续硬拒（D7）。

> **③ PR** —— cutover 完成后开 PR（base = `branch-catalog-spi`，squash），引用 tracking issue apache/doris#65185。

## 开场要点（承接）

1. **起步先读**本文顶部 🎯 段 + 设计文档 §6/§8 + P5/P6 翻闸样板。cutover 是**新原子批**，勿与已提交的 FINAL 混淆。
2. **INC-1..INC-5 已全部提交（`0b19506acfe`）+ 全绿 + 复审，勿重写/勿再复审已提交代码。** 工作树若还有零散 scratch（`.bak`/`regression-conf`/`.audit-scratch/` 等）均为历史遗留、非本线程产物，勿混入任何 commit。
3. **范围锁定（勿重议）**：翻闸 / fe-core 写链 retype / 摘 `HiveTransactionMgr` / 读侧放锁接线 / 删 legacy = cutover（P7.4/P7.5）本体。full-ACID **写**继续硬拒（D7），full-ACID + insert-only **读**在范围（INC-5，已落地）。
4. **硬门 = ACID 集成测试套件**（R-002 项目最大风险，需 live 读/写路径 → cutover 翻闸时跑，勿静默跳过——Rule 12）。
5. **纪律**：每轮完成即更新本 HANDOFF + commit（memory `handoff-discipline-per-phase`）；上下文超 30% 找干净节点交接（memory `session-handoff-at-30pct-context`）；path-whitelist add，严禁 `-A`。

---

# 📦 分支 / Commit 须知

- **工作分支 = `catalog-spi-11-hive`**（off `branch-catalog-spi` @ `8b391c7459d`）。PR base = `branch-catalog-spi`，**squash 合并**（复用 P5-T29 #64653 / P6 #64688 范式）。
- **公开 tracking issue = apache/doris#65185**（catalog-SPI 迁移 umbrella）；P7 PR 应引用它。进度按已合入 `branch-catalog-spi` PR 口径。
- **⚠️ path-whitelist `git add`，严禁 `git add -A`**（scrub `regression-test/conf/regression-conf.groovy` 明文 key + `*.bak` + scratch `.audit-scratch/`·`conf.cmy/`·`META-INF/`·`docker/...` + `plan-doc/reviews/P5-paimon-rereview3-*`；`.claude/` 是 memory、非仓内）。
- **⚠️ 本批 FINAL 原子 commit 的白名单现已跨通用层**（DEC-1，用户 2026-07-06 授权）：除 `fe-connector-hms`/`fe-connector-hive` 外，还含 `fe-connector-api`（`ConnectorWritePlanProvider`/`Connector`/`ConnectorContractValidator`）+ `fe-core`（`datasource/PluginDrivenExternalTable.java`、`nereids/.../physical/PhysicalConnectorTableSink.java` + 两个对应 test）。设计文档 §4.5"fe-core ZERO changes"就此**作废**（改为一处通用、connector-agnostic、默认关的新能力）。
- commit message：见 `git log` 范式 + 末尾 `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>` + `Claude-Session: …`。**每阶段/每条 fix = 独立 commit**；HANDOFF + 任务清单 + 设计文档 + port-map 单独 commit。

# ⚙️ 操作须知（复用）

- maven：`-f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl :<art> **-am** -DfailIfNoTests=false -Dmaven.build.cache.enabled=false`（漏 `-am`→假错 `${revision}`）。**checkstyle 别加 `-am`**：`mvn -pl :<art> checkstyle:check`。
- **⚠️ bash 工具默认 timeout 120s**：fe-connector-hive 全模块 build/test ~2min → 调 `timeout` ~580000ms。**后台 task 通知的 "exit code" 是末尾 echo 的、非 maven 的**——读 LOG 的 `BUILD SUCCESS` 行或 surefire XML（`Tests run=/Failures=/Errors=`），别信通知 exit。maven 经管道 `$?` 是管道尾的 → grep `BUILD SUCCESS`。**改代码后 commit 前务必 fresh recompile**（stale `.class` 假红）。
- **连接器测试无 Mockito**（真 recording fakes；本轮 `HiveConnectorTransactionTest` 即用手写 recording `HmsClient` fake + `FakeConnectorContext`，`HmsClient` 多数 Phase-3+ 方法 default-throw、fake 只覆盖用到的）。checkstyle **禁 static import**（用 `Assertions.assertX`）、**扫 test 源**。
- 连接器禁 import fe-core：`bash tools/check-connector-imports.sh`。**HMS `HiveVersionUtil` 命中 = 误报非违规**（memory `catalog-spi-hms-hiveversionutil-gate-false-positive`）。
- **cwd 会被 harness 重置** → 一律绝对路径。**⚠️ `/mnt/disk1` 紧**（2.0T ~82% used，360G free）；**勿用 worktree 隔离编译 agent**（复制整仓，盘不够；本轮实现 agent 即在主树工作）。

# 🧠 起步必读

1. 本文档顶部 🎯/🚀 段 + `tasks/P7.3-INC-3-portmap.md` + 设计文档 `tasks/P7.3-hive-write-txn-implementation-design.md` §6。
2. **样板**：`tasks/P5-paimon-migration.md`（翻闸+删 legacy 全流程）；`tasks/P6-iceberg-migration.md`（阶段拆分范式）。模板事务 = `IcebergConnectorTransaction`/`IcebergWriteContext`。
3. **铁律**：fe-core 不得新增 `if(hive)`/`instanceof HMSExternal*`/引擎名判别；fe-core 不解析属性（memory `catalog-spi-no-property-parsing-in-fecore`）；通用 SPI 节点 connector-agnostic（memory `catalog-spi-plugindriven-no-source-specific-code`）；插件跨边界须 pin TCCL（memory `catalog-spi-plugin-tccl-classloader-gotcha`）。
4. **memory 相关项**：`handoff-discipline-per-phase`、`clean-room-adversarial-review-pref`、`ask-user-explain-in-chinese-first`、`session-handoff-at-30pct-context`、`memory-keep-only-general-or-requested`、`doris-build-verify-gotchas`、`catalog-spi-fe-core-test-infra`、`catalog-spi-plugin-tccl-classloader-gotcha`、`catalog-spi-hms-hiveversionutil-gate-false-positive`、`catalog-spi-tracking-issue`。

---

## 背景：#64688（P6 iceberg 收官，已合入 `branch-catalog-spi`）+ P7 关键遗留

整条 catalog-SPI 主线阶段链均已合入 upstream `branch-catalog-spi`：P0 #63582 · P1 #63641 · P2 trino #64096 · P3 hudi #64143 · P4 maxcompute #64300 · P5 paimon #64446+#64653 · P3b kerberos #64655 · **P6 iceberg #64688**。#64688 把原生 iceberg 整体迁到自包含 `fe-connector-iceberg` + 翻闸 + 删 fe-core 原生 iceberg 子系统 + 属性/鉴权全归插件（用户 2026-07-05 架构裁定，memory `catalog-spi-no-property-parsing-in-fecore`）。

**⚠️ P7 必须接手的遗留**：`#64688` 删的是原生 iceberg；但 **iceberg-on-HMS**（`type=hms` 下 `DlaType.ICEBERG`）仍走 fe-core，故 fe-core `datasource/iceberg/` 还**故意保活 23 个 HMS-iceberg 支撑类**（`IcebergUtils`/`IcebergMetadataOps`/`source/IcebergScanNode`+…/`cache/`/`IcebergMvccSnapshot`/… ）。→ P7 hive 迁移把 HMS-iceberg 挪到连接器路径后，这 23 文件才能删（P7.4/P7.5 阶段四）。同理 fe-core `datasource/hudi/`、`datasource/hive/` 也在 P7 范围。**删除排序（最硬约束）**：`datasource/hive/` 删不掉，直到 `HudiUtils`/`HudiScanNode`/`IcebergHMSSource`/`HMSAnalysisTask`/`StatisticsUtil.getIcebergColumnStats` 等全 retype 到 generic。
