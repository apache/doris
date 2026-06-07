# P4 复审发现修复 Task List（re-review round）

> 来源：`plan-doc/reviews/P4-maxcompute-full-rereview-2026-06-07.md`（8 newGaps ∪ 6 disagreements，verdict `attention-needed`）。
> 前置：P4-T06d 6 fix 已 DONE（见 `plan-doc/task-list.md`）。本轮处理复审**新**发现。
> 流程（用户定）：每 issue = 独立设计文档 → 修复 → 编译+UT(无 e2e) → 对抗 review agent → review 有问题则回设计循环（最多 5 轮）→ 记录每轮结论防跨轮矛盾 → 独立 commit + summary + 更新本表。
> 每 issue 产物：
>   - 设计：`plan-doc/tasks/designs/P4-T06e-<FIX>-design.md`（跨轮更新）
>   - review 轮次记录：`plan-doc/reviews/P4-T06e-<FIX>-review-rounds.md`（每轮 finding+verdict+处置）
>   - summary：写回本文件「review 轮次累计结论」+ 设计文档尾

## ▶ RESUME（fresh session 从这里接）

- **当前**：**P0-2 FIX-WRITE-DISTRIBUTION ✅ DONE**（1 轮收敛 0 must-fix，commit 见下方进度表）。**下一步 = P0-3 FIX-BIND-STATIC-PARTITION**（静态分区无列名 INSERT bind 失败，blocker；**与本 P0-2 耦合**——P0-3 落后 all-static 分支才端到端可达，且 Batch-D 删 legacy `PhysicalMaxComputeTableSink`/`bindMaxComputeTableSink` 须待 P0-2+P0-3 双落）。建议在此 `/clear` 开新 session（本 issue 全部状态已落盘）。
  - 已完成：P0-1 FIX-OVERWRITE-GATE（2 轮，`59699a62f33`）、P0-2 FIX-WRITE-DISTRIBUTION（1 轮，hash 见进度表）。
- 动手前按指针核码（Rule 8）。triage 顺序 = 3 写 blocker → DG-1 裁剪透传 → DB-DDL/CTAS → 写并行+limit 默认 → minors（报告 §E.7）。
- **operational**（来自 HANDOFF / auto-memory）：maven 必绝对 `-f` + `-pl`（改 fe-core 带 `:fe-core -am`，改连接器带 `:fe-connector-maxcompute`）；带 `-Dmaven.build.cache.enabled=false`；读真实 `Tests run:`/`BUILD`/`MVN_EXIT`，**勿信**后台 task 通知 exit code；checkstyle `-pl :fe-core checkstyle:check`；import-gate `bash tools/check-connector-imports.sh`。分支 `catalog-spi-05`，本地不 push，每 issue 独立 commit（msg 用 `[P4-T06e] ...`）。
- **clean-room 对抗 review 偏好**：多 agent 对抗 + 先 code 独立判断、后交叉核对历史结论（auto-memory `clean-room-adversarial-review-pref`）。

## 进度

| # | issue | sev | layer | 决策类型 | 设计 | 实现 | 编译+UT | review 轮次 | 状态 |
|---|---|---|---|---|---|---|---|---|---|
| P0-1 | FIX-OVERWRITE-GATE      | blocker | fe-core+connector(SPI cap) | 明确修复 | ✅ | ✅ | ✅ | 2 轮→收敛 | ✅ DONE (`59699a62f33`) |
| P0-2 | FIX-WRITE-DISTRIBUTION  | blocker+major | fe-core+connector(SPI cap) | 明确修复 | ✅ | ✅ | ✅ | 1 轮→收敛(0 must-fix) | ✅ DONE (`<HASH>`) |
| P0-3 | FIX-BIND-STATIC-PARTITION | blocker | fe-core | 明确修复 | ⬜ | ⬜ | ⬜ | — | ⬜ |
| P1-4 | FIX-PRUNE-PUSHDOWN      | major | fe-core+connector(SPI) | 修复或重分类+DV | ⬜ | ⬜ | ⬜ | — | ⬜ |
| P2-5 | FIX-DROP-DB-FORCE       | major | fe-core(+connector?) | 需 live ODPS / fail-loud+DV | ⬜ | ⬜ | ⬜ | — | ⬜ |
| P2-6 | FIX-CREATE-DB-PRECHECK  | major | fe-core | 明确修复 | ⬜ | ⬜ | ⬜ | — | ⬜ |
| P2-7 | FIX-CTAS-IF-NOT-EXISTS  | major | fe-core | 明确修复 | ⬜ | ⬜ | ⬜ | — | ⬜ |
| P2-8 | FIX-AUTOINC-REJECT      | minor | connector(SPI?) | 修复或 DV | ⬜ | ⬜ | ⬜ | — | ⬜ |
| P3-9 | FIX-LIMIT-SPLIT-DEFAULT | major | connector | 修复或 DV | ⬜ | ⬜ | ⬜ | — | ⬜ |
| P3-10 | FIX-ISKEY-METADATA     | minor | connector | 修复或 DV | ⬜ | ⬜ | ⬜ | — | ⬜ |
| P3-11 | FIX-BATCH-MODE-SPLIT   | minor | connector(SPI) | DV（与 P1-4 耦合） | ⬜ | ⬜ | ⬜ | — | ⬜ |
| P3-12 | FIX-POSTCOMMIT-REFRESH | minor | fe-core | 无码改，DV+javadoc | ⬜ | ⬜ | ⬜ | — | ⬜ |

图例：⬜ 未开始 / 🔄 进行中 / ✅ 完成

## 横切（全程守 / 别忘）

- 🔴 **Batch-D 红线扩充**：删 legacy 前须先在 PluginDriven/connector 路径补齐 → `PhysicalMaxComputeTableSink`（写分发唯一副本，P0-2）、`allowInsertOverwrite` 的 MC 分支（P0-1）、`bindMaxComputeTableSink` 静态分区过滤（P0-3）。复查 Batch-D 设计「zero survivor」声明。
- 🟡 **F9 CAST 谓词剥壳下推 ODPS → 可能丢行**（correctness, confirms 3/3，`ExprToConnectorExpressionConverter.java:108-109`）：虽归「已登记降级」，属正确性/丢行风险，二次确认是否真安全/真已登记。
- 📝 **doc-sync**：修复同时更正各 design/decisions-log/deviations-log 措辞（尤其 DG-1 证伪 FIX-PART-GATES「pruning 不变式 clean」、DG-2 证伪 DECISION-3「忠实镜像」、DG-4/DG-6 task-list「6/6 完成」）。把本轮结论登记进 decisions-log/deviations-log。

## review 轮次累计结论（防跨轮矛盾，精简索引；详见各 issue round 文件）

- **P0-1 FIX-OVERWRITE-GATE（2 轮收敛，commit `59699a62f33`）**: `allowInsertOverwrite` 网关接 PluginDriven，但**经新 SPI capability `supportsInsertOverwrite()` 守门**（非 round-1 的 bare instanceof）。改 3 模块：`ConnectorWriteOps` 加 `default supportsInsertOverwrite()=false`；`MaxComputeConnectorMetadata` override true；fe-core 网关 `instanceof PluginDrivenExternalTable && pluginConnectorSupportsInsertOverwrite(...)`（helper 经 catalog→connector→metadata 链查能力，镜像 PhysicalPlanTranslator）+ 拒绝消息更正。**Round 1**(needs-revision): 对抗 review 证伪设计的 bare-instanceof deferral —— jdbc（`supportsInsert=true` 但 `getWriteConfig` 不透传 overwrite）被网关纳入后**静默退化 overwrite→plain INSERT 丢数据**（Rule 12）；es/trino（`supportsInsert=false`）被纳入后下游泛化报错。**用户决策=Option A（SPI capability）**。**Round 2**(rawFindings=0 收敛): 4 项 round-1 finding 全关闭，testVacuousRisk=false，contradictsHistory=false。UT 3/3、mutation 还原 bare instanceof 唯回归守门 test (b) 红。⚠️登记: jdbc/es/trino overwrite 现于网关 fail-loud（= legacy 产品行为，从不在 allow-list）；pre-existing JDBC getWriteConfig overwrite gap 留另开 ticket（现不可达）；新增 SPI 方法默认 false → 现有连接器零行为变更。**Batch-D 红线**: 删 legacy `MaxComputeExternalTable` arm（`InsertOverwriteTableCommand`）须排在本 commit 之后（本 fix 已加 PluginDriven arm）。**doc-sync WIP（未随本 commit）**: HANDOFF :26 round-1 描述更正、decisions-log 登记新 capability+Option A。详见 `plan-doc/reviews/P4-T06e-FIX-OVERWRITE-GATE-review-rounds.md`。

- **P0-2 FIX-WRITE-DISTRIBUTION（1 轮收敛 0 must-fix，commit `<HASH>`）**: 翻闸后 MaxCompute 写走通用 `PhysicalConnectorTableSink`,丢 legacy 动态分区 hash+local-sort（"writer has been closed"）+ 并行写退化 GATHER（NG-2/NG-4）。**改 4 文件**:① `ConnectorCapability` 加 `SINK_REQUIRE_PARTITION_LOCAL_SORT`;② `MaxComputeDorisConnector.getCapabilities()` 声明 `{SUPPORTS_PARALLEL_WRITE, SINK_REQUIRE_PARTITION_LOCAL_SORT}`（此前无 override=空集→GATHER）;③ `PluginDrivenExternalTable.requirePartitionLocalSortOnWrite()`（镜像 `supportsParallelWrite`）;④ `getRequirePhysicalProperties()` 重写 legacy 3 分支。**关键修正 vs legacy**:分区列→child output 索引按 **cols 位置**（通用 sink child 投影到 cols 序）非 legacy full-schema 位置。**blast radius**:两能力仅 2+1 reader,唯一另一 `getCapabilities` consumer（`QueryTableValueFunction` 查 `SUPPORTS_PASSTHROUGH_QUERY`）MaxCompute 不声明→不受影响。编译 3 模块绿、checkstyle/import-gate 净、UT 4/4、mutation 唯 T1 红、blast-radius 回归 92/92（含 `RequestPropertyDeriverTest`14/`ShuffleKeyPrunerTest`11）。**Round 1**(`ww1g95bba`,29 agents): rawFindings=8→survived=3→**newGaps=0/disagreements=0/mustFix=0**,3 存活全 `known-degradation`+`matchesDesignIntent=true`（F2/F4=NG-3/P0-3 耦合本设计已登记;F5=T2 reachability,已澄清 javadoc）。ShuffleKeyPruner non-strict 分歧 Phase B 即退（确认更保守无正确性损）。**Batch-D 红线**:删 `PhysicalMaxComputeTableSink`/`bindMaxComputeTableSink` 须待 P0-2+P0-3 双落。**doc-sync WIP（未随本 commit）**: decisions-log 登记新 capability `SINK_REQUIRE_PARTITION_LOCAL_SORT`+MaxCompute 能力集;deviations-log 登记 ShuffleKeyPruner non-strict 少剪 + `enable_strict_consistency_dml=false` 丢 local-sort（legacy parity,非回归）。详见 `plan-doc/reviews/P4-T06e-FIX-WRITE-DISTRIBUTION-review-rounds.md`。
