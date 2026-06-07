# 🤝 Session Handoff

> 滚动文档：每次 session 结束覆盖更新；历史见 `git log plan-doc/HANDOFF.md`。
> 协作规范：[AGENT-PLAYBOOK.md](./AGENT-PLAYBOOK.md)

---

## 📅 最后一次 handoff

- **日期**：2026-06-07（第 7 次 handoff）
- **本 session 主题**：复审发现的 **3 个 P0 写路径 blocker 全清**（P0-1/P0-2 前序 session + 本 session 完成 P0-3）。live tracker = `plan-doc/task-list-P4-rereview.md`。
  - **P0-1 FIX-OVERWRITE-GATE** ✅ `59699a62f33`（2 轮）— 新 SPI cap `supportsInsertOverwrite()` 守门。
  - **P0-2 FIX-WRITE-DISTRIBUTION** ✅ `f0adedba20c`（1 轮）— 新 cap `SINK_REQUIRE_PARTITION_LOCAL_SORT`+`SUPPORTS_PARALLEL_WRITE`，重写 legacy 3 分支。
  - **P0-3 FIX-BIND-STATIC-PARTITION** ✅ `7cc86c66440`（本 session，3 轮收敛 0 mustFix）— **判别键三轮迭代 static→partitioned→capability**：新 cap `SINK_REQUIRE_FULL_SCHEMA_ORDER`（MaxCompute BE 按位置写 full-schema），`bindConnectorTableSink` 据此投影 = legacy `bindMaxComputeTableSink` 全写形 parity；并回退 P0-2 分布索引 cols→full-schema（[D-030] 回退 [D-029]）。详见 `plan-doc/reviews/P4-T06e-FIX-BIND-STATIC-PARTITION-review-rounds.md`。
- **分支**：`catalog-spi-05`（本地,未 push）。本 session 4 commit（P0-3 fix `7cc86c66440` + task-list 回填 `e38cb635446` + 本次 HANDOFF/doc-sync commit）。
- **doc-sync 已落**：[D-030]/[DV-014] 登记、cutover-design §4.2 + FIX-WRITE-DISTRIBUTION-design「index-by-cols」superseded 更正、本 HANDOFF——**随本 session commit（不再 WIP）**。
- **复审已验层（legacy parity 达成）**：返回行结果正确、descriptor/JNI/BE 线、事务生命周期、schema cache（含 downcast 安全）、editlog/replay——均独立验为与 legacy 等价。**剩余问题集中在读裁剪下推 / DB-DDL / minors（见下 P1+）。**
- **状态详情**：见报告 §A(newGaps) / §B(disagreements) / §C(各域 parity 判定) / §D(33 存活 finding 表) / §E(元观察+triage 顺序)。

---

# 🎯 下一 session = P1 起（P0 三写 blocker 已全清，按优先级续）

> 来源全部出自 `plan-doc/reviews/P4-maxcompute-full-rereview-2026-06-07.md`（每条带 `file:line` + cutover↔legacy diff + 处置建议 + 历史交叉核对证据）。下面是浓缩可执行清单——**动手前按指针核码（Rule 8）**。
> **⚠️ 把 newGaps∪disagreements 当一个"必须 triage"集**：同一根因被两个审阅者按各自查到的历史 artifact 分别归 new-gap / disagreement（静态分区 bind F19=F48；CREATE DB 预检 F23=F26），别被 status 标签的细分误导。
> **每 issue 走既有流程**：设计→改→编译+UT+mutation→对抗 review 收敛→独立 commit + hash 回填。

## 🔴 P0 — 写路径 3 个 blocker（✅ 全清，2026-06-07）

- [x] **FIX-OVERWRITE-GATE**（blocker, F42/F47）✅ **DONE @`59699a62f33`**（本轮 live tracker = `plan-doc/task-list-P4-rereview.md`；详见 `plan-doc/reviews/P4-T06e-FIX-OVERWRITE-GATE-review-rounds.md`）。⚠️**下面这句已过时**：实际未用 bare instanceof（round-1 对抗 review 证伪——会令 jdbc 静默退化 overwrite→plain INSERT 丢数据），改为 **Option A：新增 SPI capability `supportsInsertOverwrite()`（ConnectorWriteOps 默认 false / MaxCompute=true），网关经能力守门**。〔原始计划：〕`InsertOverwriteTableCommand.allowInsertOverwrite:315-323` 加 `PluginDrivenExternalTable` 分支（keyed on SPI 泛型类型，对齐 FIX-PART-GATES 决策①）。下层 OVERWRITE 机器(`:420-440`)已完整接好、只是被顶层网关挡得到不了（典型"分发只接一半"）。**Batch-D 红线**：删 legacy `MaxComputeExternalTable` 分支前必须先加 PluginDriven 分支。测试(Rule 9)：翻闸表 INSERT OVERWRITE 修前红(`AnalysisException "...only support OLAP..."`)、修后过网关 + 静态分区 spec 仍流。
- [x] **FIX-WRITE-DISTRIBUTION**（blocker+major, F17/F18/F43）✅ **DONE @`f0adedba20c`**（1 轮收敛 0 must-fix；详见 `plan-doc/reviews/P4-T06e-FIX-WRITE-DISTRIBUTION-review-rounds.md`、[D-029]/[DV-013]）。做法 = **Option A：新增 SPI capability `SINK_REQUIRE_PARTITION_LOCAL_SORT`**（`ConnectorCapability` 默认不声明 / MaxCompute `getCapabilities()` 声明它 + `SUPPORTS_PARALLEL_WRITE`），`PluginDrivenExternalTable.requirePartitionLocalSortOnWrite()` 读之，`getRequirePhysicalProperties()` 重写 legacy 3 分支。**关键修正 vs legacy**：分区列→child output 索引按 **cols 位置**（通用 sink child 投影到 cols 序）非 legacy full-schema。〔原始计划：〕`PhysicalConnectorTableSink.getRequirePhysicalProperties:114-121` 照搬 legacy `PhysicalMaxComputeTableSink:111-155` 三分支。**⚠️ 不只翻 `SUPPORTS_PARALLEL_WRITE`**——那缺 local-sort，动态分区照样 "writer has been closed"。**Batch-D 红线**：删 `PhysicalMaxComputeTableSink`（唯一逻辑副本）须待本 fix + P0-3 双落。**真值闸**：live e2e 跨多动态分区无 "writer has been closed" + 并行吞吐（CI 跳，须与 P0-3 一并 live 验）。
- [x] **FIX-BIND-STATIC-PARTITION**（blocker, F19/F48）✅ **DONE @`7cc86c66440`**（3 轮收敛 0 mustFix；[D-030]/[DV-014]；详见 `plan-doc/reviews/P4-T06e-FIX-BIND-STATIC-PARTITION-review-rounds.md`）。⚠️**下面原始计划不完整**——只剔除静态分区列不够：MaxCompute BE/JNI writer **按位置**映射数据到完整表 schema，故**所有** MC 写（不止静态/分区）须投影 full-schema 序（非分区/重排或部分显式列名否则静默错列/丢列）。实际做法 = **新增 SPI cap `SINK_REQUIRE_FULL_SCHEMA_ORDER`**（MaxCompute 声明 / JDBC 不声明），`bindConnectorTableSink` 据此分支（true→full-schema 投影镜像 legacy `bindMaxComputeTableSink` 全写形 + 剔除静态分区列;false→cols 序 JDBC/ES）+ `InsertUtils` VALUES 分支 + **回退 P0-2 分布索引 cols→full-schema**（[D-030] 回退 [D-029]）。判别键三轮 static→partitioned→capability。〔原始计划：`BindSink.bindConnectorTableSink` 剔除 `getStaticPartitionKeyValues().keySet()` + `InsertUtils:377-389` VALUES 分支〕。**doc-sync 已落**：cutover-design §4.2 + FIX-WRITE-DISTRIBUTION-design「index-by-cols」superseded 更正（随本 session commit）。**Batch-D 红线**：删 legacy `bindMaxComputeTableSink`/`PhysicalMaxComputeTableSink` 须待本 fix 落（已落）。**真值闸**：live e2e（p2 `test_mc_write_insert` Test 3/3b + `test_mc_write_static_partitions`）；bind 投影无 fe-core analyze harness 单测 = DV-014。

## 🟠 P1 — 分区裁剪下推证伪（disagreement, major）

- [ ] **FIX-PRUNE-PUSHDOWN**（F1/F7）：FIX-PART-GATES 只落了 FE 元数据半边，裁剪集在 translator 被丢、ODPS read session 仍跨全分区。(a) `PluginDrivenScanNode` 加 SelectedPartitions 字段/setter，`PhysicalPlanTranslator:756-758` 调 `setSelectedPartitions(fileScan.getSelectedPartitions())`（照 legacy `:797`）；(b) 扩 SPI `planScan` 把裁剪分区集穿到 `MaxComputeScanPlanProvider:198-202,320`，由 prunedSpecs 建 `requiredPartitions`（替 `Collections.emptyList()`），补 legacy 空选短路(`MaxComputeScanNode:724-727`)。**或** 接受+重分类：更正 FIX-PART-GATES design/review-rounds/decisions-log 措辞（"只恢复元数据可见性, read-session requiredPartitions 下推仍 open"）+ 登记 deviations-log。**无论哪条，"production CLEAN / pruning 不变式 clean" 裁决必须更正。**

## 🟠 P2 — DB-DDL / CTAS 语义回归

- [ ] **DROP DB FORCE 级联**（disagreement major, F22/F27）：先用真实 ODPS 验 `schemas().delete` 对非空库行为。若拒删 → 在 `PluginDrivenExternalCatalog.dropDb:337-355` 的 `force==true` 时枚举+dropTable（或扩 SPI 带 force/cascade）。若不支持 → 至少 fail-loud（force+非空库抛明确错）+ 登记 deviation。**别把 T06c §5"记 OQ/可接受"当作已解决**（后续对抗 review 已推翻该定级）。
- [ ] **CREATE DB IF NOT EXISTS 远端预检**（disagreement major, F26/F23）：重开 DDL-C4。`createDb:312-326` 在 `ifNotExists && getDbNullable==null` 时先查 `connector...databaseExists`（已暴露、无需改 SPI 签名）。UT + mutation。或登记 deviation——别留"孤儿修 verdict"（task-list `:12` 称 6/6 完成但此条无 fix commit、亦无 deviation）。
- [ ] **CTAS IF-NOT-EXISTS 误写已存在表**（disagreement, DDL-C5 minor→**major**, F33）：`createTable:264-300` 区分"新建 vs 已存在"——IF-NOT-EXISTS 命中 → 返回 true + 跳 editlog + 跳 `resetMetaCacheNames`（镜像 legacy `createTableImpl:179-197` → `ExternalCatalog:1063-1075`）。测试：CTAS-IF-NOT-EXISTS 对已存在表**不**INSERT + editlog 未写。（历史只分析了 editlog 冗余那半、漏了数据变更后果。）
- [ ] **AUTO_INCREMENT 拒绝丢失**（disagreement minor, F24）：定夺 (a) `ConnectorColumn` 加 `isAutoInc` 透传 + `validateColumns` 重校验；或 (b) 接受+登记 deviation + 更正 `P4-maxcompute-migration.md:117` 的假声明（"nereids 上游已拒"对 auto-inc 为假）。聚合列那半已被非-OLAP key 路径覆盖、无需单独修。

## 🟡 P3 — 写并行 / 读默认 / minors

- [ ] **limit-split 默认反转**（major, F11）：`MaxComputeScanPlanProvider:187-196` 把 `enable_mc_limit_split_optimization`(默认 false) 透传 ConnectorSession + 实现真正的 `checkOnlyPartitionEquality`（恢复三重闸、默认 OFF）；或接受"默认优化无过滤 LIMIT"+ DV + release-note 能力收敛说明。
- [ ] **isKey=false 元数据分歧**（minor, F3/F10）：`MaxComputeConnectorMetadata.java:138-143,150-155` 两个列循环改 6 参 `ConnectorColumn(...,true)`（2 处、converter 已透传、无 SPI 变更）；或接受+DV。加 DESCRIBE/information_schema Key 列回归断言。
- [ ] **丢 batch-mode 异步 split**（minor, F6/F13）：通用插件层缺口（每 full-adopter 继承非-batch 默认）。登记 DV + 大分区压测；或给 SPI 加 batch 路径（**与 FIX-PRUNE-PUSHDOWN 耦合**——裁剪喂进真实 selected-partition 后 batch-by-spec 才有意义）。
- [ ] **post-commit refresh 吞异常**（minor, regression=no, F15）：无需改码（cutover 行为反而更安全）；登记 DV + 在 `PluginDrivenInsertExecutor:164-176` Javadoc 注明理由覆盖 connector-transaction(MC) 路径，不只 JDBC_WRITE。

## ⛓️ 横切 / 别忘

- [ ] **Batch-D 红线扩充**：删 legacy 前须先在 PluginDriven/connector 路径补齐 → `PhysicalMaxComputeTableSink`(写分发唯一副本)、`allowInsertOverwrite` 的 MC 分支、`bindMaxComputeTableSink` 静态分区过滤。复查 Batch-D 设计对这些文件的"zero survivor"声明（连同既有 `PartitionsTableValuedFunction` 红线）。
- [ ] **复查一条 known-degradation**：F9 `CAST 谓词被剥壳下推 ODPS → 可能丢行`（category=**correctness**, `ExprToConnectorExpressionConverter.java:108-109`, confirms 3/3）。虽被 Phase C 归"已登记降级"，但属正确性/丢行风险，建议二次确认是否真安全/真已登记。
- [~] **doc-sync**：P0-1/P0-2/P0-3 部分**已落并 commit**（decisions-log D-027..D-030、deviations-log DV-013/DV-014、cutover-design §4.2、FIX-WRITE-DISTRIBUTION-design index-by-cols superseded、本 HANDOFF、task-list）。**剩余（随 P1-4 处理）**：更正 FIX-PART-GATES「pruning 不变式 clean」措辞（DG-1，见 P1）、其余 P1+ 各项落地时同步 design/log。

---

## ⚙️ 操作须知(无结论,纯工程)
- **maven 必绝对 `-f` + `-pl :artifactId`**:改 fe-core 带 `:fe-core -am`;改连接器带 `:fe-connector-maxcompute`。读真实 `BUILD SUCCESS/FAILURE` 与尾部 `echo "MVN_EXIT=$?"`;**勿信**后台 task-notification 的 exit code。
- **build cache 坑**:守门/跑测带 `-Dmaven.build.cache.enabled=false`,否则会 restore 旧 build 且 **surefire XML 可能 stale**(前序 session 多次踩到:mutation 跑出 BUILD FAILURE 但读到旧 XML 显示 0 fail)。直接读 mvn 输出的 `Tests run:` 行,别只读 XML。
- **checkstyle**:`-pl :fe-core checkstyle:check`;`CustomImportOrder`(doris→第三方[com.*/org.* 非 doris]→java)/`UnusedImports`/`LineLength 120`;扫 test 源。
- **import-gate**:`bash tools/check-connector-imports.sh`(repo 根跑)。
- **分支**:`catalog-spi-05`,本地;未跟踪 `.audit-scratch/` `conf.cmy/` `regression-conf.groovy.bak`(勿提交)。
- **mutation 验证技巧**:改产线一处→跑相关 UT→确认对应 test 变红→还原。用 `cp` 备份产线文件做 mutation(比 perl 删块安全——perl 易匹配到首个同名 `if` 误删方法)。

## 🧠 给下一个 agent 的 meta
- **live e2e(真实 ODPS)仍是翻闸真正完成门**——本复审是静态代码层面的高置信判定,**不替代 e2e**;写路径 blocker(动态/静态分区 / INSERT OVERWRITE)最终须 live 验。runbook 见 `git show` 历史 HANDOFF 或 decisions-log。
- 复审脚本可复用:`plan-doc/reviews/maxcompute-full-rereview.workflow.js`(clean-room 编排,Phase A/B 只读码、Phase C 解禁先验;args 可调 `verifyVotes/lensesPerDomain/includeBe`)。clean-room 偏好见 auto-memory `clean-room-adversarial-review-pref`。
- 先验/历史交叉核对账(P4-T06d designs/reviews、cutover-fix-design、decisions/deviations-log、task-list)即将随上述修复更新——改前先读对应条目(Rule 8)。
