# P4 — MaxCompute 全路径 clean-room 对抗复审报告

> **日期**：2026-06-07 ｜ **分支**：`catalog-spi-05` (HEAD `e89ce146cee`) ｜ **复审对象**：cutover 后 MaxCompute 全路径（含 P4-T06d 6 处修复本身），对照 legacy 基线
> **方法**：`plan-doc/reviews/maxcompute-full-rereview.workflow.js`（clean-room：Phase A/B 子 agent 只读 `fe/ be/ gensrc/` 源码、禁读 plan-doc/memory；Phase C 才解禁先验做交叉核对）
> **运行**：`w4eua10d5` / 201 agents / 9.28M subagent tokens / 46min

## 0. 运行统计与裁决

| 指标 | 值 |
|---|---|
| 域 × lens（Phase A 审阅者） | 6 × 2 = 12 |
| 每 finding refute 票（Phase B） | 3（≥2 票存活 + 多数） |
| 原始 findings | 52 |
| 存活（对抗后） | 33 |
| **新缺口 newGaps** | **12（去重后 8 个独立问题）** |
| **与历史分歧 disagreements** | **8（去重后 6 个独立问题）** |
| 总裁决 | **`attention-needed`** |

**一句话结论**：返回行**结果正确**这一层站得住（descriptor/JNI/BE 线、事务生命周期、schema cache、editlog 序列化都被独立验为与 legacy 等价）。但**写入路径有 3 个 blocker 级回归**（INSERT OVERWRITE 整条被网关挡死、动态分区 INSERT 丢 local-sort、静态分区无列名 INSERT bind 失败），**读取路径的"分区裁剪已恢复"声明被证伪**（FIX-PART-GATES 只落了 FE 元数据半边，裁剪结果在 translator 被丢弃，ODPS read session 仍跨全分区），以及一批 DB 级 DDL 语义回归（DROP DB FORCE 不级联、CREATE DB IF NOT EXISTS 丢远端预检、CTAS IF-NOT-EXISTS 误写已存在表）。这些大多在写入/DDL 域，且**多为上一轮开发遗漏或被低估/搁置**。

> ⚠️ **性质说明**：本节所有 finding 均带 `file:line` 证据并通过 3 票对抗验证 + Phase C 交叉核对，置信度高；但仍是**复审结论，落地前请按指针核码**。写入域 blocker（尤其动态分区 local-sort、INSERT OVERWRITE）的真值闸仍是 **live e2e（真实 ODPS）**——CI 默认跳。

---

## A. 🆕 新缺口（newGaps）—— 开发遗漏、未在任何 plan-doc 登记

> 8 个独立问题（原始 12 条，含 lens 间重复：F3=F10、F6=F13、F42=F47、F17/F18 ⊂ F43）。按严重度降序。

### NG-1 ｜🔴 blocker ｜INSERT OVERWRITE 整条被网关挡死
- **findings**：F42、F47（fallback 域，parity+delivery 双 lens 各自独立命中）
- **位置**：`InsertOverwriteTableCommand.java:315-323`（`allowInsertOverwrite` 网关），调用点 `:143`
- **根因**：`allowInsertOverwrite()` 只对 `OlapTable / RemoteDorisExternalTable / HMSExternalTable / IcebergExternalTable / MaxComputeExternalTable` 返回 true。翻闸后表是 `PluginDrivenExternalTable`，一个都不匹配 → `run()` 在 `:143` 抛 `AnalysisException("...only support OLAP/Remote OLAP and HMS/ICEBERG table. But current table type is PLUGIN_EXTERNAL_TABLE")`。
- **cutover↔legacy**：legacy `MaxComputeExternalTable` → 网关放行 → OVERWRITE 执行；cutover → 网关拒绝，**整条命令在到达下层之前就抛错**。讽刺的是下层 `insertIntoValuesOrSelect`（`:420-440`）**已经完整接好** `UnboundConnectorTableSink` + `overwrite=true` + 静态分区 spec，只是永远到不了——典型"分发只接了一半"。
- **处置**：作为第 7 个 cutover-fix（建议名 `FIX-OVERWRITE-GATE`），给 `allowInsertOverwrite` 加 `PluginDrivenExternalTable` 分支（按 FIX-PART-GATES 决策①走 SPI 泛型类型，OVERWRITE 是否支持由下游是否产出 `UnboundConnectorTableSink` 决定）。**Batch-D 红线**：删 legacy `MaxComputeExternalTable` 分支前必须先加 PluginDriven 分支。Rule-9 测试：翻闸表 INSERT OVERWRITE 修前红（AnalysisException）、修后过网关。

### NG-2 ｜🔴 blocker ｜动态分区 INSERT 丢失强制 local-sort（"writer has been closed" 回归）
- **findings**：F17（write），并入 F43（fallback 综合）
- **位置**：`PhysicalConnectorTableSink.java:114-121`（vs legacy `PhysicalMaxComputeTableSink.java:111-155`）
- **根因**：翻闸后 sink 是泛型 `PhysicalConnectorTableSink`，其 `getRequirePhysicalProperties()` 只做 `supportsParallelWrite()? SINK_RANDOM_PARTITIONED : GATHER`。legacy `PhysicalMaxComputeTableSink` 对**动态分区写**专门返回 `DistributionSpecHiveTableSinkHashPartitioned + MustLocalSortOrderSpec`（按分区列），并注释（`:144-147`）说明：ODPS Storage API 在看到不同分区时会关闭上一个 partition writer，**未排序数据会触发 "writer has been closed"**。cutover 两者都没做（`MaxComputeDorisConnector` 无 `SUPPORTS_PARALLEL_WRITE` → 落 GATHER、且无 local-sort）。
- **cutover↔legacy**：legacy 动态分区写 = hash-partition + local-sort（每 writer 收到按分区分组的行）；cutover = GATHER 单 writer、无排序 = 单 writer 收到交错的多分区行 → BE 写失败风险。
- **处置**：**不要只翻 `SUPPORTS_PARALLEL_WRITE` 能力位**——那只给 `SINK_RANDOM_PARTITIONED`（并行 writer）但仍缺 local-sort，照样 "writer has been closed"。正解：给 `PhysicalConnectorTableSink` 引入"连接器声明所需 distribution+sort"的钩子，`MaxComputeDorisConnector` 声明动态分区需 hash+local-sort（含 static/dynamic 三分支判别，照搬 legacy `:116-128`）。Batch-D 删 `PhysicalMaxComputeTableSink` 前必须先迁此逻辑（否则唯一副本丢失）。真值闸：真实 ODPS 跨多分区动态 INSERT 断言无 "writer has been closed"。

### NG-3 ｜🔴 blocker / 🟠 major ｜静态分区无列名 INSERT 在 bind 阶段失败
- **findings**：F48（fallback，new-gap）＋ **F19（write，被 Phase C 归为 disagreement——见 DG-2，同一根因）**
- **位置**：`BindSink.java:917-943`（`bindConnectorTableSink`）
- **根因**：`sink.getColNames()` 为空时，`bindColumns = table.getBaseSchema(true)`，**未剔除静态分区列**，也从不读 `sink.getStaticPartitionKeyValues()`。MaxCompute 的 `initSchema` 把分区列也加进 base schema，于是 `INSERT INTO mc_part_tbl PARTITION(pt='x') SELECT <非分区列>` 时 `bindColumns` 含 `pt` 而 `child.getOutput()` 不含 → `:941` 列数校验抛 `"insert into cols should be corresponding to the query output"`。legacy `bindMaxComputeTableSink`（`:875-879`）显式过滤静态分区列。`bindConnectorTableSink` 是从 `bindJdbcTableSink` 克隆（JDBC 无静态分区），注释 `"Currently only JDBC catalogs use connector sink"`（`:947`）翻闸后未更新。
- **处置**：`bindConnectorTableSink` 在 colNames 为空分支剔除 `getStaticPartitionKeyValues().keySet()`，并对 `InsertUtils.java:377-389` 的 VALUES 路径加 `UnboundConnectorTableSink` 分支。Rule-9 UT：`PARTITION(p='x') SELECT 非分区列`（无列名）binds 不抛。**与 DG-2 同一根因**——Phase C 因两个审阅者查到的历史 artifact 不同而分别归类（F48 未查到 DECISION-3 承诺→new-gap；F19 查到→disagreement）。

### NG-4 ｜🟠 major ｜所有 MaxCompute 写从并行 writer 退化为单 GATHER writer
- **findings**：F18（write），并入 F43（fallback）
- **位置**：`PhysicalConnectorTableSink.java:114-121`；能力源 `Connector.java:54-55`（`MaxComputeDorisConnector` 无 `getCapabilities` override → 空集）
- **cutover↔legacy**：legacy 非分区/全静态分区写 = `SINK_RANDOM_PARTITIONED`（多并行 writer）；cutover = GATHER（单 writer 处理所有行）= 每个 MC 写的吞吐回归。
- **处置**：与 NG-2 同源、同一修复入口。最小修是声明 `SUPPORTS_PARALLEL_WRITE`，但**必须同时**带上 NG-2 的动态分区 hash+local-sort（否则动态分区反而被并行化且无序，回归更重）。或显式接受 GATHER 并登记 deviation。

### NG-5 ｜🟠 major ｜limit-split 优化忽略 session 变量、默认即触发
- **finding**：F11（read）
- **位置**：`MaxComputeScanPlanProvider.java:187-196`
- **根因**：`useLimitOpt = limit>0 && (onlyPartitionEquality || !filter.isPresent())`，**从不读** `enable_mc_limit_split_optimization`（`SessionVariable.java:2908`，默认 **false**）。于是无 WHERE 的 `SELECT ... LIMIT n` **默认**就被压成单个 n 行 offset split。
- **cutover↔legacy**：legacy（`MaxComputeScanNode.java:735-737`）三重闸：`enableMcLimitSplitOptimization`(默认 off) **且** 分区等值谓词 **且** hasLimit——**默认不开**。cutover 默认开、且丢了 session-var 闸。语义反转。
- **处置**：要么把 `enable_mc_limit_split_optimization` 透传到 `ConnectorSession` 并实现真正的 `checkOnlyPartitionEquality`（恢复三重闸、默认 OFF）；要么明确接受"默认优化无过滤 LIMIT"并写 deviation + release-note 能力收敛说明。**不可继续留在"待定"**。

### NG-6 ｜🟡 minor ｜所有列 isKey=false（DESCRIBE / information_schema 显示 Key=NO，legacy 为 YES）
- **findings**：F3、F10（read，双 lens 命中）
- **位置**：`MaxComputeConnectorMetadata.java:138-143,150-155`（5 参 `ConnectorColumn` ctor → isKey=false）；`ConnectorColumnConverter.java:65-70` 透传 `cc.isKey()`
- **cutover↔legacy**：legacy `MaxComputeExternalTable.initSchema`（`:177,189`）每列 isKey=**true**；cutover 全 false。仅元数据展示，不影响读正确性。
- **处置**：低风险首选 FIX——data+partition 两个列循环改用 6 参 `new ConnectorColumn(..., true)`（converter 已透传 isKey，2 处调用、无 SPI 变更）。或接受并登记 DV + release-note，并加 DESCRIBE/information_schema Key 列回归断言。

### NG-7 ｜🟡 minor ｜丢失 batch-mode（异步、按分区分批）split 生成
- **findings**：F6、F13（read，双 lens）
- **位置**：`PluginDrivenScanNode.java`（无 `isBatchMode/numApproximateSplits/startSplit` override，继承 `SplitGenerator` 默认）；legacy `MaxComputeScanNode.java:214-298`
- **cutover↔legacy**：legacy 对多分区表分批异步建 read session、流式喂 split；cutover 单 session 跨全分区、一次性同步枚举所有 split → 大分区表规划慢、session+split 内存大（潜在 OOM）。**与 DG-1（裁剪未透传）耦合**：只有裁剪喂进真实 selected-partition 集后 batch-by-spec 才有意义。
- **处置**：通用插件层缺口（每个 full-adopter 都继承非 batch 默认）。短期登记 DV + 大分区压测；长期给 SPI 加 batch 路径。

### NG-8 ｜🟡 minor（regression=no）｜post-commit cache-refresh 失败被吞（INSERT 报成功）
- **finding**：F15（write）
- **位置**：`PluginDrivenInsertExecutor.java:178`（override `doAfterCommit()` 用 try/catch 包 `super.doAfterCommit()` = `handleRefreshTable`，仅 log warning 后正常返回）
- **cutover↔legacy**：legacy `MCInsertExecutor` 不 override → refresh 异常传播 → INSERT 报 FAILED；cutover 吞掉 → INSERT 报 OK（cache 暂 stale）。**cutover 行为反而更安全**（数据已提交 ODPS，报失败会诱发重试/重复写），但是**可观察的行为变更**且无书面登记。
- **处置**：无需改码，但补登记 DV + release-note（行为收敛），并在 `:164-176` Javadoc 注明该理由也覆盖 connector-transaction(MC) 路径，不只 JDBC_WRITE。

---

## B. ⚖️ 与历史结论的分歧（disagreements）—— 代码与 plan-doc 的"已修/正确/可接受"声明相矛盾

> 6 个独立问题（原始 8 条，含 F1=F7、F22=F27 重复）。**这一节最关键**：每条都是"历史说已解决，代码说没有"。

### DG-1 ｜🟠 major ｜分区裁剪从未推到 ODPS read session（`requiredPartitions=emptyList`）
- **findings**：F1、F7（read，双 lens 各自独立锁定）
- **位置**：`MaxComputeScanPlanProvider.java:198-202,320`（`createReadSession(..., Collections.emptyList())`）；`PhysicalPlanTranslator.java:753-758`（路由到 `PluginDrivenScanNode.create()` 时**从不**调 `setSelectedPartitions`，对比 legacy 分支 `:797` 传 `fileScan.getSelectedPartitions()`）
- **代码事实**：FIX-PART-GATES **确实**加了 `PluginDrivenExternalTable` 的 `supportInternalPartitionPruned/getPartitionColumns/getNameToPartitionItems`（`:163-226`），Nereids `PruneFileScanPartition` **能**算出 SelectedPartitions——**但该结果在 translator 被丢弃**，`PluginDrivenScanNode`/`MaxComputeScanPlanProvider` 根本没有承接 selected-partition 的字段/参数，`planScan` 无条件传空 `requiredPartitions`。返回行因 conjunct 在 BE 重算而正确，但 **ODPS storage session 建在全分区上**。
- **历史分歧**：`P4-cutover-review-findings.md` 原本把这条记为 **READ-P3 (major) + READ-C2 (blocker)**，修复建议**两半**：①override 分区元数据 API ②`create() 透传 selectedPartitions → planScan 接 requiredPartitions(prunedSpecs)`。**FIX-PART-GATES 只落了①**——其 design 自述 `scope = fe-core only / 不涉及 fe-connector`（`:104`），却以 READ-P3 为"所解决问题"，review-rounds 宣称 `production 正确 / pruning 不变式 clean`。**②从未实现，裁剪不端到端生效，D-028"分区裁剪恢复"叙事被代码证伪。**
- **处置**：**大声 surface**。①给 `PluginDrivenScanNode` 加 SelectedPartitions 字段/setter，`PhysicalPlanTranslator:756-758` 照 legacy 调 `setSelectedPartitions`；②扩 SPI `planScan` 签名把裁剪分区集穿到 `MaxComputeScanPlanProvider`，从 `Collections.emptyList()` 改为按 prunedSpecs 建 `requiredPartitions`，补 legacy 空选短路（`MaxComputeScanNode:724-727`）。若改为接受，则**必须**改写 FIX-PART-GATES design/review-rounds 与 decisions-log，明确"只恢复了元数据可见性，read-session requiredPartitions 下推仍为已知降级"并入 deviations-log。**无论哪条，`production CLEAN / pruning 不变式 clean` 的裁决必须更正。**

### DG-2 ｜🔴 blocker ｜静态分区无列名 INSERT 在 bind 失败（DECISION-3 承诺未兑现）
- **finding**：F19（write）；**与 NG-3/F48 同根因**
- **位置**：`BindSink.java:917-943`、`InsertUtils.java:377-389`
- **历史分歧**：`P4-T05-T06-cutover-design.md §4.2`（G4/G5）称静态分区 cutover 是"legacy MC 路径的忠实泛型镜像"，**DECISION-3**（§5/风险表 `:168`）明确承诺静态分区+overwrite 绑定落地以"避免翻闸时 INSERT-OVERWRITE-PARTITION 回归"。但 G4/G5 只把 spec 带进 `UnboundConnectorTableSink` 和 `PluginDrivenInsertCommandContext.staticPartitionSpec`（给 BE write-plan），**bind 期列数剔除从未镜像**——DECISION-3 声称要防的那个回归恰恰是 live 的。全 plan-doc grep `bindConnectorTableSink` / `insert into cols should be corresponding` 零命中 = 未登记。
- **处置**：同 NG-3 修复。并更正 `P4-T05-T06-cutover-design.md` G4/G5/DECISION-3：「忠实镜像」不完整，漏了 bind 期静态分区列剔除。

### DG-3 ｜🟠 major ｜DROP DATABASE FORCE 不再级联删表
- **findings**：F22、F27（ddl，双 lens）
- **位置**：`PluginDrivenExternalCatalog.java:337-355`（`force` 形参拿到后从不使用，注释自述"级联交给连接器"）；连接器 `dropDatabase`(`:408-413`)→`schemas().delete()` 无表清理
- **cutover↔legacy**：legacy `MaxComputeMetadataOps.dropDbImpl:142-155` 在 `force=true` 时显式枚举远端表逐个 `dropTableImpl` 后才删 schema（该循环的存在本身证明 ODPS `schemas().delete()` 不自级联）；cutover 在非空 schema 上 DROP DB FORCE 退化成非 FORCE 行为（很可能直接失败/留残表）。
- **历史分歧（自相矛盾）**：T06c design（`:57,111,185`）把它框为"可接受已知边界 / 记 OQ / 不复刻级联"；但**后续对抗 review 明确推翻**——`P4-cutover-review-findings.md` DDL-P2(`:206`)/DDL-C3(`:211`) 均"✅存活 3✓/0✗ major"，`:225,232` 显式标"disagreement，不认同其'可接受'定级"。**争议从未在代码或账本解决**，仍停在 cutover-fix-design §5 `:496`"本批次外…待用户定(question)"。
- **处置**：**别把 T06c §5"记 OQ/可接受"当作已解决**。先用真实 ODPS 验 `schemas().delete` 对非空库行为；若拒删则必须补级联（`force==true` 时枚举 dropTable，或扩 SPI `dropDatabase` 带 force/cascade）。若决定不支持，**至少 fail-loud**（`force==true`+非空库抛明确错）并登记 deviation——当前静默丢 force 违反 Rule 12。

### DG-4 ｜🟠 major ｜CREATE DATABASE IF NOT EXISTS 丢远端存在性预检（`ifNotExists` 在到连接器前被硬编码成 false）
- **finding**：F26（ddl）；**注**：同一问题的 F23 被另一审阅者归为 known-degradation——分类分歧见 §D 备注
- **位置**：`PluginDrivenExternalCatalog.java:312-326`（只按 `getDbNullable` FE-cache 短路）；`MaxComputeConnectorMetadata.java:404`（硬编码 `structureHelper.createDb(odps, dbName, false)`，丢用户 `ifNotExists`）
- **cutover↔legacy**：legacy `createDbImpl:110-124` 同时查 FE-cache **和**远端 `databaseExist`，已存在+ifNotExists 时干净 no-op；cutover 对"远端已存在但 FE-cache 没有"的库执行 `CREATE DATABASE IF NOT EXISTS` 会命中 `schemas().create()` 抛 "already exists"。
- **历史分歧**：`P4-cutover-review-findings.md` DDL-C4(`:216` major,"✗否决→修")+DDL-P5(`:217` minor,"→修")已记此缺陷并开修复处方；但 P4-T06d 只排了 6 个 fix（DDL 的是 ENGINE 与 REMOTE），`cutover-fix-design.md:239` 明确"createDb/dropDb 不在本 issue 范围"，**DDL-C4 无对应 fix commit**；task-list `:12` 却称"✅全部完成(6/6)"，deviations/decisions-log 均无登记。
- **处置**：重开 DDL-C4。`createDb()` 在 `ifNotExists && getDbNullable==null` 时先做远端存在检查（`connector...databaseExists` 已暴露，无需改 SPI 签名、对 full-adopter 泛型）；补 UT（stub `databaseExists=true` → 不调 `createDatabase`、不写 editlog）。或显式登记 deviation——别留"孤儿修 verdict"。

### DG-5 ｜🟡 minor ｜CREATE TABLE 不再拒绝 AUTO_INCREMENT 列
- **finding**：F24（ddl）
- **位置**：`MaxComputeConnectorMetadata.java:417-431`（`validateColumns` 只查空/重/类型）；`CreateTableInfoToConnectorRequestConverter.java:90-92`（丢 auto-inc flag）；`ConnectorColumn` 结构上无 auto-inc 字段
- **cutover↔legacy**：legacy `MaxComputeMetadataOps.validateColumns:422-425` 显式抛 "Auto-increment columns are not supported for MaxCompute tables"；cutover 静默建表（auto-inc 被悄悄丢弃）。
- **历史分歧**：`P4-maxcompute-migration.md:117`(P4-T01) 称此丢弃是**有意接受**——理由"nereids 上游已拒"，但该前提**对 auto-inc 为假**（`ColumnDefinition.validate` 以 `isOlap=false` 调用、无 auto-inc 拒绝）。后续 DDL-P4(major,存活 3/3) 已抓到并要求"先确认 nereids 是否已拦"（从未验），停在"待用户定"。**两份历史 artifact 互相矛盾。**
- **处置**：surface 分歧，用户定夺：(a) 视为 parity 要求→给 `ConnectorColumn` 加 `isAutoInc` 字段透传并在 `validateColumns` 重新校验；或 (b) 明确接受并在 deviations-log 登记（理由如"ODPS 本就忽略/拒绝 auto-inc"），并更正 `P4-maxcompute-migration.md:117` 的假声明。聚合列那半已被非-OLAP key 列路径覆盖，无需单独修。

### DG-6 ｜🟠 major（建议从 minor 上调）｜createTable 恒返回 false → CTAS IF-NOT-EXISTS 误写已存在表
- **finding**：F33（replay 域）
- **位置**：`PluginDrivenExternalCatalog.java:264-300`（`:290` 无条件写 `OP_CREATE_TABLE`，`:299` 恒 `return false`，即便连接器在 IF NOT EXISTS 下 no-op 了已存在表 `MaxComputeConnectorMetadata.java:330-338`）
- **cutover↔legacy**：`Env.createTable` 契约（`:3746-3747`）要求表已存在时返回 true；legacy `createTableImpl:179-197` 在 existing+IF-NOT-EXISTS 返回 true，`ExternalCatalog.createTable:1063-1075` 仅 `!res` 时写 editlog。cutover 恒 false → **CTAS 链 `CreateTableCommand.java:103` `if(createTable(...)) return;` 不短路** → `CREATE TABLE IF NOT EXISTS ... AS SELECT` 对已存在表**执行 INSERT 而非跳过**。
- **历史分歧**：此处曾被 review 为 **DDL-C5**（`:213`），但**定级 minor**、处置"待定/可接受/当前不阻塞"，且**分析只覆盖 editlog 冗余**（单 FE 上无害），**CTAS 数据写入后果完全缺席**。FIX-DDL-ENGINE 重新打开 CTAS 路径（design `:215` 自承认"CTAS 同样修好"）反而把这条 return-false 暴露成真实的数据变更缺陷——而历史把它评为 minor/可接受。
- **处置**：surface 并把 DDL-C5 **从 minor 上调 major**。修：`createTable` 区分"新建 vs 已存在"——IF-NOT-EXISTS 命中时 FE 侧查 `getTableNullable`/远端存在，返回 true + 跳 editlog + 跳 `resetMetaCacheNames`（镜像 legacy）。Rule-9 测试：CTAS-IF-NOT-EXISTS 对已存在表**不**INSERT + editlog 未写。若延期，必须在 deviations-log 登记为"已知数据变更回归"（不只 editlog 备注）。

---

## C. 各域独立 parity 判定（每域一句，来自 12 份 parityAssessment 的综合）

| 域 | 独立判定 | 是否达成 legacy parity |
|---|---|---|
| **1 读取** | 返回行**结果正确**（descriptor=`MAX_COMPUTE_TABLE`+TMCTable 与 legacy 逐字一致、BE static_cast/JNI 一致、split offset/`-1` sentinel 一致、谓词类型/时区转换镜像 legacy、conjunct 始终留给 BE 重算）；但**分区裁剪未端到端生效**（DG-1）、limit-split 默认反转（NG-5）、isKey=false（NG-6）、单子表达式失败致整 filter NO_PREDICATE（F8 已登记）、CAST 下推丢行（F9 ⚠️复查证为**未登记回归**、已修 `cc32521ed99`/[D-036]）、无 batch-mode（NG-7）。 | ❌ **分区扫描效率 + 元数据保真未达**；行正确性达成。主要是**设计/wiring 缺口**（SPI scan node 无通道传 selected-partition/limit 上下文）。 |
| **2 写入** | 事务生命周期（begin/finalizeSink/doBeforeCommit 抓 `loadedRows=getUpdateCnt()`/commit/rollback）、affected-rows 来源、提交协议（TBinaryProtocol/TMCCommitData）、write-session 参数、BE writer+block-id RPC **均与 legacy 等价（BE 零 diff）**；但 planner 侧**写分发**（GATHER vs hash+local-sort/并行，NG-2/NG-4）与**静态分区 bind**（NG-3/DG-2）回归，block-count 上限硬编码 20000（F14/F20 已登记），post-commit refresh 吞异常（NG-8）。 | ❌ **写分发 + 静态分区未达**（含 blocker）；事务/数据面达成。 |
| **3 DDL** | 常规良构 case 达 parity（engine padding/一致性、local→remote 名解析、类型拒绝集、lifecycle/bucket/property、identity-only 分区、editlog 用 local 名）；jdbc/es/trino 共享路径未受波及。但 **DB 级 DDL** 与一项列校验回归：DROP DB FORCE 不级联（DG-3）、CREATE DB IF NOT EXISTS 丢远端预检（DG-4）、auto-inc 拒绝丢失（DG-5）、CTAS IF-NOT-EXISTS 误写（DG-6）。 | ⚠️ 常规 case 达成；**DB-DDL/CTAS 边界未达**。是**实现缺口**非设计缺口（SPI 形状能承载，代码没做）。 |
| **4 元数据回放** | editlog/image 序列化、replay 重建 cache、follower、GSON 三注册（catalog/db/table）compat、replay key 用 local 名——**parity，无回归**。（注：`createTable` 返回值/CTAS 语义缺陷 DG-6 挂在本域，但属 DDL 语义而非 replay 机制问题。） | ✅ 回放机制达成 parity。 |
| **5 元数据 cache** | schema cache 走 `default` engine（TTL/eviction 与 legacy `maxcompute` 条目**完全一致**）；`(PluginDrivenSchemaCacheValue)` 下转型**类型安全**（唯一生产者 `initSchema` 只产该类型，绝不会缓存裸 `SchemaCacheValue`）；cache key（NameMapping）一致；列名映射 identity。**有意分歧**：legacy 二级 partition-VALUE cache 被去除→每查询直连 ODPS 列分区（更新鲜、多一次往返、无正确性损失，F35 已登记）；row-count/stats 从 legacy 的 -1 变为真取（增强非回归）。 | ✅ schema cache 达 parity；partition-value 缓存是**有意设计变更**非交付缺口。 |
| **6 旧逻辑残留/fallback** | dispatch 面**基本干净**：legacy `instanceof MaxCompute*` / `MAX_COMPUTE` type-switch 分支翻闸后**死而存**（compat 残留，非活 fallback），PluginDriven 并行分支在 read scan/BindRelation/SHOW PARTITIONS/partitions TVF/CreateTableInfo/Alter/UnboundTableSinkCreator/BindSink/GsonUtils 三注册/CatalogFactory **均已接且先于 legacy 匹配**；`buildTableDescriptor` 无 SCHEMA_TABLE 兜底。**但写路径未达 parity**——本 lens 独立复现了 NG-1(INSERT OVERWRITE 挡死) + NG-2/NG-4(写分发 GATHER) + NG-3(静态分区 bind)，正是 domain-6"半接 dispatch"问题。 | ⚠️ 元数据/DDL/读 dispatch 达成；**写路径 dispatch 半接（blocker）**。 |

---

## D. 全部存活 findings（33）一览

> `status`：new-gap=开发遗漏未登记 ｜ disagreement=与历史"已修/可接受"矛盾 ｜ known-degradation=已登记的已知降级（仍为真，但有账可查）。`confirms` = 3 票中确认票数。

| id | 域 | sev | category | status | 标题（简） | confirms |
|---|---|---|---|---|---|---|
| F1 | read | major | regression | **disagreement** | 裁剪未推到 ODPS read session（=F7） | 3 |
| F7 | read | major | regression | **disagreement** | 同 F1（另一 lens） | 3 |
| F2 | read | minor | regression | known-degr | limit-split opt 永久禁用（`checkOnlyPartitionEquality` 恒 false） | 2 |
| F8 | read | major | regression | known-degr | 单子表达式不可转→整 filter NO_PREDICATE | 3 |
| F9 | read | major | **correctness** | ~~known-degr~~→**regression** ⚠️ | **CAST 谓词被剥壳下推 ODPS→丢行**（⚠️2026-06-08 复查 `wzoa6dkvw` 0/3 refuted 推翻「known-degr/已登记」定级：实为**未登记静默丢行回归**，legacy 丢弃 CAST 谓词故正确、cutover 推下剥壳谓词更紧。已 **Fix** `cc32521ed99` [D-036]/[DV-020]） | 3 |
| F3/F10 | read | minor | parity | **new-gap** | 所有列 isKey=false | 3/3 |
| F6/F13 | read | minor | regression/d-i-gap | **new-gap** | 丢失 batch-mode 异步 split | 3/3 |
| F11 | read | major | regression | **new-gap** | limit-split 忽略 session-var、默认触发 | 3 |
| F12 | read | minor | design-impl-gap | known-degr | `checkOnlyPartitionEquality` stub 恒 false | 2 |
| F14/F20 | write | major/minor | parity/regression | known-degr | block-id 上限硬编码 20000（非 Config） | 3/3 |
| F15 | write | minor | fallback | **new-gap** | post-commit refresh 吞异常（report OK） | 3 |
| F21 | write | minor | regression | known-degr | 同 F15（refresh 吞异常，另一 lens） | 3 |
| F17 | write | **blocker** | regression | **new-gap** | 动态分区 INSERT 丢 local-sort | 3 |
| F18 | write | major | regression | **new-gap** | 写退化为单 GATHER writer | 3 |
| F19 | write | **blocker** | regression | **disagreement** | 静态分区无列名 INSERT bind 失败 | 3 |
| F22/F27 | ddl | major | regression | **disagreement** | DROP DB FORCE 不级联 | 3/3 |
| F23 | ddl | major | regression | known-degr | CREATE DB IF NOT EXISTS 丢远端预检（≈F26，分类分歧） | 3 |
| F26 | ddl | major | regression | **disagreement** | 同上，归为分歧（评 "6/6完成/修" 矛盾） | 3 |
| F24 | ddl | minor | regression | **disagreement** | 不再拒 AUTO_INCREMENT | 3 |
| F25/F28 | ddl | nit/minor | regression/replay | known-degr | IF NOT EXISTS 已存在表仍写 editlog | 3/3 |
| F31 | ddl | minor | parity | known-degr | 丢防御性 auto-inc/agg 列拒绝 | 3 |
| F33 | replay | major | regression | **disagreement** | createTable 恒 false→CTAS 误写已存在表 | 3 |
| F34 | replay | minor | design-impl-gap | known-degr | createDb IF-NOT-EXISTS 仅查 FE-cache + dropDb 丢 force | 2 |
| F35 | cache | minor | cache | known-degr | 去 legacy 二级 partition-value cache（每查询直连） | 3 |
| F42/F47 | fallback | blocker/major | regression | **new-gap** | INSERT OVERWRITE 被网关挡死 | 3/3 |
| F43 | fallback | major | regression | **new-gap** | 写分发 fallback 到 GATHER（综合 F17+F18） | 3 |
| F48 | fallback | major | design-impl-gap | **new-gap** | 静态分区 INSERT bind 忽略静态分区列（=F19 根因） | 3 |

---

## E. 元观察 / 注意事项 / 后续

1. **分类分歧本身是模糊的**：同一根因被两个审阅者按各自查到的历史 artifact 分别归 new-gap / disagreement / known-degradation（如 CREATE DB 预检 F23 known-degr vs F26 disagreement；静态分区 bind F48 new-gap vs F19 disagreement）。**建议把 newGaps∪disagreements 的并集统一当"必须 triage"处理**，不要被 status 标签的细分误导。
2. **写路径是这轮的重灾区，且大量被上一轮遗漏/低估**：3 个写 blocker（INSERT OVERWRITE、动态分区 local-sort、静态分区 bind）+ 写并行退化，集中暴露"通用 `PhysicalConnectorTableSink`/`bindConnectorTableSink` 是从 JDBC 语义克隆、未承接 MaxCompute 分区语义"。fallback lens 的"半接 dispatch"问题独立复现了它们——交叉验证有效。
3. **`FIX-PART-GATES` 的"分区裁剪恢复 / pruning 不变式 clean"是本轮最明确的证伪**（DG-1）：只落 FE 元数据半边，裁剪集在 translator 丢弃。建议优先更正该 design/review-rounds/decisions-log 的措辞。
4. **Batch-D 红线扩充**：删 legacy 前，至少 `PhysicalMaxComputeTableSink`（NG-2/NG-4 唯一逻辑副本）、`MaxComputeExternalTable` allowInsertOverwrite 分支（NG-1）、legacy `bindMaxComputeTableSink` 静态分区过滤（NG-3）必须先在 PluginDriven/connector 路径补齐，否则删除即永久丢失。
5. **一项数据质量瑕疵**：`write/parity` 的 `parity_assessment` 文本尾部混入了 `</parameter></invoke>` 工具调用残片（某子 agent 输出泄漏），不影响结论实质，已在本报告中清理引用。
6. **真值闸未变**：写路径 blocker（动态/静态分区、OVERWRITE）的最终确认仍需 **live e2e（真实 ODPS，CI 默认跳）**——本复审是静态代码层面的高置信判定，不替代 e2e。
7. **建议 triage 顺序**：3 个写 blocker（NG-1/NG-2/NG-3 = DG-2）→ DG-1 裁剪透传 → DG-3/DG-4/DG-6 DB-DDL/CTAS → NG-4/NG-5 写并行+limit 默认 → NG-6~8 与剩余 minor。

> **来源**：workflow `w4eua10d5` 结构化输出（`parityAssessments`/`newGaps`/`disagreements`/`confirmed`）。原始 JSON 见 `/tmp/.../tasks/w4eua10d5.output`；脚本 `plan-doc/reviews/maxcompute-full-rereview.workflow.js`。
