# 决策日志（ADR）

> **Append-only**：新决策置顶；旧决策永不删除（即使被推翻，也只标"已废止"而不删除）。
> 编号规则：`D-NNN` 三位数字，从 001 起单调递增，永不复用。
> 历史决策 D1-D12（master plan §5）+ U1-U6（RFC §16.2）已迁入并映射到 D-001..D-018。
> 与"偏差"的区别见 [README §3.1](./README.md)。
>
> 每条决策模板见文末 §附录。

---

## 📋 索引

> 时间倒序；带 ✅ 表示生效中，❌ 表示已废止，🟡 表示待评审

| 编号 | 别名 | 简述 | 日期 | 状态 |
|---|---|---|---|---|
| D-030 | — | **P4-T06e FIX-BIND-STATIC-PARTITION 新增 SPI capability `SINK_REQUIRE_FULL_SCHEMA_ORDER` + 回退 D-029 的 cols 位置索引为 full-schema 索引（用户批准扩 scope）**：翻闸后 MaxCompute 写走通用 `bindConnectorTableSink`,该路径克隆自 JDBC（按名 cols 序投影）,而 MaxCompute BE/JNI writer **按位置**映射数据到完整表 schema → 静态分区无列名 INSERT bind 抛、重排/部分显式列名静默错列。修正 = 镜像 legacy `bindMaxComputeTableSink`：对**按位置写**的连接器（声明新 capability `SINK_REQUIRE_FULL_SCHEMA_ORDER`,MaxCompute 声明、JDBC/ES 不声明）恒投影到 full-schema 序(填 NULL/默认);JDBC 维持 cols 序。**并回退 D-029**：分布索引 cols→full-schema（否则 partial-static/重排错列）。判别键三轮收敛 static→partitioned→capability。clean-room 3 轮收敛 0 mustFix（`wi3mnjymb`/`wy299gtsh`/`wlwpw0b2s`）。commit `7cc86c66440` | 2026-06-07 | ✅ |
| D-029 | — | **P4-T06e FIX-WRITE-DISTRIBUTION 新增 SPI capability `SINK_REQUIRE_PARTITION_LOCAL_SORT`（Option A）**〔⚠️其「分区列按 **cols** 位置索引」已被 **D-030** 回退为 full-schema 索引——partial-static/重排显式列名下 cols 索引会错列〕：翻闸后 MaxCompute 写走通用 `PhysicalConnectorTableSink`,丢 legacy 动态分区 hash+local-sort（ODPS Storage API "writer has been closed"）。新增 `ConnectorCapability.SINK_REQUIRE_PARTITION_LOCAL_SORT`（default 不声明）+ MaxCompute `getCapabilities()` 声明它 + `SUPPORTS_PARALLEL_WRITE`;sink 重写 legacy 3 分支（分区列按 **cols** 位置索引非 legacy full-schema）。替代（隐式 derive / `ConnectorWriteOps` 方法）见详录。clean-room `ww1g95bba` 1 轮收敛 0 must-fix。commit `f0adedba20c` | 2026-06-07 | ✅ |
| D-028 | — | **翻闸功能未完整,补 P4-T06c 接线（用户签字）**：live 验证 recon 代码核实——翻闸（Batch C）只接通 读(SELECT)/CREATE TABLE/写(INSERT);**DROP TABLE / CREATE DB / DROP DB / SHOW PARTITIONS / partitions() TVF 的 FE 分发从未接到 SPI**（连接器侧 P4-T01/T02 已实现,FE 零调用方）→ live 会红 5 项。根因 `PluginDrivenExternalCatalog` 仅 override `createTable`、`metadataOps==null`,且 SHOW PARTITIONS/TVF 仍 legacy `instanceof MaxComputeExternalCatalog` 分发。**决策 = 翻闸前全补接线**：Batch D 前插 **P4-T06c**（通用 PluginDriven 分发,非 MC 专有）把 DDL(create/drop db、drop table)+ SHOW PARTITIONS + partitions TVF 接到已有 SPI,目标 **live 全绿**,再 Batch D。同解 Batch D §2 删-vs-rewire 冲突（先 rewire,Batch D 只删残留 legacy） | 2026-06-07 | ✅ |
| D-027 | — | P4-T06b 翻闸落地 + Batch D 移除范围（2 决策，用户签字）：**翻闸** `CatalogFactory.SPI_READY_TYPES += "max_compute"` + 删 legacy `case "max_compute"`（gate 全绿：compile/checkstyle 0/import-gate 0）；**D-1 时序** = flip 先行、legacy 子系统删除 + fe-core odps 依赖 drop **待用户 live ODPS 验证后**做（保 flip 独立可回退）；**D-2 依赖范围** = fe-core 仅删直接 `odps-sdk-*` 声明，transitive-via-fe-common 留（fe-common 供连接器/be-extensions）。Batch D 完整闭包（21 删 / ~30 清 / keep / pom）见 `designs/P4-batchD-maxcompute-removal-design.md`（OQ-3 穷举 re-grep 满足）。**2 SPI 新增登记 §20 E11**（D-026 预授）：`ConnectorSession.setCurrentTransaction` + `ConnectorWriteOps.usesConnectorTransaction`；T06a 复核修 `PluginDrivenTableSink.getExplainString` `writeConfig==null` NPE 守卫记一笔 | 2026-06-07 | ✅ |
| D-026 | — | P4 Batch C 翻闸设计（用户签字，design-only）：**D-1** capability signal = 新增 `ConnectorWriteOps.usesConnectorTransaction()` default false（MC=true；executor 据此在调任何 throwing-default 写法前分流 txn-model vs JDBC insert-handle）；**D-2** 两 commit（`[P4-T06a]` 写接线/绑定/R-004 隔离测 dormant + `[P4-T06b]` flip 末提）；**D-3** 静态分区/overwrite 绑定**入 cutover**（避 INSERT OVERWRITE PARTITION 翻闸回归）。**两新 SPI**（均 default-preserving）：`ConnectorSession.setCurrentTransaction` + `ConnectorWriteOps.usesConnectorTransaction`（impl 时 E11 登记）。设计 `designs/P4-T05-T06-cutover-design.md` | 2026-06-06 | ✅ |
| D-025 | — | P4-T04 写计划 5 决策（D-1/D-2a 用户签字、D-3/D-4/D-5 主线定）：D-1 **OQ-2=Approach A**（`planWrite` 在 finalizeSink 一处建 ODPS 写 session + `setWriteSession` 绑 txn + 盖 `txn_id`/`write_session_id`，无运行期注入 hook）；D-2a 含 **fe-core seam fill**（`PluginDrivenTableSink.bindViaWritePlanProvider(insertCtx)` 读 overwrite+静态分区；`staticPartitionSpec` 加 `PluginDrivenInsertCommandContext` 非基类——避 `MCInsertCommandContext` override/shadow）；D-3 抽 `MaxComputeDorisConnector.getSettings()`（legacy 单 `settings` 同供 scan+write，抽出=忠实港）；D-4 `supportsInsert()`=true 余最小化（`beginInsert`/`finishInsert`/`getWriteConfig` 留 throwing-default，实际 executor 调用面待 Batch C）；D-5 静态分区作 `getWriteContext()` col→val map | 2026-06-06 | ✅ |
| D-024 | — | P4-T03 两 fork（用户签字）：(1) txn id 经新增 `ConnectorSession.allocateTransactionId()`（fe-core `Env.getNextId` 背书）由连接器分配——尊重 [D-015]/U3，补 id-less 连接器（MC 无外部 id）的分配器机制；(2) ODPS 写 session 创建挪 T04 planWrite（T03 = 纯事务容器，over W4 委派、gate 关 dormant）| 2026-06-06 | ✅ |
| D-023 | — | P4 maxcompute 启 full adopter（recon §9 option A）：W-phase 后按 5 批（A 读/DDL parity → B 写/事务 → C 翻闸 → D 清引用+删 legacy → E 测）落地 + cutover；批次计划 tasks/P4 | 2026-06-06 | ✅ |
| D-022 | — | 写/事务 SPI 设计：A 连接器事务为源·桥接 / B1 commit 载荷 opaque bytes / C1 block-id 窄 callback seam / D INSERT·DELETE·MERGE（defer procedures）/ E 写-plan-provider 仿 scan | 2026-06-06 | ✅ |
| D-021 | — | P4 maxcompute 采 scope=C（写-SPI RFC 先行）：先做共享写/事务 SPI + 通用层解耦（W-phase），再逐连接器 adopter | 2026-06-06 | ✅ |
| D-020 | — | 单 `hms` catalog 多格式 scan 路由 = 方案 B（`ConnectorMetadata.getScanPlanProvider(handle)` per-table default）；细化 D-005（design-only，实现批 E/P7）| 2026-06-05 | ✅ |
| D-019 | — | P3 hudi 采用 hybrid：现做 model-agnostic 连接器硬化+测试（behind gate），推迟 catalog 模型落地+cutover 到 hive/HMS migration | 2026-06-04 | ✅ |
| D-018 | U6 | `ConnectorColumnStatistics` 用 javadoc 类型映射表 + IAE 保证类型安全 | 2026-05-24 | ✅ |
| D-017 | U5 | sys-table 命名统一 `$suffix`，别名机制留待未来 | 2026-05-24 | ✅ |
| D-016 | U4 | `getCredentialsForScans` 批量化，返回 `Map<Range, Credentials>` | 2026-05-24 | ✅ |
| D-015 | U3 | `ConnectorTransaction.getTransactionId` 由连接器分配 | 2026-05-24 | ✅ |
| D-014 | U2 | 不新增 `invalidateColumnStatistics`，挂在 `invalidateTable` | 2026-05-24 | ✅ |
| D-013 | U1 | `ConnectorProcedureOps.listProcedures` 一次性返回，生命周期稳定 | 2026-05-24 | ✅ |
| D-012 | D12 | 用户安装 connector 后初版强制重启 FE | 2026-05-24 | ✅ |
| D-011 | D11 | `RemoteDorisExternalCatalog` 长期做 connector，不在本计划主线 | 2026-05-24 | ✅ |
| D-010 | D10 | `LakeSoulExternalCatalog` 在 P8 删除剩余类 | 2026-05-24 | ✅ |
| D-009 | D9 | API 版本号本计划范围内永不 +1，只新增 default 方法 | 2026-05-24 | ✅ |
| D-008 | D8 | 生产环境不允许 built-in connector，强制目录式插件 | 2026-05-24 | ✅ |
| D-007 | D7 | kafka/kinesis/odbc/doris 子目录不在本计划范围 | 2026-05-24 | ✅ |
| D-006 | D6 | Iceberg snapshot/manifest cache 放连接器内，fe-core 不感知 | 2026-05-24 | ✅ |
| D-005 | D5 | hudi/iceberg-on-HMS 用 `ConnectorTableSchema.tableFormatType` 区分 | 2026-05-24 | ✅ |
| D-004 | D4 | HMS event pipeline 放 `fe-connector-hms`，通过 `ConnectorMetaInvalidator` 回调 | 2026-05-24 | ✅ |
| D-003 | D3 | 旧 `*ExternalCatalog` 子类**全部删除**，不保留中间形态 | 2026-05-24 | ✅ |
| D-002 | D2 | `PluginDrivenScanNode` 长期保持 `extends FileQueryScanNode` | 2026-05-24 | ✅ |
| D-001 | D1 | 沿用已有 `SUPPORTS_PASSTHROUGH_QUERY`，不新增 query SPI | 2026-05-24 | ✅ |

---

## 详细记录（时间倒序）

### D-030 — P4-T06e FIX-BIND-STATIC-PARTITION 新增 SPI capability SINK_REQUIRE_FULL_SCHEMA_ORDER + 回退 D-029 索引（用户批准扩 scope）

- **日期**：2026-06-07
- **状态**：✅ 生效
- **关联**：[FIX-BIND-STATIC-PARTITION 设计](./tasks/designs/P4-T06e-FIX-BIND-STATIC-PARTITION-design.md)、[review-rounds](./reviews/P4-T06e-FIX-BIND-STATIC-PARTITION-review-rounds.md)、[D-029]（被部分回退）、[D-026 DECISION-3]
- **背景**：翻闸后真实 MaxCompute catalog = `PluginDrivenExternalCatalog`，所有 MC 写走通用 `bindConnectorTableSink`。该方法克隆自 `bindJdbcTableSink`（JDBC 按列名生成 INSERT SQL、数据 cols/用户序即可），但 **MaxCompute BE/JNI writer 按位置映射** Arrow 列到 `writeSession.requiredSchema()`（完整表 schema 序）。后果：① 静态分区无列名 `INSERT INTO mc PARTITION(pt='x') SELECT <非分区列>` 列数校验抛（F19/F48 blocker）；② 静态分区列未在 full-schema 末尾 → BE 末尾擦除契约错位；③ **非分区** MC 重排/部分显式列名静默错列/丢列。legacy `bindMaxComputeTableSink` **无条件** full-schema 投影（不论分区与否）——通用路径漏了这层。
- **决策**：(a) 新增 `ConnectorCapability.SINK_REQUIRE_FULL_SCHEMA_ORDER`（"连接器按位置写 full-schema"，default 不声明）；MaxCompute `getCapabilities()` 声明之、JDBC/ES 不声明；`PluginDrivenExternalTable.requiresFullSchemaWriteOrder()` 读之。(b) `bindConnectorTableSink` 分支键 = `table.requiresFullSchemaWriteOrder()`：true→full-schema 投影（`getColumnToOutput`+`getOutputProjectByCoercion(getFullSchema())`,镜像 legacy,对**全**MC 写形）；false→cols 序（JDBC/ES）。(c) **回退 D-029**：`PhysicalConnectorTableSink.getRequirePhysicalProperties` 分区列索引 cols→full-schema（因 child 现恒 full-schema 序；cols 索引在 partial-static/重排下错列）。(d) `selectConnectorSinkBindColumns` 无列名时剔除静态分区列（镜像 legacy）；`InsertUtils` VALUES 路径加 `UnboundConnectorTableSink` 分支。
- **替代方案**：判别键 = `!staticPartitionColNames.isEmpty()`（round-1 证伪：纯动态重排错列）→ `!getPartitionColumns().isEmpty()`（round-2 证伪：非分区 MC 重排/部分错列）→ **capability**（终态 = legacy 全 parity）。亦考虑 bind 期查 `connector.getWritePlanProvider()!=null`（更重、less explicit）；capability 与 P0-2 模式一致且可扩展（未来按位置写连接器自声明）。
- **影响**：4 产线文件（`ConnectorCapability` SPI / `MaxComputeDorisConnector` / `PluginDrivenExternalTable` reader / `BindSink` bind + `PhysicalConnectorTableSink` 索引）+ `InsertUtils`。两写 capability 正交但有硬依赖（`SINK_REQUIRE_PARTITION_LOCAL_SORT` ⟹ `SINK_REQUIRE_FULL_SCHEMA_ORDER`，已 javadoc 登记，nit P03-V3-1）。**Batch-D 红线**：删 legacy `bindMaxComputeTableSink`/`PhysicalMaxComputeTableSink` 须待本 fix 落（已落）。**follow-up**：bind 投影无 fe-core 单测 harness → DV-014；真值闸 live e2e（p2 `test_mc_write_insert` Test 3/3b + `test_mc_write_static_partitions`）。

---

### D-029 — P4-T06e FIX-WRITE-DISTRIBUTION 新增 SPI capability SINK_REQUIRE_PARTITION_LOCAL_SORT

- **日期**：2026-06-07
- **状态**：✅（已落 commit `f0adedba20c`；live e2e 真值闸待真实 ODPS）
- **关联**：[FIX-WRITE-DISTRIBUTION 设计](./tasks/designs/P4-T06e-FIX-WRITE-DISTRIBUTION-design.md)、[review-rounds](./reviews/P4-T06e-FIX-WRITE-DISTRIBUTION-review-rounds.md)、[复审报告 §A.NG-2/NG-4](./reviews/P4-maxcompute-full-rereview-2026-06-07.md)、[D-001]（capability 沿用先例）、[DV-013]、Batch-D 红线
- **背景**：翻闸后 MaxCompute 写走通用 `PhysicalConnectorTableSink`,其 `getRequirePhysicalProperties()` 只有 `supportsParallelWrite?RANDOM:GATHER`,且 `MaxComputeDorisConnector` 无 `getCapabilities` override（空集）→ 每写落 GATHER。丢 legacy `PhysicalMaxComputeTableSink` 的动态分区 hash-by-partition + 强制 local-sort（ODPS Storage API 流式分区 writer,见新分区即关上一 writer,未分组行触发 "writer has been closed"）+ 非分区/全静态并行写。通用 sink 从 JDBC/ES 克隆,无通道让连接器声明该需求。
- **决策（Option A）**：新增 `ConnectorCapability.SINK_REQUIRE_PARTITION_LOCAL_SORT`（连接器声明动态分区写需 hash-by-partition + 强制 local-sort）;MaxCompute `getCapabilities()` 声明它 + `SUPPORTS_PARALLEL_WRITE`;`PluginDrivenExternalTable.requirePartitionLocalSortOnWrite()` 读之（镜像 `supportsParallelWrite()`,经 `connector.getCapabilities().contains(...)`）;`PhysicalConnectorTableSink.getRequirePhysicalProperties()` 重写 legacy 3 分支。**关键修正 vs legacy**：分区列 → child output 索引按 **cols 位置**（通用 sink 的 child 投影到 cols 序,`BindSink` 强制 `cols.size()==child output size`）,非 legacy 的 full-schema 位置。default 不声明 → 其他连接器零行为变更。
- **替代方案**：(B) 隐式 derive（`supportsParallelWrite && hasPartition && dynamic → 强制 hash+local-sort`）—— 拒：把 MC Storage-API 的 local-sort 政策强加到所有并行写分区连接器（含 per-partition 缓冲、本不需 sort 的）;(C) `ConnectorWriteOps` 方法（仿 `supportsInsertOverwrite`）—— 拒：sink 读它需在 property-derivation 热路建 `ConnectorSession` + `getMetadata`,而 sibling `supportsParallelWrite()`（同方法内读）用更廉价的 `getCapabilities()` 集,不一致。
- **影响**：fe-connector-api（1 枚举值）+ fe-connector-maxcompute（`getCapabilities`）+ fe-core（1 table 方法 + sink 3 分支重写）。blast radius：`SUPPORTS_PARALLEL_WRITE`/新能力仅 sink 分发路径读（grep 实证 2+1 reader;唯一另一 `getCapabilities` consumer `QueryTableValueFunction` 查 `SUPPORTS_PASSTHROUGH_QUERY`,MC 不声明 → 不受影响）。**Batch-D 红线**：删 `PhysicalMaxComputeTableSink`（写分发唯一逻辑副本）须待本 fix + P0-3 双落。`ShuffleKeyPruner` non-strict 少剪 + `enable_strict_consistency_dml=false` 丢 local-sort = [DV-013]。

### D-028 — 翻闸功能未完整,补 P4-T06c FE 分发接线（用户签字）

- **日期**：2026-06-07
- **状态**：✅（翻闸前置工作;实现 = P4-T06c,下一 session）
- **关联**：[tasks/P4](./tasks/P4-maxcompute-migration.md)（新增 P4-T06c）、[HANDOFF](./HANDOFF.md)「⚠️ 关键发现」、[D-027]（翻闸落地）、[Batch D 设计](./tasks/designs/P4-batchD-maxcompute-removal-design.md)（前置门 + §2 处置随之改）、DV-007（`listPartition*` 零 live caller）
- **背景**：用户问「如何做 live 验证 / 验证哪些内容」。并行 recon（catalog 建法 / smoke SQL / SPI 路径映射 / build-deploy）+ **代码逐条核实** 暴出：T05/T06 翻闸**只接通**了 读(SELECT,`PluginDrivenScanNode`)/CREATE TABLE(`PluginDrivenExternalCatalog.createTable:257` override)/写(INSERT 全家,G1–G5)。**未接通**（live 会 FAIL,均 file:line 核实）：
  - **DROP TABLE / CREATE DB / DROP DB**：`PluginDrivenExternalCatalog` **不** override 这些、`metadataOps` **永远 null** → `ExternalCatalog.dropTable:1105`/`createDb:1004`/`dropDb:1029` 抛 `... is not supported for catalog`。（RENAME TABLE 同,且连接器侧未 port。）
  - **SHOW PARTITIONS**：`ShowPartitionsCommand:202-207` allow-list 仍按 `instanceof MaxComputeExternalCatalog`,翻闸后 catalog 是 `PluginDrivenExternalCatalog` → `not allowed`。
  - **partitions() TVF**：`MetadataGenerator.partitionMetadataResult:1308-1319` `instanceof MaxComputeExternalCatalog` 落空 → `not support catalog`。
  - 连接器侧 `createDatabase/dropDatabase/dropTable`（P4-T01）+ `listPartitionNames/listPartitions/listPartitionValues`（P4-T02）**已实现但 FE 零调用方**（DV-007 已记）。tasks/P4 §批次依赖原写「翻闸即 读/写/DDL/分区/show 全切 SPI」**与代码不符**,已纠正。
- **决策（用户 AskUserQuestion 签字,选「翻闸前全补接线」）**：视翻闸为**未完成**;Batch D 之前插 **P4-T06c**,把 DDL（createDb/dropDb/dropTable）+ SHOW PARTITIONS + partitions() TVF 的 **FE 分发接到已有连接器 SPI**。要点：
  - **通用实现**（keyed on `PluginDrivenExternalCatalog` / `PLUGIN_EXTERNAL_TABLE`,**非 MC 专有**）→ ① 同时修 jdbc/es/trino 同类缺口;② 让 Batch D §2 对 `ShowPartitionsCommand`/`MetadataGenerator`/`PartitionsTableValuedFunction` 的处置从 **delete-branch** 退化为**删残留 legacy MC 引用**（先 rewire 后删,解 Batch D 设计 §2 与 RFC `:1065`/master-plan `:126` 的删-vs-rewire 冲突）。
  - DDL override 镜像现有 `createTable:257`（路由 `connector.getMetadata().{createDatabase/dropDatabase/dropTable}` + editlog）。SHOW PARTITIONS / partitions TVF 加 `PluginDrivenExternalCatalog` 分支路由 `listPartitionNames`。
  - **本任务只补 FE 接线**（连接器方法已存在）= "接线"非"重写"。
- **scope 边界**：`partition_values()` TVF（`MetadataGenerator:2080` HMS-only）**不入 T06c**（OQ-5：legacy MC 很可能本就不支持 = 既有限制非回归,待确认）。RENAME TABLE 需连接器先 port,次要/可推迟（不在 live smoke 列表）。
- **完成门**：T06c 落（fe-core gate + UT）→ **用户报 live 验证全绿**（[D-027] D-1 的 `OdpsLiveConnectivityTest` + 手测 smoke 11 项全绿）= 翻闸真正完成 → 才解锁 Batch D。**flip 在 live 绿前保持独立可 revert**（沿 [D-027] D-1）。

### D-027 — P4-T06b 翻闸落地 + Batch D 移除范围（2 决策，用户签字）

- **日期**：2026-06-07
- **状态**：✅（翻闸已落、gate 全绿；Batch D 移除 = 待 live 验证后做）
- **背景**：用户要求「开始下一步（T06b 翻闸）」+ 追加「fe-core 不再依赖任何 maxcompute jar」。recon（并行 re-grep + 对抗验证，OQ-3 入口门满足）证：fe-core `odps-sdk-core`/`odps-sdk-table-api` 仅经 legacy MaxCompute 子系统（7 文件 `import com.aliyun.odps`，全在删除集）可达 → 去依赖 = 删整套 legacy（21 文件）+ 清 ~30 反向引用（即整个 Batch D）。
- **决策**：
  - **翻闸（T06b）**：`CatalogFactory.SPI_READY_TYPES += "max_compute"`(:52) + 删 `case "max_compute"`(原 :146-149) + 删 unused import + 注释去 max_compute。gate 全绿（compile BUILD SUCCESS/MVN_EXIT=0 + checkstyle 0/CS_EXIT=0 + import-gate 0，真实 EXIT 核）。
  - **D-1（时序）= flip 先行、移除待 live 验证**：本任务只落 flip（独立可回退）；legacy 子系统删除 + pom odps drop（Batch D）挪到**用户跑 `OdpsLiveConnectivityTest`（4 个 `MC_*` 环境变量）+ 手测 smoke 绿之后**的紧邻 follow-up。理由：删 legacy 即去掉易回退的 fallback，故 flip 在 live 验证前保持独立可 revert（trino 翻闸亦 flip 先于删除）。
  - **D-2（依赖范围）= 仅删直接声明**：fe-core/pom.xml 删两 `odps-sdk-*` 块即可；fe-core 删后**零** odps 源引用，但仍经 fe-common transitive 见 `odps-sdk-core`（fe-common 留 odps 供 `MCUtils` → 连接器 + be-java-extensions），可接受（用户选 "Direct declarations only"）。镜像 trino `c4ac2c5911d`（只删 fe-core 直接声明）。
- **2 SPI 新增登记**（D-026 预授，default-preserving）：`ConnectorSession.setCurrentTransaction` + `ConnectorWriteOps.usesConnectorTransaction` 录入 `01-spi-extensions-rfc.md` §20 E11。T06a 对抗复核已修 `PluginDrivenTableSink.getExplainString` 加 `writeConfig==null` 守卫（防 plan-provider 模式 EXPLAIN NPE，翻闸后可达）——记一笔。
- **设计文档（Batch D 执行源，turnkey）**：[tasks/designs/P4-batchD-maxcompute-removal-design.md](./tasks/designs/P4-batchD-maxcompute-removal-design.md)（21 删除集 + 84 反向引用闭包 + keep 集 + pom drop + ordered TODO；执行前置门 = live 验证绿）。

### D-026 — P4 Batch C 翻闸设计（3 子决策 + 2 SPI 新增，用户签字）

- **日期**：2026-06-06
- **状态**：✅（design-only；实现 = T05 → T06，下一 fresh session）
- **背景**：Batch A+B 全完成（gate 关 dormant），下一 = Batch C（唯一 live 切点）。本场 design-first：4 路 Explore re-verify recon 锚点 + 主线核读 executor/txn 生命周期，定 dormant→live 写接线（坑3 三点）+ flip + R-004。recon 校正：GsonUtils 真锚 `:397`/`:472`（非 ~405/~478）；`legacyLogTypeToCatalogType` 默认分支已出 `"max_compute"`（**无需加 case**）；live executor = `PluginDrivenInsertExecutor`（非裸 `beginTransaction`）；`PluginDrivenTransactionManager.begin(connectorTx)` **未** `putTxnById`（G3）；`UnboundConnectorTableSink` 不携静态分区（G4）。
- **决策**：
  - **D-1（capability signal）= (A)** 新增 `ConnectorWriteOps.usesConnectorTransaction()` default false，`MaxComputeConnectorMetadata` override true。executor 据此在调任何 throwing-default 写法（`getWriteConfig`/`beginInsert`/`beginTransaction` 全 default 抛、MC 留抛=D-4）前分流 txn-model（MC）vs JDBC insert-handle。否决 (B) `getWritePlanProvider()!=null` 代理（耦合松）/(C) 复用 `ConnectorWriteType`（逆 D-4 + enum churn + getWriteConfig 调用前移）。
  - **D-2（commit 粒度）= 两 commit、flip 末**：`[P4-T06a]` = 写接线（W-a..d）+ 静态分区/overwrite 绑定（G4/G5）+ R-004 隔离 UT（全 additive/dormant-safe）；`[P4-T06b]` = `CatalogFactory.SPI_READY_TYPES += "max_compute"`(:52) + 删 :146 case（唯一 live-switch 单点，易 review/revert）。
  - **D-3（静态分区/overwrite 绑定 scope）= 入 cutover（T06）**：扩 `UnboundConnectorTableSink` 携静态分区 + `InsertIntoTableCommand`/`InsertOverwriteTableCommand` 填 `PluginDrivenInsertCommandContext`（overwrite + staticPartitionSpec）。避免翻闸瞬间 INSERT OVERWRITE / 静态分区 INSERT 回归。
- **SPI 新增（2，均 default-preserving，零 jdbc/es/trino 影响）**：`ConnectorSession.setCurrentTransaction(ConnectorTransaction)`（+ `ConnectorSessionImpl` 字段/`getCurrentTransaction` override；把 connectorTx 绑入 sink session 供 T04 `planWrite` 读，解 G1）；`ConnectorWriteOps.usesConnectorTransaction()`（D-1）。impl 时登记 `01-spi-extensions-rfc.md` §20 E11。
- **不重开 T03/T04**：Approach A locked（`planWrite` 读 `getCurrentTransaction`）；本设计接线 *到* 它。R-004 拆两分：① classloader 隔离（无 creds，CI 可跑）+ ② live 连通（creds，用户跑）。
- **设计文档**：[tasks/designs/P4-T05-T06-cutover-design.md](./tasks/designs/P4-T05-T06-cutover-design.md)（verified file:line 锚点 + 5 gap G1–G5 + lifecycle order + R-004 两分测 + ordered TODO）。
- **T05 实现校正（2026-06-06，gate-green、待 commit）**：实现期 4-agent 对抗复核发现 §3.1/§8 ordered TODO **漏 GSON DB `:452`**（`MaxComputeExternalDatabase`，仅列了 catalog `:397`+table `:472`）；折入 T05（三注册齐迁 `registerCompatibleSubtype` + 删 3 unused import），否则翻闸后 `MaxComputeExternalDatabase.buildTableInternal:44` cast `PluginDrivenExternalCatalog`→`MaxComputeExternalCatalog` 抛 `ClassCastException`。另 2 告警判非问题（`getMetaCacheEngine` 假阳性=plugin 路径经连接器取 schema、走 "default" 桶同 es/jdbc/trino；`getMysqlType`→"BASE TABLE" 同 ES 既定行为）；dormancy 告警 = 既载中间态 caveat（其"保留 registerSubtype"修法错，会撞 duplicate-label IAE）。详见设计 §3.4。

### D-025 — P4-T04 写计划 5 决策（OQ-2 解法 + seam fill + 三主线定）

- **日期**：2026-06-06
- **状态**：✅ 生效
- **关联**：[tasks/P4 P4-T04](./tasks/P4-maxcompute-migration.md)、[P4-T04 设计](./tasks/designs/P4-T04-write-plan-design.md)、[D-024]（T03/T04 边界、`setWriteSession` 槽）、[DV-009]（W5 planWrite layer）、[DV-012]（partition_columns 源）、OQ-2
- **背景**：T04 把 legacy 写计划（`MCTransaction.beginInsert` 建写 session + `MaxComputeTableSink.bindDataSink`/`setWriteContext` 产 `TMaxComputeTableSink`）港入连接器 over W5 opaque-sink seam。核心难点 OQ-2 = legacy 经 `MCInsertExecutor.beforeExec` **运行期注入**的 `txn_id`/`write_session_id`、overwrite/静态分区 context 需在 plugin-driven 侧重建。
- **决策**：
  - **D-1（OQ-2 架构，用户签字）= Approach A**：executor 生命周期序 `beginTransaction`(txn_id 译前生)→translate→`finalizeSink`/`bindDataSink(insertCtx)`→`beforeExec`→coordinator ⇒ `planWrite` 跑在 finalizeSink、txn_id 已在 + ODPS 写 session 可就地建 → **planWrite 一处做完**（建 session + `session.getCurrentTransaction()`→`MaxComputeConnectorTransaction.setWriteSession` + 盖 `txn_id`/`write_session_id`）。**无运行期注入 hook**（否决 Approach B = 泛化 legacy `setWriteContext` dance）。
  - **D-2a（fe-core seam 填充，用户签字）= 含 seam fill**：`PluginDrivenTableSink.bindViaWritePlanProvider` 改收 `Optional<InsertCommandContext>`、读 `isOverwrite()`+`getStaticPartitionSpec()` 填 handle；**实现期细化**：`staticPartitionSpec` 加在 `PluginDrivenInsertCommandContext`（非设计「Why」倾向的基类 `BaseExternalTableInsertCommandContext`）——因 `MCInsertCommandContext` 已自带 `staticPartitionSpec`+getter 且 shadow 基类 `overwrite`，加基类会成 override/shadow 缠结（Rule 3 surgical）；plugin-driven seam 只见 `PluginDrivenInsertCommandContext`，post-migration hive/iceberg 复用同类，复用目标仍满足。在设计「`PluginDrivenInsertCommandContext`（或基类）」envelope 内。
  - **D-3（EnvironmentSettings 复用，主线定）= 抽 `MaxComputeDorisConnector.getSettings()`**：决定性证据——legacy `MaxComputeExternalCatalog` 持**单** `settings` 字段同供 scan（`MaxComputeScanNode`）+ write（`MCTransaction.beginInsert`），故抽出共用是**忠实港 legacy 设计**（非投机重构，化解 Rule 3 张力）；scan provider :146-162 构造上移、scan/write 共用。连接器 gate 关 dormant，动 scan 零 live 风险。
  - **D-4（insert 机制面，主线定）= `supportsInsert()`=true 余最小化**：MC sink 经 `planWrite`、commit 经 `ConnectorTransaction.commit()`，故 `beginInsert`/`finishInsert`/`getWriteConfig` 留 throwing-default（无 MC 实质活）；实际 executor 调用面以 Batch C 为准（不投机加 no-op，Rule 2；显式 doc 不静默，Rule 12）。
  - **D-5（writeContext 编码，主线定）= 静态分区作 `getWriteContext()` 的 col→val map**；overwrite 经 `isOverwrite()`。planWrite 据 ODPS 分区列序拼 `"col=val,..."` 喂 `PartitionSpec`、原样 set 入 `static_partition_spec`(field 10)。
- **影响**：T04 dormant（gate 关，plan-provider 分支无 live caller）；binding 期填充 `PluginDrivenInsertCommandContext.staticPartitionSpec`/overwrite 归 Batch C/D（坑3，`InsertIntoTableCommand:598` 现传空 ctx）；planWrite `getCurrentTransaction()` 要返 MC txn ⇒ Batch C `beginTransaction`→置 `ConnectorSessionImpl`。T04 不新增 SPI 面（W1 全建）。立 paimon/iceberg/hive 写-plan adopter 样板。

---

### D-024 — P4-T03 写/事务 SPI 两 fork（txn id 机制 + T03/T04 边界）

- **日期**：2026-06-06
- **状态**：✅ 生效
- **关联**：[tasks/P4 P4-T03](./tasks/P4-maxcompute-migration.md)、[P4-T03 设计](./tasks/designs/P4-T03-write-txn-design.md)、[D-015]/U3（getTransactionId 连接器分配）、[D-022]（写 SPI）、[01-spi-extensions-rfc E11](./01-spi-extensions-rfc.md)
- **背景**：handoff 标注 T03/T04 未逐行定稿；recon 暴两处需拍板的 fork（[D-015]「连接器分配 id」对 MC 不成立——MC 无外部 id 且连接器够不到 `Env.getNextId`；写 session 创建需 overwrite/静态分区 context = OQ-2）。
- **决策**（用户 AskUserQuestion 签字 2026-06-06）：
  - **Fork 1（txn id）**：给 `ConnectorSession` 加 `default long allocateTransactionId()`（default 抛；fe-core `ConnectorSessionImpl` override 回 `Env.getCurrentEnv().getNextId()`），MC `beginTransaction` 经它分配。**仍属「连接器分配」语义**（经注入的引擎分配器），尊重 [D-015]；id 即 Doris 全局 txn_id，与 sink `txn_id` / `GlobalExternalTransactionInfoMgr` 一致。SPI 加面记 E11。
  - **Fork 2（T03/T04 边界）**：ODPS 写 session 创建挪 **T04 planWrite**（`ConnectorWriteHandle` 带 overwrite+writeContext，顺解 OQ-2）；**T03 = 纯事务容器**（commitDataList/nextBlockId/writeSessionId 槽 + addCommitData[TBinaryProtocol]/block-alloc/commit[港 finishInsert]/rollback/getUpdateCnt）+ `beginTransaction`。
- **影响**：executor 接线（`beginTransaction`→`begin(connectorTx)`）+ `GlobalExternalTransactionInfoMgr` 注册推迟翻闸期（Batch C），保 T03 dormant、不破 JDBC/ES。立 paimon/iceberg/hive 后续事务 adopter 的 id-source 样板。

---

### D-023 — P4 maxcompute 启 full adopter（option A，5 批 cutover）

- **日期**：2026-06-06
- **状态**：✅ 生效
- **关联**：[tasks/P4-maxcompute-migration.md](./tasks/P4-maxcompute-migration.md)、[research/p4-maxcompute-migration-recon.md §9](./research/p4-maxcompute-migration-recon.md)、[D-021]（scope=C→本决策接 option A）、[D-022]（写 SPI）、[写 RFC §12](./tasks/designs/connector-write-spi-rfc.md)、[R-004]
- **背景**：W-phase（W1–W7）已落地共享写/事务 SPI + 通用层解耦（[D-021]/[D-022]），recon §9 scope fork（B hybrid / A full / C 写-SPI 先行）中 C 已完成、写路径 keystone 已解耦。现决 P4 余下走 **option A（full adopter + 翻闸）**，非 P3 式 hybrid。
- **决策**（用户批准 2026-06-06）：按 [tasks/P4](./tasks/P4-maxcompute-migration.md) 的 **5 批 / 11 task** 落地：A 连接器读/DDL/分区 parity（gate 关）→ B 写/事务 SPI（gate 关）→ **C 翻闸（唯一 live 切点，含 R-004 防御测）** → D 清 ~19 反向引用 + 删 `datasource/maxcompute/`（收口 P1-T02 McStructureHelper 去重）→ E 连接器测试基线 + PR。A、B 并行、均 dormant；两者全绿 + R-004 过方进 C。
- **影响**：P4 成首个 full adopter，为 P5 paimon / P6 iceberg / P7 hive 立样板。recon §3「~36 反向引用」经 post-W-phase re-grep 校正为 **~19**（W-phase 灭 `Coordinator`/`LoadProcessor`/`FrontendServiceImpl` 3 热点 txn 站，grep 证）。每批独立 commit。

---

### D-022 — 写/事务 SPI 设计（A / B1 / C1 / D / E）

- **日期**：2026-06-06
- **状态**：✅ 生效
- **关联**：[写/事务 SPI RFC](./tasks/designs/connector-write-spi-rfc.md)、[research/connector-write-spi-recon.md](./research/connector-write-spi-recon.md)、[D-021]（scope=C）、[D-009]（default-only）、[01-spi-extensions-rfc.md E11](./01-spi-extensions-rfc.md)、W-phase commits（W1+W2 `be945476ba7`、W3+W6 `9ad2bbe40ec`、W4 `759cc0874c8`、W5 `9ebe5e27fa4`）
- **背景**：P4 maxcompute recon 证它在热路径会写（`MCTransaction` 在 `Coordinator`/`FrontendServiceImpl`/`LoadProcessor` concrete cast）；写路径 = 翻闸 keystone。三现存写者 maxcompute/hive/iceberg 同写生命周期 ⊥ 三处分歧（commit 载荷型 / mc block-id / iceberg procedures+delete/merge），paimon 今读后写需前瞻。须定写/事务 SPI 形状。
- **决策**（用户签字 2026-06-06）：
  - **A 事务模型统一·桥接**：连接器 `ConnectorTransaction` 为单一事实源；fe-core 通用写编排经 `PluginDrivenTransaction`（`PluginDrivenTransactionManager` 产）桥接，只调多态 fe-core `Transaction`；现存 `MC/HMS/IcebergTransaction` 过渡期 override 适配，逐连接器迁入 plugin。
  - **B1 commit 载荷 opaque bytes**：BE→FE commit 载荷（`TMCCommitData`/`THivePartitionUpdate`/`TIcebergCommitData`）`TBinaryProtocol` 序列化为 `byte[]`，经 `Transaction.addCommitData(byte[])` / `ConnectorTransaction.addCommitData` 交连接器反序列化。零 BE 改、保全富信息、消除 3 处 concrete cast。留一处序列化 shim（fail-loud，Open-1）。
  - **C1 block-id 窄 callback seam**：`Transaction.supportsWriteBlockAllocation()` + `allocateWriteBlockRange()` 默认方法，仅 maxcompute override，消 `FrontendServiceImpl` `instanceof MCTransaction`。拒 C2 过度泛化 / C3 留特例。
  - **D INSERT/DELETE/MERGE**：SPI 形状定全；实现 mc/hive=insert、iceberg=+delete/merge（P6）。**defer**：iceberg procedures（E2/P6）、hive 行级 ACID、各连接器代码搬迁（adopter 阶段）。
  - **E 写-plan-provider 仿 scan**：连接器经 `ConnectorWritePlanProvider.planWrite()` 产 opaque `TDataSink`（仿 `ConnectorScanPlanProvider`）；`Connector.getWritePlanProvider()` default null。
- **替代方案**：B2 中立 envelope（丢富信息，否决）/ B3 thrift union 漏进 SPI（否决）；C2/C3（否决）。见 RFC §11。
- **影响**：W-phase（W1–W7）落地共享 SPI 面 + 通用层解耦，**behind gate、零行为变更、golden 等价**；逐连接器 adopter（P4 mc / P6 iceberg / P7 hive）后续。新方法均 default（满足 [D-009]），BE 契约不变。W5 落地暴露 [DV-009]（写 sink 收口位置修正）。

---

### D-021 — P4 maxcompute 采 scope=C（写-SPI RFC 先行）

- **日期**：2026-06-06
- **状态**：✅ 生效
- **关联**：[research/p4-maxcompute-migration-recon.md](./research/p4-maxcompute-migration-recon.md)、[写/事务 SPI RFC](./tasks/designs/connector-write-spi-rfc.md)、[D-022]（写 SPI 设计）、[connectors/maxcompute.md](./connectors/maxcompute.md)
- **背景**：P4 启动 recon 发现 maxcompute 在热路径**会写**（非只读骨架），写路径是翻闸前提。可选 scope：A 仅迁读+推迟写；B 连写一起但不先定 SPI；**C 写-SPI RFC 先行**（先设计共享写/事务 SPI + 通用层解耦，再迁连接器）。
- **决策**（用户签字 2026-06-06）：采 **scope=C**——先出写/事务 SPI RFC（[D-022]）并落 **W-phase**（共享解耦 + SPI 面，gate 不动、零行为变更），再做 maxcompute full adopter（搬类 + impl 写 SPI + 翻闸）。理由：写面是 mc/hive/iceberg 共享 keystone，先收口避免每连接器重造、降低反向 instanceof 清理风险。
- **影响**：P4 在 adopter 前插入 W-phase（写 RFC 直接后续）；hive(P7)/iceberg(P6) 复用同一 SPI。W-phase 不翻闸、不搬类、不删 legacy。

---

### D-020 — 单 `hms` catalog 多格式 scan 路由 = 方案 B（per-table SPI provider）

- **日期**：2026-06-05
- **状态**：✅ 生效
- **关联**：[D-005](#d-005)（被细化）、[D-009](#d-009)（default-only 约束）、[D-019](#d-019)（hybrid）、[tasks/P3 T08](./tasks/P3-hudi-migration.md)、[designs/P3-T08-tableformat-dispatch-design.md](./tasks/designs/P3-T08-tableformat-dispatch-design.md)、[research/spi-multi-format-hms-catalog-analysis.md](./research/spi-multi-format-hms-catalog-analysis.md)
- **背景**：legacy 单 `hms` catalog 靠 `HMSExternalTable.dlaType` per-table tag + 处处 `switch(dlaType)` 同时暴露 Hive/Hudi/Iceberg。SPI 侧 `ConnectorTableSchema.tableFormatType` **产而不用**——`PluginDrivenExternalTable.initSchema:79-109` 只读 columns、`Connector.getScanPlanProvider:40-42` per-catalog 单点、`HiveScanPlanProvider` 硬编码 `tableFormatType="hive"`（research §6①②③ + 本场 firsthand 核读）。T08（批 D，design-only）须定 per-table 路由 seam；研究浮现三互斥方案（A 连接器内 router / B per-table SPI provider / C fe-core 发现期分派）。
- **决策**：M2 scan 路由采 **方案 B**——在 `ConnectorMetadata` 新增**向后兼容 default** `getScanPlanProvider(ConnectorTableHandle handle)`（默认返 null → fe-core 回落 per-catalog `Connector.getScanPlanProvider()`）；fe-core `PluginDrivenScanNode.getSplits` 优先 per-table provider、回落 per-catalog；注册 `"hms"` 的连接器 override 之、按 `handle.getTableType()` 委派 Hudi/Iceberg provider。把"per-table 选 provider"升为一等 SPI 契约。配套 **M1**（fe-core 按缓存的 `tableFormatType` 做 per-table 引擎名/身份，作 opaque 串逐字上报、热路径不读）三方案通用。**design-only，实现 = 批 E/P7**。
- **替代方案**：**A 连接器内 router**（`Connector.getScanPlanProvider()` 返回一个 `planScan` 按 `handle.getTableType()` 委派的 router）——零 SPI churn（`planScan` 已带 handle，本场核实），但路由藏进连接器、per-table 语义非一等契约；列为备选，批 E 实现期可据 iceberg 接入复杂度复核。**C fe-core 发现期分派**（fe-core 读 `tableFormatType` 建 format-specific 表对象，≈legacy DLAType→多态 DlaTable）——**否决**：fe-core 回退到 per-format 分派，违背瘦 fe-core 北极星（import-gate / D-003 / D-006）。
- **影响**：**细化 [D-005]**——D-005 的"`tableFormatType` 区分符"结论沿用；但其"fe-core dispatch 到对应 `PhysicalXxxScan`"措辞（2026-05-24，**早于 P1 scan-node 统一**为单 `PluginDrivenScanNode` + per-range format）由 per-table provider seam 取代（SPI 路径已无 per-format `PhysicalXxxScan`）。批 E/P7 据此实现 M1+M2；新 default 方法满足 [D-009]（不破签名）。Iceberg-on-hms 经 SPI 依赖 **P6** 先补 `IcebergScanPlanProvider`（M3）；hms 网关引入对 `-hudi`/`-iceberg` 模块依赖边（A/B 同担）。**本场无代码改动**。

---

### D-019 — P3 hudi 采用 hybrid 推进策略

- **日期**：2026-06-04
- **状态**：✅ 生效
- **关联**：[DV-005](./deviations-log.md)、[D-005](#d-005)、[tasks/P3](./tasks/P3-hudi-migration.md)、master plan §3.4/§3.8
- **背景**：两轮 code-grounded recon（+ 对抗验证）揭示：HMS-over-SPI 读码已存在但 dormant（gate 关、零 live caller）；scan/split plumbing 正确（单 `PluginDrivenScanNode` 混合 COW-native+MOR-JNI 非问题，与 legacy 结构等价）；真正阻塞是 catalog 模型错配（独立 `"hudi"` type vs 寄生 `"hms"` 的 `DLAType.HUDI`，fe-core 不消费 `tableFormatType`）+ 关闭的 gate；另有一批**与模型无关**的 SPI-surface 正确性缺口（`schema_id`/`history_schema_info` 缺、`column_types` 双 bug、time-travel 静默返最新、增量读无表示、partition 裁剪缺、三模块零测试）。
- **决策**：P3 走 **hybrid**。**现在做 (b)**（批 A–D，全部 behind 关闭的 gate，零 live-path 风险）：hudi 连接器 model-agnostic 正确性修复 + metadata 补全 + 测试基线 + 模型 dispatch 设计（design-only）。**推迟 (a)**（批 E，登记不编码）：fe-core 消费 `tableFormatType` 的 per-table 分流、gate flip（`SPI_READY_TYPES` 加 hms/hudi）、live cutover、删 legacy `datasource/hudi/`、完整增量/time-travel、集群/runtime 验证 —— 并入一个 properly-scoped hive/HMS migration（P7 或专门子阶段）。
- **替代方案**：(a) **hms-first 一次到位** —— 否决为 P3 首交付（把 P7 范围拉进 P3、re-route live 重度使用的 HMS 路径、零测试网，回归风险大）；(c) **直接 flip gate** —— 早已否决（模型错配下 `"hudi"` provider 不可达 + 高回归）。
- **影响**：P3（hybrid）**不交付用户可见行为变化**（hudi 仍走 legacy，gate 不翻）；产出是连接器硬化 + 测试网 + 设计。批 A–C 验证为单测/设计级，端到端/集群验证随批 E cutover。tasks/P3 据此划批。

---

### D-018 — `ConnectorColumnStatistics` 类型安全契约（原 U6）

- **日期**：2026-05-24
- **状态**：✅ 生效
- **关联**：[01-spi-extensions-rfc.md §11.2](./01-spi-extensions-rfc.md)
- **背景**：`ConnectorColumnStatistics.minValue / maxValue` 用 `Object` 装载，缺少静态类型检查可能导致 connector 间不一致。
- **决策**：在 `ConnectorColumnStatistics` javadoc 中列出 `ConnectorType` ↔ Java 装箱类型完整映射表（如 INT→Integer、TIMESTAMP→Instant、BINARY→byte[]）；连接器读取不匹配类型时**抛 `IllegalArgumentException`**，由 fe-core 转成 `UserException`。
- **替代方案**：（a）引入泛型 `ConnectorColumnStatistics<T>`——过于复杂、跨方法签名传染；（b）引入 union 类型——Java 不原生支持。
- **影响**：仅 javadoc 与运行时检查，无签名变化。

---

### D-017 — sys-table 命名统一 `$suffix`（原 U5）

- **日期**：2026-05-24
- **状态**：✅ 生效
- **关联**：[01-spi-extensions-rfc.md §10](./01-spi-extensions-rfc.md)
- **背景**：Iceberg / Paimon 各自有 sys-table（`tbl$snapshots`、`tbl$history` 等）。命名风格 `$xxx` vs `xxx@` vs `[xxx]` 跨方言不一致。
- **决策**：SPI 层固定 `$suffix` 约定。如未来出现冲突（如某 SQL dialect 把 `$` 视为变量前缀），通过 catalog property `sys_table_separator` 提供别名机制，但**不在本计划范围**。
- **影响**：所有 sys-table 实现统一遵循。

---

### D-016 — `getCredentialsForScans` 批量化（原 U4）

- **日期**：2026-05-24
- **状态**：✅ 生效
- **关联**：[01-spi-extensions-rfc.md §9](./01-spi-extensions-rfc.md)
- **背景**：原设计单 range 调一次 `getCredentialsForScan`，N 个 range 触发 N 次 STS 调用，可能撞限流。
- **决策**：签名定为 `Map<ConnectorScanRange, ConnectorCredentials> getCredentialsForScans(session, handle, List<ConnectorScanRange>)`。连接器自由决定 STS 调用粒度（1 次共享 / 按 prefix 分组 / 1:1）。fe-core 一个 scan node 一次调用。
- **替代方案**：保持单个 + 加内部缓存——把缓存策略推给每个 connector，不一致风险更高。
- **影响**：替换原 `getCredentialsForScan` 单个签名。调用位置从 `setScanParams` 移到 `createScanRangeLocations`。

---

### D-015 — `ConnectorTransaction.getTransactionId` 由连接器分配（原 U3）

- **日期**：2026-05-24
- **状态**：✅ 生效
- **关联**：[01-spi-extensions-rfc.md §7.2](./01-spi-extensions-rfc.md)
- **背景**：transaction ID 是连接器自己分配还是 fe-core 统一分配？
- **决策**：连接器分配。连接器最清楚事务 ID 与外部系统（如 HMS transaction id、Iceberg snapshot id）的对应关系。fe-core 在 `PluginDrivenTransactionManager` 用 `Map<Long, ConnectorTransaction>` 索引即可。
- **影响**：`ConnectorTransaction.getTransactionId()` 是 connector-side 字段。

---

### D-014 — 不新增 `invalidateColumnStatistics`（原 U2）

- **日期**：2026-05-24
- **状态**：✅ 生效
- **关联**：[01-spi-extensions-rfc.md §6](./01-spi-extensions-rfc.md)
- **背景**：是否给 `ConnectorMetaInvalidator` 加 `invalidateColumnStatistics(...)`？
- **决策**：暂不加。column stats 失效一并挂在 `invalidateTable` 上，避免接口表面膨胀。如后续发现频繁需要单独失效列统计，再加方法（向后兼容 default 即可）。
- **影响**：`ConnectorMetaInvalidator` 接口保持 5 个方法（catalog / database / table / partition / statistics 整张表）。

---

### D-013 — `ConnectorProcedureOps.listProcedures` 一次性返回（原 U1）

- **日期**：2026-05-24
- **状态**：✅ 生效
- **关联**：[01-spi-extensions-rfc.md §5.2](./01-spi-extensions-rfc.md)
- **背景**：connector 暴露的 procedure 列表是初始化时固定还是允许运行时变化？
- **决策**：一次性。Connector 生命周期内稳定；如外部系统的可用 procedure 集合变化，必须重新创建 catalog。
- **理由**：fe-core 可缓存该列表用于 `SHOW PROCEDURES`、autocompletion；动态变化模型复杂度不值得。
- **影响**：在 `listProcedures()` 的 javadoc 中明确写出"Lifecycle contract"。

---

### D-012 — Connector 安装初版强制重启 FE（原 D12）

- **日期**：2026-05-24
- **状态**：✅ 生效
- **关联**：[00-master-plan.md §5](./00-master-plan.md)
- **背景**：装新 connector 后是否要求重启 FE？
- **决策**：初版强制重启。原因：跨连接器共享类型可能有 classloader 缓存问题，强制重启避免难复现的 corner case。后续版本可考虑热加载。
- **影响**：文档明确 + 装包流程明确。

---

### D-011 — `RemoteDorisExternalCatalog` 不在本计划主线（原 D11）

- **日期**：2026-05-24
- **状态**：✅ 生效
- **关联**：[00-master-plan.md §5](./00-master-plan.md)
- **背景**：Doris-to-Doris federation 是否做成 connector？
- **决策**：长期目标做 connector，但**单独立项**，不在本计划主线（25 周计划中）。
- **影响**：`RemoteDorisExternalCatalog` 在 P8 不删除；保留独立路径。

---

### D-010 — `LakeSoulExternalCatalog` 在 P8 删除（原 D10）

- **日期**：2026-05-24
- **状态**：✅ 生效
- **关联**：[00-master-plan.md §5](./00-master-plan.md)
- **背景**：`CatalogFactory` 已抛 "Lakesoul catalog is no longer supported"，但类文件仍在。
- **决策**：在 P8 收尾时删除剩余 `datasource/lakesoul/` 全部类。
- **影响**：P8 task 增加 lakesoul 清理项。

---

### D-009 — API 版本号本计划永不 +1（原 D9）

- **日期**：2026-05-24
- **状态**：✅ 生效
- **关联**：[00-master-plan.md §5](./00-master-plan.md)、[01-spi-extensions-rfc.md §2.1](./01-spi-extensions-rfc.md)
- **背景**：`ConnectorProvider.apiVersion()` 何时 +1？
- **决策**：本计划范围内（25 周）保持 `apiVersion=1`，只新增 default 方法，不破坏现有签名。
- **影响**：所有 SPI 扩展必须用 default 方法。如真有不可避免的 breaking change，需走 deviation 流程并升级到 v2。

---

### D-008 — 生产强制目录式插件（原 D8）

- **日期**：2026-05-24
- **状态**：✅ 生效
- **关联**：[00-master-plan.md §5](./00-master-plan.md)
- **背景**：是否允许 built-in connector（classpath 中直接打进 FE jar）？
- **决策**：否。built-in 模式只用于测试（ServiceLoader 扫 classpath）；生产部署必须从 `connector_plugin_root` 目录加载 plugin zip。
- **影响**：FE 发行包不含 connector jar；运维流程文档要明确插件部署步骤。

---

### D-007 — kafka/kinesis/odbc/doris 不在本计划范围（原 D7）

- **日期**：2026-05-24
- **状态**：✅ 生效
- **关联**：[00-master-plan.md §5](./00-master-plan.md)
- **背景**：`datasource/` 下还有 kafka / kinesis / odbc / doris 子目录，是否一并迁移？
- **决策**：否。流式数据源（kafka/kinesis）与外部 catalog 模型不同；odbc 是 BE-driven；doris 是内部联邦。单独立项。
- **影响**：P8 不删除这 4 个子目录。

---

### D-006 — Iceberg snapshot/manifest cache 放连接器内（原 D6）

- **日期**：2026-05-24
- **状态**：✅ 生效
- **关联**：[00-master-plan.md §5](./00-master-plan.md)、[01-spi-extensions-rfc.md §8](./01-spi-extensions-rfc.md)
- **背景**：Iceberg 的 snapshot cache 和 manifest cache 是 fe-core 通用基础设施还是连接器内部细节？
- **决策**：连接器内部细节。fe-core 不感知。连接器自己管理生命周期、淘汰策略。
- **替代方案**：放 `fe-core/datasource/metacache/` 通用框架——会增加 fe-core 对 Iceberg 概念的耦合。
- **影响**：P6 迁移时把 `cache/IcebergManifestCacheLoader` 等整体搬到 `fe-connector-iceberg`。

---

### D-005 — Hudi / Iceberg-on-HMS DLA 模型方案 A（原 D5）

- **日期**：2026-05-24
- **状态**：✅ 生效
- **关联**：[00-master-plan.md §3.4](./00-master-plan.md)
- **背景**：HMS 表可能"实际是" Hudi 或 Iceberg。如何在 SPI 层建模？
- **决策**：方案 A — 用 `ConnectorTableSchema.tableFormatType` 字段（值如 `"HIVE"` / `"HUDI"` / `"ICEBERG"`），由 HMS connector 探测后填充；fe-core 据此 dispatch 到对应 `PhysicalXxxScan`。
- **替代方案**：方案 B — Hudi 作为独立 catalog type，内部委托 HMS——增加 catalog 实例数，用户混淆度高。
- **影响**：P3 hudi 和 P7 hive 迁移都依赖此模型。

---

### D-004 — HMS event pipeline 放 fe-connector-hms（原 D4）

- **日期**：2026-05-24
- **状态**：✅ 生效
- **关联**：[00-master-plan.md §3.8](./00-master-plan.md)、[01-spi-extensions-rfc.md §6](./01-spi-extensions-rfc.md)
- **背景**：21 个 HMS event 类放 fe-core 还是 fe-connector-hms？
- **决策**：fe-connector-hms。通过新 SPI 接口 `ConnectorMetaInvalidator`（在 `ConnectorContext` 暴露）回调 fe-core 的 `ExternalMetaCacheMgr`。
- **替代方案**：只把"轮询 HMS 拿事件流"放 connector，"解析事件 + 分发失效"留 fe-core——分散，不利于演化。
- **影响**：P7.2 完整迁移 21 个类 + `MetastoreEventsProcessor`。`HiveConnector.create(...)` 启动 listener 线程；`close()` 停止。

---

### D-003 — 旧 `*ExternalCatalog` 子类全部删除（原 D3）

- **日期**：2026-05-24
- **状态**：✅ 生效
- **关联**：[00-master-plan.md §5](./00-master-plan.md)
- **背景**：迁移过程中是保留旧 `IcebergExternalCatalog` 等类作为"中间形态"还是彻底删除？
- **决策**：全部删除。中间形态会让代码长期处于"两套并存"状态，维护负担、bug 风险都更大。
- **替代方案**：保留一段"deprecated 但可用"期——拒绝，因为旧实现实质上不会被维护。
- **影响**：P8 强制删除所有 `*ExternalCatalog` / `*ExternalDatabase` / `*ExternalTable` 类；前置工作是 P2-P7 把所有反向 `instanceof` 改为通用接口调用。

---

### D-002 — `PluginDrivenScanNode` 长期保持 extends `FileQueryScanNode`（原 D2）

- **日期**：2026-05-24
- **状态**：✅ 生效
- **关联**：[00-master-plan.md §5](./00-master-plan.md)
- **背景**：`PluginDrivenScanNode` 当前继承 `FileQueryScanNode`，但 JDBC / ES 本质不是文件扫描，用 `FORMAT_JNI` 兜底。是否要重构为更彻底的多态？
- **决策**：长期保持当前继承结构。JDBC / ES 的 `FORMAT_JNI` 兜底已被 ES/JDBC 验证可行。重构成本高、收益不明确。
- **影响**：所有 plugin-driven connector 走同一 scan-node 子类，简化 dispatch 逻辑。

---

### D-001 — 沿用 `SUPPORTS_PASSTHROUGH_QUERY`（原 D1）

- **日期**：2026-05-24
- **状态**：✅ 生效
- **关联**：[00-master-plan.md §5](./00-master-plan.md)
- **背景**：是否要为 SQL 透传以外的远程 query 类型（如 `query()` TVF）新增 SPI？
- **决策**：不新增。已有 `ConnectorCapability.SUPPORTS_PASSTHROUGH_QUERY` + `ConnectorTableOps.getColumnsFromQuery` 覆盖了主要场景，沿用。
- **影响**：无新增 API。

---

## 附录：决策模板

新增决策时复制以下模板到顶部（在 §详细记录 下方），并更新 §📋 索引表。

```markdown
### D-NNN — <一句话主题>

- **日期**：YYYY-MM-DD
- **状态**：✅ 生效 / 🟡 待评审 / ❌ 已废止（被 D-MMM 取代）
- **关联**：[文档章节链接]、[相关 task ID]
- **背景**：为什么需要做这个决策？触发场景是什么？
- **决策**：具体决定是什么？
- **替代方案**：考虑过哪些其他方案？为什么没选？
- **影响**：哪些代码 / 文档 / 流程会受影响？是否需要后续 follow-up？
```
