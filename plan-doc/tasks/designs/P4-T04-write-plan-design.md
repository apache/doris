# P4-T04 设计 — 连接器写计划（`ConnectorWritePlanProvider.planWrite`，gate 关 dormant）

> 批次 B 次 task（T03 后继）。事实底座见「Recon 事实」（4 路 subagent + 主线核读 `PluginDrivenTableSink`，2026-06-06）。
> 关联：[P4 计划 T04](../P4-maxcompute-migration.md)、[写 RFC §5.5/§6/§7/§9](./connector-write-spi-rfc.md)、[P4-T03 设计](./P4-T03-write-txn-design.md)（T03 留的 `setWriteSession` 槽）、[DV-009]（W5 planWrite layer）、OQ-2。

---

## Problem

`max_compute` 连接器写**计划**面缺失：无 `Connector.getWritePlanProvider` / `ConnectorWritePlanProvider.planWrite` / ODPS 写 session 创建。T04 把 legacy 写计划（`MCTransaction.beginInsert` 建写 session + `MaxComputeTableSink.bindDataSink/setWriteContext` 产 `TMaxComputeTableSink`）港入连接器，over W5 opaque-sink seam。**gate 关、dormant**（`max_compute` 未进 `SPI_READY_TYPES`，executor/binding 未接线），零 live 风险。

**OQ-2 = 本 task 核心难点**：W5 的 `PluginDrivenTableSink.bindViaWritePlanProvider()` 现以**空** writeContext + 硬编码 `overwrite=false` 调 `planWrite`；legacy 经 `MCInsertExecutor.beforeExec` 运行期注入的 `txn_id`/`write_session_id`、以及 overwrite/静态分区 context，需在 plugin-driven 写侧**重建**。

---

## Recon 事实（code-grounded，2026-06-06）

### A. SPI 写-plan 面（`fe-connector-api`，W1 已建，MC 是首个 adopter）
- `write/ConnectorWritePlanProvider.java`：`ConnectorSinkPlan planWrite(ConnectorSession session, ConnectorWriteHandle handle)`（单方法）。
- `write/ConnectorSinkPlan.java`：`ConnectorSinkPlan(TDataSink dataSink)` + `getDataSink()`（包 opaque thrift）。
- `handle/ConnectorWriteHandle.java`：`getTableHandle()` / `getColumns()` / `isOverwrite()` / `getWriteContext():Map<String,String>`（**自由 map**）。
- `Connector.java`：`default getWritePlanProvider()` 回 null（getScanPlanProvider 同形，镜像之）。
- `ConnectorSession.java`：`getCurrentTransaction():Optional<ConnectorTransaction>`（default empty）、`allocateTransactionId()`（T03 加）、`getCatalogProperties()`/`getProperty()`。
- **全连接器无 `ConnectorWritePlanProvider` impl** → MC 首个，无模板。

### B. W5 接线（fe-core `planner/PluginDrivenTableSink`）—— OQ-2 seam
- `bindDataSink(Optional<InsertCommandContext> insertCtx)`（:180）= 入口，于 **`finalizeSink`** 调（译后、`beforeExec` 前，**携 insertCtx**）。plan-provider 模式 → `bindViaWritePlanProvider()`（:210）。
- `bindViaWritePlanProvider()`（:210-215）**现忽略 insertCtx**：`new PluginDrivenWriteHandle(tableHandle, connectorColumns, false, Collections.emptyMap())` → `planWrite()` → `this.tDataSink = sinkPlan.getDataSink()`。类注释明示「per-connector adopter (P4+) 从自己的 insert context 填，W-phase 只立空 seam」。
- 两构造：config-bag（`writeConfig`，JDBC/hive-file 用）与 plan-provider（`writePlanProvider`+session+tableHandle+columns，:134）互斥。**MC 走 plan-provider；JDBC 不受影响**。
- `PluginDrivenInsertCommandContext extends BaseExternalTableInsertCommandContext`：**仅 `overwrite` 标志，无静态分区**。
- `PhysicalPlanTranslator.visitPhysicalConnectorTableSink`（:645-704）：`writePlanProvider=connector.getWritePlanProvider(); if(!=null) new PluginDrivenTableSink(targetTable, writePlanProvider, connSession, providerTableHandle, connectorColumns)`。

### C. Executor 生命周期序（OQ-2 关键）
`beginTransaction`（`txnId=transactionManager.begin()`，**译前**）→ **PLAN TRANSLATE** → `finalizeSink`→`sink.bindDataSink(insertCtx)` → `beforeExec` → `execImpl`（coordinator 下发）→ `onComplete`（finishInsert）→ commit。
⇒ **`txn_id` 译前已生；legacy `write_session_id` 译后于 `MCInsertExecutor.beforeExec` 建**（`(MCTransaction)txnMgr.getTransaction(txnId)).beginInsert(table,insertCtx)` + `mcTableSink.setWriteContext(txnId, tx.getWriteSessionId())`，:62-71）。

### D. Legacy 写计划逻辑（港的源）
- `MCTransaction.beginInsert`（:87-142）：`TableIdentifier tableId=catalog.getOdpsTableIdentifier(db,name)`；`isDynamicPartition=!table.getPartitionColumns().isEmpty()`；静态分区由 `MCInsertCommandContext.getStaticPartitionSpec()`（map）→ 按**分区列序**拼 `"col=val,col=val"`；`isOverwrite=mcCtx.isOverwrite()`。
  ```java
  TableWriteSessionBuilder b = new TableWriteSessionBuilder()
      .identifier(tableId).withSettings(catalog.getSettings())
      .withMaxFieldSize(catalog.getMaxFieldSize())
      .withArrowOptions(ArrowOptions.newBuilder().withDatetimeUnit(MILLI).withTimestampUnit(MILLI).build());
  if (isStaticPartition) b.partition(new PartitionSpec(staticPartitionSpecStr));
  else if (isDynamicPartition) b.withDynamicPartitionOptions(DynamicPartitionOptions.createDefault());
  if (isOverwrite) b.overwrite(true);
  TableBatchWriteSession ws = b.buildBatchWriteSession();
  writeSessionId = ws.getId(); nextBlockId.set(0);
  ```
- `MaxComputeTableSink.bindDataSink`：`tSink` set `properties(catalog.getProperties())`/`endpoint`/`project(defaultProject)`/`tableName`/`quota`/`connectTimeout`/`readTimeout`/`retryCount`/`partitionColumns`（**取自 table 分区列名**，非 insert 列）/`staticPartitionSpec`（map，field 10）→ `tDataSink=new TDataSink(MAXCOMPUTE_TABLE_SINK).setMaxComputeTableSink(tSink)`。`setWriteContext(txnId, writeSessionId)` 仅盖 `txn_id`+`write_session_id`。

### E. thrift `TMaxComputeTableSink`（`gensrc/thrift/DataSinks.thrift:586`，18 字段）
`session_id`(1, legacy tunnel, **不用**) · access_key(2)/secret_key(3)/endpoint(4)/project(5)/table_name(6)/quota(7) · `block_id_start`(8)/`block_id_count`(9)（**运行期** BE 经 txn_id 调 T03 `allocateWriteBlockRange` 分配，**planWrite 不盖**）· `static_partition_spec`(10, map) · connect/read_timeout(11/12)/retry_count(13) · `partition_columns`(14, list) · `write_session_id`(15) · `properties`(16, map, 含鉴权) · max_write_batch_rows(17, deprecated) · `txn_id`(18, 注释「for runtime block_id allocation」)。

### F. 连接器脚手架（已就位）
- `MaxComputeConnectorTransaction.setWriteSession(String writeSessionId, TableIdentifier tableIdentifier, EnvironmentSettings settings)`（T03 槽，:92）+ `getTransactionId()`。
- `MaxComputeTableHandle`：`getDbName/getTableName/getOdpsTable():Table/getTableIdentifier():TableIdentifier`。
- `MaxComputeScanPlanProvider`（:157）唯一建 `EnvironmentSettings.newBuilder().withCredentials().withServiceEndpoint(connector.getClient().getEndpoint()).withQuotaName(connector.getQuota()).withRestOptions().build()`；构造仅持 `MaxComputeDorisConnector`。
- `MaxComputeDorisConnector`：`getScanPlanProvider()` 模式（`ensureInitialized()`+持有字段）；持 `odps/endpoint/defaultProject/quota/structureHelper/properties` + getters。
- `MaxComputeConnectorMetadata`：`beginTransaction(session)`（T03）+ `getTableHandle(session,db,tbl)`（产 `MaxComputeTableHandle`，含 live `Table`+`TableIdentifier`）；**未** impl `ConnectorWriteOps`。
- `MCConnectorProperties`：ENDPOINT/PROJECT/ACCESS_KEY/SECRET_KEY/QUOTA/CONNECT_TIMEOUT/READ_TIMEOUT/RETRY_COUNT/`MAX_FIELD_SIZE`(8388608)/MAX_WRITE_BATCH_ROWS/REGION/TUNNEL/auth.type。

---

## OQ-2 解法 = **Approach A（planWrite 一处定，finalizeSink 时机）**

`planWrite` 于 `finalizeSink`（bindDataSink）跑——此时 `txnId` 已生（beginTransaction，译前）、ODPS 写 session 可就地建——故 **planWrite 一处做完**：
1. 读 `handle.isOverwrite()` + `handle.getWriteContext()`（静态分区）；
2. 建 `EnvironmentSettings`（连接器侧，见决策 D-3）；
3. 港 `beginInsert` 建 ODPS 写 session → `writeSessionId`；
4. `session.getCurrentTransaction()` → `MaxComputeConnectorTransaction` → `setWriteSession(writeSessionId, tableId, settings)` 绑定（T03 槽）；
5. 建 `TMaxComputeTableSink`：静态字段（D 节）+ `static_partition_spec` + `partition_columns` + `write_session_id` + `txn_id`(= `tx.getTransactionId()`)；**不盖 block_id**（运行期 T03）；
6. 回 `ConnectorSinkPlan(new TDataSink(MAXCOMPUTE_TABLE_SINK).setMaxComputeTableSink(tSink))`。

**否决 Approach B（泛化 legacy 运行期注入）**：给 `PluginDrivenTableSink` 加 `setWriteContext` + executor 存 sink 引用 + `beforeExec` 注入 + 新 SPI「beforeExec 产运行期 context」。理由：写 session 建在 finalizeSink vs beforeExec **语义无差**（均 FE 侧、译后、BE 前）；A 单 locus、无 sink 后改、无新 SPory/executor hook（**Rule 2**）。handoff 亦定 A。

> **依赖（Batch C 接线，非 T04）**：planWrite 的 `getCurrentTransaction()` 要返 MC txn ⇒ Batch C 的 `beginTransaction` 须 `writeOps.beginTransaction(session)` 并把 connectorTx 置于 `ConnectorSessionImpl`（加 setCurrentTransaction）。dormant 期 planWrite 不跑，correct-by-design。

---

## 决策（fork，**用户签字 2026-06-06**）

### D-1（OQ-2 架构）= **Approach A**（见上）。handoff 预定，列 B 为否决备选。

### D-2（fe-core seam 填充范围）= **(a) 含 seam fill**（✅ **用户签字 2026-06-06**）
T04 含 fe-core W5 seam 填充：① `PluginDrivenTableSink.bindViaWritePlanProvider(insertCtx)` 读 insertCtx 填 handle 的 `overwrite` + `writeContext`(静态分区)；② `PluginDrivenInsertCommandContext`（或基类）加**通用** `Map<String,String> staticPartitionSpec` + getter（dormant，binding 期填充归 Batch C/D）。**理由**：OQ-2 是 T04 核心（handoff/DV-009）；这是「填 W-phase 立的空 seam」非「改 W-phase 决策」；zero live（仅 plan-provider 分支、dormant）。T03 已先例改 fe-core（ConnectorSession/Impl）。
- **(b) 纯连接器侧**（否决）：全部 seam 填充挪 Batch C；T04 不自洽（planWrite 写就但无 fe-core 喂数据）。

> **执行节奏（用户签字 2026-06-06）**：本 session = 设计 + 签字，**不写实现**；下一 fresh session 按本文 Ordered TODO 落地（split-session 节奏，playbook §7.1→§7.2）。

### D-3（EnvironmentSettings 复用）= **抽到连接器 `MaxComputeDorisConnector.getSettings()`**（镜像 legacy `catalog.getSettings()`），scan/write provider 共用。轻动 scan provider（把 :157 构造上移）。备选：write provider 自建（~5 行重复，Rule 3 不碰 scan）。倾向抽出（单源、对齐 legacy）。**次要，可主线定**。

### D-4（insert 机制面）= **`supportsInsert()`=true，其余最小化**。`getWriteConfig`/`beginInsert`/`finishInsert`：MC 走 plan-provider（sink 经 planWrite）、commit 经 T03 `ConnectorTransaction.commit()`，故 beginInsert/finishInsert 对 MC **无实质活**（no-op 或不实现）。最终以 Batch C executor 实际调用面为准；T04 先 `supportsInsert`=true + 必要 no-op。**次要，可主线定**。

### D-5（writeContext 编码）= **静态分区直接作 `getWriteContext()` 的 col→val map**；overwrite 经 `isOverwrite()`。planWrite 据分区列序拼 `"col=val,..."` 喂 `PartitionSpec`、并原样 set 入 `static_partition_spec`(field 10)。**次要，可主线定**。

---

## legacy → T04 SPI 映射

| legacy | T04 | 备注 |
|---|---|---|
| `MCTransaction.beginInsert`（建写 session）| `MaxComputeWritePlanProvider.planWrite` 内港 | 时机 beforeExec→finalizeSink（均译后，无差）|
| `MaxComputeTableSink.bindDataSink`（静态字段）| planWrite 建 `TMaxComputeTableSink` 静态字段 | endpoint/project/tableName/quota/timeouts/partitionColumns/staticPartitionSpec/properties |
| `MaxComputeTableSink.setWriteContext(txnId,wsid)`（运行期注入）| planWrite 直盖 `txn_id`(=`tx.getTransactionId()`)+`write_session_id` | **A 解法**：一处盖，无运行期 hook |
| `MCInsertExecutor.beforeExec`（cast+注入）| **消失**（逻辑入 planWrite）| OQ-1：验 MCInsertExecutor 成死代码（Batch D/T08）|
| `catalog.getSettings()` | `connector.getSettings()`（D-3 抽出）| EnvironmentSettings |
| `catalog.getMaxFieldSize()` | `MCConnectorProperties.MAX_FIELD_SIZE`(8388608) | |
| block_id 分配 | **不在 planWrite**（T03 `allocateWriteBlockRange` 运行期）| txn_id 使能之 |
| `Connector.getWritePlanProvider`（缺）| `MaxComputeDorisConnector.getWritePlanProvider()` | 镜像 getScanPlanProvider |

---

## Why
- **A 单 locus**：finalizeSink 时 txn_id 已在、session 可就地建 → 无需 sink 后改 / 运行期 hook（Rule 2）。
- **填空 seam ≠ 改 W-phase**：W5 注释明示 adopter 填 writeContext；T04 是 adopter（不违「别回头改 W-phase」）。
- **静态分区入通用 context**：放基类 `Map<String,String>`，未来 hive/iceberg 复用（非 MC 特例）。
- **commit 归 T03**：finishInsert 对 MC no-op；`ConnectorTransaction.commit()`（T03）落 `session.commit`。

---

## Deviations / 坑（R12 不静默）
- **DV（提案 DV-012）**：legacy `partition_columns` 取 `targetTable.getPartitionColumns()`（fe-core Doris Column）；连接器侧取 `MaxComputeTableHandle.getOdpsTable()` 的 ODPS 分区列（odps-sdk）——**源不同、值同**（分区列名）。doc-sync 入 deviations-log。
- **DV-009（已存）**：W5 planWrite layer；T04 是其 adopter 落地。
- **import-gate**（坑5）：连接器禁 `common.*`（`Config`/`UserException`）；异常用 `DorisConnectorException`。允许 `thrift.*`（`TMaxComputeTableSink`/`TDataSink`/`TDataSinkType`）。
- **fe-core 侧改**（D-2a）：`PluginDrivenTableSink`/`PluginDrivenInsertCommandContext` 在 fe-core，**不受 import-gate**（gate 只扫连接器→fe-core 单向）。
- **ODPS SDK jar**（坑10）：写 session 类在 odps-sdk-table-api（`EnvironmentSettings`/`TableWriteSessionBuilder`/`TableBatchWriteSession`/`ArrowOptions`/`DynamicPartitionOptions`）；`PartitionSpec`/`TableIdentifier` 在 odps-sdk-commons。**实现前 javap 核** `.identifier/.withMaxFieldSize/.withArrowOptions/.partition/.withDynamicPartitionOptions/.overwrite/.buildBatchWriteSession`、`TableBatchWriteSession.getId`。

---

## Risk Analysis
- **R-dormant**：T04 全 dormant（plan-provider 分支无 live caller、`max_compute` 未翻闸）。风险=Batch C 接线遗漏（getCurrentTransaction 喂 txn / binding 填 staticPartitionSpec）→ 编入 Batch C 检查单。
- **R-OQ2-时机**：A 把写 session 建挪 finalizeSink（legacy beforeExec）。二者均译后/BE 前，**核读确认无中间态依赖**（insertCtx 译后即定、txnId 译前即定）。
- **R-JDBC 回归**：seam 填充仅动 plan-provider 分支；config-bag（JDBC/hive-file）零触。守门 fe-core compile + 既有测护。
- **R-static-partition 未填**：D-2a 加字段但 binding 期填充归 Batch C/D；翻闸前 INSERT OVERWRITE PARTITION 静态分区**不可用**——**设计意图**（dormant），Batch D binding 接线补，编入检查单。

---

## Test Plan（R12 不静默）
- **T04 gate**（与 T01/T02/T03 一致）：连接器 compile（`-pl :fe-connector-maxcompute -am`）+ checkstyle 0 + import-gate 0；**改 fe-core ⇒ `-pl :fe-connector-maxcompute,:fe-core -am`**（坑6）；读真实 BUILD/MVN_EXIT/CS_EXIT（坑7）。
- **单测延至 P4-T10**（JUnit5 手写替身，无 mockito）：planWrite golden（静态/动态分区 builder 参数、overwrite、`TMaxComputeTableSink` 字段、setWriteSession 绑定后 txn.commit 通）。T04 不加测（与计划一致，非静默跳过）。

---

## Ordered TODO
1. **写前核**：javap 核 odps-sdk 写 session API（坑10，见 Deviations）。
2. **连接器**：新建 `MaxComputeWritePlanProvider implements ConnectorWritePlanProvider`：`planWrite`（OQ-2 解法 A 六步）；持 `MaxComputeDorisConnector`（镜像 scan provider 构造）。
3. **连接器**：`MaxComputeDorisConnector` 加 `writePlanProvider` 字段 + `getWritePlanProvider()`（ensureInitialized 模式）+ `getSettings()`（D-3 抽出 EnvironmentSettings）。
4. **连接器**：`MaxComputeConnectorMetadata` impl `ConnectorWriteOps.supportsInsert()`=true（+ D-4 必要 no-op）。
5. **fe-core seam（D-2a，待签字）**：`PluginDrivenTableSink.bindViaWritePlanProvider(insertCtx)` 读 insertCtx 填 handle overwrite+writeContext；`PluginDrivenInsertCommandContext`/基类加 `staticPartitionSpec` map + getter。
6. **gate**：compile（后台）+ checkstyle + import-gate，读真实 EXIT。
7. **doc-sync + 独立 commit `[P4-T04]`**（用户定时机）：P4 计划 T04 ⏳→✅、PROGRESS、HANDOFF、decisions（D-025 T04 forks）、deviations（DV-012 partition_columns 源）。
