# Connector: `maxcompute`

---

## 概况

| 项 | 值 |
|---|---|
| **catalog type 名** | `max_compute` |
| **fe-connector 模块** | `fe/fe-connector/fe-connector-maxcompute/` |
| **fe-core 旧路径** | `fe/fe-core/src/main/java/org/apache/doris/datasource/maxcompute/` |
| **共享依赖** | 无 |
| **计划迁移阶段** | **P4** |
| **当前状态** | 🚧 **Batch C 翻闸完成**（T05 image-compat + T06a 写接线/UT + **T06b flip ✅** `SPI_READY_TYPES += "max_compute"`，gate 全绿 [D-027]）；下一 = **Batch D**（删 legacy 子系统 + drop fe-core odps 依赖，**待用户 live ODPS 验证后做**）|
| **完成度** | 75% |
| **主 owner** | @me |

---

## 迁移 Playbook 进度

| 步骤 | 状态 | 备注 |
|---|---|---|
| 1 | 🟡 | fe-core 8 个顶层（ExternalCatalog/Database/Table、MetaCache、MetadataOps、MCTransaction、SchemaCacheValue、McStructureHelper）+ `source/` 2 个 |
| 2 | 🟡 | fe-connector 13 个文件，scan 路径已迁 |
| 3 | ⏳ | 反向 instanceof：12 处（`PhysicalPlanTranslator`、`ShowPartitionsCommand`、`PartitionsTableValuedFunction` 等）|
| 4 | ✅ | Metadata 读 + **DDL（P4-T01 ✅）** + **分区 listing（P4-T02 ✅）** + **写/事务 `ConnectorTransaction`+`beginTransaction`（P4-T03 ✅）** + **写计划 `getWritePlanProvider`→`planWrite`→`TMaxComputeTableSink`（P4-T04 ✅，OQ-2=Approach A）** 全实现（cutover 接线归 Batch C）|
| 5 | ⏳ | |
| 6 | ✅ | META-INF/services 已注册 |
| 7 | ⏳ | |
| 8-9 | ✅ | T05：GSON `registerCompatibleSubtype`（catalog/db/table）迁 PluginDriven（image 兼容）|
| 10 | ⏳ | 清理 12 处反向 instanceof |
| 11 | ⏳ | PhysicalPlanTranslator 删 `MaxComputeExternalTable` 分支 |
| 12 | ⏳ | 0 个测试 |
| 13 | ⏳ | 删 `datasource/maxcompute/` |

---

## SPI 实现完成度

| 扩展点 | 是否需要 | 实现状态 | 备注 |
|---|---|---|---|
| E1 CreateTableRequest | ✅ 需要 | ✅ P4-T01 | `createTable(request)` 港 legacy（identity 分区 / hash bucket / lifecycle / `mc.tblproperty.*`）|
| E2 Procedures | ❌ | n/a | |
| E3 MetaInvalidator | ❌ | n/a | |
| E4 Transactions | ✅ 需要 | ✅ P4-T03（事务）+ P4-T04（写计划）| `beginTransaction`+`MaxComputeConnectorTransaction`（`addCommitData`[TBinaryProtocol]/block-alloc/commit/rollback/getUpdateCnt）✅；`getWritePlanProvider`→`MaxComputeWritePlanProvider.planWrite`→`TMaxComputeTableSink`（建写 session + `setWriteSession` 绑 txn + 盖 txn_id/write_session_id，OQ-2=Approach A）✅ |
| E5 MvccSnapshot | ❌ | n/a | |
| E6 VendedCredentials | ❌ | n/a | |
| E7 SysTables | ❌ | n/a | |
| E8 ColumnStatistics | 🟡 | |
| E9 Delete/Merge sink | ❌ | |
| E10 listPartitions | ✅ 需要 | ✅ P4-T02 | `listPartitions/Names/Values` 直取 ODPS `getPartitions`，filter 忽略返全量（OQ-4 无自有 cache）|

---

## 已知特殊性

- 12 处反向 instanceof 是 4 个连接器（trino-connector 2、hudi 0、maxcompute 12、paimon 10）中 trino-connector 的 6 倍量级，是 P4 主要工作。
- `McStructureHelper` 当前在 fe-core 和 fe-connector 中**重复**，P1 已计划删除 fe-core 版本。
- 用阿里云 ODPS SDK，classloader 隔离需要测试。
- 0 个测试 → P4 启动前需要补 mock SDK 测试。

---

## 关联

- 阶段 task：P4（待启动时建）
- 决策：[D-025](../decisions-log.md)（P4-T04 写计划 5 决策：Approach A / seam fill / 抽 getSettings / supportsInsert / 静态分区 map）、[D-024](../decisions-log.md)（P4-T03 两 fork：txn id 分配器 / 写 session 挪 T04）、D-002（scan-node 复用）
- 偏差：[DV-012](../deviations-log.md)（P4-T04 partition_columns 取 ODPS 表列，源不同值同）、[DV-011](../deviations-log.md)（P4-T03 block 上限常量 + 异常类型）、[DV-010](../deviations-log.md)（P4-T01 修 fe-core 转换器 CHAR/VARCHAR 长度）
- 风险：R-004

---

## 进度日志

### 2026-06-07
- **P4-T06b 翻闸落地（Batch C 完成，唯一 live 切点）= max_compute 进 SPI**：`CatalogFactory.SPI_READY_TYPES += "max_compute"`(:52) + 删 legacy `case "max_compute"`(原 :146-149) + 删 unused `MaxComputeExternalCatalog` import + 注释去 max_compute。翻闸后 `max_compute` catalog→`PluginDrivenExternalCatalog`、table→`PluginDrivenExternalTable`（GSON T05 兼容），读/写/DDL/分区/show 全经 SPI；legacy `instanceof MaxCompute*` 分支全失配（dead）。gate 全绿（compile BUILD SUCCESS/MVN_EXIT=0 + checkstyle 0/CS_EXIT=0 + import-gate 0，真实 EXIT 核）。**前继 T05/T06a 已 commit**（image-compat + dormant 写接线 W-a..d/G1–G5 + UT）。**SPI_READY ✅**。**2 决策 [D-027]**：flip 先行/移除待 live 验证；fe-core 仅删直接 odps 声明（transitive-via-fe-common 留）。Batch D 完整移除闭包（21 删 / ~30 清 / keep / pom drop）已 verify → [Batch D 移除设计](../tasks/designs/P4-batchD-maxcompute-removal-design.md)，**执行前置门 = 用户跑 `OdpsLiveConnectivityTest`（4 个 `MC_*` 环境变量）+ 手测 smoke 绿**。

### 2026-06-06
- **P4-T04 连接器写计划完成 = Batch A+B 全完成**（Batch B 收尾，gate 关、dormant、零 live 风险）：新建 `MaxComputeWritePlanProvider.planWrite`（**OQ-2=Approach A**：finalizeSink 一处建 ODPS 写 session → `session.getCurrentTransaction()`→`MaxComputeConnectorTransaction.setWriteSession` 绑 txn → 盖 `TMaxComputeTableSink`（静态字段 + `static_partition_spec` + `partition_columns`(ODPS 表列) + `write_session_id` + `txn_id`），无运行期注入 hook）+ `MaxComputeDorisConnector.getSettings()`（D-3 抽出，scan/write 共用，镜像 legacy 单 settings）/`getWritePlanProvider()` + `supportsInsert()`=true（D-4，余 throwing-default 待 Batch C）+ fe-core seam（`PluginDrivenTableSink.bindViaWritePlanProvider(insertCtx)` 读 overwrite+静态分区 / `PluginDrivenInsertCommandContext.staticPartitionSpec`，非基类避 `MCInsertCommandContext` shadow）。5 决策 [D-025]；偏差 [DV-012]（partition_columns 取 ODPS 表列）。坑10 javap 全核；写路径 ArrowOptions MILLI/MILLI（≠scan）；block_id 不盖（运行期 T03）。守门全绿（compile BUILD SUCCESS + checkstyle 0 + import-gate，真实 EXIT）。单测延 P4-T10。下一步 = **Batch C 翻闸**（live，前置 R-004 防御测）。
- **P4-T03 连接器写/事务 SPI 完成**（Batch B 启，gate 关、dormant）：新建 `MaxComputeConnectorTransaction`（港 `MCTransaction`：`addCommitData`[TBinaryProtocol 红线]/block-alloc/commit/rollback/getUpdateCnt）+ `MaxComputeConnectorMetadata.beginTransaction`，over W4 委派。两 fork [D-024]：txn id 经新增 `ConnectorSession.allocateTransactionId()`（尊重 [D-015]）/ 写 session 创建挪 T04。偏差 [DV-011]（block 上限常量、`DorisConnectorException`）。JDBC 仅半样板（无 `ConnectorTransaction`），MC 首个有状态事务 adopter。守门全绿（compile + checkstyle 0 + import-gate，真实 EXIT）。单测延 P4-T10。下一步 = P4-T04 写计划。
- **P4-T02 连接器分区 listing 完成**（Batch A 收尾，gate 关、dormant、零 live 风险）：`MaxComputeConnectorMetadata` impl SPI `listPartitionNames`/`listPartitions`/`listPartitionValues`，三方法直取 `structureHelper.getPartitions(odps, db, tbl)`：names = `PartitionSpec.toString(false,true)`（镜像 legacy `MaxComputeExternalCatalog:283`/`MaxComputeExternalTable:201`）；`listPartitions` filter **忽略**返全量（values 由 `PartitionSpec.keys()`/`get(k)`、props=emptyMap）；`listPartitionValues` 按入参列序 `spec.get(col)`。**OQ-4 定**：不建连接器自有 cache，直取 ODPS（Rule 2 不投机）。**保真**：legacy 双路径分歧（catalog 无 emptiness guard / table 有），SPI 锚 catalog SHOW PARTITIONS 故不加 guard；写前 javap 验 ODPS `PartitionSpec` API。测试延至 **P4-T10**（无 mockito 基线）。守门全绿（compile BUILD SUCCESS + checkstyle 0 + import-gate，真实 EXIT 核验）。下一步 = Batch B（P4-T03 写/事务 SPI）。
- **P4-T01 连接器 DDL 完成**（Batch A，gate 关、dormant、零 live 风险）：`MaxComputeConnectorMetadata` impl SPI `createTable(ConnectorCreateTableRequest)` / `dropTable` / `createDatabase` / `dropDatabase`（忠实港 legacy `MaxComputeMetadataOps`，消费 P0 request 非 fe-core `CreateTableInfo`；连接器 `McStructureHelper` ODPS DDL 原语已具备）+ 新 `MCTypeMapping.toMcType(ConnectorType)` 反向类型映射（递归 ARRAY/MAP/STRUCT）。附带修 fe-core 共享转换器 CHAR/VARCHAR 长度 [DV-010](../deviations-log.md)（用户签字）+ 回归测。守门全绿（compile + checkstyle 0 + import-gate + `ConnectorColumnConverterTest` 9/0F0E）。下一步 = P4-T02 分区 listing。
- **P4 adopter 设计批准**（[D-023](../decisions-log.md)）：5 批 / 11 task 计划见 [tasks/P4](../tasks/P4-maxcompute-migration.md)。re-grep 校正反向引用 **~19**（旧称「12」失真；W-phase 已灭 `Coordinator`/`LoadProcessor`/`FrontendServiceImpl` 3 热点 txn 站）。连接器现状核实：写 SPI **全缺**（无 `getWritePlanProvider`/`beginTransaction`/`ConnectorWriteOps`）、DDL **缺**（仅 `McStructureHelper` 低层 helper）、分区 listing **缺**；`MCTransaction` 已含 W2 `addCommitData(byte[])`，`TMaxComputeTableSink` 18 字段齐。**下一步 = Batch A**（P4-T01 DDL + P4-T02 分区，gate 关）。
- **W-phase（共享写/事务 SPI）全落地**（[D-021](../decisions-log.md) / [D-022](../decisions-log.md)）：maxcompute 是首个 adopter 的靶。**写接线 seam 已就位**——fe-core `Transaction` 写回调 + `PluginDrivenTransaction` 桥（W4 `759cc0874c8`）、写-plan-provider layer 进既有 plugin-driven 写路径（W5 `9ebe5e27fa4`，[DV-009](../deviations-log.md)）。**P4 adopter 待做**：搬 `datasource/maxcompute/` → `fe-connector-maxcompute`；impl `ConnectorWriteOps`(insert) / `ConnectorTransaction`(over `addCommitData` + `allocateWriteBlockRange`，仅 mc 需 block-id seam) / `ConnectorWritePlanProvider`(产 `TMaxComputeTableSink`)；翻闸 `SPI_READY_TYPES+="max_compute"` + 删 `CatalogFactory` case + GSON 兼容 + `getEngine` 分支；清 ~12 反向 instanceof；连接器测试基线。详见 [写 RFC §12](../tasks/designs/connector-write-spi-rfc.md)。

### 2026-05-24
- 跟踪文件建立。60% 实现已就位；重复类 `McStructureHelper` 已在 P1 清单。
