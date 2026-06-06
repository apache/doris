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
| **当前状态** | 🚧 Batch A 进行中（P4-T01 DDL ✅；T02 分区中）|
| **完成度** | 35% |
| **主 owner** | @me |

---

## 迁移 Playbook 进度

| 步骤 | 状态 | 备注 |
|---|---|---|
| 1 | 🟡 | fe-core 8 个顶层（ExternalCatalog/Database/Table、MetaCache、MetadataOps、MCTransaction、SchemaCacheValue、McStructureHelper）+ `source/` 2 个 |
| 2 | 🟡 | fe-connector 13 个文件，scan 路径已迁 |
| 3 | ⏳ | 反向 instanceof：12 处（`PhysicalPlanTranslator`、`ShowPartitionsCommand`、`PartitionsTableValuedFunction` 等）|
| 4 | 🟡 | Metadata 读 + **DDL（create/drop table+db，P4-T01 ✅）** 已实现；分区 listing(T02) / 写事务(批 B) 待补 |
| 5 | ⏳ | |
| 6 | ✅ | META-INF/services 已注册 |
| 7 | ⏳ | |
| 8-9 | ⏳ | gsonPostProcess 加 `max_compute → plugin` 迁移 |
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
| E4 Transactions | ✅ 需要 | `MCTransaction` 待迁 SPI | |
| E5 MvccSnapshot | ❌ | n/a | |
| E6 VendedCredentials | ❌ | n/a | |
| E7 SysTables | ❌ | n/a | |
| E8 ColumnStatistics | 🟡 | |
| E9 Delete/Merge sink | ❌ | |
| E10 listPartitions | ✅ 需要 | 走 SPI |

---

## 已知特殊性

- 12 处反向 instanceof 是 4 个连接器（trino-connector 2、hudi 0、maxcompute 12、paimon 10）中 trino-connector 的 6 倍量级，是 P4 主要工作。
- `McStructureHelper` 当前在 fe-core 和 fe-connector 中**重复**，P1 已计划删除 fe-core 版本。
- 用阿里云 ODPS SDK，classloader 隔离需要测试。
- 0 个测试 → P4 启动前需要补 mock SDK 测试。

---

## 关联

- 阶段 task：P4（待启动时建）
- 决策：D-002（scan-node 复用）
- 偏差：[DV-010](../deviations-log.md)（P4-T01 修 fe-core 转换器 CHAR/VARCHAR 长度）
- 风险：R-004

---

## 进度日志

### 2026-06-06
- **P4-T01 连接器 DDL 完成**（Batch A，gate 关、dormant、零 live 风险）：`MaxComputeConnectorMetadata` impl SPI `createTable(ConnectorCreateTableRequest)` / `dropTable` / `createDatabase` / `dropDatabase`（忠实港 legacy `MaxComputeMetadataOps`，消费 P0 request 非 fe-core `CreateTableInfo`；连接器 `McStructureHelper` ODPS DDL 原语已具备）+ 新 `MCTypeMapping.toMcType(ConnectorType)` 反向类型映射（递归 ARRAY/MAP/STRUCT）。附带修 fe-core 共享转换器 CHAR/VARCHAR 长度 [DV-010](../deviations-log.md)（用户签字）+ 回归测。守门全绿（compile + checkstyle 0 + import-gate + `ConnectorColumnConverterTest` 9/0F0E）。下一步 = P4-T02 分区 listing。
- **P4 adopter 设计批准**（[D-023](../decisions-log.md)）：5 批 / 11 task 计划见 [tasks/P4](../tasks/P4-maxcompute-migration.md)。re-grep 校正反向引用 **~19**（旧称「12」失真；W-phase 已灭 `Coordinator`/`LoadProcessor`/`FrontendServiceImpl` 3 热点 txn 站）。连接器现状核实：写 SPI **全缺**（无 `getWritePlanProvider`/`beginTransaction`/`ConnectorWriteOps`）、DDL **缺**（仅 `McStructureHelper` 低层 helper）、分区 listing **缺**；`MCTransaction` 已含 W2 `addCommitData(byte[])`，`TMaxComputeTableSink` 18 字段齐。**下一步 = Batch A**（P4-T01 DDL + P4-T02 分区，gate 关）。
- **W-phase（共享写/事务 SPI）全落地**（[D-021](../decisions-log.md) / [D-022](../decisions-log.md)）：maxcompute 是首个 adopter 的靶。**写接线 seam 已就位**——fe-core `Transaction` 写回调 + `PluginDrivenTransaction` 桥（W4 `759cc0874c8`）、写-plan-provider layer 进既有 plugin-driven 写路径（W5 `9ebe5e27fa4`，[DV-009](../deviations-log.md)）。**P4 adopter 待做**：搬 `datasource/maxcompute/` → `fe-connector-maxcompute`；impl `ConnectorWriteOps`(insert) / `ConnectorTransaction`(over `addCommitData` + `allocateWriteBlockRange`，仅 mc 需 block-id seam) / `ConnectorWritePlanProvider`(产 `TMaxComputeTableSink`)；翻闸 `SPI_READY_TYPES+="max_compute"` + 删 `CatalogFactory` case + GSON 兼容 + `getEngine` 分支；清 ~12 反向 instanceof；连接器测试基线。详见 [写 RFC §12](../tasks/designs/connector-write-spi-rfc.md)。

### 2026-05-24
- 跟踪文件建立。60% 实现已就位；重复类 `McStructureHelper` 已在 P1 清单。
