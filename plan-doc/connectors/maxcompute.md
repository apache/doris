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
| **当前状态** | ⏸ 未启动 |
| **完成度** | 25% |
| **主 owner** | TBD |

---

## 迁移 Playbook 进度

| 步骤 | 状态 | 备注 |
|---|---|---|
| 1 | 🟡 | fe-core 8 个顶层（ExternalCatalog/Database/Table、MetaCache、MetadataOps、MCTransaction、SchemaCacheValue、McStructureHelper）+ `source/` 2 个 |
| 2 | 🟡 | fe-connector 13 个文件，scan 路径已迁 |
| 3 | ⏳ | 反向 instanceof：12 处（`PhysicalPlanTranslator`、`ShowPartitionsCommand`、`PartitionsTableValuedFunction` 等）|
| 4 | 🟡 | 多数 Metadata 方法已实现；事务相关待补 |
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
| E1 CreateTableRequest | 🟡 | MaxCompute 支持 partition | |
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
- 偏差：（暂无）
- 风险：R-004

---

## 进度日志

### 2026-06-06
- **W-phase（共享写/事务 SPI）全落地**（[D-021](../decisions-log.md) / [D-022](../decisions-log.md)）：maxcompute 是首个 adopter 的靶。**写接线 seam 已就位**——fe-core `Transaction` 写回调 + `PluginDrivenTransaction` 桥（W4 `759cc0874c8`）、写-plan-provider layer 进既有 plugin-driven 写路径（W5 `9ebe5e27fa4`，[DV-009](../deviations-log.md)）。**P4 adopter 待做**：搬 `datasource/maxcompute/` → `fe-connector-maxcompute`；impl `ConnectorWriteOps`(insert) / `ConnectorTransaction`(over `addCommitData` + `allocateWriteBlockRange`，仅 mc 需 block-id seam) / `ConnectorWritePlanProvider`(产 `TMaxComputeTableSink`)；翻闸 `SPI_READY_TYPES+="max_compute"` + 删 `CatalogFactory` case + GSON 兼容 + `getEngine` 分支；清 ~12 反向 instanceof；连接器测试基线。详见 [写 RFC §12](../tasks/designs/connector-write-spi-rfc.md)。

### 2026-05-24
- 跟踪文件建立。60% 实现已就位；重复类 `McStructureHelper` 已在 P1 清单。
