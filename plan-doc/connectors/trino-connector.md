# Connector: `trino-connector`

---

## 概况

| 项 | 值 |
|---|---|
| **catalog type 名** | `trino-connector` |
| **fe-connector 模块** | `fe/fe-connector/fe-connector-trino/` |
| **fe-core 旧路径** | `fe/fe-core/src/main/java/org/apache/doris/datasource/trinoconnector/` |
| **共享依赖** | 无 |
| **计划迁移阶段** | **P2**（首个完整 playbook 实施） |
| **当前状态** | ⏸ 未启动（P0/P1 完成后启动） |
| **完成度** | 30% |
| **主 owner** | TBD（P2 启动前指派） |

---

## 迁移 Playbook 进度

| 步骤 | 状态 | 备注 |
|---|---|---|
| 1 | 🟡 | fe-core 旧路径下 6 个顶层类 + `source/`（4 个） |
| 2 | 🟡 | fe-connector 已有 13 个类：Provider/Metadata/ScanPlanProvider/Predicate/PluginManager/...|
| 3 | ⏳ | 反向 instanceof：2 处（仅 `PhysicalPlanTranslator` 与 `LakeSoulScanNode` 附近）|
| 4 | 🟡 | 大部分 ConnectorMetadata 方法已实现，需要核对边界 |
| 5 | ⏳ | validateProperties / preCreateValidation 待补 |
| 6 | ✅ | META-INF/services 已注册 |
| 7 | ⏳ | `SPI_READY_TYPES` 未加 |
| 8 | ⏳ | gsonPostProcess 未加 trinoconnector → plugin 迁移 |
| 9 | ⏳ | registerCompatibleSubtype 未注册 |
| 10 | ⏳ | 替换 2 处反向 instanceof |
| 11 | ⏳ | PhysicalPlanTranslator 删 `TrinoConnectorExternalTable` 分支 |
| 12 | ⏳ | 0 个测试 → 需要补 |
| 13 | ⏳ | 删 `datasource/trinoconnector/` |

---

## SPI 实现完成度

| 扩展点 | 是否需要 | 实现状态 | 备注 |
|---|---|---|---|
| E1 CreateTableRequest | 🟡 | 透传到 Trino connector | Trino 自身 CREATE 透传 |
| E2 Procedures | 🟡 | Trino 有 Procedure SPI | 可考虑桥接到 ConnectorProcedureOps |
| E3 MetaInvalidator | ❌ | n/a | Trino 一般无 push notification |
| E4 Transactions | 🟡 | Trino ConnectorTransactionHandle | 桥接到新 ConnectorTransaction |
| E5 MvccSnapshot | 🟡 | 部分 Trino connector 有 | 视具体 plugin 而定 |
| E6 VendedCredentials | ❌ | n/a | |
| E7 SysTables | ❌ | n/a | |
| E8 ColumnStatistics | 🟡 | Trino 有 column stats | |
| E9 Delete/Merge sink | ❌ | 用通用 sink | |
| E10 listPartitions | 🟡 | Trino 有 partition handles | |

---

## 已知特殊性

- **第一个完整 playbook 实施样板**——爆炸半径最小（只有 2 处反向 instanceof，没有 transaction/event 负担），用于把整个迁移流程跑通。
- 包含 Trino plugin loader（`TrinoBootstrap`、`TrinoPluginManager`、`TrinoServicesProvider`）—— classloader 隔离已在 fe-connector 内部完成。
- 委托给底层 Trino plugin 处理元数据，本质是"trino-on-doris"包装层。
- 0 个测试——P2 启动前需要补单元测试 + 至少一个集成测试（用 mock Trino plugin）。

---

## 关联

- 阶段 task：P2（待启动时建 `tasks/P2-trino-connector.md`）
- 决策：D-002（scan-node 复用 FileQueryScanNode）
- 偏差：（暂无）
- 风险：R-004（classloader 隔离 — Trino plugin loader 是主要测试点）

---

## 进度日志

### 2026-05-24
- 跟踪文件建立。70% 实现已就位，等 P0/P1 完成后启动 P2 整体推动。
