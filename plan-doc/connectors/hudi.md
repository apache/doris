# Connector: `hudi`

---

## 概况

| 项 | 值 |
|---|---|
| **catalog type 名** | （依附 hms；通过 `tableFormatType=HUDI` 区分，见 D-005）|
| **fe-connector 模块** | `fe/fe-connector/fe-connector-hudi/` |
| **fe-core 旧路径** | `fe/fe-core/src/main/java/org/apache/doris/datasource/hudi/` |
| **共享依赖** | `fe-connector-hms`（通过 HMS 拿元数据） |
| **计划迁移阶段** | **P3** |
| **当前状态** | ⏸ 未启动 |
| **完成度** | 20% |
| **主 owner** | TBD |

---

## 迁移 Playbook 进度

| 步骤 | 状态 | 备注 |
|---|---|---|
| 1 | 🟡 | fe-core 9 个顶层类（cache key、schema cache、MvccSnapshot、partition utils、HudiUtils）+ `source/` 6 个（含 4 个 incremental relation）|
| 2 | 🟡 | fe-connector 9 个文件：Provider/Metadata/ScanPlanProvider/ScanRange/TableHandle/...|
| 3 | ✅ | 反向 instanceof：0 处（hudi 寄生在 Hive 上，没有独立 `HudiExternalCatalog`）|
| 4 | 🟡 | ConnectorMetadata 骨架完成；incremental query 路径未补 |
| 5 | ⏳ | |
| 6 | ✅ | META-INF/services 已注册 |
| 7 | ⏳ | `SPI_READY_TYPES` 未加（hudi 不能独立创建 catalog）|
| 8-9 | 🚫 | hudi 无独立 catalog；走 D-005 的 `tableFormatType` 模型 |
| 10 | ⏳ | 替换 `visitPhysicalHudiScan` 中 `HMSExternalTable.dlaType=HUDI` 检查 |
| 11 | ⏳ | 删 `HudiScanNode`，由 `PluginDrivenScanNode` + `HudiScanPlanProvider` 承接 |
| 12 | ⏳ | 0 个测试 |
| 13 | ⏳ | 删 `datasource/hudi/` |

---

## SPI 实现完成度

| 扩展点 | 是否需要 | 实现状态 | 备注 |
|---|---|---|---|
| E1 CreateTableRequest | ❌ | n/a | hudi 不支持 CREATE TABLE |
| E2 Procedures | 🟡 | hudi 有 `archive_log` 等 procedure | 后续可考虑 |
| E3 MetaInvalidator | 🟡 | 通过 HMS event 同步 | 复用 `fe-connector-hms` 的 invalidator |
| E4 Transactions | 🟡 | hudi 有 timeline | 暂用 no-op |
| E5 MvccSnapshot | ✅ 需要 | `HudiMvccSnapshot` 待迁移到 SPI | incremental query 时序 |
| E6 VendedCredentials | ❌ | n/a | |
| E7 SysTables | ❌ | n/a | |
| E8 ColumnStatistics | 🟡 | hudi 有 column stats | 后续 |
| E9 Delete/Merge sink | ❌ | hudi 写路径不在本计划范围 | 与 BE 强耦合 |
| E10 listPartitions | ✅ 需要 | 走 HMS connector 的 listPartitions | |

---

## 已知特殊性（**重要**）

- **没有独立的 `HudiExternalCatalog`**！hudi 表通过 `HMSExternalTable.dlaType=HUDI` 暴露，本质上是寄生在 Hive 连接器上。
- D-005 决定：用 `ConnectorTableSchema.tableFormatType=HUDI` 显式建模，由 HMS connector 探测后填充。
- 4 个 `HoodieIncremental*Relation` 类是和 hudi-spark 库交互——必须在 fe-connector 模块内（classpath 隔离）。
- P3 实质上要做的是：
  1. 把 `HudiUtils` / `HudiSchemaCacheKey/Value` / `HudiMvccSnapshot` / `HudiPartitionProcessor` 搬到 `fe-connector-hudi`。
  2. 把 `HudiScanNode` 删除，由 `PluginDrivenScanNode` + 增强后的 `HudiScanPlanProvider`（已存在）承接 incremental relation 逻辑。
  3. 改造 `PhysicalHudiScan` 让它走 SPI 路径。
- **P3 启动前必须 P5 paimon 或 P7 hive 进入到至少完成 hms metadata 路径**，否则 hudi 拿不到底层 HMS 表元数据。**这是依赖序的隐藏约束**——见 master plan §3.4 第一段。

---

## 关联

- 阶段 task：P3（待启动时建）
- 决策：D-005（DLA 模型方案 A）
- 偏差：（暂无）
- 风险：（暂无独立的）

---

## 进度日志

### 2026-05-24
- 跟踪文件建立。50% 实现已就位，但 P3 依赖 hms-connector 路径先打通（D-005 模型）。
