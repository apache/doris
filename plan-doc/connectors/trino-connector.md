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
| **当前状态** | 🚧 P2 进行中（recon 完成 / task 划分敲定 / 编码待启动） |
| **完成度** | 30% → 目标 100%（P2 收尾时翻闸） |
| **主 owner** | @me |

---

## 迁移 Playbook 进度

> Recon 后实测（2026-05-25）：fe-core 旧目录 10 个 .java；反向 instanceof 实际 1 处（dashboard "2" 为过时数字）。

| 步骤 | 状态 | 备注 |
|---|---|---|
| 1 | 🟡 | fe-core 旧路径 10 个 .java / ~1760 LOC（TrinoConnectorExternalCatalog 329 / Scan 342 / PredicateConverter 334）|
| 2 | 🟡 | fe-connector 已有 13 个类 / 2162 LOC：Provider/Metadata/ScanPlanProvider/Predicate/PluginManager/Bootstrap/TypeMapping/Json/3 个 Handle |
| 3 | ⏳ | 反向 instanceof：**1 处**（PhysicalPlanTranslator:779 — P1 批 A 已加 SPI fallback 在它之上，待 P2-T08 删除）|
| 4 | 🟢 | ConnectorMetadata 方法 ~95% IMPL/DEFAULT；DDL 类（createTable/dropTable）DEFAULT throws 是合理的（Trino 此路径 read-only）|
| 5 | ⏳ | validateProperties / preCreateValidation 待补（P2-T01）|
| 6 | ✅ | META-INF/services 已注册 `TrinoConnectorProvider` |
| 7 | ⏳ | `SPI_READY_TYPES` 未加（P2-T07 翻闸）|
| 8 | ⏳ | gsonPostProcess 未加 trinoconnector → plugin 迁移（P2-T04）|
| 9 | ⏳ | registerCompatibleSubtype 未注册（P2-T05）|
| 10 | ⏳ | 替换 1 处反向 instanceof（P2-T08）|
| 11 | ⏳ | PhysicalPlanTranslator 删 `TrinoConnectorExternalTable` 分支（P2-T08）|
| 12 | ⏳ | 0 个测试 → 需要补（P2-T11/T12）|
| 13 | ⏳ | 删 `datasource/trinoconnector/`（P2-T10；同步删 GsonUtils 3 个 class-token 注册）|

---

## SPI 实现完成度

| 扩展点 | 是否需要 | 实现状态 | 备注 |
|---|---|---|---|
| E1 CreateTableRequest | 🟡 | 透传到 Trino connector | Trino 自身 CREATE 透传（Doris 端走 SPI default throw 即可）|
| E2 Procedures | 🟡 | Trino 有 Procedure SPI | 推迟评估（不在 P2 scope）|
| E3 MetaInvalidator | ❌ | n/a | Trino 一般无 push notification（DEFAULT NOOP 即合）|
| E4 Transactions | 🟡 | Trino ConnectorTransactionHandle | 桥接到新 ConnectorTransaction（P2 不做 write 路径，DEFAULT 即合）|
| E5 MvccSnapshot | 🟡 | 部分 Trino connector 有 | 视具体 plugin；P2 不做 |
| E6 VendedCredentials | ❌ | n/a | |
| E7 SysTables | ❌ | n/a | |
| E8 ColumnStatistics | 🟡 | Trino 有 column stats | P2 不做（可推迟）|
| E9 Delete/Merge sink | ❌ | 用通用 sink | |
| E10 listPartitions | 🟡 | Trino 有 partition handles | DEFAULT empty 即合（Trino 自己 plan-time 处理 partition pruning）|
| **pushdown** | 🟡 | applyFilter / applyProjection | **P2-T02 实施**（用户决议 Q1，2026-05-25：纳入 P2 批 A）|

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

### 2026-05-25（晚 ②）— P2 启动 + recon 完成
- 3 路 Explore subagent 并行 recon 输出（详见 [tasks/P2-trino-connector-migration.md §阶段日志](../tasks/P2-trino-connector-migration.md)）
- 关键修正：dashboard 反向 instanceof "0/2" 为过时数字，实测仅 1 处（PhysicalPlanTranslator:779）；fe-connector-trino 模块 "70%" 在 SPI 表面层面其实更接近 95%，真缺只有 validateProperties / preCreateValidation / pushdown 三处
- 13 task / 5 批次方案敲定，进入编码阶段

### 2026-05-24
- 跟踪文件建立。70% 实现已就位，等 P0/P1 完成后启动 P2 整体推动。
