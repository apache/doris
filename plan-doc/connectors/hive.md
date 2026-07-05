# Connector: `hive` (含 `hms` 共享库)

---

## 概况

| 项 | 值 |
|---|---|
| **catalog type 名** | `hms`（CATALOG_TYPE_PROP=hms）|
| **fe-connector 模块** | `fe/fe-connector/fe-connector-hive/` + `fe/fe-connector/fe-connector-hms/`（共享库）|
| **fe-core 旧路径** | `fe/fe-core/src/main/java/org/apache/doris/datasource/hive/` |
| **共享依赖** | 自身 `fe-connector-hms`；被 hudi/iceberg-HMS/paimon-HMS 依赖 |
| **计划迁移阶段** | **P7**（最复杂，6 周；**当前活跃阶段**，phase-split spec 已立，起步 P7.1）|
| **当前状态** | 🚧 P7 进行中：**code-grounded recon（10-agent）+ 阶段拆分 spec `tasks/P7-hive-migration.md` 已完成**；下一 = **P7.1 HiveMetadataOps 全功能搬迁（实现）** |
| **完成度** | 12%（recon+spec 立 + hive scan 只读已立 + hms 共享读库已立；DDL/txn/event/stats 端 0） |
| **主 owner** | TBD |

---

## 迁移 Playbook 进度

| 步骤 | 状态 | 备注 |
|---|---|---|
| 1 | 🟥 | fe-core **52** 文件（29 顶层 + `event/` 21 + `source/` 2）|
| 2 | 🟨 | fe-connector-hive 12 文件（**只读 scan 已立**，DDL/txn/stats override=0）；fe-connector-hms 9 文件（**共享读库**，无写/txn/lock/col-stats）|
| 3 | ⏳ | 反向 instanceof/cast：**85 occurrence / 33 文件**（最高；plan 旧记 31 已校正）+ ~7 处 type-level 耦合（CatalogFactory/GsonUtils/HudiUtils/IcebergHMSSource…）|
| 4 | ⏳ | `HiveMetadataOps` 全功能未迁；P7.1 重头 |
| 5 | ⏳ | |
| 6 | ✅ | META-INF/services 已注册（HiveConnectorProvider）；hms 共享库无 service 注册 |
| 7 | ⏳ | |
| 8-9 | ⏳ | |
| 10 | ⏳ | 清理 31 处反向 instanceof |
| 11 | ⏳ | PhysicalPlanTranslator 删 `HMSExternalTable` 分支（含 dlaType=HIVE/ICEBERG/HUDI 三路）|
| 12 | ⏳ | 0 个测试 |
| 13 | ⏳ | 删 `datasource/hive/` |

---

## SPI 实现完成度

| 扩展点 | 是否需要 | 实现状态 | 备注 |
|---|---|---|---|
| E1 CreateTableRequest | ✅ 需要 | Hive identity partition + bucket | |
| E2 Procedures | ❌ | n/a | |
| E3 MetaInvalidator | ✅ 需要 | **HMS 21 个 event 类整体搬到 fe-connector-hms** | D-004；P7.2 重头 |
| E4 Transactions | ✅ 需要 | **HMSTransaction（1866 行）+ HiveTransactionMgr 搬到 fe-connector-hive** | P7.3，ACID |
| E5 MvccSnapshot | ❌ | n/a | |
| E6 VendedCredentials | ❌ | n/a | |
| E7 SysTables | ❌ | n/a | |
| E8 ColumnStatistics | ✅ 需要 | Hive ANALYZE column stats 写回 HMS | E8 SPI 的主要消费者 |
| E9 Delete/Merge sink | ✅ 需要 | Hive ACID delete/merge | |
| E10 listPartitions | ✅ 需要 | HMS partition 主消费者 | |

---

## 子阶段（P7.1 - P7.5）

来自 master plan §3.8：

| 子阶段 | 范围 | 估时 |
|---|---|---|
| P7.1 | `HiveMetadataOps` 全功能搬到 `HiveConnectorMetadata`（DDL/partition/statistics） | 2 周 |
| P7.2 | event pipeline 21 个类搬到 `fe-connector-hms`；接 `ConnectorMetaInvalidator` | 1.5 周 |
| P7.3 | HMSTransaction + HiveTransactionMgr 搬；ACID 写路径联调 | 2 周 |
| P7.4 | DLA 分流改造（让 `HMSExternalTable` 退化为 PluginDrivenExternalTable 承接） | 0.5 周 |
| P7.5 | 删除 fe-core/hive + 31 处反向 instanceof | 0.5 周 |

---

## 已知特殊性（**最复杂的连接器**）

- **HMS 是共同后端**：hive、hudi、iceberg-HMS-flavor、paimon-HMS-flavor 都依赖。HMS 连接器必须在 P7 之前就稳定可用（事实上 P3/P5/P6 已经在用 `fe-connector-hms` 共享库）。
- **21 个 metastore event 类** + `MetastoreEventsProcessor` 后台线程——D-004 决定整体搬到 `fe-connector-hms`。
- **HMSTransaction 1866 行 + HiveTransactionMgr** —— ACID 事务管理是**最难重写**的部分。R-002 高风险。
- **HMSExternalTable 1293 行** 处理 hive/hudi/iceberg 三种 dlaType 的分流逻辑。这部分被 D-005 模型吸收。
- **31 处反向 instanceof** 是所有连接器中最多的，散布在 `nereids/glue/translator`、`tablefunction/MetadataGenerator`、`AnalyzeTableCommand`、`ShowPartitionsCommand` 等。
- **Kerberos UGI 上下文**——`ConnectorContext.executeAuthenticated` 已支持，但需要逐条审查 HMS 代码路径。
- 0 个测试（fe-connector-hive 端） → P7 启动前需要建独立 ACID test suite + chaos test（R-002 缓解条件）。

---

## 关联

- 阶段 task：P7（待启动时建）
- 决策：D-002, D-003, D-004, D-005
- 偏差：（暂无）
- 风险：**R-002（ACID 数据不一致，High）**、R-004（classloader）、R-010（event listener leak）

---

## 进度日志

### 2026-07-05（下午 · P7 recon + 阶段拆分 spec 完成）
- **10-agent code-grounded recon**（`wf-p7-hive-recon` + 补充 type-coupling recon，~1.3M token）核清 52 文件分类、ACID 写路径、event pipeline、DLA 三分流、85 处反向 instanceof、跨连接器耦合 + 翻闸机制（CatalogFactory:50/133 + GsonUtils:366/447/471 兼容 + 6 文件写路径 retype 链 + 删除排序）。校正过时数字（instanceof 31→85、HMSTransaction 1866→1895、HMSExternalTable 1293→1332）。
- **关键澄清**：recon 标"最大未知"= iceberg/hudi-on-HMS 归属，实为**已定** —— D-020（per-table SPI provider，hive 网关委派 -iceberg/-hudi）+ D-019（hudi live cutover 并入 P7）。故本阶段目标含删 `datasource/hudi/` + 23 HMS-iceberg 类。
- **产出** `tasks/P7-hive-migration.md`（阶段拆分 spec，P7.1–P7.5 + old→new 映射 + SPI 缺口 + 8 条开放决策待各子阶段签字）。**下一 = P7.1 实现**（HiveMetadataOps → HiveConnectorMetadata + HmsClient 写方法）。

### 2026-07-05（P7 启动 = 当前活跃迁移目标）
- **iceberg P6 已 squash-合入 `branch-catalog-spi`（#64688 `8b391c7459d`）→ hive 成为下一个活跃迁移目标**。工作分支 `catalog-spi-11-hive`。
- **下个 session 起步**：建 `tasks/P7-hive-migration.md` 阶段拆分 spec + code-grounded recon；起步 P7.1 HiveMetadataOps 全功能搬迁。权威计划 master plan §3.8 + 本文 §子阶段。**R-002 ACID 写路径（P7.3）= 项目最大风险，须专门集成测试作 gate。**
- **P7 连带清理**：删 fe-core `datasource/hive/`（P7.5）+ 23 个 HMS-iceberg 支撑类 + `datasource/hudi/`（阶段四）；hudi 批 E（live cutover）并入 P7。

### 2026-05-24
- 跟踪文件建立。当前最复杂的连接器；R-002（ACID 数据不一致）是项目最大风险。
- 注意：hive 是 hudi/iceberg/paimon 共同的底座（通过 HMS 共享库），P7 启动 = 项目核心冲刺。
