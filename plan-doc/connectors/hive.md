# Connector: `hive` (含 `hms` 共享库)

---

## 概况

| 项 | 值 |
|---|---|
| **catalog type 名** | `hms`（CATALOG_TYPE_PROP=hms）|
| **fe-connector 模块** | `fe/fe-connector/fe-connector-hive/` + `fe/fe-connector/fe-connector-hms/`（共享库）|
| **fe-core 旧路径** | `fe/fe-core/src/main/java/org/apache/doris/datasource/hive/` |
| **共享依赖** | 自身 `fe-connector-hms`；被 hudi/iceberg-HMS/paimon-HMS 依赖 |
| **计划迁移阶段** | **P7**（最复杂，6 周）|
| **当前状态** | ⏸ 未启动 |
| **完成度** | 10%（hive 20% + hms 共享库已立） |
| **主 owner** | TBD |

---

## 迁移 Playbook 进度

| 步骤 | 状态 | 备注 |
|---|---|---|
| 1 | 🟥 | fe-core 30 个顶层 + `event/`（21 个）+ `source/`（HiveScanNode 等） |
| 2 | 🟥 | fe-connector-hive 12 个文件（scan path + handles）；fe-connector-hms 9 个文件 |
| 3 | ⏳ | 反向 instanceof：**31 处**（最高）|
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

### 2026-05-24
- 跟踪文件建立。当前最复杂的连接器；R-002（ACID 数据不一致）是项目最大风险。
- 注意：hive 是 hudi/iceberg/paimon 共同的底座（通过 HMS 共享库），P7 启动 = 项目核心冲刺。
