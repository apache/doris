# Connector: `jdbc`

---

## 概况

| 项 | 值 |
|---|---|
| **catalog type 名** | `jdbc` |
| **fe-connector 模块** | `fe/fe-connector/fe-connector-jdbc/` |
| **fe-core 旧路径** | `fe/fe-core/src/main/java/org/apache/doris/datasource/jdbc/`（残留 13 个方言 client + 1 util） |
| **共享依赖** | 无（独立 plugin） |
| **计划迁移阶段** | 已在 SPI 前置阶段完成，残留清理在 P1 |
| **当前状态** | ✅ 已 SPI 化 + 🚧 旧 client 清理待办 |
| **完成度** | 95% |
| **主 owner** | @me |

---

## 迁移 Playbook 进度

| 步骤 | 描述 | 状态 | 备注 |
|---|---|---|---|
| 1 | 列出 fe-core 类 | ✅ | 仅剩 13 个 `Jdbc<Dialect>Client` + `util/JdbcFieldSchema` |
| 2 | 列出 fe-connector 类 | ✅ | 25 个 java 文件，含 13 个方言 client（新版） |
| 3 | 反向 instanceof grep | ✅ | 0 处（已彻底清理） |
| 4 | 实现 ConnectorMetadata / ScanPlanProvider | ✅ | `JdbcConnectorMetadata`、`JdbcScanPlanProvider` |
| 5 | ConnectorProvider 验证 | ✅ | `JdbcConnectorProvider.validateProperties` 已实现 |
| 6 | META-INF/services | ✅ | `org.apache.doris.connector.jdbc.JdbcConnectorProvider` |
| 7 | `SPI_READY_TYPES` 加入 | ✅ | `CatalogFactory.SPI_READY_TYPES = ["jdbc", "es"]` |
| 8 | gsonPostProcess 迁移 | ✅ | logType JDBC → PLUGIN 已就位 |
| 9 | registerCompatibleSubtype | ✅ | |
| 10 | 替换反向 instanceof | ✅ | |
| 11 | PhysicalPlanTranslator 删分支 | ✅ | |
| 12 | 测试 | ✅ | 13 个测试文件 |
| 13 | 删 fe-core 旧目录 | 🚧 | **P1 处理**：删 `datasource/jdbc/client/Jdbc*Client.java` 13 个 + `util/JdbcFieldSchema.java` |

---

## SPI 实现完成度

| 扩展点 | 是否需要 | 实现状态 | 备注 |
|---|---|---|---|
| E1 CreateTableRequest | ❌ | n/a | JDBC 不支持复杂 CREATE TABLE，旧 createTable 已够用 |
| E2 Procedures | ❌ | n/a | |
| E3 MetaInvalidator | ❌ | n/a | JDBC 无 push notification |
| E4 Transactions | 🟡 | 当前 auto-commit | P0 批 0 后改为返回 no-op transaction |
| E5 MvccSnapshot | ❌ | n/a | JDBC 无快照 |
| E6 VendedCredentials | ❌ | n/a | |
| E7 SysTables | ❌ | n/a | |
| E8 ColumnStatistics | 🟡 | 现有 `getTableStatistics` 已有；列级未实现 | 用户 ANALYZE 走 fe-core 缓存 |
| E9 Delete/Merge sink | 🟡 | 当前用 `JDBC_WRITE` 类型 | 不需要 file-based sink |
| E10 listPartitions | ❌ | n/a | JDBC 表无分区 |

---

## 已知特殊性

- 13 个方言 client（MySQL/PG/Oracle/SQLServer/ClickHouse/...）每个都有独立的 quoting / type mapping / pushdown 规则。
- `JdbcUrlNormalizer` 处理各种 vendor 特定 URL 格式。
- `defaultTestConnection()` 返回 `true`（CREATE CATALOG 时强制验连接）。
- 旧 fe-core 13 个 `Jdbc*Client` 当前是 dead code（fe-connector 内已有等价实现），但还在 fe-core 编译路径中——P1 删除前要确认没有任何残留引用。

---

## 关联

- 阶段 task：N/A（已完成的连接器）；残留清理在 [P1](../tasks/P1-cleanup-and-scan-node.md)（待建）
- 决策：D-001（沿用 PASSTHROUGH_QUERY，JDBC 用到 query() TVF）
- 偏差：（暂无）
- 风险：R-004（classloader 隔离 — JDBC 已验证可行）

---

## 进度日志

### 2026-05-24
- 跟踪文件建立。当前状态：已 SPI 化，等待 P1 清理 fe-core 残留方言 client。
