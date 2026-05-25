# Connector: `es`

---

## 概况

| 项 | 值 |
|---|---|
| **catalog type 名** | `es` |
| **fe-connector 模块** | `fe/fe-connector/fe-connector-es/` |
| **fe-core 旧路径** | `fe/fe-core/src/main/java/org/apache/doris/datasource/es/`（**目录已删除** ✅）|
| **共享依赖** | 无 |
| **计划迁移阶段** | 已完成（在 SPI 前置阶段） |
| **当前状态** | ✅ 100% 完成 |
| **完成度** | 100% |
| **主 owner** | @me |

---

## 迁移 Playbook 进度

| 步骤 | 状态 | 备注 |
|---|---|---|
| 1-13 | ✅ | 全部 13 步完成 |

---

## SPI 实现完成度

| 扩展点 | 是否需要 | 实现状态 |
|---|---|---|
| E1 CreateTableRequest | ❌ | n/a（ES 不支持 CREATE TABLE） |
| E2 Procedures | ❌ | n/a |
| E3 MetaInvalidator | ❌ | n/a |
| E4 Transactions | ❌ | n/a |
| E5 MvccSnapshot | ❌ | n/a |
| E6 VendedCredentials | ❌ | n/a |
| E7 SysTables | ❌ | n/a |
| E8 ColumnStatistics | ❌ | n/a |
| E9 Delete/Merge sink | ❌ | n/a |
| E10 listPartitions | ❌ | n/a |

ES 不需要任何 P0 新增 SPI——它的所有功能都用现有 SPI 表达完毕。

---

## 已知特殊性

- ES 是**第一个**真正打通 SPI 端到端的连接器，是后续迁移的**参考样板**。
- ES 用 `FORMAT_ES_HTTP` 作为 `TFileFormatType` 兜底；不是文件扫描但寄生于 `FileQueryScanNode`。
- ES 有独特的 `terminate_after` 优化（`PluginDrivenScanNode.createScanRangeLocations` line 422-428）：limit 全推下时附加给 ES 减少 scroll。这是连接器特定逻辑残留在 fe-core 的小缺口，等价的"scan-level 自定义参数"未来可考虑通过 `populateScanLevelParams` 完整下放。
- 20 个 java 源文件 + 7 个测试文件，完整 REST 客户端 / DSL 构建 / 映射工具自含。

---

## 关联

- 阶段 task：N/A（已完成）
- 决策：D-001（沿用 PASSTHROUGH_QUERY）、D-002（PluginDrivenScanNode extends FileQueryScanNode 由 ES/JDBC 验证可行）
- 偏差：（暂无）
- 风险：（暂无）

---

## 进度日志

### 2026-05-24
- 跟踪文件建立。状态：100% 完成，作为后续连接器迁移的参考样板。
