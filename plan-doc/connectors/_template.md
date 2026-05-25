# Connector: `<name>`

> 复制本模板到 `connectors/<name>.md` 创建新连接器跟踪文件。
> 维护规则：每次该连接器有动作（playbook 步骤完成、PR 合入、SPI 实现更新）时同步更新。

---

## 概况

| 项 | 值 |
|---|---|
| **catalog type 名** | `<name>` |
| **fe-connector 模块** | `fe/fe-connector/fe-connector-<name>/` |
| **fe-core 旧路径** | `fe/fe-core/src/main/java/org/apache/doris/datasource/<name>/` |
| **共享依赖** | `fe-connector-hms` / 无 / 其他 |
| **计划迁移阶段** | P<n> |
| **当前状态** | ⏸ 未启动 / 🚧 进行中 / ✅ 完成 |
| **完成度** | 0% / 50% / 100% |
| **主 owner** | @xxx |

---

## 迁移 Playbook 进度（13 步，来自 master plan §4）

> 状态：✅ 完成 / 🚧 进行中 / ⏳ 未启动 / 🚫 不适用

| 步骤 | 描述 | 状态 | 备注 |
|---|---|---|---|
| 1 | 列出 fe-core 类，按终态分类 | ⏳ | |
| 2 | 列出 fe-connector 已有类，对照差距 | ⏳ | |
| 3 | 列出反向 instanceof / cast 调用点 | ⏳ | grep 结果数量 |
| 4 | 实现 ConnectorMetadata / ScanPlanProvider 缺失方法 | ⏳ | |
| 5 | 实现 ConnectorProvider.validateProperties + preCreateValidation | ⏳ | |
| 6 | META-INF/services 注册 | ⏳ | |
| 7 | CatalogFactory.SPI_READY_TYPES 加入 | ⏳ | |
| 8 | PluginDrivenExternalCatalog.gsonPostProcess 加迁移分支 | ⏳ | |
| 9 | ExternalCatalog.registerCompatibleSubtype 注册 | ⏳ | |
| 10 | 替换反向 instanceof（nereids/planner/...） | ⏳ | |
| 11 | PhysicalPlanTranslator 删该连接器分支 | ⏳ | |
| 12 | 写 / 跑回归测试 + image 兼容用例 | ⏳ | |
| 13 | 删除 fe-core 旧目录 + import 清理 | ⏳ | |

---

## SPI 实现完成度（对照 RFC §2.1 扩展点）

| 扩展点 | 是否需要 | 实现状态 | 备注 |
|---|---|---|---|
| E1 CreateTableRequest | | | |
| E2 Procedures | | | |
| E3 MetaInvalidator | | | |
| E4 Transactions | | | |
| E5 MvccSnapshot | | | |
| E6 VendedCredentials | | | |
| E7 SysTables | | | |
| E8 ColumnStatistics | | | |
| E9 Delete/Merge sink | | | |
| E10 listPartitions | | | |

---

## 已知特殊性 / 风险

> 该连接器独有的难点。

- ...

---

## 关联

- 阶段 task：[tasks/P<n>](../tasks/P<n>-xxx.md)
- 决策：D-NNN, ...
- 偏差：DV-NNN, ...
- 风险：R-NNN, ...
- 关键 PR：#NNN, ...

---

## 进度日志（倒序）

### YYYY-MM-DD
- 描述
