# Connector: `paimon`

---

## 概况

| 项 | 值 |
|---|---|
| **catalog type 名** | `paimon` |
| **fe-connector 模块** | `fe/fe-connector/fe-connector-paimon/` |
| **fe-core 旧路径** | `fe/fe-core/src/main/java/org/apache/doris/datasource/paimon/` |
| **共享依赖** | `fe-connector-hms`（paimon-HMS-flavor 用） |
| **计划迁移阶段** | **P5** |
| **当前状态** | ⏸ 未启动 |
| **完成度** | 20%（scan 路径 50%，catalog 路径 10%）|
| **主 owner** | TBD |

---

## 迁移 Playbook 进度

| 步骤 | 状态 | 备注 |
|---|---|---|
| 1 | 🟡 | fe-core 22 个顶层 + `source/`（5 个）+ `profile/`（2 个）|
| 2 | 🟡 | fe-connector 10 个文件，scan/predicate/handle 完整 |
| 3 | ⏳ | 反向 instanceof：10 处 |
| 4 | 🟡 | ConnectorMetadata 部分实现；6 个 catalog flavor（HMS/DLF/REST/File/Base/Factory）未迁 |
| 5 | ⏳ | |
| 6 | ✅ | META-INF/services 已注册 |
| 7 | ⏳ | |
| 8-9 | ⏳ | |
| 10 | ⏳ | 清理 10 处反向 instanceof |
| 11 | ⏳ | PhysicalPlanTranslator 删 `PAIMON_EXTERNAL_TABLE` 分支 |
| 12 | ⏳ | 0 个测试 |
| 13 | ⏳ | 删 `datasource/paimon/` |

---

## SPI 实现完成度

| 扩展点 | 是否需要 | 实现状态 | 备注 |
|---|---|---|---|
| E1 CreateTableRequest | ✅ 需要 | 含 bucket spec | |
| E2 Procedures | 🟡 | paimon 有 expire-snapshots 等 | 后续 |
| E3 MetaInvalidator | 🟡 | paimon-HMS-flavor 需要 | 复用 `fe-connector-hms` |
| E4 Transactions | ✅ 需要 | |
| E5 MvccSnapshot | ✅ 需要 | `PaimonMvccSnapshot` 待迁 SPI | |
| E6 VendedCredentials | ✅ 需要 | `PaimonVendedCredentialsProvider` 待迁 | |
| E7 SysTables | ✅ 需要 | `PaimonSysExternalTable` 待迁 | |
| E8 ColumnStatistics | 🟡 | snapshot summary 已含部分 | 可选 |
| E9 Delete/Merge sink | 🟡 | merge-on-read 路径 | |
| E10 listPartitions | ✅ 需要 | |

---

## 已知特殊性

- **6 个 catalog flavor** —— 用工厂模式重组：`PaimonConnectorProvider.create()` 根据 properties 实例化 paimon Catalog。
- **重复类 `PaimonPredicateConverter`** 在 fe-core 和 fe-connector 两边都有，P1 清理 fe-core 版本。
- BE 通过 JNI 调用 paimon-reader；连接器通过 `ConnectorScanPlanProvider.getSerializedTable(props)` 序列化 paimon `Table` 对象给 BE。
- 0 个测试。

---

## 关联

- 阶段 task：P5（待启动时建）
- 决策：D-006（cache 放连接器内）、D-005（HMS flavor 走 tableFormatType）
- 偏差：（暂无）
- 风险：R-004（classloader）、R-012（snapshotId 类型）

---

## 进度日志

### 2026-05-24
- 跟踪文件建立。scan 路径已就绪，但 6 个 catalog flavor + MVCC + sys-tables + vended creds 都还在 fe-core。
