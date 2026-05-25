# Connector: `iceberg`

---

## 概况

| 项 | 值 |
|---|---|
| **catalog type 名** | `iceberg` |
| **fe-connector 模块** | `fe/fe-connector/fe-connector-iceberg/` |
| **fe-core 旧路径** | `fe/fe-core/src/main/java/org/apache/doris/datasource/iceberg/` |
| **共享依赖** | `fe-connector-hms`（iceberg-HMS-flavor 用） |
| **计划迁移阶段** | **P6**（最大阶段，5 周）|
| **当前状态** | ⏸ 未启动 |
| **完成度** | 5% |
| **主 owner** | TBD |

---

## 迁移 Playbook 进度

| 步骤 | 状态 | 备注 |
|---|---|---|
| 1 | 🟥 | fe-core 34 个顶层 + `source/`(7) + `action/`(10) + `cache/`(2) + `broker/`(3) + `dlf/`(3) + `fileio/`(4) + `helper/`(3) + `profile/`(1) + `rewrite/`(6) = **73 个文件** |
| 2 | 🟥 | fe-connector 只有 6 个文件（Provider/Metadata/Properties/TableHandle/TypeMapping）—— **骨架**|
| 3 | ⏳ | 反向 instanceof：19 处 |
| 4 | ⏳ | ConnectorMetadata 仅基础 list/get 实现；分子阶段 P6.1-P6.6 全面补 |
| 5 | ⏳ | |
| 6 | ✅ | META-INF/services 已注册 |
| 7 | ⏳ | |
| 8-9 | ⏳ | |
| 10 | ⏳ | 清理 19 处反向 instanceof |
| 11 | ⏳ | PhysicalPlanTranslator 删 `IcebergExternalTable / IcebergSysExternalTable` 分支 |
| 12 | ⏳ | 0 个测试 |
| 13 | ⏳ | 删 `datasource/iceberg/` |

---

## SPI 实现完成度

| 扩展点 | 是否需要 | 实现状态 | 备注 |
|---|---|---|---|
| E1 CreateTableRequest | ✅ 需要 | 含 transform partition（year/month/day/bucket/truncate）| |
| E2 Procedures | ✅ 需要 | **10 个 action**（rewrite_data_files、expire_snapshots、...） | P6.4 重点 |
| E3 MetaInvalidator | 🟡 | 部分 iceberg-HMS-flavor 需要 | 复用 `fe-connector-hms` |
| E4 Transactions | ✅ 需要 | `IcebergTransaction`（966 行）待迁 | P6.3 |
| E5 MvccSnapshot | ✅ 需要 | `IcebergMvccSnapshot` 待迁 SPI | snapshot/timestamp 时光机 |
| E6 VendedCredentials | ✅ 需要 | `IcebergVendedCredentialsProvider` 待迁 | Iceberg REST 主战场 |
| E7 SysTables | ✅ 需要 | `IcebergSysExternalTable.SysTableType` 9 个 | $snapshots/$history/... |
| E8 ColumnStatistics | 🟡 | snapshot summary | 可选 |
| E9 Delete/Merge sink | ✅ 需要 | `IcebergDeleteSink/MergeSink/TableSink` 删除 | P6.3 |
| E10 listPartitions | ✅ 需要 | |

---

## 子阶段（P6.1 - P6.6）

来自 master plan §3.7：

| 子阶段 | 范围 | 估时 |
|---|---|---|
| P6.1 | 元数据 only（7 个 catalog flavor + ConnectorMetadata） | 2 周 |
| P6.2 | scan path（ScanPlanProvider + MVCC + cache） | 1 周 |
| P6.3 | write path（commit/transaction + DML SPI + planner 改造） | 1 周 |
| P6.4 | actions（procedure SPI 接 10 个 action） | 0.5 周 |
| P6.5 | sys tables + metadata columns | 0.5 周 |
| P6.6 | 删除 fe-core/iceberg + 清 19 处反向 instanceof | 0.5 周 |

---

## 已知特殊性（**极重要**）

- **7 个 catalog flavor**（HMS/Glue/Hadoop/Jdbc/REST/S3Tables/DLF）—— Iceberg SDK 本身有 Catalog 抽象，连接器只需 dispatch property → 实例化哪个 SDK Catalog。
- **10 个 IcebergXxxAction**（`RewriteDataFiles`、`ExpireSnapshots`、`RollbackToSnapshot`、`CherrypickSnapshot`、`PublishChanges`、`SetCurrentSnapshot`、`RewriteManifests`、`FastForward`、`RollbackToTimestamp`、`PublishChanges`）—— 必须用 P0 新增的 `ConnectorProcedureOps` 承接。
- **写路径深度耦合**：`IcebergConflictDetectionFilterUtils`、`IcebergConflictDetectionFilterUtils`、`IcebergRowId`、`IcebergMergeOperation` 都和 nereids 优化器纠缠。**P6.3 前必须单独写 `plan-doc/06-iceberg-write-path-rfc.md` 评审方案**（master plan 已注明）。
- **5400+ 行核心代码**（IcebergMetadataOps 1247 + IcebergTransaction 966 + IcebergUtils 1718 + IcebergScanNode 1228 + IcebergExternalCatalog 241）。
- **DLA 寄生**：iceberg-on-HMS flavor 通过 `HMSExternalTable.dlaType=ICEBERG` 暴露——D-005 决定用 `tableFormatType` 区分。

---

## 关联

- 阶段 task：P6（待启动时建）
- 决策：D-002, D-005, D-006
- 偏差：（暂无）
- 风险：R-003（Procedure SPI 抽象失败）、R-004（classloader）、R-005（nereids 写命令耦合）、R-012（snapshotId 类型）

---

## 进度日志

### 2026-05-24
- 跟踪文件建立。当前 fe-connector 仅 6 个文件骨架，是所有连接器中 **fe-connector 端最不完整** 的——P6 工作量巨大（5 周）。
