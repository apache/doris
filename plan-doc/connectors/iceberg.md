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
| **当前状态** | 🟢 **P6.1 DONE + P6.2 DONE + P6.3 进行中（T01~T05 ✅）**：P6.1〔T01–T10〕7-flavor 装配 + 读元数据 parity + per-flavor 校验 + metastore 模块拆分；P6.2〔T01–T11〕scan+MVCC+cache+vended（UT 278/0/1）；**P6.3 写路径 RFC ✅ + T01~T05**（框架统一·SPI 收口 + jdbc planWrite + `IcebergConnectorTransaction` 骨架·op 选择·WriterHelper·begin guards + commit 校验套件·O5-2 `applyWriteConstraint`·V3 DV removeDeletes；UT **341/0/1**）。下一 = **T06 sink 统一**。**翻闸阻塞 = [DV-038]**（GLOBAL_ROWID + getColumnHandles，P6.6 前必修） |
| **完成度** | ~62%（P6.1+P6.2 实现完 + P6.3 T01–T05；剩 P6.3 T06–T09〔sink 统一 / 通用命令壳〔含 O5-2 fe-core 生产半〕/ 审计 / 收口〕 / P6.4 procedure / P6.5 sys-table / P6.6 翻闸）|
| **阶段拆分 spec** | [`tasks/P6-iceberg-migration.md`](../tasks/P6-iceberg-migration.md) |
| **主 owner** | TBD |

---

## 迁移 Playbook 进度

| 步骤 | 状态 | 备注 |
|---|---|---|
| 1 | 🟥 | fe-core 34 个顶层 + `source/`(7) + `action/`(10) + `cache/`(2) + `broker/`(3) + `dlf/`(3) + `fileio/`(4) + `helper/`(3) + `profile/`(1) + `rewrite/`(6) = **73 个文件** |
| 2 | 🟥 | fe-connector 只有 6 个文件（Provider/Metadata/Properties/TableHandle/TypeMapping）—— **骨架**|
| 3 | ⏳ | 反向 instanceof：~49 处（写命令层最密，P6.7 清理）|
| 4 | ⏳ | ConnectorMetadata 仅基础 list/get 实现；分子阶段 P6.1-P6.6 全面补 |
| 5 | ⏳ | |
| 6 | ✅ | META-INF/services 已注册 |
| 7 | ⏳ | |
| 8-9 | ⏳ | |
| 10 | ⏳ | 清理 ~49 处反向 instanceof（P6.7）|
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
- 偏差：[DV-038]（🔴 翻闸阻塞：GLOBAL_ROWID + getColumnHandles 共享 fe-core field-id 路径 BE DCHECK）、[DV-039]（parity-忠实 HIGH-MEDIUM）、[DV-040]（perf-cosmetic ~36 项批）
- 风险：R-003（Procedure SPI 抽象失败）、R-004（classloader）、R-005（nereids 写命令耦合）、R-012（snapshotId 类型）

---

## 进度日志

### 2026-06-23（P6.3 写路径 RFC ✅ + T01~T05 实现）

- **RFC ✅ 评审通过**（`a49720820f9`）= `06-iceberg-write-path-rfc.md`：写框架全面统一（单 `ConnectorTransaction`）+ 行级-DML Route B（iceberg plan 合成暂留 fe-core，DV-04x）+ O5-2 冲突检测接缝 + Trino 式通用化北极星。
- **T01** 框架统一·SPI 收口（option B）：删 insert-handle/`usesConnectorTransaction` 双模型 fork → 单 `ConnectorTransaction`；jdbc no-op txn 迁移。
- **T02** jdbc thrift 入 `planWrite`（OQ-1）+ 删 config-bag 三件套（OQ-2）+ source-agnostic `appendExplainInfo` EXPLAIN-保留 hook（用户增补）。
- **T03** `IcebergConnectorTransaction implements ConnectorTransaction` 骨架：单 SDK txn/表经 seam+auth、14 字段 `TIcebergCommitData` 反序列化累积、`getUpdateCnt`、新 `WriteOperation` 枚举。对抗 1 confirmed 修（`newTransaction()` 须在 auth 内）。
- **T04** op 选择收进 `commit()`（SPI 无 finishWrite 钩子）+ begin* guards（fmt≥2 / branch 非 tag / baseSnapshotId 捕获）+ 新 `IcebergWriterHelper`/`IcebergPartitionUtils` parse 助手/`IcebergWriteContext`。对抗 0 finding。
- **T05** commit 校验套件（`validateFromSnapshot`/serializable `validateNoConflictingDataFiles`/`validateDeletedFiles`/`validateNoConflictingDeleteFiles`/`validateDataFilesExist`/`delete_isolation_level` 默认 serializable）+ O5-2 `applyWriteConstraint`（新 `ConnectorPredicate` SPI default-no-op + 连接器惰性转 `IcebergPredicateConverter` 暂存 + 与 identity-分区 filter 合并）+ V3 DV `removeDeletes`（fmt≥3 / `ContentFileUtil.isFileScoped` / dedup）。**[D-061] O5-2 fe-core 生产半（analyzed-plan 抽取）挪 T07**（唯一消费者 = T07 `RowLevelDmlCommand`）。对抗 `wf_0960ef5f-52c` = 0 finding。
- **验收（T01~T05 累计）**：connector-api UT 30/0/0、fe-connector-iceberg UT **341/0/1**（278→341）、jdbc 190 / maxcompute 102 / paimon 318 无回归、checkstyle 0、import-gate 0、iceberg 仍**不在** `SPI_READY_TYPES`、**0 BE 改**（T01–T05 全程）。下一 = **T06 sink 统一**（连接器 `planWrite` 自建 `TIceberg{Table,Delete,Merge}Sink` + 删 planner sink + 走 `visitPhysicalConnectorTableSink` + 复用 T02 `appendExplainInfo` hook）。

### 2026-06-23（P6.1 DONE + P6.2 DONE；T11 收口）
- **P6.1 DONE〔T01–T10〕**：5-flavor CatalogUtil 装配（T05）+ s3tables bespoke（T06）+ DLF 子树 port（T07）+ 读路径列/format-version/listing/auth parity（T09）+ metastore 模块拆分〔`fe-connector-metastore-{paimon,iceberg}` per-engine + `-spi` 共享基类〕+ per-flavor CREATE 校验（T10 A+B）。
- **P6.2 DONE〔T01–T11〕**：scan provider 骨架（T01）+ 谓词下推/split（T02）+ typed range-params/`path_partition_keys`（T03）+ merge-on-read delete（T04）+ COUNT 下推（T05）+ field-id 字典（T06）+ MVCC time-travel（T07）+ 连接器内 cache + manifest 级 planning + vendored `DeleteFileIndex`（T08）+ vended + 静态凭据（T09）+ parity-UT 审计补测（T10）+ **T11 收口**（汇总设计 `designs/P6-T11-iceberg-scan-summary-design.md` + validation gate 核对〔7/0〕+ deviation 中央注册 [DV-038]/[DV-039]/[DV-040]）。**净 0 新 SPI**（唯一例外 = T03 非破坏 `isPartitionBearing()` 默认）。
- **验收全绿**：fe-connector-iceberg UT **278/0/1**（本 session `mvn -pl :fe-connector-iceberg -am test` cache-off 重跑 BUILD SUCCESS）、checkstyle 0、import-gate 净、iceberg 仍**不在** `SPI_READY_TYPES`（零行为变更）。审计 workflow `wf_edde7eac-a5b`（9 reader + completeness-critic）。
- **🔴 翻闸阻塞（P6.6 前必修）= [DV-038]**：GLOBAL_ROWID（top-N 合成列误归 REGULAR）+ getColumnHandles 无 snapshot 重载（rename+time-travel）= 同一共享 fe-core field-id 路径 BE StructNode DCHECK，跨 paimon，须 holistic 修 + paimon 影响分析。
- **下一 = P6.3 写路径**（先写 `06-iceberg-write-path-rfc.md` 过 PMC，再实现）。

### 2026-06-22（T08 commit + T04 pom 依赖闭包）
- **T08 已 commit `d41fa4faf3e`**（type-mapping read parity：TIMESTAMPTZ 名 + 点分 mapping-flag key + BINARY 无界长度 3 修；36 UT 绿）。
- **T04（pom 依赖闭包，[D-060]，本 session）**：`fe-connector-iceberg/pom.xml` 补 7-flavor 闭包——HMS/DLF=**复用 `hive-catalog-shade`**（用户签字 vs 专建 iceberg-hive-shade；**修正 D-059「iceberg-hive-metastore」误述——该 artifact 不存在**，HiveCatalog + DLF ProxyMetaStoreClient + aliyun SDK 均捆在 hive-catalog-shade 内）+ AWS SDK v2 child-first（glue/sts/s3tables/s3/s3-transfer-manager/sdk-core/...）+ `s3-tables-catalog-for-iceberg` + `fe-connector-metastore-spi`（Q2=B）；`fe/pom.xml` + s3tables dM；`plugin-zip.xml` + `fe-thrift`/`libthrift` 排除。**无 Java 改**（flavor 由 CatalogUtil 按名反射加载）。验证：36 UT + checkstyle 0 + import-gate 0 + `dependency:tree` iceberg-core 恰 1 + **plugin-zip 实查**（143 jar：iceberg 全 1.10.1 无 skew、libthrift 缺席、hadoop 仅 3.4.2）+ `SPI_READY_TYPES` iceberg 缺席。残留→P6.6 docker：shade 内 iceberg 与直接 iceberg-core child-first 共存（版本同→预期 benign）；glue 显式-AK provider 类来源待 T05 核。

### 2026-06-21（P6.1 recon + T01-T03）
- **recon**（7-agent，`research/p6.1-iceberg-metadata-recon.md`）+ **10-task 拆解**（`tasks/P6-iceberg-migration.md` §P6.1）+ **[D-059]**（Q1 DLF port-now read-only / Q2 扩 metastore-spi 加 iceberg provider）。
- **T01-T03 实现+验证（commit `ae54a2174ff`）**：新建 `IcebergCatalogFactory`（纯静态）+ `IcebergCatalogOps`（注入 seam）+ rewire `IcebergConnectorMetadata`（behavior frozen）+ 测试基建从无到有（`RecordingIcebergCatalogOps`/`FakeIcebergTable`/`RecordingConnectorContext` + 2 test class）。`mvn test`（cache off）= 27 run/0F/0E/0skip + checkstyle 0 + import-gate 净。连接器主文件 6→8。
- 测试独立确证 2 个 silent parity bug（format-version 恒 2 / mapping-flag 下划线 key）已 pin frozen 待 T08/T09。

### 2026-05-24
- 跟踪文件建立。当前 fe-connector 仅 6 个文件骨架，是所有连接器中 **fe-connector 端最不完整** 的——P6 工作量巨大（5 周）。
