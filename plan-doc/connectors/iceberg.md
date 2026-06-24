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
| **当前状态** | 🟢 **P6.1 DONE + P6.2 DONE + P6.3 DONE（T01~T09 ✅）**：P6.1〔T01–T10〕7-flavor 装配 + 读元数据 parity + per-flavor 校验 + metastore 模块拆分；P6.2〔T01–T11〕scan+MVCC+cache+vended（UT 278/0/1）；**P6.3 写路径 RFC ✅ + T01~T09 = P6.3 DONE**（框架统一·SPI 收口 + jdbc planWrite + `IcebergConnectorTransaction` 骨架·op 选择·WriterHelper·begin guards + commit 校验套件·O5-2·V3 DV + sink 统一〔INSERT/OVERWRITE〕·写排序 SPI + DELETE/MERGE sink 方言〔T07a〕+ O5-2 生产半〔T07b〕+ 通用 `RowLevelDmlCommand` 壳·注册表·6 派发重接·O5-2 dormant〔T07c，commit `a61cd9262b9`〕+ parity 审计·deviation 中央登记 DV-041..044〔T08：审计 wf 40→20 confirmed→11 gap-fill〕+ 收口汇总设计·faithfulness 对抗验证〔T09，`designs/P6.3-T09-iceberg-write-summary-design.md`〕；iceberg UT **389/0/1** + fe-core 30/0）。**P6.4 procedures 进行中**（T01 设计+三签字 [D-062] + T02 `ConnectorProcedureOps` SPI 骨架 ✅ + T03 ✅ 2026-06-24 base+factory + dispatch 骨架〔`runInAuthScope` body 进 `executeAuthenticated`〕，§4=4-A 连接器自校验、去死 `table` 参；arg 框架〔`NamedArguments`/`ArgumentParsers`/`ArgumentParser`〕**移 `fe-foundation` 共享**〔引擎+连接器一份，`validate` 抛 unchecked `IllegalArgumentException` 两侧 re-wrap〕；iceberg UT **401/0/1**，faithfulness wf 4→0 confirmed；**T04 ✅ 2026-06-24 港 8 pure-SDK 体 + `RewriteManifestExecutor` 接 factory switch**〔逐字 bug-for-bug；**必须改** `executeAction(Table,ConnectorSession)` 透会话 TZ〔新 `IcebergTimeUtils.msTimeStringToLong`〕；cache 失效搬 dispatch 级；iceberg UT **444/0/1**，faithfulness wf 1→0 confirmed/1 refuted+0 critic gaps〕；T05–T09 未做，仍 behind gate）。**翻闸阻塞 = [DV-038]（field-id BE DCHECK）+ [DV-041]（写路径 visitPhysicalConnectorTableSink 缺合成列物化，DV-038 同主题新面）**，P6.6 前必修 |
| **完成度** | ~75%（P6.1+P6.2+P6.3 实现完 + P6.4-T01~T04；剩 P6.4-T05~T09 procedure / P6.5 sys-table / P6.6 翻闸 / P6.7 删 legacy / P6.8 回归）|
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

### 2026-06-24（P6.4-T01 recon+设计+三签字 / T02 SPI 骨架 / T03 base+factory+dispatch / T04 8 pure-SDK 体）

- **T01**（recon + 设计 + 用户三签字 [D-062]，0 产品码）：recon `wf_cb757c7c-708`（10 reader + 对抗 completeness critic，3 源码核实更正）；新 `research/p6.4-iceberg-procedures-recon.md` + `designs/P6.4-T01-procedure-spi-design.md`。**关键认知**：①Doris `ALTER TABLE EXECUTE` 唯对应 Trino `TableProcedureMetadata`（非 CALL/MethodHandle）→ 保扁平 `ExecuteAction` 模型；②9 action 二分 = 8 pure-SDK（机械可移）+ 1 `rewrite_data_files`（分布式 INSERT-SELECT 写，执行半留 fe-core）；③dormant-pre-flip（镜像 P6.3 写）。**三签字**：Q1=R-A 分相位、Q2=S-1 扁平 `execute()`、§4=4-A 连接器自包含 arg 校验（import-gate 禁 `org.apache.doris.common.NamedArguments`）。
- **T02**（SPI 骨架，dormant）：新 `connector.api.procedure.{ConnectorProcedureOps,ConnectorProcedureResult}`（S-1 扁平 + 复用 `ConnectorColumn` 中立列型，0 新结果型）+ `Connector.getProcedureOps()` default-null（证 jdbc/es/mc/paimon/trino 继承 no-op）+ `IcebergProcedureOps` dormant 占位（镜像 `IcebergWritePlanProvider` 三元组，两方法 throw 直到 T03/T04）+ `IcebergConnector.getProcedureOps()` override。connector-api `ConnectorProcedureOpsDefaultsTest` 3/0 + 全模块 37/0；iceberg 389/0/1；checkstyle 0；import-gate 0；iceberg 仍**不在** `SPI_READY_TYPES`；0 BE/fe-core/pom 改。下一 = T03 port base/factory。
- **T03**（base+factory + dispatch 骨架，dormant）：`connector.iceberg.action.{BaseIcebergAction, IcebergExecuteActionFactory}`（去死 `table` 参；base 折入 `BaseExecuteAction` 被消费机器，SPI 中立型，`validate` 无 priv，单行包装+宽度 `checkState`，去 `getDescription`）+ `IcebergProcedureOps` dispatch 骨架（`getSupportedProcedures` + `runInAuthScope`：load+body+commit 同一 `executeAuthenticated`）；arg 框架 `NamedArguments`/`ArgumentParsers`/`ArgumentParser` **移 `fe-foundation` 共享**（引擎+连接器一份）。iceberg **401/0/1**，faithfulness wf 4→0 confirmed；0 BE/fe-core/pom。
- **T04**（港 8 pure-SDK procedure 体 + `RewriteManifestExecutor`，dormant）：`Iceberg{RollbackToSnapshot,RollbackToTimestamp,SetCurrentSnapshot,CherrypickSnapshot,FastForward,ExpireSnapshots,PublishChanges,RewriteManifests}Action` 各 `extends BaseIcebergAction` 接 `createAction` 8 case（`rewrite_data_files`=T05/T06 留 default-throw）。body = legacy 去 fe-core import + 5 机械换型（SDK `Table` 直用 / cache 失效搬 dispatch / `UserException`→`DorisConnectorException` message 字节同 / `Column`→`ConnectorColumn`〔**更正：第 3 参 `isAllowNull` 非 isKey** ⇒ `fast_forward.previous_ref` 唯一 NULLABLE〕 / 去 `getDescription`）；逐字 bug-for-bug（publish STRING+`"null"`、fast_forward 无-guard+trim 只输出、cherrypick 泛化 not-found、rollback not-found try 外〔不 wrap〕vs set/cherry try 内〔wrap〕、expire 6×BIGINT+双 wrap+`systemDefault` zone+bulk warn-skip+finally shutdown、rewrite 双 wrap+空表短路）。**🔧 必须改签名**（更正 HANDOFF「无须改签名」）：`rollback_to_timestamp` 需会话 TZ ⇒ `BaseIcebergAction.execute/executeAction` 加 `ConnectorSession`（7 个非 TZ body 忽略；SPI/factory 签名不动）+ 新 `IcebergTimeUtils.msTimeStringToLong`（ms 格式 + alias-map + `-1` sentinel，**非** `datetimeToMillis` 的 ss 格式）+ `resolveSessionZone` 提 public。cache 失效 = dispatch 级 `context.getMetaInvalidator().invalidateTable`（无条件含短路 = 幂等微差）。**faithfulness 对抗 `wl33dyokd`/`wf_973bd34f`**（11 finder + refute-by-default skeptic + critic）= **1 raw→0 confirmed/1 refuted+0 critic gaps**（refuted=`resolveSessionZone` null-session 回落 NIT，EXECUTE 路不可达 + P6.2-T07 既有件）。8 新测类 + 扩 `IcebergProcedureOpsTest`（auth-scope/dispatch invalidate/会话 TZ 透传/failAuth 不失效）+ `ActionTestTables` + `RecordingConnectorContext` recording invalidator。iceberg **444/0/1**（401→444）、checkstyle 0、import-gate 0、iceberg 仍**不在** `SPI_READY_TYPES`、**0 BE/fe-core/pom 改**。auth 补 + cache 搬家 + 短路多失效 + `executeAction` 加参 = pre-flip 行为偏差 → T08 批量 DV。下一 = T05 `rewrite_data_files` 规划半。

### 2026-06-24（P6.3-T07c + T08 + T09 实现 ⇒ P6.3 DONE）

- **T07c**（commit `a61cd9262b9`）通用 `RowLevelDmlCommand` 壳 + `RowLevelDmlTransform` 注册表 + `IcebergRowLevelDmlTransform` + 6 instanceof 派发站点重接（Update/DeleteFrom/MergeInto→capability）；合成留 `Iceberg{Delete,Update,Merge}Command` 原地经 transform 委派（D1：仅放宽 3 private→包级，单 live 循环，legacy loop transitional-dead→P6.7 删）；O5-2 现接 dormant（D2：新 `BaseExternalTableInsertExecutor.getConnectorTransactionOrNull()`→iceberg 走 legacy txn→null→不可达直到 P6.6）。fe-core 目标测 **104/0/0**（oracle `IcebergDDLAndDMLPlanTest` 14/0 byte-parity 铁证 + `IcebergRowLevelDmlTransformTest` 7/0）。对抗 `wf_a80f8edb-bed` = 24 raw/0 REAL/24 refuted。
- **T08** 写路径 parity-UT 审计 + deviation 中央登记（设计 `designs/P6.3-T08-write-parity-audit-design.md`）。10 维对抗审计 `wf_c1067212-ab8`（132 agents）= 40 报告→**20 confirmed/20 refuted**→11 交付（8 新测 + 3 强化）：分区 identity 冲突 filter 窄化 / 非-identity 禁窄化 / snapshot 隔离 / PUFFIN DV dedup（连接器 +4）；dataLocation 级联 + ORC/codec 矩阵 + partitionSpecsJson 字节（+2+强化）；O5-2 per-conjunct drop + OR all-or-nothing（fe-core +1+1）；DELETE/UPDATE operation-literal 值断（oracle 2 强化）。**deviation 中央登记 DV-041**（🔴 翻闸 BLOCKER：通用 sink 缺合成列物化+分布=DV-038 同主题新面 + 休眠激活集）/ **DV-042**（北极星 iii 有界：DML 合成 fe-resident）/ **DV-043**（parity-忠实 correctness-bearing）/ **DV-044**（perf/cosmetic/EXPLAIN-diff）。mutation 实证 PUFFIN dedup 测可红已 revert。iceberg UT **389/0/1**（383→389）、fe-core 3 测类绿、0 SPI/BE/fe-core 产品/pom 改、iceberg 仍**不在** `SPI_READY_TYPES`。下一 = **T09 收口（= P6.3 DONE）**。
- **T09**（收口 = P6.3 DONE）写汇总设计 `designs/P6.3-T09-iceberg-write-summary-design.md`（7 节，镜像 `P6-T11`）：架构总览 + T01–T08 逐 task 索引 + **写路径 SPI 收口核对**（与 P6.2「净 0 新 SPI」相反——P6.3 有意 SPI 统一：删双模型 fork + config-bag 三件套→单 `ConnectorTransaction` 写模型 + capability 派发）+ deviation 回指 DV-041..044 + 翻闸阻塞汇总 + 验收门 + 下一阶段。**faithfulness 对抗验证 `wf_9234a18e-1d9`**（6 cluster verifier refute-by-default + 1 completeness critic）= 全 CONFIRMED，唯 1 真错（§5 通用 `visitPhysicalConnectorTableSink` 行号 `:589-627`→实测 `:630-681`，`:589-627` 是 legacy delete+merge visitor）**已修**；critic cheap-check 证 UT 计数静态精确。纯文档 0 产品码、0 BE/pom、iceberg 仍**不在** `SPI_READY_TYPES`。**P6.3 全 9 task DONE**，下一 = **P6.4 procedures**（仍 behind gate）。

### 2026-06-23（P6.3 写路径 RFC ✅ + T01~T05 实现）

- **RFC ✅ 评审通过**（`a49720820f9`）= `06-iceberg-write-path-rfc.md`：写框架全面统一（单 `ConnectorTransaction`）+ 行级-DML Route B（iceberg plan 合成暂留 fe-core，DV-04x）+ O5-2 冲突检测接缝 + Trino 式通用化北极星。
- **T01** 框架统一·SPI 收口（option B）：删 insert-handle/`usesConnectorTransaction` 双模型 fork → 单 `ConnectorTransaction`；jdbc no-op txn 迁移。
- **T02** jdbc thrift 入 `planWrite`（OQ-1）+ 删 config-bag 三件套（OQ-2）+ source-agnostic `appendExplainInfo` EXPLAIN-保留 hook（用户增补）。
- **T03** `IcebergConnectorTransaction implements ConnectorTransaction` 骨架：单 SDK txn/表经 seam+auth、14 字段 `TIcebergCommitData` 反序列化累积、`getUpdateCnt`、新 `WriteOperation` 枚举。对抗 1 confirmed 修（`newTransaction()` 须在 auth 内）。
- **T04** op 选择收进 `commit()`（SPI 无 finishWrite 钩子）+ begin* guards（fmt≥2 / branch 非 tag / baseSnapshotId 捕获）+ 新 `IcebergWriterHelper`/`IcebergPartitionUtils` parse 助手/`IcebergWriteContext`。对抗 0 finding。
- **T05** commit 校验套件（`validateFromSnapshot`/serializable `validateNoConflictingDataFiles`/`validateDeletedFiles`/`validateNoConflictingDeleteFiles`/`validateDataFilesExist`/`delete_isolation_level` 默认 serializable）+ O5-2 `applyWriteConstraint`（新 `ConnectorPredicate` SPI default-no-op + 连接器惰性转 `IcebergPredicateConverter` 暂存 + 与 identity-分区 filter 合并）+ V3 DV `removeDeletes`（fmt≥3 / `ContentFileUtil.isFileScoped` / dedup）。**[D-061] O5-2 fe-core 生产半（analyzed-plan 抽取）挪 T07**（唯一消费者 = T07 `RowLevelDmlCommand`）。对抗 `wf_0960ef5f-52c` = 0 finding。
- **T06** sink 统一（INSERT/OVERWRITE，增量·dormant）：新 `IcebergWritePlanProvider`（`planWrite` 建字节-parity `TIcebergTableSink`）+ 写排序 SPI（`ConnectorWriteSortColumn`/`getWriteSortColumns`）+ 新 `ConnectorContext.getBackendFileType` 接缝 + 声明 `SINK_REQUIRE_FULL_SCHEMA_ORDER`；**首动 fe-core/planner**；legacy sink 链留 P6.7。对抗 `wf_aaa45689-db4` = 2 confirmed〔均已修·均 dormant〕。
- **T07a** DELETE/MERGE sink 方言（连接器·dormant）：`planWrite` switch `writeOperation`→`buildDeleteSink`/`buildMergeSink`（`TIceberg{Delete,Merge}Sink` 字节-parity，⚠️ delete=`compress_type`(6)·merge=`compression_type`(8)·merge `sort_fields`(6) 经 baseColumnFieldIds 过滤·fv≥3 row-lineage）+ `supportsDelete`/`supportsMerge`=true。对抗 `wf_4e117651-e54` = 0 REAL/4 refuted。
- **T07b** O5-2 生产半：新 fe-core `NereidsToConnectorExpressionConverter`（nereids→中立 expr，矩阵=真实 legacy 冲突路 **Option A**，字面量经 `toLegacyLiteral()` 字节 token parity）+ `WriteConstraintExtractor`（移植 legacy 收集半，合成列经注入 `Predicate<SlotReference>` 排除——闭合 critic BLOCKER）+ 连接器 `IcebergPredicateConverter` 加 `conflictMode` flag + `buildConflict*`（移植 `convertPredicateToIcebergExpression`；scan 路 2-arg 字节不变），T05 `buildWriteConstraintExpression` 改 `conflictMode=true`。**实际 `applyWriteConstraint` 调用 + iceberg 排除谓词供给 = T07c**。deviation [DV-T07b-matrix/literal/exclusion]。对抗 `wf_433b98d4-08d` = 0 REAL/4 refuted。
- **验收（T01~T06 + T07a + T07b 累计）**：fe-core UT **28/0/0**（converter 18 + extractor 10）、fe-connector-iceberg UT **383/0/1**（278→383）、connector-api/spi 经 `-am` 绿、jdbc·maxcompute·paimon 无回归、scan 回归门 `IcebergPredicateConverterTest` 17/0 不动、checkstyle 0（fe-core+iceberg）、import-gate 0、iceberg 仍**不在** `SPI_READY_TYPES`、**0 BE / 0 SPI 改**（T01–T07b 全程）。下一 = **T07c 通用 `RowLevelDmlCommand` 壳**（命令壳 + 注册表 + 6 instanceof 派发站点重接 + iceberg 排除谓词供给 + 实际 `applyWriteConstraint` 调用；**实现前单独 checkpoint**）。

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
