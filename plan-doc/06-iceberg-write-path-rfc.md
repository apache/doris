# RFC：Iceberg 写路径（P6.3）

> 设计文档（design-doc-first）。日期 2026-06-23。**须先过 PMC 评审，再实现**（master plan §3.7 / `P6-iceberg-migration.md:80,130`）。
> 本 RFC 是叠加在 **已批准的核心写/事务 SPI RFC**（`tasks/designs/connector-write-spi-rfc.md`，D-022/D-024/D-026）之上的 **iceberg 连接器 adopter + 框架统一**设计，**非**从零重造写 SPI。
> 事实底座：[`research/p6.3-iceberg-write-recon.md`](./research/p6.3-iceberg-write-recon.md)（写 SPI 面 + jdbc/maxcompute/legacy-iceberg 写者深挖 + 12-fork 碎片化地图 + Trino 对照）。原始 workflow 产出 `.audit-scratch/p6.3-research/{findings.md, unification.md, trino-dml-analysis.md}`。
> **用户裁定（2026-06-23，本 RFC 前）**：写框架**全面统一**（Q2=a）；nereids 行级-DML plan 合成层走**务实迁移 Route B / option (i)**（保 EXPLAIN parity，iceberg plan 合成暂留 fe-core 有界 deviation）；O5 冲突检测走 **O5-2**；Trino 式通用化 (iii) 定为**北极星**、列后续专门 RFC。

---

## 1. Goals

1. **iceberg 完整写能力 parity**：INSERT / INSERT OVERWRITE（动态/静态/空表清空）/ DELETE / UPDATE / MERGE + 事务提交 + commit-时冲突检测/快照隔离 + V3 deletion-vector，迁入 `fe-connector-iceberg`，行为与 legacy 等价。
2. **写路径框架在 jdbc / maxcompute / iceberg 三连接器间完全一致**（用户硬约束）：**无为某个连接器实现的框架接口类**。统一到单一 `ConnectorTransaction` 模型 + 单一 plan-provider sink 路径 + capability 派发（无 `instanceof`）。
3. **删 legacy 写半的反向耦合靶**：planner `Iceberg{Table,Delete,Merge}Sink` → 统一 `PhysicalConnectorTableSink`；nereids `Iceberg{Update,Delete,Merge}Command` → 通用 `RowLevelDmlCommand` 壳 + capability 派发。
4. **保 BE 契约不变 / 零 BE 改**：`T{Iceberg}TableSink/DeleteSink/MergeSink` 与 `TIcebergCommitData`（14 字段）一字不动；连接器经 opaque `planWrite` 发同款 thrift。
5. **复用既有面、扩展不重造**：`ConnectorTransaction`/`ConnectorWritePlanProvider`/`PluginDrivenInsertExecutor`/`PluginDrivenTransactionManager`；新增方法 **default-only**（D-009）。
6. **明确北极星**：把 Trino 式「通用引擎 DML 合成 + 声明式连接器 SPI」(iii) 定为目标架构并给出演进触发条件，使本期 (i) 的 fe-resident 残留**可被后续 RFC 彻底消除**。

## 2. Non-goals

- iceberg **PROCEDURES**（`rewrite_data_files`/`expire_snapshots`/`rollback_to_snapshot` 等 10 个 action）→ `ConnectorProcedureOps`(E2) / **P6.4**。本 RFC 只保证不预排除（legacy `RewriteDataFileExecutor` 写半的 `RewriteFiles`/`updateRewriteFiles` 不在本 RFC 解）。
- hive 行级 ACID delete/update/merge：越界（P7）。
- **Trino 式 (iii) 通用化基座**（通用 nereids merge/delete/update 合成 + 声明式 row-id/paradigm SPI）：本 RFC 定为北极星 + 后续 RFC，**不在 P6.3 实现**（§10）。
- **BE 侧改动**：零。
- **`SPI_READY_TYPES` 翻闸**：只在 P6.6（全有或全无）；本 RFC 落地后 iceberg 仍**不在** `SPI_READY_TYPES`。

## 3. Constraints / context（RFC 须遵守）

| # | 约束 | 来源 |
|---|---|---|
| C1 | **import-gate**：连接器模块禁 import `org.apache.doris.{catalog,common,datasource,qe,analysis,nereids,planner}`。实测 0 连接器文件 import nereids。 | D-009 / DV-011；实测 |
| C2 | **零 BE 改**：BE→FE `TIcebergCommitData`、`T{Iceberg}{Table,Delete,Merge}Sink` thrift 不动。 | connector-write-spi-rfc §2 / 用户 |
| C3 | **default-only**：所有新增 SPI 方法带 default（throws/no-op/empty），不破 jdbc/es/trino/paimon/maxcompute。 | D-009 |
| C4 | **commit 载荷 opaque bytes**：`TIcebergCommitData` 经 `TBinaryProtocol`→`addCommitData(byte[])`，连接器自反序列化。零 BE 改、fail-loud。 | D-022 B1 |
| C5 | **无 block-id seam for iceberg**：`allocateWriteBlockRange` 取 default(false)。 | D-022 C1 |
| C6 | **opaque-sink 非 config-bag**：连接器经 `planWrite()` 自建 `TDataSink`，layered on `visitPhysicalConnectorTableSink`。撤回 §12.3/E9 的 config-bag delete/merge 描述（DV-009 已定 opaque-sink 优先）。 | D-022 E / DV-009 |
| C7 | **overwrite/静态分区经 `ConnectorWriteHandle.writeContext`**（无通用 overwrite marker）。 | D-024/D-025/D-026 |
| C8 | **写分布需求经 `ConnectorCapability`**（非连接器特定 planner 码）。 | FIX-WRITE-DISTRIBUTION 先例 |
| C9 | **依赖 P6.2 scan/MVCC**（写需读快照/base-snapshot）。已就绪。 | master plan |
| C10 | **EXPLAIN/执行不回归**（acceptance gate）。Route B 保 plan 形 parity；统一 sink 后的 EXPLAIN diff 须登记并经 PMC 接受（§9）。 | P6.3 gate |

> **Rule 7 撤回声明**：旧设计文本 §12/E9 描述的 `getDeleteConfig`/`getMergeConfig` + `ConnectorWriteType.FILE_DELETE`/`FILE_MERGE` **在树里不存在**（recon firsthand 证伪），本 RFC **正式撤回**该 config-bag delete/merge 计划；iceberg DELETE/MERGE 与 INSERT 同走 `beginTransaction`→`planWrite`→`addCommitData`→`commit`，由 `ConnectorWriteHandle` 上的 `writeOperation` 区分。

## 4. 决策总览（用户/PMC 裁定）

| 轴 | 裁定 | 备选（拒） |
|---|---|---|
| **写框架统一深度**（Q2） | **(a) 全面统一**：单 `ConnectorTransaction` 模型；删 `usesConnectorTransaction()` fork + `ConnectorInsertHandle`/`beginInsert·finishInsert·abortInsert` + dead 的 `beginDelete/beginMerge` handle 面；jdbc 变退化 no-op txn；改 jdbc/maxcompute 配字节 parity 测。 | (b) 保守保 fork（与"无连接器特定接口"冲突，拒） |
| **nereids 行级-DML plan 合成层**（Q1） | **Route B / option (i)**：通用 `RowLevelDmlCommand` 壳 + capability 派发（无 `instanceof`），iceberg 的 `$row_id`/branch-label/投影代数 + nereids→iceberg expr 转换暂留 fe-core（**有界 deviation**），保现有 plan/EXPLAIN parity。 | (ii) 新 nereids-spi 模块放松 import-gate（为单一消费者放松核心不变量，违 Rule 2，拒）；(iii) 通用化重写（北极星，工程大/破 EXPLAIN parity，本期不做） |
| **O5 冲突检测 seam**（Q3） | **O5-2**：`ConnectorTransaction.applyWriteConstraint(ConnectorPredicate)` default-no-op；fe-core 通用抽 target-only 合取→中性 `ConnectorPredicate`；连接器复用 P6.2-T02 `IcebergPredicateConverter` 转 iceberg expr、暂存到 commit。 | O5-1（`writeContext` 字符串载、生命周期错位）；O5-3（暴露 plan 视图、撞 import-gate，拒） |
| **北极星** | Trino 式 (iii) 通用引擎 DML 合成 + 声明式连接器 SPI，定后续专门 RFC（演进触发 = hive/paimon 第二行级-DML 消费者落地）。 | — |

**Trino 实证**（`trino-dml-analysis.md`）：Trino 连接器主代码 0 优化器 import，DML plan 合成全在引擎核心，连接器只供 `getMergeRowIdColumnHandle`(row-id handle) + `getRowChangeParadigm`(paradigm) + `ConnectorMergeSink`；冲突检测谓词走读下推 `Constraint` 同一 seam（验证 O5-2）。⇒ (iii) 是已落地的正确终态；本期 (i) 是其务实前身。

## 5. 架构设计

### 5.0 全景

```
┌──────────────────────── fe-core 通用写编排（统一后，无 instanceof）────────────────────────┐
│ InsertIntoTableCommand / RowLevelDmlCommand(通用壳)                                          │
│   → 按 ConnectorCapability 派发(supportsDelete/supportsMerge)，非 instanceof IcebergExternal* │
│ PluginDrivenInsertExecutor: 单一 ConnectorTransaction 模型(删 usesConnectorTransaction fork)  │
│   beginTransaction → planWrite(opaque TDataSink) → addCommitData(byte[]) → commit/rollback    │
│ Coordinator/LoadProcessor: txn.addCommitData(byte[])     (B1, 已存在)                          │
│ PhysicalPlanTranslator: visitPhysicalConnectorTableSink  (E, 删 visitPhysicalIceberg*Sink)     │
│ [有界 deviation] iceberg 行级-DML plan 合成(连接器-键控但 fe-resident，§5.3)                    │
└───────────────┬─────────────────────────────────────────────────────────┬────────────────────┘
       持有 fe-core Transaction(多态)                          经 ConnectorWritePlanProvider 取 TDataSink
                │                                                            │
   ┌────────────┴───────────────┐  wraps & delegates    ┌────────────────────┴──────────────────┐
   │ PluginDrivenTransaction    │ ───────────────────▶  │ fe-connector-iceberg(plugin, 隔离)      │
   │ implements fe-core Transaction                     │  IcebergConnectorTransaction            │
   └────────────────────────────┘                       │  ConnectorWritePlanProvider(planWrite)  │
                                                         │  applyWriteConstraint(O5-2)             │
                                                         └─────────────────────────────────────────┘
```

### 5.1 框架统一（Q2=a）—— 单 `ConnectorTransaction` 模型

**删除（消除连接器特定 framework 接口）**：
- `ConnectorWriteOps.usesConnectorTransaction()`（F1，路由开关）。
- `ConnectorWriteOps.{beginInsert, finishInsert, abortInsert}` + `ConnectorInsertHandle`（insert-handle 模型，jdbc 专形）。
- dead 的 `ConnectorWriteOps.{beginDelete, finishDelete, abortDelete, beginMerge, finishMerge, abortMerge}` + `ConnectorDeleteHandle`/`ConnectorMergeHandle`（从未接线、对事务式文件写错形）。
- `ConnectorWriteType.{JDBC_WRITE, REMOTE_OLAP_WRITE}`（F4，连接器命名值）；保 `FILE_WRITE`/`CUSTOM`（或整 enum 退化为 profile label，§6 待定项）。

**统一为**：
- **`beginTransaction(session) → ConnectorTransaction` 变 mandatory**（默认返回退化 no-op txn）。所有写（INSERT/OVERWRITE/DELETE/UPDATE/MERGE）经 `beginTransaction` → `planWrite` → `addCommitData(byte[])`(逐 fragment) → `commit()`。
- **写操作种类**由 `ConnectorWriteHandle.writeOperation`(新增枚举字段 INSERT/OVERWRITE/DELETE/UPDATE/MERGE) 携带，单一 `planWrite` 据它选 sink 方言。
- **jdbc** = 退化 adopter：`beginTransaction` 返回 `JdbcNoOpTransaction`（commit/rollback no-op，`getUpdateCnt` 读 BE 上报行数）；jdbc 的 thrift 装配从 fe-core `bindJdbcWriteSink` **移入 jdbc 连接器 `planWrite`**（消 F2 fe-core 拥有连接器 thrift 的泄漏）。
- **maxcompute** = 已是 txn 模型，几乎不动（保 block-id seam F5 = 已正确隔离的本质，default-false，iceberg 忽略）。
- **`PluginDrivenInsertExecutor`** 删 `beforeExec`/`doBeforeCommit` 双臂（F7/F8）→ 单路：`beginTransaction`→exec→`addCommitData`(report 路)→`finishWrite`→`txn.commit()`/`txn.getUpdateCnt()`。删 `transactionType()` 硬编 enum（F3）→ SPI 提供 profile label。

> **✅ OQ-1 裁定（2026-06-23）= 本期移入**：jdbc thrift 装配（`bindJdbcWriteSink`）移入 jdbc 连接器 `planWrite`，**F2 全消**、不留 fallback。须 `PROP_JDBC_*`/`connection_pool_*` 键**字节 parity 测**（T02）。⇒ config-bag 路径整条死（连带 OQ-2，见 §6）。

### 5.2 iceberg 事务 adopter —— `IcebergConnectorTransaction`

实现 `ConnectorTransaction`，镜像 legacy `IcebergTransaction`(981) 语义（recon §3 复现清单），连接器内自包含（仅 import iceberg SDK + `connector.api`）：

- **持单 SDK `org.apache.iceberg.Transaction` / 表 / 语句**；`begin*`(经 P6.2 `IcebergCatalogOps` seam loadTable + auth 包裹) `table.newTransaction()`；捕获 `baseSnapshotId`/`startingSnapshotId` 供 commit 校验；delete/merge guard format-version ≥ 2；insert 解析+校验 branch（须 branch 非 tag）。
- **`addCommitData(byte[])`**：`TDeserializer(TBinaryProtocol)` 反序列化 `TIcebergCommitData`，`synchronized` 累积（C4）。消费全 14 字段 + `TIcebergColumnStats`（recon §3.6）。
- **op 选择**（recon §3.5）：`writeOperation` + overwrite-mode → AppendFiles / ReplacePartitions / OverwriteFiles(空表清空) / OverwriteFiles.overwriteByRowFilter(静态) / RowDelta(delete 仅 deletes；merge rows+deletes)。`IcebergWriterHelper` 等价物（BE 人类可读分区串→`PartitionData`、`TIcebergColumnStats`→`Metrics`、DV→PUFFIN+position-deletes、equality-delete 拒绝）连接器内移植。
- **`commit()`** = `transaction.commitTransaction()`（无 FE 重试，靠 SDK 乐观提交；冲突 SDK 抛→executor rollback）。`rollback()` = 丢弃未提交 manifest（no-op 不 commit）。
- **commit-时校验套件**（recon §3.7）：`validateFromSnapshot(baseSnapshotId)`；`applyWriteConstraint` 注入的优化器 filter（O5-2，§5.4）与 commit-时 identity-分区 filter AND 合并 → `rowDelta.conflictDetectionFilter`；serializable→`validateNoConflictingDataFiles`；`validateDeletedFiles`/`validateNoConflictingDeleteFiles`/`validateDataFilesExist`。隔离级读表属性 `delete_isolation_level`（默认 serializable）。
- **V3 DV "rewrite previous delete files"**：`removeDeletes(...)` 旧 file-scoped delete 文件，由 scan-node 派生的 `rewrittenDeleteFilesByReferencedDataFile` 喂（P6.2-T04 delete 信息已在连接器 scan 侧；写半的关联映射本 RFC 接线）。
- **`getUpdateCnt()`**：affectedRows-或-rowCount、data/delete 拆分、dataRows 优先（recon §3.9）。
- **txn-id 绑定**：iceberg 自带 id（U3 连接器分配）；双注册表（per-manager + `GlobalExternalTransactionInfoMgr`）保持（report 路按 id 找 txn）。`PluginDrivenTransaction` 桥接到 fe-core `Transaction`（已有）。

### 5.3 行级 DML（nereids 层，Route B / option i）—— 有界 deviation

**通用化（消反向 instanceof）**：
- 新 fe-core 通用 `RowLevelDmlCommand` 壳，吸收三命令 ~50% 通用脚手架（recon §4：run/explain、copy-on-write 检查、`icebergRowIdTargetTableId` save/restore、`executeWithExternalTableBatchModeDisabled`、planner-drive loop、`getPhysicalSink`/`childIsEmptyRelation`、conflict-filter plumbing）。
- `UpdateCommand`/`DeleteFromCommand`/`MergeIntoCommand` 的路由从 `instanceof IcebergExternalTable` → **capability 查询**（`metadata.supportsDelete()`/`supportsMerge()`，已存在）。任何声明该能力的连接器派发到通用 `RowLevelDmlCommand`。

**有界 deviation（暂留 fe-core，连接器-键控）**：iceberg 的 ~50% 不可约 plan 合成（`$row_id` 注入 = `IcebergNereidsUtils.IcebergRowIdInjector`；operation-number/branch-label 投影代数 = `IcebergMergeCommand` 等价；nereids→iceberg expr 转换 = `IcebergNereidsUtils` cluster B）**因根本性需 nereids 类型、连接器禁 import**，保留在 fe-core，由 `RowLevelDmlCommand` 经**连接器-键控变换注册表**（非 `instanceof`）调用。`ConnectContext.icebergRowIdTargetTableId` thread-local + `IcebergExternalTable.needInternalHiddenColumns` scan-schema hook 同属此 deviation（写驱动的 scan-schema 变异，同 import-gate 墙）。

> **登记为 DV-04x（本 RFC 新）**：iceberg DML plan 合成 fe-resident。**理由**：import-gate 是既有架构不变量；为单一消费者放松它（option ii）违 Rule 2。**消除路径**：北极星 (iii) 通用化重写（§10），届时此 deviation 关闭。**约束**：deviation 仅限 plan-合成叶子；框架（txn/commit/sink/dispatch）零 iceberg 特定码。

### 5.4 O5 冲突检测（O5-2 seam）

- **新 SPI（default-no-op）**：`ConnectorTransaction.applyWriteConstraint(ConnectorPredicate targetOnlyFilter)`。
- **fe-core（通用）**：`RowLevelDmlCommand` 在 analyzed plan 上抽 target-only 合取（slot 的 origin-table == 目标表，排 `$row_id`/metadata 列——通用 slot-origin 过滤，非 iceberg 特定），转中性 `ConnectorPredicate`（复用 scan 下推已有的 `ConnectorExpression` 表示），经 `transaction.applyWriteConstraint(pred)` 交连接器。
- **连接器**：`IcebergConnectorTransaction.applyWriteConstraint` 用 **P6.2-T02 已造的 `IcebergPredicateConverter`**（`ConnectorExpression`→iceberg `Expression`）转换、暂存；commit 时与 identity-分区 filter 合并应用（§5.2）。
- jdbc/maxcompute：default no-op 忽略。
- **与 Trino 一致**：Trino 冲突检测谓词走读下推 `Constraint` 同一 seam；O5-2 是其 Doris 对应（中性谓词到连接器、连接器转 SDK expr）。

### 5.5 Sink 统一（删 3 iceberg sink）

- 删 planner `IcebergTableSink`/`IcebergDeleteSink`/`IcebergMergeSink` + translator `visitPhysicalIceberg{Table,Delete,Merge}Sink`（F9 FE 侧）。
- iceberg 经 `ConnectorWritePlanProvider.planWrite(session, ConnectorWriteHandle)` 据 `writeOperation` 自建 `TIcebergTableSink`/`TIcebergDeleteSink`/`TIcebergMergeSink`（**同款 thrift、C2 零 BE 改**），layered on 既有 `visitPhysicalConnectorTableSink`（DV-009 路径）。
- iceberg-特定 thrift 字段（schema-json/sort/partition-spec/row-lineage/`rewritableDeleteFileSets`/`setMaterializedColumnName`）在连接器 `planWrite` 内构建。vended-creds 经 P6.2-T09 既有接缝。
- 写分布/sort 需求经 `ConnectorCapability`（C8，FIX-WRITE-DISTRIBUTION 先例），非连接器特定 planner 码。

## 6. SPI 变更清单

| 类别 | 变更 | 影响 |
|---|---|---|
| **新增（default-no-op）** | `ConnectorTransaction.applyWriteConstraint(ConnectorPredicate)`（O5-2） | jdbc/maxcompute/es/trino/paimon 零影响 |
| **新增** | `ConnectorWriteHandle.writeOperation`（INSERT/OVERWRITE/DELETE/UPDATE/MERGE 枚举）+ `ConnectorTransaction.profileLabel()`（default，替 F3） | 默认 INSERT，向后兼容 |
| **删除** | `ConnectorWriteOps.usesConnectorTransaction()`（F1） | **改 maxcompute**（去 override）；fe-core executor 去 fork |
| **删除** | `ConnectorWriteOps.{beginInsert,finishInsert,abortInsert}` + `ConnectorInsertHandle`（insert-handle 模型） | **改 jdbc**（迁到 no-op txn 模型 + planWrite） |
| **删除** | `ConnectorWriteOps.{beginDelete,finishDelete,abortDelete,beginMerge,finishMerge,abortMerge}` + `ConnectorDeleteHandle`/`ConnectorMergeHandle`（dead 面） | 无（dead，未接线） |
| **删除（OQ-2）** | config-bag 三件套：`ConnectorWriteType` enum + `ConnectorWriteConfig` 类 + `ConnectorWriteOps.getWriteConfig` + `PluginDrivenTableSink` config-bag 分支（F2/F4，实测仅 jdbc 用，OQ-1 移入后死） | jdbc thrift 经 `planWrite` 自建；profile 标签从 `writeOperation`/`profileLabel` |
| **fe-core 新增** | 通用 `RowLevelDmlCommand` 壳 + 连接器-键控 plan-变换注册表（capability 派发） | 替 3 iceberg 命令路由 instanceof |
| **fe-core 删除** | planner `Iceberg{Table,Delete,Merge}Sink` + translator `visitPhysicalIceberg*Sink` | iceberg 走 `visitPhysicalConnectorTableSink` |

**待定项裁定（2026-06-23 用户签字）**：
- **✅ OQ-1 = 本期移入**：jdbc thrift 装配移入 jdbc 连接器 `planWrite`，F2 全消、不留 fallback（须字节 parity 测，T02）。
- **✅ OQ-2 = 整组删除 config-bag 三件套**：`ConnectorWriteType` enum + `ConnectorWriteConfig` 类 + `ConnectorWriteOps.getWriteConfig` 方法 + `PluginDrivenTableSink` 的 config-bag 分支。**实测这一整组仅 jdbc config-bag 路在用**（`PluginDrivenTableSink:179` `writeType==JDBC_WRITE`→`TJdbcTableSink`；`PluginDrivenInsertExecutor:221`；`PhysicalPlanTranslator:677`；maxcompute/iceberg 不碰），OQ-1=移入后整条死，无通用消费者残留 → 直接删，**不留作 label hint**（profile/EXPLAIN 写类型标签从 `ConnectorWriteHandle.writeOperation`/`profileLabel()` 取）。`FILE_DELETE`/`FILE_MERGE` 问题随枚举删除自动 moot。
- **✅ OQ-3 = 接受为非回归**：统一 sink 后 EXPLAIN 文本 diff（`PhysicalConnectorTableSink` vs `IcebergTableSink` 显示，plan-形不变仅 sink 标签）接受为非回归，登记 deviations-log（T08）。

## 7. 数据流（端到端，统一后）

```
INSERT/OVERWRITE:  RowLevelDmlCommand?No → InsertIntoTableCommand
  → executor.beginTransaction()=txnMgr.begin()→IcebergConnectorTransaction(table.newTransaction())
  → finalizeSink/planWrite(writeOp=INSERT/OVERWRITE, writeContext={overwrite,staticPartition})→TIcebergTableSink
  → BE 写 data 文件 → report 路 addCommitData(TIcebergCommitData) 累积
  → onComplete: getUpdateCnt() + finishInsert(updateManifestAfterInsert: Append/Replace/Overwrite) → commit()=commitTransaction()

DELETE/UPDATE/MERGE:  RowLevelDmlCommand(capability supportsDelete/Merge)
  → [fe-resident deviation] iceberg plan 合成: $row_id 注入 + op-number/branch-label 投影
  → applyWriteConstraint(target-only ConnectorPredicate)  ← O5-2(plan 时抽，连接器转 iceberg expr 暂存)
  → beginTransaction → planWrite(writeOp=DELETE/MERGE)→TIcebergDeleteSink/TIcebergMergeSink
  → BE 写 position-delete/DV(+data for merge) → report 路 addCommitData 累积
  → onComplete: finishDelete/Merge(updateManifestAfterDelete/Merge: RowDelta + applyWriteConstraint filter
       + validateFromSnapshot + serializable validateNoConflictingDataFiles + V3 removeDeletes) → commit()
```

## 8. 反向 instanceof 清理（与 P6.7 关系）

写层 ~49 处反向 `instanceof IcebergExternal*`（recon §4 全量）本 RFC **部分清**：路由 6 处 → capability 派发；planner sink cast + translator cast → 删 sink 类后消。**保留**（属 §5.3 deviation）：fe-resident iceberg plan 合成内的 cast（`IcebergNereidsUtils`、plan-变换实现）→ 北极星 (iii) 时随 deviation 关闭。其余（catalog/statistics/glue 等读侧）属 P6.7。

## 9. 测试 / parity / 回滚

- **UT**：`IcebergConnectorTransaction`（op 选择矩阵 / commit 校验套件 / V3 DV / getUpdateCnt / addCommitData 14 字段往返）；`applyWriteConstraint`→`IcebergPredicateConverter` 复用；通用 `RowLevelDmlCommand` capability 派发；jdbc no-op txn parity。镜像 P6.2 风格（真 InMemoryCatalog、无 Mockito、fail-loud）。
- **regression（P6.6 docker，翻闸后）**：INSERT/OVERWRITE(动态/静态/空表)/DELETE/UPDATE/MERGE 结果 parity；并发冲突→serializable 中止；V3 DV merge；事务回滚。
- **parity gate（C10）**：Route B 保 plan 形 parity；EXPLAIN sink-标签 diff 登记 OQ-3 + deviations-log。jdbc/maxcompute 写**字节 parity 测**（框架统一不得改其 thrift 输出，除 OQ-1 jdbc 移位时显式 parity）。
- **回滚**：iceberg 不在 `SPI_READY_TYPES`（翻闸只 P6.6），本 RFC 落地全程 legacy 写路径仍在、零行为变更直到 P6.6；框架统一改动（删 fork、jdbc no-op txn）behind gate 对 LIVE jdbc/maxcompute 须 golden 等价。

## 10. 北极星：Trino 式 (iii) 通用化（后续 RFC）

**目标架构**（Trino 实证，`trino-dml-analysis.md`）：连接器供 3 个声明式 SPI（row-id `ConnectorColumnHandle` + `RowChangeParadigm` 枚举 + 通用 merge sink），引擎核心通用做全部 DML plan 合成（$row_id 声明式注入 scan、op-number/branch-label/CASE 投影、MERGE join、运行时 delete/insert 展开）。连接器 **0 优化器类型**，§5.3 的 fe-resident deviation 彻底关闭。

**演进触发条件**：hive(P7) / paimon 第二个行级-DML 消费者落地（Rule 2「多消费者」满足）。**成本**：通用 nereids merge-合成 + 声明式 row-id 注入机制（Doris 今无、Trino 有）；**EXPLAIN plan 形会变**（须届时放宽 plan-层 parity gate）；BE 大概率不改（连接器仍经 opaque `planWrite` 发 iceberg sink thrift）。**本 RFC 的 (i) 设计为此预留**：框架已统一、命令已通用壳化、O5 已 seam 化 → 届时只需把 fe-resident plan 合成替换为通用 paradigm-driven 合成。

## 11. TODO（实现顺序，过 PMC 后）

> 串行、每步 RED→GREEN + 对抗 parity 复核（镜像 P6.2 节奏）。框架统一改动 behind gate、零行为变更。

1. **T01 框架统一·SPI 收口**：删 `usesConnectorTransaction`/`ConnectorInsertHandle`/insert-handle 方法/dead delete-merge handle 面；`beginTransaction` mandatory + 退化 no-op 默认；`ConnectorWriteHandle.writeOperation`；`ConnectorTransaction.profileLabel`。改 maxcompute（去 override）。UT + checkstyle。
2. **T02 jdbc 退化 adopter**：jdbc → no-op txn 模型 + jdbc thrift 装配移入连接器 `planWrite`（OQ-1）+ 删 config-bag 三件套 `ConnectorWriteType`/`ConnectorWriteConfig`/`getWriteConfig`/`PluginDrivenTableSink` config-bag 分支（OQ-2）；jdbc 写**字节 parity 测**。
3. **T03 `IcebergConnectorTransaction` 骨架 + addCommitData**：SDK txn 持有 + 14 字段反序列化 + getUpdateCnt + txn-id 双注册表桥接。
4. **T04 op 选择 + `IcebergWriterHelper` 等价**：INSERT/OVERWRITE（4 子case）+ DELETE + MERGE 的 SDK op + PartitionData/Metrics/DV 转换。
5. **T05 commit 校验套件 + O5-2**：`applyWriteConstraint` SPI(default-no-op) + fe-core target-only 抽取 + 连接器 `IcebergPredicateConverter` 复用 + commit 校验套件 + V3 DV removeDeletes。
6. **T06 sink 统一**：连接器 `planWrite` 自建 3 thrift sink 方言；删 planner `Iceberg{Table,Delete,Merge}Sink` + translator 分支；走 `visitPhysicalConnectorTableSink`。EXPLAIN diff 登记。
7. **T07 通用 `RowLevelDmlCommand` 壳 + capability 派发**：抽 ~50% 通用脚手架；路由 instanceof → capability；iceberg plan 合成经连接器-键控注册表调用（DV-04x）。
8. **T08 parity-UT 审计 + deviation 注册**：补 gap-fill；DV-04x（fe-resident plan 合成）+ EXPLAIN-diff + jdbc-移位（若 OQ-1）登记 deviations-log。
9. **T09 收口**：HANDOFF + PROGRESS + connectors 同步；gate 核对（iceberg 仍不在 `SPI_READY_TYPES`）。

## 12. 引用

- 事实底座：`research/p6.3-iceberg-write-recon.md`；workflow 产出 `.audit-scratch/p6.3-research/{findings.md, unification.md, trino-dml-analysis.md}`。
- 既有 SPI：`tasks/designs/connector-write-spi-rfc.md`；`01-spi-extensions-rfc.md` §7/§12/§20；`decisions-log.md` D-021..D-026；`deviations-log.md` DV-009/011/012/013/018。
- legacy iceberg 写：`datasource/iceberg/{IcebergTransaction,IcebergMetadataOps,IcebergConflictDetectionFilterUtils,IcebergNereidsUtils}.java`、`helper/*`、`transaction/IcebergTransactionManager.java`、`nereids/.../commands/Iceberg{Update,Delete,Merge}Command.java`、`planner/Iceberg{Table,Delete,Merge}Sink.java`。
- Trino 北极星：`/mnt/disk1/yy/git/trino` core-spi `ConnectorMetadata.java:898-942`、`RowChangeParadigm.java`、`ConnectorMergeSink.java`；trino-main `QueryPlanner.java:740-990`、`StatementAnalyzer.java:2299-2322`、`MergeProcessorOperator.java:58-71`；plugin-trino-iceberg `IcebergMetadata.java:2214-2374`。
- 迁移计划：`tasks/P6-iceberg-migration.md` P6.3(:49,:68,:91)、O5(:106)。
