# 连接器写/事务 SPI — code-grounded research note（3 写者 + paimon 前瞻）

> 产出 2026-06-06，P4 启动·scope=C（写-SPI RFC 先行）。用户指令：**先完整调研 maxcompute / hive / iceberg 三个现存写者的写入能力，再做完整设计；paimon 当前不写但后续会写，需前瞻纳入**。
> 方法：11 路只读 Explore code-grounded 调研（6 maxcompute 面 + 写框架 + 现存 SPI + paimon + hive 深挖 + iceberg 深挖）+ 主线 firsthand 核读 leak 锚点。
> 用途：research-design-workflow 的 research note；写-SPI RFC（设计文档）的事实底座与 fork 清单。**本文不是设计定稿**——设计待用户在 §8 forks 给方向后再写。

---

## 1. 三写者写入能力一览（write surface）

| 能力 | maxcompute | hive（HMSTransaction）| iceberg（IcebergTransaction）|
|---|---|---|---|
| INSERT（append）| ✅ | ✅ `HiveInsertExecutor:46` | ✅ `IcebergTransaction.beginInsert:129` |
| INSERT OVERWRITE | ✅ | ✅（partition append/overwrite，`HMSTransaction:247-312`）| ✅ 动态/静态（`commitReplaceTxn:838`/`commitStaticPartitionOverwrite:878`）|
| 行级 DELETE | ❌ | ❌（仅 `HiveTransaction` 读侧 ACID 校验，非写）| ✅ position delete（`beginDelete:268`，`RowDelta`）|
| UPDATE/MERGE | ❌ | ❌ | ✅ merge-on-read v2+（`beginMerge:295`）|
| PROCEDURES | ❌ | ❌ | ✅ rewrite_data_files / expire_snapshots（`ExecuteActionFactory:99`，**非 SPI**，自定义 action）|
| schema evolution on write | ❌ | ❌ | ✅（`SchemaParser.toJson` in `IcebergTableSink:137`）|

> 关键：**hive 当前不做行级 ACID 写**（R-002 主要是读侧一致性 + 外部 compaction 风险）；**iceberg 是三者中写面最宽**（insert+delete+merge+procedures）。设计若只对 maxcompute 会漏掉 iceberg 的 delete/merge/procedure 形态。

---

## 2. 公共写生命周期（三者同骨架）

```
1. beginTransaction      → transactionManager.begin() → 连接器 Transaction（txnId 记入 GlobalExternalTransactionInfoMgr）
2. begin{Insert/Delete/Merge} → 连接器专有 begin（load table、建 session/manifest/staging）
3. FE 建连接器专有 thrift sink（T{MaxCompute/Hive/Iceberg}TableSink）于 *TableSink.bindDataSink()
4. BE 执行写 → 发连接器专有 commit 载荷（TMCCommitData / THivePartitionUpdate / TIcebergCommitData）
   └─ maxcompute 额外：BE↔FE allocateBlockIdRange RPC（写期间）
5. FE 收 commit 载荷 → 连接器.updateXxxCommitData()   ← ★ LEAK：Coordinator/LoadProcessor 里 concrete cast
6. finish{Insert/Delete/Merge/Rewrite} → 连接器把 commit 数据落到自己元数据（ODPS session.commit / HMS action queue+FS rename / iceberg manifest txn）
7. transactionManager.commit(txnId) → 连接器.commit() / rollback()
8. getUpdateCnt() → 结果行数
```

第 1/2/6/7/8 步**已是接口化形状**（`Transaction`/`TransactionManager`/begin/finish/commit）；**真正的 leak 在第 4→5 步**（typed BE commit 载荷经 concrete cast 进连接器）+ maxcompute 第 4 步的 block-id RPC。

---

## 3. 各写者模型（精炼）

### maxcompute（有状态 session + FE 分配 block-id）
- `MCTransaction`：ODPS Storage API `TableBatchWriteSession`；`beginInsert`(建 session+writeSessionId) → BE 写、`allocateBlockIdRange`(BE↔FE RPC) → BE 回 `WriterCommitMessage`(序列化二进制) 经 `updateMCCommitData` → `finishInsert`(反序列化 + `session.commit`)。
- 专有数据：writeSessionId、block-id 范围、`WriterCommitMessage`（opaque）。
- sink：`TMaxComputeTableSink`（endpoint/project/credentials/partition + 运行期 write_session_id/txn_id/block_ids）。

### hive（无状态文件 IO + HMS 批元数据；staging+rename）
- `HMSTransaction`：`beginInsertTable`(ctx:queryId/overwrite/writePath) → BE 写 staging、发 `THivePartitionUpdate`(name/mode/file_names/row_count/file_size/S3-MPU) 经 `updateHivePartitionUpdates` → `finishInsertTable`(转 action queue：add/alter partition) → `commit`(FS rename + HMS API + stats + S3 MPU complete)。
- **无 block-id、无 write-id**；分区级原子性靠 action queue + FS staging+rename。
- R-002：外部 Hive compaction 产生 Doris 不追踪的 write-id → 读一致性风险（设计可不解，登记）。
- sink：`THiveTableSink`（db/table/columns/partitions/format/location/hadoop_config/overwrite）。

### iceberg（无状态 manifest/snapshot；写面最宽 + procedures）
- `IcebergTransaction`：begin{Insert/Delete/Merge/Rewrite} → BE 写数据/删除文件、回 `TIcebergCommitData`(file_path/row_count/partition/column_stats/delete-file 信息) 经 `updateIcebergCommitData` → finish{Insert→Append/Replace/Overwrite；Delete/Merge→RowDelta；Rewrite→RewriteFiles} → `transaction.commit()`。
- DELETE：position delete files / deletion vectors(v3)；conflict detection filter。
- PROCEDURES：`ALTER TABLE EXECUTE rewrite_data_files(...)` 经 `ExecuteActionCommand`→`ExecuteActionFactory`→`IcebergRewriteDataFilesAction`→`RewriteDataFileExecutor`（cast `IcebergTransaction`，`beginRewrite/updateRewriteFiles/finishRewrite`）。**当前是硬编码 action，非 `ConnectorProcedureOps`**。
- sink：`TIcebergTableSink`（schema_json/partition_specs/sort/format/write_type INSERT|REWRITE）+ `TIcebergDeleteSink`（delete_type POSITION_DELETES|DELETION_VECTOR/format_version）。

---

## 4. 对比矩阵（COMMON ⊥ DIVERGENT）= 设计核心输入

| 维度 | COMMON（可泛化为 SPI）| DIVERGENT（连接器专有，需 opaque/seam）|
|---|---|---|
| 事务壳 | begin/commit/rollback + txnId 注册（三者同 `Transaction`/`AbstractExternalTransactionManager`）| 无 |
| 操作粒度 | begin/finish per-op（SPI 已有 insert/delete/merge）| 哪些 op 支持：mc/hive=insert；iceberg=+delete/merge/rewrite |
| BE→FE commit 载荷 | 「BE 写完回一批 commit 数据给连接器」这一动作 | **载荷类型**：opaque binary(mc) / typed partition-update(hive) / typed file-metadata(iceberg) |
| 落元数据 | finish 钩子 | 机制：ODPS session.commit / HMS action queue+rename / iceberg manifest |
| 写期 BE↔FE 交互 | （多数无）| **block-id 分配**：maxcompute-only RPC |
| thrift sink | 「连接器产 sink desc 给 BE」 | 每连接器自有 T*TableSink（BE 已认，不变）|
| procedures | — | iceberg-only（rewrite 等）|
| MVCC 读快照 | SPI 已有 `beginQuerySnapshot/getSnapshotAt/ById`| iceberg/paimon 用；mc/hive 不用 |

**结论**：公共骨架可泛化；分歧集中在 **(i) commit 载荷类型、(ii) maxcompute block-id、(iii) iceberg procedures/多 op**。设计 = 泛化骨架 + 为这 3 处留 seam。

---

## 5. 现存 SPI 写面（P0，`ConnectorWriteOps`）— 形状已在，仅 JDBC 实现

- `supportsInsert/Delete/Merge()`→false；`getWriteConfig→ConnectorWriteConfig`（throws）；
- `beginInsert→ConnectorInsertHandle` / `finishInsert(session,handle,Collection<byte[]> fragments)` / `abortInsert`（JDBC override insert）；
- `beginDelete/finishDelete/abortDelete`、`beginMerge/finishMerge/abortMerge`（throws/no-op）；
- `beginTransaction(session)→ConnectorTransaction`；`ConnectorTransaction extends ConnectorTransactionHandle, Closeable`：`getTransactionId/commit/rollback/close`；
- `ConnectorInsert/Delete/MergeHandle`（opaque）；`ConnectorWriteType{FILE_WRITE,JDBC_WRITE,REMOTE_OLAP_WRITE,CUSTOM}`；
- `ConnectorSession.getCurrentTransaction→Optional<ConnectorTransaction>`；`ConnectorTableOps.createTable×2/dropTable`；
- MVCC：`ConnectorMvccSnapshot` + `beginQuerySnapshot/getSnapshotAt/getSnapshotById`（paimon 读用）。
- **关键洞察**：Trino 式 begin/finish + opaque handle + `Collection<byte[]>` fragments **已经是现成形状**；`finishInsert` 收 `Collection<byte[]>` 正好可承接「BE commit 载荷序列化为 bytes」。`PluginDrivenInsertExecutor` + `PluginDrivenTransactionManager`(P0-T11 加 `begin(ConnectorTransaction)`) 脚手架已存在。

---

## 6. 必须消除的 leak（generic 层 concrete cast）

| 站点 | cast | 替换为 |
|---|---|---|
| `Coordinator:2531/2536/2539` | `((HMS/Iceberg/MC)Transaction) …).updateXxxCommitData(typed)` | 多态 SPI：把 BE commit 载荷交连接器（§8-B）|
| `LoadProcessor:232-240` | 同上三 cast | 同上 |
| `FrontendServiceImpl:3697-3702` | `((MCTransaction)txn).allocateBlockIdRange(...)` | 连接器写期 RPC seam（§8-C）|
| `RewriteDataFileExecutor:61` | `((IcebergTransaction)…).beginRewrite/finishRewrite` | iceberg procedure，**本 RFC 不解**（§8-D defer）|

---

## 7. paimon 前瞻（今读、后写）

- 今：**只读 + MVCC**（`pom.xml:40`「DML 暂留 fe-core」；`PaimonConnectorMetadata` 不 impl `ConnectorWriteOps`；无 Paimon*Sink/Transaction）。MVCC 读已用 SPI `beginQuerySnapshot` 等。
- 后（P5）写：预计 Paimon `BatchWriteBuilder`/`TableWrite`/`TableCommit`，commit 载荷 paimon-native。落进**与 iceberg 同形**（manifest/snapshot 式、无 block-id、有 MVCC）。
- 设计约束：写 SPI 必须**允许 paimon 后续以 opaque handle + bytes-fragment + ConnectorTransaction 接入，零 SPI 改动**（验证：W4 verdict 现有形状足够，paimon 写时只需 impl `ConnectorWriteOps` + 仿 `PluginDrivenInsertExecutor`）。

---

## 8. 关键设计 FORKS（待用户给方向，再写 RFC）

> A/E 给出推荐默认（不同意再说）；**B/C/D 是真分歧，请签字**。

**A.〔事务模型统一〕**（推荐默认）连接器 `ConnectorTransaction` 成单一事实源；fe-core `MCTransaction/HMSTransaction/IcebergTransaction` 逻辑**迁入各自连接器模块**（在 P4/P6/P7 执行期搬），generic 层经 `PluginDrivenTransactionManager` 桥接，只调多态 SPI。← 与已迁连接器一致；确认是否反对。

**B.〔BE→FE commit 载荷如何泛化〕**（真分歧）
- **B1 opaque bytes（推荐）**：BE commit 载荷序列化为 `byte[]`，经 `ConnectorTransaction`/`finishInsert(Collection<byte[]>)` 交连接器自行反序列化其 thrift。最泛化、零 BE 改动、fe-core 不见 typed、契合现有 SPI。
- **B2 通用 typed envelope**：定义中立 `ConnectorCommitData`（files/rows/partition/deletes）三者映射。结构化但有「最小公约数」丢信息风险（iceberg delete-file/stats、hive S3-MPU、mc block 难统一）。
- **B3 保留 thrift union 经 SPI 路由**：generic 方法收 thrift union，连接器认自己的。保 BE 契约但 thrift 漏进 SPI。

**C.〔maxcompute block-id 分配（唯一写期 BE↔FE op）〕**（真分歧）
- **C1 窄 seam（推荐）**：加一个通用「连接器写期 BE→FE 回调」hook（`FrontendServiceImpl` 据 txn 查连接器 write-callback 委派），**仅 maxcompute 实现**，他者不需。消 instanceof 又不过度泛化。
- **C2 完全泛化**：SPI 加 `allocateWriteRange` 等一等公民方法（过度泛化一个 mc-only 需求）。
- **C3 暂留特例**：block-id 仍 maxcompute 特判（最小改动，但留一处 instanceof）。

**D.〔RFC scope〕**（真分歧，建议）
- **In**：INSERT/DELETE/MERGE 的写/事务 SPI——begin/finish + `ConnectorTransaction` 生命周期 + commit 载荷回调(B) + block-id seam(C) + 写-sink-provider(E) + `PluginDrivenTransactionManager` 桥。以 mc(insert)/hive(insert)/iceberg(insert+delete+merge) 为锚。
- **Defer（不预排除）**：iceberg PROCEDURES（rewrite 等，归 `ConnectorProcedureOps` E2 + P6）；hive 行级 ACID（今未实现）；**各连接器代码搬迁**本身（在 P4/P6/P7 执行期做，本 RFC 只定它们要对的 SPI）。

**E.〔写 sink 构建位置〕**（推荐默认）连接器模块出**写-plan-provider**（仿 `ConnectorScanPlanProvider`）产 `T*TableSink`；BE 不变；`*TableSink.bindDataSink()` 逻辑搬入连接器。← 仿 scan 先例；确认是否反对。

---

## 9. 给设计的取向（我的建议汇总）

A=统一（连接器事务为源）；**B=B1 opaque bytes**；**C=C1 窄 callback seam**；**D=DML 三 op in、procedures/搬迁 defer**；E=写-plan-provider 仿 scan。
→ 净效果：generic 写编排（Coordinator/LoadProcessor/FrontendServiceImpl/BaseExternalTableInsertExecutor）全多态化、零 `instanceof *Transaction`；连接器以 `ConnectorWriteOps`+`ConnectorTransaction`+opaque handle/bytes 接入；BE 契约与各 T*Sink 不变；paimon 后续零-SPI-改动接入。

## 10. 开放问题（写 RFC 前需澄清）
1. B1 下，BE commit 载荷的 bytes 是「原 thrift 序列化」还是连接器自定义？（倾向原 thrift bytes，连接器 TDeserialize）——影响 BE↔FE 契约描述，需在 RFC 钉死。
2. iceberg delete/merge 的 `ConnectorDeleteHandle/MergeHandle` 是否本 RFC 就定义全，还是 insert 先行、delete/merge 留 P6 细化？（倾向 SPI 形状本 RFC 定全，P6 落实现）。
3. 事务跨「多语句」隔离/只读传播是否纳入（今三者皆单语句 per-INSERT，倾向不纳入）。
