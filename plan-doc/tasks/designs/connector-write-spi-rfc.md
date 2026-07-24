# RFC：连接器写/事务 SPI（Connector Write/Transaction SPI）

> 设计文档（design-doc-first）。日期 2026-06-06。Scope = **C（写-SPI RFC 先行）**，P4 启动决策。
> 锚定 3 个现存写者 **maxcompute / hive / iceberg**，前瞻 **paimon**（今读后写）。
> 决策方向（用户签字）：**A** 连接器事务为单一源·桥接；**B1** commit 载荷 opaque bytes；**C1** block-id 窄 callback seam；**D** 覆盖 INSERT/DELETE/MERGE、defer procedures。
> 事实底座：[research/connector-write-spi-recon.md](../../research/connector-write-spi-recon.md)（3 写者深挖 + 现存 SPI + leak 锚点）。
> 本文是设计；**实现待用户批准本 RFC 后**按 §12 TODO 分阶段落地。

---

## 1. Goals

1. 把 fe-core **通用写编排**（`Coordinator` / `LoadProcessor` / `FrontendServiceImpl` / `BaseExternalTableInsertExecutor` / `TransactionManager`）完全**多态化**——消除全部 `instanceof MCTransaction/HMSTransaction/IcebergTransaction` 与 concrete cast（leak 见 §recon-6）。
2. 定义连接器侧**写/事务 SPI**：maxcompute(P4)/iceberg(P6)/hive(P7) 将实现它；**paimon(P5) 零 SPI 改动**即可接入。
3. 覆盖 **INSERT / DELETE / MERGE** DML + 事务生命周期 + **BE→FE commit 载荷回调** + maxcompute **block-id seam** + **写-plan-provider**。
4. **保 BE 契约不变**：各 `T{MaxCompute,Hive,Iceberg}TableSink` 与 BE→FE commit thrift（`TMCCommitData`/`THivePartitionUpdate`/`TIcebergCommitData`）一字不动。
5. 复用 P0 既有面（`ConnectorWriteOps`/`ConnectorTransaction`/`PluginDrivenInsertExecutor`/`PluginDrivenTransactionManager`），**扩展不重造**；新增方法**default-only**（D-009，不破签名）。

## 2. Non-goals

- iceberg **PROCEDURES**（`rewrite_data_files`/`expire_snapshots`）→ 归 `ConnectorProcedureOps`(E2)/**P6**；本 RFC 只保证不预排除（`RewriteDataFileExecutor:61` 不在本 RFC 解）。
- hive **行级 ACID delete/update/merge**：今未实现，越界。
- **各连接器代码搬迁**本身：在 P4/P6/P7 执行期做；本 RFC 只定它们要对的 SPI 靶。
- **BE 侧改动**：零。
- 多语句事务隔离/只读传播：三者皆单语句 per-DML，暂不纳入。

## 3. Constraints / context

- **import-gate**：禁 connector→fe-core；SPI 必须落 `fe-connector-api`/`fe-connector-spi`。
- **classloader 隔离**：fe-core 不能引用连接器类 → 一切耦合走 SPI。
- **两层事务抽象**并存且需桥接：fe-core `Transaction`(commit/rollback，`Coordinator` 持有它) ⟷ SPI `ConnectorTransaction`(getTransactionId/commit/rollback/close，连接器实现)。`PluginDrivenTransactionManager`(P0-T11 已加 `begin(ConnectorTransaction)`) 是桥接点。
- **default-only**（D-009）：所有新增 SPI 方法带 default（no-op/throws/empty），不破现有连接器。

## 4. Architecture overview

```
                 ┌─────────────────────────── fe-core 通用写编排（多态后）────────────────────────────┐
 INSERT/DELETE/  │  BaseExternalTableInsertExecutor → TransactionManager.begin()/commit()/rollback() │
 MERGE 命令       │  Coordinator / LoadProcessor: txn.addCommitData(byte[])   ← B1（替 3 处 cast）       │
                 │  FrontendServiceImpl: txn.allocateWriteBlockRange(...)     ← C1（替 mc instanceof）   │
                 │  PhysicalPlanTranslator: PluginDrivenTableSink             ← E（替各 PhysicalXxxSink） │
                 └──────────────┬───────────────────────────────────────────────────┬─────────────────┘
                   持有 fe-core Transaction（多态）                         经 ConnectorWritePlanProvider 取 TDataSink
                                │                                                     │
        ┌───────────────────────┴────────────┐                      ┌─────────────────┴─────────────────┐
        │ PluginDrivenTransaction（fe-core）   │  wraps & delegates   │ 连接器模块（plugin，classloader 隔离）│
        │  implements fe-core Transaction      │ ───────────────────▶ │  ConnectorWriteOps                  │
        │  → 委派 SPI ConnectorTransaction     │                      │  ConnectorTransaction               │
        └──────────────────────────────────────┘                      │  ConnectorWritePlanProvider         │
                                                                       │  （maxcompute/iceberg/hive impl）    │
                                                                       └─────────────────────────────────────┘
   过渡期（W-phase）：现存 fe-core MCTransaction/HMSTransaction/IcebergTransaction 直接 impl 新增的
   fe-core Transaction.addCommitData/allocateWriteBlockRange（适配到各自 typed update），先让通用层多态、
   暂不搬类、不翻闸；之后各连接器在 P4/P6/P7 把逻辑迁入 plugin、走 PluginDrivenTransaction 桥。
```

三处 seam：**B1** commit 载荷（§5.3）、**C1** block-id（§5.4）、**E** 写 sink（§5.5）。

## 5. SPI surface（APIs）

### 5.1 事务模型（A）—— 桥接，非双轨
- **SPI `ConnectorTransaction`**（既有，不改签名）：`getTransactionId():long`、`commit()`、`rollback()`、`close()`。新增见 5.3/5.4。
- **fe-core `Transaction`**（既有：`commit()`/`rollback()`）：新增通用写回调（5.3/5.4），3 个现存 impl override。
- **`PluginDrivenTransaction`**（fe-core，新）：`implements Transaction`，wrap 一个 `ConnectorTransaction`，把 fe-core 侧 commit/rollback/addCommitData/allocateWriteBlockRange **委派**给 SPI 侧。`PluginDrivenTransactionManager.begin()` 产它。
- **效果**：`Coordinator`/`LoadProcessor`/`FrontendServiceImpl` 只见 fe-core `Transaction` 多态；连接器只实现 `ConnectorTransaction`；桥在中间。

### 5.2 写操作（D）—— INSERT/DELETE/MERGE（既有面，微调）
`ConnectorWriteOps`（既有，JDBC 已实现 insert）：
```java
boolean supportsInsert()/supportsDelete()/supportsMerge();          // default false
ConnectorWriteConfig getWriteConfig(session, tableHandle, columns); // default throws
ConnectorInsertHandle beginInsert(session, tableHandle, columns);   // default throws
void finishInsert(session, ConnectorInsertHandle, Collection<byte[]> commitFragments); // default throws
void abortInsert(session, ConnectorInsertHandle);                   // default no-op
// delete / merge 同形（beginDelete/finishDelete/abortDelete, beginMerge/finishMerge/abortMerge）
```
- `ConnectorInsert/Delete/MergeHandle`（opaque）承载连接器写态（ODPS session / iceberg txn+manifest builder / hive staging path）。
- `finishX(..., Collection<byte[]> commitFragments)`：**承接 B1 累积的 commit 载荷**（见 5.3），连接器反序列化自己的 thrift 落元数据。

### 5.3 Commit 载荷回调（B1 = opaque bytes，核心机制）
**问题**：BE 写完每个 fragment 回连接器专有 typed 载荷（`TMCCommitData`/`THivePartitionUpdate`/`TIcebergCommitData`），现由 `Coordinator`/`LoadProcessor` concrete cast txn 调 `updateXxxCommitData(typed)`。
**B1 设计**：
1. **SPI `ConnectorTransaction` + fe-core `Transaction` 各加**：
   ```java
   default void addCommitData(byte[] commitFragment) { /* no-op */ }
   ```
2. **bytes 内容 = 原 thrift 序列化**（`TSerializer` on 既有 `T*CommitData`/`THivePartitionUpdate`），连接器侧 `TDeserializer` 还原 → 零 BE 改动、保全富信息（iceberg delete-file/stats、hive S3-MPU、mc block 全留）。
3. **fe-core 写结果桥**（**唯一**仍枚举 3 thrift 字段处，一个序列化 shim，非行为）：`Coordinator`/`LoadProcessor` 收 BE 结果时，把当前非空的 `{hivePartitionUpdates|icebergCommitData|mcCommitData}` 之一 `TSerialize`→bytes，调多态 `transaction.addCommitData(bytes)`。**消除 3 处 txn cast**。
4. **过渡期** 3 个 fe-core impl override `addCommitData`：`TDeserialize`→调各自既有 `updateXxxCommitData`。迁入 plugin 后由 `ConnectorTransaction` 实现。
5. **finish**：fe-core 累积的 fragments 传 `finishInsert(..., commitFragments)`（或连接器在 addCommitData 时即累积，finish 触发落库——两种皆可，实现期定，倾向连接器内累积）。
> Open-1（§10）：序列化 shim 何时退休——待 BE 加通用 `connector_commit_data:list<binary>` 字段（未来，非本 RFC）即可消除最后这处枚举。本 RFC **fail-loud 登记**此 transitional shim。

### 5.4 Block-id seam（C1 = 窄 callback）
**问题**：`FrontendServiceImpl:3702` `((MCTransaction)txn).allocateBlockIdRange(sessionId,length)`——maxcompute 唯一写期 BE↔FE RPC。
**C1 设计**：fe-core `Transaction` + SPI `ConnectorTransaction` 加**窄默认方法**：
```java
default boolean supportsWriteBlockAllocation() { return false; }
default long allocateWriteBlockRange(String writeSessionId, long count) {
    throw new UnsupportedOperationException("write block allocation not supported");
}
```
- `FrontendServiceImpl` 改为：`if (txn.supportsWriteBlockAllocation()) return txn.allocateWriteBlockRange(sid, len); else <error/legacy>;`——**零 instanceof**。
- **仅 maxcompute** override（其余连接器默认 false）。`writeSessionId` 为 opaque 连接器自定义串。
- 不上升为方法族（拒 C2 过度泛化）、不留特例（拒 C3）。

### 5.5 写-plan-provider（E）—— 仿 scan
- 新 **`ConnectorWritePlanProvider`**（仿 `ConnectorScanPlanProvider`）：连接器据 bound sink（target table/columns/partition spec/overwrite/writePath）产 **opaque `TDataSink`**（各自 `T*TableSink`）；BE 不变。
  ```java
  interface ConnectorWritePlanProvider {
      ConnectorSinkPlan planWrite(ConnectorSession session, ConnectorWriteHandle handle);
  }
  // ConnectorWriteHandle: 承载 target table handle + columns + partition spec + overwrite + writeContext
  // ConnectorSinkPlan: 包 opaque TDataSink（thrift）
  ```
- fe-core `*TableSink.bindDataSink()` 逻辑搬入连接器；`PhysicalPlanTranslator` 各 `visitPhysicalXxxTableSink` → 统一 `PluginDrivenTableSink`（仿 scan 收口）。
- `Connector` 加 `default getWritePlanProvider()`（回 null→不支持写）。

### 5.6 paimon 前瞻校验
paimon(P5) 写时：impl `ConnectorWriteOps`（insert，FILE_WRITE 形，似 iceberg manifest）+ `ConnectorWritePlanProvider`（产 paimon sink）+ `ConnectorTransaction`（commit 载荷走 B1 opaque bytes）。**无新 SPI**。MVCC 读已用 P0 `beginQuerySnapshot`。→ 设计对 paimon 闭合。

## 6. Data flow（INSERT 时序，多态后）
```
1. InsertIntoTableCommand → BaseExternalTableInsertExecutor.beginTransaction()
     → TransactionManager.begin() → (PluginDriven)Transaction(txnId)  [记 GlobalExternalTransactionInfoMgr]
2. executor.beforeExec() → ConnectorWriteOps.beginInsert(session,tableHandle,cols) → ConnectorInsertHandle
3. PhysicalPlanTranslator → PluginDrivenTableSink ← ConnectorWritePlanProvider.planWrite() 产 TDataSink
4. Coordinator 下发 TDataSink；BE 写
     · maxcompute：BE→FE RPC → FrontendServiceImpl → txn.allocateWriteBlockRange()  [C1]
5. BE 每 fragment 回 commit 载荷 → Coordinator/LoadProcessor: TSerialize→txn.addCommitData(bytes)  [B1]
6. executor.doBeforeCommit() → ConnectorWriteOps.finishInsert(session,handle,fragments) → 连接器落元数据
7. executor.onComplete() → TransactionManager.commit(txnId) → ConnectorTransaction.commit()/rollback()
8. 结果行数：txn.getUpdateCnt()（亦泛化为 default）
```
DELETE/MERGE：2/6 换 beginDelete/finishDelete（iceberg：position-delete/RowDelta），其余同。

## 7. 三写者 → SPI 映射（证明抽象闭合）

| SPI | maxcompute | hive | iceberg | paimon(后) |
|---|---|---|---|---|
| beginInsert→Handle | ODPS write session(writeSessionId) | staging path + ctx | iceberg Transaction + AppendFiles | BatchWriteBuilder |
| addCommitData(bytes) | TDeser `TMCCommitData` | TDeser `THivePartitionUpdate` | TDeser `TIcebergCommitData` | paimon commit msg |
| finishInsert | session.commit(msgs) | action queue + FS rename | Append/Replace/Overwrite.commit | TableCommit.commit |
| allocateWriteBlockRange | ✅ override | default(false) | default(false) | default(false) |
| beginDelete/Merge | unsupported | unsupported | ✅ RowDelta/position-delete | (后续) |
| WritePlanProvider→TDataSink | TMaxComputeTableSink | THiveTableSink | TIcebergTableSink/DeleteSink | paimon sink |
| commit/rollback | session commit/abort | FS+HMS commit / staging cleanup | txn.commitTransaction / discard | commit / abort |
| getWriteConfig type | CUSTOM | FILE_WRITE | FILE_WRITE | FILE_WRITE |

## 8. fe-core 改动（通用层解耦清单）
| 站点 | 现状 | 改为 |
|---|---|---|
| `Coordinator:2531/2536/2539` | 3 处 cast `updateXxxCommitData` | `transaction.addCommitData(TSerialize(present-field))`（B1）|
| `LoadProcessor:232-240` | 3 处 cast | 同上 |
| `FrontendServiceImpl:3697-3702` | `instanceof MCTransaction`+`allocateBlockIdRange` | `supportsWriteBlockAllocation()`+`allocateWriteBlockRange()`（C1）|
| `Transaction`（接口）| commit/rollback | +`addCommitData`/`supportsWriteBlockAllocation`/`allocateWriteBlockRange`/`getUpdateCnt`（default）|
| `MC/HMS/IcebergTransaction` | typed updates | override 新 default（过渡适配）|
| `PluginDrivenTransaction`（新）| — | wrap `ConnectorTransaction`，委派 |
| `PhysicalPlanTranslator` sink 分支 | 各 PhysicalXxxTableSink | `PluginDrivenTableSink` ← `ConnectorWritePlanProvider`（E）|
| `RewriteDataFileExecutor:61` | iceberg cast | **不动**（procedure，P6）|

## 9. Edge cases
- **rollback/abort**：hive 清 staging + abort S3-MPU；mc abort/expire session；iceberg 丢弃未提交 manifest。经 `ConnectorTransaction.rollback()` + `abortInsert`。
- **0 行 insert**：commit 空 fragments；连接器 finish 应幂等空提交。
- **overwrite**（动/静态分区）：经 `ConnectorWriteHandle.writeContext`(overwrite flag + static partition spec) 透传。
- **partial failure**（部分 BE 成功）：txn 整体 rollback（现语义不变）。
- **getUpdateCnt 聚合**：连接器累加（mc 跨 block、hive 跨 partition、iceberg 跨 file）。
- **txnId 生命周期**：`GlobalExternalTransactionInfoMgr` put/get/remove 不变；`PluginDrivenTransaction` 注册同路。
- **B1 序列化失败**：fail-loud 抛（不静默丢 commit 数据）。

## 10. Open questions
1. **B1 shim 退休**：BE 加通用 `connector_commit_data` 字段后消除最后枚举——本 RFC 登记，不实现。
2. **delete/merge handle 完备度**：本 RFC **定全 SPI 形状**（含 delete/merge），**实现**留 P6 iceberg；P4 mc/P7 hive 仅 insert。
3. **commit 数据累积位置**：fe-core 累积传 finish vs 连接器内累积——倾向连接器内（少一次大集合传递），实现期定。

## 11. Risks / alternatives
- **B2/B3 否决**：B2 中立 envelope 丢富信息（iceberg delete-file/hive S3-MPU 难统一）；B3 thrift 漏进 SPI。→ B1 最泛化、零 BE 改、保信息。
- **C2/C3 否决**：C2 为 mc-only 需求过度泛化；C3 留 instanceof。→ C1 窄 seam。
- **R-002（hive ACID compaction 一致性）**：本 RFC **不恶化**（不引入 ACID 写）；登记，归 P7。
- **R-003（iceberg procedures 抽象）**：defer E2/P6；本 RFC SPI **不预排除**（`getWritePlanProvider`/事务桥可复用）。
- **R-001（image 兼容）**：写 SPI 不动持久化 logType/GSON（那是各连接器迁移期的 gate 工作）。
- **大改面风险**：W-phase 解耦**不翻闸、不搬类、零行为变更**（3 impl 适配既有逻辑），风险可控；真正搬迁逐连接器（P4/P6/P7）分摊。

## 12. Ordered TODO（实现路线，待批准）

> 本 RFC 是设计。批准后按下序落地。**W-phase = 本 scope=C 的共享产出**（解耦 + SPI 面，gate 不动）；之后各连接器在其阶段做 adopter。

**W-phase（共享，本 RFC 直接后续；低风险、不翻闸、零行为变更）**
- [ ] W1 SPI 面：`ConnectorTransaction` 加 `addCommitData`/`supportsWriteBlockAllocation`/`allocateWriteBlockRange`/`getUpdateCnt`（default）；`Connector.getWritePlanProvider` default null；`ConnectorWritePlanProvider`/`ConnectorWriteHandle`/`ConnectorSinkPlan` 新类（api/spi）。import-gate + checkstyle。
- [ ] W2 fe-core `Transaction` 接口加同名 default；`MC/HMS/IcebergTransaction` override（TDeser→既有 typed update；mc override block 分配）。**golden 等价**：行为与现状逐位一致。
- [ ] W3 解耦 `Coordinator`/`LoadProcessor`（→`addCommitData(TSerialize)`）+ `FrontendServiceImpl`（→`supportsWriteBlockAllocation`/`allocateWriteBlockRange`）。删除 6+1 处 cast/instanceof。
- [ ] W4 `PluginDrivenTransaction`（fe-core）wrap `ConnectorTransaction`；`PluginDrivenTransactionManager` 产它。
- [ ] W5 `PluginDrivenTableSink`（fe-core）+ `PhysicalPlanTranslator` 写 sink 收口（仿 scan，保留各 PhysicalXxxSink 作迁移期 fallback）。
- [ ] W6 测试：`FakeConnector` 写默认行为；W2 适配的 golden 等价测（3 txn 的 addCommitData 反序列化 == 原 typed 路径）；checkstyle 含 test 源。
- [ ] W7 文档：本 RFC 决策入 `decisions-log`（D-021 scope=C + D-022 A/B1/C1/D/E）；`01-spi-extensions-rfc.md` 加「E11 写/事务 SPI」节（脚注引 D-022，§5.2 纪律）；PROGRESS/HANDOFF 同步。

**P4 maxcompute（首个 adopter，full 迁移 + 翻闸）**——本 RFC 批准 + W-phase 落地后启
- [ ] 搬 `MCTransaction`/`MaxComputeMetadataOps`/MetaCache/SchemaCacheValue/ScanNode → `fe-connector-maxcompute`；impl `ConnectorWriteOps`(insert)+`ConnectorTransaction`(over `addCommitData`/`allocateWriteBlockRange`)+`ConnectorWritePlanProvider`(产 `TMaxComputeTableSink`)。
- [ ] McStructureHelper 去重（删 fe-core 副本，DV/P1-T02）。
- [ ] 翻闸 `SPI_READY_TYPES+="max_compute"`、删 `CatalogFactory` case、GSON 兼容、`getEngine` 分支（recon 已 pin，见 p4-maxcompute-migration-recon §5）。
- [ ] 删 `datasource/maxcompute/`；清 ~36 反向引用（21 mechanical 折 SPI 分支，15 live 由本 SPI 接管）。
- [ ] 连接器测试基线（仿 hudi 5 文件，JUnit5 手写替身）。

**P6 iceberg / P7 hive（后续 adopter）**：复用 W-phase SPI，各自 impl `ConnectorWriteOps`(iceberg +delete/merge)+`ConnectorWritePlanProvider`；iceberg procedures 经 E2 另议。

**完成判据**：W-phase 后 fe-core 通用写层零 `instanceof *Transaction`；3 现存写者经 SPI 多态、行为 golden 等价；BE 契约不变；P4 maxcompute 可独立翻闸；paimon 后续零-SPI 接入。
