# P4-T03 设计 — 连接器写/事务 SPI（`ConnectorTransaction` + `beginTransaction`，gate 关 dormant）

> 批次 B 首 task。事实底座见本文「Recon 事实」；fork 由用户签字（2026-06-06，见 §决策）。
> 关联：[P4 计划 T03](../P4-maxcompute-migration.md)、[写 RFC §5.1/§5.3/§5.4/§6/§7](./connector-write-spi-rfc.md)、[D-015]（id 连接器分配）、[D-022]（写 SPI A/B1/C1）。

---

## Problem

`max_compute` 连接器写 SPI 全缺。T03 把 legacy `MCTransaction`（fe-core `datasource/maxcompute/MCTransaction.java`，262 LOC）的**事务生命周期**港入连接器，impl SPI `ConnectorTransaction` + `ConnectorWriteOps.beginTransaction`，over W4 委派（`PluginDrivenTransactionManager.begin(connectorTx)` 已就位）。**gate 关、dormant**（`max_compute` 未进 `SPI_READY_TYPES`，executor 未接线），零 live 风险。

**T03 ≠ copy legacy**：handoff 标注 T03/T04 未逐行定稿。两处 fork 经 recon + 用户签字定稿（下）。

---

## Recon 事实（code-grounded）

1. **SPI `ConnectorTransaction`**（`fe-connector-api`，不改签名）：`getTransactionId()` / `commit()` / `rollback()` / `close()`(Closeable) + default `addCommitData(byte[])` / `supportsWriteBlockAllocation()` / `allocateWriteBlockRange(String,long)`【**无 checked throws**】/ `getUpdateCnt()`。
2. **W4 桥已就位、未接线**：`PluginDrivenTransactionManager.begin(ConnectorTransaction)` 用 `connectorTx.getTransactionId()` 作 txnId，委派 commit/rollback/addCommitData/allocateWriteBlockRange/getUpdateCnt 给连接器事务（`PluginDrivenTransaction` 内类）。但 `BaseExternalTableInsertExecutor.beginTransaction()` 现仍调无参 no-op `begin()`（`Env.getNextId`）；**无处调 `writeOps.beginTransaction()`**。
3. **BE→FE 回调已泛化（W3/W6）**：`FrontendServiceImpl:3694` 经 `GlobalExternalTransactionInfoMgr.getTxnById(txn_id)` → `txn.supportsWriteBlockAllocation()`/`allocateWriteBlockRange()`（零 instanceof）。⇒ **`getTransactionId()` 必须 = sink stamp 的 Doris 全局 txn_id，且须注册进 `GlobalExternalTransactionInfoMgr`**（注册 + executor 接线 = 翻闸期，见 §dormant 边界）。
4. **JDBC 只是半样板**：impl `ConnectorWriteOps`（no-op insert）**未** impl `ConnectorTransaction`。**MC 是首个有状态事务（block 分配）adopter**，无现成事务样板。
5. **legacy id 分配**：`AbstractExternalTransactionManager.begin()` = fe-core `Env.getNextId()` 分配 + `putTxnById` 注册；`MCTransaction` 本身不持 id。
6. **import-gate 红线**：连接器禁 import `org.apache.doris.(catalog|common|datasource|qe|analysis|nereids|planner)`。⇒ legacy 用的 `common.UserException`、`common.Config` **都禁**。`org.apache.doris.thrift.*`（含 `TMCCommitData`）允许（连接器 scan 侧已用）。
7. **`DorisConnectorException extends RuntimeException`**（unchecked）。

---

## 决策（fork，用户签字 2026-06-06）

### Fork 1 — txn id 机制 = **加 `ConnectorSession.allocateTransactionId()`（尊重 [D-015]）**
- 矛盾：[D-015]「id 由连接器分配」（理由：HMS/Iceberg 有外部 id），但 MC **无外部 id 且够不到 `Env.getNextId()`**。
- 决：给 `ConnectorSession` 加 `default long allocateTransactionId()`（default 抛 `UnsupportedOperationException`），fe-core 唯一 impl `ConnectorSessionImpl` override 回 `Env.getCurrentEnv().getNextId()`。MC `beginTransaction(session)` = `new MaxComputeConnectorTransaction(session.allocateTransactionId(), …)`。**连接器仍是 id 来源（经注入的分配器），符 D-015**；id 即 Doris 全局 id，与 sink txn_id / `GlobalExternalTransactionInfoMgr` 一致。
- **SPI 加面** → 登记 [01-spi-extensions-rfc.md] E-编号（doc-sync 期定）。default 抛保后向兼容（test fake 不强制 impl）。

### Fork 2 — ODPS 写 session 创建 = **挪到 T04 planWrite**
- 写 session builder 需 overwrite/静态分区 context（= OQ-2）；`planWrite` 的 `ConnectorWriteHandle` 正好带 `isOverwrite()`+`getWriteContext()`，T03 的 `beginInsert(session,handle,cols)` 不带。
- 决：**T03 = 纯事务容器**（持 `writeSessionId`/`settings`/`tableIdentifier` 槽 + setter，由 T04 填）。`beginInsert`/`getWriteConfig`/`finishInsert`/`supportsInsert` + 写 session 创建 + `planWrite`(sink) **全归 T04**。T03 自洽、不碰 OQ-2。

### 确认 — dormant 边界
- **不属 T03**（翻闸期 Batch C/接线）：executor 调 `writeOps.beginTransaction()`→`begin(connectorTx)`；`GlobalExternalTransactionInfoMgr` 注册；`SPI_READY_TYPES`。否则会破 JDBC/ES 的 dormant（其 `beginTransaction` 默认抛）。

---

## legacy → T03 SPI 映射

| legacy `MCTransaction` | T03 `MaxComputeConnectorTransaction` | 备注 |
|---|---|---|
| `addCommitData(byte[])`（W2 已加）| `addCommitData`：`TDeserializer(TBinaryProtocol)`→`TMCCommitData`→累积 | **红线**：必 `TBinaryProtocol`（CommitDataSerializer 单点），否则 golden 红 |
| `allocateBlockIdRange` + `allocateWriteBlockRange` override | `allocateWriteBlockRange(reqSid,count)`：校验(>0 / writeSessionId 已设 / 匹配) + CAS `nextBlockId` + 上限 | `throws UserException`→`DorisConnectorException`（unchecked）；上限 `Config.max_compute_write_max_block_count`(20000)→**连接器常量** `MAX_BLOCK_COUNT=20000`（坑6，记 DV）|
| `supportsWriteBlockAllocation()`=true | 同 | |
| `finishInsert()`（restore session + `session.commit(msgs)`）| **`commit()`** 内做（用槽 `writeSessionId`/`tableIdentifier`/`settings` + 累积的 `commitDataList`）| legacy `commit()` 是 no-op、活在 finishInsert；SPI 生命周期由 manager 调 `commit()`（data-flow §6 step7），故折进 `commit()`。槽由 T04 填 |
| `appendCommitMessages`（Base64+ObjectInputStream→`WriterCommitMessage`）| 私有 helper 直港 | 纯 java.io + odps-sdk，无 fe-core |
| `commit()`（no-op）| —（逻辑上移）| |
| `rollback()`（log no-op）| `rollback()` 同（session 自过期）| |
| `getUpdateCnt()`（Σ rowCount）| 同 | |
| —（legacy 无）| `getTransactionId()`→构造注入的 id；`close()`→no-op（无资源，session 自过期）| SPI 新增面 |
| `beginInsert`/`updateMCCommitData`/`getWriteSessionId` | **→ T04** | 写 session 创建归 T04 |

---

## Why（设计理由）
- **commit 折进事务**：SPI manager 只调 `ConnectorTransaction.commit()`（§6 step7）；commit 数据经 B1 `addCommitData` 已累积在事务内（W4 路径 `finishInsert(...,emptyList())`）。故 ODPS `session.commit` 落 `commit()` 最自然，比 legacy 拆 finishInsert 更贴 SPI。
- **槽 + setter（非构造全参）**：`writeSessionId`/`tableIdentifier`/`settings` 是写期（T04 beginInsert/planWrite）才知的态；T03 留 `volatile` 槽 + package/public setter，T04 接线。dormant 期可编译、correct-by-design、不运行。
- **Rule 2**：不建连接器自有 txn 注册表（fe-core `GlobalExternalTransactionInfoMgr` + W4 manager 已覆盖）；不抽象单点 block 上限（常量）。

---

## Deviations / 坑（R12 不静默）
- **DV（新）**：block 上限 legacy `Config.max_compute_write_max_block_count`（fe.conf 可调，默认 20000）→ 连接器常量 `MAX_BLOCK_COUNT=20000L`（import-gate 禁 `common.Config`）。**丢可调性**（Rule 2；如需再经 `MCConnectorProperties` 暴露）。doc-sync 入 deviations-log。
- **异常类型**：legacy `throws UserException`→`DorisConnectorException`（unchecked，SPI 面无 checked throws）。
- **getTxnById guard**（坑4 / 红线3）：W3 已修 `GlobalExternalTransactionInfoMgr.getTxnById` 抛非返 null；T03 不碰该路径（翻闸期接线注意）。

---

## Risk Analysis
- **R-commit-protocol（红线）**：`addCommitData` 必 `TBinaryProtocol`。T10 写 golden 单测（手写替身 round-trip `TSerializer(TBinaryProtocol)`→`addCommitData`→`getUpdateCnt`/commit 数据等价）守。
- **R-dormant**：T03 全 dormant（无 live caller）。风险点是「翻闸期接线遗漏」→ 编入 Batch C 检查单（executor 接线 + 全局注册 + id 一致）。
- **R-T04-coupling**：`commit()` 依赖 T04 填槽；T04 未落前 `commit()` 不可运行——**设计意图**，非 bug。T04 验收含「beginInsert 填 writeSessionId/tableIdentifier/settings 后 commit 通」。

---

## Test Plan
- **T03 gate**（与 T01/T02 一致，非静默跳过）：连接器 compile（`-pl :fe-connector-maxcompute -am`）+ checkstyle 0 + import-gate 0。fe-core 侧 `ConnectorSession`/`ConnectorSessionImpl` 改 → fe-core compile 绿。
- **单测延至 P4-T10**（JUnit5 手写替身，无 mockito）：write-txn golden（TBinaryProtocol round-trip、block-alloc CAS/上限/mismatch、getUpdateCnt Σ）。T03 不加测（与计划一致）。

---

## Ordered TODO
1. **SPI**：`ConnectorSession` 加 `default long allocateTransactionId()`（抛 `UnsupportedOperationException`）。
2. **fe-core**：`ConnectorSessionImpl` override `allocateTransactionId()`→`Env.getCurrentEnv().getNextId()`。
3. **连接器**：新建 `MaxComputeConnectorTransaction implements ConnectorTransaction`：
   - 构造：`long transactionId`（+ 连接器侧 commit 所需依赖入参，最小化）。
   - 字段：`final long transactionId`；`final List<TMCCommitData> commitDataList`；`final AtomicLong nextBlockId`；`static final long MAX_BLOCK_COUNT=20000L`；T04 填槽 `volatile String writeSessionId` / `volatile TableIdentifier tableIdentifier` / `volatile EnvironmentSettings settings` + setter。
   - 方法：`getTransactionId` / `addCommitData`(TBinaryProtocol 红线) / `supportsWriteBlockAllocation`=true / `allocateWriteBlockRange`(DorisConnectorException) / `getUpdateCnt` / `commit`(港 finishInsert + appendCommitMessages) / `rollback`(log) / `close`(no-op)。
4. **连接器**：`MaxComputeConnectorMetadata.beginTransaction(session)`→`new MaxComputeConnectorTransaction(session.allocateTransactionId(), …)`。
5. **写前核实**：javap 核 odps-sdk `TableWriteSessionBuilder.withSessionId/withSettings`、`WriterCommitMessage`、`EnvironmentSettings` 真实 API（认准 commons/table-api jar，坑10）。
6. **gate**：compile（后台）+ checkstyle + import-gate，读真实 BUILD/MVN_EXIT/CS_EXIT。
7. **doc-sync + 独立 commit `[P4-T03]`**（用户定时机）：P4 计划 T03 ⏳→✅、PROGRESS、HANDOFF、decisions（fork）、deviations（block 上限 DV）、E-编号（SPI 加面）。
