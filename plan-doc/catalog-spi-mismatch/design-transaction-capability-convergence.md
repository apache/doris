# 设计：事务接口收敛（下沉源专属方法到窄能力接口）

> 对应 TASKLIST 第 8 条（B4，唯一真正碰 fe-core 铁律的一项）。本文件是**动手前的设计方案**，待用户签字后再改代码。

## 1. 问题（已实证）

两个"通用事务"接口把只对**单一源**有意义的方法作为 default 方法挂在通用契约上，让每个连接器/事务实现都"白白"带着这些无关方法：

- **connector-api `ConnectorTransaction`**（通用连接器事务，157 行）挂了两对源专属方法（default 全抛异常）：
  - 写块分配（**仅 MaxCompute**）：`allocateWriteBlockRange`（+ 能力位 `supportsWriteBlockAllocation` 默认 false）。
  - 压缩重写（**仅 Iceberg**）：`registerRewriteSourceFiles` + `getRewriteAddedDataFilesCount`。
  - 通用方法（保留）：`getTransactionId`/`commit`/`rollback`/`close`/`addCommitData`/`getUpdateCnt`/`applyWriteConstraint`/`profileLabel`。
  - 实现矩阵：MaxCompute override 写块对；Iceberg override 重写对；hive / jdbc(no-op) / paimon / hudi 只用通用面、**继承抛异常的死默认**。
- **fe-core `Transaction`**（通用引擎事务，68 行）**还带了写块这对**（`supportsWriteBlockAllocation` + `allocateWriteBlockRange`）。

**严重度校准（诚实）**：这是**潜伏的接口异味，不是活跃 bug**。fe-core `Transaction` 的**唯一**实现 `PluginDrivenTransaction`（包装器）已 override 并转发，无"白继承抛异常"的真实实现（历史结论里的 `JdbcTransaction` 类**不存在**，jdbc 是 BE 自动提交的 no-op）。连接器侧 jdbc/hive/paimon 虽继承抛异常默认，但路由保证只有 MaxCompute/Iceberg 各自走到对应路径，今天不会真的抛。收敛的价值是：**接口整洁（连接器不再背无关的源专属死 API）+ 类型安全（instanceof 类型门替代运行期抛）+ 对齐 Trino**。

## 2. Trino 参照

- Trino **没有** god-transaction：事务句柄是**不透明 marker**（无业务方法）；写生命周期在 `ConnectorMetadata`（`beginInsert/finishInsert`…），能力经**注册集**（`getTableProcedures`/`getCapabilities`）由引擎在**分发前**检查。
- **OPTIMIZE（压缩）** = 只有部分连接器注册的 **table-procedure**（`getTableHandleForExecute/beginTableExecute/finishTableExecute`），引擎按注册决定是否分发 —— 和 Doris 的重写对**精确对应**。
- **写会话状态**属于连接器自己的**不透明写句柄**，不该挂在共享事务接口上。
- 结论：**引擎在调用前做类型/注册检查** > **god-interface 的 default-throw**（能力在规划期可知、连接器不背死 API）。Trino 只对**根写生命周期入口**保留 default-throw（如 `beginInsert`），能力性额外项一律走窄类型/注册。

## 3. 方案：下沉两个窄能力接口 + 消费侧 instanceof

### Part A — 重写对（Iceberg），**纯 connector-api，低风险，不碰 fe-core**

1. 新增 connector-api 窄接口：
   ```java
   public interface RewriteCapableTransaction {
       void registerRewriteSourceFiles(Set<String> dataFilePaths);
       int getRewriteAddedDataFilesCount();
   }
   ```
2. 从 `ConnectorTransaction` 删掉这两个 default 方法。
3. `IcebergConnectorTransaction implements ConnectorTransaction, RewriteCapableTransaction`（它本就 override 这两个，只加接口声明）。
4. 消费侧 `ConnectorRewriteDriver`（fe-core，但持有的是 connector-api `ConnectorTransaction`）：在 STEP 3/STEP 5 先 `instanceof RewriteCapableTransaction` 再调；否则 fail-loud（该 driver 只被 `rewrite_data_files` 过程路由到，非重写连接器到这里即真错）。

### Part B — 写块对（MaxCompute），**碰 fe-core 事务契约，须 review**

1. 新增 connector-api 窄接口：
   ```java
   public interface WriteBlockAllocatingTransaction {
       long allocateWriteBlockRange(String writeSessionId, long count);
   }
   ```
2. 从 `ConnectorTransaction` 删 `allocateWriteBlockRange` + `supportsWriteBlockAllocation`。
3. `MaxComputeConnectorTransaction implements ConnectorTransaction, WriteBlockAllocatingTransaction`（本就 override，去掉 `supportsWriteBlockAllocation` override —— instanceof 即门）。
4. 新增 fe-core 窄接口 `WriteBlockAllocatingTransaction extends Transaction`（带 `allocateWriteBlockRange(...) throws UserException`）。
5. 从 fe-core `Transaction` 删 `allocateWriteBlockRange` + `supportsWriteBlockAllocation`（**fe-core 契约收窄=只减**）。
6. 包装器 `PluginDrivenTransaction`：`begin(connectorTx)` 时若 `connectorTx instanceof WriteBlockAllocatingTransaction`(connector-api)，构造一个 **子类** `WriteBlockAllocatingPluginDrivenTransaction`（extends 包装器、implements fe-core 窄接口、把 `allocateWriteBlockRange` 委派到连接器窄接口）；否则用原包装器。这样 instanceof 才是**真门**。
7. 消费侧 `FrontendServiceImpl.getMaxComputeBlockIdRange`（RPC 端点本就叫 MaxCompute）：`if (!(transaction instanceof WriteBlockAllocatingTransaction)) throw "not a MaxCompute transaction"; else ((WriteBlockAllocatingTransaction) transaction).allocateWriteBlockRange(...)`。

## 4. 铁律评估

- **Part A**：纯 connector-api（新增窄接口 + 移两方法）；fe-core 消费侧只加一个 `instanceof`（对 connector-api 类型做类型检查，非把连接器逻辑塞进 fe-core）。**干净**。
- **Part B**：fe-core `Transaction` **收窄（删两方法）**；新增一个 fe-core 窄接口（2 行）+ 一个委派用小子类。写块**概念本就在 fe-core**（FrontendServiceImpl 有 MaxCompute 命名的 RPC 端点、GlobalExternalTransactionInfoMgr 按 fe-core `Transaction` 存取），此处是**把已存在的 fe-core 泄漏重构成更窄的类型**，非新增概念/非"为删 A 把逻辑挪进 fe-core"。TASKLIST 明确"收敛必须在 fe-core 事务层做"，Trino 亦背书窄类型。**符合铁律精神（净收窄），但因碰事务契约须 clean-room review**。

## 5. 不动的东西

- txn-id 粒度（正确，勿动）。
- 通用方法（commit/rollback/close/addCommitData/getUpdateCnt/applyWriteConstraint/profileLabel）。
- 各连接器 commit/rollback 实现。

## 6. 验证

- 编译 + checkstyle（connector-api / iceberg / maxcompute / fe-core）。
- 现有事务单测（IcebergConnectorTransactionTest 有重写测试；MaxCompute 写块）须仍绿；补一条"非重写连接器事务 instanceof 失败即清晰报错"、"非 MaxCompute 事务 RPC 报错"的守卫测试。
- 无 e2e 行为变化（纯类型重构，运行期路径不变）。

## 7. 风险

- 碰事务契约（Part B），须 review。
- 潜伏无复现（今天不抛），故属整洁化/防御，非修 bug —— 若认为收益不抵改动面，可只做 Part A（低风险）或整体 backlog。
