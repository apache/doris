# P3 实现设计 + as-built —— 写入共用一个每语句元数据实例 + 事务归属上移

> 本文记录“写入共用”这一步（读写共享同一个 memoized `ConnectorMetadata`，并把写事务上移成语句级共持体）的定稿设计与落地结果。设计先行、经用户确认“完整共持体”高度后实现。全部 `file:method` 来自本轮读码核实 + grounding workflow（`wf_1a053ce4-b22`，6 读者，其中写缝/闸门1/兄弟路由/纠缠点四读者产出，事务生命周期与身份闸门自读核实）。

## 0. 成功判据
- 一条写语句（INSERT/DELETE/MERGE）里，读/扫描/写对同一 `(catalogId, 属主连接器)` 复用**同一个** memoized `ConnectorMetadata`；写事务从该实例上铸出。
- 写入侧 8 处 `getMetadata` 裸调全部走统一收口 `PluginDrivenMetadata.get`，删除 8 个 `getMetadata-funnel-exempt` 标记 → fe-core 元数据收口门禁 100% 无例外。
- 语句末对**未提交**（中途被杀的孤儿）事务确定性回滚，且**先于**关闭元数据；幂等，绝不回滚已提交事务。
- 保留：hive 起写自取表刷新（闸门1）、读写身份一致（闸门2，fail-loud 断言）、tx↔session 绑定、全局事务注册（BE RPC）、执行器在 onComplete/onFail 的提交/回滚时机。
- NONE（离线）下字节等价。

## 1. 第一步 —— 改道 + 身份闸门（commit `f208036f3c5`）
### 1.1 8 处写缝改道（均无状态只读外壳 + 1 处开事务）
`connector.getMetadata(session)` → `PluginDrivenMetadata.get(session, connector)`，删豁免标记：
- `PhysicalPlanTranslator.visitPhysicalConnectorTableSink`（INSERT）/ `buildPluginRowLevelDmlSink`（DELETE/MERGE）
- `PluginDrivenInsertExecutor.ensureConnectorSetup`（唯一“外壳 + 开事务”缝，返回 metadata 存 `writeOps` 字段）
- `PluginDrivenExternalTable.resolveWriteTargetHandle`（藏读文件里的写专用点）
- `BindSink.checkConnectorStaticPartitions` / `checkConnectorWritePartitionNames`
- `IcebergRowLevelDmlTransform.checkPluginMode`
- `PhysicalIcebergMergeSink.buildInsertPartitionFieldsFromConnector`
8 处各建自己的 `buildConnectorSession()`，但捕获同一每语句作用域 → 改道后命中读臂建好的那一个实例。

### 1.2 身份一致断言（闸门2）
`PluginDrivenMetadata.get` 里以 `session.getUser()`（Doris 主体、非令牌）记下“首建者身份”，复用时比对，不一致抛 `IllegalStateException`。一语句一用户恒成立→永不触发；将来若被破坏则 fail-loud 而非悄悄错用凭证。NONE 下存空、检查恒真。

### 1.3 闸门1（hive 起写）天然满足
改道只动 `getMetadata` 工厂调用；hive 起写 `beginWrite` 的自取表在 `beginTransaction` 下游、未触。**纠正旧表述**：起写取表走 `CachingHmsClient` 缓存（与读同对象），吃重的是“写鉴权取 + 原始参数拒 ACID + 提交期唯一把关”，非“更新鲜快照”。闸门1 是“别把起写改成复用共享表”的前瞻约束。

## 2. 第二步 —— 事务归属上移（commit `a03b88b0d80`）
### 2.1 新增 `CatalogStatementTransaction`（fe-core，`datasource/plugin`）
co-hold 共享 `writeOps`（元数据写facet）+ session + 懒建的 `ConnectorTransaction`：
- `begin(writeHandle)`：`connectorTx = writeOps.beginTransaction(session, handle)` 从共享实例铸事务 + `mgr.begin(connectorTx)` 全局注册（BE RPC），返回事务给执行器绑 sink 会话。
- `finalizeAtStatementEnd()`：仅当 `mgr.isActive(txnId)`（孤儿）才 `mgr.rollback(txnId)`；否则空操作。
- 连接器无关：只持 `ConnectorWriteOps/Session/Transaction` 接口，绝不 cast 具体类型。

### 2.2 执行器改道（`PluginDrivenInsertExecutor.beginTransaction`）
经 `scope.computeIfAbsent("txn:"+catalogId, () -> new CatalogStatementTransaction(writeOps, session, mgr))` 取共持体，`connectorTx = stmtTxn.begin(writeHandle)`，`txnId = connectorTx.getTransactionId()`。NONE 下共持体瞬态、执行器自身提交/回滚为唯一生命周期，字节等价。

### 2.3 两趟 closeAll（`ConnectorStatementScopeImpl`）
- 趟1：对所有 `CatalogStatementTransaction` 调 `finalizeAtStatementEnd`（回滚孤儿）。
- 趟2：关闭其余 `AutoCloseable`（memoized 元数据等）。
→ “先了结事务、再关元数据”顺序显式钉死；今天元数据 close 空操作，未来变实事时该顺序必需。

### 2.4 幂等/安全论证
`PluginDrivenTransactionManager.commit/rollback` 均 `transactions.remove(id)` → 管理器的 map 即“已了结”状态源；新增 `isActive(id)=containsKey`。正常/失败路径下执行器在语句末前已提交/回滚→map 已移除→closeAll 兜底空操作，**绝不回滚已提交事务**。只有中途被杀的孤儿会被兜底扫掉。

## 3. iceberg 表 side-car：正交、保持现状
`IcebergStatementScope.sharedTable`（连接器内部每语句表缓存）与本步正交：改道只动 fe-core 的 `getMetadata`，事务上移只动 fe-core 事务生命周期，均不碰起写读表路径。其删除属更远期连接器内部重构（把表变实例字段），本步不做。

## 4. 测试与守门
- 收口身份断言：同用户复用、异用户 fail-loud（2 新）。
- 共持体：begin 注册；finalize 回滚孤儿/绝不撤已提交/回滚后幂等/无事务空操作（`CatalogStatementTransactionTest`）。
- 两趟顺序：txn 先于 metadata 关（`ConnectorStatementScopeTest`）。
- 三写路径套件补 NONE 作用域桩（改道后经收口解引用作用域）。
- 门禁：fe-core 收口门禁 exit 0（写入零裸调）+ 自测 PASS；连接器 import 门禁 exit 0；checkstyle 0。
- 异构网关写/开事务上一步已免费覆盖（兄弟经同一 memoized 收口），e2e 沿用“择机统一补”。
