# FIX-PERF-07（重定范围）设计 —— 每语句「表加载归属者」，读写共享 · 快照一致

> **本设计取代** [`FIX-PERF-07-per-statement-resolution-owner-design.md`](./FIX-PERF-07-per-statement-resolution-owner-design.md)（那版是窄范围：只 memo 写侧 resolveTable + 迁移删除清单、读路径不动、beginWrite 保持新载）。
> **用户 2026-07-18 拍板走「完整统一版」**：要一个贯穿读写的每语句对象，表只加载一次、整条语句复用、快照一致；明确不追求性能、不介意净增 SPI，目标是架构更清晰合理。并拍板**拆掉胖句柄**。
> 结论/取证：6 路侦察 workflow（`scratchpad/u-*.md`）+ Trino 源码逐条核实。相关记忆 `iceberg-table-resolution-cache-scoping`（待本设计落地后更新）。

---

## 0. 目标与非目标

**目标**
1. 一个挂在 `StatementContext` 的每语句作用域，作为整条语句（读 + 写）的表加载归属者。
2. 语句涉及的每张表只加载一次（RAW `Table`），读元数据 / 扫描 / 写成形（排序列、分区）/ `beginWrite` 全部复用同一次加载。
3. 保持既有快照一致性（S_read 钉读扫描 + 写提交 OCC），并让读写共享**同一个已加载表对象**，使全语句所有元数据派生自一次加载。

**非目标 / 明确不做**
- 不追求性能提升（写侧收益≈0；真实价值=架构连贯 + 删单例/删类）。
- 不改快照钉选机制（`StatementContext.snapshots` / `IcebergLatestSnapshotCache` / `applyMvccSnapshotPin` 不动）。
- 不把 sys 表加载、DDL 一次性加载并入登记处（保持新载）。
- 不删跨查询缓存 `IcebergTableCache`（它服务**跨语句**复用，登记处覆盖不了）。

---

## 1. 现状（侦察确认）

**三个各自为政的表解析器 + 写事务各自加载**（file:line 现值）：
- (A) 读元数据 `IcebergConnectorMetadata.resolveTableForRead:612-623`：胖句柄 → 跨查询缓存 → `loadTable`。
- (B) 扫描 `IcebergScanPlanProvider.resolveTable:2202-2224`：同两层（memo 读 2206、`loadRawTable` 2231-2237），出口再 `wrapTableForScan`（Kerberos doAs FileIO）。
- (C) 写 `IcebergWritePlanProvider.resolveTable:689-702`：**不查任何缓存，每次新载**。三个派生调用 `appendExplainInfo:241` / `getWriteSortColumns:257` / `getWritePartitioning:286` 各自触发。
- (D) `IcebergConnectorTransaction.beginWrite:193-222`：又一次 `catalogOps.loadTable:208`（存 `transaction.table`）。

**两层既有缓存**：
- L0 胖句柄 `IcebergTableHandle.resolvedTable`（`transient`，字段 :113，访问器 195/200，`with*` 拷贝 217/232/243 携带）——只覆盖扫描节点那一段（每个 fe-core 入口各建 throwaway 句柄，不跨 lineage 桥接）。
- L1 跨查询 `IcebergTableCache`（挂长生命周期 `IcebergConnector`，键 `TableIdentifier(db,table)`，存 RAW 表）——**对 `session=user` 与 REST vended 关闭**（`IcebergConnector.java:216-220`，凭证隔离）。

**审计「一次查询 3~7 次 loadTable」**由 throwaway-handle + 写侧全不缓存共同造成；写臂最糟（(C)+beginWrite+procedure/DDL 全绕缓存）。

**快照 S_read 机制（保持不变）**：`beginQuerySnapshot:1615-1627` 经 `IcebergLatestSnapshotCache` 收敛成 `(snapshotId, schemaId=最新)`，存 `StatementContext.snapshots:272`；`applyMvccSnapshotPin` 把它钉进读句柄和写句柄（`IcebergTableHandle.getSnapshotId()`）；读扫描 `useSnapshot(S_read)`（`buildScan:1022-1047`）、写 `baseSnapshotId=ctx.getReadSnapshotId()`（`applyBeginGuards:279-283`）→ `validateFromSnapshot(S_read)`。**RAW 表不冻结在 S_read**，pin 是每扫描/每提交再套。

---

## 2. Trino 对照（源码已核）

- 每事务一个 `IcebergMetadata`（`IcebergTransactionManager` 的 `MemoizedMetadata`）；一条 SQL = 一事务。
- 其 `TrinoHiveCatalog.tableMetadataCache`（键 SchemaTableName → TableMetadata）→ 一事务内 `loadTable` 只回一次远端，读规划与写共享同一 `TableMetadata`。
- 快照在 `getTableHandle` 钉进 `IcebergTableHandle.snapshotId`，穿进读 `useSnapshot` 与写 `RowDelta.validateFromSnapshot`。
- `beginInsert` 里 `catalog.loadTable(...)` **命中 tableMetadataCache** → 写事务从读期同一张表构建；提交 OCC 锚定读快照；iceberg 内部 CAS+重试吸收良性并发，`ValidationException` 硬失败、不自动重规划。

**Doris 无「每事务 metadata」**：连接器 session 一条语句被重建 ~26 次，缓存挂它即死。唯一贯穿语句的对象是 `StatementContext` → 连接器**向上够**到它（`ConnectorSession.getStatementScope()`）。这是 Trino 模型可移植的落点。

---

## 3. 设计

### 3.1 每语句作用域（中性 SPI）

- `fe-connector-api` 新增 `ConnectorStatementScope`：
  ```java
  interface ConnectorStatementScope {
      <T> T computeIfAbsent(String key, java.util.function.Supplier<T> loader);
      ConnectorStatementScope NONE = <k,l> -> l.get();   // 不缓存 = 与今天逐次加载字节一致
  }
  ```
- `ConnectorSession` 加默认方法 `default ConnectorStatementScope getStatementScope() { return ConnectorStatementScope.NONE; }`（14 实现零改）。
- `fe-core`：`ConnectorStatementScopeImpl`（`ConcurrentHashMap<String,Object>` 背书）；`StatementContext` 加懒建字段 + `synchronized` 访问器（镜像 `getOrCacheDisableRules:632`；**不在 `close()`/`releasePlannerResources:917-948` 清**，随 GC，镜像 `snapshots`）。
- **捕获方式 = 构造期捕获（非实时读）**：`ConnectorSessionImpl` 在**构造时**从 `ConnectContext.get().getStatementContext()` 取作用域引用（两级 null → `NONE`，镜像 `MvccUtil.getSnapshotFromContext:35-45`）。
  - **为何必须构造期捕获**：扫描流式/分批切分在 off-thread 线程池跑，那里 `ConnectContext.get()`（thread-local）可能为空；`PluginDrivenScanNode` 只在请求线程建**一个** session（字段 :148，`buildConnectorSession` :200）供异步任务复用（:1564）。构造期（请求线程）捕获 → session 带着作用域引用 → 任何线程都够得到。**实时读方案在 off-thread 失效，已否决。**

### 3.2 表登记处（读写归一）

作用域里一张登记处：键 `"iceberg.table:" + catalogId + ":" + db + ":" + tbl + ":" + queryId`，值 **RAW** `Table`。
- **含 queryId**：预编译多次执行各自 queryId → 天然隔离（每次执行看当次的表）。
- **不含 snapshot/ref**：RAW 表快照无关（对齐 L0/L1），pin 在下游每扫描/每提交套。

统一解析器（新增一个连接器私有 helper，姑且叫 `resolveTableShared(session, handle)`）：
```
scope = session.getStatementScope()
raw = scope.computeIfAbsent(tableKey, () ->
          L1 != null ? L1.getOrLoad(id, () -> executeAuth(ops.loadTable(db,tbl)))
                     : executeAuth(ops.loadTable(db,tbl)))
return raw   // 消费者各自再套 wrapTableForScan / openTransaction / 提取 vended token
```
- (A)(B)(C) 与 `beginWrite` 全部改走它。
- 层次：登记处（每语句、所有授权模式开）→ L1（跨语句、仅普通目录）→ 终端 `loadTable`。
- **拆掉 L0**（用户拍板）：删 `IcebergTableHandle.resolvedTable` 字段 + 访问器 + `with*` 三处携带 + (A)(B) 里的 memo 读写。句柄回归纯坐标。`NONE`（离线/无上下文）下 `computeIfAbsent` 每次调 loader = 与今天离线逐次一致。

### 3.3 `beginWrite` 改法（正面结论）

- `beginWrite` **不再自己 `loadTable`**，改成 `table = resolveTableShared(session, handle)`（与读、算排序/算分区同一张）。
- SDK 事务从这张共享表构建；**OCC 仍锚定 S_read**（`baseSnapshotId` 逻辑不变）；提交时 iceberg 自身 CAS + 内部重试对齐最新基底并校验 S_read 之后的冲突（良性并发重试吸收、真冲突硬失败、不自动重规划——与 Trino 同姿态）。**对齐 Trino `beginInsert` 命中读期缓存表来开事务。**
- 事务构造细节（keep 还是 drop `newTransaction()` 的隐式 refresh）为实现期决定项：优先**统一走 `Transactions.newTransaction(name, ops)` 从共享表 ops 直接建（不额外 refresh）**以最大化连贯（与 Trino 一致，提交 CAS 兜基底新鲜度）；以 parity 测试守门，若发现回归再退回保留 refresh。
- 分布式 rewrite 的 `startingSnapshotId`（begin 时当前快照，非 S_read）语义**不变**——共享表的 `currentSnapshot()` 即该锚点，`registerRewriteSourceFiles` 仍在该快照 re-derive。

### 3.4 删除清单下沉

- 行级 DELETE/MERGE 的「可改写删除文件」清单从连接器单例挪进作用域：键 `"iceberg.rewritable-delete-supply:" + catalogId + ":" + queryId`，值 `Map<rawPath, List<TIcebergDeleteFileDesc>>`。
- 扫描侧 `buildRangeForTask`（accumulate 现在 :768，v3 gate :679-680；流式 hard-code false/null 不触碰）改成 `scope.computeIfAbsent(supplyKey, ConcurrentHashMap::new).put(rawPath, descs)`；写侧 `planWrite:191-192` 从同键 map 直接读。
- **整删** `IcebergConnector.rewritableDeleteStash` 字段（:185）+ 两 provider ctor 参数（:680-682,:691-693）+ **整个 `IcebergRewritableDeleteStash.java`（141 行）+ 其测试**（queryId 分桶/TTL sweep 被每语句 GC + 每执行重置收编）。
- **新增响亮失败**：`formatVersion>=3 && (DELETE|UPDATE|MERGE) && getStatementScope()==NONE` → 抛 `DorisConnectorException`（今天这里是静默返回空 → 迁移引入的唯一「静默复活」风险降为响亮失败）。放写侧 `planWrite`（此处已知 writeOperation + formatVersion）。

### 3.5 预编译重置

- `ExecuteCommand.run:89` 复用同一 `StatementContext`。在其已有 per-execution 重置（`setPrepareStage(false)`/`setIsInsert(false)`，:90-91）旁加一行**重置作用域**（`statementContext.resetConnectorStatementScope()`）。
- 键含 queryId 为第二道防线（隔离），重置管内存上界；两者独立。普通语句每条新建 `StatementContext` 天然新作用域，无需依赖重置。

---

## 4. 授权 / 正确性 / parity

### 4.1 授权安全（session=user / vended）——5 条不变式
1. 作用域 session-bound、随语句生死，**永不**升进任何跨 session/表身份键的结构。
2. 存 **RAW** 表；delegated/Kerberos FileIO、vended token 取用时逐消费者重套/重取，绝不冻进作用域。
3. 一语句 = 一用户 = 一凭证；读写用同一 session 解析 ops → 无第二身份可泄漏。
4. fail-closed 保持：ops 在 auth scope 前解析，tokenless `session=user` 抛错，作用域绝不被 fallback 共享加载污染。
5. vended token 语句内新鲜：作用域不跨语句 → 无过期风险。
→ 故登记处对三种授权模式**都能全开**（正是 L1 对凭证目录关闭、此处能开的原因），且凭证目录（L1 关）受益最大。

### 4.2 快照一致性
- 「删哪些行/冲突检测/改写哪些旧删除」今天已全钉 S_read（`baseSnapshotId` + `validateFromSnapshot` + `collectRewrittenDeleteFiles` 读 baseSnapshotId 删除清单）——登记处**不碰**。
- **诚实修正**：写新文件用**最新** schema/spec 是 iceberg 有意语义（非 bug）；对普通 DML，S_read 的 schema == 最新（捕获时取最新 schemaId）。统一的价值是**读写共享同一已加载表对象、全语句派生自一次加载 = 连贯**，不是修 write-shaping bug。
- **schema-dict 不变式**：BE 字段 ID 字典 `pinnedSchema:1064-1073` 与 FE schema 元组 `getTableSchema(...,snapshot):419-440` 必须解析同一 schemaId（否则 schema 演进的时间旅行读 BE `children.at()` SIGABRT）。登记处集中加载后两侧仍读同一 RAW 表的 `schemas().get(schemaId)`，字节不变。

### 4.3 线程
- 读元数据/同步扫描/写全在请求线程；流式 `streamSplits`、分批 `planScanForPartitionBatch` 在 off-thread 池，复用请求线程建的 session → 构造期捕获保证够到作用域。iceberg `ParallelIterable` 只读已解析 `table.io()`，不新 loadTable。

---

## 5. fe-core 净增清单（碰铁律 A，用户签字）

- `fe-connector-api`：`ConnectorStatementScope.java`（接口+NONE）；`ConnectorSession.getStatementScope()` 默认方法。
- `fe-core`：`ConnectorStatementScopeImpl.java`；`StatementContext` 懒建字段+同步访问器+`resetConnectorStatementScope()`；`ConnectorSessionImpl` 构造期捕获+override；`ConnectorSessionBuilder` 传递捕获；`ExecuteCommand` 重置一行。
- 读路径 fe-core 逻辑零改（仅 SPI 默认方法 + session 捕获）。

---

## 6. 实现顺序

1. `fe-connector-api`：`ConnectorStatementScope`（接口+NONE）+ `ConnectorSession.getStatementScope()`。
2. `fe-core`：`ConnectorStatementScopeImpl` + `StatementContext` 字段/访问器/重置 + `ConnectorSessionImpl`/`Builder` 构造期捕获 + `ExecuteCommand` 重置。fe-core 单测。
3. iceberg：`resolveTableShared` helper；(A)(B)(C)+`beginWrite` 接它；**拆 L0**（删胖句柄字段/访问器/携带/memo 读写）；`beginWrite` 取共享表 + 事务从共享表构建。iceberg 单测。
4. iceberg：删除清单下沉（扫描 accumulate + 写 drain 经作用域同键 map）；fail-loud；删 stash 类+单例字段+ctor 参数+测试。iceberg 单测。
5. 度量守门 + 独立 commit（可拆 2~3 个）+ 写 summary + 更新 tasklist/HANDOFF/memory。

---

## 7. 测试守门

- **加载计数**：给终端加载加 `loadCountForTest()`（今 table 路径缺此钩子）；断言一条读+写语句对同一表 3~7→1；`NONE` 对照=逐次。
- **读写共享**：读扫描与写 `beginWrite` 拿到同一 `Table` 实例（identity 断言）。
- **凭证目录**：session=user / vended（L1 关）下仍归一。
- **快照**：`beginWrite` 仍锚定 S_read；schema 演进时间旅行读 FE/BE schemaId 一致。
- **删除清单 parity**：同 catalogId+queryId 扫描/写共享一张 map，v3 DELETE 抽出恰好那些 set；跨 catalog MERGE 隔离；fail-loud 触发。
- **预编译**：重执行不累积（重置生效）+ 跨执行隔离（queryId）。
- fe-core 侧改动另跑 fe-core 单测（iceberg 反应堆不含 fe-core，须两段验）。
- e2e（后续切换阶段补，对齐 `hms-iceberg-delegation-needs-e2e`）：独立+HMS 网关目录 INSERT/DELETE/MERGE/OVERWRITE 无复活 + 分布式 rewrite + 跨 catalog MERGE。

---

## 8. 风险 / 待验

- **拆 L0 触碰刚稳定的读热路径**：靠加载计数 + 共享 identity + off-thread 归一测试守门；off-thread 归一依赖构造期捕获（已核 session 复用）。
- **`beginWrite` 事务构造改动**（drop refresh vs keep）：以 parity 测试守门，倾向 drop refresh 对齐 Trino，回归则退回 keep。
- **行号信 grep**（PERF-06 后可能又漂）。
- **构建/验证坑**照 HANDOFF「构建/验证坑」小节（绝对 -f、`-am`、`install`、`-Dmaven.build.cache.enabled=false`、别 `-q`、别 `nohup &` 套后台）。
