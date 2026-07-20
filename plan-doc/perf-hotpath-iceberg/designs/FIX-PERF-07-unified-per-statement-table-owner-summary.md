# FIX-PERF-07 小结 — 每语句「表加载归属者」，读写共享一次加载

> 覆盖审计发现 **C20**（P1）。用户 2026-07-18 拍板走「完整统一版」（读/写共享同一对象、拆胖句柄、把删除清单暂存下沉），明确**不追性能、接受净增 fe-core SPI**，目标=架构连贯。
> 权威设计：[`FIX-PERF-07-unified-per-statement-table-owner-design.md`](./FIX-PERF-07-unified-per-statement-table-owner-design.md)。

## Problem

一条 DML（尤其 DELETE/MERGE）里同一张 iceberg 表被反复远端 `loadTable`：读元数据、扫描规划、写成形（算排序列/分区/EXPLAIN 各一次）、`beginWrite` 各自加载，最糟一条语句 3~5 次。原有两层缓存只覆盖读扫描一段：胖句柄（`IcebergTableHandle.resolvedTable`，仅同一 handle 实例）+ 跨查询 `IcebergTableCache`（对 `session=user`/REST vended 关闭）；写臂完全绕过。

## Root Cause

连接器 session 一条语句内被重建约 26 次，缓存挂它即死；三个表解析器（读/扫描/写）各自为政、写事务又新载。唯一贯穿整条语句的对象是 fe-core 的 `StatementContext`——连接器需向上够到它。对齐 Trino「每事务一个 metadata + tableMetadataCache」：Doris 无「每事务 metadata」，可移植的落点=把每语句作用域挂 `StatementContext`，经 `ConnectorSession.getStatementScope()` 够到。

## Fix

**中性 SPI（`fe-connector-api`）**
- 新增 `ConnectorStatementScope`：`<T> T computeIfAbsent(String key, Supplier<T>)` + `NONE`（不缓存=逐次加载，离线/无上下文默认）。
- `ConnectorSession` 加默认方法 `getStatementScope()`→`NONE`（其余 20 个实现全测试用、零改，自动继承 NONE）。

**fe-core**
- `ConnectorStatementScopeImpl`（`ConcurrentHashMap` 背书；连接器 loader 不回环，`computeIfAbsent` 单键原子安全）。
- `StatementContext`：懒建字段 + `synchronized` 访问器（镜像 `getOrCacheDisableRules`），**不在 `close()`/`releasePlannerResources` 清**（随 GC，镜像 `snapshots`）；`resetConnectorStatementScope()`。
- `ConnectorSessionImpl`/`Builder`：**构造期捕获**作用域引用（`from(ctx)` 的 ctx 优先，回退 `ConnectContext.get()`，两级 null→NONE，镜像 `MvccUtil.getSnapshotFromContext`）——因扫描流式/分批在 off-thread 池跑、复用请求线程建的**同一** session，实时读 thread-local 会失效。
- `ExecuteCommand`：预编译 EXECUTE 复用同一 `StatementContext`，在既有每执行重置旁加一行 `resetConnectorStatementScope()`（唯一复用路径、每执行恰一次、规划前；作用域 key 含 queryId 为第二道防线）。

**iceberg**
- 新增连接器私有 `IcebergStatementScope`：`sharedTable(session, db, tbl, loader)`（键 `iceberg.table:catalogId:db:tbl:queryId`）+ `rewritableDeleteSupply(session)`（键 `iceberg.rewritable-delete-supply:catalogId:queryId`）；`session==null` 时逐次加载/抛式 map（等价 NONE）。
- 读 `resolveTableForRead`、扫描 `resolveTable`、写 `resolveTable`、`beginWrite` 四处全改走 `sharedTable`；loader 各自保留原授权（读 `catalogOps` / 扫描写 `catalogOpsResolver.apply(session)`，均 `newCatalogBackedOps(session)`，同用户）+ L1 层（扫描/读）。
- **拆 L0**：删 `IcebergTableHandle.resolvedTable` 字段/访问器/三处 `with*` 携带 + 两 seam 读写；句柄回归纯坐标。
- `beginWrite` 取共享表（DELETE/MERGE 走扫描已填的作用域=命中，写-only INSERT 才加载一次）；`openTransaction` **保持** `newTransaction()` 的 refresh（给提交新鲜 OCC 基底；读扫描显式钉快照读不可变数据，共享对象被 refresh 到 latest 无害且顺带消解跨缓存错位）。
- **删除清单下沉**：扫描 `buildRangeForTask` accumulate → 作用域同键 map.put；写 `planWrite` 读同键 map；**整删** `IcebergRewritableDeleteStash`（141 行）+ 单例字段 + 两 provider 四/二个 ctor 的 stash 参数 + 其测试（204 行）。
- **响亮失败**：`formatVersion>=3 && (DELETE|UPDATE|MERGE) && getStatementScope()==NONE` → 抛（迁移唯一新增的「静默复活」风险降为响亮失败；生产恒有 StatementContext，仅离线触发）。

## 关键取舍（诚实）

- **写侧性能收益 ≈ 0**：DELETE/MERGE 写解析命中作用域（扫描已载）、INSERT 仍一次；`beginWrite` 保留 refresh。真交付=**架构连贯 + 删单例/删类**（原则性，非修 bug）。
- **`beginWrite` 保留 refresh（非 drop）**：设计原倾向 drop 以最大连贯，但 `Transactions.newTransaction(name, ops)` 本身会 `ops.refresh()`；保留它更安全（新鲜 OCC 基底 + 消解「共享表比 pin 旧」的跨缓存错位），且共享对象被 refresh 到 latest 对已显式钉快照的读扫描无害。
- **跨缓存快照错位不新增**：pin 由 `IcebergLatestSnapshotCache` 铸、共享表由 `IcebergTableCache`/直载——该错位今天已存在于读扫描路径；写扫描共享读已用过的同一对象，故只在读本就会抛处抛，严格不更差。
- **授权**：读/扫描/写均 `newCatalogBackedOps(session)`，一语句=一用户=一凭证；作用域存 RAW、session 生死、key 含 catalogId+queryId；`session=user`/vended 全开（L1 对其关，作用域可开，凭证目录受益最大）。

## 测试与守门

- **iceberg（968 pass / 0 fail / 1 skip）**：`planningPassLoadsSameTableOnceViaSharedScope`（读+扫描共享作用域→远端 `loadTable` 计数 1）+ 对照 `...WithoutSharedScopeLoadsEachTime`（NONE→2）；扫描 accumulate / 写 drain 经作用域（v3 命中、v2 不填、NONE inert）；写 fail-loud（v3 DELETE + NONE → 抛，消息含 "per-statement scope"）；`IcebergStatementScopeTest`（同键共享 = 读写同实例 / 跨 queryId 隔离 = 预编译重执行 / 跨 catalogId 隔离 = 跨-catalog MERGE / NONE 逐次 / 供给图同语句共享跨 catalog 隔离）。删 `IcebergTableHandleTest` 两个 L0 测试 + `IcebergRewritableDeleteStashTest`。
- **fe-core（两段验，iceberg 反应堆不含 fe-core）**：`ConnectorStatementScopeTest`（NONE 不 memo / impl 每键 memo+隔离 / `StatementContext` 懒建稳定 + reset 使下次全新、旧值不泄漏）3 绿；`ConnectorSessionImplTest` 17 绿（ctor 新增作用域参数无回退）。
- **e2e（后续切换阶段补，对齐 `hms-iceberg-delegation-needs-e2e`）**：独立 + HMS 网关目录 INSERT/DELETE/MERGE/OVERWRITE 无复活 + 分布式 rewrite + 跨 catalog MERGE；session=user/vended 归一。

## 净增 fe-core（碰铁律 A，用户签字）

`fe-connector-api`：`ConnectorStatementScope` + `getStatementScope()` 默认方法。`fe-core`：`ConnectorStatementScopeImpl` + `StatementContext` 字段/访问器/重置 + `ConnectorSessionImpl`/`Builder` 构造期捕获 + `ExecuteCommand` 一行。连接器仍不解析属性、作用域值为 `Object`（fe-core 不知 iceberg 类型，connector-agnostic）。
