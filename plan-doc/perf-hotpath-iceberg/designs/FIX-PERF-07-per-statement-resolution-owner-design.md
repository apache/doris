# FIX-PERF-07 设计 — 每语句解析 owner（StatementContext-hosted）+ delete stash 下单例

> 本设计**取代**审计原方案（"语句级 resolve 一次传递 / connector 侧 queryId memo 挂单例"）。
> 经 3 轮对抗 workflow + 用户 2 次拍板收敛，结论落 memory `iceberg-table-resolution-cache-scoping`。

## 用户已定的两个前提（硬约束）

1. **`beginWrite` 保持新载**（fresh + `newTransaction().refresh()`）——它是 OCC 锚点，构造上永不进 memo。
2. **走"乙"**：真正把 delete stash 从 `IcebergConnector` 单例挪走（不是在单例上换个 queryId 记忆），用户已对"净增 fe-core SPI（碰铁律 A）"签字。

## 病灶 & 诚实的价值定位

- 写路径 `resolveTable`（`IcebergWritePlanProvider:689`）裸 loadTable、无 memo，`getWriteSortColumns`/`getWritePartitioning`/`appendExplainInfo` 各自加载；`beginWrite`（`IcebergConnectorTransaction:208`）再新载。
- **但**：`beginWrite` 保持新载后写侧性能收益 ≈ 0（派生在普通目录也命中 tableCache，仅 gated-catalog + EXPLAIN 有薄收益）。
- **真正价值 = 架构连贯 + stash 下单例**（原则性，非修 bug；现 stash 工作正常、TTL 兜漏从不触发）。
- **读热缝不动**：`resolveTableForRead` / 扫描侧 resolve 是 PERF-01~06 刚稳的代码，铁律 3 保护，全高度（重写读缝、删 fat-handle、tableCache 下沉 CachingCatalog）留**远期目标**。

## 设计 — 一个中性的每语句 scope

**对象模型**：新增中性 SPI 类型 `ConnectorStatementScope`（`fe-connector-api`，连接器不碰 fe-core 类型），一个方法 + no-op 单例：
```java
interface ConnectorStatementScope {
    <T> T computeIfAbsent(String key, java.util.function.Supplier<T> loader);
    ConnectorStatementScope NONE = new ConnectorStatementScope() {
        public <T> T computeIfAbsent(String k, Supplier<T> l) { return l.get(); } // 不缓存=与今天逐次加载字节一致
    };
}
```
**物理 home** = `StatementContext` 上一个懒建字段（**不用** `getOrRegisterCache`——其"首个调用者拿到的实例≠后续"的坑对可变容器是正确性 bug）。普通语句随 StatementContext GC（`ConnectContext.clear()` 置空）。⚠ **prepared-statement EXECUTE 复用同一个 StatementContext**（`ExecuteCommand:89` 取 `preparedStmtCtx.getStatementContext()`，prepared INSERT/UPDATE 亦然）——故 scope **必须在每次 EXECUTE 边界显式重置**（否则 N 次执行累积 N 个条目直到 prepared 释放；这正是现 stash 的 TTL sweep 在处理的场景，先前"从不触发"判断有误）。重置钩子挂在 StatementContext 复用点（`setConnectContext`/prepared 重指向处，iceberg-agnostic 的一行 `connectorStatementScope=null`）；确定性重置优于 TTL。key 里的 queryId 是第二道防线（每次执行 queryId 新→旧条目在新 queryId 下不可达，杜绝跨执行脏读）。
```java
private ConnectorStatementScope connectorStatementScope;              // 镜像 getOrCacheDisableRules 的同步懒建
public synchronized ConnectorStatementScope getOrCreateConnectorStatementScope() { ... }
```
**够到方式** = `ConnectorSession.getStatementScope()`（新增默认方法，返回 `NONE`）；`ConnectorSessionImpl` override 返回构建时从 `ConnectContext.get().getStatementContext()` 捕获的引用（两级 null → NONE，镜像 `MvccUtil.getSnapshotFromContext`）。一条语句 ~26 个 session 实例引用**同一个** scope（身份在 StatementContext 上）。

**freshness 契约 = 方法身份**（非可默认错的布尔）：`resolveTableForRead`(读,可命中缓存) / 写 `resolveTable`(只 memo,无 tableCache) / `beginWrite`(构造上 fresh,无 handle/memo 引用)。

## 连接器改动（iceberg）

1. **`IcebergWritePlanProvider.resolveTable`（:689-702）**：现有 body 包进
   `session.getStatementScope().computeIfAbsent("iceberg.write.table:"+catalogId+":"+db+":"+tbl+":"+queryId, () -> <现 body>)`。
   保留 `catalogOpsResolver.apply(session)` + `executeAuthenticated` + `context==null` 分支不变。收敛 3 个派生调用；`beginWrite` **不改**。
   > ops-source 安全：memo 只被写路径填/取，全程同一 per-session ops、同一用户；读路径不经此 scope，无跨 ops-source 复用。
2. **stash 迁移**：
   - 删 `IcebergConnector.rewritableDeleteStash` 字段（:185）+ getScan/getWritePlanProvider 的 ctor 参数（:680-682,:691-693）。
   - **整删 `IcebergRewritableDeleteStash.java`（~140 行）+ 其测试**（queryId 键、Entry、TTL sweep 全被每语句 GC 收编）。
   - 扫描侧（`IcebergScanPlanProvider` ~:679-680,:767）：`formatVersion>=3` 时经 scope 取一张
     `computeIfAbsent("iceberg.rewritable-delete-supply:"+catalogId+":"+queryId, ConcurrentHashMap::new)`，per range `put(rawPath, descs)`（同 raw-path 键、同 empty 守卫）；流式路径 `stashRewritableDeletes=false` 仍不触碰。
   - 写侧（`IcebergWritePlanProvider.planWrite` :191-192）：`session.getStatementScope().computeIfAbsent(同键, ConcurrentHashMap::new)` 直接读，`buildRewritableDeleteFileSets` 不变（空 map→unset thrift 字段，字节一致）。无需 remove（随语句 GC）。
3. **fail-loud 断言**：`formatVersion>=3 && (DELETE||UPDATE||MERGE) && getStatementScope()==NONE` → 抛 `DorisConnectorException`（把唯一新增的"静默复活"降为响亮失败）。

## fe-core 改动（净增，碰铁律 A——用户已签字）

- `fe-connector-api`：`ConnectorStatementScope.java`（接口+NONE）；`ConnectorSession.java` 加一个默认方法。
- `fe-core`：`ConnectorStatementScopeImpl.java`（`ConcurrentHashMap<String,Object>`）；`StatementContext` 加懒建字段+同步访问器（**不在 close()/release 清，随对象 GC**，镜像 snapshots map）；`ConnectorSessionImpl` 加字段+override；`ConnectorSessionBuilder` 两级 null 捕获 + `withStatementScope` 测试钩子。
- **读路径 fe-core 代码零改动**；连接器 SPI 14 个实现零改动（默认方法）。

## 正确性 / parity

- `beginWrite` 不变 → OCC 锚点、分布式 rewrite（writeStarted 只让首 group 加载）字节一致。
- 派生只读 `spec()/sortOrder()/schema()`（快照不变量），memo 存 RAW 表、认证取用时套 → parity。
- **线程**：v3 DELETE/MERGE 的扫描累积 + 写抽取都在 StmtExecutor 请求线程、扫描先于写入 → 同一 StatementContext；分布式 rewrite 各 group 自建 StatementContext（碎片化）但 REWRITE 不用 stash → 无害；读 off-thread split 池不经 scope（走 fat-handle）。
- **跨 catalog MERGE**：key 含 catalogId → 复刻旧 per-catalog 隔离；raw-path 全局唯一无碰撞。

## 测试守门

- 单测：计数 `IcebergCatalogOps` + 真 memoizing 测试 scope，断言 EXPLAIN INSERT 派生 loadTable 2→1；NONE 对照=逐次；stash parity（同 catalogId+queryId 的 scan/write session 共享一张 map，v3 DELETE 抽出恰好那些 set）；跨 catalog 隔离；fail-loud 触发；`beginWrite` 仍新载 + startingSnapshotId=begin 时快照。改 `IcebergScan/WritePlanProviderTest` 去掉 stash ctor 参数；删 `IcebergRewritableDeleteStashTest`。
- e2e（P6.6 cutover 补，对齐 `hms-iceberg-delegation-needs-e2e`）：独立 + HMS 网关目录 INSERT/DELETE/MERGE/OVERWRITE 无复活；分布式 rewrite；跨 catalog MERGE 只抽目标 catalog 供给。

## 待验（落地前）

- **prepared-statement 复用 StatementContext 已确认**（`ExecuteCommand:89`, `StmtExecutor:268-272`）→ scope 须每次 EXECUTE 重置（见"物理 home"）。落地前须确认**重置钩子挂点**：`StatementContext.setConnectContext`/复用重指向处是否只在语句/执行边界调用（grep 其 caller，确保不在语句中途调用而误清活 scope）；若不稳，改为在 `ExecuteCommand` 显式重置 + 普通语句天然新建两条路各自保证。

## 遗留（未做，记录）

- 每条 DML 的 1~2 次 `tableExists`（走通用 `getTableHandle`）不动（读/DDL/元数据共用，改动面大）。
- 读 gated-catalog 冗余（session=user/vended 各站点各加载）不修——需路由读经 owner、动 PERF-01~06 热缝，全高度远期目标。
