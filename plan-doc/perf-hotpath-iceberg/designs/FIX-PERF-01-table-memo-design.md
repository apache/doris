# FIX-PERF-01 — 跨查询 Table 缓存（恢复 legacy 表缓存）+ 属性失效收窄

> 任务：消除"一次 iceberg 查询规划里同一张表被远程 loadTable 3~7 次"的重操作放大。
> 覆盖审计发现 C1 C4 C6 C10 C16（簇1）。
> 基线：HEAD `b342570d6ca`，分支 `catalog-spi-review-16`。行号信 grep 不信本文档。
> 决策已定（用户 2026-07-17）：**跨查询缓存（与出厂 legacy parity）+ 自建缓存复用现有 `MetaCacheEntry` 基建（A2，= legacy `IcebergMetadataCache` 结构）**。

---

## 1. Problem

一条带 `WHERE` 、扫分区表的普通 `SELECT`，在规划期把同一张表远程 `loadTable`（= metastore RPC + 读 metadata.json）**7 次**：

| 次数 | 调用点 | file:line |
|---|---|---|
| ×3 | `getColumnHandles` → `loadTable`（属性第1次 build + 属性第2次 build + getSplits） | `IcebergConnectorMetadata.java:587` |
| ×3 | `resolveTable` → `ops.loadTable`（getScanNodeProperties 第1次 + 第2次 + planScan） | `IcebergScanPlanProvider.java:1300 / 562 / 1988` |
| ×1 | `streamingSplitEstimate` → `resolveTable` | `IcebergScanPlanProvider.java:410` |

**真正的重复来自 dual-build**（红队 2026-07-17 证伪审计的"convertPredicate 触发第二次重算"）：`doFinalize`（`FileQueryScanNode:248-253`）先 `convertPredicate()`(:252) 后 `createScanRangeLocations()`(:253)；属性缓存 `cachedPropertiesResult` 在 `createScanRangeLocations` 内首次 build（`PluginDrivenScanNode.getOrLoadPropertiesResult:1776`）→ **convertPredicate 跑时三字段皆 null，清空是 no-op**。重复实为**两条独立 build 路径各解析一次表**：getSplits 路径（`buildColumnHandles:1202` + planScan 的 resolveTable）与属性路径（`buildColumnHandles:1780` + `getScanNodeProperties:1300` 的 resolveTable），外加 `streamingSplitEstimate:410`、分析期 `getMvccPartitionView:1453`/`listPartitions:1507`、CBO `getTableStatistics:624`。

## 2. Root Cause（含复核对审计草案的修正）

- **根因**：SPI 迁移把 legacy `IcebergMetadataCache`（`LoadingCache<Key, IcebergTableCacheValue> tableCache`，`getIcebergTable()` 返回缓存 Table，快照缓存从缓存 Table 派生 —— 见 #60937 前的该文件 `:57/:102-108/:114`）拆开后，**只移植了"快照 id"那半**（`IcebergLatestSnapshotCache` 只存 `(snapshotId, schemaId)` 两个 long，`:57-64`），**丢了"缓存 Table 对象"那半**。于是各 SPI 入口只能各自 `loadTable`。全仓无 `CachingCatalog`（只 paimon 有）。
- **修正1（缓存键）**：`catalog.loadTable()` **快照无关**（永远返回最新元数据；查询要读的快照 id 在下游 `useSnapshot()` 才应用）。故 memo 键**只需 `TableIdentifier`**，审计草案的 `(id, snapshotId)` 是过度限定；一张表一条语句里的所有时间旅行/分支引用共用一个 Table 对象。也消除了"最早入口 `getColumnHandles` 在快照 pin 之前"的疑虑。
- **修正2（parity 基准）**：legacy 本就**跨查询**缓存 Table（带 TTL、REFRESH 失效）。因此"行为不变（与出厂 legacy parity）"= 跨查询缓存；审计草案的"per-planning-pass"反而比 legacy 多加载、且偏离出厂行为。paimon 亦跨查询（SDK CachingCatalog）。
- **修正3（Part B 证伪）**：审计与我早先复核以为"带 WHERE 时 `convertPredicate` 清空属性缓存 ⇒ 第二次重算"。**红队用控制流证伪**：convertPredicate 先于任何 build 跑（见 §1），清空 no-op，无第二次重算可消。故 **Part B 删除**（详见 §3 Part B）。真正的重复是 dual-build，Part A 覆盖。

## 3. Design

### Part A（定稿）：胖 handle（查询内单实例）+ 跨查询 `IcebergTableCache`（挂 `IcebergConnector`）

**背景（实例数,已核实)**：只有 `IcebergConnector` 及其内 SDK `Catalog` 是"每 catalog 1 个、长期存活"；`IcebergConnectorMetadata`/`IcebergScanPlanProvider`/`IcebergCatalogOps` 都"每次调用 new、一查询多个、用完即弃"。故跨调用共享的状态只能挂 **①长生命周期 `IcebergConnector`** 或 **②贯穿一条查询的 `handle`**。定稿两层各取其一：

**第一层 —— 胖 handle（查询内单实例,一致性由构造保证,随查询自动回收,连接器侧零累积)**
给 `IcebergTableHandle` 加 `private transient Table resolvedTable`（不序列化）+ getter/setter。读表统一走"胖 handle 优先":有则直接返回**同一实例**,无则解析后 `setResolvedTable` 挂上。`withSnapshot/withRewriteFileScope/withTopnLazyMaterialize`（`:182/198/207`）拷贝时**携带前行**（iceberg 分支/标签/时间旅行是**同一个 Table** + 下游 `useSnapshot/useRef`；`getColumnHandles` 在 pin 前 set、`pinMvccSnapshot` 用 `withSnapshot` 换 handle,故必须携带否则 pin 后重解析）。sys 表 handle **不**用（走 `loadSysTable`）。存 **raw** Table；`resolveTable` 命中后 per-call `wrapTableForScan`。

**第二层 —— 跨查询 `IcebergTableCache`（挂 `IcebergConnector`,legacy parity + 连续查询命中,作为胖 handle 首次 miss 的 backing）**
新增 `IcebergTableCache`（值=raw Table），复用 `IcebergLatestSnapshotCache` 同款 `MetaCacheEntry` 基建（TTL=`meta.cache.iceberg.table.ttl-second` 24h，REFRESH 失效）；挂 `IcebergConnector`，像 `latestSnapshotCache` 一样传进每个 fresh metadata **和** provider。**在读 helper 里消费**（非 catalog-seam 装饰器 → DDL 天然隔离）。对 `isUserSessionEnabled() || restVendedCredentialsEnabled()` **关闭**（§5.1）。

**统一读 helper**（`Metadata.loadTable(handle):540` 与 `ScanProvider.resolveTable:1981` 同规则）：
```
Table resolve(handle):
  if handle.resolvedTable != null: return handle.resolvedTable          // 胖 handle → 同一实例
  t = executeAuthenticated( tableCache!=null                            // 跨查询层(gated→null)
        ? tableCache.getOrLoad(id, () -> catalogOps.loadTable(db,tbl))
        : catalogOps.loadTable(db,tbl) )
  handle.setResolvedTable(t); return t   // provider 侧返回 wrapTableForScan(t)
```

**去重计数（诚实）**：规划期所有读共享 `currentHandle` 血缘 → **规划期 1 次**；分析期同理 1 次；分析↔规划不同血缘,靠跨查询层桥接 → 跨查询开 **全查询 1 次远端**,关(session=user/vended) **≤2 次**（仍远小于 7~10）。

**失效**：`IcebergConnector.invalidate{Table,Db,All}`（`:523-553`）各加 `tableCache.invalidate*`（与 `latestSnapshotCache` 并列）；handle 的 resolvedTable 随查询回收,无需失效。
**DDL/写隔离**：DDL/写/procedure 不碰 handle 的 resolvedTable、不经读 helper → 走裸 ops 拿 fresh base。
**鉴权/线程**：读 helper 已在 `executeAuthenticated` 内;命中零远端;miss 时 `delegate.loadTable` 在该 scope 跑 → auth 正确。缓存 raw Table 跨并发共享安全（红队 Attack 4 确认）;Kerberos FileIO 由 `wrapTableForScan` per-call 施加。

### Part B（副线）：~~属性失效收窄~~ —— **已删除（红队证伪）**

原设想：把 `PluginDrivenScanNode.convertPredicate:796-798` 的失效收进 `if (result.isPresent())`。**红队用控制流证伪**：`doFinalize` 先 convertPredicate、后 createScanRangeLocations（首次 build），故 convertPredicate 时 `cachedPropertiesResult/scanNodeProperties/filteredToOriginalIndex` 皆 null → 清空是 **no-op**，**消不掉任何真实重算**；且它是"若未来引入 finalize 前的属性 build 就会读到陈旧缓存"的**潜在隐患**（`filteredToOriginalIndex` 被 `pruneConjunctsFromNodeProperties:1739` 读，`PUSHDOWN_PREDICATES_PROP:1428` 依赖 conjunct）。→ **本任务不做 Part B**；独立 WHERE-conjunct 任务（C8）如仍需碰此点，须先用 `RecordingIcebergCatalogOps` 栈追证明存在"finalize 前的真实第一次 build"再动，否则同样删除。

## 4. Implementation Plan（最小改动，串行）

**Commit 1（Part A，一个 commit，iceberg 自包含）**：
1. `IcebergTableHandle`：加 `transient Table resolvedTable` + getter/setter；`withSnapshot/withRewriteFileScope/withTopnLazyMaterialize`（`:182/198/207`）携带前行。
2. 新增 `IcebergTableCache.java`（跨查询层，仿 `IcebergLatestSnapshotCache`，值 `Table`；`getOrLoad/invalidate/invalidateDb/invalidateAll/isEnabled/size`）。
3. `IcebergConnector`：构造 `tableCache`；`getMetadata`/`getScanPlanProvider` 传入；`invalidate*` 三钩子加跨查询失效；gate=`isUserSessionEnabled() || restVendedCredentialsEnabled()` 关跨查询层（§5.1）。
4. `IcebergConnectorMetadata`：`loadTable(handle):540` 改"胖 handle 优先 → 跨查询缓存 → remote → setResolvedTable"；`getColumnHandles`/`getTableStatistics`/`resolveTimeTravel`/`getMvccPartitionView`/`listPartitions`/`beginQuerySnapshot` 经它。sys 分支不动。
5. `IcebergScanPlanProvider`：生产构造器加 `tableCache` 参；`resolveTable:1981` 改"胖 handle 优先"+ per-call `wrapTableForScan`。

**Part B 不做**（§3 Part B 已删）。

## 5. Risks & Open Questions

1. **凭证时效/隔离（正确性红线，BLOCKER，红队 Attack 2 加强）**：第二层跨查询缓存必须对 **`isUserSessionEnabled() || restVendedCredentialsEnabled()`** 关闭（**两半都要**，二者独立）：
   - session=user（`IcebergConnector:675`）：per-user ops，raw Table 携带 per-user delegated FileIO → 跨用户共享 = 凭证泄漏。
   - plain-REST + `iceberg.rest.vended-credentials-enabled=true`（`IcebergScanPlanProvider:1243`，**不**依赖 session=user）：raw Table 的 FileIO 携带 ~60min 过期的 vended token；代码自注（`:1256`）"credentials 靠每查询重载表保持新鲜"。24h TTL 内命中 → BE 拿**过期 token → 403 中途失败**。
   关闭的只是第二层；**第一层 transient（查询内单实例）不受影响**（同一查询内 token 新鲜）。快照 id 缓存（用户无关）不变。补 UT：plain-REST-vended（无 session=user）断言 Table 不跨查询共享。
2. **DDL 隔离**：Table 缓存只挂 scan 读路径（`getColumnHandles`/`beginQuerySnapshot`/`resolveTable`）；DDL 的 `loadTable().updateSchema().commit()`（`IcebergCatalogOps:397-478`）**不走缓存**，用裸 `catalogOps` → 提交拿 fresh base，无陈旧提交风险。
3. **schema-only ALTER 可见性**：跨查询缓存下，"只改 schema 不产生新快照"的外部 ALTER 要等 TTL/REFRESH 才可见 —— **= legacy 行为**（legacy 快照/schema 亦从缓存 Table 派生），且当前快照缓存已用同样 24h TTL 冻结 → 非新增 staleness。
4. **两套缓存 TTL 不同步**：Table 缓存与 id 缓存独立淘汰，可能 id 命中但 Table miss（或反之）→ 重载 Table 拿 fresh，id 稍旧 —— 仍在"快照 TTL 内稳定"语义内，可接受。
5. **胖 handle 跨相不桥接**：分析期与规划期是不同 handle 血缘,transient 不跨相 → 分析↔规划的一致性靠跨查询层桥接（开时同一对象;关时极罕见两阶段各自 load 出不同代,但快照 pin 已钉、数据一致,仅 schema 极罕见偏移）。可接受(= 任何缓存在该边界的表现)。
6. **内存**：连接器侧**零每查询累积**（Table 引用挂各查询 handle,随查询 GC）;重 Table 对象在跨查询 `IcebergTableCache` 共享,受其 maxSize 兜（legacy 亦缓存 Table）。

## 6. Test Plan / 度量守门

- **度量守门（核心）**：用现成 `RecordingIcebergCatalogOps` 断言 `loadTable` 远端次数：**规划期对同一表恰好 1 次**（胖 handle 血缘内去重,确定性,不随分区/CBO/时间旅行变）；**全查询**跨查询层开=1（分析填、规划命中）、关(session=user/vended)=≤2（分析 1 + 规划 1）。修前基线记录以证从 7~10 大降。
- **Parity UT**：iceberg 连接器现有 UT 全绿（`IcebergConnectorMetadataTest`/`IcebergScanPlanProviderTest`/`IcebergLatestSnapshotCacheTest` 等）。
- **失效 UT**：REFRESH TABLE/DB/CATALOG 后跨查询层清空、下次 load 走 live（仿 `IcebergLatestSnapshotCacheTest`）。
- **凭证隔离 UT**：`isUserSessionEnabled() || restVendedCredentialsEnabled()` 下跨查询层关闭（第二次查询仍走 real load，不复用上次的 Table/凭证）；每查询 pin 仍在（查询内 1 次）。
- **DDL 隔离 UT/e2e**：外部 DDL / REFRESH 后 SELECT 反映变更（TTL/REFRESH 边界 = legacy parity）；DDL 提交拿 fresh base（不经读 helper、不被跨查询缓存毒化）。
- **build**：`-Dmaven.build.cache.enabled=false`（否则 surefire 静默跳过）。

## 7. Parity 论证（为什么"行为不变"）

跨查询层是**恢复** legacy `IcebergMetadataCache` 跨查询表缓存（SPI 迁移丢失的那半），键/TTL/REFRESH 失效与出厂一致；每查询 pin 只把"同一查询内多次远程取同一表"坍缩为一次（同一 Table 对象），快照 id 语义、schema 可见性、时间旅行取快照全部对齐 legacy；唯一收益是"同信息不再重取"。装饰器只拦外部 `loadTable`，DDL/写路径拿 fresh，行为不变。
