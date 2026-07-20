# FIX-PERF-01 — Summary（跨查询 Table 缓存 + 查询内胖 handle）

> 权威设计见 [`FIX-PERF-01-table-memo-design.md`](./FIX-PERF-01-table-memo-design.md)。本文件只记落地结果。
> 基线 HEAD `b342570d6ca`，分支 `catalog-spi-review-16`。

## Problem

一条带 `WHERE` 的普通 iceberg `SELECT`，规划期把同一张表远程 `loadTable`（metastore RPC + 读 `metadata.json`）**3~7 次**：`getColumnHandles`、`getTableStatistics`、scan provider 的 `resolveTable`（getScanNodeProperties/planScan/streamingSplitEstimate）各自独立 load，分析期 `getMvccPartitionView`/`listPartitions` 再各 load 一次。根因是 SPI 迁移拆散 legacy `IcebergExternalMetaCache` 时只保留了"最新快照 id"那半（`IcebergLatestSnapshotCache`），丢了"缓存 Table 对象"那半。

## Fix（一个 commit，iceberg 自包含）

两层，都在连接器侧（守 fe-core 只出不进铁律）：

1. **查询内胖 handle**：`IcebergTableHandle` 加 `transient Table resolvedTable` + getter/setter；`withSnapshot/withRewriteFileScope/withTopnLazyMaterialize` 拷贝携带前行。存 **raw** table，不入 equals/hashCode/toString，不序列化。读表统一"胖 handle 优先→解析后回填"。→ 同一查询同一表**规划期恰好 1 次远端 loadTable**（`PluginDrivenScanNode.currentHandle` 每查询新建、随计划回收，连接器侧零累积）。

2. **跨查询 `IcebergTableCache`**（新类，仿 `IcebergLatestSnapshotCache`，值=raw Table）：挂长生命周期 `IcebergConnector`，24h TTL、REFRESH 失效，作为胖 handle 首次 miss 的 backing，恢复 legacy 跨查询表缓存。**凭证红线 gate**：`isUserSessionEnabled() || restVendedCredentialsEnabled()` 时置 null 关闭（per-user delegated FileIO 跨用户泄漏；vended token 查询内过期，24h 命中会给 BE 过期 token → 403）。胖 handle 层不受 gate 影响（查询内 token 新鲜）。

- 统一读 helper：`IcebergConnectorMetadata.resolveTableForRead`（胖 handle → cache → 裸 `catalogOps.loadTable` → 回填，**不开 auth scope、不包异常**）；`loadTable(handle)` 在 `executeAuthenticated` 内包它；`getMvccPartitionView`/`listPartitions`/`listPartitionNames` 直接在各自 auth scope 内调它，`NoSuchTableException` 原样透传，保住原有"表不存在→空列表"降级。
- provider `resolveTable`：胖 handle 优先→`IcebergTableCache`(raw)→远端，命中/miss 后都 **per-call `wrapTableForScan`**（Kerberos FileIO 不冻进缓存）。
- 失效：`IcebergConnector.invalidate{Table,Db,All}` 各加 `tableCache` 失效（与 `latestSnapshotCache` 并列，null-guard）。
- DDL/写/procedure/sys 表**不经**读 helper（走裸 ops 拿 fresh base）。
- **Part B（convertPredicate 收窄）未做**（红队证伪为 no-op，见 design §3 Part B）。

## Tests（全部新增/加固，`-Dmaven.build.cache.enabled=false`）

- **度量守门**（`IcebergScanPlanProviderTest.planningPassLoadsSameTableOnceViaFatHandle`）：一条规划把 `getColumnHandles`+`planScan` 穿同一 handle → 远端 `loadTable` 恰好 **1** 次（修前 = 2，胖 handle 前每处各 load）。
- **胖 handle 携带 + 非身份 + transient**（`IcebergTableHandleTest`）：memo 随三个 `with*` 拷贝前行、不入 equals/hashCode、序列化后为 null（用不可序列化的 `FakeIcebergTable` 证 transient）。
- **`IcebergTableCacheTest`**（新）：TTL 内命中同实例、`ttl<=0`/负值关缓存、invalidate/invalidateDb/invalidateAll、`NoSuchTableException` 原样透传。
- **凭证 gate + 失效**（`IcebergConnectorCacheTest`）：plain 目录建缓存且 enabled；vended / session=user 目录 tableCache=null；REFRESH TABLE/DB/CATALOG 逐级清空。
- **回归修**：`planCount` 原用共享 `static final T1` handle，胖 handle 使其跨用例串味（首个用例的表污染后续）→ 改为每次 fresh handle，镜像生产每查询新 handle。

## Result

- 全 iceberg 模块单测 **932 通过 / 0 失败 / 1 skip**（`install -am`，checkstyle 单独绿）。
- 行为 parity：现有全部 iceberg 单测不变；胖 handle 是纯去重，快照语义/schema 可见性/时间旅行全对齐 legacy。
- 减负：每查询规划期远端 `loadTable` 从 3~7 → 1（跨查询开时连续查询命中 → 全查询 1；gate 关时 ≤2）。metastore/REST QPS 除以 3~7。
- 端到端时延收益（EXPLAIN 实测）留待后续 docker e2e 量化。
