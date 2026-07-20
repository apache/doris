# 外表分区"派生视图"二级缓存设计（连接器视图缓存 + 接入 NereidsSortedPartitionsCacheManager）

- 日期：2026-07-20
- 分支：`branch-catalog-spi`
- 状态：设计已评审通过，待实现计划
- 关联：CACHE-P1（翻闸弃二级缓存决策）、#65659（分区裁剪 TOCTOU freeze）

## 1. 问题背景

catalog-SPI 翻闸（CACHE-P1 决策）把 legacy fe-core 里引擎特定的"分区派生视图"二级缓存整层删除，改为**每查询直连列举分区**。代价（已登记为 CACHE-P1 偏差、known-degradation）分两块，按引擎轻重不同：

1. **远程枚举成本**：iceberg 分区表每查询一次 `PARTITIONS` 元数据表扫描（loadTable + 遍历 manifest-list + 全 manifest + avro 解码）；maxcompute 每查询一次 ODPS `getPartitions()`；hive 每查询把 HMS 名单解析成 `ConnectorPartitionInfo`（原始名单已被 `CachingHmsClient` 缓存，此处是解析 CPU）。
2. **派生视图重建成本**：fe-core 每查询把分区名单构建成 `nameToPartitionItem`（`PartitionItem`），并在 `PruneFileScanPartition` 里用 `SortedPartitionRanges.build()` 重建二分查找结构（O(n) 建 range + O(n log n) 排序）。legacy 里这份 `SortedPartitionRanges` 是缓存复用的（`HiveExternalMetaCache.HivePartitionValues` 构造函数里建一次、跨查询复用），翻闸后变成每查询重建。

对**大量分区**表，这两块都显著。本设计恢复这两层缓存，同时满足 SPI 隔离、且不复发 #65659 的 TOCTOU。

## 2. 目标 / 非目标

**目标**
- 恢复跨查询的分区派生视图缓存，消除上述两块重复成本。
- 严格满足 SPI 隔离：fe-core 保持连接器无关；连接器侧缓存建在 SPI 线以下。
- 不复发 #65659 的分区视图查询内分叉（TOCTOU）。
- 通用框架，全 SPI 引擎（`SPI_READY_TYPES = jdbc, es, trino-connector, max_compute, paimon, iceberg, hms`）可受益；按收益接线（iceberg/paimon/hive/maxcompute 先接，jdbc/es 分区退化可暂 no-op）。

**非目标**
- 不引入全局 as-of 快照语义；跨查询采用 bounded-staleness 契约（TTL + 失效钩子）。
- 不改变 `PruneFileScanPartition` 的二分裁剪算法（`PartitionPruner.binarySearchFiltering` 保持原样）。
- 不为带谓词的 `listPartitions`（当前不存在此调用）设计缓存分桶。
- **不考虑 `use_meta_cache=false`**：新 SPI 框架下 `use_meta_cache` **恒为 true**（连接器始终经元数据缓存），无直连绕过路径需设计。SHOW PARTITIONS / partitions-TVF 的"取新"仍由既有 per-call fresh 路径（`listPartitionNamesFresh` 等）承担，与本缓存正交、绕过缓存 A。

## 3. 现状梳理（设计所依赖的事实）

- **每语句冻结层已存在（相当于 Trino 的 per-transaction memoizer）**：hive/iceberg/paimon 都是 `PluginDrivenMvccExternalTable`；分区视图在 `materializeLatest()`（`PluginDrivenMvccExternalTable.java:127`）**每语句只物化一次**，冻结进 `PluginDrivenMvccSnapshot`（存于 `StatementContext.snapshots`，per-statement 实例字段）。`PruneFileScanPartition` 读的是冻结的 `SelectedPartitions`，绝不回连缓存——这是 #65659 TOCTOU 不复发的结构性原因。
- **裁剪路径 `listPartitions` 永远不带 filter**：全部 4 个调用点（`PluginDrivenExternalTable.java:831/:887`、`PluginDrivenMvccExternalTable.java:271`、`ShowPartitionsCommand.java:300`）传 `Optional.empty()`。
- **二分裁剪机制在分支中完好且已在用**：`PartitionPruner.pruneWithResult`（`PartitionPruner.java:163`）在 `sortedPartitionRanges.isPresent()` 时走 `binarySearchFiltering`（`:204-209, :255`），由 session 变量 `enableBinarySearchFilteringPartitions` 控制；#65659 合并后 `PruneFileScanPartition` 每查询用 `.or(() -> SortedPartitionRanges.build(nameToPartitionItem))`（`PruneFileScanPartition.java:101-102`）现建 ranges 后照常二分——功能与 legacy 一致，唯缺缓存复用。
- **fe-core 已有引擎无关、按版本 keyed 的 ranges 缓存**：`NereidsSortedPartitionsCacheManager`（`common/cache/NereidsSortedPartitionsCacheManager.java`），key=`TableIdentifier(catalog,db,table)`，`PartitionCacheContext(tableId, partitionMetaVersion, sortedPartitionRanges)`，`get()`（`:74`）用 `getPartitionMetaVersion(scan)` 校验版本、变更即重建（`:98-102`），`loadCache()`（`:117`）带"插入过频跳过排序"启发式（`:127-131`），软值 + FE 配置 TTL。**OLAP 表已通过 `PruneOlapScanPartition.java:161-168` 在用**。其接口 `SupportBinarySearchFilteringPartitions`（`catalog/SupportBinarySearchFilteringPartitions.java`）javadoc 明言"you can save the partition's meta snapshot id in the CatalogRelation and get the partitions by the snapshot id"——为外表 MVCC 快照场景预留。
- **外表当前完全绕过它**：`ExternalTable.getSortedPartitionRanges(scan)`（`ExternalTable.java:527`）返回 `Optional.empty()` 空桩，且 #65659 合并后**已无任何调用者**（死代码）。
- **已有连接器侧缓存基建**：`fe-connector-cache` 的 `CacheSpec`（属性模型 `meta.cache.<engine>.<entry>.(enable|ttl-second|capacity)`）+ `MetaCacheEntry`（Caffeine，contextual-only + manual-miss，generation 计数失效）。iceberg 已有 `IcebergPartitionCache`（key=`(TableIdentifier, snapshotId)` → `List<IcebergRawPartition>` **原始行**）、`IcebergLatestSnapshotCache`（`(snapshotId, schemaId)` 快照钉子）；hive 有 `CachingHmsClient`（缓存 `listPartitionNames`/`getPartitions` 原始 HMS 元数据）。**但派生视图（`ConnectorMvccPartitionView` / `List<ConnectorPartitionInfo>`）每次重算**。
- **连接器失效缝**：`Connector.invalidateTable/invalidateDb/invalidateAll/invalidatePartition`（`fe-connector-api/.../Connector.java:312/316/324/336`，默认 no-op，引擎 override）。REFRESH TABLE（`RefreshManager.refreshTableInternal` → `connector.invalidateTable`）/CATALOG（`PluginDrivenExternalCatalog.onRefreshCache` → `connector.invalidateAll`）/DB + HMS 事件（ADD/DROP/REFRESH_PARTITIONS → `connector.invalidatePartition`）已全部 fan-out 到这 4 个钩子。
- **session=user 凭证隔离**：iceberg 连接器缓存在 `iceberg.rest.session=user` 下被置 `null` 禁用（`check-authz-cache-sharding.sh` 标记守护）。

## 4. 方案总览：两层互补缓存

```
Nereids 裁剪 (PruneFileScanPartition.pruneExternalPartitions)
   │  nameToPartitionItem = scan.getSelectedPartitions().selectedPartitions   // 冻结 (T1)
   │  ranges = scan.getSelectedPartitions().sortedPartitionRanges             // 冻结 (#65659), 分支恒空
   │            .or(() -> externalTable.getSortedPartitionRanges(scan))       // ← 复活死桩 → 缓存 B
   │            .or(() -> Optional.ofNullable(SortedPartitionRanges.build(nameToPartitionItem)))  // 最后兜底
   │  → PartitionPruner.pruneWithResult(..., ranges) → binarySearchFiltering  // 机制不变
   ▼
[缓存 B · fe-core 已存在] NereidsSortedPartitionsCacheManager
     key      = (catalog, db, table)
     validity = getPartitionMetaVersion(scan)           // = pinned metaVersion, 见 §6
     value    = 预建 SortedPartitionRanges
     origin   = getOriginPartitions(scan) → pinned 快照的冻结 nameToPartitionItem
     ── 消除: 每查询重建 SortedPartitionRanges (大量分区 CPU)
   ▲
[Tier-B 冻结 · 已存在] PluginDrivenMvccSnapshot (每语句物化一次, materializeLatest)
   ▲   ── 唯一读下层缓存处
   │
[缓存 A · 新增, SPI 线以下] ConnectorPartitionViewCache (各连接器 final 字段)
     key   = (db, table, snapshotId, schemaId)
     value = List<ConnectorPartitionInfo> / ConnectorMvccPartitionView
     ── 消除: 远程枚举 (iceberg PARTITIONS 扫描 / ODPS getPartitions / hive 名单解析)
   ▲
[remote] iceberg catalog / hive HMS / odps
```

对照 Trino：缓存 A = Trino 的"共享 TTL 层"（`SharedHiveMetastoreCache`，跨查询、可陈旧、SPI 线以下）；Tier-B 冻结层 = Trino 的"per-transaction memoizer"（首次触碰即冻结、查询内一致）。区别是 Doris 的 per-statement 冻结层已存在，故只需补共享层 A + 复用已有 ranges 缓存 B。

## 5. 缓存 A：`ConnectorPartitionViewCache`（SPI 线以下）

**放置**：`fe/fe-connector/fe-connector-cache/.../cache/ConnectorPartitionViewCache.java`（共享工具，非 SPI 接口；与 `CachingHmsClient`/`IcebergPartitionCache` 同一构建方式）。各连接器持有一个 final 字段实例。

**形态**：内部一个 `MetaCacheEntry<PartitionViewCacheKey, V>`，沿用 contextual-only + manual-miss 并发模型。
- key `PartitionViewCacheKey`：`(db, table, snapshotId, schemaId)`。**不含 filter**（裁剪路径恒空；带 filter 的 `listPartitions` 由 loader 一行 `if (filter.isPresent()) return uncachedLoad();` 绕过）。
- value：`List<ConnectorPartitionInfo>`（iceberg 另可缓存派生的 `ConnectorMvccPartitionView`）。
- 配置：`CacheSpec` 键 `meta.cache.<engine>.partition_view.(enable|ttl-second|capacity)`，默认 TTL 86400s、ON、capacity 与同引擎其它 entry 一致。

**接入点**（把原本昂贵的枚举+派生 body 包进 `cache.get(key, () -> body)`）：
- iceberg：`IcebergConnectorMetadata.getMvccPartitionView` / `listPartitions`，叠在现有 `IcebergPartitionCache`（raw 行）之上，缓存派生视图。
- paimon：`getMvccPartitionView` / `listPartitions`，`(snapshotId, schemaId)` keyed。
- hive：`HiveConnectorMetadata.listPartitions`，`(db, table)` keyed（snapshotId=-1）+ TTL/失效。
- maxcompute：`listPartitions`，`(db, table)` keyed + TTL/失效。
- jdbc/es/trino：分区退化/罕见，框架可接，本期 no-op。

**失效**：在各连接器已 override 的 `invalidateTable/invalidateDb/invalidateAll/invalidatePartition` 里加 `partitionViewCache.invalidate*`。`invalidatePartition`（SPI 只带 values 不带 name）对本缓存按 `(db,table)` 整表失效（本就是整表视图，粒度天然对齐、偏保守=安全）。iceberg 因 key 带 snapshotId，REFRESH 后新 snapshot 自然 miss，失效为冗余保险。

**session=user**：沿用现有规则——`iceberg.rest.session=user` 下该连接器缓存置 `null` 禁用，挂 `check-authz-cache-sharding.sh` 标记。

## 6. 缓存 B：接入 `NereidsSortedPartitionsCacheManager`

**让外表实现 `SupportBinarySearchFilteringPartitions`**（`PluginDrivenMvccExternalTable` 覆盖 hive/iceberg/paimon；`PluginDrivenExternalTable` 覆盖 maxcompute 等非 MVCC）：
- `getOriginPartitions(scan)`：返回该 scan pinned 快照的**冻结 `nameToPartitionItem`**（`PluginDrivenMvccSnapshot.getNameToPartitionItem()`，数据来自缓存 A / 冻结层，非重新远程列举）。
- `getPartitionMetaVersion(scan)`：版本令牌（见下）。
- `getPartitionMetaLoadTimeMillis(scan)`：该分区元数据物化时刻（喂"插入过频跳过排序"启发式；取快照 pin / 缓存 A 条目载入时刻）。

**复活死桩**：`ExternalTable.getSortedPartitionRanges(scan)` 改为委托 `Env.getCurrentEnv().getSortedPartitionsCacheManager().get(this, scan)`——与 OLAP 的 `PruneOlapScanPartition` 同一套。

**裁剪路径接线**：`PruneFileScanPartition.pruneExternalPartitions` 的 ranges 获取改为：
```java
sortedPartitionRanges = scan.getSelectedPartitions().sortedPartitionRanges          // 冻结优先 (#65659)
        .or(() -> (Optional) externalTable.getSortedPartitionRanges(scan))           // 缓存 B
        .or(() -> Optional.ofNullable(SortedPartitionRanges.build(nameToPartitionItem)));  // 兜底
```
保留 #65659"冻结字段优先"的语义；缓存 B 命中即复用预建 ranges；缓存 B 返回空（跳过排序启发式/禁用）时退回现建；二者皆从 pinned 快照的同一 map 派生。

## 7. 版本令牌（缓存 B 正确性与两层联动）

统一走**已每语句穿到底的 SPI 类型 `ConnectorMvccSnapshot` 上新增的一个 `metaVersion` 字段**（不新增 SPI 方法，只在既有类型加字段）：
- **iceberg / paimon**：`metaVersion = (snapshotId, schemaId)`——不可变、精确（`ConnectorMvccSnapshot.getSnapshotId():55 / getSchemaId():73` 已有；iceberg 由 `loadLatestSnapshotPin`（`IcebergConnectorMetadata.java:1630`）原子捕获）。新快照自然换 key。
- **hive / 非 MVCC**：`metaVersion = 连接器维护的单调 generation`（连接器内 `ConcurrentHashMap<TableId, AtomicLong>`，`invalidateTable/invalidatePartition` 时 bump、缓存 A reload 时 bump）。廉价 in-memory、精确，且**让缓存 A 与缓存 B 版本联动**（B 的 ranges 绝不比 A 的视图更旧）。

`getPartitionMetaVersion(scan)` 从 scan 的 pinned 快照读出该 `metaVersion`（scan 携带 `tableSnapshot`/`scanParams` → `StatementContext.getSnapshot` → pinned `PluginDrivenMvccSnapshot` → `ConnectorMvccSnapshot.metaVersion`）。

## 8. 正确性论证（两层皆不复发 #65659 TOCTOU）

- **缓存 A**：只在 `materializeLatest()` 那一次被读，读出即冻结进 `PluginDrivenMvccSnapshot`；`PruneFileScanPartition` 只读冻结副本，绝不回连缓存 A。同一 key 并发命中返回同一不可变 value。跨查询：MVCC 令牌保证永不把 S+1 当 S；hive 为 bounded-staleness（TTL + 失效钩子，= `use_meta_cache` 契约）。
- **缓存 B**：`getPartitionMetaVersion(scan)` = pinned 快照/generation，`getOriginPartitions(scan)` 返回同一 pinned 快照的冻结 `nameToPartitionItem`——ranges 与 `nameToPartitionItem` **同源**；版本不符即重建。故 `prunedPartitions ⊆ nameToPartitionItem.keySet()` 恒成立，`PruneFileScanPartition` 的 `Preconditions.checkState(item != null, ...)`（#65659）永不触发。
- **查询内一致**由既有冻结层保证（物化一次）；**跨查询**由版本令牌（MVCC）/ TTL+失效（hive）保证 bounded staleness。二者组合等价于 Trino 的"共享 TTL 层 + per-transaction memoizer"。

## 9. 隔离与配置

- **SPI 隔离**：缓存 A 完全在 SPI 线以下（连接器 final 字段，缓存 SPI 类型），fe-core 一行不改。缓存 B 在 fe-core，但 key=`(表)+不透明 SPI 令牌`、`getOriginPartitions` 只消费 SPI 输出、失效走连接器钩子——不碰引擎内部，与 OLAP 同构。
- **session=user**：缓存 A 沿用"置 null 禁用"。缓存 B 只存 `(表,快照)` 维度 ranges、**不含任何凭证**，与 OLAP 同样跨用户共享安全（同 snapshotId ⇒ 同分区集），不禁用。
- **配置开关**：缓存 A `meta.cache.<engine>.partition_view.(enable|ttl-second|capacity)`（默认 ON/86400s），`enable=false`/`ttl=0` 一键关（对应 Trino `cache-partitions=false` 逃生阀）；缓存 B 复用现有 session 变量 `enableBinarySearchFilteringPartitions` + `cacheSortedPartitionIntervalSecond` + FE 配置（`NereidsSortedPartitionsCacheManager` 现有的 `sortedPartitionTableManageNum`/`expireSortedPartitionTableInFeSecond`）。

## 10. 失效汇总（复用现成管线，零新增管线）

| 触发 | 现有 fan-out | 本设计追加 |
|---|---|---|
| REFRESH TABLE / HMS REFRESH_TABLE | `RefreshManager.refreshTableInternal` → `connector.invalidateTable` | 钩子内 `partitionViewCache.invalidateTable` + bump generation；`NereidsSortedPartitionsCacheManager.invalidate(catalog,db,table)` |
| REFRESH CATALOG | `PluginDrivenExternalCatalog.onRefreshCache` → `connector.invalidateAll` | `partitionViewCache.invalidateAll`；ranges 缓存对应库表失效 |
| REFRESH DB | `connector.invalidateDb` | `partitionViewCache.invalidateDb` |
| HMS ADD/DROP/REFRESH_PARTITIONS | `connector.invalidatePartition` | `partitionViewCache.invalidateTable`(整表) + bump generation → 缓存 B 版本自然失配重建 |
| DROP/RENAME TABLE, DROP DB | `connector.invalidateTable/invalidateDb` | 同上 |

## 11. 测试

**单元测试（我方跑）**
- 缓存 A：per-engine miss→hit；按 `(snapshotId,schemaId)` 分桶（不同快照互不串味）；4 钩子失效各自生效；`session=user` 下禁用（返回 null 走直连）；带 filter 绕过。仿 `IcebergPartitionCacheTest`/`CachingHmsClientTest`。
- 缓存 B：外表实现 `SupportBinarySearchFilteringPartitions` 三方法；版本变更（snapshotId 变 / generation bump）触发重建；死桩 `getSortedPartitionRanges` 委托生效；`getOriginPartitions` 与冻结 map 同源 → `prunedPartitions ⊆ keySet`，`Preconditions` 不触发；"插入过频跳过排序"启发式。
- `PruneFileScanPartition`：二分裁剪结果与 legacy/兜底 `build()` 一致的回归（同一谓词、同一分区集 → 同一 pruned 集）。
- 复用/借鉴 `BinarySearchPartitionInconsistencyTest`（#65659）验证冻结一致性不被破坏。

**e2e（用户跑）**
- 大量分区外表重复查询：第二次起命中缓存 A（无远程枚举）、命中缓存 B（无 ranges 重建）。
- `ALTER TABLE ADD/DROP PARTITION` 后失效可见（下次查询反映新分区集）。
- iceberg 时间旅行 `FOR VERSION/TIME AS OF` 不串味（不同 snapshotId 独立缓存）。
- `set enableBinarySearchFilteringPartitions=false` / `meta.cache.<engine>.partition_view.enable=false` 回退行为。
- `REFRESH TABLE`/`REFRESH CATALOG` 立即生效。

## 12. 风险与回滚

- 缓存 A 纯旁路：`enable=false` 即回到当前每查询枚举行为。
- 缓存 B 是复用既有 OLAP 机制 + 复活死桩：`enableBinarySearchFilteringPartitions=false` 即回到 #65659 的每查询 `build()` 行为。
- 均不改冻结层、不动 `PartitionPruner`/`PruneFileScanPartition` 的裁剪算法，故不影响 #65659 的正确性结论。
- 主要风险面 = 失效遗漏导致 bounded staleness 超预期；缓解 = 两层失效均挂同一批已验证的连接器钩子 + TTL 上限。

## 13. 影响文件清单（预估）

**新增**
- `fe/fe-connector/fe-connector-cache/.../cache/ConnectorPartitionViewCache.java`（+ 测试）
- `fe/fe-connector/fe-connector-cache/.../cache/PartitionViewCacheKey.java`

**修改**
- SPI：`fe-connector-*/.../ConnectorMvccSnapshot`（加 `metaVersion` 字段 + builder）。
- iceberg：`IcebergConnector`（持有缓存 A + session=user 置 null + 4 钩子）、`IcebergConnectorMetadata`（`getMvccPartitionView`/`listPartitions` 包缓存、`beginQuerySnapshot` 填 `metaVersion`）。
- paimon：`PaimonConnector`/`PaimonConnectorMetadata` 同构。
- hive：`HiveConnector`（缓存 A + generation map + 钩子）、`HiveConnectorMetadata`（`listPartitions` 包缓存、`beginQuerySnapshot` 填 generation）。
- maxcompute：`MaxComputeConnector`/`MaxComputeConnectorMetadata` 同 hive 模式。
- fe-core：`ExternalTable`（`getSortedPartitionRanges` 委托 `NereidsSortedPartitionsCacheManager`）、`PluginDrivenMvccExternalTable`/`PluginDrivenExternalTable`（实现 `SupportBinarySearchFilteringPartitions` 三方法）、`PruneFileScanPartition`（ranges 获取链加缓存 B）。
- 失效：各连接器 `invalidate*` 钩子内追加两层失效调用。

## 14. 遗留 / follow-up（本设计不做）

- jdbc/es/trino 连接器接入缓存 A（分区退化，收益低，待需求驱动）。
- 若 profiling 显示某引擎 `getMvccPartitionView` 派生（dedup+buildRange+merge）仍是瓶颈，可在连接器内进一步拆分缓存粒度。
