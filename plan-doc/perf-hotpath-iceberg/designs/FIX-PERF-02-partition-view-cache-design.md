# FIX-PERF-02 — 分区视图跨查询缓存（(table, snapshotId) → raw partitions）

> 覆盖审计发现 C7 C23 C22（簇3）。基线 HEAD `65cc6d4da2d`（PERF-01 已合），分支 `catalog-spi-review-16`。
> 行号信 grep 不信本文档。决策已定（用户 2026-07-18）：**连接器侧 `(TableIdentifier, snapshotId)` 缓存 raw partitions；不动 fe-core、不加 MTMV refresh pin（见 §5.3）。**

---

## 1. Problem

对分区表，每条查询在**分析期**（`BindRelation → StatementContext.loadSnapshots → PluginDrivenMvccExternalTable.materializeLatest`）会走 `getMvccPartitionView`（RANGE/time-transform 表）或 `listPartitions`（其余分区表）→ `IcebergPartitionUtils.loadRawPartitions` = 扫 iceberg PARTITIONS 元数据表（`partitionsTable.newScan().useSnapshot(id).planFiles() + rows()`，SDK 聚合时读该快照**全部** data+delete manifest，O(#manifests) 远程 IO，**与查询选择性无关**）。`StatementContext` 只单语句 memoize，跨查询零缓存。SHOW PARTITIONS（`listPartitionNames`）与 selectedPartitionNum（`listPartitions`）同源。MTMV 一次 refresh 里同一视图被 materialize **4~6 次**。

## 2. Root Cause（含复核修正）

- SPI 迁移把 legacy `IcebergExternalMetaCache.loadSnapshotProjection`（把 `loadPartitionInfo` 缓进 TTL'd tableEntry）拆掉；`IcebergLatestSnapshotCache` 只存 `(snapshotId,schemaId)`，**不缓存派生的分区视图**。故对 MTMV-eligible 表这是**丢失的缓存回退**；对非 eligible 分区表，per-query PARTITIONS 扫描是 selectedPartitionNum 特性引入的**新增未缓存成本**（复核实证 vs master）。
- **PERF-01 已消除本簇的 `loadTable` 那半**（三处已走 `resolveTableForRead`）；**剩余未缓存 = `loadRawPartitions` 的 PARTITIONS 扫描**。本任务只补这半。
- **快照键可用**：三消费方内部都把 snapshotId 解析成**具体值**（`buildMvccPartitionView`：pin>=0 用 pin，否则 `currentSnapshot()`；`listPartitions`/`listPartitionNames`：`currentSnapshot()`），故缓存须在 `IcebergPartitionUtils` 内（解析点）按解析后的 snapshotId 建键。该 snapshotId 已由 `IcebergLatestSnapshotCache` 稳定（同 TTL 内不变）。
- **MTMV 放大自动收敛**：4~6 次 materialize 各经 `beginQuerySnapshot → IcebergLatestSnapshotCache`（同一 refresh 内命中同一 snapshotId）→ 同一 `(id, snapshotId)` 键 → 本缓存命中，坍缩重复远程扫描（无需 refresh pin，见 §5.3）。

## 3. Design（连接器侧，零 fe-core 改动）

**新增 `IcebergPartitionCache`（挂 `IcebergConnector`）**：`MetaCacheEntry<Key, List<IcebergRawPartition>>`，`Key=(TableIdentifier id, long snapshotId)`。值 = `loadRawPartitions` 的原始分区列表（纯元数据：名/值/transform/时间戳/快照 id，**无 FileIO/凭证**）。TTL = 同一 `meta.cache.iceberg.table.ttl-second`，capacity 复用默认；manual-miss-load（异常原样透传，保 `listPartitions` 的 `ValidationException`→空降级；失败不入缓存）。

- **无凭证 gate**（≠ PERF-01 的 `IcebergTableCache`）：值不含凭证，跨用户共享安全 → 对所有目录无条件构造。
- **消费**：`IcebergPartitionUtils.loadRawPartitions` 先查缓存；`buildMvccPartitionView`/`listPartitions`/`listPartitionNames` 三方法加 `(TableIdentifier id, IcebergPartitionCache cache)` 参（旧签名保留为 null 重载给单测）。三方法共享同一份 raw → SHOW PARTITIONS / selectedPartitionNum / MTMV 视图互相命中。
- **值不可变共享**：`IcebergRawPartition` 全 final（改为包内可见供缓存引用）；缓存的列表 `unmodifiableList` 包一层，跨并发只读安全。
- **失效**：`IcebergConnector.invalidate{Table,Db,All}` 各加 `partitionCache` 清空（`invalidate(id)` 用 `invalidateIf(k -> k.id.equals(id))` 清该表所有快照条目；`invalidateDb` 按 namespace；`invalidateAll` 全清）。
- **快照/schema 可见性 parity**：同 `(id, snapshotId)` 永远同结果（快照不可变）；跨快照（新提交→新 snapshotId→新键）miss→实扫。= master TTL 语义，非新增 staleness。

## 4. Implementation Plan（一个 commit，iceberg 自包含）

1. `IcebergPartitionUtils`：`IcebergRawPartition` `private`→包内；`loadRawPartitions` 拆 `loadRawPartitionsUncached` + cache-aware 包装；三 public 方法加 `(id, cache)` 参 + 旧签名重载（null,null）。
2. 新增 `IcebergPartitionCache.java`（+ 静态 `Key`；`getOrLoad/invalidate/invalidateDb/invalidateAll/isEnabled/size/loadCountForTest`）。
3. `IcebergConnector`：构造 `partitionCache`（无 gate）；`getMetadata` 传入；`invalidate*` 三钩子；`partitionCacheForTest()`。
4. `IcebergConnectorMetadata`：ctor 加 `partitionCache` 参（便利 ctor 传 null）；`getMvccPartitionView`/`listPartitionNames`/`listPartitions` 三处传 `(TableIdentifier.of(db,tbl), partitionCache)`。

## 5. Risks & Open Questions

1. **凭证**：raw partitions 无凭证 → 无 gate（已核 `IcebergRawPartition` 字段）。
2. **DDL 隔离**：DDL/写不走分区枚举路径，天然隔离；REFRESH 清缓存。
3. **MTMV refresh pin 不做（范围决定，用户 2026-07-18 认可）**：ttl>0（默认）时 `IcebergLatestSnapshotCache` 已稳定住一次 refresh 内的 snapshotId，本缓存据此坍缩 4~6 次重复 → 性能目标达成，无需 fe-core 的 refresh 级 pin。仅 `ttl=0`（无缓存目录，用户主动放弃缓存）残留"枚举点间快照偏移"的**既有**一致性边角（重构前即存在），修它须改 fe-core 物化视图刷新上下文，违反"缓存放连接器、fe-core 只出不进"铁律 → 标为超范围，如需单开框架侧任务。
4. **CPU 派生未缓存**：缓存的是 raw（远程 IO 那半，审计的 heavy op）；`buildMvccPartitionView` 的 mergeOverlap 等 CPU 派生每次重跑（微秒~毫秒级，非远程 IO）。可接受；如成瓶颈另立缓存 final view 的任务。

## 6. Test Plan / 度量守门

- **缓存单测** `IcebergPartitionCacheTest`：计数 loader → TTL 内命中 loader 跑 1 次；`ttl<=0`/负值关；invalidate/invalidateDb/invalidateAll；异常原样透传。
- **度量守门（集成）**：真 InMemory 分区表 + 真 cache，`buildMvccPartitionView`（及 `listPartitions`）同 `(id, snapshotId)` 调两次 → `cache.loadCountForTest() == 1`（远程分区扫描跨查询恰 1 次；修前 = 2）。
- **parity**：现有 `IcebergPartitionUtilsTest`/MVCC/分区计数单测全绿。
- **build**：`install -am` + iceberg 测试类过滤 + `-Dmaven.build.cache.enabled=false`。
