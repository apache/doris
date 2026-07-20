# FIX-PERF-02 — Summary（分区视图跨查询缓存）

> 权威设计见 [`FIX-PERF-02-partition-view-cache-design.md`](./FIX-PERF-02-partition-view-cache-design.md)。本文件只记落地结果。
> commit `518d0599cbf`（`[perf]`，iceberg 自包含）。基线 HEAD `65cc6d4da2d`（PERF-01 已合）。

## Problem

分区表每条查询在分析期扫一遍 PARTITIONS 元数据表（`loadRawPartitions` → SDK 读该快照全部 data+delete manifest，O(#manifests) 远程 IO，与查询选择性无关），只单语句 memoize、跨查询零缓存；MTMV 一次 refresh 里同一视图被 materialize 4~6 次。= 重构时丢掉的 legacy `IcebergExternalMetaCache` 分区信息缓存。PERF-01 已消掉本簇的 `loadTable` 那半，本任务补 PARTITIONS 扫描这半。

## Fix（连接器侧，零 fe-core 改动）

- **新增 `IcebergPartitionCache`**（挂 `IcebergConnector`）：键 `(TableIdentifier, snapshotId)`，值 = raw 分区列表。三消费方（`buildMvccPartitionView` / `listPartitions` / `listPartitionNames`）都经 `loadRawPartitions` → **共享同一份扫描**。快照 id 由上一轮 `IcebergLatestSnapshotCache` 稳定 → 键跨查询/跨一次 refresh 稳定 → MTMV 4~6 次也坍缩成 1 次。
- **无凭证 gate**（≠ PERF-01 的 table cache）：值是纯元数据（名/值/transform/时间戳/快照 id），不含 FileIO/凭证 → 对所有目录无条件构造，仅 TTL 控制。
- 快照不可变 → 同键永远同结果（parity）；新提交→新 snapshotId→新键→实扫。缓存值 `unmodifiableList`；loader 异常原样透传（保 `listPartitions` 的 `ValidationException`→空降级），失败不入缓存。
- `IcebergPartitionUtils`：三方法加 `(TableIdentifier, IcebergPartitionCache)` 参 + 旧签名重载；`loadRawPartitions` 拆 cache-aware 包装 + `loadRawPartitionsUncached`；`IcebergRawPartition` 改包内可见。
- `IcebergConnector`：构造 `partitionCache`（无 gate）、传入 metadata、三个 `invalidate*` 清空、`partitionCacheForTest()`。
- `IcebergConnectorMetadata`：ctor 加 `partitionCache` 参（便利 ctor 传 null）；三处传 `(TableIdentifier.of(db,tbl), partitionCache)`。

## 范围决定（用户 2026-07-18 认可）

审计另建议的 **MTMV refresh 级 MvccSnapshot pin 未做**：`ttl>0`（默认）时 `IcebergLatestSnapshotCache` 已稳定住一次 refresh 内的 snapshotId，本缓存据此坍缩 4~6 次重复 → 性能目标达成，无需改 fe-core。仅 `ttl=0`（无缓存目录）残留"枚举点间快照偏移"的**既有**一致性边角，修它须改 fe-core → 超范围。

## Tests

- **缓存单测** `IcebergPartitionCacheTest`：TTL 内命中同实例 + loadCount=1、不同 snapshotId 不同键、`ttl<=0`/负值关、invalidate（清该表所有快照）/invalidateDb/invalidateAll、`ValidationException` 原样透传+失败不缓存。
- **度量守门（集成）** `IcebergPartitionUtilsTest.partitionScanIsCachedAcrossRepeatsAndConsumersAtSameSnapshot`：真分区表 + 真 cache，重复 `buildMvccPartitionView` + `listPartitions` 同快照 → `loadCountForTest()==1`（远程分区扫描跨查询/跨消费方恰 1 次；修前每次一扫），并与无缓存枚举 parity。
- **连接器 gate + 失效** `IcebergConnectorCacheTest`：plain/vended/session=user 三目录 partitionCache 均非空（无 gate）；REFRESH TABLE/DB/CATALOG 逐级清空。

## Result

- 全 iceberg 模块 **943 pass / 0 fail / 1 skip**（`install -am`），checkstyle 绿。
- 减负：分区表每查询分析期 PARTITIONS 远程扫描从"每次一遍"→"每快照一遍"（跨查询命中）；MTMV 一次 refresh 从 4~6 遍 → 1 遍。metastore/对象存储分区 IO 除以 QPS-over-TTL 与 refresh 枚举点数。
- parity：缓存枚举 == 实时枚举；快照/schema 可见性、时间旅行取快照不变。
