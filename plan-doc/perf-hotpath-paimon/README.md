# perf-hotpath-paimon — PA-1 (partition-enumeration cache routing)

> 兄弟空间（镜像 `perf-hotpath-iceberg` 布局，但本项为单一常数倍/CPU 清理，故合并为一页）。
> 权威病灶分析在伞形空间 `plan-doc/connector-cache-unification/connectors/paimon.md`。

## 一句话
`listPartitionNames()`（SHOW PARTITIONS）与 `listPartitionValues()`（`partition_values()` TVF）此前**绕过** `partitionViewCache`、直接调 `collectPartitions()`，每次都把整张表的分区列表重新渲染成 `ConnectorPartitionInfo`；`listPartitions()`（裁剪路径）却已走缓存。本项把三个入口收敛到同一个"带缓存收集器" `cachedPartitions()`。

## 动码前重侦察（HEAD d8e2541e567，行号 vs 旧快照已漂移）
- `listPartitionNames` :1030 → `collectPartitions` 直连（旁路，确认）。
- `listPartitionValues` :1098 → `collectPartitions` 直连（旁路，确认）。
- `listPartitions` :1056 → `partitionViewCache.get(key, collectPartitions)`（走缓存）。
- key = `ConnectorTableKey(db,table,snapshotId,-1)`，`snapshotId` 经 `latestSnapshotCache`（`beginQuerySnapshot` 已预热）。
- 远程 `catalogOps.listPartitions` 已被 paimon SDK `CachingCatalog.partitionCache` 挡下 → 省的是**本地 CPU 重渲染**（名称转义/日期格式化/空值归一化/查重），非远程 IO。

## 实现（commit `59b65912104`，连接器侧，0 fe-core）
- 抽私有 `cachedPartitions(handle)` = `listPartitions` 无过滤分支的等价体（`cache==null || 无分区键 → collectPartitions`；否则 `cache.get(key, collectPartitions)`）。
- `listPartitions` 保留 `filter.isPresent() → collectPartitions`（带过滤不写缓存），其余委派 `cachedPartitions`。
- `listPartitionNames`/`listPartitionValues` 改调 `cachedPartitions`，派生循环（`getPartitionName()` / 按请求列序 `getPartitionValues()`）逐字不变。
- 更新 4 处会变陈旧的注释（两处字段注释、`collectPartitions`/`listPartitions` javadoc、测试类头 javadoc）。

## owner 拍板的语义变化（2026-07-24，选"走缓存"）
`SHOW PARTITIONS` / `partition_values()` 的新鲜度从"每次重算"变为"24h TTL + REFRESH 失效"，与已缓存的裁剪路径自洽、与 Trino 的带缓存分区列举一致；`REFRESH` 是逃生口。代价：与 hive"故意实时 SHOW PARTITIONS"不一致（但 hive 是刻意分出 fresh 方法，paimon 只读无写后读问题）。owner 明确选此方案。

## 验证
- `PaimonConnectorMetadataPartitionViewCacheTest` +4（名/值跨查询命中、三入口共享一条目 + 派生 parity、非分区表不碰快照缝）。
- `PaimonConnectorMetadataPartitionTest`（null-cache 路径字节 parity）原样全绿。
- 26/26 绿，checkstyle 0，BUILD SUCCESS。
- 3-lens 净室对抗复审：aliasing/freshness CLEAN；parity 只标"有意的新鲜度变化"（非 bug，已 owner 确认）；iron-rules/style CLEAN（修一处测试头陈旧注释）。
- **e2e 需集群本地未跑**（留标注）。
