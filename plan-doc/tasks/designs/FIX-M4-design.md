# FIX-M4 — maxcompute 分区值缓存删除 → 连接器内重建（对齐旧版）

> 来源：`plan-doc/reviews/catalog-spi-review-65185-reverify-2026-07-11.md` §3 M4。
> recon+对抗红队：`wf_40498e52-19f`（verdict **SOUND**：连接器局部、正确 scope、忠实镜像 `HiveFileListingCache`，无铁律违背，测试编码真实 bug）。

## Problem

MaxCompute 外表**每次查询规划**都发一次全量 ODPS `getPartitions()` 往返。fe-core
`PluginDrivenExternalTable.getNameToPartitionItems`（经 `initSelectedPartitions`）每规划调一次
`ConnectorMetadata.listPartitions`，连接器用裸 ODPS SDK 调用服务它（fe-core + 连接器两层都无缓存）。宽表
（数万分区）每规划多秒 + ODPS 限流风险。纯性能回归——旧版 `MaxComputeExternalMetaCache.partitionValuesEntry`
（TTL Caffeine 缓存）被 SPI 迁移删了；正确性不受影响（BE 重过滤）。Hive/HMS 已在 P7 把等价缓存 re-home 进连接器
（`CachingHmsClient`/`HiveFileListingCache`），MaxCompute 是唯一仍显式无缓存的连接器。

## Root Cause

`MaxComputeDorisConnector.getMetadata()` 每次建全新 `MaxComputeConnectorMetadata`，其 `listPartitions`/
`listPartitionNames`/`listPartitionValues` 全调 `structureHelper.getPartitions(odps, db, table)`（网络 RPC）
无任何 memoization。metadata 对象 per-call，故缓存须落在长寿的 per-catalog `MaxComputeDorisConnector` 上并注入
metadata——正是 `HiveConnector` 持 `HiveFileListingCache` 交给 metadata 的方式。

## Design（连接器局部；无 fe-core 改/不解析属性）

1. 新 `MaxComputePartitionCache` = `HiveFileListingCache` 结构副本，靠共享 `fe-connector-cache`（`CacheSpec`+
   `MetaCacheEntry`）。keyed `(db, table)`（ODPS project per-catalog 恒定，不入 key）。value=`List<Partition>`
   按引用缓存（只读约定；三消费方只读本地 accessor `getPartitionSpec()`/`spec.keys()/get()`/`toString()`，无
   per-partition 懒加载）。`MetaCacheEntry` **contextual-only + manual-miss**（flag 元组 `(false,true,0L,true)`
   与 `HiveFileListingCache` 字节一致）→ 慢 loader 在调用线程同步跑（striped-lock dedup），TCCL/classloading 与
   今日裸调用字节一致。loader 经 `PartitionLister` 注入（无 Mockito 可测），生产 loader
   `(db,t)->structureHelper.getPartitions(odps,db,t)`。config `meta.cache.max_compute.partition.{enable,ttl-second,
   capacity}`，默认对齐**旧版 MaxCompute 分区缓存**（ttl=`external_cache_refresh_time_minutes*60`=**600s**、
   capacity=`max_hive_partition_table_cache_num`=10000）。**⚠ 最终复核纠正**：初版误抄 hive 文件缓存的 knob
   （`external_cache_expire_time_seconds_after_access`=86400s）→ 144x 过陈；已改 600s（commit `fca288424fc`）。
2. `MaxComputeDorisConnector` 持 `final` 缓存（ctor 建，loader lambda 捕获 this、查询时惰读 structureHelper/odps，
   均 post-init），`getMetadata` 注入；override 4 个 `Connector` REFRESH 钩子（`invalidateTable/Db/All/Partition`）
   路由到缓存（`invalidatePartition` 降级为整表刷，正确安全——miss 重列）。镜像 `HiveConnector`。
3. `MaxComputeConnectorMetadata` 加末位 ctor 参 + 三方法改走 `partitionCache.getPartitions`；订正 stale javadoc。
4. `pom.xml` 加 `fe-connector-cache` + Caffeine `2.9.3`（compile；插件 zip 自动打入 `lib/`，镜像 hive）。

## 实现要点 / 与设计的偏差（实现 agent 记录，已核）

- `MaxComputePartitionCache` 用**单个** package-private ctor `(Map, PartitionLister)`（非 hive 的双 ctor）：
  hive 默认 lister 是自包含 static 方法故可给 `(Map)`-only 公共 ctor；MaxCompute loader 需活连接器状态
  （odps/structureHelper），按设计恒由连接器注入 lambda，故只留注入式 ctor。连接器与测试均同包，无碍。
- 跨模块 javadoc `HiveFileListingCache`/`DirectoryLister` 引用软化为 `{@code}`（maxcompute 不依赖 hive，
  `@link` 会 doclint 失败）；模块内 `@link`（CacheSpec/MetaCacheEntry/…）保留。
- 5 处既有 metadata-ctor 测试站补 `null`（含注释）+ 1 处生产 `getMetadata` 注入真缓存。

## Risk

- 陈旧（有意）：TTL 内重复规划见缓存分区集；带外新增分区 TTL/REFRESH 前不可见。恢复旧版行为，受 REFRESH 钩子 +
  `ttl-second` 约束、正确安全（BE 重过滤）。release note 记。
- 按引用缓存 `Partition`：仅因消费方读列表响应已填充的本地 accessor 而安全（无路径读懒加载字段）。
- 打包：MaxCompute 原无 Caffeine；漏打 2.9.3 → 运行期 `NoClassDefFoundError`（memory
  `catalog-spi-connector-cache-framework-caffeine-coherence`）。**已实测**插件 zip 恰含一个 `caffeine-2.9.3.jar`
  + `fe-connector-cache`，odps-sdk 无冲突版本。
- ctor churn：5 测试站传 `null`；未来触分区的测试在这些文件会 NPE（null-arg 注释已标）。
- TCCL：须 contextual-only + manual-miss（loader 在 pin 的调用线程）；勿切 auto-refresh/后台条目。

## Test

- Unit `MaxComputePartitionCacheTest`（JUnit5、无 Mockito、recording fake）9 测：
  - 直接缓存 7（per-table 命中/键作用域/invalidate table·db·all/disable 绕过/失败不缓存）；
  - metadata 集成 2：`twoListPartitionsShareOneRoundTrip`（两 listPartitions→helper 计数==1）、
    `crossMethodShareOneRoundTrip`（listPartitions+listPartitionNames→==1，杀「只缓存一个方法」的半修）。
  - RED：直接组 greenfield 非编译红；集成组变异红（只回退三方法体、留 ctor 参 → 计数读 2 → `assertEquals(1,..)` 挂）。
  - 结果：9/9 + 模块 113/113（1 live skip）、0 checkstyle、import 门 exit 0、插件 zip caffeine 单版本。
- E2E live-gated：需真 MaxCompute 目录+凭证（同 `OdpsLiveConnectivityTest` 门），往返计数 SQL 不可观测→连接器单测为
  权威验证（`HiveFileListingCache` 亦无 live e2e）。有 live env 则验修前后查询正确性不变 + REFRESH TABLE 拾带外新增分区。
