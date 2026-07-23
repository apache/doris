# Problem

iceberg 表（及任何 join 了 iceberg 表的查询）**永远启用不了 SqlCache（结果缓存）**。

**重要澄清：这不是"返回过期错误结果"的正确性 bug，而是"永远进不了缓存"的可用性 bug。** 缓存是否过期由一条**独立**的校验兜底：`SqlCacheContext`（存）+ `NereidsSqlCacheManager`（比）直接调 `getNewestUpdateVersionOrTime()` 取表版本 token 做**相等比较**，不等即作废；这条路径**不经过** `CacheAnalyzer.latestPartitionTime`，与本次改动无关，故不会引入陈旧命中。

# Root Cause

`CacheAnalyzer.CacheTable.latestPartitionTime` 一个字段承担两义：
1. **30 秒静默窗闸门**（`innerCheckCacheModeForNereids`：`now = Math.max(now, latestPartitionTime)` 后 `(now - latestTable.latestPartitionTime) >= 30s` 才允许缓存）——需要**墙钟毫秒**。
2. **BE PCache 版本键** `LastVersionTime`（`SqlCache.updateCache` → proto `setLastVersionTime(latestPartitionTime)`）——需要**单调版本 token**。

外部表 `buildCacheTableForExternalScanNode` 把 `latestPartitionTime = getNewestUpdateVersionOrTime()`（token）。hive/paimon 的 token 恰好是墙钟毫秒（transient_lastDdlTime / lastFileCreationTime），闸门凑巧正确；**iceberg 的 token 是微秒**（PARTITIONS 元数据 `last_updated_at`，约 1.7e15）。于是 `Math.max(now_millis, micros)` 把 now 抬到微秒值，`now - latestPartitionTime` **恒为 0**，永远 < 30s → iceberg 永不缓存；且多表查询里巨大的微秒值占上风，把整条查询也带崩。

上游 master 靠"只对 hive 扫描节点开外部缓存"绕过（iceberg 根本不进闸门）；本分支放宽成连接器无关的 `MTMVRelatedTableIf` 能力位，本意让 iceberg 也能缓存，却被这个单位错误默默挡住——**净效果同 master（iceberg 不缓存），但机制变成潜伏单位缺陷**。

`ExternalTable.getUpdateTime()` **不能**当墙钟源：它在 `initSchemaAndUpdateTime` 里被设成"schema 加载时刻"（`System.currentTimeMillis()`），反映的是"Doris 上次加载 schema"而非"表数据上次变更"，用它做静默窗会 schema 一加载就≈now、逻辑错位。

# Design（方向 B：新增墙钟毫秒，与版本 token 分开；用户已选）

核心：给闸门一个**真墙钟毫秒**，与"版本 token"彻底分开。token 路径（Nereids 陈旧校验 + BE 版本键）**一行不动** → 不可能引入陈旧命中；墙钟只决定"是否允许进缓存"这道启发式闸门。

墙钟值由**连接器提供**（连接器才知道自己的单位，符合"fe-core 不派生/不解析连接器语义"原则），iceberg = `max(last_updated_at) / 1000`（微秒→毫秒）。hive/paimon 的墙钟就是其现有毫秒值。

## 改动

1. **fe-connector-api** `ConnectorMvccPartitionView`：加并列字段 `newestUpdateWallClockMillis`（连接器填真墙钟毫秒；与现有 monotonic marker 并存）。iceberg 之外的连接器不产此 view，无影响。
2. **fe-connector-iceberg** `IcebergPartitionUtils`：构 view 时同时算 `max(last_updated_at)`（marker，微秒，不变）与 `/1000`（墙钟毫秒）。
3. **fe-core** `PluginDrivenMvccSnapshot`：镜像该墙钟字段 + getter。
4. **fe-core** `MTMVRelatedTableIf`：加默认方法 `default long getNewestUpdateTimeMillisForCache() { return getNewestUpdateVersionOrTime(); }`（对 token 本就是毫秒的 hive/paimon/olap 正确）。
5. **fe-core** `PluginDrivenMvccExternalTable`：override 之——`isLastModifiedFreshness`(hive)/paimon 分支同现毫秒；range-view(iceberg) 分支返回 view 的墙钟毫秒。
6. **fe-core** `CacheAnalyzer`：
   - `CacheTable` 加并列字段 `latestPartitionUpdateMillis`（墙钟）。
   - `buildCacheTableForOlapScanNode`：`latestPartitionUpdateMillis = partition.getVisibleVersionTime()`（同 OLAP 现值）。
   - `buildCacheTableForExternalScanNode`：`latestPartitionTime` **保持 = token（BE 版本键不动）**；`latestPartitionUpdateMillis = getNewestUpdateTimeMillisForCache()`。
   - 闸门改用墙钟：`maxUpdateMillis = max_over_tables(latestPartitionUpdateMillis)`；`now = max(nowtime, maxUpdateMillis)`；`(now - maxUpdateMillis) >= 30s` 才允许。**与 `latestTable`（token 排序，供 BE PCache）解耦**——闸门只关心"任一表是否 30s 内改过"。

## 为什么不改更简单的 A

方向 A（外部表干脆去掉墙钟静默窗、只要有有效 token 就允许缓存）更小、无新 SPI，正确性同样由 token 相等兜底；代价是放弃"刚改过的表先别缓存"这个纯优化，且会改变 hive/paimon 现有缓存时机。用户已选 B（保守、保留静默窗、只修 iceberg 单位），故按 B 设计。B 的 SPI 新增碰"fe-core 只出不进"铁律，用户已就方向 B 签字接受。

# Risk Analysis

- **无陈旧命中**：陈旧保护是 `getNewestUpdateVersionOrTime()`（token）相等比较，本改动不碰；BE PCache 版本键 `latestPartitionTime` 仍 = token（微秒全精度），不改。墙钟仅入闸门算术。
- **多表查询**：闸门用 `max(墙钟)`，与 `latestTable`（token 排序，供 BE PCache）解耦，行为可预期。
- **单位来源**：墙钟由连接器算（iceberg /1000），fe-core 不假设单位（守 no-derivation 原则）。
- **铁律**：新增 1 SPI 字段（fe-connector-api）+ 1 默认方法 + 1 override + CacheAnalyzer 字段/闸门（fe-core）。用户已就方向 B 签字。

# Test Plan

## Unit Tests
- `IcebergPartitionUtilsTest`：view 携带的墙钟毫秒 = marker(微秒)/1000。
- `PluginDrivenMvccExternalTableTest`：`getNewestUpdateTimeMillisForCache()` range-view 分支返回墙钟毫秒、hive/paimon 分支同 token。
- `CacheAnalyzer` 单测（若有测试基建）：iceberg 单表 30s 前更新 → 允许缓存；join 场景闸门用 max(墙钟)。

## E2E Tests
`test_iceberg_sqlcache.groovy`：iceberg 表建好静置 > `cache_last_version_interval_second`；`set enable_sql_cache=true`；同一 SELECT 连跑两次断言第二次命中（`show ...`/profile 的 cache 命中标志）；再 INSERT 一行后同 SELECT 断言**未命中**（token 变 → 作废，证不返回陈旧结果）。
