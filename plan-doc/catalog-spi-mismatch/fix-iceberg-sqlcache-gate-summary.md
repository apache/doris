# Summary

## Problem

iceberg 表（及任何 join 了 iceberg 表的查询）**永远启用不了 SqlCache**——是"永不缓存"的可用性 bug，不是"返回陈旧结果"的正确性 bug（陈旧由独立的版本 token 相等比较兜底）。

## Root Cause

`CacheAnalyzer.CacheTable.latestPartitionTime` 一字段两义：既当 30 秒静默窗闸门的墙钟输入，又当 BE PCache 版本键。外部表填 `getNewestUpdateVersionOrTime()`（token）；hive/paimon 的 token 恰是毫秒、凑巧对，**iceberg 的 token 是微秒**（`last_updated_at`，约 1.7e15）→ `Math.max(now, micros)` 把 now 抬到微秒 → `now - token` 恒 0 < 30s → iceberg 永不缓存，且带崩含它的多表查询。

## Fix（方向 B：连接器提供墙钟毫秒，与版本 token 分开）

墙钟由连接器算（iceberg = `last_updated_at ÷ 1000`），与"版本 token"彻底分开；token 路径（Nereids 陈旧校验 + BE 版本键）一行不动。

1. `ConnectorMvccPartitionView`（SPI 载体）：加并列字段 `newestUpdateWallClockMillis`（重载构造，旧 4 参调用方默认 0）。
2. `IcebergPartitionUtils`：构视图时算 `marker / 1000` 作墙钟填入（marker 仍微秒，供 token）。
3. `PluginDrivenMvccSnapshot`：镜像墙钟字段 + getter（重载构造，仅 range-view 站点传真值）。
4. `MTMVRelatedTableIf`：加默认方法 `getNewestUpdateTimeMillisForCache()`（默认返回 `getNewestUpdateVersionOrTime()`，对 token 本就毫秒的 olap/hive/paimon 正确）。
5. `PluginDrivenMvccExternalTable`：override 之——range-view(iceberg) 分支返回 `pin.getNewestUpdateWallClockMillis()`；hive/paimon 分支同现毫秒。
6. `CacheAnalyzer`：`CacheTable` 加 `latestPartitionUpdateMillis`（墙钟）；OLAP 填 `visibleVersionTime`、外部表填新方法；`latestPartitionTime` **仍 = token**（BE 版本键不动）；静默窗闸门改用 `max(latestPartitionUpdateMillis)`，与 `latestTable`（token 排序，供 BE PCache）解耦。

## Tests

- `IcebergPartitionUtilsTest`：视图墙钟 == 微秒 marker / 1000。
- `PluginDrivenMvccExternalTableTest.testRangeViewCacheGateUsesWallClockMillisNotMarker`：`getNewestUpdateTimeMillisForCache()` range-view 返回墙钟毫秒、`getNewestUpdateVersionOrTime()` 仍返回原微秒 token。
- `PluginTableCacheAnalyzerTest.testGateValueSourcedFromWallClockAccessor`：`latestPartitionUpdateMillis` 来自新墙钟方法、`latestPartitionTime` 仍是原 token（两义分开）。
- e2e `test_iceberg_sqlcache.groovy`：iceberg 表静置过静默窗后同一查询命中缓存（修复前恒不命中）；写入后立即断言不命中（token 变 → 作废，证不返回陈旧结果）。

## Result

- 编译 + checkstyle 全过（`BUILD SUCCESS`，跨 fe-connector-api/iceberg/hive/fe-core）；新单测全绿，无既有 MVCC/cache/view 测试回归（SPI 加字段向后兼容）。
- e2e 需在 iceberg docker 回归环境跑（`enableIcebergTest=true`）。
- **无陈旧命中**：陈旧保护 = 版本 token 相等比较（`SqlCacheContext`/`NereidsSqlCacheManager` 直接调 `getNewestUpdateVersionOrTime`），本次未碰；BE 版本键仍是原微秒 token（全精度）。墙钟仅入"是否允许缓存"闸门。
- 铁律：新增 1 SPI 字段 + 1 默认方法 + 1 override + CacheAnalyzer 字段/闸门（用户就方向 B 签字接受）。前置探针改名见 commit `8043536a812`。
