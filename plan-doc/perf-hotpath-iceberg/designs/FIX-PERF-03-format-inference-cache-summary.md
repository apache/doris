# FIX-PERF-03 — Summary（`file_format_type` 兜底整表 planFiles 跨查询 memoize）

> 权威设计见 [`FIX-PERF-03-format-inference-cache-design.md`](./FIX-PERF-03-format-inference-cache-design.md)。本文件只记落地结果。
> commit `0b96f2e6c78`（`[perf]`，iceberg 自包含）。基线 HEAD `cab6e1ed50e`（PERF-01/02 已合）。

## Problem

每条 `SELECT`/`EXPLAIN` 在 scan-node 初始化期算 scan 级 `file_format_type`（`getScanNodeProperties:1350` →
`IcebergWriterHelper.getFileFormat`）。表属性无 `write-format` 且无 `write.format.default`（迁移表 + 从不显式设该属性的表）
时退化成 `inferFileFormatFromDataFiles → table.newScan().planFiles()` —— **无过滤整表 manifest 扫描**（远端 IO，
`ParallelIterable` 激进 fan-out），只为读第一个数据文件的格式。node 级已 memoize 故查询内 1 次，但**跨查询零缓存**：
大表每查询数百 ms~秒级远端 IO ×QPS。这是 #64134 兜底在新框架以 per-query 乘数复活（C2/C11 同一重操作两视角）。

## Fix（连接器侧，零 fe-core 改动）

- **新增 `IcebergFormatCache`**（挂 `IcebergConnector`）：键 `(TableIdentifier, currentSnapshotId)`，值 = 推断出的
  格式名 `String`。快照不可变 ⇒ 首文件格式是键的纯函数；snapshotId 由 `IcebergLatestSnapshotCache` 跨查询稳定。
  只缓存**推断兜底**这条，两个属性探测保持 cheap 不缓存。
- **无凭证 gate**（同 PERF-02 partition cache）：值是纯元数据（格式名，无 FileIO/token）→ 所有目录无条件构造
  （含 session=user / REST vended），仅 TTL 控。REFRESH TABLE/DB/CATALOG 逐级清空。
- **`IcebergWriterHelper`**：加 cache-aware `getFileFormat(Table, TableIdentifier, IcebergFormatCache)` 重载 +
  `resolveFileFormatName` 重载；拆 `inferFirstDataFileFormat`(**透传异常**，`close()` 的 checked `IOException`
  包成 `UncheckedIOException`) / `inferFileFormatFromDataFiles`(吞→委派，写路径保留)。orc/parquet 映射与其
  unsupported-format throw 在 `getOrLoad` **之外** → 缓存永不存"会抛"的值。
- **`IcebergScanPlanProvider`**：加 nullable `formatCache` 字段 + 7 参 ctor（6 参 ctor 委派传 null，离线测试与
  既有调用全走 null 保持原行为）；`getScanNodeProperties:1350` 非系统表分支改用 cache-aware 重载。
- **写路径 7 个 `getFileFormat(Table)` 调用点不动**（PERF-07/C20 范围）。

## 路线决定（复核否决"从 planScan 反推"）

`FileQueryScanNode.createScanRangeLocations` **先** `getFileFormatType`（line 325）**后** `getSplits/planScan`
（line 422）：格式在数据扫描前就要给出，无法复用 planScan 枚举；且推断是无过滤、planScan 带谓词，剪空时反推破
parity。改时序须动 fe-core（违反铁律）→ 退回 memoize（正是设计预判）。

## Parity

- 同 currentSnapshot 推断（非 handle time-travel pin），与 legacy `IcebergUtils.getFileFormat` 一致；时旅查询共享。
- **失败不缓存**：loader 抛出（unchecked）→ `MetaCacheEntry` 不 put → 下条查询重试（legacy 吞→parquet 的可重试性保留）。
- **混格式表**：scan 级格式从"每查询任意"变"每快照确定"——correctness 中性（per-file 格式仍逐 range），记为
  **可复现性变化**（红队 R2）：某"仅 scan 级 ORC 才触发"的潜伏 BE bug 会从间歇变每快照恒发/恒不发。
- scan 级值仍只选 BE V1/V2 scanner，语义不变。

## Tests

- **单测 `IcebergFormatCacheTest`**（8）：TTL 内命中同键 loadCount=1、不同 snapshotId 不同键、`ttl<=0`/负值关、
  invalidate/DB/All、**失败不入缓存+可重试**。
- **度量守门（集成）** `IcebergWriterHelperTest`（真 InMemoryCatalog 表 + 真数据文件）：无格式属性表重复
  `getFileFormat(table,id,cache)` 3 次 → `loadCountForTest()==1`+`size()==1`，与无缓存 parity；**属性表不进缓存**
  （`size()==0`）；null cache 存活；**avro 首文件**推断一次但每次重抛（R3，loadCount==1）。
- **连接器 gate + 失效** `IcebergConnectorCacheTest`（+2）：plain/vended/session 三目录 `formatCacheForTest()` 非 null；
  REFRESH TABLE/DB/CATALOG 逐级清空。

## Result

- 全 iceberg 模块 **957 pass / 0 fail / 1 skip**（`install -am`），checkstyle 绿，0 回归。
- 减负：无格式属性表每查询整表格式推断从"每查询一遍"→"每快照一遍"（跨查询命中）。metastore/对象存储 IO 除以 QPS-over-TTL。
- 可复用产物：`IcebergFormatCache`（第三块 `(TableIdentifier[,snapshotId])` MetaCacheEntry 基建，值 `String`）；
  「值纯元数据→无 gate」判据第三次印证；「映射/抛点放 getOrLoad 之外，缓存只存 loader 原始输出」的失败不缓存范式。
