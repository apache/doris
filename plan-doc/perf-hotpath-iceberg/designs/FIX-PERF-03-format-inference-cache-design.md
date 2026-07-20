# FIX-PERF-03 — Design（`file_format_type` 兜底整表 planFiles 跨查询 memoize）

> 覆盖审计发现 **C2 / C11**（同一重操作的两个视角）。权威分析见审计报告簇2 + `findings.json` 的
> `IcebergWriterHelper.getFileFormat planFiles() fallback` / `getScanNodeProperties infers file_format_type via
> whole-table planFiles()` 两条。基线 HEAD `cab6e1ed50e`（PERF-01/02 已合）。行号以本次 grep 为准。

## Problem

每条对 iceberg 表的 `SELECT`/`EXPLAIN` 在 scan-node 初始化期都要产出 scan 级 `file_format_type`
（`IcebergScanPlanProvider.getScanNodeProperties:1329-1330`）。当表属性里**既无** `write-format`
昵称、**又无** `write.format.default`（= 迁移表 + 任何写引擎从不显式写该属性的表，parquet 是隐式默认不入元数据）
时，解析走 `IcebergWriterHelper.getFileFormat → resolveFileFormatName:284 → inferFileFormatFromDataFiles:287
→ table.newScan().planFiles():291` —— **一次无过滤的整表 manifest 扫描**，只为读**第一个数据文件的格式**。

- **重**：`planFiles()` 读该快照的 manifest-list + manifest（经 FileIO 远端对象存储 IO）；iceberg 的
  `ParallelIterable` 还会把 manifest reader 激进 fan-out 到 worker 池，虽然只消费第一个 task 就 close。
  大表（上千 manifest）每查询数百 ms~秒级远端 IO。
- **乘数**：每次 `getScanNodeProperties` 计算 1 次。该结果在 fe-core 侧被 node 级 memoize
  （`PluginDrivenScanNode.cachedPropertiesResult`），故**查询内 1 次**；但**跨查询零缓存** —— 每条查询/EXPLAIN 重扫一遍。
- **纯冗余的两个来源**：
  1. `planScan` 自己的（带谓词的）`planFiles` 枚举里，每个 `FileScanTask` 的 `dataFile.format()` 都已带格式；
  2. 每个 range 在 `buildRange:1092` 已按 `dataFile.format()` 逐文件下发格式。
  scan 级这个值只用来选 BE 的 V1/V2 scanner（`file_format_type` gate，见全局 memory），per-file 格式仍逐 range 走。

这是 #64134（旧 `IcebergUtils.getFileFormat` 的 planFiles 兜底，DORIS-27138 同型）在新框架以 per-query 乘数复活。

## Root Cause

新框架 SPI 无任何按 `(表, 快照)` 的格式 memo。快照不可变 ⇒ 该快照第一个数据文件的格式是**纯元数据、恒定**，
却被每条查询重新远端解析。legacy 侧此解析同样无缓存，但重构把它从"偶发"抬到了"每条 getScanNodeProperties 必经"。

## 为什么选"跨查询 memoize"（路线 a）而非"从 planScan 枚举反推"（路线 b）

HANDOFF 原倾向路线 b（消除而非缓存），但**复核否决 b**，落到 a，理由是 fe-core scan 生命周期的**时序**：

`FileQueryScanNode.createScanRangeLocations()` 里 **先** `getFileFormatType()`（**line 325** → 触发本重操作），
**后** `getSplits()`（**line 422** → 触发 `planScan`）。即 scan 级格式在 `planScan` **之前**就必须给出。

- 路线 b 要"复用 planScan 的枚举结果"，就得让 `getFileFormatType` 等 `planScan` 先跑、或 `planScan` 先跑再回填 —— 
  这需要**改 fe-core 时序或跨路径传值**，违反铁律「fe-core 源只出不进」，且是 source-specific reorder。
- 且两处语义不同：格式推断是 `table.newScan()`（**无过滤**、`currentSnapshot`），`planScan` 是**带谓词**扫描。
  谓词剪空所有文件时，从 `planScan` 反推会拿不到格式 → 与 legacy「无过滤兜底总能找到首文件格式」**破 parity**。

结论：正是 HANDOFF 预判的「两路径独立触发 → 退回 (a) memoize」。路线 a 无 fe-core 改动、无时序依赖、parity 严格。

## Design（连接器侧，零 fe-core 改动；复用 PERF-02 套路）

### 1. 新增 `IcebergFormatCache`（挂 `IcebergConnector`）
- 键 `(TableIdentifier, snapshotId)`，**值 = `String`**（`inferFileFormatFromDataFiles` 的原始输出格式名）。
- 结构逐字镜像 `IcebergPartitionCache`：`MetaCacheEntry<Key, String>`、TTL = `meta.cache.iceberg.table.ttl-second`
  （`<=0` 关）、`isEnabled/getOrLoad/invalidate/invalidateDb/invalidateAll/size/loadCountForTest`。
- **无凭证 gate**（同 PERF-02）：值是纯元数据（格式名字符串），不含 FileIO/凭证 → 对所有目录无条件构造，仅 TTL 控。

### 2. `IcebergWriterHelper` 加 cache-aware 重载（镜像 PERF-02 的 `loadRawPartitions` 拆包装）
- 保留现有 `getFileFormat(Table)`（写路径 + 离线继续用，行为字节不变）。
- 新增 `getFileFormat(Table, TableIdentifier, IcebergFormatCache)`：
  - `resolveFileFormatName` 里**属性优先**（`write-format` / `write.format.default` 两条 cheap 检查**不进缓存**）；
  - **仅 inference 兜底**这条走缓存：键 `(id, table.currentSnapshot().snapshotId())`，loader = 抛异常版推断
    `inferFirstDataFileFormat(table)`（`currentSnapshot==null`/`cache==null`/`id==null` → 绕过缓存走原逻辑）。
- **失败不入缓存**（严守 PERF-02 纪律）：把现有 `inferFileFormatFromDataFiles`（吞异常返回 parquet，写路径保留）
  重构成「`inferFirstDataFileFormat`（**透传异常**）+ 外层 try/catch 吞成 parquet 默认」；cache-aware 路径直接
  用透传版做 loader，`MetaCacheEntry` 对抛异常的 loader **不缓存** → 瞬时 IO 失败下条查询会重试（legacy parity）。
  空快照（有 snapshot 但零数据文件）→ 确定性 parquet，**可缓存**。
  - **R1（红队实现坑）**：loader 是 `Supplier<String>`，只能抛 unchecked。`MetaCacheEntry.loadAndTrack` 只捕
    `RuntimeException|Error` 后**重抛且不 put**（`MetaCacheEntry:337-343`），故 `inferFirstDataFileFormat` 里
    try-with-resources 的 `CloseableIterable.close()` 抛的 **checked `IOException` 必须包成 `UncheckedIOException`**
    再抛（iceberg 的 `planFiles/iterator/next` 本就抛 unchecked，直接透传即可），否则既编译不过、又破"失败不缓存"。

### 3. `IcebergScanPlanProvider`
- 加 `private final IcebergFormatCache formatCache;`（nullable，离线 ctor 传 null，同 `tableCache`）。
- 主 ctor（`IcebergConnector.getScanPlanProvider` 用的 7 参）加 `formatCache` 参；便利 ctor 链默认 null。
- **唯一改的调用点** `getScanNodeProperties:1329-1330`：非系统表分支
  `IcebergWriterHelper.getFileFormat(table)` → `IcebergWriterHelper.getFileFormat(table,
  TableIdentifier.of(iceHandle.getDbName(), iceHandle.getTableName()), formatCache)`。系统表仍 `"jni"` 不变。

### 4. `IcebergConnector`
- field `formatCache`；构造 `new IcebergFormatCache(resolveTableCacheTtlSecond(properties), DEFAULT_TABLE_CACHE_CAPACITY)`（无 gate）。
- `getScanPlanProvider` 传 `formatCache`；`invalidateTable/Db/All` 各加 `formatCache.invalidate*`；`formatCacheForTest()`。

### 写路径不动（范围边界）
`IcebergConnectorTransaction:658/690`、`IcebergWriterHelper:91`、`IcebergWritePlanProvider:400/464/525/746`
七个写路径 `getFileFormat(Table)` 调用点**保持无缓存重载**——那是 **PERF-07（C20，写路径 3~5 次 load）**的范围，
本任务只碰 scan 路径的那一个点。

## Parity 论证

- **同快照同结果**：键含 snapshotId，快照不可变；新提交→新 snapshotId→新键→实扫。缓存值 `String`，消费方
  `getFileFormat` 仍做 orc/parquet 映射（含对 `avro` 等不支持格式的 throw）——映射在 `getOrLoad` **之外**，
  故缓存永不存"会抛"的值、抛点行为与 legacy 完全一致。
- **混格式表（parquet+orc 混存）**：`inferFileFormatFromDataFiles` 取"第一个 task"的格式，`ParallelIterable`
  多线程下"第一个"本就不确定 → legacy 该值对混格式表**本就是任意的**；缓存把它固定成每快照一个确定值。
  scan 级值只选 V1/V2，per-file 格式仍逐 range 走，混格式表**读取正确性不受影响**（correctness-中性）。
  - **R2（红队精确化）**：这是**可复现性(reproducibility)的改变**，不是纯"更稳"。legacy 每条查询重掷、缓存每快照定一次；
    若 BE 存在"仅当混格式表 scan 级取到 ORC 时才触发"的潜伏 bug，其表现会从"间歇"变成"按快照恒发/恒不发"。
    correctness 不变，但把它当**可测试性变化**记账，不当卖点。
- **时间旅行**：键用 `table.currentSnapshot().snapshotId()`（= 推断真正读的快照），非 handle 的 pin 快照；
  与 legacy「格式恒从 currentSnapshot 推」一致；不同 pin 的时旅查询正确共享同一 currentSnapshot 派生的格式。
- **失败可重试**：抛异常的 loader 不入缓存 → 瞬时失败下条重试，不粘住错误默认。

## Implementation Plan（最小改动，逐文件）

1. 新建 `IcebergFormatCache.java`（≈ 130 行，镜像 `IcebergPartitionCache`，值 `String`）。
2. `IcebergWriterHelper.java`：+ import `Snapshot`、`TableIdentifier`；加 cache-aware `getFileFormat` 重载
   + `resolveFileFormatName` 重载 + 拆 `inferFirstDataFileFormat`（透传）/`inferFileFormatFromDataFiles`（吞→委派）。
3. `IcebergScanPlanProvider.java`：+ 字段 + ctor 参 + 改 1330 调用点。
4. `IcebergConnector.java`：+ 字段/构造/三个 invalidate/getScanPlanProvider 传参/`formatCacheForTest`。

## Test Plan

- **单测 `IcebergFormatCacheTest`**（镜像 `IcebergPartitionCacheTest`）：TTL 内命中同键 loadCount=1、
  不同 snapshotId 不同键、`ttl<=0`/负值关缓存、`invalidate`(清该表所有快照)/`invalidateDb`/`invalidateAll`、
  抛异常 loader 不入缓存（失败可重试）。
- **度量守门（集成）** 在 `IcebergWriterHelperTest`（或 `IcebergPartitionUtilsTest` 同款 InMemoryCatalog 真表）：
  建**无 `write.format.default` 属性**的真表 → 重复 `getFileFormat(table, id, cache)` N 次 →
  `cache.loadCountForTest()==1`（跨查询恰 1 次远端推断），并与无缓存 `getFileFormat(table)` 结果 parity。
  **MUTATION**：不把 cache 接进调用点 → 每次重扫 → loadCount>1 → 红。
  另断言**属性已带格式**的表根本不进缓存（`size()==0`，属性路径不缓存）。
  - **R3（红队可选断言）**：首文件为不支持格式（如构造一条 `FileFormat.AVRO` 的 DataFile 条目）时，`getFileFormat`
    在 `getOrLoad` **之外**的 orc/parquet 映射处每次重抛（parity），但推断只跑一次 → 两次调用都抛且 `loadCount==1`。
- **连接器 gate + 失效 `IcebergConnectorCacheTest`**：plain/vended/session=user 三目录 `formatCacheForTest()`
  均非 null（无 gate）；REFRESH TABLE/DB/CATALOG 逐级清空。
- 全 iceberg 模块 UT 绿（parity，禁回归）+ checkstyle 绿。

## Risk

- **低**：连接器自包含改动，无 fe-core、无 thrift、无 BE。唯一行为差异是"混格式表 scan 级格式从任意→每快照稳定"，
  已论证为 parity-中性。失败不缓存守住可重试。V1/V2 scanner 选择值语义不变（仍是表真实格式，非 jni）。
