# fe-connector-iceberg 热路径重操作审计报告

日期：2026-07-17
问题类定义：见 `plan-doc/perf-heavy-op-hot-path-problem-class.md`（由 DORIS-27138 问题一 / PR #64134 泛化）
完整证据（全部调用链 + 两路验证意见）：`plan-doc/reviews/perf-audit-fe-connector-iceberg-2026-07-17-findings.json`

## 0. 方法与可信度

- 多 agent 对抗审计（55 agent）：7 个视角的 finder 并行扫描（per-split 循环放大 / 单链路重复 /
  隐藏重访问器 / per-split 序列化 / 缓存旁路 / 写提交路径 / stats-分区-MTMV 路径）
  → 33 条原始发现 → 去重 24 条 → 每条经 2 个独立对抗 verifier
  （一个专职反驳乘数与成本、一个专职猎找既有缓解机制）→ **23 条确认、1 条驳回、0 存疑**。
- 人工抽验：C1/C2/C6 的核心代码主张（`getColumnHandles:587` 远程 loadTable、
  `getScanNodeProperties:1300/1311` 二次 loadTable + planFiles 兜底、`IcebergCatalogOps:340` 裸委派、
  全仓 grep 无 CachingCatalog、`getOrLoadPropertiesResult` memoize + `convertPredicate:796` 失效点）
  均已由我逐行核对属实。
- 行号基于本分支 `catalog-spi-review-16` 当前工作树。

## 1. 结论速览（按严重度分层）

**总病灶一句话：新框架的 SPI 各入口"每次自己 loadTable / 自己扫"，而 legacy 有单一 source
持缓存 Table —— 同一信息在一次规划里被远程重取 3~7 次；另有 #64134 的 planFiles 兜底
在新框架以 per-query 乘数复活。**

| 级别 | 簇 | 一句话 | 乘数 × 成本 | 触发面 | 发现项 |
|---|---|---|---|---|---|
| **P0** | 簇1 无 Table 对象缓存 | 一次 SELECT 规划 **3~7 次远程 loadTable**（同一张表） | k≈3-7/查询 × metastore RPC + metadata.json | 所有 iceberg 查询 | C1 C4 C6 C10 C16 |
| **P0** | 簇3 分区视图无跨查询缓存 | 分区表**每个查询**在分析期做一次 PARTITIONS 元数据表扫描（O(全部 manifest) 远程 IO）；MTMV refresh 还要重复 4-6 次 | 1/查询 × O(manifests)；4-6/refresh | 所有分区 iceberg 表 | C7 C23 C22 |
| **P0** | 簇2 #64134 复活 | `file_format_type` 兜底走**整表 planFiles()**，每查询 1-2 次，且与 planScan 自己的枚举完全冗余 | 1-2/查询 × 整表 manifest 扫描 | 无 write-format 属性的表（迁移表等） | C2 C11 |
| **P1** | C9 | information_schema.tables / SHOW TABLE STATUS 循环内**每表一次远程 loadTable**（只为拿 comment） | N 表 × loadTable 串行 | BI 工具高频路径 | C9 |
| **P1** | C3 | REST vended-credentials 下 **每个 data file / delete file** 重建 StorageProperties + hadoop Configuration | O(N_files+N_deletes) × Conf 构建 | REST + vended credentials | C3 |
| **P1** | 簇4 manifest cache 旁路 | streaming 路径与 COUNT(*) 下推路径都**绕过已开启的 IcebergManifestCache**（恰是大表/元数据查询最需要的场景） | 每查询整套 manifest 远程重读 | manifest cache 开启时 | C17 C18 |
| **P1** | C20 | 一条 DML 写语句 3-5 次远程 load 同一张表 | k≈3-5/语句 | 所有 iceberg 写 | C20 |
| **P2** | C5 | streaming pump 逐 split 入队：每 split 重建 backend 候选集 + 锁往返（fe-core 框架层） | per-split × 本地 CPU/锁 | ≥1024 文件的大扫描 | C5 |
| **P2** | C8 | 同一组 WHERE conjunct 被转换 5-6 次/查询 | k≈5-6 × 本地 CPU | 带谓词查询 | C8 |
| **P2** | 簇5 per-split 不变量 | buildRange 逐 byte-slice 重算分区 JSON/identity map/delete 转换；v3 delete 双重 thrift 转换；通用节点造完即弃的路径解析；不变 payload 逐 range 重复（RPC 膨胀） | per-split × 本地 CPU/字节 | 大 split 数 / MOR 表 | C12 C13 C14 C15 |
| **P2** | 维护路径 | rewrite_data_files **每 group 一次整表 planFiles**（G+1 次）；expire_snapshots 逐 snapshot×逐 manifest 读、零去重（S×M） | G/次、S×M/次 × 远程 IO | EXECUTE 维护命令 | C19 C21 |

## 2. 各簇详述

### 簇1（P0）：无 Table 对象缓存 —— 一次规划 3~7 次远程 loadTable [C1 C4 C6 C10 C16]

**根因**：`IcebergCatalogOps.loadTable`（`IcebergCatalogOps.java:340`）是对 SDK
`catalog.loadTable()` 的裸委派——每次调用都是一次 metastore RPC（HMS getTable / REST GET）
+ metadata.json 对象存储读取。全仓无 `CachingCatalog` 包装（grep 验证）；
`IcebergLatestSnapshotCache` 只缓存 `(snapshotId, schemaId)` 二元组，**不缓存 Table 对象**。
于是 SPI 的每个入口各自 load：

一次带 WHERE 的同步查询的 loadTable 计数（C6/C16 独立核对一致）：

| # | 调用点 | 链路 |
|---|---|---|
| 1 | properties 计算①之 getColumnHandles | `FileQueryScanNode.init:139 → initSchemaParams:183 → PluginDrivenScanNode.getPathPartitionKeys:537 → getOrLoadPropertiesResult:1776 → buildColumnHandles:1780 → IcebergConnectorMetadata.getColumnHandles:587 → loadTable` |
| 2 | properties 计算①之 getScanNodeProperties | `getOrLoadPropertiesResult:1801 → IcebergScanPlanProvider.getScanNodeProperties:1300 → resolveTable:1981-1993 → loadTable` |
| 3-4 | properties 计算②（同上两次重来） | `convertPredicate:795-798` **无条件**清空 `cachedPropertiesResult` → `createScanRangeLocations:325 getFileFormatType` 触发整体重算（见 C1） |
| 5 | batch-mode 探测 | `FileQueryScanNode:376 isBatchMode → computeBatchMode:1414 → streamingSplitEstimate:410 resolveTable`（顺带再读一次 manifest-list，C4） |
| 6 | getSplits 再取列句柄 | `PluginDrivenScanNode.getSplits:1202 → buildColumnHandles → getColumnHandles:587` |
| 7 | planScan 正主 | `planScanInternal:562 resolveTable` |

**放大细节（C1）**：`convertPredicate` 的失效是**一揽子**的——而 iceberg 连 `applyFilter`
override 都没有（谓词对 properties 的唯一影响是 pushdown-predicates 一个 prop），
loadTable、format 解析、schema 字典 thrift+base64 编码、凭证 overlay 全部是 filter 不变量，
却整体重算一遍。

**影响**：每查询 +3~6 次冗余远程元数据往返（单次 50-300ms 量级）≈ 规划延迟 +0.2~1.5s，
点查/小查询被规划延迟支配；metastore/REST 服务 QPS 被放大 3~7 倍。
legacy 对照：单一 source 缓存 Table，一次规划 1 次命中。

**修复方向**（verifier 建议汇总）：以 `beginQuerySnapshot` 已有的 pin 为天然 key，
做 **per-planning-pass 的 Table memo**（`(TableIdentifier, pinnedSnapshotId)` 键，
挂在长生命周期的 `IcebergConnector` 上或随 handle 传递），让
`getColumnHandles / resolveTable / streamingSplitEstimate / planScan` 共享一次 load；
同时把 `convertPredicate` 的失效收窄到真正依赖 conjunct 的 prop。

### 簇2（P0）：#64134 原型在新框架复活 [C2 C11]

**这是 JIRA 问题一的直系再现**。`getScanNodeProperties` 发 scan 级 `file_format_type`
（BE 靠它选 V1/V2 scanner，不能不发）：

```
PluginDrivenScanNode.getOrLoadPropertiesResult:1801
  → IcebergScanPlanProvider.getScanNodeProperties:1311
      → IcebergWriterHelper.getFileFormat(table):265
          → resolveFileFormatName:277（两个属性都未设时）
              → inferFileFormatFromDataFiles:287
                  → table.newScan().planFiles():291   ← 无过滤整表 manifest 扫描
```

- **乘数**：每次 properties 计算 1 次 = 无谓词查询 1 次/查询、带谓词 2 次/查询
  （C1 的双计算叠加）。对比旧 bug 的 per-split，乘数降了，但**每条 SELECT/EXPLAIN 都要付**。
- **纯冗余**：planScan 自己的 planFiles 枚举里 `dataFile.format()` 每个文件都有——
  为了拿"第一个文件的格式"专门再扫一遍整表。
- **门槛**：表属性无 `write-format` 且无 `write.format.default`。不只迁移表——
  任何写入引擎从不显式设该属性的表都中（iceberg 的 parquet 默认值是读侧约定，不落属性）。
- **修复方向**：按 `(table UUID, snapshotId)` memoize 解析结果（快照不变 ⇒ 格式不变），
  或从 split 枚举结果反推 scan 级 format 传下去；配合簇1 的失效收窄消掉第二次。

### 簇3（P0）：分区视图每查询重建，无跨查询缓存 [C7 C23 C22]

**根因**：分析期 `BindRelation.getLogicalPlan:733 → StatementContext.loadSnapshots:988-998
→ PluginDrivenMvccExternalTable.loadSnapshot:343 → materializeLatest:126` 对分区表走
`getMvccPartitionView / listPartitions → IcebergPartitionUtils.loadRawPartitions:709-718`
= PARTITIONS 元数据表 `planFiles()` + `rows()`——SDK 聚合时要读该快照**全部** data+delete
manifest。`StatementContext` 只在**单语句内** memoize；跨查询无任何缓存
（legacy 的二级分区缓存在 CACHE-P1 决策中被有意放弃——该决策的代价在这里显形）。

- **影响**：数千 manifest 的分区表：每条查询（哪怕 WHERE 全命中裁剪）在拿到 scan node
  之前先付 MB~GB 级 manifest 读 + 秒级延迟；随表历史规模而非查询选择性增长。
  与簇1 叠加后很可能是分区大表规划延迟的第一大头。
- **MTMV 放大（C22）**：一次 refresh 流程里 `isValidRelatedTable / alignMvPartition /
  generateRelatedPartitionDescs / getAndCopyPartitionItems...` 各自 materialize 同一视图
  4-6 次（无 refresh 级 pin），还引入枚举点之间的快照偏移风险。
- **修复方向**：按 `(TableIdentifier, snapshotId)` 缓存分区视图（pin 在
  `beginQuerySnapshot` 后已在 handle 上，key 天然可用），挂 fe-connector-cache；
  MTMV 侧在 `MTMVRefreshContext` 加 refresh 级 MvccSnapshot pin。

### C9（P1）：information_schema.tables 循环内每表 loadTable

`FrontendServiceImpl.listTableStatus:719 for (TableIf table : tables)` →
`:755 setComment(table.getComment())`（**无条件**，不看请求是否需要 comment 列）→
`PluginDrivenExternalTable.getComment:944 → IcebergConnectorMetadata.getTableComment:305 →
loadTable`。N 张表 = N 次串行远程 load：几百张表的库一条
`SELECT * FROM information_schema.tables` 要几十秒到分钟级，且 BI 工具高频触发。
教科书级"伪装成轻访问器"（getComment ← 远程 IO）。
**修复**：建表/schema-cache 时捕获 comment 随表对象缓存，或该路径按需惰性取。

### C3（P1）：vended-credentials 每文件重建 StorageProperties

`buildRange:1105 normalizeUri` / `convertDelete:1157` → `DefaultConnectorContext.normalizeStorageUri:392-409 → buildVendedStorageMap:225-242 → StorageProperties.createAll`（遍历所有 provider + 构建 hadoop `Configuration` + 逐 key set）——**每个 data file 和每个 delete file 各一次**，而 vended token 在整个 scan 内不变。50k 文件 ≈ 数十秒纯 FE CPU。门槛：REST + `iceberg.rest.vended-credentials-enabled=true`（MOR 表加倍）。
**修复**：token→typed-map 的推导按 scan 提升/在 `DefaultConnectorContext` 内做单条目 memo（token 恒等键）。

### 簇4（P1）：IcebergManifestCache 旁路 [C17 C18]

manifest cache（`meta.cache.iceberg.manifest.enable`，默认 off）只接在同步路径
`planFileScanTask:1681`（gate `isManifestCacheEnabled:1832`）上：

- **C17**：文件数 ≥ `num_files_in_batch_mode`（默认 1024）时走 streaming
  `streamSplits:449 → scan.planFiles():463`——**恰好把 cache 瞄准的大表全体踢出缓存**
  （legacy 在 batch 模式下仍走 cache）。
- **C18**：COUNT(*) 下推 `planScanInternal:594-598 → planCountPushdown:1021 →
  scan.planFiles():1024` 在到达 cache 分支之前 return——元数据即可回答的查询反而全量远程读
  manifest（且 `ParallelIterable` 会激进提交所有 manifest 读任务，虽然只要第一个 task）。

**修复**：cache 开启时 streaming 判定返回 -1 退回同步物化路径（一行 legacy-parity 修复）；
count 分支改走 `planFileScanTask`。

### C20（P1）：写路径 3-5 次 load 同一张表

`PhysicalIcebergMergeSink.getRequirePhysicalProperties:161→198`、
`PhysicalPlanTranslator.visitPhysicalConnectorTableSink:675,703`、
`PluginDrivenTableSink.bindDataSink:175` → `IcebergWritePlanProvider.resolveTable:689-702` /
`beginWrite`（内含 tableExists ×2 + 无条件 refresh）。每条 DML +3~5 次串行远程往返。
**修复**：语句级 resolve 一次传递；exists 从 load 结果推导。

### P2 组（CPU/payload/维护路径，影响门槛高但模式典型）

- **C5**（fe-core 框架层，惠及所有连接器）：`startStreamingSplit:1638-1642` 逐 split
  `addToQueue` → `SplitAssignment` 每 split 重建 backend 候选集
  （`FederationBackendPolicy.computeScanRangeAssignment:225-235` 全量 backend 拷贝 +
  shuffle + multimap）+ `synchronized` 往返。10⁵-10⁶ split × ~100 BE ≈ 10⁷-10⁸ 冗余操作。
  修复：pump 侧微批（64-256 个/批）。
- **C8**：同一组 conjunct 在 `buildRemainingFilter`（×3 处）+ `buildScan:974` +
  `getScanNodeProperties:1428`（EXPLAIN 专用序列化，**非 EXPLAIN 也跑**）被转换 5-6 次。
  单次微秒~毫秒级，但与簇1 同源叠加。修复：node 字段 memo，与 `cachedPropertiesResult`
  同点失效。
- **C12**：一个 data file 被 `TableScanUtil.splitFiles` 切成 k 个 byte-slice 后，
  `buildRange:1045` 对每个 slice 重算 partition JSON（Jackson 序列化 + 时区格式化）、
  identity map、delete 转换——(specId, PartitionData) 级不变量。100k split ≈ 0.5-2s CPU。
- **C13**：v3 rewritable-delete stash：同一 delete 列表 plan 期 `rewritableDeleteDescs:302-313`
  转一次 thrift，`populateRangeParams` 再转一次；每 slice 重复 put 相同 supply。
- **C14**：通用节点 per-split 重复 `LocationPath.of`（URLEncoder+URI.create）、
  重建 columns-from-path 却被 `IcebergScanRange.populateRangeParams:435-437` unset 丢弃；
  `resolveScanProvider` 每 split 经 `getFileCompressType` 反复解析。
- **C15**（payload 版放大）：同一 data file 的完整 delete 列表 + partition JSON 复制进
  **每个** byte-slice 的 `TFileRangeDesc`；共享 delete file 逐 data file 重复。大 MOR 扫描
  计划体积多出 MB 级（FE 构建 + BE 解析双向付费）。
- **C19**：`ConnectorRewriteDriver.run STEP3:143` 每个 rewrite group 调一次
  `registerRewriteSourceFiles` → 每次一遍 `useSnapshot(...).planFiles():379-383`。
  G 个 group = G+1 次整表扫描，G~50-200 时分钟级。修复：union 所有 group 一次注册
  （SPI 本来就收 `Set<String>`）。
- **C21**：`IcebergExpireSnapshotsAction.buildDeleteFileContentMap:271-293` 对**每个** snapshot
  读其全部 delete manifest，无 visited-path 去重——相邻 snapshot 的 manifest 大量重叠，
  S×M 串行远程读。按 `ManifestFile.path()` 去重即坍缩为 O(distinct)。

## 3. 驳回项与旁获发现

- **驳回 R1**："字典新鲜度 poll 每 5s 重建分区视图"——重操作属实，但乘数驱动不成立：
  `CreateDictionaryInfo.validateAndSet:164` 把源表强转 `org.apache.doris.catalog.Table`，
  而 ExternalTable 只实现 `TableIf` ⇒ **对外表 CREATE DICTIONARY 在校验期就
  ClassCastException**，poll 路径不可达。
  ⚠️ 旁获：这本身可能是一个功能缺口/待修的强转 bug（外表字典完全不可用），与本审计无关，
  单独记录。
- 去重合并说明：33→24 的合并里,同根因跨 lens 的发现（如 loadTable 簇）被保留为独立条目
  以互为佐证，报告中按簇归并陈述。

## 4. 结构性观察（为什么会长出这一类问题）

1. **SPI 入口各自为政取 Table**：`ConnectorMetadata` / `ScanPlanProvider` / 写侧 provider
   的每个方法都独立 `resolveTable/loadTable`，接口上没有"本次规划已解析的 Table"这一共享
   载体；legacy 靠 source 对象天然持有。→ 修复应该是框架级的（per-planning-pass 载体或
   connector 级 snapshot-keyed cache），否则每加一个 SPI 方法就多一次 load。
2. **成本契约缺失复刻了 #64134 的成因**：`getFileFormat / getComment / getScanNodeProperties`
   这类名字读不出"远程 IO"；调用方（含 fe-core 通用节点）按 O(1) 使用。审计确认的 23 条里
   过半是这个模式。建议在 SPI javadoc 里显式标注成本等级（cheap/loaded-metadata/remote-IO），
   新代码 review 按此把关。
3. **失效粒度过粗**：`convertPredicate` 一揽子清缓存是"正确但昂贵"的懒办法，直接把簇1、
   簇2 的成本翻倍。
4. **缓存修在旧路径上**：manifest cache 只挂在同步 materialize 路径，streaming/count 两条
   新路径没接——加新路径时没有"必须过一遍既有缓存清单"的动作。

## 5. 建议的修复优先级（供 review 讨论，非结论）

1. per-planning-pass Table memo（簇1，一并收窄 convertPredicate 失效）——单点改动收益最大；
2. 分区视图 `(table, snapshotId)` 缓存 + MTMV refresh pin（簇3）；
3. `file_format_type` 兜底 memoize / 从枚举反推（簇2）；
4. manifest cache 两条旁路接回（簇4，其中 C17 有一行式 legacy-parity 修法）；
5. C9 / C3 / C20 各自局部 hoist；
6. P2 组择机随重构顺手处理（C19/C21 改动极小收益明确，可先行）。

—— 以上待 review。任何一条立项前建议先按 findings.json 里的调用链复核一遍行号。
