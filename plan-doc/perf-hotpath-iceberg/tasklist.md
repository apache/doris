# Task List — fe-connector-iceberg 热路径重操作修复

> **唯一进度清单**。每完成一项，随 commit 把对应行改 `[x]` 并在总览表填状态/commit。
> ID 一旦分配**永不复用**；删除的任务标 `[deleted YYYY-MM-DD <原因>]` 保留占位。
> 权威分析（别在本文件复制）：审计报告 [`../reviews/perf-audit-fe-connector-iceberg-2026-07-17.md`](../reviews/perf-audit-fe-connector-iceberg-2026-07-17.md)
> · 证据 JSON [`../reviews/perf-audit-fe-connector-iceberg-2026-07-17-findings.json`](../reviews/perf-audit-fe-connector-iceberg-2026-07-17-findings.json)
> · 问题类 [`../perf-heavy-op-hot-path-problem-class.md`](../perf-heavy-op-hot-path-problem-class.md)。
> 每项立项流程、约束铁律、验收口径见 [`README.md`](./README.md) —— **动码前必读**。

## 总览（at-a-glance）

- 覆盖：审计 **23 条确认发现（C1–C23）** → **一簇一任务**归并为 **11 个可提交修复任务**（按审计 §1 总览表行边界：簇级并成一项，未成簇的独立发现各一项）。1 条驳回（R1，见报告 §3）不立项。
- **推荐顺序 = 审计 §5 优先级**（下表自上而下）。P0 收益最大先做；PERF-08（维护路径 C19/C21）改动小、收益明确，可作热身先行插队。
- 状态图例：⏳ 待启动 · 🚧 进行中 · ✅ 完成 · ❌ 阻塞（备注写原因） · 🔬 复核中（re-verify 发现行号/乘数已变，待重判）

| ID | 优先级 | 覆盖发现 | 主题（一句话） | 依赖 | 状态 | commit |
|---|---|---|---|---|---|---|
| PERF-01 | P0 | C1 C4 C6 C10 C16 | 一次规划 3~7 次远程 loadTable → 胖 handle(查询内单实例)+ 跨查询 IcebergTableCache(挂 Connector);~~convertPredicate 收窄~~已删(红队证伪) | — | ✅ 完成 | `484f0e0c125` |
| PERF-02 | P0 | C7 C22 C23 | 分区视图每查询重扫 PARTITIONS 元数据表 → `(table,snapshotId)` 缓存(连接器侧,无凭证 gate);MTMV refresh pin 判定为多余(靠 latestSnapshotCache 稳定快照坍缩,不改 fe-core) | 与 01 共享快照 pin 机制 | ✅ 完成 | `518d0599cbf` |
| PERF-03 | P0 | C2 C11 | #64134 复活：`file_format_type` 兜底走整表 planFiles → 跨查询 `(table,snapshotId)` memoize(连接器侧,无 gate);~~从枚举反推~~已否决(getFileFormatType 早于 planScan + 无过滤 vs 带谓词破 parity) | 与 01/02 共享快照 pin | ✅ 完成 | `0b96f2e6c78` |
| PERF-04 | P1 | C17 C18 | streaming / COUNT(*) 下推旁路 IcebergManifestCache → 抽惰性 `cacheBackedFileScanTasks`(delete 索引 eager + data manifest 惰性扁平映射)三处复用;缓存与不 OOM 兼得(否决审计"退回物化"因重引 OOM) | — | ✅ 完成 | `2e5f393779c` |
| PERF-05 | P1 | C9 | information_schema.tables 每表 loadTable 取 comment → 复核缩小(普通目录 PERF-01 已覆盖);补**无 gate** `IcebergCommentCache` **仅 vended 非 session=user**(红队:session=user 缓存绕过 per-user 授权=泄漏) | — | ✅ 完成 | `aea3ebdd40e` |
| PERF-06 | P1 | C3 | REST vended-cred 每 data/delete file 重建 StorageProperties+Configuration → scan 级 memo | — | ⏳ | |
| PERF-07 | P1 | C20 | 一条 DML 3~5 次 load 同表 → 语句级 resolve 一次传递 | — | ⏳ | |
| PERF-08 | P2 | C19 C21 | 维护路径逐单位重扫 / 无去重（rewrite_data_files 每 group planFiles；expire_snapshots S×M）→ union 一次注册 / 按 path 去重 | 改动小可先行 | ⏳ | |
| PERF-09 | P2 | C5 | **（fe-core 框架层）** streaming pump 逐 split 重建 backend 候选集 + 锁往返 → 微批 | ⚠ 跨连接器，见约束 | ⏳ | |
| PERF-10 | P2 | C8 | 同组 WHERE conjunct 被转换 5~6 次/查询 → node 字段 memo（与 01 同点失效） | 与 01 同源 | ⏳ | |
| PERF-11 | P2 | C12 C13 C14 C15 | per-split 不变量重算 + payload 逐 slice 复制 → hoist / (specId,PartitionData) memo / per-file 共享 | 同区域批量 · ⚠ 含 fe-core 通用节点 | ⏳ | |

---

## P0 — 无缓存/兜底类，触发面=所有 iceberg 查询

### [x] PERF-01 — 簇1：胖 handle(查询内) + 跨查询 IcebergTableCache（C1 C4 C6 C10 C16） · ✅ `484f0e0c125`
- **病灶**：`IcebergCatalogOps.loadTable:340` 是对 SDK `catalog.loadTable()` 的裸委派（每次 = metastore RPC + metadata.json 读）；全仓无 `CachingCatalog`，`IcebergLatestSnapshotCache` 只存 `(snapshotId,schemaId)` 不存 Table。SPI 各入口各自 load ⇒ 一次带 WHERE 的查询 **3~7 次** loadTable（7 个调用点见报告簇1 表）。放大器 = `convertPredicate:795-798` **无条件**清空 `cachedPropertiesResult` ⇒ 整份 properties（loadTable/format/schema thrift+base64/凭证 overlay，全是 filter 不变量）第二次重算。
- **修复方向**：以 `beginQuerySnapshot` 已有的 pin 为天然 key，做 per-planning-pass 的 **Table memo**（键 `(TableIdentifier, pinnedSnapshotId)`，挂长生命周期 `IcebergConnector` 或随 handle 传递），让 `getColumnHandles / resolveTable / streamingSplitEstimate / planScan` 共享一次 load；同时把 `convertPredicate` 失效**收窄**到真正依赖 conjunct 的 prop（谓词对 properties 的唯一影响仅 pushdown-predicates 一个 prop）。
- **收益**：每查询 −3~6 次远程往返 ≈ 规划延迟 −0.2~1.5s；metastore/REST QPS 除以 3~7。
- **约束**：memo 放**连接器侧**，**不得**在 fe-core 加 Table 缓存或属性派生（见 README 铁律）。
- **依赖**：无。**建议第一个做** —— 收益最大，且其失效收窄是 PERF-03、PERF-10 的前置。

### [x] PERF-02 — 簇3：分区视图跨查询缓存（C7 C22 C23） · ✅ `518d0599cbf`
- **病灶**：分析期 `loadSnapshot → materializeLatest:126` 对分区表走 `IcebergPartitionUtils.loadRawPartitions:709-718` = PARTITIONS 元数据表 `planFiles()+rows()`（SDK 聚合读该快照**全部** data+delete manifest）。`StatementContext` 只在单语句内 memoize，跨查询零缓存（legacy 二级分区缓存在 CACHE-P1 决策中被有意放弃）。MTMV 放大（C22）：一次 refresh 里 `isValidRelatedTable/alignMvPartition/generateRelatedPartitionDescs/getAndCopyPartitionItems` 各 materialize 同视图 **4~6 次**（无 refresh 级 pin，且引入枚举点间快照偏移风险）。
- **修复方向**：按 `(TableIdentifier, snapshotId)` 缓存分区视图（pin 在 `beginQuerySnapshot` 后已在 handle 上），挂 `fe-connector-cache`；MTMV 侧在 `MTMVRefreshContext` 加 refresh 级 `MvccSnapshot` pin。
- **依赖**：与 PERF-01 共享 `(table, snapshotId)` 快照 pin —— 先做 01 立住模式，02 复用。

### [x] PERF-03 — 簇2：#64134 planFiles 兜底复活（C2 C11） · ✅ `0b96f2e6c78`
- **病灶**：`getScanNodeProperties → IcebergWriterHelper.getFileFormat → resolveFileFormatName → inferFileFormatFromDataFiles → table.newScan().planFiles()`（无过滤整表 manifest 扫描）。门槛=表属性无 `write-format` 且无 `write.format.default`（迁移表 + 任何写引擎从不显式设该属性的表）。node 级已 memoize ⇒ 查询内 1 次，但跨查询零缓存 ⇒ 每查询/EXPLAIN 一遍整表扫描。
- **落地**：新增连接器侧 `IcebergFormatCache`（键 `(TableIdentifier, currentSnapshotId)`、值格式名 `String`、**无凭证 gate**），只缓存推断兜底（属性探测不缓存），失败不入缓存（loader 透传 unchecked），映射/抛点在 `getOrLoad` 之外。写路径 7 调用点不动（留 PERF-07）。
- **~~从枚举反推~~已否决**：`createScanRangeLocations` 先 `getFileFormatType`(line 325) 后 `getSplits/planScan`(line 422)，格式须先于数据扫描给出；且推断无过滤 vs planScan 带谓词，剪空时反推破 parity；改时序须动 fe-core（违反铁律）。
- **收益**：无格式属性表每查询整表推断 → 每快照一遍（跨查询命中）。度量守门 `loadCountForTest()==1`。全模块 957 UT 绿。

---

## P1 — 局部旁路/局部 hoist，触发面较窄或特定场景

### [x] PERF-04 — 簇4：IcebergManifestCache 两条旁路接回（C17 C18） · ✅ `2e5f393779c`
- **病灶**：manifest cache（`meta.cache.iceberg.manifest.enable`，默认 off）只接在同步物化路径。**C17**：≥`num_files_in_batch_mode` 的大表走 streaming `streamSplits → scan.planFiles()`（SDK 裸扫，恰把 cache 目标大表踢出）。**C18**：COUNT(*) 下推 `planCountPushdown → scan.planFiles()` 只为取一个占位文件（行数来自快照摘要），也不走 cache 且 `ParallelIterable` 全提交。
- **复核否决审计"一行修复"**：核 legacy(`6fef25709d3^`) batch 模式确走 cache **但一次性物化整表**（无 OOM 保护）；"退回物化"会重引 legacy 就有的 OOM 风险。用户拍板方向 = **惰性+缓存兼得**。
- **落地**：抽惰性 `cacheBackedFileScanTasks`（delete 索引 eager + data manifest 惰性扁平映射，产整文件任务不攒整表），三处复用（同步物化它保启发式切片；流式喂它保 OOM 安全+固定切片+现在命中缓存；COUNT 迭代取首文件惰性早停）。决策不改；失败 `catch(Exception)` 退 SDK；`statsQueryId` 可空(流式 null 消跨线程 stats 竞争)。红队 7 攻击核心 sound，采纳 catch 类型/线程/测试 3 修正。
- **收益**：开缓存大表流式从 0 命中→命中且不 OOM；COUNT 从全提交→首文件即停。全模块 962 UT 绿。

### [x] PERF-05 — C9：information_schema.tables 每表 loadTable 取 comment · ✅ `aea3ebdd40e`
- **病灶**：`FrontendServiceImpl.listTableStatus` fe-core 循环每表**无条件** `getComment` → 连接器 `getTableComment` → 每表一次 loadTable 只为读 `comment`。N 表 = N 串行远端 load，BI 高频。
- **复核缩小（审计早于 PERF-01）**：`getTableComment` 现走 `resolveTableForRead → IcebergTableCache` → **普通目录重复查询已命中**；残余 = 凭证 gated 目录（tableCache=null）。
- **落地**：无 gate `IcebergCommentCache`（键 TableIdentifier、值 comment String），**仅 vended 且非 session=user 时建**。**红队 HIGH**：`tableCache==null` 做 gate 会卷入 session=user，其授权在 per-user loadTable 里、缓存绕过=元数据泄漏 → 收窄为 vended-only。首次 N load / view 未缓存记为诚实局限。
- **收益**：vended 目录重复 information_schema 从次次 N load→命中；普通/session=user 不变。全模块 973 UT 绿。

### [ ] PERF-06 — C3：vended-credentials 每文件重建 StorageProperties
- **病灶**：`buildRange:1105 normalizeUri` / `convertDelete:1157 → DefaultConnectorContext.normalizeStorageUri:392-409 → buildVendedStorageMap:225-242 → StorageProperties.createAll`（遍历所有 provider + 建 hadoop `Configuration` + 逐 key set），**每 data file 和每 delete file 各一次**，而 vended token 整个 scan 内不变。50k 文件 ≈ 数十秒纯 FE CPU。门槛：REST + `iceberg.rest.vended-credentials-enabled=true`（MOR 加倍）。
- **修复方向**：token→typed-map 推导按 scan 提升 / 在 `DefaultConnectorContext` 内做单条目 memo（token 恒等键）。

### [ ] PERF-07 — C20：写路径 3~5 次 load 同表
- **病灶**：`PhysicalIcebergMergeSink.getRequirePhysicalProperties:161→198`、`PhysicalPlanTranslator.visitPhysicalConnectorTableSink:675,703`、`PluginDrivenTableSink.bindDataSink:175 → IcebergWritePlanProvider.resolveTable:689-702 / beginWrite`（内含 tableExists×2 + 无条件 refresh）。每条 DML +3~5 次串行远程往返。
- **修复方向**：语句级 resolve 一次传递；exists 从 load 结果推导。

---

## P2 — CPU/payload/维护路径，影响门槛高但模式典型

### [ ] PERF-08 — 维护路径逐单位重扫/无去重（C19 C21）  ·改动小、可先行插队
> 覆盖两条独立维护命令，同一"逐单位重扫、零去重"模式。同一任务，实现上可各自独立 commit。
- **C19 rewrite_data_files**：`ConnectorRewriteDriver.run STEP3:143` 每个 rewrite group 调一次 `registerRewriteSourceFiles` → 每次一遍 `useSnapshot(...).planFiles():379-383`。G 个 group = G+1 次整表扫描，G~50-200 时分钟级。修：**union 所有 group 一次注册**（SPI 本来就收 `Set<String>`）。
- **C21 expire_snapshots**：`IcebergExpireSnapshotsAction.buildDeleteFileContentMap:271-293` 对**每个** snapshot 读其全部 delete manifest、无 visited-path 去重 —— 相邻 snapshot manifest 大量重叠，S×M 串行远程读。修：按 `ManifestFile.path()` 去重坍缩为 O(distinct)。

### [ ] PERF-09 — C5：streaming pump 逐 split 重建 backend 候选集（**fe-core 框架层**）
- **病灶**：`startStreamingSplit:1638-1642` 逐 split `addToQueue` → `SplitAssignment` 每 split 重建 backend 候选集（`FederationBackendPolicy.computeScanRangeAssignment:225-235` 全量 backend 拷贝 + shuffle + multimap）+ `synchronized` 往返。10⁵~10⁶ split × ~100 BE ≈ 10⁷~10⁸ 冗余操作。
- **修复方向**：pump 侧微批（64~256 个/批）。
- **约束**：⚠ 改的是 **fe-core 通用框架**（惠及所有连接器，非 source-specific，允许改）；但属共享热路径，须证 **byte + cost 对所有连接器双不变**（对齐现有"共享 MVCC 方法须双不变"纪律）。

### [ ] PERF-10 — C8：WHERE conjunct 转换 5~6 次/查询
- **病灶**：同组 conjunct 在 `buildRemainingFilter`（×3 处）+ `buildScan:974` + `getScanNodeProperties:1428`（EXPLAIN 专用序列化，**非 EXPLAIN 也跑**）被转换 5~6 次。单次微秒~毫秒级，但与簇1 同源叠加。
- **修复方向**：node 字段 memo，与 `cachedPropertiesResult` 同点失效。
- **依赖**：与 PERF-01 同源，宜在 01 之后顺手。

### [ ] PERF-11 — 簇5：per-split 不变量 + payload 放大（C12 C13 C14 C15）
> 同一 `buildRange`/`populateRangeParams` 区域的 per-split 重复计算与 payload 放大，作为一个任务批量处理（可拆多个 commit）。
- **C12**：一个 data file 被 `TableScanUtil.splitFiles` 切成 k 个 byte-slice 后，`buildRange:1045` 对每个 slice 重算 partition JSON（Jackson 序列化 + 时区格式化）、identity map、delete 转换 —— (specId, PartitionData) 级不变量。100k split ≈ 0.5~2s CPU。修：按 (specId, PartitionData) memo。
- **C13**：v3 rewritable-delete stash：同一 delete 列表 plan 期 `rewritableDeleteDescs:302-313` 转一次 thrift，`populateRangeParams` 再转一次；每 slice 重复 put 相同 supply。修：转一次复用、per-file 而非 per-slice。
- **C14**：通用节点 per-split 重复 `LocationPath.of`（URLEncoder+URI.create）、重建 columns-from-path 却被 `IcebergScanRange.populateRangeParams:435-437` unset 丢弃；`resolveScanProvider` 每 split 经 `getFileCompressType` 反复解析。修：hoist 不变量 / 删造完即弃分支。⚠ 涉 **fe-core 通用节点**——保持 connector-agnostic，勿加 source-specific 分支。
- **C15**（payload 放大）：同一 data file 的完整 delete 列表 + partition JSON 复制进**每个** byte-slice 的 `TFileRangeDesc`；共享 delete file 逐 data file 重复。大 MOR 扫描计划体积多出 MB 级（FE 构建 + BE 解析双向付费）。修：per-file 共享引用，不逐 slice 复制 payload。

---

## 不立项（记录）

- **R1（驳回）**：字典新鲜度 poll 每 5s 重建分区视图 —— 重操作属实但乘数不成立（`CreateDictionaryInfo.validateAndSet:164` 对外表强转 `catalog.Table` ⇒ CREATE DICTIONARY 在校验期即 ClassCastException，poll 路径不可达）。详见报告 §3。
- **旁获（与本审计无关，另行处理）**：上述强转本身是功能缺口/潜在 bug（外表字典完全不可用）。**不在本任务空间**，如需修单开任务。
