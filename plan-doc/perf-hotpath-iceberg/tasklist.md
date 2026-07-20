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
| PERF-06 | P1 | C3 | REST vended-cred 每 data/delete file 重建 StorageProperties+Configuration → scan 级「准备一次、逐文件套用」normalizer(SPI seam+fe-core override,连接器侧 hoist);~~fe-core 框架 memo~~否决(per-catalog 跨查询共享→并发冲刷退化+凭证保留贴红线) | — | ✅ 完成 | `6294edf2833` |
| PERF-07 | P1 | C20 | 一条 DML 重复 load 同表 → 每语句「表加载归属者」(`ConnectorStatementScope` 挂 StatementContext) 读写共享一次加载；拆胖句柄；delete stash 下沉到每语句作用域(删单例+类)；beginWrite 取共享表 | — | ✅ 完成 | `97bdcd6bdbe`+`ea7fd1f6e7a` |
| PERF-08 | P2 | C19 C21 | 维护路径逐单位重扫 / 无去重（rewrite_data_files 每 group planFiles；expire_snapshots S×M）→ union 一次注册 / 按 path 去重 | 改动小可先行 | ✅ 完成 | `89cc39c8d88`(C19)+`be0035eff62`(C21) |
| PERF-09 | P2 | C5 | **（fe-core 框架层）** streaming pump 逐 split 重建 backend 候选集 + 锁往返 → 微批 | ⚠ 跨连接器，见约束 | ⏳ | |
| PERF-10 | P2 | C8 | 同组 WHERE conjunct 被转换 5~6 次/查询 → node 字段 memo（与 01 同点失效） | 与 01 同源 | 🔬 暂缓（复核判低价值） | |
| PERF-11 | P2 | C12 C13 C14 C15 | per-split 不变量重算 + payload 逐 slice 复制 → hoist / (specId,PartitionData) memo / per-file 共享 + 通用节点 provider memo | 同区域批量 · ⚠ 含 fe-core 通用节点 | 🚧 部分完成（C12+C15a+C13-plan+C14-provider） | `10b7d29423f`+`87ff73b1a95`(C14-provider) |

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

### [x] PERF-06 — C3：vended-credentials 每文件重建 StorageProperties · ✅ `6294edf2833`
- **病灶**：`buildRange normalizeUri` / `convertDelete` / `buildPositionDeleteRange` → `DefaultConnectorContext.normalizeStorageUri → buildVendedStorageMap → StorageProperties.createAll`（遍历所有 provider + 建 hadoop `Configuration` + 逐 key set），**每 data file 和每 delete file 各一次**，而 vended token 整个 scan 内不变（同一 Map 实例、贵活纯 token 派生与 URI 无关）。50k 文件 ≈ 数十秒纯 FE CPU。门槛：REST + `iceberg.rest.vended-credentials-enabled=true`（MOR 加倍）。
- **路线决策（用户 2026-07-18 拍板 route A）**：连接器侧「准备一次、逐文件套用」——新增 SPI `ConnectorContext.newStorageUriNormalizer(token)`（默认逐文件折回、零影响其它连接器），`DefaultConnectorContext` override 惰性 memo 一次派生。**否决 fe-core 框架 memo**：`DefaultConnectorContext` per-catalog 跨查询共享，单条目 memo 并发冲刷退化 + 凭证跨查询保留贴红线；scan 级作用域天然规避（对齐 Trino 每 scan 建一次 FileSystem）。
- **落地**：三处 scan-start（同步/流式/position_deletes）构造一次 normalizer 往下传，六个 per-file seam 参数由 token 换成 normalizer；`TcclPinning` 透传拿到提升；写路径 `getBackendFileType`（O(1)/写）不动。惰性 memo 对齐原逐文件版空URI短路/vended覆盖/坏路径 fail-loud/异常时机。
- **收益/守门**：一次 vended scan 内 `buildVendedStorageMap`/`createAll` 从 O(N_files+N_deletes)→1。连接器守门测试断言 3 文件 scan `newNormalizerCount==1` 且 `normalizeCount==3`；fe-core parity 测试断言逐字等价。iceberg 974 UT 绿 + fe-core NormalizeUriTest 13 绿。

### [x] PERF-07 — C20：一条 DML 重复 load 同表 · ✅ `97bdcd6bdbe`(fe-core)+`ea7fd1f6e7a`(iceberg)
- **权威设计/小结**：[`designs/FIX-PERF-07-unified-per-statement-table-owner-design.md`](./designs/FIX-PERF-07-unified-per-statement-table-owner-design.md) + [`-summary.md`](./designs/FIX-PERF-07-unified-per-statement-table-owner-summary.md)；架构结论落 memory `iceberg-table-resolution-cache-scoping`。
- **病灶**：读元数据/扫描/写成形/`beginWrite` 各自 loadTable 同表（最糟一条 DML 3~5 次）；旧两层缓存（胖句柄 + 跨查询 `IcebergTableCache`）只覆盖读扫描一段，写臂全绕。
- **方案（用户 2026-07-18 拍板「完整统一版」，取代旧窄设计与审计原方向）**：新增中性 `ConnectorStatementScope`（挂 `StatementContext`、经 `ConnectorSession.getStatementScope()` 够到、构造期捕获以跨 off-thread）作**读写共享**的每语句表加载归属者；读/扫描/写/`beginWrite` 四处全走它，整条语句一张表**只加载一次**；**拆胖句柄**（句柄回归纯坐标）；**delete stash 下沉到每语句作用域**（整删 `IcebergRewritableDeleteStash` ~140 行 + 测试 + 两 provider 六个 ctor 参数）；v3 行级 DML + NONE 作用域**响亮失败**（杜绝静默复活）。`beginWrite` **取共享表**（保留 openTransaction 的 refresh 兜新鲜 OCC 基底）。
- **⚠ 铁律**：净增 fe-core SPI（碰铁律 A，用户已签字）；prepared-stmt 复用 StatementContext→scope 每执行重置（`ExecuteCommand`）。
- **验证**：iceberg 968 pass / 0 fail / 1 skip；fe-core 两段验（`ConnectorStatementScopeTest` 3 + `ConnectorSessionImplTest` 17 绿）；6 视角多 agent 对抗复审 0 确认发现。e2e 留 P6.6 切换阶段补。
- **审计原「exists 从 load 推导 / resolve 一次跨层传递 / beginWrite 保持新载」均已作废**。

---

## P2 — CPU/payload/维护路径，影响门槛高但模式典型

### [x] PERF-08 — 维护路径逐单位重扫/无去重（C19 C21） · ✅ `89cc39c8d88`(C19)+`be0035eff62`(C21)
> 覆盖两条独立维护命令，同一"逐单位重扫、零去重"模式。两个独立 commit。权威设计/小结/红队见 [`designs/FIX-PERF-08-maintenance-path-rescan-design.md`](./designs/FIX-PERF-08-maintenance-path-rescan-design.md) + [`-summary.md`](./designs/FIX-PERF-08-maintenance-path-rescan-summary.md)。
- **C19 rewrite_data_files**（fe-core `89cc39c8d88`）：`ConnectorRewriteDriver.run` STEP3 逐组调 `registerRewriteSourceFiles`（iceberg 每次一遍 `useSnapshot(startingSnapshotId).planFiles()`）→ G+1 次整表扫描。**落地=`unionSourceFilePaths(groups)` 并集一次登记**（同一固定快照 + 组互斥 + 连接器按 path 去重 ⇒ `filesToDelete` 逐元素一致；四列计数来自组元数据）。整表扫描 G+1→1。gate=`ConnectorRewriteDriverTest` 并集/去重/空组单测（分布式实路径留 flip rehearsal e2e）。
- **C21 expire_snapshots**（iceberg `be0035eff62`）：`buildDeleteFileContentMap` 逐 snapshot 读全部 delete manifest、无去重。**落地=方法作用域 visited set 按 `manifest.path()` 去重**（manifest 不可变 + `putIfAbsent` 顺序无关 ⇒ 结果 map 逐字节不变）。远程读 O(S×M)→O(distinct M)。gate=`@VisibleForTesting lastDeleteManifestReadCount` + 共享 manifest 场景单测。
- **红队**：2 独立对抗 agent 逐一证伪均 `parity_preserved=true`、0 破坏性发现。iceberg `IcebergExpireSnapshotsActionTest` 9/9 + fe-core `ConnectorRewriteDriverTest` 5/5，两处 BUILD SUCCESS + 0 checkstyle。

### [ ] PERF-09 — C5：streaming pump 逐 split 重建 backend 候选集（**fe-core 框架层**）
- **病灶**：`startStreamingSplit:1638-1642` 逐 split `addToQueue` → `SplitAssignment` 每 split 重建 backend 候选集（`FederationBackendPolicy.computeScanRangeAssignment:225-235` 全量 backend 拷贝 + shuffle + multimap）+ `synchronized` 往返。10⁵~10⁶ split × ~100 BE ≈ 10⁷~10⁸ 冗余操作。
- **修复方向**：pump 侧微批（64~256 个/批）。
- **约束**：⚠ 改的是 **fe-core 通用框架**（惠及所有连接器，非 source-specific，允许改）；但属共享热路径，须证 **byte + cost 对所有连接器双不变**（对齐现有"共享 MVCC 方法须双不变"纪律）。

### [🔬] PERF-10 — C8：WHERE conjunct 转换 5~6 次/查询 · 暂缓（2026-07-18 复核判低价值，用户拍板跳过）
- **病灶**：同组 conjunct 在 `buildRemainingFilter`（×3 处）+ `buildScan:974` + `getScanNodeProperties:1428`（EXPLAIN 专用序列化，**非 EXPLAIN 也跑**）被转换 5~6 次。单次微秒~毫秒级，但与簇1 同源叠加。
- **修复方向（原）**：node 字段 memo，与 `cachedPropertiesResult` 同点失效。
- **🔬 复核结论（暂缓，findings.json #7 权威口径 + HEAD 核对）**：
  - **价值现已很低**：两个独立 verifier 均判 **Low**——纯 planning 线程 CPU 微秒~低毫秒、**无远程 IO**；当初记它是因「每次转换旁各跟一次昂贵远程 loadTable」，而那些 loadTable 已被 **PERF-01/07 全部修掉**，单独看只剩微秒级 CPU。
  - **且干净不了**：k≈7 中 **4 次在 fe-core 通用节点**（`PluginDrivenScanNode.buildRemainingFilter:1888`，且**带副作用** `filteredToOriginalIndex`，`pruneConjunctsFromNodeProperties`/`getSplits` 会读）——memo 须连副作用一起缓存 + 证「对所有连接器字节+成本双不变」，为微秒收益动共用热路径不划算。
  - **连接器侧「安全小切片」也做不干净**：`getScanNodeProperties:1518-1531` 的 `PUSHDOWN_PREDICATES_PROP` 转换+拼串是 **EXPLAIN 专用**（唯一消费者 `appendExplainInfo:1633`），普通查询每次白干；但要 explain-gate，连接器需知「是否 EXPLAIN」而 `ConnectorSession` **无此信号**（加 = 碰框架 SPI，用户明确不碰）；复用 `buildScan` 转换又因 `getScanNodeProperties`（节点初始化期）**早于** `buildScan`（规划期）而顺序反了、不可行。
  - **处置**：用户 2026-07-18 拍板**跳过**，改做高价值 PERF-11。若将来愿加一个中性 `ConnectorSession` explain 信号，连接器侧 explain-gate 可复活（收益仍微秒级）。
- **依赖**：与 PERF-01 同源，宜在 01 之后顺手。

### [🚧] PERF-11 — 簇5：per-split 不变量 + payload 放大（C12 C13 C14 C15） · 部分完成 `10b7d29423f`+`87ff73b1a95`
> 同一 `buildRange`/`populateRangeParams` 区域的 per-split 重复计算与 payload 放大，拆多 commit。权威设计/小结/红队见 [`designs/FIX-PERF-11-per-file-invariant-memo-*`](./designs/FIX-PERF-11-per-file-invariant-memo-design.md)。
> **已完成（`10b7d29423f`，连接器侧）= C12 + C15(a) + C13(plan 侧)**：`buildRange` 穿 1 条目 `PerFileScratch`（键 `task.file()` 身份），partition JSON/identity/ordered 值/delete 载体 per-slice→per-file；同文件 k range 共享不可变实例（省 FE heap）；v3 `rewritableDeleteSupply.put` per-slice→per-file（文件首 slice、覆盖末文件）。eager+流式共用 `buildRangeForTask` choke point，流式 scratch O(1) 不破 OOM。3 视角对抗 byte-parity 0 破坏；全模块 1064 pass/1 skip。
> **仍未做（deferred，另立 commit/任务）**：
> - **C15(b) 线路字节**：`populateRangeParams` 逐 range 把完整 delete 列表 `toThrift()` + partition JSON 塞进每个 `TFileRangeDesc` —— 真正减线路字节须 thrift params 级 delete-file 字典 + per-range 索引 + **BE reader 改**，是**协议演进非回归修复**（审计 C15 自述）。
> - **C13(wire) 二次转换**：上面的 `populateRangeParams` per-range `delete.toThrift()` 亦是 C13 的「第二次转换」——与 C15b 同一处线路侧，随 C15b 一并处理或单独 hoist（缓存 per-file 已转换 descs，牵扯持有 thrift 对象的内存权衡）。
> - **C14 fe-core 通用节点**：✅ **provider memo（子项 #3）已完成** `87ff73b1a95`——`PluginDrivenScanNode.resolveScanProvider` per-split 重分配 provider → 按 `currentHandle` 身份 memo（不可变 holder + volatile 发布），byte 等价、3 视角红队 0 缺陷；权威设计/小结见 [`designs/FIX-PERF-11-scan-provider-memo-*`](./designs/FIX-PERF-11-scan-provider-memo-design.md)。**剩余暂缓**（用户 2026-07-19 拍板只做 #3）：路径重复解析（#1/#2，`LocationPath.of`/`toStorageLocation` 有跨连接器 Hadoop Path 规整字节风险、性价比差）、build-then-discard 分区列（#4，须新增 `ConnectorScanRange` 能力位给 fe-core 加面、碰铁律 A）。
- **C12**：一个 data file 被 `TableScanUtil.splitFiles` 切成 k 个 byte-slice 后，`buildRange:1045` 对每个 slice 重算 partition JSON（Jackson 序列化 + 时区格式化）、identity map、delete 转换 —— (specId, PartitionData) 级不变量。100k split ≈ 0.5~2s CPU。修：按 (specId, PartitionData) memo。
- **C13**：v3 rewritable-delete stash：同一 delete 列表 plan 期 `rewritableDeleteDescs:302-313` 转一次 thrift，`populateRangeParams` 再转一次；每 slice 重复 put 相同 supply。修：转一次复用、per-file 而非 per-slice。
- **C14**：通用节点 per-split 重复 `LocationPath.of`（URLEncoder+URI.create）、重建 columns-from-path 却被 `IcebergScanRange.populateRangeParams:435-437` unset 丢弃；`resolveScanProvider` 每 split 经 `getFileCompressType` 反复解析。修：hoist 不变量 / 删造完即弃分支。⚠ 涉 **fe-core 通用节点**——保持 connector-agnostic，勿加 source-specific 分支。
- **C15**（payload 放大）：同一 data file 的完整 delete 列表 + partition JSON 复制进**每个** byte-slice 的 `TFileRangeDesc`；共享 delete file 逐 data file 重复。大 MOR 扫描计划体积多出 MB 级（FE 构建 + BE 解析双向付费）。修：per-file 共享引用，不逐 slice 复制 payload。

---

## 不立项（记录）

- **R1（驳回）**：字典新鲜度 poll 每 5s 重建分区视图 —— 重操作属实但乘数不成立（`CreateDictionaryInfo.validateAndSet:164` 对外表强转 `catalog.Table` ⇒ CREATE DICTIONARY 在校验期即 ClassCastException，poll 路径不可达）。详见报告 §3。
- **旁获（与本审计无关，另行处理）**：上述强转本身是功能缺口/潜在 bug（外表字典完全不可用）。**不在本任务空间**，如需修单开任务。
