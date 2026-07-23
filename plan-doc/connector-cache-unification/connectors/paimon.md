## 附录:fe-connector-paimon 热点路径与缓存审计

### 一、迁移状态(Migration status)

`paimon` 已在 `SPI_READY_TYPES` 白名单内(`fe/fe-core/.../datasource/CatalogFactory.java:57`),是一个**一等公民独立 catalog 类型**,经 SPI plugin 路径 `PaimonConnectorProvider → PaimonConnector → PaimonConnectorMetadata / PaimonScanPlanProvider`,由 `PluginDrivenExternalCatalog` 承载。它是 **LIVE**,不是 sibling:读、MVCC/time-travel、DDL(建/删库+表)、system tables、stats、SHOW CREATE 均在连接器落地。**写路径故意未迁移**(`getCapabilities` 不声明写能力,`PaimonConnector.java:318`),因此本连接器无 beginWrite/commit。

关键结构性事实:生产 catalog 被 **paimon SDK `CachingCatalog`** 包裹(`CatalogFactory.createCatalog → CachingCatalog.tryToCreate`,`CACHE_ENABLED` 默认 true,已 javap 证实 paimon-core 1.3.1)。`CachingCatalog` 自带 `tableCache`(Identifier→Table)、`partitionCache`(Identifier→List<Partition>)、`manifestCache`(Path→manifest segments)。**这是 iceberg SPI 路径所缺、而 paimon 天然具备的 metastore-client 级缓存层**,是本审计结论的决定性因素。

### 二、当前缓存(Caches)

| 缓存 | 作用域 | Key | 值 | TTL / 失效 | file:line |
|---|---|---|---|---|---|
| `PaimonLatestSnapshotCache` | cross-query | `Identifier(db,table)` | latest snapshot id (long) | `meta.cache.paimon.table.ttl-second` 默认 24h;`<=0` 关闭(no-cache catalog);Caffeine expireAfterAccess + maxSize 1000;REFRESH TABLE/DB/CATALOG 失效 | `PaimonLatestSnapshotCache.java:48`(字段 `PaimonConnector.java:124`) |
| `PaimonSchemaAtMemo` | cross-query | `(db,table,sysTable,branch,schemaId)` | `PaimonSchemaSnapshot`(fields+partKeys+pkKeys) | 无 TTL,bounded 10000 clear-on-overflow;值不可变(schemaId 内容 write-once);REFRESH 失效 | `PaimonSchemaAtMemo.java:49`(元数据 time-travel `PaimonConnectorMetadata.java:299` 与 scan schema-evolution dict `PaimonScanPlanProvider.java:1567` 共享) |
| `partitionViewCache`(`ConnectorPartitionViewCache`,共享工具箱) | cross-query | `PartitionViewCacheKey(db,table,snapshotId,schemaId=-1)` | 已构建 `List<ConnectorPartitionInfo>` | `meta.cache.paimon.partition_view.*` 默认 ON/24h/1000;snapshotId 复用 latestSnapshotCache 追踪"current";REFRESH 失效 | `PaimonConnectorMetadata.java:1012-1052`(字段 `PaimonConnector.java:143`) |
| Transient Table 胖句柄 | per-scan / per-op | `PaimonTableHandle` 实例身份 | 已加载 Table 的 transient 引用 | 句柄对象生命周期;FE→BE 序列化后丢失并 reload;不跨不同 fe-core op(每次 `resolveConnectorTableHandle` 重建),但在单个 SPI 方法内 / 单个 scan node 内(`PluginDrivenScanNode` 缓存 currentHandle+resolveScanProvider)共享 | `PaimonTableHandle.java:89`(set `PaimonConnectorMetadata.java:221`,read `PaimonTableResolver.java:65`) |
| paimon SDK `CachingCatalog`(tableCache/partitionCache/manifestCache) | metastore-client | `Identifier` / `Path` | Table / List<Partition> / manifest segments | paimon `cache.*` SDK 默认;`CACHE_ENABLED` 默认 true 故生产 catalog 被包裹 | 外部 SDK `org.apache.paimon.catalog.CachingCatalog`(包裹于 `PaimonConnector.java:454`) |
| `DRIVER_CLASS_LOADER_CACHE` / `REGISTERED_DRIVER_KEYS` | none | driver URL / url#class | ClassLoader / 已注册标记 | 进程级 JDBC 驱动去重,非元数据缓存 | `PaimonConnector.java:89-90` |

### 三、Funnel 参与(Funnel participation)

已完全接入 Layer-2 per-statement funnel:`PluginDrivenExternalTable.resolveConnectorTableHandle` 经 `PluginDrivenMetadata.get`(每 (statement,catalog) memoize 一个 metadata);`PluginDrivenScanNode` 另在 `cachedMetadata` 缓存并按 currentHandle 身份 memoize `resolveScanProvider()`(`:182,189,245-259`)。故一个 scan node 内 `planScan` 与 `getScanNodeProperties` 共享同一 provider 与同一 currentHandle(携带 transient Table),`resolveScanTable` 命中 transient。`schemaAtMemo` 由长生命周期 `PaimonConnector` 同时注入 `getMetadata`(`:247`)与 `getScanPlanProvider`(`:311`),即每 catalog 一份。

`metadataMemoizesLoadedTable = partial`:`getMetadata` 每语句返回**全新** `PaimonConnectorMetadata`(`:244-248`),每个 fe-core op 重新 `getTableHandle → catalogOps.getTable`(`:211`)构造新句柄。这与 iceberg 审计指出的 re-getTableHandle 模式相同,**但对 paimon 不构成 remote-IO 放大**:paimon SDK `CachingCatalog.tableCache` 使每次 getTable 成为内存命中,transient 胖句柄在单 op 内折叠重载。因此 funnel 的"每语句一 metadata"本身并不折叠 paimon 的表加载,折叠由 paimon SDK 缓存 + transient 句柄完成。iceberg PERF-07 式"每语句加载一次"表 memo 对 paimon 仅省下内存 tableCache 查找与 `setPaimonTable` 抖动 —— 可忽略。

### 四、热点路径审计(Hot-path findings)

| id | 区域 | 问题 | 倍率 | 成本 | 严重度 | 已memo? | file:line |
|---|---|---|---|---|---|---|---|
| PA-1 | partitions | `listPartitionNames`(`:988`)/`listPartitionValues`(`:1057`)直接调 `collectPartitions()`,**绕过** partitionViewCache;仅 `listPartitions`(`:1019-1022`)走派生缓存。SHOW PARTITIONS 与 partition_values() TVF 重跑渲染(变体 C) | 每次调用 1x(非循环);原始 remote listPartitions 已被 paimon SDK partitionCache 挡下,残留仅 CPU 重渲染 | cpu | **P2** | 否 | `PaimonConnectorMetadata.java:988` |
| PA-2 | query-plan | 每 fe-core op 经 getTableHandle→getTable(re-getTableHandle 模式),未由 metadata 折叠 | ~4-7 次/语句,首次后均为 CachingCatalog 内存命中(非 remote) | remote-io | none | 是(SDK+transient) | `PaimonConnectorMetadata.java:211` |
| PA-3 | stats-rowcount | `getTableStatistics → rowCount` 用 `newReadBuilder().newScan().plan().splits()` 全 manifest 计划求行数,连接器无 memo | 每语句 1 次;fe-core `RowCountCache`(`PluginDrivenExternalTable.java:907`)跨查询缓存,manifest 读被 paimon manifestCache 挡 | remote-io | none | 是(fe-core 层) | `PaimonConnectorMetadata.java:1205` |
| PA-4 | schema | `getTableSchema → latestSchema` 为 `schemaManager().latest()` 活读(刻意不用冻结 rowType(),以捕获外部 ALTER) | 每 schema-cache miss 1 次;fe-core 通用 schema 缓存(经 `schemaCacheTtlSecondOverride`)跨查询挡下 | remote-io | none | 是(fe-core schema cache) | `PaimonConnectorMetadata.java:250` |
| PA-5 | split-enum | `planScanInternal → planSplits(scan)` 读快照/manifest 枚举 split(即查询本身的数据规划,不可缓存);周边不变量已提升一次/scan | 每 scan 1 次;跨查询 manifest 读被 paimon manifestCache 挡 | remote-io | none | 是(paimon manifestCache) | `PaimonScanPlanProvider.java:461` |

per-scan/per-file 不变量已充分提升:vended token(`:559-560`)、FE 权重分母(`:565`)、native 目标 split size 惰性一次(`:593,624-626`)、partitionValues 每 DataSplit 一次并复用于全部字节切片子 split(`:605,634`)。即 iceberg PERF-06/PERF-11 的等价已就位。

### 五、授权 / 一致性(Authz / consistency)

`perUserCredential=false`,`sessionUserRelevant=false`。paimon **无** 按 Doris 用户的会话凭证模型:catalog 在创建时一次性认证(Kerberos 插件 UGI / HMS principal / 静态或 JDBC 凭证);REST vended 凭证在 scan 时从**表级** `RESTTokenFileIO` 提取(`PaimonScanPlanProvider.extractVendedToken:916`),**不按用户键控**。故 name-only 缓存无 "list≠load" 泄漏风险。已证实:fe-connector-paimon 与 fe-connector-metastore-paimon 内无 `SUPPORTS_USER_SESSION`/`isUserSessionEnabled`;`PaimonRestMetaStoreProvider` 无任何 per-user/delegation 逻辑;`tools/check-authz-cache-sharding.sh` 仅针对 `IcebergConnector`。三个连接器缓存无条件构造(从不置空)对 paimon 是正确的(`PaimonConnector.java:138-142` 记录了理由)。写路径 read+write 共享一 txn 的一致性(iceberg `CatalogStatementTransaction`)对 paimon **N/A**(写未迁移)。读侧 MVCC 一致性(查询内稳定快照 pin)由 `PaimonLatestSnapshotCache → beginQuerySnapshot → applySnapshot → scan.snapshot-id` 提供。

### 六、框架部件需求(Framework pieces)

- **already-has(6)**:per-statement funnel 路由;partition-cache(partitionViewCache+SDK partitionCache);manifest-cache(SDK manifestCache);per-scan-hoist;per-file-split-memo;shared-fe-connector-cache 采用(partitionViewCache/latestSnapshotCache 用共享工具箱,仅 schemaAtMemo 用裸 CHM,可选统一)。
- **no(5)**:cross-query-table-cache(与 SDK tableCache 重复,且 latestSnapshotCache 已提供稳定快照语义);format-cache(格式来自 `table.options()` 内存读,无 whole-table planFiles 回退);comment-cache(comment 来自 coreOptions,无 N-per-query remote getComment);authz-session-user-isolation(无 per-user 会话模型);write-txn-coholder(写未迁移)。

### 七、结论与建议

paimon **已充分缓存,无需套用 iceberg 热点/框架移植**。根因:(1) 生产 catalog 由 paimon SDK `CachingCatalog` 包裹,提供 iceberg SPI 路径缺失的 table/partition/manifest 内存缓存;(2) 连接器已恢复三个 legacy 语义缓存 + transient 胖句柄,并完成 per-scan/per-file 不变量提升;(3) funnel 已路由其 ConnectorMetadata。

优先建议:
1. **不要**对 paimon 移植 iceberg 式 table/format/comment/manifest 缓存或 session=user 隔离(均结构性不适用或与 SDK 缓存重复)。
2. (可选,P2)将 `listPartitionNames`/`listPartitionValues` 也路由经 `partitionViewCache`(修 PA-1),消除派生视图重渲染的兄弟路径旁路——价值偏低,因原始 remote 读已被 paimon partitionCache 挡下。
3. (可选,整洁性)将 `PaimonSchemaAtMemo` 迁到共享 `MetaCacheEntry` 工具箱,统一 TTL/metrics —— 纯粹一致性,非正确性或性能缺口。

---

## 复核结论 (adversarial verify)

**Verdict: CONFIRMED**  ·  确认发现 IDs: PA-1, PA-2, PA-3, PA-4, PA-5

| 原始声明 | 问题 | 更正 | 证据 |
|---|---|---|---|
| paimon SDK CachingCatalog ... CACHE_ENABLED default true so the live catalog IS wrapped (CatalogFactory.createCatalog -> CachingCatalog.tryToCreate); paimon cache.* options (cache.expiration-interval etc.) | Immaterial label imprecision: the actual paimon option key string is 'cache-enabled' (hyphen), and the connector calls the paimon (not Doris) org.apache.paimon.catalog.CatalogFactory at PaimonConnector.java:454. The substantive claim is fully verified in bytecode. | Verified: paimon-core-1.3.1 CatalogFactory.createCatalog(ctx,cl) invokestatic CachingCatalog.tryToCreate, which gates on CatalogOptions.CACHE_ENABLED and areturns the raw catalog when false; the CACHE_ENABLED field is built as booleanType().defaultValue(Boolean.valueOf(true)) (iconst_1 at offset 202), description 'Controls whether the catalog will cache databases, tables, manifests and partitions.' Default IS true; wrap covers db/table/manifest/partition. No change to any conclusion. |  |

> 复核备注：Every material claim holds against HEAD (aaab68ef474) code and the paimon 1.3.1 SDK bytecode. No hallucinated file:line, no invented cache class, no overstated multiplier, no false-positive finding, no missed severe finding.

MIGRATION: paimon in SPI_READY_TYPES (CatalogFactory.java:57); LIVE via PaimonConnectorProvider->PaimonConnector->PaimonConnectorMetadata/PaimonScanPlanProvider; no write capability (getCapabilities PaimonConnector.java:321-341 declares only MVCC/PARTITION_STATS/COLUMN_AUTO_ANALYZE/SHOW_CREATE_DDL). CONFIRMED.

CACHES (all 6 exist, scope/key correct):
- PaimonLatestSnapshotCache.java:48 MetaCacheEntry<Identifier,Long>, shared CacheSpec toolkit, ttl=meta.cache.paimon.table.ttl-second, maxSize 1000, <=0 disables. Built PaimonConnector.java:154-155, field :124. Invalidations :256/278/285. CONFIRMED.
- PaimonSchemaAtMemo.java:49 raw ConcurrentHashMap (:54) clear-on-overflow (:81-82) maxSize 10000, key MemoKey(db,table,sysTableName,branchName,schemaId) (:129-142), immutable value. Injected into getMetadata (:247) AND getScanPlanProvider (:311). CONFIRMED including the 'only schemaAtMemo uses a raw CHM' framework-piece note.
- partitionViewCache: ConnectorPartitionViewCache + PartitionViewCacheKey exist in fe-connector-cache; routed in PaimonConnectorMetadata.listPartitions :1012-1022, key (db,table,snapshotId,-1) via latestSnapshotCache :1046-1052; field PaimonConnector.java:143 built :158; invalidations :263/280/287. CONFIRMED.
- transient fat-handle: PaimonTableHandle.java:89 (transient Table), set PaimonConnectorMetadata.java:221, read PaimonTableResolver.java:65. CONFIRMED.
- paimon SDK CachingCatalog: DECISIVELY verified in bytecode (see corrections) - wraps db/table/manifest/partition, default on. CONFIRMED - this is the load-bearing fact behind the whole 'do not port iceberg caches' verdict and it holds.
- DRIVER_CLASS_LOADER_CACHE/REGISTERED_DRIVER_KEYS PaimonConnector.java:89-90, process-wide, not a metadata cache. CONFIRMED.

FUNNEL (partial memoization): PluginDrivenMetadata.get memoizes one ConnectorMetadata per (statement,catalog) via scope.getOrCreateMetadata (:70) with per-statement identity guard (:64-69). getMetadata returns a FRESH PaimonConnectorMetadata per statement (:244-248). resolveConnectorTableHandle (:116-119) re-calls getTableHandle->catalogOps.getTable (:211) on EVERY fe-core op with NO caching (~15 call sites in PluginDrivenExternalTable each do PluginDrivenMetadata.get + resolveConnectorTableHandle). So the metadata does NOT memoize the loaded Table; collapse comes from SDK tableCache + transient handle. PluginDrivenScanNode: cachedMetadata (:189/204-207) + resolveScanProvider memo keyed on currentHandle identity (:245-259). All CONFIRMED.

HOTPATH FINDINGS:
- PA-1 CONFIRMED as real Variant-C cache bypass: listPartitionNames (PaimonConnectorMetadata.java:988) and listPartitionValues (:1057) call collectPartitions() directly; only listPartitions (:1012-1022) routes through partitionViewCache. This is the ONLY actionable item. Its P2 rating is arguably slightly generous (CPU-only re-render since raw remote read is blunted by SDK partitionCache) but the audit itself flags it 'low-value', so self-consistent.
- PA-2..PA-5 CONFIRMED as accurately characterized non-findings (severity none / memoizedAlready true): heavy ops are all REAL (getTable :211; rowCount = full newReadBuilder().newScan().plan().splits() sum PaimonCatalogOps.java:368-376; latestSchema = live schemaManager().latest() :345-355, intentionally not off frozen rowType; planSplits :461) but each is genuinely blunted by SDK tableCache/manifestCache + transient handle + fe-core RowCountCache (PluginDrivenExternalTable.java:907) / generic schema cache. Per-scan/per-file loop-invariants are all hoisted as claimed (vendedToken :559-560, weightDenominator :565, targetSplitSize lazy-once :624-626, getPartitionInfoMap once-per-DataSplit :605 reused across sub-splits :634). No hidden per-split remote loadTable in any loop.

AUTHZ: no SUPPORTS_USER_SESSION/isUserSessionEnabled anywhere in fe-connector-paimon or fe-connector-metastore-paimon (only comments explaining paimon has no such model); tools/check-authz-cache-sharding.sh TARGET_REL is IcebergConnector.java only (:57); REST vended token extracted table-level via extractVendedToken (PaimonScanPlanProvider.java:916), not Doris-user-keyed. Name-only caches safe. CONFIRMED.

Overall verdict (do NOT port iceberg-style caching; optionally take PA-1; optionally migrate PaimonSchemaAtMemo to MetaCacheEntry) stands unchanged.
