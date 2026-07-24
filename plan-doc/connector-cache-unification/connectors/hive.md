## 连接器附录 — hive (catalog type `hms`)

### 结论速览

hive 连接器在 HEAD 已经是**框架对齐 + 缓存充分**的状态，**不需要**再套用 iceberg 的 hot-path 缓存改造。它已经具备参考 commit `0b4f72582e7` 三层框架里对 hive 有意义的全部部件：Layer-1 的连接器侧 cross-query 缓存以 hive 自己的形态存在（`CachingHmsClient` 四缓存 + `HiveFileListingCache` + `ConnectorPartitionViewCache`），Layer-2 的 per-statement funnel 完整接入（主连接器走 `PluginDrivenMetadata.get`，sibling 走 `memoizedSiblingMetadata`，写路径有 per-statement 的 `HiveConnectorTransaction`），Layer-3 的 authz 隔离对 hive **不适用**（无 session=user）。所有 planning 入口的重 IO 都命中缓存，**没有** iceberg 那种「同一 planning pass 里 loadTable 3-7 次」的回归。

> ⚠ 文档漂移：`CachingHmsClient.java:85-88`、`HiveFileListingCache.java:72-74`、`HiveConnector.wrapWithCache` 的 javadoc（:667-668）仍写着 "Dormant / `hms` is not in `SPI_READY_TYPES`"，这在 HEAD 是**错的**——commit `6e521aa64b2`(#65473) 同时把 `hms` 放进 `SPI_READY_TYPES`(CatalogFactory.java:57) 并在 `createClient()` 里接线了 `wrapWithCache`(:645-646)，却没同步更新这几处类注释。纯文档问题，不影响运行。

### 迁移状态

- `hms` ∈ `SPI_READY_TYPES`（`CatalogFactory.java:57`）→ **LIVE**。fe-core 用 `PluginDrivenExternalCatalog` 包 `HiveConnector`（`CatalogFactory.java:110-118`）。
- 同时是**异构网关（gateway）**：iceberg-on-HMS / hudi-on-HMS 作为 embedded SIBLING 连接器，在各自 child-first classloader 里按需构建（`HiveConnector.java:530-592`），其 `ConnectorMetadata` 按 owner label 做 per-statement 记忆化（`HiveConnectorMetadata.memoizedSiblingMetadata`，:302-305）。hudi 不是独立 SPI 类型（`CatalogFactory.java:51-55`）。

### 缓存清单

| 缓存 | scope | key | TTL/容量 | 失效 | 位置 |
|---|---|---|---|---|---|
| `CachingHmsClient.tableCache` | metastore-client | (db,table)→HmsTableInfo | 24h / 1万；兼容 `schema.cache.ttl-second` | REFRESH flush/flushDb/flushAll | `CachingHmsClient.java:116,172` |
| `CachingHmsClient.partitionNamesCache` | metastore-client | (db,table,maxParts) | 24h / 1万；兼容 `partition.cache.ttl-second` | 同上 + `invalidatePartitions` | `CachingHmsClient.java:117,187` |
| `CachingHmsClient.partitionsCache` | metastore-client | (db,table,partitionValues)，一分区一条 | 24h / 10万；put 有 generation guard 防 REFRESH race | 同上（按分区） | `CachingHmsClient.java:118,202-243` |
| `CachingHmsClient.columnStatsCache` | metastore-client | (db,table,columns) | 24h / 1万 | 同上 | `CachingHmsClient.java:119,274` |
| `HiveFileListingCache` | cross-query | (db,table,location,partitionValues)→List\<HiveFileStatus\> | 24h / 1万；兼容 `file.meta.cache.ttl-second` | table/db/partitions/all | `HiveFileListingCache.java:114,160-176` |
| `HiveConnector.partitionViewCache`（`ConnectorPartitionViewCache`，#65829 的 PERF-06 cache A） | cross-query | (db,table,-1,-1)（hive 无 snapshot/无 schema version，一表一条）→List\<ConnectorPartitionInfo\> | 24h / 1000 | table/db/all | `HiveConnector.java:112,136`；消费 `HiveConnectorMetadata.java:1160-1172` |
| `memoizedSiblingMetadata`（iceberg/hudi sibling 元数据） | per-statement | `metadata:{catalogId}:{ownerLabel}` | statement 生命周期；NONE scope 不记忆 | 随 scope 关闭 | `HiveConnectorMetadata.java:302-305` |
| `HiveConnectorTransaction` begin-time 表快照 | per-statement | 唯一写目标表（`hmsTableInfo`/`tableActions`） | 写事务生命周期 | 事务结束 | `HiveConnectorTransaction.java:209,546-558` |
| `ThriftHmsClient` 连接池（非元数据缓存，仅传输） | metastore-client | pool size = `hms.client.pool.size` | 连接生命周期 | — | `HiveConnector.java:613-617` |

关键点：`HiveConnector.createClient()` 用 `wrapWithCache` 把裸 `ThriftHmsClient` 包成 `CachingHmsClient`（:645-646,670-672），所以 `HiveConnectorMetadata` 里**所有** `hmsClient.getTable/listPartitionNames/getPartitions/getTableColumnStatistics` 调用都透明走 cross-query 缓存——这正是 hive 不需要 iceberg PERF-07（per-statement 表加载）的原因：iceberg 在 cutover 后**没有** cross-query 表缓存，hive 有。

### funnel 参与

- **主路径**：fe-core 所有 seam 经 `PluginDrivenMetadata.get(session, connector)`（funnel 唯一合法直呼点，`PluginDrivenMetadata.java:52-72`；调用点 `PluginDrivenExternalTable.java:133,159,189,449,...`）→ `HiveConnector.getMetadata`→`newMetadata(getOrCreateClient())`（`HiveConnector.java:140-142`）。**一 statement 一 ConnectorMetadata** 不变式成立（`tools/check-fecore-metadata-funnel.sh` 全构建守门）。
- **读侧是否 per-metadata 记忆化已加载表**：**否**——`HiveConnectorMetadata` 无 `resolvedTable` 字段（字段仅 hmsClient/缓存/sibling seam，:203-241），`getTableHandle`(:399)/`getTableSchema`(:493)/`getColumnHandles`(:604)/`getColumnStatistics`(:854)/`applyFilter`(:1093) 各自 fresh 调 getTable，但**由 CachingHmsClient cross-query 缓存兜底**，同 statement 内重复加载 = 1 RPC + N 命中。**写侧**额外在写事务内记忆 begin-time 表快照（`HiveConnectorTransaction.java:549-551`）。
- **sibling 路径**：iceberg/hudi 元数据按 (catalogId, ownerLabel) per-statement 记忆化，保证一 statement 内同 owner 复用一个 metadata（:302-350）。

### hot-path 逐项审计

| ID | 入口 | 重 op & 倍率 | 是否已记忆 | 严重度 | 位置 |
|---|---|---|---|---|---|
| HMS-H1 | table/schema 加载 | getTable 每 pass 3-4 次 → 1 RPC + N 命中（tableCache） | ✅ | none | `HiveConnectorMetadata.java:399,493,604,854` |
| HMS-H2 | 分区枚举（pruning/scan/stats/MTMV） | listPartitionNames+getPartitions 多次 → 全缓存（names/partitions/view 三层） | ✅ | none | `:1093-1106,1019-1025,1160-1172`；`HiveScanPlanProvider.java:492-499` |
| HMS-H3 | 分区目录 listStatus（scan 最重 IO，O(分区)） | 每分区一次 → `HiveFileListingCache` cross-query，scan 与 size-estimate 互暖 | ✅ | none | `HiveScanPlanProvider.java:537`；`HiveConnectorMetadata.java:945,1062` |
| HMS-H4 | ACID split 枚举 | `HiveAcidUtil.getAcidState` 每分区每 scan，**未缓存** | ❌ | none（按设计） | `HiveScanPlanProvider.java:314` |
| HMS-H5 | 写规划 | loadTable + beginWrite 双载 + buildExistingPartitions → 缓存 + 事务内快照复用 | ✅ | none | `HiveWritePlanProvider.java:103,105,255-258`；`HiveConnectorTransaction.java:549-551` |
| HMS-H6 | 读侧无 per-statement 表 memo | 仅靠 CachingHmsClient TTL；同 statement 两次 getTable 之间若发生淘汰/过期 → 第二次 RPC | ❌ | **P2** | `HiveConnectorMetadata.java:203-241` |

- **HMS-H4 是「有意不缓存」**：ACID 目录状态依赖 valid write-id 快照（每事务变），cross-query 缓存会读到过期数据；`getFileListingCache` 注释明确只服务非 ACID 路径（`HiveScanPlanProvider.java:103-104`），与 legacy 一致，**不是缺陷**。
- **SHOW PARTITIONS 走 `listPartitionNamesFresh`（绕缓存）**（`CachingHmsClient.java:192-199`；`HiveConnectorMetadata.java:1130-1136`）也是**有意 fresh**（`use_meta_cache` 契约，legacy parity），非缺陷。

### authz / 一致性

hive **不 vend 任何 per-user 凭证**、**无 session=user 类比**。metastore RPC 使用**单一 catalog 级身份**：要么 plugin 侧 Kerberos keytab 固定 principal（`HiveConnector.buildPluginAuthenticator`，:710-740），要么 `context.executeAuthenticated`（catalog 级），**从不**是查询用户的委派凭证，也没有 REST OIDC / per-identity metastore。因此 name-only 的缓存 key（db,table[,parts/cols/location]）**不会**像 `iceberg.rest.session=user` 那样泄漏跨用户元数据——所有用户看到同一 metastore 身份的视图，用户级访问由 Doris fe-core RBAC 独立管控。这正是 authz 守门脚本 `tools/check-authz-cache-sharding.sh` **只针对 `IcebergConnector.java`** 的原因，hive 无条件构建缓存（`HiveConnector.java:108-112,133-136`）。另有一处 fail-loud 不变式（`HiveConnector.java:548-555`）：若 iceberg-on-HMS sibling 竟声明 `SUPPORTS_USER_SESSION` 就直接抛错——恰因 hive 前门不是 session=user、fe-core 的 per-user schema/name 缓存 bypass 是按前门连接器 keyed 的。写读一致性：读写共享同一 cross-query `CachingHmsClient`（不可变 `HmsTableInfo`/`HmsPartitionInfo`），写事务钉住 begin-time 表快照，ACID 读每 scan 钉 write-id 快照（`getValidWriteIds`）。陈旧性由 coarse REFRESH（flush/flushDb/flushAll，`HiveConnector.java:357-419`）+ TTL 收敛。**Layer-3 对 hive 不适用。**

### 框架部件需求判定

| 部件 | 判定 | 依据 |
|---|---|---|
| cross-query-table-cache | already-has | `CachingHmsClient.tableCache`（iceberg `IcebergTableCache` 的 metastore-client 类比） |
| partition-cache | already-has | partitionNamesCache + partitionsCache + `ConnectorPartitionViewCache` 三层 |
| manifest-or-file-list-cache | already-has | `HiveFileListingCache`（manifest cache 的 hive 类比） |
| shared-fe-connector-cache-adoption | already-has | 全程用 `MetaCacheEntry`/`CacheSpec`/`ConnectorPartitionViewCache`/`PartitionViewCacheKey` |
| write-txn-coholder | already-has | `HiveConnectorTransaction` per-statement 写事务，co-hold begin-time 表 |
| per-scan-hoist | already-has | format/split-size/splittable/storage-props 均在分区循环前算一次 |
| per-statement-funnel-memoization | no | cross-query 缓存已折叠重复加载；HMS getTable 远轻于 iceberg loadTable，不值当 |
| format-cache | no | 从 handle inputFormat/serde 廉价推断，无 iceberg 那种整表 planFiles fallback |
| comment-cache | no | comment 来自已缓存的 getTable params/ConnectorColumn，无独立 N-per-query 远程 getComment |
| per-file-split-memo | no | splitFile 按引用共享分区 value map，无 per-file 派生可 memo |
| authz-session-user-isolation | no | 无 per-user 凭证 / 无 session=user；RBAC 独立管控 |

### 优先建议（均为 LOW）

1. **文档修正**：把 `CachingHmsClient.java:85-88`、`HiveFileListingCache.java:72-74`、`HiveConnector.wrapWithCache`(:667-668) 里过期的 "Dormant / hms not in SPI_READY_TYPES" 注释改为「live」——纯文档。
2. **可选 P2**：给读侧 `HiveConnectorMetadata` 加一个 per-statement `resolvedTable` memo（iceberg PERF-07 类比），关掉 HMS-H6 那个极罕见的「statement 中途缓存淘汰导致二次 RPC」race。成本收益看**不建议**做（HMS getTable 很轻）。
3. **保持** ACID `getAcidState` per-scan 不缓存（HMS-H4）——快照/write-id 依赖，legacy parity 正确。

---

## 复核结论 (adversarial verify)

**Verdict: CONFIRMED**  ·  确认发现 IDs: HMS-H1, HMS-H2, HMS-H3, HMS-H4, HMS-H5, HMS-H6

| 原始声明 | 问题 | 更正 | 证据 |
|---|---|---|---|
| HiveConnectorTransaction begin-time table snapshot ... hmsTableInfo captured in beginWrite (HiveConnectorTransaction.java:209) | Off-by-2 line cite: the hmsTableInfo field is declared at :123 and assigned inside beginWrite at :211, not :209. Immaterial — :209 falls within the beginWrite method and the reuse-in-getTable range :546-558 (specifically :549-551) is exact. | Field decl HiveConnectorTransaction.java:123 (private volatile HmsTableInfo hmsTableInfo); assignment this.hmsTableInfo = table at :211; reuse at :551. No substantive error. |  |
| detailedMarkdown enumerates only 3 stale 'Dormant/hms not in SPI_READY_TYPES' javadocs (CachingHmsClient:85-88, HiveFileListingCache:72-74, HiveConnector.wrapWithCache:667-668) | Non-exhaustive, not wrong: the icebergSibling/hudiSibling field comments at HiveConnector.java:118 and :126-127 also carry stale 'Dormant until hms enters SPI_READY_TYPES' text at HEAD. | Two additional stale doc sites exist at HiveConnector.java:118,126-127; harmless, additive to the same doc-fix recommendation (LOW). |  |

> 复核备注：Every material claim holds against HEAD (aaab68ef474). MIGRATION: hms in SPI_READY_TYPES (CatalogFactory.java:56-57); hudi correctly not a standalone type; createClient wraps ThriftHmsClient in CachingHmsClient (HiveConnector.java:645-646, wrapWithCache:670-672). The three stale 'Dormant' javadocs are genuinely factually wrong at HEAD (hms is live) — doc-fix recommendation valid.

CACHES (all 9 verified, no hallucinations, scope/key/TTL correct): tableCache field :116 / getTable :172-175 (db,table); partitionNamesCache :117 / :187-190 (db,table,maxParts); partitionsCache :118 / :202-243 per-partition entry keyed by values, generation-guarded put :235-238; columnStatsCache :119 / :274-279; DEFAULT_TTL 86400 (:109), caps 10000/10000/100000/10000 (:110-113) all match. HiveFileListingCache :114 / listDataFiles cache.get :174 keyed (db,table,location,partitionValues); TTL 86400 cap 10000 (:92-93); legacy file.meta.cache.ttl-second remap :87,141-143. partitionViewCache field HiveConnector.java:112 constructed :136 (ConnectorPartitionViewCache), consumed HiveConnectorMetadata listPartitions :1170-1172 keyed (db,table,-1,-1). memoizedSiblingMetadata :302-305 keyed 'metadata:'+catalogId+':'+ownerLabel via statementScope.getOrCreateMetadata (per-statement, NONE=no memo). Shared toolkit classes (ConnectorPartitionViewCache/PartitionViewCacheKey/MetaCacheEntry/CacheSpec) all exist in fe-connector-cache and are imported (CachingHmsClient :20-21).

FUNNEL: PluginDrivenMetadata.get (fe-core :53, memoizes on scope.getOrCreateMetadata('metadata:'+catalogId) :70); PluginDrivenExternalTable calls it 16×; getMetadata->newMetadata(getOrCreateClient()) HiveConnector :140-142,188-192. Read-side HiveConnectorMetadata fields :203-241 hold hmsClient/caches/sibling seams only — NO resolvedTable field, so getTable at :399/:493/:604 and col-stats :854 re-call fresh but collapse via CachingHmsClient; write-side reuses hmsTableInfo begin-time snapshot :551. 'metadataMemoizesLoadedTable: partial' is accurate.

HOT-PATH FINDINGS all 6 confirmed at exact sites: H1 getTable :399/:493/:604 + getTableColumnStatistics :854, all via CachingHmsClient (severity none, memoized) — multiplier 3-4x not overstated. H2 applyFilter listPartitionNames :1093 + getPartitions :1105; resolvePartitionRefs :1019-1025; listPartitions partitionViewCache :1160-1172 — three-layer cache, none. H3 fileListingCache.listDataFiles HiveScanPlanProvider :537 (non-ACID) + sumCachedFileSizes HiveConnectorMetadata :1062-1063, HiveFileListingCache :174 — shared scan/estimate, none. H4 HiveAcidUtil.getAcidState HiveScanPlanProvider :314 inside per-partition loop (:307), genuinely uncached, snapshot/write-id dependent (by design, none). H5 loadTable HiveWritePlanProvider :103 (hmsClient.getTable :353) + beginWrite :105 double-load accepted + buildExistingPartitions listPartitionNames/getPartitions :255-258, collapsed by cache + txn snapshot :549-551. H6 no per-statement resolvedTable memo (fields :203-241) — residual mid-statement TTL/eviction re-RPC race, P2, correctly characterized as not warranted.

AUTHZ: getCapabilities HiveConnector :301,341-344 returns SUPPORTS_VIEW/METADATA_PRELOAD/MVCC_SNAPSHOT — NO SUPPORTS_USER_SESSION; no per-user credential (auth via fixed keytab principal buildPluginAuthenticator :710-740 or context.executeAuthenticated :638). authz-cache-sharding gate targets ONLY IcebergConnector (TARGET_REL :57); fail-loud sibling guard :548-555; shouldBypassSchemaCache/SUPPORTS_USER_SESSION exist in fe-core PluginDrivenExternalCatalog :1179,1212. Layer-3 N/A for hive is correct.

NO MISSED SEVERE FINDING: MTMV freshness (getTableFreshness :1296-1327, getPartitionFreshnessMillis :1338-1350) reads cached collectPartitionNames + hmsClient.getPartitions — no uncached heavy op. SHOW PARTITIONS intentionally fresh (listPartitionNamesFresh :193-199, HiveConnectorMetadata :1130-1136) is interactive DDL, not a hot path. splitFile :628 shares PartitionScanInfo :707 by reference (no per-file recompute). Overall verdict 'DO NOT apply new iceberg-style caches to hive' is well-supported: every planning entrypoint sits behind a live REFRESH/TTL-invalidated cache. Confidence: high.
