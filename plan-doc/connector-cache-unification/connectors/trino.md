## 连接器附录:trino-connector(`fe/fe-connector/fe-connector-trino`)

### 定位:唯一的 LIVE pass-through bridge

trino-connector 在 8 个 SPI 连接器里独一无二:它不是自己实现元数据,而是**内嵌一个真正的 Trino Connector SPI**,把 Doris 的 metadata/scan 请求转译后委托给内嵌 Trino 连接器。链路:

- `TrinoConnectorProvider.getType()` = `"trino-connector"`,`create()` → `TrinoDorisConnector`(`TrinoConnectorProvider.java:35,40`)。
- `TrinoDorisConnector` 双检锁 `doInitialize()` 通过 `TrinoBootstrap` 加载 Trino 插件、每个 Doris catalog 造**一个** live `io.trino.spi.connector.Connector` 并 volatile 持有(`TrinoDorisConnector.java:53-57,133-188`)。
- `getMetadata()` 每次 new 一个 `TrinoConnectorDorisMetadata`(funnel 会把它 per-statement memo 掉),`getScanPlanProvider()` new `TrinoScanPlanProvider`(`TrinoDorisConnector.java:64-75`)。

**迁移状态**:`SPI_READY_TYPES` 含 `"trino-connector"`(`CatalogFactory.java:57`),经 `CatalogFactory.java:110-118` 走 SPI/`PluginDrivenExternalCatalog` 路径。**live**,非 sibling-only / dormant / legacy。**只读**:`TrinoConnectorDorisMetadata` 对 create/drop/rename/beginWrite/executeStmt 的 override 数 = 0。

### Caches 一览

| Cache | 作用域 | Key | TTL / 失效 | 位置 |
|---|---|---|---|---|
| `TrinoBootstrap.instance`(typeRegistry/handleResolver/pluginManager 插件基座) | 进程级(cross-query) | pluginDir(全 FE 唯一) | 进程生命周期,不失效 | `TrinoBootstrap.java:100,141-156` |
| `TrinoDorisConnector.trinoConnector`(每 catalog 内嵌的 live Trino Connector) | cross-query | Doris catalog | catalog 生命周期;`close()`→`shutdown()` 失效 | `TrinoDorisConnector.java:53-57,133-141,95-102` |
| **内嵌 Trino 连接器自身的元数据缓存**(CachingHiveMetastore / iceberg metadata cache / CachingJdbcClient)= Trino 表真正的跨查询元数据缓存 | metastore-client | Trino 自持 key;由连接器配置启用+定大小(如 `hive.metastore-cache-ttl`) | Trino 配置驱动(多数 Trino 连接器默认关闭) | 位于 `trinoConnector` 内部,本模块无源码;每次 `trinoConnector.getMetadata(...).getTableHandle(...)` read-through |
| `TrinoServicesProvider.catalogs` | cross-query | `CatalogHandle` | catalog 生命周期 | `TrinoServicesProvider.java:63` |
| `TrinoPluginManager.connectorFactories` | cross-query | `ConnectorName` | 进程生命周期 | `TrinoPluginManager.java:64` |
| **Doris 连接器层的 table/handle/schema 缓存** | **none** | 不存在 —— `TrinoConnectorDorisMetadata` 零 memo;pom 无 `fe-connector-cache` 依赖 | n/a | `TrinoConnectorDorisMetadata.java`(整类) |
| (fe-core 通用,非 trino 专属)`ExternalSchemaCache` + `RowCountCache` | cross-query | catalog.db.table 名;全连接器共享 | `external_cache_expire_time`,REFRESH 失效 | `PluginDrivenExternalTable.java:430,1121` |

一句话:**Doris 连接器层没有任何跨查询/每语句元数据缓存**;Trino 表的元数据缓存全部在内嵌 Trino 连接器内部,fe-core 通用的 schema/rowcount 缓存兜底跨查询重复。

### Funnel 参与情况

**已进 funnel**:`PluginDrivenScanNode`/`PluginDrivenExternalTable` 所有元数据获取均经 `PluginDrivenMetadata.get(session, connector)`(`PluginDrivenScanNode.java:206,222`;`PluginDrivenExternalTable.java:449,1126`),每语句每 catalog 只 memo 一个 `TrinoConnectorDorisMetadata`,`tools/check-fecore-metadata-funnel.sh` 全库把关。

**关键点:该 wrapper 内部零 memo**。`listDatabaseNames/listTableNames/getTableHandle/getTableSchema/applyFilter/applyProjection` 每个方法都 `beginTransaction(READ_UNCOMMITTED)` 起**一笔全新 Trino 事务** + `trinoConnector.getMetadata(...)` 再 commit(`TrinoConnectorDorisMetadata.java:101,121,153,195,273,338`);`TrinoScanPlanProvider.planScan` 再起一笔(`TrinoScanPlanProvider.java:111-113`)。`getTableHandle` 还会 eager 拉全部 column handle + 每列 `getColumnMetadata`(`:166-176`)。scope 上不 stash 任何已解析的 `TrinoTableHandle`/Trino `Table`。

所以 funnel 给出的是"每语句一个 Doris wrapper",**并不折叠 wrapper 内部的重复加载**。但残余 multiplier 低:(a) fe-core 的 `ExternalSchemaCache`/`RowCountCache` 跨查询兜底 `getTableHandle`/`getTableSchema`;(b) 每次 `getTableHandle` 又 read-through 内嵌 Trino 连接器自身的长生命周期 metastore 缓存。`metadataMemoizesLoadedTable = no`。

### Hot-path 审计(应用 problem class)

Trino wrapper 只 override 了 list/handle/schema/pushdown 这一小撮方法;`listPartitions`、`getTableStatistics`、`estimateDataSizeByListingFiles`、`getMvccPartitionView`、`getTableFreshness`、`getSyntheticScanPredicates` 以及整条写路径**全走接口默认值(空 / -1 / handle 原样返回)**。因此 iceberg 的 PERF-02(PARTITIONS 重扫)、PERF-03(format 回退)、PERF-04(manifest)、PERF-07/R6(写)在这里**没有对应热点**。

| ID | 区域 | 问题 | multiplier | cost | 严重度 | 已 memo? | 位置 |
|---|---|---|---|---|---|---|---|
| TRINO-H1 | query-plan | `getTableHandle`(唯一近似 remote 的元数据 op)在冷语句里从 3 个 fe-core 站点被调:initSchema(`PluginDrivenExternalTable.java:463`)、fetchRowCount(`:1128`)、scan create(`PluginDrivenScanNode.java:228`);wrapper 内不 memo 已解析 handle | 暖缓存 ~1x/SELECT;冷 ~3x —— 但 schema 走 ExternalSchemaCache、rowcount 走 RowCountCache,且各次 read-through Trino 自身缓存 | mixed | P2 | 是(fe-core 层) | `PluginDrivenScanNode.java:228` |
| TRINO-H2 | schema | 一次冷 schema 加载里 `getTableHandle` 已按列建好 `columnMetadataMap`(`:170-176`),`getTableSchema` 却弃之不用、每列重新 `getColumnMetadata`(`:207-209`)= 2N 次 + 第二笔 Trino 事务 | 2N getColumnMetadata / 冷 schema miss(非每查询) | cpu | P2 | 否 | `TrinoConnectorDorisMetadata.java:207` |
| TRINO-H3 | predicate | applyFilter 每次 scan 跑两遍:fe-core `convertPredicate`(`PluginDrivenScanNode.java:824`,自带一笔 Trino 事务)+ `planScan` 内再一遍(`TrinoScanPlanProvider.java:126`,另一笔事务);projection 同理 | 2x applyFilter + 2x applyProjection / scan | cpu | P2 | 否 | `TrinoScanPlanProvider.java:126` |
| TRINO-H4 | split-enum | split 枚举全委托 Trino `ConnectorSplitManager`+`BufferingSplitSource`;per-split 循环只序列化各自 split JSON + host。scan 不变字段(tableHandle/txn/columnHandles/columnMetadata/options JSON)已在循环前**一次性**预序列化 | 共享字段 1x/scan(已 hoist) | cpu | none | 是 | `TrinoScanPlanProvider.java:221-227` |
| TRINO-H5 | partitions/stats/write | 相关方法均未 override → 默认空/‑1/原样;`getNameToPartitionItems` 见空分区,`fetchRowCount` 返回 UNKNOWN;无写路径 | 0(无此调用) | cpu | none | 是 | `TrinoConnectorDorisMetadata.java:64` |

**无 P0/P1**。不存在"重 remote op × 高 multiplier × 无 memo"的 A/B/C/D 变体:唯一的 remote-ish op(`getTableHandle`)既被 fe-core 跨查询缓存兜住,又被内嵌 Trino 连接器自身缓存兜住;真正的 per-split 循环只做不可避免的 split 序列化。

### Authz / 一致性

`perUserCredential = false`,`sessionUserRelevant = false`。`TrinoBootstrap` 在 CREATE CATALOG 时把**单一静态身份** `Identity.ofUser("user")` 烤进每 catalog 的 Trino Session(`TrinoBootstrap.java:264-265`),对所有 Doris 用户复用。无 REST OIDC、无 kerberos doAs、无 per-identity metastore、无 vended per-user 凭证;内嵌 Trino 连接器用 catalog 属性里的**静态**凭证(metastore auth / 对象存储 key)访问底层。访问控制由 Doris 自身权限系统负责。

因此:**name-only 缓存不会泄漏 per-user 授权**(根本没有 per-user 维度可分片),iceberg 的 `session=user` "list ≠ load" 元数据泄漏、Layer-3 隔离与 `shouldBypassSchemaCache` 在此**N/A**。`PluginDrivenMetadata` 的 builder-identity 断言(`PluginDrivenMetadata.java:63-69`)仍跑但恒真。写一致性(读写共享一 metadata/txn)也 N/A —— 只读连接器,无写事务。

### 框架各件是否需要

- **per-statement-funnel-memoization** = partial:已进 funnel;wrapper 内部再 memo 已解析 `TrinoTableHandle` 可省掉每 SELECT 多出的 Trino 事务,但收益小(schema/rowcount 缓存 + Trino 自身缓存已兜底)。低优先。
- **cross-query-table-cache** = **no**:会与内嵌 Trino 连接器自身缓存**双重缓存**,并把外部 ALTER 后的 schema 冻住,重新引入 Trino 已解决的 stale-metadata bug;且 `TrinoTableHandle` 是事务派生 + transient/不可序列化。**禁止加**。
- **partition-cache / format-cache / comment-cache / manifest-or-file-list-cache** = no:分区(默认空)、file-format(在 Trino/BE 侧)、comment(默认 ""、随缓存 schema)、file-listing(默认 -1、split 交给 Trino)均无 FE 侧热点。
- **per-scan-hoist** = already-has(`TrinoScanPlanProvider.java:221-227` 已把 scan 不变字段循环前一次序列化)。
- **per-file-split-memo** = no:无跨 split 的 per-file 不变量可 memo。
- **authz-session-user-isolation** = no:静态单身份,无可分片缓存、无泄漏面。
- **shared-fe-connector-cache-adoption** = no:连接器层无可安全缓存之物,采纳只会制造双缓存正确性隐患。
- **write-txn-coholder** = no:只读,无写事务。

### 结论与建议

**不要**把 iceberg 热点缓存框架套到 trino-connector。它是委托型 bridge,元数据缓存 / 分区&stats / split 枚举 / 缓存失效全部由内嵌 Trino 连接器负责;跨查询重复由 fe-core 通用 `ExternalSchemaCache`/`RowCountCache` + Trino 自身缓存两层兜底。Layer-1 连接器缓存与 Layer-3 授权隔离在此**反而有害**(双缓存 + stale 风险;无 per-user 维度);Layer-2 funnel 已接好且被 gate 强制,残余 multiplier 低。

优先级:**维持现状**(委托正确)。若某个热 catalog 的 BI profile 真的暴露出来,再排期三个 P2 纯 CPU 清理:(1)per-statement memo 已解析 `TrinoTableHandle` 以消掉多余 Trino 事务;(2)冷 schema 加载去掉 `getTableHandle`/`getTableSchema` 的 2N `getColumnMetadata` 重复;(3)消除 `convertPredicate` 与 `planScan` 之间的双重 applyFilter/applyProjection。

---

## 复核结论 (adversarial verify)

**Verdict: CONFIRMED**  ·  确认发现 IDs: TRINO-H1, TRINO-H2, TRINO-H3, TRINO-H4, TRINO-H5

| 原始声明 | 问题 | 更正 | 证据 |
|---|---|---|---|
| Cache #4 'TrinoServicesProvider.catalogs' keyedBy: CatalogHandle | The map is actually keyed by the catalog-name String, not a CatalogHandle object; lookups derive the name from the handle. | TrinoServicesProvider.java:63 declares `ConcurrentMap<String, CatalogConnector> catalogs`; getConnectorServices(CatalogHandle) looks up via `catalogs.get(catalogHandle.getCatalogName())` (line 126). One entry per catalog (correct), but keyed by name String. Non-material. | fe/fe-connector/fe-connector-trino/src/main/java/org/apache/doris/connector/trino/TrinoServicesProvider.java:63,126 |
| TRINO-H3: '2x applyFilter per scan' | Precisely 2x only for a scan carrying a pushable predicate. For a filterless scan the fe-core convertPredicate arm short-circuits (guarded by empty conjuncts, and by TupleDomain.isAll() in the wrapper), so only planScan's applyFilter (with Constraint.alwaysTrue) runs = 1x. | convertPredicate returns early when conjuncts empty (PluginDrivenScanNode.java:819-820); TrinoConnectorDorisMetadata.applyFilter returns empty before opening a txn when tupleDomain.isAll() (TrinoConnectorDorisMetadata.java:266-268); planScan always calls Trino applyFilter (TrinoScanPlanProvider.java:126). So '2x' holds for the WHERE-filtered case the finding targets; overstated only for filterless scans. Finding stands. | fe/fe-core/src/main/java/org/apache/doris/datasource/scan/PluginDrivenScanNode.java:819 |
| authzConsistency: the PluginDrivenMetadata builder-identity pin is 'vacuous for Trino since the identity is constant' | The pin keys on the Doris principal session.getUser(), not the constant Trino Identity.ofUser("user"); it does vary per Doris user across statements. It is trivially satisfied WITHIN one statement (one statement = one user), which is why it never fires for trino — not because the identity is constant. | PluginDrivenMetadata.java:63-69 uses session.getUser() (Doris principal). The static Trino identity is a separate layer (TrinoBootstrap.java:264-265). Conclusion (no per-user leak) is correct; the stated reason conflates the two identity layers. Non-material. | fe/fe-core/src/main/java/org/apache/doris/datasource/plugin/PluginDrivenMetadata.java:63 |

> 复核备注：Verified against HEAD aaab68ef474 (branch branch-catalog-spi). Every material claim holds; the 3 corrections above are minor/non-material and do not undermine the audit's core.

CONFIRMED facts with file:line (HEAD):
- Migration: CatalogFactory.java:57 (SPI_READY_TYPES includes "trino-connector"), :110-118 routing to PluginDrivenExternalCatalog. TrinoConnectorProvider.java:35-36 getType()="trino-connector", :40-41 create()->TrinoDorisConnector. TrinoDorisConnector implements Connector (:45), double-checked-lock init (:133-141), embedded live trino Connector volatile (:53-57), created via TrinoBootstrap (:143-188), shutdown on close (:95-102). getMetadata() returns a FRESH TrinoConnectorDorisMetadata every call (:65-69). LIVE bridge — CONFIRMED, not sibling/dormant/legacy.
- Wrapper memoizes nothing: TrinoConnectorDorisMetadata every method opens beginTransaction(READ_UNCOMMITTED)+getMetadata+commit-in-finally: listDatabaseNames :100-101, listTableNames :120-121, getTableHandle :152-153, getTableSchema :194-195, applyFilter :272-273, applyProjection :337-338. metadataMemoizesLoadedTable=no CONFIRMED.
- Funnel: PluginDrivenMetadata.get memoizes one ConnectorMetadata per (statement,catalog) at :70, identity pin :63-69. fe-core sites route through it: PluginDrivenScanNode.java:206 (metadata()), :222 (create), :825 (convertPredicate applyFilter); PluginDrivenExternalTable.java:449 (initSchema), :1126 (fetchRowCount). Both gate scripts present: tools/check-fecore-metadata-funnel.sh, tools/check-authz-cache-sharding.sh. Only Connector#getMetadata(session) caller in fe-core is inside PluginDrivenMetadata (the other .getMetadata hits are TestExternalCatalog's Map getter). CONFIRMED.
- Caches: TrinoBootstrap.instance singleton keyed by pluginDir with mismatch guard (:100,141-156); TrinoServicesProvider.catalogs (:63, String-keyed); TrinoPluginManager.connectorFactories (:64, ConnectorName-keyed). Module-wide grep shows NO LoadingCache/Caffeine/fe-connector-cache — only those two Trino-infra ConcurrentHashMaps. Doris-layer metadata cache = NONE CONFIRMED. fe-core ExternalSchemaCache filled by initSchema (getTableSchema :469), RowCountCache read at getCachedRowCount :907-908.

Hot-path findings (all independently CONFIRMED):
- H1: getTableHandle is the only remote-ish op; invoked from initSchema (PluginDrivenExternalTable:463), fetchRowCount (:1128), scan create (PluginDrivenScanNode:228), all via resolveConnectorTableHandle->metadata.getTableHandle (:119). No resolved-handle memo. ~1x warm / ~3x cold. P2. Multiplier fair.
- H2: getTableHandle builds columnMetadataMap by per-column getColumnMetadata (TrinoConnectorDorisMetadata.java:170-176); getTableSchema ignores that map and re-loops getColumnMetadata per column (:207-209) => 2N + a second Trino txn. CONFIRMED, P2 cpu, not memoized.
- H3: applyFilter runs in fe-core convertPredicate (PluginDrivenScanNode:825 -> wrapper applyFilter, own txn) AND in planScan (TrinoScanPlanProvider:126, direct on Trino metadata); applyProjection in tryPushDownProjection (:1264 -> wrapper) AND planScan (:144). Predicate re-conversion via new TrinoPredicateConverter each time (:262-265 and :267-270). CONFIRMED (see correction re: filterless-case count).
- H4: scan-invariant fields pre-serialized ONCE before the split loop (TrinoScanPlanProvider:221-227, serializer built once :217-219); per-split loop (:235-256) serializes only each split JSON + hosts. severity none CONFIRMED.
- H5: no override of listPartitions/getTableStatistics/estimateDataSizeByListingFiles/getMvccPartitionView/getTableFreshness/getSyntheticScanPredicates/getTableComment or any ConnectorWriteOps method. Interface defaults verified: getTableComment->"" (ConnectorTableOps:336-339), listPartitions->emptyList (:400-405), getTableStatistics->empty (ConnectorStatisticsOps:32-36), estimateDataSizeByListingFiles->-1 (:62-64), ConnectorWriteOps all default/read-only. fetchRowCount->UNKNOWN. severity none CONFIRMED. No PERF-02/03/04/07 analog.
- Authz: TrinoBootstrap bakes static Identity.ofUser("user") into setIdentity/setOriginalIdentity (:264-265); no REST OIDC / kerberos doAs / per-user credential. No per-user cache dimension to leak. CONFIRMED.

No missed severe (P0/P1) finding: the single remote-ish op (getTableHandle) is caught cross-query by ExternalSchemaCache/RowCountCache and read-through by the embedded Trino connector's own cache; the genuine per-split loop does only unavoidable per-split serialization. overallVerdict (do-not-apply-framework; optional 3 P2 cleanups) is sound.
