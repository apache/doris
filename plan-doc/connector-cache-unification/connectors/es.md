## 连接器附录:es (Elasticsearch, fe-connector-es)

### 结论摘要

`es` 属于**读路径为主、无写入、无分区、单一凭证**的轻量连接器。它已经在 `SPI_READY_TYPES` 中,`EsConnectorMetadata` 已被 Layer-2 per-statement funnel 正确路由,但**连接器自身没有任何扫描元数据缓存**。iceberg 那套重型框架(Table/Partition/Format/Comment/Manifest cache、authz session=user 隔离、write-txn co-holder、per-file memo)**几乎全部不适用**。唯一真正值得做的是一个 **per-scan 的 `EsMetadataState` hoist**(把 shard routing + node 拓扑 + field-context 在一次扫描内解析一次,而不是两次),外加让扫描路径复用已被 `ExternalSchemaCache` 缓存的 mapping。都是 P1/P2 级别的**常数倍(2–4×)**冗余,而非 iceberg 那种 per-file 循环放大的 P0。

### 迁移状态

- `SPI_READY_TYPES` 含 `"es"`:`CatalogFactory.java:56-57`,createCatalog 走 SPI 路径(`:110` → `ConnectorFactory.createConnector` → `PluginDrivenExternalCatalog`)。
- `EsConnectorProvider.getType()=="es"`(`EsConnectorProvider.java:34`),经 `META-INF/services` 发现。
- **liveness = live**:legacy fe-core `EsExternalCatalog/EsExternalDatabase/EsExternalTable/EsScanNode` 已在 commit `4f455da5950` 删除,`BUILD_CONNECTOR_ES` 默认由 0 翻为 1;fe-core 仅残留与之无关的内表 `EsTable.java`/`EsResource.java` 与 `EsQuery` 函数。连接器是唯一活路径。

### 现有缓存(caches table)

| 缓存 | scope | key | TTL / 失效 | 位置 |
|---|---|---|---|---|
| `EsConnector.restClient`(REST 连接对象,DCL 懒建) | metastore-client | 无(每 catalog 一个) | catalog 生命周期,不失效 | `EsConnector.java:43,82-107` |
| `EsConnectorRestClient.PLAIN_CLIENT / sslClient`(共享 OkHttp) | metastore-client | 无(JVM 全局) | 进程生命周期 | `EsConnectorRestClient.java:61-63,310-319` |
| Layer-2 funnel:每 statement 记忆一个 `EsConnectorMetadata` | per-statement | `"metadata:"+catalogId`,`getUser` 身份钉死 | 一条语句;NONE 不记忆 | `PluginDrivenMetadata.java:64,70` |
| `ResolvedScanProvider`:每 scan node 一个 `EsScanPlanProvider` | per-scan | `currentHandle` identity | scan node 生命周期 | `PluginDrivenScanNode.java:254-259` |
| fe-core `ExternalSchemaCache` 包住 `initSchema()→getTableSchema()→getMapping()` | cross-query | `SchemaCacheKey`(index) | schema cache 过期 / REFRESH | `PluginDrivenExternalTable.java:430-470`;`ExternalTable.java:385-445` |
| fe-core `RowCountCache` 包住 `fetchRowCount()`(ES 无 stats → UNKNOWN,无 ES 侧重 IO) | cross-query | table id | row-count cache 过期 | `PluginDrivenExternalTable.java:907,1121-1133` |
| **连接器专用扫描元数据缓存(mapping/shard/node)** | **none** | **不存在** | — | `EsScanPlanProvider.java:99,169,273-294`(每次重取) |

### Funnel 参与度

`EsConnectorMetadata` 只经 `PluginDrivenMetadata.get(session, connector)` 获取(`PluginDrivenExternalTable.java:449,…,1277`;`PluginDrivenScanNode.java:1918-1920`),**确实被 funnel 路由**(每语句一个实例,arch gate 满足)。但该实例**不记忆任何东西**:

- `getTableSchema`(`EsConnectorMetadata.java:81-94`)每次远程 `restClient.getMapping()`;`getColumnHandles`(`:97-106`)再调 `getTableSchema` → 第二次远程 mapping。
- 更重的扫描态**根本不在 funneled metadata 上**:它在 `EsScanPlanProvider` 这个**独立对象**里(`EsConnector.getScanPlanProvider()` 每次 new,`EsConnector.java:56-58`;仅被 `ResolvedScanProvider` 每 scan node 记忆一次)。`planScan`(`:99`)与 `buildScanNodeProperties`(`:169`)各自独立 `fetchMetadataState`(`:273-294`)→ `getMapping + searchShards + getHttpNodes`。

因此 funnel 的"每语句一份 metadata"**并未**折叠 ES 的重复远程加载。`metadataMemoizesLoadedTable = no`。

### 热点重复加载审计(hot-path findings table)

| id | area | 问题 | 倍数 | cost | 严重度 | 已记忆? | 位置 |
|---|---|---|---|---|---|---|---|
| ES-F1 | split-enum | `fetchMetadataState` 每查询 2 次(`planScan` `:99` + `buildScanNodeProperties` `:169`),每次 `searchShards` + `getHttpNodes`(`NODES_DISCOVERY_DEFAULT=true`)+ `getMapping`;同一 shard 路由/节点拓扑在一次 planning pass 内取两遍。provider 已是每 scan node 单实例,加个 `(index,columnNames)` 字段 memo 即 2→1,零陈旧风险 | 2×/查询 | remote-io | **P1** | 否 | `EsScanPlanProvider.java:99,169,273-294`;`EsMetadataFetcher.java:51-89` |
| ES-F2 | predicate | `fetchMapping`(`EsMetadataFetcher.java:57-58` → `getMapping` `RestClient:161`)取的 index mapping 已在 schema 解析时被 `ExternalSchemaCache` 缓存;扫描路径不复用而重新远程取 | ~2×/查询 | remote-io | **P1** | 否 | `EsMetadataFetcher.java:57-58`;`EsConnectorRestClient.java:161-168` |
| ES-F3 | schema | `buildColumnHandles→getColumnHandles→getTableSchema→getMapping`(裸远程,绕过 schema cache);`buildColumnHandles` 每查询≥2 次(`PluginDrivenScanNode.java:1263` getSplits、`:1841` getOrLoadPropertiesResult)。funneled metadata 是每语句单实例,在其上记忆 schema 即可折叠到 1× | 2×/查询 | remote-io | P2 | 否 | `EsConnectorMetadata.java:81-106`;`PluginDrivenScanNode.java:1263,1841,1917-1920` |
| ES-F4 | file-list | `getTableHandle→existIndex` 每次解析 handle 发一次 `index/_mapping` GET(`EsConnectorMetadata.java:74`;`RestClient:104-114`);轻量、row-count 侧有 RowCountCache;仅列完整性,非热循环 | 1×/handle | remote-io | P2 | 否 | `EsConnectorMetadata.java:71-78`;`EsConnectorRestClient.java:104-114` |

合计:一条普通 `SELECT ... WHERE ...`(schema cache 命中的暖态)对同一 ES 集群大约发出 **~4× mapping + 2× search_shards + 2× _nodes/http** 远程往返(每次 OkHttp 10s read timeout),其中 mapping 完全冗余(已被 schema cache 缓存),shard/node 在语句内 2× 冗余。

**没有 iceberg 式的 per-file/per-split 循环放大**:`planScan` 从内存中的 shard 路由 map 简单循环生成 scan range(`EsScanPlanProvider.java:113-138`),循环内无远程调用。所以是 variant B/D(单链重复 k 次 / 循环不变量未提升),不是 variant A。

### Authz / 一致性

- `perUserCredential = false`、`sessionUserRelevant = false`。ES 用**单一 catalog 级 user/password**构建共享 `EsConnectorRestClient`(`EsConnector.java:95-100`;`EsConnectorRestClient.java:76-78` 一个 Basic auth header),无 `iceberg.rest.session=user`、无 kerberos doAs、无 REST/OIDC 每身份委派——所有 Doris 用户以同一身份访问 ES。因此 **name-only 缓存不会造成 list≠load 元数据泄露**,`tools/check-authz-cache-sharding.sh` 的 session=user 隔离对本连接器 **N/A**。
- 唯一一致性关切是**数据新鲜度**:ES shard 会 rebalance(ES 自身 refresh 模型),shard/node 拓扑必须每语句解析、**不得跨查询缓存**——这与被删的 legacy `EsScanNode`/`EsMetaStateTracker` 每次扫描重解析 shard 一致。
- **无写路径**(`EsConnectorMetadata` 只覆写 list/exists/getTableHandle/getTableSchema/getColumnHandles/buildTableDescriptor;无 beginWrite/commit/insert/sink),故无"读写共享一份 metadata/一个 txn"的需求;ES 只读且非分区(未覆写 listPartitions),无 snapshot/version 钉需求。

### 框架各件需求

| piece | 需要 | 理由 |
|---|---|---|
| per-scan-hoist | **yes** | 唯一真正收益:把 `EsMetadataState` 每扫描解析一次(ES-F1)。provider 已单实例,字段 memo 即可,零陈旧风险。 |
| per-statement-funnel-memoization | partial | funnel 已路由且 arch gate 满足,但连接器未利用:schema/getColumnHandles 仍每次远程取(ES-F3),重扫描态在 funnel 覆盖不到的独立 provider 上。 |
| cross-query-table-cache | partial | mapping 已被 `ExternalSchemaCache` 跨查询缓存,扫描路径复用即可(ES-F2);无需新建连接器 Table cache;shard/node **禁止**跨查询缓存。 |
| shared-fe-connector-cache-adoption | no | 连接器未用 fe-connector-cache,且推荐修复是 per-statement/per-scan memo 而非托管跨查询缓存,无需引入该 toolkit。 |
| partition-cache | no | ES 单索引非分区,未覆写 listPartitions;无 PARTITIONS/SHOW PARTITIONS 重路径。 |
| format-cache | no | 格式固定常量 `es_http`(`EsScanPlanProvider.java:174`),无 iceberg PERF-03 的整表 planFiles 回退。 |
| comment-cache | no | 未覆写 getComment,无 N-per-query 远程 comment 加载。 |
| manifest-or-file-list-cache | no | 无 manifest/文件清单;shard 取数是结构类比但新鲜度敏感,归入 per-scan-hoist。 |
| per-file-split-memo | no | scan range 从内存 shard map 循环生成,循环内无远程调用。 |
| authz-session-user-isolation | no | 单凭证,无 session=user/OIDC/kerberos。 |
| write-txn-coholder | no | 只读连接器。 |

### 优先级建议

1. **(P1)ES-F1 per-scan hoist**:在 `EsScanPlanProvider` 上按 `(indexName, columnNames)` 记忆一次 `EsMetadataState`,让 `planScan` 与 `getScanNodePropertiesResult` 共享同一次 shard/node 解析。这是最干净、零风险、收益最大的改动(shard 取数 2→1、node 取数 2→1)。
2. **(P1)ES-F2 复用已缓存 mapping**:扫描路径的 field-context 从已被 `ExternalSchemaCache` 缓存的 schema 派生,消除扫描侧的冗余 mapping 远程取,避免重复 `getMapping`。
3. **(P2)ES-F3 metadata schema memo**:在每语句单实例的 `EsConnectorMetadata` 上记忆已解析 schema,使 `getColumnHandles` 折叠到 1×。
4. **不做**:跨查询 shard 缓存(违背 ES rebalance/refresh 模型)、authz 隔离、write-txn、partition/format/manifest/comment 缓存。

---

## 复核结论 (adversarial verify)

**Verdict: ADJUSTED**  ·  确认发现 IDs: ES-F1, ES-F2, ES-F3, ES-F4

| 原始声明 | 问题 | 更正 | 证据 |
|---|---|---|---|
| frameworkPiecesNeeded.cross-query-table-cache + ES-F2 fix rationale: 'Serving the scan-path field-context from the already-cached schema removes the redundant cross-query mapping fetch WITHOUT a new cache' / 'reuse the cached schema'. | The fe-core ExternalSchemaCache stores only the parsed ConnectorTableSchema (column list). EsConnectorMetadata.getTableSchema builds `new ConnectorTableSchema(indexName, columns, "ELASTICSEARCH", Collections.emptyMap())` (EsConnectorMetadata.java:90-93) — an EMPTY properties map; the raw mapping JSON is discarded. But EsMetadataFetcher.fetchMapping needs the raw mapping (keyword-sniff / doc_value / date-compat) via EsMappingUtils.resolveFieldContext(columnNames, sourceIndex, indexMapping, mappingType) (EsMetadataFetcher.java:57-63). So the scan-path field context CANNOT be derived from the currently cached schema value. | The redundant remote mapping fetch (ES-F2) is REAL and the multiplier holds, but the proposed fix is understated: eliminating it requires either enriching ConnectorTableSchema to carry the raw mapping/field-context, or adding a per-statement/cross-query mapping (field-context) cache — i.e. it is NOT a zero-new-cache 'just reuse the schema' change. Files: fe/fe-connector/fe-connector-es/src/main/java/org/apache/doris/connector/es/EsConnectorMetadata.java:90-93; EsMetadataFetcher.java:57-63; EsMappingUtils.resolveFieldContext. | fe/fe-connector/fe-connector-es/src/main/java/org/apache/doris/connector/es/EsConnectorMetadata.java |
| currentCaches RowCountCache row: 'ES has no stats -> returns UNKNOWN, no ES-side remote heavy op'. | fetchRowCount (PluginDrivenExternalTable.java:1121-1133) first calls resolveConnectorTableHandle -> EsConnectorMetadata.getTableHandle -> restClient.existIndex (a remote `index/_mapping` GET, EsConnectorRestClient.java:104-114) BEFORE it can return UNKNOWN. EsConnectorMetadata does not override getTableStatistics (confirmed by grep), so getTableStatistics itself makes no ES RPC, but the handle-resolution step does perform one lightweight remote GET. | The phrase is defensible for 'heavy' op (there is none), but 'no ES-side remote op' would be wrong: the row-count seam still issues one lightweight existIndex `_mapping` GET during handle resolution (this is the same op as ES-F4). It sits behind RowCountCache so it is not per-query hot; precision note only. File: fe/fe-core/src/main/java/org/apache/doris/datasource/plugin/PluginDrivenExternalTable.java:1121-1133. | fe/fe-core/src/main/java/org/apache/doris/datasource/plugin/PluginDrivenExternalTable.java |

> 复核备注：CORE STANDS — every material claim verified against HEAD (aaab68ef474). Corrections are limited to fix-feasibility/wording nuances; no finding, multiplier, severity, or memoized-status is refuted.

MIGRATION STATUS — CONFIRMED. SPI_READY_TYPES includes "es" at CatalogFactory.java:56-57 (ImmutableSet.of("jdbc","es","trino-connector","max_compute","paimon","iceberg","hms")); SPI routing at ~line 110. EsConnectorProvider.getType() returns "es" (line 34). Legacy fe-core EsExternalCatalog/EsExternalDatabase/EsExternalTable/EsScanNode are ABSENT (find returned nothing); internal-catalog EsTable.java/EsResource.java remain. Connector is sole live ES path. (Did not locate BUILD_CONNECTOR_ES in pom.xml/build.sh — a peripheral historical detail, not required for any technical claim.)

CACHES — CONFIRMED. EsConnector.restClient DCL memo (EsConnector.java:43,82-107); static PLAIN_CLIENT/sslClient with 10s readTimeout (EsConnectorRestClient.java:61-63,310-319); single Basic authHeader built once (76-78). Layer-2 funnel memo keyed "metadata:"+catalogId + identity pin "metadata-identity:"+catalogId via getUser (PluginDrivenMetadata.java:64,70). ResolvedScanProvider memo keyed on currentHandle identity (PluginDrivenScanNode.java:254-256, class at 267). RowCountCache at PluginDrivenExternalTable.java:907. No dedicated connector scan-metadata cache — confirmed absent (EsScanPlanProvider re-fetches).

FUNNEL PARTICIPATION — CONFIRMED (funneled=true, metadataMemoizesLoadedTable=no). EsConnectorMetadata holds only restClient+properties; getTableSchema (81-94) calls restClient.getMapping every call; getColumnHandles (97-106) re-invokes getTableSchema -> 2nd remote GET; no memo fields. EsScanPlanProvider is a separate object (EsConnector.java:56-58 returns new provider each call), memoized only per scan node.

HOT-PATH FINDINGS — all four CONFIRMED:
- ES-F1 (P1, 2x, not memoized): fetchMetadataState called from planScan (EsScanPlanProvider.java:99, reached via getSplits->planScan at PluginDrivenScanNode.java:1303) AND from buildScanNodeProperties (line 169, reached via getScanNodePropertiesResult at PluginDrivenScanNode.java:1863). getOrLoadPropertiesResult IS cached (cachedPropertiesResult, line 1838/1870), so the second path fires exactly once — 2x total, constant, verified. Each fetch = fetchMapping+searchShards+getHttpNodes (EsMetadataFetcher.java:51-89); NODES_DISCOVERY_DEFAULT="true" (EsConnectorProperties.java:38) so getHttpNodes really runs. planScan loop (113-138) is over an in-memory routing map with no remote call — correctly characterized as variant B/D, not loop-amplified.
- ES-F2 (P1, ~2x): fetchMapping->getMapping (EsMetadataFetcher.java:57-58; RestClient:161-168) duplicates the schema-resolution mapping fetch. Real; see correction on fix scope.
- ES-F3 (P2, 2x): buildColumnHandles (PluginDrivenScanNode.java:1917-1920) -> getColumnHandles -> getTableSchema -> raw getMapping, bypassing ExternalSchemaCache; buildColumnHandles runs at 1263 (getSplits) and 1841 (getOrLoadPropertiesResult) — 2x confirmed.
- ES-F4 (P2, 1x/handle): getTableHandle->existIndex `_mapping` GET (EsConnectorMetadata.java:74; RestClient:104-114). Confirmed low-cost.

Per-SELECT remote tally (~4x getMapping + 2x searchShards + 2x _nodes/http) reproduces from the code. No missed P0: no heavy op sits in a per-split/per-file loop, so the audit does not UNDERSTATE anything — worst case is constant-factor.

AUTHZ — CONFIRMED. Single catalog-level Basic credential (EsConnector.java:95-100; RestClient:76-78); no session=user/OIDC/kerberos. EsConnectorMetadata overrides no write/partition/comment/stats methods (grep for getTableStatistics/listPartitions/getTableComment/beginWrite/applyFilter returned nothing) — read-only, non-partitioned confirmed. authz-session-user-isolation N/A is correct. Shard/node freshness rationale (do NOT cross-query cache) is sound.
