## 连接器附录:jdbc(fe-connector-jdbc,catalog type `jdbc`)

### 结论速览
`jdbc` 是**关系型透传(relational passthrough)**连接器:FE 只负责元数据发现 + 生成远程 SQL,真正的数据扫描由 BE 的 `JdbcJniReader` 直接对远端库执行。它**没有** iceberg 那套 split / file / manifest / partition / snapshot 规划层,所以 iceberg 热路径缓存框架(cross-query Table/Partition/Format/Manifest cache)对它基本无对应目标。其昂贵远程元数据早已被 **fe-core 跨查询缓存**前置,连接池是 **per-catalog 单例**。审计结论:**不套用 iceberg 式连接器缓存**;只存在两处 P2 级冗余远程取列,适合用轻量 per-statement memo 收口。

Migration:`jdbc` ∈ `SPI_READY_TYPES`(`CatalogFactory.java:57`),`JdbcConnectorProvider.getType()=="jdbc"`(`JdbcConnectorProvider.java:41`),经 SPI 路由到 `PluginDrivenExternalCatalog`。legacy fe-core `JdbcExternalCatalog/JdbcExternalTable` 已删;残留 fe-core `datasource/jdbc/client/JdbcClient` 仅服务 CDC/streaming-job 校验,不在 catalog 查询热路径。**LIVE**。

### 缓存与记忆一览

| 缓存 / 记忆 | 层 | scope | key | TTL / 失效 | file:line | 前置了哪个连接器远程调用 |
|---|---|---|---|---|---|---|
| HikariDataSource 连接池 | 连接器 | cross-query(per-catalog 单例) | 每 client 一池 | connector 生命周期,`close()` 销毁 | `JdbcConnectorClient.java:89` / `JdbcDorisConnector.java:188` | 所有远程 round-trip 的实际资源复用点 |
| CLASS_LOADER_MAP 驱动 classloader | 连接器 | 进程级 cross-query | driver URL | 永不淘汰(986696 Metaspace 泄漏修复) | `JdbcConnectorClient.java:78,268` | 驱动加载 |
| OceanBase compat-mode delegate | 连接器 | cross-query(per-client volatile) | 单 delegate | client 生命周期 | `JdbcOceanBaseConnectorClient.java:45,59` | `ob_compatibility_mode()` 方言探测(一次) |
| ExternalSchemaCache | fe-core | cross-query | `SchemaCacheKey(table)` | expire-after-access + refresh/DDL | `ExternalMetaCacheMgr.java:365` / `ExternalTable.java:447` | `getTableSchema`(via `initSchema`,`PluginDrivenExternalTable.java:469`) |
| ExternalRowCountCache | fe-core | cross-query | `RowCountKey(catalog,db,table)` | expireAfterWrite ~1 天,异步 | `ExternalRowCountCache.java:45` | `getTableStatistics`(via `fetchRowCount`,`:1133`) |
| MetaCache 名录/对象 | fe-core | cross-query | db 名 / (db,table) 名 | expire-after-access + refresh | `metacache/MetaCache.java:65` | `listDatabaseNames`/`listTableNamesFromRemote`(`PluginDrivenExternalCatalog.java:293,310`) |

**关键事实**:连接器模块(`fe-connector-jdbc`)自身**没有任何元数据缓存**(grep `Cache/memo/Caffeine/LoadingCache` 在连接器代码零命中)。上表前三行是资源/方言单例,后三行是 fe-core 缓存。`JdbcConnectorMetadata` 无状态,每次调用都原样转发 `JdbcConnectorClient` 做一次新的 JDBC 元数据 round-trip(`JdbcConnectorMetadata.java:119/162/144`)。

### Funnel 参与

- **已 funnel**:所有 fe-core 读 seam 经 `PluginDrivenMetadata.get` 取 metadata(`initSchema:449`、`fetchRowCount:1126`、`getComment:923`、`PluginDrivenScanNode` 的 `metadata()`、`JdbcQueryTableValueFunction:53`)。
- **metadata 不 memo loaded table**:`JdbcConnectorMetadata` 只持 `client+properties` 引用,`getTableSchema/getColumnHandles/getTableStatistics` 每次都远程取,内部无 memo。JDBC 也**没有**可缓存的昂贵 `Table` 对象。
- **funnel 对 JDBC 收益有限**:funnel 保证「每语句一个 metadata 实例」,但该实例构造近乎零成本,真正昂贵的连接池已是 per-catalog 单例;funnel 唯一有用之处是提供了一个活的 per-statement scope,可用来 memo `getColumnHandles`——但目前没用上。cross-statement 缓存加载器(`buildCrossStatementSession`,`:1153`)显式用 `ConnectorStatementScope.NONE`,把 schema/rowcount/名录的跨查询缓存契约固化,合理。

### 热路径重复取数审计

| # | 入口 | 重复取的信息 | 倍率 | 代价 | 已 memo | 严重度 | file:line |
|---|---|---|---|---|---|---|---|
| HP-1 | scan 规划 `buildColumnHandles` | `getColumnHandles`→`getJdbcColumnsInfo`(远程 `DatabaseMetaData.getColumns`;MySQL 追加 information_schema) | 每 scan node ~1-2 次(getSplits/startSplit 二选一 + getOrLoadPropertiesResult) | remote-io | 否 | **P2** | `JdbcConnectorMetadata.java:162`;`PluginDrivenScanNode.java:1917` |
| HP-2 | write 整形 `buildInsertSql` | `new JdbcConnectorMetadata(...)`(**绕开 funnel**)→`getColumnHandles`→远程取列 | 每 INSERT 1 次;EXPLAIN INSERT 2 次 | remote-io | 否 | **P2** | `JdbcWritePlanProvider.java:136` |
| HP-3 | SHOW/DESC `getComment` | `getTableComment` 无缓存直连(MySQL 远程 INFORMATION_SCHEMA) | 每 SHOW/DESC 1 次(非查询规划热路径) | remote-io | 否 | P2 | `PluginDrivenExternalTable.java:925` |
| HP-4 | split 枚举 `planScan` | (阴性)零远程 IO,恒 1 个 scan range,纯 SQL 拼装 | 1(常量) | cpu | 是 | none | `JdbcScanPlanProvider.java:66,152` |
| HP-5 | row-count | (阴性)`getTableStatistics` 被 ExternalRowCountCache 吸收;base 返回 -1 | 缓存命中后 0 | remote-io | 是 | none | `PluginDrivenExternalTable.java:1133` |

**分析**:HP-1/HP-2 是同一根问题的两面——列句柄解析(local→remote 名映射)在扫描期和写整形期各自重新远程取列,而这些列名+remote 名早已随 `getTableSchema` 进了 `ExternalSchemaCache`(两者底层同为 `getJdbcColumnsInfo`)。这是 problem-class 的 **variant C(缓存存在但热路径绕过)**。但要克制:`getJdbcColumnsInfo` 是一次「cheap-ish」的 JDBC 元数据 round-trip,倍率是 per-scan-node / per-write(非 per-split/per-file——JDBC 恒 1 split),绝对成本与放大都远小于 iceberg 的 manifest/planFiles。故 **P2**。HP-2 额外坏在自构实例、连 funnel 记忆的实例都不复用。

### Authz / 一致性

JDBC 用 **catalog 固定 user/password**(`JdbcConnectorProperties.USER/PASSWORD`),烘焙进 HikariCP 与 BE 的 `TJdbcTable`。**无 per-user 凭证、无 SUPPORTS_USER_SESSION**(`JdbcDorisConnector.java:102-105 只有 PASSTHROUGH_QUERY + METADATA_PRELOAD`),无 kerberos doAs / REST OIDC / per-identity metastore。所有身份共享同一远程用户 → name-only 缓存**不会**泄漏 can-LIST-cannot-LOAD 式 per-user 元数据,不需要 iceberg 的 `isUserSessionEnabled()` 缓存置空或 `shouldBypassSchemaCache` 分片。写侧 BE 逐行 auto-commit、`beginTransaction` 返回 `NoOpConnectorTransaction`(`JdbcConnectorMetadata.java:286`),读写无需共享 txn/snapshot。

### 框架各件需求判定

| 框架件 | 需要? | 理由 |
|---|---|---|
| per-statement-funnel-memoization | **partial** | 唯一真适用件:HP-1/HP-2 的 `getColumnHandles` 冗余远程取列。补救 = 在活 scope 上 memo,或(更优)让 fe-core `buildColumnHandles` 直接从已缓存的 `PluginDrivenSchemaCacheValue` 派生列句柄。P2,低优先。 |
| cross-query-table-cache | no | 无昂贵 loaded Table;schema/rowcount/名录已由 fe-core 跨查询缓存前置。 |
| partition-cache | no | 无分区,恒 1 scan range。 |
| format-cache | no | 无文件格式解析(关系型)。 |
| comment-cache | no | HP-3 存在但仅 SHOW/DESC,非查询热路径,价值边际。 |
| manifest-or-file-list-cache | no | 无 manifest/文件清单;无 file-list row-count 路径。 |
| per-scan-hoist | no | planScan 已 O(1) 零远程 IO,无 loop-invariant 可外提。 |
| per-file-split-memo | no | 恒 1 split,无 per-file 不变量。 |
| authz-session-user-isolation | no | 固定凭证,name-only 缓存不泄漏。 |
| shared-fe-connector-cache-adoption | no | 连接器无跨查询元数据缓存可迁移。 |
| write-txn-coholder | no | BE auto-commit,无 FE 读写共享 txn。 |

### 优先级建议

1. **(可选,P2)** 收口 HP-1/HP-2 的列句柄冗余远程取:最干净的做法是**在 fe-core 让 `buildColumnHandles`/写路径从已跨查询缓存的 `PluginDrivenSchemaCacheValue`(已含 columns + remote 名)派生列句柄**,对所有连接器都零远程 IO——这是框架级小改,而非 jdbc 专属缓存;退一步可在 `JdbcConnectorMetadata` 上按 `session.getStatementScope()` memo `getJdbcColumnsInfo` 结果。收益小、风险低,非阻塞。
2. **不新增任何 iceberg 式连接器缓存**(Rule 2 简单优先):JDBC 无对应的重复 loadTable/manifest/planFiles 目标,新增缓存纯属投机且会引入与固定凭证无关的失效/一致性负担。
3. HP-3(comment)可暂不处理——off hot-path。

Confidence:high(全部锚定 HEAD file:line;e2e 未跑,判定基于代码结构与调用图)。

---

## 复核结论 (adversarial verify)

**Verdict: CONFIRMED**  ·  确认发现 IDs: HP-1, HP-2, HP-3, HP-4, HP-5

| 原始声明 | 问题 | 更正 | 证据 |
|---|---|---|---|
| jdbc 在 CatalogFactory.SPI_READY_TYPES 中 (CatalogFactory.java:57) | Line-number anchor slightly off. The claim itself is TRUE, but the ImmutableSet containing "jdbc" is not at line 57. | At HEAD the declaration `private static final Set<String> SPI_READY_TYPES =` is on line 56 and the `ImmutableSet.of("jdbc", "es", "trino-connector", "max_compute", "paimon", "iceberg", "hms")` literal is on line 63-64 of fe/fe-core/src/main/java/org/apache/doris/datasource/CatalogFactory.java. Membership is confirmed either way. |  |
| ExternalSchemaCache TTL = expire-after-access(external_cache_expire_time_minutes_after_access) | Config key name imprecise. | The actual Config field seen at ExternalRowCountCache.java:46 is `external_cache_expire_time_seconds_after_access` (seconds, not minutes) and `external_cache_refresh_time_minutes`. This is a cosmetic naming imprecision; the expire-after-access + refresh semantics claimed are correct. |  |
| ExternalSchemaCache file: ExternalMetaCacheMgr.java:365 | Anchor points to the accessor, not a distinct cache class. | Line 365 of ExternalMetaCacheMgr.java is `public Optional<SchemaCacheValue> getSchemaCacheValue(ExternalTable table, SchemaCacheKey key)` — the fe-core schema-cache accessor. Valid anchor for the mechanism; the schema-cache front-loading of getTableSchema is real and confirmed (initSchema at PluginDrivenExternalTable.java:449→getTableSchema :469). |  |

> 复核备注：All material claims hold at HEAD (aaab68ef474). Verified: (1) Migration — jdbc live/SPI-routed, legacy fe-core Jdbc catalog/table deleted, fe-core JdbcClient only for CDC/streaming validators. (2) All 6 caches exist at ~cited locations, no invented classes: CLASS_LOADER_MAP (JdbcConnectorClient.java:78), HikariDataSource per-catalog (:89 field/:220 new/getOrCreateClient :188 single-client guard), OceanBase volatile delegate double-checked (JdbcOceanBaseConnectorClient.java:45/59), fe-core ExternalSchemaCache/ExternalRowCountCache(:40-45 AsyncLoadingCache)/MetaCache(:65). (3) Funnel: JdbcConnectorMetadata is stateless-forwarding (getTableSchema:119, getTableStatistics:144, getColumnHandles:162, getTableComment:252 all fresh client round-trips) — metadataMemoizesLoadedTable=no CONFIRMED; write path new JdbcConnectorMetadata at JdbcWritePlanProvider.java:136 bypasses funnel CONFIRMED.

Hot-path findings independently confirmed:
- HP-1 CONFIRMED: buildColumnHandles() (PluginDrivenScanNode.java:1916) → metadata.getColumnHandles → client.getJdbcColumnsInfo (real remote DatabaseMetaData.getColumns at JdbcConnectorClient.java:408). Callers: getSplits(:1263), startSplit(:1587,:1672), getOrLoadPropertiesResult(:1841) → 1-2 remote round-trips per scan node, NOT memoized, distinct from ExternalSchemaCache-fronted getTableSchema path. Variant C. Multiplier accurate.
- HP-2 CONFIRMED: buildInsertSql (JdbcWritePlanProvider.java:130) constructs `new JdbcConnectorMetadata(client, properties).getColumnHandles(...)` at :136 — fresh instance bypassing funnel + remote refetch. Called from planWrite(:68) and appendExplainInfo(:119).
- HP-3 CONFIRMED (minor/off-hot-path, as audit states): getComment (PluginDrivenExternalTable.java:925) → metadata.getTableComment uncached; base returns "" but MySQL override does remote INFORMATION_SCHEMA.TABLES query (JdbcMySQLConnectorClient.java:165). Only SHOW/DESC path.
- HP-4 CONFIRMED (negative control): planScan (JdbcScanPlanProvider.java:75) is pure SQL-string assembly + property reads, zero client/remote calls, returns Collections.singletonList(scanRange); estimateScanRangeCount()==1 (:153). No split/file/partition loop.
- HP-5 CONFIRMED (negative control): base getRowCount returns -1 (:467), MySQL override remote (:191); fronted cross-query by ExternalRowCountCache.loadRowCount→table.fetchRowCountWithMetaCache (ExternalRowCountCache.java:90-94).

Authz CONFIRMED: getCapabilities = EnumSet.of(SUPPORTS_PASSTHROUGH_QUERY, SUPPORTS_METADATA_PRELOAD) (JdbcDorisConnector.java:98-105), no SUPPORTS_USER_SESSION; buildConnectorSession injects credential only withUserSessionCapability(supportsUserSession()) which is false for jdbc; shouldBypassSchemaCache/shouldBypassTableNameCache gated on same (PluginDrivenExternalCatalog.java:1177-1212). Fixed catalog user/password → no per-user metadata divergence, no leak. beginTransaction returns NoOpConnectorTransaction (:286).

No missed severe finding: no per-split/per-file/per-partition amplification exists (structurally 1 scan range), names via MetaCache, schema/rowcount cross-query cached — audit's max severity P2 is appropriate. frameworkPiecesNeeded (partial funnel-memoization, no to all iceberg-style caches) and overallVerdict (do not apply iceberg cache framework, Rule 2) are well-supported. Corrections listed are cosmetic line-anchor/config-name nits that do not affect any conclusion.
