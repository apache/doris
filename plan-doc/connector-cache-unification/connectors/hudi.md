## 连接器附录 — hudi (fe-connector-hudi)

### 1. 迁移状态 (Migration status)

- **`hudi` 不在 `SPI_READY_TYPES`（设计如此，正确）**。`CatalogFactory.java:56-57` 的白名单是 `{jdbc, es, trino-connector, max_compute, paimon, iceberg, hms}`；`:50-55` 的注释明确禁止加入 `"hudi"`。没有独立的 `HudiExternalCatalog`；`HudiConnectorProvider.java:45-47` 返回的是 sibling-only 类型串 `"hudi"`。
- **hudi 通过 hms gateway 以 sibling 形式暴露，且已 LIVE**。`HiveConnector.newMetadata` 把 `this::getOrCreateHudiSibling` 接入 `HiveConnectorMetadata`（`HiveConnector.java:188-191`），`HiveConnectorMetadata.getTableHandle` 对检测为 HUDI 的表转发到 `hudiSiblingMetadata(session).getTableHandle(...)`（`HiveConnectorMetadata.java:415-417`）。`getOrCreateHudiSibling` 通过 `context.createSiblingConnector("hudi", ...)` 构建唯一 sibling（`HiveConnector.java:576-591`）。
- **liveness = live（非 dormant）**：`"hms"` 已由 **#65473 / commit `6e521aa64b2`（2026-07-16）** 加入 `SPI_READY_TYPES`；且 fe-core 侧的 legacy hudi 路径（`HudiScanNode` / `HudiExternalMetaCache` / `HMSExternalTable` / `HudiDlaTable`）已全部删除，连接器路径是 hudi 的唯一路径。
- **⚠ 需暴露的冲突（Rule 7）**：源码中仍散落大量 `"dormant until hms enters SPI_READY_TYPES"` / `"today getTableHandle is never called"` 注释（`HudiConnectorMetadata.java:391`、`HiveConnectorMetadata.java:410,1802,1942,1972,2007`）。这些注释写于 #65473 之前，已 **STALE**，与 HEAD 的 `SPI_READY_TYPES` 事实矛盾，不能据其判定 liveness，应清理。

### 2. 现有缓存 (Current caches)

| 缓存 | scope | key | TTL / 失效 | file:line | 说明 |
|---|---|---|---|---|---|
| per-statement `ConnectorMetadata`（funnel） | per-statement | `(catalogId, HUDI_LABEL)` on `ConnectorStatementScope` | 语句结束 | `HiveConnectorMetadata.java:302` | Layer-2 唯一 memo；只 memo **对象**，不 memo 已加载的 metaClient/schema |
| HMS `ThriftHmsClient`（**RAW，无结果缓存**） | metastore-client | 单个连接池 client memo 在 `HudiConnector` 上 | 无（连接器生命周期） | `HudiConnector.java:186` | **未包 `CachingHmsClient`**：`getTable/tableExists/listTables/listPartitionNames` 每次都是新 Thrift RPC |
| `pluginAuth`（`HadoopAuthenticator`） | cross-query | catalog 级单一 Kerberos 身份 | 连接器生命周期 | `HudiConnector.java:196` | 认证对象，非元数据缓存 |
| Hudi `InternalSchemaCache`（库内部，非 Doris 缓存） | none | `(commitTime, metaClient)` in hudi-common | 库自管 | `HudiSchemaUtils.java:278` | 随每次新建 metaClient 基本重置 |

**结论**：hudi **没有任何连接器侧 cross-query 元数据缓存**，**没有 per-statement 的 metaClient/schema memo**，**完全没用**共享工具箱（pom 无 `fe-connector-cache` 依赖，无 `CacheFactory/CacheSpec/MetaCacheEntry/ConnectorPartitionViewCache`），甚至没有像 `HiveConnector` 那样 `wrapWithCache` 包 `CachingHmsClient`。它是所有 SPI 连接器里缓存最薄的一个。

### 3. Funnel 参与 (Funnel participation)

- **已接入 Layer-2 funnel**：sibling 元数据经 `HiveConnectorMetadata.memoizedSiblingMetadata`（`:302-305`）→ `session.getStatementScope().getOrCreateMetadata("metadata:"+catalogId+":"+ownerLabel, () -> owner.getMetadata(session))`，`hudiSiblingMetadata`（`:332-334`）与 by-handle `siblingMetadata`（`:347-350`）在同一语句内共用同一实例（`HUDI_LABEL` keyed）。故一条语句内只构建 **一个** `HudiConnectorMetadata`。
- **但该实例内部零 memo**：`HudiConnectorMetadata` 字段仅 `{hmsClient, properties, metaClientExecutor, storageHadoopConfig}`（`:153-174`），无任何 resolved metaClient/schema/timeline/partitions 缓存字段。每个 `getTableSchema` / `getColumnHandles` / `applyFilter` / `listPartitions*` / `beginQuerySnapshot` / `collectPartitions` 都经 `HudiScanPlanProvider.buildMetaClient` 重新构建 `HoodieTableMetaClient`。
- **scan provider 与 metadata 割裂**：`HudiConnector.getScanPlanProvider` 每次返回 `new HudiScanPlanProvider`（`:136-138`），并在 `planScan`（`:148-151`）与 `getScanNodeProperties`（`:363`）各建一次自己的 metaClient，与 metadata 的完全独立。
- **`metadataMemoizesLoadedTable = no`**：funnel 把「元数据对象」收敛为一条语句一个，但**没有**收敛重复的远程 table load。需要 iceberg 式的内部 memo（PERF-01/PERF-07 类比）才能把每 pass 5-6 次 metaClient 构建塌缩为一次。

### 4. 热点重复加载审计 (Hot-path repeated-load audit)

重活 = 构建 `HoodieTableMetaClient`（远程读 `.hoodie/hoodie.properties` + table config + 列举 active timeline 目录）+ schema 解析（`getTableAvroSchema` + `InternalSchema`）+ 分区列举（`listAllPartitionPaths` = `HoodieTableMetadata` 扫描）。对一条「带 WHERE 分区谓词、非 hive-sync 的分区 hudi SELECT」，单个 planning pass 内对**同一张表**的重复远程加载：

| ID | area | 问题 | 倍数 | cost | 严重度 | memoized? | file:line |
|---|---|---|---|---|---|---|---|
| HD-P01 | query-plan | `HoodieTableMetaClient` 在每个 metadata+scan 入口各重建一次（getColumnHandles→getTableSchema、beginQuerySnapshot→latestInstant、applyFilter、MVCC getTableSchema、planScan、getScanNodeProperties），~5-6 次/pass；每次远程读 hoodie.properties + 列举 timeline。iceberg PERF-01/07 直接类比。 | ~5-6×/pass (B/D) | remote-io | **P1** | no | `HudiScanPlanProvider.java:148` |
| HD-P02 | schema | 最新 Avro schema + InternalSchema 在独立 metaClient 上重复解析 ~4 次：`getSchemaFromMetaClient`（`:819-821`）供 getColumnHandles 与 3-arg MVCC getTableSchema；planScan（`:188,194,220`）；getScanNodeProperties（`:382-383`）。 | ~4×/pass (B) | remote-io | **P1** | no | `HudiConnectorMetadata.java:797` |
| HD-P03 | partitions | 分区列举（`listAllPartitionPaths` = `HoodieTableMetadata.create`+`getAllPartitionPaths`，元数据表扫描）+ `latestCompletedInstant` timeline 读，无 `(table,instant)` 缓存：collectPartitions 支撑 listPartitions/Names/Values（`:707-713`）、applyFilter 非 hive-sync 再列举（`:289-291`）、planScan resolvePartitions 再列举（`:281,648`）；一次 MTMV refresh 内 fe-core 重列举 4-6 次。无 IcebergPartitionCache / hive partitionViewCache 类比。 | 3×/pass + 4-6×/MTMV (A/B) | remote-io | **P1** | no | `HudiScanPlanProvider.java:734` |
| HD-P04 | schema | HMS 元数据走 **RAW** `ThriftHmsClient` — hudi **未包** `CachingHmsClient`（`HudiConnector.createClient:186`；对照 `HiveConnector.wrapWithCache`）。getTableHandle 的 tableExists+getTable（2 RPC，`:204-207`）、hive-sync 下 applyFilter（`:278`）与 collectPartitions（`:695`）各自 listPartitionNames，均每查询重打且跨查询无缓存。缓存类 `CachingHmsClient`（keyed `(db,table)`、24h TTL）已存在且被 hive 使用，hudi 热点却绕过 = Variant C。 | 2+ RPC/pass，跨查询无缓存 (C) | remote-io | P2 | no | `HudiConnector.java:186` |
| HD-P05 | split-enum | 每 scan loop 不变量重算：`storageHadoopConfig(context)`（翻译全部 StorageProperties）+ `buildHadoopConf` 在 planScan（`:912-927`）与 getScanNodeProperties（`:341,363`）各建一遍；storage-URI 归一化 `context.normalizeStorageUri` 逐 base file / 逐 slice 调用（collectCowSplits `:445`、collectMorSplits `:474-476`），未按 iceberg PERF-06 每 scan 提升一次。 | 2×/pass（config）+ O(N_files)（uri）(D) | cpu | P2 | no | `HudiScanPlanProvider.java:912` |

倍数与 cost 说明：HD-P01/P02/P03 是真远程 IO 且真倍数、真无 memo，属问题类 Variant B（单链重复 k 次）/D（跨入口未提升的 loop-invariant）/A（MTMV 循环放大）；HD-P04 属 Variant C（缓存存在但热点未命中）；HD-P05 只是 CPU/alloc，低危。已 skeptical 排除误报：per-file schemaId resolver 是真·逐文件（各文件自带写入 schema 版本），非可提升不变量，不计入。

### 5. 授权 / 一致性 (Authz / consistency)

- **不 vend 每用户凭证**：hudi 用 catalog 级单一身份 —— plugin 端 Kerberos `doAs`（单 keytab，`buildPluginAuthenticator:223-238`、`metaClientExecutor:102-122`）或 FE 注入的 `context.executeAuthenticated`（sibling 下为 NOOP/SIMPLE）。无 REST-OIDC / `session=user` 类比，无 per-identity metastore。
- `HudiConnector` 未覆写 `getCapabilities`，继承空默认（无 `SUPPORTS_USER_SESSION`）；hive gateway 前门守卫已保证不接入 session=user sibling（`HiveConnector:548-562` 的 iceberg 守卫，同一契约覆盖 hudi）。
- **name-only 缓存是 authz-安全的**：所有用户看到同一 catalog-身份视图，与 hive 的共享 `CachingHmsClient` 一致。**不需要** iceberg Layer-3 的 `isUserSessionEnabled` 置空 / `shouldBypassSchemaCache`。
- **写路径一致性：无**。hudi 只读（`beginTransaction` 抛错 `:395-398`；`getWritePlanProvider` 留 SPI 默认 null），不存在读+写共享 metadata/txn 的需求，无需 write-txn co-holder。

### 6. 所需框架部件 (Framework pieces)

- **per-statement-funnel-memoization = partial**：funnel 路由已在（一语句一 metadata 对象），但缺「语句内 resolved metaClient+schema memo」且 scan provider 独立建 metaClient。需 iceberg PERF-07 式：让 `HudiConnectorMetadata` 与 `HudiScanPlanProvider` 经 `ConnectorStatementScope` 共享同一 resolved metaClient/schema，把 5-6 次塌缩为一次。
- **cross-query-table-cache = yes**：无 metaClient/table-config 跨查询缓存（IcebergTableCache 类比），按 basePath keyed（TTL + REFRESH 失效），塌缩 HD-P01。
- **partition-cache = yes**：无 `(table,instant)` 分区缓存（IcebergPartitionCache / hive ConnectorPartitionViewCache 类比），解决 HD-P03（MTMV/SHOW PARTITIONS 重列举）。
- **format-cache = no**：COW/MOR 从权威 table config/handle 读（`HudiScanPlanProvider:156`），`detectFileFormat` 是廉价后缀判断，无 iceberg getFileFormat whole-table planFiles 回退可 memo。
- **comment-cache = no**：未覆写 getComment，无 information_schema/SHOW TABLE STATUS 的逐表远程 comment load。
- **manifest-or-file-list-cache = partial**：planScan 每查询建 FileSystemView + 列 partition path（`:273-281`），无文件列举缓存（hive 有 HiveFileListingCache）；可加，但优先级低于 metaClient/schema/partition memo。
- **per-scan-hoist = yes**：planScan 与 getScanNodeProperties 各建 metaClient/重解析 schema（HD-P01/P02）；storageHadoopConfig/buildHadoopConf 建两遍、normalizeStorageUri 逐文件（HD-P05）。每 scan 解析一次并复用。
- **per-file-split-memo = no**：分区值已每分区解析一次并跨文件共享；per-file schemaId 是真·逐文件。仅剩 per-file uri-normalize 归入 per-scan-hoist。
- **authz-session-user-isolation = no**：非 session=user，name-only 缓存不会泄漏。
- **shared-fe-connector-cache-adoption = yes**：完全没用共享工具箱，也没包 `CachingHmsClient`。采用之（像 hive 一样 `wrapWithCache` 包 `CachingHmsClient`；分区用 `ConnectorPartitionViewCache`）是 HD-P03/P04 最低风险的底座，且与参考模式一致。
- **write-txn-coholder = no**：只读。

### 7. 结论与建议优先级 (Verdict + recommendations)

hudi 已 LIVE（hms sibling），且是 SPI 连接器中缓存最薄的一个：**零** cross-query 元数据缓存、**零** per-statement metaClient/schema memo，甚至未包 `CachingHmsClient`。它已正确接入 Layer-2 funnel，但 funnel 只塌缩「元数据对象」，重远程加载（metaClient 5-6×/pass、schema ~4×、分区元数据表扫描 3×/pass 且 4-6×/MTMV）在每个入口重复。iceberg 框架模式**直接适用且必要**。建议按序：

1. **per-statement resolved-metaClient+schema memo**（metadata 与 scan provider 共享）——消灭 HD-P01/HD-P02，旗舰改动（iceberg PERF-07/PERF-01）。
2. **把 HMS client 包进 `CachingHmsClient`**——与 hive 对齐的一行改动，立即修 HD-P04（Variant C）。
3. **cross-query metaClient/table 缓存 + `(table,instant)` 分区缓存（采用 `ConnectorPartitionViewCache`）**——修 HD-P03（MTMV/SHOW PARTITIONS 重列举）。
4. **per-scan 提升** metaClient/schema/storage-config/uri-normalizer（HD-P05）。

**不需要** authz 隔离（非 session=user）、**不需要** write-txn 工作（只读）。附带：清理散落的 stale `"dormant until hms enters SPI_READY_TYPES"` 注释。

---

## 复核结论 (adversarial verify)

**Verdict: ADJUSTED**  ·  确认发现 IDs: HD-P01, HD-P02, HD-P03, HD-P04, HD-P05

| 原始声明 | 问题 | 更正 | 证据 |
|---|---|---|---|
| authzConsistency: 'the hive gateway front-door guard already enforces that no session=user sibling can attach (HiveConnector:548-562, mirrored contract)' covering hudi. | The SUPPORTS_USER_SESSION fail-loud guard at HiveConnector.java:548-556 lives inside getOrCreateIcebergSibling and its error text is iceberg-specific ('iceberg-on-HMS sibling ... unexpectedly declares SUPPORTS_USER_SESSION'). getOrCreateHudiSibling (:576-591) has NO equivalent guard, so the '548-562 mirrored contract' does not literally cover the hudi sibling. | hudi's authz-safety instead rests on two verified facts: (a) HudiConnector does not override getCapabilities, so it inherits the empty default with no SUPPORTS_USER_SESSION (confirmed — the only getCapabilities hits are javadoc in HudiConnectorMetadata, no override); and (b) the hive front door itself is never session=user. The audit's substantive conclusion (name-only caches are authz-safe, no Layer-3 isolation / shouldBypassSchemaCache needed) is UNCHANGED and correct. |  |
| HD-P03 multiplier: '3x within one pass' partition metadata-table scans for a filtered partitioned SELECT. | For a FILTERED query the code deliberately dedups to ~1 listing: applyFilter (non-hive-sync) lists once at HudiConnectorMetadata.java:289-291, then resolvePartitions returns the prunedPaths and short-circuits WITHOUT calling listAllPartitionPaths (HudiScanPlanProvider.java:635-638). The connector's own comment at HudiConnectorMetadata.java:288 states 'a filtered query lists exactly once (here) instead of there.' So '3x within one pass' is an upper bound, not the single-pass norm. | The '3x' only materializes when fe-core independently invokes multiple of listPartitions / listPartitionNames / listPartitionValues (each re-calls collectPartitions -> a fresh HoodieTableMetadata scan, HudiConnectorMetadata.java:707-713) and/or on an UNFILTERED scan where resolvePartitions does list (HudiScanPlanProvider.java:648). The CORE finding — a metadata-table partition scan re-run at multiple entrypoints with NO (table,instant)-keyed cache — is CONFIRMED; only the per-pass count is query-shape-dependent. |  |
| HD-P01 multiplier '~5-6x per planning pass' and HD-P05 citation 'planScan.buildHadoopConf (:912-927)'. | Several metaClient build sites are conditional: applyFilter builds one only when a partition predicate is present AND non-hive-sync (:289-291); getScanNodeProperties builds one only when !force_jni (:363); the 3-arg MVCC getTableSchema fires only on the time-travel/MVCC path. HD-P05's '912-927' is the buildHadoopConf METHOD definition, not the planScan call site (the actual call is planScan:147). | Minimum unconditional metaClient builds per pass are ~3-4 (getColumnHandles->getSchemaFromMetaClient :800-801, beginQuerySnapshot->latestInstant :749-750, planScan :148-151, getScanNodeProperties :363), reaching 5-6 with the conditional sites — so '5-6x' is a valid upper bound. HD-P05 substance (buildHadoopConf/storageHadoopConfig built twice per pass at :147 and :363; normalizeNativeUri per-file at :445 and :476) is correct; only the line citation is loose. Neither adjustment weakens the 'not memoized' core. |  |

> 复核备注：Verified against HEAD aaab68ef474. All MATERIAL claims hold; corrections are precision-only and do not overturn any finding.

MIGRATION STATUS — CONFIRMED. SPI_READY_TYPES = {jdbc, es, trino-connector, max_compute, paimon, iceberg, hms} (CatalogFactory.java:56-57), with the :50-55 comment explicitly forbidding 'hudi'. HudiConnectorProvider.getType() returns sibling-only 'hudi' (:45-47). Live path wired: HiveConnector.newMetadata passes this::getOrCreateHudiSibling (:190); HiveConnectorMetadata.getTableHandle diverts HUDI to hudiSiblingMetadata(session).getTableHandle (:415-416); getOrCreateHudiSibling builds via createSiblingConnector (:576-591). Legacy fe-core HudiScanNode/HudiExternalMetaCache/HMSExternalTable/HudiDlaTable ALL removed (find returns none) — confirms live, not dormant. Stale-comment conflict is REAL and correctly surfaced: 'dormant until hms enters SPI_READY_TYPES' / 'today getTableHandle is never called' present at HiveConnectorMetadata.java:410,1802,1942,1972,2007 and HudiConnectorMetadata.java:391 — all stale since hms IS in the set.

CURRENT CACHES — all 4 CONFIRMED. (1) per-statement funnel memo via getOrCreateMetadata key 'metadata:'+catalogId+':'+HUDI_LABEL (HiveConnectorMetadata.java:302-304, hudiSiblingMetadata :332-333, by-handle siblingMetadata :347). (2) RAW ThriftHmsClient — HudiConnector.createClient returns new ThriftHmsClient (:186), NOT wrapped in CachingHmsClient; contrast HiveConnector which imports+uses CachingHmsClient/wrapWithCache (CachingHmsClient.java exists in fe-connector-hms). (3) pluginAuth double-checked memo (:196-206). (4) hudi-lib InternalSchemaCache.searchSchemaAndCache (HudiSchemaUtils.java:278).

FUNNEL PARTICIPATION — CONFIRMED. Exactly one HudiConnectorMetadata per statement, but it memoizes NOTHING: fields are only {hmsClient:153, properties:154, metaClientExecutor:156, storageHadoopConfig:161} — grep for any HoodieTableMetaClient/InternalSchema/Schema/HoodieTimeline/partition/schema FIELD returns only method signatures, no cache field. Scan provider is a separate object (HudiConnector.getScanPlanProvider returns new HudiScanPlanProvider, :136-138) building its own metaClient in planScan (:148-151) and getScanNodeProperties (:363).

HOT-PATH FINDINGS — all 5 independently CONFIRMED as real heavy remote-IO ops with genuinely NO memoization (no pre-existing cache catches them): HD-P01 (HoodieTableMetaClient.build reads .hoodie/hoodie.properties + loads active timeline, per line 916-917 comment; rebuilt at every metadata+scan entrypoint), HD-P02 (getTableAvroSchema(true) + resolveTableInternalSchema re-resolved at getSchemaFromMetaClient:819-821, planScan:188/194/220, getScanNodeProperties:382-383), HD-P03 (listAllPartitionPaths = HoodieTableMetadata.create+getAllPartitionPaths at :734-742, no (table,instant) cache), HD-P04 (raw ThriftHmsClient, Variant C — CachingHmsClient exists+used by hive but hudi bypasses it), HD-P05 (buildHadoopConf/storageHadoopConfig 2x + normalizeNativeUri per-file, CPU-only, correctly rated P2). The audit correctly EXCLUDED false positives (per-file schemaIdResolver at :232-241 is genuinely per-file, not a hoistable invariant; format-cache/comment-cache not applicable — getTableType is authoritative at :156, detectFileFormat is a cheap suffix check at :899-910, no getComment override). No severe finding was understated and no finding is covered by an existing cache. Overall verdict (hudi is the least-cached SPI connector; iceberg framework pattern applies and is needed) STANDS.
