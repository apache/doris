## 连接器附录：maxcompute（`fe-connector-maxcompute`，catalog type `max_compute`）

### 1. 迁移状态（Migration status）

`max_compute` **已在** `SPI_READY_TYPES`（`CatalogFactory.java:57`），因此 MaxCompute catalog 走 SPI plugin 路径，被包成 `PluginDrivenExternalCatalog` + `MaxComputeDorisConnector`（`CatalogFactory.java:110-118`）。fe-core 中已无任何遗留 MaxCompute 类（`find fe/fe-core -iname '*MaxCompute*'` 为空）；仅剩显示兼容用的 engine-name switch（`PluginDrivenExternalTable.java:1227-1230,1260-1261`）与 GSON 迁移引用。**读路径完全翻闸、live。** 写路径也已 live 接线（`PhysicalPlanTranslator.visitPhysicalConnectorTableSink` → `getWritePlanProvider(handle)` → `planWrite`）；`MaxComputeConnectorMetadata.beginTransaction:332` / `MaxComputeWritePlanProvider:74` 里"gate-closed / dormant until cutover"的 javadoc 相对 HEAD 的 `SPI_READY_TYPES` **已过时**。

liveness = **live**。

### 2. 现有缓存（Current caches）

| 缓存 | 作用域 | Key | TTL / 容量 | 失效 | file:line |
|---|---|---|---|---|---|
| `MaxComputePartitionCache` | cross-query | `(db, table)`；project 每 catalog 恒定，不入 key（`PartitionKey:138`） | 600s / 10000，可经 `meta.cache.max_compute.partition.*` 覆盖（`CacheSpec.fromProperties:92`） | REFRESH TABLE/DB/CATALOG + 分区增删 → 整表 flush（`invalidateTable:113`/`invalidateDb:118`/`invalidateAll:123`；connector 钩子 `MaxComputeDorisConnector.java:189/198/204/213`） | `MaxComputePartitionCache.java:60-173`；`final` 挂在长寿命 connector 上（`MaxComputeDorisConnector.java:61,79-80`） |
| ODPS `Table` 对象内 lazy memo | per-scan | 单个 handle 携带的一个 `com.aliyun.odps.Table`（transient，`MaxComputeTableHandle.java:37`） | handle 生命周期；首次访问 `reload()` 后 in-object 缓存 | 随 handle GC | `getTableHandle:124`（`getOdpsTable` 惰性、无 RPC）；首次元数据访问 reload（`MaxComputeScanPlanProvider.java:187,189,194`）。**不跨 handle 共享** |
| fe-core `SchemaCacheValue` | cross-query | fe-core ExternalTable schema 缓存（每表） | fe-core 外表元缓存 TTL / REFRESH | REFRESH | `PluginDrivenExternalTable.initSchema:430-471`；`getTableSchema` 每 schema-cache 刷新一次，非每查询 |
| `PluginDrivenScanNode.cachedMetadata` + `resolvedScanProvider` | per-scan | 每 scan node；metadata 按 catalogId，provider 按 handle identity | scan node 生命周期 | — | `PluginDrivenScanNode.java:189,184,245-260`（fe-core 通用设施） |
| `EnvironmentSettings`/`SplitOptions`/providers | cross-query | 每 connector 一次性 lazy init（配置，非远端数据） | connector 生命周期 | — | `MaxComputeDorisConnector.doInit:94-128`；`MaxComputeScanPlanProvider.initFromProperties:117-161` |

连接器**没有**：跨查询 table/schema 缓存（依赖 fe-core `SchemaCacheValue`）、per-statement handle memo、format 缓存（N/A）、comment 缓存（default 空）、manifest/file-list 缓存（N/A）。分区缓存用的是共享 `fe-connector-cache`（`CacheSpec`+`MetaCacheEntry`），但未用更高层的 `ConnectorPartitionViewCache`/`PartitionViewCacheKey`（那是分区派生视图的另一议题）。

### 3. Funnel 参与（Layer-2）

**已接入 funnel：是。** `MaxComputeConnectorMetadata` 只经 `MaxComputeDorisConnector.getMetadata(session)`（`MaxComputeDorisConnector.java:177-181`）产生，而它被 `PluginDrivenMetadata.get` → `scope.getOrCreateMetadata("metadata:"+catalogId)`（`PluginDrivenMetadata.java:70`）按 (statement, catalog) memo，故一条语句只有一个 `MaxComputeConnectorMetadata`（由 `tools/check-fecore-metadata-funnel.sh` 全构建强制）。

**但该 metadata 不 memoize 已加载表：否。** `getTableHandle`（`MaxComputeConnectorMetadata.java:118-130`）每次都重跑 `structureHelper.tableExist`（line 121 → ODPS `tables().exists()` 远程探测，`McStructureHelper.java:129-137/218-225`）**并**新建一个惰性 `Table`（line 124）。`getMetadata` 本身极廉价（只包已 init 的 `odps`/`structureHelper`/`partitionCache`），所以对 MaxCompute 而言 funnel 的 metadata memo **省不掉任何远程 IO**。funnel 收敛的是"metadata 构造"，不是"表解析"——metadata 内无 `(db,table)→handle` 映射，于是一条语句里 ~13 个 `resolveConnectorTableHandle` 站点（`PluginDrivenExternalTable.java:160,463,607,717,821,882,955,1026,1061,1128` + `resolveWriteCapabilityHandle` 205/222/375/410）与 translator 的 `getTableHandle`（`PhysicalPlanTranslator.java:624,676`；`BindSink.java:675,714`）各自付一次 `exists()` 探测；每个访问 schema 的路径各 reload 一次新 `Table`。**这正是 iceberg PERF-07 的缺口，在 MaxCompute 上尚未处理。**

metadataMemoizesLoadedTable = **no**。

### 4. 热路径重复加载审计（问题类应用）

| # | 区域 | 问题 | 乘数 | 成本 | 严重度 | 已 memo | file:line |
|---|---|---|---|---|---|---|---|
| MC-1 | schema | `getTableHandle` 每次解析都发一次**冗余**的 ODPS `tables().exists()` 远程探测（`:121`→`McStructureHelper.tableExist`），且 connector metadata 与 funnel 均不 memo handle。一条语句在多个独立站点（scan 翻译、分区裁剪、row-count、逐列 stats、写能力探测）各解析一次 handle，各付一次 `exists()`。表本就是已解析的 catalog `ExternalTable`，此探测纯属浪费（变体 B 单链重复 k 次 + C funnel 只 memo metadata 不 memo handle）。 | k = 每语句触达的解析站点数；暖 fe-core stats 缓存下的分区 SELECT ≈2（scan create + `getNameToPartitionItems`），冷 stats 缓存/写路径更高 | remote-io | **P1** | 否 | `MaxComputeConnectorMetadata.java:118-130`；`McStructureHelper.java:129-137,218-225` |
| MC-2 | schema | handle 不按语句 memo → 每个新 handle 携带各自未加载的 `Table`，每个访问 schema 的路径（`planScan` 的 `checkOperationSupported`/`getFileNum`/`getSchema`，冷 schema-cache 时的 `initSchema.getTableSchema`）各触发一次 `Table.reload()`。放大温和（多数非 scan 路径只用 db/table 名，不碰 `Table` 对象），约每语句 1–2 次 reload，而非 iceberg 的 3–7×（变体 B）。 | ~1–2×/语句 | remote-io | P2 | 是（per-handle in-object，不跨 handle 共享） | `MaxComputeConnectorMetadata.java:124`；`MaxComputeScanPlanProvider.java:183-194` |
| MC-3 | split-enum | **非问题/已干净**：split 枚举无 per-split 远程放大。读会话只建一次（`buildBatchReadSession`，规划固有成本），循环前只序列化一次（`serializeSession:329`），per-split 循环仅构造 `MaxComputeScanRange` 并按引用共享同一序列化串（`337-372`）。`getSplits` 只调一次 `planScan`。 | 1×（无放大） | cpu | none | 是 | `MaxComputeScanPlanProvider.java:326-377` |
| MC-4 | partitions | **数据侧非问题，handle 侧并入 MC-1**：分区列举（`listPartitions`/`listPartitionNames`/`listPartitionValues` + fe-core `getNameToPartitionItems`/`getNameToPartitionValues`）由跨查询 `MaxComputePartitionCache`（TTL 600s）兜住，per-table `getPartitions()` 不会每查询重发；仅 `getNameToPartitionItems` 里的 `getTableHandle` `exists()` 探测未缓存（已计入 MC-1）。`listPartitions` 故意忽略下推 filter（与遗留 SHOW PARTITIONS 一致）——全量列举但已缓存。 | 每 TTL 窗口每 (db,table) 1 次远程 `getPartitions` | remote-io | none | 是 | `MaxComputeConnectorMetadata.java:239-296`；`MaxComputePartitionCache.java:107` |

### 5. Authz / 一致性

- **perUserCredential = false，sessionUserRelevant = false。** MaxCompute 用 **catalog 级静态凭证**（AK/SK、RAM-Role-ARN、ECS-RAM-Role），CREATE CATALOG 时固化，`doInit` 里一次性建客户端（`MaxComputeDorisConnector.java:102`；`MCConnectorClientFactory.java:86-127`）。无 per-user 凭证下发、无 session=user、无 REST/OIDC 会话凭证、无 kerberos doAs、无 per-identity metastore——一个 catalog 的所有用户共享同一 ODPS 身份。
- 因此 **Layer-3 授权缓存隔离不适用**：name-only 的 `MaxComputePartitionCache`（db+table）不会泄漏 can-LIST-cannot-LOAD 用户的元数据；MC-1 的 per-statement handle memo 同样授权安全（一语句=一身份，已由 `PluginDrivenMetadata.java:63-69` 钉住）。`tools/check-authz-cache-sharding.sh` 适用但 MaxCompute 天然合规。
- 元数据陈旧有界且正确性安全：分区新增 ≤600s 内可见（与遗留 TTL 一致），删除至多陈旧一个 TTL，miss 时自愈。
- **写一致性**：MaxCompute **非 MVCC**（用基类 `PluginDrivenExternalTable`，非 `MvccTable`），无需 read+write snapshot 对齐；写正确性只需把 ODPS 写会话绑到 per-statement `MaxComputeConnectorTransaction`（`setWriteSession`）、block 分配按 `txn_id`（`MaxComputeConnectorTransaction.java:61-131`）——已具备。

### 6. 框架各件是否需要

| 件 | 需要 | 理由 |
|---|---|---|
| per-statement-funnel-memoization | **yes（当前 partial）** | 最高价值。funnel 已给每语句一个 metadata，但 `getTableHandle` 每次重探存在性+新建惰性表。在 per-statement `MaxComputeConnectorMetadata` 实例里加 `Map<(db,table),MaxComputeTableHandle>`，让 `getTableHandle` 整条语句返回同一 handle（一次 `exists()`、一次 `reload()`），一举收敛 MC-1、MC-2。比 iceberg 的 scope-keyed load memo 更简单——metadata 实例本就 per-statement。并顺手去掉已解析读的冗余 `tableExist()` 探测。 |
| cross-query-table-cache | partial | fe-core `SchemaCacheValue` 已跨查询缓存 schema，跨查询主需求已覆盖。连接器侧 cached ODPS `Table`（iceberg `IcebergTableCache` 类比，24h/REFRESH）可再省每查询 `planScan` reload（MC-2 跨查询维度），但有陈旧/TTL 代价、收益温和。可选、优先级低于 per-statement memo。 |
| partition-cache | already-has | `MaxComputePartitionCache` 已是完整跨查询分区缓存（db+table，TTL 600s，容量 10000，REFRESH 失效），MC-4 证实其兜住热点分区读。 |
| format-cache | no | N/A：MaxCompute 走 ODPS Storage API（arrow），无 per-table 文件格式推断（无 `getFileFormat`/`planFiles` 兜底），iceberg PERF-03 不存在。 |
| comment-cache | no | 未 override `getTableComment`，SPI default 返回 `""`（`ConnectorTableOps.java:336`）无远程，无需缓存。 |
| manifest-or-file-list-cache | no | N/A：无 manifest/snapshot 模型；读会话/split assigner 是每查询规划固有成本；`estimateDataSizeByListingFiles`/`listFileSizes` 未 override（default -1/空）。 |
| per-scan-hoist | already-has | scan 不变配置（`EnvironmentSettings`/`SplitOptions`/超时）已提到 connector/provider 一次性 lazy init；序列化会话在 split 循环前算一次。iceberg PERF-06 per-scan storage-URI normalizer 不适用（无 per-file 下发存储）。 |
| per-file-split-memo | no | N/A：split 只带 index/row-offset + 一份共享序列化会话串（已按引用共享），无 per-file 分区 JSON/值/删除载体需 memo。 |
| authz-session-user-isolation | no | catalog 静态凭证、单一共享 ODPS 身份；name-only 缓存不会跨用户泄漏或供陈旧授权，无需 iceberg 的 `isUserSessionEnabled` 置空/`shouldBypassSchemaCache`。 |
| shared-fe-connector-cache-adoption | already-has | `MaxComputePartitionCache` 基于共享 `CacheSpec`+`MetaCacheEntry`（`:20-21,92-97`），镜像 `CachingHmsClient`/`HiveFileListingCache`。 |
| write-txn-coholder | already-has | `MaxComputeConnectorTransaction` 持 per-statement 写态（写会话/标识/settings/`TMCCommitData`/block 高水位），`planWrite.setWriteSession` 绑定、`txn_id` 提交；非 MVCC 无需 snapshot 共享；funnel 的一语句一写事务已协调。 |

### 7. 结论与优先建议

**结论：live，且已相当好地缓存——不是 iceberg 那样的 P0 目标。** 具备跨查询分区缓存（共享工具箱）、干净的 split 枚举（无 per-split 远程放大）、per-statement 写事务，并复用 fe-core 跨查询 schema 缓存；非 MVCC、静态凭证，Layer-3 授权隔离不适用。

**唯一真正适用的框架件是 per-statement handle/table memoization：** `getTableHandle` 每次解析都发冗余的 ODPS `tables().exists()` 并返回新惰性表，而 funnel 只 memo metadata 不 memo handle——于是一条语句里 k 个独立解析站点各付一次 `exists()` RPC（MC-1，P1），访问 schema 的路径重复 reload 表（MC-2，P2）。

**建议（低风险、高杠杆）：** 在 per-statement `MaxComputeConnectorMetadata` 实例内按 `(db,table)` memo 解析出的 `MaxComputeTableHandle`，并对已解析读去掉冗余存在性探测；可选地后续再加跨查询 ODPS `Table` 缓存。改动远小于 iceberg 那套 buildout。

---

## 复核结论 (adversarial verify)

**Verdict: CONFIRMED**  ·  确认发现 IDs: MC-1, MC-2, MC-3, MC-4

| 原始声明 | 问题 | 更正 | 证据 |
|---|---|---|---|
| find fe/fe-core -iname '*MaxCompute*' returns nothing | Literally false: it returns two compiled dirs fe/fe-core/target/classes/org/apache/doris/datasource/maxcompute and target/test-classes/.../maxcompute (stale build artifacts). | The SOURCE tree fe/fe-core/src is clean (no MaxCompute .java). The read/write cutover conclusion is unaffected; only the stated grep evidence is imprecise. Verify with: find fe/fe-core/src -iname '*MaxCompute*' (empty). |  |
| PluginDrivenScanNode.cachedMetadata + resolvedScanProvider ... PluginDrivenScanNode.java:189/184/245-260 | File path implied by the funnel narrative (datasource/plugin/) is wrong. | PluginDrivenScanNode is at fe/fe-core/src/main/java/org/apache/doris/datasource/scan/PluginDrivenScanNode.java. All cited line numbers are correct there: currentHandle:152, resolvedScanProvider:184, cachedMetadata:189, metadata():203, resolveScanProvider():245-260. |  |
| ~13 resolveConnectorTableHandle / getTableHandle sites (PluginDrivenExternalTable.java:160,463,607,717,821,882,955,1026,1061,1128 + resolveWriteCapabilityHandle 205/222/375/410) | Direct resolveConnectorTableHandle count is 10, not 13; the extra 205/222/375/410 line refs point at resolveWriteCapabilityHandle callers which I did not individually confirm (the method definition is at 187). | Exactly 10 direct resolveConnectorTableHandle call sites confirmed at the listed lines, each in a distinct real planning entrypoint (getSyntheticScanPredicates:152, initSchema:430, getShowCreateTableDdl:599, fetchSyntheticWriteColumns:700, getNameToPartitionItems:807, getNameToPartitionValues:870, getSupportedSysTables:946, getColumnStatistic:1014, getChunkSizes:1049, fetchRowCount:1121). The write-capability probes add a few more. The 'k independent sites, none memoized' core of MC-1 holds regardless of the exact count. |  |
| MC-2: ~1-2 reloads per statement | Slight understatement risk on the cold per-column stats path was checked and cleared, worth recording. | getColumnStatistic (fe-core:1014-1035) resolves a FRESH handle per column (so MC-1 exists()-probe amplifies to N-per-column on cold stats), BUT MaxComputeConnectorMetadata does NOT override getColumnStatistics (SPI default no-op, no remote call and no odpsTable access), so it triggers NO extra Table.reload(). MC-2's ~1-2x reload multiplier therefore holds; the per-column amplification is exists()-probe only, which the audit already flags qualitatively under MC-1 ('higher on cold stats caches'). |  |

> 复核备注：All material claims hold against HEAD (aaab68ef474). Core thesis verified: MaxCompute is LIVE and reasonably cached (not a P0 target like iceberg), and the one real applicable framework piece is per-statement handle/table memoization.

CONFIRMED in code:
- Migration: max_compute in SPI_READY_TYPES (CatalogFactory.java:57); write path live (PhysicalPlanTranslator getTableHandle:676, getWritePlanProvider:687); dormant-until-cutover javadoc at beginTransaction is stale.
- Funnel: PluginDrivenMetadata.get memoizes ONE metadata per (statement,catalog) (line 70). getMetadata (MaxComputeDorisConnector:177-181) returns a fresh instance; MaxComputeConnectorMetadata carries no instance-level handle map (only odps/structureHelper/defaultProject/endpoint/quota/properties/partitionCache). getTableHandle (118-130) re-runs tableExist (line 121 -> tables().exists(), remote per ODPS SDK) + fresh lazy getOdpsTable (124). resolveConnectorTableHandle (PluginDrivenExternalTable:116-119) does NOT memoize -> each of 10 call sites re-resolves. => metadataMemoizesLoadedTable = no, confirmed.
- MC-1 (P1): CONFIRMED real, not memoized at any layer, multiplier honest (~2 warm, per-column on cold stats).
- MC-2 (P2): CONFIRMED real; per-handle in-object memo (MaxComputeTableHandle.java:37 transient), not cross-handle; reload sites MaxComputeScanPlanProvider 183/187/189/194; ~1-2x correct.
- MC-3: CONFIRMED valid non-finding. serializeSession once (329), per-split loop (337-372) shares the one serialized string by reference (.scanSerialize at 346/364); no per-split remote IO.
- MC-4: CONFIRMED valid non-finding. All three partition SPI methods use partitionCache.getPartitions (listPartitionNames:243, listPartitions:265, listPartitionValues:284); the only uncached cost is the getTableHandle exists() probe, correctly folded into MC-1.
- MaxComputePartitionCache: cross-query, keyed (db,table) PartitionKey:138, TTL 600s (74), cap 10000 (75), CacheSpec.fromProperties (92), built on shared fe-connector-cache (imports 20-21), REFRESH hooks 189/198/204/213 including whole-table flush on invalidatePartition:213.
- Authz: static catalog credentials (MCConnectorClientFactory:86-127 AK/SK / RAM-Role-ARN / ECS-RAM-Role), no per-user/session=user; Layer-3 isolation N/A; name-only cache authz-safe under one shared ODPS identity.
- frameworkPiecesNeeded all consistent: getTableComment default returns "" (ConnectorTableOps.java:336) and is NOT overridden -> comment-cache 'no' correct; write-txn-coholder present (MaxComputeConnectorTransaction:61-131, setWriteSession:97, TMCCommitData/nextBlockId).

No hallucinated caches, no invented classes, no over-stated multipliers, and no missed severe finding. Only immaterial nits (see corrections): the 'find returns nothing' grep evidence, the PluginDrivenScanNode directory path, and the '~13 sites' count. None change any conclusion.
