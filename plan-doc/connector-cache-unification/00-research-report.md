Spot-checks confirm the spine: `SPI_READY_TYPES` declared at `CatalogFactory.java:56`, the 7-type `ImmutableSet` literal at `:57`; `PluginDrivenMetadata.get` at `:53` with the identity pin at `:63-69` and the funnel key `"metadata:"+catalogId` at `:70`; both gates exist, `check-authz-cache-sharding.sh` hard-wired to `IcebergConnector.java` (`TARGET_REL`, `:57`). Now the report.

---

# 调研报告:`0b4f72582e7` 统一 iceberg 缓存 + per-statement metadata funnel 框架能否推广到其余连接器

> 基线:`branch-catalog-spi` @ HEAD。参考 commit `0b4f72582e7`。本报告的每条论断均挂在 HEAD 代码的 `file:line` 上;连接器逐条结论来自已经过对抗式复核(adversarial verify)的 per-connector 审计。凡输入之间有分歧或低置信处,本报告显式标注。

---

## 1. 结论先行 (Executive Summary)

一句话回答 owner:**这个框架有三层,其中 Layer-2(per-statement `ConnectorMetadata` funnel)以及它所依托的共享底座 `fe-connector-cache` 已经是连接器无关(connector-agnostic)、对全部 7 个 `SPI_READY_TYPES` 生效的通用设施——"统一(goal-1)"这一半其实已经做完;真正剩下的、需要逐连接器补齐的,是 Layer-1 那种"连接器自己是否 memoize 重活"的具体填充,而这只在极少数连接器上是真缺口。**

- **Goal-1(统一缓存框架)最重要的一条建议:不要造"一个连接器无关的元数据大缓存"。** Trino 的实证(INPUT B §3)证明正确的统一粒度是**工具箱(toolkit)**而非缓存本身——因为被缓存对象的类型、失效语义、授权语义天生按连接器不同。Doris 已经站在这条 Trino-correct 路径上:`fe-connector-cache`(`MetaCacheEntry`/`CacheSpec`/`CacheFactory`/`ConnectorPartitionViewCache`)就是 Doris 版的 `io.trino.cache`,并已被 hive/hms/iceberg/maxcompute/paimon 采用。统一工作 = 把**尚未采用工具箱且确有热路径缺口**的连接器接上去(唯一强需求是 **hudi**),外加把 3 个"格式中立"的 iceberg 缓存(table-handle / comment / file-format)在**出现第 2 个消费者时**上收为 `ConnectorXViewCache` 泛型封装——而不是投机式地提前抽象(遵循 Rule 2)。

- **Goal-2(热路径性能 + 一致性)最重要的一条建议:唯一 loop-amplified(iceberg-式)的 P1 缺口是 hudi;maxcompute、es 则是 constant-factor(2–4x)的 P1 收尾。** hudi 是所有 SPI 连接器里缓存最薄的一个:零连接器侧 cross-query 元数据缓存、零 per-statement memo,甚至没有像 hive 那样包 `CachingHmsClient`(用的是裸 `ThriftHmsClient`),导致 `HoodieTableMetaClient` 每个 planning pass 重建约 5–6 次、schema 重解析约 4 次(INPUT D `HD-P01/HD-P02/HD-P03`,P1)。这是与"翻闸前 iceberg 那批被删缓存"最像的问题。其余连接器要么已经很好地缓存(hive/paimon/maxcompute-分区),要么结构上不需要(jdbc/es/trino 是廉价透传或委托嵌入式引擎缓存),只剩若干常数倍(2–4x)的 P1/P2 收尾。

**净结论:引擎接缝(Layer-2 + 其 gate + `fe-connector-cache` + 泛型分区视图缓存 + 通用 `shouldBypass*` 谓词)已通用且承重;iceberg 特有的是"填充物"。推广工作是有选择地填充,不是重新造框架——重点投在 hudi,其次是 maxcompute / es 的少量常数倍收尾。**

---

## 2. 框架解剖:三层 + 已有共享底座

参考 commit 引入的框架分三层(INPUT A 在 HEAD 上核实并锐化):

**Layer-2 —— 连接器无关的 per-statement metadata funnel(已通用、已承重)。**
这是 Doris 版的 Trino per-transaction `CatalogMetadata`(INPUT B §1):保证 *每个 (statement, catalog) 只有一个 `ConnectorMetadata` 实例*,让一条语句里所有 read/scan/DDL/MVCC/write 解析器共享同一个 metadata。核心件:
- funnel 本体 `PluginDrivenMetadata.get(session, connector)`(`PluginDrivenMetadata.java:53`),按 `"metadata:"+catalogId` 记忆(`:70`),并有 fail-loud 身份钉(`:63-69`),把跨用户凭证复用变成硬错误。这是 fe-core 中**唯一**被允许调 `Connector#getMetadata` 的地方。
- SPI 中立接缝 `ConnectorStatementScope`(默认 `NONE` no-op),物理归宿 `StatementContext.connectorStatementScope`,写事务共持有者 `CatalogStatementTransaction`,两遍有序 teardown `ConnectorStatementScopeImpl.closeAll`。
- **关键事实:Layer-2 对全部 `SPI_READY_TYPES = {jdbc, es, trino-connector, max_compute, paimon, iceberg, hms}`(`CatalogFactory.java:56-57`)一体适用,scope 捕获无任何连接器类型分支;hudi 作为 hms 网关下的 sibling 也经 `memoizedSiblingMetadata` 骑同一 scope。** 由 `tools/check-fecore-metadata-funnel.sh` 全构建强制(当前 0 个 exempt marker,写臂已全部入 funnel)。fe-core 中经 funnel 的 call sites 约 **58–59 处**(计法差异,不影响结论)。

**Layer-1 —— 6 个 iceberg 连接器侧 cross-query 缓存(一个*可复制的模式*,不是通用代码)。**
`IcebergTableCache / IcebergPartitionCache / IcebergFormatCache / IcebergCommentCache / IcebergManifestCache / IcebergLatestSnapshotCache`,全部建在共享 `MetaCacheEntry` 之上。其中 **table-handle / comment / file-format** 在意图上格式中立(每个连接器都要 load 表对象 / 取注释 / 推断格式),只是 value 类型 iceberg 化;而 **PARTITIONS 元数据表扫描 / manifest / snapshot+schemaId 原子钉** 是真正 iceberg-格式特有。

**Layer-3 —— `iceberg.rest.session=user` 下的授权缓存隔离。** 名字为 key 的缓存会把"可 LIST 不可 LOAD"用户的元数据泄漏给他;iceberg 在 `isUserSessionEnabled()` 下把这些缓存置 null,fe-core 侧加通用谓词 `shouldBypass{Schema,TableName,DbName}Cache`。由 `tools/check-authz-cache-sharding.sh` 强制,但**该 gate 硬编码只盯 `IcebergConnector.java`**(核实:`TARGET_REL`,`:57`)。

**已有共享底座 `fe-connector-cache`(统一的天然归宿,且泛化已在进行)。** `MetaCacheEntry`(Caffeine-free 的统一 entry API)、`CacheSpec`(`meta.cache.<engine>.<entry>.*` 配置)、`CacheFactory`,以及一个**已经写好的 `IcebergPartitionCache` 泛型版** `ConnectorPartitionViewCache`/`PartitionViewCacheKey`。采用现状:`MetaCacheEntry` 被 hive/hms/iceberg/maxcompute/paimon 用;`ConnectorPartitionViewCache` 至少被 hive、paimon 实例化(INPUT D 确认;iceberg 使用其泛型形态见 INPUT A)。**一处 drift 需修:`ConnectorPartitionViewCache` 类 javadoc 写"NO consumers yet"是 HEAD 陈旧——已有 ≥2 个确认消费者(hive、paimon)(Rule 7/12,doc-only)。**

**这一层的含义:"统一"不是从零造框架,而是:(a) 已通用的 Layer-2 无需再做;(b) 剩余缺口 = Layer-1 式 per-connector memoization + 把确有缺口的连接器接上已有工具箱。**

---

## 3. 横向矩阵 (Cross-Connector Matrix)

来源 INPUT D(已对抗式复核;`verifyVerdict` 见括注)。iceberg 为参考实现,已完成。

| 连接器 | live? | funnel 记忆已加载表? | 现有缓存(要点) | 头号热路径缺口 | authz / session=user 相关? | 优先级 |
|---|---|---|---|---|---|---|
| **iceberg**(参考) | live | **是**(唯一真正 one-load-per-statement:`IcebergStatementScope.sharedTable` + `IcebergTableCache`) | 6 缓存 + per-scan/per-file hoist | —(已修) | **是**(唯一声明 `SUPPORTS_USER_SESSION`) | 参考/已完成 |
| **hive**(=hms) | live | 部分(实例无 memo;靠 cross-query `CachingHmsClient` 折叠) | `CachingHmsClient`(表/分区名/分区/列统计 4 缓存)+ `HiveFileListingCache` + `ConnectorPartitionViewCache` + sibling memo + 写事务快照 | `HMS-H4` ACID `getAcidState` 未缓存(severity none);`HMS-H6` 罕见 TTL/eviction 二次 RPC(P2) | **否**(单一 catalog 身份;RBAC 独立把关) | **LOW(仅 doc 修)** |
| **hudi**(hms sibling,**不在** `SPI_READY_TYPES`) | live | **否**(对象每语句一个,但内部零 memo;scan provider 另建 metaClient) | **零 cross-query 缓存;裸 `ThriftHmsClient`(连 `CachingHmsClient` 都没包)** | `HD-P01` metaClient 5–6x/pass、`HD-P02` schema 4x、`HD-P03` 分区扫描(均 **P1**);`HD-P04` 裸 HMS(P2);`HD-P05` hoist(P2) | 否(无 `getCapabilities` override → 无 `SUPPORTS_USER_SESSION`) | **HIGH / P1(旗舰目标)** |
| **maxcompute** | live | 否(fat-handle 携带 ODPS `Table`,不跨 handle 共享) | `MaxComputePartitionCache`(600s,建在工具箱)+ fe-core schema 缓存;split 枚举干净 | `MC-1` 每次 handle 解析冗余 `tables().exists()` 远程探测,k×/语句(**P1**);`MC-2` lazy `Table` reload(P2) | 否(catalog 静态 AK/SK,单身份) | **MEDIUM / P1(单个小修)** |
| **es** | live | 否(schema/scan-state 均不 memo) | 仅 per-catalog REST client + fe-core schema/rowcount 缓存;连接器侧零 scan 元数据缓存 | `ES-F1` `fetchMetadataState` 2x/查询(shards+nodes,**P1**);`ES-F2` mapping 重取 2x(**P1**);`ES-F3` `getColumnHandles` mapping 2x(P2) | 否(单一 catalog user/password) | **MEDIUM / P1(per-scan hoist)** |
| **jdbc** | live | 否(metadata 构造近零成本) | HikariCP 连接池(per-catalog 单例)+ driver classloader map + fe-core schema/rowcount/name 缓存 | `HP-1` scan 期 `getColumnHandles` 冗余远程取列(P2);`HP-2` 写路径 `buildInsertSql` 新建实例绕开 funnel(P2) | 否(固定 catalog 凭证) | **LOW / P2** |
| **trino-connector** | live(bridge) | 否(委托嵌入式 Trino 连接器自有缓存) | Trino 引导单例 + 每 catalog 一个嵌入式 Trino `Connector`(自带 `CachingHiveMetastore` 等)+ fe-core 缓存 | `TRINO-H1` cold 3x `getTableHandle`(P2,`memoizedAlready=true`);`TRINO-H2` 2N `getColumnMetadata`(P2);`TRINO-H3` 2x `applyFilter`(P2) | 否(静态单身份 `Identity.ofUser("user")`) | **LOW / P2(L1 缓存被明确反指)** |
| **paimon** | live | 部分(fat-handle + paimon SDK `CachingCatalog`) | `PaimonLatestSnapshotCache` + `PaimonSchemaAtMemo` + `partitionViewCache` + SDK `CachingCatalog`(tableCache/partitionCache/manifestCache) | `PA-1` `listPartitionNames/Values` 绕过 `partitionViewCache`(P2,仅 CPU 重渲染) | 否(catalog-创建期认证;REST vended token 是表级、非用户级) | **LOW / P2(一致性小清理)** |

---

## 4. Goal-1:统一缓存框架的建议

**总纲:统一在工具箱层,不在缓存层(Trino-correct)。** INPUT B §3 用 Trino 源码证明:Trino 没有"一个统一连接器缓存",只有共享的 `io.trino.cache` 构造/淘汰安全/统计机具,各连接器在其上各建各的缓存。Doris 已如此。因此 goal-1 拆成四件可落地的事:

**(1) Layer-2 已通用——不动。** funnel + 其 gate + `NONE` read-through + `CatalogStatementTransaction` + `PluginDrivenScanNode` provider/metadata memo,都已连接器无关且被强制。这半个"统一"已完成。

**(2) 真正的"统一"缺口不是缓存,是 per-statement 表解析去重(INPUT B §3 第 2 点)。** Trino 天然免费拿到"每事务每表 load 一次",因为 metadata 实例就是事务级 memo 家;Doris funnel 只保证"每语句一个 metadata 实例",是否折叠重复 load 因连接器而异。**但复核(INPUT C/D)显示这个"统一 helper"实际只对 iceberg(已有)和 hudi(metaClient 重建)是刚需**;maxcompute 因 metadata 已是每语句一个,只需在实例内加一个 `Map<(db,table),Handle>`(比 iceberg 的 scope-keyed memo 更简单);paimon/jdbc/es/trino 不构成远程放大。**建议:提供一个共享的"经 statement scope 解析表"helper 作为可选接缝,但只在 iceberg + hudi 两个真消费者上启用**;不要为 4 个不需要的连接器强推(Rule 2)。

**(3) 可复用原语 vs 必须留在 iceberg 模块的格式特有物——明确区分:**

*可上收为通用原语(有 ≥2 消费者或即将有):*
- **`ConnectorPartitionViewCache`(分区视图缓存)——已经是通用原语,已被 hive、paimon 实例化(iceberg 用其泛型形态)。** 建议 hudi 采用它解 `HD-P03`;maxcompute 现用自建 `MaxComputePartitionCache`(同样建在 `MetaCacheEntry`)可保留或迁移,非必须。
- **`CachingHmsClient`(metastore-client 缓存包装)——已被 hive/hms 共享。** hudi 应直接采用(INPUT D `HD-P04`:一行 parity,立即修 hudi 的裸 HMS 问题)。这是最干净的复用收益。
- **格式中立的 3 个 iceberg 缓存(table-handle / comment / file-format)**:结构上都是 `new MetaCacheEntry<>(name, null, spec, commonPool, false, true, 0L, true)`,可各自折叠成 `ConnectorXViewCache` 泛型封装。**但复核显示其余连接器几乎都不需要连接器侧 table 缓存**(paimon 有 SDK 缓存、hive 有 `CachingHmsClient`、trino 委托嵌入式、jdbc/es 廉价)。唯一新消费者是 hudi(需要 cross-query metaClient/table 缓存)。**因此建议:泛型 table/handle 缓存在 iceberg+hudi 双消费者出现时再上收;在此之前保持单模块,不投机抽象。**

*必须留在 iceberg 模块(真正格式特有,不要泛化):*
- `IcebergManifestCache`(manifest 文件)、`IcebergPartitionCache`(PARTITIONS 元数据表 + transform 分区)、`IcebergLatestSnapshotCache`(snapshot id + schema id 原子钉)。hudi 的对应物(metaClient / timeline / Hudi 分区元数据表)语义不同,应在 hudi 模块各自实现,只共享底层 `MetaCacheEntry`/`CacheSpec`。

**(4) doc-only 统一债:** 修 `ConnectorPartitionViewCache` 的"no consumers yet"陈旧 javadoc;修 hive/hudi/maxcompute 中"Dormant / hms is not in SPI_READY_TYPES / dormant until hms enters SPI_READY_TYPES / today getTableHandle is never called"等一批陈旧注释(hms 已 live,#65473)。

---

## 5. Goal-2:热路径性能 + 一致性路线图

**重要前提:其余连接器里没有一个存在 iceberg 翻闸前那种 P0 loop-amplified 远程回归——那批 P0 就是 iceberg 本身,已修。** 下面按证据 id 与预期倍数削减排序。

### 5.1 查询路径 (query path)

**P1(旗舰):hudi 全套 memo。** 目标削减:
- `HD-P01`(`HudiScanPlanProvider.java:148` 等):`HoodieTableMetaClient` 每 pass 重建 **~5–6x → 1x**(最小无条件 ~3–4x,含条件站点达 5–6x;复核 `verifyVerdict=ADJUSTED`,上界成立)。做法:per-statement 解析后的 metaClient+schema memo,由 metadata 与 scan provider 共享同一 `ConnectorStatementScope`。
- `HD-P02`(`HudiConnectorMetadata.java:797` 等):Avro+InternalSchema 重解析 **~4x → 1x**。
- `HD-P03`(`HudiScanPlanProvider.java:734`):分区元数据表扫描——复核修正:**过滤查询已去重到 ~1 次**(`HudiConnectorMetadata.java:288-291` 注释 + `resolvePartitions` 短路 `:635-638`),"3x/pass" 是上界,真正放大发生在 fe-core 独立多次调 `listPartitions/Names/Values` 或无过滤扫描,以及一次 MTMV refresh 的 4–6 次重枚举。做法:`(table,instant)`-keyed 分区缓存(用 `ConnectorPartitionViewCache`)。

**P1:maxcompute `MC-1`。** `getTableHandle` 每次解析都发冗余 ODPS `tables().exists()` 远程探测(`MaxComputeConnectorMetadata.java:118-130`);funnel 只 memo metadata 不 memo handle,故一条语句 k 个独立 handle 解析站点(复核确认 **10 个直接 `resolveConnectorTableHandle` 站点** + 写能力探测)各付一次 RPC。做法:在**已经是每语句一个**的 metadata 实例内加 `Map<(db,table),Handle>`,并对已解析读路径去掉多余存在性探测。削减:**k×/语句 → 1×/语句**,顺带收 `MC-2` 的 lazy `Table` reload。低风险高杠杆。

**P1:es `ES-F1`/`ES-F2`。** `fetchMetadataState`(shards+nodes+field-context)每查询被调 2 次(`EsScanPlanProvider.java:99,169`),mapping 又被重取(`EsMetadataFetcher.java:57-58`)。做法:provider 已是每 scan-node 单例,加一个按 `(index,columnNames)` 的字段 memo,把 **2x→1x**,零 staleness 风险(同语句)。**重要修正(复核 `verifyVerdict=ADJUSTED`):`ES-F2` 不是"零新增缓存直接复用 schema"——fe-core `ExternalSchemaCache` 只存解析后的列,`EsConnectorMetadata.getTableSchema` 丢弃了原始 mapping(`:90-93` 传空 properties map),而 `resolveFieldContext` 需要原始 mapping。消除它要么让 `ConnectorTableSchema` 携带 field-context,要么加一个 per-statement/cross-query mapping 缓存。** `ES-F3` getColumnHandles mapping 2x 是 P2(在每语句一个的 metadata 上 memo schema 即可)。**注意:ES shard routing 必须留在 per-statement,不能 cross-query 缓存(ES rebalance/refresh 模型)。**

**P2 收尾:** hudi `HD-P04`(裸 HMS→包 `CachingHmsClient`)、`HD-P05`(per-scan hoist metaClient/schema/storage-config/uri-normalizer);paimon `PA-1`(`listPartitionNames/Values` 路由进 `partitionViewCache`,仅省 CPU 重渲染,底层远程已被 SDK partitionCache 挡);jdbc `HP-1`;es `ES-F3`;trino `H1/H2/H3`(见 §7,L1 缓存被反指,只做 CPU 清理)。

### 5.2 写路径一致性 (write path,read+write 共享一 metadata/一 txn)

**机制已通用且被强制:** fe-core 的写路径 `getMetadata` 接缝都经同一 funnel,身份钉护住跨用户凭证复用;对**有写事务的连接器(hive / maxcompute / iceberg)**,读写共享同一 memoized `ConnectorMetadata`,写事务由 `CatalogStatementTransaction` 在同一 scope 上共持有,两遍有序 teardown(先 finalize txn 再 close metadata)。当前 gate 0 exempt marker。
> 例外(不构成一致性风险):jdbc 的 `buildInsertSql` 在连接器内部 new 一个**非 funnel** 的 `JdbcConnectorMetadata`(INPUT D `HP-2`,P2),故 jdbc 的读与写不共享同一 memoized metadata;但 jdbc 写是 BE 逐行 auto-commit(`NoOpConnectorTransaction`),无读写快照一致性需求,因此正确性中立。

**哪些连接器有写路径、需要写事务共持有者:**
- **已有且已接线:hive**(`HiveConnectorTransaction`,捕获 begin-time 表快照供 reject-guard 与 sink)、**maxcompute**(`MaxComputeConnectorTransaction`,write session + block 分配按 txn_id;非-MVCC,无需读写快照共享)、以及 **iceberg**(`CatalogStatementTransaction` 参考)。
- **不需要:** paimon(写**未**迁移,`getCapabilities` 不声明写,`PaimonConnector.java:318`)、jdbc(BE 逐行 auto-commit,`NoOpConnectorTransaction`)、es/trino/hudi(只读,`beginTransaction` throws 或无 override)。

**写路径一致性缺口(相对 Trino,INPUT B §2/§4):** Doris 对 hive 走"粗 REFRESH+TTL",不像 Trino 在事务内围绕写做失效——存在 TTL 有界的 read-your-write 窗口。建议:要么给 `CachingHmsClient` 加写路径失效,要么让写后读走 live(`NONE`/bypass)读。**优先级低(TTL 有界),且仅 hive 相关。**

---

## 6. 一致性与授权 (session=user) 的跨连接器影响

**核心事实:在全部 8 个连接器中,只有 iceberg-REST 声明 `SUPPORTS_USER_SESSION` 并做 per-user 凭证 vending / delegated `loadTable`。** 其余 7 个连接器逐一核实(INPUT D)均为**单一 catalog 级身份**:

- hive/hudi:catalog 级单一 Kerberos keytab principal 或 `context.executeAuthenticated`,无 per-user delegated 凭证、无 REST OIDC、无 per-identity metastore。hudi 的安全性(复核 `verifyVerdict=ADJUSTED`)不靠 hive 网关那个 iceberg-专用 guard(`HiveConnector.java:548-556` 文案是 iceberg-specific,`getOrCreateHudiSibling` 无等价 guard),而靠两条已核实事实:(a) hudi 无 `getCapabilities` override → 继承空默认,无 `SUPPORTS_USER_SESSION`;(b) hive 前门本身从不是 session=user。
- maxcompute:catalog 静态 AK/SK。jdbc:固定 catalog user/password。es:单一 catalog user/password。trino:静态 `Identity.ofUser("user")`,底层用 catalog 属性里的静态凭证。paimon:catalog-创建期认证;REST vended token 是**表级、从共享 catalog session 取,非按 Doris 用户 key**(`PaimonScanPlanProvider.extractVendedToken`),故名字为 key 的缓存不会 list≠load 泄漏。

**推论——直接影响 goal-1 的安全性:**

1. **今天给这 7 个连接器加名字为 key 的 cross-query 缓存都是 authz-安全的**(所有用户看同一 catalog 身份视图,per-user 访问由 fe-core RBAC 独立把关)。因此推荐给 hudi 加的缓存(`CachingHmsClient` + 分区缓存)**当前无需 Layer-3 隔离**。
2. **但这是"当前"的安全,不是"结构性"的安全。** 一旦任何连接器未来加入 per-user 凭证 vending(例如 hudi/hive 接入 REST-OIDC,或 paimon 把 vended token 变成 per-user),名字为 key 的缓存立刻变成 list≠load 泄漏面。而 `check-authz-cache-sharding.sh` **硬编码只盯 `IcebergConnector.java`**(核实 `:57`),不会保护第 2 个连接器。
3. Layer-3 的**策略谓词** `shouldBypass{Schema,TableName,DbName}Cache` 已是连接器通用的 fe-core 代码(capability+credential gated),只是目前只有 iceberg-REST 触发它——这一半已经为未来准备好了;缺的是**缓存置 null 的连接器侧动作**与**gate 的通用化**。

**建议:** 不要"统一"进一个元数据泄漏 bug。在给任何连接器加名字为 key 的 cross-query 缓存前,把"该连接器是否 vend per-user 凭证"作为准入检查项;并考虑把 `check-authz-cache-sharding.sh` 从 iceberg-only 改造成"扫描任意声明 `SUPPORTS_USER_SESSION` 的连接器"(见 §8 决策 3)。授权对比 Trino(INPUT B §2b):Trino 在 impersonation 下是**按身份分片缓存**(`LoadingCache<user, CachingHiveMetastore>`)而非禁用;Doris 目前是**禁用**——安全但更慢,长期方向是 Trino 式 per-identity keying,但那是更大的改动,当前"禁用"是正确的保守首版。

---

## 7. 风险与反对意见

**(1) 对廉价/委托型连接器套 iceberg 重框架 = 投机,违反 Rule 2。**
- **jdbc**:关系型透传,`planScan` 零远程 IO、恒一个 scan range,无 split/file/manifest/partition/snapshot 层。昂贵元数据(schema/columns、row-count、名录)已被 fe-core cross-query 缓存前置,连接池是 per-catalog 单例。加连接器侧 table 缓存无对象可缓存。
- **es**:只读、非分区、单凭证、无 snapshot/manifest/stats/write。除 `ES-F1` 的 per-scan hoist 外,几乎每个 iceberg 件都不适用;且 **shard routing 绝不能 cross-query 缓存**(freshness/rebalance)。
- **trino**:**主动反指(actively contraindicated)。** 它是 bridge,元数据缓存/失效/split 枚举全委托嵌入式 Trino 连接器。加 Doris 侧 table/handle 缓存会**双重缓存**并把 schema 跨外部 ALTER 冻结,重新引入 Trino 已解决的 stale-metadata bug 类;且 `TrinoTableHandle` 是事务派生、transient/不可序列化。只做 P2 CPU 清理(`H2` 去掉 2N 重取、`H3` 去掉 double `applyFilter`)。
- **paimon**:SDK `CachingCatalog`(默认开)已提供 in-memory tableCache/partitionCache/manifestCache;连接器又已恢复 3 个 legacy 语义缓存。加连接器级 table 缓存会与 SDK 缓存重复。只有 `PA-1` 一个 P2 一致性清理值得做。

**(2) 强推所有连接器上同一种缓存形状 = 与 per-connector 语义作对。** 被缓存 value 类型、key、失效、授权语义天生不同(一个 iceberg `Table` 不是 `HmsTableInfo` 不是 paimon `Table` 不是 ODPS `Table`)。Trino 明确拒绝"一个缓存统治所有"(INPUT B §3),Doris 若比 Trino 抽象得更狠会打同一场必输的仗。**正确姿势:统一 toolkit,按连接器各建缓存。**

**(3) hive 已 framework-aligned,勿冗余套用。** hive 已有 `CachingHmsClient`(4 缓存)+ `HiveFileListingCache` + `ConnectorPartitionViewCache` + sibling memo + 写事务快照;`HMS-H4`(ACID `getAcidState` 未缓存)是 snapshot/write-id 依赖、legacy-parity 正确,**应保持不缓存**;`HMS-H6` 罕见二次 RPC 的 belt-and-suspenders per-statement memo **不划算**(HMS `getTable` 远比 iceberg `loadTable` 便宜)。hive 的实际工作只有 doc 修。

**(4) 一致性回退风险(低但需记):** §5.2 的 hive 写后读 TTL 窗口;若强行给 hudi/es 的 freshness-敏感数据加 cross-query 缓存会引入陈旧读——故 hudi 的 metaClient/分区缓存要 TTL+REFRESH 有界,es 的 shard routing 保持 per-statement。

---

## 8. 给用户的开放问题 / 需拍板的决策

1. **范围(scope):** 本轮只打 hudi(唯一 P1 真缺口),还是把 3 个 P1 常数倍收尾(maxcompute `MC-1`、es `ES-F1/F2`)一并纳入?我的建议是 **hudi 单独立项 + maxcompute/es 各一个小 PR**,jdbc/paimon/trino P2 进 backlog。请拍板是否同意这个切分。

2. **排序(sequencing):先建共享底座还是先按连接器修?** 由于泛型 table/handle 缓存目前只有 hudi 一个新消费者,我倾向 **per-connector-first**:hudi 直接复用**已存在**的 `CachingHmsClient` + `ConnectorPartitionViewCache` + 一个 hudi 模块内的 metaClient 缓存(建在 `MetaCacheEntry`),等 iceberg 的 3 个格式中立缓存出现第 2 个消费者再上收为 `ConnectorXViewCache`。是否接受"先修后抽象、不投机泛化"?

3. **`check-authz-cache-sharding.sh` 是否现在就通用化?** 现硬编码只盯 iceberg。选项:(a) 现在改成"扫描任意声明 `SUPPORTS_USER_SESSION` 的连接器"(前瞻,防止未来第 2 个 session=user 连接器 unify 出泄漏);(b) 保持 iceberg-only,待真出现第 2 个再改。今天所有推荐的新缓存都 authz-安全,故 (b) 无当下风险,但 (a) 更稳。请选。

4. **是否投入共享的"经 statement scope 解析表"helper?** 它是 INPUT B 眼中"统一连接器缓存框架"的真正含义,但复核显示只有 iceberg(已有)+ hudi 刚需。选项:(a) 提炼成 fe-core 共享接缝供两者用;(b) 让 hudi 在自己模块内做 memo,不提炼。建议 (b)(Rule 2),除非预期近期有第 3 个消费者。

---

## 9. 建议的后续任务空间 (workspaces & sequencing)

建议按 `plan-doc/perf-hotpath-iceberg/` 的布局镜像出 per-connector workspace(每个含:热路径审计、problem-class 归类、修复设计、mutation/验证门):

- **`plan-doc/perf-hotpath-hudi/`(P1,旗舰,先做)** —— 覆盖 `HD-P01/02/03/04/05`。核心交付:(1) per-statement resolved-metaClient+schema memo,由 `HudiConnectorMetadata` 与 `HudiScanPlanProvider` 共享 scope(杀 `HD-P01/02`);(2) 包 `CachingHmsClient`(一行 parity,修 `HD-P04`);(3) cross-query metaClient/table 缓存 + `(table,instant)`-keyed 分区缓存(用 `ConnectorPartitionViewCache`,修 `HD-P03` 的 MTMV/SHOW PARTITIONS 重枚举);(4) per-scan hoist(`HD-P05`)。无 authz、无写事务工作。
- **`plan-doc/perf-hotpath-maxcompute/`(P1,小)** —— 单点:在每语句一个的 `MaxComputeConnectorMetadata` 内加 `Map<(db,table),Handle>` 并去掉已解析读路径的冗余 `exists()` 探测(修 `MC-1`,顺带 `MC-2`)。
- **`plan-doc/perf-hotpath-es/`(P1,小)** —— per-scan hoist `EsMetadataState`(修 `ES-F1`)+ 决定 `ES-F2` 的 mapping/field-context 承载方式(enrich `ConnectorTableSchema` 或新增 mapping 缓存——注意这不是"零新增缓存"改动)+ 在 metadata 上 memo schema(`ES-F3`)。**约束:shard routing 保持 per-statement。**
- **`plan-doc/cleanup-stale-connector-docs/`(doc-only,可随手做)** —— 修 `ConnectorPartitionViewCache` "no consumers yet";修 hive(`CachingHmsClient:85-88`、`HiveFileListingCache:72-74`、`HiveConnector.wrapWithCache:667-668` 及 `:118,126-127` sibling 字段注释)、hudi(`HudiConnectorMetadata.java:391`、`HiveConnectorMetadata.java:410,1802,1942,1972,2007`)、maxcompute(`beginTransaction:332`、`MaxComputeWritePlanProvider:74`)的"dormant / 未在 SPI_READY_TYPES / never called"陈旧注释。
- **`plan-doc/perf-hotpath-backlog-p2/`(P2,可延后)** —— paimon `PA-1`;jdbc `HP-1/HP-2`;trino `H1/H2/H3`(仅 CPU 清理,不加 L1 缓存);hive 写后读一致性(给 `CachingHmsClient` 加写路径失效或 bypass 读)。

**排序建议:** 先 hudi(唯一 P1 真缺口、收益最大)→ 并行 maxcompute + es 两个小修 → doc-only 清理随时 → P2 backlog 按热点 profile 触发。共享底座泛化(iceberg 3 缓存上收 + `check-authz-cache-sharding.sh` 通用化)留待 §8 决策拍板后,且仅在出现第 2 个消费者时启动,避免投机抽象。