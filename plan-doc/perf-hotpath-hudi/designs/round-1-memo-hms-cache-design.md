# Round-1 设计 — Hudi 每语句投影 memo + HMS 缓存层（新鲜读拆分 + REFRESH 失效）

> 基线 HEAD `f2e9706df5a`（动每个文件前仍按符号重 grep，行号会漂）。
> 调研+设计+3 路红队：workflow `wf_3a0b9aca-966`（transcript 在 session subagents/workflows/）。
> owner 已拍板范围：**旗舰 memo + 文档清理 + HMS 缓存层（按 hive 补齐 fresh/cached 拆分 + REFRESH 失效）**。
> 铁律：fe-core 源只出不进；通用节点 connector-agnostic；freshness-敏感跨查询缓存必 TTL+REFRESH 有界。

---

## 0. 侦察确认（HEAD 核实的真实倍数）

一条**带分区谓词的普通 SELECT**，单次规划 pass 对同一张表：
- **metaClient 重建 ~5 次**：3 处铁定（`planScan`@148、`getColumnHandles→getSchemaFromMetaClient`@802、`beginQuerySnapshot→latestInstant`@751）+ 1 处默认开（`getScanNodeProperties`@363，`!force_jni` 门控）；@incr 再 +1。每次远程读时间线。
- **最新 schema 重解析 3 次**（`HudiConnectorMetadata`@820、`HudiScanPlanProvider`@188+@382）。
- 分区列举：过滤查询已去重到 ~1 次（`resolvePartitions` 命中 `prunedPartitionPaths` 短路 @635-638）；放大在 MTMV。
- `HudiConnectorMetadata` 字段仅 `{hmsClient, properties, metaClientExecutor, storageHadoopConfig}`（@154-162）→ **零 memo**；`HudiScanPlanProvider` 字段仅 `{properties, context}`（@107-108）。两者各自独立建 metaClient。

生命周期关键事实（自测已核）：
- `ConnectorStatementScopeImpl` **语句末关闭所有 AutoCloseable 缓存值**（pass 2 @82-90）。
- `HoodieTableMetaClient`（本工作树编译的 1.0.2）只 `implements Serializable`，**非 AutoCloseable** → 放进作用域不会被关。但它可变（`reloadActiveTimeline`）、且 `planScan` 的**逐文件** schema_id resolver（@232-241）闭包捕获活 metaClient → 不宜跨 metadata→off-thread 扫描边界共享活对象。
- `HoodieTableFileSystemView` **是** AutoCloseable → **绝不能** memo（会被 pass-2 关，后续 use-after-close）。

→ **memo 只存不可变投影，绝不存活 metaClient / fsView / 可变 Configuration。**

---

## 1. 本轮三块（各自独立 commit）

### Piece A — 陈旧注释清理（纯 doc，零风险，先做）

`#65473`/`6e521aa64b2`（2026-07-16）把 hms 翻 live，但一批注释仍写"dormant until hms enters SPI_READY_TYPES / getTableHandle is never called"。删/改（动码前 grep 定位当前行）：
- 删：`HudiConnectorMetadata.java`（~392）、`HudiReadOnlyWriteRejectTest.java`、`HudiConnectorOwnsHandleTest.java` 里的过时从句。
- 改词（非删）：`HiveConnectorMetadata.java`、`HiveConnectorMetadataFreshnessTest.java`（hms live + MVCC/MTMV freshness 已落地）。
- `HudiScanPlanProvider.java`（~250）只删"while dormant"借口，**保留** @incr over-read 真实 caveat。
- **勿动**：`HiveConnector.java`（~756）"dormant"=sibling 懒建从未构建（正确）、`HudiScanPlanProvider.java`（~216）"dormant"=BE history-schema-dict 轴（不同义，正确）。

### Piece B — 旗舰：每语句"不可变投影" memo（红队收紧后）

**新增（0 fe-core）**：
1. `ConnectorStatementScopes`（fe-connector-**api**）加常量 `HUDI_METACLIENT = "hudi.metaclient"`（镜像 `ICEBERG_TABLE`；该类 javadoc 已预留该名）。**非 fe-core**。
2. `HudiStatementScope.sharedTableFacts(session, db, table, loader)`（新，fe-connector-hudi）→ 委派 `ConnectorStatementScopes.resolveInStatement(session, HUDI_METACLIENT, db, table, loader)`。
3. 连接器内投影 builder：`HudiMetaClientExecutor.execute` 内建 **一次** metaClient，导出不可变事实，**丢弃**活 metaClient。

**memo 值（不可变投影，key = `hudi.metaclient:catalogId:db:table:queryId`）**：
- `latestCompletedInstant`（String）
- 最新 schema：`List<ConnectorColumn>` + latest Avro `Schema` + `ResolvedInternalSchema`（字段 ID）
- `allHistoricalSchemas`（喂 schema-evolution dict）
- **不含**：Configuration（每消费点各自 `buildHadoopConf()` 重建，纯 CPU、无远程）；活 metaClient；fsView；at-instant schema。

**消费点改挂**：
- `getSchemaFromMetaClient`（2-arg 最新路径 @798）→ 读 memo schema。
- `latestInstant`（beginQuerySnapshot @748）→ 读 memo instant。
- `getScanNodeProperties` 普通路径 dict（@361-386）→ 读 memo 的 latest Avro + allHistoricalSchemas（把 `buildSchemaEvolutionProp` 重构成接受这两者，而非从 metaClient 现取）→ **消除其 build**。
- `planScan`（@148）→ **只读 memo schema**（@188/@220）；**保留自己的** `lastInstant`（@163-177，字节一致的既有语义，勿改）+ 自己的活 metaClient（fsView + 逐文件 resolver）。

**明确不改挂（keep 今天的独立 build）**：at-instant / FOR TIME AS OF（3-arg `getTableSchema` @336、`getScanNodeProperties` pinned 分支 @373-380）、@incr `resolveIncremental`。罕见路径，不改进不回退。

**效果**：~5 build → ~2-3；schema 3×→1×；输出逐字节不变；并发下更一致（消除今日跨 metaClient 的 latest-commit 竞态）。

**loader 放置（诚实措辞）**：loader 跑在 `HudiMetaClientExecutor.execute`（TCCL pin + plugin doAs）；正常规划 metadata 先于 scan，故首触在 metadata 侧；若 scan 侧 miss 则在扫描线程按幂等重算，**逐字节相同**（`computeIfAbsent` 原子，不重复 build）。build-count 断言 metadata 侧计数。

### Piece C — HMS 缓存层（wrap + 新鲜读拆分 + REFRESH 失效）

**wrap**：`HudiConnector.createClient`（@186）→ `new CachingHmsClient(new ThriftHmsClient(config, authAction), properties)`。无 pom 改动（fe-connector-hms 已是直接依赖，Caffeine 3.2.3 已随 hudi-common 打包——**勿加 2.9.3，否则 nearest-wins 降级 hudi-common 的 3.2.3**）。

**fresh/cached 拆分**（镜像 hive `collectPartitionNames(handle, bypassCache)`）：给 `HudiConnectorMetadata.collectPartitions` 加 `bypassCache` 参数，只影响 **hive-sync 分支**的 HMS 列举（非-hive-sync 走 metaClient，与 HMS 缓存无关）：
- `listPartitionNames`（SHOW PARTITIONS + partitions TVF）→ `bypassCache=true` → hive-sync 分支用 `hmsClient.listPartitionNamesFresh`。
- `listPartitionValues`（partition_values TVF）→ `bypassCache=true` → fresh。
- `listPartitions`（查询剪枝 / MTMV）→ `bypassCache=false` → 走缓存（这是 parity 收益点）。
- `applyFilter` hive-sync 剪枝（@279）→ 走缓存（剪枝路径，`bypassCache=false` 语义）。

**REFRESH 失效**：`HudiConnector` override `invalidateTable/invalidateDb/invalidateAll` → 读 volatile `hmsClient` 字段（**不 force-build**：null=从未建=无缓存可刷）→ `instanceof CachingHmsClient` 则 `flush(db,table)`/`flushDb(db)`/`flushAll()`。网关 `HiveConnector.forEachBuiltSibling` 已把 REFRESH 转发给 hudi sibling（@358），故 override 后 REFRESH 生效。hudi 无 fileListingCache/partitionViewCache，故仅 flush HMS 缓存。

**getTableHandle**：`tableExists`（@205，CachingHmsClient pass-through）+ `getTable`（@208，缓存）→ 每次重复查询省 1 RPC；getTable 24h 缓存与 hive 一致，REFRESH 可清。

---

## 2. 验证计划

- 全反应堆 `mvn install -pl :fe-connector-api,:fe-connector-hudi,:fe-connector-hive -am -Dmaven.build.cache.enabled=false`（install 非 test；见 build 坑）→ BUILD SUCCESS。
- **旗舰 build-count 守门（单测）**：注入 `DirectHudiMetaClientExecutor`（现有测试替身，无 auth/TCCL），在**真（非 NONE）** `ConnectorStatementScope` 下驱动 `getColumnHandles + beginQuerySnapshot + getTableSchema(2-arg)`，断言 **metadata 侧 build=1**（非 3）+ 输出 schema/instant 与 NONE-scope 路径**逐字节相同**（Rule 9：编码"同值、更少 build"）。⚠ counter 须覆盖 `buildMetaClient`@660 **和** planScan inline@148（或先把@148 路由过@660）。
- 扩现有 `HudiSchemaParityTest`/`HudiSchemaAtInstantTest`/`HudiTimeTravelTest`/`HudiConnectorPartitionListingTest`：加 memo-on 变体断 parity + NONE-scope 变体断字节一致。
- **HMS wrap 单测**：断 `createClient` 返回 `CachingHmsClient` 包 `ThriftHmsClient`；mock delegate 下第二次 `getTable` 命中缓存不重打、`tableExists` pass-through；`listPartitionNames` 走 fresh（bypass）、`listPartitions` 走缓存；`invalidateTable`→`flush` 生效。
- **e2e 本地跑不了**（p2/external + 集群 + docker）。现有套件已覆盖网关路（全 type=hms）：分区剪枝/快照/MTMV/SHOW PARTITIONS/时间旅行/增量/schema 演进——证 parity。异构三向目录新用例本轮只给规格（放 `test_hive_hudi.groovy` 旁），交上游集群跑。

---

## 3. 红队三条 load-bearing 结论（实现须守）

1. **不 memo Configuration**（reviewer-1）：`HadoopStorageConfiguration(conf)` by-reference 包裹 + 可变 + 非线程安全 + off-thread 并发读 → 共享会引入今日不存在的竞态。只 memo 不可变 map，各消费点重建 conf。
2. **planScan 只读 memo schema、不读 memo instant**（reviewer-1/3）：planScan 普通读的 `timeline.lastInstant()`（@166/@175）是**文档化的字节一致语义**（`HudiConnectorMetadata`@558-560）；喂 memo instant 会在并发写下选到旧提交、且省 0 build。
3. **旗舰 latest-only、at-instant 延后**（reviewer-3）：at-instant memo 会让投影退化成惰性/带锁/摸 metaClient，违"不可变投影"初衷；罕见路径保持独立 build。

授权/线程 HOLDS（三红队一致）：hudi 无 `SUPPORTS_USER_SESSION`（网关 fail-loud 守卫覆盖）、单身份 → name-key 缓存 authz-安全；作用域构造期捕获 → off-thread 扫描复用同 session；memo 非 AutoCloseable 不被 pass-2 关。

---

## 4. 延后（记录，勿抢跑）

- 跨查询 metaClient + `(table,instant)` 分区缓存（MTMV/重复查询延迟）——下一轮，与更宽失效设计一起。
- at-instant / FOR TIME AS OF 的 memo。
- 跨方法 `buildHadoopConf` 去重 + 逐文件 normalize（HD-P05 余项，纯 CPU，且 Configuration 别名 caveat）。
- @incr `(begin,end]` 行级过滤功能缺口（是功能 bug 非缓存，单独立项）。
- 异构 hudi+iceberg+hive 单-hms-catalog e2e（需集群）。
