# Round 1 设计 — es 连接器元数据抓取去重（per-scan hoist + per-statement schema memo + cross-path mapping scope）

> 兄弟空间 = `plan-doc/perf-hotpath-es/`（伞形 = `plan-doc/connector-cache-unification/`，行 WS-ES）。
> 镜像 `perf-hotpath-iceberg/` / `perf-hotpath-maxcompute/` 布局。铁律：0 fe-core、纯连接器侧、parity 只减不改。
> **行号是 2026-07-24 HEAD 侦察快照**（6-agent 只读侦察）。

## 1. 病灶（root cause）

es 一条普通 `SELECT ... WHERE ...`（schema cache 暖态）对同一 ES 集群约发 **~4× getMapping + 2× search_shards + 2× _nodes/http** 远程往返（每次 OkHttp 10s read timeout），大部分冗余：

- **ES-F1（P1，2×）**：`EsScanPlanProvider.planScan`（`:99`）与 `buildScanNodeProperties`（`:169`）各自完整 `fetchMetadataState`（`:273`→`EsMetadataFetcher.fetch` = getMapping + search_shards + _nodes）。二者同属**每 scan-node 单实例**的 provider（fe-core `ResolvedScanProvider` 按 currentHandle identity memo），却各抓一遍。
- **ES-F3（P2，2×）**：`EsConnectorMetadata.getTableSchema`（`:85`）每次远程 getMapping；`getColumnHandles` 再调 `getTableSchema` → 每语句 2× 冗余 mapping。metadata 是每语句单实例，未 memo。
- **ES-F2（P1，~2×，跨路径）**：schema 路径与 scan 路径**各自**远程 getMapping 同一份 index mapping、派生不同产物（列 vs field-context），且 fe-core 已缓存的 `ConnectorTableSchema` **丢弃原始 mapping**（`Collections.emptyMap()` properties），无法直接复用（复核 ADJUSTED：须让原始 mapping 有承载方）。

**硬约束**：分片路由 + 节点拓扑**必须每语句/每 scan 重解析、绝不跨查询缓存**（ES rebalance/refresh 模型）。**无写路径**（只读）、**单一凭证**（authz 安全）。

## 2. 修复（三件，按新鲜度分层）

| 件 | 位置 | 机制 | 消除 |
|---|---|---|---|
| **F1** per-scan hoist | `EsScanPlanProvider` | 加 `private EsMetadataState memoizedState` 标量字段，`fetchMetadataState` 命中即复用；guard on `(index, columns)`（不同请求重抓）。**plain 字段**（ES 非 batch、单线程规划；注释标注若接 batch 须 volatile）。 | scan 侧 search_shards 2→1、_nodes 2→1、mapping 2→1 |
| **F3** per-statement schema memo | `EsConnectorMetadata` | 加 `Map<String, ConnectorTableSchema> schemaMemo`（CHM）按 index `computeIfAbsent`。read-only metadata，无失效。镜像 maxcompute 每语句 memo。 | schema 侧 getMapping 2→1 |
| **F2** cross-path raw-mapping scope | `ConnectorStatementScopes`(api) + `EsStatementScope` + 两路 getMapping 包裹 | 新命名空间 `ES_INDEX_MAPPING`；`EsStatementScope.sharedIndexMapping(session, index, loader)` 经 `resolveInStatement` 把**原始 mapping 字符串**存每语句作用域（key=`es.index_mapping:catalogId:default_db:index:queryId`）；schema 路径与 `EsMetadataFetcher.fetchMapping` 都走它；session 穿到 fetcher/`fetchMetadataState`/`buildScanNodeProperties`。**只共享原始 mapping（语句内稳定）；分片/节点绝不入作用域**。 | 跨路径 getMapping 2→1 |

**总账**：`~4× getMapping + 2× search_shards + 2× _nodes` → **1× getMapping + 1× search_shards + 1× _nodes**（每语句/每 scan）。**0 fe-core**（`ConnectorStatementScopes` 加命名空间常量在 `fe-connector-api`，是 iceberg/hudi 已用的既定模式）。

## 3. 关键取舍（诚实）

| # | 取舍 | 决定 | 理由 |
|---|---|---|---|
| A | F2 原始 mapping 承载方式（甲 塞 ConnectorTableSchema.properties / 乙 每语句作用域 / 丙 不做） | **乙**（owner 拍板"三件全做"） | 甲=污染 schema 缓存 + 喂回 scan 须改 fe-core（碰铁律 A，坑）；乙=0 fe-core、只需 session 穿一层（连接器内部）；两路都收 `session`，经既存作用域机制共享。 |
| B | F1 memo 键：标量+guard vs Map | **标量 + `(index,columns)` guard** | 一 provider = 一 scan-node = 一 (index,columns)；guard 令复用不同请求时重抓（`testDifferentIndexes` 绿）。分片随 provider per-scan GC，新鲜。 |
| C | F1 线程安全：plain vs volatile | **plain** | ES 从不进 batch mode（不 override `supportsBatchScan`）→ planScan/buildScanNodeProperties 同步单线程；fe-core 兄弟 memo 字段亦 plain。注释标注 batch 须 volatile。 |
| D | 只共享原始 mapping，不共享 field-context/分片 | **只 mapping** | field-context 依赖投影列（每 scan 不同）；分片/节点新鲜度敏感（硬约束）。两路各自从共享 mapping 派生自己的产物。 |
| E | metadata 死方法 `fetchMetadataState`（无调用者）threading session | **保留**（穿 session 使其编译且语义正确） | 纯连接器死代码、非 SPI；删属 scope creep（Rule 3）；穿 session 行为中性。**记为未来可清理**。 |

## 4. 验证

- **守门单测**（`EsScanPlanProviderTest`，复用 `CountingRestClient`，无 Mockito、离线）：
  - F1：`ShareOneFetch`（一 scan-node planScan+props → mapping/shard/node 各 1）；`DifferentIndexesFetchSeparately`（一 provider 两 index → 2）；`SeparateProviderInstancesEachFetch`（两 provider 同 index → 2，per-scan 新鲜）；`DifferentColumnsRefetch`（不同投影重抓）。
  - F3：`SchemaMemoizedPerStatement`（getTableSchema+getColumnHandles → getMapping 1）；`SchemaFreshPerStatement`（新 metadata → 重抓）。
  - F2：`MappingSharedAcrossSchemaAndScanPathsWithinStatement`（共享 live scope → getMapping 1 across 两路）；`ScopedMappingNotSharedAcrossStatements`（不同 scope → 2）；**`ShardRoutingNeverSharedViaScope`（两 provider 共享 live scope → shard/node 各 2、mapping 1，钉死硬约束）**。
- **回归**：`install -pl :fe-connector-api,:fe-connector-es -am -Dtest='Es*,ConnectorStatementScopes*'` → BUILD SUCCESS + 全绿（92+ 测试）+ checkstyle 0。
- **Rule 9 变异**（三处，均逐一验证变红）：去 F1 store → `ShareOneFetch` 红；F3 `schemaMemo.clear()` → `SchemaMemoized` 红；F2 fetcher 绕作用域 → `MappingSharedAcross` 红。
- **净室对抗复审**（4 lens + verify）：parity/concurrency/freshness `PARITY_HOLDS`；唯一 CONFIRMED = "无门禁钉分片不入作用域" → **已补 `ShardRoutingNeverSharedViaScope`**；nits（List.equals 顺序=错失优化非错误、死代码 E）已评估接受。
- **e2e**：需集群，本地未跑；留标注（独立 es catalog 的过滤 SELECT 断言 getMapping/search_shards/_nodes 计数 + 分片再平衡后新鲜度）。

## 5. commit（两笔）

1. `[perf](catalog) fe-connector-es: hoist per-scan metadata state and memoize schema per statement`（F1+F3）。
2. `[perf](catalog) fe-connector-es: share one index mapping fetch per statement across schema and scan paths`（F2，含 api 命名空间 + EsStatementScope）。
