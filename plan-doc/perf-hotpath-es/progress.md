# Progress — perf-hotpath-es（append-only）

## 2026-07-24 — Round 1 三件全做（commits `7d74ba1161b` F1+F3、`7466b354901` F2）

**做了什么**：es 连接器一条 SELECT 对同一索引重复远程抓 mapping/分片/节点。三件收敛，按新鲜度分层：
1. **per-scan hoist**（ES-F1）：`EsScanPlanProvider` 加 `EsMetadataState memoizedState` 标量字段（guard on (index,columns)，plain——ES 非 batch 单线程），`planScan`/`buildScanNodeProperties` 共用一次 fetch。分片随 provider per-scan 新鲜、不跨查询。
2. **per-statement schema memo**（ES-F3）：`EsConnectorMetadata` 加 `Map<index,ConnectorTableSchema>`（CHM）`computeIfAbsent`；`getColumnHandles→getTableSchema` 折叠为每语句 1 次。镜像 maxcompute。
3. **cross-path mapping**（ES-F2，owner 拍板走"每语句共享作用域"）：`fe-connector-api` 加命名空间 `ES_INDEX_MAPPING`（iceberg/hudi 既定模式），`EsStatementScope.sharedIndexMapping` 把**原始 mapping 字符串**存每语句 `ConnectorStatementScope`；schema 路径与 `EsMetadataFetcher.fetchMapping` 都走它、各自派生自己产物；session 穿到 fetcher/provider fetch 路径。**只共享原始 mapping；分片/节点绝不入作用域**。

**总账**：每语句 `~4× getMapping + 2× search_shards + 2× _nodes` → **1×/1×/1×**。**0 fe-core**（`git diff fe/fe-core` 空）。

**流程**：6-agent HEAD 只读侦察（确认 provider per-scan 单例、ES 非 batch→plain 字段、承载 mapping 须原始 JSON、`ConnectorStatementScopes` 已预留 es 命名空间）→ 设计（`designs/round-1-design.md`）→ 向 owner 中文讲清三件 + F2 三条承载路（甲坑/乙净/丙延），**owner 选"三件全做（F2 走乙）"** → 实现（分两笔：F1+F3、F2）→ 验证。

**验证**：全 `Es*` **94 测试绿** + checkstyle 0；`EsScanPlanProviderTest` 12 例。**Rule 9 变异三处**：去 F1 store → `ShareOneFetch` 红；F3 `schemaMemo.clear()` → `SchemaMemoized` 红；F2 fetcher 绕作用域 → `MappingSharedAcross` 红（各只对应门禁红、其余绿）。**净室 4-lens + verify**：parity/concurrency（含 schemaMemo↔scope 嵌套 computeIfAbsent 不同 map 无重入）/freshness 全 `PARITY_HOLDS`；唯一 CONFIRMED = "无门禁钉分片不入作用域" → **补 `ShardRoutingNeverSharedViaScope`**（两 provider 共享 live scope → shard/node 各 2、mapping 1）；nits（List.equals 顺序=错失优化非错误、死代码 threading）评估接受。

**未做/延后**：ES-F4（`existIndex` 的 `_mapping` GET 去重，低优先）；死代码 `EsConnectorMetadata.fetchMetadataState` 清理（本轮保留）。**e2e 需集群本地未跑**（独立 es catalog 过滤 SELECT 断言 getMapping/search_shards/_nodes 计数 + 分片再平衡新鲜度），留标注。

**下一步（伞形）**：WS-ES 完成 → mc/es 两小 PR 均落地。剩 P2 backlog（热点触发）+ 门禁通用化 + 陈旧注释清理 + 各连接器 e2e 统一补。
