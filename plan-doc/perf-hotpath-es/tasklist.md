# Task List — perf-hotpath-es

> ID 一旦分配永不复用。状态：⏳待启动 · 🚧进行中 · ✅完成 · 🔬复核中 · 🅿待拍板 · 🚫不立项
> 权威分析：伞形 `plan-doc/connector-cache-unification/connectors/es.md` + 本空间 `designs/round-1-design.md`。

## Round 1（全部完成 2026-07-24；owner 拍板"三件全做"）

| ID | 主题 | 覆盖发现 | 依赖 | 状态 |
|---|---|---|---|---|
| **PERF-ES01** | per-scan hoist：`EsScanPlanProvider` memo `EsMetadataState`（标量 + (index,columns) guard，plain 字段/非 batch）→ scan 侧 mapping/shard/node 各 2→1 | ES-F1（P1） | — | ✅ commit `7d74ba1161b` |
| **PERF-ES02** | per-statement schema memo：`EsConnectorMetadata` 按 index memo `ConnectorTableSchema`（CHM，read-only 无失效，镜像 maxcompute）→ schema 侧 getMapping 2→1 | ES-F3（P2） | — | ✅ commit `7d74ba1161b` |
| **PERF-ES03** | cross-path mapping：`ConnectorStatementScopes.ES_INDEX_MAPPING` + `EsStatementScope` 把原始 mapping 存每语句作用域，schema/scan 两路共享 → 跨路径 getMapping 2→1（分片/节点绝不入作用域） | ES-F2（P1） | — | ✅ commit `7466b354901`（0 fe-core） |

> 三件总账：每语句 `~4× getMapping + 2× search_shards + 2× _nodes` → **1×/1×/1×**。94 测试绿 + checkstyle 0 + 三处变异验证 + 4-lens 净室复审（parity/concurrency/freshness HOLD；补 `ShardRoutingNeverSharedViaScope` 钉硬约束）。

## 延后 / 不立项（记录，勿抢跑 — 伞形审计 §7）

| ID | 主题 | 覆盖发现 | 状态 |
|---|---|---|---|
| PERF-ES04 | `getTableHandle→existIndex` 的 `_mapping` GET（与首次 schema getMapping 同 endpoint，可再去重）；轻量、RowCountCache 兜底 | ES-F4（P2） | ⏳（低优先，热点触发） |
| — | 跨查询 shard 缓存 / authz 隔离 / write-txn / partition·format·manifest·comment 缓存 | — | 🚫 不适用（ES rebalance 硬约束 / 只读 / 单凭证 / 非分区 / 格式常量 / 无 manifest） |
| — | 死代码 `EsConnectorMetadata.fetchMetadataState`（无调用者，被 provider 版取代） | — | ⏳（可随手清理；本轮保留，穿 session 行为中性，见设计 §3-E） |
