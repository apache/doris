# Task List — perf-hotpath-maxcompute

> ID 一旦分配永不复用。状态：⏳待启动 · 🚧进行中 · ✅完成 · 🔬复核中 · 🅿待拍板 · 🚫不立项
> 权威分析：伞形 `plan-doc/connector-cache-unification/connectors/maxcompute.md` + 本空间 `designs/round-1-handle-memo-design.md`。

## Round 1

| ID | 主题 | 覆盖发现 | 依赖 | 状态 |
|---|---|---|---|---|
| **PERF-MC01** | 每语句表句柄记忆化（per-statement metadata 实例上 `Map<(db,table),handle>`，`computeIfAbsent`，present-only）→ 冗余 ODPS `exists()` 探测 k×→1×、Table reload 每表 1× | MC-1（P1）+ MC-2（P2） | — | ✅ commit `58daadd10e0`（全模块 120 测试绿 + checkstyle 0 + 两处变异验证 + 净室复审 parity/staleness HOLD、concurrency REFUTED、test-quality 补跨-db 用例） |

> 0 fe-core、无 pom 改动、纯连接器侧、authz 天然合规（静态凭证单身份）。

## 延后 / 不立项（记录，勿抢跑 — 伞形审计 §6/§7）

| ID | 主题 | 覆盖发现 | 状态 |
|---|---|---|---|
| PERF-MC02 | 陈旧注释清理（`beginTransaction` / `MaxComputeWritePlanProvider` 的 "gate-closed / dormant until cutover" 已过时；写路径早已 live） | 陈旧注释 | ⏳（doc-only，随手可做，可并进伞形 WS-DOC） |
| PERF-MC03 | 跨查询 ODPS `Table` 缓存（iceberg `IcebergTableCache` 类比，24h/REFRESH）省每查询 `planScan` reload 的跨查询维度 | MC-2 跨查询维度 | ⏳（可选、低优先；有陈旧/TTL 代价、收益温和；fe-core `SchemaCacheValue` 已覆盖跨查询主需求） |
| — | 连接器侧 table/handle 重缓存、format/comment/manifest 缓存 | — | 🚫 不适用（伞形审计 §6：ODPS Storage API 无格式推断、comment 默认空、无 manifest 模型、split 枚举已干净） |
