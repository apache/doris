# Task List — perf-hotpath-hudi

> ID 一旦分配永不复用。状态：⏳待启动 · 🚧进行中 · ✅完成 · 🔬复核中 · 🅿待拍板 · 🚫不立项
> 权威分析：伞形 `plan-doc/connector-cache-unification/connectors/hudi.md` + 本空间 `designs/round-1-memo-hms-cache-design.md`。

## Round 1（owner 已确认范围，设计定稿，未动码）

| ID | 主题 | 覆盖发现 | 依赖 | 状态 |
|---|---|---|---|---|
| **PERF-H01** | 陈旧 "dormant hms" 注释清理（纯 doc） | 陈旧注释 | — | ✅ commit `1cb0f95f8ed` |
| **PERF-H02** | HMS 缓存层：wrap `CachingHmsClient` + fresh/cached 拆分 + REFRESH flush | HD-P04（+新鲜度修正） | — | ✅ commit `e26ab33b001`（183 测试绿） |
| **PERF-H03** | 旗舰：每语句不可变投影 memo（metaClient/schema） | HD-P01 + HD-P02 | — | ⏳ 蓝图就绪 `designs/round-1-pieceB-flagship-impl-notes.md`，未动码 |

> 三项独立，实施序 A(H01)→C(H02)→B(H03)（风险从低到高）。全程 0 fe-core、无 pom 改动。
> **PERF-H03 蓝图关键**：Scope B（planScan 不碰）；改挂 getTableSchema(2-arg)/beginQuerySnapshot/getScanNodeProperties 读投影 memo；HudiSchemaAtInstantTest 2-arg control 须改。

## 延后（记录，勿抢跑 — 设计 §4）

| ID | 主题 | 覆盖发现 | 状态 |
|---|---|---|---|
| PERF-H04 | 跨查询 metaClient + `(table,instant)` 分区缓存 | HD-P03 | ⏳（下一轮，与更宽失效设计一起） |
| PERF-H05 | at-instant / FOR TIME AS OF 的 memo | HD-P02 子集 | ⏳ |
| PERF-H06 | 跨方法 buildHadoopConf 去重 + 逐文件 normalize | HD-P05 余项 | ⏳（纯 CPU，低优先） |
| PERF-H07 | @incr `(begin,end]` 行级过滤功能缺口 | STALE-6 | ⏳（功能 bug，单独立项） |
| PERF-H08 | 异构 hudi+iceberg+hive 单-hms-catalog e2e | — | ⏳（需集群，本轮只给规格） |
