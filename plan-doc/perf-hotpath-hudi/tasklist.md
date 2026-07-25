# Task List — perf-hotpath-hudi

> ID 一旦分配永不复用。状态：⏳待启动 · 🚧进行中 · ✅完成 · 🔬复核中 · 🅿待拍板 · 🚫不立项
> 权威分析：伞形 `plan-doc/connector-cache-unification/connectors/hudi.md` + 本空间 `designs/round-1-memo-hms-cache-design.md`。

## Round 1（全部完成 2026-07-24：H01 文档 / H02 HMS 缓存 / H03 旗舰 memo）

| ID | 主题 | 覆盖发现 | 依赖 | 状态 |
|---|---|---|---|---|
| **PERF-H01** | 陈旧 "dormant hms" 注释清理（纯 doc） | 陈旧注释 | — | ✅ commit `1cb0f95f8ed` |
| **PERF-H02** | HMS 缓存层：wrap `CachingHmsClient` + fresh/cached 拆分 + REFRESH flush | HD-P04（+新鲜度修正） | — | ✅ commit `e26ab33b001`（183 测试绿） |
| **PERF-H03** | 旗舰：每语句 memo（最新 schema + 最新 instant） | HD-P01 + HD-P02 | — | ✅ commit `26690775c81`（HudiStatementMemoTest 4 测试绿，全模块 0 失败） |

> 三项独立，实施序 A(H01)→C(H02)→B(H03)（风险从低到高）。全程 0 fe-core、无 pom 改动。
> **PERF-H03 最终实现（owner 拍板"两块独立 memo"）**：重侦察推翻蓝图——`getScanNodeProperties` 在 `HudiScanPlanProvider`（异构 build/线程/鉴权）→**本轮不碰扫描规划器**；`HudiSchemaAtInstantTest` 无须改（两块独立 memo 下最新路径仍经 `getSchemaFromMetaClient`）。落地=`ConnectorStatementScopes` 加 `HUDI_LATEST_SCHEMA`/`HUDI_LATEST_INSTANT` 两命名空间、`HudiStatementScope` 两 helper，`getTableSchema`(最新)/`beginQuerySnapshot` 各走一块 memo（loader=现成方法，字节等价、零耦合）。合并版（变体二）经 4-agent 净室复审有两 minor 固有边角，已按 owner 拍板退回两块独立 memo。

## 延后（记录，勿抢跑 — 设计 §4）

| ID | 主题 | 覆盖发现 | 状态 |
|---|---|---|---|
| PERF-H04 | 跨查询 metaClient + `(table,instant)` 分区缓存 | HD-P03 | ⏳（下一轮，与更宽失效设计一起） |
| PERF-H05 | at-instant / FOR TIME AS OF 的 memo | HD-P02 子集 | ⏳ |
| PERF-H06 | 跨方法 buildHadoopConf 去重 + 逐文件 normalize | HD-P05 余项 | ⏳（纯 CPU，低优先） |
| PERF-H07 | @incr `(begin,end]` 行级过滤功能缺口 | STALE-6 | ⏳（功能 bug，单独立项） |
| PERF-H08 | 异构 hudi+iceberg+hive 单-hms-catalog e2e | — | ⏳（需集群，本轮只给规格） |
