# 🤝 HANDOFF — perf-hotpath-hudi（连接器缓存统一的旗舰连接器）

> 覆盖式更新，只留下一个 session 必需的上下文。完成明细进 git log + `progress.md`。
> 伞形空间 = `plan-doc/connector-cache-unification/`（本兄弟空间进度回填其 tasklist WS-HUDI 行）。

## 开场读序
1. Read 本 HANDOFF
2. Read [`designs/round-1-memo-hms-cache-design.md`](./designs/round-1-memo-hms-cache-design.md) ← **本轮完整设计蓝图**（3 块 + 红队 3 修正 + 验证）
3. Read `tasklist.md` ← 勾到哪、下一个 PERF 是谁
4. 需要发现细节 → 伞形空间 `connectors/hudi.md` / `data/connector-audits.json` hudi 节

## 当前状态（2026-07-24，Round 1 全部完成+提交+验证）
- 调研 + 设计 + 红队 + owner 范围拍板全部完成；**Round 1 三块全部落地**。
- **✅ Piece A（文档清理，PERF-H01）** commit `1cb0f95f8ed`（4 处 stale "dormant hms" 注释改词，纯 doc）。
- **✅ Piece C（HMS 缓存层，PERF-H02）** commit `e26ab33b001`（wrap `CachingHmsClient` + `collectPartitions` fresh/cached 拆分 + `HudiConnector.invalidate*` flush；无 pom 改动；`HudiConnectorHmsCacheTest` 8 测试；183 测试 0 失败）。
- **✅ Piece B（旗舰 memo，PERF-H03）** commit `26690775c81`——**两块独立 per-statement memo**（最新 schema + 最新 instant），各 loader = 现成方法（`getSchemaFromMetaClient(path,null)` / `latestInstant`），逐字节等价、零耦合。0 fe-core、无 pom、扫描规划器/at-instant 未碰；`HudiStatementMemoTest` 4 测试（去重×2 + 两 memo 独立 + NONE/null）；全模块 0 失败。详见 [`progress.md`](./progress.md) "2026-07-24 (3)"。

## ⚠ 重要教训（动 hudi 前必读，勿再照抄蓝图）
- 蓝图 [`designs/round-1-pieceB-flagship-impl-notes.md`](./designs/round-1-pieceB-flagship-impl-notes.md) 已被侦察**推翻多处**：`getScanNodeProperties` 在 `HudiScanPlanProvider`（异构 build 源 / 扫描线程 / 无鉴权包装，本轮不碰）；`HudiSchemaAtInstantTest` 不用改。
- 蓝图的"变体二（schema+instant 合并一次 metaClient 构建）"虽实现过并全绿，但 4-agent 净室复审发现**两个 minor 固有耦合边角**（取 schema 多依赖 instant 可解析；取快照顺带读 schema），**owner 拍板退回两块独立 memo**（零耦合，见 progress "2026-07-24 (3)"）。这是"执行蓝图常高估 / 错位、动码前必按 HEAD 重侦察"的又一实证。

## 下一步（Round 1 完成，选一）
- **转 mc/es 两个小连接器**（伞形 `WS-MC` / `WS-ES`），各独立小 PR；或
- **hudi Round 2 延后项**：`PERF-H04`（跨查询 metaClient + `(table,instant)` 分区缓存，配更宽失效设计）、`PERF-H05/06/07`（见 [`tasklist.md`](./tasklist.md)「延后」）。
- **e2e（PERF-H08，需集群，本轮只给规格）**：异构 hudi+iceberg+hive 单-hms-catalog + 独立 hudi-on-HMS，跑分区 / 时间旅行 / schema 演进 parity。

## 验证（每块后）
- `mvn install -pl :fe-connector-api,:fe-connector-hudi -am -Dmaven.build.cache.enabled=false`（install 非 test）→ 抓 `BUILD SUCCESS`+`Tests run:`。
- 旗舰守门：`HudiStatementMemoTest`（真 scope 下 loader=1 去重 + 两 memo 独立 + NONE/null 每次加载）。
- e2e 本地跑不了（external+集群），异构三向新用例只给规格。

## 稳定区（勿覆盖）
- **build 坑**：绝对 `-f <abs>/fe/pom.xml`；`-am`；**install 非 test**（hudi 经 hms 依赖 shade jar）；`-Dmaven.build.cache.enabled=false`；别 `-q`；别 `nohup &` 套后台，读日志 `BUILD SUCCESS` 行。
- **无 pom 改动**（fe-connector-hms/api 已是直接依赖；Caffeine 3.2.3 已随 hudi-common 打包，**勿加 2.9.3**——nearest-wins 会降级）。
- **fe-core 0 行**（`HUDI_METACLIENT` 在 fe-connector-api 非 fe-core）；遇"不得不碰 fe-core"停手交 review。
- 提交只精确 `git add` 本任务文件（工作树大量无关 untracked，禁 `git add -A`）；`.git` 是文件（linked worktree）。
- 并发探测：`find fe -newermt '-120 sec'`（滤 target）+ `pgrep -a -f maven`。
