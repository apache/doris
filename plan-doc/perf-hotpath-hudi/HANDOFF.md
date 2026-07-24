# 🤝 HANDOFF — perf-hotpath-hudi（连接器缓存统一的旗舰连接器）

> 覆盖式更新，只留下一个 session 必需的上下文。完成明细进 git log + `progress.md`。
> 伞形空间 = `plan-doc/connector-cache-unification/`（本兄弟空间进度回填其 tasklist WS-HUDI 行）。

## 开场读序
1. Read 本 HANDOFF
2. Read [`designs/round-1-memo-hms-cache-design.md`](./designs/round-1-memo-hms-cache-design.md) ← **本轮完整设计蓝图**（3 块 + 红队 3 修正 + 验证）
3. Read `tasklist.md` ← 勾到哪、下一个 PERF 是谁
4. 需要发现细节 → 伞形空间 `connectors/hudi.md` / `data/connector-audits.json` hudi 节

## 当前状态（2026-07-24，Piece A + C 已完成+提交+验证；Piece B 已细化到可编码，未动）
- 调研（13-agent workflow `wf_3a0b9aca-966`）+ 设计 + 红队 + owner 范围拍板**全部完成**。
- **✅ Piece A（文档清理）已提交** commit `1cb0f95f8ed`（4 处 stale "dormant hms" 注释改词，纯 doc）。
- **✅ Piece C（HMS 缓存层）已提交** commit `e26ab33b001`（wrap `CachingHmsClient` + `collectPartitions` fresh/cached 拆分 + `HudiConnector.invalidate*` flush；无 pom 改动；`HudiConnectorHmsCacheTest` 8 测试；hudi 全模块 **183 测试 0 失败**）。
- **⏳ Piece B（旗舰 memo）未动** —— 因它触及连接器最易 SIGABRT 的 schema/field-id/演进派生 + 需 session 穿线 + 测试改造，在长 session 尾部收尾风险高，**已 checkpoint 交下一轮**（owner 决定继续或新 session）。

## ⚠ 下一步 = 实施 Piece B（旗舰）——**完整可编码蓝图见** [`designs/round-1-pieceB-flagship-impl-notes.md`](./designs/round-1-pieceB-flagship-impl-notes.md)
精要（细节见蓝图，含已侦察的测试交互坑）：
- **Scope B：planScan 一行不碰**（它必建自己 metaClient 做 fsView，读不读 memo 只省 schema 读不省 build，且是刚稳定的读热路径 + 红队 focus）→ 风险最低、build 收敛同样达成（~5→~3）。
- 新增（0 fe-core）：`ConnectorStatementScopes.HUDI_METACLIENT` 常量 + `HudiStatementScope.sharedTableFacts` + 不可变投影 `HudiTableFacts`{instant, latestColumns, resolvedInternalSchema, historicalSchemas} + loader（一次 metaClient 导出全部 fact）。
- 改挂 3 个元数据侧消费点（全有 session）：`getTableSchema(2-arg latest)` / `beginQuerySnapshot` / `getScanNodeProperties(plain)` 读 memo；`getSchemaFromMetaClient`/`latestInstant` 方法保留（3-arg at-instant / collectPartitions 仍用）。`buildSchemaEvolutionProp` 拆重载。
- **⚠ 测试坑（蓝图已列）**：`HudiSchemaAtInstantTest` 的 **2-arg control 会挂须改**（改挂后 2-arg 不再经 getSchemaFromMetaClient）；`HudiColumnFieldIdTest`/`HudiSchemaParityTest` 大概率纯函数不受影响（动码前 grep 确认）。补 build-count 守门 + parity + NONE 对照测试。

**实施铁律**（红队 load-bearing，违反=parity 回归）：①不 memo Configuration ②planScan 一律不碰 ③latest-only、at-instant 保持独立 build。

## 验证（每块后）
- `mvn install -pl :fe-connector-api,:fe-connector-hudi,:fe-connector-hive -am -Dmaven.build.cache.enabled=false`（install 非 test）→ 抓 `BUILD SUCCESS`+`Tests run:`。
- 旗舰补 build-count 守门单测（真 scope 下 metadata 侧 build=1 + 字节 parity + NONE-scope 对照）。
- HMS 补 wrap/fresh/cached/flush 单测。
- e2e 本地跑不了（p2/external+集群），异构三向新用例只给规格。

## 稳定区（勿覆盖）
- **build 坑**：绝对 `-f <abs>/fe/pom.xml`；`-am`；**install 非 test**（hudi 经 hms 依赖 shade jar）；`-Dmaven.build.cache.enabled=false`；别 `-q`；别 `nohup &` 套后台，读日志 `BUILD SUCCESS` 行。
- **无 pom 改动**（fe-connector-hms/api 已是直接依赖；Caffeine 3.2.3 已随 hudi-common 打包，**勿加 2.9.3**——nearest-wins 会降级）。
- **fe-core 0 行**（`HUDI_METACLIENT` 在 fe-connector-api 非 fe-core）；遇"不得不碰 fe-core"停手交 review。
- 提交只精确 `git add` 本任务文件（工作树大量无关 untracked，禁 `git add -A`）；`.git` 是文件（linked worktree）。
- 并发探测：`find fe -newermt '-120 sec'`（滤 target）+ `pgrep -a -f maven`。
