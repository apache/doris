# 🤝 HANDOFF — perf-hotpath-hudi（连接器缓存统一的旗舰连接器）

> 覆盖式更新，只留下一个 session 必需的上下文。完成明细进 git log + `progress.md`。
> 伞形空间 = `plan-doc/connector-cache-unification/`（本兄弟空间进度回填其 tasklist WS-HUDI 行）。

## 开场读序
1. Read 本 HANDOFF
2. Read [`designs/round-1-memo-hms-cache-design.md`](./designs/round-1-memo-hms-cache-design.md) ← **本轮完整设计蓝图**（3 块 + 红队 3 修正 + 验证）
3. Read `tasklist.md` ← 勾到哪、下一个 PERF 是谁
4. 需要发现细节 → 伞形空间 `connectors/hudi.md` / `data/connector-audits.json` hudi 节

## 当前状态（2026-07-24，设计定稿，owner 已确认范围，**未动产品代码**）
- 调研（13-agent workflow `wf_3a0b9aca-966`：9 侦察 + 综合 + 3 红队）+ 设计 + 红队复核**全部完成**。
- owner 拍板本轮范围 = **旗舰 memo + 文档清理 + HMS 缓存层（按 hive 补齐 fresh/cached 拆分 + REFRESH 失效）**。
- 设计蓝图见 `designs/round-1-memo-hms-cache-design.md`（自测已核实：metaClient 非 AutoCloseable、scope 关 AutoCloseable、CachingHmsClient/fresh/flush API、hive fresh/cached 模板、hudi 三分区入口）。

## ⚠ 下一步 = 实施本轮 3 块（各自独立 commit，动每个文件前按 HEAD 重 grep）
按风险从低到高：
- **Piece A（文档清理，纯 doc 零风险，先做）**：清 stale "dormant until hms enters SPI_READY_TYPES" 注释（设计 §1-A 列了 file + 勿动清单）。commit `[docs](catalog) fe-connector-hudi: drop stale dormant-hms comments`。
- **Piece C（HMS 缓存层）**：wrap `CachingHmsClient` + `collectPartitions` 加 `bypassCache`（SHOW/TVF→fresh、剪枝/MTMV→cached）+ override `HudiConnector.invalidate*` flush。设计 §1-C。commit `[perf](catalog) fe-connector-hudi: cache HMS client with fresh listing + REFRESH flush`。
- **Piece B（旗舰 memo）**：`HUDI_METACLIENT` 常量 + `HudiStatementScope` + 不可变投影 {instant, schema, allHistoricalSchemas}；改挂 getSchema/latestInstant/getScanNodeProperties/planScan(schema-only)。设计 §1-B。commit `[perf](catalog) fe-connector-hudi: per-statement metaClient/schema projection memo`。

**实施铁律**（红队 load-bearing，违反=parity 回归）：①不 memo Configuration ②planScan 只读 memo schema、保留自己 lastInstant ③latest-only、at-instant 延后。详见设计 §3。

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
