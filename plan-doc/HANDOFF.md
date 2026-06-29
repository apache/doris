# 🤝 Session Handoff

> 滚动文档：每次 session 结束**覆盖式更新**（不累积历史；历史见 `git log plan-doc/HANDOFF.md`）。协作规范：[AGENT-PLAYBOOK.md](./AGENT-PLAYBOOK.md)
> **范围** = catalog-spi **主线** handoff。metastore/storage 抽取子线**已 CLOSED**（2026-06-22 合入 #64446/#64653/#64655）——`metastore-storage-refactor/` 仅历史留存、勿读；metastore-spi 现状直接读代码 `fe/fe-connector/fe-connector-metastore-spi/`。

---

# 🎯 下一个 session 的任务 = **逐步处理 clean-room 对抗 review 发现（翻闸 BLOCKED，先修后翻）**

> **✅ 本 session 已完成（2026-06-29）**：**H-5 + H-6 合并修完成 `d6758ff71f5`**——翻闸后 iceberg 连接器自有缓存的可达性。两层缓存：引擎层（按 LOCAL 名键，route resolver 路由）+ 连接器层（按 REMOTE 名键，`IcebergConnector` 自有 `latestSnapshotCache` TTL 24h + `manifestCache`，未注册进引擎注册表→route resolver 摸不到）。**H-5**：`REFRESH CATALOG` 走 `onRefreshCache`（不经 `resetToUninitialized`，**不重建连接器**）只失效引擎层→连接器 24h 快照缓存原封不动。修=`PluginDrivenExternalCatalog` 覆写 `onRefreshCache`，invalidCache 时调 `connector.invalidateAll()`（通用 SPI，paimon 同受益；读字段+null 守护）+ `IcebergConnector.invalidateAll` 扩清 manifest（目录级平价，REFRESH TABLE 仍保留 manifest）+ `IcebergManifestCache.invalidateAll()` + 修错误注释。**H-6**：执行过程的 master 靠连接器侧通知（只带 REMOTE 名、够不到连接器层）→ 读旧快照 24h、与 follower 分裂。修（**用户裁=引擎统一接管**）=`ConnectorExecuteAction` 过程成功后调 `RefreshManager.refreshTableInternal`（follower/REFRESH TABLE 同一标准路径，从 `ExternalTable` 出发清两层、两名都对）+ 删 `IcebergProcedureOps` REMOTE-名旧通知（唯一生产调用方）+ 其单测重定义。**实证纠正 review/HANDOFF 设想 4 处**（REFRESH CATALOG 不重建连接器 / 连接器拿不到 LOCAL 名故「传 LOCAL 名」不可行 / manifest 注释错 / 通知唯一调用方）。clean-room（4 lens + adversarial verify + critic）= 生产代码确认正确，唯一 must-fix=测试覆盖缺口（super.onRefreshCache 删除变异未杀）已补。验证：fe-core 21 / iceberg 连接器全量 811 全绿 / checkstyle 0 / **mutation 8-8 KILLED** / 铁律干净。**e2e flip-gated 未跑**。**未 push**（沿用铁律）。前序：B-1+H-1 `203cda3e31a`、B-2 `d7482a39ab9`+`b09d364888b`+`ba80cfb0439`。设计 `designs/P6.6-FIX-H5-H6-cache-reachability-design.md`。
> **✅ DONE = 处理顺序第 3 项 `H-5 + H-6`（合并修，缓存一致性）`d6758ff71f5`**。
>
> **⏭ 下一步（新 session 从这里起）= 处理顺序第 4 项：其余 high（`H-2`/`H-3`/`H-4`/`H-7`/`H-8`/`H-9`/`H-10`，各自独立可并行认领）**：
> - **H-2**（REST 3 级 namespace `external_catalog.name` scan/write/procedure 静默丢）：三个 provider getter 改用携 externalCatalogName 的 5-arg ops ctor。
> - **H-3**（Kerberized HDFS hadoop catalog 丢 Kerberos 上下文）：各 `Iceberg*MetaStoreProperties` 覆写 `initExecutionAuthenticator` 镜像 paimon。
> - **H-4**（fetchRowCount 恒 -1→CBO 退化）：`IcebergConnectorMetadata` 覆写 `getTableStatistics` 从 currentSnapshot summary 算行数（镜像 paimon）。
> - **H-7**（`FOR VERSION AS OF '<branch>'` 破坏）：非数字 VERSION branch+tag 兼试。
> - **H-8**（翻闸后视图无 schema）：`initSchema` 加 isView 分支经 `getViewDefinition` 建视图列 schema。
> - **H-9**（rewrite_data_files WHERE 跨列 OR/NOT 误报错）：rewrite WHERE 改用 scan-mode 矩阵（见记忆 `r7-where-lowering-unbound-failloud`）。
> - **H-10**（嵌套列裁剪静默关闭）：新增 nested-prune `ConnectorCapability`，iceberg 声明之（**ENG-1 能力孪生审计已实证失败样本**）。
> - **起步**：先 `/step-by-step-fix` → 每条 recon（grep + `git show master:` 实证，**HANDOFF/review 行号/不变式可能过时，信控制流不信注释**）→ 设计文档 `designs/P6.6-FIX-<ID>-<slug>-design.md` → impl → test+mutation → clean-room review → 独立 commit → 回填任务清单 ☑。
> - 处理顺序（任务清单 §8）：B-1+H-1 ✅ → B-2 ✅ → H-5+H-6 ✅ → **其余 H ⏭** → ENG-1 能力孪生审计 → P2(M-*) → P3(L-BATCH) → ENG-3 flip-gated e2e 全跑 → 用户二签翻闸。

> **⚠️ 状态翻转（2026-06-28）**：上一版 HANDOFF 说"翻闸代码基本完成，仅差 docker 验证 + 二签"。**这个结论已被一轮 clean-room 对抗 review 推翻**——review 发现 **2 blocker + 11 high + 11 medium + 25 low + 18 info**，其中 blocker/high 密集覆盖写入、MTMV、统计、time-travel、缓存一致性等核心路径。**翻闸代码侧确实写完了，但不正确——必须先关 P0+关键 P1 + 跑 flip-gated e2e，才能二签翻闸。**

> **📋 任务跟踪入口（下个 session 必先读）**：
> 1. **`plan-doc/tasks/P6.6-iceberg-flip-blockers-tasklist.md`** ← **master checkbox 任务清单**，逐条 ID 对齐 review 报告（B-1/B-2/H-1..H-10/M-1..M-11/L-BATCH/ENG-1..4）。**每条任务的状态、位置、修法、验收、依赖、⚠️RECONCILE 标记都在这里。逐步处理 = 按此表逐条 ☐→◐→☑。**
> 2. **`plan-doc/reviews/P6.6-iceberg-cleanroom-adversarial-review-2026-06-28.md`** ← 完整证据源（每条发现的 file:line、vs master 差异、真回归 vs 内生缺陷、验证者保留意见）。
> 3. memory `iceberg-cleanroom-adversarial-review-2026-06-28`（结论速览 + 与历史结论的冲突清单）。

---

# 🔑 翻闸现状 = **代码侧写完但 review 判定不正确；翻闸 BLOCKED**

- **路由翻闸已在分支**（`18e1b297d7e`）：`SPI_READY_TYPES` 含 `"iceberg"`，建/重放 iceberg catalog 走 `PluginDrivenExternalCatalog`；连接器 ServiceLoader 注册 + plugin-zip 打包齐备。**⚠️ 这意味着 review 所有"this path is live"成立，in-code 的 "dormant / not yet in SPI_READY_TYPES" 注释普遍已过时（false claims）——动码时勿信注释，信控制流。**
- **GSON 兼容迁移已在分支**（`e68eb5c00c9`）：旧 8 catalog 变体 + db + table 标签 `registerCompatibleSubtype`→PluginDriven（table→Mvcc 变体）+ 删 CatalogFactory legacy case。保升级老集群（全新/docker 零影响）。**review §六确认完整且写安全（正面）。**
- **未 push、未二签**：路由翻闸 + GSON 迁移**必须一起 push**（[DEC-FLIP-1] 铁律），但**当前不应 push**——先修 review 发现。

## ⛔ 翻闸 gate（全绿才能二签翻闸最后原子提交）
1. **P0 全清**：B-1（云存储写 fs.s3a.* vs AWS_*）+ B-2（MTMV listPartitions 缺）。
2. **关键 P1 关**：至少 H-1/H-2/H-3/H-5+H-6/H-8（破坏主力部署的回归）；H-4/H-7/H-9/H-10 强烈建议。
3. **ENG-1**：legacy iceberg instanceof 臂的能力孪生全量审计（H-10 是已实证漏写样本）。
4. **ENG-3**：flip-gated e2e 全套实跑（DV/V3/MTMV/time-travel branch/vended 写/Kerberized HDFS/rewrite）。
5. **用户二签**。

---

# ⚖️ 关键决策（沿用，用户已签）

## [DEC-FLIP-1] 持久化 GSON 迁移 = 方向 A（已落地 `e68eb5c00c9`）
> **⚠️ 推送顺序铁律不变**：路由翻闸（`18e1b297d7e`）与 GSON 迁移（`e68eb5c00c9`）**必须一起 push/上线**。单 push 路由翻闸而漏 GSON 迁移到会被升级的老集群 → 老 iceberg 镜像反序列化崩。**但当前两者都不应 push——先修 review 发现，翻闸做成最后一个原子提交（路由+GSON 已在前序 commit，最后补齐 fix + e2e + 二签）。**

## [视图范围] = parity only（B0/B1/B2/B3 全 DONE）
查询 B1 / DROP+删库级联 B2 / SHOW CREATE B3 / 中立地基 B0 全完。CREATE/RENAME VIEW 出范围（fail-loud）。
> ⚠️ review 发现视图面仍有缺口：**H-8（翻闸后视图无 schema，high）** + L-17/L-18/L-19/L-20（文案/缓存，low）。B0–B3 是写出来了，但 H-8 是翻闸后才暴露的 schema-init 回归——见任务清单 H-8。

## [REVIEW 纪律] clean-room，不注入先验（本轮已执行）
本轮 review 刻意不注入开发先验（忽略 plan-doc/注释/commit message）。**后果：部分发现与历史记忆冲突**（最突出=M-10 SHOW PARTITIONS：本轮判真回归 vs 记忆 `iceberg-bclass-autoanalyze-topn-done` 判"误报死码翻闸反改善"）。**认领冲突项时回代码 + `git show master:` 重裁，不盲信任一方（Rule 7）。**

---

# ⚠️⚠️ 用户铁律：**fe-core 不得新增 `if(iceberg)` / `instanceof Iceberg*` / `import IcebergUtils` / 引擎名字符串判别（新 seam）**
iceberg 逻辑落 `fe-connector` 经中立 SPI / ConnectorCapability。**legacy 豁免类**保留 iceberg 引用合法（C4 dead 子树 + commit-bridge 旧清单 + `PhysicalIcebergTableSink`/`bindIcebergTableSink` + `StatementContext` 旧 iceberg-typed stash + `IcebergExternalCatalog` + `ShowCreateDatabaseCommand`/`Env.getDdlStmt` legacy iceberg 臂 + `BindRelation case ICEBERG_EXTERNAL_TABLE` + `ShowCreateTableCommand` legacy ICEBERG 视图臂 + `InsertUtils` 既有 `UnboundIcebergTableSink` 分支）。
> **修 review 发现时尤其注意**：H-7/H-10 等要新增能力门控（`ConnectorCapability`）而非 instanceof；H-5/H-6 的 route resolver PluginDriven 臂走能力/插件检查而非引擎名。

---

# 🟡 已登记 follow-up（部分已并入任务清单）
- **[FU-forcedrop-nosuchns]** = 任务清单 **M-11**（pre-existing，HEAD 表级联早有缺口，非翻闸引入）。
- **[FU-show-partitions-deadcode]** 与任务清单 **B-2/M-10** 相关（⚠️RECONCILE）。
- **[FU-flip-e2e]** = 任务清单 **ENG-3**（真翻闸端到端未跑）。
- **[FU-rewrite-output-sizing]（R6/R8）** 中立 driver 未线程 target-file-size + 自适应并行度（与 H-9 同文件族，可一并）。
- **[FU-view-gson-roundtrip] / [FU-view-exception-arms] / [FU-getsqldialect-deadcode] / [FU-showcreatedb-render-ut] / [FU-createtablelike-plugin]**（低）见 git log 历史 + 任务清单 L-BATCH。
- 其余（nested-nullability / where-literal-coercion / broker-write〔=M-5〕/ doris-version-prop〔=L-13〕等）多已被 review 重新发现并归入任务清单。

---

# ⚙️ 操作须知（复用）

- maven：`-f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl :<art> **-am** -DfailIfNoTests=false -Dmaven.build.cache.enabled=false`（漏 `-am`→假错 `${revision}`）。**fe-core 只依赖 `fe-connector-api`** → `:fe-core -am` 不拖 paimon。**fe-connector-paimon 单独 build 必须 `package`**（HiveConf 来自 optional shade，`test-compile` 假错）。**iceberg/api** 正常 `-am test`。
- **⚠️ checkstyle 别加 `-am`**：`-am` 把 `fe-common`（2381 既存 error）拖进假红 → `mvn -pl :<art> checkstyle:check`（不带 -am）。
- **⚠️ bash 工具默认 timeout 120s**：fe-core build 超时 → 调 `timeout` ~590000ms 或后台跑（全模块 ~2min）。
- **⚠️ maven 经管道 `$?` 是管道尾的** → 用 `${PIPESTATUS[0]}` 或 grep `BUILD SUCCESS`；`-q` 抑制 console → 读 surefire **XML** 的 `tests=`/`failures=`。
- **⚠️ stale .class 假红坑**：mutation 后 `os.utime`；**commit 前最终验证务必 fresh recompile**。
- 连接器禁 import fe-core：`bash tools/check-connector-imports.sh`。**连接器测试无 Mockito**（真 InMemoryCatalog/Recording fakes）；**fe-core 用 Mockito**（`CALLS_REAL_METHODS` + `Deencapsulation.setField` + stub `getConnector`/`getMetadata`/`buildConnectorSession`）。**⚠️ Mockito `anyString()` 不匹配 null**。
- **mutation-check（Rule 9/12）**：范式 scratchpad `mutate_*.py`（单行 exact-string 锚点 count==1 守；KILLED=maven rc!=0）。**⚠️ Python 3.6**：`subprocess.run(stdout=PIPE,stderr=STDOUT,universal_newlines=True)`（无 `capture_output`）。**⚠️ review（读源）与 mutation（改源）务必串行**。
- **cwd 会被 harness 重置** → 一律绝对路径。
- **⚠️ 环境**：`/mnt/disk1` 紧（2.0T，96% used）。**下个 session 起步先 `df -h /mnt/disk1`**；**勿用 worktree 隔离编译 agent**（复制整仓，盘不够）。

# ⚠️ Commit 须知（任何 `git add` 前必读）
- **path-whitelist `git add`，严禁 `git add -A`**（scrub `regression-test/conf/regression-conf.groovy` 明文 key + `*.bak` + scratch `.audit-scratch/`·`conf.cmy/`·`META-INF/`·`docker/...`·仓根游离 `fe/IcebergScanPlanProvider.java`·`plan-doc/reviews/P5-paimon-rereview3-*`)。
- commit message：见 `git log` 范式 + 末尾 `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>` + `Claude-Session: …`。PR base = `branch-catalog-spi`，squash。
- **每条 fix = 独立 commit**（沿用 P4-T06e-FIX-* 范式）；HANDOFF + 任务清单 + 设计文档单独 commit（memory 在 `.claude/`、非仓内）。

# 📦 阶段状态
- **工作分支 = `catalog-spi-10-iceberg`**（off `branch-catalog-spi` @ `e5959e1b53d`，PR base = `branch-catalog-spi`，squash）。
- **进度**：P6.1–P6.5 ✅ / P6.6 C1–C3 ✅ / C4 R1–R7 ✅ / C5 DDL/ALTER B1–B5 ✅ / flip-readiness 只读退化 ✅ / 视图 B0–B3 ✅ / 路由翻闸 `18e1b297d7e` ✅ / GSON 迁移 `e68eb5c00c9` ✅ → **⛔ 现卡在 clean-room review 发现修复（见 `P6.6-iceberg-flip-blockers-tasklist.md`）**：**B-1+H-1 ✅ `203cda3e31a`** → **B-2 ✅**（`d7482a39ab9`+`b09d364888b`+`ba80cfb0439`）→ **H-5+H-6 ✅ `d6758ff71f5`**（缓存可达，引擎统一接管）→ **其余 H（H-2/3/4/7/8/9/10）⏭（下一）** → M/L → ENG-1 审计 → ENG-3 e2e → 翻闸二签。
- **⚠️ 推送状态**：P6.4 T01–T06+arg-move 已推 `origin`；**其后全部未 push**（含路由翻闸 + GSON 迁移 + 视图 + C4/C5）。**先修 review 发现，勿 push 半成品翻闸。** 留用户裁量。
- **⚠️ 分支 2026-06-28 被 rebase**：commit 哈希全重写，本文档/旧 commit message 旧哈希以 `git log` 为准。rebase 仅引入 1 问题（`MergeIntoCommand` 未用 import）已修 `33b920bf877`。

# 🧠 给下一个 agent 的 meta
- **逐步处理 = 按任务清单逐条**：每条 P0/P1/P2 走 step-by-step-fix（recon→design 文档 `designs/P6.6-FIX-<ID>-<slug>-design.md`→impl→test+mutation→clean-room review→独立 commit→回填任务清单状态）。处理顺序建议见任务清单 §8（B-1+H-1 → B-2 → H-5+H-6 → 其余 H → ENG-1 → P2 → P3 → e2e）。
- **删除/parity/动码前必 grep 全调用方 + 区分 DEAD vs STILL-CONSUMED**；**HANDOFF/review/设计的依赖名/行号/不变式可能过时** —— 动码前先 recon（grep+实证）再信文档。**翻闸已生效 → in-code "dormant" 注释普遍过时，信控制流不信注释。**
- **⚠️ 冲突优先暴露（Rule 7）**：review 与历史记忆冲突项（M-10 等）回代码重裁，不盲信任一方。`git show master:` 是 legacy 原逻辑的权威来源（工作区 `datasource/iceberg/**` 是迁移后残壳，不可信）。
- **clean-room 对抗 review 偏好**：moderate+ 改动 = 多 reader 对抗 + critic（review 读源与 mutation 改源不可并发）。verbatim 镜像臂则焦点验证即可。
- **flip-gated 诚实**：真 post-flip 写/MTMV/time-travel e2e 翻闸后才能跑——**每条 fix 验收的 e2e 项标注 flip-gated 未跑，勿谎称已验**（Rule 12）。
- **上下文超 30% 即交接**。本 session = 跑完 clean-room review + 建任务清单 + 更新 HANDOFF；在干净节点交接「逐步修 review 发现」。

## 📖 起步必读
1. **`plan-doc/tasks/P6.6-iceberg-flip-blockers-tasklist.md`**（master 任务清单）+ **`plan-doc/reviews/P6.6-iceberg-cleanroom-adversarial-review-2026-06-28.md`**（证据源）。
2. memory：`iceberg-flip-blocker-fixes-progress`（**逐条修进度主索引** + M-10 reconcile 裁定）、`iceberg-cleanroom-adversarial-review-2026-06-28`（本轮结论 + 冲突）、`iceberg-b2-3of3-fecore-recon`（刚完成的 B-2 fe-core 层实证坑）、`iceberg-flip-readiness-gaps`、`handoff-discipline-per-phase`、`consult-trino-before-spi-design`、`clean-room-adversarial-review-pref`、`ask-user-explain-in-chinese-first`、`doris-build-verify-gotchas`、各 `iceberg-*-done`（已完成各面的实证坑）。
3. `plan-doc/tasks/designs/P6.6-C5-flip-readiness.md`（C 类 docker 清单 + 翻闸开关/持久化全景）。
