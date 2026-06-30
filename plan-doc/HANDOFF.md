# 🤝 Session Handoff

> 滚动文档：每次 session 结束**覆盖式更新**（不累积历史；历史见 `git log plan-doc/HANDOFF.md`）。协作规范：[AGENT-PLAYBOOK.md](./AGENT-PLAYBOOK.md)
> **范围** = catalog-spi **主线** handoff。metastore/storage 抽取子线**已 CLOSED**（2026-06-22 合入 #64446/#64653/#64655）——`metastore-storage-refactor/` 仅历史留存、勿读；metastore-spi 现状直接读代码 `fe/fe-connector/fe-connector-metastore-spi/`。

---

# 🎯 下一个 session 的任务 = **逐步处理 clean-room 对抗 review 的 Medium (M-*) 发现（翻闸 BLOCKED，先修后翻）**

> **进度**：P0（B-1/B-2）+ 全部关键 P1（H-1..H-10）**已全 ✅**；Medium **M-1 ✅ `ead0ac39328`** + **M-2 ✅ `a942d0d4a87`** + **M-3 ✅ `a75f5ffed99`**（file-count 流式 split，中立 `ConnectorSplitSource` 连接器自驱动）+ **M-4 ✅ `4427d805f65`**（Top-N 懒物化恢复全列 field-id 字典，中立 `applyTopnLazyMaterialization` 挂 handle，向 Trino 对齐）已落——逐条状态/commit 见**任务清单 §1–§3** + `git log`（HANDOFF 不再累积「修完成」条目）。
>
> **⏭ 下一步（新 session 从这里起）= 逐步处理 review 的 Medium (M-*) 发现**：
> - **入口**：任务清单 **§3（`M-1..M-11`，每条 ID/状态/位置/修法/备注/⚠️RECONCILE 在表内）** + review 报告 **§四**（证据源 file:line + vs master 差异）。**M-1/M-2/M-3/M-4 ☑**、**M-10 + H-11 ☑ 已并入 B-2 `ba80cfb0439`**；其余 **`M-5..M-9` / `M-11` 待办**（M-7/M-11 标 partial）。
> - **概览（详见任务清单 §3，勿在此累积；M-1/M-2/M-3/M-4 ✅ 已落，不再列）**：**M-5 ◀ 下一** 写 sink 对 FILE_BROKER(ofs/gfs) 不设 broker_addresses〔broker 后端写全失败；S3/HDFS/local 不受影响；经写 SPI 串入 catalog 绑定 broker 地址〕 / M-6 嵌套复杂 MODIFY 到 iceberg 不可表示窄类型报错文案变（破绿 e2e） / M-7 DLF flavor 丢 CREATE TABLE NotSupported 守护 / M-8 SHOW CREATE DATABASE 无 location namespace 丢 LOCATION 子句 / M-9 DROP DATABASE on name-mapped catalog 用 LOCAL 名而非 REMOTE 名 / M-11 DROP DATABASE FORCE 不再容忍远端已删 namespace。
> - **每条走 step-by-step-fix**（recon→design `designs/P6.6-FIX-M<n>-<slug>-design.md`→impl→test+mutation→clean-room→**独立 commit**→回填任务清单），逐条独立提交。**⚠️ 认领前先 recon+`git show master:` 重裁，HANDOFF/review 行号/不变式可能过时（信控制流不信注释）**；冲突项回代码重裁（Rule 7）。
> - **处理顺序（用户已重排 = Medium 先于 ENG-1）**：**Medium `M-*` ◀ 下一（M-5）** → **ENG-1 能力孪生审计** → P3(L-BATCH) → ENG-3 flip-gated e2e 全跑 → 用户二签翻闸。（⚠️ 任务清单 §8 仍把 ENG-1 列在 P2 之前 = 已过时，以本处用户重排为准。）
> - **⚠️ M-3 引入新中立 SPI（`ConnectorSplitSource` + `streamingSplitEstimate`/`streamSplits`）= 流式 split 通道**：将来 Hive/Hudi 迁插件路径可复用（file-count 流式是它们共用老套路）。**v3 iceberg 暂闸出流式**（commit-bridge delete stash 写规划点读，流式懒填太晚→复活已删行）；放开 v3 需先设计 plan-time stash barrier（登记 follow-up）。

> **⚠️ 为何 BLOCKED（2026-06-28）**：一轮 clean-room 对抗 review 推翻了「翻闸代码基本完成」的旧结论——发现 **2 blocker + 11 high + 11 medium + 25 low + 18 info**，blocker/high 密集覆盖写入、MTMV、统计、time-travel、缓存一致性等核心路径。**翻闸代码侧写完了但不正确**：P0+关键 P1 现已逐条修完，但仍需关 Medium、跑 ENG-1 审计与 flip-gated e2e 才能二签翻闸。

> **📋 任务跟踪入口（下个 session 必先读）**：
> 1. **`plan-doc/tasks/P6.6-iceberg-flip-blockers-tasklist.md`** ← **master checkbox 任务清单**，逐条 ID 对齐 review 报告（B-1/B-2/H-1..H-11/M-1..M-11/L-BATCH/ENG-1..4）。**每条任务的状态、位置、修法、验收、依赖、⚠️RECONCILE 标记都在这里。逐步处理 = 按此表逐条 ☐→◐→☑。**
> 2. **`plan-doc/reviews/P6.6-iceberg-cleanroom-adversarial-review-2026-06-28.md`** ← 完整证据源（每条发现的 file:line、vs master 差异、真回归 vs 内生缺陷、验证者保留意见；**Medium 见 §四**）。

---

# 🔑 翻闸现状 = **代码侧写完、P0+关键 P1 已修；翻闸 BLOCKED（待 Medium + ENG-1 + e2e + 二签）**

- **路由翻闸已在分支**（`18e1b297d7e`）：`SPI_READY_TYPES` 含 `"iceberg"`，建/重放 iceberg catalog 走 `PluginDrivenExternalCatalog`；连接器 ServiceLoader 注册 + plugin-zip 打包齐备。**⚠️ 这意味着 review 所有"this path is live"成立，in-code 的 "dormant / not yet in SPI_READY_TYPES" 注释普遍已过时（false claims）——动码时勿信注释，信控制流。**
- **GSON 兼容迁移已在分支**（`e68eb5c00c9`）：旧 8 catalog 变体 + db + table 标签 `registerCompatibleSubtype`→PluginDriven（table→Mvcc 变体）+ 删 CatalogFactory legacy case。保升级老集群（全新/docker 零影响）。**review §六确认完整且写安全（正面）。**
- **未 push、未二签**：路由翻闸 + GSON 迁移**必须一起 push**（[DEC-FLIP-1] 铁律），但**当前不应 push**——先修完 review 发现。

## ⛔ 翻闸 gate（全绿才能二签翻闸最后原子提交）
1. **P0 全清** ✅：B-1（云存储写 fs.s3a.* vs AWS_*）+ B-2（MTMV listPartitions 缺）。
2. **关键 P1 关** ✅：H-1..H-10（破坏主力部署的回归）逐条修完，详见任务清单 §2。
3. **ENG-1**：legacy iceberg instanceof 臂的能力孪生全量审计（H-10 是已实证漏写样本）。
4. **ENG-3**：flip-gated e2e 全套实跑（DV/V3/MTMV/time-travel branch/vended 写/Kerberized HDFS/rewrite）。
5. **用户二签**。
> Medium `M-*`（P2，「翻闸窗口或紧随其后」）= 用户重排的**下一步工作**（见顶部 ⏭），非严格 flip-gate；逐条见任务清单 §3。

---

# ⚖️ 关键决策（沿用，用户已签）

## [DEC-FLIP-1] 持久化 GSON 迁移 = 方向 A（已落地 `e68eb5c00c9`）
> **⚠️ 推送顺序铁律不变**：路由翻闸（`18e1b297d7e`）与 GSON 迁移（`e68eb5c00c9`）**必须一起 push/上线**。单 push 路由翻闸而漏 GSON 迁移到会被升级的老集群 → 老 iceberg 镜像反序列化崩。**但当前两者都不应 push——先修完 review 发现，翻闸做成最后一个原子提交（路由+GSON 已在前序 commit，最后补齐 fix + e2e + 二签）。**

## [视图范围] = parity only（B0/B1/B2/B3 全 DONE）
查询 B1 / DROP+删库级联 B2 / SHOW CREATE B3 / 中立地基 B0 全完。CREATE/RENAME VIEW 出范围（fail-loud）。翻闸后视图 schema 回归 = H-8（已修，见任务清单 §2）；视图面残留 low（L-17/L-18/L-19/L-20 文案/缓存）随 P3。

## [REVIEW 纪律] clean-room，不注入先验（本轮已执行）
本轮 review 刻意不注入开发先验（忽略 plan-doc/注释/commit message）。**后果：部分发现与历史记忆冲突**（最突出=M-10 SHOW PARTITIONS：本轮判真回归 vs 旧记忆判"误报死码翻闸反改善"，已裁定 M-10 正确并随 B-2 修）。**认领冲突项时回代码 + `git show master:` 重裁，不盲信任一方（Rule 7）。**

---

# ⚠️⚠️ 用户铁律：**fe-core 不得新增 `if(iceberg)` / `instanceof Iceberg*` / `import IcebergUtils` / 引擎名字符串判别（新 seam）**
iceberg 逻辑落 `fe-connector` 经中立 SPI / ConnectorCapability。**legacy 豁免类**保留 iceberg 引用合法（C4 dead 子树 + commit-bridge 旧清单 + `PhysicalIcebergTableSink`/`bindIcebergTableSink` + `StatementContext` 旧 iceberg-typed stash + `IcebergExternalCatalog` + `ShowCreateDatabaseCommand`/`Env.getDdlStmt` legacy iceberg 臂 + `BindRelation case ICEBERG_EXTERNAL_TABLE` + `ShowCreateTableCommand` legacy ICEBERG 视图臂 + `InsertUtils` 既有 `UnboundIcebergTableSink` 分支）。
> **修 Medium 发现时尤其注意**：若需新增门控走 `ConnectorCapability` / 中立 SPI 而非 instanceof / 引擎名（例：M-7 DLF 守护；M-4 字段编号链路可参照已完成的 H-10）。

---

# 🟡 已登记 follow-up（部分已并入任务清单）
- **[FU-forcedrop-nosuchns]** = 任务清单 **M-11**（pre-existing，HEAD 表级联早有缺口，非翻闸引入）。
- **[FU-flip-e2e]** = 任务清单 **ENG-3**（真翻闸端到端未跑）。
- **[FU-rewrite-output-sizing]（R6/R8）** 中立 driver 未线程 target-file-size + 自适应并行度（与已完成 H-9 同文件族）。
- **[FU-paimon-topn-dict]（low，M-4 clean-room 两 reader 独立发现，非 M-4 回归，出范围）** = 迁移后 paimon `PaimonScanPlanProvider.buildSchemaEvolutionParam` 的 `-1` 当前 schema 条目按**裁剪列**建（legacy paimon 恒全列），与 iceberg M-4 同型潜在 Top-N 懒物化缺口；但 paimon 另发**每 committed schema-id 的全列 history 条目**（iceberg 只发单 `-1`），其 topn 安全性（若有）或赖于此 → **需独立验证**（确认 paimon BE 按 row-id 回表补取是否经那些全列 history 条目解析、是否真有 wrong-rows）。若确认有缺口，可复用 M-4 的 `applyTopnLazyMaterialization` SPI（paimon 覆写即可）。
- **[FU-h10-deadcode]（LOW，cosmetic/非正确性）** 两条翻闸后死码，留 ENG-1/cleanup：① `PlanNode.mergeIcebergAccessPathsWithId`（`instanceof IcebergScanNode` EXPLAIN 访问路径合并臂，翻闸后只显示 `name` 非 `name(id)`，BE 仍收编号形路径）；② `LogicalFileScan.supportPruneNestedColumn` 的 legacy `IcebergExternalTable||IcebergSysExternalTable→return true` 臂（与 L2 现已不一致、仅反翻闸成隐患）。
- **[FU-view-gson-roundtrip] / [FU-view-exception-arms] / [FU-getsqldialect-deadcode] / [FU-showcreatedb-render-ut] / [FU-createtablelike-plugin]**（低）见 git log 历史 + 任务清单 L-BATCH。
- 其余（nested-nullability / where-literal-coercion / broker-write〔=M-5〕/ doris-version-prop〔=L-13〕等）多已被 review 重新发现并归入任务清单。

---

# ⚙️ 操作须知（复用）

- maven：`-f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl :<art> **-am** -DfailIfNoTests=false -Dmaven.build.cache.enabled=false`（漏 `-am`→假错 `${revision}`）。**fe-core 只依赖 `fe-connector-api`** → `:fe-core -am` 不拖 paimon。**fe-connector-paimon 单独 build 必须 `package`**（HiveConf 来自 optional shade，`test-compile` 假错）。**iceberg/api** 正常 `-am test`。
- **⚠️ checkstyle 别加 `-am`**：`-am` 把 `fe-common`（2381 既存 error）拖进假红 → `mvn -pl :<art> checkstyle:check`（不带 -am）。
- **⚠️ bash 工具默认 timeout 120s**：fe-core build 超时 → 调 `timeout` ~590000ms 或后台跑（全模块 ~2min）。
- **⚠️ maven 经管道 `$?` 是管道尾的** → 用 `${PIPESTATUS[0]}` 或 grep `BUILD SUCCESS`；`-q` 抑制 console → 读 surefire **XML** 的 `tests=`/`failures=`。
- **⚠️ stale .class 假红坑**：mutation 后 `os.utime`；**commit 前最终验证务必 fresh recompile**。
- **⚠️ fe-connector-iceberg 全模块测试套有预存在 flaky 污染（M-2 期间实证，非任一 fix 引入）**：跑全 849 测时偶发 3 个 field-id/能力测试红（`IcebergConnectorTest.declaresNestedColumnPruneCapability`、`IcebergTypeMappingReadTest.nestedFieldIdsCarriedForBeFieldIdScan`、`IcebergConnectorMetadataTest.getTableSchemaParsesColumnsFromLoadedTable`——field-id 读 -1 / 能力读 false），**取决于 surefire 类执行顺序**（顺序相关共享静态态污染）；三类**单独跑全绿**、stash 改动后**clean tree 同样偶发**——即非确定性、非改动引入。另 `fe-connector-metastore-iceberg` 的 `IcebergMetaStoreProvidersDispatchTest` 亦预存在 flaky（clean tree 也红，且它是 iceberg 的 -am 上游→其红会 skip 整个 iceberg 测试致 XML stale；隔离验证可加 `-Dtest='!IcebergMetaStoreProvidersDispatchTest'`）。**判 iceberg fix 是否破测勿信单次全量红**：① 单独跑相关类；② stash 后对比 clean tree。建议归 ENG（测试隔离修复），非单点 fix 范围。
- **⚠️ 后台 task 通知的 "exit code" 是末尾 echo/df 的、非 maven 的**（M-2 又踩一次）：读 LOG 里 `MAVEN_EXIT=`/`BUILD` 行或 surefire XML，别信通知的 exit 0。
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
- **进度**：P6.1–P6.5 ✅ / P6.6 C1–C3 ✅ / C4 R1–R7 ✅ / C5 DDL/ALTER B1–B5 ✅ / flip-readiness 只读退化 ✅ / 视图 B0–B3 ✅ / 路由翻闸 `18e1b297d7e` ✅ / GSON 迁移 `e68eb5c00c9` ✅ → **⛔ 现卡在 clean-room review 发现修复**：**P0（B-1/B-2）+ 关键 P1（H-1..H-10）全 ✅**（逐条 commit 见任务清单 §1–§2 + `git log`）→ **Medium：M-1 ✅ `ead0ac39328`、M-2 ✅ `a942d0d4a87`、M-3 ✅ `a75f5ffed99`、M-4 ✅ `4427d805f65`；M-5 ◀ 下一** → ENG-1 能力孪生审计 → P3(L-BATCH) → ENG-3 flip-gated e2e → 二签翻闸。
- **⚠️ 推送状态**：P6.4 T01–T06+arg-move 已推 `origin`；**其后全部未 push**（含路由翻闸 + GSON 迁移 + 视图 + C4/C5 + 全部 review fix）。**先修 review 发现，勿 push 半成品翻闸。** 留用户裁量。
- **⚠️ 分支 2026-06-28 被 rebase**：commit 哈希全重写，本文档/旧 commit message 旧哈希以 `git log` 为准。

# 🧠 给下一个 agent 的 meta
- **逐步处理 = 按任务清单逐条**：每条 Medium (M-*) 走 step-by-step-fix（recon→design 文档 `designs/P6.6-FIX-M<n>-<slug>-design.md`→impl→test+mutation→clean-room review→独立 commit→回填任务清单状态）。
- **删除/parity/动码前必 grep 全调用方 + 区分 DEAD vs STILL-CONSUMED**；**HANDOFF/review/设计的依赖名/行号/不变式可能过时** —— 动码前先 recon（grep+实证）再信文档。**翻闸已生效 → in-code "dormant" 注释普遍过时，信控制流不信注释。**
- **⚠️ 冲突优先暴露（Rule 7）**：review 与历史记忆冲突项（M-10 等）回代码重裁，不盲信任一方。`git show master:` 是 legacy 原逻辑的权威来源（工作区 `datasource/iceberg/**` 是迁移后残壳，不可信）。
- **clean-room 对抗 review 偏好**：moderate+ 改动 = 多 reader 对抗 + critic（review 读源与 mutation 改源不可并发）。verbatim 镜像臂则焦点验证即可。
- **flip-gated 诚实**：真 post-flip 写/MTMV/time-travel e2e 翻闸后才能跑——**每条 fix 验收的 e2e 项标注 flip-gated 未跑，勿谎称已验**（Rule 12）。
- **上下文超 30% 即交接**。

## 📖 起步必读
1. **`plan-doc/tasks/P6.6-iceberg-flip-blockers-tasklist.md`**（master 任务清单）+ **`plan-doc/reviews/P6.6-iceberg-cleanroom-adversarial-review-2026-06-28.md`**（证据源，Medium 见 §四）。
2. memory（仅列现存相关项）：`handoff-discipline-per-phase`、`clean-room-adversarial-review-pref`、`ask-user-explain-in-chinese-first`、`session-handoff-at-30pct-context`、`memory-keep-only-general-or-requested`、`doris-build-verify-gotchas`、`catalog-spi-fe-core-test-infra`、`catalog-spi-plugindriven-no-source-specific-code`、`catalog-spi-connector-session-tz-gotcha`、`catalog-spi-be-java-ext-shared-classpath`、`catalog-spi-h9-rewrite-where-rewrite-mode-done`。
3. `plan-doc/tasks/designs/P6.6-C5-flip-readiness.md`（C 类 docker 清单 + 翻闸开关/持久化全景）。
