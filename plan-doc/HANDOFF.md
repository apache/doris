# 🤝 Session Handoff

> 滚动文档：每次 session 结束**直接覆盖**（不保留历史；历史见 `git log plan-doc/HANDOFF.md`）。
> 协作规范：[AGENT-PLAYBOOK.md](./AGENT-PLAYBOOK.md)

---

# 🎯 下一个 session 的任务 — **P2 全清；进入 P3 覆盖缺口核查（去查，非 fix）**

第二轮 clean-room 对抗 review report：[`plan-doc/reviews/P5-paimon-rereview2-2026-06-11.md`](./reviews/P5-paimon-rereview2-2026-06-11.md)。
👉 **任务清单：[`plan-doc/task-list-P5-rereview2-fixes.md`](./task-list-P5-rereview2-fixes.md)** —— #1~#9 **全部完成**。

## ✅ 已完成（P0 BLOCKER + P1 MAJOR + P2 perf-parity 全清）
- **#1 `FIX-URI-NORMALIZE`** `20b19d19dd8` · **#2 `FIX-STATIC-CREDS-BE`** `d23d5df9914` · **#3 `FIX-SCHEMA-EVOLUTION`** `667f779af04` · **#4 `FIX-JDBC-DRIVER-URL`** `2d15b1b7ed7`（P0 全清）
- **#5 `FIX-MAPPING-FLAG-KEYS`** `9dcf6d1a9e5` · **#6 `FIX-KERBEROS-DOAS`** `2b1442fa57a` · **#7 `FIX-FORCE-JNI-SCANNER`** `05132a42768`（P1 全清）
- **#8 `FIX-COUNT-PUSHDOWN`**（M-2）`525be03371c` —— 见下。
- **#9 `FIX-NATIVE-SUBSPLIT`**（M-3）`2f5f467f53d` —— 见下。

### #8 摘要 `FIX-COUNT-PUSHDOWN` — commit `525be03371c`（[D-054]/[DV-032]/[RFC §25 E15]）
- **根因**：翻闸 plugin paimon `COUNT(*)` 结果正确但慢。recon（`wf_1ce48c93-325`）：emit 缝（`PaimonScanRange.Builder.rowCount`→`paimon.row_count`→`setTableLevelRowCount`）+ COUNT 枚举→BE（`toThrift:90`/`PhysicalPlanTranslator:873`）**已建全**；唯缺**信号+计算**——`mergedRowCount()` 是 SDK-only（连接器算），COUNT 信号 `getPushDownAggNoGroupingOp()==COUNT` 只在 fe-core 节点、无人读 → 每 split 发 `-1` → BE 物化全行去 count。
- **⚠️ 非纯连接器（更正动手前 framing）**：信号须过 SPI。**用户签字 proceed + SPI 改 + collapse-to-one**。修=3 文件：SPI `ConnectorScanPlanProvider` +1 default 7-arg `planScan(...,boolean countPushdown)`（委托 6-arg，其余连接器 no-op，[E15]）；fe-core `PluginDrivenScanNode.getSplits` 读 agg-op 传入；连接器 `planScanInternal` count 短路第一臂 + `isCountPushdownSplit` + `buildCountRange`（**collapse-to-one**=legacy `<=10000` 路径普遍化）。legacy `>10000` 并行 trim 有意丢=[DV-032]。
- **review `wf_6ead7c2c-b58`**：1 MAJOR（单-split 测 degenerate）已修→2-partition 非对称(2+3=5)fixture 钉 collapse+sum；2 MINOR 驳回。守门：连接器 252/0/0、fe-core compile+checkstyle 0、fail-before 恰 2 新测红。

### #9 摘要 `FIX-NATIVE-SUBSPLIT` — commit `2f5f467f53d`（[D-055]/[DV-033]，纯连接器零 SPI/零 fe-core）
- **根因**：大 native ORC/Parquet 文件得一个 scanner（无文件内并行）；连接器每 RawFile 发整文件 range，legacy 经 `FileSplitter.splitFile` 切。recon（`wf_ad764bf6-1c9`）：真 gap（ORC/Parquet PLAIN 可切）；**DV×sub-split 安全**（DV rowid 全局行位、BE 部分 range 仍报全局位、`_kv_cache` 按 path+offset 共享、iceberg 同机制→**同一 DV 附每个 sub-range 不 re-base**）；**纯连接器**（切分 math 5 session var via VariableMgr.toMap、连接器禁 import FileSplitter）。
- **修=1 连接器文件**：2 纯静态 `computeFileSplitOffsets`（逐字移植含 **`>1.1D` 尾吸收 guard**）+ `determineTargetSplitSize`（移植 determineTargetFileSplitSize+applyMaxFileSplitNumLimit，省 isBatchMode→0）+ `sessionLong`/lazy `resolveTargetSplitSize` + native 臂 `buildNativeRanges` 内层 loop + `buildNativeRange(+start,+length)`。**count-pushdown splittable 闸**：非 count-eligible 的 native split 在 count pushdown 下保**整文件**（target=0，legacy `splittable=!applyCountPushdown` parity）。
- **review `wf_4ac7479d-39d`**：2 confirmed 已修（① MINOR count-pushdown sub-split parity gap+假注释→加 count-pushdown 整文件闸；② MAJOR 缺 DV-on-every-sub-range 测→抽 `buildNativeRanges` + 测）；2 驳回。守门：连接器 258/0/0、checkstyle 0、import-gate 净、fail-before 3 splitting 测红 + DV-only-first 测红。split-weight 调度 nicety 不移植（pre-existing）=[DV-033]。

## 🔜 下一个 session：**P3 覆盖缺口核查（"去查"，非 fix；查出真分歧才转 FIX）**
task-list §P3（completeness critic 标本轮未追）：
1. **VERIFY `FIX-HMS-CONFRES`**：round-2 未复测 `hive.config.resources`/hive-site.xml 下流到 BE-facing scan props（round-1 MAJOR 的修）。确认到达 `getScanNodeProperties`（HMS/DLF）。
2. **TRACE DDL 写路径 parity**：`PaimonConnectorMetadata.{createTable,dropTable,createDatabase,dropDatabase}`(`:683-797`) vs legacy `PaimonMetadataOps`；branch/tag DDL 写；IF-(NOT-)EXISTS 短路、editlog/cache-refresh 序、error-code parity。
3. **TRACE ANALYZE/列统计**：`ExternalAnalysisTask`/`getColumnStatistic` parity（fetchRowCount 已核实忠实）。
4. **CHECK split-count 计账**（`SqlBlockRuleMgr` 限额、batch-mode）—— 现 #9 已落 sub-split，复核 split 计数喂 SqlBlockRuleMgr 是否仍对（[[catalog-spi-plugindriven-explain-override-gap]] 提过 split-count 须 startSplit+getSplits 两路设）。
5. **跨连接器 follow-up**（[DV-028]/[DV-030]/[DV-031]）—— hudi/iceberg 同根因缝，将来批量 close（非本轮）。

⚠️ **P3 是「去查」不是「去改」**：查出真分歧 → AskUserQuestion 定是否转 FIX；否则记录「已核实 parity」即可。
> P4 MINOR/NIT（review §5）：一次性 cleanup；唯一真实数据边 = partition null-sentinel（`__HIVE_DEFAULT_PARTITION__`/`\N` 字面值被当 NULL）。

每条遵循 per-fix 流程（`step-by-step-fix` skill）：设计 doc → 先拿当前代码复核 finding → 实现（连接器禁 import fe-core）→ build+UT（绝对 `-f`、**`-am`** 必带、读 surefire XML + `MVN_EXIT`、fail-before/pass-after）→ 独立 commit → SPI 改登 RFC + 用户签字入 decisions-log + 偏差入 deviations-log + 同步 task-list。

## 📋 优先级总览（详见 task-list）

| 层 | 条目 | 说明 |
|---|---|---|
| **P0 BLOCKER** | ✅1·✅2·✅3·✅4 | **全清** |
| **P1 MAJOR** | ✅5·✅6·✅7 | **全清** |
| **P2 perf-parity** | ✅8.`FIX-COUNT-PUSHDOWN` · ✅9.`FIX-NATIVE-SUBSPLIT` | **全清**（#8 SPI+collapse-to-one；#9 纯连接器 sub-split）。各经 4-scout recon + 对抗 review，均揪出真 finding 已修。 |
| **P3 覆盖缺口（去查）** | ⬜ FIX-HMS-CONFRES 复验 · DDL 写 parity · ANALYZE · split-count 计账 · 跨连接器 follow-up | **下一个 session 起**。查出真分歧才转 FIX。 |
| **P4 MINOR/NIT** | 见 review §5 | 一次性 cleanup；partition null-sentinel 是唯一真实数据边。 |

---

# 📦 仓库状态
- **HEAD = `2f5f467f53d`**（`fix: FIX-NATIVE-SUBSPLIT` #9）。其父 = `525be03371c`（#8 COUNT-PUSHDOWN）。**注意**：本 session 未单独打 `docs: checkpoint` commit——#8/#9 的 task-list/decisions/deviations/RFC/HANDOFF 已**折入各自 fix commit**（#9 fix commit 含 HANDOFF 外的全部 doc + #8 hash finalize）。本 HANDOFF 更新**未 commit**（下个 session 或现在可单独 `docs:` 提）。
- ⚠️ **`regression-test/conf/regression-conf.groovy` 仍 modified-未 commit 且含明文 Aliyun key** —— commit 前继续 path-whitelist，**严禁 `git add -A`**。`regression-conf.groovy.bak` 同理排除。
- scratch 仍未 commit（`.audit-scratch/` `conf.cmy/` `META-INF/`）。
- 当前分支 `catalog-spi-07-paimon`（非 `master`）。
- **legacy `datasource/paimon/*` 仍在树内**（B8 删除未做）→ 每个 fix 都能 side-by-side diff 做 parity。
- 迁移链：…→`05132a42768`(#7)→`525be03371c`(#8 COUNT-PUSHDOWN)→`2f5f467f53d`(#9 NATIVE-SUBSPLIT, HEAD)。

## ⚠️ Commit 须知（任何 `git add` 前必读）
- **硬前置**：scrub `regression-test/conf/regression-conf.groovy`（明文 Aliyun key）+ 清 scratch（`.audit-scratch/` `conf.cmy/` `META-INF/` `*.bak`）。**path-whitelist `git add`，严禁 `git add -A`。**
- 每个 fix 独立 commit；message = `fix: <ID>` + 根因 + 解法 + 测试，末尾带 `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`。
- 改 fe-core/SPI 的 fix：commit 须含连接器 + fe-core 两侧 + 测试（#8 即如此：SPI api + fe-core + 连接器）。#9 纯连接器单侧。

## ⚙️ 操作须知（复用）
- maven 绝对 `-f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl :<art> **-am** -Dmaven.build.cache.enabled=false -DfailIfNoTests=false`；验证读 surefire XML + `MVN_EXIT`（[[doris-build-verify-gotchas]]）。**`-am` 必带**（漏则报 `could not resolve fe-connector ${revision}` 假错）。改 fe-core 须单独 `-pl :fe-core -am`。**checkstyle**：连接器 `mvn -pl :fe-connector-paimon checkstyle:check`（exit 0 即净）；fe-core `mvn -pl :fe-core checkstyle:check`。
- 连接器禁 import fe-core：`bash tools/check-connector-imports.sh`（仅允许 `org.apache.doris.{thrift,connector,extension,filesystem}`）。
- cwd 跨 Bash 调用持久，`cd` 破相对路径 → 一律绝对路径。
- 测试优先 runnable FE 单测（harness：`RecordingConnectorContext`/`RecordingPaimonCatalogOps`/`FakePaimonTable`/`PaimonScanPlanProviderTest`）。**关键**：`FakePaimonTable.newReadBuilder()` 抛 → 纯静态 seam（`shouldUseNativeReader`/`isForceJniScannerEnabled`/`isCountPushdownSplit`/`computeFileSplitOffsets`/`determineTargetSplitSize`/`buildNativeRanges`）才离线可测；但**真 `DataSplit` 可经 `buildRealDataSplit`/inline FileSystemCatalog 离线构造**（#8/#9 end-to-end 测即用之：PK 表 count、append-only 表 sub-split）。live-e2e CI-gated → 注明 gated，勿谎称跑过。

## 🧠 给下一个 agent 的 meta
- **#8/#9 验证的高价值模式（再次奏效）**：finding → **多-scout recon workflow + 对抗 synthesizer**（决定 pure-connector vs needs-SPI、DV 安全性等 gating 问题）→ 设计 doc → 实现 → **fail-before 实测**（neuter helper、双向红）→ pass-after → **独立 commit 前再跑对抗 review workflow**。**两次 review 都揪出真 finding**：#8 review 抓出我自己的测 degenerate（单-split fixture 让 collapse/sum 断言失效）；#9 review 抓出 count-pushdown sub-split parity gap（我设计 doc 的假"无 interaction"声明）+ 缺 DV-on-every-sub-range 测。**教训：commit 前的对抗 review 对 test-rigor + 自身设计假设的证伪价值极高，勿跳过。**
- **#8 关键定夺**：连接器无法见 agg-op（per-query planner 输出非 session var）→ 必须过 SPI（否决 session-channel hack）；collapse-to-one = legacy `<=10000` 普遍化。
- **#9 关键定夺**：DV×sub-split 安全（全局行位）；count-pushdown 下 native split 保整文件（legacy `splittable=!applyCountPushdown`）；纯静态 math seam 可离线 mutation-test，真 DataSplit 经 inline FileSystemCatalog 可离线 end-to-end。
- **P3 是核查不是改**：先拿当前代码复核（行号已大漂移，#3/#4/#6/#7/#8/#9 都改过 scan provider / metadata），查出真分歧再 AskUserQuestion 定 scope。
- **跨连接器 follow-up 累积**：[DV-028]/[DV-030]/[DV-031]（read 法/翻闸 vs fe-core 约定）+ [DV-032]（count collapse parallel-trim）+ [DV-033]（native split-weight nicety）—— hudi/iceberg full-adopter 同复发，将来批量 close。
