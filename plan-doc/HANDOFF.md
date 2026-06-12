# 🤝 Session Handoff

> 滚动文档：每次 session 结束**直接覆盖**（不保留历史；历史见 `git log plan-doc/HANDOFF.md`）。
> 协作规范：[AGENT-PLAYBOOK.md](./AGENT-PLAYBOOK.md)

---

# 🎯 下一个 session 的任务 — **逐一修复 paimon connector 第二轮 review 的问题（#1~#7 已完成 → 从 #8 起）**

第二轮 clean-room 对抗 review report：[`plan-doc/reviews/P5-paimon-rereview2-2026-06-11.md`](./reviews/P5-paimon-rereview2-2026-06-11.md)。
👉 **任务清单（按优先级）：[`plan-doc/task-list-P5-rereview2-fixes.md`](./task-list-P5-rereview2-fixes.md)** —— 逐条含 finding 引用、连接器 `file:line`、legacy parity 锚、fix sketch、SPI 影响、测法。

## ✅ 已完成（P0 BLOCKER 全清 + P1 MAJOR #5/#6/#7 全清）
- **#1 `FIX-URI-NORMALIZE`**（B-7DF/DV）`20b19d19dd8` —— native 数据文件 + DV 路径 scheme 归一化。新 SPI `ConnectorContext.normalizeStorageUri`。
- **#2 `FIX-STATIC-CREDS-BE`**（B-9）`d23d5df9914` —— 静态 object-store 凭据→BE canonical `AWS_*`。新 SPI `ConnectorContext.getBackendStorageProperties`。
- **#3 `FIX-SCHEMA-EVOLUTION`**（B-1a；M-10 deferred）`667f779af04` —— 连接器直建 thrift schema 字典（Design C，零新 SPI）。
- **#4 `FIX-JDBC-DRIVER-URL`**（B-8a + B-8b）`2d15b1b7ed7` —— driver_url resolve+别名+CREATE-time 校验（纯连接器，零新 SPI；[D-050]/[DV-028]/[DV-029]）。
- **#5 `FIX-MAPPING-FLAG-KEYS`**（M-crit）`9dcf6d1a9e5` —— 连接器读 canonical 点分 mapping-flag 键（纯连接器，零 SPI；paimon-only，hive/iceberg 登 [DV-030]）。
- **#6 `FIX-KERBEROS-DOAS`**（M-8 + M-11）`2b1442fa57a` —— M-8 fe-core fs/jdbc authenticator 接线 + M-11 全 read RPC 包 `executeAuthenticated`（full legacy parity [D-052]/[D-053]；DLF 从句证伪 overstated；[DV-031]）。
- **#7 `FIX-FORCE-JNI-SCANNER`**（M-1；本 session）`05132a42668` —— 见下。

### #7 摘要（本 session）`FIX-FORCE-JNI-SCANNER` —— commit `05132a42668`
- **根因**：翻闸连接器 split router 只读 NAME 派生的 `paimonHandle.isForceJni()`（binlog/audit_log 名钉），**从不**读 session var `force_jni_scanner` → ORC/Parquet 永走 native；legacy 的 JNI 逃生舱（`SET force_jni_scanner=true`，用于绕 native-reader bug 含 B2 schema-evolution 那类）静默丢失。连接器只移植 legacy 三 conjunct 中的两个（`PaimonScanNode.java:430`：`!forceJniScanner && !forceJniForSystemTable && supportNativeReader`），丢的 `!forceJniScanner` 即 M-1。
- **修**（纯连接器、**零 SPI**、无 fe-core import、无 BE param —— legacy 也不序列化此 var）：
  - 新 `isForceJniScannerEnabled(session)`：逐字镜像 `isCppReaderEnabled`，读 key `force_jni_scanner`（byte-identical to `SessionVariable.FORCE_JNI_SCANNER`，同 `VariableMgr.toMap` 通道）；null-guard，默认 false（legacy 默认）。
  - **Site A**（correctness，`PaimonScanPlanProvider.java:295`）：`shouldUseNativeReader` 加显式 `forceJniScanner` 形参（1:1 镜像 legacy 三-boolean 闸），`planScan` 传 `isForceJniScannerEnabled(session)`。**handle 名钉是 OR-sibling，绝不替换**（binlog/audit_log 路由不变）。
  - **Site B**（correctness-NEUTRAL，`:436`）：force-JNI 时抑制 native-only `paimon.schema_evolution` 字典（BE 仅在 native ORC/Parquet range 消费它，JNI/cpp reader 全忽略——核 `paimon_reader.cpp:51-54,188-191` / `file_scanner.cpp:1045-1058`）；对齐连接器自身注释契约。
- **关键设计定夺（本 session，内部工程判断，无须用户签字）**：`shouldUseNativeReader` 用**显式 3rd 形参**而非 call-site OR——**推翻 workflow synthesizer 的 call-site-OR 建议，采 legacy-parity scout**。理由：`force_jni_scanner` 是与既有 `forceJni`（=`forceJniForSystemTable`）语义并列的**路由**输入（legacy `:430` 即两 sibling boolean 同闸），call-site OR 会让新维度只能经 helper 的**字符串解析**测，而那测**routing 逻辑变了也不会红**（违 Rule 9）；3rd 形参让 `shouldUseNativeReader(false, true, native-eligible)==JNI` 成 mutation-tested 事实。`cppReader=isCppReaderEnabled(session)`（序列化格式 flag，非路由）不是正确类比。
- **验证**：连接器模块 **250/0/0**（1 CI-gated live skip = `PaimonLiveConnectivityTest`）、import-gate 净、checkstyle 0；**fail-before 双向红**（neuter 丢 conjunct + helper return-false → 恰两新测红、其余 31 绿）。真 BE reader 选择 = **live-e2e only**（无离线 harness 驱动 BE reader 选择）。设计 [`P5-fix-FORCE-JNI-SCANNER-design.md`](./tasks/designs/P5-fix-FORCE-JNI-SCANNER-design.md)。
- **Site B 测覆盖诚实声明**（Rule 12）：emit-suppression **无专属离线 red 测**——`buildSchemaEvolutionParam` 需真 `FileStoreTable`+`SchemaManager`，离线 harness 只有 `FakePaimonTable`（恒返空字典），故撤 Site B 闸不会红任何离线测。Site B 由：① 共享 `isForceJniScannerEnabled` helper 测（其唯一变量项）② BE-源 correctness-neutral 证据 ③ CI-gated live-e2e 覆盖。

## 🔜 下一个 session：从 **#8 `FIX-COUNT-PUSHDOWN`** 起 —— ⚠️ **P2 严重度有争议，动手前先问用户定 scope**
> ⚠️ **先拿当前代码复核 finding**（review 只读，行号已漂移；#7 改过 `PaimonScanPlanProvider`，#3/#4/#6 亦改过 scan provider/metadata）。

**#8 `FIX-COUNT-PUSHDOWN`（M-2，round-2=MAJOR / round-1=MINOR，perf-parity）**：
- **根因**：`COUNT(*)` 下推对该 node **仍 ENABLED**（`PhysicalPlanTranslator.java:873`），但连接器**从不**算 `mergedRowCount` 也不 emit `paimon.row_count` → `table_level_row_count=-1` → BE 回退（`paimon_jni_reader.cpp:104`、`file_scanner.cpp:1298-1326`）**物化 merge 后全行**去 count（PK 表 merge/delete 尤贵）。**结果正确，仅性能回归。**
- **连接器**：`PaimonScanPlanProvider.java:186-296`（无 count 分支）。**legacy**：`source/PaimonScanNode.java:396,421-429,483-495,303-308`（`applyCountPushdown` + `dataSplit.mergedRowCount()`，在 native/JNI 闸**之前**短路）。
- **#9 `FIX-NATIVE-SUBSPLIT`（M-3，同 perf-parity）**：一个 split/RawFile，大 ORC/Parquet 单 scanner；`PaimonScanPlanProvider.java:263-286` vs `source/PaimonScanNode.java:434-465`（`determineTargetFileSplitSize`+`fileSplitter.splitFile`）。
- ⚠️ **#8/#9 都是结果正确、仅 perf/并行** → **动手前用 `AskUserQuestion` 找用户定 scope**（accept-or-defer；defer 则登 `deviations-log.md`，**勿**默认实现）。这与 #7（明确 MAJOR、无歧义、直接修）不同。

每条遵循项目既定 per-fix 流程（`step-by-step-fix` skill）：1) 设计 doc → `plan-doc/tasks/designs/P5-fix-<ID>-design.md`；2) **先拿当前代码复核 finding**；3) 实现（minimal、surgical、**连接器禁 import fe-core**）；4) build+UT（绝对 `-f`、**`-am`** 必带、读 surefire XML + `MVN_EXIT`、加 fail-before/pass-after UT）；5) **独立 commit**；6) SPI 改动登 `01-spi-extensions-rfc.md`、用户签字入 `decisions-log.md`、偏差入 `deviations-log.md`、同步 task-list。

## 📋 优先级总览（详见 task-list）

| 层 | 条目 | 说明 |
|---|---|---|
| **P0 BLOCKER（挡 commit）** | ✅1.URI-NORMALIZE · ✅2.STATIC-CREDS-BE · ✅3.SCHEMA-EVOLUTION · ✅4.JDBC-DRIVER-URL | **全清** |
| **P1 MAJOR（修或显式接受）** | ✅5.`FIX-MAPPING-FLAG-KEYS` · ✅6.`FIX-KERBEROS-DOAS` · ✅7.`FIX-FORCE-JNI-SCANNER` | **全清**。#7 纯连接器零 SPI，3rd-param 推翻 synthesizer call-site-OR（Rule 9），Site B correctness-neutral。 |
| **P2 严重度有争议（perf；R1=MINOR）** | ⬜8.`FIX-COUNT-PUSHDOWN`(M-2) · ⬜9.`FIX-NATIVE-SUBSPLIT`(M-3) | 结果正确仅性能/并行。**动手前先 `AskUserQuestion` 定 scope**（accept-or-defer，defer 则登 `deviations-log`）。 |
| **P3 覆盖缺口（去查）** | 复验 `FIX-HMS-CONFRES` · DDL 写路径 parity · ANALYZE/列统计 · split-count 计账 · 跨连接器 follow-up（[DV-028]/[DV-030]/[DV-031]） | critic 标本轮未追；查出真分歧才转 FIX。 |
| **P4 MINOR/NIT** | 见 review §5 | 一次性 cleanup；唯一真实数据边 = partition null-sentinel（`__HIVE_DEFAULT_PARTITION__`/`\N` 字面值被当 NULL）。 |

---

# 📦 仓库状态
- **HEAD = 本 checkpoint commit**（更新 task-list #7 进度+hash、HANDOFF）。其父 = `05132a42668`（`fix: FIX-FORCE-JNI-SCANNER`，本 session #7）。该 fix commit = 连接器(1 main+1 test)+设计 doc（3 文件，无 regression-conf/scratch/HANDOFF）。
- ⚠️ **`regression-test/conf/regression-conf.groovy` 仍 modified-未 commit 且含明文 Aliyun key** —— 任何 commit 前继续 path-whitelist，**严禁 `git add -A`**。`regression-conf.groovy.bak` 同理排除。
- scratch 仍未 commit（`.audit-scratch/` `conf.cmy/` `META-INF/`）。
- 当前分支 `catalog-spi-07-paimon`（非 `master`）→ 在此 commit 修复 OK。
- **legacy `datasource/paimon/*` 仍在树内**（B8 删除未做）→ 每个 fix 都能 side-by-side diff 做 parity。
- 迁移链：…→`9dcf6d1a9e5`(#5)→`2b1442fa57a`(#6 KERBEROS-DOAS)→`05132a42668`(#7 FORCE-JNI-SCANNER)→本 checkpoint(HEAD)。

## ⚠️ Commit 须知（任何 `git add` 前必读）
- **硬前置**：scrub `regression-test/conf/regression-conf.groovy`（明文 Aliyun key）+ 清 scratch（`.audit-scratch/` `conf.cmy/` `META-INF/` `*.bak`）。**path-whitelist `git add`，严禁 `git add -A`。**
- 每个 fix 独立 commit；message = `fix: <ID>` + 根因 + 解法 + 测试，末尾带 `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`。
- 改 fe-core/SPI 的 fix：commit 须含连接器 + fe-core 两侧 + 测试。#7 纯连接器，单侧。

## ⚙️ 操作须知（复用）
- maven 绝对 `-f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl :<art> **-am** -Dmaven.build.cache.enabled=false -DfailIfNoTests=false`；验证读 surefire XML + `MVN_EXIT`（[[doris-build-verify-gotchas]]）。**`-am` 必带**。`-pl :fe-connector-paimon -am` **不重编 fe-core**；改 fe-core 须单独 `-pl :fe-core -am`。**checkstyle**：连接器模块可单独 `mvn -pl :fe-connector-paimon checkstyle:check`（#7 已用，exit 0 即净）；fe-core checkstyle 绑在其 `test` build（neuter 须 checkstyle-clean）。
- 连接器禁 import fe-core：`bash tools/check-connector-imports.sh`（仅允许 `org.apache.doris.{thrift,connector,extension,filesystem}`）。
- cwd 跨 Bash 调用持久，`cd` 破相对路径 → 一律绝对路径。
- 测试优先 runnable FE **单测**（连接器 harness：`RecordingConnectorContext`/`RecordingPaimonCatalogOps`/`FakePaimonTable`/`PaimonScanPlanProviderTest`；离线**无 FileStoreTable**——`FakePaimonTable.newReadBuilder()` 抛、`buildSchemaEvolutionParam` 返空，故 native-path/schema-dict emit 的正向路径不可离线驱动，纯静态 seam（`shouldUseNativeReader`/`isForceJniScannerEnabled`）才可测）；live-e2e CI-gated → 注明 gated，勿谎称跑过。

## 🧠 给下一个 agent 的 meta
- **#7 验证的高价值模式（再次奏效）**：finding → **4-scout + 对抗 synthesizer workflow 独立复核**（sites / legacy-parity / session-plumbing / BE+test-safety）→ 设计 → 实现 → **fail-before 实测**（neuter conjunct+helper、跑测、双向红）→ pass-after。**本次关键：复核 synthesizer 自身的判断**——synthesizer 选 call-site-OR（求最小 churn），但 legacy-parity scout 选 3rd-param（求 routing 可 mutation-test）；我**站 scout 推翻 synthesizer**（Rule 9：测须能在 routing 逻辑变时红）。教训=**别盲从 synthesizer，交叉核其理由**。
- **改 fe-core handle/scan 流前，先 grep 全 `metadata.getTableHandle` / scan-node 调用方**（历史教训）。#7 纯连接器无此风险。
- **#8/#9 = P2 perf-parity，severity 有争议 → 动手前先 `AskUserQuestion` 定 scope**（与 #7 无歧义直接修不同）。accept→修；defer→登 `deviations-log` 勿默认实现。
- **跨连接器 follow-up 累积**：[DV-028]（#4 CREATE-time-only 校验）+ [DV-030]（#5 mapping-flag 键）+ [DV-031]（#6 read-vs-DDL doAs + 翻闸-authenticator-wiring）—— 三者同属「新连接器读法/翻闸 vs fe-core 既有约定」类缝，hudi/iceberg 同样复发，将来批量 close。#7 无新增 DV（full parity，Site B 是连接器自有非 legacy 偏差）。
