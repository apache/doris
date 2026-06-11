# 🤝 Session Handoff

> 滚动文档：每次 session 结束**直接覆盖**（不保留历史；历史见 `git log plan-doc/HANDOFF.md`）。
> 协作规范：[AGENT-PLAYBOOK.md](./AGENT-PLAYBOOK.md)

---

# 🎯 下一个 session 的任务 — **逐一修复 paimon connector 第二轮 review 的问题（#1~#6 已完成 → 从 #7 起）**

第二轮 clean-room 对抗 review report：[`plan-doc/reviews/P5-paimon-rereview2-2026-06-11.md`](./reviews/P5-paimon-rereview2-2026-06-11.md)。
👉 **任务清单（按优先级）：[`plan-doc/task-list-P5-rereview2-fixes.md`](./task-list-P5-rereview2-fixes.md)** —— 逐条含 finding 引用、连接器 `file:line`、legacy parity 锚、fix sketch、SPI 影响、测法。

## ✅ 已完成（P0 BLOCKER 全清 + P1 #5、#6）
- **#1 `FIX-URI-NORMALIZE`**（B-7DF/DV）`20b19d19dd8` —— native 数据文件 + DV 路径 scheme 归一化。新 SPI `ConnectorContext.normalizeStorageUri`。
- **#2 `FIX-STATIC-CREDS-BE`**（B-9）`d23d5df9914` —— 静态 object-store 凭据→BE canonical `AWS_*`。新 SPI `ConnectorContext.getBackendStorageProperties`。
- **#3 `FIX-SCHEMA-EVOLUTION`**（B-1a；M-10 deferred）`667f779af04` —— 连接器直建 thrift schema 字典（Design C，零新 SPI）。
- **#4 `FIX-JDBC-DRIVER-URL`**（B-8a + B-8b）`2d15b1b7ed7` —— driver_url resolve+别名+CREATE-time 校验（纯连接器，零新 SPI；[D-050]/[DV-028]/[DV-029]）。
- **#5 `FIX-MAPPING-FLAG-KEYS`**（M-crit）`9dcf6d1a9e5` —— 连接器读 canonical 点分 mapping-flag 键（纯连接器，零 SPI；paimon-only，hive/iceberg 登 [DV-030]）。
- **#6 `FIX-KERBEROS-DOAS`**（M-8 + M-11；本 session）`2b1442fa57a` —— 见下。

### #6 摘要（本 session）`FIX-KERBEROS-DOAS` —— commit `2b1442fa57a`
- **两半，均 Kerberos-only**（simple-auth 上 no-op authenticator 与真 authenticator 行为一致、无回归）：
  - **M-8（fe-core，filesystem+jdbc）**：翻闸后这两 flavor 在 Kerberized HDFS 上丢 UGI `doAs`——连接器 `PaimonConnector.createCatalog:194` 已把建 catalog 包进 `executeAuthenticated`，但其背后 authenticator 是**基类 no-op**：HDFS `HadoopExecutionAuthenticator` 仅在 `initializeCatalog()`(`PaimonFileSystemMetaStoreProperties:46`/`PaimonJdbcMetaStoreProperties:120`) 内建，而该方法在翻闸路径**死代码**（唯一 live 调用方=legacy `PaimonExternalCatalog:147`）→ `PluginDrivenExternalCatalog.initPreExecutionAuthenticator:130` 读到 `AbstractPaimonProperties:45` no-op。HMS 不受影响（`initNormalizeAndCheckProps:70` 即建，必跑）。**DLF/REST 排除**（DLF 用 Aliyun STS 非 Kerberos UGI、无 doAs 可丢；review「DLF」从句 **overstated**，已核）。
  - **M-11（连接器）**：metadata **读** RPC（listDatabases/getDatabase/listTables/getTable[getTableHandle+getSysTableHandle+resolveTable]/listPartitions）**不**包 `executeAuthenticated`，仅 4 DDL op 包（B3 **D7=B** 故意 read-vs-DDL 不对称）→ Kerberos HMS 上读跑在 catalog principal 之外。
- **用户签字（本 session 两决策）**：**M-11 = full legacy parity 包全部 read RPC**（[D-052]，**取代 D7=B 的 read 从句**；legacy 本就包每个 read，仅包 listPartitions 是半吊子）；**M-8 = fix-now fe-core，filesystem+jdbc only**（[D-053]）。
- **修**：M-8 = 新 fe-core hook `MetastoreProperties.initExecutionAuthenticator(List<StorageProperties>)`（default no-op）由 `PluginDrivenExternalCatalog.initPreExecutionAuthenticator` 用已安全建好的 `getOrderedStoragePropertiesList()` 调；filesystem/jdbc override 经 `AbstractPaimonProperties.initHdfsExecutionAuthenticator` 共享 helper 建 HDFS authenticator（镜像 HMS）——**零连接器改、非连接器 SPI**（`ConnectorContext`/`Connector` 表面未改，RFC 无须改）。M-11 = 7 处 read site 包 `context.executeAuthenticated`，`resolveTable`（metadata+scan 双 site）一处覆盖所有调用方（DRY）。**异常流关键**：Kerberos `UGI.doAs` 把 checked `Catalog.{Table,Database}NotExistException` 包成 `UndeclaredThrowableException`（仅 IOException/RuntimeException/Error 透传）→ domain 异常**必须在 lambda 内**捕获（镜像 legacy `getPaimonPartitions:104`）；scan `resolveTable` 对 `context==null`（2-arg ctor 离线测）走直连，同 `getScanNodeProperties` 既有约定。
- **验证**：连接器模块 **248/0/0**（1 CI-gated skip）、fe-core metastore-props **21/0/0**（含 DLF/HMS regression-clean）、checkstyle 0、import-gate 净；**fail-before 双向红**（M-8 留 no-op `AbstractPaimonProperties$1`；M-11 3 测 authCount/log-empty 向红）。**真 doAs 端到端=live-Kerberos-e2e only**（无 paimon-kerberos 套件，[DV-031]）。设计 [`P5-fix-KERBEROS-DOAS-design.md`](./tasks/designs/P5-fix-KERBEROS-DOAS-design.md)。

## 🔜 下一个 session：从 **#7 `FIX-FORCE-JNI-SCANNER`** 起，按 task-list 顺序续修
> ⚠️ **先拿当前代码复核 finding**（review 只读，行号可能漂移；#6 改过 `PaimonConnectorMetadata`/`PaimonScanPlanProvider`/`PaimonScanPlanProviderTest`，#3/#4/#5 亦改过 scan provider）。

**#7 `FIX-FORCE-JNI-SCANNER`（M-1，MAJOR，纯连接器，no SPI）**：
- **根因**：连接器只读 `paimonHandle.isForceJni()`（binlog/audit_log 的 NAME-forced 标志），**从不**读 session var `force_jni_scanner`；ORC/Parquet 永远走 native。JNI 逃生舱（用于绕开 native-reader bug，含 B2 schema-evolution 那类）丢失。
- **连接器**：`PaimonScanPlanProvider.java:261,439-441`（`shouldUseNativeReader`）。**legacy**：`source/PaimonScanNode.java:361,430`（`sessionVariable.isForceJniScanner()` gate）。
- **Fix sketch**：从 session-properties map 读 `force_jni_scanner`（该 var 已在 map 里——连接器读 sibling `enable_paimon_cpp_reader` 即出自此），set 时把所有 data split 路由到 JNI。纯连接器、无 SPI、无 BE。真值闸=live e2e（CI-gated）。
- ⚠️ **scope 无须问用户**（明确 MAJOR、纯连接器、无歧义）；直接按 per-fix 流程修。

每条遵循项目既定 per-fix 流程（`step-by-step-fix` skill）：1) 设计 doc → `plan-doc/tasks/designs/P5-fix-<ID>-design.md`；2) **先拿当前代码复核 finding**；3) 实现（minimal、surgical、**连接器禁 import fe-core**）；4) build+UT（绝对 `-f`、**`-am`** 必带、读 surefire XML + `MVN_EXIT`、加 fail-before/pass-after UT）；5) **独立 commit**；6) SPI 改动登 `01-spi-extensions-rfc.md`、用户签字入 `decisions-log.md`、偏差入 `deviations-log.md`、同步 task-list。

## 📋 优先级总览（详见 task-list）

| 层 | 条目 | 说明 |
|---|---|---|
| **P0 BLOCKER（挡 commit）** | ✅1.URI-NORMALIZE · ✅2.STATIC-CREDS-BE · ✅3.SCHEMA-EVOLUTION · ✅4.JDBC-DRIVER-URL | **全清** |
| **P1 MAJOR（修或显式接受）** | ✅5.`FIX-MAPPING-FLAG-KEYS`(M-crit) · ✅6.`FIX-KERBEROS-DOAS`(M-8+M-11) · **⬜7.`FIX-FORCE-JNI-SCANNER`(M-1)** | #6 已修（M-11 full parity 取代 D7=B read 从句 [D-052]；M-8 fs/jdbc only [D-053]；DLF 从句证实 overstated）。#7 纯连接器、scope 无歧义、直接修。 |
| **P2 严重度有争议（perf；R1=MINOR）** | 8.`FIX-COUNT-PUSHDOWN`(M-2) · 9.`FIX-NATIVE-SUBSPLIT`(M-3) | 结果正确仅性能/并行。**动手前先找用户定 scope**（accept-or-defer，defer 则登 `deviations-log`）。 |
| **P3 覆盖缺口（去查）** | 复验 `FIX-HMS-CONFRES` · DDL 写路径 parity · ANALYZE/列统计 · split-count 计账 · 跨连接器 follow-up（[DV-028]/[DV-030]/**[DV-031]**） | critic 标本轮未追；查出真分歧才转 FIX。 |
| **P4 MINOR/NIT** | 见 review §5 | 一次性 cleanup；唯一真实数据边 = partition null-sentinel（`__HIVE_DEFAULT_PARTITION__`/`\N` 字面值被当 NULL）。 |

---

# 📦 仓库状态
- **HEAD = `2b1442fa57a`**（`fix: FIX-KERBEROS-DOAS`，本 session #6）。该 fix commit = 连接器(2 main+2 test)+fe-core(5 main+2 test)+设计 doc+D-052/D-053+DV-031+task-list 进度（15 文件，无 regression-conf/scratch/HANDOFF）。
- **本 checkpoint commit 改动**：`plan-doc/HANDOFF.md`（本文件）、`plan-doc/task-list-P5-rereview2-fixes.md`（#6 commit-cell 填 hash）。
- ⚠️ **`regression-test/conf/regression-conf.groovy` 仍 modified-未 commit 且含明文 Aliyun key** —— 任何 commit 前继续 path-whitelist，**严禁 `git add -A`**。`regression-conf.groovy.bak` 同理排除。
- scratch 仍未 commit（`.audit-scratch/` `conf.cmy/` `META-INF/`）。
- 当前分支 `catalog-spi-07-paimon`（非 `master`）→ 在此 commit 修复 OK。
- **legacy `datasource/paimon/*` 仍在树内**（B8 删除未做）→ 每个 fix 都能 side-by-side diff 做 parity。
- 迁移链：…→`2d15b1b7ed7`(#4)→`9dcf6d1a9e5`(#5)→`2b1442fa57a`(#6 KERBEROS-DOAS, HEAD)。

## ⚠️ Commit 须知（任何 `git add` 前必读）
- **硬前置**：scrub `regression-test/conf/regression-conf.groovy`（明文 Aliyun key）+ 清 scratch（`.audit-scratch/` `conf.cmy/` `META-INF/` `*.bak`）。**path-whitelist `git add`，严禁 `git add -A`。**
- 每个 fix 独立 commit；message = `fix: <ID>` + 根因 + 解法 + 测试，末尾带 `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`。
- 改 fe-core/SPI 的 fix：commit 须含连接器 + fe-core 两侧 + 测试（#6 含 fe-core authenticator 接线 + 连接器 read-wrap）。

## ⚙️ 操作须知（复用）
- maven 绝对 `-f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl :<art> **-am** -Dmaven.build.cache.enabled=false -DfailIfNoTests=false`；验证读 surefire XML + `MVN_EXIT`（[[doris-build-verify-gotchas]]）。**`-am` 必带**。`-pl :fe-connector-paimon -am` **不重编 fe-core**；改 fe-core 须单独 `-pl :fe-core -am`。**checkstyle 绑在 fe-core `test` build**（#6 临时 neuter 写 `if (true) { return; }` 触 LeftCurly 挂 build，须 checkstyle-clean 的 neuter）。
- 连接器禁 import fe-core：`bash tools/check-connector-imports.sh`（仅允许 `org.apache.doris.{thrift,connector,extension,filesystem}`）。
- cwd 跨 Bash 调用持久，`cd` 破相对路径 → 一律绝对路径。
- 测试优先 runnable FE **单测**（连接器 harness：`RecordingConnectorContext`(`failAuth`/`authCount`)/`RecordingPaimonCatalogOps`(读 log)/`FakePaimonTable`/`PaimonConnectorMetadataReadAuthTest`/`PaimonScanPlanProviderTest`；fe-core metastore-props 用 `MetastoreProperties.create`+`StorageProperties.createAll` 离线）；live-e2e（S3/OSS/REST/JDBC/**Kerberos**）CI-gated → 注明 gated，勿谎称跑过。

## 🧠 给下一个 agent 的 meta
- **#6 验证的高价值模式（再次奏效）**：finding → **5-agent scout workflow 独立复核**（M-8 flow / M-8 legacy / M-11 read-sites / DLF+D7 / scope+test）+ **对抗 synthesizer** → 框定 2 个 scope 决策问用户（`AskUserQuestion`）→ 设计 → 实现 → **fail-before 实测**（neuter wrap、跑两测、向红）→ pass-after。本次 scout 揪出 finding 没说的关键事实：① M-8 不是「缺 wrap」而是「wrap 背后 authenticator 是 no-op」（connector 已 wrap、fe-core 才是 seam）② legacy 包**所有** read（getTable+listPartitions+…）非仅 listPartitions → 决定 M-11 scope ③ DLF 从句 overstated（DLF 无 Kerberos）④ **Kerberos `UGI.doAs` 包 checked 异常成 `UndeclaredThrowableException`** → domain 异常必须 lambda 内捕获。
- **改 fe-core handle/scan 流前，先 grep 全 `metadata.getTableHandle` / scan-node 调用方**（历史教训）。
- **#7 纯连接器、scope 无歧义**（明确读 session `force_jni_scanner` 路由 JNI）→ 直接修，无须问用户。**P2（#8/#9）严重度有争议 → 先找用户定 scope 再动手**。
- **跨连接器 follow-up 累积**：[DV-028]（#4 CREATE-time-only 校验）+ [DV-030]（#5 mapping-flag 键）+ **[DV-031]（#6 read-vs-DDL doAs + 翻闸-authenticator-wiring，hudi/iceberg 同样复发）** —— 将来批量 close 时三者同属「新连接器读法/翻闸 vs fe-core 既有约定」类缝。
