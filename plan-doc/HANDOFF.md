# 🤝 Session Handoff

> 滚动文档：每次 session 结束**直接覆盖**（不保留历史；历史见 `git log plan-doc/HANDOFF.md`）。
> 协作规范：[AGENT-PLAYBOOK.md](./AGENT-PLAYBOOK.md)

---

# 🎯 下一个 session 的任务 — **逐一修复 paimon connector 第二轮 review 的问题（#1~#5 已完成 → 从 #6 起）**

第二轮 clean-room 对抗 review report：[`plan-doc/reviews/P5-paimon-rereview2-2026-06-11.md`](./reviews/P5-paimon-rereview2-2026-06-11.md)。
👉 **任务清单（按优先级）：[`plan-doc/task-list-P5-rereview2-fixes.md`](./task-list-P5-rereview2-fixes.md)** —— 逐条含 finding 引用、连接器 `file:line`、legacy parity 锚、fix sketch、SPI 影响、测法。

## ✅ 已完成（P0 BLOCKER 全清 + P1 #5）
- **#1 `FIX-URI-NORMALIZE`**（B-7DF/DV）`20b19d19dd8` —— native 数据文件 + DV 路径 scheme 归一化。新 SPI `ConnectorContext.normalizeStorageUri`。
- **#2 `FIX-STATIC-CREDS-BE`**（B-9）`d23d5df9914` —— 静态 object-store 凭据→BE canonical `AWS_*`。新 SPI `ConnectorContext.getBackendStorageProperties`。
- **#3 `FIX-SCHEMA-EVOLUTION`**（B-1a；M-10 deferred）`667f779af04` —— 连接器直建 thrift schema 字典（Design C，零新 SPI）。
- **#4 `FIX-JDBC-DRIVER-URL`**（B-8a + B-8b）`2d15b1b7ed7` —— driver_url resolve+别名+CREATE-time 校验（纯连接器，零新 SPI；[D-050]/[DV-028]/[DV-029]）。
- **#5 `FIX-MAPPING-FLAG-KEYS`**（M-crit；本 session）`9dcf6d1a9e5` —— 见下。

### #5 摘要（本 session）`FIX-MAPPING-FLAG-KEYS` —— commit `9dcf6d1a9e5`
- **根因**：翻闸后 paimon 连接器类型映射两开关**静默失效**——连接器读**下划线**键 `enable_mapping_binary_as_varbinary`/`enable_mapping_timestamp_tz`（`PaimonConnectorProperties:39,42`→`PaimonConnectorMetadata.buildTypeMappingOptions`），但 fe-core 只写**点分** catalog 键 `enable.mapping.varbinary`/`enable.mapping.timestamp_tz`（`CatalogProperty:50,52`；`ExternalCatalog.setDefaultPropsIfMissing:302-306` 仅写点分；`HIDDEN_PROPERTIES` 仅藏点分），`createConnectorFromProperties` 把**原始** catalog map 原样喂连接器 → `getOrDefault(下划线,"false")` 恒 false → 用户即便开启，BINARY 仍→STRING、LTZ 仍→DATETIMEV2（legacy `PaimonExternalTable:350` 读点分 honor → cutover 回归，flag 启用前 latent）。binary 键**双重漂移**（分隔符+token `varbinary`→`binary_as_varbinary`），通用归一化器修不了。
- **复核（M-crit 未过 3-lens → 动手前先独立复核，已做）**：5-agent scout + 对抗 synthesizer（workflow `wf_a3626c54-0db`）→ **REAL_BUG high-conf**，false-positive steelman 被否（原始 feature PR #57821/#59720、全 regression CREATE CATALOG 皆点分、legacy parity、同 SPI PR 的 JDBC 连接器正确保点分均证点分为 canonical）。
- **修（纯连接器、零 SPI、无 BE）**：`PaimonConnectorProperties` 两常量重指 canonical 点分键（binary 常量并改名 `ENABLE_MAPPING_VARBINARY` 对齐 CatalogProperty/JDBC/iceberg；同修分隔符+token）+ 更新 `PaimonConnectorMetadata` 一处引用；`Options(mapBinaryToVarbinary,mapTimestampTz)` 顺序本就对、无逻辑改。**BE 一致性已核**：`PluginDrivenScanNode extends FileQueryScanNode` 不 override mapping getter → BE scan param 经继承的 `getEnableMappingVarbinary/Tz` 本就读点分（`FileQueryScanNode:192-193,635-678`），修后 FE 列型与 BE scan param 一致（修前两侧分歧）。否决 fe-core 归一化器（blast 大/破 JDBC/对双重漂移不足）。
- **作用域 = paimon-only（用户签 [D-051]）**：⚠️ **新 hive + iceberg 连接器同根因**（读下划线、fe-core 只写点分）——本轮不修，登 [DV-030] 跨连接器 follow-up（iceberg `enable_mapping_varbinary` 仅分隔符差；hive `enable_mapping_binary_as_string` 是**误名非语义反转**，`HmsTypeMapping:90-93` true→VARBINARY，**勿**反转 boolean）。
- **验证**：模块 **234/0/0**（1 CI-gated skip）、checkstyle 0、import-gate 净、**fail-before bug-catcher 向红**（期望 `<VARBINARY>` 实得 `<STRING>`）+ guard 两态绿。e2e `test_paimon_catalog_{varbinary,timestamp_tz}.groovy` **CI-gated（未跑）**（`enablePaimonTest=false` + 外部 fixture；`.out` 期望 `timestamptz(3)`）。设计 [`P5-fix-MAPPING-FLAG-KEYS-design.md`](./tasks/designs/P5-fix-MAPPING-FLAG-KEYS-design.md)、[D-051]、[DV-030]、无 SPI 改（RFC 无须改）。

## 🔜 下一个 session：从 **#6 `FIX-KERBEROS-DOAS`** 起，按 task-list 顺序续修
> ⚠️ **先拿当前代码复核 finding**（review 只读，行号可能漂移；#3/#4/#5 改过 `PaimonScanPlanProvider`/`PaimonConnector`/`PaimonConnectorProperties`/`PaimonConnectorMetadata`，但 #6 主要在 `PaimonConnector`/`PaimonCatalogOps`/`PluginDrivenExternalCatalog`）。
> ⚠️ **两处需用户定 scope**（动手前问）：① **D7=B 读-vs-DDL 不对称**——round-1 D7 故意只在 4 个 DDL op 包 `executeAuthenticated`、read 路径不包；M-11 要求把 partition-listing **read** RPC 也包 `doAs`——**确认是否要把 read 也包**（改变 D7 的故意不对称）。② **M-8 的「DLF」从句 review 自认 overstated**（DLF 继承 no-op authenticator）——**先核实**再决定范围。

**#6 `FIX-KERBEROS-DOAS`（M-8 + M-11，MAJOR，secured HMS/HDFS 部署；SPI=maybe）**：
- **M-8**：Kerberized HDFS 上 filesystem/jdbc flavor 的 fs/jdbc op 丢 `doAs`（`initializeCatalog` 在翻闸路径 dead；HMS flavor 不受影响）。
- **M-11**：MTMV / SHOW PARTITIONS / partitions-TVF 的分区枚举在 Kerberos HMS 上跑 `listPartitions` RPC **不包** `doAs`。
- **连接器**：`PaimonConnector.java:124-196`（M-8）；`PaimonCatalogOps.java:249-251`、`PaimonConnectorMetadata.java:892-894`（M-11）。**fe-core**：`PluginDrivenExternalCatalog.java:122-137,150`；`PluginDrivenMvccExternalTable.java:157`；`PluginDrivenExternalTable.java:317-318`。
- **legacy parity**：`PaimonFileSystemMetaStoreProperties.java:40-57`、`PaimonJdbcMetaStoreProperties.java:111-135`（M-8）；`PaimonExternalCatalog.java:96-118`（`executionAuthenticator.execute` wrap）、`metacache/paimon/PaimonPartitionInfoLoader.java:49`（M-11）。
- **Fix sketch**：把 fs/jdbc HDFS authenticator 接到 live(连接器)create 路径；分区枚举 read RPC 包 `executeAuthenticated`。Scope = secured HMS/HDFS 部署。真值闸=live Kerberos e2e（CI-gated）。

每条遵循项目既定 per-fix 流程（`step-by-step-fix` skill）：1) 设计 doc → `plan-doc/tasks/designs/P5-fix-<ID>-design.md`；2) **先拿当前代码复核 finding**；3) 实现（minimal、surgical、**连接器禁 import fe-core**）；4) build+UT（绝对 `-f`、**`-am`** 必带、读 surefire XML + `MVN_EXIT`、加 fail-before/pass-after UT）；5) **独立 commit**；6) SPI 改动登 `01-spi-extensions-rfc.md`、用户签字入 `decisions-log.md`、偏差入 `deviations-log.md`、同步 task-list。

## 📋 优先级总览（详见 task-list）

| 层 | 条目 | 说明 |
|---|---|---|
| **P0 BLOCKER（挡 commit）** | ✅1.URI-NORMALIZE · ✅2.STATIC-CREDS-BE · ✅3.SCHEMA-EVOLUTION · ✅4.JDBC-DRIVER-URL | **全清** |
| **P1 MAJOR（修或显式接受）** | ✅5.`FIX-MAPPING-FLAG-KEYS`(M-crit) · **⬜6.`FIX-KERBEROS-DOAS`(M-8+M-11)** · 7.`FIX-FORCE-JNI-SCANNER`(M-1) | #5 已修（paimon-only，hive/iceberg 同根因登 [DV-030]）；#6 两处需先定 scope（D7 读-vs-DDL 不对称 + M-8「DLF」从句 overstated）。 |
| **P2 严重度有争议（perf；R1=MINOR）** | 8.`FIX-COUNT-PUSHDOWN`(M-2) · 9.`FIX-NATIVE-SUBSPLIT`(M-3) | 结果正确仅性能/并行。**动手前先找用户定 scope**（accept-or-defer，defer 则登 `deviations-log`）。 |
| **P3 覆盖缺口（去查）** | 复验 `FIX-HMS-CONFRES` · DDL 写路径 parity · ANALYZE/列统计 · split-count 计账 · #4 跨连接器 follow-up（[DV-028]）· **#5 跨连接器 follow-up（hive+iceberg mapping-flag 键，[DV-030]）** | critic 标本轮未追；查出真分歧才转 FIX。 |
| **P4 MINOR/NIT** | 见 review §5 | 一次性 cleanup；唯一真实数据边 = partition null-sentinel（`__HIVE_DEFAULT_PARTITION__`/`\N` 字面值被当 NULL）。 |

---

# 📦 仓库状态
- **HEAD = `9dcf6d1a9e5`**（`fix: FIX-MAPPING-FLAG-KEYS`，本 session #5；checkpoint docs commit 紧随）。该 fix commit = #5 连接器码(main+test)+设计 doc+D-051+DV-030+task-list 进度（7 文件，无 regression-conf/scratch）。
- **本 checkpoint commit 改动**：`plan-doc/HANDOFF.md`（本文件）、`plan-doc/task-list-P5-rereview2-fixes.md`（#5 commit-cell 填 hash）。
- ⚠️ **`regression-test/conf/regression-conf.groovy` 仍 modified-未 commit 且含明文 Aliyun key** —— 任何 commit 前继续 path-whitelist，**严禁 `git add -A`**。`regression-conf.groovy.bak` 同理排除。
- scratch 仍未 commit（`.audit-scratch/` `conf.cmy/` `META-INF/`）。
- 当前分支 `catalog-spi-07-paimon`（非 `master`）→ 在此 commit 修复 OK。
- **legacy `datasource/paimon/*` 仍在树内**（B8 删除未做）→ 每个 fix 都能 side-by-side diff 做 parity。
- 迁移链：…→`667f779af04`(#3)→`2d15b1b7ed7`(#4)→`9dcf6d1a9e5`(#5 MAPPING-FLAG-KEYS, HEAD)。

## ⚠️ Commit 须知（任何 `git add` 前必读）
- **硬前置**：scrub `regression-test/conf/regression-conf.groovy`（明文 Aliyun key）+ 清 scratch（`.audit-scratch/` `conf.cmy/` `META-INF/` `*.bak`）。**path-whitelist `git add`，严禁 `git add -A`。**
- 每个 fix 独立 commit；message = `fix: <ID>` + 根因 + 解法 + 测试，末尾带 `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`。
- 改 fe-core/SPI 的 fix：commit 须含连接器 + SPI + fe-core 三侧 + 测试（#6 大概率含 fe-core authenticator 接线）。

## ⚙️ 操作须知（复用）
- maven 绝对 `-f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl :<art> **-am** -Dmaven.build.cache.enabled=false -DfailIfNoTests=false`；验证读 surefire XML + `MVN_EXIT`（[[doris-build-verify-gotchas]]）。**`-am` 必带**（漏 `-am` 会因 `${revision}` 解析失败报「could not resolve fe-connector-spi」而非真错）。`-pl :fe-connector-paimon -am` **不重编 fe-core**；改 fe-core 须单独 `-pl :fe-core -am`。checkstyle 单独 `checkstyle:check`（`test` 阶段不绑）。
- 连接器禁 import fe-core：`bash tools/check-connector-imports.sh`（仅允许 `org.apache.doris.{thrift,connector,extension,filesystem}`）。
- cwd 跨 Bash 调用持久，`cd` 破相对路径 → 一律绝对路径。
- 测试优先 runnable FE **单测**（连接器 harness：`FakePaimonTable`/`RecordingPaimonCatalogOps`/`RecordingConnectorContext`/`PaimonConnectorMetadataTest`/`PaimonScanPlanProviderTest`）；live-e2e（S3/OSS/REST/JDBC/Kerberos）CI-gated → 注明 gated，勿谎称跑过。

## 🧠 给下一个 agent 的 meta
- **#5 验证的高价值模式（再次奏效）**：critic-surfaced/未过-3-lens 的 finding → **先 scout workflow 独立复核**（5 并行 reader 各 1 角度：git-history canonical-key intent / legacy parity / docs+regression 证据 / failure-manifestation 端到端 / 跨连接器 scope）+ **对抗 synthesizer 先 steelman false-positive 再裁决** → 设计 → 实现 → **fail-before 实测**（临时 neuter 源码值、跑两测、bug-catcher 向红 guard 两态绿）→ 4-lens review 可选。本次 scout 揪出 finding 没说的关键事实：① binary 键**双重漂移**（不只分隔符）② hive/iceberg **同根因**（决定 scope 要问用户）③ BE scan param 经 `FileQueryScanNode` 继承本就读点分（决定「无 BE」）。
- **改 fe-core handle/scan 流前，先 grep 全 `metadata.getTableHandle` / scan-node 调用方**（历史教训：独立 handle 面绕 seam 会静默错行）。
- **#6 两处 scope 决策先问用户**（D7 读-vs-DDL 不对称是否打破 / M-8「DLF」从句先核实）；P2 两条（#8/#9）严重度有争议 → **先找用户定 scope 再动手**。
- **跨连接器 follow-up 累积**：[DV-028]（#4 CREATE-time-only 校验，全 plugin 连接器共有）+ [DV-030]（#5 hive+iceberg mapping-flag 键，同 paimon 根因）—— 若将来批量 close，二者都是「新连接器读法 vs fe-core 既有约定」的同类缝。
