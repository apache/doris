# 🤝 Session Handoff

> 滚动文档：每次 session 结束**直接覆盖**（不保留历史；历史见 `git log plan-doc/HANDOFF.md`）。
> 协作规范：[AGENT-PLAYBOOK.md](./AGENT-PLAYBOOK.md)

---

# 🎯 下一个 session 的任务 — **第三轮 clean-room 对抗 review（全功能路径，禁注入开发先验）**

P0/P1/P2/P3/P4 全清（见文末「迁移完成态」）。下一步**不是**改代码，而是**重新、独立地审阅整个 paimon connector 的所有功能路径**，从**设计**与**实现交付**两个维度查问题，并**逐路径对照 legacy 旧逻辑**找差异 + 找仍走旧逻辑 / 静默 fallback 到旧逻辑的地方。

## ⛔ 本轮最重要的约束 — **禁止注入开发过程中的先验知识**
本轮目的就是**重新审阅**，用户明确要求**不要让历史记忆限制 review 的公正性与开放性**。因此：

- **find-and-judge 阶段（每个路径的 reviewer 形成独立判断时）：不要预读** `decisions-log.md` / `deviations-log.md` / 前两轮 review report（`reviews/P5-paimon-rereview*.md`）/ memory 里的 `catalog-spi-p5-*` result 文件 / 各 `*-design.md`。让每个 reviewer **只**从「当前 plugin connector 代码」+「legacy `datasource/paimon/*` 代码（仍在树内可 side-by-side）」独立判断。
- **允许**提供的 = 「去哪看」的脚手架（代码包位置、构建/测试命令、import-gate 规则）——这些是**怎么做**不是**结论**。
- **历史结论只在最终 reconciliation 阶段**（每个路径的独立 verdict 已形成**之后**）交叉核对，且**历史不得据此压制 / 推翻一个 finding**——若独立 review 与历史结论冲突，作为**新 finding 上报**让用户裁决（这正是本轮想暴露的东西：被开发先验「合理化」掉的问题）。
- 参考既有偏好 [[clean-room-adversarial-review-pref]]（多 agent 对抗 + 先 code 独立判断、后交叉核对）。

## 🧭 方法（对抗 review）
1. **每个路径独立 reviewer**（建议每路径 ≥1 个「实现 vs legacy 对照」reviewer），输出：设计缺陷 / 实现缺陷 / 行为差异（plugin vs legacy）/ 旧逻辑残留 or fallback。
2. **对抗 verifier 逐 finding 证伪**（默认尝试推翻；refute 不掉才算确认）——避免「短路对≠全分支对」类误判（见过往教训：tracer PARITY 被逐分支证伪推翻）。
3. **completeness critic** 抓漏掉的路径 / 未追的 aspect / 未核的 fallback。
4. **最终 reconciliation**：独立 verdict 形成后，再与历史结论交叉核对，冲突项作为新 finding 上报。
5. 输出 **`plan-doc/reviews/P5-paimon-rereview3-<date>.md`**：逐路径 verdict + 确认 finding 表（severity / plugin-site / legacy-site / 证据）；若有真分歧 → 走 per-fix 流程（`step-by-step-fix` skill）+ AskUserQuestion 定 scope。

## 📋 要审阅的功能路径（用户指定，逐条覆盖 + 各自对照 legacy）
1. **基础读取**（normal scan）
2. **批式增量读取**（`@incr` incremental read）
3. **Time Travel**（snapshot / timestamp / `FOR TIME AS OF` / `FOR VERSION AS OF`）
4. **Branch / Tag 读取**
5. **系统表查询**（`tbl$snapshots` 等 sys-tables）
6. **元数据缓存**（schema / table-handle / partition cache）
7. **Deletion Vector 读取**（MoR）
8. **多元数据服务接入**（filesystem / HMS / DLF / REST / JDBC flavor）
9. **多存储系统接入**（s3 / oss / cos / obs / hdfs；凭据下发 + 路径 scheme 规范化）
10. **Parquet / ORC 数据格式读取**（native reader 路 + schema-evolution / field-id）
11. **列类型映射**（read 方向 paimon→doris + write 方向 doris→paimon）
12. **旧逻辑残留 / fallback 排查**：还存在哪些可能**走旧的逻辑**，或**静默 fallback 到旧逻辑**的地方（例：`instanceof PaimonExternalTable`/`PaimonSysTable`/`PaimonSource` 等 cutover 后应 dead 的分支是否真 dead；FE 分发 switch 是否每处都有 `PLUGIN_EXTERNAL_TABLE` 臂；GsonUtils 注册；`getEngine`/`SPI_READY_TYPES` 成员；任何 `instanceof legacy-type` 或 legacy 静态调用仍可达的路径）。

## 🗺️ 代码脚手架（「去哪看」，非结论）
- **Plugin connector**：`fe/fe-connector/fe-connector-paimon/src/main/java/org/apache/doris/connector/paimon/`（+ `fe-connector-paimon-api` / `-backend-{filesystem,hms,aliyun-dlf,rest}`）；connector-api `fe/fe-connector/fe-connector-api/`；SPI `fe/fe-connector/fe-connector-spi/`。
- **fe-core 桥**：`fe/fe-core/src/main/java/org/apache/doris/datasource/PluginDriven*.java` + `ConnectorColumnConverter.java`。
- **Legacy（side-by-side 对照基准，仍在树内）**：`fe/fe-core/src/main/java/org/apache/doris/datasource/paimon/`（`source/PaimonScanNode.java`、`PaimonExternalTable.java`、`PaimonUtil.java`、`PaimonMetadataOps.java`、`metacache/paimon/*` 等）。
- **BE 消费端**（如需核 thrift 契约）：`be/src/vec/exec/format/table/` + `be/src/format/table/`（paimon_reader / partition_column_filler / table_schema_change_helper）。
- ⚠️ **本轮务必保留 legacy 在树内**（side-by-side 对照基准）→ **B8 legacy 删除推迟到本轮 review 之后**。

---

# 📦 迁移完成态 & 仓库状态
- **HEAD = 本 docs 提**（更新 HANDOFF 为第三轮 review 任务）。迁移链：…→`67a9b9da6e3`(P3-fix)→`bcee91dcb52`(P4 N10.1)→`4b2c2190dc2`(P4 sentinel)→`af2037cf13b`(P4 docs)→**本 docs 提(HEAD)**。
- **进度**（完整见 [task-list](./task-list-P5-rereview2-fixes.md)）：P0 BLOCKER ✅1·2·3·4｜P1 MAJOR ✅5·6·7｜P2 perf-parity ✅8·9｜P3 覆盖缺口 ✅（3 parity + 1 fix `67a9b9da6e3`）｜P4 MINOR/NIT ✅（2 fix `bcee91dcb52`/`4b2c2190dc2` + 15 accept [DV-035]）。**无 P0~P4 阻塞剩余。**
- ⚠️ **`regression-test/conf/regression-conf.groovy` 仍 modified-未 commit 且含明文 Aliyun key** —— commit 前继续 path-whitelist，**严禁 `git add -A`**。`regression-conf.groovy.bak` 同理排除。
- scratch 仍未 commit（`.audit-scratch/` `conf.cmy/` `META-INF/`）。
- 当前分支 `catalog-spi-07-paimon`（非 `master`）。
- **legacy `datasource/paimon/*` 仍在树内**——本轮 review 的对照基准，勿删。

## ⚠️ Commit 须知（任何 `git add` 前必读）
- **硬前置**：scrub `regression-test/conf/regression-conf.groovy`（明文 Aliyun key）+ 清 scratch（`.audit-scratch/` `conf.cmy/` `META-INF/` `*.bak`）。**path-whitelist `git add`，严禁 `git add -A`。**
- 若本轮 review 转出 fix：每个 fix 独立 commit；message = `fix: <ID>` + 根因 + 解法 + 测试，末尾带 `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`。

## ⚙️ 操作须知（复用）
- maven 绝对 `-f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl :<art> **-am** -Dmaven.build.cache.enabled=false -DfailIfNoTests=false`；验证读 surefire XML + `MVN_EXIT`（[[doris-build-verify-gotchas]]）。改 fe-core 须单独 `-pl :fe-core -am`。**checkstyle**：连接器 `mvn -pl :fe-connector-paimon checkstyle:check`；fe-core `mvn -pl :fe-core checkstyle:check`。
- 连接器禁 import fe-core：`bash tools/check-connector-imports.sh`（仅允许 `org.apache.doris.{thrift,connector,extension,filesystem}`）。
- cwd 跨 Bash 调用持久，`cd` 破相对路径 → 一律绝对路径。
- 测试优先 runnable FE 单测。连接器测 harness：`RecordingConnectorContext`/`RecordingPaimonCatalogOps`/`FakePaimonTable`/`PaimonScanPlanProviderTest`/`PaimonTypeMappingReadTest`/`PaimonScanRangePartitionNullTest`。live-e2e CI-gated（S3/OSS/REST/JDBC/Kerberos 外部 fixture，`enablePaimonTest` 默认 false）→ 注明 gated，勿谎称跑过。

## 🧠 给下一个 agent 的 meta（**方法**层，非结论）
- **本轮纪律**：先独立、后核对。每个路径的 verdict 必须**先**从「当前代码 + legacy 对照」得出，**再**(且仅在 reconciliation 阶段)对历史结论。历史与独立判断冲突 → 上报为新 finding，不被历史压制。这是用户对本轮的核心要求。
- **对抗 review 反复奏效的点**：对抗 verifier 逐分支/逐角度证伪（揪「短路对≠全分支对」「deep-dive 只看单路漏另一路」）；completeness critic 抓漏路径与未核 fallback。
- **此前几轮的产物在哪**（**仅 reconciliation 阶段**参考，find-phase 勿读）：`reviews/P5-paimon-rereview2-2026-06-11.md`（第二轮逐路径 + §9 cross-check）、`decisions-log.md`（D-037…D-057）、`deviations-log.md`（DV-001…DV-035）、`task-list-P5-rereview2-fixes.md`、memory `catalog-spi-p5-*`。
