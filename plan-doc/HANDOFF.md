# 🤝 Session Handoff

> 滚动文档：每次 session 结束**直接覆盖**（不保留历史；历史见 `git log plan-doc/HANDOFF.md`）。
> 协作规范：[AGENT-PLAYBOOK.md](./AGENT-PLAYBOOK.md)

---

# 🎯 下一个 session 的任务 — **P3 覆盖缺口核查全完成（3 parity + 1 fix landed）；进入 P4 cleanup 或 B8 legacy 删除**

第二轮 clean-room 对抗 review report：[`plan-doc/reviews/P5-paimon-rereview2-2026-06-11.md`](./reviews/P5-paimon-rereview2-2026-06-11.md)。
👉 **任务清单：[`plan-doc/task-list-P5-rereview2-fixes.md`](./task-list-P5-rereview2-fixes.md)** —— #1~#9 全完成 + **P3 全核查完成 + P3-fix landed**。

## ✅ 本 session 完成（P3 覆盖缺口核查 + 1 个 fix）
P3「**去查不是去改**」：4 项 plugin-vs-legacy paimon parity，对抗 audit workflow `wf_25450c36-b7a`（tracer → 对抗 verifier → completeness critic）。**3/4 PARITY_HOLDS，1 揪出真分歧 → 用户签字转 FIX 并已落**。

- ✅ **VERIFY FIX-HMS-CONFRES — PARITY_HOLDS**：key 拼写恰 `hive.conf.resources`（**无** `hive.config.resources` 别名——疑似 MAPPING-FLAG-KEYS 类 bug **证伪**）；round-1 wiring 全在；**BE-downflow**（round-2 没测的部分）两侧同——legacy HMS hive-site.xml 本就不入 BE scan props。
- ✅ **TRACE ANALYZE/列统计 — PARITY_HOLDS**：`getColumnStatistic` 两侧 `Optional.empty()`、`createAnalysisTask` byte-同、`ExternalAnalysisTask` engine-agnostic；空 fallback generic 于桥、与 legacy paimon 共享 → 非 regression。
- ✅ **CHECK split-count 计账 — PARITY_HOLDS**：post-sub-split 数经共享父 `FileQueryScanNode.selectedSplitNum` 喂 `SqlBlockRuleMgr` 两侧同；2 项 divergence 均 cosmetic/NIT 且 **pre-date #9**（EXPLAIN inputSplitNum 行缺 + compress-suffix guard 缺-不可触发）。
- 🔴→✅ **DDL 写 parity — 1 REAL_DIVERGENCE(MAJOR) → FIXED**：见下 P3-fix。其余 6 aspect parity/NIT（branch/tag 两侧都拒、editlog↔cache 序反转=NIT、error-code collapse=cosmetic [DV-034]）。

### P3-fix `FIX-CREATE-TABLE-LOCAL-CONFLICT` — commit `67a9b9da6e3`（[D-056]/[DV-034]，纯 fe-core 桥，零 SPI/连接器/BE）
- **根因**：通用桥 `PluginDrivenExternalCatalog.createTable:293-309` 把 legacy `PaimonMetadataOps.performCreateTable:182-214` 的有序双探（先 remote `tableExist:190` 后 local `getTableNullable:206`，任一命中+`!IF NOT EXISTS`→`ERR_TABLE_EXISTS_ERROR` 1050）合并成单 `exists` OR，且只被 IF NOT EXISTS 臂消费 → `!IF NOT EXISTS` 臂忽略它无条件调 `metadata.createTable`。后果：**local-cache 命中但 remote 缺**（`lower_case_meta_names` 下 case-variant 折叠到既有本地表、case-sensitive remote 无）+`!IF NOT EXISTS` 时 legacy 报 1050 拒绝、plugin **静默在 remote 建重复表**（元数据腐败）。窄+backend-dependent（filesystem/jdbc 才中、HMS 小写化两侧都拒）但 silent correctness。通用桥 → MaxCompute/未来 iceberg/hudi 同受益。
- **修=1 fe-core 文件**：单 OR 拆回 `remoteExists`/`localExists` 两臂，`!IF NOT EXISTS`+`localExists`→`ErrorReport.reportDdlException(ERR_TABLE_EXISTS_ERROR,name)`（legacy local 臂逐字）；remote-only 仍 fall-through 连接器抛（**case-A 不动**、既有 intentional 测绿）。Option-2 外科最小修；**否决 Option 1 full-parity**（改非分歧 case-A+破既有测+越界）。残留 case-A/全 DDL-op error-code-generic = [DV-034] 留 P4 cleanup。
- **守门**：fe-core `PluginDrivenExternalCatalogDdlRoutingTest` **fail-before 恰 1 新测红**（"Expected DdlException…nothing was thrown"）→ **pass-after 26/0/0**、checkstyle 0。真值闸=live-e2e（`lower_case_meta_names=1`+case-variant CREATE 无 IF NOT EXISTS 于 case-sensitive paimon catalog；既有 legacy paimon DDL regression 覆盖契约、本 fix 无 BE 改）。

## 🔜 下一个 session：选其一（无 P0/P1/P2/P3 阻塞剩余）
1. **P4 MINOR/NIT 一次性 cleanup**（review §5）：多为 display-only（DESC Key/Extra/uniqueId、VARCHAR(65533)→STRING、EXPLAIN delete-split 计账、error-message 文本含 [DV-034] 的 error-code collapse）/ perf-architectural / benign。**唯一真实数据边 = partition null-sentinel**（`__HIVE_DEFAULT_PARTITION__`/`\N` 字面值被当 NULL，`PaimonScanRange.java:212-225` / `ConnectorPartitionValues.java:32-54` vs legacy `source/PaimonScanNode.java:323-326`）——值得单独定夺。
2. **B8 legacy `datasource/paimon/*` 删除**（迄今每个 fix 都靠它做 side-by-side parity；P3 后 parity 已全核完，可以删了）。删前确认无 live 引用。
3. **跨连接器 follow-up 批量**（[DV-028]/[DV-030]/[DV-031]/[DV-032]/[DV-033]/[DV-034]）—— hudi/iceberg full-adopter 同根因缝，将来批量 close（D-056 已替所有 plugin 连接器关掉 createTable-local-conflict 这一缝）。

每条遵循 per-fix 流程（`step-by-step-fix` skill）：设计 doc → 先拿当前代码复核 finding → 实现（连接器禁 import fe-core）→ build+UT（绝对 `-f`、**`-am`** 必带、读 surefire XML + `MVN_EXIT`、fail-before/pass-after）→ 独立 commit → SPI 改登 RFC + 用户签字入 decisions-log + 偏差入 deviations-log + 同步 task-list。

## 📋 优先级总览（详见 task-list）

| 层 | 条目 | 说明 |
|---|---|---|
| **P0 BLOCKER** | ✅1·✅2·✅3·✅4 | **全清** |
| **P1 MAJOR** | ✅5·✅6·✅7 | **全清** |
| **P2 perf-parity** | ✅8.`FIX-COUNT-PUSHDOWN` · ✅9.`FIX-NATIVE-SUBSPLIT` | **全清** |
| **P3 覆盖缺口（去查）** | ✅ HMS-CONFRES parity · ✅ ANALYZE parity · ✅ split-count parity · ✅ DDL → `FIX-CREATE-TABLE-LOCAL-CONFLICT` landed `67a9b9da6e3` | **全完成**（3 parity + 1 fix；对抗 audit `wf_25450c36-b7a`） |
| **P4 MINOR/NIT** | ⬜ 见 review §5 | 一次性 cleanup；partition null-sentinel 是唯一真实数据边；error-code collapse [DV-034] 含其中。 |
| **B8 legacy 删除** | ⬜ | parity 已全核完，可删 `datasource/paimon/*`。 |

---

# 📦 仓库状态
- **HEAD = `67a9b9da6e3`**（`fix: FIX-CREATE-TABLE-LOCAL-CONFLICT`，P3-derived）。其父 = `2f5f467f53d`（#9 NATIVE-SUBSPLIT）。本 HANDOFF 更新单独 `docs:` 提（见下条 commit）。
- ⚠️ **`regression-test/conf/regression-conf.groovy` 仍 modified-未 commit 且含明文 Aliyun key** —— commit 前继续 path-whitelist，**严禁 `git add -A`**。`regression-conf.groovy.bak` 同理排除。
- scratch 仍未 commit（`.audit-scratch/` `conf.cmy/` `META-INF/`）。
- 当前分支 `catalog-spi-07-paimon`（非 `master`）。
- **legacy `datasource/paimon/*` 仍在树内**（B8 删除未做）→ 仍可 side-by-side diff 做 parity（但 P3 后已无待核 parity，见上「下一步」选项 2）。
- 迁移链：…→`2f5f467f53d`(#9 NATIVE-SUBSPLIT)→`67a9b9da6e3`(P3-fix CREATE-TABLE-LOCAL-CONFLICT, HEAD)。

## ⚠️ Commit 须知（任何 `git add` 前必读）
- **硬前置**：scrub `regression-test/conf/regression-conf.groovy`（明文 Aliyun key）+ 清 scratch（`.audit-scratch/` `conf.cmy/` `META-INF/` `*.bak`）。**path-whitelist `git add`，严禁 `git add -A`。**
- 每个 fix 独立 commit；message = `fix: <ID>` + 根因 + 解法 + 测试，末尾带 `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`。
- 改 fe-core/SPI 的 fix：commit 须含相关两侧 + 测试。本 P3-fix 纯 fe-core 桥单侧（含 test）。

## ⚙️ 操作须知（复用）
- maven 绝对 `-f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl :<art> **-am** -Dmaven.build.cache.enabled=false -DfailIfNoTests=false`；验证读 surefire XML + `MVN_EXIT`（[[doris-build-verify-gotchas]]）。改 fe-core 须单独 `-pl :fe-core -am`。**checkstyle**：连接器 `mvn -pl :fe-connector-paimon checkstyle:check`；fe-core `mvn -pl :fe-core checkstyle:check`（exit 0 即净）。
- 连接器禁 import fe-core：`bash tools/check-connector-imports.sh`（仅允许 `org.apache.doris.{thrift,connector,extension,filesystem}`）。**本 P3-fix 改的是 fe-core 桥，不涉 import-gate。**
- cwd 跨 Bash 调用持久，`cd` 破相对路径 → 一律绝对路径。
- 测试优先 runnable FE 单测。fe-core 桥测 harness：`PluginDrivenExternalCatalogDdlRoutingTest`（Mockito 构造 `ExternalDatabase`/`metadata`/`session`、`MockedStatic<CreateTableInfoToConnectorRequestConverter>`、`mockEditLog`）；连接器测 harness：`RecordingConnectorContext`/`RecordingPaimonCatalogOps`/`FakePaimonTable`/`PaimonScanPlanProviderTest`。live-e2e CI-gated → 注明 gated，勿谎称跑过。

## 🧠 给下一个 agent 的 meta
- **P3 验证模式（再次奏效）**：finding → **多-item 对抗 audit workflow**（tracer 独立判断 → 对抗 verifier 逐条**证伪 parity 声明**→ completeness critic 抓未追 aspect）→ 真分歧 AskUserQuestion 定 scope → 转 FIX 走 per-fix 流程。**本轮对抗 verifier 把 tracer 的 createTable PARITY 推翻为 DIVERGENCE**（tracer 只看到 IF-NOT-EXISTS 短路对、没看到 !IF-NOT-EXISTS 臂丢 local-arm）——**教训：parity 核查里「短路对」≠「全分支对」，对抗 verifier 价值在逐分支证伪**。
- **P3-fix 关键定夺**：通用桥 bug（非 paimon-specific），Option-2 外科最小修（仅补 local-conflict 闸、不动 case-A remote-hit 路），保既有 intentional 测绿；否决 Option-1 full-parity（越 finding 界）。
- **completeness critic 的残留**（均评估为 benign，未翻 verdict，可按需深挖）：① HMS-CONFRES 的 DLF BE-downflow 由 HMS 路类推非独立 trace（两侧对称 drop，低危）；② DDL editlog/cache replay 收敛只展示了 plugin metadataOps==null 臂、legacy metadataOps!=null 臂未展示（同步 DDL 内 benign）；③ split-count 的 `SqlBlockRuleMgr` 实际消费 call-site 未引（共享父代码、构造即同）。
- **跨连接器 follow-up 累积**：[DV-028]/[DV-030]/[DV-031]/[DV-032]/[DV-033]/**[DV-034]** —— hudi/iceberg full-adopter 同复发，将来批量 close。
