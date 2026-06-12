# 🤝 Session Handoff

> 滚动文档：每次 session 结束**直接覆盖**（不保留历史；历史见 `git log plan-doc/HANDOFF.md`）。
> 协作规范：[AGENT-PLAYBOOK.md](./AGENT-PLAYBOOK.md)

---

# 🎯 下一个 session 的任务 — **P4 cleanup 全完成（2 fix + 15 accept）；进入 B8 legacy 删除 或 跨连接器 follow-up 批量**

第二轮 clean-room 对抗 review report：[`plan-doc/reviews/P5-paimon-rereview2-2026-06-11.md`](./reviews/P5-paimon-rereview2-2026-06-11.md)。
👉 **任务清单：[`plan-doc/task-list-P5-rereview2-fixes.md`](./task-list-P5-rereview2-fixes.md)** —— #1~#9 + P3 + **P4 cleanup 全完成**。**无 P0/P1/P2/P3/P4 阻塞剩余。**

## ✅ 本 session 完成（P4 MINOR/NIT 一次性 cleanup：2 fix + 15 accept）
P4「一次性 cleanup」：read-only 对抗 recon workflow `wf_6884d37b-8ef`（6 并行分类 agent 逐项对**当前**代码复核 + sentinel 专项 deep-dive + 2 对抗 skeptic 逐角度证伪 + completeness critic over 全批）。review §5/§7 去重 ~17 项 MINOR/NIT → 用户签字（[D-057]）：**fix 2 actionable + accept 15**（[DV-035]）。

- ✅ **FIX-VARCHAR-BOUNDARY（N10.1）— `bcee91dcb52`**：`PaimonTypeMapping.toVarcharType` `len>=65533`→STRING vs legacy `PaimonUtil:241` `>65533`（65533=`MAX_VARCHAR_LENGTH` 合法 exact-fit VARCHAR）。纯连接器 1 字符 `>=`→`>`，display-only/零风险 exact-parity。新 `PaimonTypeMappingReadTest` fail-before 恰 65533 红→pass-after，260/0/0、checkstyle 0、import-gate 净。
- ✅ **FIX-PARTITION-NULL-SENTINEL（sentinel，HANDOFF 标的「唯一真实数据边」）— `4b2c2190dc2`**：scan 路 `PaimonScanRange.populateRangeParams` 经 `ConnectorPartitionValues.normalize` 施 Hive-directory 哨兵 coercion（`\N`/`__HIVE_DEFAULT_PARTITION__`→isNull）——对 hudi（path-encoded 分区）对、对 paimon 错（paimon 分区值已 typed：genuine null=Java-null，哨兵从不出现）→ literal `\N`（paimon 不保留此 token）/`__HIVE_DEFAULT_PARTITION__` 字符串分区值在 native ORC/Parquet 读被误成 SQL NULL，diverge legacy `PaimonScanNode:323-326`。**修=纯连接器** scan `isNull=value==null` only（render genuine null=""），不动 shared `ConnectorPartitionValues`（hudi `HudiScanRange:226` 仍需）。commit 前 5-angle 对抗 review SAFE。新 `PaimonScanRangePartitionNullTest` 4-case，261/0/0、checkstyle 0、import-gate 净。
- ⬜ **15 accept（[DV-035]）**：**M5.1**（唯一 FUNCTIONAL，transient-only：sys-table 列举远端 handle 预探瞬时失败→phantom「table not found」；**accept** 因 `getTableHandle` swallow-to-empty 是有意+有测契约 `failAuth→empty` 且共享 existence 谓词含 P3 createTable `remoteExists`，无 surgical 修）；M9.1/M9.2（**假前提**——连接器跑同 `CredentialUtils` 路无 drop）；M10.1/M10.2/M10.3/M7.1（display）；M6.1/M6.2（perf）；N2.1/M3.1/N4.1/C2（text）；N3.1/M2.1（inert no-op）；M4.1/M1.3（连接器**更** correct）；M1.1（diagnostic）。

## 🔜 下一个 session：选其一（无 P0/P1/P2/P3/P4 阻塞剩余）
1. **B8 legacy `datasource/paimon/*` 删除**（迄今每个 fix 都靠它做 side-by-side parity；P3+P4 后 parity 已全核完，可以删了）。删前确认无 live 引用（legacy `PaimonExternalTable`/`PaimonScanNode`/`PaimonUtil`/`PaimonMetadataOps`/`metacache/paimon/*` 等；注意 cutover 后 `instanceof PaimonExternalTable` 站点已 dead，但删类前 grep 全 import + GsonUtils 注册 + `getEngine`/`SPI_READY_TYPES` 成员）。
2. **跨连接器 follow-up 批量**（[DV-028]/[DV-030]/[DV-031]/[DV-032]/[DV-033]/[DV-034]/**[DV-035]**）—— hudi/iceberg full-adopter 同根因缝（mapping-flag 键、createTable-local-conflict 已经 D-056 通用关掉、error-code collapse、display/text parity、sys-table transient 等），将来批量 close。

每条遵循 per-fix 流程（`step-by-step-fix` skill）：设计 doc → 先拿当前代码复核 finding → 实现（连接器禁 import fe-core）→ build+UT（绝对 `-f`、**`-am`** 必带、读 surefire XML + `MVN_EXIT`、fail-before/pass-after）→ 独立 commit → SPI 改登 RFC + 用户签字入 decisions-log + 偏差入 deviations-log + 同步 task-list。

## 📋 优先级总览（详见 task-list）

| 层 | 条目 | 说明 |
|---|---|---|
| **P0 BLOCKER** | ✅1·✅2·✅3·✅4 | **全清** |
| **P1 MAJOR** | ✅5·✅6·✅7 | **全清** |
| **P2 perf-parity** | ✅8.`FIX-COUNT-PUSHDOWN` · ✅9.`FIX-NATIVE-SUBSPLIT` | **全清** |
| **P3 覆盖缺口（去查）** | ✅ HMS-CONFRES · ✅ ANALYZE · ✅ split-count parity · ✅ DDL→`FIX-CREATE-TABLE-LOCAL-CONFLICT` `67a9b9da6e3` | **全完成** |
| **P4 MINOR/NIT** | ✅ `FIX-VARCHAR-BOUNDARY` `bcee91dcb52` · ✅ `FIX-PARTITION-NULL-SENTINEL` `4b2c2190dc2` · ⬜ 15 accept [DV-035] | **全完成**（2 fix + 15 accept；recon `wf_6884d37b-8ef`） |
| **B8 legacy 删除** | ⬜ | parity 已全核完（P3+P4），可删 `datasource/paimon/*`。 |

---

# 📦 仓库状态
- **HEAD = 本 docs 提**（更新 decisions/deviations/task-list/HANDOFF/memory）。其父 = `4b2c2190dc2`（sentinel fix）。
- 迁移链：…→`67a9b9da6e3`(P3-fix)→`bcee91dcb52`(P4 N10.1 VARCHAR-BOUNDARY)→`4b2c2190dc2`(P4 sentinel PARTITION-NULL-SENTINEL)→**本 docs 提(HEAD)**。
- ⚠️ **`regression-test/conf/regression-conf.groovy` 仍 modified-未 commit 且含明文 Aliyun key** —— commit 前继续 path-whitelist，**严禁 `git add -A`**。`regression-conf.groovy.bak` 同理排除。
- scratch 仍未 commit（`.audit-scratch/` `conf.cmy/` `META-INF/`）。
- 当前分支 `catalog-spi-07-paimon`（非 `master`）。
- **legacy `datasource/paimon/*` 仍在树内**（B8 删除未做）→ 仍可 side-by-side diff（但 P3+P4 后已无待核 parity，见上「下一步」选项 1）。

## ⚠️ Commit 须知（任何 `git add` 前必读）
- **硬前置**：scrub `regression-test/conf/regression-conf.groovy`（明文 Aliyun key）+ 清 scratch（`.audit-scratch/` `conf.cmy/` `META-INF/` `*.bak`）。**path-whitelist `git add`，严禁 `git add -A`。**
- 每个 fix 独立 commit；message = `fix: <ID>` + 根因 + 解法 + 测试，末尾带 `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`。

## ⚙️ 操作须知（复用）
- maven 绝对 `-f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl :<art> **-am** -Dmaven.build.cache.enabled=false -DfailIfNoTests=false`；验证读 surefire XML + `MVN_EXIT`（[[doris-build-verify-gotchas]]）。改 fe-core 须单独 `-pl :fe-core -am`。**checkstyle**：连接器 `mvn -pl :fe-connector-paimon checkstyle:check`；fe-core `mvn -pl :fe-core checkstyle:check`。
- 连接器禁 import fe-core：`bash tools/check-connector-imports.sh`（仅允许 `org.apache.doris.{thrift,connector,extension,filesystem}`）。
- cwd 跨 Bash 调用持久，`cd` 破相对路径 → 一律绝对路径。
- 测试优先 runnable FE 单测。连接器测 harness：`RecordingConnectorContext`/`RecordingPaimonCatalogOps`/`FakePaimonTable`/`PaimonScanPlanProviderTest`；P4 新增 `PaimonTypeMappingReadTest`（read-direction 类型映射）/`PaimonScanRangePartitionNullTest`（`populateRangeParams` 分区 isNull）。live-e2e CI-gated → 注明 gated，勿谎称跑过。

## 🧠 给下一个 agent 的 meta
- **P4 recon 模式（对抗 recon 两次见效）**：read-only 分类 workflow（多并行 classifier 逐项对**当前**代码复核 + 专项 deep-dive + 对抗 skeptic 逐角度证伪 + completeness critic）→ 真分歧 / 假前提区分 → AskUserQuestion 定 scope → 转 FIX 或 accept。**(1) sentinel deep-dive 的 ACCEPT 被 prune-路 skeptic 推翻**为真分歧（教训：partition-null parity 必须 scan **和** prune 双路看，`\N` 非 paimon-保留 token）。**(2) M5.1 的「cheap static fallback」(completeness critic) 被实现层核查证伪**——swallow-to-empty 是有意+有测契约 → flip 回 accept（教训：critic 的 fix 建议须落到代码契约/测试层再判 effort，别照单转 FIX）。
- **sentinel fix 关键**：`ConnectorPartitionValues` 是 shared API（paimon+hudi），hudi path-encoded 分区**需要** Hive 哨兵 coercion，故修必须 paimon-local（不动 shared 类）。BE 在 `is_null==true` 时忽略 render string（`partition_column_filler.h:40-44` early-return），故 genuine-null 的 `\N`-vs-`""` render diff **不可观测**——这是 sentinel「genuine-null 无 regression」的根据。
- **M5.1 残留**：若将来要修，唯一干净路 = SPI 加 `listSupportedSysTables(session)` no-handle overload（bridge 不经远端 existence gate 列静态名）——但会重引 legacy「为不存在 base 表列 sys-table」quirk + SPI surface churn，且须 RFC + 用户签字。broad `getTableHandle` retype 破有意 `failAuth→empty` 契约 + 触 P3 createTable fix，已否决。
- **跨连接器 follow-up 累积**：[DV-028]/[DV-030]/[DV-031]/[DV-032]/[DV-033]/[DV-034]/**[DV-035]** —— hudi/iceberg full-adopter 同复发，将来批量 close。
