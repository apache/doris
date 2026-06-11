# 🤝 Session Handoff

> 滚动文档：每次 session 结束**直接覆盖**（不保留历史；历史见 `git log plan-doc/HANDOFF.md`）。
> 协作规范：[AGENT-PLAYBOOK.md](./AGENT-PLAYBOOK.md)

---

# 🎯 下一个 session 的任务 — **逐一修复 paimon connector 第二轮 review 的问题（#1+#2 已完成 → 从 #3 起）**

第二轮 clean-room 对抗 review 已完成（report：[`plan-doc/reviews/P5-paimon-rereview2-2026-06-11.md`](./reviews/P5-paimon-rereview2-2026-06-11.md)，含 §9 与第一轮的交叉核对）。结论：**NOT commit-ready** —— 4 个 confirmed BLOCKER 族 + 6 个 confirmed MAJOR。问题**按优先级排成任务列表**：

👉 **任务清单（按优先级）：[`plan-doc/task-list-P5-rereview2-fixes.md`](./task-list-P5-rereview2-fixes.md)** —— 逐条含 finding 引用、连接器 `file:line`、legacy parity 锚、fix sketch、SPI 影响、测法。

## ✅ 已完成：#1 `FIX-URI-NORMALIZE`（B-7DF+B-7DV）`20b19d19dd8` · #2 `FIX-STATIC-CREDS-BE`（B-9）`d23d5df9914`
**#1**（native 数据文件 + DV 路径未归一化 oss/cos/obs/s3a→s3）：新 SPI `ConnectorContext.normalizeStorageUri`（恒等 default）；`DefaultConnectorContext` 经引擎 2-arg `LocationPath.of` + catalog 静态 storage map（lazy supplier + 4-arg ctor，`PluginDrivenExternalCatalog` 接线）；连接器在 `buildNativeRange` 对数据文件+DV 双路调 `normalizeUri`。设计 [`P5-fix-URI-NORMALIZE-design.md`]、RFC §21、[DV-025]。

**#2（本 session）**`FIX-STATIC-CREDS-BE`（BLOCKER B-9）—— commit `d23d5df9914`
- 静态 catalog 凭据（`s3.`/`oss.`/`cos.`/`obs.`…）裸拷进 `location.<rawkey>`、bridge 只剥前缀 → BE native(FILE_S3) reader 只认 `AWS_*` → 私有桶 native 读 403。凭据**第三道缝**（static→BE-scan，review §9.3，两轮均漏；FileIO 缝=FIX-STORAGE-CREDS、vended 缝=FIX-REST-VENDED 已修）。
- **修法（D-048 用户签字 full legacy-parity）**：新 SPI `ConnectorContext.getBackendStorageProperties()`（空 default）= 引擎复用 **#1 已接线的** `storagePropertiesSupplier` 跑 `CredentialUtils.getBackendPropertiesFromStorageMap`（无 ctor 改、`CredentialUtils` 已 import）；连接器**整段**替换裸前缀拷贝循环为该 overlay（vended overlay 仍后置、collision 胜）。object-store→`AWS_*`；HDFS→canonical（顺修 §211 MINOR）；丢非-parity `hive.*`。
- **ANONYMOUS-leak 边角经 BE 证伪无回归**（`s3_util.cpp` v1:383/v2:448 显式 ak/sk 优先于 `cred_provider_type`）。
- **验证**：fe-core `DefaultConnectorContextBackendStoragePropsTest` 2/0/0（+normalize 4/0/0、vend 2/0/0 未坏）、paimon 模块 **217/0/0**、checkstyle 0、import-gate 净、red-check 反转 2/3 向红。live 私有桶 native e2e CI-gated（未跑）。设计 [`P5-fix-STATIC-CREDS-BE-design.md`](./tasks/designs/P5-fix-STATIC-CREDS-BE-design.md)、RFC §22(E14)、[D-048](./decisions-log.md)。

## 🔜 下一个 session：从 **#3 `FIX-SCHEMA-EVOLUTION`** 起，按 task-list 顺序续修
> ⚠️ #3（B-1a+M-10，BLOCKER）= **P0 中 SPI surface 最大 + 失败模式最危险（静默错行）**，但触发更窄（schema-evolved + native + rename）。需 thread paimon `DataField.id()` 过 SPI `ConnectorColumn`（含 nested ARRAY/MAP/ROW）→ `Column.setUniqueId`，并经 bridge 发 `current_schema_id` + per-split `history_schema_info`（`ExternalUtil.initSchemaInfo`）。BE 契约冻结于 `table_schema_change_helper.h:219-267`。**独立于 #1/#2**（不复用 BE-scan-prop 归一化缝）→ 值得**新 session 起、fresh context**。
> ⚠️ 「BE-bound scan-prop 经 `ConnectorContext` 归一化」缝已由 #1/#2 建好两法（`normalizeStorageUri` URI / `getBackendStorageProperties` 凭据）—— 后续若有同类 BE-prop gap 可复用此模式。
> ⚠️ P2 两条（#8 count-pushdown / #9 sub-split）严重度有争议（R1=MINOR/R2=MAJOR，均结果正确仅性能）—— **动手前先找用户定 scope**（accept-or-defer），别默认全做。

每条遵循项目既定 per-fix 流程（与 `step-by-step-fix` skill 一致）：
1. 写设计 doc → `plan-doc/tasks/designs/P5-fix-<ID>-design.md`（Problem / Root Cause / Design / Impl Plan / Risk / Test Plan）。
2. **先拿当前代码复核 finding**（review 只读，行号可能漂移）。
3. 实现（minimal、surgical、match style；**连接器禁 import fe-core**）。
4. build + UT（绝对 `-f`、读 surefire XML + `MVN_EXIT`；加 fail-before/pass-after UT）。
5. **每个 fix 独立 commit**（先看下方 Commit 须知）→ 可选 `plan-doc/reviews/P5-fix-<ID>-review-rounds.md`。
6. SPI 改动登记 `01-spi-extensions-rfc.md`；用户签字决策入 `decisions-log.md`；接受的偏差入 `deviations-log.md`；同步更新 task-list 进度表。

## 📋 优先级总览（详见 task-list）

| 层 | 条目 | 说明 |
|---|---|---|
| **P0 BLOCKER（挡 commit）** | 1.`FIX-URI-NORMALIZE`(B-7DF/DV) · 2.`FIX-STATIC-CREDS-BE`(B-9) · 3.`FIX-SCHEMA-EVOLUTION`(B-1a+M-10) · 4.`FIX-JDBC-DRIVER-URL`(B-8a/b) | #1+#2 面最广（OSS/COS/OBS/私有 S3 上**所有** native 读直接挂）且共用「BE-bound scan-prop 归一化」缝（复用 `FIX-REST-VENDED` 的 `ConnectorContext` 模式）；#3 失败模式最危险（**静默错行**）但触发更窄+SPI surface 最大、**若把静默损坏排第一可先做 #3**（独立于 #1/#2）；#4 仅 JDBC flavor。 |
| **P1 MAJOR（修或显式接受）** | 5.`FIX-MAPPING-FLAG-KEYS`(M-crit) · 6.`FIX-KERBEROS-DOAS`(M-8+M-11) · 7.`FIX-FORCE-JNI-SCANNER`(M-1) | M-crit 是 critic-surfaced、**未过 3-lens**→先复核；M-8/M-11 同属 UGI `doAs` 缺失（grouped）。 |
| **P2 严重度有争议（perf；R1=MINOR）** | 8.`FIX-COUNT-PUSHDOWN`(M-2) · 9.`FIX-NATIVE-SUBSPLIT`(M-3) | 结果正确、仅性能/并行。**用户定 scope**：建议 accept-or-defer（defer 则登 `deviations-log`）。 |
| **P3 覆盖缺口（去查、非确认 bug）** | 复验 `FIX-HMS-CONFRES` 是否真生效 · DDL 写路径 parity · ANALYZE/列统计 · split-count 计账 | critic 标注本轮未追/未复验；查出真分歧才转 FIX 任务。 |
| **P4 MINOR/NIT** | 见 review §5 | 一次性 cleanup pass；唯一有真实（罕见）数据边的是 partition null-sentinel（`__HIVE_DEFAULT_PARTITION__`/`\N` 字面值被当 NULL）。 |

> **交叉核对要点（review §9）**：上一轮 8 个 fix 对**本轮复测到的**全部生效；但 (a) 上一轮 2 个 PARTIAL（DV/数据文件归一化、JDBC driver_url）从未修、本轮升级为 BLOCKER；(b) 凭据有**三道缝**，catalog-FileIO 与 vended 已修，**static→BE-scan 缝（B-9）漏修**；(c) native schema-evolution（B-1a）上一轮误判 MINOR、本轮经 BE 追踪确认 BLOCKER。无任何上一轮 CONFIRMED 被本轮推翻。

---

# 📦 仓库状态
- **HEAD = `d23d5df9914`**（`fix: FIX-STATIC-CREDS-BE`，本 session #2 修复；其父 `20b19d19dd8` = #1 `FIX-URI-NORMALIZE`）。该 commit 含 #2 代码+测试+设计 doc+SPI RFC §22(E14)+D-048+task-list 进度（9 文件，无 regression-conf/scratch）。本 session 剩余改动（**未 commit**）：`plan-doc/HANDOFF.md`（本文件）、`plan-doc/task-list-P5-rereview2-fixes.md`（#2 commit-cell 填 hash 的后续微调）；scratch 仍未 commit（`.audit-scratch/` `conf.cmy/` `META-INF/` `*.bak`）。
- ⚠️ **`regression-test/conf/regression-conf.groovy` 仍 modified-未 commit 且含明文 Aliyun key** —— 任何 commit 前继续 path-whitelist，严禁 `git add -A`。
- 当前分支 `catalog-spi-07-paimon`（非 `master`）→ 在此 commit 修复 OK。
- **legacy `datasource/paimon/*` 仍在树内**（B8 删除未做）→ 每个 fix 都能 side-by-side diff 做 parity。
- 迁移链：`512a67ee3ac`(B0)→`807308993fb`(B1)→`a2b765677d1`(B2/B3)→`ae5ad30b938`(B4)→`d2a2c8d761a`(B5/B6)→`98a73bf7692`(B7+fixes)→`20b19d19dd8`(rereview2 #1 URI-NORMALIZE)→`d23d5df9914`(rereview2 #2 STATIC-CREDS-BE, HEAD)。

## ⚠️ Commit 须知（任何 `git add` 前必读）
- **硬前置**：scrub `regression-test/conf/regression-conf.groovy`（明文 Aliyun key）+ 清 scratch（`.audit-scratch/` `conf.cmy/` `META-INF/` `*.bak`）。**path-whitelist `git add`，严禁 `git add -A`。**
- 每个 fix 独立 commit；message = `fix: <ID>` + 根因 + 解法 + 测试，末尾带项目 Co-Authored-By trailer。
- 改 fe-core/SPI 的 fix（#1/#2/#3，可能 #4/#6）：commit 须含连接器 + SPI + fe-core 三侧 + 测试，按 path-whitelist 加。

## ⚙️ 操作须知（复用）
- maven 绝对 `-f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl :<art> -am -Dmaven.build.cache.enabled=false -DfailIfNoTests=false`；验证读 surefire XML + `MVN_EXIT`（[[doris-build-verify-gotchas]]）。`-pl :fe-connector-paimon -am` **不重编 fe-core**；改 fe-core 须单独 `-pl :fe-core -am`。
- 连接器禁 import fe-core：`bash tools/check-connector-imports.sh`（决定 task-list「SPI?」列：B1/B3/B2 因不能 import `LocationPath`/`StorageProperties` 须走 fe-core 桥或新 `ConnectorContext` SPI 缝）。
- cwd 跨 Bash 调用持久，`cd` 破相对路径 → 一律绝对路径。
- 测试优先 runnable FE **单测**（连接器 harness：`FakePaimonTable`/`RecordingPaimonCatalogOps`/`RecordingConnectorContext`/`PaimonScanPlanProviderTest`）；live-e2e（S3/OSS/REST/JDBC/Kerberos）CI-gated → 注明 gated，勿谎称跑过。

## 🧠 给下一个 agent 的 meta
- 改 fe-core handle/scan 流前，先 grep 全 `metadata.getTableHandle` / scan-node 调用方（历史教训：独立 handle 面绕 seam 会静默错行）。
- P2 两条（count-pushdown、sub-split）严重度有争议（R1 判 MINOR、R2 判 MAJOR，均「结果正确仅性能」）—— **先找用户定 scope 再动手**，别默认按 MAJOR 全做。
- M-crit（mapping-flag）未过 3-lens 对抗验证 → 实现前先独立复核 dotted-vs-underscore key 事实成立再修。
