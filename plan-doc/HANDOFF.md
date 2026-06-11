# 🤝 Session Handoff

> 滚动文档：每次 session 结束**直接覆盖**（不保留历史；历史见 `git log plan-doc/HANDOFF.md`）。
> 协作规范：[AGENT-PLAYBOOK.md](./AGENT-PLAYBOOK.md)

---

# 🎯 下一个 session 的任务 — **逐一修复 paimon connector 第二轮 review 的问题（#1~#4 已完成 → 从 #5 起）**

第二轮 clean-room 对抗 review report：[`plan-doc/reviews/P5-paimon-rereview2-2026-06-11.md`](./reviews/P5-paimon-rereview2-2026-06-11.md)。
👉 **任务清单（按优先级）：[`plan-doc/task-list-P5-rereview2-fixes.md`](./task-list-P5-rereview2-fixes.md)** —— 逐条含 finding 引用、连接器 `file:line`、legacy parity 锚、fix sketch、SPI 影响、测法。

## ✅ 已完成（P0 BLOCKER 全清）
- **#1 `FIX-URI-NORMALIZE`**（B-7DF/DV）`20b19d19dd8` —— native 数据文件 + DV 路径 scheme 归一化。新 SPI `ConnectorContext.normalizeStorageUri`。
- **#2 `FIX-STATIC-CREDS-BE`**（B-9）`d23d5df9914` —— 静态 object-store 凭据→BE canonical `AWS_*`。新 SPI `ConnectorContext.getBackendStorageProperties`。
- **#3 `FIX-SCHEMA-EVOLUTION`**（B-1a；M-10 deferred）`667f779af04` —— 连接器直建 thrift schema 字典（Design C，零新 SPI）。
- **#4 `FIX-JDBC-DRIVER-URL`**（B-8a + B-8b）`2d15b1b7ed7`（本 session）—— 见下。

### #4 摘要（本 session）`FIX-JDBC-DRIVER-URL` —— commit `2d15b1b7ed7`
- **根因**：JDBC flavor 连接器（B-8a 功能 BLOCKER）`PaimonScanPlanProvider.getBackendPaimonOptions` 把 `driver_url` **裸**转发 BE 且 `startsWith("jdbc.")` filter 丢 `paimon.jdbc.*` 别名 → BE `JdbcDriverUtils.registerDriver` 的 `new URL("mysql.jar")` 抛 `MalformedURLException`；（B-8b 安全）driver_url 无 format/whitelist/secure-path 校验 + stale「paimon 不在 SPI_READY_TYPES」注释（B7 后已假）。
- **修（纯连接器、零新 SPI，复用既有 hook）**：B-8a = `getBackendPaimonOptions` 用 `firstNonBlank(JDBC_DRIVER_URL)` 认两别名 + 抽出共享 `PaimonCatalogFactory.resolveDriverUrl(driverUrl, env)`（FE 注册与 BE 选项同解析）发 canonical `jdbc.driver_url`(resolved)+`jdbc.driver_class`（镜像 legacy `PaimonJdbcMetaStoreProperties.getBackendPaimonOptions`）。B-8b = override `PaimonConnector.preCreateValidation` 对 jdbc flavor 调既有 `ConnectorValidationContext.validateAndResolveDriverPath`（镜像 `JdbcDorisConnector`）+ 删 stale 注释。
- **scout(5-agent)+4-lens clean-room review**：B-8a + CREATE-time B-8b 确认正确；review 揪出**校验仅 CREATE-time**（FE-restart reload / ALTER CATALOG / scan-time 不复校）= pre-existing fe-core 缝、全 plugin 连接器共有（含 JDBC 参考连接器）、默认配置 permissive 无可绕。**用户签 [D-050] 接受 CREATE-time parity**（vs 扩 fe-core+SPI）→ 登 [DV-028]（gap+跨连接器 follow-up）+ [DV-029]（简化 resolver + BE-side `paimon.jdbc.{user,password,uri}` 别名 out-of-scope，因 BE 从 `serialized_table` 反序列化整表、只 `registerDriverIfNeeded` 读 driver_url/class）。
- **验证**：模块 **232/0/0**（1 CI-gated skip）、checkstyle 0、import-gate 净、**fail-before 5/9 新测向红**。e2e `test_paimon_jdbc_catalog` **CI-gated（未跑）**。设计 [`P5-fix-JDBC-DRIVER-URL-design.md`](./tasks/designs/P5-fix-JDBC-DRIVER-URL-design.md)、RFC §24（无新 SPI）、[D-050]、[DV-028]/[DV-029]。

## 🔜 下一个 session：从 **#5 `FIX-MAPPING-FLAG-KEYS`** 起，按 task-list 顺序续修（已进入 P1 MAJOR）
> ⚠️ **M-crit 是 critic-surfaced、未过 3-lens** → **动手前先独立复核 dotted-vs-underscore key 事实**（grep `enable_mapping`、`enable.mapping`、`setDefaultPropsIfMissing`）。⚠️ **行号可能已漂移**（#3/#4 改过 `PaimonScanPlanProvider`/`PaimonConnector`，但 #5 主要在 `PaimonConnectorProperties`/`PaimonConnectorMetadata`/`PaimonTypeMapping`）—— **先拿当前代码复核 finding**。

**#5 `FIX-MAPPING-FLAG-KEYS`（M-crit，MAJOR，纯连接器/FE-wiring，无 BE）**：
- **现象**：连接器读**下划线**键 `enable_mapping_binary_as_varbinary` / `enable_mapping_timestamp_tz`；但 FE/legacy 设的是**点分**键 `enable.mapping.varbinary` / `enable.mapping.timestamp_tz` → flag 永 false → 即便用户开启 mapping，BINARY 仍→STRING、LTZ 仍→DATETIMEV2（类型映射静默失效）。
- **连接器**：`PaimonConnectorProperties.java:39,42`；读 `PaimonConnectorMetadata.java:1017-1027`；消费 `PaimonTypeMapping.java:130-165`。**legacy**：`CatalogProperty.java:50,52`；`ExternalCatalog.setDefaultPropsIfMissing:302-306`；`PaimonUtil.paimonPrimitiveTypeToDorisType:253,257,283-286`。
- **Fix sketch**：读 FE 实际设的**点分**键（并核对被重命名的 `varbinary` 键），或在 `PluginDrivenExternalCatalog.createConnectorFromProperties` 构造连接器前把 dots→underscores 归一化。注意核对 `enable.mapping.varbinary`(legacy) vs `enable_mapping_binary_as_varbinary`(连接器) 的**键名本身**也不一致（不只是分隔符）。
- **SPI?=no**（纯连接器/FE-wiring）。**测**：UT 用 `{"enable.mapping.timestamp_tz":"true"}` 构造连接器 → 断言 LTZ 列映射到 TIMESTAMPTZ（闭合 critic coverage-gap #2）；同理 binary→varbinary。

每条遵循项目既定 per-fix 流程（`step-by-step-fix` skill）：1) 设计 doc → `plan-doc/tasks/designs/P5-fix-<ID>-design.md`；2) **先拿当前代码复核 finding**；3) 实现（minimal、surgical、**连接器禁 import fe-core**）；4) build+UT（绝对 `-f`、**`-am`** 必带、读 surefire XML + `MVN_EXIT`、加 fail-before/pass-after UT）；5) **独立 commit**；6) SPI 改动登 `01-spi-extensions-rfc.md`、用户签字入 `decisions-log.md`、偏差入 `deviations-log.md`、同步 task-list。

## 📋 优先级总览（详见 task-list）

| 层 | 条目 | 说明 |
|---|---|---|
| **P0 BLOCKER（挡 commit）** | ✅1.URI-NORMALIZE · ✅2.STATIC-CREDS-BE · ✅3.SCHEMA-EVOLUTION · ✅4.JDBC-DRIVER-URL | **全清** |
| **P1 MAJOR（修或显式接受）** | **⬜5.`FIX-MAPPING-FLAG-KEYS`(M-crit)** · 6.`FIX-KERBEROS-DOAS`(M-8+M-11) · 7.`FIX-FORCE-JNI-SCANNER`(M-1) | #5 critic-surfaced、**未过 3-lens→先复核** dotted-vs-underscore key 事实；M-8/M-11 同属 UGI `doAs` 缺失。 |
| **P2 严重度有争议（perf；R1=MINOR）** | 8.`FIX-COUNT-PUSHDOWN`(M-2) · 9.`FIX-NATIVE-SUBSPLIT`(M-3) | 结果正确仅性能/并行。**动手前先找用户定 scope**（accept-or-defer，defer 则登 `deviations-log`）。 |
| **P3 覆盖缺口（去查）** | 复验 `FIX-HMS-CONFRES` · DDL 写路径 parity · ANALYZE/列统计 · split-count 计账 · **#4 跨连接器 follow-up（CREATE-time-only 校验，见 [DV-028]）** | critic 标本轮未追；查出真分歧才转 FIX。 |
| **P4 MINOR/NIT** | 见 review §5 | 一次性 cleanup；唯一真实数据边 = partition null-sentinel（`__HIVE_DEFAULT_PARTITION__`/`\N` 字面值被当 NULL）。 |

---

# 📦 仓库状态
- **HEAD = `2d15b1b7ed7`**（`fix: FIX-JDBC-DRIVER-URL`，本 session #4；checkpoint docs commit 紧随）。该 fix commit = #4 连接器码(main+test)+设计 doc+D-050+DV-028/029+RFC §24+task-list 进度（10 文件，无 regression-conf/scratch）。
- **本 checkpoint commit 改动**：`plan-doc/HANDOFF.md`（本文件）、`plan-doc/task-list-P5-rereview2-fixes.md`（#4 commit-cell 填 hash）。
- ⚠️ **`regression-test/conf/regression-conf.groovy` 仍 modified-未 commit 且含明文 Aliyun key** —— 任何 commit 前继续 path-whitelist，**严禁 `git add -A`**。`regression-conf.groovy.bak` 同理排除。
- scratch 仍未 commit（`.audit-scratch/` `conf.cmy/` `META-INF/`）。
- 当前分支 `catalog-spi-07-paimon`（非 `master`）→ 在此 commit 修复 OK。
- **legacy `datasource/paimon/*` 仍在树内**（B8 删除未做）→ 每个 fix 都能 side-by-side diff 做 parity。
- 迁移链：…→`667f779af04`(#3 SCHEMA-EVOLUTION)→`2d15b1b7ed7`(#4 JDBC-DRIVER-URL, HEAD)。

## ⚠️ Commit 须知（任何 `git add` 前必读）
- **硬前置**：scrub `regression-test/conf/regression-conf.groovy`（明文 Aliyun key）+ 清 scratch（`.audit-scratch/` `conf.cmy/` `META-INF/` `*.bak`）。**path-whitelist `git add`，严禁 `git add -A`。**
- 每个 fix 独立 commit；message = `fix: <ID>` + 根因 + 解法 + 测试，末尾带 `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`。
- 改 fe-core/SPI 的 fix：commit 须含连接器 + SPI + fe-core 三侧 + 测试（#5 大概率纯连接器/FE-wiring）。

## ⚙️ 操作须知（复用）
- maven 绝对 `-f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl :<art> **-am** -Dmaven.build.cache.enabled=false -DfailIfNoTests=false`；验证读 surefire XML + `MVN_EXIT`（[[doris-build-verify-gotchas]]）。**`-am` 必带**（漏 `-am` 会因 `${revision}` 解析失败报「could not resolve fe-connector-spi」而非真错——本 session 踩过）。`-pl :fe-connector-paimon -am` **不重编 fe-core**；改 fe-core 须单独 `-pl :fe-core -am`。
- 连接器禁 import fe-core：`bash tools/check-connector-imports.sh`（仅允许 `org.apache.doris.{thrift,connector,extension,filesystem}`）。
- cwd 跨 Bash 调用持久，`cd` 破相对路径 → 一律绝对路径。
- 测试优先 runnable FE **单测**（连接器 harness：`FakePaimonTable`/`RecordingPaimonCatalogOps`/`RecordingConnectorContext`/`PaimonScanPlanProviderTest`）；live-e2e（S3/OSS/REST/JDBC/Kerberos）CI-gated → 注明 gated，勿谎称跑过。

## 🧠 给下一个 agent 的 meta
- **#4 验证的高价值模式**：scout workflow（5 并行 reader 复核 finding + legacy parity + SPI 缝）→ 设计 → 实现 → **fail-before 实测**（临时 neuter 源码、`-am` 跑两测类、确认正确的 5 个 bug-catcher 向红、其余 guard 测两态皆绿）→ **4-lens clean-room review + 独立 verify**（揪出 CREATE-time-only 校验 gap）。**对 BLOCKER/安全类 fix 务必做 review**（[[clean-room-adversarial-review-pref]]）。
- 改 fe-core handle/scan 流前，先 grep 全 `metadata.getTableHandle` / scan-node 调用方（历史教训：独立 handle 面绕 seam 会静默错行）。
- P2 两条（#8/#9）严重度有争议 → **先找用户定 scope 再动手**。M-crit（#5）未过 3-lens → 实现前先独立复核 dotted-vs-underscore key 事实。
- **#4 留的跨连接器 follow-up（[DV-028]）**：CREATE-time-only driver-url 校验是 fe-core 全 plugin 连接器共有缝；若将来要 close，须新 `ConnectorContext` scan-time 校验 hook + fe-core ALTER 路接 `preCreateValidation`（注意会触发 JDBC 连接器 BE 连通测）—— 独立工单，非 paimon 专属。
