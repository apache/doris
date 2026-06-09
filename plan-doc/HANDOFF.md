# 🤝 Session Handoff

> 滚动文档：每次 session 结束覆盖更新；历史见 `git log plan-doc/HANDOFF.md`。
> 协作规范：[AGENT-PLAYBOOK.md](./AGENT-PLAYBOOK.md)

---

# 🔥 第 17 次 handoff（2026-06-09，覆盖）— 🎉 老 MaxCompute 代码移除 DONE（3 commit，全门绿）

> **本 session**：用户确认 🅰 live ODPS e2e 绿后执行 Batch-D 删除。**基于最新 upstream `9ed49571b20`(#64253) 新建分支 `catalog-spi-06`**（upstream 已含全部 cutover+gap-fix 代码，与旧 `catalog-spi-05` tree 字节一致，已核：`git diff` 0 文件差）。**2 code commit + 1 doc commit，全部守门绿。**

## ✅ 本 session 已完成
- **删 legacy（`7a4db351100`）**：删 20 fe-core 文件（`datasource/maxcompute/*` 含 MCTransaction/MaxComputeScanNode|Split + 写/事务 plumbing + 2 测）；清 21 反向引用文件（删 import + 死 instanceof/visitor/rule 分支，**保留**全部 PluginDriven/connector 兄弟分支 + §3 KEEP 集枚举/GsonUtils 串/block-id thrift）；3 测 trim/rewire——**FrontendServiceImplTest** block-id RPC 测改用 generic `Transaction` mock（`getMaxComputeBlockIdRange` 现读 `PluginDrivenTransaction`，非 MCTransaction）；**ExternalMetaCacheRouteResolverTest** 删 legacy `max_compute` engine 断言（插件路经 `ENGINE_DEFAULT`，已核 resolver fallback）；**CommitDataSerializerTest** 删 MCTransaction 等价测。守门：test-compile(main+test) + checkstyle **0** + import-gate + grep-empty（`com.aliyun.odps` fe-core/src=∅、无非注释 code ref；`MaxComputeExternal|MCTransaction|MCInsert` 仅剩 GsonUtils 串 + 注释）全绿。
- **依赖树彻底无 odps（`409300a75b8`，落实用户 Q2）**：删 fe-core/pom 两 odps 块；MCUtils 下沉 fe-common→be-java-extensions（`org.apache.doris.maxcompute`，删 legacy 后唯一消费者），JNI scanner/writer 删同包 import，MCProperties（odps-free 常量）留 fe-common；删 fe-common/pom 的 odps-sdk-core。**⚠️ 发现（DV-022）**：odps-sdk-core 此前**传递**给 fe-common 自身 `DorisHttpException`(netty)/`GsonUtilsBase`(protobuf)——删后编译暴露，fe-common 显式补 `netty-all`+`protobuf-java`。验收 `mvn -pl :fe-core dependency:tree | grep odps`=∅；fe-common+be-java-ext(max-compute)+fe-core 全编译。
- **doc commit**：PROGRESS（P4 80%/maxcompute kanban 95%）+ deviations（DV-021 T3 四接受项 / DV-022 netty-protobuf）+ Batch-D 设计 §5「✅ EXECUTED」+ 本 HANDOFF。

## 🎯 下一步
- **删除已完成**；剩 **push/PR**（用户定）。🅰 live e2e 用户已确认绿（本 session 解锁前提）；静态分发审计（任务0 `reviews/P4-cutover-completeness-audit-2026-06-08.md` PASS）+ UT 层守门均绿。
- 若日后要「fe-core 零 maxcompute 词元」= 另起 full-purge（泛化 block-id thrift / MC 枚举 / session var），用户当前**不取**（设计 §7.2 已评估升级兼容下限：GsonUtils 3 兼容串 + InitCatalogLog.Type.MAX_COMPUTE + 已持久化 TransactionType.MAXCOMPUTE 须留）。

## ⚙️ 操作须知
- 分支 `catalog-spi-06`（off upstream/branch-catalog-spi，tracking 已设）；本地 3 commit 未 push。未跟踪 `.audit-scratch/`/`conf.cmy/`/`*.bak`/`scheduled_tasks.lock`（勿提交）。
- **删多模块 dep 时核传递依赖**（DV-022 教训：模块自身代码可能白拿被删 dep 的传递 jar，删前 `dependency:tree` + 删后编译验）。maven 绝对 `-f fe/pom.xml -pl :<art> -am`，读真实 BUILD（[[doris-build-verify-gotchas]]）。

---

<details><summary>📅 历史：第 16 次 handoff（2026-06-09）— Batch-D 移除方案 finalize（design-only）</summary>

# 🔥 第 16 次 handoff（2026-06-09，覆盖）— Batch-D 移除方案 finalize + @HEAD 校验（design-only）

> **本 session 主题**：用户要求「完整移除 fe-core 下老的 maxcompute（零代码 + 零依赖）」。本 session **只分析 + finalize 方案 + 查前置，不动代码**（用户定：实际删除放下个 session）。**结论**：移除方案 = 既有 **Batch-D**（`tasks/designs/P4-batchD-maxcompute-removal-design.md`，本 session 已 @HEAD 校验 + finalize + 扩 §7/§8）；唯一硬门 = 🅰 用户 live e2e。

## ✅ 本 session 已完成（design-only，0 代码）
- **完整分析**（3 轴，多 Agent + 亲核）：① 翻闸状态——`max_compute` 已全走 SPI（`CatalogFactory.SPI_READY_TYPES`），legacy 运行时零可达，2026-06-07 评审的写/分区/DDL blocker 已全在代码修复；② fe-core footprint——20 删除文件 + ~84 反向引用（§2）；③ maven——fe-core 直接 odps 仅 `pom.xml:364/379`，余经 fe-common 传递。
- **Batch-D @HEAD 校验**（全过）：20 文件全在；**linchpin** = fe-core 内 8 个 import odps 文件全在删除单元、单元外 residual=∅（pom drop 编译安全）；近 commit `effd8edbfdb`/`2b8a732682c` 只动 `PluginDrivenScanNode`（KEEP 集），footprint 未变；**任务 0 静态分发审计已 DONE**（`reviews/P4-cutover-completeness-audit-2026-06-08.md` PASS，零 legacy 回退）。
- **finalize Batch-D design**：① 删除集计数 **21→20** 就地修正；② §1 红线补 **LIMIT-split 第 3 行为副本**（等价物 P3-9 / `MaxComputeScanPlanProvider` `952b08e0cc8`）= 原 DOC task 交付；③ 新增 **§7**（范围定夺 + @HEAD 校验 + 前置门 + 验收基线）+ **§8**（fe-common odps 解耦方案 A）。

## 👤 用户定夺（2026-06-09）
- **Q1 = 只删老实现（Batch-D），非 full-purge**：保留 live SPI 插件路径在用的 `max_compute` 胶水词元（§3 KEEP 集）。
- **Q2 = fe-core 依赖树彻底无 odps（升级，覆盖 [D-027] 决定2）**：经**方案 A**——把唯一用 odps 的 `MCUtils` 下沉到 be-java-extensions（其删 legacy 后唯一消费者）、`MCProperties`（odps-free 常量）留 fe-common、删 `fe-common/pom.xml` 的 odps。故不再「接受 fe-common 传递 odps」。详见 design §8。
- **后果（by design）**：删后 `grep com.aliyun.odps fe-core/src`=∅ **且** `dependency:tree|grep odps`=∅；但 `grep maxcompute|max_compute|odps fe-core/src/main` 仍 >0（703→低百，SPI 胶水保留，非缺陷）。真正零词元 = 另起 full-purge（用户当前不取）。

## 🎯 下一 session = 执行 Batch-D 删除（gated on 🅰 live e2e）
- **Runbook = design §5**（T07+T08+T09 + §2 edits 作 **one compiling unit** → 守门 test-compile+checkstyle+import-gate → grep-empty 验收 → commit → §4 fe-core pom drop **+ §8 fe-common 解耦** → doc-sync）。**执行前按符号 re-grep**（§2 行号已漂移 +5~+43）。
- **前置门**：
  1. 🅰 **live ODPS e2e 绿（用户跑，硬门，OPEN）**：`OdpsLiveConnectivityTest`（4 个 `MC_*` env）+ 手测 smoke（读/写/DDL/元数据全覆盖）。[D-027]：删 legacy 前 flip 须保持独立可 revert。
  2. ⬜ **T3**（登记 4 条 Tier-3 DV，doc-only，可同批）。
- **验收基线**（§7.4）：`MaxComputeExternal|MCTransaction|MCInsert` 151→仅 §3 KEEP；`com.aliyun.odps` fe-core/src→∅；`dependency:tree|grep odps`→**∅**（含 §8）。

## ⚙️ 操作须知（复用）
- maven 绝对 `-f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl :<artifact> -am -Dmaven.build.cache.enabled=false`；读真实 `BUILD`/`Tests run:`，勿信后台 task exit code。改 fe-core=`:fe-core`、改 fe-common=`:fe-common`、改 BE 扩展=`be-java-extensions/max-compute-connector`。
- 删除 + 反向引用须 **one compiling unit**（Java 不 dead-strip 源符号引用）；§3 KEEP 集勿删（GsonUtils 3 字面量、block-id thrift、各 MC 枚举、PluginDriven*）。§8 移 MCUtils 须在删 `MaxComputeExternalCatalog` 之后（否则 fe-core 仍需 MCUtils）。
- 分支 `catalog-spi-05`，本地未 push。本 session **0 代码 commit**（仅 plan-doc：design §1/§5/§7/§8 + HANDOFF + PROGRESS + tracker DOC✅）。未跟踪 `.audit-scratch/`/`conf.cmy/`/`regression-conf.groovy.bak`（勿提交）。

## 🧠 给下一个 agent 的 meta
- **🅰 live e2e（真实 ODPS）仍是翻闸 + 删除的真正完成门**；静态分发面（任务 0）已绿。
- 范围已定：Batch-D / **fe-core 依赖树彻底无 odps（方案 A 下沉 MCUtils）**，勿擅自扩成 full-purge、也勿退回 [D-027] 的「接受传递」。
- auto-memory：连接器禁 import fe-core（[[catalog-spi-connector-session-tz-gotcha]]）；FE 分发缺口史（[[catalog-spi-cutover-fe-dispatch-gap]]，任务0已复核 PASS）；构建坑（[[doris-build-verify-gotchas]]）。

</details>

---

<details><summary>📅 历史：第 15 次 handoff（2026-06-08）— G2 + GC1 完成</summary>

# 🔥 第 15 次 handoff（2026-06-08，覆盖）— G2 + GC1 完成

> **本 session 主题**：完成 Batch-D 红线扩充 gap campaign 的 **G2 + GC1**（两者逻辑独立、触不同区：G2=读谓词路径连接器局部 / GC1=写事务路径 + fe-core session 透传）。各走 recon 核码（Rule 8）→ 独立 design doc →（Ultracode off，沿用前 4 issue 的 skip 设计验证 workflow 默认）→ 实现 → 守门（编译+UT+checkstyle+import-gate+mutation）→ 单 Agent 对抗 impl-review → 独立 `[P4-T06e]` commit + hash 回填。**两 issue 全 DONE，4 commit。**

## ✅ 本 session 已完成

- **G2 FIX-PREDICATE-COLGUARD（Tier 2，minor，多半不可达）DONE @`fefbbad391d`（+回填 `1eeea30abcb`）**：列不存在守卫反转。`MaxComputePredicateConverter.formatLiteralValue:211` 在 `columnTypeMap.get(columnName)==null` 时静默引号化、下推非法谓词（如 `ghost == "5"`，整型字面量被错误引号化），而非 legacy 那样丢谓词（legacy `MaxComputeScanNode` containsKey 守卫→throw→caller per-conjunct catch 丢谓词）。**修**=该 null 分支 `return` → `throw UnsupportedOperationException`（与同方法 :198/:204/:260 既有守卫一致；连接器禁 import fe-core 的 AnalysisException），经 `convert()` 既有顶层 catch（:91-96）降级 `NO_PREDICATE` → BE 复算 = legacy「丢谓词」本质不变式。**correctness 已核（impl-review）**：MaxComputeConnectorMetadata 未 override applyFilter → conjuncts 永不在 BE 端 clear → 整树降级仅 perf、永不错结果；limit-opt 不交互（unknown 列不过 partition-equality 闸）。粒度差异（整 filter vs legacy per-conjunct）非本 fix 引入、correctness-safe。UT 16/16（+3）+ mutation 2 红。impl-review 单 Agent **APPROVE**（0 must-fix；nit=IS NULL 路 convertIsNull 无守卫=legacy parity 故意 out-of-scope）。
- **GC1 FIX-BLOCKID-CAP-CONFIG（Tier 2，minor，写路径）DONE @`95575a4954d`（+回填 `eee07156e77`）**：写 block-id 上限硬编 `MAX_BLOCK_COUNT=20000L`（`MaxComputeConnectorTransaction:72`），无视 legacy 可调 `Config.max_compute_write_max_block_count`（`Config.java:2156`，fe.conf 可调）→ 调优部署静默回归。原硬编=已登记偏差 **DV-011**。**用户定 Option A（全局 Config 透传，true parity，反转 DV-011 的 Rule-2 推迟）**：连接器禁 import Config，故经 **session-property 通道透传**（镜像既有 `lower_case_table_names` 注入）——① fe-core `ConnectorSessionBuilder.extractSessionProperties` +1 行注入 `Config.max_compute_write_max_block_count`；② 连接器 `MaxComputeConnectorTransaction` 常量→实例字段 `maxBlockCount` + ctor 加参 + `DEFAULT_MAX_BLOCK_COUNT` fallback；③ 连接器 `MaxComputeConnectorMetadata` byte-identical key 常量 + map-typed `resolveMaxBlockCount`（absent/unparseable→DEFAULT 20000，零回归）+ `beginTransaction` 透传。**无 SPI 签名变更、import-gate 净**。UT 新 `MaxComputeConnectorTransactionTest` 5 + mutation M1（resolve 忽略 prop）/M2（cap 用 DEFAULT）共 3 红。impl-review 单 Agent **APPROVE-WITH-NITS**（0 must-fix）。**DV-011 已更新**（后续动作勾销：经 session-passthrough 恢复可调、非原拟 MCConnectorProperties[catalog-scoped 错 scope]）。

## 👤 用户定夺（2026-06-08）
- **GC1 = Option A（全局 Config 透传，经 session-property）**——非原 DV-011 拟的 MCConnectorProperties（per-catalog，错 scope，非 legacy parity）。理由（采纳）：legacy 读的是 fe 全局 Config，须读同一全局值方 true parity；session-property 通道有 `lower_case_table_names` 直接先例、无 SPI 变更。见 [[catalog-spi-connector-session-tz-gotcha]]（连接器禁 import fe-core、经 session prop 读约定）。
- **G2/GC1 = 沿用前批 skip 设计验证 workflow + 单 Agent 对抗 impl-review**（Ultracode off，同 G0/G5/G6/G7）。

## 🎯 下一 session = 🆕 翻闸完整性审计（零 legacy 回退）+ T3 + DOC（用户定，2026-06-08）

> **🎉 Batch-D 红线扩充 gap campaign 的 Tier 1+2 fix 已全清**（G8/G0/G6/G5/G7/G2/GC1）。剩余 = ① 🆕 翻闸完整性审计（用户 2026-06-08 新增，下「任务 0」，无产线代码、可能查出新 gap）② T3 接受项登记 ③ 原 DOC 交付。

### 🆕 任务 0（用户新增 2026-06-08，优先）— 确认所有 MaxCompute 操作走新 SPI、零 legacy 回退

> **用户原话**：确认所有 maxcompute 的操作，都走到新的 SPI 框架上，不允许回退到老的代码上。

**目标**：对 `max_compute` catalog 的**每一类操作**，证 FE 分发可达新 SPI/PluginDriven 实现，且 legacy `MaxCompute*` 对应路径在运行时**零可达**（无静默回退）。= 🅱 Batch-D 删 legacy 的**静态前置确认**（零可达调用方 → 删除才安全）。

**审计范围（逐类核「FE 入口 → SPI 路由」+「legacy 路径零可达」）**：
- 读：scan / 分区裁剪(P1-4) / 谓词下推(G0/G2) / limit-split(P3-9) / batch-mode(P3-11) / CAST 剥壳(F9)。
- 写：INSERT / INSERT OVERWRITE(P0 gate) / 事务 begin·commit·block-alloc(GC1) / sink 分发(P0-2) / bind 投影(P0-3) / post-commit(P3-12)。
- DDL：CREATE TABLE·CTAS(P2-7) / DROP TABLE / CREATE DB(P2-6) / DROP DB FORCE(P2-5) / CREATE CATALOG 校验(G6)。
- 元数据：list db/table / get schema / DESCRIBE isKey(P3-10) / SHOW PARTITIONS / partitions() TVF。

**已知风险区（必查、勿信先验「已修」标签 — Rule 8/12）**：
- ⚠️ **FE 分发缺口** [[catalog-spi-cutover-fe-dispatch-gap]]：`PluginDrivenExternalCatalog` 仅 override `createTable`、`metadataOps` 曾永 null → DROP TABLE / CREATE DB / DROP DB / SHOW PARTITIONS / partitions TVF 的 FE 分发是否真接 SPI。**该 memory 的「已修完」状态 2026-06-07 对抗 review 两度被证伪**（见 `plan-doc/reviews/P4-maxcompute-full-rereview-2026-06-07.md`）→ 必须逐路径重核，不得信任何「已修」标签。
- legacy 删除候选逐个确认对 `max_compute` **零运行时可达调用方**：`MaxComputeExternalCatalog` / `MaxComputeScanNode` / `MaxComputeMetadataOps` / `MCTransaction` / `PhysicalMaxComputeTableSink` / `bindMaxComputeTableSink` / `allowInsertOverwrite` 的 MC 分支 / `MaxComputeExternalTable`。
- 「分发只接一半」反模式（已多次踩：P0 overwrite 顶层网关挡死下层；FE 仅 override createTable）：每个 op 须核**完整**分发链，非仅「连接器实现存在」。

**成功标准（Rule 4，强标准供独立 loop）**：产出审计报告（建议 `plan-doc/reviews/P4-cutover-completeness-audit-<date>.md`）——每 op 一行：路由✅(FE 入口→SPI 实现 file:line) / 回退⚠️(file:line + 判据)；任何回退/缺口登记为新 gap 进 `plan-doc/task-list-batchD-redline-gaps.md` 修复。**法**：grep + 调用链 trace（SPI_READY catalog 经 `PluginDrivenExternalCatalog`/`PluginDrivenExternalTable`→`PluginDrivenScanNode`）；可选 clean-room 对抗 workflow（需用户 opt-in，复用 `plan-doc/reviews/maxcompute-full-rereview.workflow.js`）。

**关系**：本任务 ⊇ 既有「Batch-D redline 扩充」DOC 的 zero-survivor 复核（DOC 是其产物/子集）；与 🅰 live e2e 并列为 🅱 Batch-D 删 legacy 的两大解锁门（本任务 = 静态分发面、🅰 = 运行时真值面）。

### 任务 1–2（原计划，T3 + DOC）

1. **T3 Tier-3 DV batch（GAP3/4/9/10，登记 deviation，无代码）**：在 `plan-doc/deviations-log.md` 登记 4 条接受项 + 各 file:line + 接受理由：
   - GAP3 CREATE DB 非-IFNE：`ERR_DB_CREATE_EXISTS`(1007/HY000 本地预抛)→透传 ODPS DdlException（P2-6 已注 pre-existing）。
   - GAP4 DROP TABLE 非-IF-EXISTS+远端缺：`ERR_UNKNOWN_TABLE`(1109/42S02)→通用 DdlException（本地名）。
   - GAP9 SHOW PARTITIONS `LIMIT`：legacy paginate-then-sort → 新路 sort-then-paginate（新路更合 ORDER-BY-LIMIT）。
   - GAP10 partitions() TVF：schema-分区但零实例表 legacy 抛→新路返 0 行（已有 in-code 注释声明 intentional）。
2. **DOC：Batch-D redline 扩充**（原任务交付，仍欠）：把全部行为逻辑副本作 must-land-before-delete 红线补入 `plan-doc/tasks/designs/P4-batchD-maxcompute-removal-design.md` §1/§2；更正 scan-node 红线注漏列 **LIMIT-split 第 3 行为副本**（等价物在 P3-9，注应 cite）；登记 ES `EsTypeMapping:191` 同款 emit "NULL" latent token bug（G7 out-of-scope，留待 ES 翻闸）。

> 其后：**🅰 live e2e 终验（真实 ODPS）= 翻闸真正完成门**（所有静态修复 DV 真值闸须 live 验，CI 跳；G2 ~不可达无自然 live 路、GC1 = fe.conf 调 block 上限→大写入越限/放宽）→ **🅱 Batch-D 删 legacy（21 文件，gated on live e2e）**。详见下方折叠历史。

## ⚙️ 操作须知（复用 + 本 session 新坑）
- maven 必绝对 `-f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml` + `-pl :<artifact>` + `-Dmaven.build.cache.enabled=false`；改连接器 `:fe-connector-maxcompute`、改 fe-core `:fe-core`。读真实 `Tests run:`/`BUILD`，勿信后台 task exit code。
- **本 session 新坑（重要）**：`.m2` 里 `fe-connector-spi` 安装的 pom 含字面 `${revision}` parent token → 独立 `-pl :fe-connector-maxcompute test`（**无 `-am`**）报 dependency resolution `fe-connector:pom:${revision} (absent)`（负缓存、不自动重试）。**解法 = 一律带 `-am`**（reactor 内解析 ${revision}，绕过 .m2 坏 pom）：`mvn -f fe/pom.xml -pl :fe-connector-maxcompute -am test [-Dtest=X -DfailIfNoTests=false] -Dmaven.build.cache.enabled=false`。⚠️ `-am install -DskipTests` **不修**该负缓存（仍须 -am 跑测）。
- mutation：cp 备份产线到 `/dev/shm`（RAM）→ Edit 重引入 bug → `-am test` 确认向红 → cp 还原 → grep 验还原。改连接器 ctor/常量时注意单 caller（`new MaxComputeConnectorTransaction` 仅 beginTransaction + 新 test）。
- 分支 `catalog-spi-05`，本地未 push。本 session 4 commit。未跟踪 `.audit-scratch/`/`conf.cmy/`/`regression-conf.groovy.bak`/`.claude/scheduled_tasks.lock`（勿提交）。

## 🧠 给下一个 agent 的 meta
- **live e2e（真实 ODPS）仍是翻闸真正完成门**——本批为静态/UT 层判定。
- auto-memory：连接器禁 import fe-core（[[catalog-spi-connector-session-tz-gotcha]]）；测基建无 fe-core/无 mockito、child-first loader（[[catalog-spi-fe-core-test-infra]]）；clean-room 对抗偏好（[[clean-room-adversarial-review-pref]]）；构建/守门坑（[[doris-build-verify-gotchas]]，本 session 已补 maven `-am` 必带 / ${revision} 负缓存坑）。

</details>

---

<details><summary>📅 历史：第 14 次 handoff（2026-06-08）— G6 + G5 + G7 批量完成</summary>

# 🔥 第 14 次 handoff（2026-06-08，覆盖）— G6 + G5 + G7 批量完成

> **本 session 主题**：批量修复 Batch-D 红线扩充 gap campaign 的 **G6 + G5 + G7**（三者逻辑独立、触不同区）。各走 recon 核码 → 独立 design doc →（Ultracode off，用户定 skip 设计验证 workflow）→ 实现 → 守门（编译+UT+checkstyle+import-gate+mutation）→ 单 Agent 对抗 impl-review → 独立 `[P4-T06e]` commit + hash 回填。**三 issue 全 DONE，6 commit。**

## ✅ 本 session 已完成

- **G6 FIX-CREATE-CATALOG-VALIDATION（Tier 2，major）DONE @`1fc00178484`（+回填 `8bc2c5cade2`）**：`MaxComputeConnectorProvider` 未 override `validateProperties`（继承 SPI no-op）→ CREATE CATALOG 跳过全部属性校验（required PROJECT/ENDPOINT、split floor、account_format、timeout>0、auth）。**修**=override `validateProperties` 逐字镜像 legacy `MaxComputeExternalCatalog.checkProperties:388-457` 六校验、抛 `IllegalArgumentException`（经 `PluginDrivenExternalCatalog.checkProperties:159` catch→DdlException，= legacy 形态）；wire 既有 dead `MCConnectorClientFactory.checkAuthProperties`（4 处 RuntimeException→IllegalArgumentException，零调用方安全）。required ENDPOINT 取**字面 key**（= legacy CREATE parity；region/odps_endpoint 为 replay backward-compat、不在新 CREATE 接受；impl-review 证 `CatalogMgr` `!isReplay`-gated、老 catalog 不受影响）。UT `MaxComputeConnectorProviderTest` 19/19 + mutation 3 组向红。impl-review 单 Agent **APPROVE-WITH-NITS**（0 must-fix；nit=纠正 legacy 错误 message 文案，故意改）。
- **G5 FIX-AGG-COLUMN-REJECT（Tier 2，minor）DONE @`c5e8ba6d9e2`（+回填 `aa28c97f8ef`）**：`CREATE TABLE (c INT SUM)` 对 mc 表静默建普通列（**证伪 P2-8「非-OLAP 路径已覆盖聚合列」**）。nereids 唯一拒 bare 非-key aggType 的 `validateKeyColumns` 仅在 `ENGINE_OLAP` 块内被调、非-OLAP 不可达。**用户定 Option B（加 SPI 字段，非 HANDOFF 原倾向的 fe-core guard）**——逐字镜像 P2-8 isAutoInc：`ConnectorColumn` 加 additive 第 8 字段 `isAggregated`（8-arg ctor、7-arg 委托 default false、getter/equals/hashCode；全 25 call site 仅 converter 改 8-arg）+ `CreateTableInfoToConnectorRequestConverter` 算 `isAggregated = getAggType()!=null && !=AggregateType.NONE`（= `Column.isAggregated()`）+ `MaxComputeConnectorMetadata.validateColumns` 在 isAutoInc 检查后加 `if(col.isAggregated())throw`（逐字镜像 legacy `MaxComputeMetadataOps.validateColumns:426-429`，**相邻** auto-inc 分支）。over-rejection 已核（隐式 aggType 赋值块 isOlap-gated、validate(isOlap=false)）。UT 4/4/11 + mutation 3 组向红。impl-review **APPROVE**（0 must-fix）。
- **G7 FIX-VOID-TYPE-MAPPING（Tier 2，minor）DONE @`49113dc7860`（+回填 `74822486792`）**：ODPS VOID 列映 UNSUPPORTED（legacy=Type.NULL）。`MCTypeMapping` VOID emit token `"NULL"`，但 `ScalarType.createType` 只认 `"NULL_TYPE"`（"NULL" 抛→`ConnectorColumnConverter` catch→UNSUPPORTED）。**修**=连接器局部：① VOID token `"NULL"`→`"NULL_TYPE"`（fe-core convertScalarType default 即产 Type.NULL，无需改 fe-core）；② switch default `return UNSUPPORTED`→`throw DorisConnectorException`（fail-fast，镜像 legacy `mcTypeToDorisType:294`）。**fix-2 安全性**：BINARY/INTERVAL_*/JSON 显式 UNSUPPORTED case 不受影响；impl-review 经 24-值 OdpsType 枚举 set-diff 证**仅 `OdpsType.UNKNOWN`（SDK sentinel、非真实列类型）落 default**、legacy 对 UNKNOWN 同 throw→parity、真实表零回归。UT `MCTypeMappingTest` 5/5 + mutation 2 组向红。impl-review **APPROVE**（0 must-fix）。**out-of-scope（留待 ES 翻闸）**：ES `EsTypeMapping:191` 同款 emit "NULL" latent token bug（其 test 还钉了 buggy token），未修。

## 👤 用户定夺（2026-06-08）
- **G5 = Option B（加 SPI 字段 `isAggregated`）**——非 HANDOFF 原倾向的 fe-core guard。理由（采纳）：聚合拒绝是 legacy `validateColumns` 中 auto-inc 拒绝的**相邻行**，连接器 `validateColumns` 已含 `isAutoInc` 检查，Option B 完成同方法的 legacy 镜像；且与 P2-8 一致（full parity 非 deviation）。见 [[catalog-spi-p2-ddl-decisions]]。
- **G6/G7 = 直接 implement（无单独设计验证 workflow，Ultracode off）**，走守门 + 单 Agent impl-review。
- **G7 secondary defect（未知 OdpsType fail-fast）= 纳入修复**（parity + Rule 12 fail-loud；零现表风险；经 `TypeInfoFactory.UNKNOWN` 可 UT）。
- **下一 session = G2 + GC1**（本次定）。

## 🎯 下一 session = G2 + GC1（用户定，2026-06-08）

> **方法论（每 issue）**：recon 核码（**Rule 8，下列 anchor 已核但仍可漂移**）→ 独立设计 `tasks/designs/P4-T06e-<FIX>-design.md` → 设计验证（**⚠️ Ultracode 仍关**：workflow 需用户 opt-in，否则单/双 Agent 对抗或用户定 skip）→ 实现 → 守门（编译+UT+checkstyle+import-gate+mutation）→ impl-review → 独立 `[P4-T06e]` commit + hash 回填 + tracker。live tracker `plan-doc/task-list-batchD-redline-gaps.md`。

1. **G2 FIX-PREDICATE-COLGUARD（Tier 2，minor，多半不可达）— 连接器**：列不存在守卫反转。legacy `MaxComputeScanNode:415-421/478-484` 谓词引用未知列→抛→丢谓词；新路 `MaxComputePredicateConverter.formatLiteralValue` 取 `columnTypeMap.get(columnName)` 为 null 时静默引号化→下推非法谓词。**已核当前 anchor（G0 已移位）**：`MaxComputePredicateConverter.java:202`(formatLiteralValue) / **`:210-211`** `OdpsType odpsType = columnTypeMap.get(columnName); if (odpsType == null) {...}`——此 null 块即守卫点。实务 bound 谓词只引真列、columnTypeMap key 集与 legacy 一致→**多半不可达**；修=该 null 分支改 throw/skip（对齐 legacy 丢谓词、不下推非法）。低优。
2. **GC1 FIX-BLOCKID-CAP-CONFIG（Tier 2，minor）— 连接器写路径**：写 block-id 上限硬编 `MAX_BLOCK_COUNT = 20000L`（**已核** `MaxComputeConnectorTransaction.java:72`，用于 `:146`；`:68` 注释已自承硬编 = `Config.max_compute_write_max_block_count` 默认），无视 legacy `MCTransaction.java:165` 读的可调 `Config.max_compute_write_max_block_count`（`Config.java:2156`，`=20000L`）→ 调优部署静默回归。修=连接器读该 Config 值。**⚠️ 关键调研点（未解）**：连接器**禁 import fe-core**（含 `org.apache.doris.common.Config`，import-gate 禁）→ 须查连接器如何拿 fe Config 值：候选 = ConnectorContext / catalog property 透传 / `ConnectorSession.getSessionProperties()`（参 P3-9 limit-opt 经 session prop 读 var、G0 经 `ConnectorSession.getTimeZone()` 的约定）。若无现成透传通道，需**设计定夺**（加 property/context 透传 vs 接受+登记 deviation）——可能需问用户。

> 其后（本批之后，**非本 session**）：**T3 Tier-3 DV batch（GAP3/4/9/10 登记 deviation，无代码）→ DOC（Batch-D redline 扩充 design §1/§2 must-land-before-delete + scan-node 注补 LIMIT-split 第 3 副本 + 登记 ES `EsTypeMapping:191` 同款 token bug）**。详见下方折叠「第 12 次 handoff」§下一 session 待办 7-8 项。

## ⚙️ 操作须知（复用）
- maven 必绝对 `-f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml` + `-pl :<artifact> -am` + `-Dmaven.build.cache.enabled=false`；改连接器 `:fe-connector-maxcompute`、改 SPI `:fe-connector-api`（**须 -am、连带 rebuild maxcompute + fe-core**）、改 fe-core `:fe-core`。读真实 `Tests run:`/`BUILD`/`MVN_EXIT`，勿信后台 task exit code。checkstyle 走 `test` 的 validate 阶段自动跑（或 `checkstyle:check`）；import-gate `bash tools/check-connector-imports.sh`（repo 根）。
- mutation：Edit 改产线一处→跑相关 UT→确认对应 test 变红→Edit 还原；备份产线文件到 `/dev/shm`（RAM，避 `/mnt/disk1` 满时 cp 截断，auto-memory [[doris-build-verify-gotchas]]）。改产线令 import 变 unused 时改用「翻转谓词」式 mutation（保 import 用、免 checkstyle 拦——本 session G5-M2 踩过）。
- 分支 `catalog-spi-05`，本地未 push。本 session 6 commit。未跟踪 `.audit-scratch/`/`conf.cmy/`/`regression-conf.groovy.bak`/`.claude/scheduled_tasks.lock`（勿提交）。

## 🧠 给下一个 agent 的 meta
- **live e2e（真实 ODPS）仍是翻闸真正完成门**——本批为静态/UT 层判定；G6 非法属性 CREATE 拒绝须 live 验（登记 DV）。
- auto-memory：连接器禁 import fe-core（[[catalog-spi-connector-session-tz-gotcha]]）；测基建无 fe-core/无 mockito、child-first loader（[[catalog-spi-fe-core-test-infra]]）；P2 DDL 定夺（[[catalog-spi-p2-ddl-decisions]]，G5 续其 isAutoInc→isAggregated SPI-字段模式）；clean-room 对抗偏好（[[clean-room-adversarial-review-pref]]）。

</details>

---

<details><summary>📅 历史：第 13 次 handoff（2026-06-08）— G0 FIX-DATETIME-PUSHDOWN-FORMAT 完成</summary>

# 🔥 第 13 次 handoff（2026-06-08，覆盖）— G0 FIX-DATETIME-PUSHDOWN-FORMAT 完成

> **本 session 主题**：续做 Batch-D 红线扩充 gap 修复 campaign 的 **G0**（Tier 1，major correctness/perf）。设计 → （用户定 **skip** 设计验证 workflow）→ 实现 → 守门 → 单 Agent impl-review → 独立 commit。

## ✅ 本 session 已完成
- **G0 FIX-DATETIME-PUSHDOWN-FORMAT（Tier 1）DONE @`0d983a1c056`**：DATETIME/TIMESTAMP/TIMESTAMP_NTZ 谓词下推坏（两 delta）。**delta-1**：`MaxComputePredicateConverter` 用 `String.valueOf(LocalDateTime)`（'T' 分隔变精度，如 `"2023-02-02T00:00"`）喂空格定长 formatter → 非 UTC session `LocalDateTime.parse` 抛 → 整 conjunct 树降 `NO_PREDICATE`（谓词永不下推=perf 回归）/ UTC session 推 malformed 字面量。**delta-2**：source TZ 取 project-region（endpoint 推）而非 session TZ → 跨 TZ 静默丢行。**修**（连接器局部、无 SPI 变更，对齐 legacy `MaxComputeScanNode.convertLiteralToOdpsValues`）=① 直接对 `LocalDateTime` 用目标 formatter 格式化（逐字镜像 legacy `getStringValue(DatetimeV2Type(3|6))`，删字符串版 `convertDateTimezone`）；② source TZ 改 `ConnectorSession.getTimeZone()`（≡ legacy `DateUtils.getTimeZone()`），TZ id 以**字符串**传入、在 converter 内**惰性** `ZoneId.of`（`convert()` 的 catch 内）。
  - **⚠️ impl-review F1（real regression，已折入）**：初版 `convertFilter` 内 eager `ZoneId.of(session.getTimeZone())`。但 Doris `SET time_zone='CST'`（华区常见，本 Alibaba 连接器尤甚）被 `TimeUtils.checkTimeZoneValidAndStandardize` **逐字存**，而 `java.time.ZoneId.of("CST")` 抛 `ZoneRulesException`（PST/EST/MST 同；UTC/GMT/+08:00/Asia*/Z/PRC OK——已实测）→ eager 解析炸出 `planScan`（无 catch）→ **整查询失败**（含非 datetime 如 `id=5`），比 legacy（per-conjunct catch 降级、仅 datetime 解析 TZ）+ 翻闸前（`resolveProjectTimeZone` 永不抛）双回归。**惰性解析修法** → datetime+CST 降级 `NO_PREDICATE`（BE 兜底，结果仍正确）、非 datetime 仍下推、NTZ 不解析 = **legacy parity**。
  - 守门：编译 + UT `MaxComputePredicateConverterTest` **13/13** + 连接器模块 55(1 skip，live) + checkstyle 0 + import-gate 0 + mutation（M1 `format→toString` 8红 / M2 `忽略 session zone` 3红 → 还原绿）。**真值闸 live ODPS=DV-022**（跨 UTC/非-UTC session TZ datetime 谓词正确下推、不丢行）。
  - 设计 `plan-doc/tasks/designs/P4-T06e-FIX-DATETIME-PUSHDOWN-FORMAT-design.md`。**Batch-D 死代码清理项**：`MCConnectorEndpoint.resolveProjectTimeZone` + `REGION_ZONE_MAP`（~60 行）翻闸后零调用方（本 fix 仅删 provider 内死的私有 wrapper）。

## 👤 用户定夺（2026-06-08）
- **G0 design-verify = Skip → 直接 implement**（设计已深度核码：format 字节级对齐 + TZ source 经 `from(ctx)` 确认）；仍走守门 + 末端 impl-review。
- **G0 死代码 = Keep + defer Batch-D**（仅删 provider 内死 wrapper；public 方法+map 留待 Batch-D 清理）。

## 🎯 下一 session = 批量修复 G6 + G5 + G7（用户定，2026-06-08）

> **用户定夺**：下一新 session **同时修复 G6 + G5 + G7**。三者**逻辑独立、触不同区**（G6=连接器 provider 校验 / G5=fe-core 列校验 / G7=类型映射），可并行 research/设计；但各仍走**独立 design doc + 独立 `[P4-T06e]` commit + 各自守门**（不合并 commit）。其后 **G2 / GC1 → T3 Tier-3 DV batch（GAP3/4/9/10 登记 deviation）→ DOC（Batch-D redline 扩充 + scan-node LIMIT-split 注补）**。live tracker `plan-doc/task-list-batchD-redline-gaps.md`。

> **方法论（每 issue）**：独立设计 `tasks/designs/P4-T06e-<FIX>-design.md` → 设计验证（**⚠️ Ultracode 仍关**：workflow 需用户 opt-in，否则单/双 Agent 对抗或经用户定 skip）→ 实现 → 守门（编译+UT+checkstyle+import-gate+mutation）→ impl-review → 独立 commit + hash 回填 + tracker。**动手前按指针核码（Rule 8）**——下列 file:line 为第 12 次 recon，G0 经验示其可漂移。
> **G0 经验**（auto-memory [[catalog-spi-connector-session-tz-gotcha]]）：连接器**禁 import fe-core**（import-gate）；mutation 改 API 时用 **in-place cp 备份**（revert-to-HEAD 不可编译）；先验 anchor 务必核码。

**本批三 issue（独立、可并行）：**

1. **G6 FIX-CREATE-CATALOG-VALIDATION（Tier 2，major）— 连接器（fe-connector-maxcompute）**：CREATE CATALOG 属性校验缺失。`MaxComputeConnectorProvider` **未 override `validateProperties`**（继承 SPI no-op `ConnectorProvider:74-76`；jdbc/es/trino 都 override）→ required PROJECT/ENDPOINT、split_byte_size≥10485760 floor、split_strategy、account_format∈{name,id}、connect/read timeout>0、retry_count>0、`checkAuthProperties`（`MCConnectorClientFactory.checkAuthProperties:42-78` **定义但零调用**）全不在 CREATE 时校验 → use-time 晚失败 / 静默接受非法（account_format='foo'→默认 DISPLAYNAME；负 timeout）。legacy `MaxComputeExternalCatalog.checkProperties:387-457`。**修**=实现 `MaxComputeConnectorProvider.validateProperties`（或 preCreateValidation）镜像 legacy 六校验 + wire `checkAuthProperties`。

2. **G5 FIX-AGG-COLUMN-REJECT（Tier 2，minor）— fe-core**：`CREATE TABLE (c INT SUM)` 聚合列拒绝丢失（证伪 P2-8「非-OLAP 路径已覆盖」）。链：`ConnectorColumn` 无 aggType 载体 → `CreateTableInfoToConnectorRequestConverter:90-92` 丢 aggType → `MaxComputeConnectorMetadata.validateColumns:476-498` 不查 → nereids `ColumnDefinition.validate(isOlap=false):358-411` 不拒 bare non-key aggType（`validateKeyColumns:1083` 拒但 gated 在 ENGINE_OLAP-only 块、非-OLAP 不可达）。legacy `MaxComputeMetadataOps:426-429` 拒。**修**=FE-core guard（convert/createTable 路径对 maxcompute engine 拒非空 aggType，因 ConnectorColumn 无 aggType 连接器看不到）。**⚠️ 设计定夺点**：FE-core guard（不动 SPI，倾向）vs 改 SPI 加 `ConnectorColumn.aggType`（如 P2-8 加 isAutoInc，见 [[catalog-spi-p2-ddl-decisions]]）。

3. **G7 FIX-VOID-TYPE-MAPPING（Tier 2，minor）— 连接器/fe-core 边界**：ODPS `VOID` → 新路映 `UNSUPPORTED`（legacy=`Type.NULL`）。链：`MCTypeMapping:51-52` emit `of("NULL")` → `ConnectorColumnConverter.convertScalarType` 无 "NULL" case → `ScalarType.createType("NULL")` 抛（只认 "NULL_TYPE"）被 catch→UNSUPPORTED。次生缺陷：未知 OdpsType legacy 硬抛、新路静默 UNSUPPORTED。**修**=加 "NULL" case 返 `Type.NULL`，或 `MCTypeMapping` emit `of("NULL_TYPE")`（设计时定哪侧）。

> G6/G5/G7 完整证据 + 其余待办（G2/GC1/T3/DOC 的 file:line + 修法）见下方折叠「第 12 次 handoff」§下一 session 待办，未变。

</details>

---

<details><summary>📅 历史：第 12 次 handoff（Batch-D 红线扩充查出 11 gap + 2 critic；G8 已修，G0 见上）</summary>

# 🔥 第 12 次 handoff（2026-06-08，覆盖）— Batch-D 红线扩充查出新 gap 修复 campaign

> **本 session 主题**：执行横切「**Batch-D 红线扩充**」——跑 clean-room 对抗 workflow `wbw4xszrg`（117 agent，13 carrier-unit × inventory→adversarial-verify + 3 critic）复查 Batch-D 设计「zero survivor」声明的**行为逻辑副本**层面（非仅实例化链）。**查出 11 gap + 2 critic-only finding。Critic-2 独立复核：13 条 per-fix 等价物全 present+wired（前修无回退）。** 这些是 per-fix review 漏掉的**新**发现。
> **⚠️ 重大发现**：其中 **GAP8 是 live 静默丢行回归**（已修，见下）；G5 证伪 P2-8「聚合列已覆盖」；G6 暴 CREATE CATALOG 校验缺失。

## ✅ 本 session 已完成
- **G8 FIX-NONPART-PRUNE-DATALOSS（blocker/correctness）DONE @`e1760d38d86`（+回填 `265cd3fa70f`）**：非分区 plugin 表 `SELECT...WHERE` 静默返 **0 行**。根因=`PluginDrivenExternalTable.supportInternalPartitionPruned()` 返 `!partCols.isEmpty()`(非分区=false) → `PruneFileScanPartition` else 支覆写 `SelectedPartitions(0,{},isPruned=true)` → `PluginDrivenScanNode.getSplits` 短路 0 split。**通用插件层**（CatalogFactory SPI_READY_TYPES={jdbc,es,trino,max_compute} 全经 PluginDrivenExternalTable→LogicalFileScan→PluginDrivenScanNode；当前仅 MC 翻闸暴露）。坏 override=`35cfa50f988`(FIX-PART-GATES,dormant)+`072cd545c54`(P1-4 加短路激活)。修=Option A：`supportInternalPartitionPruned()` 返**无条件 true**（镜像 legacy MaxComputeExternalTable/Iceberg；非分区 pruneExternalPartitions 返 NOT_PRUNED 扫全表）。设计验证 `wijd3qgk0`(4 lens design-sound,1mF+3sF 折入) + impl-review `wza2khdb2`(2 lens approve,0mF)。repro=翻转 `PluginDrivenExternalTablePartitionTest` 钉错不变式断言（mutation 还原即红）。auto-memory [[catalog-spi-nonpartitioned-prune-dataloss]]。
  - 守门：UT 6/6+5/5、mutation 向红、checkstyle 0、import-gate 净。

## 👤 用户定夺（2026-06-08，campaign 范围）
- **G8 = Fix now（repro 先行）** → 已完成。
- **其余 = Fix Tier 1+2，Tier 3 接受+登记 deviation**。

## 🎯 下一 session = 续做 gap 修复 campaign（live tracker = `plan-doc/task-list-batchD-redline-gaps.md`）

> **每 issue 走既有方法论**：独立设计文档 `tasks/designs/P4-T06e-<FIX>-design.md` → 设计验证 workflow（clean-room 对抗）→ 实现 → 守门（编译+UT+checkstyle+import-gate+mutation）→ impl-review workflow 收敛 → 独立 commit（`[P4-T06e]`）+ hash 回填 + 更 tracker。
> **⚠️ Ultracode 现已关**：跑 workflow 需用户显式 opt-in（或用户说「use a workflow」）。若关态，design-verify/impl-review 可改用单/双 Agent 对抗替代，或先问用户是否要 workflow。
> 全量 gap 证据：workflow 返回 JSON 在 `/tmp/claude-1000/-mnt-disk1-yy-git-wt-catalog-spi/.../tasks/wbw4xszrg.output`（若 /tmp 清，speca 全在 tracker；摘录曾在 `/tmp/wf_gaps.txt`/`/tmp/wf_critics.txt`）。每 gap 带 file:line + parity + evidence。

**按优先序待办（Tier 1+2 fix + Tier 3 DV + 原 doc 交付）：**

1. **G0 FIX-DATETIME-PUSHDOWN-FORMAT（Tier 1，major correctness/perf）— 下一个，本 session 已开始 design 调研**：
   - 症状：DATETIME/TIMESTAMP/TIMESTAMP_NTZ 谓词下推坏。**两 delta**：
     - **delta-1（format）**：`MaxComputePredicateConverter.formatLiteralValue:201` 用 `String.valueOf(literal.getValue())`，而 literal value 是 `java.time.LocalDateTime`，其 `toString()` 是 **'T' 分隔 + 变精度**（`"2023-02-02T00:00"`）；喂 `DATETIME_3/6_FORMATTER`（`"yyyy-MM-dd HH:mm:ss.SSS"` 空格分隔）→ `convertDateTimezone:259` 的 `LocalDateTime.parse` **抛 DateTimeParseException**（非 UTC）被 `convert():86` catch→**整 conjunct 树降 NO_PREDICATE**（谓词永不下推=perf 回归）；UTC 路（`convertDateTimezone:256` sourceTZ==UTC 短路）推 **malformed 字面量** `col=="2023-02-02T00:00"` 到 ODPS（结果未定，可能错/可能 ODPS 报错）。legacy `MaxComputeScanNode:558-593` 用 `dateLiteral.getStringValue(DatetimeV2Type(3|6))`（空格分隔定长）正确。
     - **delta-2（TZ source）**：连接器 `sourceTimeZone` = `MaxComputeScanPlanProvider:287-295` 经 `MCConnectorEndpoint.resolveProjectTimeZone(endpoint)`（**project-region TZ**）；legacy `convertDateTimezone` 用 `DateUtils.getTimeZone()`（**session TZ**）。format 修后若 TZ 仍错→**丢行**。
   - 修法方向（待设计）：① format=直接对 `LocalDateTime` 用目标 formatter（不走 toString()→reparse），即在 DATETIME/TIMESTAMP 分支把 value 当 LocalDateTime 格式化 + TZ 转换；② TZ source=改用 session TZ——**需查连接器如何拿 session TZ**（ConnectorSession 是否带 timezone？现 resolveProjectTimeZone 在 `MaxComputeScanPlanProvider`；legacy 用 ConnectContext session var，连接器不可直达 fe-core）。**关键调研点**：ConnectorSession.getSessionProperties() 是否含 time_zone（参 P3-9 limit-opt 经 session prop 读 var 的约定）。
   - 已读文件：`MaxComputePredicateConverter.java`（formatLiteralValue:195-252 / convertDateTimezone:254-263 / ctor:69-74 / formatters:55-58 / convert catch:84-89）。**待读**：`MaxComputeScanPlanProvider.java:131-133`(dateTimePushDown)`:274-295`(convertFilter+sourceTZ)、`MCConnectorEndpoint.resolveProjectTimeZone:111-125`、`ExprToConnectorExpressionConverter.convertDateLiteral:309-321`(fe-core 存 LocalDateTime)、ConnectorSession 接口（找 timezone）、legacy `MaxComputeScanNode:529-613`(对照)、`DateUtils.getTimeZone:403-408`。**无连接器测覆盖 datetime 格式**——补 `MaxComputePredicateConverter` UT 钉确切下推串 + mutation。真值闸 live ODPS=DV（datetime 谓词正确下推 + 不丢行，跨 UTC/非-UTC project TZ）。
2. **G6 FIX-CREATE-CATALOG-VALIDATION（Tier 2，major）**：CREATE CATALOG 属性校验缺失。`MaxComputeConnectorProvider`(fe-connector-maxcompute) **未 override `validateProperties`**（继承 SPI no-op `ConnectorProvider:74-76`，cf. jdbc/es/trino 都 override）→ required PROJECT/ENDPOINT、split_byte_size≥10485760 floor、split_strategy、account_format∈{name,id}、connect/read timeout>0、retry_count>0、`MCUtils.checkAuthProperties`（`MCConnectorClientFactory.checkAuthProperties:42-78` **定义但零调用**）全不在 CREATE 时校验 → 退化 use-time 晚失败 / 静默接受非法（account_format='foo'→默认 DISPLAYNAME；负 timeout）。legacy `MaxComputeExternalCatalog.checkProperties:387-457`。修=实现 `MaxComputeConnectorProvider.validateProperties`（或 preCreateValidation）镜像 legacy 六校验 + wire checkAuthProperties。
3. **G5 FIX-AGG-COLUMN-REJECT（Tier 2，minor）**：`CREATE TABLE (c INT SUM)` 聚合列拒绝丢失（**证伪 P2-8「非-OLAP 路径已覆盖」**）。链：`ConnectorColumn` 无 aggType 载体 → `CreateTableInfoToConnectorRequestConverter:90-92` 丢 aggType → `MaxComputeConnectorMetadata.validateColumns:476-498` 不查 → nereids `ColumnDefinition.validate(isOlap=false):358-411` 不拒 bare non-key aggType（`validateKeyColumns:1083` 拒但 gated 在 ENGINE_OLAP-only 块、非-OLAP 不可达）。legacy `MaxComputeMetadataOps:426-429` 拒。修=FE-core guard（convert/createTable 路径对 maxcompute engine 拒非空 aggType，因 ConnectorColumn 无 aggType 连接器看不到）。
4. **G7 FIX-VOID-TYPE-MAPPING（Tier 2，minor）**：ODPS `VOID` → 新路映 `UNSUPPORTED`（legacy=`Type.NULL`）。链：`MCTypeMapping:51-52` emit `of("NULL")` → `ConnectorColumnConverter.convertScalarType` 无 "NULL" case → `ScalarType.createType("NULL")` 抛（只认 "NULL_TYPE"）被 catch→UNSUPPORTED。次生：未知 OdpsType legacy 硬抛、新路静默 UNSUPPORTED。修=加 "NULL" case 返 Type.NULL，或 MCTypeMapping emit `of("NULL_TYPE")`。
5. **G2 FIX-PREDICATE-COLGUARD（Tier 2，minor，多半不可达）**：列不存在守卫反转。legacy `MaxComputeScanNode:415-421/478-484` 谓词引用未知列→抛→丢谓词；新路 `MaxComputePredicateConverter.formatLiteralValue:204-206` odpsType==null 静默引号化→下推非法谓词。实务 bound 谓词只引真列、columnTypeMap key 集与 legacy 一致→**多半不可达**；修=加 containsKey 守卫（throw/skip）对齐 legacy。低优，可与 G0 合并（同文件）。
6. **GC1 FIX-BLOCKID-CAP-CONFIG（Tier 2，minor）**：写 block-id 上限硬编 `20000`（`MaxComputeConnectorTransaction.java:72,146` `MAX_BLOCK_COUNT=20000L`），无视 legacy `Config.max_compute_write_max_block_count`（`MCTransaction:165`，可调）→ 调优部署静默回归。修=读 Config（连接器如何拿 fe Config？可能经 connector context/property 透传，需查）。
7. **T3 Tier-3 接受项 → 登记 deviation（不修，用户定）**：
   - GAP3 CREATE DB 非-IFNE：`ERR_DB_CREATE_EXISTS`(1007/HY000 本地预抛)→透传 ODPS DdlException（P2-6 已注 pre-existing）。
   - GAP4 DROP TABLE 非-IF-EXISTS+远端缺：`ERR_UNKNOWN_TABLE`(1109/42S02)→通用 DdlException（本地名）。
   - GAP9 SHOW PARTITIONS `LIMIT`：legacy paginate-then-sort → 新路 sort-then-paginate（新路更合 ORDER-BY-LIMIT）。
   - GAP10 partitions() TVF：schema-分区但零实例表 legacy 抛→新路返 0 行（已有 in-code 注释声明 intentional）。
   - 动作：在 `plan-doc/deviations-log.md`（或既有 deviations 文档）登记这 4 条 + 各 file:line + 接受理由。
8. **DOC：Batch-D redline 扩充（原任务交付，仍欠）**：把上述全部行为逻辑副本作为 **must-land-before-delete 红线** 补入 `plan-doc/tasks/designs/P4-batchD-maxcompute-removal-design.md` §1/§2（镜像现有 MaxComputeScanNode 红线注格式）；并**更正 scan-node 红线注**——critic-3 证其漏列 **LIMIT-split 优化（第 3 行为副本）**（等价物在 P3-9，注应 cite）。另 critic-2 提醒：`MetadataGenerator`/`PartitionsTableValuedFunction` 仍有 live-but-dead legacy refs，Batch-D 删 legacy 类前须连这些 reverse-ref 一并删否则不编译（已在 §2，复核）。

## ⚙️ 操作须知（本 session 新增/复用）
- maven 必绝对 `-f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml` + `-pl :fe-core -am`（改连接器 `:fe-connector-maxcompute`）+ `-Dmaven.build.cache.enabled=false`；读真实 `Tests run:`/`BUILD`/`MVN_EXIT`，勿信后台 task exit code。checkstyle `-pl :fe-core checkstyle:check`；import-gate `bash tools/check-connector-imports.sh`。
- 分支 `catalog-spi-05`，本地未 push。本 session 2 commit（G8 fix + 回填）。
- auto-memory 新增 [[catalog-spi-nonpartitioned-prune-dataloss]]。clean-room 对抗偏好见 [[clean-room-adversarial-review-pref]]；测基建坑见 [[catalog-spi-fe-core-test-infra]]。

</details>

---

<details><summary>📅 历史：第 11 次 handoff（P3-11/P3-12 完成 → P4-rereview triage 全 code-complete）</summary>

## 📅 最后一次 handoff

- **日期**：2026-06-08（第 11 次 handoff）
- **本 session 主题**：**P3-11 + P3-12 完成 → 🎉 P3 全清 + 整个 P4-rereview triage（P0-1..3 / P1-4 / P2-5..8 / P3-9..12）全部完成**。各走 设计文档 →（P3-11）设计验证 workflow → 实现 → 守门 → impl-review workflow → 独立 commit + hash 回填。live tracker = `plan-doc/task-list-P4-rereview.md`。
  - **P3-12 FIX-POSTCOMMIT-REFRESH** ✅ `1f2e00d3696`(+`2c4015ac7de` 回填)（NG-8/F15=F21 minor）：**无产线逻辑改动**——仅 `PluginDrivenInsertExecutor.doAfterCommit` Javadoc(`:164-176`) 从「只讲 JDBC_WRITE」泛化到覆盖 MC connector-transaction 路径。对抗性安全核查 inline（`handleRefreshTable` 只刷缓存/写 refresh editlog、丢失自愈）。[D-034]/[DV-018]。
  - **P3-11 FIX-BATCH-MODE-SPLIT** ✅ `ac8f0fc15eb`(+`2a43abc6d76` 回填)（NG-7/F6=F13 minor）：**用户定「实现 batch SPI 路径」**（Shape A 薄 SPI + fe-core 编排、逐字镜像 legacy）。SPI +2 additive default（`supportsBatchScan`/`planScanForPartitionBatch`，零破坏其余 6 连接器）+ 连接器 `supportsBatchScan`=`fileNum>0` + fe-core `PluginDrivenScanNode` 三 override（`isBatchMode`含 SF-1 null-guard / `numApproximateSplits` / `startSplit` 异步分批）+ 纯静态 `shouldUseBatchMode`。设计验证 `wcpg9lblj` + impl-review `wve7y1jst` 各 GO-WITH-EDITS 折入。守门 mutation 5/5。[D-035]/[DV-019]。
- **方法论**：每 issue = 设计文档 → 设计验证 workflow（多 lens clean-room 对抗）→ 实现 → 编译+UT+checkstyle+import-gate+mutation → impl-review workflow 收敛 → 独立 commit（fix）+ commit（hash 回填）。
- **分支**：`catalog-spi-05`（本地，未 push）。本 session 4 commit（P3-11/P3-12 各 fix + hash 回填）。**累计本轮 triage 共 12 issue 全 DONE。**
- **operational 坑（auto-memory `doris-build-verify-gotchas` 已更新）**：mutation 跑中 `/mnt/disk1` **系统级 100% 满**（1.9T/2T，非本 repo 数据——repo target 仅 ~3.65G）致 `cp` 还原失败一度 **truncate 产线文件**；已从 `/dev/shm`(RAM) 备份还原、重跑确认。教训=mutation 还原备份须放 RAM/异盘 + mutation 跑带 `-Dcheckstyle.skip=true`。**⚠️ 磁盘当前 97%，bulk 占用非本 repo，需用户排查。**
- **复审已验层（legacy parity 达成，静态层面）**：返回行结果正确、descriptor/JNI/BE 线、事务生命周期、schema cache、editlog/replay、读裁剪下推（DG-1）、limit-split 三重闸（P3-9）、isKey 元数据（P3-10）、batch-mode 异步 split（P3-11）、post-commit swallow（P3-12）、写分发/静态分区 bind/INSERT OVERWRITE（P0）——均独立验为与 legacy 等价。**triage 已 code-complete；剩余 = ① live e2e 终验（真值闸，真实 ODPS）② Batch-D 删 legacy ③ 若干横切开放项（见下）。**

---

# 🎯 下一 session = triage 已 code-complete，进入「终验 + 收尾」阶段

> **本轮 P4-rereview triage 全部完成**：P0-1..3（写 blocker）/ P1-4（读裁剪）/ P2-5..8（DB-DDL/CTAS）/ P3-9..12（写并行/读默认/minors）共 **12 issue 全 DONE**，逐条见下面 🔴/🟠/🟡 段。剩余工作不再是「修 issue」，而是三条收尾线：
> 👉 **下一 session 第一步（按价值/依赖排序）**：
> 1. **🅰 live e2e 终验（真实 ODPS）= 翻闸真正完成门**（最高价值，CI 跳）。所有静态修复的真值闸须 live 验：写 blocker（动态/静态分区、INSERT OVERWRITE，DV-013/014）+ 读裁剪（DV-015）+ limit-split（DV-016）+ DESCRIBE isKey（DV-017）+ post-commit swallow（DV-018）+ batch-mode 大分区（DV-019）+ CAST 谓词不丢行（DV-020：STRING 列 `"5"/"05"/" 5"` 的 `CAST(code AS INT)=5` 返回全部 3 行）。**需真实 ODPS 环境/凭证**——多半要用户提供或在带 ODPS 的环境跑。runbook 见历史 HANDOFF / decisions-log。
> 2. **🅱 Batch-D = 删 legacy MaxCompute（21 文件）**。**所有 per-fix 红线门现已全清**（P0 写分发/overwrite/bind + P1 读裁剪 + P3-11 batch-mode），故 Batch-D 已**解锁**；但执行仍**gated on 🅰 live e2e**（[D-027]）。设计 = `plan-doc/tasks/designs/P4-batchD-maxcompute-removal-design.md`（其 §1「zero survivor」声明已就 MaxComputeScanNode 加红线限定，仍须复查 PhysicalMaxComputeTableSink/allowInsertOverwrite/bindMaxComputeTableSink 三处，见 §横切）。
> 3. **🅲 横切开放项**（静态、不需 ODPS，可随时清，见下）。
>
> 📋 **待用户拍板 / 待清的开放项**：
> - **(决策) P2-7 KNOWN PRE-EXISTING GAP**：非-IFNE + FE-cache 命中但远端缺 → legacy 抛 `ERR_TABLE_EXISTS_ERROR`、cutover 静默建表。全 parity 可在 `PluginDrivenExternalCatalog.createTable` 的 `exists && !isIfNotExists()` 加 FE 侧 throw。**待定 fix vs 接受+DV**（见 FIX-CTAS review-rounds）。
> - **(doc-sync 欠账 — P2 session 遗留，已核实仍未落)**：decisions-log 登记 P2 三处 SPI 改动（4 参 `dropDatabase` / `supportsCreateDatabase` / `ConnectorColumn.isAutoInc`）；deviations-log 登记（P2-7 非-IFNE 文案差、CTAS KNOWN GAP、P2-8 auto-inc 接受项）；更正 `P4-maxcompute-migration.md` 的「nereids 上游已拒 auto-inc」假声明（P2-8 已证伪：nereids 仅拒 generated 列、不拒 bare auto-inc）；T06c §5「记 OQ/可接受」措辞。**注：P3-9/P3-10 的 doc-sync（D-032/D-033/DV-016/DV-017）本 session 已落。**
> - **(复查) F9 CAST 谓词剥壳下推**（`ExprToConnectorExpressionConverter:108-109`, confirms 3/3, correctness/丢行风险）：虽归「已登记降级」，建议二次确认真安全 / 真已登记。
> - **(终验) live e2e（真实 ODPS）是翻闸真正完成门**（= 上面 🅰）：写 blocker（动态/静态分区、INSERT OVERWRITE）+ 读裁剪 + limit-split + DESCRIBE + post-commit swallow + batch-mode 大分区 + CAST 谓词不丢行 的 DV 真值闸（**DV-013..020**）须 live 验，CI 跳。

> 来源全部出自 `plan-doc/reviews/P4-maxcompute-full-rereview-2026-06-07.md`（每条带 `file:line` + cutover↔legacy diff + 处置建议 + 历史交叉核对证据）。下面是浓缩可执行清单——**动手前按指针核码（Rule 8）**。
> **⚠️ 把 newGaps∪disagreements 当一个"必须 triage"集**：同一根因被两个审阅者按各自查到的历史 artifact 分别归 new-gap / disagreement（静态分区 bind F19=F48；CREATE DB 预检 F23=F26），别被 status 标签的细分误导。
> **每 issue 走既有流程**：设计→改→编译+UT+mutation→对抗 review 收敛→独立 commit + hash 回填。

## 🔴 P0 — 写路径 3 个 blocker（✅ 全清，2026-06-07）

- [x] **FIX-OVERWRITE-GATE**（blocker, F42/F47）✅ **DONE @`59699a62f33`**（本轮 live tracker = `plan-doc/task-list-P4-rereview.md`；详见 `plan-doc/reviews/P4-T06e-FIX-OVERWRITE-GATE-review-rounds.md`）。⚠️**下面这句已过时**：实际未用 bare instanceof（round-1 对抗 review 证伪——会令 jdbc 静默退化 overwrite→plain INSERT 丢数据），改为 **Option A：新增 SPI capability `supportsInsertOverwrite()`（ConnectorWriteOps 默认 false / MaxCompute=true），网关经能力守门**。〔原始计划：〕`InsertOverwriteTableCommand.allowInsertOverwrite:315-323` 加 `PluginDrivenExternalTable` 分支（keyed on SPI 泛型类型，对齐 FIX-PART-GATES 决策①）。下层 OVERWRITE 机器(`:420-440`)已完整接好、只是被顶层网关挡得到不了（典型"分发只接一半"）。**Batch-D 红线**：删 legacy `MaxComputeExternalTable` 分支前必须先加 PluginDriven 分支。测试(Rule 9)：翻闸表 INSERT OVERWRITE 修前红(`AnalysisException "...only support OLAP..."`)、修后过网关 + 静态分区 spec 仍流。
- [x] **FIX-WRITE-DISTRIBUTION**（blocker+major, F17/F18/F43）✅ **DONE @`f0adedba20c`**（1 轮收敛 0 must-fix；详见 `plan-doc/reviews/P4-T06e-FIX-WRITE-DISTRIBUTION-review-rounds.md`、[D-029]/[DV-013]）。做法 = **Option A：新增 SPI capability `SINK_REQUIRE_PARTITION_LOCAL_SORT`**（`ConnectorCapability` 默认不声明 / MaxCompute `getCapabilities()` 声明它 + `SUPPORTS_PARALLEL_WRITE`），`PluginDrivenExternalTable.requirePartitionLocalSortOnWrite()` 读之，`getRequirePhysicalProperties()` 重写 legacy 3 分支。**关键修正 vs legacy**：分区列→child output 索引按 **cols 位置**（通用 sink child 投影到 cols 序）非 legacy full-schema。〔原始计划：〕`PhysicalConnectorTableSink.getRequirePhysicalProperties:114-121` 照搬 legacy `PhysicalMaxComputeTableSink:111-155` 三分支。**⚠️ 不只翻 `SUPPORTS_PARALLEL_WRITE`**——那缺 local-sort，动态分区照样 "writer has been closed"。**Batch-D 红线**：删 `PhysicalMaxComputeTableSink`（唯一逻辑副本）须待本 fix + P0-3 双落。**真值闸**：live e2e 跨多动态分区无 "writer has been closed" + 并行吞吐（CI 跳，须与 P0-3 一并 live 验）。
- [x] **FIX-BIND-STATIC-PARTITION**（blocker, F19/F48）✅ **DONE @`7cc86c66440`**（3 轮收敛 0 mustFix；[D-030]/[DV-014]；详见 `plan-doc/reviews/P4-T06e-FIX-BIND-STATIC-PARTITION-review-rounds.md`）。⚠️**下面原始计划不完整**——只剔除静态分区列不够：MaxCompute BE/JNI writer **按位置**映射数据到完整表 schema，故**所有** MC 写（不止静态/分区）须投影 full-schema 序（非分区/重排或部分显式列名否则静默错列/丢列）。实际做法 = **新增 SPI cap `SINK_REQUIRE_FULL_SCHEMA_ORDER`**（MaxCompute 声明 / JDBC 不声明），`bindConnectorTableSink` 据此分支（true→full-schema 投影镜像 legacy `bindMaxComputeTableSink` 全写形 + 剔除静态分区列;false→cols 序 JDBC/ES）+ `InsertUtils` VALUES 分支 + **回退 P0-2 分布索引 cols→full-schema**（[D-030] 回退 [D-029]）。判别键三轮 static→partitioned→capability。〔原始计划：`BindSink.bindConnectorTableSink` 剔除 `getStaticPartitionKeyValues().keySet()` + `InsertUtils:377-389` VALUES 分支〕。**doc-sync 已落**：cutover-design §4.2 + FIX-WRITE-DISTRIBUTION-design「index-by-cols」superseded 更正（随本 session commit）。**Batch-D 红线**：删 legacy `bindMaxComputeTableSink`/`PhysicalMaxComputeTableSink` 须待本 fix 落（已落）。**真值闸**：live e2e（p2 `test_mc_write_insert` Test 3/3b + `test_mc_write_static_partitions`）；bind 投影无 fe-core analyze harness 单测 = DV-014。

## 🟠 P1 — 分区裁剪下推证伪（disagreement, major）✅ DONE 2026-06-08

- [x] **FIX-PRUNE-PUSHDOWN**（F1/F7）✅ **DONE @`072cd545c54`**（1 轮收敛 0 mustFix；[D-031]/[DV-015]；详见 `plan-doc/reviews/P4-T06e-FIX-PRUNE-PUSHDOWN-review-rounds.md`）。**用户批准「Fix it」**。做法 = (a) `PluginDrivenScanNode` 加 `selectedPartitions` 字段/setter + 三态 `resolveRequiredPartitions`（NOT_PRUNED→null / pruned-非空→names / pruned-空→`getSplits` 短路无 split，镜像 legacy `MaxComputeScanNode:718-731`）；`PhysicalPlanTranslator` plugin 分支注入 `setSelectedPartitions(fileScan.getSelectedPartitions())`；(b) **additive 6 参 SPI overload** —— `ConnectorScanPlanProvider.planScan(...,List<String> requiredPartitions)` **default** 委托 5 参（零破坏 es/jdbc/hive/paimon/hudi/trino，唯 MaxCompute override），MaxCompute `toPartitionSpecs` 喂**两** read-session 路径（标准 `:201` + limit-opt `:320`，替 `Collections.emptyList()`），空选短路上移 fe-core。**契约**：null/空=全部、非空=子集、零分区 fe-core 短路不下达 SPI。**已更正**「production CLEAN / pruning 不变式 clean」裁决（FIX-PART-GATES design/review-rounds ⚠️ + D-028 ⚠️，见 doc-sync）。**Batch-D 红线**：删 legacy `MaxComputeScanNode`（读裁剪逻辑副本）须待本 fix 落（已落）。**真值闸**：live e2e p2 `test_max_compute_partition_prune.groovy` + EXPLAIN/profile 证仅扫目标分区（DV-015；CI 跳）。**与 NG-7 batch-mode 解耦但为其前置。**

## 🟠 P2 — DB-DDL / CTAS 语义回归 ✅ 全 DONE（P2-5/6/7/8，详见 task-list-P4-rereview.md + 4 份 review-rounds）

- [x] ✅ `99d5c9d527c` **DROP DB FORCE 级联**（disagreement major, F22/F27）：先用真实 ODPS 验 `schemas().delete` 对非空库行为。若拒删 → 在 `PluginDrivenExternalCatalog.dropDb:337-355` 的 `force==true` 时枚举+dropTable（或扩 SPI 带 force/cascade）。若不支持 → 至少 fail-loud（force+非空库抛明确错）+ 登记 deviation。**别把 T06c §5"记 OQ/可接受"当作已解决**（后续对抗 review 已推翻该定级）。
- [x] ✅ `ff52f8fd478`（能力门闸 supportsCreateDatabase，jdbc/es/trino 字节不变）**CREATE DB IF NOT EXISTS 远端预检**（disagreement major, F26/F23）：重开 DDL-C4。`createDb:312-326` 在 `ifNotExists && getDbNullable==null` 时先查 `connector...databaseExists`（已暴露、无需改 SPI 签名）。UT + mutation。或登记 deviation——别留"孤儿修 verdict"（task-list `:12` 称 6/6 完成但此条无 fix commit、亦无 deviation）。
- [x] ✅ `7051b75c197`（FE-only；⚠️ 暴 KNOWN PRE-EXISTING GAP：非-IFNE+本地-only 不 fail-loud，待用户定）**CTAS IF-NOT-EXISTS 误写已存在表**（disagreement, DDL-C5 minor→**major**, F33）：`createTable:264-300` 区分"新建 vs 已存在"——IF-NOT-EXISTS 命中 → 返回 true + 跳 editlog + 跳 `resetMetaCacheNames`（镜像 legacy `createTableImpl:179-197` → `ExternalCatalog:1063-1075`）。测试：CTAS-IF-NOT-EXISTS 对已存在表**不**INSERT + editlog 未写。（历史只分析了 editlog 冗余那半、漏了数据变更后果。）
- [x] ✅ `4aa680f3e3b`（加 SPI 字段 ConnectorColumn.isAutoInc）**AUTO_INCREMENT 拒绝丢失**（disagreement minor, F24）：定夺 (a) `ConnectorColumn` 加 `isAutoInc` 透传 + `validateColumns` 重校验；或 (b) 接受+登记 deviation + 更正 `P4-maxcompute-migration.md:117` 的假声明（"nereids 上游已拒"对 auto-inc 为假）。聚合列那半已被非-OLAP key 路径覆盖、无需单独修。

## 🟡 P3 — 写并行 / 读默认 / minors

- [x] **limit-split 默认反转**（major, F11）✅ **DONE @`952b08e0cc8`**（1 轮 impl-review 收敛，1 mustFix→补测；[D-032]/[DV-016]；详见 `plan-doc/reviews/P4-T06e-FIX-LIMIT-SPLIT-DEFAULT-review-rounds.md`）。**用户定 Fix（恢复三重闸）**。做法 = **连接器局部、无 SPI 变更**：① 加 hardcode 常量 `ENABLE_MC_LIMIT_SPLIT_OPTIMIZATION` 经 `ConnectorSession.getSessionProperties()`（live 由 `from(ctx)`→`VariableMgr.toMap` 填，禁依赖 fe-core `SessionVariable`，同 JDBC 约定）读 gate(1)；② 实 `checkOnlyPartitionEquality` 遍历 `ConnectorExpression` 树镜像 legacy `checkOnlyPartitionEqualityPredicate`；③ 纯静态 `shouldUseLimitOptimization` 合成 gate(1)&&gate(3)&&gate(2)，默认 OFF=保守回退 legacy。**并闭 minors F2/F12**（旧恒 false stub）。〔原始计划：透传 session-var + 实现 checkOnlyPartitionEquality 恢复三重闸；或接受"默认优化无过滤 LIMIT"+DV〕。**真值闸**：CI-skip live e2e（var OFF→多 split / var ON+分区等值+LIMIT→单 row-offset split，EXPLAIN/profile 证）= DV-016 wiring 半。
- [x] **isKey=false 元数据分歧**（minor, F3/F10）✅ **DONE @`1b44cd4f065`**（设计验证+impl review 各 0 mustFix；[D-033]/[DV-017]；详见 `plan-doc/reviews/P4-T06e-FIX-ISKEY-METADATA-review-rounds.md`）。**用户定 Fix（isKey=true）**。做法 = **连接器局部、无 SPI 变更**：抽 `buildColumn(...)` 静态助手用 6 参 ctor 置 isKey=true，`getTableSchema` data+partition 两 loop 经之（converter 已透传 isKey）。**作用域更正**：仅影响 `DESCRIBE`（`information_schema.columns.COLUMN_KEY` 受 `FrontendServiceImpl:962-965` OlapTable 门控、MC 前后皆空、已 parity）；isKey 非纯展示（亦喂 `UnequalPredicateInfer`/BE descriptor）但 legacy 即喂 true→恢复既有值。〔原始计划：两列循环改 6 参 `ConnectorColumn(...,true)`；或接受+DV〕。**真值闸**：CI-skip live e2e `DESCRIBE <mc_table>` 显 Key=YES（wiring 半，DV-017）。
- [x] **丢 batch-mode 异步 split**（minor, F6/F13）✅ **DONE @`ac8f0fc15eb`**（[D-035]/[DV-019]；详见 `tasks/designs/P4-T06e-FIX-BATCH-MODE-SPLIT-design.md` + 设计验证 `wcpg9lblj` / impl-review `wve7y1jst` 各 GO-WITH-EDITS）。**用户定「实现 batch SPI 路径」（Shape A 薄 SPI + fe-core 编排、逐字镜像 legacy）**：① SPI `ConnectorScanPlanProvider` +2 additive default（`supportsBatchScan` false / `planScanForPartitionBatch` 委托 6 参 planScan）零破坏其余 6 连接器；② 连接器 `supportsBatchScan`=`odpsTable.getFileNum()>0`；③ fe-core `PluginDrivenScanNode`（已继承 batch dispatch+stop，`PluginDrivenSplit extends FileSplit` 故 `:381` 转型安全）override `isBatchMode`(4 闸+SF-1 null-guard)/`numApproximateSplits`/`startSplit`(getScheduleExecutor outer/inner CompletableFuture + SplitAssignment 契约，DEC-1 不下推 limit 传 -1) + 抽纯静态 `shouldUseBatchMode`。守门：编译/fe-core UT 9-9/fe-connector-api UT 2-2/checkstyle 0/import-gate/mutation 5-5 向红。**Batch-D 红线**：本 fix 落地才解锁删 legacy `MaxComputeScanNode` batch 逻辑副本（读裁剪那半 P1-4 已清，本项为最后前置闸；已在 `P4-batchD-maxcompute-removal-design.md` 加限定注）。**真值闸**：大分区 live e2e（EXPLAIN/profile 证 batched/streamed、耗时/内存≪同步路）=DV-019、CI 跳。**🎉 P3 全清。**
  - **operational 坑（auto-memory 已记）**：mutation 跑中 `/mnt/disk1` 系统级满（非本 repo）致 cp 还原失败一度 truncate 产线文件→已从 `/dev/shm` 备份还原；教训=mutation 备份须放 RAM/异盘。
- [x] **post-commit refresh 吞异常**（minor, regression=no, F15=F21）✅ **DONE @`1f2e00d3696`**（[D-034]/[DV-018]；详见 `tasks/designs/P4-T06e-FIX-POSTCOMMIT-REFRESH-design.md`）。**用户定 DV+Javadoc 泛化、不回退 legacy 传播失败**。**无产线逻辑改动**：仅 `PluginDrivenInsertExecutor.doAfterCommit` 的 Javadoc（`:164-176`）从「只讲 JDBC_WRITE」泛化到覆盖 connector-transaction(MC) 路径——两路径数据在 doAfterCommit 时均已持久、`super.doAfterCommit`(=`handleRefreshTable`) 只刷 FE 缓存 + 写 external-table refresh editlog（follower 失效提示、非数据真相源）、丢失只致 follower 缓存暂 stale 自愈。对抗性安全核查 inline 0 mustFix。守门 checkstyle 0、import-gate 净。**真值闸**：CI-skip live e2e（MC INSERT 提交后人为令 refresh 失败→断言报 OK+warn）。

## ⛓️ 横切 / 别忘

- [ ] **Batch-D 红线扩充**：删 legacy 前须先在 PluginDriven/connector 路径补齐 → `PhysicalMaxComputeTableSink`(写分发唯一副本)、`allowInsertOverwrite` 的 MC 分支、`bindMaxComputeTableSink` 静态分区过滤、**`MaxComputeScanNode` 读裁剪下推（P1-4 已补 plugin 侧）**。复查 Batch-D 设计对这些文件的"zero survivor"声明（连同既有 `PartitionsTableValuedFunction` 红线）。
- [x] **F9 CAST 剥壳下推复查** ✅ **DONE @`cc32521ed99`**（[D-036]/[DV-020]；详见 `tasks/designs/P4-T06e-FIX-CAST-PUSHDOWN-design.md`）。**复查推翻 review 的「已登记降级」定级**：对抗核验 `wzoa6dkvw` **0/3 refuted**、verdict=**real-unregistered-regression**——MaxCompute 继承 `supportsCastPredicatePushdown=true`、剥壳谓词推 ODPS 源端 under-match（`CAST(str AS INT)=5`→`str="5"` 丢 `'05'/' 5'`）、BE 复算无法找回源端已丢行；legacy 丢弃 CAST 谓词（BE-only）故正确 ⇒ **回归**（非 DV-016 的 limit-opt 资格 CAST-unwrap）。**用户定 Fix**：① 连接器 `supportsCastPredicatePushdown→false`（激活既有 strip、恢复 legacy parity）；② fe-core `getSplits` 剥壳时抑制 source LIMIT（impl-review `wj2h0120n` F9-LIMITOPT-1：否则空 filter 触发 limit-opt under-return）。守门 连接器 UT2-2+mut / fe-core LimitStrip2-2+BatchMode9-9+mut2-2 / checkstyle 0 / import-gate。真值闸 live ODPS=DV-020。**out-of-scope surface**：JDBC `applyLimit`+cast-off 理论同类（MC 不 override applyLimit、本修对 MC 完整），DV-020 备查。
- [~] **doc-sync**：P0-1/P0-2/P0-3 + **P1-4 已落并 commit**（decisions-log D-027..D-031、deviations-log DV-013/DV-014/DV-015、cutover-design §4.2、FIX-WRITE-DISTRIBUTION-design index-by-cols superseded、**FIX-PART-GATES design/review-rounds「pruning 不变式 clean」⚠️ 更正 + D-028 ⚠️ 补注（DG-1✅）**、本 HANDOFF、task-list）。**剩余（随 P2+ 处理）**：DG-2 证伪 DECISION-3「忠实镜像」、DG-4/DG-6 task-list「6/6 完成」措辞，各 P2+ 项落地时同步 design/log。

---

## ⚙️ 操作须知(无结论,纯工程)
- **maven 必绝对 `-f` + `-pl :artifactId`**:改 fe-core 带 `:fe-core -am`;改连接器带 `:fe-connector-maxcompute`。读真实 `BUILD SUCCESS/FAILURE` 与尾部 `echo "MVN_EXIT=$?"`;**勿信**后台 task-notification 的 exit code。
- **build cache 坑**:守门/跑测带 `-Dmaven.build.cache.enabled=false`,否则会 restore 旧 build 且 **surefire XML 可能 stale**(前序 session 多次踩到:mutation 跑出 BUILD FAILURE 但读到旧 XML 显示 0 fail)。直接读 mvn 输出的 `Tests run:` 行,别只读 XML。
- **checkstyle**:`-pl :fe-core checkstyle:check`;`CustomImportOrder`(doris→第三方[com.*/org.* 非 doris]→java)/`UnusedImports`/`LineLength 120`;扫 test 源。
- **import-gate**:`bash tools/check-connector-imports.sh`(repo 根跑)。
- **分支**:`catalog-spi-05`,本地;未跟踪 `.audit-scratch/` `conf.cmy/` `regression-conf.groovy.bak`(勿提交)。
- **mutation 验证技巧**:改产线一处→跑相关 UT→确认对应 test 变红→还原。用 `cp` 备份产线文件做 mutation(比 perl 删块安全——perl 易匹配到首个同名 `if` 误删方法)。

## 🧠 给下一个 agent 的 meta
- **live e2e(真实 ODPS)仍是翻闸真正完成门**——本复审是静态代码层面的高置信判定,**不替代 e2e**;写路径 blocker(动态/静态分区 / INSERT OVERWRITE)最终须 live 验。runbook 见 `git show` 历史 HANDOFF 或 decisions-log。
- 复审脚本可复用:`plan-doc/reviews/maxcompute-full-rereview.workflow.js`(clean-room 编排,Phase A/B 只读码、Phase C 解禁先验;args 可调 `verifyVotes/lensesPerDomain/includeBe`)。clean-room 偏好见 auto-memory `clean-room-adversarial-review-pref`。
- 先验/历史交叉核对账(P4-T06d designs/reviews、cutover-fix-design、decisions/deviations-log、task-list)即将随上述修复更新——改前先读对应条目(Rule 8)。

</details>
