# 🤝 Session Handoff

> 滚动文档：每次 session 结束**覆盖式更新**（不累积历史；历史见 `git log plan-doc/HANDOFF.md`）。协作规范：[AGENT-PLAYBOOK.md](./AGENT-PLAYBOOK.md)
> **范围** = catalog-spi **主线** handoff。metastore/storage 抽取子线**已 CLOSED**（2026-06-22 合入 #64446/#64653/#64655）——[`metastore-storage-refactor/`](./metastore-storage-refactor/) 仅历史留存、勿读；metastore-spi 现状直接读代码 `fe/fe-connector/fe-connector-metastore-spi/`。

---

# 🎯 下一个 session 的任务 = **C4 步骤 R8（wire + flip rehearsal，flip-gated，本地不 commit SPI_READY_TYPES）**

**本 session = C4 R7（WHERE lowering — nereids Expression→ConnectorPredicate）= ✅ DONE（单提交 `5a1a0e25e16`，9 文件，dormant）**。**P6.1–P6.5 = ✅ 全 DONE**。**P6.6：C1 ✅ / C2 ✅ / C3a ✅ / C3b-pre ✅ / C3b-core+commit-bridge 全闭 ✅ → C4 进行中（R1–R7 ✅，R8 待）**。翻闸 = 5 commit-stream（C1/C2/C3 ✅ / C4 进行中 / C5 FLIP 不可逆待）。iceberg **仍不在** `SPI_READY_TYPES`。

## 📖 起步必读（动 R8 前）
1. **`plan-doc/tasks/designs/P6.6-C4-ws-rewrite-design.md`** §7 R8 + §8 impl 期 OPEN（行号会漂，动码前 re-grep）。
2. 本 HANDOFF「R7 完成」+「R8 起步」+ FU 清单（R8 rehearsal 会真触及 FU-rewrite-output-sizing / FU-flip-e2e）。
3. memory `r6-rewrite-driver-beginwrite-once`（R6 两个正确性坑 + 完整分布式写工作集锚点，R8 真跑时对照）。

## ✅ 本 session 完成（C4 R7，单提交 `5a1a0e25e16`，dormant）
> recon = `wf_46e2c61c-ee2`（3 reader〔2 个因 StructuredOutput 重试上限失败〕+ synthesis + adversarial critic）；critic 实证挖出「连接器 conflict-mode 矩阵是 legacy 严格子集→fail-loud 须落两层」的 BLOCKER。**11 改 + 2 新**。

- **用户裁决（2026-06-27，signed）**：rewrite 的 WHERE「**无法精确下推为文件裁剪就报错**」——绝不静默扩大重写集（不走「尽力下推+变宽」）。
- **关键发现（推翻 HANDOFF/设计旧「复用 NereidsToConnectorExpressionConverter」方案）**：EXECUTE 的 WHERE 全程 **UNBOUND**（`LogicalPlanBuilder.visitAlterTableExecute` 只 `visit` 不 analyze；`ExecuteActionCommand` 只 analyze 表名）。bound-slot 的 `NereidsToConnectorExpressionConverter` 会把每个 `UnboundSlot` 叶子静默丢→整条 WHERE 为空→**重写全表**（灾难）。连接器 `IcebergPredicateConverter` 按列名从 iceberg schema 解析、**不读** `ConnectorColumnRef` 类型→fe-core 只需列名（类型仍按表 schema 填，保持诚实）。
- **两层 fail-loud 设计**（critic BLOCKER：conflict-mode 是 legacy 节点集严格子集，跨列 OR/NOT 比较被丢→单 fe-core 不够）：
  1. **fe-core 新 `datasource/UnboundExpressionToConnectorPredicateConverter`**：unbound-aware 按列名解析（含单段 `UnboundSlot`，仿 legacy `IcebergNereidsUtils.extractColumnName`）+ 表 `getColumn(name)` 取 Doris 类型→`ExprToConnectorExpressionConverter.typeToConnectorType`；镜像 legacy 节点集（And/Or/Not/EQ/GT/GE/LT/LE/IN/IsNull/Between）；**all-or-nothing fail-loud**（任一节点不可表示/未知列/多段列名→抛 `AnalysisException`，绝不产部分/空谓词）；字面量经 `ExprToConnectorExpressionConverter` 与 scan/conflict 路字节一致。**iron-law clean**（无 instanceof Iceberg / IcebergUtils）。
  2. **连接器 `RewriteDataFilePlanner.planFileScanTasks`**：WHERE 未完整下推（`pushed < countTopLevelConjuncts`）→ 抛 `DorisConnectorException`（取代 DV-T05r-where 的静默丢/变宽）；新 `countTopLevelConjuncts` 助手。
- **派发拆分**：`ConnectorExecuteAction.execute` 先取 `getExecutionMode`，WHERE 拒绝按模式拆——SINGLE_CALL（8 纯 SDK 过程）仍拒；DISTRIBUTED 降 WHERE 为 `ConnectorPredicate` 传 driver。`ConnectorRewriteDriver` 加 `ConnectorPredicate` 字段并透传 `planRewrite`（原 hardcode null）。
- **语义注**：常见 WHERE（等值/范围/IN/IS NULL/BETWEEN/同列 OR/AND 串联）全部精确执行；罕见不可下推形式（跨列 OR/NOT 比较/NE/函数/列-列/未知列/多段列名）现报清晰错误（比 legacy 略严：legacy 支持的跨列 OR/NOT 比较现也报错，用户接受）。deviations-log DV-T05r-where 已更新为 rewrite 路 fail-loud。

## ✅ 本 session 验证
- fe-core **28/0**（`UnboundExpressionToConnectorPredicateConverterTest` 10、`ConnectorExecuteActionTest` 15〔含 SINGLE_CALL 拒 + DISTRIBUTED 降并 ArgumentCaptor 验透传〕、`ConnectorRewriteDriverTest` 3〔含 predicate 透传 planRewrite〕）；连接器 `RewriteDataFilePlannerTest` **19/0**（旧 `unconvertibleCrossColumnOrWidensScan`→`unconvertibleCrossColumnOrThrows` + 新 `partiallyPushableWhereThrows`）。**fresh clean recompile 复核绿**（曾遇 mutation 后 os.utime 致 stale .class 假红→`touch` 源强制重编后绿）。
- **6 变异（Rule 9/12）全 KILLED**：M1 converter fail-loud / M2 AND all-or-nothing / M4 连接器 guard / M5 driver 透传 / M6 SINGLE_CALL 拒 / M7 DISTRIBUTED 降。脚本 `scratchpad/mutate_r7.py`（已 restore，无 .mutbak / 无 if(false) 残留，已核）。
- iron-law（3 fe-core 新/改文件无 instanceof Iceberg / IcebergUtils）+ 连接器 import gate **clean**。
- **真分布式 WHERE rewrite e2e 未跑**（flip-gated）= R8 rehearsal 才触及（诚实标注，勿谎称）。

## 🚦 R8 起步（wire + flip rehearsal，flip-gated；**SPI_READY_TYPES 改动留 C5，本地试跑不 commit**）
> R8 = 第一次让**整条**分布式 rewrite 真路径端到端跑（pin/共享事务/OCC commit/GATHER/register/WHERE）。需 docker iceberg 环境。
1. **本地（不 commit）** 把 iceberg 临时加进 `SPI_READY_TYPES`（`CatalogFactory:50-51`，现 = {jdbc,es,trino-connector,max_compute,paimon}），跑 docker `rewrite_data_files`（含 / 不含 WHERE）e2e。**跑完 revert**，勿提交该改动。
2. 端到端验（FU-flip-e2e 清单）：旧删不复活 / operation·row_id BE 解析 / OCC / **每组只扫自己文件〔pin 3 注入点〕/ 共享事务跨组绑定 / 并发 begin-once / register 顺序 / GATHER 输出文件数 / register re-scan 路径匹配 / WHERE 真裁剪到匹配文件 / 不可下推 WHERE 真报错**。
3. **R8 触及的 FU（rehearsal 前先看）**：**FU-rewrite-output-sizing**（target-file-size + 自适应并行度未线程，影响真 BE 写盘输出文件数/大组性能）；翻闸前须修。
4. R8 暴露问题 → 回头补连接器/driver；R8 本身**不动 `SPI_READY_TYPES` 提交**（那是 C5）。

---

# ⚠️⚠️ 用户铁律：**fe-core 不得 `if(iceberg)` / `instanceof Iceberg*` / `import IcebergUtils`（新 seam）**
iceberg 逻辑落 `fe-connector` 经中立 SPI。**legacy 豁免类**（C4 dead 子树 `IcebergRewriteDataFilesAction`/`RewriteDataFileExecutor`/`RewriteGroupTask`/`IcebergRewriteExecutor` + commit-bridge 旧清单 + `PhysicalIcebergTableSink`/`bindIcebergTableSink` + `StatementContext` 旧 iceberg-typed stash）保留 iceberg 引用合法。**R6/R7 新增通用类**（`ConnectorRewriteDriver`/`ConnectorRewriteGroupTask`/中立 stash/`applyRewriteFileScope`/`pinRewriteFileScope`/dispatch 按 `executionMode`/sink isRewrite 串 `WriteOperation.REWRITE`/**R7 `UnboundExpressionToConnectorPredicateConverter` 按列名 + 表 schema**）全经中立 SPI，**无** instanceof Iceberg（已核）。

---

# 🔴🔴 开放 — P6.6 翻闸（C1+C2+C3 全闭，C4 进行中，C5 待）

> 5 commit-stream（C1 ✅ / C2 ✅ / C3 ✅ / **C4 进行中** / C5 FLIP 待）。

- **[C4 进行中 = 当前]** rewrite_data_files 翻闸就绪（Option B 全对等）。**R1–R7 ✅（executionMode SPI / scan path-set 作用域 / planRewrite SPI / sink-bind+GATHER / transaction rewrite SPI gap / 分布式 driver+CRUX stash 中立化+begin-once 护栏 / WHERE lowering 两层 fail-loud）→ R8（flip rehearsal，flip-gated）**。详设计 §7。
- **[C5 FLIP，不可逆]** `SPI_READY_TYPES`+iceberg / 删 `CatalogFactory case` / GSON compat / capability 核 / Show* parity。**C5 前须 C1–C4 全绿 + 用户二签。**

## 🆕 翻闸前置项（登记）
- **[GAP-A → C5]** 翻闸后 iceberg 表类掉出 `MaterializeProbeVisitor.SUPPORT_RELATION_TYPES`→ lazy-top-N 静默失效。修须 capability/engine 判别。
- **[GAP-B = C3b-core ③] ✅** 隐藏列注入已闭。

**[pre-flip 行为偏差中央登记]**：P6.4=DV-045/046/047；P6.5=DV-048/049；commit-bridge=[DV-S2-rederive]。**C4 R1–R6 无新 DV**（dormant）。**R7：DV-T05r-where 更新**（rewrite 路从「静默丢/变宽」改为 fail-loud——撤销静默丢；常见 WHERE 零差异，罕见不可下推形式现报错；行为更安全，仍 dormant、零 live 变更）。

**⚠️ C5 才动 `SPI_READY_TYPES`**（`CatalogFactory:50-51`，现 = {jdbc,es,trino-connector,max_compute,paimon}）。

---

# 🟡 已登记 follow-up（非阻塞，勿在 C4 增量做）
- **[FU-rewrite-output-sizing]（R6 登；R8 必触及）** 中立 driver **未**线程 target-file-size + 自适应并行度（legacy `RewriteGroupTask` 经 iceberg 会话变量 `iceberg_write_target_file_size_bytes`〔TQueryOptions 字段，非 sink〕传，并按 `totalSize/targetSize` vs availableBe 选 GATHER/parallelism）。R6 各组一律经 sink `isRewrite`→GATHER 收单写者（正确但大组慢、输出文件不按大小调优）。**仅影响真 BE 写盘（R8 rehearsal 触及）**。翻闸前修：planRewrite 出 target-file-size（中立 wrapper 或 group 字段）+ driver 设会话变量 + 复算并行度；勿让 fe-core 解析 iceberg 属性名。
- **[FU-flip-e2e]（R7 扩）** commit-bridge + C4 R1–R7 全程 pre-flip UT 锁，但真翻闸端到端（旧删不复活 / operation·row_id BE 解析 / OCC / **rewrite 每组只扫自己文件〔pin 3 注入点〕/ 共享事务跨组绑定 / 并发 begin-once / register 顺序 / GATHER 输出文件数 / register re-scan 路径匹配 / WHERE 真裁剪 / 不可下推 WHERE 真报错**）**未跑**（CI-gated/flip-gated，勿谎称）。
- **[FU-connector-bind-visibility]** 见 git log 历史（R4 范围外，翻闸前若需对齐普通插入语义引中立「是否 row-lineage 写入列」表能力，禁 `IcebergUtils.isIcebergRowLineageColumn`）。
- **[FU-rewrite-rescan-perf]** R5 `registerRewriteSourceFiles` commit 前 re-scan `planFiles()`（O(表文件数)）；翻闸后大表慢可按 queryId 缓 DataFile（仿 rewritableDeleteStash）。
- **[FU-broker-write]** 连接器三 write builder 未填 `setBrokerAddresses`；翻闸前若需 broker 写盘三 builder 一并补。
- **[FU-getRowIdColumn]** `IcebergMergeCommand.getRowIdColumn(562)` 仍 `IcebergExternalTable`；翻闸/P6.7 核是否 dead。
- **[FU-where-literal-coercion]（R7 critic INFERRED）** legacy 按 iceberg **列**类型 coerce 字面量（`extractNereidsLiteralValue(literal, nestedField.type())`），新中立路按**字面量**自身类型 coerce（`IcebergPredicateConverter.extractIcebergLiteral` 读 `literal.getType()`）——跨类型比较（如 DATE 列 = 整数字面量）两路可能产不同 iceberg 值。**非 R7 引入**（既有连接器属性，DV-T05r-where 未列此轴）；R8 rehearsal 测跨类型 WHERE 字面量；勿在 fe-core 按列类型 coerce（会与连接器分叉，更糟）。

---

# ⚙️ 操作须知（复用）

- maven：`-f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl :<art> **-am** -DfailIfNoTests=false -Dmaven.build.cache.enabled=false`（漏 `-am`→假错 `${revision}` 解析不了）。**连接器模块**（`fe-connector-api`/`-iceberg`）build 快（~30s），可前台；**fe-core -am 单类首次 ~2-3min**→`run_in_background:true` 再读 surefire **XML**/日志。**全量前 `rm -f target/surefire-reports/TEST-*.xml`**。后台 build 日志里 `mismatched input '->'`/`which: syntax error` = gensrc codegen 噪声，非编译错（看 `BUILD FAILURE`/`cannot find symbol`）。
- **⚠️ stale .class 假红坑（R7 实遇）**：mutation 脚本 `os.utime` 把源 mtime 复原成旧值后，`target/.class`（mutation build 编的）比源新→maven 跳过重编→跑到旧（mutated）.class→**假红**。修=`touch` 改过的源（mtime→now）再 build，或 `mvn clean`。**commit 前的最终验证务必 fresh recompile**（touch 源或 clean）。
- **⚠️ checkstyle 全量 build 跑**：import 同组无空行 + 组内**字母序**（大写类在小写子包前：`connector.api.ConnectorType` 在 `connector.api.pushdown.*` 前）。unused-import 也会红。加 import 后 `mvn checkstyle:check -pl :<mod>` 快验，或 mutation 跑加 `-Dcheckstyle.skip=true`。
- 连接器禁 import fe-core：`bash tools/check-connector-imports.sh`。测试：连接器**无 Mockito**（真 InMemoryCatalog；`RewriteDataFilePlannerTest` 范式建表/谓词）；fe-core **Mockito**（`mockito-inline`，mock `ConnectContext`/`ExternalTable`/SPI；`ArgumentCaptor` 验透传）。live-e2e CI/flip-gated，勿谎称。
- **mutation-check（Rule 9/12）**：范式 `scratchpad/mutate_r7.py`：cp 备份→「行为禁用形」`if(false)`/`null`/翻 `return`→`mvn test -Dtest=<class> -Dcheckstyle.skip=true`→查 surefire `Failures:`/`Errors:`（KILLED）→restore + `os.utime`。**⚠️ exact-string 锚点须唯一**（脚本 count!=1 即报）；**⚠️python3.6 无 capture_output/text=**；**⚠️ commit 前核已 restore + fresh recompile**（见上 stale .class 坑）。
- **cwd 会被 harness 重置**→一律绝对路径。

# ⚠️ Commit 须知（任何 `git add` 前必读）

- **path-whitelist `git add`，严禁 `git add -A`**（scrub `regression-test/conf/regression-conf.groovy` 明文 key + `*.bak` + scratch `.audit-scratch/`·`conf.cmy/`·`META-INF/`·`docker/...`·**仓根游离 `fe/IcebergScanPlanProvider.java`**〔真文件在 `fe/fe-connector/...`，勿提交〕·`plan-doc/reviews/P5-paimon-rereview3-*`）。
- **R1=`1bddf3426d6` / R2=`0a0d5b8de83` / R3=`a7c2732d984` / R4a=`a3d7210e892` / R4b=`12fe50ee88e` / R5=`e956f0edc45` / R6=`0735aac280e` / R7=`5a1a0e25e16`**；HANDOFF 单独 commit。
- commit message：见 `git log` 范式 + 末尾 `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>` + `Claude-Session: …`。PR base = `branch-catalog-spi`，squash。

# 📦 阶段状态

- **工作分支 = `catalog-spi-10-iceberg`**（off `branch-catalog-spi` @ `e5959e1b53d`，PR base = `branch-catalog-spi`，squash）。
- **⚠️ 推送状态**：P6.4 T01–T06+arg-move 已推 `origin`；**其后全部（含 commit-bridge + C4 R1–R7）未 push**。**用户未要求 push**——留用户裁量。
- **P6.1–P6.5 ✅**。**P6.6：C1/C2/C3a/C3b-pre/C3b-core+commit-bridge 全闭 ✅ → C4 R1–R7 ✅ / R8 待 → C5 翻闸**。
- iceberg **不在** `SPI_READY_TYPES`（pre-flip 零行为变更）。metastore 子线 CLOSED（勿读）。
- **⚠️ 环境**：`/mnt/disk1` 紧（2.0T，本 session ~86G free，96% used）。**下个 session 起步先 `df -h /mnt/disk1`**；空间紧时 mutation 加 `-Dcheckstyle.skip=true`。

# 🧠 给下一个 agent 的 meta

- **删除/parity/动码前必 grep 全调用方 + 区分 DEAD vs STILL-CONSUMED**；**HANDOFF/设计/RFC 的依赖名/行号/不变式/可达性可能过时或错** —— 动码前先 recon（grep+实证）再信文档。R7 recon 实证推翻设计/HANDOFF 旧「复用 NereidsToConnectorExpressionConverter」（WHERE 是 UNBOUND，复用会静默丢→全表重写）。
- **clean-room 对抗 review 偏好**：大改动 recon = 多 reader 对抗 + synthesis + critic（R7 = `wf_46e2c61c-ee2`；2 reader 因 StructuredOutput 重试上限挂，critic 接力补上 reader 缺口、挖出 conflict-mode 子集 BLOCKER）。reader schema 别太硬（重试易超限）；critic 是最后一道、最该信。
- **既有 Doris 行为/用户裁优先**：rewrite WHERE「无法精确就报错」经用户裁（fail-loud，对维护命令更安全，对齐 Option B 全对等）；node matrix / 字面量编码照搬 legacy `IcebergNereidsUtils` / `ExprToConnectorExpressionConverter`。Trino OPTIMIZE 无 WHERE（category error，设计 §0 已定），此处参照系是 Doris 自身 legacy。
- **C4 逐子步**：R8 = flip rehearsal（**唯一**需 docker e2e 的步、且**不 commit** SPI_READY_TYPES）。**C5 前切忌动 `SPI_READY_TYPES`**。
- **上下文超 30% 即交接**。本 session 完成 C4 R7（单提交 `5a1a0e25e16`，11 改 + 2 新），在干净节点交接 R8。
