# 🤝 Session Handoff

> 滚动文档：每次 session 结束**覆盖式更新**（不累积历史；历史见 `git log plan-doc/HANDOFF.md`）。协作规范：[AGENT-PLAYBOOK.md](./AGENT-PLAYBOOK.md)
> **范围** = catalog-spi **主线** handoff。metastore/storage 抽取子线**已 CLOSED**（2026-06-22 合入 #64446/#64653/#64655）——[`metastore-storage-refactor/`](./metastore-storage-refactor/) 仅历史留存、勿读；metastore-spi 现状直接读代码 `fe/fe-connector/fe-connector-metastore-spi/`。

---

# 🎯 下一个 session 的任务 = **C5 / Batch B1 = P1 四核心 DDL 连接器移植（createTable/dropTable/createDatabase/dropDatabase）+ fe-core 引擎映射**

> **⚠️ 本 session = C5 DDL/ALTER 范围纠偏 + buildout 设计（无代码改动，纯文档）。上下文留余量，在干净节点交接。下个 session 从全新上下文实现 Batch B1（连接器侧 port，新上下文做更稳）。**

> **🔥 2026-06-27 复核（`wf_4f208490-deb` + 直接验证）推翻了原「下一步」**：原 HANDOFF 说 C5 下一步是「A1 = 最小纯 fe-core 建表引擎映射」。**实测该前提错**：iceberg 连接器（`IcebergConnectorMetadata`）实现了 **0 个 DDL/ALTER 写 op**，翻闸后 **18 个 op 全部报错（真回归）**，分 3 路径（P1 4 个有 paimon 参考 / P2 11 个无参考 / P3 3 个无参考+铁律违规）；`truncateTable` 例外（legacy 本就 throws）。**全部 fail-loud（不静默错数据）→ 这是功能 parity 门槛非安全问题**。原审计 `wf_265f4359` 的 lens 是「fe-core cast iceberg」，没追 DDL 两条断裂路，只抓到 18 里的 2 个。

> **用户决策（2026-06-27 signed）= 全量对齐**：18 个 op 全部翻闸前修（知道真实范围后重申 DEC-FLIP-2）。**实现主文档 = `plan-doc/tasks/designs/P6.6-C5-ddl-spi-buildout.md`**（SPI 签名 + 中立 DTO + PluginDriven 共享 helper + 逐 op 移植表 + 批次 B1–B5 + 测试 + Alter 铁律修复）。

**本 session 完成**：① 纠偏 `P6.6-C5-flip-readiness.md` 的 A 节（18-op/3-路径，修正 A1）；② 新建 buildout 设计 `P6.6-C5-ddl-spi-buildout.md`；③ 更新本 HANDOFF。**无代码改动**。**P6.1–P6.5 ✅ / P6.6：C1–C3 ✅ / C4 R1–R7 ✅（R8=rewrite e2e=用户 docker，归 flip-readiness C 类）→ C5 = DDL/ALTER buildout（B1 起）+ B 类 SHOW/统计 + 持久化 + 视图 + C docker → 翻闸**。iceberg **仍不在** `SPI_READY_TYPES`。

## ⛳ 结论（一句话）：**还不能翻闸**。scan + 行级 DML dispatch 已干净；**DDL/ALTER 全面缺口（18 op）是新发现的最大块**，加上原 B 类 SHOW/统计退化、持久化迁移、视图。

## ✅ 用户决策（2026-06-27 signed）
- **[DEC-FLIP-2 重申] = 全量对齐**：DDL/ALTER 18 op + B 类 + 视图(A3/B6) 全部翻闸前修；仅 C 类（写路径正确性）归用户 docker，D 类翻闸后。
- **[DEC-FLIP-1] = 方向一（GSON 迁移）**：`GsonUtils.registerCompatibleSubtype(PluginDrivenExternalCatalog.class, "Iceberg…")` 全 8 catalog 变体 + 库 + 表（跟 paimon `:389-411` 范式），重启自动升级。

## 📖 起步必读（动 B1 前）
1. **`plan-doc/tasks/designs/P6.6-C5-ddl-spi-buildout.md`**（**主**：DDL/ALTER 实现设计 + 批次 + 逐 op 移植表；动码前 re-grep 锚点）。
2. `plan-doc/tasks/designs/P6.6-C5-flip-readiness.md`（缺口全景 A/B/C/D + 决策）。
3. memory `iceberg-ddl-connector-gap-flip`（本次纠偏）、`r7-where-lowering-unbound-failloud`、`r6-rewrite-driver-beginwrite-once`。
4. **B1 参考**：paimon `PaimonConnectorMetadata.createTable:786`/`dropTable:811`/`createDatabase:849`/`dropDatabase:883`（已翻闸的范本）；legacy `IcebergMetadataOps.createTableImpl:324`/`dropTableImpl:407`/`createDbImpl:227`/`dropDbImpl:265`（移植源）。

## ✅ C4 R7（WHERE lowering）= 上一 session DONE（`5a1a0e25e16`，dormant）
rewrite WHERE「无法精确下推就报错」两层 fail-loud（fe-core `UnboundExpressionToConnectorPredicateConverter` + 连接器 `RewriteDataFilePlanner` guard）。fe-core 28/0 + 连接器 19/0 + 6 变异全 KILLED + iron-law clean。**真 e2e 未跑（flip-gated）**。详 git log `5a1a0e25e16` + memory `r7-where-lowering-unbound-failloud`。R8（rewrite 真写 e2e）已归入 flip-readiness **C 类（用户 docker）**。

## 🚦 C5 起步 — **Batch B1 = P1 四核心 DDL**（详 `P6.6-C5-ddl-spi-buildout.md`）
> **主文档 = `P6.6-C5-ddl-spi-buildout.md`**（DDL/ALTER 实现）；缺口全景 = `P6.6-C5-flip-readiness.md`。**最后才加 `SPI_READY_TYPES`**。
- **B1（先做，最低风险）= P1 四核心 DDL**：iceberg 连接器实现 createTable/dropTable/createDatabase/dropDatabase（+`supportsCreateDatabase`），移植 legacy `IcebergMetadataOps.createTableImpl:324`/`dropTableImpl:407`/`createDbImpl:227`/`dropDbImpl:265`，**照搬 paimon `PaimonConnectorMetadata:786/811/849/883` 范式**；fe-core 加一行 `CreateTableInfo.pluginCatalogTypeToEngine:932` → `case "iceberg": return ENGINE_ICEBERG;`。**SPI/PluginDriven routing 已存在，无需新增**。测试：连接器 InMemoryCatalog DDL + fe-core engine-mapping + mutation。
- **后续批**：B2 列演进(6)+共享 helper → B3 rename → B4 branch/tag(4)+DTO → B5 分区演进(3)+**Alter.java:433-456 铁律修复（去 instanceof IcebergExternalTable/cast）**。逐 op 移植表见 buildout §5。
- **DDL/ALTER 全绿后**：flip-readiness B 类（SHOW/统计，多纯 fe-core）→ DEC-FLIP-1 持久化 GSON → A3/B6 视图 SPI → C 类 docker → 加 `SPI_READY_TYPES` + 删 legacy case + 用户二签。
- **task 列表已建**（#4 B1〔blockedBy #2 设计=已 done〕… #8 B5）；下个 session `TaskList` 接续。
- **⚠️ 加 `SPI_READY_TYPES` 是最后一步**（A 全绿 + B 按 DEC-FLIP-2 取舍处理完之后），勿提前。

---

# ⚠️⚠️ 用户铁律：**fe-core 不得 `if(iceberg)` / `instanceof Iceberg*` / `import IcebergUtils`（新 seam）**
iceberg 逻辑落 `fe-connector` 经中立 SPI。**legacy 豁免类**（C4 dead 子树 `IcebergRewriteDataFilesAction`/`RewriteDataFileExecutor`/`RewriteGroupTask`/`IcebergRewriteExecutor` + commit-bridge 旧清单 + `PhysicalIcebergTableSink`/`bindIcebergTableSink` + `StatementContext` 旧 iceberg-typed stash）保留 iceberg 引用合法。**R6/R7 新增通用类**（`ConnectorRewriteDriver`/`ConnectorRewriteGroupTask`/中立 stash/`applyRewriteFileScope`/`pinRewriteFileScope`/dispatch 按 `executionMode`/sink isRewrite 串 `WriteOperation.REWRITE`/**R7 `UnboundExpressionToConnectorPredicateConverter` 按列名 + 表 schema**）全经中立 SPI，**无** instanceof Iceberg（已核）。

---

# 🔴🔴 开放 — P6.6 翻闸（C1+C2+C3 全闭，C4 ✅，**C5 = 当前**）

> 5 commit-stream（C1 ✅ / C2 ✅ / C3 ✅ / **C4 R1–R7 ✅**（R8=rewrite e2e=C5 的 C 类 docker）/ **C5 进行中** / FLIP 待）。

- **[C4 R1–R7 ✅]** rewrite_data_files 翻闸就绪（Option B 全对等）：executionMode SPI / scan path-set 作用域 / planRewrite SPI / sink-bind+GATHER / transaction rewrite SPI gap / 分布式 driver+CRUX stash 中立化+begin-once 护栏 / WHERE lowering 两层 fail-loud。**R8（flip rehearsal）= flip-gated，归 C5 的 C 类 docker 验证。** 详设计 §7。
- **[C5 = 当前]** 翻闸就绪修复。**主块 = DDL/ALTER 18-op buildout（`P6.6-C5-ddl-spi-buildout.md`，批次 B1–B5，从 B1 起）**；另含 flip-readiness B 类（SHOW/统计）+ DEC-FLIP-1 持久化 GSON + A3/B6 视图 SPI + C 类 docker。**详 `P6.6-C5-flip-readiness.md`**。
- **[FLIP，不可逆 = C5 最后一步]** `SPI_READY_TYPES`+iceberg / 删 `CatalogFactory case:137-140`(legacy `IcebergExternalCatalogFactory`) / GSON 迁移 remap（DEC-FLIP-1） / capability 核。**FLIP 前须 DDL/ALTER 全绿 + B 类 + 视图 + C 用户 docker 全绿 + 用户二签。**

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
- **P6.1–P6.5 ✅**。**P6.6：C1/C2/C3a/C3b-pre/C3b-core+commit-bridge 全闭 ✅ → C4 R1–R7 ✅ → C5 = DDL/ALTER buildout（B1 起）+ B 类 + 持久化 + 视图 + C docker → FLIP**。
- iceberg **不在** `SPI_READY_TYPES`（pre-flip 零行为变更）。metastore 子线 CLOSED（勿读）。
- **⚠️ 环境**：`/mnt/disk1` 紧（2.0T，2026-06-27 ~85G free，96% used）。**下个 session 起步先 `df -h /mnt/disk1`**；空间紧时 mutation 加 `-Dcheckstyle.skip=true`。

# 🧠 给下一个 agent 的 meta

- **删除/parity/动码前必 grep 全调用方 + 区分 DEAD vs STILL-CONSUMED**；**HANDOFF/设计/RFC 的依赖名/行号/不变式/可达性可能过时或错** —— 动码前先 recon（grep+实证）再信文档。R7 recon 实证推翻设计/HANDOFF 旧「复用 NereidsToConnectorExpressionConverter」（WHERE 是 UNBOUND，复用会静默丢→全表重写）。
- **clean-room 对抗 review 偏好**：大改动 recon = 多 reader 对抗 + synthesis + critic（R7 = `wf_46e2c61c-ee2`；2 reader 因 StructuredOutput 重试上限挂，critic 接力补上 reader 缺口、挖出 conflict-mode 子集 BLOCKER）。reader schema 别太硬（重试易超限）；critic 是最后一道、最该信。
- **既有 Doris 行为/用户裁优先**：rewrite WHERE「无法精确就报错」经用户裁（fail-loud，对维护命令更安全，对齐 Option B 全对等）；node matrix / 字面量编码照搬 legacy `IcebergNereidsUtils` / `ExprToConnectorExpressionConverter`。Trino OPTIMIZE 无 WHERE（category error，设计 §0 已定），此处参照系是 Doris 自身 legacy。
- **R8 = flip rehearsal**（**唯一**需 docker e2e 的步、且**不 commit** SPI_READY_TYPES，归 C5 的 C 类）。**FLIP（加 `SPI_READY_TYPES`）是 C5 最后一步，切忌提前。**
- **设计文档可能错**（本 session 实证）：原 HANDOFF/审计 `wf_265f4359` 把 C5 下一步说成「A1=最小纯 fe-core」，实测 iceberg 连接器 0 个 DDL 写 op、18 op 翻闸后全坏。**动 B1 前仍须 re-grep 验证** buildout 设计的锚点（行号会漂；列/分区/branch DTO 字段动批前对 legacy info 类型 re-grep 取全）。
- **B1 是最低风险首批**（SPI/routing 已存在 + paimon 参考 + 纯连接器 port + 1 行 fe-core）；B2 引入共享 helper；B5 必带 Alter.java 铁律修复。
- **上下文超 30% 即交接**。本 session = C5 DDL/ALTER 范围纠偏 + buildout 设计（纯文档，无代码），在干净节点交接 B1。
