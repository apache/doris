# 🤝 Session Handoff

> 滚动文档：每次 session 结束**覆盖式更新**（不累积历史；历史见 `git log plan-doc/HANDOFF.md`）。协作规范：[AGENT-PLAYBOOK.md](./AGENT-PLAYBOOK.md)
> **范围** = catalog-spi **主线** handoff。metastore/storage 抽取子线**已 CLOSED**（2026-06-22 合入 #64446/#64653/#64655）——[`metastore-storage-refactor/`](./metastore-storage-refactor/) 仅历史留存、勿读；metastore-spi 现状直接读代码 `fe/fe-connector/fe-connector-metastore-spi/`。

---

# 🎯 下一个 session 的任务 = **C5 / Batch B3 = renameTable（P2 路径）+ `afterExternalRename` helper + `constraintManager.renameTable` + 连接器 `IcebergCatalogOps.renameTable`**

> **⚠️ 本 session = C5 Batch B2b（复杂类型 modifyColumn 全量 + 闭 FU-nested-nullability）✅ 完成 + commit（`249130ebf27`）。下个 session 做 B3（rename，较小，可考虑并 B4）。动 B3 前先读 `P6.6-C5-ddl-spi-buildout.md` §4/§5/§6（rename 行/auth/remote-name）+ re-grep 锚点 `IcebergMetadataOps.renameTableImpl:542`/`afterRenameTable:556`/`getTableIdentifier`/`getNamespace`。**

> **🔑 B3 设计要点**：base `ExternalCatalog.renameTable` 的 replay 侧 `RefreshManager.replayRefreshTable` 已中立；`afterExternalRename` helper 与 `afterExternalDdl` 区别 = rename 用 `createForRenameTable` editlog + `unregisterTable(old)`+`resetMetaCacheNames()`（**非** refreshTableInternal）；连接器 seam `catalog.renameTable(toTableIdentifier(db,old), toTableIdentifier(db,new))` + auth-wrap；fe-core override 还要 `constraintManager.renameTable`。remote-name：源表用 remote 名，目标表名「尚不存在」不 resolve。

**本 session 完成（B2b）= commit `249130ebf27`**：iceberg 连接器复杂类型 modifyColumn（STRUCT/ARRAY/MAP 内部结构变更）翻闸全量对齐 + 闭 FU-nested-nullability（SPI/builder 层）。
- **SPI（additive）**：`ConnectorType` 加 `childrenNullable`/`childrenComments` 平行表 + getter `isChildNullable(i)`/`getChildComment(i)`（未设默认 nullable/null）+ factory 重载（`arrayOf(e,nullable)`/`mapOf(k,v,valueNullable)`/`structOf(names,types,nullable,comments)`）；**equals/hashCode 不变**（结构身份；新字段排除 → 向后兼容所有等值调用方/测试；nullability+comment 由消费方逐字段比，对齐 legacy Doris Type 比较）。
- **连接器**：新 `IcebergComplexTypeDiff`（连接器内部、纯 iceberg-vs-iceberg 递归 diff = 移植 `applyStruct·List·MapChange` + 折叠 `checkSupportSchemaChangeForComplexType` 结构守卫）；`IcebergCatalogOps.modifyColumn` seam 分 primitive/复杂分支 + 顶层 `updateColumnDoc`；`IcebergSchemaBuilder.convert` 用逐字段 nullable+comment（`ofRequired/ofOptional`+`NestedField.required/optional` 带 doc）；`IcebergConnectorMetadata.modifyColumn` 去 B2a fail-loud + 复杂 default 须 NULL 守卫。
- **fe-core**：`ConnectorColumnConverter.toConnectorType` 线程进嵌套 nullability+comment（`StructField.getContainsNull/getComment`、`ArrayType.getContainsNull`、`MapType.getIsValueContainsNull`）。
- **实测纠偏（3）**：① diff = OLD-iceberg vs **NEW-iceberg**（NEW 由扩展 ConnectorType 经 `buildColumnType` 建），非 vs 中立类型——避开 ConnectorType 精度编码等值脆弱性，保持 build-pure-outside-auth；与 **Trino `setColumnType/buildUpdateSchema`** 同形。② 嵌套 primitive 提升合法性用 **iceberg-空间小检查**（int→long/float→double/exact），等价 legacy 对所有 iceberg-可表示类型（tinyint/smallint/largeint 本不可表示；嵌套 decimal 精度变更照 legacy 拒绝）——与 **Trino「类型合法性交给 iceberg」** 同向，结构守卫仍显式移植（iceberg commit 不会按 parity 拦或会静默误用）。③ **发现** Doris `ArrayType.getContainsNull()` 硬编码 true（数组元素永远 nullable）；struct/map 经 4-arg 构造尊重 → FU-nested-nullability 在 SPI/builder 层闭，端到端嵌套 NOT NULL 另受 Doris 自身类型系统约束（正交，未改）。
- **验证**：连接器 704 全绿（含新复杂 modify 12 e2e + 嵌套 nullability 2 + metadata 路由/默认值 guard）/ fe-core converter 21 全绿 / **mutation 16/16 KILLED**（13 连接器 + 3 fe-core）/ iron-law clean / checkstyle 三模块 0 violations / e2e flip-gated 未跑。**全程 dormant**。

**P6.1–P6.5 ✅ / P6.6：C1–C3 ✅ / C4 R1–R7 ✅ / C5：B1 ✅ B2a ✅ B2b ✅ → B3–B5 + B类 + 持久化 + 视图 + C docker → 翻闸**。iceberg **仍不在** `SPI_READY_TYPES`。

## ⛳ 结论（一句话）：**还不能翻闸**。scan + 行级 DML + **P1 四核心 DDL（B1）+ P2 列演进 6 op 全量含复杂 modify（B2a+B2b）** 已干净；剩 B3–B5（rename/branch-tag/分区演进+Alter 铁律）、B 类 SHOW/统计、持久化迁移、视图、C docker。

## ✅ 用户决策（2026-06-27 signed）
- **[DEC-FLIP-2 重申] = 全量对齐**：DDL/ALTER 18 op + B 类 + 视图(A3/B6) 全部翻闸前修；仅 C 类（写路径正确性）归用户 docker，D 类翻闸后。
- **[DEC-FLIP-1] = 方向一（GSON 迁移）**：`GsonUtils.registerCompatibleSubtype(PluginDrivenExternalCatalog.class, "Iceberg…")` 全 8 catalog 变体 + 库 + 表（跟 paimon `:389-411` 范式），重启自动升级。

## 📖 起步必读（动 B3 前）
1. memory `iceberg-b2b-complex-modify-done`（本 session 落地 + 3 纠偏 + Trino 对照）、`iceberg-b1-ddl-done`、`iceberg-b2-complex-modify-decision`、`handoff-discipline-per-phase`、`consult-trino-before-spi-design`。
2. `plan-doc/tasks/designs/P6.6-C5-ddl-spi-buildout.md` §4（PluginDriven helper）/§5（逐 op 表，rename 行）/§6（auth + remote-name）、`P6.6-C5-flip-readiness.md`（缺口全景）。
3. **B3 的 legacy 锚点（动码前 re-grep，行号会漂）**：`IcebergMetadataOps.renameTableImpl:542`（`catalog.renameTable` + auth）+ `afterRenameTable:556`（`unregisterTable(old)`+`resetMetaCacheNames()`）+ `getTableIdentifier`/`getNamespace`（含外 catalog 名段）；base `ExternalCatalog.renameTable:1111`（metadataOps==null throw）；SPI 加 `ConnectorTableOps.renameTable` default-throw。

## ✅ C4 R7（WHERE lowering）= 上一 session DONE（`5a1a0e25e16`，dormant）
rewrite WHERE「无法精确下推就报错」两层 fail-loud（fe-core `UnboundExpressionToConnectorPredicateConverter` + 连接器 `RewriteDataFilePlanner` guard）。fe-core 28/0 + 连接器 19/0 + 6 变异全 KILLED + iron-law clean。**真 e2e 未跑（flip-gated）**。详 git log `5a1a0e25e16` + memory `r7-where-lowering-unbound-failloud`。R8（rewrite 真写 e2e）已归入 flip-readiness **C 类（用户 docker）**。

## 🚦 C5 进度 — **B1 ✅ / B2a ✅ / B2b ✅，下一 = B3 rename**（详 `P6.6-C5-ddl-spi-buildout.md` + 上「🔑 B3 设计要点」）
> **主文档 = `P6.6-C5-ddl-spi-buildout.md`**（DDL/ALTER 实现）；缺口全景 = `P6.6-C5-flip-readiness.md`。**最后才加 `SPI_READY_TYPES`**。
- **B1 ✅（`b7203cf6a42`）= P1 四核心 DDL**；新 SPI（ConnectorSortField/sortOrder/cleanup 钩子）B2+ 复用。
- **B2a ✅（`6afb08cefe9`）= P2 列演进 6 op 标量 + `afterExternalDdl` helper**。
- **B2b ✅（`249130ebf27`）= 复杂 modify 全量 + 闭 FU-nested-nullability（SPI/builder 层）**：见上「本 session 完成」。`ConnectorType` additive + `IcebergComplexTypeDiff` + seam 分支 + 去 B2a fail-loud。mutation 16/16 KILLED。
- **后续批**：B3 rename(+constraintManager.renameTable + `afterExternalRename` helper〔与 afterExternalDdl 区别：rename 用 createForRenameTable editlog + unregisterTable+resetMetaCacheNames，非 refreshTableInternal〕) → B4 branch/tag(4)+中立 DTO(BranchChange/TagChange/DropRefChange) → B5 分区演进(3)+中立 PartitionFieldChange DTO + **`Alter.java:433-456` 铁律修复（去 instanceof IcebergExternalTable/(IcebergExternalCatalog) cast，改走 `table.getCatalog().<op>` 中立路）**。逐 op 移植表见 buildout §5。
- **DDL/ALTER 全绿后**：flip-readiness B 类（SHOW/统计，多纯 fe-core）→ DEC-FLIP-1 持久化 GSON → A3/B6 视图 SPI → C 类 docker → 加 `SPI_READY_TYPES` + 删 legacy case + 用户二签。
- **⚠️ 加 `SPI_READY_TYPES` 是最后一步**（A 全绿 + B 按 DEC-FLIP-2 取舍处理完之后），勿提前。
- **⚠️ 每批起步先 `df -h /mnt/disk1`**（空间紧，B1 收尾时 ~84G free）；mutation 加 `-Dcheckstyle.skip=true`；mutation 后 `touch` 源或 clean 再终验（stale .class 坑）。

---

# ⚠️⚠️ 用户铁律：**fe-core 不得 `if(iceberg)` / `instanceof Iceberg*` / `import IcebergUtils`（新 seam）**
iceberg 逻辑落 `fe-connector` 经中立 SPI。**legacy 豁免类**（C4 dead 子树 `IcebergRewriteDataFilesAction`/`RewriteDataFileExecutor`/`RewriteGroupTask`/`IcebergRewriteExecutor` + commit-bridge 旧清单 + `PhysicalIcebergTableSink`/`bindIcebergTableSink` + `StatementContext` 旧 iceberg-typed stash）保留 iceberg 引用合法。**R6/R7 新增通用类**（`ConnectorRewriteDriver`/`ConnectorRewriteGroupTask`/中立 stash/`applyRewriteFileScope`/`pinRewriteFileScope`/dispatch 按 `executionMode`/sink isRewrite 串 `WriteOperation.REWRITE`/**R7 `UnboundExpressionToConnectorPredicateConverter` 按列名 + 表 schema**）全经中立 SPI，**无** instanceof Iceberg（已核）。

---

# 🔴🔴 开放 — P6.6 翻闸（C1+C2+C3 全闭，C4 ✅，**C5 = 当前**）

> 5 commit-stream（C1 ✅ / C2 ✅ / C3 ✅ / **C4 R1–R7 ✅**（R8=rewrite e2e=C5 的 C 类 docker）/ **C5 进行中** / FLIP 待）。

- **[C4 R1–R7 ✅]** rewrite_data_files 翻闸就绪（Option B 全对等）：executionMode SPI / scan path-set 作用域 / planRewrite SPI / sink-bind+GATHER / transaction rewrite SPI gap / 分布式 driver+CRUX stash 中立化+begin-once 护栏 / WHERE lowering 两层 fail-loud。**R8（flip rehearsal）= flip-gated，归 C5 的 C 类 docker 验证。** 详设计 §7。
- **[C5 = 当前]** 翻闸就绪修复。**主块 = DDL/ALTER 18-op buildout（`P6.6-C5-ddl-spi-buildout.md`，批次 B1–B5）：B1 ✅（`b7203cf6a42`）/ B2a ✅（`6afb08cefe9`）/ B2b ✅（`249130ebf27`）→ 从 B3 起**；另含 flip-readiness B 类（SHOW/统计）+ DEC-FLIP-1 持久化 GSON + A3/B6 视图 SPI + C 类 docker。**详 `P6.6-C5-flip-readiness.md`**。B1/B2a/B2b 全程 dormant（无新 DV）。
- **[FLIP，不可逆 = C5 最后一步]** `SPI_READY_TYPES`+iceberg / 删 `CatalogFactory case:137-140`(legacy `IcebergExternalCatalogFactory`) / GSON 迁移 remap（DEC-FLIP-1） / capability 核。**FLIP 前须 DDL/ALTER 全绿 + B 类 + 视图 + C 用户 docker 全绿 + 用户二签。**

## 🆕 翻闸前置项（登记）
- **[GAP-A → C5]** 翻闸后 iceberg 表类掉出 `MaterializeProbeVisitor.SUPPORT_RELATION_TYPES`→ lazy-top-N 静默失效。修须 capability/engine 判别。
- **[GAP-B = C3b-core ③] ✅** 隐藏列注入已闭。

**[pre-flip 行为偏差中央登记]**：P6.4=DV-045/046/047；P6.5=DV-048/049；commit-bridge=[DV-S2-rederive]。**C4 R1–R6 无新 DV**（dormant）。**R7：DV-T05r-where 更新**（rewrite 路从「静默丢/变宽」改为 fail-loud——撤销静默丢；常见 WHERE 零差异，罕见不可下推形式现报错；行为更安全，仍 dormant、零 live 变更）。

**⚠️ C5 才动 `SPI_READY_TYPES`**（`CatalogFactory:50-51`，现 = {jdbc,es,trino-connector,max_compute,paimon}）。

---

# 🟡 已登记 follow-up（非阻塞，勿在 C4 增量做）
- **[FU-nested-nullability]（B1 登；B2b ✅ SPI/builder 层闭）** 中立 `ConnectorType` 现带逐字段 nullability+comment，`toConnectorType` 线程进、`IcebergSchemaBuilder.convert` 用之（ofRequired/ofOptional + NestedField 带 doc）→ iceberg 建表/改列复杂嵌套 NOT NULL+comment 已能保真。**剩余正交约束**：Doris 自身 `ArrayType.getContainsNull()` 硬编码 true（数组元素永远 nullable），struct/map 经 4-arg 尊重——故端到端嵌套 NOT NULL 还取决于 Doris 类型系统是否表达（未改，非本 scope）。paimon schema builder 仍忽略逐字段 nullable（DataField 无该参数，行为不变）。
- **[FU-doris-version-prop]（B1 登）** 连接器 createTable 丢 `doris.version` 标记属性（legacy 写 `ExternalCatalog.DORIS_VERSION_VALUE`=build 版本；连接器够不到 fe-common `Version`；paimon 翻闸路同样不写）。仅 SHOW CREATE TABLE tblproperties 少一行 provenance，非功能。翻闸前若要：fe-core 在转换器/PluginDriven 注入 `doris.version` 到 request.properties。
- **[FU-iceberg-view-ddl→A3/B6]（B1 登）** B1 dropTable 不路由视图（legacy `dropTableImpl` viewExists→performDropView）；翻闸后 DROP VIEW / force-drop 含 iceberg 视图的库 → fail-loud（no such table / namespace not empty）。归视图 scope A3/B6。
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
- **C4：R1=`1bddf3426d6` / R2=`0a0d5b8de83` / R3=`a7c2732d984` / R4a=`a3d7210e892` / R4b=`12fe50ee88e` / R5=`e956f0edc45` / R6=`0735aac280e` / R7=`5a1a0e25e16`**。**C5：B1=`b7203cf6a42` / B2a=`6afb08cefe9` / B2b=`249130ebf27`**。HANDOFF 单独 commit。
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
- **设计文档可能错**（B1 再次实证）：buildout §7 说「B1 无新 SPI」，实测 createTable 的 request→iceberg-Schema 转换连接器侧不存在 + sort-order 中立 request 漏字段 + HMS 清理够不到 fe-core FS → B1 实际新增 3 处 SPI/infra。**动批前 re-grep 连接器能力完整度（不止 fe-core），别信文档「已有」**；列/分区/branch DTO 字段动批前对 legacy info 类型 re-grep 取全。
- **B2 引入 `afterExternalDdl` 共享 helper**（PluginDriven 无 metadataOps，13 op 的 editlog+cache 失效样板会重复）；B5 必带 `Alter.java` 铁律修复。**modifyColumn 含列 COMMENT 最易漏，单独测**。
- **上下文超 30% 即交接**。本 session = C5 B2b 实现（代码 + 测试 + mutation 16/16 + commit `249130ebf27`），在干净节点交接 B3。
- **B2b 实证再次印证「设计文档可能错，动码前 re-grep + 实证」**：buildout/HANDOFF 旧设计说 diff「= OLD iceberg vs NEW ConnectorType」，实现时发现按中立类型比对有 ConnectorType 精度编码等值脆弱性 → 改为 OLD-iceberg vs NEW-iceberg（NEW 由扩展 ConnectorType 建），更稳且与 Trino 同形；又发现 Doris `ArrayType` 数组元素永远 nullable（硬编码），修正了「闭 FU-nested-nullability」的精确表述（SPI/builder 层闭，Doris 端正交）。**架构决策先看 Trino**（本 session 用 general-purpose agent 读 Trino `IcebergMetadata.setColumnType/buildUpdateSchema`/`TypeConverter`，印证 SPI 形 + 类型合法性交给 iceberg）。
