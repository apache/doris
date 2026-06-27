# 🤝 Session Handoff

> 滚动文档：每次 session 结束**覆盖式更新**（不累积历史；历史见 `git log plan-doc/HANDOFF.md`）。协作规范：[AGENT-PLAYBOOK.md](./AGENT-PLAYBOOK.md)
> **范围** = catalog-spi **主线** handoff。metastore/storage 抽取子线**已 CLOSED**（2026-06-22 合入 #64446/#64653/#64655）——[`metastore-storage-refactor/`](./metastore-storage-refactor/) 仅历史留存、勿读；metastore-spi 现状直接读代码 `fe/fe-connector/fe-connector-metastore-spi/`。

---

# 🎯 下一个 session 的任务 = **C5 / Batch B5 = 分区演进 3 op（addPartitionField/dropPartitionField/replacePartitionField）+ 中立 PartitionFieldChange DTO + `Alter.java:433-456` 铁律修复**

> **⚠️ 本 session = C5 Batch B4（branch/tag 4 op）✅ 完成 + commit（`7a421b8721d`）。下个 session 做 B5（分区演进，需新中立 DTO + 改 Alter.java 去 instanceof）。动 B5 前先读 `P6.6-C5-ddl-spi-buildout.md` §3/§5/§9 + re-grep legacy `IcebergMetadataOps.addPartitionField:1139`/`dropPartitionField:1168`/`replacePartitionField:1195`（用 `UpdatePartitionSpec` + `getTransform`/`Term`）+ nereids info `AddPartitionFieldOp`/`DropPartitionFieldOp`/`ReplacePartitionFieldOp`（取全字段）+ `Alter.java:433-456`（现 P3 instanceof 门）。**

> **🔑 B5 设计要点**（动批前 re-grep 验证）：
> - **这 3 op 现在不是中立路**：`Alter.java:433-456` 硬判 `if (table instanceof IcebergExternalTable) ((IcebergExternalCatalog) table.getCatalog()).addPartitionField(...)`，方法只存在于 `IcebergExternalCatalog`（不在基类）。**B5 铁律修复** = 在基类 `CatalogIf`/`ExternalCatalog` 加中立 `addPartitionField/dropPartitionField/replacePartitionField(ExternalTable, PartitionFieldChange)`（default 抛），`Alter.java` 改走 `table.getCatalog().addPartitionField(table, change)`（与 :398-432 其他 ALTER 一致，**去 instanceof/cast**），PluginDriven override。
> - **中立 DTO `PartitionFieldChange`**（fe-connector-api）：add 需 `{transformName, transformArg?, columnName, partitionFieldName?}`；drop 需 `{partitionFieldName? 或 transform 三元组}`；**replace 需 OLD + NEW 两套**（`oldPartitionFieldName? 或 old transform 三元组` + `newPartitionFieldName? + new transform 三元组`）——legacy `replacePartitionField` 字段最多，DTO 要覆盖全（动批前对 `ReplacePartitionFieldOp` re-grep）。考虑一个 DTO 带 old/new 两组 or 三个专用 DTO。
> - **iceberg `Term`/`getTransform`（identity/bucket[N]/truncate[N]/year/month/day/hour/void）+ `UpdatePartitionSpec.addField/removeField` 逻辑全移进连接器**（`IcebergCatalogOps` seam + metadata auth-wrap），fe-core 只填 DTO。
> - **bookkeeping 复用 `afterExternalDdl`**（分区演进缓存失效同列演进/branch-tag，legacy `refreshTable` = refreshTableInternal）。B4 已建可复用：seam thin-delegation 范式、Recording fake、resolveAlterHandle、`ConnectorBranchTagConverter` 范式（仿写 `ConnectorPartitionFieldConverter`）、真 InMemoryCatalog 往返（seed snapshot via `newAppend().appendFile(DataFiles…).commit()`）。

**本 session 完成（B4）= commit `7a421b8721d`**：iceberg 连接器 branch/tag 4 op 翻闸全量对齐 + 3 中立 DTO + `ConnectorBranchTagConverter`。
- **中立 DTO**（`fe-connector-api/.../ddl/`，纯 boxed，null=未设）：`BranchChange{name,create,replace,ifNotExists,snapshotId?,maxSnapshotAgeMs?,minSnapshotsToKeep?,maxRefAgeMs?}`、`TagChange{name,create,replace,ifNotExists,snapshotId?,maxRefAgeMs?}`、`DropRefChange{name,ifExists}`。**legacy 字段映射（易错）**：`BranchOptions.retain→setMaxSnapshotAgeMs`、`numSnapshots→setMinSnapshotsToKeep`、`retention→setMaxRefAgeMs`；`TagOptions.retain→setMaxRefAgeMs`。
- **SPI**：`ConnectorTableOps` 加 4 default-throw。
- **连接器**：seam `IcebergCatalogOps` 4 实现（**ManageSnapshots 全逻辑在 seam**，需活 Table 读 currentSnapshot/refs；`resolveSnapshotId` null=current；`createBranch` helper 区分有无 snapshotId）；`IcebergConnectorMetadata` 4 override 把**整段 seam 调用**包进 `context.executeAuthenticated`（DTO 已是中立 primitive，无"纯构建"段）+ DorisConnectorException 包装；`RecordingIcebergCatalogOps` 录制。
- **fe-core**：新 `ConnectorBranchTagConverter`（info→DTO，`Optional.orElse(null)`）；`PluginDrivenExternalCatalog` 4 override（resolveAlterHandle 按 REMOTE 名 + 转换 + 路由 + **复用 `afterExternalDdl`**）。
- **关键架构裁决（对抗审查 clean 重跑确认）= 复用 `afterExternalDdl` 正确**：legacy branch/tag 走 `OP_BRANCH_OR_TAG` editlog，但其 replay（`ExternalCatalog.replayOperateOnBranchOrTag`）**metadataOps-gated** → PluginDriven（metadataOps==null）下回放是**空操作** → 必须改用 `afterExternalDdl` 的 `OP_REFRESH_EXTERNAL_TABLE`（replay 已中立）；两者都收敛到 `refreshTableInternal`（branch/tag 缓存失效 = 列演进同款，表级 refresh 是 superset）。LOCAL(editlog)/REMOTE(master inline refresh) 名不对称无害。
- **parity**：tag 空表错误信息保留 legacy 复制粘贴 bug（对 TAG 也说 "Cannot complete replace **branch** operation … main has no snapshot"）——**故意 byte-faithful 保留**（pre-flip 零行为变更；clean review 确认"fixing would break parity"）。
- **验证**：连接器全模块 **732 BUILD SUCCESS**（+24 新：17 真 InMemoryCatalog branch/tag 往返 + 7 metadata 路由/auth）/ fe-core **53**（PluginDriven routing 47 含 +7、新 `ConnectorBranchTagConverterTest` 6）/ **mutation 14/14 KILLED**（11 seam + 1 meta + 1 conv + 1 plug）/ iron-law clean / checkstyle 三模块 0 / clean 对抗 review = SAFE TO COMMIT / **e2e flip-gated 未跑**。

**P6.1–P6.5 ✅ / P6.6：C1–C3 ✅ / C4 R1–R7 ✅ / C5：B1 ✅ B2a ✅ B2b ✅ B3 ✅ B4 ✅ → B5 + B类 + 持久化 + 视图 + C docker → 翻闸**。iceberg **仍不在** `SPI_READY_TYPES`。

## ⛳ 结论（一句话）：**还不能翻闸**。scan + 行级 DML + **P1 四核心 DDL（B1）+ P2 列演进全量（B2a+B2b）+ renameTable（B3）+ branch/tag 4 op（B4）** 已干净；剩 **B5（分区演进 3 op + Alter 铁律）**、B 类 SHOW/统计、持久化迁移、视图、C docker。

## ✅ 用户决策（2026-06-27 signed）
- **[DEC-FLIP-2 重申] = 全量对齐**：DDL/ALTER 18 op + B 类 + 视图(A3/B6) 全部翻闸前修；仅 C 类（写路径正确性）归用户 docker，D 类翻闸后。
- **[DEC-FLIP-1] = 方向一（GSON 迁移）**：`GsonUtils.registerCompatibleSubtype(PluginDrivenExternalCatalog.class, "Iceberg…")` 全 8 catalog 变体 + 库 + 表（跟 paimon `:389-411` 范式），重启自动升级。

## 📖 起步必读（动 B5 前）
1. memory `iceberg-b4-branchtag-done`（本 session + afterExternalDdl 复用裁决 + 并发 review 踩坑）、`iceberg-b3-rename-done`、`iceberg-b1-ddl-done`、`iceberg-ddl-connector-gap-flip`、`handoff-discipline-per-phase`、`consult-trino-before-spi-design`、`clean-room-adversarial-review-pref`。
2. `plan-doc/tasks/designs/P6.6-C5-ddl-spi-buildout.md` §3（中立 DTO，partition transform 表示风险）+ §5（逐 op 移植表，B5 行）+ §9（PartitionFieldChange transform 全集）、`P6.6-C5-flip-readiness.md`（缺口全景）。
3. **B5 的 legacy 锚点（动码前 re-grep，行号会漂）**：`IcebergMetadataOps.addPartitionField:1139`/`dropPartitionField:1168`/`replacePartitionField:1195`（用 `UpdatePartitionSpec.addField/removeField` + `getTransform(transformName,columnName,transformArg)` 返回 `Term`）；nereids info `AddPartitionFieldOp`/`DropPartitionFieldOp`/`ReplacePartitionFieldOp`（replace 带 old+new 两套字段）；`Alter.java:433-456`（现 instanceof 门，**B5 改这里**：基类加中立方法 + 去 cast）。base `ExternalCatalog`：分区演进方法**当前不存在**（仅 IcebergExternalCatalog 有），B5 须新增基类 default-throw。

## ⚠️ 本 session 踩坑（教训写进 memory）
- **mutation 改源期间，别并发跑「读源码」的对抗 review**：本 session 把对抗审查 workflow 与 mutation 脚本并发跑 → review agent 读到瞬时被注入 `if(false)`/`(Long)null` 的源 → 报假 BLOCKER（恰好命中变异点）。mutation 14/14 KILLED 反证那些都是测试能抓的真行为。**clean 重跑 review（mutation 结束后）= SAFE TO COMMIT**。次序：先 mutation（改源），全部结束确认源已 restore + 无 .bak，再跑读源 review。

## 🚦 C5 进度 — **B1 ✅ / B2a ✅ / B2b ✅ / B3 ✅ / B4 ✅，下一 = B5 分区演进 + Alter 铁律**
> **主文档 = `P6.6-C5-ddl-spi-buildout.md`**（DDL/ALTER 实现）；缺口全景 = `P6.6-C5-flip-readiness.md`。**最后才加 `SPI_READY_TYPES`**。
- **B1 ✅（`b7203cf6a42`）= P1 四核心 DDL**。
- **B2a ✅（`6afb08cefe9`）= P2 列演进 6 op 标量 + `afterExternalDdl` helper**。
- **B2b ✅（`249130ebf27`）= 复杂 modify 全量 + 闭 FU-nested-nullability**。
- **B3 ✅（`decacb29e49`）= renameTable + `afterExternalRename` helper**。
- **B4 ✅（`7a421b8721d`）= branch/tag 4 op + 3 中立 DTO + `ConnectorBranchTagConverter`**：见上「本 session 完成」。mutation 14/14 KILLED。
- **后续批**：**B5 分区演进(3)+中立 PartitionFieldChange DTO + `Alter.java:433-456` 铁律修复（去 instanceof IcebergExternalTable/(IcebergExternalCatalog) cast，基类加中立方法 + 改走 `table.getCatalog().<partitionFieldOp>`）**。逐 op 移植表见 buildout §5。
- **DDL/ALTER 全绿后**：flip-readiness B 类（SHOW/统计，多纯 fe-core）→ DEC-FLIP-1 持久化 GSON → A3/B6 视图 SPI → C 类 docker → 加 `SPI_READY_TYPES` + 删 legacy case + 用户二签。
- **⚠️ 加 `SPI_READY_TYPES` 是最后一步**（A 全绿 + B 处理完之后），勿提前。
- **⚠️ 每批起步先 `df -h /mnt/disk1`**（空间紧，本 session 起步 ~85G free）；mutation 加 `-Dcheckstyle.skip=true`；mutation 后 `touch` 源或 clean 再终验（stale .class 坑）。

---

# ⚠️⚠️ 用户铁律：**fe-core 不得 `if(iceberg)` / `instanceof Iceberg*` / `import IcebergUtils`（新 seam）**
iceberg 逻辑落 `fe-connector` 经中立 SPI。**legacy 豁免类**（C4 dead 子树 `IcebergRewriteDataFilesAction`/`RewriteDataFileExecutor`/`RewriteGroupTask`/`IcebergRewriteExecutor` + commit-bridge 旧清单 + `PhysicalIcebergTableSink`/`bindIcebergTableSink` + `StatementContext` 旧 iceberg-typed stash + **`Alter.java:433-456` 分区演进 instanceof 门**〔B5 待修〕）保留 iceberg 引用合法。**R6/R7 + B1–B4 新增通用类**（`ConnectorRewriteDriver`/中立 stash/`UnboundExpressionToConnectorPredicateConverter` / `ConnectorBranchTagConverter` / `BranchChange`/`TagChange`/`DropRefChange` 中立 DTO）全经中立 SPI，**无** instanceof Iceberg（已核）。

---

# 🔴🔴 开放 — P6.6 翻闸（C1+C2+C3 全闭，C4 ✅，**C5 = 当前**）

> 5 commit-stream（C1 ✅ / C2 ✅ / C3 ✅ / **C4 R1–R7 ✅** / **C5 进行中**（B1–B4 ✅，B5 起）/ FLIP 待）。

- **[C4 R1–R7 ✅]** rewrite_data_files 翻闸就绪（Option B 全对等）。**R8（flip rehearsal）= flip-gated，归 C5 的 C 类 docker 验证。**
- **[C5 = 当前]** 翻闸就绪修复。**主块 = DDL/ALTER 18-op buildout（`P6.6-C5-ddl-spi-buildout.md`，批次 B1–B5）：B1 ✅ / B2a ✅ / B2b ✅ / B3 ✅ / B4 ✅（`7a421b8721d`）→ 从 B5 起**；另含 flip-readiness B 类（SHOW/统计）+ DEC-FLIP-1 持久化 GSON + A3/B6 视图 SPI + C 类 docker。B1–B4 全程 dormant（无新 DV）。
- **[FLIP，不可逆 = C5 最后一步]** `SPI_READY_TYPES`+iceberg / 删 `CatalogFactory case:137-140`(legacy `IcebergExternalCatalogFactory`) / GSON 迁移 remap（DEC-FLIP-1） / capability 核。**FLIP 前须 DDL/ALTER 全绿 + B 类 + 视图 + C 用户 docker 全绿 + 用户二签。**

## 🆕 翻闸前置项（登记）
- **[GAP-A → C5]** 翻闸后 iceberg 表类掉出 `MaterializeProbeVisitor.SUPPORT_RELATION_TYPES`→ lazy-top-N 静默失效。修须 capability/engine 判别。
- **[GAP-B = C3b-core ③] ✅** 隐藏列注入已闭。

**[pre-flip 行为偏差中央登记]**：P6.4=DV-045/046/047；P6.5=DV-048/049；commit-bridge=[DV-S2-rederive]。**C4 R1–R6 无新 DV**；**R7：DV-T05r-where 更新**（rewrite 路 fail-loud）。**C5 B1–B4 无新 DV**（dormant，pre-flip 零 live 变更）。

**⚠️ C5 才动 `SPI_READY_TYPES`**（`CatalogFactory:50-51`，现 = {jdbc,es,trino-connector,max_compute,paimon}）。

---

# 🟡 已登记 follow-up（非阻塞）
- **[FU-nested-nullability]（B2b ✅ SPI/builder 层闭）** 剩余正交约束：Doris `ArrayType.getContainsNull()` 硬编码 true（数组元素永远 nullable）。
- **[FU-doris-version-prop]（B1 登）** 连接器 createTable 丢 `doris.version` 标记属性（paimon 同；非功能）。
- **[FU-iceberg-view-ddl→A3/B6]（B1 登）** dropTable 不路由视图；翻闸后 DROP VIEW / force-drop 含视图库 fail-loud。归视图 scope。
- **[FU-rewrite-output-sizing]（R6 登；R8 必触及）** 中立 driver 未线程 target-file-size + 自适应并行度。
- **[FU-flip-e2e]（R7 扩）** 真翻闸端到端（含 branch/tag 真 ManageSnapshots commit / 分区演进真 UpdatePartitionSpec）**未跑**（CI/flip-gated，勿谎称）。
- **[FU-where-literal-coercion]（R7 critic INFERRED）** 跨类型 WHERE 字面量 coercion 轴；R8 rehearsal 测。
- 其余（broker-write / getRowIdColumn / rewrite-rescan-perf / connector-bind-visibility）见 git log 历史。

---

# ⚙️ 操作须知（复用）

- maven：`-f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl :<art> **-am** -DfailIfNoTests=false -Dmaven.build.cache.enabled=false`（漏 `-am`→假错 `${revision}`）。**连接器模块** build 快（~30s）可前台；**fe-core -am 单类首次 ~2-3min**→`run_in_background:true` 再读 surefire XML/日志。**全量前 `rm -f target/surefire-reports/TEST-*.xml`**。
- **⚠️ stale .class 假红坑**：mutation 后 `touch` 改过的源（mtime→now）再 build，或 `mvn clean`。**commit 前最终验证务必 fresh recompile**。**⚠️ 本 session 改进**：mutation 脚本 restore 后用 `os.utime(f, None)`（=now）而非复原旧 mtime → 自动避开 stale .class。脚本 = session-scratchpad `mutate_b4.py`。
- **⚠️ checkstyle 全量 build 跑**：import 同组无空行 + 组内字母序。加 import 后 `mvn checkstyle:check -pl :<mod>` 快验，或 mutation 跑加 `-Dcheckstyle.skip=true`。
- 连接器禁 import fe-core：`bash tools/check-connector-imports.sh`。测试：连接器**无 Mockito**（真 InMemoryCatalog；branch/tag seed snapshot via `newAppend().appendFile(DataFiles…).commit()`，见 `CatalogBackedIcebergCatalogOpsDdlTest.createTableWithSnapshots`）；fe-core **Mockito**（`mockito-inline`，`ArgumentCaptor` 验透传 DTO）。live-e2e CI/flip-gated，勿谎称。
- **mutation-check（Rule 9/12）**：范式 session-scratchpad `mutate_b4.py`：cp 备份→「行为禁用形」`if(false)`/`null`/翻 return→`mvn test -Dtest=<class> -Dcheckstyle.skip=true`→查 surefire `Failures:`/`Errors:`（KILLED）→restore + `os.utime(f,None)`。**⚠️ exact-string 锚点须唯一**；**⚠️ 别与读源 review 并发跑**。
- **cwd 会被 harness 重置**→一律绝对路径。

# ⚠️ Commit 须知（任何 `git add` 前必读）

- **path-whitelist `git add`，严禁 `git add -A`**（scrub `regression-test/conf/regression-conf.groovy` 明文 key + `*.bak` + scratch `.audit-scratch/`·`conf.cmy/`·`META-INF/`·`docker/...`·仓根游离 `fe/IcebergScanPlanProvider.java`·`plan-doc/reviews/P5-paimon-rereview3-*`）。
- **C4：R1=`1bddf3426d6` / R2=`0a0d5b8de83` / R3=`a7c2732d984` / R4a=`a3d7210e892` / R4b=`12fe50ee88e` / R5=`e956f0edc45` / R6=`0735aac280e` / R7=`5a1a0e25e16`**。**C5：B1=`b7203cf6a42` / B2a=`6afb08cefe9` / B2b=`249130ebf27` / B3=`decacb29e49` / B4=`7a421b8721d`**。HANDOFF 单独 commit。
- commit message：见 `git log` 范式 + 末尾 `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>` + `Claude-Session: …`。PR base = `branch-catalog-spi`，squash。

# 📦 阶段状态

- **工作分支 = `catalog-spi-10-iceberg`**（off `branch-catalog-spi` @ `e5959e1b53d`，PR base = `branch-catalog-spi`，squash）。
- **⚠️ 推送状态**：P6.4 T01–T06+arg-move 已推 `origin`；**其后全部（含 commit-bridge + C4 R1–R7 + C5 B1–B4）未 push**。**用户未要求 push**——留用户裁量。
- **P6.1–P6.5 ✅**。**P6.6：C1/C2/C3+commit-bridge ✅ → C4 R1–R7 ✅ → C5 = DDL/ALTER buildout（B1–B4 ✅，B5 起）+ B 类 + 持久化 + 视图 + C docker → FLIP**。
- iceberg **不在** `SPI_READY_TYPES`（pre-flip 零行为变更）。metastore 子线 CLOSED（勿读）。
- **⚠️ 环境**：`/mnt/disk1` 紧（2.0T，2026-06-27 ~85G free，96% used）。**下个 session 起步先 `df -h /mnt/disk1`**。

# 🧠 给下一个 agent 的 meta

- **删除/parity/动码前必 grep 全调用方 + 区分 DEAD vs STILL-CONSUMED**；**HANDOFF/设计/RFC 的依赖名/行号/不变式可能过时或错** —— 动码前先 recon（grep+实证）再信文档。B4 recon 实证确认：branch/tag 缓存失效是 `refreshTableInternal`（同列演进），但 editlog 必须从 legacy `OP_BRANCH_OR_TAG`（replay metadataOps-gated→PluginDriven 空操作）换成 `afterExternalDdl` 的 `OP_REFRESH_EXTERNAL_TABLE`。
- **clean-room 对抗 review 偏好**：大改动 recon/复审 = 多 reader 对抗 + synthesis + critic。**⚠️ B4 教训：review（读源）与 mutation（改源）不可并发** —— 并发会让 reviewer 读到瞬时变异、报假 BLOCKER。mutation 先跑完 + 确认源 restore，再跑 review。mutation 结果是测试充分性的权威判据（B4 14/14 KILLED 反证了所有「dead code」假报）。
- **既有 Doris 行为/parity 优先**：B4 保留 legacy tag「branch」错误信息复制粘贴 bug（byte-faithful；clean review 确认 fixing would break parity）。pre-flip 一律零行为变更。
- **B5 必带 `Alter.java` 铁律修复**（分区演进是最后一个 instanceof Iceberg 门）；分区 transform DTO 字段最多（replace 带 old+new 两套），动批前 re-grep `ReplacePartitionFieldOp` 取全。
- **上下文超 30% 即交接**。本 session = C5 B4 实现（13 文件：3 DTO + SPI + seam + metadata + Recording + converter + PluginDriven + 4 测试类 + buildout/HANDOFF doc；mutation 14/14；commit `7a421b8721d`），在干净节点交接 B5。
