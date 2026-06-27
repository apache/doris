# 🤝 Session Handoff

> 滚动文档：每次 session 结束**覆盖式更新**（不累积历史；历史见 `git log plan-doc/HANDOFF.md`）。协作规范：[AGENT-PLAYBOOK.md](./AGENT-PLAYBOOK.md)
> **范围** = catalog-spi **主线** handoff。metastore/storage 抽取子线**已 CLOSED**（2026-06-22 合入 #64446/#64653/#64655）——[`metastore-storage-refactor/`](./metastore-storage-refactor/) 仅历史留存、勿读；metastore-spi 现状直接读代码 `fe/fe-connector/fe-connector-metastore-spi/`。

---

# 🎯 下一个 session 的任务 = **DDL/ALTER buildout（B1–B5）全部完成 → 进入 flip-readiness「B 类」= SHOW/统计/Top-N 退化修复（多为纯 fe-core）**

> **⚠️ 本 session = C5 Batch B5（分区演进 3 op + `Alter.java` 铁律修复）✅ 完成。这是 DDL/ALTER 18-op buildout 的最后一批 → `P6.6-C5-ddl-spi-buildout.md`（B1–B5）整块 DONE。** 下个 session 转入 flip-readiness 的「B 应修」类（不再是 DDL/ALTER）。

> **🔑 下一步（flip-readiness B 类）= 翻闸后会「静默退化」的只读路径**（动批前先读 `P6.6-C5-flip-readiness.md` 的 B 节 + memory `iceberg-flip-readiness-gaps`，再对真实代码 re-grep）：
> - **SHOW CREATE TABLE / SHOW CREATE DATABASE / SHOW PARTITIONS 退化**：翻闸后 iceberg 表/库走 PluginDriven 通用渲染，会丢失 legacy iceberg-专属信息（分区 transform 显示、表属性、location 等）。须查 `ShowCreateTableStmt`/`ShowCreateDbStmt`/`ShowPartitionsStmt` 的 iceberg 分支 + PluginDriven 能否中立提供。
> - **自动统计（auto-analyze）停摆**：翻闸后 iceberg 表掉出自动统计的判别（类似 `GAP-A` 的 capability/engine 判别问题）。
> - **Top-N 懒物化失效（`GAP-A`）**：翻闸后 iceberg 表类掉出 `MaterializeProbeVisitor.SUPPORT_RELATION_TYPES` → lazy-top-N 静默失效。修须 capability/engine 判别（非 instanceof Iceberg）。
> - 这几项**与 DDL/ALTER 不同**：多是纯 fe-core 的「判别/渲染」退化（静默错结果/退化，不是 fail-loud），所以 flip 前必须修；少数可能需要新中立 SPI（如分区 transform 渲染）。**逐项先确认是「真退化」还是「已中立覆盖」再动手**。

> **之后的翻闸顺序（不变）**：flip-readiness B 类（SHOW/统计/Top-N）→ **[DEC-FLIP-1] 持久化 GSON 迁移**（`GsonUtils.registerCompatibleSubtype(PluginDrivenExternalCatalog.class, "Iceberg…")` 全 8 变体 + 库 + 表，跟 paimon `:389-411` 范式）→ **A3/B6 视图 SPI**（DROP VIEW / force-drop 含视图库 / 视图查询）→ **C 类用户 docker e2e**（rewrite e2e/sizing、BE DV-038、V3·WHERE、分区演进真 UpdatePartitionSpec、branch/tag 真 ManageSnapshots）→ **加 `SPI_READY_TYPES`+iceberg / 删 `CatalogFactory` legacy case / capability 核 / 用户二签**（FLIP，不可逆，最后一步）。

---

# ✅ 本 session 完成（B5）= **iceberg 连接器分区演进 3 op 翻闸全量对齐 + `Alter.java` 铁律修复**（commit `205d67e8e3a`）

**做了什么（中文详解，不引用代号）**：
让「Iceberg 分区演进 ALTER」——`ALTER TABLE … ADD/DROP/REPLACE PARTITION KEY`——在翻闸（iceberg 改走插件化通用 catalog）后仍然工作，并顺手清掉 fe-core 里最后一个「直接判断是不是 Iceberg 表再强转」的违规分发点。

1. **新增一个中立的「分区字段变更」数据载体 `PartitionFieldChange`**（放连接器 API 层，纯装箱字段、null=未设）：8 个字段 = 前 4 个描述「要加的字段 / 要删的字段 / replace 的新字段」（transform 名、transform 参数、列名、字段别名），后 4 个只在 replace 用、描述「要被替换掉的旧字段」。这样 SPI 不再依赖 fe-core 的 nereids op 类型。
2. **连接器侧落地真正的 iceberg 逻辑**：把 legacy 里「根据 transform 名字（identity/bucket/truncate/year/month/day/hour）构造 iceberg `Term`，再用 `UpdatePartitionSpec` 增删字段并提交」这套，整体搬进连接器的 seam（`IcebergCatalogOps`），由 `IcebergConnectorMetadata` 包一层鉴权后调用。fe-core 只负责把 nereids op 填进中立载体。
3. **铁律修复**：原来 `Alter.java` 里分区演进这三种操作是硬编码「`if (table instanceof IcebergExternalTable)` 再 `(IcebergExternalCatalog) cast`」——这是 fe-core 不该有的 iceberg 强耦合。现在改成和其他 ALTER（改列、branch/tag）完全一致的多态调用 `table.getCatalog().addPartitionField(table, op)`：在基类 `CatalogIf` 加 3 个中立的 default 方法（默认抛「不支持」），legacy 的 `IcebergExternalCatalog` 改成 `@Override` 实现（保留它对 IcebergMetadataOps 的强转——它是铁律豁免的 legacy 类），翻闸后的 `PluginDrivenExternalCatalog` 也实现一份（转中立载体→走连接器）。`Alter.java` 里那段 instanceof/cast 和两个相关 import 全部删掉。

**关键架构裁决（与 HANDOFF 字面建议分歧，已表面化——遵 Rule 7 取更成熟者）**：
- 旧 HANDOFF 建议「基类方法取中立 `PartitionFieldChange` DTO，`Alter.java` 里就转换」。但**实测上一批 branch/tag 的实际做法**（也是 HANDOFF 自己引为「一致目标」的 `:398-432`）是：基类方法**取 nereids op**，中立化转换发生在 `PluginDrivenExternalCatalog` 内部。我选了 branch/tag 的实际范式（不是 HANDOFF 字面），理由：① legacy `IcebergExternalCatalog` 仍能直接读 op、改动最小、pre-flip 行为零变化；② SPI 真正的中立边界（`ConnectorMetadata`）依然只吃中立 `PartitionFieldChange`，铁律不破。

**唯一 pre-flip live 行为变更（DV 登记）**：**非 iceberg 外表**执行分区演进 ALTER 的报错文案，从「… is only supported for Iceberg tables」变成基类 default 的「Not support … partition field operation」（fail-loud→fail-loud，等同 buildout §9 paimon「无功能变化」类）。iceberg pre-flip 路径行为不变（仍 `IcebergExternalCatalog`→`IcebergMetadataOps`；`updateTime` 从「Alter.java 统一算一次」改「每 op 内部 stamp」，sub-ms 等价）。内部表（OLAP）那条 reject 路径（`Alter.java` 的另一处）原样不动。

**落地文件（5 改 + 2 新 + 4 测试）**：
- 新：`fe-connector-api/.../ddl/PartitionFieldChange.java`、`fe-core/.../datasource/ConnectorPartitionFieldConverter.java`。
- 改 SPI/连接器：`ConnectorTableOps`（3 default-throw）、`IcebergCatalogOps`（3 seam 方法 + impl + `getTransform`，新 import `UpdatePartitionSpec`/`Expressions`/`Term`/`Locale`）、`IcebergConnectorMetadata`（3 auth-wrap override）。
- 改 fe-core：`CatalogIf`（3 中立 default-throw 取 nereids op）、`IcebergExternalCatalog`（3 `@Override`）、`PluginDrivenExternalCatalog`（3 override 复用 `afterExternalDdl`）、`Alter.java`（去 instanceof/cast + 去 `updateTime` 局部 + 删 2 import）。
- 改测试：`RecordingIcebergCatalogOps`（3 录制）、`CatalogBackedIcebergCatalogOpsDdlTest`（+10 真 InMemoryCatalog 分区 spec 往返）、`IcebergConnectorMetadataDdlTest`（+5 路由/auth）、`PluginDrivenExternalCatalogDdlRoutingTest`（+5 路由）、新 `ConnectorPartitionFieldConverterTest`（6）。

**验证**：连接器 **全模块 749 全绿**（seam DDL 39〔+12〕、metadata DDL 25〔+5〕，fresh recompile）；fe-core routing **52**（+5）、converter **7**（+7）全绿（fresh recompile）；**mutation 15/15 KILLED**（脚本 scratchpad `mutate_b5.py`：6 seam + 3 metadata + 4 plugin + 2 converter）；**clean-room 对抗 review（3 reader + critic）= SAFE_TO_COMMIT**（0 BLOCKER/MAJOR，0 产品逻辑 bug；critic 实证 executeAuthenticated/afterExternalDdl/converter 字段序/getTransform identity/DTO 构造序全对；3 MINOR 测试覆盖缺口〔replace 旧侧 identity transform×2、truncate 缺 arg〕**已补测**）；iron-law clean（`tools/check-connector-imports.sh` 0；`Alter.java` 无 instanceof Iceberg/cast/import，新 fe-core 文件无 `instanceof Iceberg`/`IcebergUtils`）；checkstyle 三模块 0；**e2e flip-gated 未跑**（勿谎称）。

**P6.1–P6.5 ✅ / P6.6：C1–C3 ✅ / C4 R1–R7 ✅ / C5：B1 ✅ B2a ✅ B2b ✅ B3 ✅ B4 ✅ B5 ✅ → DDL/ALTER buildout 整块 DONE → flip-readiness B 类 + 持久化 + 视图 + C docker → 翻闸**。iceberg **仍不在** `SPI_READY_TYPES`。

## ⛳ 结论（一句话）：**还不能翻闸**，但 **DDL/ALTER 18-op 全量对齐已干净完成**（scan + 行级 DML + 建表/列演进/rename/branch-tag/分区演进 + 铁律分发全清）；剩 **flip-readiness B 类（SHOW/统计/Top-N 退化）、持久化迁移、视图、C docker e2e**。

## ✅ 用户决策（2026-06-27 signed，仍有效）
- **[DEC-FLIP-2 重申] = 全量对齐**：DDL/ALTER 18 op + B 类 + 视图(A3/B6) 全部翻闸前修；仅 C 类（写路径正确性）归用户 docker，D 类翻闸后。
- **[DEC-FLIP-1] = 方向一（GSON 迁移）**：`GsonUtils.registerCompatibleSubtype(PluginDrivenExternalCatalog.class, "Iceberg…")` 全 8 catalog 变体 + 库 + 表（跟 paimon `:389-411` 范式），重启自动升级。

## 📖 起步必读（动下一批前）
1. memory `iceberg-flip-readiness-gaps`（缺口全景：A 硬阻塞 / B 应修 / C docker / 2 决策）、`iceberg-b5-partition-evolution-done`（本 session）、`iceberg-b4-branchtag-done`、`iceberg-ddl-connector-gap-flip`、`handoff-discipline-per-phase`、`consult-trino-before-spi-design`、`clean-room-adversarial-review-pref`。
2. `plan-doc/tasks/designs/P6.6-C5-flip-readiness.md`（**B 节 = 下一步主文档**）、`P6.6-C5-ddl-spi-buildout.md`（DDL/ALTER 已全 DONE，仅历史参考 + 范式库）。
3. **flip-readiness B 类的 legacy 锚点（动码前 re-grep，行号会漂）**：`ShowCreateTableStmt`/`ShowCreateDbStmt`/`ShowPartitionsStmt` 的 iceberg 分支；自动统计的 engine/类型判别；`MaterializeProbeVisitor.SUPPORT_RELATION_TYPES`（GAP-A，lazy top-N）。

## 🚦 C5 进度 — **B1 ✅ / B2a ✅ / B2b ✅ / B3 ✅ / B4 ✅ / B5 ✅ ⇒ DDL/ALTER buildout 整块完成**
> **主文档（DDL/ALTER 已完）= `P6.6-C5-ddl-spi-buildout.md`**；**下一步主文档 = `P6.6-C5-flip-readiness.md` 的 B 节**。**最后才加 `SPI_READY_TYPES`**。
- **B1 ✅（`b7203cf6a42`）= P1 四核心 DDL**（create/drop table+db + `IcebergSchemaBuilder` + sort-order SPI + cleanup 钩子）。
- **B2a ✅（`6afb08cefe9`）= P2 列演进 6 op 标量 + `afterExternalDdl` helper**。
- **B2b ✅（`249130ebf27`）= 复杂 modify 全量 + 闭 FU-nested-nullability**。
- **B3 ✅（`decacb29e49`）= renameTable + `afterExternalRename` helper**。
- **B4 ✅（`7a421b8721d`）= branch/tag 4 op + 3 中立 DTO + `ConnectorBranchTagConverter`**。mutation 14/14 KILLED。
- **B5 ✅（本 session）= 分区演进 3 op + `PartitionFieldChange` DTO + `ConnectorPartitionFieldConverter` + `Alter.java` 铁律修复**：见上「本 session 完成」。mutation 15/15 KILLED；clean-room 对抗 review = SAFE_TO_COMMIT（3 MINOR 测试缺口已补）。
- **DDL/ALTER 全绿后（= 现在）**：flip-readiness B 类（SHOW/统计/Top-N，多纯 fe-core）→ DEC-FLIP-1 持久化 GSON → A3/B6 视图 SPI → C 类 docker → 加 `SPI_READY_TYPES` + 删 legacy case + 用户二签。
- **⚠️ 加 `SPI_READY_TYPES` 是最后一步**（A 全绿 + B 处理完之后），勿提前。
- **⚠️ 每批起步先 `df -h /mnt/disk1`**（空间紧，本 session 起步 ~85G free）；mutation 加 `-Dcheckstyle.skip=true`；mutation 后 `os.utime` 源（脚本已做）或 clean 再终验（stale .class 坑）。

---

# ⚠️⚠️ 用户铁律：**fe-core 不得 `if(iceberg)` / `instanceof Iceberg*` / `import IcebergUtils`（新 seam）**
iceberg 逻辑落 `fe-connector` 经中立 SPI。**legacy 豁免类**（C4 dead 子树 `IcebergRewriteDataFilesAction`/`RewriteDataFileExecutor`/`RewriteGroupTask`/`IcebergRewriteExecutor` + commit-bridge 旧清单 + `PhysicalIcebergTableSink`/`bindIcebergTableSink` + `StatementContext` 旧 iceberg-typed stash + **`IcebergExternalCatalog`〔分区演进 3 op 现为基类 `@Override`，内部对 `IcebergMetadataOps` 的强转是 legacy 豁免〕**）保留 iceberg 引用合法。**B5 后 `Alter.java:433-456` 的 instanceof Iceberg 门已删除**（最后一个 ALTER instanceof 门），改走多态中立路；`Alter.java` 仅剩内部表 reject 路径里「only supported for Iceberg tables」字面错误信息（非 instanceof/cast/import，合法）。**R6/R7 + B1–B5 新增通用类**（`ConnectorRewriteDriver`/中立 stash/`UnboundExpressionToConnectorPredicateConverter`/`ConnectorBranchTagConverter`/`ConnectorPartitionFieldConverter` / `BranchChange`/`TagChange`/`DropRefChange`/`PartitionFieldChange` 中立 DTO）全经中立 SPI，**无** instanceof Iceberg（已核）。

---

# 🔴🔴 开放 — P6.6 翻闸（C1+C2+C3 全闭，C4 ✅，**C5 = DDL/ALTER buildout 整块 ✅；剩 B 类/持久化/视图/C docker**）

> 5 commit-stream（C1 ✅ / C2 ✅ / C3 ✅ / **C4 R1–R7 ✅** / **C5 = DDL/ALTER B1–B5 ✅** → flip-readiness B 类 + 持久化 + 视图 + C docker → FLIP 待）。

- **[C4 R1–R7 ✅]** rewrite_data_files 翻闸就绪（Option B 全对等）。**R8（flip rehearsal）= flip-gated，归 C 类 docker 验证。**
- **[C5 DDL/ALTER ✅]** 18-op buildout（`P6.6-C5-ddl-spi-buildout.md`，批次 B1–B5）全部完成（B1–B5 全程 dormant，无新 DV，pre-flip 零 live 变更——除 B5 非 iceberg 外表报错文案）。
- **[C5 剩余]** flip-readiness B 类（SHOW/统计/Top-N）+ DEC-FLIP-1 持久化 GSON + A3/B6 视图 SPI + C 类 docker。
- **[FLIP，不可逆 = C5 最后一步]** `SPI_READY_TYPES`+iceberg / 删 `CatalogFactory case:137-140`(legacy `IcebergExternalCatalogFactory`) / GSON 迁移 remap（DEC-FLIP-1） / capability 核。**FLIP 前须 B 类 + 视图 + C 用户 docker 全绿 + 用户二签。**

## 🆕 翻闸前置项（登记）
- **[GAP-A → flip-readiness B 类]** 翻闸后 iceberg 表类掉出 `MaterializeProbeVisitor.SUPPORT_RELATION_TYPES`→ lazy-top-N 静默失效。修须 capability/engine 判别。
- **[GAP-B = C3b-core ③] ✅** 隐藏列注入已闭。

**[pre-flip 行为偏差中央登记]**：P6.4=DV-045/046/047；P6.5=DV-048/049；commit-bridge=[DV-S2-rederive]；**C4 R7：DV-T05r-where**；**C5 B5：DV-partkey-msg**（非 iceberg 外表分区演进报错文案变更，benign fail-loud）。**C5 B1–B4 无新 DV**。

**⚠️ C5 才动 `SPI_READY_TYPES`**（`CatalogFactory:50-51`，现 = {jdbc,es,trino-connector,max_compute,paimon}）。

---

# 🟡 已登记 follow-up（非阻塞）
- **[FU-nested-nullability]（B2b ✅ SPI/builder 层闭）** 剩余正交约束：Doris `ArrayType.getContainsNull()` 硬编码 true（数组元素永远 nullable）。
- **[FU-doris-version-prop]（B1 登）** 连接器 createTable 丢 `doris.version` 标记属性（paimon 同；非功能）。
- **[FU-iceberg-view-ddl→A3/B6]（B1 登）** dropTable 不路由视图；翻闸后 DROP VIEW / force-drop 含视图库 fail-loud。归视图 scope。
- **[FU-rewrite-output-sizing]（R6 登；R8 必触及）** 中立 driver 未线程 target-file-size + 自适应并行度。
- **[FU-flip-e2e]（R7 扩 + B5 扩）** 真翻闸端到端（含 branch/tag 真 ManageSnapshots commit / 分区演进真 UpdatePartitionSpec commit）**未跑**（CI/flip-gated，勿谎称）。
- **[FU-where-literal-coercion]（R7 critic INFERRED）** 跨类型 WHERE 字面量 coercion 轴；R8 rehearsal 测。
- 其余（broker-write / getRowIdColumn / rewrite-rescan-perf / connector-bind-visibility）见 git log 历史。

---

# ⚙️ 操作须知（复用）

- maven：`-f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl :<art> **-am** -DfailIfNoTests=false -Dmaven.build.cache.enabled=false`（漏 `-am`→假错 `${revision}`）。**连接器模块** build 快（~30s）可前台；**fe-core -am 单类首次 ~2-3min**→`run_in_background:true` 再读 surefire XML/日志。**全量前 `rm -f target/surefire-reports/TEST-*.xml`**。
- **⚠️ stale .class 假红坑**：mutation 后 `touch` 改过的源（mtime→now）再 build，或 `mvn clean`。**commit 前最终验证务必 fresh recompile**。mutation 脚本 restore 后用 `os.utime(f, None)`（=now）→ 自动避开 stale .class。脚本 = session-scratchpad `mutate_b5.py`。
- **⚠️ checkstyle 全量 build 跑**：import 同组无空行 + 组内字母序。加 import 后 `mvn checkstyle:check -pl :<mod>` 快验，或 mutation 跑加 `-Dcheckstyle.skip=true`。
- 连接器禁 import fe-core：`bash tools/check-connector-imports.sh`。测试：连接器**无 Mockito**（真 InMemoryCatalog；分区 spec 断言用 transform 字符串 + live-field 计数，**别 hardcode iceberg 自动生成的字段名**〔bucket→非「id_bucket」、truncate→非「name_trunc」，B5 实测踩坑〕）；fe-core **Mockito**（`mockito-inline`，`ArgumentCaptor` 验透传 DTO）。live-e2e CI/flip-gated，勿谎称。
- **mutation-check（Rule 9/12）**：范式 session-scratchpad `mutate_b5.py`：cp 备份→「行为禁用形」`if(false)`/空串/换错值→`mvn test -Dtest=<class> -Dcheckstyle.skip=true`→查 surefire `Failures:`/`Errors:`（KILLED）→restore + `os.utime(f,None)`。**⚠️ exact-string 锚点须唯一**（多 occurrence 用多行 + 方法签名上下文锚定）；**⚠️ 别与读源 review 并发跑**（B4 教训：reviewer 读到瞬时变异报假 BLOCKER）。
- **cwd 会被 harness 重置**→一律绝对路径。

# ⚠️ Commit 须知（任何 `git add` 前必读）

- **path-whitelist `git add`，严禁 `git add -A`**（scrub `regression-test/conf/regression-conf.groovy` 明文 key + `*.bak` + scratch `.audit-scratch/`·`conf.cmy/`·`META-INF/`·`docker/...`·仓根游离 `fe/IcebergScanPlanProvider.java`·`plan-doc/reviews/P5-paimon-rereview3-*`）。
- **B5 本次涉及文件（全部在 `fe/` + `plan-doc/`）**：`fe/fe-connector/fe-connector-api/.../ddl/PartitionFieldChange.java`、`fe/fe-connector/fe-connector-api/.../ConnectorTableOps.java`、`fe/fe-connector/fe-connector-iceberg/.../IcebergCatalogOps.java`、`.../IcebergConnectorMetadata.java`、`.../test/.../RecordingIcebergCatalogOps.java`、`.../test/.../CatalogBackedIcebergCatalogOpsDdlTest.java`、`.../test/.../IcebergConnectorMetadataDdlTest.java`、`fe/fe-core/.../datasource/CatalogIf.java`、`.../datasource/ConnectorPartitionFieldConverter.java`、`.../datasource/PluginDrivenExternalCatalog.java`、`.../datasource/iceberg/IcebergExternalCatalog.java`、`.../alter/Alter.java`、`.../test/.../datasource/PluginDrivenExternalCatalogDdlRoutingTest.java`、`.../test/.../datasource/ConnectorPartitionFieldConverterTest.java` + `plan-doc/HANDOFF.md`、`plan-doc/tasks/designs/P6.6-C5-ddl-spi-buildout.md`。
- **C4：R1=`1bddf3426d6` / R2=`0a0d5b8de83` / R3=`a7c2732d984` / R4a=`a3d7210e892` / R4b=`12fe50ee88e` / R5=`e956f0edc45` / R6=`0735aac280e` / R7=`5a1a0e25e16`**。**C5：B1=`b7203cf6a42` / B2a=`6afb08cefe9` / B2b=`249130ebf27` / B3=`decacb29e49` / B4=`7a421b8721d` / B5=`205d67e8e3a`**。HANDOFF 单独 commit。
- commit message：见 `git log` 范式 + 末尾 `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>` + `Claude-Session: …`。PR base = `branch-catalog-spi`，squash。

# 📦 阶段状态

- **工作分支 = `catalog-spi-10-iceberg`**（off `branch-catalog-spi` @ `e5959e1b53d`，PR base = `branch-catalog-spi`，squash）。
- **⚠️ 推送状态**：P6.4 T01–T06+arg-move 已推 `origin`；**其后全部（含 commit-bridge + C4 R1–R7 + C5 B1–B5）未 push**。**用户未要求 push**——留用户裁量。
- **P6.1–P6.5 ✅**。**P6.6：C1/C2/C3+commit-bridge ✅ → C4 R1–R7 ✅ → C5 DDL/ALTER B1–B5 ✅ → flip-readiness B 类 + 持久化 + 视图 + C docker → FLIP**。
- iceberg **不在** `SPI_READY_TYPES`（pre-flip 零行为变更）。metastore 子线 CLOSED（勿读）。
- **⚠️ 环境**：`/mnt/disk1` 紧（2.0T，2026-06-27 ~85G free，96% used）。**下个 session 起步先 `df -h /mnt/disk1`**。

# 🧠 给下一个 agent 的 meta

- **删除/parity/动码前必 grep 全调用方 + 区分 DEAD vs STILL-CONSUMED**；**HANDOFF/设计/RFC 的依赖名/行号/不变式可能过时或错** —— 动码前先 recon（grep+实证）再信文档。B5 recon 实证确认：分区演进 3 op 现在**不是**中立路（`Alter.java` instanceof 门 + 方法只在 `IcebergExternalCatalog` 不在基类）；改法应**对齐 branch/tag 实际范式**（基类取 nereids op、PluginDriven 内转换），而非 HANDOFF 字面的「基类取 DTO」。
- **clean-room 对抗 review 偏好**：大改动 recon/复审 = 多 reader 对抗 + synthesis + critic。**⚠️ review（读源）与 mutation（改源）不可并发** —— 并发会让 reviewer 读到瞬时变异、报假 BLOCKER。mutation 先跑完 + 确认源 restore，再跑 review。mutation 结果是测试充分性的权威判据。
- **既有 Doris 行为/parity 优先**：pre-flip 一律零行为变更（B5 唯一例外 = 非 iceberg 外表分区演进报错文案，已登记 DV-partkey-msg，benign）。
- **下一批（flip-readiness B 类）≠ DDL/ALTER**：是只读路径的「静默退化」（SHOW/统计/Top-N），先逐项确认「真退化 vs 已中立覆盖」再动；可能需要新中立 SPI（如分区 transform 渲染）。修退化仍守铁律（capability/engine 判别，非 instanceof Iceberg）。
- **上下文超 30% 即交接**。本 session = C5 B5 实现（14 文件 commit `205d67e8e3a`：3 新〔DTO+converter+converterTest〕 + 改 SPI/seam/metadata/CatalogIf/IcebergExternalCatalog/PluginDriven/Alter + 3 改测试；mutation 15/15；对抗 review SAFE_TO_COMMIT），在干净节点交接「flip-readiness B 类」。
