# 🤝 Session Handoff

> 滚动文档：每次 session 结束**覆盖式更新**（不累积历史；历史见 `git log plan-doc/HANDOFF.md`）。协作规范：[AGENT-PLAYBOOK.md](./AGENT-PLAYBOOK.md)
> **范围** = catalog-spi **主线** handoff。metastore/storage 抽取子线**已 CLOSED**（2026-06-22 合入 #64446/#64653/#64655）——`metastore-storage-refactor/` 仅历史留存、勿读；metastore-spi 现状直接读代码 `fe/fe-connector/fe-connector-metastore-spi/`。

---

# 🎯 下一个 session 的任务 = **视图中立化 B2（DROP VIEW + 强制删库视图级联）= 翻闸前最后一块视图功能**

> **⚠️ 本 session 做了两件事**：完成 **视图 B1（查询）= commit `320fa406d6b`** 与 **视图 B3（SHOW CREATE 视图）= commit `91b7d049eff`**。剩 **视图 B2（DROP）** 一块视图功能未做。

> **🔑 下一步 = 视图 B2**（设计全在 `plan-doc/tasks/designs/P6.6-view-spi.md`，B2 段已带「锚点（动码前 re-grep）」；动码前读它 + 对真实代码 re-grep）：
> - **新 `dropView` SPI**：connector-api `ConnectorTableOps` 加 `dropView(session, dbName, viewName)` 默认 throw（紧挨 B1 加的 `getViewDefinition`/`viewExists`/`listViewNames`，实测 :113-135 一带）；iceberg `IcebergCatalogOps` 接口加 `dropView`（B1 已把 `loadViewDefinition` 加在 `viewExists` 后，照此范式）+ `CatalogBackedIcebergCatalogOps` 实现包 `((ViewCatalog) catalog).dropView(toTableIdentifier(...))`（gated on `isViewCatalogEnabled()`，镜像 legacy `IcebergMetadataOps.performDropView:1327-1333`）；`IcebergConnectorMetadata.dropView` 鉴权包裹（镜像 viewExists 的 wrap-all）。
> - **dropTable 路由**：fe-core `PluginDrivenExternalCatalog.dropTable`（实测 :479-522）经 `metadata.viewExists` 路由到 `dropView`（镜像 legacy `IcebergMetadataOps.dropTableImpl:407-422` 的 viewExists→performDropView 分派）。
> - **force-drop-db 视图级联**：iceberg `IcebergConnectorMetadata.dropDatabase`（实测 :579-601）force 臂补 `catalogOps.listViewNames`+`dropView` 再 `dropNamespace`（镜像 legacy `performDropDb:277-316`，**全在连接器内部**，无新 fe-core SPI）。
> - **测试 fake**：`RecordingIcebergCatalogOps` 加 `dropView` 录制；`FakeIcebergViewCatalog.dropView`（现 throw）改可录制（已被 B1 加的 `loadableViews`/`StubView` 同文件）。
> - **⚠️ 涉元数据写 → 需 e2e（flip-gated，跑不了，勿谎称）**。B2 仅依赖 B0，**不**依赖 B1/B3，可独立做。

> **之后的翻闸顺序（不变）**：视图 B2 → **C 类用户 docker e2e** → **翻闸原子提交**〔加 `SPI_READY_TYPES`+iceberg **同时** GSON 迁移（`registerCompatibleSubtype` 全 8 catalog 变体 + 库 + 表）+ 删 `CatalogFactory` legacy case + capability 核 + 用户二签〕（FLIP，不可逆，最后一步）。

---

# ⚖️ 关键决策（沿用，用户已签）

## [DEC-FLIP-1] 持久化 GSON 迁移 = 方向 A（并入翻闸原子提交，**不**单独提前做）
实证：一个 GSON label 只能登记一次 → 重映射 `"IcebergExternalCatalog"`→PluginDriven 必须先删旧 `registerSubtype`，删后任一 live `IcebergExternalCatalog` 序列化即 null-label 抛 → 翻闸前每存镜像必坏 FE checkpoint。paimon 先例 `38e7140ce56` 把「加 SPI_READY_TYPES + 删 registerSubtype + 加 registerCompatibleSubtype」放同一原子提交。**迁移就是翻闸的一部分。**

## [视图范围] = parity only（用户签）
查询（B1 ✅）/ DROP VIEW + 强制删库视图级联（B2 ⏳）/ SHOW CREATE 视图（B3 ✅）。**CREATE/RENAME VIEW 出范围**（grep 确认 pre-flip iceberg 无建视图写路径 → fail-loud 留后续）。配置开关保留 `enable_query_iceberg_views` 原名（parity）。

---

# ✅ 本 session 完成 = **视图 B1（查询，`320fa406d6b`）+ B3（SHOW CREATE，`91b7d049eff`）**

**B1 做了什么（中文详解，不引用代号）**：翻闸后 `SELECT 一个 iceberg 视图`会直接报错——视图体无处可取、绑定阶段没有视图分流。本批补「查询视图」的中立通道：
- 新中立 DTO `ConnectorViewDefinition{sql, dialect}`（Trino 对视图建模一致）。
- `ConnectorTableOps.getViewDefinition(session, dbName, viewName)` 默认 fail-loud；**按库名/视图名索引（视图无表句柄）**，与 `viewExists`/`listViewNames` 及 Trino `getView` 同形。
- iceberg：`IcebergCatalogOps.loadViewDefinition` 一次远程 `loadView` 同时取 engine-name(=dialect) 与 `sqlFor(dialect)` 的 SQL；`IcebergConnectorMetadata.getViewDefinition` 鉴权包裹镜像 viewExists。
- fe-core：`PluginDrivenExternalTable.getViewText()` 经 `getViewDefinition` 一次 round-trip 取 SQL（与 pre-flip 单次 load 等价、无缓存）；`BindRelation` 在 PAIMON/MAX_COMPUTE/TRINO/LAKESOUL/PLUGIN 共享 fall-through 内加视图臂，中立 `instanceof PluginDrivenExternalTable && isView()` 分流，**逐字节复刻** legacy ICEBERG 视图臂（同开关 + 时间旅行拒绝 + 复用现成中立 `parseAndAnalyzeExternalView` + `LogicalSubQueryAlias`，config-off 文案先于时间旅行检查）。

**📊 B1 实证纠正了设计初稿 4 处**：① `getViewDefinition` 签名 = `(session, dbName, viewName)` 非 `(session, handle)`（视图无句柄；与 Trino getView 同形）；② legacy `getSqlDialect()` **零调用方=死码**（视图体方言转换用会话级方言非视图方言），**不**移植到 fe-core；③ `dialect` 字段 fe-core 不读但**非 YAGNI**（连接器内部 sqlFor 选型所需 + Trino 对齐，clean-room 核可）；④ **B3 依赖 B1**（B3 用 `getViewText()`）→ 顺序 B1→B3→B2。

**B3 做了什么**：翻闸后 `SHOW CREATE` 一个 iceberg 视图会错渲染成 `CREATE TABLE`。`ShowCreateTableCommand.doRun` 在 legacy ICEBERG 视图臂后、`Env.getDdlStmt` 前加中立插件视图臂（`instanceof PluginDrivenExternalTable && isView()`），内联复刻 `IcebergUtils.showCreateView` 字节 + 返回同 2 列 `META_DATA`（保留 legacy 用 META_DATA 非 4 列 VIEW_META_DATA 的特性）。视图体复用 B1 `getViewText()`。

**验证**：B1 — 3 模块 fresh recompile；新增/改测试 api 6 + iceberg 65 + fe-core 20 全绿；广义回归 iceberg 769 / BindRelationTest 5 / PluginDriven* 20；**mutation 11/11 KILLED**（`mutate_view_b1.py`）；**clean-room（4 reader + critic）= SAFE_TO_COMMIT（must-fix 0）**；checkstyle 0；iron-law 0。B3 — fe-core fresh recompile；`EnvShowCreatePluginTableTest` 3/3 无回归；checkstyle 0；iron-law 0；字节对齐已核。**两批的「命令/绑定臂」均需活 FE 上下文、无单测 harness（legacy 同臂亦无；`test_iceberg_view_query_p0` 现 parked）→ e2e flip-gated 未跑（勿谎称）**。

**P6.1–P6.5 ✅ / P6.6：C1–C3 ✅ / C4 R1–R7 ✅ / C5 DDL/ALTER B1–B5 ✅ / flip-readiness 只读退化 ✅ / 视图 B0 ✅ / 视图 B1 ✅ / 视图 B3 ✅** → 剩 **视图 B2（DROP）+ C docker** → 翻闸（含 GSON 迁移）。iceberg **仍不在** `SPI_READY_TYPES`。

## ⛳ 结论（一句话）：**还不能翻闸**。视图查询 + SHOW CREATE 已落地；剩 **视图 B2（DROP）、C docker e2e、翻闸原子提交（含持久化 GSON）**。

## 📖 起步必读（动 B2 前）
1. memory `iceberg-view-b1-b3-done`（本 session）、`iceberg-view-b0-done`、`iceberg-flip-readiness-gaps`（缺口全景）、`handoff-discipline-per-phase`、`consult-trino-before-spi-design`、`clean-room-adversarial-review-pref`、`ask-user-explain-in-chinese-first`、`doris-build-verify-gotchas`。
2. `plan-doc/tasks/designs/P6.6-view-spi.md`（B2 段已带锚点；B0/B1/B3 已标 DONE）。
3. **B2 锚点（动码前 re-grep）**：`PluginDrivenExternalCatalog.dropTable:479-522`；legacy `IcebergMetadataOps.dropTableImpl:407-422`/`performDropView:1327-1333`/`performDropDb:277-316`；`IcebergConnectorMetadata.dropDatabase:579-601`（force 臂现仅级联表）；`ConnectorTableOps:113-135`（viewExists/listViewNames/getViewDefinition 旁加 dropView 默认）；`IcebergCatalogOps`（B1 在 viewExists 后加了 loadViewDefinition，dropView 照此）；`RecordingIcebergCatalogOps`/`FakeIcebergViewCatalog`（加 dropView 录制；后者 dropView 现 throw）。

---

# ⚠️⚠️ 用户铁律：**fe-core 不得新增 `if(iceberg)` / `instanceof Iceberg*` / `import IcebergUtils` / 引擎名字符串判别（新 seam）**
iceberg 逻辑落 `fe-connector` 经中立 SPI。**legacy 豁免类**保留 iceberg 引用合法（C4 dead 子树 + commit-bridge 旧清单 + `PhysicalIcebergTableSink`/`bindIcebergTableSink` + `StatementContext` 旧 iceberg-typed stash + `IcebergExternalCatalog` + `ShowCreateDatabaseCommand`/`Env.getDdlStmt` legacy iceberg 臂 + `BindRelation case ICEBERG_EXTERNAL_TABLE` + `ShowCreateTableCommand` legacy ICEBERG 视图臂〔B3 加的是**并列** PLUGIN 臂，不动它〕+ `InsertUtils` 既有 `UnboundIcebergTableSink` 分支）。**本 session 守则**：B1 的 `getViewText`/`getViewDefinition`/BindRelation 插件臂 + B3 的 ShowCreateTableCommand 插件臂全经中立能力 + 中立 SPI，无 instanceof Iceberg（已核 + clean-room 验 + iron-law 脚本 0）。

---

# 🟡 已登记 follow-up（非阻塞）
- **[FU-view-gson-roundtrip]（B0 登）** `PluginDrivenExternalTable` 的 GSON round-trip 重算 `isView` 无直测（机制 clean-room code-verified sound）。
- **[FU-view-exception-arms]（B0 登，低）** 连接器 `viewExists`/`listViewNames`/`getViewDefinition` 异常归一化臂直测（B1 已对 getViewDefinition 加 auth-wrap 直测；viewExists/listViewNames 仍 byte-mirror）。
- **[FU-getsqldialect-deadcode]（B1 登，低）** legacy `IcebergExternalTable.getSqlDialect()` 零调用方=死码，可后续清理（本轮未动 legacy）。
- **[FU-showcreatedb-render-ut]** `ShowCreateDatabaseCommand` plugin 臂渲染无直接 UT（flip-gated e2e 覆盖）。
- **[FU-createtablelike-plugin]** `getCreateTableLikeStmt` plugin 臂仍退化（paimon 已退化、非 iceberg 引入）。
- **[FU-show-partitions-deadcode]** `ShowPartitionsCommand` `instanceof IcebergExternalCatalog` 3 列臂 = 死码；翻闸后可选连接器侧补 5 列。
- **[FU-rewrite-output-sizing]（R6 登；R8 必触及）** 中立 driver 未线程 target-file-size + 自适应并行度。
- **[FU-flip-e2e]** 真翻闸端到端**未跑**（CI/flip-gated）；视图 B1/B3 的绑定/命令臂、B2 的 DROP 写路径均 flip-gated。
- 其余（nested-nullability / where-literal-coercion / broker-write / doris-version-prop 等）见 git log 历史。

---

# ⚙️ 操作须知（复用）

- maven：`-f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl :<art> **-am** -DfailIfNoTests=false -Dmaven.build.cache.enabled=false`（漏 `-am`→假错 `${revision}`）。**fe-core 只依赖 `fe-connector-api`** → `:fe-core -am` 不拖 paimon。**fe-connector-paimon 单独 build 必须 `package`**。**iceberg/api** 正常 `-am test`。
- **⚠️ checkstyle 别加 `-am`**：`-am` 会把 `fe-common`（2381 个既存 checkstyle error）拖进 `checkstyle:check` 假红 → 用 `mvn -pl :fe-core checkstyle:check`（**不带 -am**，checkstyle 读源不需依赖编译）。
- **⚠️ bash 工具默认 timeout 120s**：fe-core build 超时 → 调 `timeout` 参数到 ~600000ms，或后台跑。
- **⚠️ maven 经管道时 `$?` 是管道尾命令的** → 用 `${PIPESTATUS[0]}` 或 grep `BUILD SUCCESS`。`-q` 抑制 console → 读 surefire **XML**（`TEST-<class>.xml`）的 `tests=`/`failures=` 确认真跑。
- **⚠️ stale .class 假红坑**：mutation 后脚本已 `os.utime`；**commit 前最终验证务必 fresh recompile**。
- **⚠️ iceberg 视图测试坑（B1 踩）**：`org.apache.iceberg.view.View.sqlFor(dialect)` **接口默认方法在 1.10.1 直接 throw**（真解析在 `BaseView`）；fake View 须**自己 override sqlFor** 复刻「按 dialect 精确匹配 representations」逻辑（见 `FakeIcebergViewCatalog.StubView`）。ViewVersion/SQLViewRepresentation 用 `ImmutableViewVersion`/`ImmutableSQLViewRepresentation`（iceberg-core）构造。
- 连接器禁 import fe-core：`bash tools/check-connector-imports.sh`。**连接器测试无 Mockito**（真 InMemoryCatalog/Recording fakes）；**fe-core 用 Mockito**（`CALLS_REAL_METHODS` + `Deencapsulation.setField` + stub `getConnector`/`getMetadata`/`buildConnectorSession`）。**⚠️ Mockito `anyString()` 不匹配 null** → CALLS_REAL_METHODS 表测须 set 非空 `remoteName`+`db.getRemoteName()`。
- **mutation-check（Rule 9/12）**：范式 scratchpad `mutate_view_b1.py`（11 变异跨 4 模块；单行 exact-string 锚点 count==1 守；KILLED=rc!=0）。**⚠️ Python 3.6**：`subprocess.run(stdout=PIPE,stderr=STDOUT,universal_newlines=True)`（无 `capture_output`）。**⚠️ review（读源）与 mutation（改源）务必串行**。
- **cwd 会被 harness 重置** → 一律绝对路径。

# ⚠️ Commit 须知（任何 `git add` 前必读）

- **path-whitelist `git add`，严禁 `git add -A`**（scrub `regression-test/conf/regression-conf.groovy` 明文 key + `*.bak` + scratch `.audit-scratch/`·`conf.cmy/`·`META-INF/`·`docker/...`·仓根游离 `fe/IcebergScanPlanProvider.java`·`plan-doc/reviews/P5-paimon-rereview3-*`)。
- commit message：见 `git log` 范式 + 末尾 `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>` + `Claude-Session: …`。PR base = `branch-catalog-spi`，squash。
- **视图 B1 = `320fa406d6b`（13 文件）；视图 B3 = `91b7d049eff`（1 文件）**。HANDOFF + 设计文档单独 commit（memory 在 `.claude/`、非仓内）。

# 📦 阶段状态

- **工作分支 = `catalog-spi-10-iceberg`**（off `branch-catalog-spi` @ `e5959e1b53d`，PR base = `branch-catalog-spi`，squash）。
- **⚠️ 推送状态**：P6.4 T01–T06+arg-move 已推 `origin`；**其后全部（含 commit-bridge + C4 + C5 + flip-readiness 两轮 + 视图 B0/B1/B3）未 push**。**用户未要求 push**——留用户裁量。
- iceberg **不在** `SPI_READY_TYPES`（pre-flip 零行为变更）。metastore 子线 CLOSED（勿读）。
- **⚠️ 环境**：`/mnt/disk1` 紧（2.0T，本 session 起步 ~85G free，96% used）。**下个 session 起步先 `df -h /mnt/disk1`**；**勿用 worktree 隔离编译 agent**（复制整仓，盘不够）。

# 🧠 给下一个 agent 的 meta

- **删除/parity/动码前必 grep 全调用方 + 区分 DEAD vs STILL-CONSUMED**；**HANDOFF/设计/RFC 的依赖名/行号/不变式可能过时或错** —— 动码前先 recon（grep+实证）再信文档。**本 session 实证纠正 4 处**（句柄 vs 名字键、getSqlDialect 死码、dialect 字段非 YAGNI、B3 依赖 B1）。
- **clean-room 对抗 review 偏好**：大改动 = 多 reader 对抗 + critic。**⚠️ review（读源）与 mutation（改源）不可并发**。**6 行 verbatim 镜像臂（如 B3）不必另跑 workflow**——焦点验证（字节对齐 + iron-law + 邻接回归）即可，B1 clean-room 已立范式。
- **既有 Doris 行为/parity 优先**：pre-flip 一律零行为变更（B1/B3 零变更，无新 DV）。
- **B2 需先读 `P6.6-view-spi.md` B2 段 + 对真实代码 re-grep**；dropView SPI 紧贴 B1 的 getViewDefinition/viewExists 加；dropTable 路由经 viewExists；force-drop-db 视图级联全在连接器内部；测试 fake（Recording/FakeIcebergViewCatalog）加 dropView。**涉元数据写 → e2e flip-gated 跑不了，勿谎称**。
- **上下文超 30% 即交接**。本 session = 视图 B1（13 文件 `320fa406d6b`，mutation 11/11，clean-room SAFE）+ B3（1 文件 `91b7d049eff`，字节对齐 + 邻接回归），在干净节点交接「视图 B2（DROP）」。
