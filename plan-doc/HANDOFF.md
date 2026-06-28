# 🤝 Session Handoff

> 滚动文档：每次 session 结束**覆盖式更新**（不累积历史；历史见 `git log plan-doc/HANDOFF.md`）。协作规范：[AGENT-PLAYBOOK.md](./AGENT-PLAYBOOK.md)
> **范围** = catalog-spi **主线** handoff。metastore/storage 抽取子线**已 CLOSED**（2026-06-22 合入 #64446/#64653/#64655）——`metastore-storage-refactor/` 仅历史留存、勿读；metastore-spi 现状直接读代码 `fe/fe-connector/fe-connector-metastore-spi/`。

---

# 🎯 下一个 session 的任务 = **视图中立化 B1 / B2 / B3（翻闸前最后一块功能：视图查询 / DROP VIEW / SHOW CREATE 视图）**

> **⚠️ 本 session 做了两件事**：① **回答了「持久化 GSON 迁移」决策（用户签 = 方向 A）**——实证发现该迁移在序列化语义上**与翻闸绑死、无法安全地单独提前做**，必须并入翻闸原子提交（见下「关键决策」）；② 据此本 session **转做视图**，完成了 **视图 B0（中立地基）= commit `d3837d4984a`**（翻闸后视图经连接器能力上报 `isView()` + 留在 SHOW TABLES + INSERT 拦截）。

> **🔑 下一步 = 视图 B1/B2/B3**（设计全在 `plan-doc/tasks/designs/P6.6-view-spi.md`，动码前读它 + 对真实代码 re-grep；三批**都只依赖 B0、互不依赖、可任意顺序/并行**）：
> - **B1（视图查询）**：`BindRelation case PLUGIN_EXTERNAL_TABLE`（注意与 PAIMON/MAX_COMPUTE/TRINO/LAKESOUL **共享 fall-through**，须先 `if supportsView() && isView()` 分流）加视图臂：time-travel 拒绝 + 取视图体 + **复用现成中立 `parseAndAnalyzeExternalView`（`BindRelation.java:743` 签名全中立，HMS hive-view 共用，零改动复用）** + `LogicalSubQueryAlias`；config 门**保留 `enable_query_iceberg_views` 原名**（用户签 = parity）；**视图体 = lazy 方法**（新 `ConnectorMetadata.getViewDefinition(session, handle)` 返回中立 DTO `{sql, dialect}`，**一次 round-trip**；pre-flip 视图体本就 lazy，**勿**做 eager reserved-key）；**dialect 未知 fail-loud**（用户偏好「无法精确就报错」）。
> - **B2（DROP VIEW + 强制删库视图级联）**：新 `ConnectorMetadata.dropView` SPI（默认抛）+ iceberg 实现（包 `((ViewCatalog) catalog).dropView`）；`PluginDrivenExternalCatalog.dropTable` 经 `viewExists`（B0 已加）路由到 `dropView`（镜像 legacy `IcebergMetadataOps.dropTableImpl:407` 的 viewExists→performDropView 分派）；force-drop-db 视图级联**全在连接器内部**（`IcebergConnectorMetadata.dropDatabase` force 臂补 `listViews`+`dropView` 再 `dropNamespace`，镜像 legacy `performDropDb:298`）。**涉元数据写 → 需 e2e（flip-gated）**。
> - **B3（SHOW CREATE 视图）**：`ShowCreateTableCommand` 加 `PluginDrivenExternalTable` 视图臂（`supportsView() && isView()` gated），渲染 `CREATE VIEW \`name\` AS <视图体>`（搬 `IcebergUtils.showCreateView` 逻辑进中立臂），替代落入 `Env.getDdlStmt` 的 CREATE TABLE 路径。**纯渲染、最低风险，可先做**。

> **推荐顺序**：B3（纯渲染最安全）或 B1（核心能力）先，B2（写元数据需 e2e）随后；三批可并行提交。**CREATE VIEW / RENAME VIEW 不做**（grep 确认 pre-flip iceberg 无建视图写路径 → 出 parity 范围，明确 fail-loud 留后续）。

> **之后的翻闸顺序（已据本 session 决策更新）**：视图 B1/B2/B3 → **C 类用户 docker e2e** → **翻闸原子提交**〔加 `SPI_READY_TYPES`+iceberg **同时** GSON 迁移（`registerCompatibleSubtype` 全 8 catalog 变体 + 库 + 表）+ 删 `CatalogFactory` legacy case + capability 核 + 用户二签〕（FLIP，不可逆，最后一步）。

---

# ⚖️ 关键决策（本 session，用户已签）

## [DEC-FLIP-1 修正] 持久化 GSON 迁移 = 方向 A（并入翻闸原子提交，**不**单独提前做）
**背景**：原计划把「让旧 iceberg 持久态在翻闸后兼容」（`GsonUtils.registerCompatibleSubtype(PluginDrivenExternalCatalog.class, "Iceberg…")`）当成翻闸前的独立一步。**本 session 实证发现这不可行**：
- `RuntimeTypeAdapterFactory:275` 规定一个 label 只能登记一次 → 要把 `"IcebergExternalCatalog"` 重映射到 PluginDriven（`registerCompatibleSubtype`），**必须先删** `registerSubtype(IcebergExternalCatalog.class,…)`。
- 删掉后，序列化任一 live `IcebergExternalCatalog` 实例时 `subtypeToLabel.get()` 为 null → `write():340` 抛 "cannot serialize"。
- 翻闸前 iceberg **不**在 `SPI_READY_TYPES`，`CatalogFactory` 新建 iceberg catalog 仍造 `IcebergExternalCatalog` 对象 → 每次存镜像（`CatalogMgr.write():892` `GsonUtils.GSON.toJson` 把整张 `idToCatalog` 序列化）必撞上 → **FE checkpoint 坏**。
- **paimon 先例铁证**：commit `38e7140ce56` 把「加 paimon 进 SPI_READY_TYPES」+「删 registerSubtype(Paimon\*)」+「加 registerCompatibleSubtype」**放在同一个原子提交**——迁移**就是**翻闸的一部分。
**用户裁决 = 方向 A**：迁移留到翻闸原子提交一起做（iceberg→PluginDrivenExternalCatalog；库→PluginDrivenExternalDatabase；表→PluginDrivenMvccExternalTable〔iceberg 声明 SUPPORTS_MVCC_SNAPSHOT〕，跟 paimon `GsonUtils:389-411` 范式）。本 session 据此转做视图。

## [视图范围] = parity only（用户签）
查询 / DROP VIEW / 强制删库视图级联 / SHOW CREATE 视图（恢复 pre-flip 已有能力）；CREATE/RENAME VIEW 出范围。配置开关保留 `enable_query_iceberg_views` 原名（parity）。

---

# ✅ 本 session 完成 = **视图 B0 中立地基**（commit `d3837d4984a`）

**做了什么（中文详解，不引用代号）**：翻闸（iceberg 改走插件化通用 catalog）后，视图会**静默坏掉**——两根因：① 连接器列出表名时主动减掉视图名，pre-flip 由 fe-core 再加回，翻闸后没有地方加回 → 视图从 `SHOW TABLES` 消失；② 通用插件表的「我是不是视图」恒为 false → 所有按此分流的逻辑把视图当普通表。本批是**最安全的纯加法地基**，让翻闸后的 iceberg 视图能：(1) 经中立能力上报「我是视图」，(2) 留在 SHOW TABLES，(3) 被 INSERT 时拦下报错。具体查询/DROP/SHOW CREATE 的视图臂留作 B1/B2/B3。

**怎么改（9 产品 + 9 测试 = 18 文件）**：
- 新增连接器能力 `SUPPORTS_VIEW`（iceberg 声明；jdbc/es 不声明 = 零影响）。
- `ConnectorTableOps` 加 `viewExists`/`listViewNames`（默认 false/空 = 视图无关连接器的零变更契约）；iceberg 连接器实现（`IcebergCatalogOps` 真 ViewCatalog 逻辑 + `IcebergConnectorMetadata` 鉴权包裹，镜像 legacy `IcebergMetadataOps`）。
- `PluginDrivenExternalTable.isView()` 改为按能力经 `viewExists` 解析（`objectCreated` 缓存，镜像 legacy `IcebergExternalTable.makeSureInitialized`）；系统表子类覆盖 `resolveIsView()→false`（避免对 `$snapshots` 等合成名做无谓远程调用）。
- `PluginDrivenExternalCatalog.listTableNamesFromRemote` 在有 `SUPPORTS_VIEW` 时把 `listViewNames` 并回（镜像 legacy 合并点）。
- `InsertUtils.normalizePlan` 加「不能 INSERT 进视图」中立拦截（legacy engine sink 的 isView 守卫翻闸后走连接器 sink，故守卫上移到中立写路径）。

**📊 实证纠正了设计初稿 2 处**：① is-view **不**走 schema reserved-key——视图根本不产生 schema（`getTableHandle`→`tableExists` 对 view false → 空 handle），故必须像 pre-flip 一样独立 `viewExists` 解析；② **不需要** `BindRelation` 临时 fail-loud——`FileQueryScanNode.doInitialize:149` 已对 `isView()` fail-loud，B0→B1 间查询视图直接报错非静默（更 surgical，省该改动）。

**验证**：3 模块 fresh recompile；针对性测试全绿（api 2 + iceberg 66 + fe-core 33）+ 广义回归（iceberg 756 / fe-core PluginDriven\* 263）；**mutation 13/13 KILLED**（脚本 scratchpad `mutate_view_b0.py`）；**clean-room 对抗 review（6 reader + critic）= 产品代码 SAFE**（iron-law 干净、pre-flip 零变更、GSON-staleness〔`isView` 无 `@SerializedName` + `objectCreated` replay 重置〕与 NPE〔`connector!=null` + 真连接器返非空 EnumSet〕均排除、parity 忠实），**2 处测试有效性缺口已修**（inert gate test 补非空 remoteName/db；auth-wrap 契约 6→8）+ 1 parity（`viewExists` wrap-all 对齐 legacy 存在性检查）+ 1 convention（`ConnectorViewDefaultsTest`）；checkstyle 0；iron-law（`tools/check-connector-imports.sh`）0；**e2e flip-gated 未跑（勿谎称）**。

**P6.1–P6.5 ✅ / P6.6：C1–C3 ✅ / C4 R1–R7 ✅ / C5 DDL/ALTER B1–B5 ✅ / flip-readiness 只读退化（自动统计+Top-N、SHOW CREATE TABLE/DB）✅ / 视图 B0 ✅** → 剩 **视图 B1/B2/B3 + C docker** → 翻闸（含 GSON 迁移）。iceberg **仍不在** `SPI_READY_TYPES`。

## ⛳ 结论（一句话）：**还不能翻闸**。视图 B0 地基已干净落地；剩 **视图 B1/B2/B3、C docker e2e、翻闸原子提交（含持久化 GSON）**。

## 📖 起步必读（动下一批前）
1. memory `iceberg-view-b0-done`（本 session）、`iceberg-flip-readiness-gaps`（缺口全景）、`iceberg-showcreate-flip-done`/`iceberg-bclass-autoanalyze-topn-done`（前两轮能力化范式）、`handoff-discipline-per-phase`、`consult-trino-before-spi-design`、`clean-room-adversarial-review-pref`、`ask-user-explain-in-chinese-first`、`doris-build-verify-gotchas`。
2. `plan-doc/tasks/designs/P6.6-view-spi.md`（视图设计 + 破坏清单 + B1/B2/B3 切片 + 决策；B0 已标 DONE）。
3. **下批锚点（动码前 re-grep）**：`BindRelation.java:620`(legacy ICEBERG 臂)/`:653`(PLUGIN 共享 fall-through)/`:743`(parseAndAnalyzeExternalView 中立签名)；`IcebergExternalTable.getViewText:363`/`getSqlDialect:390`(lazy 视图体，移植进连接器)；`IcebergMetadataOps.dropTableImpl:407`/`performDropDb:298`/`performDropView`(B2 移植源)；`ShowCreateTableCommand:168`/`IcebergUtils.showCreateView:1808`(B3 移植源)；`ConnectorMetadata`(加 getViewDefinition/dropView 默认方法)。

---

# ⚠️⚠️ 用户铁律：**fe-core 不得新增 `if(iceberg)` / `instanceof Iceberg*` / `import IcebergUtils` / 引擎名字符串判别（新 seam）**
iceberg 逻辑落 `fe-connector` 经中立 SPI。**legacy 豁免类**保留 iceberg 引用合法（C4 dead 子树 + commit-bridge 旧清单 + `PhysicalIcebergTableSink`/`bindIcebergTableSink` + `StatementContext` 旧 iceberg-typed stash + `IcebergExternalCatalog`〔分区演进/ALTER〕+ `ShowCreateDatabaseCommand`/`Env.getDdlStmt` legacy iceberg 臂 + `BindRelation case ICEBERG_EXTERNAL_TABLE`〔pre-flip iceberg 仍是该类，服务那一半；B1 加的是**并列的 PLUGIN 臂**，不动 ICEBERG 臂〕+ `InsertUtils` 既有 `UnboundIcebergTableSink` 分支〔commit `87d949800b0`，非本线引入〕）。**本 session 守则**：B0 新增的能力 helper / `isView`/`resolveIsView` / 库列表并回 / 插入拦截全经中立能力 + 中立 SPI，无 instanceof Iceberg（已核 + clean-room 验 + iron-law 脚本 0）。

---

# 🟡 已登记 follow-up（非阻塞）
- **[FU-view-gson-roundtrip]（B0 登）** `PluginDrivenExternalTable` 的 GSON round-trip 重算 `isView` 无直测（机制 clean-room code-verified sound：`isView` 无 `@SerializedName` → 不序列化；`objectCreated` replay 重置 → 重算）。补一个序列化往返 + 断言 `isView()` 重算的测试。
- **[FU-view-exception-arms]（B0 登，低）** 连接器 `viewExists`/`listViewNames` 异常归一化臂（wrap-all vs rethrow-RuntimeException）无直测（byte-mirror 已测姊妹方法）；可加 RecordingIcebergCatalogOps 抛错路径。
- **[FU-showcreatedb-render-ut]** `ShowCreateDatabaseCommand` plugin 臂渲染无直接 UT（重，需 Env/CatalogMgr/权限 stub）。flip-gated e2e 覆盖。
- **[FU-createtablelike-plugin]** `getCreateTableLikeStmt` plugin 臂仍退化（paimon 已退化、非 iceberg 引入）。
- **[FU-show-partitions-deadcode]** `ShowPartitionsCommand` `instanceof IcebergExternalCatalog` 3 列臂 = 死码；翻闸后可选连接器侧补 5 列。
- **[FU-rewrite-output-sizing]（R6 登；R8 必触及）** 中立 driver 未线程 target-file-size + 自适应并行度。
- **[FU-flip-e2e]** 真翻闸端到端**未跑**（CI/flip-gated）。
- 其余（nested-nullability / where-literal-coercion / broker-write / doris-version-prop 等）见 git log 历史。

---

# ⚙️ 操作须知（复用）

- maven：`-f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl :<art> **-am** -DfailIfNoTests=false -Dmaven.build.cache.enabled=false`（漏 `-am`→假错 `${revision}`）。**fe-core 只依赖 `fe-connector-api`** → `:fe-core -am` 不拖 paimon。**fe-connector-paimon 单独 build 必须 `package`**（HiveConf 来自 `<optional>` shade）。**iceberg/api** 正常 `-am test`。
- **⚠️ bash 工具默认 timeout 120s**：fe-core build 超时 → 调 `timeout` 参数到 ~600000ms，或后台跑。
- **⚠️ maven 经管道时 `$?` 是管道尾命令的**（非 maven 的）→ 用 `set -o pipefail` + `${PIPESTATUS[0]}`，或 grep `BUILD SUCCESS`/`BUILD FAILURE`。`-q` 抑制 console → 读 surefire **XML**（`TEST-<class>.xml`）的 `tests=`/`failures=` 确认真跑。
- **⚠️ stale .class 假红坑**：mutation 后 `os.utime(f, None)`（脚本已做）或 `mvn clean`。**commit 前最终验证务必 fresh recompile**（本 session 已做）。
- **⚠️ checkstyle**：import 同组无空行 + 组内 ASCII 序（大写在小写前）。`mvn checkstyle:check -pl :<mods>`。
- 连接器禁 import fe-core：`bash tools/check-connector-imports.sh`。**连接器测试无 Mockito**（真 InMemoryCatalog〔= ViewCatalog，可建视图〕/ Recording fakes）；**fe-core 用 Mockito**（`CALLS_REAL_METHODS` + `Deencapsulation.setField` + stub `getConnector`/`getCapabilities`/`buildConnectorSession`/`getMetadata`/`getDbOrAnalysisException`）。**⚠️ Mockito `anyString()` 不匹配 null**——CALLS_REAL_METHODS 表测 isView/resolveIsView 须 set 非空 `remoteName`+`db.getRemoteName()`，否则 mutation 假绿（本 session clean-room 抓到 + 修）。
- **mutation-check（Rule 9/12）**：范式 scratchpad `mutate_view_b0.py`（13 变异跨 4 模块；**单行 exact-string 锚点须唯一**〔脚本 count==1 守〕；KILLED=build/test FAIL=rc!=0）。**⚠️ Python 3.6**：`subprocess.run(stdout=PIPE,stderr=STDOUT,universal_newlines=True)`（无 `capture_output`）。**⚠️ review（读源）与 mutation（改源）务必串行**。
- **cwd 会被 harness 重置**→一律绝对路径。

# ⚠️ Commit 须知（任何 `git add` 前必读）

- **path-whitelist `git add`，严禁 `git add -A`**（scrub `regression-test/conf/regression-conf.groovy` 明文 key + `*.bak` + scratch `.audit-scratch/`·`conf.cmy/`·`META-INF/`·`docker/...`·仓根游离 `fe/IcebergScanPlanProvider.java`·`plan-doc/reviews/P5-paimon-rereview3-*`)。
- commit message：见 `git log` 范式 + 末尾 `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>` + `Claude-Session: …`。PR base = `branch-catalog-spi`，squash。
- **视图 B0 = `d3837d4984a`**（18 文件 fe/）。HANDOFF + 设计文档单独 commit（memory 在 `.claude/`、非仓内）。

# 📦 阶段状态

- **工作分支 = `catalog-spi-10-iceberg`**（off `branch-catalog-spi` @ `e5959e1b53d`，PR base = `branch-catalog-spi`，squash）。
- **⚠️ 推送状态**：P6.4 T01–T06+arg-move 已推 `origin`；**其后全部（含 commit-bridge + C4 + C5 + flip-readiness 两轮 + 视图 B0）未 push**。**用户未要求 push**——留用户裁量。
- iceberg **不在** `SPI_READY_TYPES`（pre-flip 零行为变更）。metastore 子线 CLOSED（勿读）。
- **⚠️ 环境**：`/mnt/disk1` 紧（2.0T，本 session 起步 ~85G free，96% used）。**下个 session 起步先 `df -h /mnt/disk1`**；**勿用 worktree 隔离编译 agent**（复制整仓，盘不够）。

# 🧠 给下一个 agent 的 meta

- **删除/parity/动码前必 grep 全调用方 + 区分 DEAD vs STILL-CONSUMED**；**HANDOFF/设计/RFC 的依赖名/行号/不变式可能过时或错** —— 动码前先 recon（grep+实证）再信文档。**本 session 实证纠正 2 处**（is-view 非 schema-key 而是独立 viewExists；B0 无需 BindRelation 临时 fail-loud 因 scan 守卫已存在）。
- **clean-room 对抗 review 偏好**：大改动 = 多 reader 对抗 + critic。**⚠️ review（读源）与 mutation（改源）不可并发**。mutation 结果是测试充分性的权威判据；但 mutation 也会漏（如 auth-wrap、anyString-null）——clean-room 的 test-sufficiency reader 是互补一层（本 session 它抓到 2 处 mutation 漏的 inert-test）。
- **既有 Doris 行为/parity 优先**：pre-flip 一律零行为变更（本 session B0 零变更，无新 DV）。
- **B1/B2/B3 需先读 `P6.6-view-spi.md` + 对真实代码 re-grep**；视图体走 **lazy `getViewDefinition` 方法**（非 eager reserved-key，pre-flip 视图体本就 lazy）；dialect fail-loud；复用 `parseAndAnalyzeExternalView`（零改动）。
- **上下文超 30% 即交接**。本 session = 视图 B0（18 文件 commit `d3837d4984a`；mutation 13/13；clean-room SAFE，2 测试缺口已修），在干净节点交接「视图 B1/B2/B3」。
