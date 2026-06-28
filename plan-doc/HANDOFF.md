# 🤝 Session Handoff

> 滚动文档：每次 session 结束**覆盖式更新**（不累积历史；历史见 `git log plan-doc/HANDOFF.md`）。协作规范：[AGENT-PLAYBOOK.md](./AGENT-PLAYBOOK.md)
> **范围** = catalog-spi **主线** handoff。metastore/storage 抽取子线**已 CLOSED**（2026-06-22 合入 #64446/#64653/#64655）——`metastore-storage-refactor/` 仅历史留存、勿读；metastore-spi 现状直接读代码 `fe/fe-connector/fe-connector-metastore-spi/`。

---

# 🎯 下一个 session 的任务 = **C 类用户 docker e2e（翻闸前真实端到端验证）**，之后才是**翻闸原子提交**

> **⚠️ 本 session 做了一件事**：完成 **视图 B2（DROP VIEW + 强制删库视图级联）= commit `238e2840952`**。**视图面中立化（B0/B1/B2/B3）现已全部完成**——查询、DROP、强制删库级联、SHOW CREATE 四条 parity 能力齐活。

> **🔑 翻闸前剩余三块（顺序不变）**：
> 1. **C 类用户 docker e2e**（下一步）：跑用户的 iceberg docker 回归套件，验证 rewrite e2e / sizing / BE DV-038（可崩）/ V3·WHERE 等。**注意**：翻闸开关 `SPI_READY_TYPES` 尚未加 iceberg → 现在跑的是 **pre-flip** 路径（仍走 legacy 内置目录），docker 验证的是"翻闸前零行为变更"这一前提。真正的 **post-flip** e2e（视图 B1/B3 的绑定/命令臂、B2 的 DROP 写路径、所有 DDL/ALTER 写路径）**只能在翻闸后才能跑**（flip-gated），故称"未跑勿谎称"。
> 2. **翻闸原子提交**（FLIP，不可逆，最后一步）：加 `SPI_READY_TYPES`+iceberg **同时** GSON 迁移（`registerCompatibleSubtype` 全 8 catalog 变体 + 库 + 表 + 删旧 `registerSubtype`）+ 删 `CatalogFactory` legacy case + capability 核 + **用户二签**。详 [DEC-FLIP-1]。
> 3. 之后清理 fail-loud 范围外项（CREATE/RENAME VIEW、剩余 FU）。

---

# ⚖️ 关键决策（沿用，用户已签）

## [DEC-FLIP-1] 持久化 GSON 迁移 = 方向 A（并入翻闸原子提交，**不**单独提前做）
实证：一个 GSON label 只能登记一次 → 重映射 `"IcebergExternalCatalog"`→PluginDriven 必须先删旧 `registerSubtype`，删后任一 live `IcebergExternalCatalog` 序列化即 null-label 抛 → 翻闸前每存镜像必坏 FE checkpoint。paimon 先例 `38e7140ce56` 把「加 SPI_READY_TYPES + 删 registerSubtype + 加 registerCompatibleSubtype」放同一原子提交。**迁移就是翻闸的一部分。**

## [视图范围] = parity only（用户签，**B0/B1/B2/B3 全 DONE**）
查询（B1 ✅ `320fa406d6b`）/ DROP VIEW + 强制删库视图级联（B2 ✅ `238e2840952`）/ SHOW CREATE 视图（B3 ✅ `91b7d049eff`）/ 中立地基（B0 ✅ `d3837d4984a`）。**CREATE/RENAME VIEW 出范围**（grep 确认 pre-flip iceberg 无建视图写路径 → fail-loud 留后续）。配置开关保留 `enable_query_iceberg_views` 原名（parity）。

---

# ✅ 本 session 完成 = **视图 B2（DROP + 强制删库，`238e2840952`）**

**B2 做了什么（中文详解，不引用代号）**：翻闸到插件目录后，iceberg 视图的「删除」和「强制删库时连带删视图」会回归——插件路径用 `getTableHandle`/`tableExists` 定位对象，而 iceberg 视图存在另一个命名空间，这两个接口对视图一律返回"不存在"：`DROP VIEW 某视图` 报"找不到表"，`DROP DATABASE ... FORCE` 在最后删命名空间时报"非空"。本批补 parity（仅对齐翻闸前 legacy 行为）：

- **新中立 `dropView` SPI**：`ConnectorTableOps.dropView(session, db, view)` 默认 fail-loud（紧贴 B1 的 viewExists/listViewNames/getViewDefinition；调用方先经 `viewExists` 路由故生产不可达，是 guard）。
- **iceberg 连接器**：`IcebergCatalogOps.dropView` + 内部类 `CatalogBackedIcebergCatalogOps` 实现包 `((ViewCatalog)catalog).dropView`（gate `isViewCatalogEnabled` 否则 fail-loud，复刻 legacy `performDropView`）；`IcebergConnectorMetadata.dropView` 鉴权包裹归一 `DorisConnectorException`（与 dropTable/dropDatabase 写族一致）。
- **强制删库视图级联**：`IcebergConnectorMetadata.dropDatabase` 的 force 臂在删完所有表后，再 `listViewNames`+`dropView` 删完所有视图，最后删命名空间（复刻 legacy `performDropDb`，全在连接器内部，无新 fe-core SPI）。
- **删表路由到删视图**：fe-core `PluginDrivenExternalCatalog.dropTable` 在拿 handle 前先问 `metadata.viewExists`，是视图就走 `metadata.dropView`（复刻 legacy `dropTableImpl` 的 viewExists→performDropView 分派）；editlog(DropInfo)+unregisterTable 用 **LOCAL** 名（follower-replay parity），连接器调用用 **REMOTE** 名；`DorisConnectorException` 重包 `DdlException`。对不支持视图的连接器，`viewExists` 默认 false 且**无远程调用** → 路由对非 iceberg/paimon **零行为变化**（无需 supportsView 门）。**无新 instanceof Iceberg**（iron-law 干净；fe-core 内 iceberg 字样仅在镜像/legacy 注释里）。

**📊 B2 实证纠正**：① 设计初稿写的独立类 `CatalogBackedIcebergCatalogOps` 实为 `IcebergCatalogOps` 的**内部类**（非独立文件）；② `dropView` 放视图组（紧贴 loadViewDefinition）非 DDL-writes 组；③ view-less 连接器路由零远程调用。

**验证**：3 模块 fresh recompile；新增/改测试 **api 4 + iceberg 96 + fe-core 54 全绿**；**mutation 9/9 KILLED**（脚本 scratchpad `mutate_view_b2.py`）；**clean-room 对抗 review（5 reader + critic）= SAFE_TO_COMMIT（must-fix 0；critic 反驳 2/3 reader SHOULD_FIX 证测试非 inert）**；checkstyle 0；iron-law 0；连接器 import 0。**写路径需活 FE/真元数据 → flip-gated e2e 未跑（勿谎称）**。

**P6.1–P6.5 ✅ / P6.6：C1–C3 ✅ / C4 R1–R7 ✅ / C5 DDL/ALTER B1–B5 ✅ / flip-readiness 只读退化 ✅ / 视图 B0+B1+B2+B3 全 ✅** → 剩 **C docker e2e → 翻闸（含 GSON 迁移）**。iceberg **仍不在** `SPI_READY_TYPES`。

## ⛳ 结论（一句话）：**还不能翻闸，但视图面全清**。剩 **C 类用户 docker e2e、翻闸原子提交（含持久化 GSON）**。

## 📖 起步必读（动 C/翻闸前）
1. memory `iceberg-view-b2-done`（本 session）、`iceberg-view-b1-b3-done`、`iceberg-view-b0-done`、`iceberg-flip-readiness-gaps`（缺口全景，含 C 类 docker 清单 + DEC-FLIP）、`handoff-discipline-per-phase`、`consult-trino-before-spi-design`、`clean-room-adversarial-review-pref`、`ask-user-explain-in-chinese-first`、`doris-build-verify-gotchas`。
2. `plan-doc/tasks/designs/P6.6-C5-flip-readiness.md`（C 类 docker 清单 + 翻闸开关/持久化方案全景）；`plan-doc/tasks/designs/P6.6-view-spi.md`（视图 B0–B3 全 DONE）。

---

# ⚠️⚠️ 用户铁律：**fe-core 不得新增 `if(iceberg)` / `instanceof Iceberg*` / `import IcebergUtils` / 引擎名字符串判别（新 seam）**
iceberg 逻辑落 `fe-connector` 经中立 SPI。**legacy 豁免类**保留 iceberg 引用合法（C4 dead 子树 + commit-bridge 旧清单 + `PhysicalIcebergTableSink`/`bindIcebergTableSink` + `StatementContext` 旧 iceberg-typed stash + `IcebergExternalCatalog` + `ShowCreateDatabaseCommand`/`Env.getDdlStmt` legacy iceberg 臂 + `BindRelation case ICEBERG_EXTERNAL_TABLE` + `ShowCreateTableCommand` legacy ICEBERG 视图臂〔B3 加的是**并列** PLUGIN 臂〕+ `InsertUtils` 既有 `UnboundIcebergTableSink` 分支）。**B2 守则**：fe-core `PluginDrivenExternalCatalog.dropTable` 的视图路由全经中立 `metadata.viewExists`/`metadata.dropView`，无 instanceof Iceberg（已核 + clean-room 验 + iron-law 脚本 0）。

---

# 🟡 已登记 follow-up（非阻塞）
- **[FU-forcedrop-nosuchns]（B2 登，pre-existing 非 B2 引入）** `IcebergConnectorMetadata.dropDatabase` force 臂不吞 `NoSuchNamespaceException`（legacy `performDropDb:305-309` 吞→对已被 out-of-band 删的远程命名空间幂等成功）；**HEAD 的表级联已有此缺口**（`git show HEAD` 证），B2 仅把视图级联放进同一 `catch(Exception)` wrap → 视图臂与相邻表臂语义一致（B2 parity 契约）；要修须同时修表+视图两臂（catch NoSuchNamespaceException return），影响 force-drop 幂等性，翻闸前可选修。
- **[FU-view-gson-roundtrip]（B0 登）** `PluginDrivenExternalTable` 的 GSON round-trip 重算 `isView` 无直测（机制 clean-room code-verified sound）。
- **[FU-view-exception-arms]（B0 登，低）** 连接器 `viewExists`/`listViewNames`/`getViewDefinition` 异常归一化臂直测（B1 已对 getViewDefinition 加 auth-wrap 直测；viewExists/listViewNames/dropView 仍 byte-mirror）。
- **[FU-getsqldialect-deadcode]（B1 登，低）** legacy `IcebergExternalTable.getSqlDialect()` 零调用方=死码，可后续清理。
- **[FU-showcreatedb-render-ut]** `ShowCreateDatabaseCommand` plugin 臂渲染无直接 UT（flip-gated e2e 覆盖）。
- **[FU-createtablelike-plugin]** `getCreateTableLikeStmt` plugin 臂仍退化（paimon 已退化、非 iceberg 引入）。
- **[FU-show-partitions-deadcode]** `ShowPartitionsCommand` `instanceof IcebergExternalCatalog` 3 列臂 = 死码；翻闸后可选连接器侧补 5 列。
- **[FU-rewrite-output-sizing]（R6 登；R8 必触及）** 中立 driver 未线程 target-file-size + 自适应并行度。
- **[FU-flip-e2e]** 真翻闸端到端**未跑**（CI/flip-gated）；视图 B1/B3 绑定/命令臂、B2 DROP 写路径、所有 DDL/ALTER 写路径均 flip-gated。
- 其余（nested-nullability / where-literal-coercion / broker-write / doris-version-prop 等）见 git log 历史。

---

# ⚙️ 操作须知（复用）

- maven：`-f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl :<art> **-am** -DfailIfNoTests=false -Dmaven.build.cache.enabled=false`（漏 `-am`→假错 `${revision}`）。**fe-core 只依赖 `fe-connector-api`** → `:fe-core -am` 不拖 paimon。**fe-connector-paimon 单独 build 必须 `package`**。**iceberg/api** 正常 `-am test`。
- **⚠️ checkstyle 别加 `-am`**：`-am` 会把 `fe-common`（2381 个既存 checkstyle error）拖进假红 → 用 `mvn -pl :<art> checkstyle:check`（**不带 -am**）。
- **⚠️ bash 工具默认 timeout 120s**：fe-core build 超时 → 调 `timeout` 到 ~590000ms，或后台跑（fe-core 全模块 build ~2min）。
- **⚠️ maven 经管道 `$?` 是管道尾命令的** → 用 `${PIPESTATUS[0]}` 或 grep `BUILD SUCCESS`。`-q` 抑制 console → 读 surefire **XML** 的 `tests=`/`failures=`。
- **⚠️ stale .class 假红坑**：mutation 后脚本已 `os.utime`；**commit 前最终验证务必 fresh recompile**（本 session 已做：3 模块 clean-state 重编全绿）。
- 连接器禁 import fe-core：`bash tools/check-connector-imports.sh`。**连接器测试无 Mockito**（真 InMemoryCatalog/Recording fakes）；**fe-core 用 Mockito**（`CALLS_REAL_METHODS` + `Deencapsulation.setField` + stub `getConnector`/`getMetadata`/`buildConnectorSession`）。**⚠️ Mockito `anyString()` 不匹配 null**。
- **⚠️ InMemoryCatalog 是 ViewCatalog**（B2 seam 测可直接 buildView/dropView 真往返）；但 B2 seam 测用 `FakeIcebergViewCatalog`（与 viewExists/listViewNames 测同框架，`viewsByNs` 用**可变** ArrayList 才能 dropView 反映删除）。
- **mutation-check（Rule 9/12）**：范式 scratchpad `mutate_view_b2.py`（9 变异跨 4 模块；单行 exact-string 锚点 count==1 守；KILLED=maven rc!=0）。**⚠️ Python 3.6**：`subprocess.run(stdout=PIPE,stderr=STDOUT,universal_newlines=True)`（无 `capture_output`）。**⚠️ review（读源）与 mutation（改源）务必串行**。
- **cwd 会被 harness 重置** → 一律绝对路径。

# ⚠️ Commit 须知（任何 `git add` 前必读）

- **path-whitelist `git add`，严禁 `git add -A`**（scrub `regression-test/conf/regression-conf.groovy` 明文 key + `*.bak` + scratch `.audit-scratch/`·`conf.cmy/`·`META-INF/`·`docker/...`·仓根游离 `fe/IcebergScanPlanProvider.java`·`plan-doc/reviews/P5-paimon-rereview3-*`)。
- commit message：见 `git log` 范式 + 末尾 `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>` + `Claude-Session: …`。PR base = `branch-catalog-spi`，squash。
- **视图 B2 = `238e2840952`（10 文件）**。HANDOFF + 设计文档单独 commit（memory 在 `.claude/`、非仓内）。

# 📦 阶段状态

- **工作分支 = `catalog-spi-10-iceberg`**（off `branch-catalog-spi` @ `e5959e1b53d`，PR base = `branch-catalog-spi`，squash）。
- **⚠️ 推送状态**：P6.4 T01–T06+arg-move 已推 `origin`；**其后全部（含 commit-bridge + C4 + C5 + flip-readiness 两轮 + 视图 B0/B1/B2/B3）未 push**。**用户未要求 push**——留用户裁量。
- iceberg **不在** `SPI_READY_TYPES`（pre-flip 零行为变更）。metastore 子线 CLOSED（勿读）。
- **⚠️ 环境**：`/mnt/disk1` 紧（2.0T，本 session 起步 ~85G free，96% used）。**下个 session 起步先 `df -h /mnt/disk1`**；**勿用 worktree 隔离编译 agent**（复制整仓，盘不够）。

# 🧠 给下一个 agent 的 meta

- **删除/parity/动码前必 grep 全调用方 + 区分 DEAD vs STILL-CONSUMED**；**HANDOFF/设计/RFC 的依赖名/行号/不变式可能过时或错** —— 动码前先 recon（grep+实证）再信文档。**本 session 实证纠正 3 处**（独立类 vs 内部类、dropView 分组、view-less 路由零远程）。
- **clean-room 对抗 review 偏好**：moderate-大改动 = 多 reader 对抗 + critic（**review 读源与 mutation 改源不可并发**）。B2（10 文件、含元数据写）按 B1 范式跑了 5 reader + critic = SAFE。6 行 verbatim 镜像臂（如 B3）则焦点验证即可。
- **既有 Doris 行为/parity 优先**：pre-flip 一律零行为变更（B2 零变更，无新 DV）。
- **C docker e2e**：跑用户 iceberg docker 套件验 pre-flip 零变更；**真 post-flip 写路径 e2e 翻闸后才能跑（flip-gated 跑不了，勿谎称）**。翻闸=最后原子提交（含 GSON 迁移 + 用户二签）。
- **上下文超 30% 即交接**。本 session = 视图 B2（10 文件 `238e2840952`，mutation 9/9，clean-room SAFE），在干净节点交接「C 类用户 docker e2e → 翻闸」。
