# 🤝 Session Handoff

> 滚动文档：每次 session 结束**覆盖式更新**（不累积历史；历史见 `git log plan-doc/HANDOFF.md`）。协作规范：[AGENT-PLAYBOOK.md](./AGENT-PLAYBOOK.md)
> **范围** = catalog-spi **主线** handoff。metastore/storage 抽取子线**已 CLOSED**（2026-06-22 合入 #64446/#64653/#64655）——`metastore-storage-refactor/` 仅历史留存、勿读；metastore-spi 现状直接读代码 `fe/fe-connector/fe-connector-metastore-spi/`。

---

# 🎯 下一个 session 的任务 = **flip-readiness 剩余「B 类」= SHOW CREATE TABLE / SHOW CREATE DATABASE 退化修复（连接器侧 + 少量 fe-core）**

> **⚠️ 本 session = flip-readiness B 类「其一」✅ 完成**：翻闸后「自动统计停摆」「Top-N 懒物化失效」两条只读退化路径，已改为**按连接器能力判别**（新增 2 个 ConnectorCapability，commit `6f3c255cd52`）。**B 类只读退化还剩 2 项**（SHOW CREATE TABLE / SHOW CREATE DATABASE），下个 session 做。

> **🔑 下一步（flip-readiness B 类「其二」）= SHOW CREATE 渲染退化**（动批前先读 `P6.6-C5-flip-readiness.md` 的 B 节 + 本 HANDOFF「📊 recon 实证结论」，再对真实代码 re-grep；锚点行号会漂）：
> - **SHOW CREATE TABLE 退化**（`Env.getDdlStmt` plugin 臂 `:4936` `rendersLocationProperties` 仅认 PAIMON）：翻闸后 iceberg 落 plugin 臂只剩空壳。**两层**：① tier-1（LOCATION + 扁平 PROPERTIES）= 纯 fe-core，把 `:4936` 引擎 gate 扩到 iceberg 引擎 + LOCATION key `location`→`path` 回退（iceberg 连接器发 `location` 键、paimon 发 `path`）；② tier-2（ORDER BY 排序 + PARTITION BY LIST(transform) 子句）= fe-core 在 PluginDrivenExternalTable 上**没有 live iceberg API**，须**连接器预渲染** Doris 语法子句进新中立属性键（如 `show.partition-clause`/`show.sort-clause`）由 plugin 臂原样追加。**另**：`getCreateTableLikeStmt` plugin 臂 `:4517` 是 comment-only——这条**当前已退化 paimon**（pre-existing，非 iceberg 引入），顺手补到 tier-1 水平。
> - **SHOW CREATE DATABASE 丢 LOCATION**（`ShowCreateDatabaseCommand:95` 仅 `instanceof IcebergExternalCatalog`）：**paimon 现在也退化**（已是 PluginDriven、落 generic else 无 LOCATION）→ 非 iceberg-only、**无可抄的中立臂**，须**新中立 SPI**暴露库 location（连接器 override `getDatabase()` surface namespace location → `ConnectorDatabaseMetadata` 属性键 `location`，**Trino 用 properties-map 模型，建议对齐**）+ fe-core plugin 臂渲染。连接器侧 `IcebergCatalogOps.loadNamespaceLocation` 已存在但仅内部用、未经 SPI。
> - **决策（动手前用中文向用户表面化）**：① 新 SPI 放 `ConnectorDatabaseMetadata.getProperties()` 的 `location` 键（Trino 对齐、零新面）vs 类型化 `Optional<String> getLocation()`；② B1 翻闸首版是「降级 parity（仅 LOCATION+PROPERTIES）+ 记缺口」还是「阻塞到全 parity（含 transform/sort 预渲染）」。建议降级 + fast-follow。
> - 这两项守铁律（capability/engine 判别或新中立 SPI，**非 instanceof Iceberg**）。**[FU-doris-version-prop]/嵌套裁剪等非阻塞项不在此批**。

> **之后的翻闸顺序（不变）**：flip-readiness B 类「其二」（SHOW CREATE TABLE/DB）→ **[DEC-FLIP-1] 持久化 GSON 迁移**（`GsonUtils.registerCompatibleSubtype(PluginDrivenExternalCatalog.class, "Iceberg…")` 全 8 变体 + 库 + 表，跟 paimon `:389-411` 范式）→ **A3/B6 视图 SPI**（DROP VIEW / force-drop 含视图库 / 视图查询 / SHOW CREATE 视图）→ **C 类用户 docker e2e** → **加 `SPI_READY_TYPES`+iceberg / 删 `CatalogFactory` legacy case / capability 核 / 用户二签**（FLIP，不可逆，最后一步）。

---

# ✅ 本 session 完成 = **iceberg 翻闸后自动统计 + Top-N 懒物化按连接器能力判别**（commit `6f3c255cd52`）

**做了什么（中文详解，不引用代号）**：
让翻闸（iceberg 改走插件化通用 catalog）后两条只读路径不再「静默退化」——它们原本「先判断这个表是不是 Iceberg 专属类型」，翻闸后 iceberg 不再是那个类型就漏掉：
1. **后台自动统计信息收集**：优化器（CBO）拿不到 iceberg 表的列统计（每列不同值数/最大最小/空值比例）→ 计划变差。
2. **Top-N 懒物化**（`SELECT * … ORDER BY c LIMIT 10` 先只读排序列定位前 N 行号、再回读那 N 行其余列）→ 退化成全列读。

**怎么改（用户裁决 = 能力标记，非引擎字符串）**：
- 新增 2 个连接器能力枚举 `SUPPORTS_COLUMN_AUTO_ANALYZE`、`SUPPORTS_TOPN_LAZY_MATERIALIZE`；iceberg 连接器声明两者，paimon 连接器**只**声明自动统计（用户裁决「顺手也修 paimon 的自动统计」；懒物化 paimon 从来不在白名单 = parity 不开）。
- `PluginDrivenExternalTable` 加两个 helper（镜像既有 `supportsParallelWrite`，问连接器「你支持吗」）。
- fe-core 三处「判别网关」在**保留**的 legacy iceberg 臂旁**附加**中立的「插件表 + 能力」臂（自动统计白名单 + FULL 强制 + 懒物化白名单）。两个自动统计网关谓词**字节一致**——保证「被收进自动统计的插件表必走 FULL」（否则 sample 分析未实现会抛错）。懒物化**不做** blanket `isAssignableFrom`（jdbc/es 也是 PluginDriven，但不声明能力 → 仍排除）。

**唯一 pre-flip live 行为变更（DV 登记）= DV-paimon-autoanalyze**：**paimon 后台自动统计在本提交即生效**（paimon 已在 `SPI_READY_TYPES`、本就是 PluginDriven，非翻闸 gated）。用户已签、parity-safe（复用 manual ANALYZE 同一 `doFull` SQL 路径）。iceberg 全部 pre-flip 零变化（仍走 legacy 臂）。

**📊 recon 实证结论（关键，给下个 agent）**：原审计列的 B 类 5 项里——
- **SHOW PARTITIONS「3 列退化 1 列」= 误报**：现对 iceberg 表 `SHOW PARTITIONS` **直接报错**（`ShowPartitionsCommand:203` validate 只放行 internal/HMS/PluginDriven，不放行 iceberg），那段 3 列代码是**永不可达死代码**。翻闸后反而能过 validate = **改善非退化**。**下批不必当退化修**，顶多顺手删死码 / 可选连接器侧补 5 列（非阻塞）。
- B4 自动统计 / B5 Top-N = 真退化，**本批已修**。
- B1 SHOW CREATE TABLE / B2 SHOW CREATE DATABASE = 真退化，**下批做**（见上「下一步」，含两层 + 新 SPI + 2 决策）。
- B6 SHOW CREATE 视图 = 随 A3 视图 scope。

**落地文件（6 改 + 6 测试，commit `6f3c255cd52`）**：
- 改 SPI/连接器：`ConnectorCapability`（+2 枚举）、`IcebergConnector.getCapabilities`（+2）、`PaimonConnector.getCapabilities`（+1，仅自动统计）。
- 改 fe-core：`PluginDrivenExternalTable`（+2 helper）、`StatisticsUtil.supportAutoAnalyze`（+中立臂）、`StatisticsAutoCollector.processOneJob`（+中立 FULL 臂）、`MaterializeProbeVisitor.checkRelationTableSupportedType`（+中立臂）。
- 改测试：`IcebergConnectorTest`（+1 能力断言）、`PaimonConnectorMetadataMvccTest`（+1 能力断言，含 NOT topn 负断言）、`PluginDrivenExternalTableTest`（+3 helper：能力映射/独立性/判空守卫）、`StatisticsAutoCollectorTest`（whitelist 双向 + processOneJob FULL 网关，去掉会抛错的真 jdbc 裸 catalog 负例改 mock）、`MaterializeProbeVisitorTest`（+2：能力在/不在）。

**验证**：fe-core **19** 测试（9+5+5）全绿、iceberg **7**、paimon **41** 全绿（fresh recompile；paimon 须 `-am package` 因 hive-shade）；**mutation 9/9 KILLED**（6 fe-core + 3 连接器，脚本 scratchpad `mutate_bclass.py`/`mutate_connectors.py`）；**clean-room 对抗 review（3 reader + critic）= SAFE_TO_COMMIT**（6 不变式全独立验证，0 BLOCKER/MAJOR；2 NIT/MINOR = paimon live-on-merge 注释澄清〔已补〕 + makeSureInitialized 副作用〔幂等、在 per-job try/catch、init 本就先于网关，benign〕）；checkstyle 4 模块 0；iron-law clean（`tools/check-connector-imports.sh` 0；新 fe-core 代码无 `instanceof Iceberg`，仅注释提及）；**e2e flip-gated 未跑**（勿谎称）。

**P6.1–P6.5 ✅ / P6.6：C1–C3 ✅ / C4 R1–R7 ✅ / C5 DDL/ALTER B1–B5 ✅ / flip-readiness B 类「自动统计+Top-N」✅ → 剩 SHOW CREATE TABLE/DB + 持久化 + 视图 + C docker → 翻闸**。iceberg **仍不在** `SPI_READY_TYPES`。

## ⛳ 结论（一句话）：**还不能翻闸**。DDL/ALTER 全量对齐 + flip-readiness 两条「静默退化」（自动统计、Top-N）已干净修完；剩 **SHOW CREATE TABLE/DB 退化、持久化迁移、视图、C docker e2e**。

## ✅ 用户决策（仍有效）
- **[DEC-FLIP-2 重申] = 全量对齐**：DDL/ALTER 18 op + B 类 + 视图(A3/B6) 全部翻闸前修；仅 C 类（写路径正确性）归用户 docker，D 类翻闸后。
- **[DEC-FLIP-1] = 方向一（GSON 迁移）**：`registerCompatibleSubtype(PluginDrivenExternalCatalog.class, "Iceberg…")` 全 8 catalog 变体 + 库 + 表（跟 paimon `:389-411` 范式），重启自动升级。
- **[本 session 签] 判别机制 = 能力标记（ConnectorCapability）**；**范围 = 顺手也修 paimon 自动统计**（懒物化仍 iceberg-only）。

## 📖 起步必读（动下一批前）
1. memory `iceberg-flip-readiness-gaps`（缺口全景）、`iceberg-bclass-autoanalyze-topn-done`（本 session）、`handoff-discipline-per-phase`、`consult-trino-before-spi-design`、`clean-room-adversarial-review-pref`、`ask-user-explain-in-chinese-first`。
2. `plan-doc/tasks/designs/P6.6-C5-flip-readiness.md`（**B 节 = 下一步主文档**；注意其 B3 SHOW PARTITIONS 描述已被本 session recon 证伪=误报）。
3. **下批 legacy 锚点（动码前 re-grep，行号会漂）**：`Env.getDdlStmt` 的 iceberg 臂(~`:4885`)/plugin 臂 gate(~`:4936`) + `getCreateTableLikeStmt`(~`:4517`)；`ShowCreateDatabaseCommand`(~`:95`)；`IcebergConnectorMetadata` 的 tableProps `location` 键 + `ConnectorDatabaseMetadata`/`ConnectorSchemaOps.getDatabase`。

---

# ⚠️⚠️ 用户铁律：**fe-core 不得新增 `if(iceberg)` / `instanceof Iceberg*` / `import IcebergUtils`（新 seam）**
iceberg 逻辑落 `fe-connector` 经中立 SPI。**legacy 豁免类**保留 iceberg 引用合法（C4 dead 子树 + commit-bridge 旧清单 + `PhysicalIcebergTableSink`/`bindIcebergTableSink` + `StatementContext` 旧 iceberg-typed stash + `IcebergExternalCatalog`〔分区演进/ALTER 内对 IcebergMetadataOps 的强转〕）。**本 session 守则**：`StatisticsUtil`/`StatisticsAutoCollector`/`MaterializeProbeVisitor` 里**保留**的 legacy `instanceof IcebergExternalTable` 臂是 pre-flip 零变化所必需（pre-flip iceberg 仍是该类）——**未新增** iceberg 引用，新臂全是中立 `instanceof PluginDrivenExternalTable` + 能力。R6/R7 + B1–B5 + 本 session 新增通用类（含 2 新 ConnectorCapability + 2 helper）全经中立 SPI、无 instanceof Iceberg（已核 + clean-room 验）。

---

# 🔴🔴 开放 — P6.6 翻闸（C1+C2+C3 全闭，C4 ✅，C5 DDL/ALTER ✅，flip-readiness「自动统计+Top-N」✅；剩 SHOW CREATE TABLE/DB + 持久化 + 视图 + C docker）

- **[C4 R1–R7 ✅]** rewrite_data_files 翻闸就绪（Option B 全对等）。R8（flip rehearsal）= C 类 docker。
- **[C5 DDL/ALTER ✅]** 18-op buildout（B1–B5）全完成。
- **[flip-readiness B 类]** ✅ 自动统计（B4）+ Top-N 懒物化（B5）；❌ 剩 SHOW CREATE TABLE（B1）+ SHOW CREATE DATABASE（B2）；SHOW PARTITIONS（B3）= 误报无需修（recon 实证）。
- **[剩余]** SHOW CREATE TABLE/DB + DEC-FLIP-1 持久化 GSON + A3/B6 视图 SPI + C 类 docker。
- **[FLIP，不可逆 = 最后一步]** `SPI_READY_TYPES`+iceberg / 删 `CatalogFactory case:137-140` / GSON remap / capability 核 / 用户二签。

**[pre-flip 行为偏差中央登记]**：P6.4=DV-045/046/047；P6.5=DV-048/049；commit-bridge=[DV-S2-rederive]；C4 R7=DV-T05r-where；C5 B5=DV-partkey-msg；**本 session=DV-paimon-autoanalyze**（paimon 后台自动统计 live-on-merge，用户已签、parity-safe）。

**⚠️ C5 才动 `SPI_READY_TYPES`**（`CatalogFactory:50-51`，现 = {jdbc,es,trino-connector,max_compute,paimon}）。

---

# 🟡 已登记 follow-up（非阻塞）
- **[FU-show-partitions-deadcode]（本 session 登）** `ShowPartitionsCommand:446` `instanceof IcebergExternalCatalog` 3 列臂 = 死码（validate 不放行 iceberg）；翻闸后可选连接器侧补 5 列（声明 SUPPORTS_PARTITION_STATS + listPartitions，照 paimon）。非退化、非阻塞。
- **[FU-nested-nullability]（B2b ✅ SPI/builder 层闭）** Doris `ArrayType.getContainsNull()` 硬编码 true。
- **[FU-doris-version-prop]（B1 登）** 连接器 createTable 丢 `doris.version` 标记属性。
- **[FU-iceberg-view-ddl→A3/B6]** dropTable 不路由视图；翻闸后 DROP VIEW / 视图查询 / SHOW CREATE 视图 fail-loud。归视图 scope。
- **[FU-rewrite-output-sizing]（R6 登；R8 必触及）** 中立 driver 未线程 target-file-size + 自适应并行度。
- **[FU-flip-e2e]** 真翻闸端到端（含 branch/tag/分区演进真 commit + 自动统计/Top-N 真生效）**未跑**（CI/flip-gated）。
- 其余（where-literal-coercion / broker-write / getRowIdColumn 等）见 git log 历史。

---

# ⚙️ 操作须知（复用）

- maven：`-f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl :<art> **-am** -DfailIfNoTests=false -Dmaven.build.cache.enabled=false`（漏 `-am`→假错 `${revision}`）。**fe-core 只依赖 `fe-connector-api`**（不依赖连接器 impl）→ `:fe-core -am` 不会拖 paimon。**fe-connector-paimon 单独 build 必须 `package`**（非 `compile`/`test`）：`HiveConf` 来自 `fe-connector-paimon-hive-shade` 的 `<optional>` 依赖，只在 package 期（maven-shade）才落地——`-am compile`/`-am test` 都会因 `HiveConf` 缺失编译失败；用 `-pl :fe-connector-paimon -am package -Dtest=<X>`。**iceberg/api** 正常 `-am test` 即可。**全量前 `rm -f target/surefire-reports/TEST-*.xml`**。
- **⚠️ stale .class 假红坑**：mutation 后 `os.utime(f, None)`（脚本已做）或 `mvn clean`。**commit 前最终验证务必 fresh recompile**（comment-only 改无 bytecode 影响除外）。
- **⚠️ checkstyle**：import 同组无空行 + 组内 ASCII 序（大写在小写前：`datasource.PluginDrivenExternalTable` 在 `datasource.hive`/`datasource.iceberg` 之前）。加 import 后 `mvn checkstyle:check -pl :<mod>` 快验（连接器需 `-am`），或 mutation 跑加 `-Dcheckstyle.skip=true`。
- 连接器禁 import fe-core：`bash tools/check-connector-imports.sh`。测试：连接器**无 Mockito**（真 InMemoryCatalog / `new XConnector(emptyMap, RecordingConnectorContext)` 断言 getCapabilities）；fe-core **Mockito**（`CALLS_REAL_METHODS` + `Deencapsulation.setField(table,"catalog",mockCatalog)` + mock `connector.getCapabilities()` 测能力 helper；纯 `Mockito.mock(PluginDrivenExternalTable.class)` + stub helper 测网关）。**⚠️ 真裸 PluginDrivenExternalCatalog（无 provider）做负例会在 getConnector→makeSureInitialized 抛「No ConnectorProvider」**——网关测试用 mock 表 stub 能力（真连接器链交给 helper 测）。live-e2e CI/flip-gated，勿谎称。
- **mutation-check（Rule 9/12）**：范式 scratchpad `mutate_bclass.py`（fe-core 6 变异，test 目标）+ `mutate_connectors.py`（连接器 3 变异，iceberg=test / paimon=package 目标）。**⚠️ Python 3.6**：`subprocess.run` 用 `stdout=PIPE,stderr=STDOUT,universal_newlines=True`（无 `capture_output`）。**⚠️ exact-string 锚点须唯一**；**⚠️ 别与读源 review 并发跑**（reviewer 读到瞬时变异报假 BLOCKER——本 session 印证：harness 把瞬时变异当「intentional change」提示）。
- **cwd 会被 harness 重置**→一律绝对路径。

# ⚠️ Commit 须知（任何 `git add` 前必读）

- **path-whitelist `git add`，严禁 `git add -A`**（scrub `regression-test/conf/regression-conf.groovy` 明文 key + `*.bak` + scratch `.audit-scratch/`·`conf.cmy/`·`META-INF/`·`docker/...`·仓根游离 `fe/IcebergScanPlanProvider.java`·`plan-doc/reviews/P5-paimon-rereview3-*`）。
- **本 session commit `6f3c255cd52`** 涉及 12 文件（全 `fe/`，见上「落地文件」）。HANDOFF 单独 commit。
- **C5：B1=`b7203cf6a42` / B2a=`6afb08cefe9` / B2b=`249130ebf27` / B3=`decacb29e49` / B4=`7a421b8721d` / B5=`205d67e8e3a`；flip-readiness 自动统计+TopN=`6f3c255cd52`**。
- commit message：见 `git log` 范式 + 末尾 `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>` + `Claude-Session: …`。PR base = `branch-catalog-spi`，squash。

# 📦 阶段状态

- **工作分支 = `catalog-spi-10-iceberg`**（off `branch-catalog-spi` @ `e5959e1b53d`，PR base = `branch-catalog-spi`，squash）。
- **⚠️ 推送状态**：P6.4 T01–T06+arg-move 已推 `origin`；**其后全部（含 commit-bridge + C4 + C5 + 本 session）未 push**。**用户未要求 push**——留用户裁量。
- iceberg **不在** `SPI_READY_TYPES`（除 paimon-autoanalyze 这一 live 项外，pre-flip 零行为变更）。metastore 子线 CLOSED（勿读）。
- **⚠️ 环境**：`/mnt/disk1` 紧（2.0T，本 session 起步 ~85G free，96% used）。**下个 session 起步先 `df -h /mnt/disk1`**。

# 🧠 给下一个 agent 的 meta

- **删除/parity/动码前必 grep 全调用方 + 区分 DEAD vs STILL-CONSUMED**；**HANDOFF/设计/RFC 的依赖名/行号/不变式可能过时或错** —— 动码前先 recon（grep+实证）再信文档。**本 session 实证推翻审计 2 处**：① SHOW PARTITIONS 不是退化是误报（死码）；② 自动统计/Top-N 两条路 **paimon 现在也没接**（不是 iceberg-only 回归，是「插件 catalog 从没接进去」），故能力机制顺带也能修 paimon。
- **clean-room 对抗 review 偏好**：大改动 = 多 reader 对抗 + critic。**⚠️ review（读源）与 mutation（改源）不可并发**（本 session 印证：mutation 期 harness 把瞬时变异当「intentional change」提示——勿被误导保留变异）。mutation 结果是测试充分性的权威判据。
- **既有 Doris 行为/parity 优先**：pre-flip 一律零行为变更（本 session 唯一 live = paimon 自动统计，用户已签）。
- **下批（SHOW CREATE TABLE/DB）需新中立 SPI**（库 location；可能还有 transform/sort 预渲染）→ 先读 Trino 模型（properties-map）+ 用中文向用户表面化 2 决策（SPI 放法、B1 parity 范围）再动。
- **上下文超 30% 即交接**。本 session = flip-readiness 自动统计+Top-N（12 文件 commit `6f3c255cd52`；mutation 9/9；对抗 review SAFE_TO_COMMIT），在干净节点交接「SHOW CREATE TABLE/DB」。
