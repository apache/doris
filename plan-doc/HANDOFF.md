# 🤝 Session Handoff

> 滚动文档：每次 session 结束**覆盖式更新**（不累积历史；历史见 `git log plan-doc/HANDOFF.md`）。协作规范：[AGENT-PLAYBOOK.md](./AGENT-PLAYBOOK.md)
> **范围** = catalog-spi **主线** handoff。metastore/storage 抽取子线**已 CLOSED**（2026-06-22 合入 #64446/#64653/#64655）——`metastore-storage-refactor/` 仅历史留存、勿读；metastore-spi 现状直接读代码 `fe/fe-connector/fe-connector-metastore-spi/`。

---

# 🎯 下一个 session 的任务 = **持久化迁移（GSON registerCompatibleSubtype，让旧 iceberg catalog/库/表镜像在翻闸后重启自动升级为插件化类型）**

> **⚠️ 本 session = flip-readiness B 类「其二」✅ 完成**：翻闸后 SHOW CREATE TABLE / SHOW CREATE DATABASE 对 iceberg 退化（丢 LOCATION/分区/排序/属性、丢库 LOCATION）已修，改为**按连接器能力判别 + 中立保留键 + 中立库元数据 SPI**（commit 见下）。**flip-readiness B 类（只读退化）至此全部清完**。

> **🔑 下一步（DEC-FLIP-1 持久化 GSON 迁移）= 让旧 iceberg 持久态在翻闸后兼容**（动批前先读 memory `iceberg-flip-readiness-gaps` 的 DEC-FLIP-1 + paimon 范式 `GsonUtils:389-411`，再对真实代码 re-grep）：
> - **问题**：翻闸只改「新建 catalog 走插件化」；磁盘上已存在的旧 iceberg catalog（`IcebergExternalCatalog` 8 变体）+ 库（`IcebergExternalDatabase`）+ 表（`IcebergExternalTable`/MVCC）在重启 replay 时仍按旧 GSON 类型反序列化 → 翻闸后变「混合舰队」（旧那半仍是 iceberg-typed，单形 instanceof 对它失效），且阻塞 P6.7 删 legacy 类。
> - **方案（用户已签 = 方向一）**：`GsonUtils.registerCompatibleSubtype(PluginDrivenExternalCatalog.class, "Iceberg…")` 全 8 catalog 变体 + 库（→`PluginDrivenExternalDatabase`）+ 表（→`PluginDrivenExternalTable`/`PluginDrivenMvccExternalTable`，按连接器能力 `SUPPORTS_MVCC_SNAPSHOT` 选目标类），跟 paimon `:389-411` 范式，重启自动升级、全集群统一。
> - **实现注（动手前核实）**：① 核 paimon 实际 remap 了哪些层级（catalog/db/table 是否全做）；② 表迁移目标类（MVCC vs 非 MVCC，按能力）；③ **混合-旧-镜像兼容必须真测**（造一个旧 `IcebergExternalCatalog` 镜像 → 翻闸后加载 → 应成 `PluginDrivenExternalCatalog`）。
> - 守铁律：迁移在 GsonUtils 注册层，非 fe-core 业务 instanceof。

> **之后的翻闸顺序（不变）**：DEC-FLIP-1 持久化 GSON → **A3/B6 视图 SPI**（DROP VIEW / force-drop 含视图库 / 视图查询 / SHOW CREATE 视图，中立「视图定义」SPI）→ **C 类用户 docker e2e** → **加 `SPI_READY_TYPES`+iceberg / 删 `CatalogFactory` legacy case / capability 核 / 用户二签**（FLIP，不可逆，最后一步）。

---

# ✅ 本 session 完成 = **iceberg 翻闸后 SHOW CREATE TABLE / SHOW CREATE DATABASE 退化修复**（commit `d95819de637`）

**做了什么（中文详解，不引用代号）**：
翻闸（iceberg 改走插件化通用 catalog）后，两个只读命令对 iceberg **静默退化**——不报错但内容残缺：
1. **查看建表语句（SHOW CREATE TABLE）**：原本能输出存储路径、分区方式（含 BUCKET/DAY 等变换）、排序、属性；翻闸后只剩一行注释空壳。
2. **查看建库语句（SHOW CREATE DATABASE）**：原本能输出库的存储路径（LOCATION）；翻闸后丢失。

根因：fe-core 这两段渲染原本「先判断是不是 iceberg 专属类型」，翻闸后 iceberg 不再是那个类型就漏掉；通用插件路径此前只对 paimon 渲染、且分区/排序根本没实现。

**怎么改（用户三签：能力标记判别 / 表全量一次到位 / 库 SPI 用属性 map）**：
- **新增连接器能力 `SUPPORTS_SHOW_CREATE_DDL`**：声明它的连接器（iceberg + paimon）才渲染 LOCATION/属性/分区/排序；JDBC/ES 不声明（它们的属性含连接密码，不能打印——这正是原「仅 paimon」引擎字符串门的安全语义，现迁到能力门）。**顺手把 paimon 现有的引擎字符串门也换成能力门**（消除 fe-core 里对引擎名的字符串判断、与上一轮能力化方向一致）。
- **第二层分区/排序由连接器预渲染**：fe-core 在通用插件表上拿不到 iceberg 实时 API，所以连接器把每个分区字段（`BUCKET(8, c)`/`DAY(c)`/`TRUNCATE(4, c)`/identity 等）和排序字段（`c ASC/DESC NULLS FIRST/LAST`）预渲染成 Doris 语法子句，经**中立保留属性键**（`show.location`/`show.partition-clause`/`show.sort-clause`）传上来，fe-core 原样拼接（**与 Trino 同思路**：连接器出片段、引擎拼外壳）。
- **顺带修了一个潜在 bug + 字节保真**：连接器原先注入的 `location`/`iceberg.format-version`/`iceberg.partition-spec` 三个键**从未被 fe-core 读取**（dead）且会泄进渲染的 PROPERTIES（legacy iceberg 只 dump 原始 `table.properties()`）——本批删除它们、改发中立 show.* 键，使渲染的 PROPERTIES 与 legacy **字节一致**（format-version 仍经 `getFormatVersion` 驱动 v3 行血缘，不受影响）。
- **库 LOCATION 走新中立 SPI**：把已声明但从未启用的 `ConnectorSchemaOps.getDatabase` 默认从「抛异常」软化为「返回空属性 map」（graceful，照 `listDatabaseNames` 范式）；iceberg 连接器 override 它暴露 namespace location（属性 map 的 `location` 键，Trino 对齐）；`PluginDrivenExternalDatabase.getLocation()` 经连接器按**远程库名**取该键；`ShowCreateDatabaseCommand` 加插件化 catalog 臂渲染 LOCATION。

**pre-flip 行为（关键）**：iceberg 仍是 `IcebergExternalCatalog`/`IcebergExternalTable`（不在 `SPI_READY_TYPES`）→ 走未改动的 legacy 臂，**零变化**。**paimon SHOW CREATE TABLE/DATABASE 经 clean-room 复核 = 字节一致**（paimon 不发 show.* 键 → 无分区/排序；LOCATION 仍从 `path` 取〔getShowLocation 的 path 回退〕；PROPERTIES 仍含 coreOptions 含 path）——**本 session 无新 DV**（不像上一轮的 DV-paimon-autoanalyze）。

**📊 实证结论（给下个 agent）**：原审计/recon 误把 `toEngineName()` 当 `getEngineTableTypeName()`——**ENGINE 子句已 byte-faithful**（plugin override 与 legacy 默认都返 `"ICEBERG_EXTERNAL_TABLE"`），无需改。

**落地文件（11 改产品 + 8 测试〔5 改 3 新〕= 19 文件）**：
- SPI（fe-connector-api）：`ConnectorCapability`（+1 枚举）、`ConnectorTableSchema`（+3 保留键常量）、`ConnectorSchemaOps.getDatabase`（默认 throw→空 map）、`ConnectorDatabaseMetadata`（+`LOCATION_PROPERTY` 常量）。
- iceberg 连接器：`IcebergConnector.getCapabilities`（+1）、`IcebergConnectorMetadata`（buildTableSchema 发 show.* / 删 3 dead 键、新 `buildShowPartitionClause`/`buildShowSortClause` 移植 + `getDatabase` override）。
- paimon 连接器：`PaimonConnector.getCapabilities`（+1）。
- fe-core：`PluginDrivenExternalTable`（+`supportsShowCreateDdl` + `getShowLocation/getShowPartitionClause/getShowSortClause` + getTableProperties strip show.*）、`PluginDrivenExternalDatabase`（+`getLocation`）、`Env.getDdlStmt` plugin 臂（引擎字符串门→能力门 + 排序/分区/LOCATION/属性渲染，sys 表跳分区）、`ShowCreateDatabaseCommand`（+PluginDriven 臂）。
- 测试：`IcebergConnectorMetadataTest`（show.* + 多变换 + 排序 + getDatabase + no-leak）、`IcebergConnectorTest`（+能力）、`FakeIcebergTable`（sortOrder 可注入）、`PaimonConnectorMetadataMvccTest`（+能力）、`PluginDrivenExternalTableTest`（+能力/访问器/strip）、新 `PluginDrivenExternalDatabaseTest`、新 `EnvShowCreatePluginTableTest`、新 `ConnectorSchemaOpsDefaultsTest`。

**验证**：fe-core **20** 测试（PluginDrivenExternalTableTest 13 + PluginDrivenExternalDatabaseTest 4 + EnvShowCreatePluginTableTest 3）+ iceberg **36+8** + paimon **42** + api **1** 全绿（fresh recompile；paimon 须 `package`）；**mutation 16/16 KILLED**（含分区变换 BUCKET/TRUNCATE/DAY、排序 ASC-DESC/NULLS、能力门、sys 跳分区、getDatabase override+default、库 location、strip、path 回退，脚本 scratchpad `mutate_showcreate.py`）；**clean-room 对抗 review（6 reader + critic）= SAFE_TO_COMMIT（must-fix NONE，11 不变式全独立验证，0 BLOCKER/0 真 MAJOR 代码缺陷）**；checkstyle 4 模块 0；iron-law clean（`tools/check-connector-imports.sh` 0；新 fe-core 代码无 `instanceof Iceberg`/引擎字符串——Env 旧门已删换成能力）；**e2e flip-gated 未跑（勿谎称）**。

**P6.1–P6.5 ✅ / P6.6：C1–C3 ✅ / C4 R1–R7 ✅ / C5 DDL/ALTER B1–B5 ✅ / flip-readiness B 类（自动统计+Top-N、SHOW CREATE TABLE/DB）全 ✅** → 剩 **持久化 GSON + 视图 + C docker** → 翻闸。iceberg **仍不在** `SPI_READY_TYPES`。

## ⛳ 结论（一句话）：**还不能翻闸**。DDL/ALTER 全量对齐 + flip-readiness 所有只读退化（自动统计/Top-N/SHOW CREATE TABLE/DB）已干净修完；剩 **持久化迁移、视图 SPI、C docker e2e**。

## ✅ 用户决策（仍有效）
- **[全量对齐]**：DDL/ALTER 18 op + B 类 + 视图 全部翻闸前修；仅 C 类（写路径正确性）归用户 docker，D 类翻闸后。
- **[DEC-FLIP-1] = 方向一（GSON 迁移）**：`registerCompatibleSubtype(PluginDrivenExternalCatalog.class, "Iceberg…")` 全 8 catalog 变体 + 库 + 表（跟 paimon 范式），重启自动升级。
- **[本 session 签]**：判别机制 = 能力标记（`SUPPORTS_SHOW_CREATE_DDL`，含把 paimon 引擎字符串门一并换能力门）；表渲染 = 全量一次到位（LOCATION+属性 + 分区/排序连接器预渲染）；库元数据 SPI = 属性 map（复用 `ConnectorDatabaseMetadata`，Trino 对齐）。

## 📖 起步必读（动下一批前）
1. memory `iceberg-flip-readiness-gaps`（缺口全景 + DEC-FLIP-1 细节）、`iceberg-showcreate-flip-done`（本 session）、`iceberg-bclass-autoanalyze-topn-done`（上一轮）、`handoff-discipline-per-phase`、`consult-trino-before-spi-design`、`clean-room-adversarial-review-pref`、`ask-user-explain-in-chinese-first`、`doris-build-verify-gotchas`。
2. `plan-doc/tasks/designs/P6.6-C5-show-create.md`（本 session 设计）、`P6.6-C5-flip-readiness.md`（B 节，注意其 SHOW CREATE 描述已被本 session 实现细化）。
3. **下批锚点（动码前 re-grep）**：`GsonUtils`（paimon `registerCompatibleSubtype` 范式 `:389-411`）；iceberg 8 catalog 变体类（`IcebergExternalCatalog`/`IcebergHMSExternalCatalog`/`IcebergGlueExternalCatalog`/`IcebergRestExternalCatalog`/`IcebergDLFExternalCatalog`/`IcebergHadoopExternalCatalog`/`IcebergJdbcExternalCatalog`/`IcebergS3TablesExternalCatalog`）+ `IcebergExternalDatabase` + `IcebergExternalTable`。

---

# ⚠️⚠️ 用户铁律：**fe-core 不得新增 `if(iceberg)` / `instanceof Iceberg*` / `import IcebergUtils` / 引擎名字符串判别（新 seam）**
iceberg 逻辑落 `fe-connector` 经中立 SPI。**legacy 豁免类**保留 iceberg 引用合法（C4 dead 子树 + commit-bridge 旧清单 + `PhysicalIcebergTableSink`/`bindIcebergTableSink` + `StatementContext` 旧 iceberg-typed stash + `IcebergExternalCatalog`〔分区演进/ALTER〕+ `ShowCreateDatabaseCommand` 的 legacy iceberg 臂〔pre-flip iceberg 仍是该类，服务那一半〕+ `Env.getDdlStmt` legacy ICEBERG_EXTERNAL_TABLE 臂）。**本 session 守则**：Env plugin 臂的引擎字符串门已**删除**换成 `supportsShowCreateDdl()` 能力门；新增的能力 helper / show.* 访问器 / 库 getLocation / ShowCreateDatabaseCommand plugin 臂 / 连接器预渲染全经中立能力 + 中立保留键 + 中立 SPI，无 instanceof Iceberg（已核 + clean-room 验）。

---

# 🟡 已登记 follow-up（非阻塞）
- **[FU-showcreatedb-render-ut]（本 session 登）** `ShowCreateDatabaseCommand` 的 plugin 臂渲染（LOCATION 拼接/PROPERTIES 块）无直接 UT（重，需 Env/CatalogMgr/权限 stub）；`PluginDrivenExternalDatabase.getLocation()` 已 UT，渲染臂字节镜像 generic-else。flip-gated e2e 覆盖。clean-room 评为 optional。
- **[FU-createtablelike-plugin]（本 session 登）** `getCreateTableLikeStmt` plugin 臂（`:4517` comment-only）仍退化（paimon 已退化、非 iceberg 引入）。CREATE TABLE LIKE 是另一命令，补它会引入 paimon live 变更（需另签）+ LIKE 复制 LOCATION 本身存疑 → **未在本批**，留单独决策。
- **[FU-showcreate-flip-e2e]（本 session 登）** 翻闸后 SHOW CREATE TABLE/DB 真 e2e `.out`：锁定（有意非 legacy 字节一致的）翻闸后 DB 渲染 + 防 PROPERTIES 键顺序漂移（连接器 `HashMap`）。flip-gated。
- **[FU-jdbc-es-negcap]（本 session 登，低）** jdbc/es 连接器无「不声明 `SUPPORTS_SHOW_CREATE_DDL`」负断言（凭力 getCapabilities 默认空集守；且 Env「不支持→仅注释」测试已行为性覆盖该守卫）。
- **[FU-show-partitions-deadcode]** `ShowPartitionsCommand` `instanceof IcebergExternalCatalog` 3 列臂 = 死码；翻闸后可选连接器侧补 5 列。非退化、非阻塞。
- **[FU-nested-nullability]（B2b ✅ SPI/builder 层闭）** Doris `ArrayType.getContainsNull()` 硬编码 true。
- **[FU-doris-version-prop]** 连接器 createTable 丢 `doris.version` 标记属性。
- **[FU-iceberg-view-ddl→A3/B6]** dropTable 不路由视图；翻闸后 DROP VIEW / 视图查询 / SHOW CREATE 视图 fail-loud。归视图 scope。
- **[FU-rewrite-output-sizing]（R6 登；R8 必触及）** 中立 driver 未线程 target-file-size + 自适应并行度。
- **[FU-flip-e2e]** 真翻闸端到端**未跑**（CI/flip-gated）。
- 其余（where-literal-coercion / broker-write / getRowIdColumn 等）见 git log 历史。

---

# ⚙️ 操作须知（复用）

- maven：`-f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl :<art> **-am** -DfailIfNoTests=false -Dmaven.build.cache.enabled=false`（漏 `-am`→假错 `${revision}`）。**fe-core 只依赖 `fe-connector-api`** → `:fe-core -am` 不拖 paimon。**fe-connector-paimon 单独 build 必须 `package`**（`HiveConf` 来自 `<optional>` shade，只 package 期落地；`-am compile`/`-am test` 都因缺 HiveConf 编译失败）：`-pl :fe-connector-paimon -am package -Dtest=<X>`。**iceberg/api** 正常 `-am test`。
- **⚠️ bash 工具默认 timeout 120s**：fe-core build 超时 → 调 `timeout` 参数到 ~600000ms，或后台跑。
- **⚠️ surefire 报告是 XML**（`TEST-<class>.xml`，不是 `.txt`）；`-q` 抑制 console 摘要，读 XML 的 `tests=`/`failures=` 确认真跑（非 0 用例）。
- **⚠️ stale .class 假红坑**：mutation 后 `os.utime(f, None)`（脚本已做）或 `mvn clean`。**commit 前最终验证务必 fresh recompile**。
- **⚠️ checkstyle**：import 同组无空行 + 组内 ASCII 序（大写在小写前）。`mvn checkstyle:check -pl :<mods> -am`。
- 连接器禁 import fe-core：`bash tools/check-connector-imports.sh`。**连接器测试无 Mockito**（真 InMemoryCatalog / `new XConnector(emptyMap, RecordingConnectorContext)`）；**fe-core 用 Mockito**（`CALLS_REAL_METHODS` + `Deencapsulation.setField(table,"catalog",mockCatalog)` + stub `getSchemaCacheValue`/`makeSureInitialized`/`getConnector`/`buildConnectorSession`）。**Env.getDdlStmt 可 UT**：mock PluginDrivenExternalTable(CALLS_REAL_METHODS) + stub getType/getName/getComment/getBaseSchema(false)/isManagedTable/isTemporary/getEngineTableTypeName + 渲染访问器（见 `EnvShowCreatePluginTableTest`）。
- **mutation-check（Rule 9/12）**：范式 scratchpad `mutate_showcreate.py`（16 变异跨 4 模块；exact-string 锚点须唯一；KILLED=build/test FAIL）。**⚠️ Python 3.6**：`subprocess.run(stdout=PIPE,stderr=STDOUT,universal_newlines=True)`（无 `capture_output`）。**⚠️ 别与读源 review 并发跑**（reviewer 读到瞬时变异报假 BLOCKER）→ review（读源）与 mutation（改源）务必**串行**。
- **cwd 会被 harness 重置**→一律绝对路径。

# ⚠️ Commit 须知（任何 `git add` 前必读）

- **path-whitelist `git add`，严禁 `git add -A`**（scrub `regression-test/conf/regression-conf.groovy` 明文 key + `*.bak` + scratch `.audit-scratch/`·`conf.cmy/`·`META-INF/`·`docker/...`·仓根游离 `fe/IcebergScanPlanProvider.java`·`plan-doc/reviews/P5-paimon-rereview3-*`)。
- **本 session 涉及 19 文件**（全 `fe/`，11 产品 + 8 测试，见上「落地文件」）+ 设计文档 `plan-doc/tasks/designs/P6.6-C5-show-create.md`。HANDOFF + 设计文档单独 commit（memory 在 `.claude/`、非仓内）。
- **C5：B1=`b7203cf6a42` / B2a=`6afb08cefe9` / B2b=`249130ebf27` / B3=`decacb29e49` / B4=`7a421b8721d` / B5=`205d67e8e3a`；flip-readiness 自动统计+TopN=`6f3c255cd52`；SHOW CREATE TABLE/DB=`d95819de637`**。
- commit message：见 `git log` 范式 + 末尾 `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>` + `Claude-Session: …`。PR base = `branch-catalog-spi`，squash。

# 📦 阶段状态

- **工作分支 = `catalog-spi-10-iceberg`**（off `branch-catalog-spi` @ `e5959e1b53d`，PR base = `branch-catalog-spi`，squash）。
- **⚠️ 推送状态**：P6.4 T01–T06+arg-move 已推 `origin`；**其后全部（含 commit-bridge + C4 + C5 + flip-readiness 两轮）未 push**。**用户未要求 push**——留用户裁量。
- iceberg **不在** `SPI_READY_TYPES`（pre-flip 零行为变更；paimon SHOW CREATE 字节一致，本 session 无新 DV）。metastore 子线 CLOSED（勿读）。
- **⚠️ 环境**：`/mnt/disk1` 紧（2.0T，本 session 起步 ~85G free，96% used）。**下个 session 起步先 `df -h /mnt/disk1`**。

# 🧠 给下一个 agent 的 meta

- **删除/parity/动码前必 grep 全调用方 + 区分 DEAD vs STILL-CONSUMED**；**HANDOFF/设计/RFC 的依赖名/行号/不变式可能过时或错** —— 动码前先 recon（grep+实证）再信文档。**本 session 实证纠正 1 处**：ENGINE 子句用 `getEngineTableTypeName()`（=enum name），非 `toEngineName()`（recon 误判）→ ENGINE 已 byte-faithful 无需改。
- **clean-room 对抗 review 偏好**：大改动 = 多 reader 对抗 + critic。**⚠️ review（读源）与 mutation（改源）不可并发**（串行：先 review 干净源、再 mutation 改源验充分性）。mutation 结果是测试充分性的权威判据。
- **既有 Doris 行为/parity 优先**：pre-flip 一律零行为变更（本 session paimon SHOW CREATE 字节一致，无 DV）。
- **下批（持久化 GSON）需先读 paimon `GsonUtils:389-411` 范式 + 造旧镜像真测升级**；属注册层改动，非 fe-core 业务 instanceof。
- **上下文超 30% 即交接**。本 session = SHOW CREATE TABLE/DB（16 文件 commit `d95819de637`；mutation 16/16；对抗 review SAFE_TO_COMMIT），在干净节点交接「持久化 GSON 迁移」。
