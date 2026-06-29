# 🤝 Session Handoff

> 滚动文档：每次 session 结束**覆盖式更新**（不累积历史；历史见 `git log plan-doc/HANDOFF.md`）。协作规范：[AGENT-PLAYBOOK.md](./AGENT-PLAYBOOK.md)
> **范围** = catalog-spi **主线** handoff。metastore/storage 抽取子线**已 CLOSED**（2026-06-22 合入 #64446/#64653/#64655）——`metastore-storage-refactor/` 仅历史留存、勿读；metastore-spi 现状直接读代码 `fe/fe-connector/fe-connector-metastore-spi/`。

---

# 🎯 下一个 session 的任务 = **逐步处理 clean-room 对抗 review 发现（翻闸 BLOCKED，先修后翻）**

> **✅ 本 session 已完成（2026-06-29）= H-9**（前序同日 session：H-2 + H-3 + H-4 + H-7 + H-8）。
> **H-9 修完成 `89658999f49`**——翻闸后 iceberg `rewrite_data_files` 的 WHERE 跨列 OR（`a=1 OR b=2`）、`NOT(比较)`（`NOT(a>5)`）、`!=` 被误报 "cannot be pushed down to file pruning"；常见形（=/>/IN/IS NULL/BETWEEN/同列 OR/顶层 AND）正常。根因：rewrite 的 WHERE 复用了 `IcebergPredicateConverter` 的 **conflict 模式**（写时冲突检测窄矩阵：`buildConflictOr` 仅同列、`buildConflictNot` 仅 `NOT(IS NULL)`、丢 NE），被 `RewriteDataFilePlanner` 的 fail-loud 守护（`size < 顶层合取数`）转成报错；权威参照 master `IcebergNereidsUtils.convertNereidsToIcebergExpression` 无这些收窄（Or/Not 推任意可转子节点、全 all-or-nothing）。**⚠️ Rule 7 冲突（已 surface 并复核裁定）**：DV-046 的 R7（2026-06-27 用户签「无法精确就报错」）有意让此路 fail-loud 并接受跨列 OR/NOT 报错，本轮 clean-room 复核不注入先验、重标记为回归；**2026-06-29 当面用中文背景+示例复核，用户裁定「修对：精确下推否则报错」**（报错是借 conflict 矩阵的实现副作用、非真无法精确——这些形本可精确下推；从不变宽的 R7 守护保留）。**修（参 Trino——扫描与表维护用同一套谓词下推、不能完全下推即报错）= 新增连接器内第三 `Mode { SCAN, CONFLICT, REWRITE }`**：REWRITE 镜像 master 矩阵（宽：跨列 OR/任意子 NOT/NE/IN + 节点直发 IS NULL/BETWEEN；严格 all-or-nothing）。**⚠️ 非当初设想的「改用 scan-mode」**（scan 无 IS NULL/BETWEEN 节点 case + `buildAnd` 退化会 silent-widen）。all-or-nothing 在 **两层**强制：`buildAnd`（显式 AND）+ `checkConversion` 的 AND 退化分支——**后者是 clean-room parity reader 抓到的 silent-widen BLOCKER 的修复**（`c_date BETWEEN '2020-01-01' AND '2020-12-1'`：畸形上界仅在 bind 时才失败，共享退化会留单 `gte` → 悄悄扩大压缩范围；已修 + 复验 SAFE）。0 fe-core/SPI/BE/引擎名（铁律干净）；保留 boolean 构造器 → scan/conflict 调用方零改。验证：iceberg 连接器全量 **840/0（1skip）** / checkstyle 0 / import-gate 干净 / **mutation 7/7 KILLED** / clean-room 2 reader（parity-vs-master、iron-rule+regression）均 **SAFE_TO_COMMIT**（唯 1 LOW=`col=NULL` 现 fail-loud vs master `isNull`，errs-safe、在「精确否则报错」契约内）。**e2e（rewrite WHERE 各形 file-set parity）flip-gated 未跑**。设计/小结 `designs/P6.6-FIX-H9-rewrite-where-exact-or-error-{design,summary}.md`。
> **H-8 修完成 `5bf7c90a710`**——翻闸后 iceberg ViewCatalog（REST/HMS-with-views）下视图的列元数据全空（`DESC`/`SHOW COLUMNS`/`information_schema.columns`/JDBC 元数据/BI 列内省），`SELECT * FROM <view>` 仍正常（`BindRelation` 展开视图体）。根因：`PluginDrivenExternalTable.initSchema` 无 isView 分支，无条件经 `getTableHandle` 解析句柄，而 iceberg `getTableHandle` 用 `tableExists` 对视图返 false → handle 空 → schema 缓存恒空；master 对视图单独读 `icebergView.schema()`。**修（用户裁定方案一，对齐 Trino——视图定义天然带列；当面用中文背景+示例确认）**=① SPI 中立 `ConnectorViewDefinition` 加 `List<ConnectorColumn> columns`（3-arg ctor+防御拷贝+equals/hashCode/toString 纳入，删旧 2-arg）；② seam `IcebergCatalogOps.loadViewDefinition→loadView`（返 SDK `View`，镜像 `loadTable`）；③ metadata `IcebergConnectorMetadata.getViewDefinition` 一次 `loadView` 内抽 sql/dialect（移自 seam，4 处 null 守护+文案逐字保留）+`parseSchema(view.schema())` 产列，全包 `executeAuthenticated`（**mapping flag 仅在 metadata 层** → 列转换在此，镜像表路径分层）；④ fe-core `initSchema` 加 `if (isView())` 分支经 `getViewDefinition().getColumns()`+既有 `toSchemaCacheValue` 建 schema（空 props→无分区列=master `loadViewSchemaCacheValue`），**门控于既有 `isView()`(SUPPORTS_VIEW capability)、非 instanceof/引擎名（铁律干净）**。已核 MVCC 子类不覆写 `initSchema` → 翻闸视图（Mvcc 实例）schema 加载经基类视图分支（cache/no-cache 两路都到）。**已知 LOW 背离（设计登记）**：视图 initSchema 现经 getViewDefinition 顺带要求 engine-name/sqlFor，仅"有列但缺 engine-name"的畸形视图（master 也不可 SELECT）DESC/批量枚举（`information_schema.columns`/SHOW/MTMV related-table）会报错；生产视图恒有 engine-name，影响可忽略。验证：api **53/0** + iceberg **826/0/1** + fe-core `PluginDrivenExternalTableTest` **22/0** 全绿 / checkstyle 0 / import-gate 干净 / **mutation 3/3 KILLED**（删 isView 分支 / 连接器丢列 / SPI equals 漏列）/ clean-room 2 reader（parity+iron-rule、正确性+回归）均 **SAFE_TO_COMMIT**（唯 1 LOW=上述畸形视图）。0 BE/thrift/pom/引擎名判别。**e2e（翻闸后视图列非空）flip-gated 未跑**。设计/小结 `designs/P6.6-FIX-H8-view-schema-init-{design,summary}.md`。
> **⚠️ 给 H-10 的旁注（嵌套列裁剪）**：仓内有用户已确认的设计 `designs/connector-capability-unification-design.md`（统一 Connector 能力声明，Trino「seam 即声明」）。**该设计与翻闸解耦、应在翻闸稳定后作为独立任务落地**；其与 H-10「嵌套列裁剪」修复（计划新增 nested-prune `ConnectorCapability`）有交集——认领 H-10 前宜先对齐该设计，避免能力枚举重复/冲突。
> **H-7 修完成 `a8f1ae9ca2e`**——翻闸后 iceberg `SELECT ... FOR VERSION AS OF '<branch>'`（分支名、无同名 tag）被拒报 "can't find snapshot by tag"；master 接受任意 ref（branch∪tag）。根因：fe-core `PluginDrivenMvccExternalTable.toTimeTravelSpec` 把非数字 `FOR VERSION AS OF` **无条件分派 `Kind.TAG`**（为 paimon「非数字 VERSION=tag-only」语义写、泄漏进通用层），而 `@tag('name')` **也**分派 `Kind.TAG` → 连接器收到无法区分二者 → 只能按 tag-only 解析（`resolveRef` 要求 `ref.isTag()`）拒 branch ref。legacy 三契约：`@tag`=tag-only / `@branch`=branch-only / 非数字 `FOR VERSION AS OF`=任意 ref（`IcebergUtils.getQuerySpecSnapshot` 用 `table.refs().containsKey`）。**修（参 Trino「引擎传中立版本指针、连接器自行解释」模型）**=新增中立 `ConnectorTimeTravelSpec.Kind.VERSION_REF`（+`versionRef()` 工厂，纯加性）；fe-core 非数字 VERSION→`versionRef`（源无关，不预判 tag-vs-branch）；iceberg `case VERSION_REF`→`resolveRef(...,ref->true)` 任意 ref（=legacy `refs().containsKey`；`resolveRef` 第三参 boolean→`Predicate<SnapshotRef>`；VERSION_REF→branch 产 snapshot 与既有 BRANCH 臂同形、下游零新增）；paimon `case VERSION_REF` 空 fall-through 到 `case TAG`（tag-only=parity、行为字节不变）。**notFoundMessage = Option B（清复核纠错）**：初稿把文案改 "can't find snapshot by tag or branch" 被 **2/3 清复核 agent 独立抓到**破 `paimon_time_travel.groovy:344`（我 recon 用 `head` 截断漏看该 `for version as of '<不存在>'` 负向断言，Rule 12 已落档）→ 回退保 "can't find snapshot by tag"（"...or branch" 对 paimon 是假陈述——paimon `FOR VERSION AS OF` 从不查 branch；"no such tag" 永不为假；字节一致 + groovy **零改动**仍绿；iceberg 更精确 "tag or branch" 文案=既有 cosmetic 缺口，非本功能修复范围）。0 SPI 破坏性改 / 0 fe-core 引擎判别 / 0 BE。验证：iceberg **825/0/1**、paimon **321/0/1**、api **51/0**、fe-core MVCC **46/0**（+scan/fake 31/0）全绿 / checkstyle 0 / **mutation 5/5 KILLED** / import-gate 0 / clean-room 3 agent（iceberg-parity **SAFE**、iron-rule+SPI **SAFE**、paimon+completeness 抓 1 真缺陷=已 Option B 解；Reviewer 提示 **DV-038**〔pinned-branch schema 的 field-id 字典〕=既有翻闸 blocker、对所有 time-travel〔snapshot-id/tag/branch〕等同、**非 H-7 新引入**）。**e2e（branch SELECT FOR VERSION AS OF）flip-gated 未跑**。设计/落档 `designs/P6.6-FIX-H7-for-version-branch-or-tag-{design,summary}.md`。
> **H-4 修完成 `7b26fdbac88`**——翻闸后 iceberg 外表行数恒 -1 致 CBO 退化。通用 `PluginDrivenExternalTable.fetchRowCount`（`:661-678`）经 `metadata.getTableStatistics` 取统计，但 `IcebergConnectorMetadata` 未覆写 → 落 `ConnectorStatisticsOps` 默认 `Optional.empty()` → 行数恒 -1（`StatsCalculator` 基数塌 1 + 整链 join 失 CBO 重排序 + `SHOW TABLE STATUS`=-1；仅 Paimon/Jdbc 连接器有覆写）。**修**=`IcebergConnectorMetadata` 覆写 `getTableStatistics`，从 `currentSnapshot().summary()` 算 `total-records - total-position-deletes`（legacy `IcebergUtils.getIcebergRowCount` 公式；镜像 paimon **结构**、用 iceberg **公式**）。3 关键 parity 决策：① 系统表→`empty`（legacy `IcebergSysExternalTable.fetchRowCount` 恒 UNKNOWN，**刻意背离 paimon**——sys 表的"行"非数据行 + 会误 load base 表）；② `rowCount>0` 才报否则 `empty`（钉死 0→UNKNOWN，对齐新消费方 `getRowCount()>=0` 才取值的契约 + paimon 同款）；③ 刻意**不**复用 `IcebergScanPlanProvider.getCountFromSnapshot`（COUNT(*) 下推用，带 equality-delete 门 + 会话变量，轴不同——复用会让有 equality-delete 的表统计错误退化）。纯连接器内多态覆写，0 新 SPI/fe-core/BE（铁律干净）。验证：iceberg 连接器 **822 全绿**（+7 新测，真 InMemoryCatalog + 真 delete 文件；1 既有 skip）/ checkstyle 0 / **mutation 4/4 KILLED**（>0 门 / sys 守护 / 减法 / catch 降级）/ clean-room 对抗 8 向量**全 REFUTED**（唯 1 LOW=瞬时 load 失败缓存 -1，与 paimon sibling 一致、correctness 无影响，不修）。**e2e（SHOW TABLE STATUS / join CBO 重排序恢复）flip-gated 未跑**。设计 `designs/P6.6-FIX-H4-rowcount-table-statistics-design.md`。
> **H-3 修完成 `7850c6ad03f`**——翻闸后 Kerberized HDFS hadoop(filesystem) 类型 iceberg 目录丢执行认证器。fe-core 元存储属性层经 `PluginDrivenExternalCatalog.initPreExecutionAuthenticator`（`:153` 调 `msp.initExecutionAuthenticator` → `:154` 读 `getExecutionAuthenticator` → `:174` 注入连接器 context）接认证器；基类 `initExecutionAuthenticator` 默认 no-op，需派生覆写。`IcebergFileSystemMetaStoreProperties` 在私有 `buildExecutionAuthenticator` 里建 Kerberos `HadoopExecutionAuthenticator`，**但只在 legacy `initCatalog`（翻闸死码）内调** → live 钩子未覆写 → 基类 no-op → `getExecutionAuthenticator` 返默认 no-op → 连接器 context 无 UGI `doAs`（paimon 已修同 bug M-8）。**范围核对（Rule 7）：review「各 flavor 均未覆写」回代码+`git show master:` 收窄=仅 filesystem 真翻闸回归**——jdbc：master `IcebergJdbcMetaStoreProperties` 本就无 ExecutionAuthenticator/Kerberos 逻辑 → 非翻闸回归（pre-existing），出范围；hms 在 `initNormalizeAndCheckProps` 接（live）；glue/dlf/s3tables/rest 云无 HDFS UGI。**修**=`IcebergFileSystemMetaStoreProperties` 覆写 `initExecutionAuthenticator` 委派既有 `buildExecutionAuthenticator`（保留 `isKerberos()` 守护=legacy parity，非 Kerberos 留基类 no-op）；不动死码 `initCatalog`。无字段遮蔽（`AbstractIcebergProperties` 的 `@Getter executionAuthenticator` 覆写基类 `NOOP_AUTH` getter，已核）。验证：`IcebergFileSystemMetaStorePropertiesTest` 4 全绿（+2 新测）/ checkstyle 0 / **mutation 2-2 KILLED**（neuter override + drop isKerberos 守护）/ 纯加性 0 SPI/连接器/BE 改；铁律=iceberg 专属类多态覆写（非新 instanceof/引擎名判别），与 paimon 同款。**e2e（Kerberized HDFS hadoop SELECT/INSERT）flip-gated 未跑**。设计 `designs/P6.6-FIX-H3-filesystem-kerberos-authenticator-design.md`。
> **H-2 修完成 `b0c34ec8fe7`**——翻闸后 REST 三级命名空间（`external_catalog.name`）在 scan/write/procedure 静默丢失。REST iceberg 目录配 `external_catalog.name=<cat>` 时库名多套一层（库 `mydb` 的表 `t` 实位 iceberg 命名空间 `[mydb, cat]` 而非 `[mydb]`）。legacy 用**单一** `IcebergMetadataOps` 携 `externalCatalogName`，metadata/scan/write/procedure 全经 `getNamespace(externalCatalogName,dbName)` 故各路径都对；SPI 拆四入口后**只有** `getMetadata` 用 5-arg ctor 线程它，三 provider getter 用 1-arg（externalCatalogName=empty）→ 三级 REST 目录 SELECT/INSERT/EXECUTE 落错命名空间 `[mydb]`→表不存在（三 provider 解析目标表**全部且仅**经 `catalogOps.loadTable`→`toTableIdentifier`→`toNamespace`，而 `toNamespace` 只读 externalCatalogName）。**修**=抽私有 `newCatalogBackedOps()` 集中计算四门控值（与 getMetadata 旧内联块逐字相同）返 5-arg ops，**四处**（getMetadata + 三 provider getter）共用——复刻 master 单 ops 设计、消除当初漂移源；listing-only 三标志（restFlavor/nestedNamespace/viewEnabled）在 loadTable 路径 inert（toNamespace 不读它们），仅为 legacy 平价一并线程；getMetadata 的 ops 字节不变；修正三处过时注释（原称「1-arg 足够」「Inert pre-cutover」——翻闸已生效，信控制流不信注释）。clean-room 对抗复核 5 探针**全 REFUTED**（getMetadata 非回归/loadTable 唯一消费/toNamespace 仅读 externalCatalogName/非 REST 与缺省匹配 master/无遗漏 1-arg 生产调用点）。验证：iceberg 连接器全量 **815 全绿**（+4 新测，1 既有 skip）/ checkstyle 0 / **mutation 3-3 KILLED + 1 反向守护**（两级命名空间不误追加层）/ 铁律干净（纯连接器内部，0 SPI/fe-core/BE）。**e2e（3 级 REST SELECT/INSERT/EXECUTE）flip-gated 未跑**。**未 push**（沿用铁律）。前序：H-5+H-6 `d6758ff71f5`、B-1+H-1 `203cda3e31a`、B-2 `d7482a39ab9`+`b09d364888b`+`ba80cfb0439`。设计 `designs/P6.6-FIX-H2-rest-3level-namespace-providers-design.md`。
> **✅ DONE = `H-2`（REST 3 级 namespace）`b0c34ec8fe7` + `H-3`（filesystem Kerberos 认证器）`7850c6ad03f` + `H-4`（表级行数统计）`7b26fdbac88` + `H-7`（`FOR VERSION AS OF` branch∪tag）`a8f1ae9ca2e` + `H-8`（视图 schema）`5bf7c90a710` + `H-9`（rewrite WHERE 精确下推否则报错）`89658999f49`**。
>
> **⏭ 下一步（新 session 从这里起）= 处理顺序第 4 项剩余：`H-10`（最后一个 high，可独立认领）**：
> - **H-10**（嵌套列裁剪静默关闭，性能-only 回归故 high 封顶）：翻闸后表=`PluginDrivenMvccExternalTable` → `LogicalFileScan.supportPruneNestedColumn` 命中 `instanceof PluginDrivenExternalTable → false` 短路、legacy iceberg 臂死码、无能力可重开 → 触 struct/list/map 子字段查询读全列（读放大）。**修**=新增 nested-prune `ConnectorCapability`，iceberg 声明之（铁律：fe-core 走能力门控、非 instanceof/引擎名）。**⚠️ 这是「能力孪生臂逐点覆盖」脆弱性的已实证失败样本（ENG-1）**；认领前先对齐 `designs/connector-capability-unification-design.md`（见上旁注）。
> - **起步**：先 `/step-by-step-fix` → recon（grep + `git show master:` 实证，**HANDOFF/review 行号/不变式可能过时，信控制流不信注释**）→ 设计文档 `designs/P6.6-FIX-H10-<slug>-design.md` → impl → test+mutation → clean-room review → 独立 commit → 回填任务清单 ☑。
> - 处理顺序（任务清单 §8）：B-1+H-1 ✅ → B-2 ✅ → H-5+H-6 ✅ → H-2 ✅ → H-3 ✅ → H-4 ✅ → H-7 ✅ → H-8 ✅ → H-9 ✅ → **H-10 ⏭** → ENG-1 能力孪生审计 → P2(M-*) → P3(L-BATCH) → ENG-3 flip-gated e2e 全跑 → 用户二签翻闸。

> **⚠️ 状态翻转（2026-06-28）**：上一版 HANDOFF 说"翻闸代码基本完成，仅差 docker 验证 + 二签"。**这个结论已被一轮 clean-room 对抗 review 推翻**——review 发现 **2 blocker + 11 high + 11 medium + 25 low + 18 info**，其中 blocker/high 密集覆盖写入、MTMV、统计、time-travel、缓存一致性等核心路径。**翻闸代码侧确实写完了，但不正确——必须先关 P0+关键 P1 + 跑 flip-gated e2e，才能二签翻闸。**

> **📋 任务跟踪入口（下个 session 必先读）**：
> 1. **`plan-doc/tasks/P6.6-iceberg-flip-blockers-tasklist.md`** ← **master checkbox 任务清单**，逐条 ID 对齐 review 报告（B-1/B-2/H-1..H-10/M-1..M-11/L-BATCH/ENG-1..4）。**每条任务的状态、位置、修法、验收、依赖、⚠️RECONCILE 标记都在这里。逐步处理 = 按此表逐条 ☐→◐→☑。**
> 2. **`plan-doc/reviews/P6.6-iceberg-cleanroom-adversarial-review-2026-06-28.md`** ← 完整证据源（每条发现的 file:line、vs master 差异、真回归 vs 内生缺陷、验证者保留意见）。
> 3. memory `iceberg-cleanroom-adversarial-review-2026-06-28`（结论速览 + 与历史结论的冲突清单）。

---

# 🔑 翻闸现状 = **代码侧写完但 review 判定不正确；翻闸 BLOCKED**

- **路由翻闸已在分支**（`18e1b297d7e`）：`SPI_READY_TYPES` 含 `"iceberg"`，建/重放 iceberg catalog 走 `PluginDrivenExternalCatalog`；连接器 ServiceLoader 注册 + plugin-zip 打包齐备。**⚠️ 这意味着 review 所有"this path is live"成立，in-code 的 "dormant / not yet in SPI_READY_TYPES" 注释普遍已过时（false claims）——动码时勿信注释，信控制流。**
- **GSON 兼容迁移已在分支**（`e68eb5c00c9`）：旧 8 catalog 变体 + db + table 标签 `registerCompatibleSubtype`→PluginDriven（table→Mvcc 变体）+ 删 CatalogFactory legacy case。保升级老集群（全新/docker 零影响）。**review §六确认完整且写安全（正面）。**
- **未 push、未二签**：路由翻闸 + GSON 迁移**必须一起 push**（[DEC-FLIP-1] 铁律），但**当前不应 push**——先修 review 发现。

## ⛔ 翻闸 gate（全绿才能二签翻闸最后原子提交）
1. **P0 全清**：B-1（云存储写 fs.s3a.* vs AWS_*）+ B-2（MTMV listPartitions 缺）。
2. **关键 P1 关**：至少 H-1/H-2/H-3/H-5+H-6/H-8（破坏主力部署的回归）；H-4/H-7/H-9 ✅，H-10 强烈建议。
3. **ENG-1**：legacy iceberg instanceof 臂的能力孪生全量审计（H-10 是已实证漏写样本）。
4. **ENG-3**：flip-gated e2e 全套实跑（DV/V3/MTMV/time-travel branch/vended 写/Kerberized HDFS/rewrite）。
5. **用户二签**。

---

# ⚖️ 关键决策（沿用，用户已签）

## [DEC-FLIP-1] 持久化 GSON 迁移 = 方向 A（已落地 `e68eb5c00c9`）
> **⚠️ 推送顺序铁律不变**：路由翻闸（`18e1b297d7e`）与 GSON 迁移（`e68eb5c00c9`）**必须一起 push/上线**。单 push 路由翻闸而漏 GSON 迁移到会被升级的老集群 → 老 iceberg 镜像反序列化崩。**但当前两者都不应 push——先修 review 发现，翻闸做成最后一个原子提交（路由+GSON 已在前序 commit，最后补齐 fix + e2e + 二签）。**

## [视图范围] = parity only（B0/B1/B2/B3 全 DONE）
查询 B1 / DROP+删库级联 B2 / SHOW CREATE B3 / 中立地基 B0 全完。CREATE/RENAME VIEW 出范围（fail-loud）。
> ⚠️ review 发现视图面仍有缺口：**H-8（翻闸后视图无 schema，high）** + L-17/L-18/L-19/L-20（文案/缓存，low）。B0–B3 是写出来了，但 H-8 是翻闸后才暴露的 schema-init 回归——见任务清单 H-8。

## [REVIEW 纪律] clean-room，不注入先验（本轮已执行）
本轮 review 刻意不注入开发先验（忽略 plan-doc/注释/commit message）。**后果：部分发现与历史记忆冲突**（最突出=M-10 SHOW PARTITIONS：本轮判真回归 vs 记忆 `iceberg-bclass-autoanalyze-topn-done` 判"误报死码翻闸反改善"）。**认领冲突项时回代码 + `git show master:` 重裁，不盲信任一方（Rule 7）。**

---

# ⚠️⚠️ 用户铁律：**fe-core 不得新增 `if(iceberg)` / `instanceof Iceberg*` / `import IcebergUtils` / 引擎名字符串判别（新 seam）**
iceberg 逻辑落 `fe-connector` 经中立 SPI / ConnectorCapability。**legacy 豁免类**保留 iceberg 引用合法（C4 dead 子树 + commit-bridge 旧清单 + `PhysicalIcebergTableSink`/`bindIcebergTableSink` + `StatementContext` 旧 iceberg-typed stash + `IcebergExternalCatalog` + `ShowCreateDatabaseCommand`/`Env.getDdlStmt` legacy iceberg 臂 + `BindRelation case ICEBERG_EXTERNAL_TABLE` + `ShowCreateTableCommand` legacy ICEBERG 视图臂 + `InsertUtils` 既有 `UnboundIcebergTableSink` 分支）。
> **修 review 发现时尤其注意**：H-7/H-10 等要新增能力门控（`ConnectorCapability`）而非 instanceof；H-5/H-6 的 route resolver PluginDriven 臂走能力/插件检查而非引擎名。

---

# 🟡 已登记 follow-up（部分已并入任务清单）
- **[FU-forcedrop-nosuchns]** = 任务清单 **M-11**（pre-existing，HEAD 表级联早有缺口，非翻闸引入）。
- **[FU-show-partitions-deadcode]** 与任务清单 **B-2/M-10** 相关（⚠️RECONCILE）。
- **[FU-flip-e2e]** = 任务清单 **ENG-3**（真翻闸端到端未跑）。
- **[FU-rewrite-output-sizing]（R6/R8）** 中立 driver 未线程 target-file-size + 自适应并行度（与 H-9 同文件族，可一并）。
- **[FU-view-gson-roundtrip] / [FU-view-exception-arms] / [FU-getsqldialect-deadcode] / [FU-showcreatedb-render-ut] / [FU-createtablelike-plugin]**（低）见 git log 历史 + 任务清单 L-BATCH。
- 其余（nested-nullability / where-literal-coercion / broker-write〔=M-5〕/ doris-version-prop〔=L-13〕等）多已被 review 重新发现并归入任务清单。

---

# ⚙️ 操作须知（复用）

- maven：`-f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl :<art> **-am** -DfailIfNoTests=false -Dmaven.build.cache.enabled=false`（漏 `-am`→假错 `${revision}`）。**fe-core 只依赖 `fe-connector-api`** → `:fe-core -am` 不拖 paimon。**fe-connector-paimon 单独 build 必须 `package`**（HiveConf 来自 optional shade，`test-compile` 假错）。**iceberg/api** 正常 `-am test`。
- **⚠️ checkstyle 别加 `-am`**：`-am` 把 `fe-common`（2381 既存 error）拖进假红 → `mvn -pl :<art> checkstyle:check`（不带 -am）。
- **⚠️ bash 工具默认 timeout 120s**：fe-core build 超时 → 调 `timeout` ~590000ms 或后台跑（全模块 ~2min）。
- **⚠️ maven 经管道 `$?` 是管道尾的** → 用 `${PIPESTATUS[0]}` 或 grep `BUILD SUCCESS`；`-q` 抑制 console → 读 surefire **XML** 的 `tests=`/`failures=`。
- **⚠️ stale .class 假红坑**：mutation 后 `os.utime`；**commit 前最终验证务必 fresh recompile**。
- 连接器禁 import fe-core：`bash tools/check-connector-imports.sh`。**连接器测试无 Mockito**（真 InMemoryCatalog/Recording fakes）；**fe-core 用 Mockito**（`CALLS_REAL_METHODS` + `Deencapsulation.setField` + stub `getConnector`/`getMetadata`/`buildConnectorSession`）。**⚠️ Mockito `anyString()` 不匹配 null**。
- **mutation-check（Rule 9/12）**：范式 scratchpad `mutate_*.py`（单行 exact-string 锚点 count==1 守；KILLED=maven rc!=0）。**⚠️ Python 3.6**：`subprocess.run(stdout=PIPE,stderr=STDOUT,universal_newlines=True)`（无 `capture_output`）。**⚠️ review（读源）与 mutation（改源）务必串行**。
- **cwd 会被 harness 重置** → 一律绝对路径。
- **⚠️ 环境**：`/mnt/disk1` 紧（2.0T，96% used）。**下个 session 起步先 `df -h /mnt/disk1`**；**勿用 worktree 隔离编译 agent**（复制整仓，盘不够）。

# ⚠️ Commit 须知（任何 `git add` 前必读）
- **path-whitelist `git add`，严禁 `git add -A`**（scrub `regression-test/conf/regression-conf.groovy` 明文 key + `*.bak` + scratch `.audit-scratch/`·`conf.cmy/`·`META-INF/`·`docker/...`·仓根游离 `fe/IcebergScanPlanProvider.java`·`plan-doc/reviews/P5-paimon-rereview3-*`)。
- commit message：见 `git log` 范式 + 末尾 `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>` + `Claude-Session: …`。PR base = `branch-catalog-spi`，squash。
- **每条 fix = 独立 commit**（沿用 P4-T06e-FIX-* 范式）；HANDOFF + 任务清单 + 设计文档单独 commit（memory 在 `.claude/`、非仓内）。

# 📦 阶段状态
- **工作分支 = `catalog-spi-10-iceberg`**（off `branch-catalog-spi` @ `e5959e1b53d`，PR base = `branch-catalog-spi`，squash）。
- **进度**：P6.1–P6.5 ✅ / P6.6 C1–C3 ✅ / C4 R1–R7 ✅ / C5 DDL/ALTER B1–B5 ✅ / flip-readiness 只读退化 ✅ / 视图 B0–B3 ✅ / 路由翻闸 `18e1b297d7e` ✅ / GSON 迁移 `e68eb5c00c9` ✅ → **⛔ 现卡在 clean-room review 发现修复（见 `P6.6-iceberg-flip-blockers-tasklist.md`）**：**B-1+H-1 ✅ `203cda3e31a`** → **B-2 ✅**（`d7482a39ab9`+`b09d364888b`+`ba80cfb0439`）→ **H-5+H-6 ✅ `d6758ff71f5`**（缓存可达，引擎统一接管）→ **H-2 ✅ `b0c34ec8fe7`**（REST 3 级 namespace 接通 scan/write/procedure）→ **H-3 ✅ `7850c6ad03f`**（filesystem Kerberos 执行认证器）→ **H-4 ✅ `7b26fdbac88`**（表级行数统计 getTableStatistics）→ **H-7 ✅ `a8f1ae9ca2e`**（`FOR VERSION AS OF` branch∪tag，新中立 `Kind.VERSION_REF`）→ **H-8 ✅ `5bf7c90a710`**（视图 schema：ConnectorViewDefinition 加列 + initSchema isView 分支）→ **H-9 ✅ `89658999f49`**（rewrite WHERE 新 `Mode.REWRITE` 精确下推否则报错）→ **H-10 ⏭** → M/L → ENG-1 审计 → ENG-3 e2e → 翻闸二签。
- **⚠️ 推送状态**：P6.4 T01–T06+arg-move 已推 `origin`；**其后全部未 push**（含路由翻闸 + GSON 迁移 + 视图 + C4/C5）。**先修 review 发现，勿 push 半成品翻闸。** 留用户裁量。
- **⚠️ 分支 2026-06-28 被 rebase**：commit 哈希全重写，本文档/旧 commit message 旧哈希以 `git log` 为准。rebase 仅引入 1 问题（`MergeIntoCommand` 未用 import）已修 `33b920bf877`。

# 🧠 给下一个 agent 的 meta
- **逐步处理 = 按任务清单逐条**：每条 P0/P1/P2 走 step-by-step-fix（recon→design 文档 `designs/P6.6-FIX-<ID>-<slug>-design.md`→impl→test+mutation→clean-room review→独立 commit→回填任务清单状态）。处理顺序建议见任务清单 §8（B-1+H-1 → B-2 → H-5+H-6 → 其余 H → ENG-1 → P2 → P3 → e2e）。
- **删除/parity/动码前必 grep 全调用方 + 区分 DEAD vs STILL-CONSUMED**；**HANDOFF/review/设计的依赖名/行号/不变式可能过时** —— 动码前先 recon（grep+实证）再信文档。**翻闸已生效 → in-code "dormant" 注释普遍过时，信控制流不信注释。**
- **⚠️ 冲突优先暴露（Rule 7）**：review 与历史记忆冲突项（M-10 等）回代码重裁，不盲信任一方。`git show master:` 是 legacy 原逻辑的权威来源（工作区 `datasource/iceberg/**` 是迁移后残壳，不可信）。
- **clean-room 对抗 review 偏好**：moderate+ 改动 = 多 reader 对抗 + critic（review 读源与 mutation 改源不可并发）。verbatim 镜像臂则焦点验证即可。
- **flip-gated 诚实**：真 post-flip 写/MTMV/time-travel e2e 翻闸后才能跑——**每条 fix 验收的 e2e 项标注 flip-gated 未跑，勿谎称已验**（Rule 12）。
- **上下文超 30% 即交接**。本 session = 跑完 clean-room review + 建任务清单 + 更新 HANDOFF；在干净节点交接「逐步修 review 发现」。

## 📖 起步必读
1. **`plan-doc/tasks/P6.6-iceberg-flip-blockers-tasklist.md`**（master 任务清单）+ **`plan-doc/reviews/P6.6-iceberg-cleanroom-adversarial-review-2026-06-28.md`**（证据源）。
2. memory：`iceberg-flip-blocker-fixes-progress`（**逐条修进度主索引** + M-10 reconcile 裁定）、`iceberg-cleanroom-adversarial-review-2026-06-28`（本轮结论 + 冲突）、`iceberg-b2-3of3-fecore-recon`（刚完成的 B-2 fe-core 层实证坑）、`iceberg-flip-readiness-gaps`、`handoff-discipline-per-phase`、`consult-trino-before-spi-design`、`clean-room-adversarial-review-pref`、`ask-user-explain-in-chinese-first`、`doris-build-verify-gotchas`、各 `iceberg-*-done`（已完成各面的实证坑）。
3. `plan-doc/tasks/designs/P6.6-C5-flip-readiness.md`（C 类 docker 清单 + 翻闸开关/持久化全景）。
