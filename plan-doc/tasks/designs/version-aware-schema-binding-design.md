# 版本感知的 schema 绑定（research note + design）

> 状态：**用户已批准（2026-07-16）；TODO 0–8 已完成并提交 `4f8b35c2126`；剩 TODO 9（e2e）。**
>
> ## 🔧 施工实况：与设计的 4 处偏差（**照设计原文施工会踩坑**）
>
> 1. **TODO 0 闸门：`INSERT INTO t@branch(b1)` 确实支持，但设计【不需要】扩。** 语法 `optSpecBranch`
>    (`DorisParser.g4:132-134`) → `LogicalPlanBuilder:1460-1461` → `InsertIntoTableCommand.branchName` →
>    `IcebergInsertCommandContext.setBranchName`。**分支名从不进 `snapshots` 表**：`loadSnapshots` 全仓只有
>    2 个调用者（`BindRelation:733` 按引用、`PreloadExternalMetadata:107` 只写默认键）。且写入端
>    (`PhysicalPlanTranslator:639,698`) 用 blind 查找是**有意为之**（`:690-696` 注释：让 DML 的写锚定在其读
>    所用的同一快照）。本设计不动写入端 ⇒ **逐字节不变**。R4 担心的「sink 要加版本字段」是**既存的独立缺口**
>    （写分支时不为分支钉快照），非本设计引入，**不并入**。
> 2. **🔴 `getFullSchema()` 0-arg【绝不能】委派给 `getFullSchema(Optional.empty())`**（施工时踩到）。
>    `empty` 有歧义：「本引用无 pin」(=> latest) vs 「无引用，走环境查找」。合并二者会**剥掉所有
>    statement-global 调用者（物化视图/预加载/写入端）的环境解析**。0-arg 与 1-arg 必须是**兄弟**。
>    同理 `PluginDrivenMvccExternalTable` 的两个 override 亦为兄弟（共用私有 `schemaAt(snapshot)`）。
> 3. **触点 5 的 `PluginDrivenExternalTable:710`（`rawTableProperties()`）【不可执行】，已跳过。**
>    它**作用域里没有任何引用**（6 个调用者全在本类内，喂能力位 CSV / SHOW PARTITION 子句 / 分布列 =
>    表级显示与能力元数据，不在按引用绑定的路径上）。穿 snapshot 需改签名 + 6 个调用者，属另一件事。
>    实做 = `:631`(getPartitionColumns) + `:780`(getNameToPartitionItems) + `:842`(getNameToPartitionValues)
>    + **设计未列的** `PluginDrivenMvccExternalTable:604,610` 的两处 `super.getPartitionColumns()`（R2 的真身）。
> 4. **触点 4 有既有单测覆盖，设计说「无」是错的。** `LogicalFileScanTest` 存在，且其
>    `testComputeOutputIncludesInvisibleRowLineageColumnsForIcebergTable` **会被本改动打挂**（mock 打的是
>    0-arg 桩）—— 这反而证明触点 4 被覆盖。已把桩对齐为 `getFullSchema(Mockito.any())`，并**新增**
>    `computeOutputBindsThisReferencesOwnVersionNotLatest` 直接守触点 4。
>
> **验证实况**：新增 2 条测试（schema 解析层 + 计划层各一）均**先红后绿**；计划层那条的变异（换回
> version-blind 调用）红在 `expected: <[c1]> but was: <[c1, c2]>` —— **`c2` 正是 CI 996541 里 guard 报警的
> 幽灵列**。相关既有单测 **156/156 绿**（含 `StatementContextMvccSnapshotTest` 5/5 —— 「零既有单测被推翻」
> 由推演升级为实测），checkstyle 绿。

> 原始状态：研究完成 + 设计待用户批准，未施工。
> 缘起：用户 2026-07-15 拍板 —— 不接受「回退 LATEST 碰巧能跑」的现状，直接做版本感知重构。
> 取代：`T4-mvcc-version-aware-binding-design.md` 的 C1-a（「blind 读取第一个 pin」兜底方案）。**C1-a 作废**，理由见 §3。

---

# 1. Problem

一条语句里同一张表被引用在**两个不同版本**上（`@tag`/`@branch`/`FOR TIME|VERSION AS OF`）、且**没有裸引用**时，
绑定层会给两个引用都绑上**表的最新 schema** —— 一个**没有任何引用要求过**的第三种 schema。

复现（CI 996541，`external_table_p0.iceberg.iceberg_query_tag_branch:132`）：

```sql
SELECT t1.c1, t2.c1 FROM tag_branch_table@tag(t1) t1 JOIN tag_branch_table@tag(t2) t2 ON t1.c1 = t2.c1;
```

| ref | 那一刻的 schema |
|---|---|
| tag t1 / t2 | `{c1}` |
| tag t3 | `{c1,c2}` |
| LATEST | `{c1,c2,c3}` |

- 绑定层走 **version-blind** `StatementContext.getSnapshot(TableIf)`（`:1015-1034`）→ 两个非默认 pin 且无默认 →
  判「歧义」→ `Optional.empty()` → `PluginDrivenMvccExternalTable.getSchemaCacheValue:497-506` 回退
  **LATEST `{c1,c2,c3}`**。
- 扫描层走 **version-aware** `getSnapshot(TableIf, ts, sp)`（`:1048-1055`）→ 正确解析 t1 → `{c1}`。
- 两者不一致 → L17 guard（`PluginDrivenScanNode.assertBoundColumnsResolveInPinnedSchema`）在 **c2** 上抛。
  **查询从未引用 c2。guard 只是信使，`c2` 是回退 LATEST 凭空造出来的幽灵列。**

**这是本分支自造的 skew**：上游 `fbef303da5f` 是单 version-blind key + `containsKey` 守卫（写入时冲突=丢弃）
⇒ 上游为 first-write-wins，t1/t2 的 schema 同为 `{c1}` ⇒ 上游这条查询零 skew。

**被本轮核验推翻的怀疑**（交接文档 §「施工前必须先解决的 3 点」第 1 条）：
「上游单 key、两次 pin 会互相覆盖 ⇒ 上游实为最后一个赢」—— **错**。上游 `StatementContext.java:717`
`if (!snapshots.containsKey(mvccTableInfo))` 把冲突变成**丢弃而非覆盖**，故上游确为 first-write-wins。
2 个专职唱反调的 agent 各试 5 条绕过路径（MTMV 的无守卫 `setSnapshot`、子查询/视图是否换新上下文、null 值、
早退让第二次 pin 到不了、场景是否不可达）全部失败，均判 SURVIVES。

---

# 2. Research —— Trino 怎么解的

## 2.1 关键史实：**Trino 踩过一模一样的坑，并且是靠删掉 Doris 今天这个设计解决的**

Trino 早期把版本编码进**表名**（`SELECT * FROM "tbl@4903761131980730388"`），于是走了 name-keyed 解析而撞车：

- **issue #8868**（closed 2021-08-12）"Accessing two different snapshot ids in the same query is broken"：
  `SELECT * FROM "tbl@v2" EXCEPT SELECT * FROM "tbl@v1"` 返回 **0 行**（应为 1 行）。
- 候选修复 **PR #8843** 的正文逐字写明根因：
  > *"Snapshot ids were incorrectly cached and when more than one snapshot id was used in the query only first one was used."*

**这就是 Doris 今天的 bug（按名解析 → 第一个/最新版本通吃）。** Trino 的结论是：
**版本不能是标识符的一部分**，因为标识符会被 name-key 化并缓存。现由
`TestIcebergReadVersionedTable.java:90-98` 回归钉死 —— `tbl@snapshotid` 名字形式**已完全不再解析**。

> ⚠️ 对 Doris 的直接含义：`StatementContext` 用 `MvccTableInfo(catalog,db,table)+versionKey` 做 map key
> 已经是「版本进 key」的修正版，方向正确；剩下的问题**只**在于 schema 读取那条路径拿不到版本。

## 2.2 Trino 现行设计的 4 条承重属性

| # | 属性 | 证据 |
|---|---|---|
| 1 | **版本是分析器侧的一等值，不进名字**。语法 `queryPeriod: FOR rangeType AS OF end=valueExpression`，分析期 `evaluateConstant` **常量折叠**成字面值 → `io.trino.metadata.TableVersion`（record） | `StatementAnalyzer.java:6292-6334`、`TableVersion.java:24-35` |
| 2 | **版本 + 解析后的 schema 一起烤进不可变的 per-reference handle**。`IcebergTableHandle` 同时带 `OptionalLong snapshotId` 与 `String tableSchemaJson`，后者 = `SchemaParser.toJson(schemaFor(table, snapshotId))` = **该快照时刻的历史 schema**，在 handle 构造期算好 | `IcebergMetadata.java:713-722, :767` |
| 3 | **schema 派生只读 handle**：`getColumnHandles` = `SchemaParser.fromJson(table.getTableSchemaJson())` —— 零 `catalog.loadTable`、零 ambient/session 查找、零「这个表名当前是哪个版本」 | `IcebergMetadata.java:1127` |
| 4 | **强制性**：不带版本的 `getTableHandle(session, tableName)` 重载**已从 SPI 删除**，master 只剩唯一重载 ⇒ 没有任何连接器有逃生口；~28 个不支持版本的连接器被迫显式 `throw "This connector does not support versioned tables"` 而不是静默忽略 | `ConnectorMetadata.java:107-119`（`grep -c` = 1）；`HiveMetadata.java:523-527` |

补充：Trino 的分析是**及早（eager）**的（`visitTable` 内联物化 output Fields），并按 **AST 节点身份**
（`Map<NodeRef<Table>, TableEntry>`）而非名字索引。`@branch`/`@tag` **不是**名字后缀语法 —— 读分支/标签统一是
`FOR VERSION AS OF 'name'`（VARCHAR → TARGET_ID → `table.refs().get(refName)`）；`@branch` 只作为**写侧**语法存在。
两个版本在一条查询里 join 是**显式支持**且有回归（`testReadMultipleVersions`）。

## 2.3 Trino → Doris 概念映射（**这是本文最重要的一张表**）

| Trino | Doris 对应物 | 已存在？ |
|---|---|---|
| `ConnectorTableHandle` —— per-reference、带版本、不可变 | **`LogicalFileScan`** —— 已持有 `Optional<TableSnapshot> tableSnapshot`(`:59`) + `Optional<TableScanParams> scanParams`(`:60`)，`final`、ctor 期赋值(`:91-92`)，6 个 wither 全线穿透。**不是 `ExternalTable`** —— 那是 Caffeine 缓存的、跨语句跨 session 共享的目录对象，对应 Trino 的 *live* `BaseTable`，**永远不是 handle** | ✅ **版本已经在对象上** |
| `IcebergTableHandle.tableSchemaJson` —— 烤进 handle 的历史 schema | **`PluginDrivenMvccSnapshot.pinnedSchema`** —— 完整 `PluginDrivenSchemaCacheValue`（columns + partitionColumns + remoteNames + tableProperties），在 `loadSnapshot` 内联构建(`PluginDrivenMvccExternalTable:396-399`)，用的是与 LATEST 路径**同一个 builder**，不会漂移 | ✅ **元数据已抓好、已经是历史的** |
| `schemaFor(table, snapshotId)` | `metadata.applySnapshot(...)` → `metadata.getTableSchema(session, pinnedHandle, connectorSnapshot)`(`:396-397`)。**`pinnedHandle` 本身就是一个 Trino 式带版本的 `ConnectorTableHandle`**，构造→使用→丢弃，只留派生出的 schema。4 个连接器全部 override 3-arg `getTableSchema` | ✅ **SPI 零改动** |
| `Analysis.tables` 按 AST 身份索引 | `RelationId`。`LogicalRelation.equals():54-64` **只**比 `relationId`，`hashCode():67-70` = `Objects.hash(relationId)` ⇒ 两个 `@tag` 引用拿到不同 RelationId，**在 Memo 里不可能合并** | ✅ **给 `LogicalFileScan` 加字段零 equals/hashCode 代价** |
| `getColumnHandles` = 从参数取 schema | **`PluginDrivenMvccExternalTable.getSchemaCacheValue():497-506` —— `MvccUtil.getSnapshotFromContext(this)`，ambient、无引用参数** | ❌ **这是唯一缺失的一环，也就是全部缺陷** |
| 版本化表 wrapper/decorator | **不存在**，且不该造 —— 本仓两个「作用域内 schema 变体」的既有范式都是 **relation 字段**范式（`LogicalOlapScan.selectedIndexId` → `getOutputByIndex:799-805`） | ❌ 造 wrapper 是外来构造 |

---

# 3. 决定性发现：**原设计的核心前提是错的（错在悲观一侧）**

`T4-mvcc-version-aware-binding-design.md:160-171` 判定「真正治本的版本感知绑定」不适合本次，理由是：

> **结构性障碍**：`getSchemaCacheValue()` 无引用参数，且其调用点 `LogicalCatalogRelation.computeOutput:152-164`
> 是惰性求值的（`AbstractPlan:91-93`）—— 版本必须随对象走，不能靠调用时的上下文传。**这直接判死所有「小改」变体。**
> **改动面**：…… 即当前 20 个 blind 调用点逐个定性。**保守估计 2–4 人天设计+实现，外加 review/回归轮次。**

**三条都不成立**（每条已亲自核实，非 agent 转述）：

1. **「版本必须随对象走」—— 它已经随对象走了。** `LogicalFileScan:59-60` 的 `tableSnapshot`/`scanParams` 是
   `final`、ctor 期(`:91-92`)赋值，**早于** `LazyCompute.of(this::computeLogicalProperties)`(`AbstractPlan:91-94`) 触发。
   `computeOutput()` 执行时版本**就在 `this` 上**。惰性求值**根本不构成障碍**。
2. **「fix locus = `LogicalCatalogRelation.computeOutput:152-164` 调 `getBaseSchema()`」—— 错，会改错文件。**
   `LogicalFileScan:195-207` **override 了** `computeOutput()`，把 `table instanceof PluginDrivenExternalTable`
   路由到 `computePluginDrivenOutput():210-220`，后者调的是 **`table.getFullSchema()`** 而非 `getBaseSchema()`。
   `LogicalCatalogRelation:152` 只有 non-PluginDriven 表才走得到。**真正的 fix locus = `LogicalFileScan:210-220`。**
3. **「20 个 blind 调用点要逐个定性」—— 数字错（真值 24），且绝大多数不用动。** 24 个里只有**极少数**是
   per-reference 的；其余是 statement-global（MTMV/preload/dictionary/sink），**本就没有 per-reference 版本可穿**。

**并且 version-aware 的查找函数已经存在**：`MvccUtil.getSnapshotFromContext(tableIf, tableSnapshot, scanParams)`
(`MvccUtil.java:58-69`) → `StatementContext.getSnapshot(TableIf, Optional, Optional)`(`:1048-1055`)。
它是 **key-exact** 的（用 `loadSnapshots` 写入时的同一个 key），**不关心存在几个 pin** ⇒ 求值早晚完全无关。
只有 *blind* 那条的歧义启发式(`:1015-1034`)才会随 pin 数量退化。

**⇒ 结论：这不是 2–4 人天的 Nereids 层重构，是一处「把已有的 3-arg 查找接到已有的 relation 字段上」的接线。**
**⇒ C1-a（blind 读取第一个 pin）作废** —— 它是在原前提（治本太贵）下的妥协；前提没了，妥协就没有存在理由。

**既有范式，照抄即可（勿发明）**：`CheckPolicy.java:136-139` 已经在做这件事 —— 一条分析规则从 relation 上取
`scan.getTableSnapshot()`/`scan.getScanParams()` 然后调 3-arg `getSnapshot`。**把它推广，别另起炉灶。**

---

# 4. Design

## 4.1 一句话

> **每个表引用在绑定期用它自己的版本解析自己的 schema。** 版本已经在 `LogicalFileScan` 上，历史 schema 已经在
> `MvccSnapshot` 上，version-aware 查找已经存在 —— 只需让 schema 读取 API **接受一个 snapshot 参数**，
> 并在 `computePluginDrivenOutput()` 处把三者接起来。

对齐 Trino 的 §2.2 属性 1/2/3；属性 4（SPI 强制）不适用（Doris 的 4 个连接器已全部 override 3-arg `getTableSchema`）。

## 4.2 Goals / Non-Goals

**Goals**
- G1 同表多版本引用，各自绑定各自的 schema；LATEST 永不出现在没人要求它的语句里。
- G2 `iceberg_query_tag_branch` 的 `qt_sub_join_tag_with_tag_1` 由红转绿，`.out` **零改动**。
- G3 消除「删列形状」的语义收窄（用户拒绝接受的那条）——版本感知下它**由构造正确**，不再报错也不再蒙对。
- G4 顺带修掉 §7 那条**至今无人记录**的静默错误：`t@tag(v1) JOIN t` 今天走 blind 规则 (1)「默认 pin 通吃」，
  **两个引用都绑 LATEST**；tag 的 schema 若是兼容子集则**静默通过 guard**（读到被绑错 schema 的数据）。

**Non-Goals**
- N1 **不动 SPI**、不动连接器、不动 `PluginDrivenScanNode` 的扫描侧 pin（`:874-875` 已是 version-aware）。
- N2 **不动 L17 guard**（见 §6 决策 3）。
- N3 **不修 sys 表时间旅行的范畴错误**（既存 KNOWN GAP，`PluginDrivenScanNode:899-902`）—— 显式排除，不继承。
- N4 **不碰 `TableIf.getBaseSchema()`/`getFullSchema()`**（在接口上，`:148/:150/:167`，4 个实现含 `Table` = 所有内表）。
- N5 不给 statement-global 消费者穿版本（MTMV/preload/dictionary/sink）—— 它们没有 per-reference 版本可言。
- N6 **不为「类型级 skew」（同列名不同类型，如 int→long promotion）做任何事** —— 版本感知下两侧各读各的类型，
  该洞随之关闭，但不新增校验。

## 4.3 触点（5 处，全部在 fe-core，零 SPI、零连接器）

| # | 文件:行 | 改动 |
|---|---|---|
| 1 | `ExternalTable.java:423` | 新增 `getSchemaCacheValue(Optional<MvccSnapshot> snapshot)`；基类实现**忽略**该参数，保持按名读缓存。0-arg 保留为 `getSchemaCacheValue(Optional.empty())`。**不上 `TableIf`** |
| 2 | `PluginDrivenMvccExternalTable.java:497-506` | override 1-arg 形式：用**传入的** snapshot 的 `getPinnedSchema()`；`null`（latest / `@incr` / 降级）落 `getLatestSchemaCacheValue()`。0-arg override 保留为 ambient 遗留路径，供 statement-global 消费者用 |
| 3 | `ExternalTable.java:175-179` | 新增 `getFullSchema(Optional<MvccSnapshot> snapshot)`（0-arg 委派给它 + empty） |
| 4 | **`LogicalFileScan.java:210-220`** ⭐ | `computePluginDrivenOutput()`：用 `this.tableSnapshot`/`this.scanParams` 走 3-arg `MvccUtil.getSnapshotFromContext`，把结果传给 `getFullSchema(snapshot)`。**本设计最高杠杆的一处编辑**（零子类、零 equals/hashCode 涟漪） |
| 5 | `PluginDrivenExternalTable.java:631,710,780,842` | 把 snapshot 穿进这 4 处 `getSchemaCacheValue()`，让 `super.*` 委派不再重入 ambient（见 §6 决策 2） |

**近乎免费的两处顺带修**（形参已在手，drop-in）：
- `LogicalFileScan.java:71` —— ctor 里的 `initSelectedPartitions`，`tableSnapshot`/`scanParams` **就是该 ctor 的形参**(`:68-69`)。
- `PruneFileScanPartition.java:79,88` —— `scan` 是形参，**逐字照抄 `CheckPolicy:138`**。

## 4.4 数据流（改后）

```
BindRelation:733  loadSnapshots(table, ts, sp)         → snapshots[(ctl,db,tbl)+versionKeyOf(ts,sp)] = snapshot(含 pinnedSchema)
   ↓ （构造 LogicalFileScan，ts/sp 存为 final 字段 :91-92）
LogicalFileScan.computeOutput()  ← 惰性触发，但 this.tableSnapshot/this.scanParams 早已在 :91-92 就位
   ↓
MvccUtil.getSnapshotFromContext(table, this.tableSnapshot, this.scanParams)   ← key-exact，与 pin 数量无关
   ↓
table.getFullSchema(snapshot) → getSchemaCacheValue(snapshot) → snapshot.getPinnedSchema()   ← 该引用自己的历史 schema
   ↓
tuple = {c1}   ← 与扫描侧 pinMvccSnapshot:874-875 解析出的 pinnedSchema 同源同值
   ↓
L17 guard 比对同源的两者 → 恒等 → 永不误报
```

---

# 5. Risk

## R1 —— schema 缓存**没有版本维度**，且 `SchemaCacheKey.equals` 是个陷阱

`SchemaCacheKey` 只有一个字段 `nameMapping`，`equals` 用的是 **`instanceof` 而非 `getClass()`**，会忽略任何子类状态。
tree-wide **零子类**。⇒ 一个天真的 `VersionedSchemaCacheKey extends SchemaCacheKey` 会与基类 key、与**兄弟版本
互相 equals** → 两个版本静默撞进同一个缓存条目 → **本 bug 原样复活，且升级为跨 session**。

**化解**：**本设计不需要任何版本化缓存 key**。pinned schema 骑在 `MvccSnapshot` 上（`loadSnapshot:396-399` 内联构建、
`:500-503` 直接返回），**pinned 路径今天就是绕过缓存的**。缓存 loader
（`loadSchemaCacheValue` → `ExternalCatalog.getSchema` → `initSchemaAndUpdateTime` → `initSchema()` →
`PluginDrivenExternalTable:473` 的 **2-arg LATEST** 重载）严格 context-free。

> **🔴 设计铁律**：**永不**把版本化 schema 路由进 `ExternalTable.getSchemaCacheValue()` 的缓存分支；
> **永不**让 `initSchema()` 变得 ambient-sensitive。那是 Caffeine + TTL + 跨语句跨 session 共享的，
> 且 `initSchemaAndUpdateTime` 有 `setUpdateTime(now)` 副作用(`ExternalTable:372`)——历史读会**篡改表的更新时间**。

## R2 —— 签名加宽本身不够：`super.*` 会重新掉回 ambient

`PluginDrivenMvccExternalTable.getPartitionColumns(Optional<MvccSnapshot>):577-587` **收**了显式 snapshot，
但只用它做「分区失效/range-view」的**判定**，列表本身仍委派 `super.getPartitionColumns()` →
`PluginDrivenExternalTable:629-634` → ambient 的 `getSchemaCacheValue()`。同形状还有 `:710`、`:780`、`:842`。
且 `PluginDrivenExternalTable.getPartitionColumns(Optional<MvccSnapshot>):624-627` **直接丢弃**其参数。

**这不是第二个独立缺陷** —— `PluginDrivenMvccExternalTable:400-406` 的注释写明该委派是**有意为之**：
*"getPartitionColumns(snapshot) flows through super -> the schema-aware getSchemaCacheValue() below -> the pinned
schema's partition columns."* 设计意图就是让 `getSchemaCacheValue()` 做**唯一的版本解析点**。它对 1 个 pin 有效、
对 ≥2 个失效 —— **同一个缺陷，一个修法**。
**但操作性结论成立**：**不要**设计成「把 `Optional<MvccSnapshot>` 顺着现有 accessor 一路往下传」，
要么修解析点本身，要么每处 `super.*` 委派都得重新布线。

## R3 —— `versionKeyOf` 按**原始参数**做 key，同一个 tag 的两种写法会 pin 两次

`StatementContext.java:1064-1080`：`key.append("p:").append(sp.getParamType()).append(':')
.append(sp.getMapParams()).append(':').append(sp.getListParams())`。
而 `extractBranchOrTagName`（`PluginDrivenMvccExternalTable:449-460`）对同一个 tag **接受两种写法**
（`mapParams{name->v1}` 与 `listParams[v1]`）⇒ `@tag("v1")` 与 `@tag("name"="v1")` 产生**不同 key → 同一有效版本 pin 两次**。

**爆炸半径小于初判**：**不破坏 version-aware 路径** —— `loadSnapshots` 与 aware 查找是从**同一个引用的同一个
选择器对象**算 key 的，「永远命中自己那条」成立。代价只是多一次 `loadSnapshot` 物化。**非阻塞项，记一笔。**

## R4 —— 本设计**不覆盖**的面

- **sys 表**：`PluginDrivenSysExternalTable.getSchemaCacheValue():154` 是第 3 个 override；BindRelation 对
  `$` 后缀 relation **短路** `loadSnapshots`(`:727-731`)，走 `resolveSysTableSnapshotPin()`。
  `PluginDrivenScanNode:899-902` 的 KNOWN GAP 机制已确认（`loadSnapshot` 用**源表** handle 建 pinnedSchema，
  而 `IcebergConnectorMetadata:363-369` 对 sys handle 有意返回**固定的** sys 表 schema → 范畴错误）。**显式排除**。
- **写路径**：`PhysicalPlanTranslator:639,698`（sink pin）在 2 版本下退化为 unpinned-write。
  **仅核实了 sink 今天不带版本字段**；若 Doris 支持 `INSERT INTO t@branch(b1)`，该 2 处需从 statement-global
  升级为 per-reference（`PhysicalConnectorTableSink` 加版本字段）。**施工前须 grep 确认**。

---

# 6. 设计决策（施工者不得擅自改变，改则重新 review）

1. **不造版本化表 wrapper/decorator。** 本仓无此范式；既有范式是 relation 字段范式（`LogicalOlapScan.selectedIndexId`）。
2. **唯一版本解析点 = `getSchemaCacheValue(snapshot)`。** 不把 snapshot 顺着 accessor 链往下传（R2）。
3. **L17 guard 原样保留，一个字不改。** 改后它的用户可见抛出**不可达**（tuple 与校验同源于 `pinnedSchema`），
   但它的两个输入仍来自**独立路径** —— `boundColumns` 来自 `desc.getSlots()`（分析期绑定），`pinnedSchema` 来自
   扫描期经 translator 穿线的选择器(`:874-875` ← `PhysicalPlanTranslator:817-818`)。**若将来某条 rewrite 规则丢了
   `tableSnapshot`/`scanParams`，guard 是唯一的兜底。** 不做「降级为内部断言」的改写（Rule 3：不改没坏的东西）。
4. **LATEST pin 不物化 schema。** `materializeLatest` 今天不抓 schema，pinnedSchema==null → 落
   `getLatestSchemaCacheValue()` = 缓存的 LATEST —— **这正是裸引用想要的**。物化它会：每语句多一次
   `getTableSchema`、绕过缓存 = 真实性能回退。**不做。**
5. **0-arg `getSchemaCacheValue()` 保留**，供 24 个 blind 点里的 statement-global 消费者继续用。
   **不删**（Rule 3），但其 javadoc 须写明：per-reference 消费者必须用 1-arg 形式。

---

# 7. 本轮新发现、且**至今无人记录**的静默错误（G4）

`StatementContext.getSnapshot(TableIf)` 的规则 (1)「default(`""`) key 通吃」(`:1021-1024`) 对
**`t@tag(v1) JOIN t`** 也是**错的、且静默**：latest 的 pin 赢下**两个**引用的 schema 绑定；
tag 那侧的 scan 只有在 tag schema **缺**某个已绑定列时才会撞上 L17 guard —— 若 tag schema 是**兼容子集**，
**静默通过**（读到被绑错 schema 的数据）。

**⇒ 「guard 是完整兜底」这个说法，即使在今天也不成立。** 版本感知设计**顺带**关掉这个洞（G4）：
裸引用 key=`""` → 默认 pin → pinnedSchema==null → LATEST（正确）；`@tag(v1)` → 自己的 pin → `{c1}`（正确）。

---

# 8. TODO（有序；每步的成功判据可独立验证）

- [ ] **0. 施工前 grep**：确认 Doris 是否支持 `INSERT INTO t@branch(...)`（R4）。若支持 → 先扩设计再动工。
- [ ] **1. 测试先行（必须先红）**：`PluginDrivenMvccExternalTableTest` 加
      `testGetSchemaCacheValueBindsOwnVersionWhenTwoVersionsPinned` —— 两个不同 `versionKeyOf` 的 pin、无 default，
      断言各自 `getSchemaCacheValue(snapshot)` 拿到**自己**的 pinnedSchema、且 `assertNotSame(latestCacheValue)`。
      跑 → **必须红**（Rule 9）。
- [ ] **2. 触点 1+3**：`ExternalTable` 新增 1-arg `getSchemaCacheValue(snapshot)` / `getFullSchema(snapshot)`，
      0-arg 委派 + empty。编译。
- [ ] **3. 触点 2**：`PluginDrivenMvccExternalTable` override 1-arg。跑步骤 1 的测试 → **绿**。
- [ ] **4. 触点 4 ⭐**：`LogicalFileScan.computePluginDrivenOutput()` 接线（3-arg lookup → `getFullSchema(snapshot)`）。
- [ ] **5. 触点 5 + 两处顺带修**：`PluginDrivenExternalTable:631,710,780,842`；`LogicalFileScan:71`；
      `PruneFileScanPartition:79,88`（照抄 `CheckPolicy:138`）。
- [ ] **6. 回归既有单测**：
      `mvn -o -f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml test -pl fe-core -am -Dexec.skip=true -Dmaven.build.cache.enabled=false -Dtest=StatementContextMvccSnapshotTest,PluginDrivenMvccExternalTableTest,PluginDrivenScanNodeMvccSchemaGuardTest,PluginDrivenScanNodeSysTablePinTest,PluginDrivenExternalTableTest -DfailIfNoTests=false`
      —— **务必核对 `Tests run:` 数字**（踩过「`A+B+C` 静默假绿、读到旧报告」的坑；多类必须**逗号**分隔）。
      预期：`StatementContextMvccSnapshotTest` 的 2 条 blind 歧义断言 **保持绿**（本设计**不改** blind 语义！
      这是相对 C1-a 的一大优势 —— **零既有单测被推翻**）。
- [ ] **7. 变异验证**：把触点 4 换回 0-arg `getFullSchema()` → 步骤 1 的测试**必须红**。
- [ ] **8. checkstyle**（fe-core 扫 test 源）。
- [ ] **9. e2e**：`iceberg_query_tag_branch`（闸门，`.out` 零改动）+ `iceberg_tag_retention_and_consistency`
      + `test_iceberg_time_travel` + `paimon_time_travel` + `iceberg_branch_complex_queries`
      + `iceberg_branch_partition_operations`。
- [ ] **10. 文档**：本文件记结论；`T4-mvcc-version-aware-binding-design.md` 顶部标注 **SUPERSEDED**（保留，不删）。

---

# 9. 仍未证实（不得当成已验证）

1. **本 session 未编译、未跑任何测试。** 所有行号/语义/git 祖先关系均已对 HEAD 核实（承重项由我本人 grep 复核，
   非 agent 转述）；**编译性与测试结果未验证**。
2. **「修完就绿」未证。** `iceberg_query_tag_branch` 在 `:132` 第一个 tag-join 就 abort ⇒
   `sub_join_tag_with_tag_2/3/4`、`sub_join_tag_with_branch_1/2/3`、`sub_with_tag_1..4` 及第二轮
   (`num_files_in_batch_mode=1024`) 的全部断言**在本分支从未执行过**。本设计只证明抛出机制被消除。以真实 CI 为准。
3. **写路径（`INSERT INTO t@branch`）未查**（R4）。TODO 0 是它的闸门。
4. **`PartitionIncrementMaintainer:311,319` 的桶归属（per-reference vs 不可达）未定**。若 MV rewrite 对
   time-travel relation 另有闸门，per-reference 桶还会更小。
5. **Trino 侧两处引用不可尽信**：#8868 的关闭 commit 与 PR #8843 的合并状态对不上（`merged=false` 却同日关闭）；
   「PR #12542 移除 `isValidTableVersion` 并随 Release 386 发布」的说法其 3 个 commit 不支持。
   **可安全引用的**：master 的 `ConnectorMetadata.java` 里 `isValidTableVersion` **零命中**、`getTableHandle`
   **只剩一个重载**。#8843 正文的根因陈述独立成立，不依赖其合并状态。
6. **Trino 的 scan/split 侧版本传播未调查**（`IcebergSplitSource` 未读）。Doris 扫描侧本就正确
   (`PluginDrivenScanNode:874`)，此处无需向 Trino 取经。
7. **`rg` 工具告警**：本机 `rg` 存在**静默改写匹配文本**的行为 ⇒ 本文所有否定/零命中结论均以 `grep` 得出。
