> # ⛔ SUPERSEDED（2026-07-15）—— 保留作史料，**不要照此施工**
>
> 接替者：[`version-aware-schema-binding-design.md`](./version-aware-schema-binding-design.md)。
>
> **作废理由**：用户拒绝接受本文 Risk §E 的语义收窄（「删列形状」从碰巧能跑变成 fail-loud 报错），拍板直接做
> 版本感知重构。而重构一经调研，本文 `:160-171` 判死它的**三条前提全部不成立**（每条已对 HEAD 核实）：
> ① 「版本必须随对象走、惰性求值判死小改」—— **版本已经随对象走**：`LogicalFileScan:59-60` 的
> `tableSnapshot`/`scanParams` 是 `final`、ctor 期(`:91-92`)赋值，**早于**惰性求值触发；且 version-aware 查找
> `MvccUtil.getSnapshotFromContext(t, ts, sp)`(`:58-69`) 是 **key-exact** 的，与 pin 数量、求值时机全无关。
> ② 「fix locus = `LogicalCatalogRelation.computeOutput:152-164` 调 `getBaseSchema()`」—— **错，会改错文件**：
> `LogicalFileScan:195-207` override 了 `computeOutput()`，PluginDriven 表走 `computePluginDrivenOutput():210-220`
> 调的是 `getFullSchema()`。③ 「20 个 blind 调用点」—— 真值 **24**，且绝大多数是 statement-global、无需动。
>
> **⇒ 治本不是 2–4 人天的 Nereids 重构，是 5 处接线，且零既有单测被推翻**（本文的 C1-a 反要推翻 2 条）。
>
> **本文仍然有效、已被接替者继承的部分**：Problem 的事实核验表、`run11.sql` 版本谱系、
> 「上游 `containsKey` 守卫 = first-write-wins」（本轮 2 个对抗 agent 复核 SURVIVES，交接文档里那条
> 「上游实为最后一个赢」的怀疑**被证伪**）、被否决方案表（ThreadLocal / binding-scope / 改 `.out` / 放宽 guard）。

# Problem

CI 996541（`Doris_External_Regression`, commit `fa2fcf4b246`）中 `external_table_p0.iceberg.iceberg_query_tag_branch`
在 `qt_sub_join_tag_with_tag_1`（`iceberg_query_tag_branch.groovy:132-135`）失败：

```sql
SELECT t1.c1, t2.c1
FROM tag_branch_table@tag(t1) t1
JOIN tag_branch_table@tag(t2) t2
ON t1.c1 = t2.c1 order by t1.c1;
```

期望 `.out` 是 `1	1`（`regression-test/data/external_table_p0/iceberg/iceberg_query_tag_branch.out:264-265`），实际抛
L17 fail-loud guard（`PluginDrivenScanNode.assertBoundColumnsResolveInPinnedSchema:941-945`）的
`"Reading the same table at multiple versions with different schemas in one statement is not supported yet:
column 'c2' ..."`。

**查询根本没引用 c2** —— guard 只是信使。

## 事实核验（信 HEAD，不信文档）

| 断言 | 核验方式 | 结果 |
|---|---|---|
| `.out` / `.groovy` / `run11.sql` 本分支未改 | `md5sum` vs `git show c0865b021b0:<f>` | ✅ 三个文件与 merge-base **逐字节相同**；`git log c0865b021b0..HEAD -- <f>` **全空** |
| `.out` 与上游 `fbef303da5f`(#51272) 相同 | md5 | ✅ `.out`（`3f5687afe3b8...`）与 `run11.sql`（`48d8ef495f22...`）相同；**⚠️ 更正 issue 文档**：`.groovy` 与 `fbef303da5f` **不同**（上游后续 `202c75d92cc`/`a73850955e2` 收紧了异常文案与 label），但那些改动同样在 merge-base 内，**不是本分支所为**。正确表述应为「与 merge-base 逐字节相同」，比原表述更强。 |
| 上游 master 的 `getSnapshot(TableIf)` 是单 key、从不返 empty | `git show fbef303da5f:...StatementContext.java` | ✅ `MvccTableInfo mvccTableInfo = new MvccTableInfo(tableIf); return Optional.ofNullable(snapshots.get(mvccTableInfo));`（且 `loadSnapshots(ts, sp)` 遍历 `tables.values()` + `containsKey` 守卫 = **first-write-wins**） |
| version-aware key 由 `442a1081e6d` 引入 | `git log -S"versionKeyOf"` / `-S"isSameTable"` | ✅ 唯一命中 `442a1081e6d`（本地原始 commit = `de1af7a594e`，后被 squash） |
| `442a1081e6d` 是否已进上游 | `git merge-base --is-ancestor 442a1081e6d c0865b021b0` | ✅ **是 merge-base 的祖先** → 该代码**已合入 upstream `branch-catalog-spi`(#64688)**。本改动 = 对已上游代码的 follow-up fix（不是改自己未 push 的 commit）。对照 `22516845ca3`（L17 guard）**不是**祖先 = 本地未上游。 |
| 本分支是否动过 `StatementContext.java` | `git log c0865b021b0..HEAD -- .../StatementContext.java` | ✅ **空**（本分支零改动） |

`run11.sql` 的版本谱系（决定各 ref 的 schema）：

| 行 | 动作 | 该时刻 schema |
|---|---|---|
| :5-6 | `create table (c1 int)` + `insert (1)` | `{c1}` |
| :8-9 | `create branch b1` / `create tag t1` | `{c1}` |
| :11 | `insert (2)` | `{c1}` |
| :13-14 | `create branch b2` / `create tag t2` | `{c1}` |
| :16 | `add column c2` | `{c1,c2}` |
| :18 | `insert (3,4)` | `{c1,c2}` |
| :20-21 | `create branch b3` / `create tag t3` | `{c1,c2}` |
| :23 | `add column c3` | LATEST=`{c1,c2,c3}` |

⇒ **tag t1 与 tag t2 的 schema 都是 `{c1}`**。上游单 key 绑定 `{c1}`，这条查询在 master 上**零 skew**。

---

# Root Cause

## 链路（每一环已对 HEAD 核实）

1. `BindRelation.getLogicalPlan:733` 逐 relation 调
   `statementContext.loadSnapshots(table, unboundRelation.getTableSnapshot(), scanParams)`。
   `StatementContext.loadSnapshots:988-998` 以 `new MvccTableInfo(specificTable, versionKeyOf(ts, sp))` 为 key
   落 pin（`versionKeyOf:1065-1082`：`@tag(t1)` → `"p:tag:{}:[t1]"`）。**两个 tag ⇒ 两个 entry，无 default(`""`) entry**。
2. 该 relation 的 schema 经 `LogicalCatalogRelation.computeOutput:152-164` → `table.getBaseSchema()` →
   `ExternalTable.getFullSchema:176-177` → **`PluginDrivenMvccExternalTable.getSchemaCacheValue:494-504`**（override）
   → `MvccUtil.getSnapshotFromContext(this):35-44` → **version-blind `StatementContext.getSnapshot(TableIf):1015-1034`**。
3. `getSnapshot(TableIf)` 的三分支（自带 javadoc `:1000-1014`）：
   - **case (1)** `:1019-1023` default(`""`) entry 存在 → 返回它；
   - **case (2)** `:1024-1031` 该表**恰好一个** pin（忽略 version）→ 返回它；
   - **case (3)** `:1027-1029` **两个及以上非 default pin 且无 default → `return Optional.empty()`**。
   本例命中 case (3)。
4. `getSchemaCacheValue:496-503`：`ctx` 为 empty → `return getLatestSchemaCacheValue()` → **LATEST `{c1,c2,c3}`**。
5. `LogicalFileScan` 不是 `OutputPrunable`（`ColumnPruning` 只裁剪 `OutputPrunable`）⇒ scan tuple 携带未裁剪的
   `c1,c2,c3`；`PhysicalPlanTranslator.generateTupleDesc` 由 `fileScan.getOutput()` 造 tuple。
6. `PluginDrivenScanNode.pinMvccSnapshot:868-909` 的 **version-aware** 查找（`:874-875`，
   `getSnapshot(TableIf, ts, sp):1048-1055`）**正确**解析到本 ref 的 pin → `pinnedSchema = {c1}`；
   `:898-908` 拿 tuple 的 `c1,c2,c3` 去比 → `c2` 的 field-id 不在 `{c1}` → `:941` 抛。

## 关键补充证据（issue 文档未记，改变了方案选择）

**`AbstractPlan:91-93` 的 `logicalPropertiesSupplier = LazyCompute.of(this::computeLogicalProperties)`**
⇒ relation 的 `getOutput()`（从而第 2 步的 blind 查找）是**惰性**的，**不保证**发生在下一个 relation 的
`loadSnapshots` 之前。所以今天 case (2)/case (3) 谁生效**取决于 `getOutput()` 何时被 force**：

- 早求值：relation#1 见 1 个 entry → case (2) → `{c1}`（正确）；
- 晚求值：relation#1 见 2 个 entry → case (3) → LATEST `{c1,c2,c3}`（错）。

**同一个 relation、同一条语句，答案随求值时机翻转** —— 这是当前实现的隐性不确定性，也解释了 issue 文档为何把抛点
归到 t1 那侧（两侧 tuple 都可能被污染成 LATEST）。**任何依赖「blind 读发生在某个时间窗内」的修法（例如「返回最后
pin 的 entry」或在 BindRelation 前后开 binding-scope）都不成立** —— 见 Design §「被否决的方案」。

## 「为什么引入 case (3)→empty」（读了原始设计与 commit）

- 原设计：`plan-doc/tasks/designs/iceberg-branch-mvcc-and-static-partition-overwrite-fixes.md:37-40`
  —— 修的 bug 是 **①「同语句 main + `@branch` 塌缩」**（`select ...(select max(value) from T@branch(b1)).. from T`
  读到 main 数据）。原文对 case (3) 的理由是：
  > "Keeps existing behavior for the common cases; **only stops returning an arbitrary version** for a
  > version-blind read when multiple versions are pinned."
- 即：**case (3)→empty 是「不想返回任意一个版本」的保守选择，不是 bug ① 的必要条件**。
  bug ① 的场景**有 default entry** → 走 **case (1)**；真正治好 bug ① 的是 ①版本化 key ②`pinMvccSnapshot`
  改用 version-aware 查找（`PluginDrivenScanNode:874-875`）。两者本设计**都不动**。
- 同一设计文档 `:112-115` 已把「同表多版本 schema 分歧」登记为 `[FU-mvcc-mixed-schema]` follow-up
  = 本次要处理的正是那条欠账的**误伤面**。

**⇒ 结论：C1-a 不破坏 `442a1081e6d` 引入该 key 的理由。**

## 本分支自造 skew 的判据

| | 上游 master (`fbef303da5f`) | 本分支 HEAD |
|---|---|---|
| blind 查找 | 单 key，有 pin 时**从不 empty** | 版本化 key，case (3) → empty |
| t1/t2 join 绑定的 schema | 某一个 pin 的 `{c1}` | **LATEST `{c1,c2,c3}`**（没有任何 ref 要求过它） |
| 结果 | `1	1` | guard 抛 c2 |

LATEST 是**语句里没有任何引用点过名的第三种 schema**。这就是误报的制造机。

---

# Design

## 选定方案：**C1-a**（修 `getSnapshot(TableIf)` 的兜底），**不选 C1-b**

一句话：**blind 读在语句已经 pin 过该表时，永远返回「该表第一个被 pin 的 entry」，永不回退 LATEST。**
case (2) 与 case (3) 因此合并成同一条规则，`isSameTable` 循环从「数到 2 就放弃」变成「取第一个」。

```
default("") entry 存在        → 返回它                （case (1)，不动）
否则该表有 ≥1 个 pin          → 返回 pin 顺序中的第一个 （case (2)+(3) 合并）
否则                          → empty                （无 pin：MTMV/统计/无 ConnectContext 等）
```

配套：`snapshots` 由 `Maps.newHashMap()` 改为 `Maps.newLinkedHashMap()`，使「第一个」= **插入序** = 语句里第一个被
分析器绑定的该表引用。**顺序是本设计的载重语义，不是巧合**，必须在字段处注释钉死。

## 为什么是「第一个」而不是「任意一个 / 最后一个」

1. **等价于上游 master 语义**：master 的 `loadSnapshots` + `containsKey` 守卫就是 first-write-wins，其单 key
   entry 恒等于「第一个被绑定的引用的 pin」。C1-a = 恢复该行为（并保留本分支 scan 侧的 version-aware 改进）。
2. **求值时机不变性（决定性理由）**：因为 `getOutput()` 惰性（`AbstractPlan:91-93`），blind 读可能在只有 1 个 pin 时
   发生，也可能在有 N 个 pin 时发生。
   - 「第一个」：早求值 → 该表唯一 pin = 第一个；晚求值 → 仍是第一个。**答案与求值时机无关**。
   - 「最后一个」/「任意一个（HashMap 迭代序）」：早晚求值答案不同 ⇒ 引入不确定性。
     实测反例：`t@tag(t1) JOIN t@tag(t3)`，若取「最后一个」且晚求值，两个 relation 都绑 t3 的 `{c1,c2}`，
     t1 那侧的 scan（pinned `{c1}`）就会在 c2 上抛 ⇒ **打挂 `qt_sub_join_tag_with_tag_2`**。
3. **HashMap 迭代序不可依赖**：现状 `:272` 是 `HashMap`，「任意一个」= 按 hash 桶序，跨 JDK/容量变化不稳定，
   且无法写出能表达意图的单测（Rule 9）。

## 为什么这不是「把 fail-loud 换成静默错误」（本设计最重要的论证）

诚实拆解「两个版本 schema **不同**」时 C1-a 的行为。设第一个被 pin 的 ref 为 A、另一个为 B：

| 情形 | C1-a 行为 | 是否静默错 |
|---|---|---|
| **列集合不同**（B 缺 A 的某列，如 t3-first × t1-second） | tuple 绑 A 的 schema → B 的 scan 在 `pinMvccSnapshot:898-908` 用 **B 自己的** pinned schema 校验 → **L17 guard 抛** | ❌ 不静默：**fail-loud 由 guard 保住**（上游没有 guard，本分支有） |
| **B 比 A 宽**（t1-first × t3-second） | tuple 绑 A 的 `{c1}` ⊆ B 的 `{c1,c2}` → guard 放行 → B 读自己的 `{c1}` | ✅ 正确（列子集投影，本就是合法读法） |
| **列被 rename（field-id 不变）** | 按 field-id 解析成功 → 放行 | ✅ 与既有已签字语义一致（`PluginDrivenScanNodeMvccSchemaGuardTest.fieldIdStableRenameResolvesByIdNoThrow:88-97`，guard javadoc `:919-923`「a rename that keeps the id is fine」） |
| **列集合相同但 TYPE 不同**（如 iceberg int→long promotion） | tuple 绑 A 的类型，B 按同 field-id 读 → guard **不查类型** → 可能读到被绑错类型的值 | ⚠️ **不覆盖**。但：**上游同样不覆盖，今天的 LATEST 兜底也同样不覆盖**（LATEST 的类型同样被强加给两个 ref）。C1-a **不新增**该洞，也**不修**它。这正是 `[FU-mvcc-mixed-schema]` / `D-MVCC-VERSION-SCHEMA` 的范围。**如实记入「仍未证实」。** |

**⇒ C1-a 不是「用静默错误换 fail-loud」**：它把「LATEST 制造的**假** skew」删掉，把「真 skew」原封不动留给 L17 guard。
guard 一个字都不改，`assertBoundColumnsResolveInPinnedSchema` 的 7 个单测全部保持绿。

用一句可证伪的话概括本设计的语义：
> **blind 读的结果永远是语句里某个引用真实请求过的版本；guard 负责在该版本对另一个引用不成立时喊停。**
> 今天的实现违反前半句（LATEST 没人请求过），于是 guard 对着幽灵列报警。

## 为什么**不**选 C1-b（per-reference version-aware binding = `D-MVCC-VERSION-SCHEMA`）

C1-b 是真正的治本（Trino 式版本作用域 handle / 逐引用列句柄），但**不适合塞进这个 CI 修复 PR**：

- **结构性障碍**：`getSchemaCacheValue()` 无引用参数，且其调用点 `LogicalCatalogRelation.computeOutput:152-164`
  是**惰性**求值的（`AbstractPlan:91-93`）——版本必须**随对象走**（挂在 relation/handle 上），不能靠调用时的上下文
  或 ThreadLocal 传（惰性求值时窗口早已关闭）。这直接判死所有「小改」变体。
- **改动面**（静态清点，至少）：`ExternalTable.getFullSchema/getBaseSchema` 契约（对**所有**外表生效）、
  `PluginDrivenMvccExternalTable.getSchemaCacheValue`、`LogicalCatalogRelation.computeOutput`、`LogicalFileScan:71`
  （构造期就调 blind `initSelectedPartitions`）、`PruneFileScanPartition:79/88`、`PhysicalPlanTranslator:639/698`、
  `CheckPolicy:138`、`PartitionIncrementMaintainer:311/319`、`MetadataGenerator:749/2097`、
  `PartitionValuesTableValuedFunction:175`、MTMV 6 处 —— 即当前 **20 个 blind 调用点**逐个定性。
- **归属**：属 Nereids/plan 层重构，需 Nereids owner 参与 + 全量 external p0/p2 回归 + 多轮对抗 review。
  规模上是**独立 PR**（保守估计 2–4 人天设计+实现，外加 review/回归轮次），不是 CI 绿化的一步。
- **C1-a 不阻挡 C1-b**：C1-a 只收窄 blind 兜底，C1-b 落地时整个 blind 路径本就要被替换。零返工冲突。

**如实说明 C1-a 的能力边界**：它**不让**「同表两版本 + schema 真的不同」跑通 —— 那仍然抛（或按 field-id 解析）。
它只让「同表两版本 + schema 相同/子集」由**构造**跑通，而不是靠侥幸。`D-MVCC-VERSION-SCHEMA` 继续挂账。

## 被否决的方案

| 方案 | 否决理由 |
|---|---|
| blind 返回「**最后**一个 pin」（bind 期恰好=当前 relation 自己的 pin，看似"免费版本感知"） | 依赖「`getOutput()` 在下一个 relation 的 `loadSnapshots` 之前 force」——`AbstractPlan:91-93` 惰性求值**不保证**。晚求值时 `t1 JOIN t3` 两侧都绑 t3 的 `{c1,c2}` → t1 侧 guard 抛 → **打挂 `qt_sub_join_tag_with_tag_2`**。且是隐式时序耦合，任何未来在分析后期新增 pin 的代码都会静默改变查询结果。 |
| BindRelation 前后设「current binding version」作用域（ThreadLocal / StatementContext 字段） | 同上：惰性求值时作用域已关闭 → 退化回 blind → 同一 bug。且是往 fe-core 加**脚手架状态**（记忆铁律 B）。 |
| 在 `StatementContext` 里比较各 pin 的 schema，相同才返回 | `MvccSnapshot` 是通用接口无 schema；要 `instanceof PluginDrivenMvccSnapshot` ⇒ nereids 层反向依赖 datasource 实现，且「不同」时仍无解。 |
| 放宽 / 删除 L17 guard（issue 文档 C3） | 两位 reviewer 已推翻；会打挂 4 个现有单测、部分推翻 2026-07-13 已签字决策、并依赖「connector projection push-down 容忍请求快照中不存在的列」这一**至今无人验证**的前提。 |
| 改 `.out` | 三个 fixture 文件与 merge-base **逐字节相同**（已 md5 核实），`1	1` 是上游验证过的正确答案。 |

---

# Implementation Plan

**共 4 个文件：2 个产品文件（`StatementContext.java` 1 处字段 + 1 个方法体/javadoc；`MvccUtil.java` 仅 javadoc）、
1 个 javadoc（`MvccTableInfo.java`）、1 个测试文件。产品逻辑净减 3 行。**
`PluginDrivenScanNode`（guard）、`PluginDrivenMvccExternalTable`（`getSchemaCacheValue`）、`BindRelation`、
`versionKeyOf`、version-aware `getSnapshot(3-arg)` —— **一律不动**。

## 1. `fe/fe-core/src/main/java/org/apache/doris/nereids/StatementContext.java:272`

改前：
```java
    private final Map<MvccTableInfo, MvccSnapshot> snapshots = Maps.newHashMap();
```
改后（`Maps.newLinkedHashMap()` 是本文件既有写法，见 `:131`；无需改 import）：
```java
    // Insertion-ORDERED: the version-blind getSnapshot(TableIf) falls back to the FIRST snapshot pinned for a
    // table, so the iteration order is load-bearing (see that method's javadoc). Do NOT switch back to HashMap.
    private final Map<MvccTableInfo, MvccSnapshot> snapshots = Maps.newLinkedHashMap();
```

## 2. `StatementContext.java:1000-1034` —— 替换 javadoc + 方法体

改前（`:1000-1034`，逐字）：
```java
    /**
     * Obtain snapshot information of mvcc, version-blind. Used by the metadata/schema/partition readers
     * that do not thread the per-reference version. Resolution order:
     * (1) the default ("" version) entry if present — covers a plain/latest reference, and is the
     * deterministic choice when a statement pins both main and {@code @branch}/{@code @tag} of one table;
     * (2) else, if EXACTLY ONE snapshot is pinned for this table (ignoring version) — e.g. a standalone
     * {@code @branch}/{@code @tag}/FOR-TIME read — that lone entry, so those readers still see the pinned
     * snapshot; (3) else (two or more non-default versions pinned and no default, e.g. {@code t@tag('v1')}
     * joined with {@code t@tag('v2')}) the version is ambiguous here, so empty and the caller falls back to
     * latest. The scan path always resolves the exact per-reference snapshot via
     * {@link #getSnapshot(TableIf, Optional, Optional)} regardless.
     *
     * @param tableIf tableIf
     * @return MvccSnapshot
     */
    public Optional<MvccSnapshot> getSnapshot(TableIf tableIf) {
        if (!(tableIf instanceof MvccTable)) {
            return Optional.empty();
        }
        MvccTableInfo defaultKey = new MvccTableInfo(tableIf);
        MvccSnapshot defaultSnapshot = snapshots.get(defaultKey);
        if (defaultSnapshot != null) {
            return Optional.of(defaultSnapshot);
        }
        MvccSnapshot only = null;
        for (Map.Entry<MvccTableInfo, MvccSnapshot> entry : snapshots.entrySet()) {
            if (defaultKey.isSameTable(entry.getKey())) {
                if (only != null) {
                    return Optional.empty();
                }
                only = entry.getValue();
            }
        }
        return Optional.ofNullable(only);
    }
```
改后：
```java
    /**
     * Obtain snapshot information of mvcc, version-blind. Used by the metadata/schema/partition readers
     * that do not thread the per-reference version. Resolution order:
     * (1) the default ("" version) entry if present — covers a plain/latest reference, and is the
     * deterministic choice when a statement pins both main and {@code @branch}/{@code @tag} of one table;
     * (2) else the FIRST snapshot pinned for this table (ignoring version) — a standalone
     * {@code @branch}/{@code @tag}/FOR-TIME read has exactly one, and a statement pinning several (e.g.
     * {@code t@tag('v1')} joined with {@code t@tag('v2')}) binds the first reference's, which is master's
     * single-key first-write-wins behavior. Never fall back to LATEST while a pin exists: LATEST is a schema
     * NO reference asked for, and handing it to the readers manufactures a schema skew the
     * {@code PluginDrivenScanNode} L17 guard then reports on a column the query never referenced.
     * Order matters and is why {@code snapshots} is insertion-ordered: a relation's output is computed
     * LAZILY ({@code AbstractPlan.logicalPropertiesSupplier}), so this may be called before OR after later
     * references pin theirs — "first" is the only choice that answers identically either way.
     * A genuine cross-version schema skew is still caught per-reference at scan time by
     * {@code PluginDrivenScanNode.assertBoundColumnsResolveInPinnedSchema}; making the binding version-aware
     * end-to-end is tracked as D-MVCC-VERSION-SCHEMA. The scan path always resolves the exact per-reference
     * snapshot via {@link #getSnapshot(TableIf, Optional, Optional)} regardless.
     *
     * @param tableIf tableIf
     * @return MvccSnapshot
     */
    public Optional<MvccSnapshot> getSnapshot(TableIf tableIf) {
        if (!(tableIf instanceof MvccTable)) {
            return Optional.empty();
        }
        MvccTableInfo defaultKey = new MvccTableInfo(tableIf);
        MvccSnapshot defaultSnapshot = snapshots.get(defaultKey);
        if (defaultSnapshot != null) {
            return Optional.of(defaultSnapshot);
        }
        for (Map.Entry<MvccTableInfo, MvccSnapshot> entry : snapshots.entrySet()) {
            if (defaultKey.isSameTable(entry.getKey())) {
                return Optional.of(entry.getValue());
            }
        }
        return Optional.empty();
    }
```
（净删 `MvccSnapshot only` 局部变量与 `if (only != null) return Optional.empty();` 早退；`Map.Entry` import 已在用。）

## 3. `fe/fe-core/src/main/java/org/apache/doris/datasource/mvcc/MvccTableInfo.java:72-76` —— 仅 javadoc

改前：
```java
    /**
     * Whether {@code other} refers to the same (catalog, db, table) as this, IGNORING the version
     * selector. Used by the version-blind {@code StatementContext.getSnapshot(TableIf)} to recognise a
     * lone pinned snapshot for a table when no default ("") entry exists (e.g. a standalone @branch read).
     */
```
改后：
```java
    /**
     * Whether {@code other} refers to the same (catalog, db, table) as this, IGNORING the version
     * selector. Used by the version-blind {@code StatementContext.getSnapshot(TableIf)} to find the FIRST
     * pinned snapshot for a table when no default ("") entry exists (e.g. a standalone @branch read, or a
     * self-join of two @tag references).
     */
```

## 4. `fe/fe-core/src/main/java/org/apache/doris/datasource/mvcc/MvccUtil.java:51-53` —— 仅 javadoc

改前（末句）：
```java
     * mixing main and {@code @branch} of one table reads each at its own snapshot. The version-blind
     * {@link #getSnapshotFromContext(TableIf)} cannot disambiguate once more than one snapshot is pinned.
```
改后：
```java
     * mixing main and {@code @branch} of one table reads each at its own snapshot. The version-blind
     * {@link #getSnapshotFromContext(TableIf)} cannot disambiguate once more than one snapshot is pinned
     * (it binds the first pinned reference), so every SCAN must use this overload.
```

## 5. 测试改动 —— 见 Test Plan（`StatementContextMvccSnapshotTest` 2 改 2 增、`PluginDrivenMvccExternalTableTest` 1 增）

## 施工顺序

1. 先改测试到新语义（2 个断言翻转）→ 跑 → **必须红**（证明它们真的在守这条语义，Rule 9）。
2. 改 `:272` + `:1015-1034` → 跑 → 全绿。
3. 补 3 个新单测 → 跑 → 绿；对每个新测试做 mutation（把 `return Optional.of(entry.getValue())` 换回
   `return Optional.empty()`；把「第一个」换成「最后一个」）→ **必须红**。
4. `checkstyle`（`fe-core` 扫 test 源）。

---

# Risk Analysis

## A. 会打挂哪些**现有单测**？（已逐个读过）

**`fe/fe-core/src/test/java/org/apache/doris/nereids/StatementContextMvccSnapshotTest.java` —— 2 个方法必挂，必须随本改动改写：**

| 类#方法 | 行 | 现断言 | C1-a 后 | 处置 |
|---|---|---|---|---|
| `StatementContextMvccSnapshotTest#twoBranchesWithoutMainAreAmbiguousForVersionBlindReader` | `:138-139` `assertFalse(ctx.getSnapshot(table).isPresent())` | **红**（现在返回 snap1） | 改写为 `assertSame(snap1, ...)` + 更名 | **本设计有意翻转的语义**，见下 |
| `StatementContextMvccSnapshotTest#forVersionAndForTimeSelectorsKeyDistinctly` | `:161-162` `assertFalse(...)` | **红**（返回 versionSnap） | 末断言改 `assertSame(versionSnap, ...)` | 同上 |

这两条**正是 C1-a 要推翻的决策**，其注释 `:136-137`（"rather than returning an arbitrary branch, the pre-fix bug"）
必须一并改掉，且**必须在 commit message 里点名**：本改动**有意推翻**
`iceberg-branch-mvcc-and-static-partition-overwrite-fixes.md:37-40` 里「不返回任意版本」的保守选择，理由是
①那不是它所修 bug ① 的必要条件（bug ① 走 case (1)）②它制造了 LATEST 假 skew ③「第一个」不是"任意"，
是上游 master 语义且求值时机不变。

**已读并确认 NOT 受影响：**

| 类 | 为什么不受影响 |
|---|---|
| `StatementContextMvccSnapshotTest#mainAndBranchOfSameTablePinSeparateSnapshots`（`:70-98`） | 断言 `:96` 走 **case (1)**（default 存在），不动 |
| `StatementContextMvccSnapshotTest#standaloneBranchResolvesForVersionBlindReader`（`:101-117`） | 单 entry：旧「lone」与新「first」同解 |
| `StatementContextMvccSnapshotTest#nonMvccTableNeverPinsOrResolves`（`:166-173`） | 非 MvccTable 早退，不动 |
| `PluginDrivenScanNodeMvccSchemaGuardTest`（**全部 7 个**：`fieldIdRenumberBetweenBoundAndScannedVersionThrows:52`、`columnAddedAfterScannedVersionThrows:66`、`nameMissWhenNoFieldIdThrows:77`、`fieldIdStableRenameResolvesByIdNoThrow:88`、`subsetProjectionAllResolvedNoThrow:99`、`nameMatchWhenNoFieldIdNoThrow:109`、`nullPinnedSchemaIsNoOp:119`） | 全是对 `assertBoundColumnsResolveInPinnedSchema` 的**纯 helper 测试**，本设计**一个字都不改 guard**。⇒ **C3 方案会打挂的那 4 个，C1-a 一个都不碰**（这正是 C1 相对 C3 的核心优势） |
| `PluginDrivenMvccExternalTableTest`（`testGetSchemaCacheValue*` `:847-890`、`withContextSnapshot` `:1181-1193`） | 全部用 `stmtCtx.setSnapshot(new MvccTableInfo(table), snapshot)` = **default key** → case (1)，不动 |
| `PluginDrivenScanNodeSysTablePinTest` | 不碰 `getSnapshot(TableIf)`（sys 表走 `resolveSysTableSnapshotPin`） |
| MTMV 全线（`MTMVTask:167/333/492` 用 `new MvccTableInfo(mvccTable)` 单 default key） | 恒 case (1)；且只有一个 entry 时「first」= 它 |

## B. 会不会影响那 550 个通过的用例？

**先界定命中面**：只有**同一条语句里、同一张表、≥2 个不同版本选择器、且无 default（无裸引用）** 才进 case (3)。
全库扫描（`grep` 口径，`rg` 不可信见 issue 文档 §12）后，p0 external 里命中 case (3) 的只有两处：

1. **`iceberg_query_tag_branch.groovy`** 的 `sub_join_*`（`:111-152`）—— 本次要修的用例所在。
   - `qt_sub_join_branch_with_branch_1..5`（`:111-130`）**在失败点之前，本次运行已跑过并通过**（suite 遇错即 abort，
     而失败在 `:132` 的 `qt_sub_join_tag_with_tag_1`）⇒ **直接证据**：case (3) + 三个 schema 恰好相同
     （branch 的 pinned schema = 表当前 schema `{c1,c2,c3}`，见 `.out:2-3` `-- !branch_1 -- 1 \N \N` 三列 = LATEST）
     时今天通过。C1-a 后：first-pinned = b1 的 pin，其 pinnedSchema 与 LATEST **同列同 id** ⇒ tuple 不变 ⇒
     **逐字节同结果**。
   - `qt_tag_1..13` / `qt_version_1..13`（`:70-97`）单引用 → **case (2)**，「lone」≡「first」⇒ 不变。
2. **`iceberg_tag_retention_and_consistency.groovy:266-271` `qt_inter_join`**（`test_tag_branch_interaction@tag(baseline)`
   full outer join `@branch(feature_branch)`）—— **该 suite 不在 13 个失败里**。建表 `:243`
   `create table (id int, name string, source string)` 后**无 schema change** ⇒ tag/branch/LATEST 三者同 schema
   ⇒ C1-a 后 tuple 不变 ⇒ 不变。

**逐个复核 `iceberg_query_tag_branch` 里 case (3) 的 join（first-pinned 规则下，两种求值时机都算过）：**

| qt | 引用 | first-pinned schema | 另一侧 scan 的 pinned schema | guard |
|---|---|---|---|---|
| `sub_join_branch_with_branch_1..5` | b1×b2 / b1×b3 / b2×b3 / b1×b1 / b3×b3 | `{c1,c2,c3}` | `{c1,c2,c3}` | 放行 ✅ |
| `sub_join_tag_with_tag_1`（**目标**） | t1×t2 | `{c1}` | t2 = `{c1}` | 放行 ✅ → `1	1` |
| `sub_join_tag_with_tag_2` | t1×t3 | `{c1}` | t3 = `{c1,c2}` ⊇ `{c1}` | 放行 ✅ |
| `sub_join_tag_with_tag_3` | t2×t3 | `{c1}` | t3 ⊇ | 放行 ✅ |
| `sub_join_tag_with_tag_4` | t1×t1 | 同 key → 单 entry，case (2) | — | 今天已放行 |
| `sub_join_tag_with_branch_1` | t1×b1 | `{c1}` | b1 = `{c1,c2,c3}` ⊇ | 放行 ✅ |
| `sub_join_tag_with_branch_2/3` | t3×b3 | `{c1,c2}` | b3 = `{c1,c2,c3}` ⊇ | 放行 ✅ |

**⚠️ 诚实标注**：`sub_join_tag_with_tag_2` 及其后的每一条**在本分支从未执行过**（suite 在 `:132` abort）。
上表是静态推演，**不是**「修完必绿」的证明。见「仍未证实」。

**其余 external p0 suite 全部不进 case (3)**：`iceberg_branch_complex_queries.groovy:81-83`（`subquery_in`/
`subquery_exists`/`subquery_scalar`）是「裸引用 + `@branch`」⇒ **有 default entry** ⇒ case (1)，行为不变
（该 suite 本次因 **触发器 1（row-id）** 失败，与本设计无关，由 T2/T3 处理）；
`iceberg_branch_partition_operations.groovy` 每条查询只引用一个 branch ⇒ case (2)；
`test_iceberg_time_travel` / `paimon_time_travel` 均为**单表单 `FOR TIME/VERSION AS OF`** ⇒ **case (2)，逐字不变**。

## C. 分区路径的行为变化（本设计唯一有实质变化、且**未被任何 p0 用例覆盖**的面）

`LogicalFileScan:71` 在**构造期**就调 `table.initSelectedPartitions(MvccUtil.getSnapshotFromContext(table))`（blind），
`PruneFileScanPartition:79/88` 亦 blind。对一张**分区表**的 case (3) 引用：

- **今天**：blind → empty → `ExternalTable.initSelectedPartitions:439-447` 用 `getNameToPartitionItems(empty)`
  = **LATEST 分区全集**（对一个 tag 读来说本就可疑）；
- **C1-a**：blind → first-pinned（point-in-time pin 按 `PluginDrivenMvccExternalTable.java:399-406` 携带
  **空**分区 map）→ `SelectedPartitions(0, {}, false)`。

**为什么低风险**：`PluginDrivenScanNode.resolveRequiredPartitions:263-277` 有**显式契约**——
`selectedPartitions.isEmpty() && totalPartitionNum == 0`（"an MVCC time-travel pin ... deliberately carries an empty
partition-item map and defers partition resolution to the connector's predicate pushdown"）→ 返回 `null` = **scan-all**，
交给连接器谓词下推。这正是**今天每一个 standalone tag/time-travel 读（case (2)）已经走的路径**，非新路径。
`selectedPartitionNum` 也不受影响：`PluginDrivenScanNode:330-333` 优先采用连接器 SDK 的真实 distinct 计数
（`IcebergScanPlanProvider:297-309`），非 Nereids 剪枝数。

**但**：p0 里**没有**「分区表 + 同表两版本引用」的用例（`iceberg_branch_partition_operations` 每条只引用一个 branch），
⇒ 该面**只有静态论证，无实测覆盖**。记入「仍未证实」。

## D. 其余 20 个 blind 调用点

`PhysicalPlanTranslator:639/698`、`CheckPolicy:138`、`PartitionIncrementMaintainer:311/319`、
`MetadataGenerator:749/2097`、`PartitionValuesTableValuedFunction:175`、`UpdateMvByPartitionCommand:341`、
MTMV 6 处、`PreloadExternalMetadata:113`：全部只在「无 pin（→ 仍 empty）」或「default pin（→ case (1)）」下运行；
只有当语句真的 pin 了 ≥2 个非 default 版本时它们的返回值才从 `empty` 变成 first-pinned ——
那**恰恰是它们本来就该看到的**（今天它们看 LATEST，是语句里没人请求过的版本）。
`PreloadExternalMetadata:107/113` 只对 `shouldPreloadLatestSnapshot()` 的 latest-only relation 打 default pin，
且整个 preload 在「无内表需要 plan 锁」时被 `getSkipReason:88-92` 跳过 ⇒ 纯外表查询根本不进。

## E. 会不会让今天**通过**的查询开始失败？

存在一个**理论**回归形状：first-pinned 的 schema **不是** LATEST 的子集，且另一个 ref 解析不了它 ——
需要「**删列** + 同表两版本引用」同时成立（如 A=`{c1,c2}`、B=`{c1}`、LATEST=`{c1}`：今天 tuple=LATEST `{c1}` 两边都过；
C1-a 后 tuple=A 的 `{c1,c2}` → B 侧 guard 抛）。

- 本次 CI 的 550 个通过用例里**不存在**该形状（p0 的 case (3) 只有上文两处，均无删列）。
- 语义上这是 **guard 的设计意图**（真 skew fail-loud，2026-07-13 已签字），不是"新坑"：该查询在上游 master 上会
  **静默**让两个 ref 都读 A 的快照（读错数据），今天在本分支上会因 LATEST 兜底而**恰好**蒙对。
- **必须向用户点名**：这是 C1-a 唯一「行为可能变差（从蒙对到报错）」的形状，且它无法用 C1-a 修好，只有 C1-b 能。

---

# Test Plan

## Unit Tests

### 1. `org.apache.doris.nereids.StatementContextMvccSnapshotTest`（同文件，2 改 2 增）

| 方法 | 类型 | 断言意图（Rule 9：业务语义变了必须挂） |
|---|---|---|
| `twoTagsWithoutMainBindTheFirstPinnedSnapshot`（**改写自** `twoBranchesWithoutMainAreAmbiguousForVersionBlindReader:120-140`） | 改 | pin `b1` 再 pin `b2`（无 default）→ `assertSame(snap1, ctx.getSnapshot(table).orElse(null))`，消息：*"version-blind readers must bind the FIRST pinned reference; falling back to latest (empty) manufactures a schema no reference asked for"*。**保留**原有的 version-aware 两条 `assertSame(snap1/snap2, getSnapshot(table, .., b1/b2))`（证明 scan 侧逐引用解析未被本改动破坏）。**MUTATION**：恢复 `return Optional.empty()` → 红；返回 snap2 → 红。 |
| `versionBlindAnswerIsStableAsLaterPinsArrive`（**新增**） | 增 | **本设计的核心不变式**。pin `b1` → `assertSame(snap1, ctx.getSnapshot(table).orElse(null))`；**再** pin `b2` → **再次** `assertSame(snap1, ...)`。注释写明 why：`AbstractPlan.logicalPropertiesSupplier` 惰性 ⇒ 同一 relation 的 blind 读可能发生在后续 pin 之前**或之后**，答案必须相同。**MUTATION**：改成「返回最后一个 pin」→ 第二个断言红；改回 empty 兜底 → 第二个断言红。（这条测试是"最后一个/任意一个"方案的处刑架，也是 `LinkedHashMap` 的守门人。） |
| `defaultPinWinsEvenWhenPinnedAfterATagReference`（**新增**） | 增 | **先** pin `@branch(b1)`、**后** pin default（main）→ `assertSame(mainSnap, ctx.getSnapshot(table).orElse(null))`。钉死 case (1) 优先级**不是**插入序的副产物，而是有意为之（MTMV 与 `t JOIN t@branch(b)` 依赖它）。**MUTATION**：删掉 `:1019-1023` 的 default 早退 → 返回 branchSnap → 红。 |
| `forVersionAndForTimeSelectorsKeyDistinctly:143-163` | 改 | 仅末条 `:161-162` 由 `assertFalse(isPresent())` 改为 `assertSame(versionSnap, ctx.getSnapshot(table).orElse(null))`（"FOR VERSION 先 pin ⇒ blind 绑它"）。**前 4 条 version-aware 断言保持不动**（key 区分度不受影响）。 |
| `mainAndBranchOfSameTablePinSeparateSnapshots:70-98` / `standaloneBranchResolvesForVersionBlindReader:101-117` / `nonMvccTableNeverPinsOrResolves:166-173` | 不动 | 已验证在新语义下保持绿。 |

类 javadoc `:37-45` 末句（"…and the version-blind fallback the metadata readers rely on"）追加一句说明兜底规则由
"lone/ambiguous→empty" 改为 "first-pinned"，并指向 D-MVCC-VERSION-SCHEMA。

### 2. `org.apache.doris.datasource.PluginDrivenMvccExternalTableTest`（1 增）

| 方法 | 断言意图 |
|---|---|
| `testGetSchemaCacheValueBindsFirstPinnedSchemaWhenTwoVersionsPinned`（**新增**，放在 `:845` "getSchemaCacheValue: schema-at-snapshot override" 段内，紧随 `testGetSchemaCacheValueFallsBackToLatestWhenNoPin:883`） | **这是缺陷咬人的那一层的直接回归**。用 `Fixture.timeTravel()`；建 `ConnectContext`+`StatementContext` 并 `setThreadLocalInfo()`（镜像 `withContextSnapshot:1181-1193` 的 setup，但改用 `loadSnapshots`）；`stmtCtx.loadSnapshots(f.table, Optional.of(TableSnapshot.versionOf("5")), Optional.empty())` 然后 `...versionOf("6")...`（两个不同 `versionKeyOf` ⇒ 两个 entry、无 default）；取 `firstPin = (PluginDrivenMvccSnapshot) stmtCtx.getSnapshot(f.table, Optional.of(versionOf("5")), Optional.empty()).get()`；断言 ①`assertSame(firstPin.getPinnedSchema(), f.table.getSchemaCacheValue().get())` ②`assertNotSame(f.latestCacheValue, ...)` + 消息 *"two pinned versions and no default must bind the FIRST pinned reference's schema, never LATEST — LATEST is a schema no reference asked for and makes the L17 guard fire on a column the query never referenced"*。**MUTATION**：恢复 "ambiguous → empty" → 返回 `f.latestCacheValue` → 红（**这正是 CI 996541 的失败机制**）；改成"最后一个" → `assertSame` 红。 |

> 说明（Rule 2/3，**不新增重复测试**）：任务要求的「同表两 tag、schema **不同** → 行为是什么」的语义，其**另一半**
> （guard 侧）已被现有测试完整编码，**不再复制**：
> - 窄 schema 先 pin、宽版本后扫 → 放行 = `PluginDrivenScanNodeMvccSchemaGuardTest#subsetProjectionAllResolvedNoThrow:99-106`；
> - 宽 schema 先 pin、窄版本后扫 → **fail-loud** = `#columnAddedAfterScannedVersionThrows:66-75`。
>
> 两者合起来就是 C1-a 下 schema 不同时的完整语义。再写一个「两个 tag」的变体只是把同样的入参换个名字，
> 属恒等复制，不增加可失败面。

## E2E Tests

**不新增 e2e。理由（三条，缺一不可）：**

1. **闸门已存在且就是本次的失败用例**：`external_table_p0/iceberg/iceberg_query_tag_branch`
   `qt_sub_join_tag_with_tag_1`（`:132-135`）— 由红转绿即验收。其 fixture `run11.sql:16` 的 `add column c2`
   正是制造 tag/LATEST schema 分歧的那一步，`.out:264-265` 的 `1	1` 是上游验证过的答案。**`.out` 不改**
   （三个文件与 merge-base 逐字节相同，已 md5 核实）。
2. 同 suite 的 `sub_join_tag_with_tag_2/3/4` + `sub_join_tag_with_branch_1/2/3` + `sub_with_tag_1..4`
   （本分支**从未执行过**，因 `:132` abort）会被本修复解锁 —— 它们是**免费的**额外覆盖，含 tag×branch、
   tag×tag 跨 schema 版本的组合。无需新写。
3. 新增 e2e 需要新 fixture（`run11.sql` 与上游逐字节相同，**不应改**）+ 新 docker 预置数据，成本远高于收益，
   且本 PR 的约束是不改 fixture。

**验收命令（本 session 不执行——另一 session 正在跑 maven）**：
`external_table_p0/iceberg/iceberg_query_tag_branch`、`iceberg_tag_retention_and_consistency`（case (3) 的另一处）、
`test_iceberg_time_travel`、`paimon_time_travel`、`iceberg_branch_complex_queries`（case (1) 回归面）、
`iceberg_branch_partition_operations`（分区 + branch）。

---

# 仍未证实

1. **「修完就绿」未证**。`iceberg_query_tag_branch` 在 `:132` 的第一个 tag-join 就 abort，
   `sub_join_tag_with_tag_2/3/4`、`sub_join_tag_with_branch_1/2/3`、`sub_with_tag_1..4` 以及第二轮
   （`num_files_in_batch_mode=1024`）的全部断言**在本分支从未执行过**。本设计只证明「`qt_sub_join_tag_with_tag_1`
   的抛出机制被消除」+「上表推演的 guard 放行」，**不证明**这些被解锁的断言的数据结果。以真实 CI 为准。
   （继承 issue 文档 §B「无论 C1/C2/C3，没人能静态证明这 3 个 iceberg 用例修完变绿」。）
2. **类型级 skew 不覆盖**：同表两版本、列集合相同但**类型**不同（如 iceberg int→long promotion）时，L17 guard
   只查 field-id/名字**不查类型**，C1-a 会把 first-pinned 版本的类型强加给另一个 ref。**上游 master 与今天的
   LATEST 兜底同样不覆盖** ⇒ C1-a 不新增该洞、也不修它。归属 `D-MVCC-VERSION-SCHEMA` /
   `[FU-mvcc-mixed-schema]`（`iceberg-branch-mvcc-and-static-partition-overwrite-fixes.md:112-115`）。**未验证是否可构造出实际读到错值的用例。**
3. **删列形状未证**：「同表两版本引用 + 某版本的列在 LATEST 里已被删除」时，C1-a 可能把今天**恰好蒙对**的查询
   变成 guard 报错（Risk §E）。已确认 p0 的 550 通过用例里**不存在**该形状，但**未**扫 p2 / 其他流水线。
4. **分区表 + 同表两版本引用**（Risk §C）**无任何用例覆盖**。「空分区宇宙 → scan-all」的论证依据是
   `PluginDrivenScanNode.resolveRequiredPartitions:263-277` 的显式契约 + standalone tag 读（case (2)）已在跑同一路径，
   **属静态论证，未实测**。
5. **「第一个被 pin 的 = SQL 里最左的引用」未证**：本设计只依赖「对给定 plan 稳定」+「与 `getOutput()` 求值时机无关」，
   **不依赖**它等于最左引用。分析器的 relation 绑定顺序未逐个 rule 核实（也不需要）。若将来有 rule 在分析后期
   新增 pin，"first" 语义仍成立（新 pin 排在后面）。
6. **`.out` 的 `1	1` 在 C1-a 下由构造得出，但数据侧未实测**：scan 侧 pin 一直是 version-aware 的（`:874-875`，
   本设计不动），故 t1 读 1 行、t2 读 2 行、join 得 `1	1` —— 与 `.out` 一致。这是推理，非运行结果。
7. **未证实 `qt_sub_join_branch_with_branch_1..5` 在本次运行中「通过」而非「未执行」** 的唯一依据是
   「runner 遇错 abort 整个 suite」+ 它们在 `.groovy` 里位于 `:132` 之前。**未去 buildlog 逐行核对**。
8. **本 session 未编译、未跑任何测试**（另一 session 正占用 maven / 同工作树）。所有行号、语义、
   md5 与 git 祖先关系均已对 HEAD 核实；**编译性与测试结果未验证**。
9. 继承 issue 文档 §B 的未证条目：C3 依赖的「connector projection push-down 能容忍请求快照中不存在的列」
   **至今无人验证** —— 这也是本设计不选 C3、且不把 C1-a 建成「放宽 guard」的实质理由之一。
