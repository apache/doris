# Problem

CI 996541 中 2 个 e2e 用例失败，报错均为：

```
Failed to pin MVCC snapshot for plugin-driven scan
```

| 用例 | 断言点 | 文件 |
|---|---|---|
| `test_iceberg_time_travel` | `qt_q4` | `regression-test/suites/external_table_p0/iceberg/test_iceberg_time_travel.groovy` |
| `iceberg_branch_complex_queries` | `qt_order_limit` | `regression-test/suites/external_table_p0/iceberg/branch_tag/iceberg_branch_complex_queries.groovy` |

两个用例的共同形状：**一个带 MVCC pin 的引用（`FOR TIME AS OF` / `@branch`）+ 一个 `ORDER BY ... LIMIT`**。

`fe.audit.log` 的 2×2 阶乘证据（继承自 issue 文档 §B 触发器 1，本轮未重跑 audit log）：

| | 无 `order by ... limit` | 有 `order by ... limit` |
|---|---|---|
| **无 pin** | EOF | EOF |
| **有 pin**（`FOR TIME AS OF`） | EOF | **ERR** |

即：pin 与 lazy-materialization **单独都不炸，同时出现才炸** —— 指向二者的交点。

本条属于 issue 文档 `plan-doc/tasks/ci-996541-failure-analysis.md` §B（L17 guard `assertBoundColumnsResolveInPinnedSchema` 的 3 种误报）中的**触发器 1**。同一函数的触发器 2（sys-table pin，`paimon_system_table`）由另一任务处理，本设计**不碰**。

背景（继承 issue 文档，已核实）：`assertBoundColumnsResolveInPinnedSchema` 在 master **不存在**，由本分支 commit `22516845ca3`（2026-07-13，"[fix](catalog) fail-loud on same-table multi-version schema skew (L17)"）纯新增。**本分支自引入的回归**，不是上游行为。本次 CI 该 guard 共触发 4 次，**4/4 全是误报，0 真阳性**。

---

# Root Cause

## 事实链（每一环均已对照 HEAD 核实）

**1. `LazyMaterializeTopN` 注入合成 row-id 列，`colUniqueId = Integer.MAX_VALUE`**

`fe/fe-core/src/main/java/org/apache/doris/nereids/processor/post/materialize/LazyMaterializeTopN.java:178-180`：

```java
Column rowIdCol = new Column(Column.GLOBAL_ROWID_COL + catalogRelation.getTable().getName(),
        Type.STRING, false, AggregateType.REPLACE, false,
        catalogRelation.getTable().getName() + ".global_row_id", false, Integer.MAX_VALUE);
```

走的是 8-arg ctor `fe/fe-catalog/src/main/java/org/apache/doris/catalog/Column.java:272-276`，最后一个形参就是 `int colUniqueId`：

```java
public Column(String name, Type type, boolean isKey, AggregateType aggregateType, boolean isAllowNull,
        String comment, boolean visible, int colUniqueId) {
```

> 核实修正：issue 文档写 `LazyMaterializeTopN.java:177-180`，HEAD 实际是 **178-180**（177 是 `CatalogRelation catalogRelation = ...`）。`Column.java:272-276` 与文档一致。
>
> 同文件 `:190-192` 还有第二个注入点（`PhysicalTVFRelation` 分支），同样是 `Column.GLOBAL_ROWID_COL + <name>` 前缀 + `Integer.MAX_VALUE`。本设计的前缀判断对两个注入点都生效。

**2. 常量与命名形状**

`fe/fe-catalog/src/main/java/org/apache/doris/catalog/Column.java:59`：

```java
public static final String GLOBAL_ROWID_COL = "__DORIS_GLOBAL_ROWID_COL__";
```

> ✅ 核实第 1 点（任务要求）：常量名确为 `Column.GLOBAL_ROWID_COL`，值为 `__DORIS_GLOBAL_ROWID_COL__`。注入时是 **前缀 + 表名/函数名**（`GLOBAL_ROWID_COL + catalogRelation.getTable().getName()`），故**必须用 `startsWith` 而非 `equals`**。

**3. guard 的 key 选择走 field-id 分支**

`fe/fe-core/src/main/java/org/apache/doris/datasource/PluginDrivenScanNode.java:936-939`：

```java
        for (Column bound : boundColumns) {
            boolean resolved = bound.getUniqueId() >= 0
                    ? pinnedFieldIds.contains(bound.getUniqueId())
                    : pinnedNames.contains(bound.getName().toLowerCase());
```

> ✅ 核实第 3 点（任务要求）：`Integer.MAX_VALUE = 2147483647 >= 0` → 三元表达式取 **true 分支** → `pinnedFieldIds.contains(Integer.MAX_VALUE)`。
>
> 而 `pinnedFieldIds` 由 `:932-935` 从 `pinnedSchema.getSchema()` 的真实表列 `getUniqueId()` 填充。`Column.COLUMN_UNIQUE_ID_INIT_VALUE = -1`（`Column.java:73`），iceberg 填真实 field-id（小整数），paimon 顶层无 field-id（-1）。全仓 grep `setUniqueId(Integer.MAX_VALUE)` **零命中**，`PluginDrivenScanNode` 内 `Integer.MAX_VALUE` 的 3 处命中（`:1425-1427`）全是 split 估算的封顶，与 uniqueId 无关。
>
> 结论：**没有任何 pinned schema 会包含 `Integer.MAX_VALUE` 这个 field-id**，`contains` 必然 false → `resolved = false` → `:940-945` 无条件抛 `UserException`。

**4. 异常被包成不透明的 RuntimeException**

guard 抛的 `UserException` 沿 `pinMvccSnapshot`（调用点 `:905-907`）上抛，被外层包成 `RuntimeException("Failed to pin MVCC snapshot for plugin-driven scan")` —— 这就是 CI 里看到的文案，真因（"Reading the same table at multiple versions..."）被吞掉。

## 为什么这是误报（零真阳性）

guard 的契约写在它自己的 javadoc（`:918-924`）里：`boundColumns` 是 **projected tuple-slot columns**，要验证它们能在 **本引用实际扫描的那个版本的 schema** 里解析出来，否则 BE 会 field-id/name 错配。

但 `__DORIS_GLOBAL_ROWID_COL__*` **根本不是表列**：它由 connector reader 合成，按构造就不该出现在任何 pinned schema 里。拿它去比 pinned schema 是**范畴错误** —— guard 在问一个对合成列无意义的问题。

`PluginDrivenScanNode` **自己已经在另外两处认定它是合成列并排除**：

- `:557-560` `classifyColumn()`：
  ```java
        String name = slot.getColumn().getName();
        if (name.startsWith(Column.GLOBAL_ROWID_COL)) {
            return TColumnCategory.SYNTHESIZED;
        }
  ```
  javadoc `:547-549` 明说它是 "the engine-wide lazy-materialization row-id (injected by `LazyMaterializeTopN`) ... classified here as `SYNTHESIZED`"，目的正是"keep the synthesized / generated special columns out of the file-read set so they are materialized by the connector reader rather than read from a data file **where they do not exist**"。

- `:1017-1022` `hasTopnLazyMaterializeSlot()`：
  ```java
        for (SlotDescriptor slot : slots) {
            Column col = slot.getColumn();
            if (col != null && col.getName().startsWith(Column.GLOBAL_ROWID_COL)) {
                return true;
            }
  ```

**guard 是第三处，漏了这个排除。** 根因就是这个遗漏 —— 不是 `LazyMaterializeTopN` 的问题（`Integer.MAX_VALUE` 是既有上游行为），也不是 pin 的问题。

---

# Design

**在 guard 的循环首句排除合成 row-id 列，与既有两处排除写法逐字一致。**

## 为什么放在 static helper 内（`:936`）而不是收集点（`:900-903`）

调用点 `:899-904` 从 `desc.getSlots()` 收集 `boundColumns`。理论上可以在那里过滤。选择放在 helper 内，理由：

1. **可测性**：`assertBoundColumnsResolveInPinnedSchema` 是 package-private static，`PluginDrivenScanNodeMvccSchemaGuardTest` 直接调它（不构造 scan node）。排除逻辑放在 helper 内才能被单测直接覆盖；放在收集点则本次必须补的 `rowIdColumnIsExcludedNoThrow` 无法触达该逻辑（要求构造 `TupleDescriptor` + scan node，与该测试类"takes plain Columns + a SchemaCacheValue so it is exercised without constructing a scan node"的既定风格冲突）。
2. **契约归属**：「合成列不参与 pinned-schema 解析」是 guard 自身语义的一部分，属于 helper 的契约，不是调用点的过滤职责。
3. 与 issue 文档定稿位置一致（`:936` 循环首句）。

## 架构铁律核对

- **fe-core 禁 source-specific 代码**：✅ `Column.GLOBAL_ROWID_COL` 是 `fe-catalog` 的**通用引擎常量**，由通用 optimizer rule `LazyMaterializeTopN` 注入，与连接器名/类型无关。既有 `:559` javadoc 明确称其为 "a generic Doris mechanism"，并把 connector-specific 的分类委派给 `ConnectorScanPlanProvider#classifyColumn`。本改动**不引入任何连接器分支**，不需要新增 `supports*()` 能力位。
- **fe-core 不解析连接器属性**：✅ 不涉及属性。
- **fe-core 源只出不进**：本改动是**修本分支自己引入的 guard（`22516845ca3`）**，属任务书明示的豁免；且净增 6 行（1 行逻辑 + 5 行注释），不搬迁任何逻辑。
- **Rule 3 最小改动**：不动 `LazyMaterializeTopN`、不动 `Column`、不动调用点、不碰触发器 2 的代码路径。

## 备选方案与否决理由

| 方案 | 否决理由 |
|---|---|
| 改 `LazyMaterializeTopN` 不发 `Integer.MAX_VALUE`（改成 -1） | 上游既有行为，`Integer.MAX_VALUE` 被 BE/其他路径依赖，改动面远超本 bug；违反 Rule 3。 |
| guard 对 `uniqueId == Integer.MAX_VALUE` 特判 | 按**魔数**而非**语义**排除。若将来 row-id 换 id，或别处出现 MAX_VALUE，行为漂移。且与既有两处（按名字前缀）不一致，违反 Rule 11。 |
| 让 guard 只按名字匹配（去掉 field-id 分支） | 会打挂 `fieldIdRenumberBetweenBoundAndScannedVersionThrows`，且丢失 field-id renumber 的真阳性检测能力。 |
| 在收集点 `:901` 过滤 | 见上，牺牲可测性；且盲区会以"下一个直接调 helper 的调用方"形式复发。 |

---

# Implementation Plan

## 变更 1 — `PluginDrivenScanNode.java:936` 循环首句加排除

**文件**：`/mnt/disk1/yy/git/wt-catalog-spi/fe/fe-core/src/main/java/org/apache/doris/datasource/PluginDrivenScanNode.java`

**当前 `:936-939`**：

```java
        for (Column bound : boundColumns) {
            boolean resolved = bound.getUniqueId() >= 0
                    ? pinnedFieldIds.contains(bound.getUniqueId())
                    : pinnedNames.contains(bound.getName().toLowerCase());
```

**改为**：

```java
        for (Column bound : boundColumns) {
            // The engine-wide lazy-materialization row-id (LazyMaterializeTopN injects it with
            // colUniqueId = Integer.MAX_VALUE) is synthesized by the connector reader, not a table column, so
            // by construction it is in NO pinned schema: the field-id branch below would always miss and
            // reject a benign `ORDER BY ... LIMIT` over a pinned reference. Same prefix test already used by
            // classifyColumn and hasTopnLazyMaterializeSlot.
            if (bound.getName().startsWith(Column.GLOBAL_ROWID_COL)) {
                continue;
            }
            boolean resolved = bound.getUniqueId() >= 0
                    ? pinnedFieldIds.contains(bound.getUniqueId())
                    : pinnedNames.contains(bound.getName().toLowerCase());
```

**说明**：
- `Column` 已在 `:27` import（`import org.apache.doris.catalog.Column;`），**无需新增 import**。
- 写法与 `:559`（`name.startsWith(Column.GLOBAL_ROWID_COL)`）、`:1019`（`col.getName().startsWith(Column.GLOBAL_ROWID_COL)`）**逐字一致**：`startsWith` + 同一常量。✅ 核实第 1 点。
- 注释引用兄弟方法用方法名而非行号（行号会漂），与该文件既有注释风格一致。
- 所有行 < 120 字符，符合 FE checkstyle。

## 变更 2 — 同方法 javadoc 补一句（`:923-924`）

guard 现有 javadoc 末句（`:923-924`）：

```java
     * caught); {@code uniqueId < 0} (paimon has no top-level field-id) matches by name. A {@code null}
     * pinnedSchema (latest / {@code @incr} / sys-table / hive reference) is a no-op.</p>
```

**改为**：

```java
     * caught); {@code uniqueId < 0} (paimon has no top-level field-id) matches by name. A {@code null}
     * pinnedSchema (latest / {@code @incr} / sys-table / hive reference) is a no-op, and the synthesized
     * {@code Column#GLOBAL_ROWID_COL} row-id slot is skipped (it is reader-synthesized, never a table
     * column).</p>
```

**理由**：现 javadoc 声明的契约是 "If any bound column cannot be resolved in the pinned schema, ... throw"，加了 carve-out 后该句不再准确。留一个未文档化的 carve-out 正是本 bug 的成因（三处排除漏了一处）。这是**必要的**契约修正，不是顺手改邻近代码。

## 变更 3 — 补单测（**强制，本条非 test-neutral**）

**文件**：`/mnt/disk1/yy/git/wt-catalog-spi/fe/fe-core/src/test/java/org/apache/doris/datasource/PluginDrivenScanNodeMvccSchemaGuardTest.java`

现状核实：该类共 124 行 / 7 个测试，`grep GLOBAL_ROWID` **零命中** —— 对合成 row-id **零覆盖**，这就是 guard 漏排除还能合入的原因。

既有构造方式（`:41-49`，已核实）：

```java
    private static Column col(String name, int uniqueId) {
        Column c = new Column(name, Type.INT);
        c.setUniqueId(uniqueId);
        return c;
    }

    private static SchemaCacheValue schema(Column... cols) {
        return new SchemaCacheValue(Arrays.asList(cols));
    }
```

guard 调用方式（`:59-60`）：直接调 static `PluginDrivenScanNode.assertBoundColumnsResolveInPinnedSchema(bound, pinned, "db.t")`。

`Column` 已在 `:20` import，`Arrays` 在 `:27` import → **无需新增 import**。

**在 `:114`（`nameMatchWhenNoFieldIdNoThrow` 结束）与 `:116`（`nullPinnedSchemaIsNoOp` 的 `@Test`）之间插入**：

```java
    @Test
    public void rowIdColumnIsExcludedNoThrow() throws UserException {
        // `ORDER BY ... LIMIT` over a pinned reference: LazyMaterializeTopN injects the synthesized row-id
        // `__DORIS_GLOBAL_ROWID_COL__<table>` with colUniqueId = Integer.MAX_VALUE. It is materialized by the
        // connector reader and is NOT a table column, so by construction NO pinned schema carries that
        // field-id -> without the carve-out the guard rejects a perfectly valid query (the
        // test_iceberg_time_travel / iceberg_branch_complex_queries CI failures). The real column `id`@1 still
        // resolves. MUTATION: dropping the `startsWith(GLOBAL_ROWID_COL) -> continue` carve-out -> the
        // Integer.MAX_VALUE field-id misses -> throws -> red.
        List<Column> bound = Arrays.asList(col("id", 1),
                col(Column.GLOBAL_ROWID_COL + "t", Integer.MAX_VALUE));
        SchemaCacheValue pinned = schema(col("id", 1));
        Assertions.assertDoesNotThrow(() ->
                PluginDrivenScanNode.assertBoundColumnsResolveInPinnedSchema(bound, pinned, "db.t"));
    }
```

**Rule 11 conformance**：
- `col(...)` / `schema(...)` 用既有 helper；
- `Column.GLOBAL_ROWID_COL + "t"` 与既有 rowid 测试的命名一致（`PluginDrivenScanNodeTopnLazyMatTest.java:67` 用 `Column.GLOBAL_ROWID_COL + "t"`，`PluginDrivenScanNodeClassifyColumnTest.java:74` 用 `Column.GLOBAL_ROWID_COL + "my_tbl"`）；
- `throws UserException` + `Assertions.assertDoesNotThrow` 与 `:98-105`/`:107-114` 同形；
- 注释含 `MUTATION:` 一行，与该类既有 5 处注释风格一致。

**Rule 9 非恒真性论证**：该测试**不是**恒真的 —— 在**未打补丁的 HEAD 上它必红**（`Integer.MAX_VALUE >= 0` → field-id 分支 → `pinnedFieldIds = {1}` 不含 `Integer.MAX_VALUE` → 抛 `UserException` → `assertDoesNotThrow` 失败）。它精确锁定业务逻辑：删掉 carve-out 即红。同时 `bound` 里保留真实列 `id`@1，保证测试不会因为"guard 被整体禁用"而误绿 —— 但整体禁用会被既有 3 个 `*Throws` 测试抓住（见下）。

## 施工顺序

1. 先加变更 3 的测试 → 在 HEAD 上跑应 **红**（确认测试有效，Rule 9）。
2. 再加变更 1 + 变更 2 → 测试转 **绿**，既有 7 个测试保持绿。
3. 单测命令（**本设计不执行，交由施工 session**）：
   `mvn -f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml test -pl fe-core -Dtest=PluginDrivenScanNodeMvccSchemaGuardTest`

---

# Risk Analysis

## R1 — 会不会打挂现有单测？

**已逐个读过 `PluginDrivenScanNodeMvccSchemaGuardTest.java` 全部 7 个测试**（该类是全仓**唯一**调 `assertBoundColumnsResolveInPinnedSchema` 的测试；`grep -rln` 结果只有产品文件 + 该测试类两个）：

| 类名 | 方法名 | bound 列名 | 是否受影响 |
|---|---|---|---|
| `PluginDrivenScanNodeMvccSchemaGuardTest` | `fieldIdRenumberBetweenBoundAndScannedVersionThrows` | `c`@7 | ❌ 不受影响 |
| 同上 | `columnAddedAfterScannedVersionThrows` | `id`@1, `added`@9 | ❌ |
| 同上 | `nameMissWhenNoFieldIdThrows` | `newname`@-1 | ❌ |
| 同上 | `fieldIdStableRenameResolvesByIdNoThrow` | `newname`@5 | ❌ |
| 同上 | `subsetProjectionAllResolvedNoThrow` | `a`@1, `c`@3 | ❌ |
| 同上 | `nameMatchWhenNoFieldIdNoThrow` | `a`@-1, `b`@-1 | ❌ |
| 同上 | `nullPinnedSchemaIsNoOp` | `anything`@42 | ❌（`pinnedSchema == null` 在 `:929` 先返回） |

**没有任何一个测试的 bound 列名以 `__DORIS_GLOBAL_ROWID_COL__` 开头** → 新增的 `continue` 对全部 7 个测试是 no-op → **打挂风险 = 0**。

反向保护：前 3 个 `*Throws` 测试同时构成 over-carve-out 的护栏 —— 若有人把 `continue` 写成无条件的，这 3 个立刻红。

其他 `PluginDrivenScanNode*Test`（共 20 个类，含 `PluginDrivenScanNodeMvccPinTest`、`PluginDrivenScanNodeTopnLazyMatTest`、`PluginDrivenScanNodeSysTablePinTest` 等）均不调用本方法，不受影响。

## R2 — 会不会影响那 550 个通过的用例？

**不会。** 论证：

1. 本改动**只可能减少抛出，不可能增加抛出**（新增的是一条 `continue`）。一个当前 **PASS** 的用例意味着它没被这个 guard 抛掉；让 guard 更宽松无法把 PASS 变 FAIL。
2. 唯一的理论反例是"某个用例断言这个 guard **必须**抛"。已 grep `regression-test/`：`"multiple versions with different schemas"` 与 `"Rewrite as separate statements"` **零命中** → **没有任何 e2e 断言该异常**。
3. 生效条件极窄：需同时满足 `snapshot.isPresent() && instanceof PluginDrivenMvccSnapshot`（`:898`）+ `pinnedSchema != null`（`:929`）+ 某个 bound 列名以 `__DORIS_GLOBAL_ROWID_COL__` 开头。无 pin 或无 lazy-mat 的路径逐字不变 —— 与 2×2 audit-log 证据一致。

## R3 — 这个 `continue` 会不会让**真正**的 schema skew 漏网？（任务要求点名论证）

**不会，零损失。** 三段论：

1. **被排除的只有名字以 `__DORIS_GLOBAL_ROWID_COL__` 开头的列。** `__DORIS_` 是 Doris 引擎保留前缀；该名字由 `LazyMaterializeTopN:178`/`:190` 在 **post-optimizer 阶段合成**，不来自任何外部 catalog 的 schema。
2. **该列不可能是表列**，故它**永远不可能是 skew 的真阳性载体** —— skew 的定义是"FE tuple 绑定的**表列** schema ≠ 本引用扫描版本的**表列** schema"。合成列在两个版本里都不存在（都由 reader 现造），无从 skew。
3. **同一 tuple 里的所有真实表列仍被逐个校验。** carve-out 是 per-column 的 `continue`，不是提前 `return`。新增单测 `rowIdColumnIsExcludedNoThrow` 的 `bound = [id@1, rowid@MAX]` 正是为锁死这一点：`id`@1 仍走完整解析。若有人误写成 `return`，`columnAddedAfterScannedVersionThrows`（`bound = [id@1, added@9]`）不会红（它没有 rowid 列）—— 但 `rowIdColumnIsExcludedNoThrow` 本身也不会红。**这是一个已知的测试盲区**，见 R5。

**理论残余风险**：若某外部表真有一个列名字面以 `__DORIS_GLOBAL_ROWID_COL__` 开头，该列的 skew 会被漏检。但 (a) 这是 Doris 保留前缀；(b) 更重要的是 —— **既有 `:559` `classifyColumn` 会先把它分类成 `SYNTHESIZED` 而完全不从数据文件读取**，那个 breakage 比漏检 skew 严重得多且先发生。故本改动在这个假想场景下**不新增任何暴露**，与既有两处排除承担完全相同的假设（Rule 11：一致优先）。

## R4 — 触发器 2 的耦合

`assertBoundColumnsResolveInPinnedSchema` 同时被触发器 2（`paimon_system_table`）的路径触及。触发器 2 的定稿修法是在 `resolveSysTableSnapshotPin()`（`:1044`）加 `sysTableSupportsTimeTravel()` 能力位门控 —— **不改本方法**。两处改动在**不同函数**，无文本冲突。

若两个任务同时施工同一文件，存在 **git 层面的并发编辑风险**（见 CLAUDE.md memory「并行 session 共享工作树踩踏风险」）。**建议串行合入**，或至少确认另一 session 不在编辑 `PluginDrivenScanNode.java`。

## R5 — 本改动**不修**触发器 1 之外的任何东西

`test_iceberg_time_travel` 的**其他** `qt_*`、`iceberg_branch_complex_queries` 的**其他** `qt_*` 若还有别的失败原因，本改动不覆盖。issue 文档记录这两个用例的失败点分别只有 `qt_q4` 与 `qt_order_limit`，但**本设计未重跑 CI 验证修复后这两个 suite 整体转绿**（约束：不编译、不跑 maven）。

---

# Test Plan

## Unit Tests

**新增 1 个，无删除、无修改既有测试。**

| 项 | 内容 |
|---|---|
| **类名** | `org.apache.doris.datasource.PluginDrivenScanNodeMvccSchemaGuardTest` |
| **方法名** | `rowIdColumnIsExcludedNoThrow` |
| **文件** | `/mnt/disk1/yy/git/wt-catalog-spi/fe/fe-core/src/test/java/org/apache/doris/datasource/PluginDrivenScanNodeMvccSchemaGuardTest.java`（插在 `:114` 与 `:116` 之间） |
| **输入** | `bound = [col("id", 1), col(Column.GLOBAL_ROWID_COL + "t", Integer.MAX_VALUE)]`；`pinned = schema(col("id", 1))` |
| **断言** | `Assertions.assertDoesNotThrow(() -> PluginDrivenScanNode.assertBoundColumnsResolveInPinnedSchema(bound, pinned, "db.t"))` |
| **断言意图** | 锁定「reader 合成的 row-id 列不参与 pinned-schema 解析」这条 guard 契约。它编码的 **WHY**：该列由 connector reader 现造、按构造不在任何版本的表 schema 中，拿它比 pinned schema 是范畴错误；而同 tuple 内的**真实**表列（`id`@1）必须仍被校验。 |
| **非恒真性（Rule 9）** | 在**未打补丁的 HEAD 上必红**：`Integer.MAX_VALUE >= 0` → 走 `:937` field-id 分支 → `pinnedFieldIds = {1}` ∌ `Integer.MAX_VALUE` → 抛 `UserException`。删掉 `startsWith(...) -> continue` 即红。 |

**既有 7 个测试全部保留不改**，其中 3 个 `*Throws`（`fieldIdRenumberBetweenBoundAndScannedVersionThrows` / `columnAddedAfterScannedVersionThrows` / `nameMissWhenNoFieldIdThrows`）自动构成 carve-out 过宽（写成无条件 `continue`）的护栏。

**未补的盲区（诚实声明）**：没有测试能区分 `continue` 与 `return`（见 R3 第 3 点）。补一个 `rowIdBeforeSkewedColumnStillThrows`（`bound = [rowid@MAX, added@9]`，`pinned = schema(col("id", 1))` → `assertThrows`）可以关掉它 —— **本设计不主张加**，理由：任务书要求最小改动 + 只点名了 `rowIdColumnIsExcludedNoThrow`。**此点提请用户拍板**（见摘要）。

## E2E Tests

**无需新增 e2e。** 理由：

1. **修复目标本身就是 e2e 回归**。`test_iceberg_time_travel`(`qt_q4`) 与 `iceberg_branch_complex_queries`(`qt_order_limit`) 是**既有**用例，且**在 master 上是绿的**（guard 由本分支 `22516845ca3` 引入）。它们就是本条的 e2e 验收 —— 修好 = 它们转绿。再造一个"pin + order by limit"的新 suite 是对既有覆盖的重复。
2. 该形状（`FOR TIME AS OF` / `@branch` + `ORDER BY ... LIMIT`）**已被这两个用例覆盖**，2×2 audit-log 证据表明正是它们的交点触发。
3. 本改动**不新增任何用户可见行为** —— 只是取消一个误抛，把行为恢复到 master 的状态。无新特性需要新 e2e 覆盖。

**验收命令**（交由施工 session，本设计未执行）：

```
sh run-regression-test.sh --run -s test_iceberg_time_travel
sh run-regression-test.sh --run -s iceberg_branch_complex_queries
```

**验收标准**：两个 suite 全绿（不只是 `qt_q4` / `qt_order_limit`），且 `Failed to pin MVCC snapshot for plugin-driven scan` 不再出现在 fe.log。

---

# 仍未证实

1. **BE 侧行为未验证**：本设计断言"合成 row-id 由 connector reader 材化、不从数据文件读"，依据是 `PluginDrivenScanNode:547-549` 的 javadoc + `:559` 把它分类为 `TColumnCategory.SYNTHESIZED`。**未读 BE C++ 代码确认** reader 确实合成该列。若 BE 侧另有依赖，本设计不覆盖（但该行为是 master 既有的，与本改动无关）。
2. **修复后 e2e 未跑**：约束禁止编译/跑 maven。「补丁后两个 suite 转绿」**未证实**，仅由静态推理支撑。同理，「新单测在 HEAD 上必红、打补丁后转绿」也**未实跑证实**，仅由代码路径推演（`Integer.MAX_VALUE >= 0` → field-id 分支 → miss → throw）得出。
3. **fe.audit.log 的 2×2 阶乘证据继承自 issue 文档 §B**，本轮**未重新采集 audit log** 核对。行号/代码事实已全部对照 HEAD 核实，audit 证据未。
4. **这两个 suite 是否还有触发器 1 之外的失败点未证实**（见 R5）。issue 文档记录失败点分别只有 `qt_q4` / `qt_order_limit`，本设计接受该记录未独立复核完整 suite 日志。
5. **继承 issue 文档已列的"仍未证实"条目**：该文档中与 §B 触发器 1 相关的未证实项一并继承，本设计未新增证据也未推翻。
6. **`Integer.MAX_VALUE` 作为 uniqueId 的唯一性**：全仓 grep `setUniqueId(Integer.MAX_VALUE)` 零命中、`LazyMaterializeTopN` 的两个 ctor 注入点是仅有的 `Integer.MAX_VALUE` colUniqueId 来源 —— 这是**基于 grep 的证据，非穷尽证明**（可能有经变量传入 `colUniqueId` 的间接路径未被 grep 覆盖）。不影响本修法（按名字前缀排除，不依赖 MAX_VALUE 的唯一性）。
