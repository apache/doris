# P4-T06e — FIX-BIND-STATIC-PARTITION (P0-3) — Design

> 来源 finding：`plan-doc/reviews/P4-maxcompute-full-rereview-2026-06-07.md` §A NG-3 (F48) / §B DG-2 (F19)。
> 关联：P0-1 FIX-OVERWRITE-GATE(`59699a62f33`)、**P0-2 FIX-WRITE-DISTRIBUTION(`f0adedba20c`)——本 fix 经用户批准回退其 cols→full-schema 索引**。
> 流程：设计→改→编译+UT+mutation→对抗 review→commit。本文跨轮更新。

---

## Problem

翻闸后 MaxCompute 写走通用 connector SPI sink（`UnboundConnectorTableSink` → `BindSink.bindConnectorTableSink` → `LogicalConnectorTableSink` → `PhysicalConnectorTableSink` → `MaxComputeWritePlanProvider` → BE `VMCTableWriter`）。

**Blocker（F19/F48，all-static 无列名）**：
```sql
INSERT INTO mc_part_tbl PARTITION(pt='x') SELECT <非分区列>   -- 无列名
```
在 `BindSink.java:941` 抛 `"insert into cols should be corresponding to the query output"`。`SELECT` 只产数据列（child output = N），但 `bindConnectorTableSink` 在 `colNames` 为空时 `bindColumns = table.getBaseSchema(true)`（含分区列 `pt`，= N+M），列数校验失败。

**深层耦合（partial-static，复审未覆盖、本设计新发现）**：legacy-parity 要求支持混合静态/动态分区（`PARTITION(ds='x') SELECT id,val,region`，ds 静态、region 动态——legacy 支持且 `test_mc_write_static_partitions.groovy` Test 7 回归断言其有 SORT 节点）。修 blocker 时把 child 投影成 **full-schema**（BE 需要、见下）会与 **P0-2 的「按 cols 位置索引分区列」** 冲突：partial-static 下 `cols` 排除了静态 `ds`，但 child 是 full-schema 含 `ds`，cols 位置与 full-schema 位置错位 → 分布按错列 hash/sort → MaxCompute Storage API streaming 写 "writer has been closed"。**两者不可同时满足**（无任何 child 列序能同时满足「BE 末尾擦除 full-schema 分区列」与「P0-2 cols 位置索引」），故须把 P0-2 的索引回退为 legacy 的 full-schema 索引。

---

## Root Cause

1. **bind 期未剔除静态分区列**：`bindConnectorTableSink`（克隆自 `bindJdbcTableSink`，JDBC 无静态分区）`:917-919` 取 full base schema、从不读 `sink.getStaticPartitionKeyValues()`，亦不像 legacy `bindMaxComputeTableSink:870-879` 那样过滤静态分区列。过期注释 `:944-948`「Currently only JDBC catalogs use connector sink」翻闸后未更新。
2. **VALUES 路径未接 connector**：`InsertUtils.java:377-389` 只对 `UnboundIcebergTableSink`/`UnboundMaxComputeTableSink` 在无列名时剔除静态分区列做默认值生成，未加 `UnboundConnectorTableSink` 分支。
3. **P0-2 cols 索引与 BE full-schema 契约冲突（partial-static）**：见上「深层耦合」。

### BE 契约（决定 child 必须 full-schema）——已逐层核证

| 环节 | 证据 | 结论 |
|---|---|---|
| BE 静态分区擦除 | `be/.../vmc_table_writer.cpp:83-95` `if(!_partition_column_names.empty() && _has_static_partition){ data_cols = total_cols - num_partition_cols; 擦除末尾 num_partition_cols; }` + `:154-163` 按 `_static_partition_spec` 路由、`output_block.erase(_non_write_columns_indices)` | BE **假定** FE 传的 `output_exprs` = 数据列 + **全部分区列在末尾**，擦除末尾 `num_partition_cols` |
| 连接器 thrift 总设 partition_columns | `MaxComputeWritePlanProvider:123-128` 表有分区即 `setPartitionColumns(全部分区列)`，静态时 `setStaticPartitionSpec` | all-static / partial-static 均触发 BE 擦除分支（与 legacy `MaxComputeTableSink:79-93` 等价） |
| output_exprs 来源 | `PhysicalPlanTranslator.translatePlan:308-314` fallback：root fragment outputExprs 空 → 取 `physicalPlan.getOutput()`（= sink `outputExprs.toSlot()` = `withChildAndUpdateOutput(project)` 后的 child 输出）；BE `pipeline_fragment_context.cpp` 取 `fragment.output_exprs` 传 `MCTableSinkOperatorX`(`maxcompute_table_sink_operator.h:47,55`) | **FE 的 child 投影直接决定 BE 列集**。child 投 full-schema → BE 收 full-schema → 正确擦末尾分区列 |
| 分区列可空性 | legacy `MaxComputeExternalTable.initSchema:188-190` partition col `isAllowNull=true`；connector `MaxComputeConnectorMetadata.getTableSchema` partition col `isNullable=true`（硬编码） | `getColumnToOutput:457-465` 对未提及静态分区列填 `NullLiteral` **不抛**（两路一致） |

**净结论**：connector 静态分区写要 BE 正确，child 必须 = full-schema（数据列 + 分区列在末尾，静态列填 NULL），**与 legacy `bindMaxComputeTableSink` 完全一致**。

---

## Design

**总纲：把 connector 写路径在「分区表」下做成 legacy `bindMaxComputeTableSink` + `PhysicalMaxComputeTableSink` 的忠实泛化**（capability 门保留 P0-2 对 JDBC/ES 的 GATHER 隔离），非分区表（JDBC/ES）维持现状。

### 改动 1 — `BindSink.bindConnectorTableSink`（fe-core）

```java
Map<String, Expression> staticPartitions = sink.getStaticPartitionKeyValues();
Set<String> staticPartitionColNames = staticPartitions != null
        ? staticPartitions.keySet() : Sets.newHashSet();

List<Column> bindColumns;
if (sink.getColNames().isEmpty()) {
    bindColumns = table.getBaseSchema(true).stream()
            .filter(col -> !staticPartitionColNames.contains(col.getName()))   // ← 新增过滤
            .collect(ImmutableList.toImmutableList());
} else { /* 不变：用户列 */ }

LogicalConnectorTableSink<?> boundSink = new LogicalConnectorTableSink<>(... bindColumns, child.getOutput()...);
if (boundSink.getCols().size() != child.getOutput().size()) { throw ...; }   // 现在 N==N 通过

if (!staticPartitionColNames.isEmpty()) {
    // 静态分区：镜像 legacy bindMaxComputeTableSink:904-907 —— child 投 full-schema，
    // 静态分区列填 NULL 在 full-schema 末尾，使 BE 按位置擦除正确。
    Map<String, NamedExpression> columnToOutput = getColumnToOutput(ctx, table, false, boundSink, child);
    LogicalProject<?> fullProject = getOutputProjectByCoercion(table.getFullSchema(), child, columnToOutput);
    return boundSink.withChildAndUpdateOutput(fullProject);
}
// 无静态分区（JDBC/ES/纯动态）：维持现有 JDBC 风格投影（user/cols 序）。
Map<String, NamedExpression> columnToOutput = getConnectorColumnToOutput(bindColumns, child);
LogicalProject<?> outputProject = getOutputProjectByCoercion(bindColumns, child, columnToOutput);
return boundSink.withChildAndUpdateOutput(outputProject);
```

**分支键 = `!staticPartitionColNames.isEmpty()`（仅静态分区走 full-schema 投影）**：
- 纯动态：`staticPartitions` 空 → ELSE 分支，`bindColumns = full base schema`、JDBC 投影后 child = full-schema 序（与 full-schema 投影同效），不变。
- JDBC（无分区、可能有用户列子集）：ELSE 分支，维持 user 序，**零行为变更**（JDBC 无静态分区）。
- 复用 legacy helper `getColumnToOutput`/`getOutputProjectByCoercion` → 与 legacy 逐字一致（OLAP 分支被 `instanceof OlapTable` 守门、对外表惰性；`isPartialUpdate=false`）。
- 类型安全：`LogicalConnectorTableSink extends LogicalTableSink`（与 `LogicalMaxComputeTableSink` 同基），`getColumnToOutput(... LogicalTableSink<?> ...)` 接受；`UnboundConnectorTableSink` 与 `UnboundMaxComputeTableSink` 同基（`UnboundBaseExternalTableSink`）满足 ctx 泛型。

更正过期注释 `:944-948`。

### 改动 2 — `PhysicalConnectorTableSink.getRequirePhysicalProperties`（fe-core，**回退 P0-2**）

把 P0-2 的「按 cols 位置索引分区列」改回 legacy `PhysicalMaxComputeTableSink:111-155` 的「按 full-schema 位置索引」。**保留 P0-2 的 capability 门**（`requirePartitionLocalSortOnWrite()` / `supportsParallelWrite()` / 否则 GATHER），只换索引方式：

```java
if (table.requirePartitionLocalSortOnWrite()) {
    Set<String> partitionNames = table.getPartitionColumns()→names;
    if (!partitionNames.isEmpty()) {
        Set<String> colNames = cols→names;
        boolean hasDynamicPartition = partitionNames.anyMatch(colNames::contains);  // cols 仍排除静态列
        if (hasDynamicPartition) {
            List<Column> fullSchema = targetTable.getFullSchema();                  // ← 按 full-schema 索引
            columnIdx = [i | partitionNames.contains(fullSchema[i].name)];
            exprIds   = columnIdx.map(i -> child().getOutput().get(i).exprId);
            orderKeys = columnIdx.map(i -> new OrderKey(child().getOutput().get(i), true, false));
            return hash(exprIds) + MustLocalSort(orderKeys);
        }
        // 全静态：落下
    }
}
return table.supportsParallelWrite() ? SINK_RANDOM_PARTITIONED : GATHER;
```

为何正确（child 现为 full-schema，全 case 与 legacy 一致）：
- **纯动态** `SELECT ...,ds,region`：cols=child=fullSchema → cols 索引≡full-schema 索引，行为不变（hash/sort by 全分区列）。
- **partial-static** `PARTITION(ds='x') SELECT ...,region`：cols 排除 ds、含 region → `hasDynamicPartition`=true；child=full-schema `[...,ds(null),region]`；full-schema 索引 columnIdx={ds_pos,region_pos} → hash/sort by `[ds, region]`（ds 为 NULL 常量、实质 by region）= **legacy 同款**。〔cols 索引则 region@cols_pos 命中 child 的 ds → 错列，正是要修的 bug。〕
- **全静态** `PARTITION(ds='x',region='y') SELECT ...`：cols 无分区列 → `hasDynamicPartition`=false → 落 `SINK_RANDOM_PARTITIONED`（不索引 child）= legacy branch-2。
- **JDBC/ES**：`requirePartitionLocalSortOnWrite()`=false → 直落 `supportsParallelWrite()?RANDOM:GATHER`（capability 门保留）。

更新该方法 + 类 javadoc 的「index by cols」表述为「index by full-schema」。

### 改动 3 — `InsertUtils.java:377-389`（VALUES 路径）

`UnboundMaxComputeTableSink` 分支后加：
```java
} else if (unboundLogicalSink instanceof UnboundConnectorTableSink) {
    staticPartitions = ((UnboundConnectorTableSink<?>) unboundLogicalSink).getStaticPartitionKeyValues();
}
```
（`getStaticPartitionKeyValues()` 已暴露，line 84。补 import。）使 `PARTITION(p='x') VALUES (...)` 无列名时默认值生成剔除静态分区列。

### 改动 4 — 测试更新（`PhysicalConnectorTableSinkTest`）

P0-2 测试基于 cols 索引；改 full-schema 索引后：
- `table()` helper 增 `getFullSchema()` stub。
- `dynamicPartitionWriteRequiresHashAndLocalSort`：纯动态 cols==fullSchema，断言不变（partSlot@idx1）。
- `allStaticPartitionWriteUsesRandomPartitioned`：不索引 child，不变。
- **新增 `partialStaticPartitionHashesByDynamicColumn`**：cols=[data,region]、child=[dataSlot,dsSlot,regionSlot]（full-schema [data,ds,region]）、partitionCols=[ds,region]、fullSchema=[data,ds,region] → 断言 hash keys=`[dsSlot,regionSlot]`、sort=`[dsSlot,regionSlot]`（pin full-schema 索引；cols 索引会得 `[dsSlot]`/错列 → 红）。

### 改动 5 — doc-sync

- `P4-T06e-FIX-WRITE-DISTRIBUTION-design.md`：在「index by cols」节加 superseded 注（P0-3 因 partial-static parity 回退为 full-schema 索引）。
- `P4-T05-T06-cutover-design.md` G4/G5/DECISION-3：更正「忠实镜像」声明漏了 bind 期静态分区列剔除。
- `decisions-log.md` / `deviations-log.md`：登记本轮结论 + P0-2 索引回退。
- HANDOFF / task-list-P4-rereview：回填。

---

## Implementation Plan

1. `BindSink.bindConnectorTableSink` — 过滤静态分区列 + 静态分支 full-schema 投影 + 改注释。
2. `PhysicalConnectorTableSink.getRequirePhysicalProperties` — cols→full-schema 索引 + javadoc。
3. `InsertUtils.java` — 加 `UnboundConnectorTableSink` 分支 + import。
4. `PhysicalConnectorTableSinkTest` — stub getFullSchema + 新增 partial-static 测试。
5. 新增 `BindConnectorSinkStaticPartitionTest`（见 Test Plan）— pin bind 期列过滤。
6. doc-sync。
7. 编译(`:fe-core -am`)+checkstyle+import-gate+UT+mutation。

---

## Risk Analysis

- **R1 回退 P0-2（committed）**：用户已批准。capability 门保留→JDBC/ES 不受影响；纯动态 cols==fullSchema→行为不变；只有 partial-static 的索引行为改变（修复，非回归）。P0-2 测试随改。
- **R2 复用 `getColumnToOutput` 的 OLAP 包袱**：OLAP 分支 `instanceof OlapTable` 守门惰性；`isPartialUpdate=false`；外表无 generated/mv/shadow 列、循环空转。legacy MC 已长期复用证其对外表安全。
- **R3 分区列可空性**：两路均 `isAllowNull/isNullable=true`（已核），NullLiteral 填充不抛。
- **R4 BE partial-static 末尾擦除 region**：BE 对 partial-static 擦全部分区列、按静态 spec 路由——此为 **legacy 既有行为**（本 fix 不改 BE，parity 保持）；其端到端正确性属 live-e2e 门 + 既有 legacy 限制，**不在本 fix scope**（若 BE 实有 partial-static 数据落位问题，legacy 同存，另立 ticket）。
- **R5 e2e 未验**：CI 无 live ODPS。本 fix 静态层 parity 高置信，但写路径最终须 `test_mc_write_static_partitions.groovy` live 验（与 P0-1/P0-2 一并）。**真值闸**：all-static / partial-static / 纯动态 INSERT(+VALUES) 无 "writer has been closed" 且数据落对分区。

---

## Test Plan

### Unit Tests（fe-core，无 e2e）

- **`BindConnectorSinkStaticPartitionTest`（新）** — pin bind 期列过滤（Rule 9：静态分区列必须从 cols 排除否则列数校验抛/写丢列）。因 `bind()` 走真实 Env 解析较重，采 `PhysicalConnectorTableSinkTest` 同款 mock：mock `PluginDrivenExternalTable`（stub `getBaseSchema(true)`/`getColumn`/`getPartitionColumns`/`getFullSchema`），驱动列选择逻辑（必要时抽 `@VisibleForTesting` 包级静态 helper `selectConnectorBindColumns(table, colNames, staticPartitionColNames)`），断言：
  - all-static 无列名 `{pt}` → bindColumns = 数据列（排除 pt）。
  - 纯动态 无静态 spec → bindColumns = full base schema（不排除）。
  - 显式列名 → 用户列（不受影响）。
  - **mutation**：删 `.filter(...)` → all-static 断言含 pt → 红。
- **`PhysicalConnectorTableSinkTest`（改）** — 见改动 4，新增 partial-static 用例 pin full-schema 索引。
  - **mutation**：full-schema 索引改回 cols 索引 → partial-static 用例红。

### E2E Tests

复用既有 `regression-test/suites/external_table_p2/maxcompute/write/test_mc_write_static_partitions.groovy`（p2 / live ODPS / CI 跳）：all-static（无 SORT）、partial-static（有 SORT）、纯动态、VALUES 形式、INSERT OVERWRITE。**作为 live 真值闸记录**，本轮不在 CI 跑。

---

## review 轮次累计结论（防跨轮矛盾）

> 详见 `plan-doc/reviews/P4-T06e-FIX-BIND-STATIC-PARTITION-review-rounds.md`。3 轮 clean-room 对抗 review 收敛（0 mustFix）。

- **判别键三轮收敛**：`!staticPartitionColNames.isEmpty()`（R1 证伪：纯动态重排显式列名错列）→ `!getPartitionColumns().isEmpty()`（R2 证伪：非分区 MaxCompute 重排/部分列名静默错列/丢列，因 MC BE 按位置写）→ **`table.requiresFullSchemaWriteOrder()`**（capability `SINK_REQUIRE_FULL_SCHEMA_ORDER`）。终态 = MaxCompute 全写形与 legacy `bindMaxComputeTableSink` 逐字 parity；JDBC/ES cols 序 parity。〔本文上方「Design 改动 1」分支键 `!staticPartitionColNames.isEmpty()` 与「改动 2 partitioned」均为中间态，**已被 capability 取代**——以 review-rounds R2/R3 + 代码为准。〕
- **R1（`wi3mnjymb`）**：13→8 confirmed（3 major 同根因 = 投影分支太窄 + 分布 full-schema 索引不匹配 cols 序 child）。修：分支改 partitioned + 分布回退 full-schema 索引 + 新增 reordered-dynamic 分布测。
- **R2（`wy299gtsh`）**：1 new major（非分区 MC 重排/部分列名）。修：分支 partitioned→capability；新增 SPI `SINK_REQUIRE_FULL_SCHEMA_ORDER`；p2 `test_mc_write_insert` Test 3b。
- **R3（`wlwpw0b2s`）**：0 mustFix 收敛。1 nit（跨 capability 隐式耦合 LOCAL_SORT⟹FULL_SCHEMA_ORDER）→ javadoc 登记。确认全 connector/写形 legacy parity。
- **登记**：[D-030]（capability + 回退 D-029 索引）、[DV-014]（bind 投影单测 KNOWN-LIMITATION）。**Batch-D 红线**：删 legacy `bindMaxComputeTableSink`/`PhysicalMaxComputeTableSink` 须待本 fix 落（已落）。
- **真值闸**：live e2e（p2 `test_mc_write_insert` Test 3/3b + `test_mc_write_static_partitions`：all-static/partial-static/纯动态/重排/部分/VALUES 无 "writer has been closed" 且数据落对列/分区）。
