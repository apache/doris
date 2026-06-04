# 复杂类型谓词过滤和统计信息过滤实现方案

本文聚焦 `STRUCT` 内 primitive leaf 的谓词过滤和 Parquet file-layer pruning。目标是参考 DuckDB 的 nested filter 语义，同时保持 Doris 当前 new parquet reader 的 block 布局和 `Expr` 行级过滤原则。

## 1. DuckDB 参考模型

DuckDB 的核心不是把 `struct_extract(s, 'id')` 改写成一个普通 leaf slot，而是在 filter 中保留 nested 结构。

### 1.1 StructFilter

DuckDB 使用 `StructFilter` 表示 `s.id > 5`：

```text
StructFilter(
  child_idx = id 在当前 struct 中的位置,
  child_name = "id",
  child_filter = ConstantFilter(GT, 5)
)
```

多层嵌套 `s.a.b > 5` 会递归包装成：

```text
StructFilter(a_idx, StructFilter(b_idx, ConstantFilter(GT, 5)))
```

关键行为：

- `StructFilter::CheckStatistics()` 先从 struct stats 中取 child stats，再递归调用 child filter。
- `StructFilter::ToExpression()` 可以还原成 `struct_extract` 表达式，用于运行时过滤。
- `MultiFileColumnMapper::TryCastTableFilter()` 通过 `MultiFileIndexMapping.child_mapping` 把 global child index 递归重映射为 local child index。

参考：

- `/Users/xiaogangsu/code/duckdb/src/planner/filter/struct_filter.cpp`
- `/Users/xiaogangsu/code/duckdb/src/common/multi_file/multi_file_column_mapper.cpp`

### 1.2 Parquet reader / statistics

DuckDB 的 Parquet reader 按 logical schema 构造 reader tree：

- root 是 `StructColumnReader`。
- `STRUCT` child 可以只为 selected child 创建 reader，未选 child reader 为空。
- `LIST`/`MAP` 仍作为 nested reader，非 primitive leaf 不直接参与普通 min/max pruning。

统计信息处理方式：

- primitive schema 直接读取对应 `column_index` 的 ColumnChunk stats。
- `STRUCT` 没有自己的 Parquet leaf stats，但 DuckDB 会递归构造 struct child stats。
- `LIST`/`MAP`/`ARRAY` 返回不支持。
- row group pruning 时，拿 selected column reader 的 stats，再调用 table filter 的 `CheckStatistics()`。如果 filter 是 `StructFilter`，就递归检查 child stats。
- bloom filter 只在 selected reader 是非 nested primitive column 时应用。

参考：

- `/Users/xiaogangsu/code/duckdb/extension/parquet/parquet_reader.cpp`
- `/Users/xiaogangsu/code/duckdb/extension/parquet/parquet_statistics.cpp`
- `/Users/xiaogangsu/code/duckdb/extension/parquet/reader/struct_column_reader.cpp`

## 2. Doris 当前约束

Doris new parquet reader 和 DuckDB 的内部布局不同：

- `FileScanRequest::column_positions` 是 top-level `file_column_id -> file-local block_position`。
- `FieldProjection.children` 表示 top-level complex column 内部的 nested projection。
- file block 中 `STRUCT` 是一个 `ColumnStruct`，不是每个 child 一个独立 block slot。
- `TableReader` materialize struct child 时从 `ColumnStruct::get_column_ptr(child_idx)` 取 child column。

因此不能把 `struct_element(VSlotRef(parent), 'id')` 改写为 child `VSlotRef`，除非先改变 file block layout 和 materialization contract。本方案不做这种改变。

当前原则仍然是：

- 行级过滤使用 `Expr` / `VExprContext`。
- `ColumnPredicate` 只用于 file-layer pruning：row group statistics、dictionary、bloom filter、page index。
- 所有 pruning 只能在确定不会漏读时生效；无法解析 nested target、类型不匹配、schema 缺失或 repeated 语义不明确时直接保留 row group/page。

## 3. 目标模型

Doris 应对齐 DuckDB 的 nested filter 语义，但使用适合当前代码的表示：

```cpp
struct FileNestedPredicateTarget {
    // Top-level file column id. For scalar top-level predicates this is also the leaf column id
    // resolver entry.
    ColumnId file_column_id = -1;

    // File-local child field-id path under file_column_id. Empty means top-level scalar.
    // Example: s.id -> [id_file_field_id], s.a.b -> [a_file_field_id, b_file_field_id].
    // This is not table column id and not child ordinal.
    std::vector<int32_t> file_child_id_path;
};
```

`FileColumnPredicateFilter` 继续表达“一个 file-local target 上的一组 `ColumnPredicate`”，但 target 需要从 top-level 扩展到 nested leaf：

```cpp
struct FileColumnPredicateFilter {
    FileNestedPredicateTarget target;
    std::vector<std::shared_ptr<ColumnPredicate>> predicates;
};
```

兼容实现可以先保留现有 `file_column_id` 字段，再新增 `file_child_id_path`，但文档语义必须明确：path 是 file-local child field id path。

## 4. 谓词过滤实现方案

### 4.1 行级 Expr localization

`WHERE s.id > 5` 的 VExpr 形态仍保留为：

```text
binary_predicate(GT, struct_element(VSlotRef(s), 'id'), literal(5))
```

localization 只把 `VSlotRef(s)` 从 table slot 改写成 file block 中 top-level struct slot。`struct_element` 函数本身不替换成 child slot。

需要做的事：

1. `build_file_slot_rewrite_map()` 继续只为 top-level file block slot 建映射。
2. `rewrite_table_expr_to_file_expr()` 递归改写 `struct_element` 内部的 parent `VSlotRef`。
3. literal 类型重写可以新增 nested fast path，但只用于把 literal 转成 file child type，不改变表达式的 slot 形态。

### 4.2 filter-only nested projection

当前更关键的问题不是行级表达式无法执行，而是 filter 引用的 nested child 可能不在输出 projection 中。

例如：

```sql
SELECT s.name FROM t WHERE s.id > 5;
```

这里 `s.name` 是输出 child，`s.id` 是 filter-only child。File reader 应读取同一个 top-level `s`，但 nested projection 应包含 `name` 和 `id`。不能把 `id` 当作独立 block slot，也不能因为输出只需要 `name` 而漏读 `id`。

实现要求：

- 从 table filters 中识别 `struct_element(VSlotRef(parent), literal child_name/index)` 链。
- 将 filter 需要的 nested child path 合并到 `ColumnMapping.child_mappings` 或等价的 `FieldProjection.children`。
- 同一 top-level complex column 的 output child 和 predicate child 需要去重合并。
- 如果无法解析 nested path，退回读取 parent struct 的必要范围，保证行级 Expr 可以执行。

这一步对齐 DuckDB 的 selected child reader 思路，但落在 Doris 的 `FieldProjection` 上。

### 4.3 nested path 识别范围

第一阶段只识别稳定且可控的模式：

- `struct_element(VSlotRef(parent), literal_name_or_index)`
- 多层嵌套的连续 `struct_element(struct_element(...), ...)`
- 比较谓词中的常量 literal，用于后续 pruning target 构造

暂不识别：

- `LIST`/`MAP` 元素访问
- 动态 field name
- 非 deterministic 表达式
- 需要 row-level 计算才能确定 child path 的表达式

## 5. 当前实现状态

### 5.1 行级 Expr localization

已实现：

- `struct_element(VSlotRef(parent), literal child)` 链可以被识别为 nested path。
- 行级表达式仍保留 `struct_element(file_struct_slot, field)` 形态，只改写 parent slot 到 file-local top-level block slot。
- 不把 struct child 注册为独立 block slot，也不把 `struct_element` 改成 child `VSlotRef`。

### 5.2 filter-only nested projection

已实现：

- filter 引用的 struct child 会合并到同一个 top-level complex column 的 `FieldProjection.children`。
- output child 顺序保持优先，filter-only child 追加到 read projection。
- filter-only child 不加入 `ColumnMapping.child_mappings`，避免 table output materialization 把它当作输出字段。
- `ColumnMapping` 保存 `original_file_type` / `original_file_children`，重复创建 split-local request 时可以从原始 file schema 重建 read projection。
- nested filter projection 优先通过 `ColumnMapping.child_mappings` 映射 table child 到 file child；没有 child mapping 的 filter-only path 再回退到 file schema 解析。

### 5.3 nested file-layer pruning target

已实现：

- `FileColumnPredicateFilter` 保留 `file_column_id`，新增 `file_child_id_path`。
- `file_child_id_path` 是 top-level file column 下的 file-local child field id path，不是 table id，也不是 ordinal。
- mapper 会从 AND 语义下的 `struct_element(...) op literal` / `literal op struct_element(...)` 构造 nested file-layer pruning hint。
- mapper 会从 AND 语义下的 `struct_element(...) IN (...)` 构造 nested `IN_LIST` pruning hint。
- 对已经存在 `ColumnMapping` 的 nested child，mapper 使用 table child name + field-id mapping 生成 file-local `file_child_id_path`，支持 table/file nested child rename。
- 不从 OR/NOT/任意函数子树中提取 pruning predicate，避免把非必要条件当成必需条件裁剪。
- literal 转换到 file leaf type 失败、path 解析失败、leaf 不是 primitive 时，不生成 pruning hint。

### 5.4 Parquet leaf resolver and pruning

已实现：

- `ParquetStatisticsUtils::ResolvePredicateLeafSchema()` 统一解析 top-level 或 nested target。
- 解析结果必须是 primitive leaf、`leaf_column_id >= 0` 且 `max_repetition_level == 0`。
- row group min/max statistics 使用 resolved leaf schema。
- dictionary pruning 使用 resolved leaf schema 和 leaf `ColumnChunk`，仍保持 string-like、dictionary-encoded、EQ/IN_LIST 限制，并已有 nested struct 真实 parquet fixture 覆盖。
- bloom filter 使用 resolved leaf schema，仍保持 supported primitive type、EQ/IN_LIST/null 相关限制；当前 Arrow writer 头文件没有稳定的 bloom 写入开关，因此先以 Arrow bloom adapter / pruning 逻辑单元测试覆盖。
- page index 使用 resolved leaf schema，只允许 non-repeated primitive leaf；LIST/MAP/repeated leaf 直接跳过 page range pruning，并已有 nested struct 真实 parquet fixture 覆盖。

## 6. 统计信息 / pruning 设计约束

### 6.1 leaf schema resolver

当前 resolver：

```cpp
const ParquetColumnSchema* ParquetStatisticsUtils::ResolvePredicateLeafSchema(
        const std::vector<std::unique_ptr<ParquetColumnSchema>>& file_schema,
        const reader::FileColumnPredicateFilter& column_filter);
```

解析规则：

1. `target.file_column_id` 必须是合法 top-level file schema id。
2. `target.file_child_id_path` 为空时，target schema 就是 top-level schema。
3. path 非空时，逐层在 `ParquetColumnSchema::children` 中按 file-local `field_id` 匹配。
4. 最终 schema 必须是 primitive leaf，且 `leaf_column_id >= 0`。
5. 本轮只允许 `max_repetition_level == 0` 的 leaf。任何 LIST/MAP/repeated path 直接不剪枝。

这相当于 Doris 版本的 DuckDB `StructFilter::CheckStatistics()`：不是在 filter 对象里递归拿 child stats，而是在 pruning 层先把 nested target 解析到 Parquet leaf schema，再复用现有 primitive pruning 逻辑。

### 6.2 支持范围

第一阶段支持：

- top-level primitive column 的现有 pruning。
- `STRUCT` / nested `STRUCT` 下的 primitive leaf：
  - min/max statistics row group pruning。
  - string-like dictionary pruning。
  - supported primitive bloom filter pruning。
  - page index row range pruning。

第一阶段不支持：

- `LIST` element predicate。
- `MAP` key/value predicate。
- repeated primitive / repeated group 的 leaf pruning。
- page index pruning on repeated leaf。
- complex child schema change 的完整 pruning 语义。

### 6.3 pruning 类型处理

#### Row group min/max

将 `schema[column_filter.file_column_id]` 替换为 `resolve_predicate_leaf_schema()` 的结果。对 resolved leaf 调用现有 `TransformColumnStatistics()` 和 `CheckStatistics()`。

#### Dictionary

dictionary pruning 只对 resolved primitive leaf 生效。现有 string-like 限制保持不变。无法读取 dictionary page 或 predicate 不支持时保留 row group。

#### Bloom filter

DuckDB 只对非 nested primitive reader 应用 bloom filter。Doris 当前通过 resolved leaf 接入 nested struct primitive leaf，但仍只处理 Arrow adapter 已支持且 predicate 可安全转换的 primitive 类型。不确定时保留 row group。

#### Page index

page index 对 repeated leaf 的 row range 语义复杂。本轮只允许 non-repeated primitive leaf。`STRUCT` 下 non-repeated primitive leaf 可以复用现有 page index range 逻辑；LIST/MAP/repeated leaf 直接跳过。

## 7. 本轮完成结论

本轮实现已经覆盖 `STRUCT` / nested `STRUCT` 下 primitive leaf 的行级 Expr localization、filter-only nested projection、file-layer pruning target 构造、statistics / dictionary / bloom / page index pruning 入口，以及 mapper-based nested child rename。

仍然不放入本轮实现范围的事项如下：

- nested bloom pruning 真实 parquet fixture：当前 Arrow writer 头文件没有稳定的 bloom filter metadata 写入开关；如果后续 Arrow writer 或外部 fixture 能稳定提供 bloom filter metadata，再补真实文件级 fixture。
- 完整复杂 child schema change：需要 FE/table reader 提供完整 nested table mapping；file reader 只消费 file-local mapping，不理解 table/global schema。
- LIST/MAP/repeated leaf pruning：只有在 Dremel row semantics 和 row-range 语义明确后再接入 pruning。

## 8. 需要避免的实现

- 不要把 struct child 注册成独立 `column_positions` block slot。
- 不要把 `struct_element(...)` 改写成 child `VSlotRef`。
- 不要把 `ColumnPredicate` 用于行级过滤。
- 不要对 LIST/MAP/repeated leaf 做 row group/page pruning，除非后续有明确的 Dremel row semantics 证明。
- 不要新增语义不清的 `child_field_path`。如果新增 path 字段，必须明确它是 file-local child field id path。
