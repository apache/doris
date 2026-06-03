# 复杂类型谓词过滤和统计信息过滤实现方案

本文分析当前实现的限制，并以 DuckDB 为参考提出实现方案。

## 1. 谓词过滤：嵌套字段无法下推到 Parquet 层

### 1.1 问题根因

`WHERE s.id > 5` 的 VExpr 树是 `binary_predicate(GT, struct_element(VSlotRef(s), 'id'), literal(5))`。`VSlotRef` 的 `slot_id` 是父 struct `s` 的 table_column_id（记为 X），不是子字段 `id` 的 table_column_id（记为 Y，存储在 `child_mappings` 中）。

`localize_filters()` 在以下三步中丢失了嵌套信息：

```
build_file_slot_rewrite_map()
  → 只遍历 _mappings（顶级列），不遍历 child_mappings
  → 子字段 Y 不在映射表中

rewrite_table_expr_to_file_expr() 中的 fast path
  → find_slot_rewrite_info 寻找 slot_ref 的 slot_id
  → struct_element(VSlotRef(s)) 不是裸 VSlotRef，不会被 fast path 匹配
  → 回退到递归重写子节点
  → VSlotRef(s) 被找到并重写为 file block slot
  → 但 struct_element 函数调用本身不会被重写

localize_filters() Phase 1
  → filter_slot_ids() 收集的是 VSlotRef 的 slot_id = X（父 struct）
  → _find_mapping(X) 找到父 struct 的映射 ✓
  → add_scan_column 把父 struct 加入 predicate_columns
  → 但子字段 Y 未被单独加入 → 无法享受统计信息剪枝
```

**结论**：VExpr 表达式最终能在 file block 上正确求值（因为 struct_element 运行时会从 struct column 中提取子字段），但无法分解为子字段级别的优化（单列统计剪枝、literal 类型推断、子字段级 predicate_column 标记）。

### 1.2 DuckDB 的解决方案：StructFilter

DuckDB 在优化器阶段（`FilterCombiner`）将 `struct_extract(s, 'id') > 5` 转换为：

```
StructFilter(
  child_idx  = id 在 struct 中的位置,
  child_name = "id",
  child_filter = ConstantFilter(GT, 5)
)
```

这个 `StructFilter` 是一个递归包装器。对于多层嵌套 `s.a.b > 5`，会产生：
```
StructFilter(a_idx, StructFilter(b_idx, ConstantFilter(GT, 5)))
```

`StructFilter` 在三层发挥作用：

| 层 | 行为 |
|---|---|
| **统计信息剪枝** | `CheckStatistics(stats)` 递归提取子字段的 `BaseStatistics`，调用 `child_filter->CheckStatistics(child_stats)` |
| **Filter 重映射**（MultiFileColumnMapper） | `TryCastTableFilter` 通过 `MultiFileIndexMapping.child_mapping` 将 global child_idx 重新映射为 local child_idx |
| **运行时过滤** | `FilterSelection(STRUCT_EXTRACT)` 从 struct vector 中提取子 vector，递归应用 `child_filter` |

**在 Parquet reader 层**，DuckDB 把 struct 的每个子字段当作独立的列 reader。`root_reader.GetChildReader(column_id)` 可以拿到子字段的 reader，`Filter()` 时直接作用于 leaf column vector，StructFilter 在 `ApplyFilter` → `FilterSelection` 中被展开。

### 1.3 Doris 的实现方案

Doris 不需要完全照搬 DuckDB 的 StructFilter。因为 Doris 的 VExpr 已经能在 file block 上执行 `struct_element`，过滤的执行层不是问题。需要补齐的是**让 file reader 能感知到子字段级别的列依赖**。

#### Step 1: 扩展 `build_file_slot_rewrite_map` 遍历 child_mappings

当前函数只遍历 `_mappings`（顶级列）。需要改为同时遍历 `child_mappings`，将子字段的 `table_column_id` → `FileSlotRewriteInfo` 加入映射。子字段的 `block_position` 不同于父 struct——struct 在 file block 中是一个列（`ColumnStruct`），子字段通过 `ColumnStruct::get_column(child_idx)` 访问。

**关键问题**：子字段没有独立的 block_position。struct_element 在运行时从 struct 列中提取。所以子字段的 rewrite info 不能直接指向独立的 file block slot。

**替代方案**：不重写 slot_ref，而是**重写 struct_element 函数**。将 `struct_element(VSlotRef(struct_slot), 'field_name')` 替换为 `VSlotRef(child_slot)`，前提是子字段在 file block 中有独立位置。当前 Doris 的 struct 在 file block 中是展开的（每个投影子字段都有独立的 column），所以子字段确实有独立 block_position。

这意味着需要在 `rewrite_table_expr_to_file_expr` 中新增一个 fast path：匹配 `struct_element(VSlotRef(parent_slot), field_name)`，找到对应 `ColumnMapping.child_mappings`，替换为子字段的 `VSlotRef`（使用子字段在 file block 中的 block_position）。

#### Step 2: 扩展 `localize_filters` Phase 1 遍历子字段

当前 `filter_slot_ids()` 只收集裸 VSlotRef 的 slot_id。需要扩展为：对于 `struct_element(VSlotRef(parent), field_name)` 形式的表达式，收集子字段的 table_column_id。

然后在 Phase 1 中，对子字段也调用 `_find_mapping`（需要扩展为递归查找 `child_mappings`），将其加入 `predicate_columns`。

#### Step 3: literal 类型推断适配

`find_slot_rewrite_info` 当前要求 binary_predicate 的直接 child 是 VSlotRef（或 Cast(VSlotRef)）。对于 `struct_element(...) > 5`，第一个 child 是 function call。需要新增一个 fast path：识别 `struct_element(VSlotRef(parent), literal(name)) > literal` 模式，提取子字段的 file_type 并重写 literal。

### 1.4 影响范围

| 文件 | 改动 |
|---|---|
| `column_mapper.cpp` | `build_file_slot_rewrite_map` 递归遍历 child_mappings；`_find_mapping` 扩展为递归查找 child_mappings；新增 `struct_element` fast path |
| `table_reader.h` | `_build_table_filters_from_conjuncts` 扩展 slot_id 收集，识别 struct_element 内部引用的子字段 |
| 无需改动 parquet reader 内部 | 谓词的运行时执行路径不变 |

---

## 2. 统计信息过滤：复杂类型叶子列无法参与剪枝

### 2.1 问题根因

`parquet_statistics.cpp` 中有 4 处 `kind != PRIMITIVE` 检查，会在遇到复杂类型时直接跳过全部剪枝：

| 函数 | 行号 | 跳过的剪枝类型 |
|---|---|---|
| `RowGroupPruneReason` | 566 | Min/max + Dictionary |
| `BloomFilterPruneReason` | 293 | Bloom filter |
| `select_ranges_for_filter` | 889 | Page index |
| `supports_dictionary_pruning` | 363 | Dictionary（间接） |

根因是统计剪枝函数使用 `file_schema[column_filter.file_column_id]`（平铺索引），而 `file_column_id` 是顶级列索引（struct/list/map 的索引，其 `leaf_column_id = -1`）。

**DuckDB 的做法**：每个 leaf `ParquetColumnSchema` 有自己的 `column_index`（等价于 Doris 的 `leaf_column_id`），直接指向 `RowGroup.ColumnChunk[column_index]`。`PrepareRowGroupBuffer` 通过 `root_reader.GetChildReader(column_id)` 获取叶子 reader，`column_reader.Stats()` 返回该叶子列的统计信息。`StructColumnReader` 只是中间节点，其 `Stats()` 递归聚合子字段统计。

Doris 的 `leaf_column_id` 已经存在于每个叶子 `ParquetColumnSchema` 中。需要补齐的是：让统计剪枝函数能够从顶级列索引 + 子路径 → 定位叶子 `ParquetColumnSchema`。

### 2.2 实现方案

#### Step 1: 在 `FileColumnPredicateFilter` 中增加子字段路径

```cpp
struct FileColumnPredicateFilter {
    ColumnId file_column_id = -1;         // 顶级列索引（不变）
    std::vector<int> child_field_path;    // 新增：struct/map 内部子字段的 field_id 路径
    std::vector<std::shared_ptr<ColumnPredicate>> predicates;
};
```

`child_field_path` 为空表示过滤顶级原始列（现有行为不变）。非空表示过滤复杂类型内部的叶子列：
- `s.id` → `child_field_path = [0]`（struct 的第 0 个子字段 id）
- `m.value` → 对于 MAP，key_value 是第 0 个子（struct），value 是 struct 的第 1 个子 → `child_field_path = [0, 1]`

#### Step 2: 新增 schema tree 中的叶子定位函数

```cpp
// parquet_column_schema.h
// 沿 child_field_path 从顶级列定位到叶子 ParquetColumnSchema
const ParquetColumnSchema* resolve_leaf_schema(
    const std::vector<std::unique_ptr<ParquetColumnSchema>>& file_schema,
    ColumnId file_column_id,
    const std::vector<int>& child_field_path);
```

实现沿 `children` 逐级递进，返回叶子 `ParquetColumnSchema*`。

#### Step 3: 修改 4 处 PRIMITIVE 检查

将 `schema[column_filter.file_column_id]` 替换为 `resolve_leaf_schema(schema, column_filter.file_column_id, column_filter.child_field_path)`。解析成功后，`leaf_column_id`、`descriptor`、`type` 均可直接使用，后续统计逻辑无需修改。

```cpp
// 之前
const auto& column_schema = *schema[column_filter.file_column_id];
if (column_schema.kind != ParquetColumnSchemaKind::PRIMITIVE) { return NONE; }

// 之后
const auto* leaf_schema = resolve_leaf_schema(schema, column_filter.file_column_id,
                                               column_filter.child_field_path);
if (leaf_schema == nullptr || leaf_schema->kind != ParquetColumnSchemaKind::PRIMITIVE
    || leaf_schema->leaf_column_id < 0) { return NONE; }
// 使用 leaf_schema 替代 column_schema
```

#### Step 4: 在 `localize_filters` 中填充 `child_field_path`

当 `_find_mapping` 查找到的是子字段（在 child_mappings 中）时，沿 ColumnMapping 树向上回溯，构建 `child_field_path`。

### 2.3 限制

以下场景的统计剪枝仍然不可用（当前和改为后均不可用，不在此方案范围）：

- **LIST 元素**：`list[0] > 5` 形式的过滤不适用于行组级统计剪枝（元素级别的 min/max 不代表行组级别的 min/max）
- **MAP key/value**：`m['k'] > 5` 同理——映射到特定 key 的 value 没有行组级聚合统计
- **嵌套 LIST**：`List<List<INT>>` 中内层元素的统计没有行组级意义

### 2.4 影响范围

| 文件 | 改动 |
|---|---|
| `file_reader.h` | `FileColumnPredicateFilter` 增加 `child_field_path` 字段 |
| `parquet_column_schema.h/.cpp` | 新增 `resolve_leaf_schema()` |
| `parquet_statistics.cpp` | 4 处 PRIMITIVE 检查改为 `resolve_leaf_schema()` + 叶子判断 |
| `column_mapper.cpp` | `localize_filters` 中为子字段 filter 填充 `child_field_path` |
| `parquet_reader.cpp` | `open()` 中的列验证适配含有 child_field_path 的 filter |

---

## 3. 实施顺序

建议分两个独立 PR：

**PR 1: 谓词过滤**（Filter localization for nested fields）
- Step 1: `build_file_slot_rewrite_map` 递归 child_mappings
- Step 2: `localize_filters` 识别 struct_element，收集子字段 slot_id 并匹配 child_mappings
- Step 3: `struct_element` → 子字段 VSlotRef fast path
- 测试：`SELECT * FROM t WHERE s.id > 5` 验证谓词列被正确标记

**PR 2: 统计信息过滤**（Statistics pruning for nested leaf columns）
- Step 1: `FileColumnPredicateFilter` 增加 `child_field_path`
- Step 2: `resolve_leaf_schema()`
- Step 3: 4 处 PRIMITIVE 检查改为路径解析
- Step 4: `localize_filters` 填充 `child_field_path`
- 测试：验证 `s.id > 5` 能正确剪枝（准备只有 2 个 row group 的 parquet 文件，一个 `s.id` 全 < 5，一个全 > 5，验证前者被剪枝）

两个 PR 独立：PR 1 不依赖 PR 2（filter localization 可在没有统计剪枝时工作），但 PR 2 依赖 PR 1 的 `child_mappings` 遍历能力。
