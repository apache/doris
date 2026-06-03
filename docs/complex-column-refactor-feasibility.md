# New Parquet Reader 后续任务

## 1. 删除 NestedScalarBatch::value_indices

### 1.1 背景

`value_indices` 是一个 `vector<int64_t>`，在 `arrow_leaf_reader_adapter.cpp:280-296` 中预计算：为每个 level 映射到 `values_column` 中的 value 位置。映射需要的原因是：对于 REPEATED 字段，并非每个 level 都对应一个值（空列表/结构边界不产生值）。

内联 Dremel 后，LIST/MAP 的内联循环已经显式检查 `def_level` 来决定当前 level 是否有值。但 `append_scalar_batch_value()` 仍然通过 `nested_scalar_value_index(batch, level_idx)` 间接查找 value 位置——而这个查找完全可以被调用方已经知道的 value_idx 替代。

### 1.2 当前 value_indices 的数据流

```
arrow_leaf_reader_adapter.cpp:277-296
  当 values_written != levels_written 时，预计算 value_indices[level_idx] → value_idx
  当 values_written == levels_written 时，value_indices 为空（dense 情况）

nested_scalar_value_index(batch, level_idx) → batch.value_indices[level_idx] 或 level_idx

消费者：
  append_scalar_batch_value()       → nested_scalar_value_index(batch, level_idx)
  move_nested_scalar_tail()         → nested_scalar_value_index(src, level_idx)
  MAP value stream read_aligned     → move_nested_scalar_tail()
```

### 1.3 改动

**Step 1: 改 `append_scalar_batch_value()` 接口，接收显式 `value_idx`**

```cpp
// 之前
Status append_scalar_batch_value(const ScalarColumnReader& column_reader,
                                 const NestedScalarBatch& batch, int64_t level_idx,
                                 MutableColumnPtr& column);

// 之后
Status append_scalar_batch_value(const ScalarColumnReader& column_reader,
                                 const NestedScalarBatch& batch, int64_t value_idx,
                                 MutableColumnPtr& column);
```

实现中删除 `nested_scalar_value_index(batch, level_idx)` 调用，直接用传入的 `value_idx`：
```cpp
// 之前
const int64_t value_idx = nested_scalar_value_index(batch, level_idx);
if (value_idx < 0) { ... }
column->insert_from(*batch.values_column, static_cast<size_t>(value_idx));

// 之后
column->insert_from(*batch.values_column, static_cast<size_t>(value_idx));
```

value_idx 的合法性（>=0）由调用方保证——调用方只在 `def_level >= element_slot_definition_level` 时才调用 append，此时必然有对应的 value。

**Step 2: LIST/MAP 内联函数中跟踪 value_idx**

以 `read_scalar_list_values()` 为例，当前每个 level 的处理：

```cpp
// 当前：通过 value_appender.append(batch, level_idx, ...) 内部 lookup
value_appender.append(column_name, *batch, level_idx, element_column);

// 之后：调用方跟踪 value_idx，直接传入
value_appender.append(column_name, *batch, value_idx, element_column);
++value_idx;
```

需要修改的函数（共 ~8 处）：
- `read_scalar_list_values()` — `list_column_reader.cpp:99,113`
- `read_struct_list_values()` — 结构体元素的 append 不经过 value_indices（走 `append_struct_batch_value`），不受影响
- `read_nested_list_values()` — `list_column_reader.cpp:222` 的 `NestedScalarValueAppender`
- MAP 中所有通过 `append_scalar_batch_value()` 的路径 — `map_column_reader.cpp:322,380`

**Step 3: 删除 `value_indices` 构建和查询**

- `NestedScalarBatch::value_indices` 字段删除
- `arrow_leaf_reader_adapter.cpp:277-296` 删除（value_indices 预计算循环）
- `nested_scalar_value_index()` 删除
- `nested_column_reader.h:130-135` 删除

**Step 4: 简化 `move_nested_scalar_tail()`**

当前分两条路径：dense（value_indices 为空，level_idx 即 value_idx）和 non-dense（需要查 value_indices）。删除 value_indices 后，tail move 需要知道哪些 level 有值。

方案：`move_nested_scalar_tail` 增加参数 `int16_t value_slot_definition_level`，内部通过检查 `def_level >= value_slot_definition_level` 来判断：

```cpp
inline void move_nested_scalar_tail(const NestedScalarBatch& src, int64_t start_level,
                                    int16_t value_slot_definition_level,
                                    NestedScalarOverflow* overflow) {
    NestedScalarBatch dst;
    dst.levels_written = src.levels_written - start_level;
    dst.def_levels.assign(src.def_levels.begin() + start_level, src.def_levels.end());
    dst.rep_levels.assign(src.rep_levels.begin() + start_level, src.rep_levels.end());
    dst.values_column = src.values_column->clone_empty();
    int64_t value_idx = 0;  // count values before start_level
    for (int64_t i = 0; i < start_level; ++i) {
        if (src.def_levels[i] >= value_slot_definition_level) ++value_idx;
    }
    for (int64_t i = start_level; i < src.levels_written; ++i) {
        if (src.def_levels[i] >= value_slot_definition_level) {
            dst.values_column->insert_from(*src.values_column, value_idx++);
        }
    }
    overflow->batch = std::move(dst);
}
```

或者更简单：因为 `move_nested_scalar_tail` 的所有调用方（LIST skip 路径）都已知道 `value_slot_definition_level`，直接传递即可。

**Step 5: 更新 MAP value stream 中的 tail move**

`map_column_reader.cpp` 中 MAP value stream 的 `move_tail_from_driver_overflow` 调用了通用的 `move_nested_tail` → `move_nested_scalar_tail`。需要传入 `value_slot_definition_level`。

### 1.4 影响范围

| 文件 | 改动 |
|---|---|
| `nested_column_reader.h` | 删除 `value_indices` 字段、`nested_scalar_value_index()`；`move_nested_scalar_tail` 增加参数、简化实现 |
| `arrow_leaf_reader_adapter.cpp` | 删除 value_indices 构建循环（~20 行） |
| `list_column_reader.cpp` | `read_scalar_list_values` / `read_nested_list_values` 增加 value_idx 跟踪（~10 行） |
| `map_column_reader.cpp` | 所有 `append_scalar_batch_value` 调用点传入显式 value_idx（~6 处） |
| `nested_column_reader.cpp` | `append_scalar_batch_value`、`append_nullable_scalar_child` 接口改为接收 value_idx |

### 1.5 验证

1. `grep -rn "value_indices" be/src/format/new_parquet/reader/` 结果为空
2. `grep -rn "nested_scalar_value_index" be/src/format/new_parquet/` 结果为空
3. 已有测试回归通过（`parquet_column_reader_test` 中的嵌套类型 read/skip 测试、`list_column_reader` 的 scalar/struct/nested 路径、`map_column_reader` 的 scalar/struct/list value 路径）

---

## 2. 测试补充

| 场景 | 优先级 |
|---|---|
| Struct 子字段上的 conjunct 过滤：`SELECT * FROM t WHERE s.id > 5` | P1 |
| 复杂列的 `select()` 路径：非谓词复杂列在过滤后通过 select 读取 | P1 |
| 投影 + 过滤交互：`SELECT s.b FROM t WHERE s.a > 0` | P1 |

## 3. 已知限制

- **嵌套字段过滤无法下推到 Parquet 层**：`build_file_slot_rewrite_map()` 仅映射顶级列，不遍历 `child_mappings`。需要参照 DuckDB 的 `StructFilter` 机制改动 `VExpr`/`TableFilter` 体系，不属于 complex column reader 重构范围。
