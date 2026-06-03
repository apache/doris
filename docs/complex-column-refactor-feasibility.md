# New Parquet Reader 后续任务

## 1. 删除 NestedScalarBatch::value_indices

### 1.1 背景

`NestedScalarBatch::value_indices` 是一个 `vector<int64_t>`，在
`arrow_leaf_reader_adapter.cpp` 中预计算 level slot 到 `values_column` 中 value
位置的映射。它存在的根本原因是 Parquet Dremel level stream 和 Doris-owned
compact values column 不是一一对应：

- null parent、empty parent 不产生 value。
- nullable scalar child 的 null slot 不产生 value。
- repeated/nested 场景中，某些 level slot 只表达 shape，不表达当前 value stream 的值。

LIST/MAP Dremel 组装已经内联后，reader 循环可以在遍历 def/rep levels 的同时推进
value cursor。因此 `value_indices` 可以被删除，但不能简单把 `level_idx` 替换成
`value_idx`。必须先把“当前 scalar stream 的 value 消费位置”显式建模，否则
nullable child、STRUCT child batch、MAP key/value 多 stream 和 overflow tail 都容易错位。

### 1.2 当前数据流

```
read_nested_leaf_batch()
  -> copy def_levels / rep_levels
  -> materialize compact values_column
  -> build value_indices[level_idx] -> value_idx

append_scalar_batch_value()
  -> nested_scalar_value_index(batch, level_idx)
  -> values_column[value_idx]

move_nested_scalar_tail()
  -> nested_scalar_value_index(src, level_idx)
  -> compact tail values_column + tail value_indices
```

当前 `value_indices` 已有一个小优化：当 `values_written == levels_written` 且没有
repetition filter 时，`value_indices` 为空，`nested_scalar_value_index()` 直接把
`level_idx` 当作 `value_idx`。后续目标是把 sparse 场景也改成 cursor 消费，从而彻底删除
`value_indices`。

### 1.3 设计原则

删除 `value_indices` 前必须满足以下约束：

1. **value presence 使用 leaf max definition level 判断**
   对 nullable scalar child，`def >= element_slot_definition_level` 只代表 child slot 存在，
   不代表有实际 value。只有 `def == leaf_max_definition_level` 才消费 scalar value。

2. **repetition filter 必须保留**
   STRUCT 混合 child、MAP list value 等场景会使用 `value_slot_repetition_level` 限定当前
   stream 应消费哪些 level slot。cursor 判断 actual value 时必须同时满足：

   ```cpp
   def_level == leaf_max_definition_level &&
   rep_level <= value_slot_repetition_level
   ```

3. **每个 scalar stream 独立维护 cursor**
   MAP 至少有 key stream 和 value stream；`Map<K, List<V>>` 有 key stream 和 list element
   stream；STRUCT 每个 scalar child 都是独立 value stream。不能用一个全局 `value_idx`。

4. **overflow tail 必须保持 compact 后的 cursor 语义**
   overflow batch 的 `values_column` 是 tail compact 后的 Doris-owned column。下一次消费
   overflow 时，cursor 应从 value index 0 开始，而不是沿用原 batch 的全局 value index。

### 1.4 新增 NestedScalarValueCursor

先新增 cursor，不立即删除 `value_indices`，让新旧路径可以分阶段切换。

```cpp
class NestedScalarValueCursor {
public:
    NestedScalarValueCursor(const NestedScalarBatch* batch,
                            int16_t value_definition_level,
                            int16_t value_repetition_level);

    bool level_has_value(int64_t level_idx) const;

    Status append_value(const ScalarColumnReader& reader,
                        int64_t level_idx,
                        MutableColumnPtr& column);

    Status append_nullable_value(const std::string& column_name,
                                 std::string_view parent_kind,
                                 std::string_view child_kind,
                                 const ScalarColumnReader& reader,
                                 int64_t level_idx,
                                 MutableColumnPtr& column);

    void skip_level(int64_t level_idx);
    void reset();

private:
    const NestedScalarBatch* _batch = nullptr;
    int16_t _value_definition_level = 0;
    int16_t _value_repetition_level = 0;
    int64_t _next_value_idx = 0;
};
```

语义：

- `level_has_value()` 只判断当前 level 是否实际消费 scalar value。
- `append_value()` 只允许在 `level_has_value(level_idx)` 为 true 时调用，并消费一个 value。
- `append_nullable_value()` 在有 value 时 append value，否则对 nullable child append default；required child 出现 null 返回 `Corruption`。
- `skip_level()` 用于 shape-only 或未投影 child，只有当前 level 有实际 value 时推进 cursor。

### 1.5 分阶段改动

#### Step 1: 引入 cursor，保留 value_indices fallback

新增 `NestedScalarValueCursor`，内部暂时仍可通过 `nested_scalar_value_index()` 校验或 fallback。
这一步只改变调用方式，不删除字段。

目标：

- 明确 value cursor 的生命周期。
- 把 `append_scalar_batch_value()` 的调用方逐步改成 cursor append。
- dense/sparse 行为先保持等价。

#### Step 2: LIST scalar / nested LIST scalar 切换到 cursor

修改：

- `read_scalar_list_values()`
- `read_nested_list_values()`
- scalar LIST skip 路径

注意：

- null list、empty list 不消费 value。
- nullable element 的 null slot 不消费 value，但需要 append null child。
- repeated element 继续属于当前 parent row，不能因为 output rows 满而拆到 overflow。

#### Step 3: MAP scalar value / key stream 切换到 cursor

修改：

- `MapValueSink` 中 key stream cursor 和 value stream cursor 分离。
- `MapListValueSink` 中 key cursor 与 list element cursor 分离。
- MAP skip sink 同样走 cursor skip，不能依赖 child value-level skip。

注意：

- key 必须 defined；key cursor 只在 entry slot 消费。
- value nullable 时，null value 不消费 value cursor。
- `Map<K, List<V>>` 中 map entry shape 和 list value element shape 是两层 cursor，不能混用。

#### Step 4: STRUCT scalar child batch 切换到 per-child cursor

修改：

- `append_struct_batch_value()` 内为每个 scalar child batch 使用独立 cursor。
- `StructColumnReader` all-scalar path 也使用 child cursor。
- 未投影 scalar child 仍需要推进对应 cursor，保持 child stream 对齐。

注意：

- `List<Struct<nullable child>>` 和 `Map<K, Struct<nullable child>>` 是必须覆盖的场景。
- 不能通过从头扫描 def/rep 计算 value_idx，否则会把 append 变成 O(n^2)。

#### Step 5: cursor 化 overflow tail compact

`move_nested_scalar_tail()` 不再查询 `value_indices`，而是接收 value presence 参数：

```cpp
void move_nested_scalar_tail(const NestedScalarBatch& src,
                             int64_t start_level,
                             int16_t value_definition_level,
                             int16_t value_repetition_level,
                             NestedScalarOverflow* overflow);
```

tail compact 规则：

```cpp
for each level before start_level:
    if has_value(level): ++src_value_idx

for each level from start_level:
    if has_value(level):
        dst.values_column->insert_from(*src.values_column, src_value_idx++)
```

compact 后的 overflow batch 不需要继承原 batch 的 value index；下一次 cursor 从 0 开始。

#### Step 6: 删除 value_indices

所有 LIST/MAP/STRUCT read/skip/select 和 overflow 路径都切换到 cursor 后，删除：

- `NestedScalarBatch::value_indices`
- `nested_scalar_value_index()`
- `arrow_leaf_reader_adapter.cpp` 中 value_indices 构建逻辑

### 1.6 影响范围

| 文件 | 改动 |
|---|---|
| `nested_column_reader.h/.cpp` | 新增 `NestedScalarValueCursor`；后续删除 `value_indices` 和 `nested_scalar_value_index()` |
| `arrow_leaf_reader_adapter.cpp` | 最后阶段删除 value_indices 构建；保留 def/rep copy 和 values materialization |
| `list_column_reader.cpp` | scalar LIST、nested LIST read/skip 使用 cursor |
| `map_column_reader.cpp` | key/value/list-element 多 stream 使用独立 cursor；overflow tail move 传 value presence 参数 |
| `struct_column_reader.cpp` / `nested_column_reader.cpp` | STRUCT scalar child 使用 per-child cursor |

### 1.7 验证

实现完成后必须满足：

1. `rg -n "value_indices|nested_scalar_value_index" be/src/format/new_parquet/reader` 结果为空。
2. `git diff --check` 通过。
3. Fedora `BUILD_TYPE=DEBUG ./build.sh --be` 通过。
4. `./run-be-ut.sh --run '--filter=ParquetColumnReaderTest.*'` 通过。

## 2. 测试补充

| 场景 | 优先级 |
|---|---|
| nullable list element：`[null, 1]`、`[1, null]`、empty list、null list | P0 |
| nullable map value：`{k:null}`、`{k:v}`、empty map、null map | P0 |
| `List<Struct<nullable child>>` read/skip/select | P0 |
| `Map<K, Struct<nullable child>>` read/skip/select | P0 |
| `Map<K, List<nullable V>>` small batch read + overflow | P0 |
| `skip()` / `select()` 跨 long list/map/struct，验证 cursor 与 overflow 一致 | P0 |
| Struct 子字段上的 conjunct 过滤：`SELECT * FROM t WHERE s.id > 5` | P1 |
| 复杂列的 `select()` 路径：非谓词复杂列在过滤后通过 select 读取 | P1 |
| 投影 + 过滤交互：`SELECT s.b FROM t WHERE s.a > 0` | P1 |

## 3. 已知限制

- **嵌套字段过滤无法下推到 Parquet 层**：`build_file_slot_rewrite_map()` 仅映射顶级列，不遍历 `child_mappings`。需要参照 DuckDB 的 `StructFilter` 机制改动 `VExpr`/`TableFilter` 体系，不属于 complex column reader 重构范围。
