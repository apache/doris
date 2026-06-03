# New Parquet Reader 后续任务

## 1. NestedScalarBatch::value_indices 删除状态

**状态：已完成。**

`NestedScalarBatch::value_indices` 和 `nested_scalar_value_index()` 已删除。当前实现不再为
每个 level slot 预计算 value index，而是在每个 scalar stream 上维护
`NestedScalarValueCursor`。

当前数据流：

```text
read_nested_leaf_batch()
  -> copy def_levels / rep_levels
  -> record value_slot_definition_level / value_slot_repetition_level
  -> materialize Doris-owned values_column

NestedScalarValueCursor
  -> 顺序扫描 def/rep level
  -> 对满足 value slot 阈值的 level 推进 compact value index

append_nullable_scalar_child()
  -> 用 leaf max definition level 判断是否写实际 value
  -> 写实际 value 时通过 cursor 定位 values_column[value_idx]

move_nested_scalar_tail()
  -> 用 cursor compact tail values_column
  -> overflow batch 从 value index 0 重新开始消费
```

实现原则：

1. `value_slot_definition_level` / `value_slot_repetition_level` 描述当前 scalar stream
   在 Dremel level 中对应哪些 value slots。
2. `leaf max_definition_level` 仍负责判断 nullable scalar child 是否写非空 value。
3. LIST scalar element、nested LIST scalar element、MAP key/value/list-element、STRUCT scalar child
   都使用独立 cursor，避免跨 stream 共享 value position。
4. overflow tail 只保存 tail levels 和 compact 后的 Doris-owned values column，不再保存
   level-to-value index 映射。

### 1.1 影响范围

| 文件 | 状态 |
|---|---|
| `nested_column_reader.h/.cpp` | 已新增 `NestedScalarValueCursor`，删除 `value_indices` / `nested_scalar_value_index()` |
| `arrow_leaf_reader_adapter.cpp` | 已删除 value index 构建逻辑，保留 def/rep copy 和 values materialization |
| `list_column_reader.cpp` | scalar LIST、nested LIST scalar path 已使用 cursor |
| `map_column_reader.cpp` | MAP key/value/list-element stream 已使用独立 cursor |
| `struct_column_reader.cpp` / `nested_column_reader.cpp` | STRUCT scalar child 已使用 per-child cursor |

### 1.2 验证

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
