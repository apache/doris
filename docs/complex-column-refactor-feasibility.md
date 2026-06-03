# 复杂列重构可行性分析

基于 [complex-column-reader-comparison.md](./complex-column-reader-comparison.md) 中的四个建议方向，逐一分析依赖、影响范围、风险和可行性。

## 1. MAP 复用 LIST<STRUCT<key,value>> 模式

### 1.1 依赖范围

`ParquetColumnSchemaKind::MAP` 的完整依赖链：

```
parquet_column_schema.h:43        MAP 枚举值定义
parquet_column_schema.cpp:53-58   is_map_node() 辅助函数
parquet_column_schema.cpp:176-237 设置 kind=MAP，构建子 schema，创建 DataTypeMap
column_reader.cpp:409-410        factory switch 中的 case MAP
column_reader.cpp:346-394        create_map_column_reader()
map_column_reader.h              MapColumnReader 类声明
map_column_reader.cpp:1-287      全量实现（read/skip）
nested_column_reader.h:509-543   NestedValueStream（仅 MAP 使用）
nested_column_reader.h:545-605   NestedScalarSlotStream（仅 MAP 使用）
nested_column_reader.h:607-658   RepeatedMapValueSink（仅 MAP 使用）
nested_column_reader.h:660-745   RepeatedMapListValueSink（仅 MAP 使用）
nested_column_reader.h:747-760   RepeatedAlignedValueSkipSink（仅 MAP 使用）
nested_column_reader.cpp:297-302 map_column_from_output()
```

全部 6 个 MAP 专属类型/函数在 `MapColumnReader` 之外**零调用**。删除路径非常干净。

### 1.2 唯一的技术障碍：ColumnMap vs ColumnArray 输出

这是整个方案中需要决策的关键点。

当前 `MapColumnReader::read()` 输出 `ColumnMap`（key_column + value_column + offsets + nulls）。如果改用 `ListColumnReader` 读取，输出将是 `ColumnArray(ColumnStruct(key, value))`。

DuckDB 的做法值得仔细理解，不能简单类比。

DuckDB 的 schema type 仍然是 `LogicalType::MAP`，只是 **reader 层复用 `ListColumnReader`**，因为 DuckDB MAP 的物理 Vector 表示和 LIST 兼容（都是 list-like，内部是 `ListVector`）。DuckDB 并不是"先输出 LIST<STRUCT<>> 再在 MultiFileReader 层转换"——它在语义层就是 MAP，物理层 list-like，不需要类型转换。

Doris 的 `ColumnMap` 和 `ColumnArray(ColumnStruct)` 是**物理上不同的 Column 类型**（`ColumnMap` 有独立的 key_column 和 value_column，`ColumnArray` 是 offsets + 单一嵌套 data column）。因此不能像 DuckDB 那样零成本复用。

**可行的实现方向**：

| 方案 | 做法 | 风险 |
|---|---|---|
| A. Reader 内部转换 | 复用 `ListColumnReader` / `assemble_repeated_levels` 处理 def/rep level 和 shape，但在构造 Column 的阶段直接输出 `ColumnMap`（将 key-value struct 拆分为 key + value column），而非经过中间 `ColumnArray(ColumnStruct)` | 需要在 ListColumnReader 或新增的 MapReader 中加 MAP 分支，但 shape 逻辑完全复用 |
| B. Reader 输出 + 后处理 | ListColumnReader 输出 `ColumnArray(ColumnStruct)`，然后通过专门的 materialization helper 转换为 `ColumnMap` | 多一次数据移动；`ColumnArray(Struct)` 和 `ColumnMap` 物理布局不同，不是 reinterpret cast 能完成的 |

方案 A 更务实：既复用了 def/rep level 的 shape 组装逻辑（这是 DuckDB 的实际收益——ListColumnReader 已经处理了所有 repeated level 场景），又在输出阶段直接生成 `ColumnMap`（不需要先构造中间 `ColumnArray` 再拆解）。

转换逻辑：
```
assemble_repeated_levels() 处理 def/rep 边界
  → 在每个 entry 边界：消费一个 key slot，消费 value slot(s)
  → 追加到 ColumnMap:
      key_column->insert(key_value)
      value_column->insert(value_slot)
      map_offsets->push_back(entry_count)
```

### 1.3 其他影响

- **DataTypeMap 构造**：Doris 输出语义仍保持 `DataTypeMap`。后续可以评估是否删除 `ParquetColumnSchemaKind::MAP`，但不应把 reader 主输出改成 `DataTypeArray(DataTypeStruct(key_type, value_type))`
- **Statistics/pruning**：无影响。当前所有 pruning 代码对 MAP 列直接跳过（检查 `kind != PRIMITIVE`）
- **key non-null 验证必须保留**：当前 `MapColumnReader` 在每个 entry 中检查 `def_level < key_max_definition_level` 则返回 `Status::Corruption`。Parquet schema 的 REQUIRED 标记不能替代 reader 对损坏/非法 def level 的运行时校验——文件可能被错误写入或损坏。即使重构后复用 ListColumnReader 的 shape 逻辑，也必须在 entry 边界处保留这个检查
- **MapColumnReader 的三个子路径**（标量值、结构体值、列表值）全部消失——它们等价于 ListColumnReader 的对应路径

### 1.4 可行性结论：可行，优先选择 reader 内复用 shape

- 删除范围明确：~400 行代码，6 个专属类型，零意外依赖
- 优先选择方案 A：在 reader 内复用 LIST/repeated shape 组装逻辑，输出阶段直接写 `ColumnMap`
- 不建议把 `ColumnArray(ColumnStruct)` 作为中间主路径；如果后续确实需要方案 B，必须实现显式 materialization helper，不能依赖 reinterpret cast 或现有 ColumnMapping projection

---

## 2. 删除 ShapeOnlyColumnReader（已完成）

### 2.1 依赖范围

| 引用 | 位置 |
|---|---|
| 构造 | 已删除 |
| 类定义 + 实现 | 已删除；`RowPositionColumnReader` 已拆到 `row_position_column_reader.h/.cpp` |
| `dynamic_cast<ShapeOnlyColumnReader*>` | **零处** |

### 2.2 替换分析

当前 `ShapeOnlyColumnReader` 包裹了一个未投影的非标量子 reader。它的 `read()` 和 `select()` 本质上就是 `skip()`——推进游标但不物化。

`StructColumnReader` 中对 `output_idx < 0` 的子 reader 的所有代码路径：

- **Branch 1（全标量）**：`output_idx < 0` → `continue`，不物化。数据已由 `read_nested_scalar_batch` 读取并丢弃，record reader 游标自然推进 ✓
- **Branch 2（混合）**：非标量 child 通过 `advance_non_scalar_struct_children()` 处理，其中 `output_idx < 0` → `child->skip(1)` ✓
- **Branch 3（无非标量）**：`output_idx < 0` → `child_reader->skip(rows)` ✓
- **`skip()`**：遍历所有 child 调用 `skip(rows)` ✓
- **`skip_non_scalar_children()`**：对非标量 child 调用 `skip(rows)` ✓

**所有路径都已经正确处理了无包裹的非标量 child reader。** ShapeOnlyColumnReader 的唯一附加价值是 `read()` 中的 `DCHECK(column == nullptr)` 断言——冗余保护，因为调用方不会为未投影 child 传 column。

### 2.3 已完成改动

```cpp
// column_reader.cpp — 未投影 child 保留原始 reader，只记录 output_idx=-1
if (!child_is_projected) {
    child_output_indices.push_back(-1);
}
```

未投影的非标量 child 保持原始类型（StructColumnReader / ListColumnReader / MapColumnReader），`output_idx = -1`。下游通过 `skip()` 推进游标。

### 2.4 可行性结论：可行，需补充 UT

- 唯一的构造点，零 `dynamic_cast` 依赖
- 下游所有 `output_idx < 0` 路径已正确调用 `skip()`
- 已补充的 UT：
  - 投影 struct 的 nested LIST child-only 后 read/skip/select
  - 投影 struct 的 nested MAP child-only 后 read/skip/select

---

## 3. 内联 Dremel 组装

### 3.1 当前 assemble_repeated_levels 的调用全景

共 12 个调用点，分布在 `list_column_reader.cpp`（6 个）和 `map_column_reader.cpp`（6 个）：

| # | 文件 | 场景 | Batch | Sink |
|---|---|---|---|---|
| 1 | list_column_reader.cpp:76 | LIST + 标量元素 read | NestedScalarBatch | RepeatedListValueSink<Scalar> |
| 2 | list_column_reader.cpp:94 | LIST + 结构体元素 read | NestedStructBatch | RepeatedListValueSink<Struct> |
| 3 | list_column_reader.cpp:145 | 嵌套 LIST read | NestedScalarBatch | RepeatedNestedListValueSink |
| 4 | list_column_reader.cpp:165 | LIST + 标量元素 skip | NestedScalarBatch | RepeatedShapeSkipSink |
| 5 | list_column_reader.cpp:171 | LIST + 结构体元素 skip | NestedStructBatch | RepeatedShapeSkipSink |
| 6 | list_column_reader.cpp:184 | 嵌套 LIST skip | NestedScalarBatch | RepeatedShapeSkipSink |
| 7-12 | map_column_reader.cpp | MAP 的各变体 | — | — |

如果 Change 1（删除 MAP）通过，MAP 的 6 个调用点消失，剩 6 个 LIST 调用点。

### 3.2 如果全部内联

对这 6 个 LIST 场景，分别内联为直接循环：

**LIST + 标量元素 read**（场景 1）：
```cpp
// 等价于 DuckDB ListColumnReader::ReadInternal 的 ~90 行核心逻辑
for (child_idx < levels_count) {
    if (rep_levels[child_idx] == max_repeat) {
        // 追加到当前列表的最后一个 entry
        entry_counts[result_offset - 1]++;
    } else if (def_levels[child_idx] >= element_slot_def_level) {
        // 新列表，有元素
        if (parent_null) set_null; else start_new_list(1);
        append_value(value);
    } else if (def_levels[child_idx] == element_slot_def_level - 1) {
        // 空列表
        if (parent_null) set_null; else start_new_list(0);
    } else {
        // NULL 列表
        set_null;
    }
}
```

**LIST + 结构体元素 read**（场景 2）— 类似，但 `append_value` 替换为对每个结构体子字段的 `append_struct_batch_value`。

**嵌套 LIST read**（场景 3）— 需要两级 offset 跟踪。

**Skip 变体**（场景 4-6）— 去掉值追加，只推进游标。

### 3.3 泛型模板的实际收益

当前泛型 `assemble_repeated_levels<Batch, Sink>` 的参数化维度：

- `Batch`：`NestedScalarBatch` 或 `NestedStructBatch`（两种）
- `Sink`：`RepeatedListValueSink` / `RepeatedNestedListValueSink` / `RepeatedShapeSkipSink`（三种 LIST 场景）

当 MAP 被删除后，Batch × Sink 组合从 12 种降到 6 种，其中 read 路径仅 3 种。这 3 种 read 路径的实际差异在于：
- 值追加方式（标量 append vs 结构体 append vs 嵌套 list append）
- 是否跳过（shape-only）

这些差异可以在 `ListColumnReader::read()` 中用 ~150 行分支代码表达，不需要泛型模板。

### 3.4 可行性结论：可行，但建议作为独立 PR

- MAP 删除后，`assemble_repeated_levels` 的泛型抽象收益大幅降低
- 内联后的代码更接近 DuckDB，可读性和可调试性提升
- 风险：涉及 ListColumnReader 的核心 read/skip 路径，需要充分的嵌套类型测试覆盖
- 建议：在 Change 1 和 Change 2 之后单独进行，避免一次改动过大

---

## 4. 简化叶子层集成

### 4.1 当前两条路径

**路径 A — 扁平标量**（`ScalarColumnReader::read()`）：
```
RecordReader::ReadRecords() → append_leaf_values() → Column
```
不经过 `NestedScalarBatch`。✓ 已经是最简路径。

**路径 B — 嵌套上下文中的叶子**（`read_nested_leaf_batch()`）：
```
RecordReader::ReadRecords()
  → 复制 def_levels[] / rep_levels[]
  → 构建 value_indices[]（过滤不属于此嵌套槽位的值）
  → append_leaf_values() → NestedScalarBatch.values_column
```
然后上层 `assemble_repeated_levels` 通过 `value_indices` 和 `def_levels` 消费。

### 4.2 优化空间

`read_nested_leaf_batch` 的主要开销是构建 `value_indices`——将每个 level 映射到一个 value 位置。这个映射对于 Dremel 组装是必要的：当重复组中有多个元素时，rep levels 编码了元素边界，value_indices 确保每个 level 取到正确的值。

在 DuckDB 中，这个映射是隐式的——RecordReader 返回的 def/rep levels 和 values 是同步的，不需要额外的 value_indices 数组。`ListColumnReader::ReadInternal` 直接索引 `child_idx`（等价于 level_idx）。

**可行优化**：
- `value_indices` 的构建可以与 `assemble_repeated_levels` 的消费合并：在遍历 def/rep levels 时同步消费 values，而不是先构建完整的 value_indices 再遍历
- 这与 Change 3（内联 Dremel 组装）是同一个改动——内联后的循环可以同步访问 levels 和 values

### 4.3 对 NestedScalarBatch 的影响

如果 Change 3 通过，`NestedScalarBatch` 的用途变为：
- 仅作为 `read_nested_leaf_batch` 的输出载体
- `assemble_repeated_levels` 被内联后不再需要 Batch 抽象

此时 `NestedScalarBatch` 可以进一步简化为：
- `def_levels` + `rep_levels`（**必须拷贝**，因为 Arrow `RecordReader::ReadRecords()` 后内部 buffer 会被下一次 read 覆盖，不能引用内部 buffer）
- `values_column`（解码后的值，可直接为 Column）

废弃 `value_indices`、`levels_written`、`records_read`、`values_written` 等冗余字段。`def_levels`/`rep_levels` 的拷贝仍然保留——这是与 Arrow RecordReader 生命周期解耦的必要代价。

### 4.4 可行性结论：可随 Change 3 一并实施

- 扁平标量路径已是最优，无需改动
- 嵌套路径的优化是 Change 3 的自然延伸
- 不建议独立进行（依赖内联 Dremel 组装先完成）

---

## 5. 总体评估

### 5.1 建议的改动顺序

```
Phase 1: Change 2 — 删除 ShapeOnlyColumnReader 包装层
         状态: 已完成
         文件: column_reader.cpp, row_position_column_reader.h/.cpp

Phase 2: Change 1 — MAP 复用 LIST<STRUCT<>>
         影响: 中，复用 LIST/repeated shape，但输出阶段直接写 ColumnMap
         文件: parquet_column_schema.h/.cpp, column_reader.h/.cpp,
               map_column_reader.h/.cpp (删除), nested_column_reader.h (删 200 行)

Phase 3: Change 3+4 — 内联 Dremel + 简化叶子集成
         影响: 大，重写 ListColumnReader 核心路径
         文件: list_column_reader.h/.cpp, nested_column_reader.h/.cpp (大幅缩减),
               arrow_leaf_reader_adapter.cpp
```

### 5.2 每个 Phase 的依赖关系

```
Phase 1 (ShapeOnly) ──→ 无依赖，可独立进行，但必须覆盖 read/skip/select

Phase 2 (MAP→LIST)  ──→ 可独立进行（不依赖 Phase 1 但建议先做 Phase 1）
                        保持 DataTypeMap 输出，reader 内复用 repeated shape

Phase 3+4 (内联+简化) ──→ 依赖 Phase 2
                         MAP 删除后 Sink 变体减半，内联才有足够收益
```

### 5.3 风险和缓解

| 风险 | 缓解 |
|---|---|
| Phase 2 错误引入 ColumnArray→ColumnMap 中间路径 | 首选 reader 内直接写 ColumnMap；如必须后处理，使用显式 materialization helper，并承认额外列拆分/数据移动成本 |
| Phase 3 内联可能引入 def/rep level 处理 bug | DuckDB 的 90 行逻辑已在 production 验证，可直接参考。需充分测试嵌套 LIST、nullable LIST、空 LIST 等边界 |
| Phase 3 删除 NestedScalarBatch 影响其他调用方 | grep 确认 `NestedScalarBatch` 的消费者全部在 nested_column_reader 和 struct/list/map reader 内部，无外部依赖 |
| 测试覆盖 | 需要在修改前确认现有回归测试中复杂类型的覆盖率 |

### 5.4 收益预估

| Phase | 删除行数 | 新增行数 | 净变化 |
|---|---|---|---|
| Phase 1 | ~190 行 (ShapeOnlyColumnReader) | ~90 行 (RowPosition split) | **~-100** |
| Phase 2 | ~400 行 (MapColumnReader + MAP Sinks/Streams) | ~50 行 (Array→Map 转换 + schema 构建调整) | **-350** |
| Phase 3+4 | ~700 行 (泛型模板 + NestedScalarBatch 冗余字段) | ~200 行 (内联循环) | **-500** |
| **合计** | **~1290** | **~250** | **~-1040** |

整体复杂度：从 2745 行降到 ~1700 行，消除 MAP 概念、消除泛型模板抽象、消除 ShapeOnly 装饰器。

### 5.5 整体可行性：可行

三个 Phase 相互解耦，可逐步推进。Phase 1 已完成。Phase 2 的目标是保留 Doris `DataTypeMap` 输出语义，在 reader 内复用 LIST/repeated shape 逻辑，避免 `ColumnArray(ColumnStruct)` 中间主路径。Phase 3 工作量最大但收益也最大，需要在 Phase 2 完成后独立评估。
