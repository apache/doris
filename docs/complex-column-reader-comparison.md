# Complex Column Reader: DuckDB vs Doris New Parquet Reader

本文比较 DuckDB 和 Doris new parquet reader 在处理复杂类型（STRUCT / LIST / MAP）时的设计差异。

## 1. 整体架构对比

### DuckDB

```
ParquetReader
  └── root_schema (ParquetColumnSchema 树)
  └── CreateReaderRecursive(schema, column_indexes) → ColumnReader 树

ColumnReader (base)
  ├── TemplatedColumnReader<T>       标量叶子
  ├── StructColumnReader             vector<ColumnReader> children (nullptr = 未投影)
  ├── ListColumnReader               ColumnReader child (也用于 MAP)
  ├── RowNumberColumnReader          虚拟列
  └── ExpressionColumnReader         通过表达式转换的包装器
```

- Reader 树直接镜像 schema 树
- **MAP = LIST<STRUCT<key, value>>**，ListColumnReader 统一处理 LIST 和 MAP
- 投影：未选中的 child 在 `StructColumnReader::child_readers` 中为 `nullptr`
- Def/rep level 处理：**内联在各类 Reader 的 read() 方法中**，无独立的"Dremel 组装器"层
- 叶子 Reader 使用自研 page/header/level/value decoder（RleBpDecoder、dictionary_decoder、delta_*、byte_stream_split_decoder 等），不依赖 Arrow

### Doris

```
ParquetReader
  └── ParquetScanScheduler
        └── ParquetColumnReaderFactory.create(schema, projection) → ParquetColumnReader 树

ParquetColumnReader (base)
  ├── ScalarColumnReader             标量叶子
  ├── StructColumnReader             vector<ParquetColumnReader> children + vector<int> child_output_indices
  ├── ListColumnReader               ParquetColumnReader element_reader
  ├── MapColumnReader                key_reader + value_reader
  └── RowPositionColumnReader        虚拟列

辅助层:
  NestedColumnReader (nested_column_reader.h/cpp, 1200+ 行)
    ├── NestedScalarBatch / NestedStructBatch   捕获 def/rep levels + values
    ├── NestedScalarOverflow / NestedStructOverflow   跨批次溢出
    ├── assemble_repeated_levels<Batch, Sink>()    通用 Dremel 组装模板
    ├── NestedShapeCursor                          def/rep level 遍历器
    ├── RepeatedListValueSink / RepeatedMapValueSink   各类型的接收器
    └── NestedValueStream / NestedScalarSlotStream      对齐流
```

- Reader 树镜像 schema 树，但有额外的 **Dremel 组装抽象层**
- MAP 有独立的 `MapColumnReader`（不是 LIST<STRUCT<>> 的语法糖）
- 投影：未选中 child 保留原始 reader，`StructColumnReader` 通过 `child_output_indices=-1` 只推进游标不物化
- Def/rep level 处理：提取到 `nested_column_reader` 的泛型模板中
- 叶子通过 `ArrowLeafReaderAdapter` 包装 Arrow RecordReader

## 2. 类型系统：MAP 的处理

这是最显著的架构差异。

### DuckDB：MAP = LIST<STRUCT<key,value>>

```cpp
// ParseSchemaRecursive 中 (parquet_reader.cpp:574-589)
if (is_map_kv) {
    // 内部 STRUCT(key, value) 作为 LIST 的 child
    auto result_type = LogicalType::MAP(key_type, value_type);
    ParquetColumnSchema struct_schema(...);  // key-value 对
    struct_schema.children = {key_schema, value_schema};
    ParquetColumnSchema map_schema(...);     // LIST 包装
    map_schema.children.push_back(std::move(struct_schema));
}
```

Schema 树：
```
MAP  (ListColumnReader)
  └── STRUCT (StructColumnReader)   ← key_value 对
        ├── key    (TemplatedColumnReader)
        └── value  (TemplatedColumnReader)
```

读取时只用 `ListColumnReader` + `StructColumnReader` 两个已有的 Reader 类型，**零新增 Reader 代码**。

### Doris：MAP 独立 Reader

```cpp
// column_reader.cpp:create_map_column_reader()
// MAP 有自己的 schema 节点
ParquetColumnSchema: MAP kind, children = [key_value STRUCT]
  └── key_value STRUCT: children = [key, value]
```

读取时 `MapColumnReader` 持有独立的 `_key_reader` 和 `_value_reader`，通过 `RepeatedMapValueSink` 用**键驱动**的方式组装。这引入了：
- `NestedValueStream` — 使值流对齐到键的 rep level
- `NestedScalarSlotStream` — 在值驱动模式下逐槽位消费键
- `RepeatedMapListValueSink` — 值驱动模式的特殊接收器

## 3. Def/Rep Level 组装

### DuckDB：内联处理

```cpp
// ListColumnReader::ReadInternal — 核心列表组装逻辑 (~90 行)
for (child_idx = 0; child_idx < child_count; child_idx++) {
    if (child_repeats_ptr[child_idx] == MaxRepeat()) {
        // 重复 → 追加到当前列表
        HandleRepeat(data, result_offset - 1);
    } else if (child_defines_ptr[child_idx] >= MaxDefine()) {
        // 完整定义 → 新列表，有元素
        HandleListStart(data, result_offset, offset_in_child, 1);
    } else if (child_defines_ptr[child_idx] == MaxDefine() - 1) {
        // 列表级定义 → 空列表
        HandleListStart(data, result_offset, offset_in_child, 0);
    } else {
        // 上层未定义 → NULL 列表
        HandleNull(data, result_offset);
    }
}
```

特点：
- 逻辑直接嵌入 `ListColumnReader::Read()`，无中间抽象
- 通过模板 `TemplatedListReader` / `TemplatedListSkipper` 区分读和跳
- 总代码量：`list_column_reader.cpp` ~170 行

### Doris：泛型 Dremel 组装器

```cpp
// nested_column_reader.h — assemble_repeated_levels() 模板 (~50 行核心)
template <Batch, Overflow, ReadBatchFn, MoveTailFn, Sink>
assemble_repeated_levels(column_name, repeated_level, rows, overflow,
                         read_batch, move_tail, sink, rows_read) {
    // 1. 优先消费 overflow
    // 2. NestedShapeCursor 遍历 def/rep levels
    //    rep_level < repeated_level  → sink.start_parent()
    //    rep_level >= repeated_level → sink.append_repeated()
    // 3. 尾部移入 overflow
}
```

特点：
- Dremel 算法提取为泛型模板，通过 Sink 参数化 LIST/MAP 行为
- `NestedScalarBatch` / `NestedStructBatch` 作为中间载体，捕获原始 def/rep levels + values
- 引入了 overflow 机制处理跨批次边界
- 总代码量：`nested_column_reader.h` 789 行 + `.cpp` 426 行 = **1215 行**

## 4. 投影机制

### DuckDB：nullptr

```cpp
// CreateReaderRecursive — 未投影 child 留为 nullptr
if (indexes.empty()) {
    for (all children) children[i] = CreateReader(...);
} else {
    for (selected indexes) children[index] = CreateReader(...);
    // 未选择的 → 保持 nullptr
}

// StructColumnReader::Read — nullptr child → 常量 NULL 向量
if (!child_readers[i]) {
    auto &child_vector = result_struct_vectors[i];
    child_vector.SetVectorType(VectorType::CONSTANT_VECTOR);
    ConstantVector::SetNull(child_vector, true);
}
```

### Doris：output_idx=-1 + skip

```cpp
// create_struct_column_reader — 未投影 child 保留原始 reader，但不分配输出 slot
if (!child_is_projected) {
    child_output_indices.push_back(-1);
}

// StructColumnReader — output_idx < 0 的 child 只 skip，不写入 ColumnStruct
if (output_idx < 0) {
    RETURN_IF_ERROR(child_reader->skip(rows));
}
```

差异：DuckDB 用 `nullptr` 标记未投影 child；Doris 保留 child reader，用 `child_output_indices=-1` 表示不物化，并在 read/skip/select 路径中显式推进游标。

## 5. 叶子层集成

### DuckDB：自研 Decoder

ColumnReader::CreateReader() 根据不同物理类型创建对应的 TemplatedColumnReader，后者在 Read() 中直接调用自研的 page/header/level/value decoder（`RleBpDecoder`、`dictionary_decoder`、`delta_*`、`byte_stream_split_decoder` 等），将值写入 DuckDB Vector。无中间适配层。

### Doris：ArrowLeafReaderAdapter

```cpp
// arrow_leaf_reader_adapter.cpp — 桥接 Arrow RecordReader → NestedScalarBatch
read_nested_leaf_batch(context, batch_rows, value_slot_definition_level,
                       batch, value_slot_repetition_level) {
    // 1. context.record_reader->ReadRecords(batch_rows)
    // 2. 复制 def_levels / rep_levels → batch
    // 3. 构建 value_indices (过滤不属于此嵌套槽位的值)
    // 4. append_leaf_values() 将值写入 batch.values_column
}
```

Doris 多了一层：Arrow RecordReader → NestedScalarBatch → Column。对于扁平列（ScalarColumnReader），`read()` 方法只取 `values_column` + null_map。对于嵌套列，`NestedScalarBatch` 的 def/rep levels + value_indices 被 `assemble_repeated_levels` 消费。

## 6. 代码量对比

| 模块 | DuckDB | Doris |
|---|---|---|
| Struct reader | ~70 行 | ~380 行 (.h + .cpp) |
| List reader | ~170 行 | ~230 行 |
| Map reader | **无**（复用 List+Struct） | ~340 行 |
| Dremel 组装器 | **无**（内联在 reader 中） | ~1215 行 |
| Arrow 适配层 | **无**（自研 decoder） | ~390 行 |
| Shape only reader | **无**（nullptr 标记） | ~190 行 |
| **合计** | **~240 行** | **~2745 行** |

> 注：DuckDB 行数少部分原因是它把 page 解码/字典/Delta 等也内联在 TemplatedColumnReader 中，但即使只看复杂类型组装部分，差距仍然显著。

## 7. 关键设计差异总结

| 维度 | DuckDB | Doris |
|---|---|---|
| **Dremel 组装位置** | 内联在各 Reader 的 read() 中 | 提取为泛型模板 `assemble_repeated_levels()` |
| **MAP 实现** | `LIST<STRUCT<key,value>>`，零新代码 | 独立 `MapColumnReader` + `RepeatedMapValueSink` |
| **未投影 child** | `nullptr` | 保留 reader + `child_output_indices=-1` |
| **叶子层集成** | 自研 page/level decoder，直接写 Vector | `ArrowLeafReaderAdapter` → `NestedScalarBatch` |
| **中间数据结构** | 无（Vector 直接写入） | `NestedScalarBatch` / `NestedStructBatch` |
| **跨批次溢出** | ListColumnReader 内部 `overflow_child_count` | 泛型 `NestedScalarOverflow` + `move_nested_tail()` |
| **流对齐** | 不需要（LIST 只有 1 个 child；MAP=LIST<STRUCT>） | `NestedValueStream` / `NestedScalarSlotStream`（MAP 键值对齐） |

## 8. DuckDB 设计可借鉴的点

### 8.1 MAP 复用 LIST<STRUCT<>> 模式

DuckDB 在 schema 解析阶段将 MAP 展开为 LIST<STRUCT<key,value>>，之后所有 reader 逻辑完全复用。Doris 当前的 `MapColumnReader` 独立实现了键值对齐、溢出管理、skip 等逻辑，其中很大部分与 `ListColumnReader` 重叠。

**可行改动**：保留 Doris 的 `DataTypeMap` 输出语义，复用 LIST/repeated shape 的组装逻辑处理 MAP 的 entry 边界，但在 reader 输出阶段直接写 `ColumnMap` 的 key/value/offsets。这样借鉴 DuckDB "MAP 复用 LIST-like reader" 的核心收益，同时避免把 `ColumnArray(ColumnStruct)` 作为中间主路径。

### 8.2 简化未投影 child 处理

DuckDB 用 `nullptr` 标记未投影 child，StructColumnReader 直接将其输出为常量 NULL 向量。Doris 当前已删除 `ShapeOnlyColumnReader` 包装层，改为保留原始 child reader 并通过 `child_output_indices=-1` 调用 `skip()` 推进游标。

**已完成**：删除 `ShapeOnlyColumnReader`，并将 `RowPositionColumnReader` 拆到独立文件。已补充未投影 nested child 的 read/skip/select 覆盖。

### 8.3 内联 Dremel 组装

DuckDB 的 Dremel 处理直接写在 ListColumnReader 的 `Read()` / `Skip()` 中，约 90 行的核心逻辑。Doris 的 `assemble_repeated_levels()` 泛型模板通过 Sink 参数化实现了通用性，但支持的类型组合有限（标量 LIST、结构体 LIST、标量 MAP、结构体 MAP、嵌套 LIST），并没有无限种组合。使用泛型模板反而使代码难以跟踪和调试。

**可行改动**：将 `assemble_repeated_levels` 和各类 Sink 的逻辑内联回 `ListColumnReader`（以及 MAP 路径，如果保留的话）。`NestedScalarBatch` 可以简化为只保留 def/rep levels + value_indices 的直接 buffer，不需要泛型 Batch 抽象。

### 8.4 简化叶子层集成

DuckDB 的叶子 Reader 通过自研 decoder 直接从 page 数据解码并写入 Vector。Doris 的 `ArrowLeafReaderAdapter` 先写入 `NestedScalarBatch`（含 def/rep levels + value_indices + values_column），再让上层从 `NestedScalarBatch` 消费。对于标量路径（ScalarColumnReader），这个中间层是完全多余的——它只需要 values_column + null_map。

**可行改动**：在 ScalarColumnReader（扁平，非嵌套）路径中，直接从 RecordReader 解码到 Column，跳过 NestedScalarBatch。NestedScalarBatch 仅在嵌套路径（List/Map 的 def/rep level 需要被 Dremel 组装消费）时使用。

## 9. 不做的事项

- **不改变 DuckDB 的总体结论**：DuckDB 更简洁不等同于 Doris 应该全部照搬。Doris 的复杂类型代码已在生产环境验证，大规模重构的风险需要评估。
- **不在此文档中给出完整实施计划**：以上 8.1-8.4 是方向性建议，具体实施需要单独的设计文档。
