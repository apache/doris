# Doris New Parquet Reader 内部分层设计

本文档只描述 Doris `be/src/format/new_parquet/` 内部的 reader 分层。这里假设输入已经是 file-local scan request，输出也是 file-local `Block`。table schema、table reader、schema evolution 策略、partition/default/generated column 等上层语义不在本文档范围内。

目标是把 new parquet reader 从当前较集中的实现，逐步收敛成职责清晰的 file-local scan pipeline：

```text
ParquetReader
    -> FileContext
    -> Metadata / Schema Layer
    -> RowGroupScanPlanner
    -> BatchScanScheduler
    -> Predicate / Selection Layer
    -> ParquetColumnReaderFactory
    -> ParquetColumnReader Tree
    -> Nested Shape / Level Assembler
    -> Leaf Decode Adapter
    -> Arrow Parquet Core API
    -> Doris Column / Block
```

## 范围和非目标

本文档范围：

- 单个 Parquet 文件内的 schema、metadata、row group、column chunk、page index、dictionary/bloom/statistics。
- file-local projection、file-local predicate、row group selection、row range selection。
- file-local `Block` 的读取、skip、select、lazy materialization。
- `STRUCT`、`LIST`、`MAP` 等复杂类型的 def/rep level 组装。
- Arrow Parquet core API 的复用边界。

本文档不讨论：

- table/global schema 和 file schema 的映射。
- 多文件 schema evolution 策略。
- partition/default/generated column。
- Iceberg delete file。
- 最终 table block finalize/cast。
- FE/Planner 层 predicate 生成。

## 总体原则

1. `ParquetReader` 是 file-local scan orchestration，不直接处理 leaf decode 和 nested level 组装。
2. `ParquetColumnReader` 是 file-local column reader tree，统一提供 `read`、`skip`、`select`。
3. `Nested Shape / Level Assembler` 统一处理 def/rep level 到 Doris complex column 的映射。
4. `Leaf Decode Adapter` 封装 Arrow `RecordReader`，避免 Arrow internal API 扩散到 scan 调度层。
5. Row group/page/dictionary/bloom pruning 统一输出 row group-local row ranges，供 scan scheduler 消费。
6. pruning/filter 的安全性与 DuckDB 对齐：只有在 statistics、dictionary、bloom、page index 或 batch predicate 能证明某批数据不可能匹配时才裁剪；无法证明时保留。这个判断应集中在 planner/filter helper 中，不应在调用路径散落重复的防御式分支。
7. Arrow 负责底层 Parquet metadata/page/value/level 能力，Doris 负责 scan 语义、selection、projection 和输出 column 组织。

## 目标模块

```text
be/src/format/new_parquet/
    parquet_reader.*
    parquet_file_context.*
    parquet_column_schema.*
    parquet_type.*
    parquet_scan_planner.*
    parquet_statistics.*
    parquet_page_index.*
    parquet_bloom_filter.*
    selection_vector.*
    column_reader.*
    scalar_column_reader.*
    struct_column_reader.*
    list_column_reader.*
    map_column_reader.*
    shape_only_column_reader.*
    nested_level_assembler.*
    arrow_leaf_reader_adapter.*
```

当前代码还没有完全拆到这些文件。该列表描述目标边界，后续可以按功能演进逐步拆分。

## 1. FileContext 层

职责：

- 持有物理文件句柄。
- 将 Doris `io::FileReader` 适配成 Arrow `RandomAccessFile`。
- 创建并持有 Arrow `ParquetFileReader`。
- 持有 footer metadata、schema descriptor、file size 等 file-level 对象。
- 提供统一的 Arrow status/exception 到 Doris `Status` 的转换入口。

目标对象：

```text
ParquetFileContext
    arrow_file
    file_reader
    metadata
    schema_descriptor
    file_size
```

当前实现位置：

- `DorisRandomAccessFile` 在 `parquet_reader.cpp` 内。
- `ParquetReaderScanState` 持有 Arrow file reader 和 metadata。

后续方向：

- 将 `DorisRandomAccessFile` 和 metadata 持有逻辑从 `parquet_reader.cpp` 中拆出。
- `FileContext` 只处理文件和 Arrow core reader 生命周期，不处理 scan cursor。

## 2. Metadata / Schema 层

职责：

- 从 Arrow `SchemaDescriptor` 构造 Doris file-local `ParquetColumnSchema` tree。
- 解析 Parquet physical/logical/converted type，生成 Doris file-local type。
- 记录 leaf column id、schema node id、file path、field id path、name path、max def/rep level。
- 提供 row group metadata、column chunk metadata、statistics/page index/bloom filter 查询所需的 file-local id。

目标对象：

```text
ParquetColumnSchema
ParquetTypeDescriptor
ParquetSchemaTree
```

当前实现位置：

- `parquet_column_schema.*`
- `parquet_type.*`

边界：

- 只描述 Parquet 文件内部 schema。
- 不决定某个 file field 是否对应 table field。
- 不补 missing column。

## 3. RowGroupScanPlanner 层

职责：

- 基于 row group metadata 和 file-local predicate 选择 row group。
- 基于 statistics、dictionary page、bloom filter、page index 做安全 pruning：能证明不匹配才跳过，否则保留。
- 将 page-level keep/drop 结果合并成 row group-local row ranges。
- 处理 scan range ownership，避免多个 scan range 重复读同一个 row group。
- 输出 `RowGroupScanPlan` 给 scan scheduler。

目标输入：

```text
File metadata
ParquetColumnSchema tree
file-local predicates
scan range
```

目标输出：

```text
struct RowRange {
    int64_t first_row;
    int64_t row_count;
};

struct RowGroupScanPlan {
    int row_group_id;
    int64_t first_file_row;
    int64_t row_group_rows;
    std::vector<RowRange> selected_ranges;
};
```

当前实现位置：

- `parquet_statistics.*` 已承载 row group statistics pruning 和 string-like dictionary row group pruning。
- `parquet_reader.cpp` 仍持有部分 row group cursor 和 scan range 判断。

后续方向：

- 将 statistics、dictionary、page index、bloom filter 都收敛到 planner。
- planner 不读取 column values，只读取 metadata 和必要的 dictionary/bloom/page index 辅助结构。
- page index 第一阶段只生成 row ranges，不要求自研 page decoder。

## 4. BatchScanScheduler 层

职责：

- 管理当前 row group 和 row range cursor。
- 决定每次 `get_block()` 读取多少 file-local rows。
- 创建当前 row group 的 column reader tree。
- 先读 predicate columns，再根据 selection 延时物化 non-predicate columns。
- 推进所有 reader 的 row-level cursor。
- 组装 file-local output `Block`。

目标状态：

```text
ParquetScanState
    selected_row_group_plans
    current_row_group
    current_row_range
    current_row_group_rows_read
    predicate_readers
    non_predicate_readers
    row_position_reader
```

当前实现位置：

- `ParquetReaderScanState`
- `ParquetReader::_read_filter_columns`
- `ParquetReader::_read_non_filter_columns`
- `ParquetReader::get_block`

边界：

- scheduler 不理解 leaf page encoding。
- scheduler 不做 def/rep level 组装。
- scheduler 只消费 column reader 的 `read/skip/select`。

## 5. Predicate / Selection 层

职责：

- 在 batch 内执行 file-local predicate。
- 将 predicate 结果表达成 `SelectionVector`。
- 支持 predicate column 同时输出时的 column 复用。
- 为空 selection 提供快速 skip。
- 将 sparse selection 合并成连续 ranges，驱动 column reader `select()`。

目标对象：

```text
SelectionVector
BatchFilterExecutor
PredicateColumnBuilder
```

当前实现位置：

- `selection_vector.h`
- `parquet_reader.cpp` 中的 predicate column 构造和 selection 生成逻辑。

后续方向：

- 把 batch predicate 执行从 `parquet_reader.cpp` 中拆出。
- 接入结构化 `ColumnPredicate` 的 batch 内执行。
- 保持 `SelectionVector` 只表达 batch 内 row offset，不携带 schema 语义。

## 6. ParquetColumnReaderFactory 层

职责：

- 根据 `ParquetColumnSchema` 和 file-local projection 创建 reader tree。
- 创建并缓存 Arrow internal `RecordReader`。
- 决定 primitive、struct、list、map、row position、shape-only reader 的具体类型。
- 保证 Arrow internal `RecordReader` 不暴露给 `ParquetReader`。

目标 reader：

```text
ScalarColumnReader
StructColumnReader
ListColumnReader
MapColumnReader
ShapeOnlyColumnReader
RowPositionColumnReader
```

当前实现位置：

- `column_reader.h`
- `column_reader.cpp`

后续方向：

- 显式增加 `ShapeOnlyColumnReader`，用于未投影 child 的 row shape 推进。
- 将 Arrow `RecordReader` 创建逻辑集中在 factory 或 leaf adapter。
- complex child projection 只改变 output child index，不破坏 file child slot。

## 7. ParquetColumnReader Tree 层

统一接口：

```text
Status read(int64_t rows, MutableColumnPtr& column, int64_t* rows_read)
Status skip(int64_t rows)
Status select(const SelectionVector& sel, uint16_t selected_rows,
              int64_t batch_rows, MutableColumnPtr& column)
```

语义：

- `rows` 是 parent rows，不是 leaf values。
- `skip()` 是 row-level skip，不能直接退化成 value-level skip。
- `select()` 使用 `skip + read` 推进 cursor。
- reader tree 必须能跨多次 `read/skip/select` 保持 overflow/cursor 状态。

目标 reader 关系：

```text
ScalarColumnReader
    -> LeafDecodeAdapter

StructColumnReader
    -> child ParquetColumnReader slots

ListColumnReader
    -> element ParquetColumnReader
    -> NestedLevelAssembler

MapColumnReader
    -> key ParquetColumnReader
    -> value ParquetColumnReader
    -> NestedLevelAssembler
```

当前实现状态：

- scalar flat read/skip/select 已有。
- struct 支持 nullable parent、nullable child、child projection，以及复杂 child 的部分组合。
- list/map 支持 null/empty parent、nullable scalar child/value、overflow、skip/select，并已扩展部分 complex child。
- reader 类型和 assembler 逻辑仍集中在一个 `column_reader.cpp` 文件内。

## 8. Nested Shape / Level Assembler 层

职责：

- 读取或消费 leaf def/rep level stream。
- 折叠 repeated levels，生成 parent row。
- 区分 null parent、empty parent、non-empty parent。
- 写入 Doris array/map/struct offsets 和 null map。
- 支持 nullable scalar child、struct child、nested list/map child。
- 维护 overflow state。
- 校验多 leaf stream alignment。

目标抽象：

```text
NestedLeafBatch
    def_levels
    rep_levels
    value_column
    level_count
    value_count

NestedOverflow
    def_levels
    rep_levels
    compact_value_column

RepeatedShapeAssembler
    assemble(rows, driver_stream, sink)
    skip(rows, driver_stream)

NestedSink
    start_parent()
    append_null_parent()
    append_empty_parent()
    append_entry()
    finish_parent()
```

设计要求：

- Arrow buffer 不能跨 `RecordReader::ReadRecords()` 生命周期保存。
- string/binary 必须复制到 Doris-owned column。
- output rows 满时，如果已经进入下一条 parent row，需要把 tail 放入 overflow。
- 同一 parent row 内的 repeated entries 必须完整消费。
- map key/value、struct 多 child 必须做 level alignment check。

当前实现位置：

- `NestedScalarBatch`
- `NestedScalarOverflow`
- `NestedStructBatch`
- `NestedStructOverflow`
- `assemble_repeated_levels`
- `assemble_repeated_struct_levels`

后续方向：

- 统一 scalar/struct/list/map 的 batch 和 overflow 结构。
- 引入 shape cursor，减少 list/map/struct 组合分支。
- 支持 shape-only stream，用于未投影 child 推进。

## 9. Leaf Decode Adapter 层

职责：

- 封装 Arrow internal `RecordReader::ReadRecords()`、`SkipRecords()`。
- 复制 def/rep levels。
- 将 Arrow decoded values materialize 成 Doris-owned temporary column。
- 暴露 leaf batch 给 scalar reader 或 nested assembler。
- 转换 Arrow status/exception。

目标对象：

```text
ArrowLeafReaderAdapter
    read_records(parent_rows)
    skip_records(parent_rows)
    read_nested_batch(parent_rows)
```

当前实现位置：

- `ScalarColumnReader`
- `read_nested_scalar_batch`
- `read_nested_struct_batch`
- 多个 `RecordReader` helper 函数。

边界：

- adapter 可以理解 Arrow `RecordReader`。
- adapter 不决定 list/map parent shape。
- adapter 不执行 predicate。
- adapter 不处理 row group planning。

## 10. Arrow Parquet Core API 复用边界

当前应继续复用：

- `arrow::io::RandomAccessFile`
- `parquet::ParquetFileReader`
- `parquet::RowGroupReader`
- `parquet::internal::RecordReader`
- `parquet::PageIndexReader`
- `parquet::RowGroupPageIndexReader`
- `parquet::ColumnIndex`
- `parquet::OffsetIndex`
- `parquet::PageReader`

使用原则：

- `RecordReader` 是当前 leaf value/level decode 主路径。
- `PageIndexReader` 用于 page-level metadata pruning。
- `GetColumnPageReader()` 可用于 dictionary page pruning，以及未来必要的 page-level decoder。
- 不使用 `parquet::arrow::FileReader` 作为 scan 输出路径。
- 不把 Arrow `Array` / `RecordBatch` / `Table` 作为主中间表示。

关于 page-level pruning：

- 复用 Arrow 不妨碍读取 page index。
- 当前缺口是 Doris 还没有把 page index 转成 row ranges 并接入 scan scheduler。
- 如果 row-range `select(skip + read)` 已经足够减少 decode，可以继续保留 `RecordReader`。
- 如果 `SkipRecords()` 仍会产生过多 IO/decode，再在 adapter 层引入更细粒度 page reader。

## 11. Doris Column Materialization 层

职责：

- 将 decoded values 写入 Doris `MutableColumnPtr`。
- 使用 `DataTypeSerDe::read_column_from_decoded_values(...)` 复用 Doris 类型写入逻辑。
- 处理 nullable wrapper、array/map offsets、struct child columns。

当前实现位置：

- `DecodedColumnView`
- `DataTypeSerDe` 相关调用。
- scalar/list/map/struct reader 内部的 column 写入逻辑。

后续方向：

- scalar value 写入继续通过 SerDe。
- complex offsets/null map 写入由 nested assembler sink 统一处理。
- reader 不应保存指向 Arrow buffer 的 value view。

## 12. Profile / Observability 层

职责：

- 统计 row group/page/dictionary/bloom pruning 效果。
- 统计 Arrow read、decode、skip、select、filter、materialization 时间。
- 统计 selected rows、skipped rows、empty selection、overflow 次数。
- 帮助判断是否需要从 `RecordReader` 进一步下沉到 page-level decoder。

当前状态：

- `parquet_reader.cpp` 已有部分 profile counter。
- page index/bloom/page cache 相关 counter 还没有完整实现闭环。

后续方向：

- planner 层输出 pruning reason 和 skipped rows/pages。
- column reader 层输出 read/skip/select/overflow 统计。
- adapter 层输出 Arrow read/decode 统计。

## 当前代码到目标分层的映射

| 目标层 | 当前主要位置 | 当前状态 |
| --- | --- | --- |
| FileContext | `parquet_reader.cpp` | 已有能力，未独立拆分 |
| Metadata / Schema | `parquet_column_schema.*`, `parquet_type.*` | 基本成型 |
| RowGroupScanPlanner | `parquet_statistics.*`, `parquet_reader.cpp` | 部分成型 |
| BatchScanScheduler | `parquet_reader.cpp` | 已实现主流程，职责偏集中 |
| Predicate / Selection | `selection_vector.h`, `parquet_reader.cpp` | 部分成型 |
| ColumnReaderFactory | `column_reader.*` | 已实现，需补 shape-only |
| ColumnReader Tree | `column_reader.*` | 已实现主干，文件过大 |
| Nested Assembler | `column_reader.cpp` | 已有雏形，需独立抽象 |
| Leaf Decode Adapter | `column_reader.cpp` | 已有 helper，需独立封装 |
| Column Materialization | `column_reader.cpp`, `DataTypeSerDe` | 已接入 SerDe |
| Profile | `parquet_reader.cpp` | 部分 counter |

## 推荐重构顺序

### Step 1：抽出纯内部 helper

- 将 Arrow `RecordReader` 相关 read/skip/copy level helper 抽成 leaf adapter。
- 将 repeated level assembler 从 `column_reader.cpp` 抽出。
- 不改变 public API。
- 不改变 scan 行为。

### Step 2：显式 shape-only reader

- 为未投影 child 建立统一 reader。
- struct/list/map child projection 都通过 file child slot + output child index 表达。
- 避免 unprojected child 在各个 reader 内写特殊分支。

### Step 3：统一 nested shape cursor

- list/map scalar child、struct child、nested list/map child 都走同一个 shape cursor。
- 多 leaf stream alignment 统一检查。
- overflow state 统一管理。

### Step 4：拆出 scan planner

- row group statistics、dictionary、bloom、page index 都输出 row ranges。
- `ParquetReader` 主循环只消费 `RowGroupScanPlan`。
- page index 第一阶段继续复用 `RecordReader + select(skip + read)`。

### Step 5：拆出 batch filter executor

- 统一 expression filter 和 `ColumnPredicate` batch filter。
- 输出 `SelectionVector`。
- predicate column 复用和 non-predicate lazy materialization 保持在 scheduler。

## 功能归位规则

新增功能按以下规则放置：

- 需要 Parquet footer/row group/page metadata：放在 scan planner 或 metadata helper。
- 需要 Arrow `RecordReader`：放在 leaf adapter 或 column reader factory。
- 需要 def/rep level：放在 nested assembler。
- 需要 Doris column 写入：放在 column reader sink/materialization。
- 需要 batch selection：放在 predicate/selection 层。
- 需要 row group/page pruning：输出 row ranges，不直接读取 output column。
- 需要 page-level decode 控制：先放在 Arrow adapter 层，不进入 `ParquetReader` 主流程。

这个分层的核心目的是：`ParquetReader` 只负责 file-local scan 编排，`ParquetColumnReader` 只负责 file-local column cursor，nested assembler 只负责 shape，Arrow adapter 只负责底层 Parquet API。这样后续实现复杂类型、page index、dictionary id filter、bloom filter 和 selected read 优化时，不需要继续把逻辑叠加到同一个大函数或同一个大文件里。
