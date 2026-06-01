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
    parquet_scan_scheduler.*
    parquet_batch_filter.*
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

## 当前实现快照

当前分支已经完成 reader 主流程的第一轮分层，`ParquetReader` 已基本退回 file-local orchestration：文件上下文、schema 构造、row group/page range planning、batch scheduling、batch filter、scalar leaf decode、shape-only reader 都已经拆出独立模块。`column_reader.cpp` 当前只保留 base `ParquetColumnReader` 行为和 factory，`STRUCT` / `LIST` / `MAP` reader 已拆到独立实现文件。最大遗留点转为 complex reader 内部的 nested child 组合分支和更彻底的 sink 抽象。

当前已实现：

- `ParquetFileContext` 持有 Arrow file reader、footer metadata 和 schema descriptor。
- `ParquetColumnSchema` / `ParquetTypeDescriptor` 描述 file-local schema，不承担 table schema evolution。
- `RowGroupScanPlan` 已从 row group id 列表升级为 per-row-group `RowGroupReadPlan`，包含 `first_file_row`、row group rows 和 row group-local `selected_ranges`。
- row group pruning 已支持 min/max statistics 和 string-like dictionary page。
- page index pruning 已接入 Arrow `PageIndexReader`，并输出 row group-local row ranges。
- scheduler 已消费 `selected_ranges`，range gap 通过所有 column reader 的 row-level `skip()` 推进。
- batch filter 已通过 `SelectionVector` 驱动 lazy materialization。
- scalar reader、shape-only reader、row-position reader 和 Arrow leaf adapter 已从 `column_reader.cpp` 拆出。
- `StructColumnReader`、`ListColumnReader`、`MapColumnReader` 已从 `column_reader.cpp` 拆出到独立文件，并通过 `complex_column_reader.h` 保持 factory 构造边界。
- list/map 的 parent null map 与 entry count 写入已抽出 `RepeatedParentSinkState`，repeated child null map、entry count 和 nullable scalar child 写入已抽出 `RepeatedChildSinkState` / `append_nullable_scalar_child`。
- nested level 侧已有 repeated assembler、scalar/struct batch overflow、`NestedShapeCursor`，支持跨 `read/skip/select` 维护 cursor。
- complex type 当前支持 `STRUCT` nullable parent/child/projection，`LIST` null/empty/nullable scalar element，`MAP` null/empty/nullable scalar value，以及部分 `LIST<STRUCT>`、嵌套 `LIST`、`MAP` value 为 `STRUCT` / `LIST` 的组合。
- planner pruning stats 已接入 profile，包括 row group、dictionary、page index、filtered rows、selected ranges 和 page index read calls。

当前未完成或仍需重构：

- bloom filter pruning 只有 profile counter 和类型支持判断入口，尚未形成独立 `parquet_bloom_filter.*` helper 并接入 planner。
- complex reader 的 class 声明当前集中在 `complex_column_reader.h`，后续如需进一步降低耦合，可再按 reader 类型拆成独立 private header。
- list/map/struct 的 Doris column 写入 sink 已完成 parent state 和 repeated scalar child state 抽取，但 map key/value alignment、struct value append 和更通用的 shape/value stream 组合仍在各 reader 内部，复杂 child 组合仍依赖 `dynamic_cast` 和多处分支。
- nested batch/overflow 仍区分 scalar、struct 和部分 complex child 路径，还没有形成统一 shape stream + value stream 抽象。
- scheduler、column reader、adapter 的 skip/select/overflow/decode profile 还不完整。
- schema change / schema evolution 仍不在当前实现范围内；现有 file-local schema、row-range plan 和 reader tree 边界为后续接入保留空间。
- page-level decoder 尚未实现；当前 page index pruning 仍通过 `RecordReader + skip/read/select` 消费 row ranges。

当前功能矩阵：

| 功能 | 当前状态 | 说明 |
| --- | --- | --- |
| File-local schema | 已实现 | 只描述 Parquet 文件内部 schema，不处理 table schema evolution |
| Row group statistics pruning | 已实现 | 安全裁剪，无法证明不匹配时保留 |
| Dictionary row group pruning | 已实现 | 当前覆盖 string-like dictionary predicate 场景 |
| Page index row range pruning | 已实现 | 输出 row group-local selected ranges，scheduler 已消费 |
| Bloom filter pruning | 待实现 | 需要新增 planner helper，不应放回 `ParquetReader` |
| Batch predicate / lazy read | 已实现 | 通过 `SelectionVector` 和 selected ranges 推进 |
| Scalar leaf reader | 已拆分 | `scalar_column_reader.*` + `arrow_leaf_reader_adapter.*` |
| Shape-only / row-position reader | 已拆分 | `shape_only_column_reader.*` |
| STRUCT reader | 已实现，已拆分 | 支持 nullable parent/child/projection，实现在 `struct_column_reader.cpp` |
| LIST reader | 已实现，已拆分，sink 第二阶段统一 | 支持 null/empty/nullable scalar element、overflow、skip/select，部分 nested child |
| MAP reader | 已实现，已拆分，sink 第二阶段统一 | 支持 null/empty/nullable scalar value、overflow、skip/select，部分 complex value |
| Complex child projection | 部分实现 | struct child projection 已有，list/map nested child 仍需继续收敛 |
| Schema change | 未实现 | 当前边界应保持 file-local，后续由上层映射接入 |
| Page-level decode control | 未实现 | 暂不替代 Arrow `RecordReader` |

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

- `parquet_file_context.*`
- `ParquetReaderScanState` 持有 `ParquetFileContext` 和 file-local schema。

后续方向：

- `FileContext` 只处理文件和 Arrow core reader 生命周期，不处理 scan cursor。
- 后续如果接入 footer/page index cache，应继续放在 FileContext 或 planner 辅助层，不回流到 `ParquetReader`。

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
    int64_t start;
    int64_t length;
};

struct RowGroupReadPlan {
    int row_group_id;
    int64_t first_file_row;
    int64_t row_group_rows;
    std::vector<RowRange> selected_ranges;
};

struct RowGroupScanPlan {
    std::vector<RowGroupReadPlan> row_groups;
    ParquetPruningStats pruning_stats;
};
```

当前实现位置：

- `parquet_scan_planner.*`
- `parquet_statistics.*` 已承载 row group statistics pruning 和 string-like dictionary row group pruning。
- `parquet_page_index.*` 已把 Arrow `PageIndexReader` / `ColumnIndex` / `OffsetIndex` 转成 row group-local row ranges。
- `parquet_pruning.h` 已承载 planner 输出的 pruning reason/stats，包括 statistics、dictionary、page index、filtered rows 和 selected ranges。
- 当前 `RowGroupScanPlan` 已升级为 per-row-group row range plan；没有 page index 或无法安全裁剪时输出完整 row group range。

后续方向：

- 将 bloom filter 也收敛到 planner，形态与 statistics/dictionary/page index 一致。
- planner 不读取 column values，只读取 metadata 和必要的 dictionary/bloom/page index 辅助结构。
- page index 第一阶段已复用 `RecordReader + select(skip + read)`；只有证明确实需要更细粒度 IO 控制时再考虑 `GetColumnPageReader()` 自研 page decoder。

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

- `parquet_scan_scheduler.*`

当前状态：

- scheduler 已消费 `RowGroupReadPlan::selected_ranges`。
- range 间 gap 通过所有当前 column reader 的 `skip()` 推进。
- row position reader 使用 `first_file_row` 保持 file-local position。
- `ParquetReader::get_block` 只委托 scheduler 读取下一批。

后续方向：

- 增加 scheduler 级 selected rows、skipped rows、range cursor、lazy read filtered rows 统计。
- 后续如果切到 page-level decoder，scheduler 仍只消费 row ranges，不直接处理 page。

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
- `parquet_batch_filter.*`

后续方向：

- 继续保持 expression filter、delete filter 和 `ColumnPredicate` batch filter 统一输出 `SelectionVector`。
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
- `scalar_column_reader.*`
- `shape_only_column_reader.*`
- `complex_column_reader.h`

后续方向：

- 将 Arrow `RecordReader` 创建逻辑集中在 factory 或 leaf adapter。
- complex child projection 只改变 output child index，不破坏 file child slot。
- factory 继续只负责 reader tree 构造；complex reader 逻辑保留在各自实现文件，不回流到 factory。

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

- scalar flat read/skip/select 已拆入 `scalar_column_reader.*`。
- shape-only / row-position reader 已拆入 `shape_only_column_reader.*`。
- struct 支持 nullable parent、nullable child、child projection，以及复杂 child 的部分组合。
- list/map 支持 null/empty parent、nullable scalar child/value、overflow、skip/select，并已扩展部分 complex child。
- `StructColumnReader`、`ListColumnReader`、`MapColumnReader` 已拆入独立 `.cpp`，factory 仍在 `column_reader.cpp`。

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
- `nested_level_assembler.h`
- `complex_column_reader_helpers.*`
- `struct_column_reader.cpp`、`list_column_reader.cpp`、`map_column_reader.cpp` 中仍保留不同 child 组合分支。

后续方向：

- 统一 scalar/struct/list/map 的 batch 和 overflow 结构。
- 继续用 shape cursor 减少 list/map/struct 组合分支。
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

- `arrow_leaf_reader_adapter.*`
- `scalar_column_reader.*`
- `read_nested_scalar_batch`
- `read_nested_struct_batch`

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
- Doris new parquet reader 已把 page index 转成 row ranges 并接入 scan scheduler。
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
- scalar value 写入在 `scalar_column_reader.*` / `arrow_leaf_reader_adapter.*`。
- complex outer parent null map / entry count 已由 `RepeatedParentSinkState` 统一。
- nested child append、inner offsets/null map 仍在 list/map/struct reader sink 内。

后续方向：

- scalar value 写入继续通过 SerDe。
- complex nested offsets/null map 写入继续向 nested assembler sink/helper 收敛。
- reader 不应保存指向 Arrow buffer 的 value view。

## 12. Profile / Observability 层

职责：

- 统计 row group/page/dictionary/bloom pruning 效果。
- 统计 Arrow read、decode、skip、select、filter、materialization 时间。
- 统计 selected rows、skipped rows、empty selection、overflow 次数。
- 帮助判断是否需要从 `RecordReader` 进一步下沉到 page-level decoder。

当前状态：

- `parquet_reader.cpp` 已有 profile counter 初始化。
- planner 已通过 `ParquetPruningStats` 输出 row group、dictionary、page index、filtered rows、selected ranges、page index read calls 等统计，并由 `ParquetReader` 写入 profile。
- bloom filter、scheduler skip/select、column reader overflow、Arrow decode/page cache 的统计还没有完整迁移到各自分层。

后续方向：

- column reader 层输出 read/skip/select/overflow 统计。
- adapter 层输出 Arrow read/decode 统计。
- bloom filter 接入后复用 planner stats，不让 `ParquetReader` 直接判断裁剪原因。

## 当前代码到目标分层的映射

| 目标层 | 当前主要位置 | 当前状态 |
| --- | --- | --- |
| FileContext | `parquet_file_context.*` | 已独立拆分 |
| Metadata / Schema | `parquet_column_schema.*`, `parquet_type.*` | 基本成型 |
| RowGroupScanPlanner | `parquet_scan_planner.*`, `parquet_statistics.*`, `parquet_page_index.*`, `parquet_pruning.h` | statistics/dictionary/page index row range pruning 已接入，bloom 待补 |
| BatchScanScheduler | `parquet_scan_scheduler.*` | 已独立拆分，已按 selected row ranges 扫描 |
| Predicate / Selection | `selection_vector.h`, `parquet_batch_filter.*` | 已独立拆分 |
| ColumnReaderFactory | `column_reader.*`, `complex_column_reader.h` | 已实现，只负责 reader tree 构造 |
| ColumnReader Tree | `scalar_column_reader.*`, `shape_only_column_reader.*`, `struct_column_reader.cpp`, `list_column_reader.cpp`, `map_column_reader.cpp` | scalar/shape/complex reader 均已拆分 |
| Nested Assembler | `nested_level_assembler.h`, `complex_column_reader_helpers.*`, complex reader files | repeated assembler 和 shape cursor 已抽出，outer parent 和 repeated scalar child sink 已部分统一 |
| Leaf Decode Adapter | `arrow_leaf_reader_adapter.*`, `scalar_column_reader.*` | 已独立拆分 |
| Column Materialization | `arrow_leaf_reader_adapter.*`, `scalar_column_reader.*`, `complex_column_reader_helpers.*`, complex reader files, `DataTypeSerDe` | 已接入 SerDe，complex outer parent 和 repeated scalar child state 已统一，更通用 child sink 待继续收敛 |
| Profile | `parquet_reader.cpp`, `parquet_pruning.h` | planner pruning counter 已接入，scheduler/adapter/overflow 统计待补 |

## 推荐重构顺序

### 已完成：拆分 complex column reader 文件

- `StructColumnReader`、`ListColumnReader`、`MapColumnReader` 已从 `column_reader.cpp` 移到独立实现文件。
- `column_reader.cpp` 当前保留 base `ParquetColumnReader` 行为、reader factory、projection helper 和 Arrow `RecordReader` 缓存。
- complex reader class 声明集中在 `complex_column_reader.h`，供 factory 和 complex helper 使用。

### 已完成第二阶段：统一 complex materialization sink

- list/map 的 outer parent null map 和 entry count 维护已收敛到 `RepeatedParentSinkState`。
- nested list/list value 的 repeated child null map 和 entry count 维护已收敛到 `RepeatedChildSinkState`。
- nullable scalar element/value/list element 写入已收敛到 `append_nullable_scalar_child`。
- `complex_column_reader_helpers.*` 已承载 nested scalar/struct batch、alignment、output column unwrap、offset/null map append 等公共逻辑。
- 剩余 sink 工作是继续收敛 map key/value alignment、struct value append，以及更通用的 shape stream + value stream 组合分支。

### Step 1：统一 shape stream / value stream

- 把 scalar、struct、list value 的 batch/overflow 和 alignment 抽成统一 shape stream / value stream helper。
- 目标是减少 list/map 中按 child 类型展开的 `dynamic_cast` 分支，并让 reader 文件只描述 LIST/MAP/STRUCT 自身的 def/rep 语义。
- 保持当前 schema change 预留接口，不在这一步实现 schema evolution。

### Step 2：接入 bloom filter planner helper

- 新增 `parquet_bloom_filter.*`，负责读取和判断 bloom filter。
- planner 只消费 keep/drop 结论，并把 reason 写入 `ParquetPruningStats`。
- bloom 判断必须和 statistics/dictionary/page index 一样安全：无法证明不匹配时保留。

### Step 3：补 scheduler / column reader / adapter profile

- scheduler 统计 selected ranges、skip rows、lazy read filtered rows、empty selection。
- column reader 统计 read/skip/select、nested overflow 次数。
- Arrow adapter 统计 Arrow read/decode/null map/materialization 时间。

### Step 4：评估 page-level decoder 必要性

- 先用 profile 判断 `RecordReader + row range select` 的收益和开销。
- 只有当 page index 已证明可跳过大量 page、但 Arrow `RecordReader` 无法避免对应 IO/解码时，再考虑 `GetColumnPageReader()`。
- 如需实现，放在 adapter/page decoder 层，不回流到 `ParquetReader` 主流程。

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
