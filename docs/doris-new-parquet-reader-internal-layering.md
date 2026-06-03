# Doris New Parquet Reader 内部分层设计

本文档只描述 `be/src/format/new_parquet/` 内部的 file-local reader，不描述 table reader。

这里的 file-local reader 指：输入已经是单个 Parquet 文件上的 scan request，输出也是该文件语义下的 Doris `Block`。table schema、跨文件 schema evolution、partition/default/generated column、Iceberg delete file、FE predicate 生成和最终 table block finalize/cast 都不在本文档范围内。

本文档同时区分两个概念：

- **层**：长期稳定的职责边界，描述一个功能应该归属到哪里。
- **组件**：当前代码中的文件、类和 helper。组件可以合并或拆分，但不能打破层的职责边界。

## 当前源码结构

当前 `new_parquet` 的主要文件结构如下：

```text
be/src/format/new_parquet/
    parquet_reader.*
    parquet_file_context.*
    parquet_column_schema.*
    parquet_type.*
    parquet_scan.*
    parquet_statistics.*
    selection_vector.h
    reader/
        column_reader.*
        scalar_column_reader.*
        row_position_column_reader.*
        struct_column_reader.*
        list_column_reader.*
        map_column_reader.*
        nested_column_reader.*
        arrow_leaf_reader_adapter.*
```

当前已经删除或合并的旧组件：

- `column_reader/` 目录已重命名为 `reader/`，对齐 DuckDB `extension/parquet/reader/` 的 reader 归属方式。
- `complex_column_reader_helpers.*` 和 `nested_level_assembler.h` 已合并到 `reader/nested_column_reader.*`。
- `shape_only_column_reader.*` 已删除；未投影 child 通过保留原始 reader 并设置 `child_output_indices=-1` 走 `skip()` 推进，`RowPositionColumnReader` 拆到 `row_position_column_reader.*`。
- `parquet_scan_planner.*`、`parquet_scan_scheduler.*`、`parquet_batch_filter.*` 已合并到 `parquet_scan.*`。
- `parquet_pruning.h`、`parquet_page_index.*` 已合并到 `parquet_statistics.*`。

保留 `selection_vector.h` 的原因是它同时被 column reader `select()` API、scan batch filter 和 row range planning 使用，是跨层共享的轻量数据结构。强行塞入 `parquet_scan.h` 会让 reader 层依赖 scan 层。

## 目标分层

目标 pipeline：

```text
ParquetReader facade
    -> FileContext
    -> File-local Schema / Type
    -> Pruning / Row Range Planning
    -> Scan Scheduler / Batch Selection
    -> Column Reader Factory
    -> Column Reader Tree
    -> Nested Shape / Value Assembler
    -> Arrow Leaf Decode Adapter
    -> Doris Column Materialization
```

核心原则：

1. `ParquetReader` 只做 file-local orchestration，不直接处理 leaf decode、def/rep level 和 nested column 写入。
2. pruning/filter 与 DuckDB 一致：metadata、dictionary、bloom filter、page index 等文件级信息只有在能证明数据不可能匹配时才裁剪；无法证明时保留。batch `Expr` 只做 row-level filter，不参与 row group/page 裁剪。这里的“保留”是正确性原则，不代表实现上要散落重复防御分支。
3. row group/page/dictionary/bloom pruning 统一输出 row group-local row ranges 或 row group keep/drop 结论。
4. row-level filter 只使用 file-local `Expr`/`VExprContext` 执行；`ColumnPredicate` 只作为 row group、page index、dictionary、statistics、bloom filter 等 pruning hint，不参与 batch 内行级过滤。
5. `ParquetColumnReader` 的 `read/skip/select` 参数语义是 parent rows，不是 leaf values。
6. nested reader 必须通过 def/rep level 组装 parent shape，不能把复杂类型 skip 退化成 leaf value-level skip。
7. Arrow 负责 Parquet footer、metadata、page、level 和 value decode 的底层能力；Doris 负责 scan 语义、selection、projection、nested shape 和 Doris column 输出。
8. Arrow buffer 不能跨 `RecordReader::ReadRecords()` 生命周期保存；binary/string 必须 materialize 到 Doris-owned column。

## 层与组件映射

| 层 | 当前组件 | 当前状态 |
| --- | --- | --- |
| Reader facade | `parquet_reader.*` | 已收敛为 file-local reader 入口，负责 init/open/get_schema/get_block/profile 聚合 |
| FileContext | `parquet_file_context.*` | 已独立，封装 Doris file reader 到 Arrow `RandomAccessFile`、`ParquetFileReader` 和 footer metadata |
| File-local schema/type | `parquet_column_schema.*`, `parquet_type.*` | 已独立，描述 Parquet 文件内部 schema/type，不处理 table schema evolution |
| Pruning / row range planning | `parquet_statistics.*`, `parquet_scan.*` | statistics、dictionary、bloom filter、page index 已接入 |
| Scan scheduler / batch selection | `parquet_scan.*`, `selection_vector.h` | 已按 selected row ranges 扫描，predicate columns 先读，non-predicate columns lazy materialize |
| Column reader factory | `reader/column_reader.*` | 已集中创建 reader tree 和 Arrow `RecordReader` cache |
| Column reader tree | `reader/scalar_column_reader.*`, `reader/row_position_column_reader.*`, `reader/struct_column_reader.*`, `reader/list_column_reader.*`, `reader/map_column_reader.*` | scalar、row-position、struct、list、map reader 已拆分；未投影 child 不再包 shape-only reader |
| Nested shape/value assembler | `reader/nested_column_reader.*` + LIST/MAP reader 局部 helper | 公共 header 已缩减到 357 行（batch/overflow/cursor/校验/appender）；LIST 内联 def/rep 循环，MAP 局部 assemble/repeated/value stream/slot stream 收敛到 map_column_reader.cpp |
| Arrow leaf decode adapter | `reader/arrow_leaf_reader_adapter.*` | 已封装 Arrow `RecordReader` read/skip/value materialization |
| Doris column materialization | `reader/*`, Doris `DataTypeSerDe` | scalar 主要走 SerDe；complex offsets/null map/child append 仍分布在 reader sink 中 |
| Observability | `parquet_reader.*`, `parquet_statistics.*` | pruning profile 已接入；scheduler/adapter/nested overflow profile 仍不足 |

## 各层职责

### 1. Reader Facade

组件：

- `ParquetReader`
- `ParquetScanRequest`
- `ParquetReaderScanState`

职责：

- 打开单个 Parquet 文件。
- 初始化 file context、file schema 和 scan plan。
- 将 `get_block()` 委托给 scan scheduler。
- 聚合 profile counter。
- 暴露 file-local `get_schema()`。

不负责：

- table/global schema 映射。
- partition/default/generated column。
- nested level 组装。
- Arrow page/value decode 细节。
- row group/page pruning 的具体判断。

### 2. FileContext

组件：

- `ParquetFileContext`

职责：

- 持有 Doris `io::FileReader`。
- 适配 Arrow `RandomAccessFile`。
- 创建并持有 Arrow `ParquetFileReader`。
- 持有 footer metadata、schema descriptor、file size。
- 提供 Arrow status/exception 到 Doris `Status` 的转换边界。

后续约束：

- Footer cache、page index cache 等 file-level cache 可以挂在这里或 metadata helper，不能回流到 `ParquetReader` 主流程。

### 3. File-local Schema / Type

组件：

- `ParquetColumnSchema`
- `ParquetTypeDescriptor`

职责：

- 从 Arrow `SchemaDescriptor` 构造 Doris file-local schema tree。
- 记录 file column id、leaf column id、schema node id、field id path、name path、max def/rep level。
- 解析 Parquet physical/logical/converted type。
- 为 reader factory、statistics/page index/dictionary/bloom helper 提供 file-local id。

不负责：

- table schema evolution。
- missing column/default column 补齐。
- 跨文件字段对齐。

### 4. Pruning / Row Range Planning

组件：

- `ParquetStatisticsUtils`
- `ParquetColumnStatistics`
- `ParquetPruningStats`
- `select_row_groups_by_statistics(...)`
- `select_row_group_ranges_by_page_index(...)`
- `plan_parquet_row_groups(...)`
- `RowGroupReadPlan`
- `RowGroupScanPlan`
- `RowRange`

职责：

- 基于 scan range 选择当前 reader 拥有的 row group。
- 基于 row group statistics 做 row group keep/drop。
- 基于 dictionary page 做 row group keep/drop。
- 基于 bloom filter 做 row group keep/drop。
- 基于 page index 做 row group-local row range selection。
- 输出 `RowGroupScanPlan`，而不是直接读取 output columns。
- 只消费 `FileScanRequest::column_predicate_filters` 作为 pruning hint；这些 `ColumnPredicate`
  不要求对应列 materialize 到 scan block，也不能决定 batch 内某一行是否返回。

当前边界：

- `parquet_statistics.*` 承载 statistics、dictionary、bloom filter、page index 和 pruning stats。
- `parquet_scan.*` 承载 row group plan 的 orchestration。
- 这两个组件的职责可以后续再拆细，但不应把 pruning 逻辑放回 `ParquetReader`。

与 DuckDB 的对齐点：

- 先把 Parquet metadata 转换成统一的本地统计表达，再交给 predicate/filter 判断。
- 裁剪判断必须是 proof-based：能证明不匹配才 skip。
- bloom filter 只处理常量等值类 predicate；当前支持 `EQ` / `IN_LIST`，跳过 null-accepting predicate、null-only predicate、复杂类型和非 primitive leaf。
- bloom filter 底层读取复用 Arrow `BloomFilterReader`，Doris 侧只做 predicate value 到 Parquet hash 语义的 adapter。
- page index 是 metadata pruning，输出 row ranges；是否进一步自研 page decoder 是后续 adapter 层决策。

### 5. Scan Scheduler / Batch Selection

组件：

- `ParquetScanScheduler`
- `SelectionVector`
- `execute_batch_filters(...)`
- `execute_reader_expression_map(...)`
- `selection_to_filter(...)`

职责：

- 管理 row group cursor 和 row range cursor。
- 打开当前 row group 的 reader tree。
- 对 range gap 调用所有 reader 的 row-level `skip()`。
- 先读取 predicate columns，执行 file-local `Expr` batch filter。
- 根据 `SelectionVector` lazy materialize non-predicate columns。
- 处理 predicate column 同时输出时的复用。
- 生成 file-local output `Block`。

过滤契约：

- `request.conjuncts` 是 batch 内行级过滤的唯一普通谓词来源。
- `request.delete_conjuncts` 是 delete file 语义的行级过滤来源。
- `request.column_predicate_filters` 不在 scheduler 内执行，只在 pruning 层用于证明 row group/page/dictionary/statistics/bloom 可以跳过。

不负责：

- Parquet page encoding。
- def/rep level 组装。
- statistics/dictionary/page index 判断。
- table-level cast/finalize。

### 6. Column Reader Factory

组件：

- `ParquetColumnReaderFactory`

职责：

- 根据 `ParquetColumnSchema` 和 `FieldProjection` 创建 reader tree。
- 创建并缓存 Arrow internal `RecordReader`。
- 决定 scalar、struct、list、map、row-position reader。
- 保证 Arrow internal `RecordReader` 不泄露到 scan scheduler。

设计约束：

- factory 只负责构造 reader tree，不承载复杂类型 read 语义。
- complex child projection 只影响输出 child，不破坏 file child slot 和 shape 推进。
- 后续 schema evolution 应先在上层完成 file-local projection 映射，再传入 factory。

### 7. Column Reader Tree

组件：

- `ParquetColumnReader`
- `ScalarColumnReader`
- `RowPositionColumnReader`
- `StructColumnReader`
- `ListColumnReader`
- `MapColumnReader`

统一接口：

```text
Status read(int64_t rows, MutableColumnPtr& column, int64_t* rows_read)
Status skip(int64_t rows)
Status select(const SelectionVector& sel, uint16_t selected_rows,
              int64_t batch_rows, MutableColumnPtr& column)
```

语义：

- `rows` 是 file-local parent rows。
- `skip()` 必须推进 row-level cursor。
- `select()` 使用 `skip + read` 推进 cursor，不能整批 read 后丢弃。
- reader tree 必须跨多次 `read/skip/select` 保持 overflow/cursor 状态。

当前状态：

- scalar flat read/skip/select 已实现。
- shape-only reader 已删除；struct 未投影 child 保留原始 reader，输出 slot 为 `-1`，由 struct reader 调用 `skip()` 推进 shape/cursor。
- row-position reader 支持 file-local row position 输出。
- struct 支持 nullable parent、nullable child、projection 和部分 complex child。
- list/map 支持 null parent、empty parent、nullable scalar child/value、overflow、skip/select，并支持部分 nested complex child。

### 8. Nested Shape / Value Assembler

公共组件（`reader/nested_column_reader.*`）：

- `NestedScalarBatch` / `NestedScalarOverflow` — scalar leaf 的 def/rep levels + values 的中间载体和 overflow
- `NestedStructBatch` / `NestedStructOverflow` — struct element 的多 child batch 和 overflow
- `NestedShapeCursor` — 统一的 def/rep level 只读遍历器
- `RepeatedParentSinkState` / `RepeatedChildSinkState` — LIST/MAP 共用的 parent/child 状态辅助
- `NestedScalarValueAppender` / `NestedStructValueAppender` — scalar/struct child 值追加器
- `read_nested_scalar_batch(...)` / `read_nested_struct_batch(...)` — 从 Arrow RecordReader 读取并填充 batch
- 校验函数：`validate_nested_shape_alignment`、`validate_nested_scalar_alignment`、`validate_nested_struct_alignment`
- overflow tail 函数：`move_nested_scalar_tail`、`move_nested_struct_tail`

LIST/MAP 局部组件（各 reader 内部，不对外暴露）：

- `list_column_reader.cpp`：`consume_list_level_stream`、`read_scalar_list_values`、`read_struct_list_values`、`read_nested_list_values` 及其 skip 变体
- `map_column_reader.cpp`：`assemble_map_repeated_levels`、`MapValueStream`、`MapScalarSlotStream`、`MapValueSink`、`MapListValueSink` 及其 skip 变体

职责：

- 公共层：复制并持有 def/rep levels；将 Arrow decoded values materialize 到 Doris-owned temporary column；提供 level 遍历和校验；维护 overflow 机制。
- LIST 层：折叠 repeated levels 生成 parent rows；区分 null/empty/non-empty parent；写入 `ColumnArray` offsets + null map + child column。
- MAP 层：折叠 repeated levels 生成 parent rows；处理 key/value 两个独立 stream 的对齐；写入 `ColumnMap` key + value + offsets + null map。

当前状态（post inline refactoring）：

- 公共 `assemble_repeated_levels` 模板已删除。LIST 使用内联 def/rep level 循环直接写 `ColumnArray`，MAP 使用局部 `assemble_map_repeated_levels` 直接写 `ColumnMap`。
- `NestedValueStream` / `NestedScalarSlotStream` 移到 `map_column_reader.cpp`，重命名为 `MapValueStream` / `MapScalarSlotStream`。
- `RepeatedListValueSink` / `RepeatedNestedListValueSink` / `RepeatedMapValueSink` / `RepeatedMapListValueSink` / `RepeatedAlignedValueSkipSink` 从公共 header 删除——LIST 内联循环不需要 Sink 抽象，MAP 的 Sink 局部化在 `map_column_reader.cpp`。
- 公共 `nested_column_reader.h` 从 789 行缩减到 357 行，仅保留 LIST 和 MAP 真正共用的基础件。

剩余不足：

- `NestedScalarBatch::value_indices` 仍存在（内联循环中 def_level 检查与 value lookup 存在逻辑重复），可进一步删除。
- nested MAP value、struct 内更深层 complex child 的组合覆盖还不完整。
- struct value append 中非 scalar child 的推进仍依赖 `StructColumnReader` 当前接口。

后续目标：

- reader 文件只表达 LIST/MAP/STRUCT 自身的 Parquet 语义。
- def/rep stream、overflow、alignment 继续由各 reader 内联或局部 helper 处理。
- 新增 complex child 类型时不复制 list/map 主循环。

### 9. Arrow Leaf Decode Adapter

组件：

- `ArrowLeafReaderContext`
- `read_leaf_records(...)`
- `read_nested_leaf_batch(...)`
- `build_leaf_null_map(...)`
- `append_leaf_values(...)`

职责：

- 封装 Arrow `RecordReader::ReadRecords()`。
- 封装 Arrow `RecordReader::SkipRecords()`。
- 复制 def/rep levels。
- 将 Arrow decoded values 写入 Doris-owned column。
- 转换 Arrow status/exception。

不负责：

- list/map/struct parent shape。
- predicate/filter。
- row group/page pruning。
- table schema evolution。

关于 `GetColumnPageReader()`：

- 当前主路径仍复用 Arrow `RecordReader`。
- 当前 page index pruning 已能读取 Arrow page index 并转成 row ranges。
- 如果 profile 证明 `RecordReader + skip/read/select` 不能有效避免 page IO/decode，再在 adapter/page decoder 层评估自研 page-level decoder。
- 即使实现 page-level decoder，也不应改变 scan scheduler 和 column reader tree 的 row-level API。

### 10. Doris Column Materialization

组件：

- `DataTypeSerDe::read_column_from_decoded_values(...)`
- scalar reader/adapter materialization helper
- complex reader sink
- `RepeatedParentSinkState`
- `RepeatedChildSinkState`

职责：

- 写入 Doris `MutableColumnPtr`。
- 处理 nullable wrapper。
- 处理 array/map offsets。
- 处理 struct child columns。
- 保证 binary/string 数据由 Doris column 持有。

后续目标：

- scalar value 写入继续走 SerDe。
- complex offsets/null map/child append 继续向 nested sink helper 收敛。
- 不保存 Arrow buffer view。

### 11. Observability

组件：

- `ParquetReader::ParquetProfile`
- `ParquetPruningStats`

当前已有：

- total/to-read/filtered row groups。
- statistics/dictionary/page index filtered row groups。
- filtered row group rows。
- filtered page rows。
- selected row ranges。
- page index read calls 和相关耗时 counter。
- bloom filter filtered row groups 和 read time counter。
- lazy read filtered rows counter。

仍需补齐：

- scheduler selected/skipped rows。
- empty selection 次数。
- reader read/skip/select rows。
- nested overflow 次数和 tail rows。
- Arrow adapter read/decode/materialization 细分耗时。
- bloom filter check time 细分统计。

## 功能矩阵

| 功能 | 当前状态 | 归属层 | 说明 |
| --- | --- | --- | --- |
| File open/footer parse | 已实现 | FileContext | 通过 Arrow `ParquetFileReader` |
| File-local schema 输出 | 已实现 | Schema/Reader facade | `get_schema()` 返回 Parquet 文件自身 schema |
| Primitive scalar read | 已实现 | Column reader / Adapter | Doris-owned column materialization |
| Primitive scalar skip/select | 已实现 | Column reader | `select()` 走 `skip + read` |
| Row position column | 已实现 | Column reader | 使用 row group `first_file_row` |
| Top-level projection | 已实现 | Scheduler / Factory | 按 file-local column id 创建 reader |
| Struct projection | 已实现 | Factory / Struct reader | 支持 child projection 和 nullable child |
| List/Map nested projection | 部分实现 | Factory / Reader | LIST 标量/STRUCT/nested LIST、MAP 标量/STRUCT/LIST value 路径已支持；MAP nested map value 和更深层 complex child 组合仍未覆盖 |
| Row group statistics pruning | 已实现 | Pruning | min/max/null count 转 Doris statistics 后判断 |
| Dictionary row group pruning | 已实现 | Pruning | 当前覆盖 string-like dictionary predicate |
| Page index pruning | 已实现 | Pruning / Scheduler | Arrow page index 转 row group-local row ranges |
| Bloom filter pruning | 已实现 | Pruning | 复用 Arrow bloom reader，支持 primitive `BOOLEAN`、`INT`、`BIGINT`、`FLOAT`、`DOUBLE`、`STRING` 的 `EQ` / `IN_LIST` 保守裁剪 |
| Batch predicate filter | 已实现 | Scheduler / Selection | 输出 `SelectionVector` |
| Lazy materialization | 已实现 | Scheduler / Selection | predicate columns 先读，non-predicate columns 按 selection 读 |
| Selected row range scan | 已实现 | Scheduler | range gap 通过 row-level `skip()` 推进 |
| Nullable STRUCT | 已实现 | Struct reader / Nested assembler | 对齐 child shape/null map |
| STRUCT complex child | 部分实现 | Struct reader / Nested assembler | 支持部分 list/map child，仍需收敛分支 |
| LIST scalar element | 已实现 | List reader / Nested assembler | 支持 null/empty/nullable scalar/overflow |
| LIST struct element | 已实现 | List reader | `read_struct_list_values` 直接写 `ColumnArray`，使用 `NestedStructValueAppender` |
| Nested LIST | 已实现 | List reader | `read_nested_list_values` 直接写 `ColumnArray`，两级 offset 跟踪 |
| MAP scalar value | 已实现 | Map reader | key required，value 支持 nullable scalar；`MapValueSink` + `MapValueStream` 局部写入 `ColumnMap` |
| MAP struct/list value | 已实现 | Map reader | struct value 使用 `MapValueSink<NestedStructBatch>`；LIST value 使用 `MapListValueSink` + `MapScalarSlotStream` 局部写入 `ColumnMap` |
| MAP nested map value | 未完成 | Map reader / Nested assembler | 后续在统一 shape/value stream 上补 value appender |
| Required/nullability corruption check | 部分实现 | Column reader / Nested assembler | list/map scalar 路径较完整，complex child 需继续统一 |
| Schema change / schema evolution | 已实现 | TableReader / ColumnMapper | top-level 和 struct child 缺失列已通过 `is_missing` + 默认值填充处理；各 table format 集成验证待补 |
| Page-level decoder | 未实现 | Adapter | 当前不替代 Arrow `RecordReader` |
| Profile/metrics | 部分实现 | Observability | pruning 较完整，scheduler/adapter/nested 仍需补齐 |
| BE unit tests | 部分实现 | Tests | column reader complex/path tests 已有，覆盖矩阵仍需补 |

## 后续实现计划

### P1：补 Observability

目标：

- 让后续是否需要 page-level decoder 有数据依据。

建议步骤：

1. scheduler 统计 selected ranges、range gap skip rows、batch selected rows、empty selection。
2. column reader 统计 read/skip/select rows。
3. nested assembler 统计 overflow 次数、overflow level slots、tail value count。
4. adapter 统计 Arrow read、decode、null map、materialization 时间。

验收标准：

- 能从 profile 判断 page index pruning 是否真正减少 decode/materialization。
- 能定位复杂类型 read-ahead overflow 的开销。

### P2：补复杂类型剩余覆盖

目标：

- 在统一 assembler 之上补齐 nested projection 和 nested complex combinations。

建议覆盖：

- `Array(Array(T))`
- `Array(Map<K,V>)`
- `Array(Struct(...complex child...))`
- `Map<K, Array(T)>`
- `Map<K, Map<K2,V2>>`
- `Map<K, Struct(...complex child...)>`
- struct 内 list/map projection 与未投影 child shape-only 推进
- required/nullable 组合的 corruption check

验收标准：

- 所有复杂 child 组合使用统一 shape/value stream。
- 列裁剪不会破坏未投影 child 的 row-level cursor。

### P3：Schema Change 接入准备

**状态：已基本完成，改为验证项。**

当前 TableReader/ColumnMapper 已实现 P3 所需的全部机制：

- `ColumnMapping::is_missing` + `allow_missing_columns` 处理文件不存在但表 schema 包含的列。
- `_materialize_struct_mapping_column()` 对缺失子字段用 `create_column_const_with_default_value(rows)` 填充默认值。
- `TableColumn::default_expr` 支持 FE 注入的非空默认值（ALTER TABLE ADD COLUMN DEFAULT）。
- `BY_FIELD_ID` / `BY_NAME` / `BY_INDEX` 三种映射模式已覆盖各 table format 的列匹配需求。
- `ProjectedStructFillsMissingChildWithDefault` UT 已覆盖 struct 子字段缺失场景。

仍需验证：

- 各 table format（Iceberg/Hive/Paimon）的 scan node 是否正确设置了 `allow_missing_columns` 和 `default_expr`。
- 所有 schema change 场景的集成测试（add/drop/rename column, type promotion）。

### P4：评估 Page-level Decoder

**状态：保留，但前置条件（P1 observability）尚未满足。**

背景：
- 当前 page-index pruning 已能算出哪些 page 需要读，输出 row ranges。
- 但 Arrow `RecordReader::SkipRecords()` 仍然解压和解码被跳过的 page（只跳过 value 写入，不跳过 decompression + level decoding）。
- 自研 page-level decoder 可以直接跳过 page 的 I/O 和解压（如旧 reader 的 `skip_page_data()`——纯文件 offset 前进），page gap 越大收益越明显。

当前无 benchmark 数据支撑决策：
- 新 reader 没有任何性能 test 或 profiling 基础设施。
- 旧 reader 有详细的 decode 耗时统计（`decompress_time`、`decode_header_time`、`skip_page_header_num` 等），可作为参考。
- P1（observability）必须先完成，才能判断 `RecordReader + skip/read/select` 的 page 级开销占比。

如果 P1 证明 page-level skip 是瓶颈：
- 编码器已全部存在于 `format/parquet/decoder/`（PLAIN、RLE_DICT、DELTA_*、BYTE_STREAM_SPLIT 共 7 种），不需要从零实现。
- 主要工作是封装 Arrow `GetColumnPageReader()` + 复用已有 decoder + 集成到 `ScalarColumnReader` skip 路径。
- 保持 column reader tree 的 row-level API 不变；page-level decoder 是 adapter 内部实现细节。

## 功能归位规则

新增代码按以下规则放置：

- 需要 Parquet footer、row group metadata、column chunk metadata、dictionary、bloom 或 page index：放在 pruning/statistics 组件。
- 需要 scan range、row group cursor、row range cursor、batch selection：放在 `parquet_scan.*`。
- 需要 Arrow `RecordReader`：放在 reader factory 或 Arrow leaf adapter。
- 需要 def/rep level：放在 nested assembler。
- 需要 Doris column offsets/null map/child append：放在 column reader sink 或 nested sink helper。
- 需要 batch predicate：放在 selection/batch filter 路径。
- 需要 table/global schema：不放在 `new_parquet` file-local reader 内。

## 当前与理想状态的差距

当前结构已经完成文件归并和职责收敛：reader 代码集中在 `reader/`，scan 调度集中在 `parquet_scan.*`，metadata pruning 集中在 `parquet_statistics.*`。LIST/MAP 的 Dremel 组装已从公共泛型模板改为各 reader 内联/局部 helper。

主要差距：

- LIST 和 MAP 的已支持路径已完成内联/局部化，但 MAP nested map value、struct 内更深层 complex child 组合仍需要补 value appender。
- schema change 还停留在边界预留阶段。
- observability 还没补完整；scheduler selected/skipped rows、nested overflow 次数、adapter 细分耗时仍未接入 profile。
- 复杂类型的统计信息过滤和谓词过滤下推尚未实现（见独立文档）。
