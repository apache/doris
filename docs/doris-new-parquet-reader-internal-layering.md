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
        shape_only_column_reader.*
        struct_column_reader.*
        list_column_reader.*
        map_column_reader.*
        nested_column_reader.*
        arrow_leaf_reader_adapter.*
```

当前已经删除或合并的旧组件：

- `column_reader/` 目录已重命名为 `reader/`，对齐 DuckDB `extension/parquet/reader/` 的 reader 归属方式。
- `complex_column_reader_helpers.*` 和 `nested_level_assembler.h` 已合并到 `reader/nested_column_reader.*`。
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
| Pruning / row range planning | `parquet_statistics.*`, `parquet_scan.*` | statistics、dictionary、page index 已接入；bloom filter 仅有 profile/支持判断入口 |
| Scan scheduler / batch selection | `parquet_scan.*`, `selection_vector.h` | 已按 selected row ranges 扫描，predicate columns 先读，non-predicate columns lazy materialize |
| Column reader factory | `reader/column_reader.*` | 已集中创建 reader tree 和 Arrow `RecordReader` cache |
| Column reader tree | `reader/scalar_column_reader.*`, `reader/shape_only_column_reader.*`, `reader/struct_column_reader.*`, `reader/list_column_reader.*`, `reader/map_column_reader.*` | scalar、shape-only、row-position、struct、list、map reader 已拆分 |
| Nested shape/value assembler | `reader/nested_column_reader.*`, complex reader sinks | 已有 repeated assembler、overflow、parent/child sink state；shape/value stream 还未完全统一 |
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
- 基于 page index 做 row group-local row range selection。
- 后续基于 bloom filter 做 row group keep/drop。
- 输出 `RowGroupScanPlan`，而不是直接读取 output columns。
- 只消费 `FileScanRequest::column_predicate_filters` 作为 pruning hint；这些 `ColumnPredicate`
  不要求对应列 materialize 到 scan block，也不能决定 batch 内某一行是否返回。

当前边界：

- `parquet_statistics.*` 承载 statistics、dictionary、page index 和 pruning stats。
- `parquet_scan.*` 承载 row group plan 的 orchestration。
- 这两个组件的职责可以后续再拆细，但不应把 pruning 逻辑放回 `ParquetReader`。

与 DuckDB 的对齐点：

- 先把 Parquet metadata 转换成统一的本地统计表达，再交给 predicate/filter 判断。
- 裁剪判断必须是 proof-based：能证明不匹配才 skip。
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
- 决定 scalar、struct、list、map、shape-only、row-position reader。
- 保证 Arrow internal `RecordReader` 不泄露到 scan scheduler。

设计约束：

- factory 只负责构造 reader tree，不承载复杂类型 read 语义。
- complex child projection 只影响输出 child，不破坏 file child slot 和 shape 推进。
- 后续 schema evolution 应先在上层完成 file-local projection 映射，再传入 factory。

### 7. Column Reader Tree

组件：

- `ParquetColumnReader`
- `ScalarColumnReader`
- `ShapeOnlyColumnReader`
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
- shape-only reader 用于未投影 child 的 shape 推进。
- row-position reader 支持 file-local row position 输出。
- struct 支持 nullable parent、nullable scalar child、child projection，以及 list/map child 的
  read/skip/select 推进；当 struct 位于 repeated parent 内时，仍要求 struct 至少有一个 scalar
  child 作为 shape driver。
- list 支持 scalar element、scalar-child struct element、nested list scalar element、scalar map
  element 的
  null/empty/nullable child、overflow、skip/select。
- map 支持 scalar key，以及 scalar value、scalar-child struct value、list scalar value 的
  null/empty/nullable value、overflow、skip/select。

### 8. Nested Shape / Value Assembler

组件：

- `NestedScalarBatch`
- `NestedScalarOverflow`
- `NestedStructBatch`
- `NestedStructOverflow`
- `NestedShapeCursor`
- `NestedValueStream`
- `NestedScalarSlotStream`
- `NestedScalarValueAppender`
- `NestedStructValueAppender`
- `assemble_repeated_levels(...)`
- `read_nested_scalar_batch(...)`
- `read_nested_struct_batch(...)`
- `RepeatedParentSinkState`
- `RepeatedChildSinkState`
- `RepeatedListValueSink`
- `RepeatedNestedListValueSink`
- `RepeatedMapValueSink`
- `RepeatedMapListValueSink`
- `RepeatedAlignedValueSkipSink`

职责：

- 复制并持有 def/rep levels。
- 将 Arrow decoded values materialize 到 Doris-owned temporary column。
- 折叠 repeated levels，生成 parent rows。
- 区分 null parent、empty parent、non-empty parent。
- 写入 array/map offsets 和 nullable parent null map。
- 写入 nullable scalar child/value 和 scalar-child struct value。
- 对 map key/value 和 struct 多 child 做 level alignment check。
- 维护 read-ahead overflow。

当前状态：

- scalar 和 struct batch 已适配到统一 shape view。
- `NestedScalarBatch` 支持 levels-only batch；未投影 scalar child 可以只复制 def/rep levels，
  不再 materialize Doris-owned temporary value column。
- MAP scalar/struct value 已通过 `NestedValueStream` 统一 read、alignment 和 overflow tail。
- MAP LIST value 的 key stream 已通过 `NestedScalarSlotStream` 统一按 value driver 消费 key slot。
- LIST scalar element、LIST struct element、nested LIST scalar element 已通过统一 repeated sink 写入。
- LIST MAP scalar value element 已通过 key shape driver 和 aligned value stream 写入。
- MAP scalar value、MAP struct value、MAP LIST scalar element value 已通过统一 repeated sink 写入。
- struct 中未投影 complex child 可以通过 shape-only reader 推进 cursor，已投影 list/map child
  通过 `advance_non_scalar_struct_children(...)` 逐 parent row 推进。

剩余不足：

- reader factory 仍需要按 child reader 类型选择合适的 value stream/appender。
- nested MAP value 仅覆盖 `Array(Map<K, scalar V>)`；`Map<K, Map<K2,V2>>` 尚无 value
  appender/sink。
- nested list 目前只覆盖 scalar inner element；`Array(Array(Struct))`、`Array(Array(Map))`
  等更深组合未覆盖。
- repeated struct batch 仍以 scalar child 作为 shape driver；纯 complex-child struct、以及
  repeated struct 内更深 complex child 的 shape-only/value stream 还未统一。
- struct value append 中非 scalar child 的推进仍依赖 `StructColumnReader` 当前接口逐行 read/skip。

后续目标：

- reader 文件只表达 LIST/MAP/STRUCT 自身的 Parquet 语义。
- def/rep stream、overflow、alignment、shape-only 推进统一收敛到 nested assembler。
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
- lazy read filtered rows counter。

仍需补齐：

- scheduler selected/skipped rows。
- empty selection 次数。
- reader read/skip/select rows。
- nested overflow 次数和 tail rows。
- Arrow adapter read/decode/materialization 细分耗时。
- bloom filter read/check 统计。

## 功能矩阵

| 功能 | 当前状态 | 归属层 | 说明 |
| --- | --- | --- | --- |
| File open/footer parse | 已实现 | FileContext | 通过 Arrow `ParquetFileReader` |
| File-local schema 输出 | 已实现 | Schema/Reader facade | `get_schema()` 返回 Parquet 文件自身 schema |
| Primitive scalar read | 已实现 | Column reader / Adapter | Doris-owned column materialization |
| Primitive scalar skip/select | 已实现 | Column reader | `select()` 走 `skip + read` |
| Row position column | 已实现 | Column reader | 使用 row group `first_file_row` |
| Top-level projection | 已实现 | Scheduler / Factory | 按 file-local column id 创建 reader |
| Struct projection | 已实现 | Factory / Struct reader | 支持 scalar child 裁剪；未投影 complex child 用 shape-only 推进 |
| List/Map nested projection | 部分实现 | Factory / Nested assembler | 支持 struct child 裁剪和部分 list/map child；深层组合仍需补齐 |
| Row group statistics pruning | 已实现 | Pruning | min/max/null count 转 Doris statistics 后判断 |
| Dictionary row group pruning | 已实现 | Pruning | 当前覆盖 string-like dictionary predicate |
| Page index pruning | 已实现 | Pruning / Scheduler | Arrow page index 转 row group-local row ranges |
| Bloom filter pruning | 未完成 | Pruning | 有 profile/支持判断入口，未形成完整 helper |
| Batch predicate filter | 已实现 | Scheduler / Selection | 输出 `SelectionVector` |
| Lazy materialization | 已实现 | Scheduler / Selection | predicate columns 先读，non-predicate columns 按 selection 读 |
| Selected row range scan | 已实现 | Scheduler | range gap 通过 row-level `skip()` 推进 |
| Nullable STRUCT | 已实现 | Struct reader / Nested assembler | scalar child shape/null map 对齐；struct 可整体 nullable |
| STRUCT scalar children | 已实现 | Struct reader / Nested assembler | 支持 nullable child、projection、skip/select |
| STRUCT list/map child | 部分实现 | Struct reader / Nested assembler | 已支持 read/skip/select 和 projection 的主路径；repeated struct 内仍依赖 scalar child driver |
| STRUCT only complex children | 部分实现 | Struct reader | non-nullable top-level 可逐 child 推进；nullable 或 repeated 场景仍缺 shape driver |
| LIST scalar element | 已实现 | List reader / Nested assembler | 支持 null list、empty list、nullable element、overflow、skip/select |
| LIST struct element | 部分实现 | List reader / Nested assembler | 支持 scalar-child struct element、projection、overflow；struct 内深层 complex child 未完全统一 |
| LIST list scalar element | 已实现 | List reader / Nested assembler | 支持 `Array(Array(T))` 中 inner scalar element 的 null/empty/overflow、skip/select |
| LIST map scalar value element | 已实现 | List reader / Nested assembler | 支持 `Array(Map<K,V>)` 中 scalar key/value 的 null/empty/overflow、skip/select |
| LIST map complex value element | 未完成 | List reader / Nested assembler | `Array(Map<K, Struct/List/Map>)` 尚未覆盖 |
| MAP scalar value | 已实现 | Map reader / Nested assembler | key required，value 支持 nullable scalar、null/empty map、overflow、skip/select |
| MAP struct value | 部分实现 | Map reader / Nested assembler | 支持 scalar-child struct value、projection、overflow；struct 内深层 complex child 未完全统一 |
| MAP list scalar value | 已实现 | Map reader / Nested assembler | 支持 `Map<K, Array(T)>` 中 list scalar value 的 null/empty/overflow、skip/select |
| MAP nested map value | 未完成 | Map reader / Factory | factory 当前不创建 nested MAP value reader |
| Legacy LIST schema | 部分实现 | Schema parser | 支持 LIST annotated repeated primitive、repeated multi-field group；top-level unannotated repeated 尚未归一化 |
| Required/nullability corruption check | 部分实现 | Column reader / Nested assembler | list/map scalar 路径较完整；complex child 和纯 complex struct 需继续统一 |
| Schema change / schema evolution | 未实现 | 上层映射 + Factory | 当前只保留 file-local 边界 |
| Page-level decoder | 未实现 | Adapter | 当前不替代 Arrow `RecordReader` |
| Profile/metrics | 部分实现 | Observability | pruning 较完整，scheduler/adapter/nested 仍需补齐 |
| BE unit tests | 部分实现 | Tests | column reader complex/path tests 已有，覆盖矩阵仍需补 |

## 后续实现计划

### P1：补复杂类型剩余覆盖

目标：

- 在统一 assembler 之上补齐 nested projection 和 nested complex combinations。

建议覆盖：

- `Array(Map<K, Struct/List/Map>)`
- `Array(Struct(...complex child...))`
- `Map<K, Map<K2,V2>>`
- `Map<K, Struct(...complex child...)>`
- pure-complex struct：`Struct<Array<T>, Map<K,V>>`
- struct 内 list/map projection 与未投影 child levels-only 推进
- required/nullable 组合的 corruption check

验收标准：

- 所有复杂 child 组合使用统一 shape/value stream。
- 列裁剪不会破坏未投影 child 的 row-level cursor。

### P2：补 Observability

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

### P3：补 Bloom Filter Pruning

目标：

- 在 pruning 层补完整 bloom filter row group pruning。
- 与 statistics/dictionary/page index 一样输出 proof-based keep/drop 结论和 profile stats。

建议步骤：

1. 在 `parquet_statistics.*` 内先实现 bloom helper，必要时后续再拆独立组件。
2. 复用 `ParquetColumnSchema` 做类型支持判断。
3. 只对能安全判断的 predicate 启用 bloom pruning。
4. 接入 `ParquetPruningStats` 和 `ParquetReader::ParquetProfile`。
5. 添加 positive/negative/unsupported type 测试。

验收标准：

- bloom filter 不能证明不匹配时保留 row group。
- statistics、dictionary、page index、bloom 的 pruning stats 不互相覆盖。

### P4：Schema Change 接入准备

目标：

- 不在 file-local reader 内直接实现 table schema evolution，但保证 reader 边界可以承接上层映射结果。

建议步骤：

1. 明确 `FieldProjection` 中 file child slot 与 output child slot 的表达。
2. 支持 missing projected child 由上层以 default/null reader 形式注入。
3. 保证 factory 只消费 file-local schema + projection mapping。
4. 为 field id/name/path mapping 预留测试入口。

验收标准：

- file-local reader 不知道 table/global schema。
- schema evolution 不需要改 nested assembler 主循环。

### P5：评估 Page-level Decoder

目标：

- 在 profile 证明必要时，再考虑从 Arrow `RecordReader` 下沉到 page-level decoder。

建议步骤：

1. 基于 P2 profile 分析 page index pruning 后的 IO/decode 成本。
2. 如果 `RecordReader + skip/read/select` 不能避免主要开销，在 adapter 层封装 `GetColumnPageReader()`。
3. 保持 column reader tree 的 row-level API 不变。
4. 先支持 scalar leaf，再考虑 nested leaf stream。

验收标准：

- page-level decoder 是 adapter 内部实现细节。
- scan scheduler 仍只消费 row ranges。

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

当前结构已经完成第一轮文件归并和职责收敛：reader 代码集中在 `reader/`，scan 调度集中在 `parquet_scan.*`，metadata pruning 集中在 `parquet_statistics.*`。这比之前“每个小 helper 一个文件”的结构更接近 DuckDB 的 reader 目录组织。

主要差距仍在 nested 层：

- 现在已经有 repeated assembler、overflow state、shape/value stream 和统一 sink，但 complex child 组合还没有全部覆盖。
- list/map 对已支持路径复用统一 sink；新增 nested MAP、struct deep complex child 仍需要补 value appender/shape-only reader 组合。
- schema change 还停留在边界预留阶段。
- bloom filter pruning 和 observability 还没补完整。

后续优先级应先补剩余 complex child 组合和 pure-complex struct 的 shape/value stream，再补 observability 与 Bloom Filter pruning。新增复杂类型 case 必须继续落在 nested assembler 的 value stream/appender/sink 结构上，避免 list/map reader 重新变厚。
