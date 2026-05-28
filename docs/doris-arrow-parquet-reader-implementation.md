# Doris Arrow Parquet Reader 实现方案与当前状态

本文档描述 `be/src/format/new_parquet/` 下新 Parquet reader 的设计、当前实现状态和后续缺口。

当前目标不是替换旧 `vparquet` 路径，而是在新 reader API 下先实现一个 file-local Parquet reader：

- 底层复用 Arrow C++ Parquet core API 解析文件、row group 和 column chunk。
- 输出仍然是 Doris 自己的 `Block` 和 `Column`。
- 不使用 `parquet::arrow::FileReader`、`arrow::RecordBatch` 或 `arrow::Table` 作为 scan 输出路径。
- `ParquetReader` 只理解 Parquet file-local schema，不理解 Iceberg/global schema。
- schema change、filter localization、default/generated/partition column 等 table-level 语义放在 `TableReader` 和 `TableColumnMapper`。

## 分层边界

当前分层如下：

```text
FileScanner / TableReader / IcebergTableReader
    -> TableColumnMapper
    -> reader::FileScanRequest
    -> doris::parquet::ParquetReader
    -> DorisRandomAccessFile
    -> parquet::ParquetFileReader
    -> parquet::RowGroupReader
    -> parquet::internal::RecordReader
    -> Doris Block / Column
```

关键边界：

- `TableReader` 输出 table/global schema block。
- `ParquetReader` 输出 file-local block。
- `TableColumnMapper` 负责把 table projection/filter 转成 file-local projection/filter。
- `ParquetReader` 不补 default column，不物化 partition column，不处理 generated column，不做 Iceberg schema evolution。
- 所有 table-level cast/finalize/delete/virtual column 都不能塞回 `ParquetReader`。

## FileReader 生命周期

`ParquetReader` 继承 `reader::FileReader`，当前生命周期是：

```text
init(RuntimeState*)
    -> get_schema(std::vector<SchemaField>*)
    -> open(std::unique_ptr<FileScanRequest>&)
    -> get_block(Block* file_block, size_t* rows, bool* eof)
    -> close()
```

语义约束：

- `init()` 打开物理文件并解析 Parquet footer metadata。
- `get_schema()` 在 `init()` 成功后可调用，不要求 `open()`。
- `open()` 接收已经 localize 的 `FileScanRequest`，并完成 row group pruning 和 reader 游标初始化。
- `get_block()` 只能在 `open()` 成功后调用，输出 file-local block。
- `rows` 表示本批 file-local block 输出行数，`eof` 表示当前物理文件是否读完。

## 代码布局

```text
be/src/format/new_parquet/parquet_reader.h
be/src/format/new_parquet/parquet_reader.cpp
be/src/format/new_parquet/column_reader.h
be/src/format/new_parquet/column_reader.cpp
be/src/format/new_parquet/parquet_column_schema.h
be/src/format/new_parquet/parquet_column_schema.cpp
be/src/format/new_parquet/parquet_type.h
be/src/format/new_parquet/parquet_type.cpp
be/src/format/new_parquet/parquet_statistics.h
be/src/format/new_parquet/parquet_statistics.cpp
be/src/format/new_parquet/selection_vector.h
```

职责划分：

- `parquet_reader.*`：文件打开、schema 导出、scan state、row group 调度、谓词列优先读取、file-local block 组装。
- `column_reader.*`：单个 Parquet 字段到 Doris column 的读取；封装 Arrow internal `RecordReader`。
- `parquet_column_schema.*`：从 Parquet schema descriptor 构建 file-local schema tree。
- `parquet_type.*`：解析 Parquet physical/logical/converted type，生成 Doris file-local type 和额外类型信息。
- `parquet_statistics.*`：基于 row group metadata 做保守的统计信息裁剪。
- `selection_vector.h`：表达 batch 内被选中的 row offset，用于延时物化。

## 核心组件

### DorisRandomAccessFile

`DorisRandomAccessFile` 把 Doris `io::FileReader` 适配成 `arrow::io::RandomAccessFile`。

它只处理随机读和文件大小查询，不解析 Parquet schema，不携带 table schema，也不执行 filter。

### ParquetReaderScanState

`ParquetReaderScanState` 是 `parquet_reader.cpp` 内部状态，记录：

- Arrow random access file；
- Arrow Parquet file reader；
- Parquet footer metadata；
- Parquet schema descriptor；
- file-local schema tree；
- 被 row group statistics 选中的 row group；
- 当前 row group reader；
- 当前 row group 内已读行数；
- predicate column readers；
- non-predicate column readers。

该状态不暴露给 table reader。

### ParquetColumnSchema 和 ParquetTypeDescriptor

`ParquetColumnSchema` 描述 file-local schema tree，包括：

- Parquet node name；
- Parquet field id；
- top-level field id；
- leaf column id；
- Doris file-local type；
- 子列 schema；
- primitive column 的 `ParquetTypeDescriptor`。

`ParquetTypeDescriptor` 负责保存 Parquet annotation 解析结果，包括：

- physical type；
- logical type / converted type 推导后的 Doris type；
- decimal precision/scale；
- time/timestamp unit；
- 是否 string-like；
- 是否支持当前 RecordReader 读取路径。

类型解析已经从 `column_reader.cpp` 前移到 `parquet_type.*`，`ColumnReader` 热路径只消费解析结果。

### ParquetColumnReader

`ParquetColumnReader` 是 Doris 自己的 file-local column reader 抽象，不是 Arrow 的 `parquet::ColumnReader`。

当前接口收敛为：

```text
read(rows, column, rows_read)
skip(rows)
select(selection, selected_rows, batch_rows, column)
```

当前实现：

- `ScalarColumnReader`：基于 Arrow internal `RecordReader` 读取 flat primitive/string/decimal/time/timestamp。
- `StructColumnReader`：支持 top-level struct 的 scalar child 组装，包含 nullable parent struct、nullable scalar child 和 struct child projection。
- `ListColumnReader`：支持 scalar element 的 LIST level 组装，包含 null list、empty list、nullable element 和 overflow state。
- `MapColumnReader`：支持 scalar key/value 的 MAP level 组装，包含 null map、empty map、nullable scalar value 和 overflow state。

`select()` 在基类中统一实现：把 `SelectionVector` 合并成连续 row ranges，然后交替调用 `skip()` 和 `read()`。当前不实现整批 read 后再 filter 的 fallback。

### ParquetColumnReaderFactory

`ParquetColumnReaderFactory` 根据当前 row group 和 `ParquetColumnSchema` 创建 column reader。

它集中封装 Arrow internal `RecordReader` 的创建和缓存，避免 Arrow internal API 泄露到 `ParquetReader` 主流程。

### DataTypeSerDe decoded value 读取接口

`ScalarColumnReader` 不直接把 Parquet value switch 到 Doris column，而是构造 `DecodedColumnView`，再调用：

```text
DataTypeSerDe::read_column_from_decoded_values(...)
```

当前已接入的 SerDe 包括 number、string、decimal、date/time/datetime、nullable 等类型。这样可以把“Parquet 解码”和“Doris 类型写入”拆开，减少 `ColumnReader` 内部的 Doris 类型分发逻辑。

## Scan Request 语义

新 reader 消费 `reader::FileScanRequest`。

重要字段：

- `predicate_columns`：需要先读取，用于计算 selection 的 file-local columns。
- `non_predicate_columns`：selection 确定后再读取的 file-local columns。
- `column_positions`：file column id 到 file-local output block position 的映射。
- `local_filters`：已经 localize 到 file schema 的 filter。
- `reader_expression_map`：table filter 无法安全转换成 file-local predicate 时的 fallback 表达式。

输出 block 的列顺序和类型遵守 `column_positions`，不是 table/global schema。

## 谓词下推

当前已实现：

- row group 级 min/max 统计信息裁剪；
- null count 驱动的 `IS NULL` / `IS NOT NULL` 裁剪；
- unsupported statistics、缺失 statistics、不安全比较时保守保留 row group。

当前未实现：

- page index pruning；
- bloom filter pruning；
- dictionary pruning；
- batch 内直接执行结构化 `ColumnPredicate`；
- `reader_expression_map` fallback 表达式执行。

注意：当前 `local_filters.predicates` 已经进入 row group statistics 路径，但在 batch 内过滤阶段，`ParquetReader::_read_filter_columns()` 主要处理 `local_filter.conjunct`。因此如果某个谓词只以 `ColumnPredicate` 形式存在，目前还缺少 batch 内二次过滤闭环。

## 延时物化当前状态

当前 scan loop 是 predicate-first 模型：

1. 读取 `predicate_columns`。
2. 执行表达式 filter，生成 `SelectionVector`。
3. 如果谓词列也在 output block 中，则复用已经解码的谓词列，并按 selection filter。
4. 对 `non_predicate_columns` 调用 `ColumnReader::select()`，只读取被选中的行。
5. 返回 file-local block。

已有能力：

- flat primitive/string/decimal/time/timestamp 的基础 selected read；
- empty selection 时跳过整批 non-predicate columns；
- sparse selection 会被合并成多个连续 ranges；
- predicate column 同时是 projection 时，不会重新读取该列。

主要缺口：

- batch 内 `ColumnPredicate` 执行未接入 selection；
- `reader_expression_map` 仍是 TODO；
- selection index 当前是 `uint16_t`，需要显式约束 batch size；
- selected read 依赖 Arrow internal `RecordReader::SkipRecords` 和 `ReadRecords`，需要继续隔离在 `column_reader.*`；
- 没有 page-level row range selection；
- LIST/MAP 的 `select()` 已经复用 `skip() + read()` range 策略，并通过 nested overflow state 保持 cursor 正确；
- Struct 的 complex child selected read 仍依赖 child reader 自身能力，后续需要补多 stream assembler。

## Schema Change 当前状态

当前原则是：`ParquetReader` 不理解 schema change，schema change 由 `TableColumnMapper` 和 `TableReader` 处理。

已有能力：

- `TableReader` 初始化时默认使用 `TableColumnMappingMode::BY_FIELD_ID`。
- `TableColumnMapper` 可以根据 table column 和 file schema 建立 `ColumnMapping`。
- 缺失 partition column 可以用 partition value 生成 constant mapping。
- 缺失普通列可以使用 `default_expr`。
- file type 与 table type 不同的时候，可以生成 finalize cast projection。
- virtual column 有 `ROW_ID` 和 `LAST_UPDATED_SEQUENCE_NUMBER` 的 mapping 标记。

主要缺口：

- 当前 `SchemaField::id` 同时承担 file-local column id 和 mapping id，边界还不够清晰。尤其 top-level primitive 目前会使用 leaf column id，Iceberg field id 映射还需要重新梳理。
- `_is_same_type()` 只是 `DataTypePtr` 指针比较，不能可靠表达类型等价。
- filter localization 仍是 stub，没有完整实现 trivial mapping、safe cast、reader expression fallback、finalize-only filter。
- `reader_filter_expr` 没有真正生成或执行。
- 复杂列 schema change 没有 child-level mapping。
- `IcebergTableReader` 的 equality delete、position delete、virtual column、finalize 仍是框架 stub。

## 复杂列当前状态

已有能力：

- schema builder 能识别 `STRUCT`、`LIST`、`MAP`。
- 可以把复杂 Parquet schema 组合成 Doris `DataTypeStruct`、`DataTypeArray`、`DataTypeMap`。
- `ParquetColumnSchema` 已记录 file path、field id path、name path、definition level、repetition level、nullable definition level 和 repeated repetition level，为后续 child-level mapping/schema change 留入口。
- `TableColumnMapper` 可以为 struct child 生成 `FieldProjection`，`ParquetReader` 会把 projected file-local schema 暴露给上层。
- `StructColumnReader` 支持 top-level struct 的 scalar children：
  - required struct；
  - nullable struct；
  - required scalar child；
  - nullable scalar child；
  - projected scalar child，例如只读 `s.b` 时仍能根据该 leaf 的 definition level 还原 parent null map。
- `LIST` 支持 scalar element：
  - required / nullable list；
  - null list；
  - empty list；
  - required / nullable scalar element；
  - 小批量 read 下跨 batch 的 overflow；
  - `skip()` / `select()` 通过同一个 level assembler 推进。
- `MAP` 支持 scalar key/value：
  - required / nullable map；
  - null map；
  - empty map；
  - required key；
  - required / nullable scalar value；
  - key leaf 作为 shape driver，value leaf 校验 row count、level count 和 repetition level 对齐；
  - `skip()` / `select()` 通过同一个 level assembler 推进。
- `NestedScalarBatch` 在每次 `RecordReader::ReadRecords()` 后复制 def/rep levels，并把 defined values materialize 到 Doris-owned 临时列，避免保存 Arrow buffer 或 `StringRef`。
- `NestedScalarOverflow` 保存未消费的 level tail 和 compact 后的 Doris-owned value column，LIST/MAP read-ahead 不再假设 child records 等于 output rows。
- `RepeatedLevelAssembler` 统一折叠 repeated level stream，生成 parent row、entry count、parent null map，并由 sink 写入 list/map child column。

主要缺口：

- `Array(Struct)`、`Map<K, Struct>` 还未实现。当前 Struct reader 可以组装 scalar child，但 LIST/MAP assembler 还没有接 complex child sink。
- 嵌套 list/map 还未实现，例如 `Array(Array<T>)`、`Map<K, Array<T>>`。
- nullable struct 如果包含 complex child，目前仍返回 `NotSupported`，避免在缺少多 stream assembler 时误读。
- LIST/MAP 的 nested projection 还未实现。当前只支持完整读取 scalar element/value，不支持只投影 `array.element.x` 或 `map.value.y`。
- 复杂类型 schema change 还未实现 child-level remap/default/cast。当前 schema/path/projection 结构按后续扩展预留，但缺失 child、rename、field id remap、default child、nested cast 都还没有接入。
- primitive reader 的 flat scalar 路径仍只支持 `max_repetition_level == 0 && max_definition_level <= 1`；nested scalar 只能通过 complex reader 使用。
- complex child 的 lazy materialization 还不完整，尤其是 Struct complex child 和未来多 leaf value 需要统一 cursor/overflow。

结论：当前复杂列已经从“schema 可见”推进到“scalar-child LIST/MAP/STRUCT 可读”。下一阶段重点不是再补单个特殊 case，而是把 Struct child 接入 LIST/MAP assembler，并建立多 leaf stream 的统一 cursor/overflow 模型。

## 当前可用能力总结

当前新 reader 已经具备：

- 打开 Parquet 文件并解析 footer；
- 导出 file-local schema；
- 基于 row group statistics 做保守裁剪；
- 读取 flat required/nullable primitive；
- 读取 string/binary；
- 读取 decimal precision <= 38 的常见物理编码；
- 读取 date/time/datetime 的部分编码；
- 通过 `DataTypeSerDe::read_column_from_decoded_values()` 写入 Doris column；
- 基础 predicate-first scan；
- flat column selected read；
- non-nullable / nullable struct 的 scalar child 读取；
- struct scalar child projection；
- scalar LIST / MAP 读取；
- LIST / MAP 的 skip/select overflow 推进。

当前还不具备完整生产能力，尤其缺少：

- schema change 的完整 field id 语义；
- filter localization 的完整实现；
- batch 内 `ColumnPredicate` 执行；
- `reader_expression_map`；
- page index / bloom filter / dictionary pruning；
- `Array(Struct)` / `Map<K, Struct>`；
- nested list/map；
- LIST/MAP child projection；
- 复杂类型 schema change；
- complex child nested lazy materialization；
- 充分单测覆盖。

最近验证状态：

- `git diff --check` 通过。
- Fedora `/home/socrates/code/doris` 上 `BUILD_TYPE=DEBUG ./build.sh --be` 通过。
- 本地 macOS 运行 `./run-be-ut.sh --run '--filter=ParquetColumnReaderTest.*'` 被环境阻断，CMake 检查 clang++ 时失败：`ld: library 'c++' not found`，未进入测试体。

## 下一步优先级

建议按以下顺序推进：

1. 抽象 Struct child sink，把 `Array(Struct)` 和 `Map<K, Struct>` 接到现有 LIST/MAP level assembler。
2. 将 LIST/MAP projection 从 top-level projection 扩展到 child projection，先支持 `array.element.<field>` 和 `map.value.<field>` 这类 Struct child 裁剪。
3. 为多 leaf stream 引入统一 cursor/overflow 状态，避免 Struct、Array、Map 各自维护不兼容的 read-ahead。
4. 收敛 `SchemaField` 和 `ColumnMapping` 的 id 语义，区分 Iceberg field id、Parquet leaf column id 和 file-local output position。
5. 设计复杂类型 schema change 的 child-level mapping 接口，先预留缺失 child/default/null/cast sink，不立即实现完整语义。
6. 补齐 batch 内 `ColumnPredicate` 执行，让 row group pruning 之后仍有正确 residual filter。
7. 实现 `reader_expression_map`，支撑 schema change 下无法安全下推的 filter fallback。
8. 在复杂列 assembler 稳定后，再做 nested pruning、nested lazy materialization、page index、bloom filter、dictionary pruning。

## 核心规则

`ParquetReader` 必须保持 file-local reader。

只要某个功能需要 table schema、Iceberg schema evolution、partition value、default/generated column、delete file 或最终 table block 语义，就应该放在 `TableColumnMapper`、`TableReader` 或具体 table reader 中，而不是放进 `be/src/format/new_parquet/`。
