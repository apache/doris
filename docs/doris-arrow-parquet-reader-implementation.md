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
- `StructColumnReader`：递归读取 children，支持非常基础的 struct 组装。

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
- 复杂列延时物化尚未实现。

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
- `StructColumnReader` 可以递归读取 children，支持非常基础的非 nullable struct。

主要缺口：

- nullable struct 未实现。
- list reader 未实现。
- map reader 未实现。
- repeated / nested definition level assembler 未实现。
- primitive reader 当前只支持 `max_repetition_level == 0 && max_definition_level <= 1` 的 RecordReader 路径。
- 复杂列裁剪未实现。
- 复杂列延时物化未实现。
- 复杂列 schema evolution / child remap 未实现。

结论：当前复杂列“schema 可见”，但“读取能力不完整”。真正可用还需要实现 Dremel assembler 或等价的 nested column assembler。

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
- 非 nullable struct 的初步读取框架。

当前还不具备完整生产能力，尤其缺少：

- schema change 的完整 field id 语义；
- filter localization 的完整实现；
- batch 内 `ColumnPredicate` 执行；
- `reader_expression_map`；
- page index / bloom filter / dictionary pruning；
- list/map/nullable struct；
- nested column pruning；
- nested lazy materialization；
- 充分单测覆盖。

## 下一步优先级

建议按以下顺序推进：

1. 收敛 `SchemaField` 和 `ColumnMapping` 的 id 语义，区分 Iceberg field id、Parquet leaf column id 和 file-local output position。
2. 补齐 batch 内 `ColumnPredicate` 执行，让 row group pruning 之后仍有正确 residual filter。
3. 实现 `reader_expression_map`，支撑 schema change 下无法安全下推的 filter fallback。
4. 补 flat primitive/string/decimal/timestamp 的 selected read 单测。
5. 实现 nullable struct，再实现 list/map assembler。
6. 在复杂列 assembler 稳定后，再做 nested pruning 和 nested lazy materialization。
7. 后续再接 page index、bloom filter、dictionary pruning。

## 核心规则

`ParquetReader` 必须保持 file-local reader。

只要某个功能需要 table schema、Iceberg schema evolution、partition value、default/generated column、delete file 或最终 table block 语义，就应该放在 `TableColumnMapper`、`TableReader` 或具体 table reader 中，而不是放进 `be/src/format/new_parquet/`。
