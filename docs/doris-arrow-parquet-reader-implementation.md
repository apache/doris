# Doris Arrow Parquet Reader 实现方案

本文档描述新 `ParquetReader` 的实现方案。目标是在 Doris 新 reader 分层中，使用
Arrow C++ Parquet core API 作为 Parquet 文件格式实现，但仍然输出 Doris 自己的
`Block` / column，不引入 Arrow 内存格式作为 scan 结果。

本文档描述实现设计，并作为后续实现阶段的边界说明。当前分支已开始落地阶段一代码：
`ParquetReader` 通过 Arrow Parquet core API 完成文件打开、footer metadata 读取、
file-local schema 导出和 scan 初始化；真实 `Block` 解码仍留到 flat primitive read
阶段实现。

## 目标

- 使用 Arrow C++ Parquet core API 替代 Doris 自研 Parquet page/column 解码栈。
- 保持 `doris::parquet::ParquetReader` 的 file-local reader 边界。
- 不让 `ParquetReader` 理解 Iceberg table schema、schema evolution、default column、
  generated column 或 partition column。
- 让 `TableColumnMapper` 继续作为 table schema 到 file schema 映射和 filter
  localization 的唯一入口。
- 第一阶段优先支持 flat primitive columns，后续再补复杂类型、page index、bloom
  filter 和高级延时物化。

## 非目标

- 不使用 `parquet::arrow::*` reader。
- 不输出 `arrow::Table`、`arrow::RecordBatch` 或 `arrow::Array`。
- 不在 `ParquetReader` 中处理 Iceberg field id mapping。
- 不在 `ParquetReader` 中执行 table-level cast/default/generated/finalize 语义。
- 不在第一阶段重写旧 `vparquet` 调用路径。

## 依赖边界

允许依赖 Arrow C++ Parquet core API：

- `parquet::ParquetFileReader`
- `parquet::FileMetaData`
- `parquet::SchemaDescriptor`
- `parquet::RowGroupReader`
- `parquet::ColumnReader`
- `parquet::TypedColumnReader<DType>`
- `parquet::ReaderProperties`
- `parquet::PageIndexReader`
- `parquet::BloomFilterReader`

禁止在 scan 输出路径中依赖：

- `parquet::arrow::FileReader`
- `parquet::arrow::RowGroupReader`
- `parquet::arrow::ColumnReader`
- `arrow::Table`
- `arrow::RecordBatch`
- `arrow::Array`

推荐 include 入口：

```cpp
#include <parquet/api/reader.h>
#include <parquet/api/schema.h>
```

`parquet/*internal*.h` 不应作为稳定依赖。Arrow 标记 experimental 的 API，例如
dictionary expose 或 record reader，需要封装在 Doris 内部 wrapper 后面，不能扩散到
`TableReader` / `IcebergTableReader` 接口。

## 总体架构

```text
IcebergTableReader / other TableReader
    ->
TableColumnMapper
    ->
reader::FileScanRequest
    ->
doris::parquet::ParquetReader
    ->
DorisRandomAccessFile
    ->
parquet::ParquetFileReader
    ->
parquet::RowGroupReader
    ->
parquet::TypedColumnReader<DType>
    ->
Doris Block / Column
```

其中 `DorisRandomAccessFile` 是 Doris `io::FileReader` 到
`arrow::io::RandomAccessFile` 的薄适配层。它只负责随机读和文件大小查询，不承载
Parquet schema、filter、projection 或 table-level 语义。

## 模块拆分

### DorisRandomAccessFile

职责：

- 持有 `io::FileReaderSPtr`。
- 实现 `arrow::io::RandomAccessFile`。
- 将 `ReadAt(position, nbytes, out)` 转发到 `io::FileReader::read_at`。
- 将 `GetSize()` 转发到 `io::FileReader::size()`。
- 保存可选 `io::IOContext*`，用于 Doris IO 统计、cache 和 tracing。

边界：

- 不解析 Parquet footer。
- 不缓存 row group。
- 不实现 Doris scan 逻辑。
- 不暴露到 table reader 层。

### ParquetReaderScanState

作为 `ParquetReader` 内部 scan 状态，当前直接定义在
`be/src/format/new_parquet/parquet_reader.cpp` 中，不单独拆文件。它负责保存：

- `std::shared_ptr<arrow::io::RandomAccessFile>`；
- `std::unique_ptr<parquet::ParquetFileReader>`；
- `std::shared_ptr<parquet::FileMetaData>`；
- `const parquet::SchemaDescriptor*`；
- row group 总数、当前 row group 下标；
- projected column ordinals；
- selected row groups；
- 当前 row group 的 `parquet::RowGroupReader`；
- 当前 row group 的 column readers。

该状态不应暴露到 `FileReader` 基类，也不应暴露到 `IcebergTableReader`。
命名对齐 DuckDB 的 `ParquetReaderScanState`，但内部仍然使用 Arrow Parquet core
reader 作为物理列读取实现。

### ParquetColumnReader

为每个 projected leaf column 维护一个 `ParquetColumnReader` 对象。这个对象是 Doris
自己的 file-local column reader 抽象，当前内部包装 Arrow Parquet core
`parquet::ColumnReader`，后续可以在同一抽象下扩展 `skip`、selective read、cache/reuse
和复杂类型递归读取。

当前阶段有 `PrimitiveColumnReader` 和第一版 `StructColumnReader`：

- `PrimitiveColumnReader` 支持 `max_repetition_level == 0` 的 primitive、string、
  decimal 和 int64 timestamp。
- `StructColumnReader` 支持 required struct，并递归读取 child reader 后组装
  `ColumnStruct`。

list/map 和 nullable struct 仍需要完整 Dremel definition/repetition level assembler，
当前不会伪装支持。

`ParquetColumnReader` 不通过自由函数创建，而是由 `ParquetColumnReaderFactory`
统一创建。factory 绑定当前 row group 的 Arrow Parquet core `ColumnReader` 列表，
再根据 `ParquetColumnSchema` 递归创建 primitive/struct reader。这样后续 reader
options、Dremel assembler、延时物化 cache/skip 策略可以挂在同一个 file-local
创建上下文中，避免把这些状态继续散落到自由函数参数里。

实际代码文件：

```text
be/src/format/new_parquet/column_reader.h
be/src/format/new_parquet/column_reader.cpp
```

该模块对齐 DuckDB 的 `column_reader.*` 命名，但职责不同：DuckDB 的
`ColumnReader` 自己处理 page/encoding/definition/repetition 解码；Doris 新实现中，
底层解码由 Arrow Parquet core `TypedColumnReader` 完成，`column_reader.*` 只负责
把 Arrow 读出的 file-local values 转换成 Doris-owned column。

### ParquetStatistics

实际代码文件：

```text
be/src/format/new_parquet/parquet_statistics.h
be/src/format/new_parquet/parquet_statistics.cpp
```

该模块对齐 DuckDB 的 `parquet_statistics.*` 命名，作为 row group statistics、page
index、bloom filter pruning 的统一入口。当前阶段实现保守返回全部 row group，后续
所有基于 Parquet metadata 的 pruning 都应放在这里，避免把 stats/bloom filter 逻辑塞回
`ParquetReader` 的 scan 调度代码。

## 方法设计

### open

输入：

- Doris `io::FileReaderSPtr`
- 可选 `io::IOContext*`

职责：

- 调用 `reader::FileReader::open` 保存基础文件句柄。
- 构造 `DorisRandomAccessFile`。
- 使用 `parquet::ParquetFileReader::Open` 打开 Parquet 文件。
- 读取 `FileMetaData`。
- 保存 `SchemaDescriptor`。
- 初始化 reader 状态为未开始扫描。

错误处理：

- Arrow status 转换为 Doris `Status`。
- Parquet exception 转换为 Doris `Status::InternalError` 或 `Status::Corruption`。
- 如果文件不是合法 Parquet，返回错误，不伪装为空文件。

### get_schema

输入：

- 输出参数 `std::vector<reader::SchemaField>*`

职责：

- 遍历 `SchemaDescriptor::num_columns()`。
- 对每个 leaf column 读取：
  - leaf ordinal；
  - column path；
  - physical type；
  - logical type；
  - converted type；
  - precision / scale；
  - max definition level；
  - max repetition level。
- 转换成 file-local `SchemaField`。

命名建议：

- flat column 使用 leaf name；
- nested column 第一阶段可以使用 dot path，例如 `a.b.c`；
- 后续复杂列完整支持时，再把 tree shape 填入 `children`。

类型映射原则：

- Parquet physical/logical type 只映射成 file-local Doris type。
- Iceberg schema evolution 的类型提升不在这里处理。
- 缺失列、default、generated、partition column 不在这里出现。

### init

输入：

- `reader::FileScanRequest`

职责：

- 保存 scan request。
- 校验 projected file columns 是否存在于 Parquet schema。
- 将 `projected_file_columns` 转成 Parquet leaf column ordinal。
- 根据 `local_filters` 执行 row group pruning。
- 生成 selected row group 列表。
- 初始化 row group 游标。
- 准备延时物化状态。

第一阶段 row group pruning：

- 只使用 column chunk statistics。
- 只处理能确定安全的单列结构化谓词。
- 对无法判断的 predicate 保守保留 row group。

后续扩展：

- page index pruning；
- bloom filter pruning；
- reader expression fallback；
- column IO range prefetch。

### next

输入：

- `Block* file_block`
- `size_t* rows`
- `bool* eof`

职责：

- 如果当前 row group 没有打开，打开下一个 selected row group。
- 通过 `ParquetColumnReaderFactory` 为当前 row group 创建 projected column readers。
- 按 batch size 读取一批 file-local rows。
- 将 decoded values 写入 Doris columns。
- 当前 row group 读完后切换到下一个 selected row group。
- 所有 row group 读完后设置 `eof=true`。

第一阶段建议能力：

- flat required primitive columns；
- flat nullable primitive columns；
- string/binary 拷贝到 Doris-owned memory；
- decimal/timestamp 可先按明确子任务逐步补齐。

第一阶段不建议直接实现：

- struct/list/map；
- repeated column 延时物化；
- dictionary-aware 输出；
- table-level cast；
- Iceberg equality delete。

### close

职责：

- 关闭 Arrow Parquet reader；
- 释放 row group / column reader 状态；
- 释放 `DorisRandomAccessFile`；
- 调用 `reader::FileReader::close` 清理基础状态。

要求：

- 幂等；
- 可在 `open` 失败后调用；
- 可在中途扫描失败后调用。

## Doris Block 写入策略

`ParquetReader` 输出的是 file-local block。block 列顺序应与 `FileScanRequest` 中的
`projected_file_columns` 一致。

建议流程：

1. 根据每个 projected column 的 file-local type 创建 Doris mutable column。
2. 调用 Arrow Parquet typed reader 解码 physical values 和 levels。
3. 根据 definition levels 填充 null map。
4. 对 required flat column，直接批量写入 values。
5. 对 nullable flat column，按 levels 将 compact values 扩展成 Doris nullable column。
6. 对 binary/string，必须复制到 Doris column，不能保存 Arrow/Parquet page buffer 指针。
7. 将 mutable column 封装成 `ColumnWithTypeAndName` 插入 `Block`。

注意：

- `ParquetReader` 不能输出 table/global schema 类型。
- 如果 file type 与 table type 不同，由 `IcebergTableReader` 根据 `ColumnMapping` 执行
  finalize。
- 如果谓词列同时也是 projection 列，可以在 file-local block 级别复用第一次解码结果。

## Nullable 处理

Arrow Parquet `ReadBatch` 返回两类数量：

- levels read：definition/repetition level 数量；
- values read：真正解码到 values buffer 的非 null physical values 数量。

对 nullable flat column：

- `levels_read` 对应 rows；
- `values_read` 对应非 null values；
- 当 definition level 达到 column max definition level 时，从 values buffer 消费一个值；
- 否则在 Doris null map 中标记 null，并给 nested column 填默认占位值。

对 required flat column：

- 可以不读取 definition levels；
- `levels_read` 和 rows 通常等价；
- values 可以批量写入 Doris column。

## 延时物化

第一阶段只建议支持 flat columns 的延时物化。

流程：

1. 将 filter columns 和 output-only columns 分开。
2. 第一阶段只读取 filter columns。
3. 在 Doris 层计算 selection。
4. 第二阶段读取 output-only columns。
5. 如果 filter column 同时在 projection 中，复用第一阶段 file-local 解码结果。

限制：

- `TypedColumnReader::Skip` 跳过的是 physical values，不是 semantic rows。
- 对 nullable column，需要通过 definition levels 才能知道 row 到 value 的映射。
- 对 repeated/nested column，row 和 value 不是一一对应，不能直接用 `Skip` 实现按行跳过。
- 因此复杂列延时物化需要单独设计 row boundary 和 def/rep level 状态。

## 谓词下推

输入只来自 `FileLocalFilter`。

分层：

- `TableColumnMapper`：决定 table filter 是否能 localize 到 file column。
- `ParquetReader`：只使用 file-local predicate 做 pruning 或解码后过滤。
- `IcebergTableReader`：负责 table-level finalize 后的语义。

优化顺序：

1. row group statistics；
2. page index；
3. bloom filter；
4. dictionary filtering；
5. decoded value filtering。

正确性原则：

- 任何不确定的统计信息都不能用于排除数据。
- 写入器兼容性未知时必须保守保留 row group/page。
- bloom filter 只能用于等值类谓词。
- 如果谓词涉及 table-level cast，必须先由 `TableColumnMapper` 转成 file-local filter。

## 复杂类型计划

复杂类型不要在第一阶段和 flat primitive reader 混在一起实现。

需要独立设计：

- Parquet schema tree 到 Doris nested type 的映射；
- 多 leaf columns 的 row 对齐；
- definition level 到 struct/list/map nullability 的转换；
- repetition level 到 list/map offset 的转换；
- 空 list、null list、含 null element list 的区分；
- map key/value 的一致性校验；
- nested column 的延时物化边界。

如果考虑复用 Arrow 的 record-level reader，需要先确认 API 稳定性。当前建议不直接依赖
`parquet::internal::RecordReader` 作为 Doris 新 reader 的公共实现基础。

## 字典编码计划

第一阶段：

- 让 Arrow Parquet core reader 解码 dictionary；
- Doris 接收普通 values；
- 不把 dictionary encoding 暴露到 Doris execution。

后续优化：

- 调研 `ColumnWithExposeEncoding`；
- 调研 `ReadBatchWithDictionary`；
- 只在 Doris 内部 wrapper 中使用；
- 不改变 `FileReader` / `TableReader` API。

## 错误处理

建议统一转换：

- Arrow IO error -> `Status::IOError`
- Arrow invalid / Parquet exception -> `Status::InternalError` 或 `Status::Corruption`
- unsupported physical/logical type -> `Status::NotSupported`
- corrupted levels / inconsistent values -> `Status::Corruption`

原则：

- 不把实现未完成的路径伪装成 EOF。
- 不 silently fallback 到旧 `vparquet`。
- 不吞掉 Arrow/Parquet 的错误上下文。

## 实施阶段

### 阶段 1：Metadata And Schema

- 新增 `DorisRandomAccessFile` adapter。
- `ParquetReader::open` 使用 `parquet::ParquetFileReader` 打开文件。
- `ParquetReader::get_schema` 转出 file-local schema。
- `ParquetReader::init` 保存 projection 并初始化 row group 列表。
- `next` 对未支持的数据读取返回 `NotSupported`。

当前实现说明：

- `DorisRandomAccessFile` 只适配 `io::FileReader::read_at`、`size`、`Seek`、`Tell` 和
  `ReadAt`，不承载 schema/filter/projection 语义。
- `get_schema` 只展开 Parquet leaf columns，字段 id 使用 Parquet leaf ordinal，字段名
  使用 column dot path。
- schema type 映射优先使用 logical type，其次使用 converted type，最后才回退到
  physical type。无法明确映射的类型返回 `NotSupported`。
- `init` 只校验 file-local projection，并保守选择所有 row groups；row group/page/bloom
  pruning 留到后续阶段。
- `next` 不伪装 EOF。只要文件仍有待读 row group，就返回 `NotSupported`，明确表示
  Doris `Block` 解码尚未实现。

验收：

- 能打开合法 Parquet 文件。
- 能返回 leaf schema。
- 非法 Parquet 文件返回明确错误。

### 阶段 2：Flat Primitive Read

- 支持 required bool/int32/int64/float/double。
- 支持 nullable bool/int32/int64/float/double。
- 输出 Doris file-local block。

当前实现说明：

- `ParquetReader::next` 已支持按 row group 顺序读取 flat physical primitive columns。
- 输出 `Block` 的列顺序与 `FileScanRequest::projected_file_columns` 一致，列名使用
  Parquet file-local dot path，列类型使用 file-local Doris type。
- nullable primitive column 根据 definition level 展开成 Doris nullable column。
- 第二阶段只接受没有 logical/converted annotation、`max_repetition_level == 0`、
  `max_definition_level <= 1` 的物理 primitive 列。带 annotation 的 int/date/time/
  timestamp/decimal/string 仍留给后续阶段。
- 空 projection 只推进 row group 行游标，不向 `Block` 插入列。

验收：

- flat primitive Parquet 文件可读。
- null bitmap 正确。
- projection 顺序正确。

### 阶段 3：String / Decimal / Timestamp

- 支持 BYTE_ARRAY / FIXED_LEN_BYTE_ARRAY string/binary。
- 支持 decimal physical representation。
- 支持 INT64 physical 的 logical / converted timestamp。
- INT96 和 TIMESTAMPTZ 作为后续兼容性子任务。

当前实现说明：

- 已支持 `BYTE_ARRAY` / `FIXED_LEN_BYTE_ARRAY` 到 Doris `String`，读取时会复制
  Parquet page buffer 中的字节到 Doris column。
- 已支持 precision <= 38 的 decimal，物理类型覆盖 `INT32`、`INT64`、`BYTE_ARRAY` 和
  `FIXED_LEN_BYTE_ARRAY`，byte array 按 Parquet 规范的 big-endian two's complement
  unscaled value 解码到 `DECIMAL128I`。
- 已支持 INT64 physical 的 logical / converted timestamp millis 和 micros，输出
  `DateTimeV2`，当前使用 UTC 做基础转换。
- 暂未支持 `INT96`、`TIMESTAMPTZ`、nanosecond timestamp 和 `DECIMAL256`。这些路径需要
  明确 timezone、兼容性和溢出策略后单独实现。

验收：

- string 生命周期由 Doris column 持有。
- decimal precision/scale 正确。
- timestamp 单位转换明确。

### 阶段 4：Pruning

- row group statistics pruning。
- page index pruning。
- bloom filter pruning。

验收：

- pruning 不影响正确性。
- 不可靠 stats 保守跳过优化。
- filter 仍然只使用 file-local predicate。

### 阶段 5：Lazy Materialization

- 支持 flat column 两阶段读取。
- 谓词列同时是 projection 时复用第一阶段结果。

验收：

- selection 正确。
- 不重复读取可复用谓词列。
- nested column 不误用 value skip。

### 阶段 6：Complex Types

- struct；
- list；
- map；
- nested nullable/repeated combinations；
- complex column projection 和 lazy materialization。

验收：

- 能区分 null list、empty list、list with null element。
- 多 leaf columns row alignment 正确。

## 测试建议

- metadata-only test：只验证 `open` / `get_schema`。
- flat required primitive read。
- flat nullable primitive read。
- projection order test。
- row group pruning correctness test。
- corrupted footer / invalid file test。
- unsupported type returns `NotSupported`。
- string lifetime test。
- decimal precision/scale test。
- timestamp unit test。
- nested list/map golden files。

## 关键结论

新 `ParquetReader` 可以基于 Arrow C++ Parquet core API 实现，但它仍然必须是 Doris
file-local reader。Arrow 负责 Parquet 文件格式和 page/column 解码，Doris 负责输出
Doris columns、执行 table-level mapping/finalize，并保持 Iceberg schema evolution 在
`TableColumnMapper` / `IcebergTableReader` 层处理。
