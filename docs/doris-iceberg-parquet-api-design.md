# Doris Iceberg + Parquet 新架构 API 设计

本文档用于描述 Doris 中 Iceberg + Parquet 新架构的 API 设计。本文档作为后续从
`master` 新开重构分支时的起点，只定义 API 形状、职责边界、依赖方向和兼容原则，
不定义函数实现细节，不提供伪代码，不包含迁移 patch。

## 架构总览

目标架构包含 table 调度层、表格式语义层、schema 映射层、文件通用层和文件格式实现层：

```text
FileScanner / split producer
    ->
TableReader
    ->
IcebergTableReader
    ->
TableColumnMapper + FileReader
    ->
ParquetReader
```

核心职责如下：

- `TableReader`
  负责多文件、多 split 的上层调度，统一 scan 生命周期，对外输出 table block，
  并承接动态分区裁剪等 table-level 通用逻辑。
- `IcebergTableReader`
  负责 Iceberg 表语义，包括 schema 绑定、scan task、delete file、虚拟列和 table
  block finalize。
- `TableColumnMapper`
  负责 table schema 到 file schema 的映射，负责 filter localization 和 schema
  change 映射。
- `FileReader`
  负责文件层通用读取接口，只理解 file-local schema 和 file-local scan request。
- `ParquetReader`
  作为 `FileReader` 的 Parquet 实现，负责 Parquet 文件物理读取。

依赖方向必须保持单向：

```text
TableReader
  -> IcebergTableReader
    -> TableColumnMapper
    -> FileReader
      -> ParquetReader
```

低层不反向理解高层语义，尤其 `ParquetReader` 不得反向理解 Iceberg/global schema。

## 核心 API 设计

### TableReader

`TableReader` 是最上层读取接口，作为 `IcebergTableReader` 的基类，负责多 split /
多 file 调度，并承接 table-level 的通用裁剪逻辑，不下沉文件格式语义。

实际 API 文件：

```text
be/src/format/reader/table_reader.h
```

实际命名空间：

```cpp
namespace doris::reader
```

建议职责：

- 接收 split 列表或 scan task 列表；
- 控制当前 reader 的创建、切换和关闭；
- 管理 scan 生命周期；
- 承接动态分区裁剪等 table-level 通用过滤逻辑；
- 对外统一输出 table block。
- `next` 是基类统一入口，内部负责 EOF 后切换 reader；具体表格式只提供打开和读取
  当前 reader 的 hook。

建议接口形状：

```cpp
namespace doris::reader {

class TableReader {
public:
    virtual ~TableReader() = default;

    virtual Status init(const TableReadOptions& options);
    virtual Status filter(const VExprContextSPtr& expr, bool* can_filter_all);
    Status next(Block* table_block, size_t* rows, bool* eof);
    virtual Status close();

protected:
    Status next_reader();
    virtual Status open_next_reader(bool* has_reader);
    virtual Status read_current(Block* table_block, size_t* rows, bool* eof);
    virtual Status close_current_reader();
};

} // namespace doris::reader
```

接口约束：

- `TableReader` 输出的是 table block，不输出 file-local block。
- `TableReader` 负责多文件编排和 table-level 通用裁剪，不负责 schema mapping，不负责
  Parquet 物理解码。
- `next_reader` 是 `TableReader` 自己的通用切换逻辑，不作为子类公开 override 接口。
- 动态分区裁剪这类逻辑应下放到 `TableReader`，而不是散落在具体表格式 reader 中。
- `TableReader` 不直接依赖旧 `vparquet` 表层语义。

### IcebergTableReader

`IcebergTableReader` 是 Iceberg 表语义层，负责把单个 Iceberg data file 的读取组织成
table 语义输出。

实际 API 文件：

```text
be/src/format/table/iceberg_reader_v2.h
```

实际命名空间：

```cpp
namespace doris::iceberg
```

建议职责：

- 绑定 Iceberg 当前 table schema；
- 接收 `IcebergScanTask` 列表，并按 `TableReader` 的统一调度打开当前 task；
- 处理 position delete、equality delete、deletion vector；
- 物化 `_row_id`、`_last_updated_sequence_number` 等虚拟列；
- 将 `ParquetReader` 返回的 file-local block finalize 成 table block。

建议接口形状：

```cpp
namespace doris::iceberg {

class IcebergTableReader : public reader::TableReader {
public:
    virtual ~IcebergTableReader() = default;

    Status init(IcebergTableReadParams params);
    Status close() override;

protected:
    Status open_next_reader(bool* has_reader) override;
    Status read_current(Block* table_block, size_t* rows, bool* eof) override;
    Status close_current_reader() override;
};

} // namespace doris::iceberg
```

接口约束：

- `IcebergTableReader` 继承 `TableReader`，并通过组合使用 `FileReader`。
- `IcebergTableReader` 不做 Parquet page/column 解码。
- `IcebergTableReader` 负责 table-level finalize，不负责 file-local pruning 实现。
- `IcebergTableReader` 的 schema、scan request、scan tasks 和底层 `FileReader` 应通过
  一个初始化参数对象一次性传入；除非存在明确生命周期差异，不拆成 `bind` /
  `init(TableScanRequest)` / `set_scan_tasks` 多阶段接口。
- `IcebergTableReader` 不重新实现 reader 切换循环，只实现打开 Iceberg task、读取当前
  task 和关闭当前 reader 的 hook。

### TableColumnMapper

`TableColumnMapper` 是 table schema 到 file schema 的通用映射层，不是
Iceberg-only 组件。

实际 API 文件：

```text
be/src/format/reader/table_reader.h
```

实际命名空间：

```cpp
namespace doris::reader
```

建议职责：

- 输入 table schema、file schema、table scan request；
- 输出 `ColumnMapping` 和通用 `FileScanRequest`；
- 负责 filter localization；
- 负责 schema change 映射；
- 负责复杂列 child mapping；
- 负责缺失列、default、partition、generated 列的 finalize 语义描述。

建议接口形状：

```cpp
namespace doris::reader {

class TableColumnMapper {
public:
    explicit TableColumnMapper(TableColumnMapperOptions options = {});

    virtual Status create_mapping(const std::vector<TableColumn>& table_schema,
                                  const std::vector<SchemaField>& file_schema,
                                  std::vector<ColumnMapping>* mappings);

    virtual Status create_scan_request(const TableScanRequest& table_request,
                                       const std::vector<ColumnMapping>& mappings,
                                       FileScanRequest* file_request);
};

} // namespace doris::reader
```

接口约束：

- `TableColumnMapper` 的输入是 table schema + file schema + table scan request。
- `TableColumnMapper` 的输出是 `ColumnMapping` + `FileScanRequest`。
- `TableColumnMapper` 必须是通用层，不做 Iceberg-only 命名。
- Iceberg 场景默认按 field id 映射；按 name 映射不是本轮默认路径。

### FileReader

`FileReader` 是文件物理读取层的通用接口，为后续 Parquet 之外的文件格式适配预留。

实际 API 文件：

```text
be/src/format/reader/file_reader.h
```

实际命名空间：

```cpp
namespace doris::reader
```

建议职责：

- 打开物理文件；
- 暴露 file-local schema；
- 接收 `FileScanRequest`；
- 输出 file-local block；
- 不理解 table/global schema。

建议接口形状：

```cpp
namespace doris::reader {

class FileReader {
public:
    virtual ~FileReader() = default;

    virtual Status open(io::FileReaderSPtr file, io::IOContext* io_ctx = nullptr);
    virtual Status get_schema(std::vector<SchemaField>* file_schema) const;
    virtual Status init(const FileScanRequest& request);
    virtual Status next(Block* file_block, size_t* rows, bool* eof);
    virtual Status close();
};

} // namespace doris::reader
```

接口约束：

- `FileReader` 输出的是 file-local block，不输出 table/global schema block。
- `FileReader` 不处理 Iceberg schema evolution、default/generated/partition 列。
- `IcebergTableReader` 组合 `FileReader`，不直接绑定具体文件格式 reader。

### ParquetReader

`ParquetReader` 是 `FileReader` 的 Parquet 实现，只负责 Parquet file-local schema
和 Parquet file-local scan request。

实际 API 文件：

```text
be/src/format/parquet/parquet_reader.h
```

实际命名空间：

```cpp
namespace doris::parquet
```

建议职责：

- 打开 Parquet 文件；
- 解析 footer 和 file schema；
- 接收 `ParquetScanRequest` 或通用 `FileScanRequest`；
- 执行 file-local projection 和 file-local filter；
- 输出 file-local block。

建议接口形状：

```cpp
namespace doris::parquet {

class ParquetReader : public reader::FileReader {
public:
    virtual ~ParquetReader() = default;

    virtual Status open(io::FileReaderSPtr file, io::IOContext* io_ctx = nullptr);
    virtual Status get_schema(std::vector<reader::SchemaField>* file_schema) const;
    virtual Status init(const ParquetScanRequest& request);
    virtual Status next(Block* file_block, size_t* rows, bool* eof);
    virtual Status close();
};

} // namespace doris::parquet
```

接口约束：

- `ParquetReader` 输出的是 file-local block，不输出 table/global schema block。
- `ParquetReader` 不理解 Iceberg schema evolution。
- `ParquetReader` 不负责 default/generated/partition 列。
- 任何 table-level cast/default/generated/partition 语义都不能重新塞回
  `ParquetReader`。

## ParquetReader 实现方案

新 `ParquetReader` 建议基于 Arrow C++ Parquet core API 实现，而不是继续在 Doris 中
自研完整 Parquet 解码栈。这里的“使用 Arrow”只指使用 Arrow C++ 的 Parquet 文件格式
实现，不使用 Arrow 的内存格式适配层。

### 依赖边界

允许依赖：

- `parquet::ParquetFileReader`
- `parquet::FileMetaData`
- `parquet::SchemaDescriptor`
- `parquet::RowGroupReader`
- `parquet::ColumnReader`
- `parquet::TypedColumnReader<DType>`
- `parquet::ReaderProperties`
- `parquet::PageIndexReader`
- `parquet::BloomFilterReader`

不应依赖：

- `parquet::arrow::FileReader`
- `parquet::arrow::RowGroupReader`
- `parquet::arrow::ColumnReader`
- `arrow::Table`
- `arrow::RecordBatch`
- `arrow::Array`

原因是 Doris 仍然保留自己的 `Block` 和列式内存格式。Arrow Parquet core API 只负责
解析 Parquet 文件、解码 page、输出 physical values、definition levels 和 repetition
levels；Doris 自己负责把这些结果写入 Doris column。

建议 include 入口：

```cpp
#include <parquet/api/reader.h>
#include <parquet/api/schema.h>
```

避免直接依赖 `parquet/*internal*.h`。如果后续确实需要 Arrow 标记为 experimental 的
API，例如 dictionary expose 或 record reader，应在 Doris 自己的 wrapper 后面隔离。

### 内部调用链

`ParquetReader` 内部建议按以下链路实现：

```text
Doris io::FileReader
    ->
arrow::io::RandomAccessFile adapter
    ->
parquet::ParquetFileReader
    ->
parquet::FileMetaData / parquet::SchemaDescriptor
    ->
parquet::RowGroupReader
    ->
parquet::TypedColumnReader<DType>::ReadBatch
    ->
Doris Block / Column
```

其中 `arrow::io::RandomAccessFile adapter` 是 Doris 文件系统和 Arrow Parquet core API
之间的薄适配层。它只转发随机读请求，不引入 Arrow table/array 作为 scan 输出。

### 方法落点

`ParquetReader::open`：

- 保存 Doris 文件句柄和 IO 上下文；
- 构造 Doris file 到 `arrow::io::RandomAccessFile` 的 adapter；
- 创建 `parquet::ParquetFileReader`；
- 读取 footer metadata；
- 保存 `FileMetaData` 和 `SchemaDescriptor`。

`ParquetReader::get_schema`：

- 从 `parquet::SchemaDescriptor` 展开 Parquet leaf columns；
- 结合 physical type、logical type、converted type、precision、scale、max definition
  level、max repetition level 构造 `reader::SchemaField`；
- 输出仍然是 file-local schema，不做 Iceberg field id mapping，不填 default/generated
  columns。

`ParquetReader::init`：

- 接收 `reader::FileScanRequest` 或 `ParquetScanRequest`；
- 根据 `projected_file_columns` 建立待读取的 Parquet column ordinal 集合；
- 根据 `local_filters` 做 row group pruning；
- 根据 page index 和 bloom filter 规划 page-level pruning；
- 初始化当前 row group、column readers 和延时物化状态；
- 不解释 table/global schema，不做 table-level cast/default/generated/partition 语义。

`ParquetReader::next`：

- 按 Doris batch size 从当前 row group 读取数据；
- 对每个投影 leaf column 调用对应的 `TypedColumnReader<DType>::ReadBatch`；
- 使用 definition levels 还原 nullable slots；
- 使用 repetition levels 组装 nested value 边界；
- 将 physical values 转换为 Doris file-local column；
- 当前 row group 读完后切换到下一个未被裁剪的 row group；
- 所有 row group 读完后返回 `eof=true`。

### 谓词下推

谓词下推仍然由 `TableColumnMapper` 决定哪些 table filter 可以变成 file-local filter。
`ParquetReader` 只消费 `FileLocalFilter`。

推荐分层：

- row group pruning：使用 `ColumnChunkMetaData::statistics()`；
- page pruning：使用 `PageIndexReader` / `ColumnIndex` / `OffsetIndex`；
- bloom filter：使用 `BloomFilterReader`；
- 解码后过滤：使用 `FileLocalFilter::conjunct` 或 reader expression fallback 的结果。

关键约束：

- Parquet 层只能基于 file-local column id 和 file-local type 做判断；
- 如果 filter 涉及 table-level cast/default/generated/partition 语义，必须先由
  `TableColumnMapper` 转换；
- 不能让 Parquet 层重新理解 Iceberg schema evolution。

### 延时物化

基于 Arrow core column reader 可以支持 flat column 的延时物化：

- 第一阶段读取谓词列；
- 在 Doris 层计算 selection；
- 第二阶段读取 projection 列并按 selection 输出。

需要注意：

- `TypedColumnReader::Skip` 跳过的是 physical values，不是 semantic rows；
- 对 repeated/nested column，values 和 rows 不是一一对应关系；
- 因此第一阶段建议只把 flat columns 的延时物化作为默认目标；
- nested columns 的延时物化需要基于 definition/repetition levels 维护 row 边界，不能简单
  用 value skip 代替 row skip。

如果谓词列同时也是 projection 列，`ParquetReader` 可以在第一阶段缓存该 file-local
column 的解码结果，第二阶段直接复用，避免重复读取。但该缓存仍然是 file-local block
语义；是否需要执行 `finalize_expr` 由 `IcebergTableReader` 决定。

### 复杂类型

Arrow core `TypedColumnReader` 是 leaf-column API。它适合直接读取 primitive leaf
values 和 def/rep levels，但不会直接生成 Doris 的 struct/list/map column。

Doris 需要自己实现：

- struct/list/map 的 schema 展开；
- leaf column 到复杂列 child 的对应关系；
- definition level 到 nullability 的转换；
- repetition level 到 list/map row boundary 的转换；
- 多个 leaf columns 之间的 row 对齐。

第一阶段建议优先支持 flat primitive columns，并把复杂类型作为独立能力分阶段补齐。
如果后续考虑使用 Arrow 的 record-level reader，需要单独评估 API 稳定性，因为相关路径
存在 experimental/internal 风险。

### 类型转换

`ParquetReader` 只负责 Parquet physical/logical type 到 Doris file-local type 的转换。
例如：

- Parquet `INT32` / `INT64` 到 Doris 整数类型；
- Parquet `BYTE_ARRAY` 到 Doris string；
- Parquet decimal physical representation 到 Doris decimal；
- Parquet timestamp logical type 或 `INT96` 到 Doris file-local timestamp 表示。

table schema change 引入的类型转换不在 `ParquetReader` 中处理。比如 Iceberg table
schema 从 `int` 演进为 `long`，应由 `TableColumnMapper` 生成 mapping/finalize 表达式，
`ParquetReader` 仍然按文件里的 `int` 读取。

### 字典编码

默认方案是让 Arrow Parquet core reader 解码 dictionary，输出普通 values。这样 API
稳定性最好，也最容易接入 Doris 现有列。

后续如果要保留 dictionary indices 以支持 dictionary-aware execution，可以考虑
`ColumnWithExposeEncoding` / `ReadBatchWithDictionary`，但这些 API 带 experimental 属性，
必须封装在 Doris 内部 wrapper 后面，不能扩散到 `TableReader` 或 `IcebergTableReader`
接口。

### 实施顺序

建议按以下顺序落地：

1. 实现 Doris file 到 `arrow::io::RandomAccessFile` 的 adapter。
2. 用 `parquet::ParquetFileReader` 打开文件并解析 metadata/schema。
3. 支持 flat primitive columns 的 projection 读取。
4. 支持 nullable flat columns，通过 definition levels 还原 null bitmap。
5. 接入 row group statistics pruning。
6. 接入 flat column 的延时物化。
7. 接入 page index 和 bloom filter。
8. 分阶段支持 decimal、timestamp、string lifetime 管理和复杂类型。

## 关键类型

### SchemaField

`SchemaField` 表示文件层 schema 中的列定义。

建议包含的信息：

- file-local column id；
- 列名；
- 类型；
- child fields。

它服务于 `TableColumnMapper` 做 schema matching，不携带 table-level 语义。

### TableColumn

`TableColumn` 表示 table/global schema 中的列定义。

建议包含的信息：

- table column id；
- 列名；
- 类型；
- child columns。

Iceberg 场景下，column id 默认对应 field id。

### TableFilter

`TableFilter` 表示 table 层过滤条件。

建议包含的信息：

- `table_column_id`
- `conjunct`
- `predicates`

职责约束：

- `conjunct` 偏表达式过滤，适合表达 cast、复杂表达式、复杂列提取等语义；
- `predicates` 偏结构化单列下推，适合驱动 row group stats、page index、dictionary、
  bloom filter 等文件层优化。

### FileLocalFilter

`FileLocalFilter` 表示已经 localize 到 file-local schema 的过滤条件。

建议包含的信息：

- `file_column_id`
- `conjunct`
- `predicates`

职责约束：

- `conjunct` 用于 file-local 表达式过滤；
- `predicates` 用于 file-local 结构化下推；
- 其输入必须来自 `TableColumnMapper`，不能由具体文件 reader 自己推导 table 语义。

### ColumnMapping

`ColumnMapping` 是 table schema 与 file schema 之间的核心边界对象。

建议包含的信息：

- `table_column_id`
- `file_column_id`
- `file_type`
- `table_type`
- `finalize_expr`
- `reader_filter_expr`
- `child_mappings`

职责约束：

- `finalize_expr` 服务最终输出，把 file-local value 转成 table/global value；
- `reader_filter_expr` 服务读时 filter fallback；
- 二者语义不同，不能混用；
- `child_mappings` 用于复杂列 remap、复杂列裁剪和复杂列 schema change。

### TableScanRequest

`TableScanRequest` 描述 table 层 scan 请求。

建议包含的信息：

- projected table columns；
- table filters。

它由 `IcebergTableReader` 接收，再交给 `TableColumnMapper` 生成 file-local request。

### ParquetScanRequest

`ParquetScanRequest` 继承 `FileScanRequest`，描述 Parquet file-local scan 请求。

### FileScanRequest

`FileScanRequest` 描述通用 file-local scan 请求。

建议包含的信息：

- projected file columns；
- local filters；
- reader expression map。

它是 `FileReader` 的唯一 scan 输入，不包含 table/global schema 语义。

### IcebergScanTask

`IcebergScanTask` 表示一次 Iceberg data file 读取任务。

建议包含的信息：

- data file 信息；
- position delete 文件；
- equality delete 文件；
- deletion vector 信息。

它是 `IcebergTableReader` 的输入，不应直接传给 `ParquetReader`。

### IcebergTableReadParams

`IcebergTableReadParams` 表示一次 Iceberg table scan 的完整初始化输入。

建议包含的信息：

- Iceberg read options；
- Iceberg table schema；
- table scan request；
- Iceberg scan task 列表；
- 底层 `FileReader`。

它用于避免 `IcebergTableReader` 暴露多个半初始化阶段。调用方应一次性构造完整
参数并调用 `init`。

## 设计原则

### 边界原则

- `FileReader` 不理解 global schema，不直接处理 Iceberg schema evolution。
- `ParquetReader` 是 `FileReader` 的 Parquet 实现。
- `TableColumnMapper` 是 schema mapping 和 filter localization 的唯一入口。
- `IcebergTableReader` 不做 Parquet 解码，只负责 table-level finalize、delete、
  virtual columns。
- `TableReader` 只负责多文件编排和 table-level 通用裁剪，不下沉文件格式语义。
- 任何 table-level cast/default/generated/partition 语义都不能重新塞回
  `ParquetReader`。

### 依赖原则

- 低层不能反向依赖高层语义。
- `FileReader` 只依赖 file-local request。
- `IcebergTableReader` 继承 `TableReader`，复用其多文件编排和通用裁剪能力。
- `IcebergTableReader` 通过组合使用 `FileReader`。
- `TableColumnMapper` 可以被 Iceberg 之外的其他表格式复用。

### 命名原则

- 表层抽象使用 `TableReader`、`IcebergTableReader`、`TableColumnMapper`、
  `FileReader`、`ParquetReader` 命名。
- `TableColumnMapper` 不使用 Iceberg-only 命名。
- file schema 类型使用 `SchemaField`，table schema 类型使用 `TableColumn`。

## 兼容原则

新架构重构期间，新旧代码允许并存，但必须遵守以下约束：

- 旧 `vparquet` / Hive / Hudi / Paimon 路径在新架构稳定前允许保留。
- 新架构实现不得继续向旧 `vparquet` 表层语义回灌依赖。
- 先搭新框架 API，再逐步迁移调用点。
- 不允许边改 API 边混入临时裸逻辑、实验性草稿或未收敛命名。
- 兼容层可能需要存在，但本文档不定义兼容层的具体实现方案。

## 验收标准

该文档应满足以下目标：

- 不引用错误实验代码作为既成事实；
- 不出现实现性草稿、裸伪代码、未收敛命名混用；
- 让另一个工程师从 `master` 新开分支时，可以直接按本文档搭 API 骨架；
- 读完文档后，不需要再讨论以下问题：
  - 新架构分几层；
  - 每层负责什么；
  - 哪层理解 global schema；
  - 哪层做 schema change / filter localization / finalize；
  - 哪层允许依赖旧实现，哪层不允许。
