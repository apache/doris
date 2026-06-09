# New Reader 列标识实现说明

本文说明 Doris new table/file reader 栈中各种列标识的当前含义，以及它们在
`FileScannerV2`、`TableReader`、`TableColumnMapper` 和 new Parquet reader 中的流转逻辑。

核心原则是把 **schema identity** 和 **执行期位置** 分开：

- schema identity 用来判断 table column 和 file column 是否是同一列。
- index/position 用来表示 block、projection tree、scan request 或 constant map 中的位置。
- FE column unique id 只在 scanner 边界用于定位 slot，进入 table/file reader 后不再出现。

共享定义集中在 `be/src/format_v2/column_data.h`。file reader 通用请求定义在
`be/src/format_v2/file_reader.h`。new Parquet reader 自己的 Parquet 内部 schema tree 定义在
`be/src/format_v2/parquet/parquet_column_schema.h`。

## 层级边界

当前 reader 栈可以按语义分成三层。

### FileScannerV2：FE 标识到 reader 标识的边界

`FileScannerV2` 仍能看到 FE 下发的 `slot_id`、`col_unique_id`、`TFileScanSlotInfo` 和
`TColumnAccessPath`。这些 FE 侧标识只在这里使用。

`FileScannerV2::_build_projected_columns()` 会把 `_params->required_slots` 转成
`std::vector<format::ColumnDefinition>`：

- vector 下标就是 `GlobalIndex`。
- `_slot_id_to_global_index` 把 FE `slot_id` 转成 `GlobalIndex`，用于 row-level conjunct。
- `_column_unique_id_to_global_index` 把 FE `col_unique_id` 转成 `GlobalIndex`，用于 column predicate。
- `ColumnDefinition::identifier` 表示 table-side schema identity，默认是列名；如果外部 schema
  提供 field id，则改用 field id。
- partition/default/generated 信息被挂到 `ColumnDefinition` 上，由 table reader 层处理。

从这一层往下，table/file reader 不再使用 FE column unique id。

### TableReader / TableColumnMapper：table schema 到 file schema

`TableReader::open_reader()` 对每个 split 打开一个具体 `FileReader`，先通过
`FileReader::get_schema()` 获取当前文件的 file-local schema，再用 `TableColumnMapper` 建立映射。

`TableColumnMapper` 的输入是：

- table/global schema：`FileScannerV2` 构造的 `projected_columns`。
- file-local schema：具体 file reader 返回的 `std::vector<ColumnDefinition>`。
- per-split partition values。
- table-level row filters 和 column predicates。

`TableColumnMapper` 的输出是：

- `ColumnMapping`：构造阶段使用的 table column 到 file/constant/virtual source 的映射。
- `FileScanRequest`：只含 file-local projection、file-local block layout 和 file-local filters。
- `ColumnMapResult` / `ResultColumnMapping`：给 table reader finalize 阶段消费的最终映射。
- `FilterEntry`：给 filter localization 使用的 `GlobalIndex -> LOCAL/CONSTANT/UNSET` target。
- `ConstantMap`：partition/default/generated 常量列。

### FileReader / ParquetReader：只理解 file-local 请求

`FileReader` 只暴露两类 schema/request：

- `get_schema(std::vector<ColumnDefinition>*)`：返回文件自身 schema。
- `open(std::unique_ptr<FileScanRequest>&)`：接收已经 localize 后的 file-local scan request。

具体 file reader 不理解 table/global schema、Iceberg default、partition column、FE slot id 或
FE column unique id。

new Parquet reader 使用 `FileScanRequest` 中的 `LocalColumnIndex` 创建 column reader，并使用
`local_positions` 决定 file-local block layout。

## ColumnDefinition

定义位置：`be/src/format_v2/column_data.h`

`ColumnDefinition` 是 table/global schema 和 file-local schema 共用的列定义。它表示列名、类型、
nested children、默认表达式、partition 属性和 file-local column kind。

关键字段：

- `identifier`：schema identity。用于 table column 和 file column 匹配。
- `local_id`：file reader 返回的 schema node 在当前 parent 下的 reader-local id。
- `name`：逻辑列名。BY_NAME 且没有显式 string identifier 时会回退到它。
- `type`：当前 schema node 的 Doris 类型。
- `children`：nested children。table/global schema 中是 table children；file schema 中是
  file-local children。
- `default_expr`：missing/default/generated column 的物化表达式。
- `is_partition_key`：partition column 标记。
- `column_type`：file-local column kind，例如普通数据列或 row number virtual column。

`ColumnDefinition` 不保存 FE column unique id。它也不保存“应该按什么方式匹配”。匹配方式由
`TableColumnMapperOptions::mode` 统一决定。

### identifier

`identifier` 是一个 `Field`，语义接近 DuckDB `MultiFileColumnDefinition::identifier`：

- `TYPE_NULL`：没有显式 identifier。BY_NAME 时使用 `name`。
- `TYPE_INT`：在 BY_FIELD_ID 中表示 field id；在 BY_INDEX 中表示 file schema position。
- `TYPE_STRING`：显式 name identifier。

访问 helper：

- `has_identifier_field_id()` / `get_identifier_field_id()`：BY_FIELD_ID 使用。
- `get_identifier_name()`：BY_NAME 使用；没有显式 string identifier 时返回 `name`。
- `get_identifier_position()`：BY_INDEX 使用。
- `file_local_id()`：file reader projection 使用；优先返回 `local_id`，否则回退到 int
  identifier。这个回退只用于兼容某些 file schema 构造路径，不应重新引入 FE id 语义。

## 强类型位置

### GlobalIndex

定义位置：`be/src/format_v2/column_data.h`

`GlobalIndex` 表示 table/global output block 中的 top-level 列位置。当前等于
`_params->required_slots` 的下标。

主要使用位置：

- `ColumnMapping::global_index`
- `TableFilter::global_indices`
- `TableColumnPredicates` 的 key
- `ColumnMapResult` / `ResultColumnMapping` 的 key
- `FilterEntry` map 的 key

`GlobalIndex` 不是 FE slot id，也不是 FE column unique id。

### LocalColumnId

定义位置：`be/src/format_v2/column_data.h`

`LocalColumnId` 表示当前物理文件 schema 的 top-level reader-local column id。

主要使用位置：

- `FileScanRequest::local_positions` 的 key。
- `LocalColumnIndex::top_level()`。
- new Parquet reader 创建 top-level column reader。
- page index、statistics、bloom filter 等 file-local pruning 的 root column key。
- row position 这类 reader 内部 virtual column id。

`LocalColumnId` 不是 file-local block position。一个 top-level file column 在本次 scan request
输出 block 中的位置由 `LocalIndex` 表示。

### LocalIndex

定义位置：`be/src/format_v2/column_data.h`

`LocalIndex` 表示一次 `FileScanRequest` 内 file-local block 的列位置。

主要使用位置：

- `FileScanRequest::local_positions` 的 value。
- file-local rewritten `SlotRef` 的 input position。
- `TableReader` 从 file block 取列。
- `ParquetScanScheduler` 把 column reader 读出的数据写入 file block。

`LocalIndex` 是 request-local block layout，不是 file schema ordinal。

### ConstantIndex

定义位置：`be/src/format_v2/column_data.h`

`ConstantIndex` 表示 `ConstantMap` 中的 entry 位置。它用于 per-split/per-file 常量列：

- partition column。
- schema evolution default column。
- generated/default expression column。
- 将来可扩展到更多 virtual/constant source。

`FilterEntry` 可以指向 `ConstantIndex`。当一个 row-level conjunct 只引用 constant target 时，
`TableReader` 会在打开 file reader 前用 1 行常量 block 求值；如果结果为 false/NULL，当前 split
直接跳过。

### LocalColumnIndex

定义位置：`be/src/format_v2/column_data.h`

`LocalColumnIndex` 表示递归 file-local projection path：

```cpp
struct LocalColumnIndex {
    int32_t index = -1;
    bool project_all_children = true;
    std::vector<LocalColumnIndex> children;
};
```

语义：

- root entry 的 `index` 是 `LocalColumnId`。
- nested entry 的 `index` 是当前 parent 下的 file-local child id。
- `project_all_children = true` 表示读取整个 subtree。
- `project_all_children = false` 表示只读取 `children` 中列出的 child paths。

通用 helper：

- `is_full_projection()`
- `is_partial_projection()`
- `find_child_projection()`
- `is_child_projected()`
- `merge_local_column_index()`

new Parquet reader 的 STRUCT/LIST/MAP reader 都消费这套 projection helper：

- STRUCT：只创建被投影 child 的 reader。
- LIST：把 element projection 递归传给 element reader。
- MAP：总是读取 key，把 value projection 递归传给 value reader。

## FileScanRequest

定义位置：`be/src/format_v2/file_reader.h`

`FileScanRequest` 是 table reader 交给 file reader 的唯一 scan 输入。它不包含 table/global schema。

关键字段：

- `predicate_columns`：row-level conjunct/delete conjunct 需要先读取的 file-local projection。
- `non_predicate_columns`：最终输出需要读取、且不需要先参与 row-level filter 的 file-local
  projection。
- `local_positions`：`LocalColumnId -> LocalIndex`，决定 file-local block layout。
- `conjuncts` / `delete_conjuncts`：已经把 table/global slot 改写成 file-local slot 的表达式。
- `column_predicate_filters`：file-layer pruning hints，只用于 min/max、page index、dictionary、
  bloom filter 等剪枝，不参与 batch row filtering。

`predicate_columns` 和 `non_predicate_columns` 都按 file-local schema 表达。file reader 只需要根据
这两个列表创建 reader，并按 `local_positions` 写入 file block。

## TableColumnMapper 逻辑

定义位置：

- `be/src/format_v2/column_mapper.h`
- `be/src/format_v2/column_mapper.cpp`

### 匹配模式

`TableColumnMapperOptions::mode` 决定 `identifier` 的解释方式：

- `BY_FIELD_ID`：`TYPE_INT` identifier 是 field id。
- `BY_NAME`：`TYPE_STRING` identifier 或 `name` 是匹配名。
- `BY_INDEX`：`TYPE_INT` identifier 是 file schema position。

`TableReader::open_reader()` 当前默认按 field id 映射；如果 file schema 首列没有 int identifier，
会 fallback 到 BY_NAME。Hive reader 可覆盖默认模式，Hive1 ORC 这类场景可使用 BY_INDEX。

### create_mapping()

`create_mapping()` 为每个 `GlobalIndex` 生成一个 `ColumnMapping`：

1. partition column 优先映射到 `ConstantMap`。
2. BY_INDEX 时按 file position 取 file schema。
3. 普通列通过 matcher 在 file schema 中找对应 file field。
4. 缺失但带 default expr 的列映射到 `ConstantMap`。
5. 特殊 virtual column 记录 virtual column type。
6. 允许 missing column 时保留空 mapping，由 table finalize 阶段补 NULL/default。

`ColumnMapping::file_local_id` 是 table column 绑定到 file schema 后的 reader-local id：

- root mapping 中可转成 `LocalColumnId`。
- nested mapping 中表示 parent 下的 child id。
- constant/missing/virtual mapping 没有 `file_local_id`。

schema identity field id 不保存在 `ColumnMapping` 中，只保存在
`ColumnDefinition::identifier` 中，并由 mapper 的匹配模式解释。

### create_scan_request()

`create_scan_request()` 把 table-level scan 信息转换成 file-local request：

1. 先把不参与 row-level filter 的输出列加入 `non_predicate_columns`。
2. 调用 `localize_filters()`，把 row-level conjunct 和 column predicates 定位到 file-local source。
3. 为所有已读取 file column 重建 output projection，让 `ColumnMapping::projection` 指向正确的
   `LocalIndex`。
4. 生成 `ColumnMapResult` 和 `ResultColumnMapping`，供 table reader finalize。

`local_positions` 在这个阶段确定。同一个 file column 如果同时被 filter 和 output 使用，只会有
一个 `LocalIndex`。

### FilterEntry

`FilterEntry` 是 `GlobalIndex` 到 filter target 的结果：

- `LOCAL`：filter 可以在 file-local block 上求值，target 是 `LocalIndex`。
- `CONSTANT`：filter 只依赖 `ConstantMap` entry。
- `UNSET`：当前 split 无法下推到 file reader。

`TableColumnMapper::_build_filter_entries()` 在 `FileScanRequest::local_positions` 确定后生成
`FilterEntry`。表达式改写时只把 `LOCAL` target 改写成 file-local slot；`CONSTANT` target 用于
split-level constant filter evaluation。

### ColumnMapResult / ResultColumnMapping

`ColumnMapResult` 记录一个 global result column 的递归映射结果：

- `local_column_id`：root file column。
- `column_index`：file-local projection tree。
- `mapping`：root 指向 `LocalIndex`，nested child 通过 `IndexMapping::child_mapping` 递归映射。

`ResultColumnMapping` 是最终可消费的 `GlobalIndex -> ColumnMapEntry` map。`ColumnMapEntry` 包含：

- `IndexMapping mapping`
- `local_type`
- `global_type`
- `filter_conversion`

TableReader finalize 阶段用它把 file-local block 转成 table/global block。

### nested child mapping

复杂列映射时，`IndexMapping::child_mapping` 的 key 是 table/global child ordinal，value 是对应
file-local child mapping。这样 filter 中的 `STRUCT_EXTRACT` 可以按 table child ordinal 找到
file child ordinal。

Doris 不再维护额外的 `NestedPredicateTargetInfo` / filter target path。nested filter localization
直接沿 `IndexMapping::child_mapping` 转换 selector path。

对于 `SELECT s.name WHERE s.id > 5` 这类 filter-only child：

- `s.name` 进入 output projection。
- `s.id` 会进入 predicate projection。
- `original_file_children` 保留 projection 前的 file children，用于定位 filter-only child。
- `child_mappings` 只描述输出 shape，避免 filter-only child 改变最终 STRUCT/LIST/MAP shape。

## Parquet 内部 schema 标识

定义位置：`be/src/format_v2/parquet/parquet_column_schema.h`

`ParquetColumnSchema` 是 new Parquet reader 内部 schema tree。它描述 Parquet 逻辑字段和 primitive
leaf column 的关系，不暴露给 table reader。对外统一通过 `ParquetReader::get_schema()` 返回
`std::vector<format::ColumnDefinition>`。

关键字段：

- `local_id`：当前 parent 下的 reader-local id。top-level 是 root field ordinal，nested 是 child
  ordinal。`LocalColumnIndex` 传给 `ParquetColumnReaderFactory` 的就是这个 id。
- `parquet_field_id`：Parquet schema element 中可选的 field_id。Arrow 在不存在 field_id 时返回
  `-1`。它只作为 schema matching identifier，不用于读取 column chunk。
- `name`：Parquet schema name。
- `type`：转换后的 Doris 类型。
- `leaf_column_id`：Parquet primitive leaf column ordinal。用于访问 `ColumnDescriptor`、
  row group column chunk、statistics、page index、bloom filter 等。复杂节点为 `-1`。
- `type_descriptor`：primitive leaf 的 Parquet physical/logical type 信息。
- `descriptor`：primitive leaf 的 Arrow Parquet `ColumnDescriptor`。
- `max_definition_level` / `max_repetition_level`：该 node 下的最大 Dremel level。
- `nullable_definition_level`：当前 node 自身为 NULL 时对应的 definition level。
- `repeated_repetition_level`：当前或最近 repeated container 的 repetition level。

`ParquetReader::get_schema()` 会把 `ParquetColumnSchema` 转成 `ColumnDefinition`：

- 如果 `parquet_field_id >= 0`，`ColumnDefinition::identifier` 是 `TYPE_INT` field id。
- 否则 `identifier` 是 `TYPE_STRING` name。
- `ColumnDefinition::local_id` 是 `ParquetColumnSchema::local_id`。
- children 递归转换。

因此 table reader 可以按 field id 或 name 匹配，而 Parquet reader 自己仍只按 `local_id`、
`leaf_column_id` 和 Dremel levels 读取数据。

## 端到端流转

一次 split 的列标识流转如下：

1. `FileScannerV2::_build_projected_columns()`：
   FE `slot_id` / `col_unique_id` 被翻译成 `GlobalIndex`，并生成 table-side
   `ColumnDefinition`。
2. `ParquetReader::init()`：
   解析 Arrow Parquet schema，构造内部 `ParquetColumnSchema`。
3. `ParquetReader::get_schema()`：
   把 Parquet 内部 schema 暴露成 file-side `ColumnDefinition`。
4. `TableReader::open_reader()`：
   根据 file schema 是否带 int identifier 选择 BY_FIELD_ID 或 BY_NAME，并调用 mapper。
5. `TableColumnMapper::create_mapping()`：
   用 `ColumnDefinition::identifier` 匹配 table/global schema 和 file-local schema，生成
   `ColumnMapping`。
6. `TableColumnMapper::create_scan_request()`：
   生成 `FileScanRequest`，其中所有 projection 和 block position 都是 file-local 的。
7. `ParquetReader::open()`：
   校验 `LocalColumnId`，用 `LocalColumnIndex` 创建 column readers，并规划 row group pruning。
8. `ParquetScanScheduler`：
   按 `local_positions` 把 predicate/non-predicate column 写入 file-local block。
9. `TableReader` finalize：
   使用 `ResultColumnMapping`、`ConstantMap` 和 projection expression，把 file-local block 转成
   table/global output block。

## 使用约定

修改 new reader 代码时应遵守以下约定：

- 不要在 table/file reader 层重新传递 FE column unique id。
- 不要把 `ColumnDefinition::identifier` 当作 file reader 读取 id。
- 不要把 `LocalColumnId` 当作 block position；block position 使用 `LocalIndex`。
- 不要把 `LocalIndex` 当作 schema ordinal。
- `LocalColumnIndex::index` 在 root 和 child 层含义不同，调用方必须知道当前 projection node
  所在层级。
- file reader 只能消费 `FileScanRequest`，不能理解 partition/default/generated/table schema。
- column predicate pruning 是 file-layer hint，不等价于 row-level filter。
- constant filter 可以在 table reader 层提前求值，但不应下推到 file reader。

## 已知限制

TVF 查询 Parquet 且文件没有 field id 时，top-level BY_NAME 已经可以通过 name identifier 工作。
但 nested access path 的 fallback 目前仍有一处 TODO：STRUCT child fallback 使用 struct ordinal
构造 int identifier。对于没有 field id 的 nested Parquet schema，BY_NAME 场景应保留 string
identifier，让 `TableColumnMapper` 从 Parquet file schema 中按 name 解析 file-local child id。
该问题已在 `be/src/exec/scan/file_scanner_v2.cpp` 代码中记录，当前未修复。
