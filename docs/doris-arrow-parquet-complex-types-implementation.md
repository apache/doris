# Doris Arrow Parquet Reader 复杂类型完整支持方案

本文档描述 `be/src/format/new_parquet/` 新 Parquet reader 对 `STRUCT`、`LIST`、`MAP` 复杂类型的完整支持方案。

目标是在现有 file-local reader 边界内补齐复杂类型读取能力：

- 继续复用 Arrow C++ Parquet core API 解析文件、row group、column chunk 和 leaf value。
- 输出仍然是 Doris `Block` / `Column`，不引入 `parquet::arrow::FileReader`、`arrow::RecordBatch` 或 `arrow::Table` 作为 scan 输出路径。
- `ParquetReader` 仍只理解 Parquet file-local schema，不处理 Iceberg/global schema evolution。
- schema change、default/generated/partition column、delete、virtual column 仍由 `TableReader` / `TableColumnMapper` 负责。
- 复杂类型读取必须以 Parquet definition level / repetition level 为准，不能依赖简单 row count 拼接。
- 复杂类型列裁剪是本轮实现目标：读取 top-level complex column 时，只读取被请求的 child subtree。
- 复杂类型 schema change 不在本轮实现，但本轮设计必须保留 field id、path、level 和 projection tree 边界，保证后续可以在 `TableColumnMapper` 中补齐 child-level mapping。

## 参考实现：DuckDB Parquet Reader

参考目录：

```text
/Users/xiaogangsu/code/duckdb/extension/parquet/
```

重点参考文件：

```text
extension/parquet/include/parquet_column_schema.hpp
extension/parquet/include/column_reader.hpp
extension/parquet/parquet_reader.cpp
extension/parquet/column_reader.cpp
extension/parquet/reader/struct_column_reader.cpp
extension/parquet/reader/list_column_reader.cpp
```

DuckDB 中值得借鉴的核心结构：

- `ParquetColumnSchema` 保存 `max_define`、`max_repeat`、`schema_index`、`column_index`，schema tree 本身携带 Dremel level 信息。
- `ParseSchemaRecursive()` 在解析 schema 时递增 definition/repetition level，并把 legacy repeated field、3-level LIST、MAP/MAP_KEY_VALUE 统一成 reader 可消费的 schema tree。
- primitive reader 读取 leaf value 的同时输出 definition/repetition level。
- struct reader 递归读取 children，并用 child 输出的 definition level 设置 struct null。
- list/map reader 不直接按 row 数读取 child；它读取 child leaf stream，根据当前 list/map 层的 repetition level 折叠出 parent rows、offsets 和 null map。
- skip/select 是 reader 级语义，不是 column filter fallback；复杂类型 skip 也必须消费对应的 level stream，保证所有 child reader 游标一致。

Doris 不需要照搬 DuckDB 的 thrift/page decoder；当前方案仍优先封装 Arrow internal `RecordReader`。但 DuckDB 的 reader 分层和 level 组装模型应作为 Doris 复杂类型支持的主参考。

## 当前 Doris 状态

现有文件：

```text
be/src/format/new_parquet/parquet_reader.*
be/src/format/new_parquet/column_reader.*
be/src/format/new_parquet/parquet_column_schema.*
be/src/format/new_parquet/parquet_type.*
be/src/format/new_parquet/selection_vector.h
```

已有能力：

- schema builder 可以识别 `STRUCT`、`LIST`、`MAP`，并生成 `DataTypeStruct`、`DataTypeArray`、`DataTypeMap`。
- `ScalarColumnReader` 支持 flat primitive/string/decimal/date/time/timestamp。
- `StructColumnReader` 递归读取 children，支持非常基础的非 nullable struct。
- `ColumnReader::select()` 已经定义为 `skip + read` 的 selected read，不退化为整批读取后过滤。

主要缺口：

- `ParquetColumnSchema` 没有保存完整 `max_definition_level` / `max_repetition_level` 和各复杂节点的 level 边界。
- `ScalarColumnReader` 当前只支持 `max_repetition_level == 0 && max_definition_level <= 1`。
- primitive reader 没有向 parent reader 暴露 leaf definition/repetition level stream。
- nullable struct、list、map 没有 assembler。
- repeated primitive、legacy repeated group、嵌套 list/map/struct 没有统一 schema 规约。
- `skip(rows)` 对复杂类型还不是 parent-row 语义。

## 总体设计

复杂类型读取分为两层：

```text
ParquetReader
    -> ParquetColumnReader public API
        read(parent_rows, output_column, rows_read)
        skip(parent_rows)
        select(selection, selected_rows, batch_rows, output_column)
    -> Nested read API
        read_nested(parent_rows, level_state, output_column, rows_read)
        skip_nested(parent_rows, level_state)
    -> Leaf RecordReader adapter
        read leaf values + definition levels + repetition levels
    -> Dremel assembler
        Struct / List / Map build Doris columns
```

对 `ParquetReader` 来说，接口仍然是 top-level file-local row batch；复杂类型细节只存在于 `column_reader.*` 内部。

### 关键原则

- public `read(rows)` 和 `skip(rows)` 的 `rows` 始终表示当前 reader 对外暴露的 parent rows。
- leaf reader 内部可以读取更多 physical records，但不能把 physical value count 泄露给 `ParquetReader`。
- list/map 的 offsets 只能由 repetition level 生成，不能用 child column size 推断。
- nullable 信息只能由 definition level 生成，不能通过 value 缺失猜测。
- 所有复杂类型 reader 必须保持 child reader 游标严格同步；遇到不一致 level stream 应返回 `Corruption`。
- 复杂类型 reader 不处理 table/global schema change；child-level schema evolution 后续在 `TableColumnMapper` 处理。
- 复杂类型 reader 必须支持 file-local child projection。未投影 child 不创建 leaf reader，不读取对应 column chunk，不参与 value materialization。
- 即使 child 被裁剪，也必须保留足够的 schema/path/level 元数据，使后续 schema change 可以把 table child 映射到 file child、default child 或 cast projection。

## Schema 扩展

扩展 `ParquetColumnSchema`：

```text
struct ParquetColumnSchema {
    int field_id;
    int top_level_field_id;
    int leaf_column_id;
    int schema_node_id;
    int parent_schema_node_id;
    std::vector<int> file_path;
    std::vector<int32_t> field_id_path;
    std::vector<std::string> name_path;
    std::string name;
    DataTypePtr type;
    ParquetColumnSchemaKind kind;
    const parquet::schema::Node* node;
    const parquet::ColumnDescriptor* descriptor;
    ParquetTypeDescriptor type_descriptor;
    int16_t max_definition_level;
    int16_t max_repetition_level;
    int16_t nullable_definition_level;
    int16_t repeated_repetition_level;
    std::vector<std::unique_ptr<ParquetColumnSchema>> children;
};
```

字段含义：

- `schema_node_id`：Parquet schema tree 中的 node ordinal，用于 debug、error message、field id tracing。
- `top_level_field_id`：FileScanRequest 使用的 file-local top-level id。
- `leaf_column_id`：Parquet physical leaf column ordinal。复杂节点为 `-1`。
- `file_path`：从 top-level field 到当前节点的 file-local child ordinal path，例如 `profile.address.city` 可以表示为 `[3, 0, 1]`。
- `field_id_path`：从 top-level field 到当前节点的 Parquet field id path。缺失 field id 时使用 `-1` 占位，不在 file reader 层解释 Iceberg 语义。
- `name_path`：从 top-level field 到当前节点的 Parquet node name path，用于 by-name fallback、error message 和后续 schema change。
- `max_definition_level` / `max_repetition_level`：该节点下 leaf stream 的最大 level。复杂节点取其 subtree leaf 的约束值。
- `nullable_definition_level`：该节点自身从 null 变成 defined 所需的 definition level。required 节点为 parent level，不额外增加。
- `repeated_repetition_level`：该 repeated/list/map 层对应的 repetition level。非 repeated 节点为 parent level。

Schema builder 改造：

- 从 root 递归解析，每进入 optional 节点 `definition_level + 1`。
- 每进入 repeated 节点 `definition_level + 1` 且 `repetition_level + 1`。
- 识别标准 3-level LIST：

```text
optional group a (LIST) {
  repeated group list {
    optional <element_type> element;
  }
}
```

- 识别 legacy repeated primitive/group：

```text
repeated int32 a;
repeated group a { ... }
```

并规约为 Doris `Array(element_type)`。

- 识别 MAP/MAP_KEY_VALUE：

```text
optional group m (MAP) {
  repeated group key_value {
    required key_type key;
    optional value_type value;
  }
}
```

并规约为 Doris `Map(key_type, value_type)`。

- MAP key 按 Parquet 规范应为 required。若文件声明 nullable key，应在 schema 阶段返回 `NotSupported` 或 `Corruption`，不生成可继续执行的 reader。

## 复杂类型列裁剪

复杂类型列裁剪应在 file-local 层实现，语义是“只读取投影需要的 child subtree”，不是 table schema evolution。

建议扩展 `reader::FileScanRequest`，增加嵌套 projection tree：

```text
struct FieldProjection {
    ColumnId file_column_id;
    std::vector<int> file_path;
    bool project_all_children;
    std::vector<FieldProjection> children;
};

struct FileScanRequest {
    std::vector<ColumnId> predicate_columns;
    std::vector<ColumnId> non_predicate_columns;
    std::map<ColumnId, size_t> column_positions;
    std::map<ColumnId, FieldProjection> complex_projections;
    ...
};
```

约束：

- `predicate_columns` / `non_predicate_columns` 仍表示 top-level file-local fields。
- `complex_projections` 只描述 top-level complex field 内部需要读取哪些 child。
- 没有出现在 `complex_projections` 的 top-level complex field 默认 `project_all_children = true`，保持兼容。
- 对 `STRUCT`，允许只投影部分 children，输出 `DataTypeStruct` 只包含被投影 children，child 顺序保持 file schema 顺序。
- 对 `LIST`，允许裁剪 element subtree。例如 `Array(Struct<a,b,c>)` 投影 `a,c` 时，输出 `Array(Struct<a,c>)`。
- 对 `MAP`，key 永远需要读取并输出；value subtree 可以裁剪。例如 `Map<String, Struct<a,b>>` 投影 value.a 时，输出 `Map<String, Struct<a>>`。
- 对 nullable parent，parent null map 和 offsets 必须完整生成；裁剪只影响 child value materialization，不能影响 parent row shape。
- 对所有 children 都被裁剪的 `STRUCT`，仍要能够根据某个保留的 level-driving child 生成 parent row/null 形态。第一版可以要求至少保留一个 child；如果上层真的只需要 parent 存在性，后续补充 `NullShapeColumnReader`。

`ParquetColumnReaderFactory` 应接收 projection tree：

```text
Status create(const ParquetColumnSchema& column_schema,
              const FieldProjection* projection,
              std::unique_ptr<ParquetColumnReader>* reader) const;
```

实现要求：

- factory 只为投影中的 leaf 创建 `ScalarColumnReader`。
- struct/list/map reader 保存 child reader slot；未投影 child 用 `nullptr` 表示，参考 DuckDB `StructColumnReader` 的 child reader 布局。
- `TotalCompressedSize`、prefetch、statistics 等后续能力只能统计已投影 leaf。
- 对 top-level output block，`TableReader` 需要使用 projection 后的 `SchemaField` / `DataTypePtr` 构建 block template，而不是原始完整 file schema。

列裁剪与延时物化的关系：

- predicate complex child projection 和 output complex child projection 需要合并，避免同一 leaf 重复读取。
- 如果 predicate 只依赖 complex child，FileScanRequest 应能表达该 child path 是 predicate projection。
- 本轮可以先支持 output child pruning；predicate child pruning 可在 batch 内 complex predicate 接入时补齐，但 projection tree 的结构必须现在预留。

## Schema Change 兼容边界

复杂类型 schema change 不在本轮实现，原因是它涉及 table/global schema、Iceberg field id、default value、cast、generated column 和 filter fallback，属于 `TableColumnMapper` / `TableReader` 范围。

但本轮实现必须保证后续可扩展：

- file schema 中每个 node 都必须导出 `file_path`、`field_id_path`、`name_path`、file-local type 和 child schema。
- reader 内部不得把 `SchemaField::id` 同时当作 Iceberg field id 和 file-local column id。top-level scan id 只表示 file-local top-level ordinal。
- `TableColumnMapper` 后续可以根据 table child field id/name path 生成 `FieldProjection`，也可以为缺失 child 生成 default/constant/finalize projection。
- file reader 输出的 pruned complex type 是 file-local projected type；table reader 负责把它 finalize 成 table/global type。
- filter localization 后续可以定位到 complex child path。无法安全定位或需要 cast 的 filter 进入 `reader_expression_map` 或 table-level finalize filter。
- 不在 `ParquetReader` 中补缺失 child，不在 `ParquetReader` 中做 child cast，不在 `ParquetReader` 中解释 Iceberg field id。

后续 schema change 的目标形态：

```text
table projection/filter
    -> TableColumnMapper child-level mapping
    -> FieldProjection(file-local child paths)
    -> ParquetReader reads projected file-local complex block
    -> TableReader fills default/generated/partition children
    -> TableReader applies child cast/finalize/delete/virtual semantics
```

因此，本轮列裁剪实现时不能把 output type 和 original file type 强绑定。所有 `ColumnReader` 创建和 block template 构造都应基于 projected schema view。

## Level 读取抽象

新增内部结构，位置建议：

```text
be/src/format/new_parquet/level.h
be/src/format/new_parquet/level.cpp
```

核心结构：

```text
struct LevelBatch {
    int64_t record_count;
    int64_t value_count;
    std::vector<int16_t> definition_levels;
    std::vector<int16_t> repetition_levels;
};

struct NestedReadResult {
    int64_t parent_rows;
    int64_t physical_records;
};
```

`ScalarColumnReader` 内部新增 leaf read 路径：

```text
read_leaf_records(max_records, decoded_values, level_batch)
skip_leaf_records(max_records, level_batch)
```

要求：

- Arrow internal `RecordReader` 的创建和调用继续封装在 `column_reader.*`，不能泄露到 `ParquetReader`。
- flat primitive 保持当前 `read()` 快路径。
- nested primitive 必须允许 `max_repetition_level > 0` 或 `max_definition_level > 1`，并输出 definition/repetition levels。
- `DecodedColumnView::row_count` 对 nested leaf 应表示 value slots 数量，null slot 由 definition level 决定。

如果 Arrow internal `RecordReader` 无法稳定提供 Doris 需要的 level/value 对齐语义，则新增 Doris 自己的 leaf page decoder，范围仍限制在 `format/new_parquet/`，不要把 page decoder 细节扩散到 `ParquetReader` 主流程。

## Reader 分层

建议拆分 `column_reader.cpp`，避免复杂类型 assembler 混在 scalar 读值热路径：

```text
be/src/format/new_parquet/column_reader.h
be/src/format/new_parquet/column_reader.cpp
be/src/format/new_parquet/scalar_column_reader.cpp
be/src/format/new_parquet/struct_column_reader.cpp
be/src/format/new_parquet/list_column_reader.cpp
be/src/format/new_parquet/map_column_reader.cpp
be/src/format/new_parquet/level.h
be/src/format/new_parquet/level.cpp
```

### ScalarColumnReader

职责：

- 读取 primitive leaf values。
- 生成 leaf-level definition/repetition level。
- 对 flat column 直接写 Doris scalar/nullable column。
- 对 nested leaf 只作为 child reader 被复杂类型 assembler 调用。

flat path：

```text
read(rows)
    -> RecordReader::ReadRecords(rows)
    -> DecodedColumnView
    -> DataTypeSerDe::read_column_from_decoded_values
```

nested path：

```text
read_nested(parent_rows, level_state)
    -> read leaf records until parent_rows complete
    -> append valid leaf values into child column
    -> expose level_batch to parent assembler
```

### StructColumnReader

输出：

- non-nullable struct：`ColumnStruct`。
- nullable struct：`ColumnNullable(ColumnStruct, null_map)`。

算法：

1. 对每个 child reader 读取同样的 parent row count。
2. child reader 返回的 parent rows 必须一致。
3. struct 自身 nullable 时，根据 definition level 判断 struct row 是否 null。
4. 对 null struct row，每个 child column 仍必须补一个 default/null slot，保证 `ColumnStruct` 所有 child size 等于 struct row count。
5. child 本身的 null 由 child reader 自己根据更深层 definition level 处理。

注意：

- 当前实现仅递归读取 children，没有处理 nullable struct；应改为显式处理 struct-level null map。
- 对未投影 children 不创建 reader、不写入 output `ColumnStruct`。
- 对所有 children 都未投影的 struct，第一版可以返回 `NotSupported`，后续用 shape-only reader 支持 parent 存在性读取。

### ListColumnReader

输出：

- non-nullable array：`ColumnArray(element_column, offsets)`。
- nullable array：`ColumnNullable(ColumnArray, null_map)`。

核心算法参考 DuckDB list reader：

1. 从 child reader 读取 leaf stream，获得 child values、definition levels、repetition levels。
2. 根据当前 list 层的 `repeated_repetition_level` 判断一个 child record 是否属于当前 list：
   - `rep == list_repetition_level`：当前 list 的后续 element。
   - `rep < list_repetition_level`：新的 parent row 开始。
3. 根据 definition level 判断 parent row 状态：
   - `def < list_defined_level`：null list。
   - `def == empty_list_level`：empty list。
   - `def >= element_defined_level`：有 element。
4. 对每个 parent row 写一个 offset。
5. 只有 element defined 时向 element column append value；empty/null list 不 append element。

需要维护 overflow：

- child reader 一次读取可能跨过本次 `parent_rows` 的边界。
- list reader 必须缓存未消费的 child values 和 levels，下一次 `read()` 继续使用。
- 该缓存是 reader 游标状态的一部分，`skip()` 和 `read()` 都必须共享。

### MapColumnReader

输出：

- non-nullable map：`ColumnMap(key_column, value_column, offsets)`。
- nullable map：`ColumnNullable(ColumnMap, null_map)`。

实现方式：

- 按 Parquet schema 将 map 规约为 `LIST<STRUCT<key, value>>` 的 level stream。
- 复用 list assembler 的 parent row 边界判断。
- 对每个 entry：
  - key 必须 defined；key 缺失是文件格式错误。
  - value 可 nullable；由 value child definition level 生成 value null map。
- append entry 时分别写 key column 和 value column。
- offsets 表示每个 map row 的 entry 数。

不要把 `MAP` 先 materialize 成 `Array(Struct(key,value))` 再转换为 `ColumnMap`，否则会产生额外内存和拷贝。可以在内部复用 list 的边界识别逻辑，但直接写 `ColumnMap` 的 keys/values/offsets。

## Skip 和 Select

public 语义保持不变：

```text
skip(parent_rows)
select(selection, selected_rows, batch_rows, column)
```

复杂类型要求：

- `skip()` 必须消费 parent rows 对应的所有 child physical records 和 level stream。
- `select()` 继续使用现有 range 合并策略，即按 selected row ranges 调用 `skip()` + `read()`。
- list/map 的 `skip()` 不能只跳过 child value count；必须按 repetition level 找到 parent row 边界。
- empty selection 时必须跳过整个 batch 的 parent rows，保证 reader 游标推进。

第一阶段不实现 page-level row range selection；只保证 `skip + read` 的 selected read 正确。

## 与 ParquetReader Scan Loop 的关系

`ParquetReader::_read_current_row_group_batch()` 不需要理解复杂类型：

- predicate columns 仍先读。
- non-predicate columns 仍根据 selection 调用 `read()` 或 `select()`。
- column reader 自己负责 complex column 的 parent-row 语义。

限制：

- 初期不支持复杂类型直接作为 filter column 执行 batch predicate。
- row group statistics 仍只对 primitive leaf 做保守裁剪。
- complex child-level projection 是本轮 reader 实现目标；但 complex child predicate 执行和 schema change finalize 不在本轮完成。

## 错误处理

遇到明确违反 Parquet spec 或 reader invariant 的情况，应返回错误或触发检查，不能静默修复：

- MAP key nullable 或 key definition level 缺失。
- 同一 struct 的 children parent row count 不一致。
- list/map repetition level 非法回退或超过当前 schema 最大值。
- leaf reader 返回的 value count、definition/repetition level 数量不一致。
- child reader overflow 状态与下一次 read/skip 请求冲突。

对合法但暂未支持的编码形态返回 `NotSupported`，例如后续若发现 Arrow internal `RecordReader` 无法支持某类 nested level 输出。

## 测试计划

新增或扩展 BE UT：

```text
be/test/format/new_parquet/parquet_complex_reader_test.cpp
```

优先用 Arrow writer 生成小 Parquet 文件，覆盖：

- required struct。
- optional struct。
- struct child nullable。
- array of primitive：null array、empty array、array with null element。
- array of struct。
- nested array：`Array(Array(String))`。
- map：empty map、null map、nullable value。
- struct containing array/map。
- multiple row groups。
- child projection：struct child 裁剪、array element struct child 裁剪、map value struct child 裁剪。
- selected read：复杂列作为 non-predicate column，predicate column 过滤出稀疏 selection。
- skip then read：直接验证复杂列 reader 游标。

后续回归测试：

```text
regression-test/suites/external_table_p0/parquet_complex_types.groovy
```

要求：

- 结果排序稳定，使用 `order_qt` 或显式 `order by`。
- 错误场景使用 `test { sql; exception }`。
- 测试前 drop table，不在测试末尾 drop，便于失败后排查。

## 分阶段落地

### 阶段 1：Schema level 信息补齐

- 扩展 `ParquetColumnSchema`，保存 definition/repetition level。
- 增加 `file_path`、`field_id_path`、`name_path`，并明确 top-level file-local id 与 table field id 的边界。
- 重写 `build_parquet_column_schema()` 的复杂类型规约逻辑。
- 增加 schema-only UT，覆盖 LIST/MAP legacy 和 standard encodings。

### 阶段 1.5：Projection tree 和 projected schema view

- 扩展 `FileScanRequest`，表达 top-level complex field 的 child projection tree。
- 增加 projected `SchemaField` / `DataTypePtr` 构造逻辑。
- `ParquetColumnReaderFactory` 接收 projection tree，只创建被投影 child reader。
- 增加 child pruning UT，验证未投影 leaf 不创建 reader、不读取 column chunk。

### 阶段 2：Leaf level reader

- 为 `ScalarColumnReader` 增加 nested leaf read API。
- 去掉 `max_repetition_level == 0 && max_definition_level <= 1` 的硬限制，改成 flat path 和 nested path 分支。
- 验证 nullable primitive 在 nested path 下的 value/null 对齐。

### 阶段 3：Struct reader 完整化

- 实现 nullable struct。
- 保证 null struct row 对所有 children 插入 default/null slot。
- 增加 required/optional struct UT。

### 阶段 4：List reader

- 实现 list assembler、offset 写入、null/empty list 区分。
- 实现 overflow child buffer。
- 实现 list `skip()`。
- 增加 array、nested array、array of struct UT。

### 阶段 5：Map reader

- 实现 map schema 规约到 key/value children。
- 直接写 `ColumnMap` keys、values、offsets。
- 校验 required key。
- 增加 map UT。

### 阶段 6：Selected read 和集成测试

- 验证 complex non-predicate column 在 lazy materialization 下正确。
- 验证 complex projected child 在 lazy materialization 下正确。
- 增加 sparse selection、empty selection、multi-row-group 测试。
- 将复杂类型 reader 接入 `ParquetReader` 现有 scan loop，不改 table/global schema 边界。

### 阶段 7：优化和扩展

- complex child predicate execution。
- complex column statistics 和 page index 支持。
- complex predicate fallback。
- 复杂列 schema change child-level mapping。

## 验收标准

完成“复杂类型完整支持”至少需要满足：

- `STRUCT`、nullable `STRUCT`、`LIST`、nested `LIST`、`MAP` 可以正确读入 Doris complex columns。
- 复杂类型 child projection 可以裁剪未请求 leaf，并输出 projected complex type。
- null、empty、missing element/value 的语义与 Parquet definition/repetition level 一致。
- `read()`、`skip()`、`select()` 在复杂类型上均保持 parent-row 语义。
- flat primitive 现有测试不退化。
- 新增 BE UT 覆盖复杂类型基础、嵌套、selected read 和 multi-row-group。
- `ParquetReader` 不引入 table/global schema 语义。
- schema/path/level 元数据足够后续 `TableColumnMapper` 实现 child-level schema change，不需要重写复杂类型 reader 主体。
