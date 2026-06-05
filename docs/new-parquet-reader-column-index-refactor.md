# New Parquet Reader 列标识重构实现说明 + TODO

本文记录 new table/file reader 栈中列标识重构的当前实现和后续 TODO。

这次重构参考 DuckDB multi-file reader 的列标识模型，核心原则是把不同层级的
column id 和 index 拆开：column id 表示 schema identity，index 表示某个 block、
vector、projection list 或 constant map 中的位置。Doris 当前实现不再用裸 `int32_t`、
`ColumnId` 或 `size_t` 同时表示 table column id、file column id、block position、
projection path 和 constant map position。

## DuckDB 参考

DuckDB 在 `src/include/duckdb/common/multi_file/multi_file_data.hpp` 中把 multi-file
reader 的列身份拆成几类：

- `MultiFileColumnDefinition`：table reader 和 base file reader 共享的 schema column
  definition。
- `ColumnIndex`：递归 projection path。
- `MultiFileLocalColumnId`：当前文件 schema 中的列 id。
- `MultiFileLocalIndex`：local file reader 输出或 local expression 输入中的位置。
- `MultiFileGlobalIndex`：global projection/output 列表中的位置。
- `MultiFileConstantMapIndex`：per-file constant map 中的位置。
- `MultiFileFilterEntry`：filter 目标，指向 local index 或 constant map index。

Doris 对应把这些共享定义集中放在 `be/src/format/reader/column_data.h`。这个文件名覆盖
schema definition、local/global/constant index、file-local projection tree 等数据结构，比
只表达 column definition 的命名更接近 DuckDB `multi_file_data.hpp` 的职责范围。

## 当前实现

### ColumnDefinition

定义位置：`be/src/format/reader/column_data.h`

`ColumnDefinition` 对标 DuckDB 的 `MultiFileColumnDefinition`，用于表示 table/global
schema 和 file reader 返回的 file-local schema。它描述列名、类型、children、默认表达式、
partition 属性和 file-local column kind。

`ColumnDefinition` 不保存 FE column unique id。FE column unique id 只在
`FileScannerV2` 边界内使用，并在进入 table/file reader 前翻译成 `GlobalIndex`。
table reader 层及以下只通过 `ColumnDefinition::Identifier` 做 schema 匹配，通过
`GlobalIndex` 表示 table/global output column 位置。

旧的 file-local schema 专用类型已删除，`FileReader::get_schema()` 直接返回
`std::vector<ColumnDefinition>`。Parquet reader、Iceberg reader、schema projection 和
table reader tests 都已经迁移到该类型。

### ColumnDefinition::Identifier

定义位置：`be/src/format/reader/column_data.h`

`ColumnDefinition::Identifier` 用于描述一个 column 如何匹配另一份 schema，不表示 block
位置，也不表示 file reader 输出位置。

当前支持三种匹配方式：

- `FIELD_ID`：schema evolution aware field id，例如 Iceberg/Parquet field id。
- `NAME`：逻辑列名，用于按名字匹配的文件格式。
- `POSITION`：物理文件顺序，用于 Hive1 ORC 这类文件名不可用的场景。

`ColumnDefinition::field_id()`、`file_position()` 和 `match_name()` 是带语义断言的访问
helper，调用方应先选择正确匹配模式，不要在 reader 层重新引入 FE column unique id。

### LocalColumnId

定义位置：`be/src/format/reader/column_data.h`

`LocalColumnId` 表示当前物理文件 schema 中的 top-level column id。

使用场景：

- `FileScanRequest::local_positions` 的 key。
- new parquet reader 选择 top-level file column。
- page index、statistics、bloom filter 等 file-local pruning 元数据的 key。
- row-position 这类 reader 内部 virtual local column id。

不要把 `LocalColumnId` 当作 block position、table/global id 或 nested child id。

### LocalIndex

定义位置：`be/src/format/reader/column_data.h`

`LocalIndex` 表示一次 `FileScanRequest` 内，file reader 输出 block 中的列位置。

使用场景：

- `FileScanRequest::local_positions` 的 value。
- file-local `SlotRef` 输入位置。
- `TableReader` 从 file-local block 读取数据的位置。
- equality delete、row position 等需要定位 file block column 的路径。

不要把 `LocalIndex` 当作 schema id。

### GlobalIndex

定义位置：`be/src/format/reader/column_data.h`

`GlobalIndex` 表示 table/global output block 中的列位置。

当前使用场景：

- `ColumnMapping::global_index`。
- `TableFilter::global_indices`。
- `TableColumnPredicates` 的 key。
- `FileScannerV2` 将 FE `slot_id` / `col_unique_id` 翻译到 reader 层时的目标索引。

### ConstantIndex

定义位置：`be/src/format/reader/column_data.h`

`ConstantIndex` 表示 per-split 或 per-file constant map 中的位置。

当前类型已经引入，但 `ConstantMap` 尚未实现。因此 missing/default/partition/generated/
virtual column 仍主要通过 `ColumnMapping` flags 和 expression 表示。

### LocalColumnIndex

定义位置：`be/src/format/reader/column_data.h`

`LocalColumnIndex` 是递归 file-local projection path：

```cpp
struct LocalColumnIndex {
    int32_t index = -1;
    bool project_all_children = true;
    std::vector<LocalColumnIndex> children;
};
```

当前约定：

- 顶层 `index` 表示 `LocalColumnId` 的值。
- nested 层 `index` 表示当前 parent 下的 child id。
- `project_all_children = true` 表示读取该节点下完整 subtree。
- `project_all_children = false` 时，`children` 表示需要读取的 child projection。

`merge_local_column_index` 用于合并同一个 file-local node 的 projection tree：full
projection 覆盖 partial projection，两个 partial projection 按 child id 递归合并。

### FileScanRequest

定义位置：`be/src/format/reader/file_reader.h`

`FileScanRequest` 只描述所有文件格式共享的 file-local 读取输入，不出现 table/global
schema。所有 schema change、filter localization、default/generated/partition 列都在 table
层完成。

核心字段：

```cpp
struct FileScanRequest {
    std::vector<LocalColumnIndex> predicate_columns;
    std::vector<LocalColumnIndex> non_predicate_columns;
    std::map<LocalColumnId, LocalIndex> local_positions;
};
```

语义：

- `predicate_columns`：predicate 需要读取的 file-local projection。
- `non_predicate_columns`：最终输出需要读取的 file-local projection。
- `local_positions`：top-level `LocalColumnId` 到 request-local `LocalIndex` 的映射。

`local_positions` 是 request-local 的 block layout，不是 file schema 顺序，也不是最终
table/global output 顺序。

### Nested Projection

`TableColumnMapper` 根据 table/global projection 和 nested predicate，构造 file-local
`LocalColumnIndex` 树。主要职责包括：

- 通过 `ColumnDefinition::Identifier` 找到 table column 对应的 file field。
- 为 output column 构造需要读取的 file projection。
- 为 nested predicate 构造额外的 file projection。
- 合并 predicate projection 和 output projection。
- 保存 `ColumnMapping::original_file_children`，用于后续 projected type 重建和 nested
  filter localization。

new parquet reader 使用 `LocalColumnIndex` 创建 column reader。`STRUCT` 已按 projection
裁剪 child reader：full projection 读取全部 children，partial projection 只为选中的 child
创建 reader。`LIST` 和 `MAP` 已有 projection 校验和部分递归传递，但支持范围更保守。

`TableReader` 仍负责把 file-local block 转成 table/global block，包括 projected
struct/list/map child remap、missing/default/partition/generated/virtual column materialization
和 nested projection 后的 type 重建。

### ColumnMapping

定义位置：`be/src/format/reader/column_mapper.h`

`ColumnMapping` 现在承担 table/global column 到 file-local column 的映射职责。当前关键字段：

- `global_index`：table/global output block 中的列位置。
- `field_id`：file-local field id。root mapping 可转成 `LocalColumnId`，nested mapping 表示
  parent 下的 child id。
- `file_path`：从 top-level file column 到当前 mapping 的 child id path。
- `original_file_type` / `original_file_children`：projection 前的 file type 和 child schema。
- `file_type` / `table_type`：投影和 cast 后参与读取/输出的类型。
- `projection`：从 file-local 或 constant 输入生成 table/global 输出的表达式。
- `child_mappings`：nested table child 到 file child 的映射树。
- `is_constant` / `is_missing`：当前仍用于表达非真实 file column 来源。
- `has_complex_projection`：表示读取到的 nested value 需要在 finalize 阶段重建 shape。

## TODO

### TODO 1：实现 ConstantMap

目标：把 missing/default/partition/generated/virtual column 从 file column 逻辑中拆出来。

建议结构：

```cpp
struct ConstantEntry {
    GlobalIndex global_index;
    VExprContextSPtr expr;
    DataTypePtr type;
};

class ConstantMap {
public:
    ConstantIndex add(ConstantEntry entry);
    const ConstantEntry& get(ConstantIndex index) const;

private:
    std::vector<ConstantEntry> entries;
};
```

收益：

- `ColumnMapping` 可以显式表示当前 column 来源于 file-local column 还是 constant。
- filter 可以在打开 file reader 前对 constant column 求值。
- partition/default/missing column 不需要伪装成 file projection。

### TODO 2：引入 LocalFilterEntry

目标：对齐 DuckDB `MultiFileFilterEntry`，让 filter target 明确指向 local column 或 constant。

建议结构：

```cpp
struct LocalFilterEntry {
    std::optional<LocalIndex> local_index;
    std::optional<ConstantIndex> constant_index;
};
```

约束：

- `local_index` 和 `constant_index` 只能有一个。
- 指向 `LocalIndex` 的 filter 在 file reader 读出 block 后执行。
- 指向 `ConstantIndex` 的 filter 可以提前在 split/file 打开前执行。

### TODO 3：收紧 LocalColumnIndex 类型

当前 `LocalColumnIndex::index` 同时表示 top-level file column id 和 nested child id。

如果后续误用风险继续增加，可以拆成：

```cpp
struct LocalChildIndex {
    int32_t index = -1;
    bool project_all_children = true;
    std::vector<LocalChildIndex> children;
};

struct LocalColumnIndex {
    LocalColumnId root;
    bool project_all_children = true;
    std::vector<LocalChildIndex> children;
};
```

这样能在类型层面阻止 top-level id 和 child id 混用，但迁移成本更高。

### TODO 4：沉淀 reader projection helper

new parquet reader 中 struct/list/map child projection 查找和校验逻辑仍分散在 column
reader factory 内部。

后续可以继续沉淀公共 helper：

- 根据 parent projection 查找指定 child projection。
- 统一 full projection、partial projection、empty projection 的判断。
- 统一 struct/list/map 对 unsupported nested projection 的校验和错误信息。

### TODO 5：继续完善 LIST/MAP nested projection

当前 `STRUCT` reader 裁剪收益最明确。`LIST` 和 `MAP` 的复杂 nested projection 仍偏保守。

后续可以继续优化：

- list element 是 struct 时，只读取被投影的 struct children。
- map value 是 struct 时，只读取被投影的 value children。
- 统一 key/value/list element projection 的错误信息和 schema validation。
- 增加针对 list/map partial projection 的单测。

### TODO 6：拆分 ColumnMapping source 和 position

当前 `ColumnMapping` 已经引入 `global_index`，但 file source、constant source 和 request-local
position 仍混在多个 flags 和 optional 字段中。

后续目标结构可以继续向下面形态收敛：

```cpp
struct ColumnMapping {
    GlobalIndex global_index;

    std::optional<LocalColumnId> local_column_id;
    std::optional<LocalIndex> local_index;
    std::optional<ConstantIndex> constant_index;

    LocalColumnIndex local_projection;
    DataTypePtr local_type;
    DataTypePtr global_type;

    VExprContextSPtr projection_expr;
    VExprContextSPtr reader_filter_expr;
    std::vector<ColumnMapping> child_mappings;
};
```
