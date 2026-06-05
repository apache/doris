# New Parquet Reader 列标识重构实现说明

本文记录 new table/file reader 栈中列标识重构的当前实现状态和后续 TODO。

这次重构参考 DuckDB multi-file reader 的列标识模型，核心目标是把不同层级的
column id 和 index 拆开，避免继续用裸 `int32_t`、`ColumnId`、`size_t` 同时表示
table column id、file column id、block position、projection path 和 constant map
position。

## 状态概览

### 已完成

- 已引入 `TableColumnDefinition`，调用点已直接使用该类型，不再保留 `TableColumn`
  过渡 alias。
- 已引入 `TableColumnIdentifier`，用于描述 table/global column 如何匹配 file-local
  schema。
- `TableColumnDefinition` 已清理为纯 schema definition，不再保存 FE column unique id。
- `TableReadOptions::projected_column_unique_ids` 已用于在投影列旁路携带 FE/SlotRef id，
  `ColumnMapping::table_column_id` 由该数组建立。
- 已引入 `LocalColumnId`、`LocalIndex`、`GlobalIndex`、`ConstantIndex` 强类型。
- `FileScanRequest` 已改为使用 `LocalColumnIndex` 和
  `std::map<LocalColumnId, LocalIndex> local_positions`。
- new parquet reader 已改为消费 `LocalColumnIndex` 和 `LocalIndex`。
- nested struct projection 已开始按 projection tree 跳过未投影 child reader。
- Iceberg row position 和 equality delete 相关路径已迁移到新的 local id/index 表示。
- column 相关类型已补充注释，说明各自语义边界和使用场景。

### 部分完成

- `LocalColumnIndex` 已统一替代原来的 `FieldProjection` 主要用途，但它的 `index`
  字段在顶层表示 file-local top-level column id，在 nested 层表示 child id。
- `STRUCT` projection 的 reader 创建路径已经按 projection 裁剪 child reader；
  `LIST` 和 `MAP` 已有 projection 校验和部分递归传递，但后续还可以继续收敛。
- `GlobalIndex` 和 `ConstantIndex` 已定义，但还没有形成完整的一等数据流。
- `ColumnMapping` 已增加更明确的注释和局部字段语义，但还没有完全改造成
  `global_index/local_column_id/local_index/constant_index` 的结构。
- `LocalColumnIndex` projection merge 和 schema projection 已沉淀为公共 helper，但 reader
  projection helper 还没有完全收敛。

### 未完成

- `ConstantMap` 尚未实现。
- filter target 尚未建模为 DuckDB 风格的 local-or-constant entry。
- table/global output 位置还没有全面替换为 `GlobalIndex`。
- reader projection 相关 helper 还比较分散。
- `LocalColumnIndex` 顶层 id 和 nested child id 是否继续共用同一个字段仍需评估。

## DuckDB 参考

DuckDB 在 `src/include/duckdb/common/multi_file/multi_file_data.hpp` 中把 multi-file
reader 的列身份拆成几类：

- `MultiFileColumnDefinition`：table/global schema column definition。
- `ColumnIndex`：递归 projection path。
- `MultiFileLocalColumnId`：当前文件 schema 中的列 id。
- `MultiFileLocalIndex`：local file reader 输出或 local expression 输入中的位置。
- `MultiFileGlobalIndex`：global projection/output 列表中的位置。
- `MultiFileConstantMapIndex`：per-file constant map 中的位置。
- `MultiFileFilterEntry`：filter 目标，指向 local index 或 constant map index。

最关键的原则是：column id 表示 schema identity，index 表示某个 vector、block、
projection list 或 map 中的位置。Doris 当前实现沿用了这个原则，但还没有完全补齐
constant map 和 filter entry。

## 当前核心类型

### TableColumnDefinition

定义位置：`be/src/format/reader/table_reader.h`

`TableColumnDefinition` 是 table/global schema 中的列定义，对标 DuckDB 的
`MultiFileColumnDefinition`。它描述 table 侧列的名字、类型、children、默认表达式、
partition 属性和 virtual column 属性。

当前调用点已直接使用 `TableColumnDefinition`，不再通过 `TableColumn` alias 过渡。

### TableColumnIdentifier

定义位置：`be/src/format/reader/table_reader.h`

`TableColumnIdentifier` 只用于 table/global schema 到 file-local schema 的匹配，不表示
output block 位置，也不表示 file reader block 位置。

当前支持三种匹配方式：

- `FIELD_ID`：用于 Iceberg 等有 schema evolution field id 的 table format。
- `NAME`：用于按名字匹配的普通文件格式。
- `POSITION`：用于只能按物理顺序匹配的文件，比如 Hive1 ORC 场景。

FE column unique id 不再保存在 `TableColumnDefinition` 中。投影列通过
`TableReadOptions::projected_column_unique_ids` 平行传递 FE/SlotRef id，filter rewrite
通过 `TableFilter::column_unique_ids`、`TableColumnPredicates` 和
`ColumnMapping::table_column_id` 继续使用这些 id；schema 匹配只使用
`TableColumnIdentifier`。

### LocalColumnId

定义位置：`be/src/format/reader/file_reader.h`

`LocalColumnId` 表示当前文件 schema 中的 top-level column id。

使用场景：

- `FileScanRequest::local_positions` 的 key。
- new parquet reader 选择 top-level file column。
- page index、statistics、bloom filter 等 file-local pruning 元数据的 key。
- row-position 这类 reader 内部 virtual local column id。

不要把 `LocalColumnId` 当作 block position、table/global id 或 nested child id。

### LocalIndex

定义位置：`be/src/format/reader/file_reader.h`

`LocalIndex` 表示一次 `FileScanRequest` 内，file reader 输出 block 中的列位置。

使用场景：

- `FileScanRequest::local_positions` 的 value。
- file-local `SlotRef` 输入位置。
- `TableReader` 从 file-local block 读取数据的位置。
- equality delete、row position 等需要定位 file block column 的路径。

不要把 `LocalIndex` 当作 schema id。

### GlobalIndex

定义位置：`be/src/format/reader/file_reader.h`

`GlobalIndex` 表示 table/global output block 中的列位置。

当前类型已经引入，但使用面还没有完全铺开。后续 `TableReadOptions::projected_columns`、
`ColumnMapping`、table-level filter map 等位置可以继续迁移到 `GlobalIndex`。

### ConstantIndex

定义位置：`be/src/format/reader/file_reader.h`

`ConstantIndex` 表示 per-split 或 per-file constant map 中的位置。

当前类型已经引入，但 `ConstantMap` 尚未实现。因此 missing/default/partition/generated/
virtual column 仍主要通过 `ColumnMapping` flags 和 expression 表示。

## FileScanRequest

定义位置：`be/src/format/reader/file_reader.h`

当前结构已经改为：

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

## LocalColumnIndex

定义位置：`be/src/format/reader/file_reader.h`

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

这个结构和 DuckDB `ColumnIndex` 类似，迁移成本较低。但它没有在类型层面区分 top-level
file column id 和 nested child id，这是后续可以继续收紧的地方。

## Nested Projection 当前实现

### Mapper 阶段

相关位置：`be/src/format/reader/column_mapper.cpp`

`TableColumnMapper` 会根据 table/global projection 和 nested predicate，构造 file-local
`LocalColumnIndex` 树。主要职责包括：

- 通过 `TableColumnIdentifier` 找到 table column 对应的 file field。
- 为 output column 构造需要读取的 file projection。
- 为 nested predicate 构造额外的 file projection。
- 合并 predicate projection 和 output projection。
- 保存 `ColumnMapping::original_file_children`，用于后续 projected type 重建和 nested
  filter localization。

当前已有两个公共 helper：

- `merge_local_column_index`：合并同一个 file-local node 的 projection tree。
- `project_schema_field`：按 `SchemaField::id` 对 file-local schema 应用 projection。

### Parquet Reader 阶段

相关位置：

- `be/src/format/new_parquet/parquet_reader.cpp`
- `be/src/format/new_parquet/parquet_scan.cpp`
- `be/src/format/new_parquet/reader/column_reader.cpp`

new parquet reader 使用 `LocalColumnIndex` 创建 column reader。

`STRUCT` 当前已经按 projection 裁剪 reader：

- full projection 时读取全部 children。
- partial projection 时只为选中的 child 创建 reader。
- 未投影 child 不再创建底层 reader。

`LIST` 和 `MAP` 当前也接受 projection 递归传递，但支持范围更保守：

- `LIST` 的 scalar element 不允许携带 child projection。
- `LIST` 的 complex element 可以继续递归到 struct/list/map。
- `MAP` 默认读取 key，value 可以递归投影。
- 对 unsupported complex nested projection 仍返回 `NotSupported`。

### TableReader Finalize 阶段

相关位置：`be/src/format/reader/table_reader.cpp`

`TableReader` 仍负责把 file-local block 转成 table/global block：

- 对 projected struct/list/map 做 child remap。
- 对 missing/default/partition/generated/virtual column 执行 expression。
- 对 nested projection 后的 file type 重建 table 侧形态。

这块逻辑还没有完全拆成 constant map + projection expression 的统一模型。

## ColumnMapping 当前状态

定义位置：`be/src/format/reader/column_mapper.h`

`ColumnMapping` 现在承担 table/global column 到 file-local column 的映射职责。当前字段中：

- `table_column_id`：table/global column unique id。
- `field_id`：file-local field id。root mapping 可转成 `LocalColumnId`，nested mapping 表示
  parent 下的 child id。
- `file_path`：从 top-level file column 到当前 mapping 的 child id path。
- `original_file_type` / `original_file_children`：projection 前的 file type 和 child schema。
- `file_type` / `table_type`：投影和 cast 后参与读取/输出的类型。
- `projection`：从 file/local 或 constant 输入生成 table/global 输出的表达式。
- `child_mappings`：nested table child 到 file child 的映射树。
- `is_constant` / `is_missing`：当前仍用于表达非真实 file column 来源。
- `has_complex_projection`：表示读取到的 nested value 需要在 finalize 阶段重建 shape。

后续目标是把 `ColumnMapping` 中的 source 和 position 拆得更直接：

```cpp
struct ColumnMapping {
    GlobalIndex global_index;
    TableColumnDefinition global_column;

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

## Iceberg 相关路径

相关位置：`be/src/format/table/iceberg_reader_v2.cpp`

已迁移的内容：

- row position 使用 `LocalColumnId` 加入 `FileScanRequest`。
- `_row_position_block_position` 从 `local_positions` 中读取 `LocalIndex::value()`。
- equality delete 读取 delete file 时使用 `LocalColumnIndex` 和 `local_positions`。
- equality delete predicate 构造时，用 data file schema field id 定位 file-local block
  column。

需要继续关注：

- equality delete 当前仍按 field id 找 data file schema；如果 nested equality delete 后续要支持，
  需要重新设计 projection path 和 predicate 输入。
- row position 更像 virtual local column。后续可以决定是否把它放入统一 virtual column
  source，而不是特殊 `LocalColumnId`。

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

### TODO 5：扩大 GlobalIndex 使用面

当前 `GlobalIndex` 已定义，但 table/global output position 还没有全面替换。

后续可以优先迁移：

- `TableReadOptions::projected_columns` 的访问位置。
- `ColumnMapping` 中的 output column position。
- table-level filter map 的 key。
- final output block materialize 阶段的 column position。

### TODO 6：继续完善 LIST/MAP nested projection

当前 `STRUCT` reader 裁剪收益最明确。`LIST` 和 `MAP` 的复杂 nested projection 仍偏保守。

后续可以继续优化：

- list element 是 struct 时，只读取被投影的 struct children。
- map value 是 struct 时，只读取被投影的 value children。
- 统一 key/value/list element projection 的错误信息和 schema validation。
- 增加针对 list/map partial projection 的单测。

## 测试和验证建议

已进行的验证：

- `build-support/clang-format.sh` 已用于相关 C++ 文件。
- `git diff --check` 已通过。
- Fedora 上使用 `BUILD_TYPE=DEBUG ./build.sh --be` 编译，已暴露并修复多个 DEBUG 下的
  类型/字段名问题。

建议继续补充：

- `be/test/format/new_parquet/parquet_reader_test.cpp`：覆盖 struct partial projection。
- `be/test/format/new_parquet/parquet_column_reader_test.cpp`：覆盖 reader factory 只创建投影 child。
- `be/test/format/reader/table_reader_test.cpp`：覆盖 table/global schema 到 file-local schema
  的 field id/name/position 匹配。
- Iceberg equality delete 相关单测：覆盖 `field_ids` 到 data file local positions 的映射。
- 如果后续实现 ConstantMap，需要补 partition/default/missing column filter 的单测。
