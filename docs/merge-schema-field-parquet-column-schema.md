# Simplify File Reader Schema: Eliminate Redundancy Between SchemaField and ParquetColumnSchema

## 1. 问题

当前 new parquet reader 存在两个 schema 节点：

| 类型 | 位置 | 字段数 |
|---|---|---|
| `ParquetColumnSchema` | `new_parquet/parquet_column_schema.h:48` | 14 字段（含 `const schema::Node*`、`const ColumnDescriptor*`、`leaf_column_id`、`max_definition_level` 等 Parquet 内部指针/元数据） |
| `SchemaField` | `reader/file_reader.h:59` | 5 字段（id, name, type, children, column_type） |

冗余不在于这两个类型的存在，而在于：

1. **死代码**：`_fill_projected_schema_field()` 和 `_get_projected_schema_field()` 从未被调用（grep 全仓，`_get_projected_schema_field` 只有声明和定义，零调用方）。它们是之前聚合下推路径的残留，当前 `get_aggregate_result()` 已直接使用 `find_projected_minmax_leaf()` 沿 `ParquetColumnSchema` 树查找 leaf，不再经过这些方法。

2. **STRUCT/ARRAY/MAP 类型重建逻辑重复三份**：
   - `parquet_reader.cpp:138-167` — `_fill_projected_schema_field` 内的 switch（死代码，但仍占用维护心智）
   - `table_reader.h:910-958` — `_rebuild_projected_type()`
   - `column_mapper.cpp:364-407` — `build_projected_child_type()`

3. **`DataReader::block_schema` 用 `vector<SchemaField>` 存储**——`SchemaField` 的 `children` 树在 block layout 场景下从未被使用（仅用 `id`、`name`、`type`），用完整 `SchemaField` 类型过度表达了。

4. **`ColumnType::FILE_NAME`**——枚举值定义了但从未被赋值，dead code。

## 2. DuckDB 参考

DuckDB 的 Parquet 内部类型不跨越 parquet extension 边界，与 Doris 当前分层一致：

```
DuckDB                                          Doris
──────                                          ─────

ParquetColumnSchema (内部，14 字段)              ParquetColumnSchema (内部，14 字段)
    │ ParseColumnDefinition()                       │ _fill_schema_field()
    │ 提取 name/type/identifier/children             │ 提取 id/name/type/children/column_type
    ▼                                                ▼
MultiFileColumnDefinition (通用，6 字段)          SchemaField (通用，5 字段)
    │                                                │
    │ MapColumn() 操作 MultiFileColumnDefinition     │ _project_schema_field() 操作 SchemaField  ✓
    │ 不产生 projected schema 树                     │ block_schema 用完整 SchemaField 过度表达   ✗
    │                                                │ _fill_projected_schema_field 死代码         ✗
    ▼                                                ▼
ColumnIndex + column_ids + expressions           FieldProjection + column_positions + block_template
```

**SchemaField 不是冗余**——它与 DuckDB 的 `MultiFileColumnDefinition` 角色严格对应，都是隔离 Parquet 内部指针（`Node*`、`ColumnDescriptor*`）的正确分层。`_fill_schema_field()` 也不是冗余——它与 DuckDB 的 `ParseColumnDefinition()` 等价。

## 3. 设计方案

### 3.1 删除项

| 删除 | 位置 | 核实依据 |
|---|---|---|
| `_fill_projected_schema_field()` | `parquet_reader.h:126-128`, `parquet_reader.cpp:96-171` | 死代码。唯一调用方是 `_get_projected_schema_field`，后者同样零调用 |
| `_get_projected_schema_field()` | `parquet_reader.h:129-131`, `parquet_reader.cpp:173-184` | 死代码。grep 全仓无调用 |
| `ColumnType::FILE_NAME` | `file_reader.h:52` | 枚举值从未被赋值 |

### 3.2 缩减项

| 当前 | 改为 | 原因 |
|---|---|---|
| `DataReader::block_schema` 类型 `vector<SchemaField>` | `vector<FileBlockColumn>`（新轻量结构） | block layout 只用 `id`/`name`/`type`，不需要 `children` 树。且 `SchemaField` 暗示它是一个完整的 schema 节点，误导读者 |

### 3.3 新增项

| 新增 | 位置 | 说明 |
|---|---|---|
| `struct FileBlockColumn { ColumnId file_column_id; std::string name; DataTypePtr type; };` | `table_reader.h` 的 `TableReader` private 区域 | 替代 `block_schema` 的轻量结构；只服务 `TableReader` 内部 file block layout，不暴露到 `FileReader` API |
| `rebuild_projected_type()` | `format/reader/schema_projection.h`（新建） | 合并三份 STRUCT/ARRAY/MAP 类型重建为单一共享函数 |

### 3.4 保留项

| 保留 | 原因 |
|---|---|
| `SchemaField` | 与 DuckDB `MultiFileColumnDefinition` 等价，通用 FileReader 接口类型 |
| `_fill_schema_field()` | 与 DuckDB `ParseColumnDefinition()` 等价，轻量提取 |
| `ParquetColumnSchema` | Parquet 内部，不跨越 FileReader 边界 |
| `FieldProjection` | 投影描述符，与 schema 节点正交 |
| `DataReader::file_schema` | `get_schema()` 返回值，table 层列映射的输入 |

### 3.5 FileBlockColumn 替代 block_schema

`block_schema` 当前的三个用途及替代方式：

**用途 A：构建 `block_template`**（`table_reader.h:302-306`）

```cpp
// 之前：遍历 block_schema（vector<SchemaField>）
for (const auto& field : _data_reader.block_schema) {
    _data_reader.block_template.insert(
        {field.type->create_column(), field.type, field.name});
}

// 之后：遍历 file_block_layout（vector<FileBlockColumn>）
for (const auto& col : _data_reader.file_block_layout) {
    _data_reader.block_template.insert(
        {col.type->create_column(), col.type, col.name});
}
```

**用途 B：聚合下推构建 `file_block` 和查找列位置**（`table_reader.h:726-756`）

```cpp
// 之前：遍历 block_schema
for (const auto& field : _data_reader.block_schema) {
    file_block.insert({field.type->create_column(), field.type, field.name});
}
// ... block_schema[block_position].id == result_column.projection.field_id

// 之后：遍历 file_block_layout
for (const auto& col : _data_reader.file_block_layout) {
    file_block.insert({col.type->create_column(), col.type, col.name});
}
// ... file_block_layout[block_position].file_column_id == result_column.projection.field_id
```

**用途 C：DCHECK 完整性检查**（`table_reader.h:215`）

```cpp
// 之前
DCHECK_EQ(_data_reader.block_template.columns(), _data_reader.block_schema.size());

// 之后
DCHECK_EQ(_data_reader.block_template.columns(), _data_reader.file_block_layout.size());
```

**构建方式**不变（`table_reader.h:269-298`），仅将输出类型从 `SchemaField` 改为 `FileBlockColumn`：

```cpp
_data_reader.file_block_layout.resize(file_request->column_positions.size());
for (const auto& [file_column_id, block_position] : file_request->column_positions) {
    const auto* field = _find_schema_field(_data_reader.file_schema, file_column_id);
    // ... 找到对应的 FieldProjection ...
    SchemaField projected;
    RETURN_IF_ERROR(_project_schema_field(*field, projection, &projected));
    _data_reader.file_block_layout[block_position] = {
        .file_column_id = file_column_id,
        .name = projected.name,
        .type = projected.type,
    };
}
```

注意：这里仍然调用 `_project_schema_field()` 来获得投影后的类型，但结果只提取 `name` 和 `type` 存入 `FileBlockColumn`，不再保留整个 `SchemaField`（含 `children` 树）。`_project_schema_field()` 本身保留，它在通用层的位置是正确的。

### 3.6 共享类型重建函数

`rebuild_projected_type()` 接收最小输入，由各调用方适配：

```cpp
// format/reader/schema_projection.h
namespace doris::reader {

// 根据裁剪后的 children 重建复杂类型。不依赖 SchemaField 或 ColumnMapping，
// 只接收类型系统原生类型，三个调用方各自从自己的数据结构提取 child type/name。
//
// original_type: 投影前的完整 DataType（用于保留 nullability 和 MAP key type）
// child_types:   投影后保留的 child 类型列表
// child_names:   投影后保留的 child 名称列表
// projected_type: 输出重建后的类型
Status rebuild_projected_type(const DataTypePtr& original_type,
                              const std::vector<DataTypePtr>& child_types,
                              const std::vector<std::string>& child_names,
                              DataTypePtr* projected_type);

} // namespace doris::reader
```

三处调用方适配：

| 调用方 | 适配方式 |
|---|---|
| `column_mapper.cpp:364` `build_projected_child_type()` | 遍历 `child_mappings` 提取 `child_mapping.file_type` → `child_types`，`child_mapping.file_column_name` → `child_names`，跳过 `!field_id.has_value()` 的 missing child |
| `table_reader.h:910` `_rebuild_projected_type()` | 遍历 `projected_field->children` 提取 `child.type` → `child_types`，`child.name` → `child_names` |
| `parquet_reader.cpp:138-167`（死代码） | 随 `_fill_projected_schema_field` 一并删除 |

## 4. 实施步骤

### Step 1: 删除 dead code

**文件**: `parquet_reader.h`
- 删除 `_fill_projected_schema_field()` 声明（line 126-128）
- 删除 `_get_projected_schema_field()` 声明（line 129-131）

**文件**: `parquet_reader.cpp`
- 删除 `_fill_projected_schema_field()` 实现（lines 96-171）
- 删除 `_get_projected_schema_field()` 实现（lines 173-184）
- 删除相关的 `#include`（`data_type_array.h`、`data_type_map.h`、`data_type_nullable.h`、`data_type_struct.h` —— 如果不再被文件中其他代码使用）

**文件**: `file_reader.h`
- 删除 `ColumnType::FILE_NAME = 2`，保留 `DATA_COLUMN = 0` 和 `ROW_NUMBER = 1`

### Step 2: 提取共享 `rebuild_projected_type()`

**新建** `format/reader/schema_projection.h` 和 `schema_projection.cpp`。

合并三处的 STRUCT/ARRAY/MAP switch 逻辑。关键是在 MAP 处理上统一：三处对 value 索引的提取方式略有不同（`entry_type->get_elements().size() == 1 ? 0 : 1` vs 直接取 `get_element(1)`），统一为一个参数 `value_idx` 或从 original_type 自动推导。

**修改调用方**：
- `column_mapper.cpp`：`build_projected_child_type()` 改为调用共享函数，传入从 `child_mappings` 提取的 `child_types`/`child_names`
- `table_reader.h`：`_rebuild_projected_type()` 改为调用共享函数
- `parquet_reader.cpp`：随 Step 1 删除，不涉及

### Step 3: 引入 `FileBlockColumn` 替代 `block_schema`

**文件**: `table_reader.h`

```cpp
// 描述 file block 中单个列的类型信息。索引方式为 block_position。
// 仅在 TableReader 内部使用，用于构建 block_template 和聚合下推 file_block。
struct FileBlockColumn {
    ColumnId file_column_id = -1;
    std::string name;
    DataTypePtr type;
};
```

**修改 `DataReader`**：

```cpp
struct DataReader {
    std::unique_ptr<FileReader> reader;
    TableColumnMapper column_mapper;
    std::vector<SchemaField> file_schema;
    std::vector<FileBlockColumn> file_block_layout;  // 原 block_schema
    Block block_template;
};
```

**修改所有 `block_schema` 引用**（14 处）：类型替换 + `.id` → `.file_column_id`。逻辑不变——仍通过 `_project_schema_field()` 获得投影后的 `name` 和 `type`，只是最终存入 `FileBlockColumn` 而非完整 `SchemaField`。

### Step 4: 清理残留引用

- `column_reader.h:44` 的 `struct SchemaField;` 前向声明保留（仍被 `row_position_schema_field()` 使用）
- 检查 `iceberg_reader_v2.cpp`：确认只访问 `file_schema`，不访问 `block_schema`（当前搜索结果确认无 `block_schema` 引用）
- 测试文件中的 `block_schema` 引用同步更新

## 5. 验证

1. `grep -rn "_get_projected_schema_field\|_fill_projected_schema_field" be/src/` 结果为空
2. `rg -n "FILE_NAME\s*=\s*2" be/src/format/reader/file_reader.h` 结果为空；`enum ColumnType` 只保留 `DATA_COLUMN` / `ROW_NUMBER`
3. `grep -rn "block_schema" be/src/format/` 结果为空（被 `file_block_layout` 替代）
4. `build_projected_child_type` / `_rebuild_projected_type` 调用同一个 `rebuild_projected_type()`
5. BE 编译通过
6. format/reader 和 new_parquet 相关单元测试通过

## 6. 不做的事项

- **不删除 `SchemaField`**：与 DuckDB `MultiFileColumnDefinition` 等价，隔离 Parquet 内部指针
- **不删除 `_fill_schema_field()`**：与 DuckDB `ParseColumnDefinition()` 等价
- **不把 `ParquetColumnSchema` 暴露到通用层**：含生命周期敏感的 Parquet 内部指针
- **不修改 `ParquetColumnSchema`**：内部类型不变
- **不动 `FieldProjection`、`TableColumn`、`FileScanRequest`**：与本次清理正交
