# Parquet LIST/MAP Compatibility Design

本文描述如何参考 Arrow Parquet 的 LIST/MAP 兼容策略，在 Doris new parquet reader 中支持更多 Parquet 标准和 legacy 复杂类型 schema。

目标不是改变 `ListColumnReader` / `MapColumnReader` 的读取模型，而是在 schema 构建阶段把不同物理 schema 归一化成 Doris 当前 reader 可以消费的统一 `ParquetColumnSchema` tree。

## 背景

Parquet 的复杂类型是通过 group schema、logical/converted annotation、definition levels 和 repetition levels 共同表达的。

标准 LIST/MAP schema 比较明确，但历史 writer 产生过多种 legacy 形态。例如 LIST 可能缺少标准 `list.element` wrapper，MAP entry group 可能叫 `key_value`、`entries` 或其它名字。

Arrow C++ 的处理思路是：

1. 在 Parquet schema conversion 阶段识别标准和 legacy schema。
2. 将这些 schema 归一化为 Arrow `ListType` / `MapType` / `StructType`。
3. 后续 reader 只消费归一化后的 nested field tree，不在读取阶段继续判断 legacy schema 名字。

Doris new parquet reader 应采用相同边界：

1. `parquet_column_schema.cpp` 负责兼容不同 LIST/MAP physical schema。
2. `ParquetColumnSchema` 输出统一的 LIST/MAP child tree。
3. `ListColumnReader` / `MapColumnReader` / `ParquetLeafReader` 不感知 legacy schema 形态。

## 当前 Doris 限制

当前 `build_node_schema()` 的 LIST 分支只支持标准 3-level LIST：

```text
optional group a (LIST) {
  repeated group list {
    optional int32 element;
  }
}
```

当前限制：

- outer LIST group 必须只有一个 child。
- repeated child 必须是 group。
- repeated group 必须只有一个 child。
- 不支持 repeated primitive list。
- 不支持 repeated group 多字段 struct element。
- 不支持 `array` / `<parent>_tuple` 这类 legacy structural name。

当前 MAP 分支支持标准 MAP 结构：

```text
optional group m (MAP) {
  repeated group key_value {
    required binary key;
    optional int32 value;
  }
}
```

当前限制：

- outer MAP group 必须只有一个 child。
- entry child 必须 repeated group。
- entry group 必须正好两个 children。
- key 必须 required。
- 不支持 key-only map。
- 不支持没有 repeated entry layer 的非标准 MAP。

## 设计原则

1. 兼容逻辑只放在 schema 构建阶段。
2. reader 层继续消费统一 schema tree。
3. 不支持会改变 reader model 的格式，例如没有 repeated entry layer 的 MAP。
4. 第一阶段不支持 key-only map，因为 Doris `ColumnMap` 需要 values column。
5. 对容易误判的 schema 保持严格，避免把普通 struct 错解析成 LIST/MAP。
6. 支持范围对齐 Arrow 的稳定 legacy compatibility 规则，而不是无限放宽。

## LIST 兼容规则

对于 outer group annotated as `LIST`：

```text
optional group a (LIST) {
  repeated ... repeated_child;
}
```

先要求：

- outer LIST group 必须只有一个 child。
- child 必须是 repeated。

然后根据 repeated child 形态判断 element schema node。

### 1. 标准 3-level LIST

```text
optional group a (LIST) {
  repeated group list {
    optional int32 element;
  }
}
```

解析：

- repeated child 是 wrapper。
- element 是 wrapper 的唯一 child：`list.element`。
- `ParquetColumnSchema(LIST).children[0]` 指向 element schema。

### 2. Repeated primitive legacy LIST

```text
optional group a (LIST) {
  repeated int32 element;
}
```

解析：

- repeated primitive 本身是 element。
- element 本身不 nullable，因为 repeated primitive 不提供额外 optional element level。
- array 自身 nullable 仍由 outer LIST group 决定。

### 3. Repeated group as struct element

```text
optional group a (LIST) {
  repeated group element {
    optional int32 x;
    optional binary y;
  }
}
```

解析：

- repeated group 有多个 children。
- repeated group 本身是 element。
- element type 是 `STRUCT<x, y>`。

### 4. Legacy structural name

Arrow 会将某些名字视作 structural element，而不是标准 wrapper。

```text
optional group a (LIST) {
  repeated group array {
    optional int32 item;
  }
}
```

```text
optional group a (LIST) {
  repeated group a_tuple {
    optional int32 item;
  }
}
```

解析：

- repeated group 名为 `array`，或名为 `<list_name>_tuple`。
- repeated group 本身是 element。
- 即使它只有一个 child，也不要剥掉这一层。

### 5. One-child repeated group wrapper

```text
optional group a (LIST) {
  repeated group list {
    optional int32 element;
  }
}
```

如果 repeated group 只有一个 child，且不是 legacy structural name，则按 wrapper 处理：

- element 是 repeated group 的唯一 child。

但这里不能只按 child 数量判断。需要额外保持 Arrow / parquet-format 的 backward compatibility 规则：

- 如果 repeated group 自身带 `LIST` 或 `MAP` annotation，则 repeated group 本身是 element，不剥 wrapper。
- 如果 repeated group 的唯一 child 也是 repeated，则 repeated group 本身是 element，不剥 wrapper。
- 只有当 repeated group 无 logical annotation、唯一 child 非 repeated、且不是 legacy structural name 时，才把它当作标准 wrapper 剥掉。

这样可以避免把 two-level `List<List<T>>`、two-level `List<Map<K, V>>` 或单字段 repeated struct element 错解析成少一层的结构。

## LIST schema resolver

建议在 `parquet_column_schema.cpp` 中新增 helper：

```cpp
struct ListElementResolution {
    const parquet::schema::Node* repeated_node = nullptr;
    const parquet::schema::Node* element_node = nullptr;
    SchemaBuildContext repeated_context;
    SchemaBuildContext element_context;
    bool element_is_repeated_node = false;
};

Status resolve_list_element_node(
        const parquet::SchemaDescriptor& schema,
        const parquet::schema::GroupNode& list_group,
        const SchemaBuildContext& list_context,
        ListElementResolution* result);
```

Resolver 逻辑：

```text
if list_group.field_count != 1:
    reject

repeated_node = list_group.field(0)
if !repeated_node.is_repeated:
    reject

repeated_context = child_context(list_context, repeated_node, 0)

if repeated_node.is_primitive:
    element_node = repeated_node
    element_context = repeated_context
    element_is_repeated_node = true
    return

repeated_group = as_group(repeated_node)
if repeated_group.field_count == 0:
    reject

if repeated_group.field_count > 1:
    element_node = repeated_node
    element_context = repeated_context
    element_is_repeated_node = true
    return

if has_structural_list_name(list_group.name, repeated_group.name):
    element_node = repeated_node
    element_context = repeated_context
    element_is_repeated_node = true
    return

if repeated_group has LIST or MAP annotation:
    element_node = repeated_node
    element_context = repeated_context
    element_is_repeated_node = true
    return

only_child = repeated_group.field(0)
if only_child.is_repeated:
    element_node = repeated_node
    element_context = repeated_context
    element_is_repeated_node = true
    return

element_node = only_child
element_context = child_context(repeated_context, only_child, 0)
element_is_repeated_node = false
```

`has_structural_list_name()` 对齐 Arrow 的 legacy rule：

```text
name == "array" || name == list_name + "_tuple"
```

## LIST schema build

`build_node_schema()` 的 LIST 分支改为：

```text
resolve_list_element_node(...)

column_schema.kind = LIST
column_schema.definition_level = repeated_context.definition_level
column_schema.repetition_level = repeated_context.repetition_level
column_schema.repeated_repetition_level = repeated_context.repeated_repetition_level

build child schema from resolved element_node and element_context
column_schema.type = nullable_if_needed(DataTypeArray(child.type), list_node)
column_schema.children = [child]
propagate_child_levels(column_schema)
```

### repeated group itself as element

当 element 是 repeated group 本身时，需要注意不要把这个 repeated group 再解释成一层 LIST。

预期效果：

```text
optional group a (LIST) {
  repeated group element {
    optional int32 x;
    optional binary y;
  }
}
```

应构造成：

```text
LIST
  child: STRUCT<x, y>
```

而不是：

```text
LIST
  child: LIST or extra repeated container
```

实现上可以新增一个 internal build mode：

```cpp
enum class SchemaBuildMode {
    NORMAL,
    REPEATED_GROUP_AS_LIST_ELEMENT,
};
```

当 mode 是 `REPEATED_GROUP_AS_LIST_ELEMENT`：

- 当前 repeated group 作为 element 本身构造成 STRUCT 或 annotated logical type。
- 它的 repeated level 已经由 list entry 层消费，不再把 repeated 当作额外 array 层。
- 如果当前 repeated group 是普通 group，则构造成 `STRUCT` element。
- 如果当前 repeated group 带 `LIST` annotation，则继续按 LIST 解析它的 child repeated layer，构造成 nested list element。
- 如果当前 repeated group 带 `MAP` 或 `MAP_KEY_VALUE` annotation，则继续按 MAP 解析它的 child repeated entry layer，构造成 map element。
- 构造当前 element schema 时，不得再次因为“当前节点本身是 repeated”引入隐式 list；只有它内部的 child repeated layer 才能产生下一层 list/map repetition 语义。

如果希望保持改动更小，也可以新增专用函数：

```cpp
Status build_repeated_group_as_list_element_schema(...);
```

该函数至少需要处理 repeated group 作为普通 struct element 的场景；如果选择不用通用 build mode，则还需要显式覆盖 repeated group annotated as LIST/MAP 的场景。

## MAP 兼容规则

对于 outer group annotated as `MAP` 或 legacy `MAP_KEY_VALUE`：

```text
optional group m (MAP) {
  repeated group entries {
    required binary key;
    optional int32 value;
  }
}
```

支持：

- 只有 outer group 带 `MAP` / `MAP_KEY_VALUE` annotation 时，才进入 MAP 兼容解析。
- entry group 名字可以是 `key_value`、`entries` 或其它。
- key/value 字段名不强制必须叫 `key` / `value`。
- 第一个 child 是 key。
- 第二个 child 是 value。
- key 必须 required。
- value 可以 required 或 optional。

不支持：

- outer MAP group 多个 children。
- entry child 非 repeated。
- entry child 是 primitive。
- entry group 没有 value，即 key-only map。
- 没有 repeated entry layer 的 MAP。
- nullable key。

## MAP schema resolver

建议新增 helper：

```cpp
struct MapEntryResolution {
    const parquet::schema::GroupNode* entry_group = nullptr;
    SchemaBuildContext entry_context;
};

Status resolve_map_entry_group(
        const parquet::schema::GroupNode& map_group,
        const SchemaBuildContext& map_context,
        MapEntryResolution* result);
```

Resolver 逻辑：

```text
if map_group.field_count != 1:
    reject

entry_node = map_group.field(0)
if !entry_node.is_repeated:
    reject
if entry_node.is_primitive:
    reject

entry_group = as_group(entry_node)
if entry_group.field_count != 2:
    reject

key_node = entry_group.field(0)
value_node = entry_group.field(1)
if key_node.repetition != REQUIRED:
    reject

entry_context = child_context(map_context, entry_node, 0)
return
```

## MAP schema build

`build_node_schema()` 的 MAP 分支继续输出当前 reader 需要的结构：

```text
MAP
  child[0]: STRUCT(key, value)   // entry / key_value schema
      child[0]: key
      child[1]: value
```

构造流程：

```text
resolve_map_entry_group(...)

column_schema.kind = MAP
column_schema.definition_level = entry_context.definition_level
column_schema.repetition_level = entry_context.repetition_level
column_schema.repeated_repetition_level = entry_context.repeated_repetition_level

build key child from entry_group.field(0)
build value child from entry_group.field(1)

entry_schema.kind = STRUCT
entry_schema.children = [key, value]
entry_schema.type = DataTypeStruct([nullable(key.type), nullable(value.type)], names)

column_schema.type = nullable_if_needed(DataTypeMap(nullable(key.type), nullable(value.type)), map_node)
column_schema.children = [entry_schema]
propagate_child_levels(column_schema)
```

这里保持当前 `MapColumnReader` 的假设：

- `column_schema.children[0]` 是 entry struct。
- `entry.children[0]` 是 key。
- `entry.children[1]` 是 value。

注意：`DataTypeStruct` / `DataTypeMap` 中把 key type 包成 nullable 是 Doris nested column materialization 的内部类型约定，不代表 Parquet nullable key 被支持。Schema resolver 仍必须在 `key_node.repetition != REQUIRED` 时 reject。

## 不支持 key-only map 的原因

Key-only map 可能长这样：

```text
optional group m (MAP) {
  repeated group entries {
    required binary key;
  }
}
```

理论上可以解释为 set-like map 或 `MAP<K, NULL>`，但 Doris `ColumnMap` 需要 keys column 和 values column。

若要支持，需要额外设计：

- synthetic null value schema。
- constant-null value reader。
- `MapColumnReader` value stream 缺失时的特殊路径。

这会改变 reader tree，不属于本次 schema compatibility 的最小范围。因此第一阶段明确 reject。

## 不支持 no-entry MAP 的原因

No-entry MAP 可能长这样：

```text
optional group m (MAP) {
  required binary key;
  optional int32 value;
}
```

它缺少 repeated entry layer，因此没有 repetition level 可以表达多个 map entries，也无法生成 Doris `ColumnMap` offsets。

这不是标准 MAP，也不是 Arrow 主要兼容的 legacy 形态。第一阶段应 reject。

## 对 reader 层的影响

预期不修改 reader 层核心逻辑。

保持：

- `ListColumnReader` 只读取 `column_schema.children[0]` 作为 element reader。
- `MapColumnReader` 只读取 `column_schema.children[0].children[0/1]` 作为 key/value reader。
- `ParquetLeafReader` 只负责 leaf records/levels/values 读取和 batch materialization。
- `nested_column_materializer.*` 只负责 Doris nested Column 构造 helper。

风险点在 LIST repeated group as element：

- 如果该 repeated group 是 struct element，需要确保 schema builder 不把 repeated group 再解释成一个额外 repeated container。
- 这个风险应通过专用 build mode 或专用 helper 解决。

## 错误处理策略

错误信息应明确指出具体 unsupported schema 原因：

- LIST outer group child count invalid。
- LIST child is not repeated。
- LIST repeated group has no child。
- MAP outer group child count invalid。
- MAP entry is not repeated group。
- MAP entry child count is not 2。
- MAP key is nullable。

不要用过于笼统的 `Unsupported parquet LIST encoding` 覆盖所有错误，否则后续排查文件兼容性问题会困难。

## 测试计划

### LIST 正例

1. 标准 3-level LIST：

```text
optional group a (LIST) {
  repeated group list {
    optional int32 element;
  }
}
```

2. Repeated primitive legacy LIST：

```text
optional group a (LIST) {
  repeated int32 element;
}
```

3. Repeated group struct element：

```text
optional group a (LIST) {
  repeated group element {
    optional int32 x;
    optional binary y;
  }
}
```

4. Legacy `array` name：

```text
optional group a (LIST) {
  repeated group array {
    optional int32 item;
  }
}
```

5. Legacy `<parent>_tuple` name：

```text
optional group a (LIST) {
  repeated group a_tuple {
    optional int32 item;
  }
}
```

6. Repeated group annotated as nested LIST：

```text
optional group a (LIST) {
  repeated group array (LIST) {
    repeated int32 array;
  }
}
```

预期解析为 `ARRAY<ARRAY<INT>>`，不要剥掉 `array (LIST)` 这一层。

7. Repeated group annotated as MAP：

```text
optional group a (LIST) {
  repeated group array (MAP) {
    repeated group key_value {
      required binary key;
      optional int32 value;
    }
  }
}
```

预期解析为 `ARRAY<MAP<STRING, INT>>`，不要剥掉 `array (MAP)` 这一层。

8. One-child repeated group whose child is repeated：

```text
optional group a (LIST) {
  repeated group element {
    repeated int32 items;
  }
}
```

预期 repeated group 本身是 struct element，解析为 `ARRAY<STRUCT<items: ARRAY<INT>>>`，不要把 `items` 提升成 list element。

### LIST 反例

1. outer LIST group 多 child。
2. outer LIST child 非 repeated。
3. repeated group 无 child。
4. repeated LIST-annotated outer group，除非它作为 another two-level LIST 的 element 被专门支持。

### MAP 正例

1. 标准 `key_value` entry group。
2. `entries` entry group name。
3. entry group 任意名字，但结构为 repeated group with required key and value。
4. `MAP_KEY_VALUE` legacy converted type。
5. key/value 字段名非 `key`/`value`，但位置正确。

### MAP 反例

1. nullable key。
2. outer MAP group 多 child。
3. entry child 非 repeated。
4. entry child 是 primitive。
5. key-only map。
6. no-entry MAP。

## 实施步骤

1. 在 `parquet_column_schema.cpp` 增加 LIST helper：
   - `has_structural_list_name()`
   - `resolve_list_element_node()`
   - 必要时增加 repeated group as element 的 build helper。
2. 改造 LIST 分支，输出统一 `ParquetColumnSchemaKind::LIST` schema tree。
3. 增加 LIST schema/unit/regression 测试。
   - 覆盖 repeated primitive、multi-field struct element、`array` / `<parent>_tuple` structural name。
   - 覆盖 two-level `List<List<T>>`、two-level `List<Map<K, V>>`、单 child repeated group 且 child repeated 的 struct element。
   - read 测试至少覆盖 null list、empty list、单元素、多元素，验证 def/rep materialization。
4. 增加 MAP helper：
   - `resolve_map_entry_group()`
5. 改造 MAP 分支，放宽 entry group 名字限制，但保持 key/value 结构严格。
6. 增加 MAP schema/unit/regression 测试。
7. 如后续确有需求，再单独设计 key-only map 支持。

## 预期收益

- 支持更多由 Arrow、Spark、Hive、旧 Parquet writer 产生的 LIST/MAP schema。
- 兼容逻辑集中在 schema builder，reader 层保持稳定。
- 为后续 complex parquet reader 的兼容性测试建立清晰边界。
