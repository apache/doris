# Parquet Schema UT 规划

## 测试范围

`build_parquet_column_schema()` 及其内部函数：物理 schema → 语义 schema 树的转换。

不需要写 Parquet 文件。直接用 Arrow API 构造 `SchemaDescriptor`，调用 `build_parquet_column_schema()` 后验证输出树。

## 一、PRIMITIVE 节点

```
required_primitive     INT32 required → kind=PRIMITIVE, leaf_column_id=有效值, descriptor!=null
optional_primitive     INT32 optional → type 为 Nullable, nullable_definition_level 正确
logical_type           TIMESTAMP(MICROS) → type_descriptor 中 type/extra/time_unit 正确
converted_type         INT_8 → type_descriptor 正确
physical_fallback      无 annotation → 物理兜底正确
```

## 二、STRUCT 节点

```
required_struct         required group {int32, string} → kind=STRUCT, leaf=-1
                        children 数量和类型正确
                        DataTypeStruct 的 child type 全部 make_nullable
optional_struct         optional group → type 被 nullable_if_needed
struct_children_levels  children 的 definition_level 按 optional 逐层递增
```

## 三、LIST 节点

```
list_3level_standard    LIST{repeated group list{optional int32 item}}
                        → kind=LIST, wrapper "list" 被折叠
                        → children = [item], type=Array(Nullable(Int32))
list_2level_legacy      LIST{repeated int32 item}
                        → kind=LIST, element 就是 repeated 本身
                        → children = [item]
list_compat_array       repeated group 名为 "array"
                        → 不 strip wrapper, group 本身作为 STRUCT element
list_compat_tuple       repeated group 名为 "<name>_tuple"
                        → 不 strip wrapper
list_struct_element     repeated group 有多个 children
                        → group 本身作为 STRUCT element
list_logical_annotation repeated group 带 logical annotation
                        → 不 strip wrapper
list_nested_repeated    wrapper 内唯一的 child 也是 repeated
                        → 不 strip wrapper (保留嵌套 LIST)
list_repeated_level     wrapper 被折叠后, MAP/LIST levels 继承自 repeated_context
list_nullable_level     LIST itself optional → nullable_definition_level 正确
list_rejected           不合法 layout → 返回 error
                        - LIST group field_count != 1
                        - LIST 唯一 child 不是 repeated
                        - repeated group field_count == 0
                        - top-level repeated LIST annotation
```

## 四、MAP 节点

```
map_standard            MAP{repeated group key_value{required key, optional value}}
                        → kind=MAP, wrapper "key_value" 被折叠
                        → children = [key, value]
                        → type=Map(Nullable(key_type), Nullable(value_type))
map_optional_key        key 字段标记为 optional (Hive 兼容)
                        → schema 构建成功, key type 为 Nullable
map_rejected            不合法 layout → 返回 error
                        - field_count != 1
                        - entry 不是 repeated
                        - entry 是 primitive
                        - entry group field_count != 2
                        - top-level repeated MAP annotation
map_levels              key/value children 的 levels 继承自 entry_context
```

## 五、裸 repeated 自动 wrap

```
bare_repeated           repeated int32 items (无 LIST annotation)
                        → 自动 wrap 为 LIST, kind=LIST
bare_repeated_group     repeated group with struct fields
                        → wrap 为 LIST<STRUCT>
inside_list_element     LIST 内部的 nested repeated child
                        → REPEATED_NODE_AS_LIST_ELEMENT mode
                        → 只 wrap 嵌套的 repeated, 不重复 wrap
```

## 六、Dremel Levels 计算

```
child_context           测试逐层累加逻辑:
                        optional → def_level++, nullable_dl = def_level
                        repeated → def_level++, rep_level++
                        repeated → repeated_rep_level = rep_level, repeated_ancestor_dl = def_level
propagate_levels        复杂节点 max_dl/max_rl = max(children)
levels_chain            多层嵌套的 level 链:
                        root→optional struct(dl=1)→optional child(dl=2)→repeated(dl=3,rl=1)
                        验证每层 levels 正确
```

## 七、SchemaBuildContext 完整性

```
inherit_state           local_id / parquet_field_id / name 正确赋值
                        max_dl / max_rl 最初设为 context 的累计值
                        PRIMITIVE 节点被 ColumnDescriptor 的值覆盖
nullable_children       所有复杂类型 children type 被 make_nullable
                        需区分:
                        - STRUCT type 内部 child type 必须 Nullable
                        - MAP type 内部 key/value type 必须 Nullable
                        - LIST type 使用 element child->type, element 是否 Nullable 由 schema 决定
                        - ParquetColumnSchema::children[i]->type 是子节点自身类型, 不等同于父容器内部声明
```

## 八、入口错误与边界

```
null_fields_pointer     build_parquet_column_schema(schema, nullptr) → InvalidArgument
empty_root              root 无 children → 返回空 fields
unsupported_primitive   primitive 解析后 doris_type=nullptr → NotSupported
leaf_column_not_found   SchemaDescriptor 无法解析 leaf_column_id → InvalidArgument（如可构造）
field_id_preserved      parquet field_id 正确写入 ParquetColumnSchema::parquet_field_id
converted_time_rejected ConvertedType::TIME_MILLIS/TIME_MICROS → NotSupported
logical_time_utc_rejected Logical TIME(isAdjustedToUTC=true) → NotSupported
```

## 九、测试文件组织

```
parquet_schema_test.cpp

每个 test case:
  1. 用 PrimitiveNode::Make() / GroupNode::Make() 构造 schema 树
  2. 构造 SchemaDescriptor
  3. build_parquet_column_schema()
  4. 验证 ParquetColumnSchema 树的 kind / leaf_id / levels / children / type

不需要写 Parquet 文件，纯内存操作。
```
