# Column Reader UT 规划

## 测试范围

只测**控制流逻辑**，不测类型转换（那是 Type UT 和 Serde UT 的责任）。

用 INT32 + STRING + 一个简单嵌套类型（如 `STRUCT<INT, STRING>`）即可驱动全部路径。

## 一、ScalarColumnReader

### 1.1 read() — 基本读取

```
read_all_rows            一次 read 全部行，验证 rows_read 和 column size
read_multiple_batches    分多批 read，验证跨批游标连续、值顺序正确
read_with_nulls          optional 列含 NULL，验证 null_map + 非 NULL 值正确
read_all_nulls           全部 NULL，验证全 null_map=1
read_required_no_null    required 列（max_dl=0），验证 build_null_map 短路
```

### 1.2 skip() — 跳过

```
skip_zero                skip(0)，空操作
skip_some_then_read      skip N 行后 read M 行，验证读到的值与原始数据对齐
skip_all                 跳过全部行，read 返回 0 行
skip_with_nulls          含 NULL 行时 skip，验证游标推进
skip_page_filtered       page skip plan 命中时，验证 page_filtered + record_skip 合计正确
```

### 1.3 select() — 部分读取

```
select_all               selected_rows == batch_rows，结果与 read 一致
select_disjoint          不连续行 [0, 2, 4]
select_single            单选首行 / 中间行 / 末行
select_first_last        选首行+末行，中间全部跳过
select_zero              选中 0 行
select_then_read         select 后再 read 下一批，游标正确
select_with_nulls        含 NULL 行时 select
```

### 1.4 嵌套协议

```
load_nested_non_nullable  non-nullable leaf: materialized_slot_dl == _definition_level
load_nested_nullable      nullable leaf: materialized_slot_dl == _definition_level - 1
value_layout_levels       values_written == levels_written → LEVELS
value_layout_slots        values_written == value_slot_count → VALUE_SLOTS
value_layout_leaf_values  values_written == leaf_value_count → LEAF_VALUES
value_layout_payload      Arrow 为 NULL 祖先写 placeholder → PAYLOAD_VALUE_SLOTS 降级
value_layout_mismatch     以上均不匹配 → Corruption
build_nested_values       有值 slot → append_scalar_batch_value
build_nested_nulls        NULL slot → insert_default
build_nested_skip_levels  def < slot_dl 或 rep > _rep_level 跳过
cursor_across_batches     load→build 两批，游标连续
```

`ParquetLeafReader::read_nested_batch()` 的 value layout 是嵌套 reader 的高风险核心。
如果真实 Parquet 文件难以稳定构造某种 Arrow RecordReader 输出布局，可以加 test-only
helper/friend 注入 `ParquetLeafBatch`，但仍需保留至少一个真实嵌套文件端到端用例。

## 二、StructColumnReader

```
shape_source              选择第一个非 repeated child 作为 shape source
parent_nulls              解析: def_level < nullable_dl → parent NULL
null_parent_scalar_child  NULL 父行下 Scalar child cursor 推进到 parent_level_idx + 1
null_parent_complex_child NULL 父行下复杂 child skip_nested_column(1)
present_rows_batch        连续 present 父行合并为一次 build 调用
partial_projection        child_output_indices 映射正确
all_children_repeated     shape source 退回第一个 child
```

## 三、ListColumnReader

```
offsets_from_rep          rep < _rep_level→新行, rep==_rep_level→继续, 累计 entry_count
empty_array               entry_count=0, offset 不变
null_array                def < _dl-1, parent NULL → null_map
null_element              element 含 NULL ✅
build_element             element build_nested_column(total_entries)
```

## 四、MapColumnReader

```
offsets_from_key          key stream 提供 shape（同 LIST 逻辑）
empty_map                 entry_count=0
null_map_parent           parent NULL
null_key_rejected         key=NULL 数据 → 报错
scalar_value_path         逐个 entry 调用 append_nested_value
                         key/value rep level 对齐校验
complex_value_path        递归 build_nested_column
key_always_full           key projection 强制 nullptr
```

## 五、虚拟列 Reader

```
row_position_read         read() → ColumnInt64: first_row + position 递增
row_position_skip         skip() → _next_row_position += rows
global_rowid_read         read() → ColumnString: 17-byte RowId 编码
global_rowid_skip         skip() → _next_row_position += rows
global_rowid_select       predicate 过滤后 select() → row_id 与 file-local row position 对齐
global_rowid_scan_range   只扫描中间 RowGroup → row_id 从 first_file_row 开始
global_rowid_schema       get_schema() 在 GlobalRowIdContext 存在时追加虚拟列定义
global_rowid_invalid_args read(nullptr / rows_read=nullptr / rows<0) → InvalidArgument
```

## 六、投影

```
struct_partial            只读部分子字段，重建 DataTypeStruct ✅
struct_with_nulls         投影 + NULL struct 父行
list_partial              element 部分读 ✅
list_with_nulls           投影 + NULL/空 list
map_partial               value 部分读 ✅
map_key_with_value        projection 中包含 key+value 时允许，实际仍强制完整 key stream
map_reject_key_only       拒绝 key-only 投影 ✅
map_with_nulls            投影 + NULL/空 map
```

## 七、Chunk Overflow

```
一层嵌套: LIST/MAP/STRUCT overflow → 游标连续性 ✅
两层嵌套: STRUCT<LIST> / LIST<STRUCT> / MAP<LIST> overflow ✅
深层嵌套: LIST<STRUCT<MAP<LIST>>> overflow ✅
注意: 不需要为每种类型组合单独测，3层覆盖所有模式
```

## 八、基类 & 辅助

```
base_skip                 ParquetColumnReader::skip() → NotSupported
base_load_nested          ParquetColumnReader::load_nested_batch() → NotSupported
base_build_nested         ParquetColumnReader::build_nested_column() → NotSupported
base_select_default       SelectionVector → ranges → skip+read+skip
base_skip_nested          build → scratch → 验证行数
selection_to_ranges       连续/不连续/空
selection_identity        未绑定 data 时 get_index(i)==i，selection_to_ranges 正确
selection_external_buffer initialize(data,count) 后不拥有 buffer，索引正确
selection_verify_negative count > batch_rows / count > vector size / index 越界 / 非严格递增
append_offsets            [3,0,2] → [3,3,5]
append_parent_nulls       dst nullptr 短路 / 正常追加
```

## 九、Factory 错误路径

```
invalid_leaf_id           leaf_column_id < 0 或 >= num_leaf_columns → InvalidArgument
null_descriptor           descriptor=nullptr → InvalidArgument
flat_scalar_nested_levels top-level scalar 但 max_rl!=0 或 max_dl>1 → NotSupported
nested_scalar_projection  对 scalar leaf 传 partial projection → InvalidArgument
struct_invalid_projection projection child 不存在 → InvalidArgument
struct_empty_projection   partial projection 不含任何有效 child → NotSupported
list_empty_projection     LIST partial projection 不含 element → NotSupported
map_invalid_projection    MAP projection 含非 key/value child → InvalidArgument
map_key_only_projection   只投影 key、不投影 value → NotSupported
```

## 十、测试数据

用一个 fixture 生成共享的 Parquet 文件，包含所有需要的场景：

```
columns:
  required_int32       — read/skip/select 基础
  optional_int32       — NULL 行
  required_string      — 验证多类型基本路径
  optional_struct      — NULL struct + null child
  required_list_int    — 空/NULL 数组 + NULL 元素
  optional_list_int    — NULL list
  required_map_str_int — 空/NULL map + NULL value
  optional_map_str_int — NULL map
  nested_struct        — STRUCT<LIST, MAP> 多层嵌套
  每个嵌套类型都设 row_count 足够大（>100）以触发 chunk overflow
```

补充说明:

```
page skip plan            只构造整页过滤导致的 skipped_ranges；ScalarColumnReader 对
                          部分页交叠有 DORIS_CHECK，测试不要制造非法部分交叠
dense nullable            当前 factory 默认 read_dense_for_nullable=false，本文件只做普通
                          read/skip/select；dense 展开归 Serde/LeafReader helper 覆盖
```
