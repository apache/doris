# Parquet Scan UT 规划

## 测试范围

扫描调度和裁剪的完整链路：`plan_parquet_row_groups()`、statistics 裁剪、page index 裁剪、`ParquetScanScheduler` 调度编排。

需要写 Parquet 文件（多 RowGroup），通过完整流程验证。

注意: `row_group_prune_reason()`、`check_statistics()`、`intersect_ranges()`、
`build_page_skip_plans()` 等 helper 位于 `.cpp` 匿名 namespace。UT 以公开入口
`TransformColumnStatistics()`、`BloomFilterExcludes()`、`select_row_groups_by_statistics()`、
`select_row_group_ranges_by_page_index()`、`plan_parquet_row_groups()`、`ParquetReader` /
`ParquetScanScheduler` 间接覆盖为主；若必须精确断言 helper 行为，再考虑抽出可测试接口。

## 一、plan_parquet_row_groups — 裁剪流水线

### 1.1 三级流水线正确性

```
all_rg_selected          无 filter → 全部 RG 被选中
statistics_filtered      部分 RG 被 min/max 裁剪
scan_range_filtered      部分 RG 在 scan range 外被过滤
                         验证: scan_range 过滤先于 statistics（代价从低到高）
page_index_filtered      部分 RG 内 page 被裁剪 → selected_ranges 非连续
page_index_fully_filtered 整个 RG 被 page index 完全裁剪 → 不进入 plan
multi_filter_intersection 多个 column_filter 的 page range 取交集
```

### 1.2 first_file_row 计算

```
first_file_row            跨多 RG 的 first_file_row 正确累加
                          RG0: rows=1000 → RG1: first_file_row=1000 → RG2: first_file_row=2000
                          裁剪后跳过中间 RG, first_file_row 仍然按原始顺序正确
```

## 二、Statistics 裁剪

### 2.1 TransformColumnStatistics

```
min_max_normal           正常 min/max 转换 → ParquetColumnStatistics
min_max_decimal          DECIMAL 列 → min/max 按 type_descriptor 转 Doris Field
min_max_unsigned         UINT32/UINT64 → min/max unsigned 转 signed 同宽度
min_max_timestamp        TIMESTAMP → min/max 的 timezone 处理
min_max_string           STRING 列 → min/max 转 StringRef
no_statistics            无 statistics → has_min_max=false
null_count               null_count 正确传递
```

### 2.2 check_statistics

```
exclude_by_min_max       filter id>500, RG max=300 → 可以裁剪
not_exclude_by_min_max   filter id>100, RG max=300 → 不能裁剪
null_predicate           IS_NULL / IS_NOT_NULL → has_null_count 参与判断
multi_predicates         多个 predicate → 任一命中裁剪即可
```

### 2.3 dictionary pruning

```
dictionary_exclude       EQ predicate，字典中无该值 → 可以裁剪
dictionary_not_exclude   EQ predicate，字典中有该值 → 不能裁剪
dictionary_in_list       IN_LIST predicate → 任一值在字典中则不能裁剪
not_dictionary_encoded   非 dictionary encoding → 跳过 dictionary pruning
not_supported_type      非 string-like 类型 → 跳过
not_supported_predicate  非 EQ/IN_LIST → 跳过
```

### 2.4 bloom filter pruning

```
bloom_exclude            predicate 值不在 bloom filter 中 → 可以裁剪
bloom_not_exclude        predicate 值可能在 bloom filter 中 → 不能裁剪
bloom_not_available      文件无 bloom filter → 跳过
bloom_unsupported_type  非 BOOL/INT/BIGINT/FLOAT/DOUBLE/STRING → 跳过
```

### 2.5 row_group_prune_reason 流水线

```
statistics_first         先查 statistics → 命中则不再查 dictionary/bloom
dictionary_after         statistics 未命中 → 查 dictionary → 命中则不再查 bloom
bloom_last               statistics 和 dictionary 都未命中 → 查 bloom
none_all                 全部未命中 → NONE
```

## 三、Page Index 裁剪

### 3.1 select_row_group_ranges_by_page_index

```
full_range_no_filter     无 page index 可用 → selected_ranges = [0, row_group_rows]
page_index_disabled      config 关闭 → 跳过
single_filter_ranges     单个 column_filter → 对应的 selected_ranges
multi_filter_intersect   多个 column_filter → ranges 取交集
fully_filtered           交集为空 → selected_ranges 为空，RG 被完全裁剪
```

### 3.2 build_page_skip_plans

```
skip_plan_generated      不在 selected_ranges 内的 page → skip plan 中标记
skip_plan_empty          全部 page 都在 selected_ranges 内 → empty skip plan
skip_plan_compressed_size 被跳过的 page 的 compressed_size 记录正确
repeated_column           repeated leaf → 跳过（OffsetIndex 不准确）
```

### 3.3 辅助函数

```
intersect_ranges         交集计算: [0,5]∩[3,8]=[3,5]
                         多段交集: [0,4][6,10]∩[2,8]=[2,4][6,8]
page_row_range           单 page / 最后 page 的 row range 计算
append_row_range         相邻 range 合并, 不相邻单独追加
```

这些 helper 目前不是公开符号。优先通过 `select_row_group_ranges_by_page_index()`
构造 page index 文件间接验证:

```
intersect_ranges         用多个 column_filter 得到非连续交集
append_row_range         用相邻保留 page 验证 selected_ranges 合并
page_row_range           用最后 page 行数不同的文件验证 range end=row_group_rows
```

## 四、ParquetScanScheduler

### 4.1 open_next_row_group

```
factory_created           创建 ParquetColumnReaderFactory
predicate_readers         为 predicate_columns 创建 reader
non_predicate_readers     为 non_predicate_columns 创建 reader
row_position_reader       虚拟列 RowPosition 正确创建
global_rowid_reader       虚拟列 GlobalRowId 正确创建
page_skip_plans_passed     page_skip_plans 正确传入 factory
no_more_rg                所有 RG 读完 → has_row_group=false
```

### 4.2 read_current_row_group_batch — Late Materialization

```
full_hit                  全部行命中 → non-predicate 走 read()
partial_hit               部分行命中 → non-predicate 走 select()
all_filtered              selected_rows=0 → 继续下一批
predicate_first            predicate 列在全量读取后执行 conjuncts
selection_correct          SelectionVector 索引正确
filter_predicate_columns   部分命中时 predicate 列被 filter
```

### 4.3 read_filter_columns

```
conjuncts_executed        predicate 列读取后 → 执行 conjuncts
delete_conjuncts_executed conjuncts 后 → 执行 delete_conjuncts
selection_generated       生成 SelectionVector
filter_all_rows           全过滤 → can_filter_all=true
```

### 4.4 execute_batch_filters

```
conjuncts_only            只有 conjuncts
delete_conjuncts_only     只有 delete_conjuncts
both_conjuncts            两者都有 → conjuncts 先执行
no_filter                 两者都为空 → 不修改 selection；调用方保持 selected_rows=batch_rows
```

### 4.5 range gap 跳过

```
gap_skipped               跳过 selected_ranges 之间的 gap
no_gap                    连续 selected_ranges → 无跳过
skip_readers_consistent   predicate 和 non-predicate reader 同步 skip
```

### 4.6 调度主循环

```
single_rg_single_batch    1 RG, 1 batch → 直接返回
single_rg_multi_batch     1 RG, 多 batch → 游标连续
multi_rg_single_batch     多 RG, 各 1 batch → RG 切换正确
multi_rg_multi_batch      多 RG, 各多 batch → 完整循环
empty_plan                 空 plan → 直接 eof
batch_fully_filtered      当前 batch 全部被 filter → 继续循环下一批
batch_size_limit          range length > DEFAULT_PARQUET_READ_BATCH_SIZE → 多 batch 连续读取
no_requested_columns      predicate/non-predicate 都为空 → 只返回 rows，不物化列
```

## 五、端到端耦合路径

### 5.1 page skip plan 全链路

```
plan → page_skip_plans → factory::create → page_skip_plan* 
     → ScalarColumnReader::skip → page_filtered_rows_to_skip
     
验证: page index 裁剪后, ScalarColumnReader 实际跳过了对应 page 的行
```

### 5.2 late materialization 全链路

```
predicate reader::read → execute_batch_filters → SelectionVector
     → filter predicate columns → non-predicate reader::select
     
验证: select 后列值与全量 read 后 filter 一致
```

### 5.3 嵌套 + selection 联合

```
含 STRUCT/LIST/MAP 的复杂列 → select() 部分读取
验证: select 后嵌套结构完整性（offsets/null_map/子列值）
```

### 5.4 虚拟列全链路

```
row_position_scan_range   多 RowGroup + scan range → row position 仍为 file-local position
global_rowid_full_read    GlobalRowIdContext → schema 追加 global rowid，读取 17-byte RowId
global_rowid_selection    predicate 过滤后 global rowid 与保留行 file-local position 对齐
global_rowid_scan_range   只扫中间 RowGroup → global rowid row_id 从 first_file_row 开始
```

## 六、Aggregate Pushdown

`ParquetReader::get_aggregate_result()` 是 reader 顶层能力，单独覆盖，不混入普通 scan batch UT。

```
count_all_selected        COUNT 返回已选中 RowGroup 的总行数
count_after_pruning       先 open() 触发 row group pruning，再 COUNT，只统计 scan_plan 中 RG
count_page_index_note     当前 COUNT 按选中 RowGroup 的 num_rows 累加，不按 page index
                          selected_ranges 裁掉的行数扣减；测试预期应与当前语义一致
minmax_single_column      MINMAX 从 row group statistics 汇总 min/max
minmax_multi_row_group    多 RG min/max 取全局最小/最大
minmax_after_pruning      被 statistics/scan_range 裁掉的 RG 不参与 min/max
minmax_nested_leaf        STRUCT 单 leaf projection 可下推 min/max
minmax_repeated_rejected  repeated leaf/list/map projection → NotSupported
missing_statistics        某 RG 缺 min/max statistics → NotSupported
empty_scan_plan_minmax    无选中 RG → NotSupported
invalid_agg_type          非 COUNT/MINMAX → NotSupported
invalid_column_id         projection local id 越界 → InvalidArgument
invalid_projection        projection 指向不存在 child / 多 leaf nested path → InvalidArgument/NotSupported
```

## 七、Profile / Counters

```
pruning_counters          RowGroupsTotalNum / RowGroupsReadNum / RowGroupsFiltered 等与 plan 一致
page_index_counters       FilteredRowsByPage / SelectedRowRanges / PageIndexReadCalls 正确
late_materialize_counters RawRowsRead / SelectedRows / RowsFilteredByConjunct /
                          FilteredRowsByLazyRead / EmptySelectionBatches 正确
range_gap_counters        RangeGapSkippedRows 与 selected_ranges gap 一致
reader_counters           ReaderReadRows / ReaderSkipRows / ReaderSelectRows 基本正确
```

## 八、测试数据要求

```
需要多 RowGroup 文件:
  - 至少 3 个 RG, 每个有 min/max statistics 差距
  - 部分 RG 带 bloom filter
  - 部分 RG 带 page index
  - 部分 RG 带 dictionary encoding

需要 predicate 场景:
  - 有 column_predicate_filters
  - 有 conjuncts
  - 有 delete_conjuncts

需要 scan_range 场景:
  - scan_range 只覆盖部分 RG

需要 aggregate 场景:
  - 有完整 min/max statistics 的多 RowGroup 文件
  - 至少一个缺失 statistics 的文件或人工 metadata 场景
  - 一个含 nested primitive leaf 的 STRUCT 文件
  - 一个 repeated/list/map 文件用于验证 rejected path
```

## 九、测试文件组织

```
parquet_scan_test.cpp

分为三类测试:

1. Statistics 测试 — 构造 schema + 统计数据, 调用 TransformColumnStatistics / 
   select_row_groups_by_statistics 验证裁剪结果
   （部分可纯内存，部分需要写有统计数据的 Parquet 文件）
   row_group_prune_reason 为匿名 namespace helper，不直接调用，除非后续抽出可测试接口

2. Scheduler 测试 — 写多 RG Parquet 文件, 走完整流程:
   plan_parquet_row_groups → 初始化 Scheduler → read_next_batch 循环验证

3. Aggregate 测试 — 通过 ParquetReader::open() 生成 scan_plan 后调用:
   get_aggregate_result(COUNT/MINMAX) → 验证结果和错误路径
```
