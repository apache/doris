# Observability / Profile 补充方案

本文档记录 new parquet reader 当前 profile 状态，以及后续需要补齐的指标。

## 当前 New Parquet Profile 状态

new parquet reader 在 `ParquetReader::_init_profile()` 下创建 `ParquetReader` profile 节点，并注册了一批和旧 parquet reader 同名的 counter/timer。需要注意：当前“已注册”和“已有有效值”不是一回事，new parquet 目前只有一部分 counter 有实际更新路径。

### 已注册且已有实际更新

| 类别 | 指标 | 当前更新位置 | 语义 |
|---|---|---|---|
| Row group pruning | `RowGroupsTotalNum` | `ParquetReader::open()` | parquet metadata 中总 row group 数 |
| Row group pruning | `RowGroupsReadNum` | `ParquetReader::open()` | 经过 row group / page index planning 后需要读取的 row group 数 |
| Row group pruning | `RowGroupsFiltered` | `ParquetReader::open()` | `total_row_groups - selected_row_groups` |
| Row group pruning | `RowGroupsFilteredByMinMax` | `ParquetReader::open()` | row group statistics 裁剪的 row group 数 |
| Row group pruning | `RowGroupsFilteredByDictionary` | `ParquetReader::open()` | dictionary page 裁剪的 row group 数 |
| Row group pruning | `RowGroupsFilteredByBloomFilter` | `ParquetReader::open()` | bloom filter 裁剪的 row group 数 |
| Row range/page-index planning | `SelectedRowRanges` | `ParquetReader::open()` | page index planning 后保留的 row range 数 |
| Row range/page-index planning | `FilteredRowsByGroup` | `ParquetReader::open()` | row group 级 pruning 跳过的行数 |
| Row range/page-index planning | `FilteredRowsByPage` | `ParquetReader::open()` | page index planning 认为可跳过的行数；这是 planner 结果，不等同于 Arrow callback 实际跳过 page |
| Page index | `PageIndexReadCalls` | `ParquetReader::open()` | 尝试读取 page index 的次数 |
| Bloom filter | `BloomFilterReadTime` | `parquet_statistics.cpp` | 读取 row group bloom filter 的耗时 |
| Runtime page skip | `PagesSkippedByFilter` | `PageReader::set_data_page_filter()` callback | Arrow data page filter callback 实际返回 skip 的 page 数 |
| Runtime page skip | `PageSkipBytes` | `PageReader::set_data_page_filter()` callback | 实际 skip page 的 compressed bytes，来自 OffsetIndex `compressed_page_size` |

`FilteredRowsByPage` 和 `PagesSkippedByFilter` 的含义不同：

- `FilteredRowsByPage` 是 planner 根据 page index row ranges 计算出来的可过滤行数。
- `PagesSkippedByFilter` 是 Arrow `PageReader` 在读数据页时真正调用 callback 并返回 skip 的次数。
- `PageSkipBytes` 和 `PagesSkippedByFilter` 使用同一个 page ordinal，从 `ParquetPageSkipPlan` 中取 OffsetIndex compressed size，不依赖 Arrow `DataPageStats` 重新判断。

### 已注册但当前 New Parquet 基本未有效填充

这些 counter/timer 已经在 `ParquetReader::_init_profile()` 创建，但 new parquet 当前没有完整更新路径，查询 profile 中大概率为 0 或无业务含义。它们主要是从旧 parquet profile 结构迁移过来的占位。

| 类别 | 指标 | 当前缺口 |
|---|---|---|
| File lifecycle | `FileReaderCreateTime`、`FileNum` | `FileReader::init()` 会维护 `_reader_statistics`，但 new parquet 没有把它 publish 到 profile counter |
| Metadata/footer | `ParseMetaTime`、`ParseFooterTime`、`FileFooterReadCalls`、`FileFooterHitCache` | new parquet 直接使用 Arrow `ParquetFileReader`，尚未在 metadata/footer 路径接入这些 timer/counter |
| Row read | `RawRowsRead` | `ParquetScanScheduler::get_block()` / `read_next_batch()` 没有累计实际输出或读取行数 |
| Column read | `ColumnReadTime` | `ParquetColumnReader::read/select/skip` 和 Arrow `RecordReader::ReadRecords()` 没有接入 timer |
| Predicate execution | `PredicateFilterTime` | `execute_batch_filters()` 没有单独计时 |
| Lazy materialization | `FilteredRowsByLazyRead`、`FilteredBytes` | new parquet 现在没有旧 reader 的 lazy materialization 统计路径 |
| Dictionary rewrite | `DictFilterRewriteTime` | new parquet 当前 dictionary pruning 发生在 planning，未接入 rewrite timer |
| Convert/materialization | `ConvertTime` | `arrow_leaf_reader_adapter.cpp` 的 Doris column 写入/转换没有接入 timer |
| Page index timing | `PageIndexFilterTime`、`PageIndexReadTime`、`PageIndexParseTime`、`RowGroupFilterTime` | 当前只统计 `PageIndexReadCalls`；row group pruning/page index select 没有细分耗时 |
| Page IO/decode | `PageReadCount`、`DecompressTime`、`DecompressCount`、`PageHeaderDecodeTime`、`PageHeaderReadTime`、`DecodeValueTime`、`DecodeDictTime`、`DecodeLevelTime`、`DecodeNullMapTime`、`SkipPageHeaderNum`、`ParsePageHeaderNum` | new parquet 通过 Arrow internal `RecordReader` 读 page，没有 Doris 自己的 page reader 统计 |
| Doris page cache | `PageCacheHitCount`、`PageCacheMissingCount`、`PageCacheWriteCount`、`PageCacheCompressedHitCount`、`PageCacheDecompressedHitCount`、`PageCacheCompressedWriteCount`、`PageCacheDecompressedWriteCount` | new parquet 当前没有复用旧 parquet 的 Doris page cache reader，因此这些计数没有有效来源 |

## 需要补充

### 1. Scheduler / Scan 批次级指标

| 指标 | 说明 | 用途 |
|---|---|---|
| `RawRowsRead` | `get_block()` 最终返回的行数累计 | 和旧 parquet profile 对齐，作为 scan 输出基准 |
| `TotalBatches` | scheduler 处理的 batch 总数 | 评估 batch 粒度 |
| `SelectedRows` | expression/delete filter 后 selected rows 累计 | 了解 filter 选择性 |
| `RowsFilteredByConjunct` | batch filter 过滤掉的行数 | 区分 predicate 过滤和 page index pruning |
| `EmptySelectionBatches` | filter 后 selected rows 为 0 的 batch 数 | 发现只读 predicate 列但输出为空的情况 |
| `RangeGapSkippedRows` | page-index selected row ranges 之间通过 `skip_current_row_group_rows()` 跳过的行数 | 验证 row-range pruning 是否减少 reader 推进量 |

实现位置：`parquet_scan.cpp` 的 `read_next_batch()`、`read_current_row_group_batch()`、`skip_current_row_group_rows()`。

### 2. Reader / Column Reader 级指标

| 指标 | 说明 | 用途 |
|---|---|---|
| `ReaderReadRows` | reader tree `read()` 累计请求行数 | reader 实际物化路径基准 |
| `ReaderSkipRows` | reader tree `skip()` 累计行数 | 验证 row-range pruning 和 selection 路径是否真的走 skip |
| `ReaderSelectRows` | reader tree `select()` 累计 selected rows | 衡量 late materialization / selection 读路径占比 |
| `ArrowReadRecordsTime` | `RecordReader::ReadRecords()` 耗时 | 观察 Arrow page read/decode 总成本 |
| `MaterializationTime` | `append_leaf_values()` / nested assembler 写 Doris column 的耗时 | 区分 Arrow 解码和 Doris 列写入 |

实现位置：

- `ParquetColumnReader::skip()` / `select()` 和各子类 `read()`。
- `arrow_leaf_reader_adapter.cpp::read_leaf_records()`。
- `arrow_leaf_reader_adapter.cpp::append_leaf_values()`。

### 3. Page Index / Row Group Planning 计时

| 指标 | 说明 | 用途 |
|---|---|---|
| `RowGroupFilterTime` | row group statistics / dictionary / bloom filter planning 总耗时 | 评估 pruning planning 开销 |
| `PageIndexFilterTime` | page index range select + page skip plan 生成耗时 | 评估 page-index pruning 开销 |
| `PageIndexReadTime` | Arrow `GetPageIndexReader()` / `RowGroup()` 读取耗时 | 区分 page index I/O 和过滤计算 |
| `PageIndexParseTime` | 如果 Arrow API 可拆分，统计 page index 解析耗时 | 对齐旧 profile；拆不开时不要伪造 |

实现位置：`plan_parquet_row_groups()`、`select_row_groups_by_statistics()`、`select_row_group_ranges_by_page_index()`。

### 4. Nested Assembler 级指标

| 指标 | 说明 | 用途 |
|---|---|---|
| `NestedOverflowCount` | overflow 发生次数 | 评估 batch size 是否合适 |
| `NestedOverflowTailRows` | overflow tail 累计 rows | 评估 overflow 携带的数据量 |
| `NestedLevelSlotsTotal` | 累计处理的 level slots | 复杂类型 assembler 基准 |

实现位置：`list_column_reader.cpp`、`map_column_reader.cpp`、`nested_column_reader.cpp` 的 overflow 和 level loop 路径。

### 5. Page-level Skip

已实现：

| 指标 | 说明 | 用途 |
|---|---|---|
| `PagesSkippedByFilter` | data page filter callback 实际跳过的 page 数 | 验证 page-level skip 实际命中率 |
| `PageSkipBytes` | 跳过的 compressed bytes 累计，来自 OffsetIndex `compressed_page_size` | 估算 page-level skip 节省的 I/O/解压量 |

后续可以补充：

| 指标 | 说明 | 用途 |
|---|---|---|
| `PagesSeenByFilter` | callback 被调用的 data page 数 | 计算 skip page ratio |
| `PageFilterRowsSkipped` | runtime callback 对应 page 的行数累计 | 与 `FilteredRowsByPage` 做 planner/runtime 对照 |
| `PageFilterColumns` | 生成 page skip plan 的 leaf column 数 | 判断 page skip 覆盖面 |

## 实施优先级

1. 先补 scheduler/read path：`RawRowsRead`、batch、selected rows、range gap skipped rows。这些指标不依赖 Arrow 内部实现，风险最低。
2. 再补 column reader / adapter timer：`ArrowReadRecordsTime`、`MaterializationTime`、`ReaderReadRows/SkipRows/SelectRows`。这能回答 page skip 是否减少了 Arrow read/decode 成本。
3. 补 planning timer：`RowGroupFilterTime`、`PageIndexFilterTime`、`PageIndexReadTime`。这能判断 page index pruning 自身是否过重。
4. nested assembler 指标跟 LIST/MAP/repeated leaf pruning 一起补，避免为了 profile 单独改复杂类型路径。
5. `Decompress*`、`Decode*`、`PageHeader*`、`PageCache*` 这些旧 parquet page-reader 级指标，不建议先伪造。除非 Arrow 能暴露等价 stats，或者 new parquet 后续接入 Doris 自己的 page reader，否则应保持为未接线状态或从 profile 中移出。

## 验证

1. page-index pruning 场景：确认 `FilteredRowsByPage > 0`、`PagesSkippedByFilter > 0`、`PageSkipBytes > 0`。
2. predicate 过滤场景：确认 `SelectedRows + RowsFilteredByConjunct` 和 batch 输入行数一致。
3. row-range pruning 场景：确认 `RangeGapSkippedRows` 和 selected ranges 之间的 gap 行数一致。
4. 复杂类型场景：确认 `NestedOverflowCount`、`NestedLevelSlotsTotal` 随 LIST/MAP 数据量增长。
5. 无 page index 或无命中场景：确认 `PagesSkippedByFilter == 0`，但其他 scan/read counters 仍有合理值。
