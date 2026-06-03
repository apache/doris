# Observability / Profile 补充方案

## 当前已有

已接入 profile 的指标（`parquet_reader.cpp::_init_profile()`）：

| 类别 | 指标 |
|---|---|
| Row group | `RowGroupsTotalNum`、`RowGroupsReadNum`、`RowGroupsFiltered`、`RowGroupsFilteredByMinMax`、`RowGroupsFilteredByDictionary`、`RowGroupsFilteredByBloomFilter` |
| Row/page | `FilteredRowsByGroup`、`FilteredRowsByPage`、`FilteredRowsByLazyRead`、`SelectedRowRanges` |
| I/O | `PageIndexReadCalls`、`PageIndexReadTime`、`PageIndexParseTime`、`FileFooterReadCalls`、`FileFooterHitCache` |
| Decode | `ColumnReadTime`、`DecompressTime`、`DecompressCount`、`DecodeValueTime`、`DecodeDictTime`、`DecodeLevelTime`、`DecodeNullMapTime`、`PageHeaderDecodeTime`、`PageHeaderReadTime` |
| Cache | `PageReadCount`、`PageCacheHitCount`、`PageCacheMissingCount`、`PageCacheWriteCount`、`PageCacheCompressedHitCount`、`PageCacheDecompressedHitCount`、`PageCacheCompressedWriteCount`、`PageCacheDecompressedWriteCount` |
| Filter | `PredicateFilterTime`、`DictFilterRewriteTime`、`BloomFilterReadTime` |
| Misc | `FileNum`、`ParseMetaTime`、`ParseFooterTime`、`FileReaderCreateTime`、`RawRowsRead`、`FilteredBytes` |

## 需要补充

### 1. Scheduler 级别

| 指标 | 说明 | 用途 |
|---|---|---|
| `SelectedRows` | 每次 batch selected rows 累计 | 了解 filter 选择性 |
| `SkippedRows` | range gap skip 累计行数 | 评估 page-index pruning 跳过的行数占比 |
| `TotalBatches` | batch 总次数 | 基准 |
| `EmptySelectionBatches` | filter 后 selected_rows==0 的 batch 次数 | 了解 filter 效率 |

实现位置：`parquet_scan.cpp` 的 `read_current_row_group_batch()` 中，每次执行 filter + select 后更新 counter。

```cpp
COUNTER_UPDATE(_profile.selected_rows, selected_rows);
COUNTER_UPDATE(_profile.skipped_rows, batch_rows - selected_rows);
if (selected_rows == 0) COUNTER_UPDATE(_profile.empty_selection_batches, 1);
COUNTER_UPDATE(_profile.total_batches, 1);
```

### 2. Column Reader 级别

| 指标 | 说明 | 用途 |
|---|---|---|
| `ReaderReadRows` | reader tree read() 累计行数 | reader 实际物化的行数 |
| `ReaderSkipRows` | reader tree skip() 累计行数 | reader 跳过的行数（应与 skipped_rows 对应） |
| `ReaderSelectRows` | reader tree select() 累计 selected rows | select 路径的相对占比 |

实现位置：`ParquetColumnReader` 基类，在 `read()`/`skip()`/`select()` 中更新。

### 3. Nested Assembler 级别

| 指标 | 说明 | 用途 |
|---|---|---|
| `NestedOverflowCount` | overflow 发生次数 | 评估 batch size 是否合适（overflow 过多说明 batch 太小） |
| `NestedOverflowTailRows` | overflow tail 累计 rows | 评估 overflow 携带的数据量 |
| `NestedLevelSlotsTotal` | 累计处理的 level slots | 基准 |

实现位置：`list_column_reader.cpp`、`map_column_reader.cpp` 中，每次 move tail 到 overflow 时更新。

### 4. Adapter 级别（细分耗时）

| 指标 | 说明 | 用途 |
|---|---|---|
| `ArrowReadTime` | `RecordReader::ReadRecords()` 总耗时 | 与 decode/materialization 解耦 |
| `ArrowDecodeTime` | page decode（解压+解码）总耗时 | 评估 page-level skip 的潜在收益 |
| `MaterializationTime` | `append_leaf_values()` 总耗时 | 值写入 Doris column 的开销 |

当前 `ColumnReadTime` 是一个聚合指标，无法区分 ReadRecords 内部开销。需要在 `read_leaf_records()` 和 `append_leaf_values()` 前后加 SCOPED_TIMER。

### 5. Page-level Skip（P4 完成后）

| 指标 | 说明 | 用途 |
|---|---|---|
| `PagesSkippedByFilter` | data_page_filter 跳过的 page 数 | 验证 page-level skip 实际命中率 |
| `PageSkipBytes` | 跳过的 compressed bytes 累计 | page-level skip 节省的 I/O 量 |

## 实施步骤

### Step 1: 添加 Timer/Counter 声明

在 `parquet_reader.h` 的 `ParquetProfile` struct 中新增 counter 成员。

### Step 2: Scheduler 指标

`parquet_scan.cpp` 的 `read_current_row_group_batch()` 中更新。

### Step 3: Reader 指标

`ParquetColumnReader` 基类中新增 `_reader_read_rows` / `_reader_skip_rows` / `_reader_select_rows`，子类 read/skip/select 中更新。`ParquetReader` 在 close 前聚合所有 reader 的计数。

### Step 4: Nested 指标

`list_column_reader.cpp`、`map_column_reader.cpp` 的 overflow 路径中更新。

### Step 5: Adapter 指标

`arrow_leaf_reader_adapter.cpp` 的 `read_leaf_records()` 和 `append_leaf_values()` 中加 SCOPED_TIMER。

## 验证

1. 运行包含 filter + selection + complex types 的查询，确认所有新 counter 有合理值
2. 空 selection 场景确认 `EmptySelectionBatches > 0`
3. Page-index pruning 场景确认 `SkippedRows` 与 page-index filter 跳过的行数一致
