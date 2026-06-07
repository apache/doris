# Page-level Skip 实现方案

## 背景

当前 page-index pruning 已能选出需要读的 page range，输出 row ranges。Scan scheduler 通过 `skip(row_count)` 跳过 range gap。但 `skip()` 底层是 Arrow `RecordReader::SkipRecords()`，它仍然解压和解码被跳过的 page（只跳过 value 写入），无法节省 page 级 I/O 和解压开销。

旧 reader 的 `VPageReader::skip_page_data()` 仅做 `_offset += compressed_page_size`，可以完全跳过 page 数据。

## Arrow 已有 API

Arrow 的 `PageReader` 提供了 `set_data_page_filter()`，不需要自己实现 decoder：

```cpp
// thirdparty/installed/include/parquet/column_reader.h:124-151
class PageReader {
  using DataPageFilter = std::function<bool(const DataPageStats&)>;
  void set_data_page_filter(DataPageFilter data_page_filter);
};

struct DataPageStats {
  const EncodedStatistics* encoded_statistics;  // page header 中的 min/max/null_count
  int32_t num_values;
  std::optional<int32_t> num_rows;
};
```

- `NextPage()` 在返回每个 page 前调用 callback。
- callback 返回 `true` → Arrow 内部跳过该 page（纯 offset 前进，零解压）。
- callback 返回 `false` → 正常读取并解压。

## DuckDB 参考

DuckDB 的 `PrepareRead()` → `PageIsFilteredOut()` → `trans.Skip(page_hdr.compressed_page_size)` 与 Arrow 的 `set_data_page_filter` 等价——都是先读 page header 再决定是否跳过 data。

## 当前 gap

`get_record_reader()`（`column_reader.cpp:210-241`）调用 `_row_group->RecordReader(leaf_column_id)`。Arrow 的 `RowGroupReader::RecordReader()` 内部创建 `PageReader` 时未设置 filter：

```cpp
_record_readers[leaf_column_id] =
    _row_group->RecordReader(leaf_column_id, /*read_dictionary=*/false);
```

这导致 `SkipRecords()` 仍然解压所有 page。

## 方案

### Step 1: 保留 RecordReader，只手动注入 PageReader filter

不要把 Doris 的 `ScalarColumnReader` 从 Arrow `RecordReader` 切换到 Arrow
`ColumnReader`：

- `ColumnReader::Skip()` 是 value-level skip，不是 record-level skip。对于 repeated
  列，它可能停在同一个 logical record 内部。
- 当前 nested assembler 依赖 `RecordReader` 的
  `def_levels()`/`rep_levels()`/`values()`/`levels_position()`/`values_written()` 等
  internal API。切换到 `ColumnReader` 会扩大改动面，并且容易破坏复杂类型读取。

Arrow `RowGroupReader::RecordReader()` 的实现本身很薄，等价于：

```cpp
auto page_reader = row_group->GetColumnPageReader(leaf_column_id);
auto level_info = ::parquet::internal::LevelInfo::ComputeLevelInfo(descriptor);
auto record_reader = ::parquet::internal::RecordReader::Make(
        descriptor, level_info, pool, /*read_dictionary=*/false,
        row_group_properties->read_dense_for_nullable());
record_reader->SetPageReader(std::move(page_reader));
```

因此 Doris 在 `get_record_reader()` 中复制这条路径，在 `SetPageReader()` 前插入
`set_data_page_filter()`：

```cpp
auto page_reader = _row_group->GetColumnPageReader(leaf_column_id);
page_reader->set_data_page_filter([filter = page_filter](const DataPageStats& stats) -> bool {
    return filter.should_skip_next_data_page(stats);
});

auto record_reader = ::parquet::internal::RecordReader::Make(...);
record_reader->SetPageReader(std::move(page_reader));
```

这样上层 `ScalarColumnReader`、`read_leaf_records()`、`read_nested_leaf_batch()` 的接口
可以保持不变。

### Step 2: 规划阶段产出 page skip plan

不要在 callback 中重新基于 page header stats 做一套独立判断。当前
`select_row_group_ranges_by_page_index()` 已经使用 ColumnIndex/OffsetIndex 和
`ParquetStatisticsUtils::CheckStatistics()` 计算了 page-index pruning 结果；callback 应该消费
同一份规划结果，避免两套判断逻辑不一致。

在 `RowGroupReadPlan` 中增加 per-leaf page skip 信息，例如：

```cpp
struct LeafPageSkipPlan {
    int leaf_column_id = -1;
    std::vector<bool> skipped_pages;
};

struct RowGroupReadPlan {
    int row_group_id = -1;
    int64_t first_file_row = 0;
    int64_t row_group_rows = 0;
    std::vector<RowRange> selected_ranges;
    std::map<int, LeafPageSkipPlan> leaf_page_skip_plans;
};
```

`select_row_group_ranges_by_page_index()` 在计算 `selected_ranges` 的同时，为参与
page-index pruning 的 primitive leaf 生成 skipped page bitmap。对于无法可靠判断的 page
或 column，不生成 skip plan。

### Step 3: 避免 row-range skip 和 page filter double skip

`set_data_page_filter()` 不能作为静态 page blacklist 直接叠加到现有
`selected_ranges + SkipRecords()` 上，否则会 double skip：

- scheduler 看到 range gap 会调用 `skip_current_row_group_rows(gap_rows)`。
- 如果 `PageReader` 已经把 gap 对应的完整 page filter 掉，`RecordReader` 看不到这些
  records。
- 此时再调用 `SkipRecords(gap_rows)` 会从下一个未过滤 page 继续跳，跳过本应读取的数据。

因此需要调整 skip accounting：

1. page-index planner 区分完整 page gap 和非 page-aligned 残余 gap。
2. 对完整被过滤 page，只通过 `set_data_page_filter()` 物理跳过 page body，不再把这些
   rows 交给 `SkipRecords()`。
3. `SkipRecords()` 只处理 page 内残余 rows，或者没有 page skip plan 的列。
4. scheduler 的 logical row cursor 仍按 `selected_ranges` 前进，但 column reader 的 physical
   skip 需要扣除已经由 page filter 消费的 rows。

第一阶段可以把 page-level filter 限定在 page-aligned range gap 上。对于跨 page 的残余
rows，继续使用现有 `SkipRecords()`，保证正确性优先。

### Step 4: 支持范围

第一阶段只支持满足以下条件的 leaf：

- primitive leaf
- `max_repetition_level == 0`
- page index 的 ColumnIndex 和 OffsetIndex 都存在且 page 数匹配
- page row range 可以安全映射到 logical row range

LIST/MAP/repeated leaf 暂不启用 page-level filter。对于 repeated leaf，logical row 可能跨
page；在没有完整 Dremel row boundary 证明前，只能用 page index 做整 row group pruning，
不应做局部 page skip。

## 影响范围

| 文件 | 改动 |
|---|---|
| `parquet_scan.h` | `RowGroupReadPlan` 增加 per-leaf page skip plan |
| `parquet_statistics.cpp` | page-index pruning 同时产出 `selected_ranges` 和 skipped page bitmap |
| `reader/column_reader.h/.cpp` | `ParquetColumnReaderFactory` 接收当前 row group 的 page skip plan；`get_record_reader()` 手动创建 `PageReader`，设置 `set_data_page_filter()`，再创建 `RecordReader` |
| `parquet_scan.cpp` | open row group 时把 page skip plan 传给 factory；skip accounting 避免 double skip |

不需要改动：`read_leaf_records()`、`read_nested_leaf_batch()`、list/map/struct assembler。
后续可以补充 profile counter，区分 page-index filtered rows 和实际物理 skipped
pages/bytes。

## 验证

1. 构造包含多个 page 的 parquet 文件，其中部分 page 的 min/max 完全落在 filter 范围外
2. 验证 `SELECT * FROM t WHERE col > 100` 跳过的 page 数量与 page-index pruning 一致
3. 验证 page-aligned gap 不再调用 `SkipRecords()` 跳对应 rows，避免 double skip
4. Profile 确认 skipped page 的 compressed size 不产生 I/O/解压
5. 验证 partial-page gap 仍走 `SkipRecords()`，结果正确
6. 验证 LIST/MAP/repeated leaf 不启用局部 page skip
