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

### Step 1: 改用 PageReader + ColumnReader 替代 RecordReader

在 `get_record_reader()` 中：

```cpp
// 之前
_record_readers[leaf_column_id] =
    _row_group->RecordReader(leaf_column_id, false);

// 之后
auto page_reader = _row_group->GetColumnPageReader(leaf_column_id);
page_reader->set_data_page_filter([&](const DataPageStats& stats) -> bool {
    // 用已收集的 page-level 统计信息判断是否跳过
    return should_skip_page(stats);
});
// 创建 ColumnReader 替代 RecordReader
auto column_reader = ColumnReader::Make(descriptor, std::move(page_reader), pool);
```

### Step 2: 实现 filter callback

callback 接收 page header 中的 `EncodedStatistics`（min/max/null_count），用已有的 `ColumnPredicate` 判断该 page 不可能包含匹配数据时返回 `true`：

```cpp
bool should_skip_page(const DataPageStats& stats,
                      const std::vector<ColumnPredicate>& predicates) {
    if (stats.encoded_statistics == nullptr) return false;  // 无统计信息，无法判断
    for (const auto& pred : predicates) {
        if (!pred_can_skip(*stats.encoded_statistics, pred)) return false;
    }
    return true;
}
```

`pred_can_skip` 复用已有的 `ParquetStatisticsUtils::CheckStatistics()` 逻辑。

### Step 3: 适配 ScalarColumnReader

`ScalarColumnReader` 当前持有 `RecordReader`，需要改为持有 `ColumnReader`（或一个统一的 adapter interface）：

- `ColumnReader::ReadRecords(batch_rows)` → 等价于 `RecordReader::ReadRecords()`
- `ColumnReader::SkipRecords(rows)` → 等价于 `RecordReader::SkipRecords()`，但被跳过的 page 不再解压
- `ColumnReader::def_levels()` / `ColumnReader::rep_levels()` / `ColumnReader::values()` → 需要确认 `ColumnReader` 是否暴露这些接口。如果接口不同，需要加 adapter

### Step 4: 保持 read_nested_leaf_batch 接口不变

`read_nested_leaf_batch()` 是 adapter 和 nested assembler 之间的桥梁。改成 `ColumnReader` 后，只要 adapter 内部完成适配，上层完全不受影响。

## 影响范围

| 文件 | 改动 |
|---|---|
| `reader/column_reader.cpp` | `get_record_reader()` 改用 `PageReader` + filter callback |
| `reader/scalar_column_reader.cpp` | `_record_reader` 改为 Arrow `ColumnReader` 或 adapter |
| `reader/arrow_leaf_reader_adapter.cpp` | `read_leaf_records` / `read_nested_leaf_batch` 适配新 API |
| `parquet_statistics.cpp` | 提取 `should_skip_page` 为可复用函数 |

不需要改动：parquet_scan、list/map/struct reader、nested_column_reader。

## 验证

1. 构造包含多个 page 的 parquet 文件，其中部分 page 的 min/max 完全落在 filter 范围外
2. 验证 `SELECT * FROM t WHERE col > 100` 跳过的 page 数量与 page-index pruning 一致
3. Profile 确认 skipped page 的 compressed size 不产生 I/O
