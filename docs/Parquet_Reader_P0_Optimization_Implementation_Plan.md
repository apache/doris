# Doris Parquet Reader P0 优化方向详细实现方案

> 基于 Doris 现有代码结构和 StarRocks 参考实现，给出三个 P0 优化方向的详细实现方案。

---

## P0-1：Filter Bitmap 下推到 Decoder 层

### 1.1 问题分析

#### 当前数据流

```
ScalarColumnReader::read_column_data(filter_map)
  → _read_values(filter_map)
    → ColumnSelectVector::init(null_map, filter_map)  // 合并 null + filter 为 4 种 run
    → ColumnChunkReader::decode_values(select_vector)
      → Decoder::decode_values(doris_column, data_type, select_vector, is_dict_filter)
```

#### 浪费点 1：字典 Index 全量解码

**文件**: `be/src/vec/exec/format/parquet/fix_length_dict_decoder.hpp:97-98`

```cpp
// _decode_values<has_filter>() 中：
size_t non_null_size = select_vector.num_values() - select_vector.num_nulls();
_indexes.resize(non_null_size);
_index_batch_decoder->GetBatch(_indexes.data(), non_null_size);  // 解码 ALL 非空 index
```

所有非空行的 RLE dict index 被全部解码，包括那些将被 `FILTERED_CONTENT` 跳过的行。在低选择率场景（如 5% 存活），95% 的 index 解码是浪费的。

**同样存在于**: `be/src/vec/exec/format/parquet/byte_array_dict_decoder.cpp:116-117`

#### 浪费点 2：BaseDictDecoder::skip_values() 的无效解码

**文件**: `be/src/vec/exec/format/parquet/decoder.h:149-153`

```cpp
Status skip_values(size_t num_values) override {
    _indexes.resize(num_values);
    _index_batch_decoder->GetBatch(_indexes.data(), num_values);  // 解码后丢弃
    return Status::OK();
}
```

跳过值时仍需完整解码 RLE index 到内存，然后丢弃。缺少 `RleBatchDecoder::Skip()` 方法。

#### 浪费点 3：FILTERED_CONTENT 行的字典值 Lookup

在 `_decode_fixed_values<true>()` 中 (`fix_length_dict_decoder.hpp:166-168`)：
```cpp
case ColumnSelectVector::FILTERED_CONTENT: {
    dict_index += run_length;  // 跳过 index，但 index 已经在上面被解码了
    break;
}
```

虽然 FILTERED_CONTENT 不做 dict lookup（只是 `dict_index += run_length`），但这些 index 已经在步骤 1 被解码出来了。

### 1.2 实现方案

#### 方案概述

**不修改 ColumnSelectVector 机制**，在 Decoder 内部接收原始 filter bitmap，当选择率 < 阈值时，用 filter bitmap 跳过无用的字典值 lookup（对于大字典尤其有效，减少 cache miss）。

#### 步骤 1：为 RleBatchDecoder 添加 SkipBatch 方法

**文件**: `be/src/util/rle_encoding.h`

```cpp
template <typename T>
class RleBatchDecoder {
public:
    // 已有方法
    int32_t GetBatch(T* values, uint32_t batch_size);

    // 新增：跳过 num_values 个值，不写入任何缓冲区
    int32_t SkipBatch(uint32_t num_values) {
        DCHECK_GT(num_values, 0);
        int32_t num_skipped = 0;
        while (num_skipped < num_values) {
            if (UNLIKELY(num_buffered_values_ == 0)) {
                if (UNLIKELY(!NextCounts<T>())) return num_skipped;
            }
            uint32_t to_skip = std::min<uint32_t>(
                num_values - num_skipped, num_buffered_values_);
            if (repeat_count_ > 0) {
                // RLE run：直接减少 repeat_count_
                uint32_t skip = std::min<uint32_t>(to_skip, repeat_count_);
                repeat_count_ -= skip;
                num_buffered_values_ -= skip;
                num_skipped += skip;
            } else {
                // Literal run：推进 literal buffer 位置
                uint32_t skip = std::min<uint32_t>(to_skip, literal_count_);
                for (uint32_t i = 0; i < skip; ++i) {
                    // 需要从 bit reader 读取并丢弃
                    T unused;
                    if (!bit_reader_.GetValue(bit_width_, &unused)) return num_skipped;
                }
                literal_count_ -= skip;
                num_buffered_values_ -= skip;
                num_skipped += skip;
            }
        }
        return num_skipped;
    }
};
```

**注意**：检查 Doris 现有的 `RleBatchDecoder` 实现（可能已有类似方法，需确认）。如果 literal run 的跳过无法避免 bit 读取，至少能避免内存分配和写入。

#### 步骤 2：修改 BaseDictDecoder::skip_values()

**文件**: `be/src/vec/exec/format/parquet/decoder.h:149-153`

```cpp
// 修改前
Status skip_values(size_t num_values) override {
    _indexes.resize(num_values);
    _index_batch_decoder->GetBatch(_indexes.data(), num_values);
    return Status::OK();
}

// 修改后
Status skip_values(size_t num_values) override {
    auto skipped = _index_batch_decoder->SkipBatch(cast_set<uint32_t>(num_values));
    if (UNLIKELY(skipped < num_values)) {
        return Status::InternalError("RLE skip error, not enough values");
    }
    return Status::OK();
}
```

**收益**：消除 skip 场景下的内存分配 (`_indexes.resize`) 和无效写入。

#### 步骤 3：修改 Decoder 接口，添加 filter bitmap 参数

**文件**: `be/src/vec/exec/format/parquet/decoder.h:69-70`

```cpp
// 修改前
virtual Status decode_values(MutableColumnPtr& doris_column, DataTypePtr& data_type,
                             ColumnSelectVector& select_vector, bool is_dict_filter) = 0;

// 修改后：添加可选的 filter bitmap 参数
virtual Status decode_values(MutableColumnPtr& doris_column, DataTypePtr& data_type,
                             ColumnSelectVector& select_vector, bool is_dict_filter,
                             const uint8_t* filter_data = nullptr) = 0;
```

#### 步骤 4：在 ColumnChunkReader 中传递 filter bitmap

**文件**: `be/src/vec/exec/format/parquet/vparquet_column_chunk_reader.cpp:528-544`

修改 `ColumnChunkReader::decode_values()` 签名，添加 `filter_data` 参数并转发给 decoder：

```cpp
template <bool IN_COLLECTION, bool OFFSET_INDEX>
Status ColumnChunkReader<IN_COLLECTION, OFFSET_INDEX>::decode_values(
        MutableColumnPtr& doris_column, DataTypePtr& data_type,
        ColumnSelectVector& select_vector, bool is_dict_filter,
        const uint8_t* filter_data) {
    // ... 现有检查 ...
    return _page_decoder->decode_values(doris_column, data_type, select_vector,
                                         is_dict_filter, filter_data);
}
```

同步修改头文件 `vparquet_column_chunk_reader.h` 中的声明。

#### 步骤 5：在 ScalarColumnReader::_read_values() 中决策并传递 filter bitmap

**文件**: `be/src/vec/exec/format/parquet/vparquet_column_reader.cpp:390-397`

```cpp
// 在 ColumnSelectVector::init() 之后，decode_values() 之前：
const uint8_t* filter_data_for_decoder = nullptr;
if (select_vector.has_filter() && filter_map.has_filter()) {
    // 计算选择率
    size_t total = select_vector.num_values();
    size_t filtered = select_vector.num_filtered();
    double selectivity = 1.0 - static_cast<double>(filtered) / total;
    // 选择率 < 20% 时下推 filter bitmap
    if (selectivity < 0.2) {
        filter_data_for_decoder = filter_map.filter_map_data() + _filter_map_index - num_values;
    }
}
return _chunk_reader->decode_values(data_column, type, select_vector,
                                     is_dict_filter, filter_data_for_decoder);
```

#### 步骤 6：修改 FixLengthDictDecoder 使用 filter bitmap

**文件**: `be/src/vec/exec/format/parquet/fix_length_dict_decoder.hpp`

```cpp
Status decode_values(MutableColumnPtr& doris_column, DataTypePtr& data_type,
                     ColumnSelectVector& select_vector, bool is_dict_filter,
                     const uint8_t* filter_data = nullptr) override {
    if (select_vector.has_filter()) {
        return _decode_values<true>(doris_column, data_type, select_vector,
                                    is_dict_filter, filter_data);
    } else {
        return _decode_values<false>(doris_column, data_type, select_vector,
                                     is_dict_filter, nullptr);
    }
}

template <bool has_filter>
Status _decode_values(MutableColumnPtr& doris_column, DataTypePtr& data_type,
                      ColumnSelectVector& select_vector, bool is_dict_filter,
                      const uint8_t* filter_data) {
    size_t non_null_size = select_vector.num_values() - select_vector.num_nulls();

    // ... dict column 初始化代码不变 ...

    // 仍需全量解码 RLE index（RLE 是顺序解码，无法跳过）
    _indexes.resize(non_null_size);
    _index_batch_decoder->GetBatch(_indexes.data(), cast_set<uint32_t>(non_null_size));

    if (doris_column->is_column_dictionary() || is_dict_filter) {
        return _decode_dict_values<has_filter>(doris_column, select_vector, is_dict_filter);
    }

    return _decode_fixed_values<has_filter>(doris_column, data_type, select_vector, filter_data);
}
```

修改 `_decode_fixed_values` 在 `CONTENT` 分支中利用 filter bitmap：

```cpp
template <bool has_filter>
Status _decode_fixed_values(MutableColumnPtr& doris_column, DataTypePtr& data_type,
                            ColumnSelectVector& select_vector,
                            const uint8_t* filter_data) {
    // ... 现有的 resize 和 raw_data 获取 ...
    size_t dict_index = 0;
    size_t filter_offset = 0;  // 跟踪 filter bitmap 位置
    ColumnSelectVector::DataReadType read_type;
    while (size_t run_length = select_vector.get_next_run<has_filter>(&read_type)) {
        switch (read_type) {
        case ColumnSelectVector::CONTENT: {
            if (filter_data != nullptr) {
                // 有 filter bitmap：仅对 filter[i]=1 的行做 dict lookup
                for (size_t i = 0; i < run_length; ++i) {
                    if (filter_data[filter_offset + i]) {
                        auto& item = _dict_items[_indexes[dict_index]];
                        memcpy(raw_data + data_index, &item, _type_length);
                    }
                    // 无论是否 filter，都要推进 data_index 和 dict_index
                    data_index += _type_length;
                    dict_index++;
                }
            } else {
                // 原有路径不变
                for (size_t i = 0; i < run_length; ++i) {
                    *(cppType*)(raw_data + data_index) = _dict_items[_indexes[dict_index++]];
                    data_index += _type_length;
                }
            }
            filter_offset += run_length;
            break;
        }
        case ColumnSelectVector::NULL_DATA: {
            data_index += run_length * _type_length;
            filter_offset += run_length;
            break;
        }
        case ColumnSelectVector::FILTERED_CONTENT: {
            dict_index += run_length;
            break;
        }
        case ColumnSelectVector::FILTERED_NULL: {
            break;
        }
        }
    }
    return Status::OK();
}
```

**核心收益**：在 `CONTENT` run 中，`filter_data[i]=0` 的行跳过 `_dict_items[_indexes[...]]` 的随机内存访问。对于大字典（> L2 cache），这可以显著减少 cache miss。

#### 步骤 7：同样修改 ByteArrayDictDecoder

**文件**: `be/src/vec/exec/format/parquet/byte_array_dict_decoder.h` 和 `.cpp`

对 `ByteArrayDictDecoder::_decode_values<has_filter>()` 做类似修改：在 `CONTENT` 分支中，仅对 `filter_data[i]=1` 的行执行 `_dict_items[_indexes[dict_index]]` 的 StringRef 构造和 `insert_many_strings_overflow()`。

对于 string 类型，收益更大：跳过 filter 的行不仅避免了 dict lookup 的 cache miss，还避免了 string copy。

#### 步骤 8：添加配置开关

**文件**: `be/src/common/config.h`

```cpp
CONF_mBool(parquet_push_down_filter_to_decoder_enable, "true");
```

**文件**: `be/src/common/config.cpp` 中注册。

在步骤 5 的选择率判断中加入配置检查：

```cpp
if (selectivity < 0.2 && config::parquet_push_down_filter_to_decoder_enable) {
    filter_data_for_decoder = ...;
}
```

### 1.3 涉及修改的文件清单

| 文件 | 修改内容 |
|------|----------|
| `be/src/util/rle_encoding.h` | 添加 `RleBatchDecoder::SkipBatch()` 方法 |
| `be/src/vec/exec/format/parquet/decoder.h` | 修改 `Decoder::decode_values()` 签名（添加 `filter_data`）；修改 `BaseDictDecoder::skip_values()` 使用 SkipBatch |
| `be/src/vec/exec/format/parquet/vparquet_column_chunk_reader.h` | 修改 `decode_values()` 签名 |
| `be/src/vec/exec/format/parquet/vparquet_column_chunk_reader.cpp` | 转发 `filter_data` |
| `be/src/vec/exec/format/parquet/vparquet_column_reader.cpp` | 计算选择率，决策是否下推 filter bitmap |
| `be/src/vec/exec/format/parquet/fix_length_dict_decoder.hpp` | CONTENT 分支利用 filter bitmap 跳过 dict lookup |
| `be/src/vec/exec/format/parquet/byte_array_dict_decoder.h/.cpp` | 同上 |
| `be/src/vec/exec/format/parquet/fix_length_plain_decoder.h/.cpp` | 签名同步修改（Plain 编码受益较小，可选实现） |
| `be/src/vec/exec/format/parquet/byte_array_plain_decoder.h/.cpp` | 签名同步修改 |
| `be/src/vec/exec/format/parquet/delta_bit_pack_decoder.h` | 签名同步修改 |
| `be/src/vec/exec/format/parquet/byte_stream_split_decoder.h` | 签名同步修改 |
| `be/src/vec/exec/format/parquet/bss_page_decoder.h` | 签名同步修改 |
| `be/src/common/config.h` | 新增 `parquet_push_down_filter_to_decoder_enable` |

### 1.4 StarRocks 参考

- **选择率门控**: `stored_column_reader.h:155-161`，`_convert_filter_row_to_value()` 使用 `SIMD::count_nonzero(*filter) * 1.0 / filter->size() < 0.2` 作为阈值
- **Cache-Aware 门控**: `encoding_dict.h:122-126`，字典 > L2 cache 时才传 filter（可在后续 P1 优化中实现）
- **Decoder 使用 filter**: `encoding_dict.h:359-363`，`if (filter[i]) { data[i] = _dict[_indexes[i]]; }`

---

## P0-2：谓词列读取顺序优化

### 2.1 问题分析

#### 当前数据流

**文件**: `be/src/vec/exec/format/parquet/vparquet_group_reader.cpp:518-725` (`_do_lazy_read()`)

```
_do_lazy_read():
  Phase 1: _read_column_data(block, predicate_columns.first, ...)
           → 读 ALL 谓词列（一次性，schema 顺序）
           → VExprContext::execute_conjuncts(_filter_conjuncts, ...)
           → 产出 filter_map
  Phase 2: _read_column_data(block, lazy_columns, filter_map)
           → 带 filter 读懒加载列
```

**问题**：Phase 1 中所有谓词列**一次性全部读取**，没有中间过滤。假设有 3 个谓词列 A、B、C：
- 列 A 的选择率 5%（过滤掉 95% 的行）
- 列 B、C 的过滤效果较弱

当前做法：A、B、C 三列全部解码所有行 → 然后整体过滤。
优化做法：先读 A → 过滤 → 只对存活行读 B → 过滤 → 只对存活行读 C → ...

#### 当前谓词列顺序

谓词列顺序 = **Parquet 文件 schema 顺序**（`vparquet_reader.cpp:539-558`），与列的选择率无关。

### 2.2 实现方案

#### 方案概述

将 `_do_lazy_read()` 的 Phase 1 从"一次性读所有谓词列"改为"逐列读取+中间过滤"，并引入自适应列排序机制选择最优读取顺序。

#### 步骤 1：重构谓词 conjuncts 按列分组

**文件**: `be/src/vec/exec/format/parquet/vparquet_group_reader.h`

在 `RowGroupReader` 中新增成员：

```cpp
// 按列分组的 conjuncts：slot_id -> conjuncts 列表
// 仅包含单列谓词（引用单个 slot_id 的 conjunct）
std::unordered_map<int, VExprContextSPtrs> _single_col_filter_conjuncts;
// 多列谓词（引用多个 slot_id 的 conjunct），在所有涉及列读完后评估
VExprContextSPtrs _multi_col_filter_conjuncts;
```

#### 步骤 2：在 init() 中分类 conjuncts

**文件**: `be/src/vec/exec/format/parquet/vparquet_group_reader.cpp`

在现有的 `_filter_conjuncts` 构建之后，分析每个 conjunct 引用的列：

```cpp
void RowGroupReader::_classify_conjuncts_by_column() {
    for (auto& conjunct : _filter_conjuncts) {
        std::set<int> referenced_slot_ids;
        _collect_slot_ids(conjunct->root(), referenced_slot_ids);

        if (referenced_slot_ids.size() == 1) {
            int slot_id = *referenced_slot_ids.begin();
            _single_col_filter_conjuncts[slot_id].push_back(conjunct);
        } else {
            _multi_col_filter_conjuncts.push_back(conjunct);
        }
    }
}

void RowGroupReader::_collect_slot_ids(VExpr* expr, std::set<int>& slot_ids) {
    if (expr->is_slot_ref()) {
        slot_ids.insert(static_cast<VSlotRef*>(expr)->slot_id());
    }
    for (auto& child : expr->children()) {
        _collect_slot_ids(child.get(), slot_ids);
    }
}
```

#### 步骤 3：引入 ColumnReadOrderCtx 类

**新建文件**: `be/src/vec/exec/format/parquet/column_read_order_ctx.h`

```cpp
#pragma once

#include <vector>
#include <unordered_map>
#include <random>
#include <algorithm>

namespace doris::vectorized {

class ColumnReadOrderCtx {
public:
    ColumnReadOrderCtx(std::vector<int> col_slot_ids,
                       std::unordered_map<int, size_t> col_cost_map,
                       size_t total_cost)
        : _best_order(std::move(col_slot_ids)),
          _col_cost_map(std::move(col_cost_map)),
          _min_round_cost(total_cost) {}

    // 获取当前轮次的列读取顺序
    // 前 EXPLORATION_ROUNDS 轮返回随机顺序；之后返回最优顺序
    const std::vector<int>& get_column_read_order() {
        if (_exploration_remaining > 0) {
            _trying_order = _best_order;
            std::shuffle(_trying_order.begin(), _trying_order.end(),
                         std::mt19937(std::random_device()()));
            return _trying_order;
        }
        return _best_order;
    }

    // 每轮结束后更新统计：round_cost = 实际读取的数据量
    // first_selectivity = 第一列过滤后的存活比例
    void update(size_t round_cost, double first_selectivity) {
        if (_exploration_remaining > 0) {
            if (round_cost < _min_round_cost ||
                (round_cost == _min_round_cost &&
                 first_selectivity > 0 && first_selectivity < _best_first_selectivity)) {
                _best_order = _trying_order;
                _min_round_cost = round_cost;
                _best_first_selectivity = first_selectivity;
            }
            _trying_order.clear();
            _exploration_remaining--;
        }
    }

    size_t get_column_cost(int slot_id) const {
        auto it = _col_cost_map.find(slot_id);
        return it != _col_cost_map.end() ? it->second : 0;
    }

private:
    static constexpr int EXPLORATION_ROUNDS = 10;

    std::vector<int> _best_order;        // 已知最优顺序
    std::vector<int> _trying_order;      // 当前尝试的顺序
    std::unordered_map<int, size_t> _col_cost_map;  // slot_id -> 平面大小 cost
    size_t _min_round_cost;
    double _best_first_selectivity = 1.0;
    int _exploration_remaining = EXPLORATION_ROUNDS;
};

} // namespace doris::vectorized
```

#### 步骤 4：在 RowGroupReader 中初始化 ColumnReadOrderCtx

**文件**: `be/src/vec/exec/format/parquet/vparquet_group_reader.cpp`

在 `init()` 最后，如果有谓词列且启用了列顺序优化：

```cpp
if (_lazy_read_ctx.can_lazy_read &&
    _lazy_read_ctx.predicate_columns.first.size() > 1) {
    // 只有多于 1 个谓词列时才需要排序优化
    std::vector<int> pred_slot_ids = _lazy_read_ctx.predicate_columns.second;
    std::unordered_map<int, size_t> cost_map;
    size_t total_cost = 0;
    for (size_t i = 0; i < pred_slot_ids.size(); ++i) {
        const auto& col_name = _lazy_read_ctx.predicate_columns.first[i];
        // cost 使用列的物理类型大小作为近似
        size_t col_cost = _column_readers[col_name]->get_type_length();
        cost_map[pred_slot_ids[i]] = col_cost;
        total_cost += col_cost;
    }
    _column_read_order_ctx = std::make_unique<ColumnReadOrderCtx>(
        pred_slot_ids, std::move(cost_map), total_cost);
}
```

#### 步骤 5：重构 _do_lazy_read() Phase 1 为逐列读取

**文件**: `be/src/vec/exec/format/parquet/vparquet_group_reader.cpp:518-725`

将 Phase 1 的 `_read_column_data(block, predicate_columns.first, ...)` 替换为逐列读取 + 中间过滤：

```cpp
Status RowGroupReader::_do_lazy_read(Block* block, size_t batch_size,
                                      size_t* read_rows, bool* batch_eof) {
    // ... 现有的初始化代码 ...

    while (!_state->is_cancelled()) {
        // Phase 1: 逐列读取谓词列
        FilterMap filter_map;  // 初始为空

        if (_column_read_order_ctx) {
            // === 新路径：逐列读取 + 中间过滤 ===
            const auto& read_order = _column_read_order_ctx->get_column_read_order();
            size_t round_cost = 0;
            double first_selectivity = -1;
            bool all_filtered = false;

            for (size_t round = 0; round < read_order.size(); ++round) {
                int slot_id = read_order[round];
                // 找到对应的列名
                std::string col_name = _find_col_name_by_slot_id(slot_id);

                round_cost += _column_read_order_ctx->get_column_cost(slot_id);

                // 读取单列（带 filter_map，如果有的话）
                _read_single_column_data(block, col_name, batch_size,
                                          &pre_read_rows, &pre_eof, filter_map);

                // 如果该列有单列谓词，执行过滤
                auto it = _single_col_filter_conjuncts.find(slot_id);
                if (it != _single_col_filter_conjuncts.end()) {
                    IColumn::Filter result_filter;
                    bool can_filter_all = false;
                    VExprContext::execute_conjuncts(it->second, nullptr,
                                                    block, &result_filter, &can_filter_all);

                    if (can_filter_all) {
                        all_filtered = true;
                        if (first_selectivity < 0) first_selectivity = 0;
                        break;  // 所有行被过滤，提前退出
                    }

                    // 更新 filter_map
                    _update_filter_map_with_result(filter_map, result_filter);

                    if (first_selectivity < 0) {
                        size_t hit = simd::count_nonzero(result_filter.data(),
                                                          result_filter.size());
                        first_selectivity = static_cast<double>(hit) / result_filter.size();
                    }
                }
            }

            // 执行多列谓词（所有列都已读取）
            if (!all_filtered && !_multi_col_filter_conjuncts.empty()) {
                // ... 执行 _multi_col_filter_conjuncts ...
            }

            _column_read_order_ctx->update(round_cost, first_selectivity);

        } else {
            // === 原有路径：一次性读取所有谓词列 ===
            _read_column_data(block, _lazy_read_ctx.predicate_columns.first,
                              batch_size, &pre_read_rows, &pre_eof, filter_map);
            // ... 原有的 conjunct 执行和 filter_map 构建 ...
        }

        // ... Phase 2: 读取 lazy 列（不变） ...
        // ... 后续逻辑不变 ...
    }
}
```

#### 步骤 6：添加辅助方法

**文件**: `be/src/vec/exec/format/parquet/vparquet_group_reader.h` 和 `.cpp`

```cpp
// 按 slot_id 找列名
std::string RowGroupReader::_find_col_name_by_slot_id(int slot_id) {
    const auto& names = _lazy_read_ctx.predicate_columns.first;
    const auto& ids = _lazy_read_ctx.predicate_columns.second;
    for (size_t i = 0; i < ids.size(); ++i) {
        if (ids[i] == slot_id) return names[i];
    }
    return "";
}

// 读取单个列
Status RowGroupReader::_read_single_column_data(
        Block* block, const std::string& col_name,
        size_t batch_size, size_t* read_rows, bool* eof,
        FilterMap& filter_map) {
    // 与 _read_column_data 类似，但只读一列
    // 包括 dict filter column 的类型替换逻辑
    std::vector<std::string> single_col = {col_name};
    return _read_column_data(block, single_col, batch_size, read_rows, eof, filter_map);
}

// 合并新的 filter 结果到已有的 filter_map
void RowGroupReader::_update_filter_map_with_result(
        FilterMap& filter_map, const IColumn::Filter& new_filter) {
    if (!filter_map.has_filter()) {
        // 首次过滤：直接使用 new_filter
        _filter_map_data = new_filter;  // 成员变量存储
        filter_map.init(_filter_map_data.data(), _filter_map_data.size(), false);
    } else {
        // 后续过滤：AND 合并
        const uint8_t* existing = filter_map.filter_map_data();
        for (size_t i = 0; i < new_filter.size(); ++i) {
            _filter_map_data[i] &= new_filter[i];
        }
        bool all_zero = simd::count_zero_num(_filter_map_data.data(),
                                              _filter_map_data.size())
                         == _filter_map_data.size();
        filter_map.init(_filter_map_data.data(), _filter_map_data.size(), all_zero);
    }
}
```

### 2.3 涉及修改的文件清单

| 文件 | 修改内容 |
|------|----------|
| **新建** `be/src/vec/exec/format/parquet/column_read_order_ctx.h` | ColumnReadOrderCtx 类定义 |
| `be/src/vec/exec/format/parquet/vparquet_group_reader.h` | 新增成员：`_column_read_order_ctx`、`_single_col_filter_conjuncts`、`_multi_col_filter_conjuncts`、`_filter_map_data` |
| `be/src/vec/exec/format/parquet/vparquet_group_reader.cpp` | 重构 `_do_lazy_read()` Phase 1；新增 `_classify_conjuncts_by_column()`、`_read_single_column_data()`、`_update_filter_map_with_result()` |

### 2.4 StarRocks 参考

- **ColumnReadOrderCtx**: `column_read_order_ctx.h:24-54`，10 次随机搜索 + cost-based 选择
- **逐列读取**: `group_reader.cpp:272-335` `_read_range_round_by_round()`，每列读完后执行 dict filter 和 non-dict conjuncts
- **提前退出**: `hit_count == 0` 时立即返回，跳过后续列
- **Cost 度量**: 使用 `slot_type().get_flat_size()` 作为列 cost

### 2.5 注意事项

1. **dict filter 列的处理**：逐列读取时，dict filter 列的类型替换（String → Int32）和 dict conjunct 评估需要在对应列读取后立即执行，而非等所有列读完。

2. **谓词的列归属**：有些 conjunct 可能引用多个列（如 `WHERE a + b > 10`），这些无法在单列读完后评估，需要延迟到所有涉及列读完后执行。

3. **探索期性能**：前 10 个 batch 使用随机顺序，可能不是最优。但由于每个 batch 通常有数千行，10 个 batch 的探索开销可以接受。

4. **只对 lazy read 路径有效**：非 lazy read 路径（所有列同时读取）不适用此优化。但 lazy read 是最常见的分析查询模式。

---

## P0-3：Lazy Dictionary Decode

### 3.1 问题分析

#### 当前字典过滤流程

Doris 已有一套字典过滤机制，但与 StarRocks 的 Lazy Dict Decode 有本质区别：

**Doris 现有 Dict Filter 流程** (`vparquet_group_reader.cpp:1042-1266`):

```
1. init() 时：
   _rewrite_dict_predicates()
   → 读取字典页所有值到 ColumnString
   → 在字典值上执行 conjuncts
   → 收集存活的 dict codes
   → 将 string 谓词改写为 int32 IN/EQ 谓词

2. 读取时 (_read_column_data):
   → 列类型替换：DataTypeString → DataTypeInt32
   → ByteArrayDictDecoder 输出 int32 dict codes（而非 string）
   → 执行改写后的 int32 谓词

3. 过滤后 (_convert_dict_cols_to_string_cols):
   → ColumnInt32 → 查字典 → ColumnString
```

**局限性**：
- 只对**有 IN/EQ 谓词的 string 列**有效（`_can_filter_by_dict()` 严格限制）
- 不是 "Lazy Decode"，而是 "Predicate Rewrite" — 谓词改写为 dict code 上的操作
- 对于**没有谓词但属于懒加载列的 string 列**，无法利用字典编码的优势

#### StarRocks 的 Lazy Dict Decode 范围更广

StarRocks 的 Lazy Dict Decode 不仅用于有谓词的列，还用于**所有 lazy 列的 string 类型字典编码列**。核心思想是：

1. 先只读 dict codes (int32) — 非常便宜
2. 等 active 列过滤后，只对存活行做 dict code → string 的转换
3. 如果 95% 的行被过滤，就只需转换 5% 的行

### 3.2 实现方案

#### 方案概述

扩展 Doris 现有的 dict filter 机制，使其覆盖到所有 lazy 列中的 string 类型字典编码列，即使这些列没有谓词。

#### 步骤 1：引入 ColumnContentType 枚举

**新建文件**: `be/src/vec/exec/format/parquet/parquet_utils.h`（或添加到 `parquet_common.h`）

```cpp
enum class ColumnContentType : uint8_t {
    VALUE = 0,     // 解码为实际值（string、int 等）
    DICT_CODE = 1  // 仅输出 dict codes (int32)
};
```

#### 步骤 2：修改 Decoder 接口支持 DICT_CODE 输出

**文件**: `be/src/vec/exec/format/parquet/decoder.h`

在 `Decoder` 基类中添加 DICT_CODE 模式支持。但考虑到 Doris 已有 `is_dict_filter` 参数实现了类似功能（当 `is_dict_filter=true` 时，`BaseDictDecoder::_decode_dict_values` 输出 int32），可以**复用现有机制**：

```cpp
// 现有接口不变，但扩展 is_dict_filter 的含义：
// is_dict_filter=true → 输出 dict codes (int32) 到 doris_column
// 这与 StarRocks 的 ColumnContentType::DICT_CODE 等价
```

因此不需要修改 Decoder 接口。Doris 现有的 `is_dict_filter=true` + `_decode_dict_values` 已经能输出 dict codes。

#### 步骤 3：在 LazyReadContext 中标记可延迟解码的列

**文件**: `be/src/vec/exec/format/parquet/vparquet_group_reader.h`

在 `LazyReadContext` 中新增：

```cpp
struct LazyReadContext {
    // ... 现有成员 ...

    // Lazy Dict Decode：可以延迟字典解码的 lazy 列
    // (col_name, slot_id) 对
    std::vector<std::pair<std::string, int>> lazy_dict_decode_columns;
};
```

#### 步骤 4：在 set_fill_columns 中识别可延迟解码的列

**文件**: `be/src/vec/exec/format/parquet/vparquet_reader.cpp`

在 `set_fill_columns()` 分类 lazy 列时，检查是否满足 lazy dict decode 条件：

```cpp
// 在 lazy_read_columns 分类之后
for (auto& lazy_col : _lazy_read_ctx.lazy_read_columns) {
    // 条件: string 类型列
    // 全字典编码在 RowGroupReader::init() 时才能确认
    const auto& slot_desc = _get_slot_desc_by_name(lazy_col);
    if (slot_desc && slot_desc->type().is_string_type()) {
        _lazy_read_ctx.lazy_dict_decode_candidates.push_back(
            {lazy_col, slot_desc->id()});
    }
}
```

#### 步骤 5：在 RowGroupReader::init() 中确认全字典编码

**文件**: `be/src/vec/exec/format/parquet/vparquet_group_reader.cpp`

```cpp
// 在 _column_readers 创建之后
for (auto& [col_name, slot_id] : _lazy_read_ctx.lazy_dict_decode_candidates) {
    auto it = _column_readers.find(col_name);
    if (it != _column_readers.end()) {
        const auto& column_metadata = _get_column_metadata(col_name);
        // 复用已有的 _can_filter_by_dict 中的字典编码检查逻辑
        if (column_metadata.encoding_stats.has_value()) {
            bool all_dict = true;
            for (auto& stat : column_metadata.encoding_stats.value()) {
                if (stat.page_type == tparquet::PageType::DATA_PAGE ||
                    stat.page_type == tparquet::PageType::DATA_PAGE_V2) {
                    if (stat.encoding != tparquet::Encoding::PLAIN_DICTIONARY &&
                        stat.encoding != tparquet::Encoding::RLE_DICTIONARY) {
                        all_dict = false;
                        break;
                    }
                }
            }
            if (all_dict) {
                _lazy_read_ctx.lazy_dict_decode_columns.push_back({col_name, slot_id});
            }
        }
    }
}
```

#### 步骤 6：修改 _do_lazy_read() 中 lazy 列的读取

**文件**: `be/src/vec/exec/format/parquet/vparquet_group_reader.cpp`

在 Phase 2（读取 lazy 列）中，对 `lazy_dict_decode_columns` 中的列使用 dict code 模式读取：

```cpp
// Phase 2: 读取 lazy 列
// 先决策：是否使用 lazy dict decode（基于选择率）
bool use_lazy_dict_decode = false;
if (!_lazy_read_ctx.lazy_dict_decode_columns.empty() && filter_map.has_filter()) {
    double selectivity = 1.0 - filter_map.filter_ratio();
    use_lazy_dict_decode = (selectivity < 0.2);  // 存活率 < 20%
}

if (use_lazy_dict_decode) {
    // 分两组读取 lazy 列
    std::vector<std::string> normal_lazy_cols;
    std::vector<std::string> dict_decode_lazy_cols;
    std::set<std::string> dict_decode_set;
    for (auto& [name, _] : _lazy_read_ctx.lazy_dict_decode_columns) {
        dict_decode_set.insert(name);
    }
    for (auto& col : _lazy_read_ctx.lazy_read_columns) {
        if (dict_decode_set.count(col)) {
            dict_decode_lazy_cols.push_back(col);
        } else {
            normal_lazy_cols.push_back(col);
        }
    }

    // 读取普通 lazy 列（原有路径）
    if (!normal_lazy_cols.empty()) {
        _read_column_data(block, normal_lazy_cols, pre_read_rows,
                          &lazy_read_rows, &lazy_eof, filter_map);
    }

    // 读取 dict decode lazy 列（dict code 模式）
    for (auto& col_name : dict_decode_lazy_cols) {
        // 替换 block 中列类型为 Int32
        // （复用现有的 dict filter 列类型替换逻辑）
        _replace_column_type_to_dict_code(block, col_name);
    }
    _read_column_data(block, dict_decode_lazy_cols, pre_read_rows,
                      &lazy_read_rows, &lazy_eof, filter_map,
                      /*is_dict_filter=*/true);
} else {
    // 原有路径：直接读取所有 lazy 列
    _read_column_data(block, _lazy_read_ctx.lazy_read_columns,
                      pre_read_rows, &lazy_read_rows, &lazy_eof, filter_map);
}
```

#### 步骤 7：在过滤后转换 dict codes 到 strings

在 `_do_lazy_read()` 的后续代码中（Phase 4，过滤后处理），添加 dict code 列的转换：

```cpp
// 过滤 block
Block::filter_block_internal(block, filter_columns, result_filter);

// 转换 dict filter 列（已有逻辑）
_convert_dict_cols_to_string_cols(block);

// 转换 lazy dict decode 列（新增）
if (use_lazy_dict_decode) {
    _convert_lazy_dict_cols_to_string_cols(block);
}
```

新增方法：

```cpp
void RowGroupReader::_convert_lazy_dict_cols_to_string_cols(Block* block) {
    for (auto& [col_name, slot_id] : _lazy_read_ctx.lazy_dict_decode_columns) {
        // 找到 block 中对应的列
        auto col_idx = block->get_position_by_name(col_name);
        auto& col_type_name = block->get_by_position(col_idx);
        const auto& column = col_type_name.column;

        // 提取 ColumnInt32（可能是 Nullable 包装的）
        const ColumnInt32* dict_column = nullptr;
        ColumnPtr null_column = nullptr;
        if (auto* nullable = check_and_get_column<ColumnNullable>(*column)) {
            dict_column = assert_cast<const ColumnInt32*>(
                nullable->get_nested_column_ptr().get());
            null_column = nullable->get_null_map_column_ptr();
        } else {
            dict_column = assert_cast<const ColumnInt32*>(column.get());
        }

        // 调用 column reader 的字典转换
        MutableColumnPtr string_col =
            _column_readers[col_name]->convert_dict_column_to_string_column(dict_column);

        // 替换回 block
        if (null_column) {
            col_type_name.type = make_nullable(std::make_shared<DataTypeString>());
            block->replace_by_position(col_idx,
                ColumnNullable::create(std::move(string_col),
                                       null_column->clone_resized(string_col->size())));
        } else {
            col_type_name.type = std::make_shared<DataTypeString>();
            block->replace_by_position(col_idx, std::move(string_col));
        }
    }
}
```

#### 步骤 8：添加辅助方法

```cpp
// 替换 block 中列类型为 dict code (Int32)
void RowGroupReader::_replace_column_type_to_dict_code(Block* block,
                                                        const std::string& col_name) {
    auto col_idx = block->get_position_by_name(col_name);
    auto& col_type_name = block->get_by_position(col_idx);
    bool is_nullable = col_type_name.type->is_nullable();
    if (is_nullable) {
        col_type_name.type = make_nullable(std::make_shared<DataTypeInt32>());
        auto null_col = ColumnUInt8::create();
        col_type_name.column = ColumnNullable::create(ColumnInt32::create(), std::move(null_col));
    } else {
        col_type_name.type = std::make_shared<DataTypeInt32>();
        col_type_name.column = ColumnInt32::create();
    }
}
```

### 3.3 涉及修改的文件清单

| 文件 | 修改内容 |
|------|----------|
| `be/src/vec/exec/format/parquet/vparquet_group_reader.h` | `LazyReadContext` 添加 `lazy_dict_decode_columns`；`RowGroupReader` 新增相关方法声明 |
| `be/src/vec/exec/format/parquet/vparquet_group_reader.cpp` | `init()` 中识别可延迟解码列；`_do_lazy_read()` Phase 2 分路径处理；新增 `_convert_lazy_dict_cols_to_string_cols()`、`_replace_column_type_to_dict_code()` |
| `be/src/vec/exec/format/parquet/vparquet_reader.cpp` | `set_fill_columns()` 中标记候选 lazy dict decode 列 |

### 3.4 StarRocks 参考

- **ColumnContentType 枚举**: `utils.h:30`，`VALUE` vs `DICT_CODE`
- **决策逻辑**: `scalar_column_reader.cpp:453-467`，`_need_lazy_decode` 基于 `_can_lazy_dict_decode && filter && selectivity < 0.2`
- **临时列切换**: `scalar_column_reader.cpp:504-545`，`dst = _tmp_code_column` 重定向输出到 Int32 列
- **延迟解码**: `scalar_column_reader.cpp:567-591`，`_dict_decode()` 在 `_fill_dst_column_impl` 中执行
- **条件判断**: `scalar_column_reader.h:161-164`，`_can_lazy_dict_decode = can_lazy_decode && is_string_type() && all_pages_dict_encoded()`

### 3.5 与现有 Dict Filter 的关系

| 维度 | 现有 Dict Filter | 新增 Lazy Dict Decode |
|------|------------------|----------------------|
| **适用列** | 有 IN/EQ 谓词的 string 列 | 无谓词的 lazy string 列 |
| **触发条件** | 谓词类型匹配 + 全字典编码 | 全字典编码 + 选择率 < 20% |
| **机制** | 谓词改写（String → Int32 谓词） | 延迟物化（先读 codes，过滤后再转 string） |
| **转换时机** | `_convert_dict_cols_to_string_cols` | `_convert_lazy_dict_cols_to_string_cols` |
| **互不冲突** | 作用于 predicate columns | 作用于 lazy columns |

两者可以并行工作：谓词列使用 Dict Filter，非谓词 lazy 列使用 Lazy Dict Decode。

### 3.6 注意事项

1. **非全字典编码的列**：Parquet 允许同一列的不同 page 使用不同编码（字典增长超限时回退到 PLAIN）。必须确认该列所有数据页都是字典编码，否则 DICT_CODE 模式会失败。

2. **Converter 兼容性**：`PhysicalToLogicalConverter` 在 `is_dict_filter=true` 时跳过类型转换。需确认 lazy 列走 dict code 路径时 converter 行为正确。

3. **选择率阈值**：与 P0-1 统一使用 0.2（20%）作为阈值。可通过配置参数调整。

4. **内存开销**：dict code 列 (Int32) 比实际 string 列小得多，不会增加内存压力。转换发生在过滤之后，此时行数已大幅减少。

---

## 总结：三个 P0 优化的协同效果

在一个典型的低选择率分析查询中（如 `SELECT * FROM t WHERE string_col = 'value' AND int_col > 100`，选择率 5%）：

```
原有流程：
  1. 读 string_col 的全部 1M 行（dict decode → string copy）
  2. 读 int_col 的全部 1M 行
  3. 执行 filter → 存活 50K 行
  4. 读 lazy 列的全部 1M 行
  5. 过滤 lazy 列到 50K 行

P0-1 (Filter 下推) + P0-2 (列顺序优化) + P0-3 (Lazy Dict Decode)：
  1. 先读 string_col（选择率高的列先读）→ 50K 行存活
  2. 带 filter 读 int_col（仅 50K 行物化）→ 45K 行存活
  3. 读 lazy string 列为 dict codes (int32) → 仅 45K 行读取
  4. 过滤后只对 45K 行做 dict code → string 转换
```

**估算收益**：
- **P0-1**: dict 解码热路径减少 80% 无用 dict lookup（大字典时效果更明显）
- **P0-2**: 第二个谓词列只需解码 5% 的行（95% 被第一列过滤）
- **P0-3**: lazy string 列只转换 4.5% 的行，省去 95.5% 的 string copy

三者叠加，在典型多列低选择率查询中可达到 **3-10x** 的纯读取层性能提升。

---

## P0-1 测试与验证方案

### T1. 正确性验证

#### T1.1 已有单元测试基线

**文件**: `be/test/vec/exec/format/parquet/byte_array_dict_decoder_test.cpp`

已有 10 个测试用例覆盖了以下场景：
- `test_decode_values`: 基本字典解码
- `test_decode_values_with_filter`: 带 filter 的解码
- `test_decode_values_with_filter_and_null`: 带 filter + null 的解码
- `test_decode_values_to_column_dict_i32`: 输出 dict codes 到 ColumnDictI32
- `test_decode_values_to_column_int32`: 输出 dict codes 到 ColumnInt32
- `test_skip_values`: 跳过值

**修改后必须确保所有已有测试通过**。

#### T1.2 新增 P0-1 正确性测试用例

在 `byte_array_dict_decoder_test.cpp` 和 `fix_length_dict_decoder_test.cpp`（如不存在则新建）中新增以下测试：

```cpp
// 1. filter bitmap 下推 —— 低选择率场景
TEST_F(ByteArrayDictDecoderTest, test_decode_with_filter_bitmap_low_selectivity) {
    // 构造 1000 行数据，只有 5% 存活（filter bitmap 中 50 个 1）
    // 验证：输出列内容与不使用 filter bitmap 的结果完全一致
    // 验证：CONTENT run 中 filter[i]=0 的行位置数据正确（值可以是任意的，但列长度正确）
}

// 2. filter bitmap 下推 —— 高选择率场景（不应下推）
TEST_F(ByteArrayDictDecoderTest, test_decode_with_filter_bitmap_high_selectivity) {
    // 构造 1000 行数据，80% 存活
    // 验证：selectivity > 0.2 时 filter_data 不传入 decoder
    // 验证：结果与原有路径一致
}

// 3. filter bitmap + null 混合
TEST_F(ByteArrayDictDecoderTest, test_decode_with_filter_bitmap_and_nulls) {
    // 构造含 null 的数据，filter bitmap 与 null map 交叉
    // 验证：null 行不受 filter bitmap 影响
    // 验证：CONTENT 中 filter[i]=1 的非 null 行正确解码
}

// 4. RleBatchDecoder::SkipBatch 正确性
TEST_F(RleBatchDecoderTest, test_skip_batch) {
    // 构造 RLE 编码数据（混合 RLE run + literal run）
    // 执行 SkipBatch(n) 后继续 GetBatch()
    // 验证：GetBatch() 返回的值与跳过后预期位置的值一致
}

// 5. BaseDictDecoder::skip_values 使用 SkipBatch
TEST_F(ByteArrayDictDecoderTest, test_skip_values_with_skip_batch) {
    // 跳过若干值后继续解码
    // 验证：结果与旧实现（分配 buffer + GetBatch 丢弃）完全一致
}

// 6. 边界情况：全部被过滤
TEST_F(ByteArrayDictDecoderTest, test_decode_with_filter_bitmap_all_filtered) {
    // filter bitmap 全 0
    // 验证：不 crash，列长度正确
}

// 7. 边界情况：全部存活
TEST_F(ByteArrayDictDecoderTest, test_decode_with_filter_bitmap_all_pass) {
    // filter bitmap 全 1
    // 验证：结果与无 filter bitmap 完全一致
}
```

#### T1.3 FixLengthDictDecoder 的对应测试

在 `fix_length_dict_decoder_test.cpp` 中新增类似测试，覆盖 INT32/INT64/FLOAT/DOUBLE 等定长类型的 filter bitmap 下推。

### T2. 性能验证

#### T2.1 方法一：Profile Counters（最简单）

Doris 已有 Query Profile 机制，相关计数器定义在 `be/src/vec/exec/format/parquet/vparquet_reader.h`：

```
decode_value_time    — Decoder 解码耗时（核心指标）
column_read_time     — 列读取总耗时
decode_dict_time     — 字典解码耗时
predicate_filter_time — 谓词过滤耗时
lazy_read_filtered_rows — 懒加载跳过行数
```

**测试步骤**：

```sql
-- 1. 准备测试表：字典编码 string 列 + 低选择率谓词
CREATE TABLE test_parquet_filter AS
SELECT * FROM parquet_file("path/to/large_dict_file.parquet");

-- 2. 关闭优化，记录 baseline
SET parquet_push_down_filter_to_decoder_enable = false;
SELECT count(*) FROM test_parquet_filter WHERE string_col = 'rare_value';
-- 查看 Profile 中 decode_value_time

-- 3. 开启优化，对比
SET parquet_push_down_filter_to_decoder_enable = true;
SELECT count(*) FROM test_parquet_filter WHERE string_col = 'rare_value';
-- 查看 Profile 中 decode_value_time
```

**预期**：`decode_value_time` 在低选择率（< 20%）场景下降低 30-80%。

#### T2.2 方法二：Microbenchmark（最精确）

新建 `be/test/vec/exec/format/parquet/decoder_benchmark.cpp`，使用 Google Benchmark 框架：

```cpp
#include <benchmark/benchmark.h>

// 测试矩阵：dict_size × selectivity × type
// dict_size: 100, 1000, 10000, 100000（模拟 L2 cache 内/外）
// selectivity: 0.01, 0.05, 0.1, 0.2, 0.5, 1.0
// type: INT32, INT64, STRING

static void BM_DictDecode_NoFilter(benchmark::State& state) {
    int dict_size = state.range(0);
    double selectivity = state.range(1) / 100.0;
    // 构造 dict decoder + 1M 行 RLE 数据
    // 构造 ColumnSelectVector（有 FILTERED_CONTENT runs）
    for (auto _ : state) {
        // 调用 decode_values(..., filter_data = nullptr)
    }
    state.SetItemsProcessed(state.iterations() * 1000000);
}

static void BM_DictDecode_WithFilter(benchmark::State& state) {
    int dict_size = state.range(0);
    double selectivity = state.range(1) / 100.0;
    // 同上，但传入 filter_data
    for (auto _ : state) {
        // 调用 decode_values(..., filter_data = bitmap)
    }
    state.SetItemsProcessed(state.iterations() * 1000000);
}

// 测试矩阵
BENCHMARK(BM_DictDecode_NoFilter)
    ->Args({100, 5})     // 小字典, 5% 选择率
    ->Args({100, 50})    // 小字典, 50% 选择率
    ->Args({100000, 5})  // 大字典, 5% 选择率
    ->Args({100000, 50}); // 大字典, 50% 选择率

BENCHMARK(BM_DictDecode_WithFilter)
    ->Args({100, 5})
    ->Args({100, 50})
    ->Args({100000, 5})
    ->Args({100000, 50});
```

**预期结果矩阵**：

| 字典大小 | 选择率 | WithFilter vs NoFilter |
|---------|--------|----------------------|
| 100（L2 内）| 5% | 持平或略优（dict lookup 本身很快） |
| 100（L2 内）| 50% | 持平（不应下推） |
| 100K（L2 外）| 5% | **显著提升 3-5x**（减少大量 cache miss） |
| 100K（L2 外）| 50% | 略有提升 |

#### T2.3 方法三：端到端 SQL 测试（最贴近生产）

准备测试数据集：

```bash
# 生成测试 Parquet 文件
# - 10M 行
# - string_col: 字典编码，字典大小 50000（超过 L2 cache）
# - int_col: 普通 INT32
# - 谓词 string_col = 'value_42' 选择率约 0.002%

python3 generate_test_parquet.py \
    --rows 10000000 \
    --dict-size 50000 \
    --output /path/to/test_large_dict.parquet
```

**测试 SQL**：

```sql
-- Case 1: 低选择率 string 谓词（最大收益场景）
SELECT count(*), sum(int_col)
FROM parquet_file("/path/to/test_large_dict.parquet")
WHERE string_col = 'value_42';

-- Case 2: 多列低选择率谓词
SELECT count(*)
FROM parquet_file("/path/to/test_large_dict.parquet")
WHERE string_col IN ('value_1', 'value_2', 'value_3')
  AND int_col > 900000;

-- Case 3: 高选择率谓词（应无差异，验证不退化）
SELECT count(*)
FROM parquet_file("/path/to/test_large_dict.parquet")
WHERE int_col > 0;  -- 几乎全部存活
```

### T3. 关键观测指标

| 指标 | 获取方式 | 预期变化 |
|------|---------|---------|
| `decode_value_time` | Query Profile | 低选择率场景降低 30-80% |
| `column_read_time` | Query Profile | 随 decode_value_time 降低 |
| 查询总延迟 | SQL 客户端 | 取决于 decode 在总耗时中的占比 |
| L2 cache miss | `perf stat -e cache-misses` | 大字典场景显著降低 |
| 内存分配 | `skip_values` 路径 | 消除 `_indexes.resize()` 分配 |

### T4. 验证执行顺序

1. **单元测试**（T1）：实现后第一时间运行，确保功能正确
   ```bash
   cd be && ./run_ut.sh --test ByteArrayDictDecoderTest
   cd be && ./run_ut.sh --test FixLengthDictDecoderTest
   ```

2. **Microbenchmark**（T2.2）：确认性能数据符合预期
   ```bash
   cd be && ./run_benchmark.sh decoder_benchmark
   ```

3. **回归测试**：运行完整 Parquet 读取相关回归测试
   ```bash
   cd regression-test && ./run.sh -s external_table_p0/parquet
   ```

4. **端到端 SQL**（T2.3）：在测试环境中执行，对比 Profile

5. **（可选）perf stat**：验证 cache miss 降低
   ```bash
   perf stat -e cache-references,cache-misses,L1-dcache-load-misses \
       doris_be --query "SELECT count(*) FROM ... WHERE ..."
   ```

### T5. 新增 Profile Counter（建议）

为更精确追踪 P0-1 的效果，建议在 `ReaderStatistics` 中新增计数器：

```cpp
// be/src/vec/exec/format/parquet/vparquet_reader.h
struct ReaderStatistics {
    // ... 现有计数器 ...

    // P0-1 新增
    int64_t filter_bitmap_pushdown_count = 0;   // filter bitmap 下推次数
    int64_t filter_bitmap_skipped_lookups = 0;  // 跳过的 dict lookup 次数
    int64_t rle_skip_batch_count = 0;           // SkipBatch 调用次数
};
```

对应的 Profile 名称：
- `FilterBitmapPushdownCount`
- `FilterBitmapSkippedLookups`
- `RLESkipBatchCount`

这些计数器可以在 Query Profile 中直观展示优化的触发频率和效果。
