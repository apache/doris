// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <cstddef>
#include <cstdint>
#include <limits>
#include <utility>
#include <vector>

#include "common/check.h"
#include "common/status.h"

namespace doris::format::parquet {

struct RowRange {
    int64_t start = 0;
    int64_t length = 0;
};

struct ParquetPageSkipPlan {
    int leaf_column_id = -1;
    // Page ordinal is the data-page ordinal in the column chunk. It intentionally excludes
    // dictionary pages, matching the native page reader's ordinal domain.
    std::vector<uint8_t> skipped_pages;
    std::vector<int64_t> skipped_page_compressed_sizes;
    // Row ranges covered by skipped data pages. NativeColumnReader uses these ranges to avoid
    // consuming logical rows twice after the page reader has already skipped their payload.
    std::vector<RowRange> skipped_ranges;

    bool empty() const { return skipped_ranges.empty(); }

    bool should_skip_page(size_t page_idx) const {
        return page_idx < skipped_pages.size() && skipped_pages[page_idx] != 0;
    }

    int64_t skipped_page_compressed_size(size_t page_idx) const {
        DCHECK_LT(page_idx, skipped_page_compressed_sizes.size());
        return skipped_page_compressed_sizes[page_idx];
    }
};

class SelectionVector {
public:
    using Index = uint16_t;

    SelectionVector() = default;

    explicit SelectionVector(size_t count) { resize(count); }

    SelectionVector(Index* data, size_t count) { initialize(data, count); }

    void initialize(Index* data, size_t count) {
        _owned.clear();
        _data = data;
        _size = count;
        ++_generation;
    }

    void resize(size_t count) {
        _owned.resize(count);
        _data = _owned.data();
        _size = count;
        for (size_t idx = 0; idx < count; ++idx) {
            _data[idx] = static_cast<Index>(idx);
        }
        ++_generation;
    }

    void clear() {
        _owned.clear();
        _data = nullptr;
        _size = 0;
        ++_generation;
    }

    size_t size() const { return _size; }

    bool is_set() const { return _data != nullptr; }

    Index* data() { return _data; }

    const Index* data() const { return _data; }

    size_t get_index(size_t idx) const {
        if (_data == nullptr) {
            return idx;
        }
        return _data[idx];
    }

    void set_index(size_t idx, Index value) {
        _data[idx] = value;
        ++_generation;
    }

    Status materialize_filter(size_t count, int64_t batch_rows, const uint8_t** filter) const {
        DORIS_CHECK(filter != nullptr);
        RETURN_IF_ERROR(verify(count, batch_rows));
        if (_filter_generation != _generation || _filter_count != count ||
            _filter_batch_rows != batch_rows) {
            // Selection is shared by all readers in one scheduler batch. Cache its dense bitmap so
            // a wide lazy projection does not rebuild the same O(batch_rows) filter per column.
            _filter.assign(static_cast<size_t>(batch_rows), 0);
            for (size_t idx = 0; idx < count; ++idx) {
                _filter[get_index(idx)] = 1;
            }
            _filter_generation = _generation;
            _filter_count = count;
            _filter_batch_rows = batch_rows;
        }
        *filter = _filter.data();
        return Status::OK();
    }

    Status verify(size_t count, int64_t batch_rows) const {
        if (batch_rows < 0) {
            return Status::InvalidArgument("Negative parquet selection batch rows {}", batch_rows);
        }
        if (std::cmp_greater(count, batch_rows)) {
            return Status::InvalidArgument("Parquet selection count {} exceeds batch rows {}",
                                           count, batch_rows);
        }
        if (_data != nullptr && count > _size) {
            return Status::InvalidArgument("Parquet selection count {} exceeds vector size {}",
                                           count, _size);
        }
        size_t previous = 0;
        for (size_t idx = 0; idx < count; ++idx) {
            const size_t current = get_index(idx);
            if (std::cmp_greater_equal(current, batch_rows)) {
                return Status::InvalidArgument(
                        "Parquet selection index {} out of range [0, {}) at position {}", current,
                        batch_rows, idx);
            }
            if (idx > 0 && current <= previous) {
                return Status::InvalidArgument(
                        "Parquet selection index {} is not strictly greater than previous {} at "
                        "position {}",
                        current, previous, idx);
            }
            previous = current;
        }
        return Status::OK();
    }

private:
    std::vector<Index> _owned;
    Index* _data = nullptr;
    size_t _size = 0;
    uint64_t _generation = 0;
    mutable std::vector<uint8_t> _filter;
    mutable uint64_t _filter_generation = std::numeric_limits<uint64_t>::max();
    mutable size_t _filter_count = 0;
    mutable int64_t _filter_batch_rows = -1;
};

inline void selection_to_ranges(const SelectionVector& selection, uint16_t selected_rows,
                                std::vector<RowRange>* ranges) {
    DORIS_CHECK(ranges != nullptr);
    ranges->clear();
    if (selected_rows == 0) {
        return;
    }

    int64_t range_start = selection.get_index(0);
    int64_t previous = selection.get_index(0);
    for (uint16_t selection_idx = 1; selection_idx < selected_rows; ++selection_idx) {
        const int64_t current = selection.get_index(selection_idx);
        if (current == previous + 1) {
            previous = current;
            continue;
        }
        ranges->push_back(RowRange {.start = range_start, .length = previous - range_start + 1});
        range_start = current;
        previous = current;
    }
    ranges->push_back(RowRange {.start = range_start, .length = previous - range_start + 1});
}

inline std::vector<RowRange> selection_to_ranges(const SelectionVector& selection,
                                                 uint16_t selected_rows) {
    std::vector<RowRange> ranges;
    selection_to_ranges(selection, selected_rows, &ranges);
    return ranges;
}

} // namespace doris::format::parquet
