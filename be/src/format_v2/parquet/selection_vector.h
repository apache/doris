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

inline int64_t count_range_rows(const std::vector<RowRange>& ranges) {
    int64_t rows = 0;
    for (const auto& range : ranges) {
        rows += range.length;
    }
    return rows;
}

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
        _identity = data == nullptr;
        _mutable_data_exposed = data != nullptr;
        ++_generation;
    }

    void resize(size_t count) {
        // Identity is the overwhelmingly common initial state. Keep it implicit until a caller
        // actually changes an index, avoiding one write per source row for every scanner batch.
        // Scanner batches repeatedly reuse this object. Retaining the initialized high-water mark
        // avoids value-initializing the entire scratch vector before every sparse compaction.
        _data = nullptr;
        _size = count;
        _identity = true;
        _mutable_data_exposed = false;
        ++_generation;
    }

    void clear() {
        _owned.clear();
        _data = nullptr;
        _size = 0;
        _identity = true;
        _mutable_data_exposed = false;
        ++_generation;
    }

    size_t size() const { return _size; }

    bool is_set() const { return _data != nullptr; }

    Index* data() {
        _materialize_identity();
        // A mutable pointer can change indices without set_index(), so identity can no longer be
        // proven until resize() rebuilds it. This keeps the O(1) dense fast path conservative.
        _identity = false;
        _mutable_data_exposed = true;
        ++_generation;
        return _data;
    }

    const Index* data() const { return _data; }

    size_t get_index(size_t idx) const {
        if (_data == nullptr) {
            return idx;
        }
        return _data[idx];
    }

    void set_index(size_t idx, Index value) {
        _materialize_identity();
        _data[idx] = value;
        if (value != idx) {
            _identity = false;
        }
        ++_generation;
    }

    size_t compact_with_row_filter(const uint8_t* filter, size_t count) {
        return _compact(filter, count, true);
    }

    size_t compact_with_selection_filter(const uint8_t* filter, size_t count) {
        return _compact(filter, count, false);
    }

    Status materialize_filter(size_t count, int64_t batch_rows, const uint8_t** filter) const {
        DORIS_CHECK(filter != nullptr);
        if (batch_rows >= 0 && std::cmp_equal(count, batch_rows) && _identity &&
            (_size == 0 || count <= _size)) {
            // A proven identity selection is equivalent to no FilterMap. Returning nullptr avoids
            // constructing and rescanning one dense byte per source row.
            *filter = nullptr;
            return Status::OK();
        }
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
        if (_data == nullptr && _size != 0 && count > _size) {
            return Status::InvalidArgument("Parquet selection count {} exceeds vector size {}",
                                           count, _size);
        }
        if (_identity) {
            return Status::OK();
        }
        if (!_mutable_data_exposed && _verified_generation == _generation &&
            _verified_count == count && _verified_batch_rows == batch_rows) {
            return Status::OK();
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
        if (!_mutable_data_exposed) {
            _verified_generation = _generation;
            _verified_count = count;
            _verified_batch_rows = batch_rows;
        }
        return Status::OK();
    }

private:
    void _materialize_identity() {
        if (_data != nullptr) {
            return;
        }
        if (_owned.size() < _size) {
            _owned.resize(_size);
        }
        _data = _owned.data();
        for (size_t idx = 0; idx < _size; ++idx) {
            _data[idx] = static_cast<Index>(idx);
        }
    }

    size_t _compact(const uint8_t* filter, size_t count, bool filter_uses_row_index) {
        DORIS_CHECK(filter != nullptr);
        DORIS_CHECK(count <= _size);
        Index* source = _data;
        if (_data == nullptr) {
            if (_owned.size() < _size) {
                _owned.resize(_size);
            }
            _data = _owned.data();
            // An implicit identity maps both filter coordinate systems to the same position.
            // Specialize this first compaction so split batches do not pay source/coordinate
            // branches for every row after already avoiding identity materialization.
            size_t output = 0;
            while (output < count && filter[output] != 0) {
                _data[output] = static_cast<Index>(output);
                ++output;
            }
            bool remains_identity = true;
            for (size_t position = output; position < count; ++position) {
                if (filter[position] != 0) {
                    _data[output++] = static_cast<Index>(position);
                    remains_identity = false;
                }
            }
            _identity = remains_identity;
            ++_generation;
            return output;
        }
        size_t output = 0;
        bool remains_identity = true;
        for (size_t position = 0; position < count; ++position) {
            const Index row = source == nullptr ? static_cast<Index>(position) : source[position];
            const size_t filter_position = filter_uses_row_index ? row : position;
            if (filter[filter_position] != 0) {
                _data[output] = row;
                remains_identity &= row == output;
                ++output;
            }
        }
        // Compaction is one logical mutation. Invalidating caches once is important when several
        // predicates successively refine a wide batch.
        _identity = remains_identity;
        ++_generation;
        return output;
    }

    std::vector<Index> _owned;
    Index* _data = nullptr;
    size_t _size = 0;
    bool _identity = true;
    bool _mutable_data_exposed = false;
    uint64_t _generation = 0;
    mutable std::vector<uint8_t> _filter;
    mutable uint64_t _filter_generation = std::numeric_limits<uint64_t>::max();
    mutable size_t _filter_count = 0;
    mutable int64_t _filter_batch_rows = -1;
    mutable uint64_t _verified_generation = std::numeric_limits<uint64_t>::max();
    mutable size_t _verified_count = 0;
    mutable int64_t _verified_batch_rows = -1;
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
