// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <cstddef>
#include <cstdint>
#include <vector>

#include "common/status.h"

namespace doris::parquet {

struct RowRange {
    int64_t start = 0;
    int64_t length = 0;
};

// 类似 DuckDB SelectionVector 的轻量行号视图。
// 它只表达一个 batch 内被选中的 row offset，不持有 table/global schema 语义。
// 未绑定 data 时表示 identity selection：get_index(i) == i。
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
    }

    void resize(size_t count) {
        _owned.resize(count);
        _data = _owned.data();
        _size = count;
        for (size_t idx = 0; idx < count; ++idx) {
            _data[idx] = static_cast<Index>(idx);
        }
    }

    void clear() {
        _owned.clear();
        _data = nullptr;
        _size = 0;
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

    void set_index(size_t idx, Index value) { _data[idx] = value; }

    Status verify(size_t count, int64_t batch_rows) const {
        if (batch_rows < 0) {
            return Status::InvalidArgument("Negative parquet selection batch rows {}", batch_rows);
        }
        if (count > static_cast<size_t>(batch_rows)) {
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
            if (current >= static_cast<size_t>(batch_rows)) {
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
};

inline std::vector<RowRange> selection_to_ranges(const SelectionVector& selection,
                                                 uint16_t selected_rows) {
    std::vector<RowRange> ranges;
    if (selected_rows == 0) {
        return ranges;
    }

    int64_t range_start = selection.get_index(0);
    int64_t previous = selection.get_index(0);
    for (uint16_t selection_idx = 1; selection_idx < selected_rows; ++selection_idx) {
        const int64_t current = selection.get_index(selection_idx);
        if (current == previous + 1) {
            previous = current;
            continue;
        }
        ranges.push_back(RowRange {range_start, previous - range_start + 1});
        range_start = current;
        previous = current;
    }
    ranges.push_back(RowRange {range_start, previous - range_start + 1});
    return ranges;
}

} // namespace doris::parquet
