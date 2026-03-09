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

#include "vec/columns/column_varbinary.h"

#include <glog/logging.h>

#include <cstddef>
#include <cstdint>

#include "runtime/primitive_type.h"
#include "vec/columns/column.h"
#include "vec/columns/column_string.h"
#include "vec/columns/columns_common.h"
#include "vec/common/arena.h"
#include "vec/common/assert_cast.h"
#include "vec/core/sort_block.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"
MutableColumnPtr ColumnVarbinary::clone_resized(size_t size) const {
    auto res = create();
    if (size > 0) {
        auto& new_col = assert_cast<Self&>(*res);
        size_t count = std::min(this->size(), size);
        for (size_t i = 0; i < count; ++i) {
            auto value = this->get_data_at(i);
            new_col.insert_data(value.data, value.size);
        }
        if (size > count) {
            new_col.insert_many_defaults(size - count);
        }
    }
    return res;
}

void ColumnVarbinary::insert_range_from(const IColumn& src, size_t start, size_t length) {
    if (length == 0) {
        return;
    }
    const auto& src_col = assert_cast<const ColumnVarbinary&>(src);

    if (start + length > src_col.size()) {
        throw doris::Exception(doris::ErrorCode::INTERNAL_ERROR,
                               "Parameters start = {}, length = {} are out of bound in "
                               "ColumnVarbinary::insert_range_from method (data.size() = {})",
                               start, length, src_col.size());
    }

    for (size_t i = 0; i < length; ++i) {
        auto value = src_col.get_data_at(start + i);
        insert_data(value.data, value.size);
    }
}

void ColumnVarbinary::insert_indices_from(const IColumn& src, const uint32_t* indices_begin,
                                          const uint32_t* indices_end) {
    const Self& src_vec = assert_cast<const Self&>(src);
    auto new_size = indices_end - indices_begin;

    for (uint32_t i = 0; i < new_size; ++i) {
        auto offset = *(indices_begin + i);
        auto value = src_vec.get_data_at(offset);
        insert_data(value.data, value.size);
    }
}

ColumnPtr ColumnVarbinary::filter(const IColumn::Filter& filt, ssize_t result_size_hint) const {
    size_t size = this->size();
    column_match_filter_size(size, filt.size());

    if (_data.size() == 0) {
        return doris::vectorized::ColumnVarbinary::create();
    }
    auto res = doris::vectorized::ColumnVarbinary::create();
    Container& res_data = res->get_data();

    if (result_size_hint) {
        res_data.reserve(result_size_hint > 0 ? result_size_hint : size);
    }

    const UInt8* filt_pos = filt.data();
    const UInt8* filt_end = filt_pos + size;
    const auto* data_pos = _data.data();

    while (filt_pos < filt_end) {
        if (*filt_pos) {
            res->insert_data(data_pos->data(), data_pos->size());
        }
        ++filt_pos;
        ++data_pos;
    }
    return res;
};

size_t ColumnVarbinary::filter(const IColumn::Filter& filter) {
    size_t pos = 0;
    const Self& src_vec = assert_cast<const Self&>(*this);
    for (size_t i = 0; i < filter.size(); i++) {
        if (filter[i]) {
            if (src_vec.get_data()[i].isInline()) {
                _data[pos] = src_vec.get_data()[i];
            } else {
                auto val = src_vec.get_data()[i];
                const auto* dst = _arena.insert(val.data(), val.size());
                _data[pos] = doris::StringView(dst, val.size());
            }
            pos++;
        }
    }
    resize(pos);
    return pos;
};

MutableColumnPtr ColumnVarbinary::permute(const IColumn::Permutation& perm, size_t limit) const {
    size_t size = _data.size();

    limit = limit ? std::min(size, limit) : size;

    if (perm.size() < limit) {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                               "Size of permutation is less than required.");
        __builtin_unreachable();
    }

    auto res = doris::vectorized::ColumnVarbinary::create(limit);
    typename Self::Container& res_data = res->get_data();
    for (size_t i = 0; i < limit; ++i) {
        auto val = _data[perm[i]];
        if (val.isInline()) {
            res_data[i] = val;
            continue;
        }
        const auto* dst = res->_arena.insert(val.data(), val.size());
        res_data[i] = doris::StringView(dst, val.size());
    }

    return res;
};

void ColumnVarbinary::replace_column_data(const IColumn& rhs, size_t row, size_t self_row) {
    DCHECK(this->size() > self_row);
    const auto& rhs_col = assert_cast<const Self&, TypeCheckOnRelease::DISABLE>(rhs);
    auto val = rhs_col.get_data()[row];
    if (val.isInline()) {
        _data[self_row] = val;
        return;
    }
    const auto* dst = _arena.insert(val.data(), val.size());
    _data[self_row] = doris::StringView(dst, val.size());
}

size_t ColumnVarbinary::get_max_row_byte_size() const {
    uint32_t max_size = 0;
    size_t num_rows = size();
    for (size_t i = 0; i < num_rows; ++i) {
        max_size = std::max(max_size, _data[i].size());
    }
    return max_size + sizeof(uint32_t);
}

void ColumnVarbinary::serialize(StringRef* keys, size_t num_rows) const {
    for (size_t i = 0; i < num_rows; ++i) {
        keys[i].size += serialize_impl(const_cast<char*>(keys[i].data + keys[i].size), i);
    }
}

void ColumnVarbinary::deserialize(StringRef* keys, const size_t num_rows) {
    for (size_t i = 0; i != num_rows; ++i) {
        auto sz = deserialize_impl(keys[i].data);
        keys[i].data += sz;
        keys[i].size -= sz;
    }
}

template <bool positive>
struct ColumnVarbinary::less {
    const ColumnVarbinary& parent;
    explicit less(const ColumnVarbinary& parent_) : parent(parent_) {}
    bool operator()(size_t lhs, size_t rhs) const {
        int res = parent._data[lhs].compare(parent._data[rhs]);
        return positive ? (res < 0) : (res > 0);
    }
};

void ColumnVarbinary::get_permutation(bool reverse, size_t limit, int /*nan_direction_hint*/,
                                      HybridSorter& sorter, IColumn::Permutation& res) const {
    size_t s = _data.size();
    res.resize(s);
    for (size_t i = 0; i < s; ++i) {
        res[i] = i;
    }

    if (reverse) {
        sorter.sort(res.begin(), res.end(), less<false>(*this));
    } else {
        sorter.sort(res.begin(), res.end(), less<true>(*this));
    }
}

void ColumnVarbinary::insert_many_strings(const StringRef* strings, size_t num) {
    for (size_t i = 0; i < num; i++) {
        insert_data(strings[i].data, strings[i].size);
    }
}

void ColumnVarbinary::insert_many_strings_overflow(const StringRef* strings, size_t num,
                                                   size_t max_length) {
    insert_many_strings(strings, num);
}

void ColumnVarbinary::sort_column(const ColumnSorter* sorter, EqualFlags& flags,
                                  IColumn::Permutation& perms, EqualRange& range,
                                  bool last_column) const {
    sorter->sort_column(assert_cast<const ColumnVarbinary&>(*this), flags, perms, range,
                        last_column);
}

#include "common/compile_check_end.h"
} // namespace doris::vectorized
