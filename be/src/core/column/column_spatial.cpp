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

#include "core/column/column_spatial.h"

#include <algorithm>
#include <cstring>

#include "common/exception.h"
#include "core/column/columns_common.h"
#include "exec/sort/sort_block.h"

namespace doris {

void ColumnSpatial::insert_data(const char* pos, size_t length) {
    if (length <= StringView::kInlineSize) {
        _data.emplace_back(pos, cast_set<uint32_t>(length));
    } else {
        _data.emplace_back(_arena.insert(pos, length), cast_set<uint32_t>(length));
    }
}

int ColumnSpatial::compare_at(size_t n, size_t m, const IColumn& rhs,
                              int /* nan_direction_hint */) const {
    const auto& spatial = assert_cast<const ColumnSpatial&>(rhs);
    DCHECK_EQ(_primitive_type, spatial._primitive_type);
    return _data[n].compare(spatial._data[m]);
}

MutableColumnPtr ColumnSpatial::clone_resized(size_t size) const {
    auto result = create(_primitive_type);
    const size_t copied = std::min(this->size(), size);
    for (size_t i = 0; i < copied; ++i) {
        const auto value = get_data_at(i);
        result->insert_data(value.data, value.size);
    }
    result->insert_many_defaults(size - copied);
    return result;
}

void ColumnSpatial::insert_range_from(const IColumn& src, size_t start, size_t length) {
    const auto& spatial = assert_cast<const ColumnSpatial&>(src);
    DCHECK_EQ(_primitive_type, spatial._primitive_type);
    if (start + length > spatial.size()) {
        throw Exception(ErrorCode::INTERNAL_ERROR,
                        "Spatial column range start = {}, length = {} is out of bounds for {} rows",
                        start, length, spatial.size());
    }
    for (size_t i = 0; i < length; ++i) {
        const auto value = spatial.get_data_at(start + i);
        insert_data(value.data, value.size);
    }
}

void ColumnSpatial::insert_indices_from(const IColumn& src, const uint32_t* begin,
                                        const uint32_t* end) {
    const auto& spatial = assert_cast<const ColumnSpatial&>(src);
    DCHECK_EQ(_primitive_type, spatial._primitive_type);
    for (auto it = begin; it != end; ++it) {
        const auto value = spatial.get_data_at(*it);
        insert_data(value.data, value.size);
    }
}

bool ColumnSpatial::has_enough_capacity(const IColumn& src) const {
    const auto& spatial = assert_cast<const ColumnSpatial&>(src);
    return _data.capacity() - _data.size() > spatial.size();
}

ColumnPtr ColumnSpatial::filter(const IColumn::Filter& filter, ssize_t result_size_hint) const {
    column_match_filter_size(size(), filter.size());
    auto result = create(_primitive_type);
    if (result_size_hint > 0) {
        result->_data.reserve(result_size_hint);
    }
    for (size_t i = 0; i < size(); ++i) {
        if (filter[i]) {
            const auto value = get_data_at(i);
            result->insert_data(value.data, value.size);
        }
    }
    return result;
}

size_t ColumnSpatial::filter(const IColumn::Filter& filter) {
    column_match_filter_size(size(), filter.size());
    auto filtered = this->filter(filter, -1);
    auto& spatial = assert_cast<const ColumnSpatial&>(*filtered);
    clear();
    insert_range_from(spatial, 0, spatial.size());
    return size();
}

MutableColumnPtr ColumnSpatial::permute(const IColumn::Permutation& perm, size_t limit) const {
    limit = limit ? std::min(size(), limit) : size();
    if (perm.size() < limit) {
        throw Exception(ErrorCode::INTERNAL_ERROR, "Size of permutation is less than required");
    }
    auto result = create(_primitive_type);
    for (size_t i = 0; i < limit; ++i) {
        const auto value = get_data_at(perm[i]);
        result->insert_data(value.data, value.size);
    }
    return result;
}

void ColumnSpatial::replace_column_data(const IColumn& rhs, size_t row, size_t self_row) {
    DCHECK_LT(self_row, size());
    const auto& spatial = assert_cast<const ColumnSpatial&>(rhs);
    DCHECK_EQ(_primitive_type, spatial._primitive_type);
    const auto value = spatial.get_data_at(row);
    if (value.size <= StringView::kInlineSize) {
        _data[self_row] = StringView(value.data, cast_set<uint32_t>(value.size));
    } else {
        _data[self_row] =
                StringView(_arena.insert(value.data, value.size), cast_set<uint32_t>(value.size));
    }
}

size_t ColumnSpatial::get_max_row_byte_size() const {
    size_t maximum = 0;
    for (const auto& value : _data) {
        maximum = std::max(maximum, static_cast<size_t>(value.size()));
    }
    return maximum + sizeof(uint32_t);
}

size_t ColumnSpatial::deserialize_impl(const char* pos) {
    const auto value_size = unaligned_load<uint32_t>(pos);
    pos += sizeof(value_size);
    insert_data(pos, value_size);
    return value_size + sizeof(value_size);
}

size_t ColumnSpatial::serialize_impl(char* pos, size_t row) const {
    const auto value = _data[row];
    const auto value_size = value.size();
    memcpy_fixed<uint32_t>(pos, reinterpret_cast<const char*>(&value_size));
    memcpy(pos + sizeof(uint32_t), value.data(), value_size);
    return value_size + sizeof(uint32_t);
}

size_t ColumnSpatial::serialize_size_at(size_t row) const {
    return _data[row].size() + sizeof(uint32_t);
}

StringRef ColumnSpatial::serialize_value_into_arena(size_t n, Arena& arena,
                                                    char const*& begin) const {
    char* position = arena.alloc_continue(serialize_size_at(n), begin);
    return {position, serialize_impl(position, n)};
}

const char* ColumnSpatial::deserialize_and_insert_from_arena(const char* pos) {
    return pos + deserialize_impl(pos);
}

void ColumnSpatial::serialize_vec(StringRef* keys, size_t num_rows) const {
    for (size_t i = 0; i < num_rows; ++i) {
        keys[i].size += serialize_impl(const_cast<char*>(keys[i].data + keys[i].size), i);
    }
}

void ColumnSpatial::deserialize_vec(StringRef* keys, size_t num_rows) {
    for (size_t i = 0; i < num_rows; ++i) {
        const auto size = deserialize_impl(keys[i].data);
        keys[i].data += size;
        keys[i].size -= size;
    }
}

template <bool positive>
struct ColumnSpatial::less {
    const ColumnSpatial& parent;
    bool operator()(size_t lhs, size_t rhs) const {
        const int comparison = parent._data[lhs].compare(parent._data[rhs]);
        return positive ? comparison < 0 : comparison > 0;
    }
};

void ColumnSpatial::get_permutation(bool reverse, size_t /* limit */, int /* nan_direction_hint */,
                                    HybridSorter& sorter, IColumn::Permutation& result) const {
    result.resize(size());
    for (size_t i = 0; i < size(); ++i) {
        result[i] = i;
    }
    if (reverse) {
        sorter.sort(result.begin(), result.end(), less<false> {*this});
    } else {
        sorter.sort(result.begin(), result.end(), less<true> {*this});
    }
}

void ColumnSpatial::insert_many_strings(const StringRef* strings, size_t num) {
    for (size_t i = 0; i < num; ++i) {
        insert_data(strings[i].data, strings[i].size);
    }
}

void ColumnSpatial::insert_many_strings_overflow(const StringRef* strings, size_t num,
                                                 size_t /* max_length */) {
    insert_many_strings(strings, num);
}

void ColumnSpatial::sort_column(const ColumnSorter* sorter, EqualFlags& flags,
                                IColumn::Permutation& perms, EqualRange& range,
                                bool last_column) const {
    sorter->sort_column(*this, flags, perms, range, last_column);
}

} // namespace doris
