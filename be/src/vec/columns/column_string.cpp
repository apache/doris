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
// This file is copied from
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Columns/ColumnStr<T>.cpp
// and modified by Doris

#include "vec/columns/column_string.h"

#include <algorithm>
#include <boost/iterator/iterator_facade.hpp>
#include <ostream>

#include "util/memcpy_inlined.h"
#include "util/simd/bits.h"
#include "vec/columns/columns_common.h"
#include "vec/common/arena.h"
#include "vec/common/assert_cast.h"
#include "vec/common/memcmp_small.h"
#include "vec/common/unaligned.h"
#include "vec/core/sort_block.h"

namespace doris::vectorized {

template <typename T>
void ColumnStr<T>::sanity_check() const {
    auto count = offsets.size();
    if (chars.size() != offsets[count - 1]) {
        LOG(FATAL) << "row count: " << count << ", chars.size(): " << chars.size() << ", offset["
                   << count - 1 << "]: " << offsets[count - 1];
    }
    if (offsets[-1] != 0) {
        LOG(FATAL) << "wrong offsets[-1]: " << offsets[-1];
    }
    for (size_t i = 0; i < count; ++i) {
        if (offsets[i] < offsets[i - 1]) {
            LOG(FATAL) << "row count: " << count << ", offsets[" << i << "]: " << offsets[i]
                       << ", offsets[" << i - 1 << "]: " << offsets[i - 1];
        }
    }
}

template <typename T>
MutableColumnPtr ColumnStr<T>::clone_resized(size_t to_size) const {
    auto res = ColumnStr<T>::create();
    if (to_size == 0) {
        return res;
    }

    size_t from_size = size();

    if (to_size <= from_size) {
        /// Just cut column.

        res->offsets.assign(offsets.begin(), offsets.begin() + to_size);
        res->chars.assign(chars.begin(), chars.begin() + offsets[to_size - 1]);
    } else {
        /// Copy column and append empty strings for extra elements.

        if (from_size > 0) {
            res->offsets.assign(offsets.begin(), offsets.end());
            res->chars.assign(chars.begin(), chars.end());
        }

        res->offsets.resize_fill(to_size, chars.size());
    }

    return res;
}

template <typename T>
MutableColumnPtr ColumnStr<T>::get_shrinked_column() {
    auto shrinked_column = ColumnStr<T>::create();
    shrinked_column->get_offsets().reserve(offsets.size());
    shrinked_column->get_chars().reserve(chars.size());
    for (int i = 0; i < size(); i++) {
        StringRef str = get_data_at(i);
        reinterpret_cast<ColumnStr<T>*>(shrinked_column.get())
                ->insert_data(str.data, strnlen(str.data, str.size));
    }
    return shrinked_column;
}

template <typename T>
void ColumnStr<T>::insert_range_from_ignore_overflow(const doris::vectorized::IColumn& src,
                                                     size_t start, size_t length) {
    if (length == 0) {
        return;
    }

    const auto& src_concrete = assert_cast<const ColumnStr<T>&>(src);
    if (start + length > src_concrete.offsets.size()) {
        throw doris::Exception(
                doris::ErrorCode::INTERNAL_ERROR,
                "Parameter out of bound in IColumnStr<T>::insert_range_from method.");
    }

    size_t nested_offset = src_concrete.offset_at(start);
    size_t nested_length = src_concrete.offsets[start + length - 1] - nested_offset;

    size_t old_chars_size = chars.size();
    chars.resize(old_chars_size + nested_length);
    memcpy(&chars[old_chars_size], &src_concrete.chars[nested_offset], nested_length);

    if (start == 0 && offsets.empty()) {
        offsets.assign(src_concrete.offsets.begin(), src_concrete.offsets.begin() + length);
    } else {
        size_t old_size = offsets.size();
        size_t prev_max_offset = offsets.back(); /// -1th index is Ok, see PaddedPODArray
        offsets.resize(old_size + length);

        for (size_t i = 0; i < length; ++i) {
            offsets[old_size + i] =
                    src_concrete.offsets[start + i] - nested_offset + prev_max_offset;
        }
    }
}

template <typename T>
void ColumnStr<T>::insert_range_from(const IColumn& src, size_t start, size_t length) {
    if (length == 0) {
        return;
    }

    const auto& src_concrete = assert_cast<const ColumnStr<T>&>(src);

    if (start + length > src_concrete.offsets.size()) {
        throw doris::Exception(
                doris::ErrorCode::INTERNAL_ERROR,
                "Parameter out of bound in IColumnStr<T>::insert_range_from method.");
    }

    size_t nested_offset = src_concrete.offset_at(start);
    size_t nested_length = src_concrete.offsets[start + length - 1] - nested_offset;

    size_t old_chars_size = chars.size();
    check_chars_length(old_chars_size + nested_length, offsets.size() + length);
    chars.resize(old_chars_size + nested_length);
    memcpy(&chars[old_chars_size], &src_concrete.chars[nested_offset], nested_length);

    if (start == 0 && offsets.empty()) {
        offsets.assign(src_concrete.offsets.begin(), src_concrete.offsets.begin() + length);
    } else {
        size_t old_size = offsets.size();
        size_t prev_max_offset = offsets.back(); /// -1th index is Ok, see PaddedPODArray
        offsets.resize(old_size + length);

        for (size_t i = 0; i < length; ++i) {
            offsets[old_size + i] =
                    src_concrete.offsets[start + i] - nested_offset + prev_max_offset;
        }
    }
}

template <typename T>
void ColumnStr<T>::insert_indices_from(const IColumn& src, const uint32_t* indices_begin,
                                       const uint32_t* indices_end) {
    auto do_insert = [&](const auto& src_str) {
        const auto* __restrict src_offset_data = src_str.get_offsets().data();

        auto old_char_size = chars.size();
        size_t total_chars_size = old_char_size;

        auto dst_offsets_pos = offsets.size();
        offsets.resize(offsets.size() + indices_end - indices_begin);
        auto* dst_offsets_data = offsets.data();

        for (const auto* x = indices_begin; x != indices_end; ++x) {
            int64_t src_offset = *x;
            total_chars_size += src_offset_data[src_offset] - src_offset_data[src_offset - 1];
            dst_offsets_data[dst_offsets_pos++] = total_chars_size;
        }
        check_chars_length(total_chars_size, offsets.size());

        chars.resize(total_chars_size);

        const auto* __restrict src_data_ptr = src_str.get_chars().data();
        auto* dst_data_ptr = chars.data();

        size_t dst_chars_pos = old_char_size;
        for (const auto* x = indices_begin; x != indices_end; ++x) {
            int64_t src_offset = *x;
            const size_t size_to_append =
                    src_offset_data[src_offset] - src_offset_data[src_offset - 1];
            const size_t offset = src_offset_data[src_offset - 1];

            memcpy_inlined(dst_data_ptr + dst_chars_pos, src_data_ptr + offset, size_to_append);

            dst_chars_pos += size_to_append;
        }
    };
    if (src.is_column_string64()) {
        do_insert(assert_cast<const ColumnStr<uint64_t>&>(src));
    } else {
        do_insert(assert_cast<const ColumnStr<uint32_t>&>(src));
    }
}

template <typename T>
void ColumnStr<T>::update_crcs_with_value(uint32_t* __restrict hashes, doris::PrimitiveType type,
                                          uint32_t rows, uint32_t offset,
                                          const uint8_t* __restrict null_data) const {
    auto s = rows;
    DCHECK(s == size());

    if (null_data == nullptr) {
        for (size_t i = 0; i < s; i++) {
            auto data_ref = get_data_at(i);
            hashes[i] = HashUtil::zlib_crc_hash(data_ref.data, data_ref.size, hashes[i]);
        }
    } else {
        for (size_t i = 0; i < s; i++) {
            if (null_data[i] == 0) {
                auto data_ref = get_data_at(i);
                hashes[i] = HashUtil::zlib_crc_hash(data_ref.data, data_ref.size, hashes[i]);
            }
        }
    }
}

template <typename T>
ColumnPtr ColumnStr<T>::filter(const IColumn::Filter& filt, ssize_t result_size_hint) const {
    if (offsets.size() == 0) {
        return ColumnStr<T>::create();
    }

    if constexpr (std::is_same_v<UInt32, T>) {
        auto res = ColumnStr<T>::create();
        Chars& res_chars = res->chars;
        IColumn::Offsets& res_offsets = res->offsets;

        filter_arrays_impl<UInt8, IColumn::Offset>(chars, offsets, res_chars, res_offsets, filt,
                                                   result_size_hint);
        return res;
    } else {
        throw doris::Exception(doris::ErrorCode::INTERNAL_ERROR,
                               "should not call filter in ColumnStr<UInt64>");
    }
}

template <typename T>
size_t ColumnStr<T>::filter(const IColumn::Filter& filter) {
    CHECK_EQ(filter.size(), offsets.size());
    if (offsets.size() == 0) {
        resize(0);
        return 0;
    }

    if constexpr (std::is_same_v<UInt32, T>) {
        return filter_arrays_impl<UInt8, IColumn::Offset>(chars, offsets, filter);
    } else {
        throw doris::Exception(doris::ErrorCode::INTERNAL_ERROR,
                               "should not call filter in ColumnStr<UInt64>");
    }
}

template <typename T>
Status ColumnStr<T>::filter_by_selector(const uint16_t* sel, size_t sel_size, IColumn* col_ptr) {
    if constexpr (std::is_same_v<UInt32, T>) {
        auto* col = static_cast<ColumnStr<T>*>(col_ptr);
        Chars& res_chars = col->chars;
        IColumn::Offsets& res_offsets = col->offsets;
        IColumn::Filter filter;
        filter.resize_fill(offsets.size(), 0);
        for (size_t i = 0; i < sel_size; i++) {
            filter[sel[i]] = 1;
        }
        filter_arrays_impl<UInt8, IColumn::Offset>(chars, offsets, res_chars, res_offsets, filter,
                                                   sel_size);
        return Status::OK();
    } else {
        return Status::InternalError("should not call filter_by_selector in ColumnStr<UInt64>");
    }
}

template <typename T>
ColumnPtr ColumnStr<T>::permute(const IColumn::Permutation& perm, size_t limit) const {
    size_t size = offsets.size();

    if (limit == 0) {
        limit = size;
    } else {
        limit = std::min(size, limit);
    }

    if (perm.size() < limit) {
        throw doris::Exception(doris::ErrorCode::INTERNAL_ERROR,
                               "Size of permutation is less than required.");
        __builtin_unreachable();
    }

    if (limit == 0) {
        return ColumnStr<T>::create();
    }

    auto res = ColumnStr<T>::create();

    Chars& res_chars = res->chars;
    auto& res_offsets = res->offsets;

    if (limit == size) {
        res_chars.resize(chars.size());
    } else {
        size_t new_chars_size = 0;
        for (size_t i = 0; i < limit; ++i) {
            new_chars_size += size_at(perm[i]);
        }
        res_chars.resize(new_chars_size);
    }

    res_offsets.resize(limit);

    T current_new_offset = 0;

    for (size_t i = 0; i < limit; ++i) {
        size_t j = perm[i];
        size_t string_offset = offsets[j - 1];
        size_t string_size = offsets[j] - string_offset;

        memcpy_small_allow_read_write_overflow15(&res_chars[current_new_offset],
                                                 &chars[string_offset], string_size);

        current_new_offset += string_size;
        res_offsets[i] = current_new_offset;
    }

    return res;
}

template <typename T>
StringRef ColumnStr<T>::serialize_value_into_arena(size_t n, Arena& arena,
                                                   char const*& begin) const {
    uint32_t string_size(size_at(n));
    uint32_t offset(offset_at(n));

    StringRef res;
    res.size = sizeof(string_size) + string_size;
    char* pos = arena.alloc_continue(res.size, begin);
    memcpy(pos, &string_size, sizeof(string_size));
    memcpy(pos + sizeof(string_size), &chars[offset], string_size);
    res.data = pos;

    return res;
}

template <typename T>
const char* ColumnStr<T>::deserialize_and_insert_from_arena(const char* pos) {
    const uint32_t string_size = unaligned_load<uint32_t>(pos);
    pos += sizeof(string_size);

    const size_t old_size = chars.size();
    const size_t new_size = old_size + string_size;
    check_chars_length(new_size, offsets.size() + 1);
    chars.resize(new_size);
    memcpy(chars.data() + old_size, pos, string_size);

    offsets.push_back(new_size);
    return pos + string_size;
}

template <typename T>
size_t ColumnStr<T>::get_max_row_byte_size() const {
    size_t max_size = 0;
    size_t num_rows = offsets.size();
    for (size_t i = 0; i < num_rows; ++i) {
        max_size = std::max(max_size, size_at(i));
    }

    return max_size + sizeof(uint32_t);
}

template <typename T>
void ColumnStr<T>::serialize_vec(std::vector<StringRef>& keys, size_t num_rows,
                                 size_t max_row_byte_size) const {
    for (size_t i = 0; i < num_rows; ++i) {
        uint32_t offset(offset_at(i));
        uint32_t string_size(size_at(i));

        auto* ptr = const_cast<char*>(keys[i].data + keys[i].size);
        memcpy_fixed<uint32_t>(ptr, (char*)&string_size);
        memcpy(ptr + sizeof(string_size), &chars[offset], string_size);
        keys[i].size += sizeof(string_size) + string_size;
    }
}

template <typename T>
void ColumnStr<T>::serialize_vec_with_null_map(std::vector<StringRef>& keys, size_t num_rows,
                                               const UInt8* null_map) const {
    DCHECK(null_map != nullptr);

    const bool has_null = simd::contain_byte(null_map, num_rows, 1);

    if (has_null) {
        for (size_t i = 0; i < num_rows; ++i) {
            char* __restrict dest = const_cast<char*>(keys[i].data + keys[i].size);
            // serialize null first
            memcpy(dest, null_map + i, sizeof(uint8_t));

            if (null_map[i] == 0) {
                UInt32 offset(offset_at(i));
                UInt32 string_size(size_at(i));

                memcpy_fixed<UInt32>(dest + 1, (char*)&string_size);
                memcpy(dest + 1 + sizeof(string_size), &chars[offset], string_size);
                keys[i].size += sizeof(string_size) + string_size + sizeof(UInt8);
            } else {
                keys[i].size += sizeof(UInt8);
            }
        }
    } else {
        // All rows are not null, serialize null & value
        for (size_t i = 0; i < num_rows; ++i) {
            char* __restrict dest = const_cast<char*>(keys[i].data + keys[i].size);
            // serialize null first
            memcpy(dest, null_map + i, sizeof(uint8_t));

            UInt32 offset(offset_at(i));
            UInt32 string_size(size_at(i));

            memcpy_fixed<UInt32>(dest + 1, (char*)&string_size);
            memcpy(dest + 1 + sizeof(string_size), &chars[offset], string_size);
            keys[i].size += sizeof(string_size) + string_size + sizeof(UInt8);
        }
    }
}

template <typename T>
void ColumnStr<T>::deserialize_vec(std::vector<StringRef>& keys, const size_t num_rows) {
    for (size_t i = 0; i != num_rows; ++i) {
        auto original_ptr = keys[i].data;
        keys[i].data = deserialize_and_insert_from_arena(original_ptr);
        keys[i].size -= keys[i].data - original_ptr;
    }
}

template <typename T>
void ColumnStr<T>::deserialize_vec_with_null_map(std::vector<StringRef>& keys,
                                                 const size_t num_rows, const uint8_t* null_map) {
    for (size_t i = 0; i != num_rows; ++i) {
        if (null_map[i] == 0) {
            auto original_ptr = keys[i].data;
            keys[i].data = deserialize_and_insert_from_arena(original_ptr);
            keys[i].size -= keys[i].data - original_ptr;
        } else {
            insert_default();
        }
    }
}

template <typename T>
template <bool positive>
struct ColumnStr<T>::less {
    const ColumnStr<T>& parent;
    explicit less(const ColumnStr<T>& parent_) : parent(parent_) {}
    bool operator()(size_t lhs, size_t rhs) const {
        int res = memcmp_small_allow_overflow15(
                parent.chars.data() + parent.offset_at(lhs), parent.size_at(lhs),
                parent.chars.data() + parent.offset_at(rhs), parent.size_at(rhs));

        return positive ? (res < 0) : (res > 0);
    }
};

template <typename T>
void ColumnStr<T>::get_permutation(bool reverse, size_t limit, int /*nan_direction_hint*/,
                                   IColumn::Permutation& res) const {
    size_t s = offsets.size();
    res.resize(s);
    for (size_t i = 0; i < s; ++i) {
        res[i] = i;
    }

    if (reverse) {
        pdqsort(res.begin(), res.end(), less<false>(*this));
    } else {
        pdqsort(res.begin(), res.end(), less<true>(*this));
    }
}

template <typename T>
ColumnPtr ColumnStr<T>::replicate(const IColumn::Offsets& replicate_offsets) const {
    size_t col_size = size();
    column_match_offsets_size(col_size, replicate_offsets.size());

    auto res = ColumnStr<T>::create();

    if (0 == col_size) {
        return res;
    }

    Chars& res_chars = res->chars;
    auto& res_offsets = res->offsets;
    res_chars.reserve(chars.size() / col_size * replicate_offsets.back());
    res_offsets.reserve(replicate_offsets.back());

    T prev_replicate_offset = 0;
    T prev_string_offset = 0;
    T current_new_offset = 0;

    for (size_t i = 0; i < col_size; ++i) {
        size_t size_to_replicate = replicate_offsets[i] - prev_replicate_offset;
        size_t string_size = offsets[i] - prev_string_offset;

        for (size_t j = 0; j < size_to_replicate; ++j) {
            current_new_offset += string_size;
            res_offsets.push_back(current_new_offset);

            res_chars.resize(res_chars.size() + string_size);
            memcpy_small_allow_read_write_overflow15(&res_chars[res_chars.size() - string_size],
                                                     &chars[prev_string_offset], string_size);
        }

        prev_replicate_offset = replicate_offsets[i];
        prev_string_offset = offsets[i];
    }

    check_chars_length(res_chars.size(), res_offsets.size());
    return res;
}

template <typename T>
void ColumnStr<T>::reserve(size_t n) {
    offsets.reserve(n);
    chars.reserve(n);
}

template <typename T>
void ColumnStr<T>::resize(size_t n) {
    auto origin_size = size();
    if (origin_size > n) {
        offsets.resize(n);
    } else if (origin_size < n) {
        insert_many_defaults(n - origin_size);
    }
}

template <typename T>
void ColumnStr<T>::sort_column(const ColumnSorter* sorter, EqualFlags& flags,
                               IColumn::Permutation& perms, EqualRange& range,
                               bool last_column) const {
    sorter->sort_column(static_cast<const ColumnStr<T>&>(*this), flags, perms, range, last_column);
}

template <typename T>
void ColumnStr<T>::compare_internal(size_t rhs_row_id, const IColumn& rhs, int nan_direction_hint,
                                    int direction, std::vector<uint8>& cmp_res,
                                    uint8* __restrict filter) const {
    auto sz = offsets.size();
    DCHECK(cmp_res.size() == sz);
    const auto& cmp_base =
            assert_cast<const ColumnStr<T>&, TypeCheckOnRelease::DISABLE>(rhs).get_data_at(
                    rhs_row_id);
    size_t begin = simd::find_zero(cmp_res, 0);
    while (begin < sz) {
        size_t end = simd::find_one(cmp_res, begin + 1);
        for (size_t row_id = begin; row_id < end; row_id++) {
            auto value_a = get_data_at(row_id);
            int res = memcmp_small_allow_overflow15(value_a.data, value_a.size, cmp_base.data,
                                                    cmp_base.size);
            cmp_res[row_id] = res != 0;
            filter[row_id] = res * direction < 0;
        }
        begin = simd::find_zero(cmp_res, end + 1);
    }
}

template <typename T>
ColumnPtr ColumnStr<T>::convert_column_if_overflow() {
    if (std::is_same_v<T, UInt32> && chars.size() > config::string_overflow_size) {
        auto new_col = ColumnStr<uint64_t>::create();

        const auto length = offsets.size();
        std::swap(new_col->get_chars(), chars);
        new_col->get_offsets().resize(length);
        auto& large_offsets = new_col->get_offsets();

        size_t loc = 0;
        // TODO: recheck to SIMD the code
        // if offset overflow. will be lower than offsets[loc - 1]
        while (offsets[loc] >= offsets[loc - 1] && loc < length) {
            large_offsets[loc] = offsets[loc];
            loc++;
        }
        while (loc < length) {
            large_offsets[loc] = (offsets[loc] - offsets[loc - 1]) + large_offsets[loc - 1];
            loc++;
        }

        offsets.clear();
        return new_col;
    }
    return this->get_ptr();
}

template class ColumnStr<uint32_t>;
template class ColumnStr<uint64_t>;
} // namespace doris::vectorized
