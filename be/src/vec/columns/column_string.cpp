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
#include <cstring>

#include "runtime/primitive_type.h"
#include "util/memcpy_inlined.h"
#include "util/simd/bits.h"
#include "util/simd/vstring_function.h"
#include "vec/columns/columns_common.h"
#include "vec/common/arena.h"
#include "vec/common/assert_cast.h"
#include "vec/common/memcmp_small.h"
#include "vec/common/unaligned.h"
#include "vec/core/sort_block.h"
namespace doris::vectorized {
#include "common/compile_check_begin.h"

template <typename T>
void ColumnStr<T>::sanity_check() const {
#ifndef NDEBUG
    sanity_check_simple();
    auto count = cast_set<int64_t>(offsets.size());
    for (int64_t i = 0; i < count; ++i) {
        if (offsets[i] < offsets[i - 1]) {
            throw Exception(Status::InternalError("row count: {}, offsets[{}]: {}, offsets[{}]: {}",
                                                  count, i, offsets[i], i - 1, offsets[i - 1]));
        }
    }
#endif
}

template <typename T>
void ColumnStr<T>::sanity_check_simple() const {
#ifndef NDEBUG
    auto count = cast_set<int64_t>(offsets.size());
    if (chars.size() != offsets[count - 1]) {
        throw Exception(Status::InternalError("row count: {}, chars.size(): {}, offset[{}]: {}",
                                              count, chars.size(), count - 1, offsets[count - 1]));
    }
    if (offsets[-1] != 0) {
        throw Exception(Status::InternalError("wrong offsets[-1]: {}", offsets[-1]));
    }
#endif
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
        // If offset is uint32, size will not exceed, check the size when inserting data into ColumnStr<T>.
        res->offsets.resize_fill(to_size, static_cast<T>(chars.size()));
    }

    return res;
}

template <typename T>
void ColumnStr<T>::shrink_padding_chars() {
    if (size() == 0) {
        return;
    }
    char* data = reinterpret_cast<char*>(chars.data());
    auto* offset = offsets.data();
    size_t size = offsets.size();

    // deal the 0-th element. no need to move.
    auto next_start = offset[0];
    offset[0] = static_cast<T>(strnlen(data, size_at(0)));
    for (size_t i = 1; i < size; i++) {
        // get the i-th length and whole move it to cover the last's trailing void
        auto length = strnlen(data + next_start, offset[i] - next_start);
        memmove(data + offset[i - 1], data + next_start, length);
        // offset i will be changed. so save the old value for (i+1)-th to get its length.
        next_start = offset[i];
        offset[i] = offset[i - 1] + static_cast<T>(length);
    }
    chars.resize_fill(offsets.back()); // just call it to shrink memory here. no possible to expand.
}

// This method is only called by MutableBlock::merge_ignore_overflow
// by hash join operator to collect build data to avoid
// the total string length of a ColumnStr<uint32_t> column exceeds the 4G limit.
//
// After finishing collecting build data, a ColumnStr<uint32_t> column
// will be converted to ColumnStr<uint64_t> if the total string length
// exceeds the 4G limit by calling Block::replace_if_overflow.
template <typename T>
void ColumnStr<T>::insert_range_from_ignore_overflow(const doris::vectorized::IColumn& src,
                                                     size_t start, size_t length) {
    if (length == 0) {
        return;
    }

    const auto& src_concrete = assert_cast<const ColumnStr<T>&>(src);
    if (start + length > src_concrete.offsets.size()) {
        throw doris::Exception(doris::ErrorCode::INTERNAL_ERROR,
                               "Parameter out of bound in "
                               "IColumnStr<T>::insert_range_from_ignore_overflow method.");
    }

    auto nested_offset = src_concrete.offset_at(start);
    auto nested_length = src_concrete.offsets[start + length - 1] - nested_offset;

    size_t old_chars_size = chars.size();
    chars.resize(old_chars_size + nested_length);
    memcpy(&chars[old_chars_size], &src_concrete.chars[nested_offset], nested_length);

    if (start == 0 && offsets.empty()) {
        offsets.assign(src_concrete.offsets.begin(), src_concrete.offsets.begin() + length);
    } else {
        size_t old_size = offsets.size();
        auto prev_max_offset = offsets.back(); /// -1th index is Ok, see PaddedPODArray
        offsets.resize(old_size + length);

        for (size_t i = 0; i < length; ++i) {
            // unsinged integer overflow is well defined in C++,
            // so we don't need to check the overflow here.
            offsets[old_size + i] =
                    src_concrete.offsets[start + i] - nested_offset + prev_max_offset;
        }
    }

#ifndef NDEBUG
    auto count = cast_set<int64_t>(offsets.size());
    // offsets may overflow, so we make chars.size() as T to do same overflow check
    if (offsets.back() != T(chars.size())) {
        throw Exception(Status::InternalError("row count: {}, chars.size(): {}, offset[{}]: {}",
                                              count, chars.size(), count - 1, offsets[count - 1]));
    }
#endif
}

template <typename T>
bool ColumnStr<T>::has_enough_capacity(const IColumn& src) const {
    const auto& src_concrete = assert_cast<const ColumnStr<T>&>(src);
    return (this->get_chars().capacity() - this->get_chars().size() >
            src_concrete.get_chars().size()) &&
           (this->get_offsets().capacity() - this->get_offsets().size() >
            src_concrete.get_offsets().size());
}

template <typename T>
void ColumnStr<T>::insert_range_from(const IColumn& src, size_t start, size_t length) {
    if (length == 0) {
        return;
    }
    auto do_insert = [&](const auto& src_concrete) {
        const auto& src_offsets = src_concrete.get_offsets();
        const auto& src_chars = src_concrete.get_chars();
        if (start + length > src_offsets.size()) {
            throw doris::Exception(
                    doris::ErrorCode::INTERNAL_ERROR,
                    "Parameter out of bound in IColumnStr<T>::insert_range_from method.");
        }
        auto nested_offset = src_offsets[static_cast<ssize_t>(start) - 1];
        auto nested_length = src_offsets[start + length - 1] - nested_offset;

        size_t old_chars_size = chars.size();
        check_chars_length(old_chars_size + nested_length, offsets.size() + length, size());
        chars.resize(old_chars_size + nested_length);
        memcpy(&chars[old_chars_size], &src_chars[nested_offset], nested_length);

        using OffsetsType = std::decay_t<decltype(src_offsets)>;
        if (std::is_same_v<T, typename OffsetsType::value_type> && start == 0 && offsets.empty()) {
            offsets.assign(src_offsets.begin(), src_offsets.begin() + length);
        } else {
            size_t old_size = offsets.size();
            auto prev_max_offset = offsets.back(); /// -1th index is Ok, see PaddedPODArray
            offsets.resize(old_size + length);

            for (size_t i = 0; i < length; ++i) {
                // if Offsets is uint32, size will not exceed range of uint32, cast is OK.
                offsets[old_size + i] =
                        static_cast<T>(src_offsets[start + i] - nested_offset) + prev_max_offset;
            }
        }
    };
    // insert_range_from maybe called by ColumnArray::insert_indices_from(which is used by hash join operator),
    // so we need to support both ColumnStr<uint32_t> and ColumnStr<uint64_t>
    if (src.is_column_string64()) {
        do_insert(assert_cast<const ColumnStr<uint64_t>&>(src));
    } else {
        do_insert(assert_cast<const ColumnStr<uint32_t>&>(src));
    }
    sanity_check_simple();
}

template <typename T>
void ColumnStr<T>::insert_many_from(const IColumn& src, size_t position, size_t length) {
    const auto& string_column = assert_cast<const ColumnStr<T>&>(src);
    auto [data_val, data_length] = string_column.get_data_at(position);

    size_t old_chars_size = chars.size();
    check_chars_length(old_chars_size + data_length * length, offsets.size() + length, size());
    chars.resize(old_chars_size + data_length * length);

    auto old_size = offsets.size();
    offsets.resize(old_size + length);

    auto start_pos = old_size;
    auto end_pos = old_size + length;
    auto prev_pos = old_chars_size;
    for (; start_pos < end_pos; ++start_pos) {
        memcpy(&chars[prev_pos], data_val, data_length);
        offsets[start_pos] = static_cast<T>(prev_pos + data_length);
        prev_pos = prev_pos + data_length;
    }
    sanity_check_simple();
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
            // if Offsets is uint32, size will not exceed range of uint32, cast is OK.
            dst_offsets_data[dst_offsets_pos++] = static_cast<T>(total_chars_size);
        }
        check_chars_length(total_chars_size, offsets.size(), dst_offsets_pos);

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
    sanity_check_simple();
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
            // If offset is uint32, size will not exceed, check the size when inserting data into ColumnStr<T>.
            hashes[i] = HashUtil::zlib_crc_hash(data_ref.data, static_cast<uint32_t>(data_ref.size),
                                                hashes[i]);
        }
    } else {
        for (size_t i = 0; i < s; i++) {
            if (null_data[i] == 0) {
                auto data_ref = get_data_at(i);
                hashes[i] = HashUtil::zlib_crc_hash(
                        data_ref.data, static_cast<uint32_t>(data_ref.size), hashes[i]);
            }
        }
    }
}

template <typename T>
ColumnPtr ColumnStr<T>::filter(const IColumn::Filter& filt, ssize_t result_size_hint) const {
    if constexpr (std::is_same_v<UInt32, T>) {
        if (offsets.size() == 0) {
            return ColumnStr<T>::create();
        }

        auto res = ColumnStr<T>::create();
        Chars& res_chars = res->chars;
        IColumn::Offsets& res_offsets = res->offsets;

        filter_arrays_impl<UInt8, IColumn::Offset>(chars, offsets, res_chars, res_offsets, filt,
                                                   result_size_hint);
        sanity_check_simple();
        return res;
    } else {
        throw doris::Exception(doris::ErrorCode::INTERNAL_ERROR,
                               "should not call filter in ColumnStr<UInt64>");
    }
}

template <typename T>
size_t ColumnStr<T>::filter(const IColumn::Filter& filter) {
    if constexpr (std::is_same_v<UInt32, T>) {
        CHECK_EQ(filter.size(), offsets.size());
        if (offsets.size() == 0) {
            resize(0);
            return 0;
        }

        auto res = filter_arrays_impl<UInt8, IColumn::Offset>(chars, offsets, filter);
        sanity_check();
        return res;
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
        // CAUTION: the order of the returned rows DOES NOT match
        // the order of row indices that are specified in the sel parameter,
        // instead, the result rows are picked from start to end if the index
        // appears in sel parameter.
        // e.g., sel: [3, 0, 1], the result rows are: [0, 1, 3]
        for (size_t i = 0; i < sel_size; i++) {
            filter[sel[i]] = 1;
        }
        filter_arrays_impl<UInt8, IColumn::Offset>(chars, offsets, res_chars, res_offsets, filter,
                                                   sel_size);
        sanity_check();
        return Status::OK();
    } else {
        return Status::InternalError("should not call filter_by_selector in ColumnStr<UInt64>");
    }
}

template <typename T>
MutableColumnPtr ColumnStr<T>::permute(const IColumn::Permutation& perm, size_t limit) const {
    size_t size = offsets.size();

    if (limit == 0) {
        limit = size;
    } else {
        limit = std::min(size, limit);
    }

    if (perm.size() < limit) {
        throw doris::Exception(doris::ErrorCode::INTERNAL_ERROR,
                               "Size of permutation is less than required.");
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
    sanity_check();
    return res;
}

template <typename T>
StringRef ColumnStr<T>::serialize_value_into_arena(size_t n, Arena& arena,
                                                   char const*& begin) const {
    char* pos = arena.alloc_continue(serialize_size_at(n), begin);
    return {pos, serialize_impl(pos, n)};
}

template <typename T>
const char* ColumnStr<T>::deserialize_and_insert_from_arena(const char* pos) {
    return pos + deserialize_impl(pos);
}

template <typename T>
size_t ColumnStr<T>::get_max_row_byte_size() const {
    T max_size = 0;
    size_t num_rows = offsets.size();
    for (size_t i = 0; i < num_rows; ++i) {
        max_size = std::max(max_size, T(size_at(i)));
    }

    return max_size + sizeof(uint32_t);
}

template <typename T>
void ColumnStr<T>::serialize_vec(StringRef* keys, size_t num_rows) const {
    for (size_t i = 0; i < num_rows; ++i) {
        // Used in hash_map_context.h, this address is allocated via Arena,
        // but passed through StringRef, so using const_cast is acceptable.
        keys[i].size += serialize_impl(const_cast<char*>(keys[i].data + keys[i].size), i);
    }
}

template <typename T>
size_t ColumnStr<T>::serialize_impl(char* pos, const size_t row) const {
    auto offset(offset_at(row));
    auto string_size(size_at(row));

    memcpy_fixed<uint32_t>(pos, (char*)&string_size);
    memcpy(pos + sizeof(string_size), &chars[offset], string_size);
    return sizeof(string_size) + string_size;
}

template <typename T>
void ColumnStr<T>::deserialize_vec(StringRef* keys, const size_t num_rows) {
    for (size_t i = 0; i != num_rows; ++i) {
        auto sz = deserialize_impl(keys[i].data);
        keys[i].data += sz;
        keys[i].size -= sz;
    }
}

template <typename T>
size_t ColumnStr<T>::deserialize_impl(const char* pos) {
    const auto string_size = unaligned_load<uint32_t>(pos);
    pos += sizeof(string_size);

    const size_t old_size = chars.size();
    const size_t new_size = old_size + string_size;
    check_chars_length(new_size, offsets.size() + 1, size());
    chars.resize(new_size);
    memcpy(chars.data() + old_size, pos, string_size);

    offsets.push_back(new_size);
    sanity_check_simple();
    return string_size + sizeof(string_size);
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
void ColumnStr<T>::reserve(size_t n) {
    offsets.reserve(n);
    chars.reserve(n);
}

template <typename T>
void ColumnStr<T>::resize(size_t n) {
    auto origin_size = size();
    if (origin_size > n) {
        offsets.resize(n);
        chars.resize(offsets[n - 1]);
    } else if (origin_size < n) {
        insert_many_defaults(n - origin_size);
    }
    sanity_check_simple();
}

template <typename T>
void ColumnStr<T>::sort_column(const ColumnSorter* sorter, EqualFlags& flags,
                               IColumn::Permutation& perms, EqualRange& range,
                               bool last_column) const {
    sorter->sort_column(static_cast<const ColumnStr<T>&>(*this), flags, perms, range, last_column);
}

template <typename T>
void ColumnStr<T>::compare_internal(size_t rhs_row_id, const IColumn& rhs, int nan_direction_hint,
                                    int direction, std::vector<uint8_t>& cmp_res,
                                    uint8_t* __restrict filter) const {
    sanity_check_simple();
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
            // need to covert to unsigned char, orelse the compare semantic is not consistent
            // with other member functions, e.g. get_permutation and compare_at,
            // and will result wrong result.
            int res = memcmp_small_allow_overflow15((Char*)value_a.data, value_a.size,
                                                    (Char*)cmp_base.data, cmp_base.size);
            cmp_res[row_id] = (res != 0);
            filter[row_id] = (res * direction < 0);
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

template <typename T>
void ColumnStr<T>::erase(size_t start, size_t length) {
    if (start >= offsets.size() || length == 0) {
        return;
    }
    length = std::min(length, offsets.size() - start);

    auto char_start = offsets[start - 1];
    auto char_end = offsets[start + length - 1];
    auto char_length = char_end - char_start;
    memmove(chars.data() + char_start, chars.data() + char_end, chars.size() - char_end);
    chars.resize(chars.size() - char_length);

    const size_t remain_size = offsets.size() - length;
    memmove(offsets.data() + start, offsets.data() + start + length,
            (remain_size - start) * sizeof(T));
    offsets.resize(remain_size);

    for (size_t i = start; i < remain_size; ++i) {
        offsets[i] -= char_length;
    }
}

template <typename T>
Field ColumnStr<T>::operator[](size_t n) const {
    assert(n < size());
    sanity_check_simple();
    return Field::create_field<TYPE_STRING>(
            String(reinterpret_cast<const char*>(&chars[offset_at(n)]), size_at(n)));
}

template <typename T>
void ColumnStr<T>::get(size_t n, Field& res) const {
    assert(n < size());
    sanity_check_simple();
    if (res.get_type() == PrimitiveType::TYPE_JSONB) {
        // Handle JsonbField
        res = Field::create_field<TYPE_JSONB>(
                JsonbField(reinterpret_cast<const char*>(&chars[offset_at(n)]), size_at(n)));
        return;
    }
    res = Field::create_field<TYPE_STRING>(
            String(reinterpret_cast<const char*>(&chars[offset_at(n)]), size_at(n)));
}

template <typename T>
void ColumnStr<T>::insert(const Field& x) {
    StringRef s;
    if (x.get_type() == PrimitiveType::TYPE_JSONB) {
        // Handle JsonbField
        const auto& real_field = vectorized::get<const JsonbField&>(x);
        s = StringRef(real_field.get_value(), real_field.get_size());
    } else {
        DCHECK(is_string_type(x.get_type()));
        // If `x.get_type()` is not String, such as UInt64, may get the error
        // `string column length is too large: total_length=13744632839234567870`
        // because `<String>(x).size() = 13744632839234567870`
        s.data = vectorized::get<const String&>(x).data();
        s.size = vectorized::get<const String&>(x).size();
    }
    const size_t old_size = chars.size();
    const size_t size_to_append = s.size;
    const size_t new_size = old_size + size_to_append;

    check_chars_length(new_size, old_size + 1);

    chars.resize(new_size);
    DCHECK(s.data != nullptr);
    DCHECK(chars.data() != nullptr);
    memcpy(chars.data() + old_size, s.data, size_to_append);
    offsets.push_back(new_size);
    sanity_check_simple();
}

template <typename T>
bool ColumnStr<T>::is_ascii() const {
    return simd::VStringFunctions::is_ascii(StringRef(chars.data(), chars.size()));
}

template class ColumnStr<uint32_t>;
template class ColumnStr<uint64_t>;
} // namespace doris::vectorized
