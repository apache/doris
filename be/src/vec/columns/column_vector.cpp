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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Columns/ColumnVector.cpp
// and modified by Doris

#include "vec/columns/column_vector.h"

#include <fmt/format.h>
#include <pdqsort.h>

#include <limits>
#include <ostream>
#include <string>

#include "util/hash_util.hpp"
#include "util/simd/bits.h"
#include "vec/columns/columns_common.h"
#include "vec/common/arena.h"
#include "vec/common/assert_cast.h"
#include "vec/common/memcpy_small.h"
#include "vec/common/nan_utils.h"
#include "vec/common/sip_hash.h"
#include "vec/common/unaligned.h"
#include "vec/core/sort_block.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"

template <PrimitiveType T>
size_t ColumnVector<T>::serialize_impl(char* pos, const size_t row) const {
    memcpy_fixed<value_type>(pos, (char*)&data[row]);
    return sizeof(value_type);
}

template <PrimitiveType T>
size_t ColumnVector<T>::deserialize_impl(const char* pos) {
    data.push_back(unaligned_load<value_type>(pos));
    return sizeof(value_type);
}

template <PrimitiveType T>
StringRef ColumnVector<T>::serialize_value_into_arena(size_t n, Arena& arena,
                                                      char const*& begin) const {
    auto* pos = arena.alloc_continue(sizeof(value_type), begin);
    return {pos, serialize_impl(pos, n)};
}

template <PrimitiveType T>
const char* ColumnVector<T>::deserialize_and_insert_from_arena(const char* pos) {
    return pos + deserialize_impl(pos);
}

template <PrimitiveType T>
size_t ColumnVector<T>::get_max_row_byte_size() const {
    return sizeof(value_type);
}

template <PrimitiveType T>
void ColumnVector<T>::serialize_vec(StringRef* keys, size_t num_rows) const {
    for (size_t i = 0; i < num_rows; ++i) {
        keys[i].size += serialize_impl(const_cast<char*>(keys[i].data + keys[i].size), i);
    }
}

template <PrimitiveType T>
void ColumnVector<T>::deserialize_vec(StringRef* keys, const size_t num_rows) {
    for (size_t i = 0; i != num_rows; ++i) {
        auto sz = deserialize_impl(keys[i].data);
        keys[i].data += sz;
        keys[i].size -= sz;
    }
}

template <PrimitiveType T>
void ColumnVector<T>::update_hash_with_value(size_t n, SipHash& hash) const {
    hash.update(data[n]);
}

template <PrimitiveType T>
void ColumnVector<T>::update_hashes_with_value(uint64_t* __restrict hashes,
                                               const uint8_t* __restrict null_data) const {
    auto s = size();
    if (null_data) {
        for (int i = 0; i < s; i++) {
            if (null_data[i] == 0) {
                hashes[i] = HashUtil::xxHash64WithSeed(reinterpret_cast<const char*>(&data[i]),
                                                       sizeof(value_type), hashes[i]);
            }
        }
    } else {
        for (int i = 0; i < s; i++) {
            hashes[i] = HashUtil::xxHash64WithSeed(reinterpret_cast<const char*>(&data[i]),
                                                   sizeof(value_type), hashes[i]);
        }
    }
}

template <PrimitiveType T>
void ColumnVector<T>::sort_column(const ColumnSorter* sorter, EqualFlags& flags,
                                  IColumn::Permutation& perms, EqualRange& range,
                                  bool last_column) const {
    sorter->sort_column(static_cast<const Self&>(*this), flags, perms, range, last_column);
}

template <PrimitiveType T>
void ColumnVector<T>::compare_internal(size_t rhs_row_id, const IColumn& rhs,
                                       int nan_direction_hint, int direction,
                                       std::vector<uint8_t>& cmp_res,
                                       uint8_t* __restrict filter) const {
    const auto sz = data.size();
    DCHECK(cmp_res.size() == sz);
    const auto& cmp_base = assert_cast<const ColumnVector<T>&, TypeCheckOnRelease::DISABLE>(rhs)
                                   .get_data()[rhs_row_id];
    size_t begin = simd::find_zero(cmp_res, 0);
    while (begin < sz) {
        size_t end = simd::find_one(cmp_res, begin + 1);
        for (size_t row_id = begin; row_id < end; row_id++) {
            auto value_a = data[row_id];
            int res = value_a > cmp_base ? 1 : (value_a < cmp_base ? -1 : 0);
            cmp_res[row_id] = (res != 0);
            filter[row_id] = (res * direction < 0);
        }
        begin = simd::find_zero(cmp_res, end + 1);
    }
}

template <PrimitiveType T>
Field ColumnVector<T>::operator[](size_t n) const {
    return Field::create_field<T>((typename PrimitiveTypeTraits<T>::NearestFieldType)data[n]);
}

template <PrimitiveType T>
void ColumnVector<T>::update_crcs_with_value(uint32_t* __restrict hashes, PrimitiveType type,
                                             uint32_t rows, uint32_t offset,
                                             const uint8_t* __restrict null_data) const {
    auto s = rows;
    DCHECK(s == size());

    if constexpr (is_date_or_datetime(T)) {
        char buf[64];
        auto date_convert_do_crc = [&](size_t i) {
            const auto& date_val = (const VecDateTimeValue&)data[i];
            auto len = date_val.to_buffer(buf);
            hashes[i] = HashUtil::zlib_crc_hash(buf, len, hashes[i]);
        };

        if (null_data == nullptr) {
            for (size_t i = 0; i < s; i++) {
                date_convert_do_crc(i);
            }
        } else {
            for (size_t i = 0; i < s; i++) {
                if (null_data[i] == 0) {
                    date_convert_do_crc(i);
                }
            }
        }
    } else {
        if (null_data == nullptr) {
            for (size_t i = 0; i < s; i++) {
                hashes[i] = HashUtil::zlib_crc_hash(
                        &data[i], sizeof(typename PrimitiveTypeTraits<T>::ColumnItemType),
                        hashes[i]);
            }
        } else {
            for (size_t i = 0; i < s; i++) {
                if (null_data[i] == 0)
                    hashes[i] = HashUtil::zlib_crc_hash(
                            &data[i], sizeof(typename PrimitiveTypeTraits<T>::ColumnItemType),
                            hashes[i]);
            }
        }
    }
}

template <PrimitiveType T>
struct ColumnVector<T>::less {
    const Self& parent;
    int nan_direction_hint;
    less(const Self& parent_, int nan_direction_hint_)
            : parent(parent_), nan_direction_hint(nan_direction_hint_) {}
    bool operator()(size_t lhs, size_t rhs) const {
        return Compare::less(parent.data[lhs], parent.data[rhs]);
    }
};

template <PrimitiveType T>
struct ColumnVector<T>::greater {
    const Self& parent;
    int nan_direction_hint;
    greater(const Self& parent_, int nan_direction_hint_)
            : parent(parent_), nan_direction_hint(nan_direction_hint_) {}
    bool operator()(size_t lhs, size_t rhs) const {
        return Compare::greater(parent.data[lhs], parent.data[rhs]);
    }
};

template <PrimitiveType T>
void ColumnVector<T>::get_permutation(bool reverse, size_t limit, int nan_direction_hint,
                                      IColumn::Permutation& res) const {
    size_t s = data.size();
    res.resize(s);

    if (s == 0) return;

    // std::partial_sort need limit << s can get performance benefit
    if (static_cast<double>(limit) > (static_cast<double>(s) / 8.0)) limit = 0;

    if (limit) {
        for (size_t i = 0; i < s; ++i) res[i] = i;

        if (reverse)
            std::partial_sort(res.begin(), res.begin() + limit, res.end(),
                              greater(*this, nan_direction_hint));
        else
            std::partial_sort(res.begin(), res.begin() + limit, res.end(),
                              less(*this, nan_direction_hint));
    } else {
        /// Default sorting algorithm.
        for (size_t i = 0; i < s; ++i) res[i] = i;

        if (reverse)
            pdqsort(res.begin(), res.end(), greater(*this, nan_direction_hint));
        else
            pdqsort(res.begin(), res.end(), less(*this, nan_direction_hint));
    }
}

template <PrimitiveType T>
MutableColumnPtr ColumnVector<T>::clone_resized(size_t size) const {
    auto res = this->create();
    if (size > 0) {
        auto& new_col = assert_cast<Self&>(*res);
        size_t count = std::min(this->size(), size);
        new_col.data.resize(count);
        memcpy(new_col.data.data(), data.data(), count * sizeof(data[0]));

        if (size > count) {
            new_col.insert_many_defaults(size - count);
        }
    }

    return res;
}

template <PrimitiveType T>
void ColumnVector<T>::insert_range_from(const IColumn& src, size_t start, size_t length) {
    const ColumnVector& src_vec = assert_cast<const ColumnVector&>(src);
    //  size_t(start)  start > src_vec.data.size() || length > src_vec.data.size() should not be negative which cause overflow
    if (start + length > src_vec.data.size()) {
        throw doris::Exception(doris::ErrorCode::INTERNAL_ERROR,
                               "Parameters start = {}, length = {}, are out of bound in "
                               "ColumnVector<T>::insert_range_from method (data.size() = {}).",
                               start, length, src_vec.data.size());
    }

    size_t old_size = data.size();
    data.resize(old_size + length);
    memcpy(data.data() + old_size, &src_vec.data[start], length * sizeof(data[0]));
}

template <PrimitiveType T>
void ColumnVector<T>::insert_indices_from(const IColumn& src, const uint32_t* indices_begin,
                                          const uint32_t* indices_end) {
    auto origin_size = size();
    auto new_size = indices_end - indices_begin;
    data.resize(origin_size + new_size);

    auto copy = [](const value_type* __restrict src, value_type* __restrict dest,
                   const uint32_t* __restrict begin, const uint32_t* __restrict end) {
        for (const auto* it = begin; it != end; ++it) {
            *dest = src[*it];
            ++dest;
        }
    };
    copy(reinterpret_cast<const value_type*>(src.get_raw_data().data), data.data() + origin_size,
         indices_begin, indices_end);
}

template <PrimitiveType T>
ColumnPtr ColumnVector<T>::filter(const IColumn::Filter& filt, ssize_t result_size_hint) const {
    size_t size = data.size();
    column_match_filter_size(size, filt.size());

    auto res = this->create();
    Container& res_data = res->get_data();

    res_data.reserve(result_size_hint > 0 ? result_size_hint : size);

    const UInt8* filt_pos = filt.data();
    const UInt8* filt_end = filt_pos + size;
    const value_type* data_pos = data.data();

    /** A slightly more optimized version.
        * Based on the assumption that often pieces of consecutive values
        *  completely pass or do not pass the filter.
        * Therefore, we will optimistically check the parts of `SIMD_BYTES` values.
        */
    static constexpr size_t SIMD_BYTES = simd::bits_mask_length();
    const UInt8* filt_end_sse = filt_pos + size / SIMD_BYTES * SIMD_BYTES;

    while (filt_pos < filt_end_sse) {
        auto mask = simd::bytes_mask_to_bits_mask(filt_pos);
        if (0 == mask) {
            //pass
        } else if (simd::bits_mask_all() == mask) {
            res_data.insert(data_pos, data_pos + SIMD_BYTES);
        } else {
            simd::iterate_through_bits_mask(
                    [&](const size_t idx) { res_data.push_back_without_reserve(data_pos[idx]); },
                    mask);
        }

        filt_pos += SIMD_BYTES;
        data_pos += SIMD_BYTES;
    }

    while (filt_pos < filt_end) {
        if (*filt_pos) {
            res_data.push_back_without_reserve(*data_pos);
        }

        ++filt_pos;
        ++data_pos;
    }

    return res;
}

template <PrimitiveType T>
size_t ColumnVector<T>::filter(const IColumn::Filter& filter) {
    size_t size = data.size();
    column_match_filter_size(size, filter.size());

    const UInt8* filter_pos = filter.data();
    const UInt8* filter_end = filter_pos + size;
    value_type* data_pos = data.data();
    value_type* result_data = data_pos;

    /** A slightly more optimized version.
        * Based on the assumption that often pieces of consecutive values
        *  completely pass or do not pass the filter.
        * Therefore, we will optimistically check the parts of `SIMD_BYTES` values.
        */
    static constexpr size_t SIMD_BYTES = simd::bits_mask_length();
    const UInt8* filter_end_sse = filter_pos + size / SIMD_BYTES * SIMD_BYTES;

    while (filter_pos < filter_end_sse) {
        auto mask = simd::bytes_mask_to_bits_mask(filter_pos);
        if (0 == mask) {
            //pass
        } else if (simd::bits_mask_all() == mask) {
            memmove(result_data, data_pos, sizeof(value_type) * SIMD_BYTES);
            result_data += SIMD_BYTES;
        } else {
            simd::iterate_through_bits_mask(
                    [&](const size_t idx) {
                        *result_data = data_pos[idx];
                        ++result_data;
                    },
                    mask);
        }

        filter_pos += SIMD_BYTES;
        data_pos += SIMD_BYTES;
    }

    while (filter_pos < filter_end) {
        if (*filter_pos) {
            *result_data = *data_pos;
            ++result_data;
        }

        ++filter_pos;
        ++data_pos;
    }

    const auto new_size = result_data - data.data();
    resize(new_size);

    return new_size;
}

template <PrimitiveType T>
void ColumnVector<T>::insert_many_from(const IColumn& src, size_t position, size_t length) {
    auto old_size = data.size();
    data.resize(old_size + length);
    auto& vals = assert_cast<const Self&>(src).get_data();
    std::fill(&data[old_size], &data[old_size + length], vals[position]);
}

template <PrimitiveType T>
MutableColumnPtr ColumnVector<T>::permute(const IColumn::Permutation& perm, size_t limit) const {
    size_t size = data.size();

    if (limit == 0)
        limit = size;
    else
        limit = std::min(size, limit);

    if (perm.size() < limit) {
        throw doris::Exception(doris::ErrorCode::INTERNAL_ERROR,
                               "Size of permutation ({}) is less than required ({})", perm.size(),
                               limit);
    }

    auto res = this->create(limit);
    typename Self::Container& res_data = res->get_data();
    for (size_t i = 0; i < limit; ++i) res_data[i] = data[perm[i]];

    return res;
}

template <PrimitiveType T>
void ColumnVector<T>::replace_column_null_data(const uint8_t* __restrict null_map) {
    auto s = size();
    size_t null_count = s - simd::count_zero_num((const int8_t*)null_map, s);
    if (0 == null_count) {
        return;
    }
    for (size_t i = 0; i < s; ++i) {
        data[i] = null_map[i] ? default_value() : data[i];
    }
}

template <PrimitiveType T>
void ColumnVector<T>::replace_float_special_values() {
    if constexpr (is_float_or_double(T)) {
        static constexpr float f_neg_zero = -0.0F;
        static constexpr double d_neg_zero = -0.0;
        static constexpr size_t byte_size = sizeof(value_type);
        static const void* p_neg_zero = (byte_size == 4 ? static_cast<const void*>(&f_neg_zero)
                                                        : static_cast<const void*>(&d_neg_zero));
        auto s = size();
        auto* data_ptr = data.data();
        for (size_t i = 0; i < s; ++i) {
            // replace negative zero with positive zero
            if (0 == std::memcmp(data_ptr + i, p_neg_zero, byte_size)) {
                data[i] = 0.0;
            } else if (is_nan(data[i])) {
                data[i] = std::numeric_limits<value_type>::quiet_NaN();
            }
        }
    }
}

/// Explicit template instantiations - to avoid code bloat in headers.
template class ColumnVector<TYPE_BOOLEAN>;
template class ColumnVector<TYPE_TINYINT>;
template class ColumnVector<TYPE_SMALLINT>;
template class ColumnVector<TYPE_INT>;
template class ColumnVector<TYPE_BIGINT>;
template class ColumnVector<TYPE_LARGEINT>;
template class ColumnVector<TYPE_FLOAT>;
template class ColumnVector<TYPE_DOUBLE>;
template class ColumnVector<TYPE_IPV4>;
template class ColumnVector<TYPE_IPV6>;
template class ColumnVector<TYPE_DATE>;
template class ColumnVector<TYPE_DATEV2>;
template class ColumnVector<TYPE_DATETIME>;
template class ColumnVector<TYPE_DATETIMEV2>;
template class ColumnVector<TYPE_TIME>;
template class ColumnVector<TYPE_TIMEV2>;
template class ColumnVector<TYPE_UINT32>;
template class ColumnVector<TYPE_UINT64>;
} // namespace doris::vectorized
