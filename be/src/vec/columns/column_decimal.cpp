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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/AggregateFunctions/ColumnDecimal.cpp
// and modified by Doris

#include "vec/columns/column_decimal.h"

#include <fmt/format.h>

#include <limits>
#include <ostream>
#include <string>

#include "olap/decimal12.h"
#include "runtime/decimalv2_value.h"
#include "util/hash_util.hpp"
#include "util/simd/bits.h"
#include "vec/columns/columns_common.h"
#include "vec/common/arena.h"
#include "vec/common/assert_cast.h"
#include "vec/common/int_exp.h"
#include "vec/common/sip_hash.h"
#include "vec/common/unaligned.h"
#include "vec/core/sort_block.h"
#include "vec/data_types/data_type.h"

template <typename T>
bool decimal_less(T x, T y, doris::vectorized::UInt32 x_scale, doris::vectorized::UInt32 y_scale);

namespace doris::vectorized {

template <typename T>
int ColumnDecimal<T>::compare_at(size_t n, size_t m, const IColumn& rhs_, int) const {
    auto& other = assert_cast<const Self&, TypeCheckOnRelease::DISABLE>(rhs_);
    const T& a = data[n];
    const T& b = other.data[m];

    if (scale == other.scale) return a > b ? 1 : (a < b ? -1 : 0);
    return decimal_less<T>(b, a, other.scale, scale)
                   ? 1
                   : (decimal_less<T>(a, b, scale, other.scale) ? -1 : 0);
}

template <typename T>
StringRef ColumnDecimal<T>::serialize_value_into_arena(size_t n, Arena& arena,
                                                       char const*& begin) const {
    auto pos = arena.alloc_continue(sizeof(T), begin);
    memcpy(pos, &data[n], sizeof(T));
    return StringRef(pos, sizeof(T));
}

template <typename T>
const char* ColumnDecimal<T>::deserialize_and_insert_from_arena(const char* pos) {
    data.push_back(unaligned_load<T>(pos));
    return pos + sizeof(T);
}

template <typename T>
size_t ColumnDecimal<T>::get_max_row_byte_size() const {
    return sizeof(T);
}

template <typename T>
void ColumnDecimal<T>::serialize_vec(std::vector<StringRef>& keys, size_t num_rows,
                                     size_t max_row_byte_size) const {
    for (size_t i = 0; i < num_rows; ++i) {
        memcpy_fixed<T>(const_cast<char*>(keys[i].data + keys[i].size), (char*)&data[i]);
        keys[i].size += sizeof(T);
    }
}

template <typename T>
void ColumnDecimal<T>::serialize_vec_with_null_map(std::vector<StringRef>& keys, size_t num_rows,
                                                   const UInt8* null_map) const {
    DCHECK(null_map != nullptr);
    const bool has_null = simd::contain_byte(null_map, num_rows, 1);
    if (has_null) {
        for (size_t i = 0; i < num_rows; ++i) {
            char* __restrict dest = const_cast<char*>(keys[i].data + +keys[i].size);
            // serialize null first
            memcpy(dest, null_map + i, sizeof(UInt8));
            if (null_map[i] == 0) {
                memcpy_fixed<T>(dest + 1, (char*)&data[i]);
            }

            keys[i].size += sizeof(UInt8) + (1 - null_map[i]) * sizeof(T);
        }
    } else {
        for (size_t i = 0; i < num_rows; ++i) {
            if (null_map[i] == 0) {
                char* __restrict dest = const_cast<char*>(keys[i].data + +keys[i].size);
                memset(dest, 0, 1);
                memcpy_fixed<T>(dest + 1, (char*)&data[i]);
                keys[i].size += sizeof(T) + sizeof(UInt8);
            }
        }
    }
}

template <typename T>
void ColumnDecimal<T>::deserialize_vec(std::vector<StringRef>& keys, const size_t num_rows) {
    for (size_t i = 0; i < num_rows; ++i) {
        keys[i].data = deserialize_and_insert_from_arena(keys[i].data);
        keys[i].size -= sizeof(T);
    }
}

template <typename T>
void ColumnDecimal<T>::deserialize_vec_with_null_map(std::vector<StringRef>& keys,
                                                     const size_t num_rows,
                                                     const uint8_t* null_map) {
    for (size_t i = 0; i < num_rows; ++i) {
        if (null_map[i] == 0) {
            keys[i].data = deserialize_and_insert_from_arena(keys[i].data);
            keys[i].size -= sizeof(T);
        } else {
            insert_default();
        }
    }
}

template <typename T>
void ColumnDecimal<T>::update_hash_with_value(size_t n, SipHash& hash) const {
    hash.update(data[n]);
}

template <typename T>
void ColumnDecimal<T>::update_crc_with_value(size_t start, size_t end, uint32_t& hash,
                                             const uint8_t* __restrict null_data) const {
    if (null_data == nullptr) {
        for (size_t i = start; i < end; i++) {
            if constexpr (!IsDecimalV2<T>) {
                hash = HashUtil::zlib_crc_hash(&data[i], sizeof(T), hash);
            } else {
                decimalv2_do_crc(i, hash);
            }
        }
    } else {
        for (size_t i = start; i < end; i++) {
            if (null_data[i] == 0) {
                if constexpr (!IsDecimalV2<T>) {
                    hash = HashUtil::zlib_crc_hash(&data[i], sizeof(T), hash);
                } else {
                    decimalv2_do_crc(i, hash);
                }
            }
        }
    }
}

template <typename T>
void ColumnDecimal<T>::update_crcs_with_value(uint32_t* __restrict hashes, PrimitiveType type,
                                              uint32_t rows, uint32_t offset,
                                              const uint8_t* __restrict null_data) const {
    auto s = rows;
    DCHECK(s == size());

    if constexpr (!IsDecimalV2<T>) {
        DO_CRC_HASHES_FUNCTION_COLUMN_IMPL()
    } else {
        if (null_data == nullptr) {
            for (size_t i = 0; i < s; i++) {
                decimalv2_do_crc(i, hashes[i]);
            }
        } else {
            for (size_t i = 0; i < s; i++) {
                if (null_data[i] == 0) decimalv2_do_crc(i, hashes[i]);
            }
        }
    }
}

template <typename T>
void ColumnDecimal<T>::update_xxHash_with_value(size_t start, size_t end, uint64_t& hash,
                                                const uint8_t* __restrict null_data) const {
    if (null_data) {
        for (size_t i = start; i < end; i++) {
            if (null_data[i] == 0) {
                hash = HashUtil::xxHash64WithSeed(reinterpret_cast<const char*>(&data[i]),
                                                  sizeof(T), hash);
            }
        }
    } else {
        for (size_t i = start; i < end; i++) {
            hash = HashUtil::xxHash64WithSeed(reinterpret_cast<const char*>(&data[i]), sizeof(T),
                                              hash);
        }
    }
}

template <typename T>
void ColumnDecimal<T>::update_hashes_with_value(uint64_t* __restrict hashes,
                                                const uint8_t* __restrict null_data) const {
    auto s = size();
    if (null_data) {
        for (int i = 0; i < s; i++) {
            if (null_data[i] == 0) {
                hashes[i] = HashUtil::xxHash64WithSeed(reinterpret_cast<const char*>(&data[i]),
                                                       sizeof(T), hashes[i]);
            }
        }
    } else {
        for (int i = 0; i < s; i++) {
            hashes[i] = HashUtil::xxHash64WithSeed(reinterpret_cast<const char*>(&data[i]),
                                                   sizeof(T), hashes[i]);
        }
    }
}

template <typename T>
void ColumnDecimal<T>::get_permutation(bool reverse, size_t limit, int,
                                       IColumn::Permutation& res) const {
#if 1 /// TODO: perf test
    if (data.size() <= std::numeric_limits<UInt32>::max()) {
        PaddedPODArray<UInt32> tmp_res;
        permutation(reverse, limit, tmp_res);

        res.resize(tmp_res.size());
        for (size_t i = 0; i < tmp_res.size(); ++i) res[i] = tmp_res[i];
        return;
    }
#endif

    permutation(reverse, limit, res);
}

template <typename T>
ColumnPtr ColumnDecimal<T>::permute(const IColumn::Permutation& perm, size_t limit) const {
    size_t size = limit ? std::min(data.size(), limit) : data.size();
    if (perm.size() < size) {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                               "Size of permutation ({}) is less than required ({})", perm.size(),
                               limit);
        __builtin_unreachable();
    }

    auto res = this->create(size, scale);
    typename Self::Container& res_data = res->get_data();

    for (size_t i = 0; i < size; ++i) res_data[i] = data[perm[i]];

    return res;
}

template <typename T>
MutableColumnPtr ColumnDecimal<T>::clone_resized(size_t size) const {
    auto res = this->create(0, scale);

    if (size > 0) {
        auto& new_col = assert_cast<Self&>(*res);
        new_col.data.resize(size);

        size_t count = std::min(this->size(), size);
        memcpy(new_col.data.data(), data.data(), count * sizeof(data[0]));

        if (size > count) {
            void* tail = &new_col.data[count];
            memset(tail, 0, (size - count) * sizeof(T));
        }
    }

    return res;
}

template <typename T>
void ColumnDecimal<T>::insert_data(const char* src, size_t /*length*/) {
    T tmp;
    memcpy(&tmp, src, sizeof(T));
    data.emplace_back(tmp);
}

template <typename T>
void ColumnDecimal<T>::insert_many_fix_len_data(const char* data_ptr, size_t num) {
    size_t old_size = data.size();
    data.resize(old_size + num);

    if constexpr (IsDecimalV2<T>) {
        DecimalV2Value* target = (DecimalV2Value*)(data.data() + old_size);
        for (int i = 0; i < num; i++) {
            const char* cur_ptr = data_ptr + sizeof(decimal12_t) * i;
            int64_t int_value = unaligned_load<int64_t>(cur_ptr);
            int32_t frac_value = *(int32_t*)(cur_ptr + sizeof(int64_t));
            target[i].from_olap_decimal(int_value, frac_value);
        }
    } else {
        memcpy(data.data() + old_size, data_ptr, num * sizeof(T));
    }
}

template <typename T>
void ColumnDecimal<T>::insert_range_from(const IColumn& src, size_t start, size_t length) {
    const ColumnDecimal& src_vec = assert_cast<const ColumnDecimal&>(src);

    if (start + length > src_vec.data.size()) {
        throw doris::Exception(doris::ErrorCode::INTERNAL_ERROR,
                               "Parameters start = {}, length = {} are out of bound in "
                               "ColumnDecimal<T>::insert_range_from method (data.size() = {})",
                               start, length, src_vec.data.size());
    }

    size_t old_size = data.size();
    data.resize(old_size + length);
    memcpy(data.data() + old_size, &src_vec.data[start], length * sizeof(data[0]));
}

template <typename T>
ColumnPtr ColumnDecimal<T>::filter(const IColumn::Filter& filt, ssize_t result_size_hint) const {
    size_t size = data.size();
    column_match_filter_size(size, filt.size());

    auto res = this->create(0, scale);
    Container& res_data = res->get_data();

    if (result_size_hint) res_data.reserve(result_size_hint > 0 ? result_size_hint : size);

    const UInt8* filt_pos = filt.data();
    const UInt8* filt_end = filt_pos + size;
    const T* data_pos = data.data();

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
                    [&](const size_t bit_pos) { res_data.push_back(data_pos[bit_pos]); }, mask);
        }

        filt_pos += SIMD_BYTES;
        data_pos += SIMD_BYTES;
    }

    while (filt_pos < filt_end) {
        if (*filt_pos) res_data.push_back(*data_pos);

        ++filt_pos;
        ++data_pos;
    }

    return res;
}

template <typename T>
size_t ColumnDecimal<T>::filter(const IColumn::Filter& filter) {
    size_t size = data.size();
    column_match_filter_size(size, filter.size());

    const UInt8* filter_pos = filter.data();
    const UInt8* filter_end = filter_pos + size;
    const T* data_pos = data.data();
    T* result_data = data.data();

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
            memmove(result_data, data_pos, sizeof(T) * SIMD_BYTES);
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

    const auto result_size = result_data - data.data();
    data.set_end_ptr(result_data);

    return result_size;
}

template <typename T>
ColumnPtr ColumnDecimal<T>::replicate(const IColumn::Offsets& offsets) const {
    size_t size = data.size();
    column_match_offsets_size(size, offsets.size());

    auto res = this->create(0, scale);
    if (0 == size) return res;

    typename Self::Container& res_data = res->get_data();
    res_data.reserve(offsets.back());

    IColumn::Offset prev_offset = 0;
    for (size_t i = 0; i < size; ++i) {
        size_t size_to_replicate = offsets[i] - prev_offset;
        prev_offset = offsets[i];

        for (size_t j = 0; j < size_to_replicate; ++j) res_data.push_back(data[i]);
    }

    return res;
}

template <typename T>
void ColumnDecimal<T>::sort_column(const ColumnSorter* sorter, EqualFlags& flags,
                                   IColumn::Permutation& perms, EqualRange& range,
                                   bool last_column) const {
    sorter->template sort_column(static_cast<const Self&>(*this), flags, perms, range, last_column);
}

template <typename T>
void ColumnDecimal<T>::compare_internal(size_t rhs_row_id, const IColumn& rhs,
                                        int nan_direction_hint, int direction,
                                        std::vector<uint8>& cmp_res,
                                        uint8* __restrict filter) const {
    auto sz = this->size();
    DCHECK(cmp_res.size() == sz);
    const auto& cmp_base = assert_cast<const ColumnDecimal<T>&>(rhs).get_data()[rhs_row_id];

    size_t begin = simd::find_zero(cmp_res, 0);
    while (begin < sz) {
        size_t end = simd::find_one(cmp_res, begin + 1);
        for (size_t row_id = begin; row_id < end; row_id++) {
            auto value_a = get_data()[row_id];
            int res = 0;
            res = value_a > cmp_base ? 1 : (value_a < cmp_base ? -1 : 0);
            if (res * direction < 0) {
                filter[row_id] = 1;
                cmp_res[row_id] = 1;
            } else if (res * direction > 0) {
                cmp_res[row_id] = 1;
            }
        }
        begin = simd::find_zero(cmp_res, end + 1);
    }
}

template <>
Decimal32 ColumnDecimal<Decimal32>::get_scale_multiplier() const {
    return common::exp10_i32(scale);
}

template <>
Decimal64 ColumnDecimal<Decimal64>::get_scale_multiplier() const {
    return common::exp10_i64(scale);
}

template <>
Decimal128V2 ColumnDecimal<Decimal128V2>::get_scale_multiplier() const {
    return common::exp10_i128(scale);
}

template <>
Decimal128V3 ColumnDecimal<Decimal128V3>::get_scale_multiplier() const {
    return common::exp10_i128(scale);
}

// duplicate with
// Decimal256 DataTypeDecimal<Decimal256>::get_scale_multiplier(UInt32 scale) {
template <>
Decimal256 ColumnDecimal<Decimal256>::get_scale_multiplier() const {
    return Decimal256(common::exp10_i256(scale));
}

template <typename T>
void ColumnDecimal<T>::replace_column_null_data(const uint8_t* __restrict null_map) {
    auto s = size();
    size_t null_count = s - simd::count_zero_num((const int8_t*)null_map, s);
    if (0 == null_count) {
        return;
    }
    for (size_t i = 0; i < s; ++i) {
        data[i] = null_map[i] ? T() : data[i];
    }
}

template class ColumnDecimal<Decimal32>;
template class ColumnDecimal<Decimal64>;
template class ColumnDecimal<Decimal128V2>;
template class ColumnDecimal<Decimal128V3>;
template class ColumnDecimal<Decimal256>;
} // namespace doris::vectorized
