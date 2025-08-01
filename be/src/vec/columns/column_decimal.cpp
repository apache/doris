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
#include "vec/data_types/data_type_decimal.h"

template <typename T>
bool decimal_less(T x, T y, doris::vectorized::UInt32 x_scale, doris::vectorized::UInt32 y_scale);

namespace doris::vectorized {

template <PrimitiveType T>
int ColumnDecimal<T>::compare_at(size_t n, size_t m, const IColumn& rhs_, int) const {
    auto& other = assert_cast<const Self&, TypeCheckOnRelease::DISABLE>(rhs_);
    const value_type& a = data[n];
    const value_type& b = other.data[m];

    if (scale == other.scale) {
        return a > b ? 1 : (a < b ? -1 : 0);
    }
    return decimal_less<value_type>(b, a, other.scale, scale)
                   ? 1
                   : (decimal_less<value_type>(a, b, scale, other.scale) ? -1 : 0);
}

template <PrimitiveType T>
StringRef ColumnDecimal<T>::serialize_value_into_arena(size_t n, Arena& arena,
                                                       char const*& begin) const {
    auto* pos = arena.alloc_continue(sizeof(value_type), begin);
    return {pos, serialize_impl(pos, n)};
}

template <PrimitiveType T>
const char* ColumnDecimal<T>::deserialize_and_insert_from_arena(const char* pos) {
    return pos + deserialize_impl(pos);
}

template <PrimitiveType T>
size_t ColumnDecimal<T>::get_max_row_byte_size() const {
    return sizeof(value_type);
}

template <PrimitiveType T>
void ColumnDecimal<T>::serialize_vec(StringRef* keys, size_t num_rows) const {
    for (size_t i = 0; i < num_rows; ++i) {
        keys[i].size += serialize_impl(const_cast<char*>(keys[i].data + keys[i].size), i);
    }
}

template <PrimitiveType T>
size_t ColumnDecimal<T>::serialize_impl(char* pos, const size_t row) const {
    memcpy_fixed<value_type>(pos, (char*)&data[row]);
    return sizeof(value_type);
}

template <PrimitiveType T>
void ColumnDecimal<T>::deserialize_vec(StringRef* keys, const size_t num_rows) {
    for (size_t i = 0; i < num_rows; ++i) {
        auto sz = deserialize_impl(keys[i].data);
        keys[i].data += sz;
        keys[i].size -= sz;
    }
}

template <PrimitiveType T>
size_t ColumnDecimal<T>::deserialize_impl(const char* pos) {
    data.push_back(unaligned_load<value_type>(pos));
    return sizeof(value_type);
}

template <PrimitiveType T>
void ColumnDecimal<T>::update_hash_with_value(size_t n, SipHash& hash) const {
    hash.update(data[n]);
}

template <PrimitiveType T>
void ColumnDecimal<T>::update_crc_with_value(size_t start, size_t end, uint32_t& hash,
                                             const uint8_t* __restrict null_data) const {
    if (null_data == nullptr) {
        for (size_t i = start; i < end; i++) {
            if constexpr (T != TYPE_DECIMALV2) {
                hash = HashUtil::zlib_crc_hash(&data[i], sizeof(value_type), hash);
            } else {
                decimalv2_do_crc(i, hash);
            }
        }
    } else {
        for (size_t i = start; i < end; i++) {
            if (null_data[i] == 0) {
                if constexpr (T != TYPE_DECIMALV2) {
                    hash = HashUtil::zlib_crc_hash(&data[i], sizeof(value_type), hash);
                } else {
                    decimalv2_do_crc(i, hash);
                }
            }
        }
    }
}

template <PrimitiveType T>
void ColumnDecimal<T>::update_crcs_with_value(uint32_t* __restrict hashes, PrimitiveType type,
                                              uint32_t rows, uint32_t offset,
                                              const uint8_t* __restrict null_data) const {
    auto s = rows;
    DCHECK(s == size());

    if constexpr (T != TYPE_DECIMALV2) {
        if (null_data == nullptr) {
            for (size_t i = 0; i < s; i++) {
                hashes[i] = HashUtil::zlib_crc_hash(&data[i], sizeof(value_type), hashes[i]);
            }
        } else {
            for (size_t i = 0; i < s; i++) {
                if (null_data[i] == 0)
                    hashes[i] = HashUtil::zlib_crc_hash(&data[i], sizeof(value_type), hashes[i]);
            }
        }
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

template <PrimitiveType T>
void ColumnDecimal<T>::update_xxHash_with_value(size_t start, size_t end, uint64_t& hash,
                                                const uint8_t* __restrict null_data) const {
    if (null_data) {
        for (size_t i = start; i < end; i++) {
            if (null_data[i] == 0) {
                hash = HashUtil::xxHash64WithSeed(reinterpret_cast<const char*>(&data[i]),
                                                  sizeof(value_type), hash);
            }
        }
    } else {
        for (size_t i = start; i < end; i++) {
            hash = HashUtil::xxHash64WithSeed(reinterpret_cast<const char*>(&data[i]),
                                              sizeof(value_type), hash);
        }
    }
}

template <PrimitiveType T>
void ColumnDecimal<T>::update_hashes_with_value(uint64_t* __restrict hashes,
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
Field ColumnDecimal<T>::operator[](size_t n) const {
    return Field::create_field<T>(DecimalField<value_type>(data[n], scale));
}

template <PrimitiveType T>
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
}

template <PrimitiveType T>
MutableColumnPtr ColumnDecimal<T>::permute(const IColumn::Permutation& perm, size_t limit) const {
    size_t size = limit ? std::min(data.size(), limit) : data.size();
    if (perm.size() < size) {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                               "Size of permutation ({}) is less than required ({})", perm.size(),
                               limit);
    }

    auto res = this->create(size, scale);
    typename Self::Container& res_data = res->get_data();

    for (size_t i = 0; i < size; ++i) res_data[i] = data[perm[i]];

    return res;
}

template <PrimitiveType T>
MutableColumnPtr ColumnDecimal<T>::clone_resized(size_t size) const {
    auto res = this->create(0, scale);

    if (size > 0) {
        auto& new_col = assert_cast<Self&>(*res);
        new_col.data.resize(size);

        size_t count = std::min(this->size(), size);
        memcpy(new_col.data.data(), data.data(), count * sizeof(data[0]));

        if (size > count) {
            void* tail = &new_col.data[count];
            memset(tail, 0, (size - count) * sizeof(value_type));
        }
    }

    return res;
}

template <PrimitiveType T>
void ColumnDecimal<T>::insert_data(const char* src, size_t /*length*/) {
    value_type tmp;
    memcpy(&tmp, src, sizeof(value_type));
    data.emplace_back(tmp);
}

template <PrimitiveType T>
void ColumnDecimal<T>::insert_many_fix_len_data(const char* data_ptr, size_t num) {
    size_t old_size = data.size();
    data.resize(old_size + num);

    if constexpr (T == TYPE_DECIMALV2) {
        DecimalV2Value* target = (DecimalV2Value*)(data.data() + old_size);
        for (int i = 0; i < num; i++) {
            const char* cur_ptr = data_ptr + sizeof(decimal12_t) * i;
            auto int_value = unaligned_load<int64_t>(cur_ptr);
            auto frac_value = unaligned_load<int32_t>(cur_ptr + sizeof(int64_t));
            target[i].from_olap_decimal(int_value, frac_value);
        }
    } else {
        memcpy(data.data() + old_size, data_ptr, num * sizeof(value_type));
    }
}

template <PrimitiveType T>
void ColumnDecimal<T>::insert_many_from(const IColumn& src, size_t position, size_t length) {
    auto old_size = data.size();
    data.resize(old_size + length);
    auto& vals = assert_cast<const Self&>(src).get_data();
    std::fill(&data[old_size], &data[old_size + length], vals[position]);
}

template <PrimitiveType T>
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

template <PrimitiveType T>
ColumnPtr ColumnDecimal<T>::filter(const IColumn::Filter& filt, ssize_t result_size_hint) const {
    size_t size = data.size();
    column_match_filter_size(size, filt.size());

    auto res = this->create(0, scale);
    Container& res_data = res->get_data();

    if (result_size_hint) res_data.reserve(result_size_hint > 0 ? result_size_hint : size);

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

template <PrimitiveType T>
size_t ColumnDecimal<T>::filter(const IColumn::Filter& filter) {
    size_t size = data.size();
    column_match_filter_size(size, filter.size());

    const UInt8* filter_pos = filter.data();
    const UInt8* filter_end = filter_pos + size;
    const value_type* data_pos = data.data();
    value_type* result_data = data.data();

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

    const auto result_size = result_data - data.data();
    data.resize(result_size);

    return result_size;
}

template <PrimitiveType T>
void ColumnDecimal<T>::sort_column(const ColumnSorter* sorter, EqualFlags& flags,
                                   IColumn::Permutation& perms, EqualRange& range,
                                   bool last_column) const {
    sorter->sort_column(static_cast<const Self&>(*this), flags, perms, range, last_column);
}

template <PrimitiveType T>
void ColumnDecimal<T>::compare_internal(size_t rhs_row_id, const IColumn& rhs,
                                        int nan_direction_hint, int direction,
                                        std::vector<uint8_t>& cmp_res,
                                        uint8_t* __restrict filter) const {
    auto sz = this->size();
    DCHECK(cmp_res.size() == sz);
    const auto& cmp_base = assert_cast<const ColumnDecimal<T>&>(rhs).get_data()[rhs_row_id];

    size_t begin = simd::find_zero(cmp_res, 0);
    while (begin < sz) {
        size_t end = simd::find_one(cmp_res, begin + 1);
        for (size_t row_id = begin; row_id < end; row_id++) {
            auto value_a = get_data()[row_id];
            int res = value_a > cmp_base ? 1 : (value_a < cmp_base ? -1 : 0);
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

template <PrimitiveType T>
void ColumnDecimal<T>::replace_column_null_data(const uint8_t* __restrict null_map) {
    auto s = size();
    size_t null_count = s - simd::count_zero_num((const int8_t*)null_map, s);
    if (0 == null_count) {
        return;
    }
    for (size_t i = 0; i < s; ++i) {
        data[i] = null_map[i] ? value_type() : data[i];
    }
}

template <PrimitiveType T>
ColumnDecimal<T>::CppNativeType ColumnDecimal<T>::get_intergral_part(size_t n) const {
    return data[n].value / DataTypeDecimal<value_type::PType>::get_scale_multiplier(scale);
}
template <PrimitiveType T>
ColumnDecimal<T>::CppNativeType ColumnDecimal<T>::get_fractional_part(size_t n) const {
    return data[n].value % DataTypeDecimal<value_type::PType>::get_scale_multiplier(scale);
}

template class ColumnDecimal<TYPE_DECIMAL32>;
template class ColumnDecimal<TYPE_DECIMAL64>;
template class ColumnDecimal<TYPE_DECIMALV2>;
template class ColumnDecimal<TYPE_DECIMAL128I>;
template class ColumnDecimal<TYPE_DECIMAL256>;
} // namespace doris::vectorized
