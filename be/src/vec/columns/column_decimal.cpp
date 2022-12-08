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

#include "common/config.h"
#include "util/simd/bits.h"
#include "vec/common/arena.h"
#include "vec/common/assert_cast.h"
#include "vec/common/exception.h"
#include "vec/common/sip_hash.h"
#include "vec/common/unaligned.h"
#include "vec/core/sort_block.h"

template <typename T>
bool decimal_less(T x, T y, doris::vectorized::UInt32 x_scale, doris::vectorized::UInt32 y_scale);

namespace doris::vectorized {

template <typename T>
int ColumnDecimal<T>::compare_at(size_t n, size_t m, const IColumn& rhs_, int) const {
    auto& other = assert_cast<const Self&>(rhs_);
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
        memcpy(const_cast<char*>(keys[i].data + keys[i].size), &data[i], sizeof(T));
        keys[i].size += sizeof(T);
    }
}

template <typename T>
void ColumnDecimal<T>::serialize_vec_with_null_map(std::vector<StringRef>& keys, size_t num_rows,
                                                   const uint8_t* null_map,
                                                   size_t max_row_byte_size) const {
    for (size_t i = 0; i < num_rows; ++i) {
        if (null_map[i] == 0) {
            memcpy(const_cast<char*>(keys[i].data + keys[i].size), &data[i], sizeof(T));
            keys[i].size += sizeof(T);
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
UInt64 ColumnDecimal<T>::get64(size_t n) const {
    if constexpr (sizeof(T) > sizeof(UInt64)) {
        LOG(FATAL) << "Method get64 is not supported for " << get_family_name();
    }
    return static_cast<typename T::NativeType>(data[n]);
}

template <typename T>
void ColumnDecimal<T>::update_hash_with_value(size_t n, SipHash& hash) const {
    hash.update(data[n]);
}

template <typename T>
void ColumnDecimal<T>::update_hashes_with_value(std::vector<SipHash>& hashes,
                                                const uint8_t* __restrict null_data) const {
    SIP_HASHES_FUNCTION_COLUMN_IMPL();
}

template <typename T>
void ColumnDecimal<T>::update_crcs_with_value(std::vector<uint64_t>& hashes, PrimitiveType type,
                                              const uint8_t* __restrict null_data) const {
    auto s = hashes.size();
    DCHECK(s == size());

    if constexpr (!IsDecimalV2<T>) {
        DO_CRC_HASHES_FUNCTION_COLUMN_IMPL()
    } else {
        DCHECK(type == TYPE_DECIMALV2);
        auto decimalv2_do_crc = [&](size_t i) {
            const DecimalV2Value& dec_val = (const DecimalV2Value&)data[i];
            int64_t int_val = dec_val.int_value();
            int32_t frac_val = dec_val.frac_value();
            hashes[i] = HashUtil::zlib_crc_hash(&int_val, sizeof(int_val), hashes[i]);
            hashes[i] = HashUtil::zlib_crc_hash(&frac_val, sizeof(frac_val), hashes[i]);
        };

        if (null_data == nullptr) {
            for (size_t i = 0; i < s; i++) {
                decimalv2_do_crc(i);
            }
        } else {
            for (size_t i = 0; i < s; i++) {
                if (null_data[i] == 0) decimalv2_do_crc(i);
            }
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
        LOG(FATAL) << "Size of permutation is less than required.";
    }

    auto res = this->create(size, scale);
    typename Self::Container& res_data = res->get_data();

    for (size_t i = 0; i < size; ++i) res_data[i] = data[perm[i]];

    return res;
}

template <typename T>
MutableColumnPtr ColumnDecimal<T>::clone_resized(size_t size) const {
    auto res = this->create(0, scale);
    if (this->is_decimalv2_type()) {
        res->set_decimalv2_type();
    }

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

    if (this->is_decimalv2_type()) {
        DecimalV2Value* target = (DecimalV2Value*)(data.data() + old_size);
        for (int i = 0; i < num; i++) {
            const char* cur_ptr = data_ptr + sizeof(decimal12_t) * i;
            int64_t int_value = *(int64_t*)(cur_ptr);
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
        LOG(FATAL) << fmt::format(
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
    if (size != filt.size()) {
        LOG(FATAL) << "Size of filter doesn't match size of column.";
    }

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
    static constexpr size_t SIMD_BYTES = 32;
    const UInt8* filt_end_sse = filt_pos + size / SIMD_BYTES * SIMD_BYTES;

    while (filt_pos < filt_end_sse) {
        uint32_t mask = simd::bytes32_mask_to_bits32_mask(filt_pos);

        if (0xFFFFFFFF == mask) {
            res_data.insert(data_pos, data_pos + SIMD_BYTES);
        } else {
            while (mask) {
                const size_t idx = __builtin_ctzll(mask);
                res_data.push_back(data_pos[idx]);
                mask = mask & (mask - 1);
            }
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
ColumnPtr ColumnDecimal<T>::replicate(const IColumn::Offsets& offsets) const {
    size_t size = data.size();
    if (size != offsets.size()) {
        LOG(FATAL) << "Size of offsets doesn't match size of column.";
    }

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
void ColumnDecimal<T>::replicate(const uint32_t* counts, size_t target_size, IColumn& column,
                                 size_t begin, int count_sz) const {
    size_t size = count_sz < 0 ? data.size() : count_sz;
    if (0 == size) return;

    auto& res = reinterpret_cast<ColumnDecimal<T>&>(column);
    typename Self::Container& res_data = res.get_data();
    res_data.reserve(target_size);

    size_t end = size + begin;
    for (size_t i = begin; i < end; ++i) {
        res_data.add_num_element_without_reserve(data[i], counts[i]);
    }
}

template <typename T>
void ColumnDecimal<T>::get_extremes(Field& min, Field& max) const {
    if (data.size() == 0) {
        min = NearestFieldType<T>(0, scale);
        max = NearestFieldType<T>(0, scale);
        return;
    }

    T cur_min = data[0];
    T cur_max = data[0];

    for (const T& x : data) {
        if (x < cur_min)
            cur_min = x;
        else if (x > cur_max)
            cur_max = x;
    }

    min = NearestFieldType<T>(cur_min, scale);
    max = NearestFieldType<T>(cur_max, scale);
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
Decimal128 ColumnDecimal<Decimal128>::get_scale_multiplier() const {
    return common::exp10_i128(scale);
}

template <>
Decimal128I ColumnDecimal<Decimal128I>::get_scale_multiplier() const {
    return common::exp10_i128(scale);
}

template class ColumnDecimal<Decimal32>;
template class ColumnDecimal<Decimal64>;
template class ColumnDecimal<Decimal128>;
template class ColumnDecimal<Decimal128I>;
} // namespace doris::vectorized
