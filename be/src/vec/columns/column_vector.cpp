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
#include "vec/columns/column_impl.h"
#include "vec/columns/columns_common.h"
#include "vec/common/arena.h"
#include "vec/common/assert_cast.h"
#include "vec/common/bit_cast.h"
#include "vec/common/memcpy_small.h"
#include "vec/common/nan_utils.h"
#include "vec/common/radix_sort.h"
#include "vec/common/sip_hash.h"
#include "vec/common/unaligned.h"
#include "vec/core/sort_block.h"
#include "vec/data_types/data_type.h"

namespace doris::vectorized {

template <typename T>
StringRef ColumnVector<T>::serialize_value_into_arena(size_t n, Arena& arena,
                                                      char const*& begin) const {
    auto pos = arena.alloc_continue(sizeof(T), begin);
    unaligned_store<T>(pos, data[n]);
    return StringRef(pos, sizeof(T));
}

template <typename T>
const char* ColumnVector<T>::deserialize_and_insert_from_arena(const char* pos) {
    data.push_back(unaligned_load<T>(pos));
    return pos + sizeof(T);
}

template <typename T>
size_t ColumnVector<T>::get_max_row_byte_size() const {
    return sizeof(T);
}

template <typename T>
void ColumnVector<T>::serialize_vec(std::vector<StringRef>& keys, size_t num_rows,
                                    size_t max_row_byte_size) const {
    for (size_t i = 0; i < num_rows; ++i) {
        memcpy_fixed<T>(const_cast<char*>(keys[i].data + keys[i].size), (char*)&data[i]);
        keys[i].size += sizeof(T);
    }
}

template <typename T>
void ColumnVector<T>::serialize_vec_with_null_map(std::vector<StringRef>& keys, size_t num_rows,
                                                  const uint8_t* null_map) const {
    for (size_t i = 0; i < num_rows; ++i) {
        if (null_map[i] == 0) {
            memcpy_fixed<T>(const_cast<char*>(keys[i].data + keys[i].size), (char*)&data[i]);
            keys[i].size += sizeof(T);
        }
    }
}

template <typename T>
void ColumnVector<T>::deserialize_vec(std::vector<StringRef>& keys, const size_t num_rows) {
    for (size_t i = 0; i != num_rows; ++i) {
        keys[i].data = deserialize_and_insert_from_arena(keys[i].data);
        keys[i].size -= sizeof(T);
    }
}

template <typename T>
void ColumnVector<T>::deserialize_vec_with_null_map(std::vector<StringRef>& keys,
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
void ColumnVector<T>::update_hash_with_value(size_t n, SipHash& hash) const {
    hash.update(data[n]);
}

template <typename T>
void ColumnVector<T>::update_hashes_with_value(std::vector<SipHash>& hashes,
                                               const uint8_t* __restrict null_data) const {
    SIP_HASHES_FUNCTION_COLUMN_IMPL();
}

template <typename T>
void ColumnVector<T>::update_hashes_with_value(uint64_t* __restrict hashes,
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
void ColumnVector<T>::sort_column(const ColumnSorter* sorter, EqualFlags& flags,
                                  IColumn::Permutation& perms, EqualRange& range,
                                  bool last_column) const {
    sorter->template sort_column(static_cast<const Self&>(*this), flags, perms, range, last_column);
}

template <typename T>
void ColumnVector<T>::compare_internal(size_t rhs_row_id, const IColumn& rhs,
                                       int nan_direction_hint, int direction,
                                       std::vector<uint8>& cmp_res,
                                       uint8* __restrict filter) const {
    auto sz = this->size();
    DCHECK(cmp_res.size() == sz);
    const auto& cmp_base = assert_cast<const ColumnVector<T>&>(rhs).get_data()[rhs_row_id];
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

template <typename T>
void ColumnVector<T>::update_crcs_with_value(std::vector<uint64_t>& hashes, PrimitiveType type,
                                             const uint8_t* __restrict null_data) const {
    auto s = hashes.size();
    DCHECK(s == size());

    if constexpr (!std::is_same_v<T, Int64>) {
        DO_CRC_HASHES_FUNCTION_COLUMN_IMPL()
    } else {
        if (type == TYPE_DATE || type == TYPE_DATETIME) {
            char buf[64];
            auto date_convert_do_crc = [&](size_t i) {
                const VecDateTimeValue& date_val = (const VecDateTimeValue&)data[i];
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
            DO_CRC_HASHES_FUNCTION_COLUMN_IMPL()
        }
    }
}

template <typename T>
struct ColumnVector<T>::less {
    const Self& parent;
    int nan_direction_hint;
    less(const Self& parent_, int nan_direction_hint_)
            : parent(parent_), nan_direction_hint(nan_direction_hint_) {}
    bool operator()(size_t lhs, size_t rhs) const {
        return CompareHelper<T>::less(parent.data[lhs], parent.data[rhs], nan_direction_hint);
    }
};

template <typename T>
struct ColumnVector<T>::greater {
    const Self& parent;
    int nan_direction_hint;
    greater(const Self& parent_, int nan_direction_hint_)
            : parent(parent_), nan_direction_hint(nan_direction_hint_) {}
    bool operator()(size_t lhs, size_t rhs) const {
        return CompareHelper<T>::greater(parent.data[lhs], parent.data[rhs], nan_direction_hint);
    }
};

namespace {
template <typename T>
struct ValueWithIndex {
    T value;
    UInt32 index;
};

template <typename T>
struct RadixSortTraits : RadixSortNumTraits<T> {
    using Element = ValueWithIndex<T>;
    static T& extract_key(Element& elem) { return elem.value; }
};
} // namespace

template <typename T>
void ColumnVector<T>::get_permutation(bool reverse, size_t limit, int nan_direction_hint,
                                      IColumn::Permutation& res) const {
    size_t s = data.size();
    res.resize(s);

    if (s == 0) return;

    if (limit >= s) limit = 0;

    if (limit) {
        for (size_t i = 0; i < s; ++i) res[i] = i;

        if (reverse)
            std::partial_sort(res.begin(), res.begin() + limit, res.end(),
                              greater(*this, nan_direction_hint));
        else
            std::partial_sort(res.begin(), res.begin() + limit, res.end(),
                              less(*this, nan_direction_hint));
    } else {
        /// A case for radix sort
        if constexpr (std::is_arithmetic_v<T> && !std::is_same_v<T, UInt128>) {
            /// Thresholds on size. Lower threshold is arbitrary. Upper threshold is chosen by the type for histogram counters.
            if (s >= 256 && s <= std::numeric_limits<UInt32>::max()) {
                PaddedPODArray<ValueWithIndex<T>> pairs(s);
                for (UInt32 i = 0; i < s; ++i) pairs[i] = {data[i], i};

                RadixSort<RadixSortTraits<T>>::execute_lsd(pairs.data(), s);

                /// Radix sort treats all NaNs to be greater than all numbers.
                /// If the user needs the opposite, we must move them accordingly.
                size_t nans_to_move = 0;
                if (std::is_floating_point_v<T> && nan_direction_hint < 0) {
                    for (ssize_t i = s - 1; i >= 0; --i) {
                        if (is_nan(pairs[i].value))
                            ++nans_to_move;
                        else
                            break;
                    }
                }

                if (reverse) {
                    if (nans_to_move) {
                        for (size_t i = 0; i < s - nans_to_move; ++i)
                            res[i] = pairs[s - nans_to_move - 1 - i].index;
                        for (size_t i = s - nans_to_move; i < s; ++i)
                            res[i] = pairs[s - 1 - (i - (s - nans_to_move))].index;
                    } else {
                        for (size_t i = 0; i < s; ++i) res[s - 1 - i] = pairs[i].index;
                    }
                } else {
                    if (nans_to_move) {
                        for (size_t i = 0; i < nans_to_move; ++i)
                            res[i] = pairs[i + s - nans_to_move].index;
                        for (size_t i = nans_to_move; i < s; ++i)
                            res[i] = pairs[i - nans_to_move].index;
                    } else {
                        for (size_t i = 0; i < s; ++i) res[i] = pairs[i].index;
                    }
                }

                return;
            }
        }

        /// Default sorting algorithm.
        for (size_t i = 0; i < s; ++i) res[i] = i;

        if (reverse)
            pdqsort(res.begin(), res.end(), greater(*this, nan_direction_hint));
        else
            pdqsort(res.begin(), res.end(), less(*this, nan_direction_hint));
    }
}

template <typename T>
const char* ColumnVector<T>::get_family_name() const {
    return TypeName<T>::get();
}

template <typename T>
MutableColumnPtr ColumnVector<T>::clone_resized(size_t size) const {
    auto res = this->create();
    if constexpr (std::is_same_v<T, vectorized::Int64>) {
        res->copy_date_types(*this);
    }

    if (size > 0) {
        auto& new_col = assert_cast<Self&>(*res);
        new_col.data.resize(size);

        size_t count = std::min(this->size(), size);
        memcpy(new_col.data.data(), data.data(), count * sizeof(data[0]));

        if (size > count)
            memset(static_cast<void*>(&new_col.data[count]), static_cast<int>(value_type()),
                   (size - count) * sizeof(value_type));
    }

    return res;
}

template <typename T>
UInt64 ColumnVector<T>::get64(size_t n) const {
    return ext::bit_cast<UInt64>(data[n]);
}

template <typename T>
Float64 ColumnVector<T>::get_float64(size_t n) const {
    return static_cast<Float64>(data[n]);
}

template <typename T>
void ColumnVector<T>::insert_range_from(const IColumn& src, size_t start, size_t length) {
    const ColumnVector& src_vec = assert_cast<const ColumnVector&>(src);
    if (start + length > src_vec.data.size()) {
        LOG(FATAL) << fmt::format(
                "Parameters start = {}, length = {}, are out of bound in "
                "ColumnVector<T>::insert_range_from method (data.size() = {}).",
                start, length, src_vec.data.size());
    }

    size_t old_size = data.size();
    data.resize(old_size + length);
    memcpy(data.data() + old_size, &src_vec.data[start], length * sizeof(data[0]));
}

template <typename T>
void ColumnVector<T>::insert_indices_from(const IColumn& src,
                                          const uint32* __restrict indices_begin,
                                          const uint32* __restrict indices_end) {
    auto origin_size = size();
    auto new_size = indices_end - indices_begin;
    data.resize(origin_size + new_size);

    const T* src_data = reinterpret_cast<const T*>(src.get_raw_data().data);

    if constexpr (std::is_same_v<T, UInt8>) {
        // nullmap : indices_begin[i] == 0 means is null at the here, set true here
        for (int i = 0; i < new_size; ++i) {
            data[origin_size + i] =
                    (indices_begin[i] == 0) + (indices_begin[i] == 0) * src_data[indices_begin[i]];
        }
    } else {
        // real data : indices_begin[i] == 0 what at is meaningless
        for (int i = 0; i < new_size; ++i) {
            data[origin_size + i] = src_data[indices_begin[i]];
        }
    }
}

template <typename T>
ColumnPtr ColumnVector<T>::filter(const IColumn::Filter& filt, ssize_t result_size_hint) const {
    size_t size = data.size();
    column_match_filter_size(size, filt.size());

    auto res = this->create();
    if constexpr (std::is_same_v<T, vectorized::Int64>) {
        res->copy_date_types(*this);
    }
    Container& res_data = res->get_data();

    res_data.reserve(result_size_hint > 0 ? result_size_hint : size);

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
                res_data.push_back_without_reserve(data_pos[idx]);
                mask = mask & (mask - 1);
            }
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

template <typename T>
size_t ColumnVector<T>::filter(const IColumn::Filter& filter) {
    size_t size = data.size();
    column_match_filter_size(size, filter.size());

    const UInt8* filter_pos = filter.data();
    const UInt8* filter_end = filter_pos + size;
    T* data_pos = data.data();
    T* result_data = data_pos;

    /** A slightly more optimized version.
        * Based on the assumption that often pieces of consecutive values
        *  completely pass or do not pass the filter.
        * Therefore, we will optimistically check the parts of `SIMD_BYTES` values.
        */
    static constexpr size_t SIMD_BYTES = 32;
    const UInt8* filter_end_sse = filter_pos + size / SIMD_BYTES * SIMD_BYTES;

    while (filter_pos < filter_end_sse) {
        uint32_t mask = simd::bytes32_mask_to_bits32_mask(filter_pos);

        if (0xFFFFFFFF == mask) {
            memmove(result_data, data_pos, sizeof(T) * SIMD_BYTES);
            result_data += SIMD_BYTES;
        } else {
            while (mask) {
                const size_t idx = __builtin_ctzll(mask);
                *result_data = data_pos[idx];
                ++result_data;
                mask = mask & (mask - 1);
            }
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

template <typename T>
ColumnPtr ColumnVector<T>::permute(const IColumn::Permutation& perm, size_t limit) const {
    size_t size = data.size();

    if (limit == 0)
        limit = size;
    else
        limit = std::min(size, limit);

    if (perm.size() < limit) {
        LOG(FATAL) << "Size of permutation is less than required.";
    }

    auto res = this->create(limit);
    if constexpr (std::is_same_v<T, vectorized::Int64>) {
        res->copy_date_types(*this);
    }
    typename Self::Container& res_data = res->get_data();
    for (size_t i = 0; i < limit; ++i) res_data[i] = data[perm[i]];

    return res;
}

template <typename T>
ColumnPtr ColumnVector<T>::replicate(const IColumn::Offsets& offsets) const {
    size_t size = data.size();
    column_match_offsets_size(size, offsets.size());

    auto res = this->create();
    if constexpr (std::is_same_v<T, vectorized::Int64>) {
        res->copy_date_types(*this);
    }
    if (0 == size) return res;

    typename Self::Container& res_data = res->get_data();
    res_data.reserve(offsets.back());

    // vectorized this code to speed up
    IColumn::Offset counts[size];
    for (ssize_t i = 0; i < size; ++i) {
        counts[i] = offsets[i] - offsets[i - 1];
    }

    for (size_t i = 0; i < size; ++i) {
        res_data.add_num_element_without_reserve(data[i], counts[i]);
    }

    return res;
}

template <typename T>
void ColumnVector<T>::replicate(const uint32_t* __restrict indexs, size_t target_size,
                                IColumn& column) const {
    auto& res = reinterpret_cast<ColumnVector<T>&>(column);
    typename Self::Container& res_data = res.get_data();
    DCHECK(res_data.empty());
    res_data.resize(target_size);
    auto* __restrict left = res_data.data();
    auto* __restrict right = data.data();
    auto* __restrict idxs = indexs;

    for (size_t i = 0; i < target_size; ++i) {
        left[i] = right[idxs[i]];
    }
}

template <typename T>
ColumnPtr ColumnVector<T>::index(const IColumn& indexes, size_t limit) const {
    return select_index_impl(*this, indexes, limit);
}

/// Explicit template instantiations - to avoid code bloat in headers.
template class ColumnVector<UInt8>;
template class ColumnVector<UInt16>;
template class ColumnVector<UInt32>;
template class ColumnVector<UInt64>;
template class ColumnVector<UInt128>;
template class ColumnVector<Int8>;
template class ColumnVector<Int16>;
template class ColumnVector<Int32>;
template class ColumnVector<Int64>;
template class ColumnVector<Int128>;
template class ColumnVector<Float32>;
template class ColumnVector<Float64>;
} // namespace doris::vectorized
