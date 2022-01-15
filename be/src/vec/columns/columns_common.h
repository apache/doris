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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Columns/ColumnsCommon.h
// and modified by Doris

#pragma once

#include "vec/columns/column.h"

#ifdef __AVX2__
#include <immintrin.h>
#elif __SSE2__
#include <emmintrin.h>
#endif

/// Common helper methods for implementation of different columns.

namespace doris::vectorized {

/// Transform 32-byte mask to 32-bit mask
inline uint32_t bytes32_mask_to_bits32_mask(const uint8_t* filt_pos) {
#ifdef __AVX2__
    auto zero32 = _mm256_setzero_si256();
    uint32_t mask = static_cast<uint32_t>(_mm256_movemask_epi8(_mm256_cmpgt_epi8(
            _mm256_loadu_si256(reinterpret_cast<const __m256i*>(filt_pos)), zero32)));
#elif __SSE2__
    auto zero16 = _mm_setzero_si128();
    uint32_t mask =
            (static_cast<uint32_t>(_mm_movemask_epi8(_mm_cmpgt_epi8(
                    _mm_loadu_si128(reinterpret_cast<const __m128i*>(filt_pos)), zero16)))) |
            ((static_cast<uint32_t>(_mm_movemask_epi8(_mm_cmpgt_epi8(
                      _mm_loadu_si128(reinterpret_cast<const __m128i*>(filt_pos + 16)), zero16)))
              << 16) &
             0xffff0000);
#else
    uint32_t mask = 0;
    for (size_t i = 0; i < 32; ++i) {
        mask |= static_cast<uint32_t>(1 == *(filt_pos + i)) << i;
    }
#endif
    return mask;
}

/// Counts how many bytes of `filt` are greater than zero.
size_t count_bytes_in_filter(const IColumn::Filter& filt);

/// Returns vector with num_columns elements. vector[i] is the count of i values in selector.
/// Selector must contain values from 0 to num_columns - 1. NOTE: this is not checked.
std::vector<size_t> count_columns_size_in_selector(IColumn::ColumnIndex num_columns,
                                                   const IColumn::Selector& selector);

/// Returns true, if the memory contains only zeros.
bool memory_is_zero(const void* data, size_t size);
bool memory_is_byte(const void* data, size_t size, uint8_t byte);

/// The general implementation of `filter` function for ColumnArray and ColumnString.
template <typename T>
void filter_arrays_impl(const PaddedPODArray<T>& src_elems, const IColumn::Offsets& src_offsets,
                        PaddedPODArray<T>& res_elems, IColumn::Offsets& res_offsets,
                        const IColumn::Filter& filt, ssize_t result_size_hint);

/// Same as above, but not fills res_offsets.
template <typename T>
void filter_arrays_impl_only_data(const PaddedPODArray<T>& src_elems,
                                  const IColumn::Offsets& src_offsets, PaddedPODArray<T>& res_elems,
                                  const IColumn::Filter& filt, ssize_t result_size_hint);

namespace detail {
template <typename T>
const PaddedPODArray<T>* get_indexes_data(const IColumn& indexes);
}

/// Check limit <= indexes->size() and call column.index_impl(const PaddedPodArray<Type> & indexes, UInt64 limit).
template <typename Column>
ColumnPtr select_index_impl(const Column& column, const IColumn& indexes, size_t limit) {
    if (limit == 0) limit = indexes.size();

    if (indexes.size() < limit) {
        LOG(FATAL) << "Size of indexes is less than required.";
    }

    if (auto* data_uint8 = detail::get_indexes_data<UInt8>(indexes))
        return column.template index_impl<UInt8>(*data_uint8, limit);
    else if (auto* data_uint16 = detail::get_indexes_data<UInt16>(indexes))
        return column.template index_impl<UInt16>(*data_uint16, limit);
    else if (auto* data_uint32 = detail::get_indexes_data<UInt32>(indexes))
        return column.template index_impl<UInt32>(*data_uint32, limit);
    else if (auto* data_uint64 = detail::get_indexes_data<UInt64>(indexes))
        return column.template index_impl<UInt64>(*data_uint64, limit);
    else {
        LOG(FATAL) << "Indexes column for IColumn::select must be ColumnUInt, got"
                   << indexes.get_name();
        return nullptr;
    }
}

#define INSTANTIATE_INDEX_IMPL(Column)                                                  \
    template ColumnPtr Column::indexImpl<UInt8>(const PaddedPODArray<UInt8>& indexes,   \
                                                size_t limit) const;                    \
    template ColumnPtr Column::indexImpl<UInt16>(const PaddedPODArray<UInt16>& indexes, \
                                                 size_t limit) const;                   \
    template ColumnPtr Column::indexImpl<UInt32>(const PaddedPODArray<UInt32>& indexes, \
                                                 size_t limit) const;                   \
    template ColumnPtr Column::indexImpl<UInt64>(const PaddedPODArray<UInt64>& indexes, \
                                                 size_t limit) const;
} // namespace doris::vectorized
