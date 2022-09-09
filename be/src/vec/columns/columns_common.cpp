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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Columns/ColumnsCommon.cpp
// and modified by Doris

#if defined(__SSE2__)
#include <emmintrin.h>
#endif
#if defined(__aarch64__)
#include <sse2neon.h>
#endif

#include "util/simd/bits.h"
#include "vec/columns/column.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_vector.h"
#include "vec/columns/columns_common.h"

namespace doris::vectorized {

size_t count_bytes_in_filter(const IColumn::Filter& filt) {
    size_t count = 0;

    /** NOTE: In theory, `filt` should only contain zeros and ones.
      * But, just in case, here the condition > 0 (to signed bytes) is used.
      * It would be better to use != 0, then this does not allow SSE2.
      */

    const Int8* pos = reinterpret_cast<const Int8*>(filt.data());
    const Int8* end = pos + filt.size();

#if defined(__SSE2__) || defined(__aarch64__) && defined(__POPCNT__)
    const __m128i zero16 = _mm_setzero_si128();
    const Int8* end64 = pos + filt.size() / 64 * 64;

    for (; pos < end64; pos += 64) {
        count += __builtin_popcountll(
                static_cast<UInt64>(_mm_movemask_epi8(_mm_cmpgt_epi8(
                        _mm_loadu_si128(reinterpret_cast<const __m128i*>(pos)), zero16))) |
                (static_cast<UInt64>(_mm_movemask_epi8(_mm_cmpgt_epi8(
                         _mm_loadu_si128(reinterpret_cast<const __m128i*>(pos + 16)), zero16)))
                 << 16) |
                (static_cast<UInt64>(_mm_movemask_epi8(_mm_cmpgt_epi8(
                         _mm_loadu_si128(reinterpret_cast<const __m128i*>(pos + 32)), zero16)))
                 << 32) |
                (static_cast<UInt64>(_mm_movemask_epi8(_mm_cmpgt_epi8(
                         _mm_loadu_si128(reinterpret_cast<const __m128i*>(pos + 48)), zero16)))
                 << 48));
    }

    /// TODO Add duff device for tail?
#endif

    for (; pos < end; ++pos) {
        count += *pos > 0;
    }

    return count;
}

std::vector<size_t> count_columns_size_in_selector(IColumn::ColumnIndex num_columns,
                                                   const IColumn::Selector& selector) {
    std::vector<size_t> counts(num_columns);
    for (auto idx : selector) {
        ++counts[idx];
    }

    return counts;
}

bool memory_is_byte(const void* data, size_t size, uint8_t byte) {
    if (size == 0) {
        return true;
    }
    auto ptr = reinterpret_cast<const uint8_t*>(data);
    return *ptr == byte && memcmp(ptr, ptr + 1, size - 1) == 0;
}

bool memory_is_zero(const void* data, size_t size) {
    return memory_is_byte(data, size, 0x0);
}

namespace {
/// Implementation details of filterArraysImpl function, used as template parameter.
/// Allow to build or not to build offsets array.

template <typename OT>
struct ResultOffsetsBuilder {
    PaddedPODArray<OT>& res_offsets;
    OT current_src_offset = 0;

    explicit ResultOffsetsBuilder(PaddedPODArray<OT>* res_offsets_) : res_offsets(*res_offsets_) {}

    void reserve(ssize_t result_size_hint, size_t src_size) {
        res_offsets.reserve(result_size_hint > 0 ? result_size_hint : src_size);
    }

    void insert_one(size_t array_size) {
        current_src_offset += array_size;
        res_offsets.push_back_without_reserve(current_src_offset);
    }

    template <size_t SIMD_BYTES>
    void insert_chunk(const OT* src_offsets_pos, bool first, OT chunk_offset, size_t chunk_size) {
        const auto offsets_size_old = res_offsets.size();
        res_offsets.resize_assume_reserved(offsets_size_old + SIMD_BYTES);
        memcpy(&res_offsets[offsets_size_old], src_offsets_pos, SIMD_BYTES * sizeof(OT));

        if (!first) {
            /// difference between current and actual offset
            const auto diff_offset = chunk_offset - current_src_offset;

            if (diff_offset > 0) {
                const auto res_offsets_pos = &res_offsets[offsets_size_old];

                /// adjust offsets
                for (size_t i = 0; i < SIMD_BYTES; ++i) {
                    res_offsets_pos[i] -= diff_offset;
                }
            }
        }
        current_src_offset += chunk_size;
    }
};

template <typename OT>
struct NoResultOffsetsBuilder {
    explicit NoResultOffsetsBuilder(PaddedPODArray<OT>*) {}
    void reserve(ssize_t, size_t) {}
    void insert_one(size_t) {}

    template <size_t SIMD_BYTES>
    void insert_chunk(const OT*, bool, OT, size_t) {}
};

template <typename T, typename OT, typename ResultOffsetsBuilder>
void filter_arrays_impl_generic(const PaddedPODArray<T>& src_elems,
                                const PaddedPODArray<OT>& src_offsets, PaddedPODArray<T>& res_elems,
                                PaddedPODArray<OT>* res_offsets, const IColumn::Filter& filt,
                                ssize_t result_size_hint) {
    const size_t size = src_offsets.size();
    if (size != filt.size()) {
        LOG(FATAL) << "Size of filter doesn't match size of column.";
    }

    constexpr int ASSUME_STRING_LENGTH = 5;
    ResultOffsetsBuilder result_offsets_builder(res_offsets);

    result_offsets_builder.reserve(result_size_hint, size);

    if (result_size_hint < 0) {
        res_elems.reserve(src_elems.size() * ASSUME_STRING_LENGTH);
    } else if (result_size_hint < 1000000000 && src_elems.size() < 1000000000) { /// Avoid overflow.
        res_elems.reserve(result_size_hint * ASSUME_STRING_LENGTH);
    }

    const UInt8* filt_pos = filt.data();
    const auto filt_end = filt_pos + size;

    auto offsets_pos = src_offsets.data();
    const auto offsets_begin = offsets_pos;

    /// copy array ending at *end_offset_ptr
    const auto copy_array = [&](const OT* offset_ptr) {
        const auto arr_offset = offset_ptr == offsets_begin ? 0 : offset_ptr[-1];
        const auto arr_size = *offset_ptr - arr_offset;

        result_offsets_builder.insert_one(arr_size);

        const auto elems_size_old = res_elems.size();
        res_elems.resize(elems_size_old + arr_size);
        memcpy(&res_elems[elems_size_old], &src_elems[arr_offset], arr_size * sizeof(T));
    };

    static constexpr size_t SIMD_BYTES = 32;
    const auto filt_end_aligned = filt_pos + size / SIMD_BYTES * SIMD_BYTES;

    while (filt_pos < filt_end_aligned) {
        auto mask = simd::bytes32_mask_to_bits32_mask(filt_pos);

        if (mask == 0xffffffff) {
            /// SIMD_BYTES consecutive rows pass the filter
            const auto first = offsets_pos == offsets_begin;

            const auto chunk_offset = first ? 0 : offsets_pos[-1];
            const auto chunk_size = offsets_pos[SIMD_BYTES - 1] - chunk_offset;

            result_offsets_builder.template insert_chunk<SIMD_BYTES>(offsets_pos, first,
                                                                     chunk_offset, chunk_size);

            /// copy elements for SIMD_BYTES arrays at once
            const auto elems_size_old = res_elems.size();
            res_elems.resize(elems_size_old + chunk_size);
            memcpy(&res_elems[elems_size_old], &src_elems[chunk_offset], chunk_size * sizeof(T));
        } else {
            while (mask) {
                const size_t bit_pos = __builtin_ctzll(mask);
                copy_array(offsets_pos + bit_pos);
                mask = mask & (mask - 1);
            }
        }

        filt_pos += SIMD_BYTES;
        offsets_pos += SIMD_BYTES;
    }

    while (filt_pos < filt_end) {
        if (*filt_pos) {
            copy_array(offsets_pos);
        }

        ++filt_pos;
        ++offsets_pos;
    }
}
} // namespace

template <typename T, typename OT>
void filter_arrays_impl(const PaddedPODArray<T>& src_elems, const PaddedPODArray<OT>& src_offsets,
                        PaddedPODArray<T>& res_elems, PaddedPODArray<OT>& res_offsets,
                        const IColumn::Filter& filt, ssize_t result_size_hint) {
    return filter_arrays_impl_generic<T, OT, ResultOffsetsBuilder<OT>>(
            src_elems, src_offsets, res_elems, &res_offsets, filt, result_size_hint);
}

template <typename T, typename OT>
void filter_arrays_impl_only_data(const PaddedPODArray<T>& src_elems,
                                  const PaddedPODArray<OT>& src_offsets,
                                  PaddedPODArray<T>& res_elems, const IColumn::Filter& filt,
                                  ssize_t result_size_hint) {
    return filter_arrays_impl_generic<T, OT, NoResultOffsetsBuilder<OT>>(
            src_elems, src_offsets, res_elems, nullptr, filt, result_size_hint);
}

/// Explicit instantiations - not to place the implementation of the function above in the header file.
#define INSTANTIATE(TYPE, OFFTYPE)                                                              \
    template void filter_arrays_impl<TYPE, OFFTYPE>(                                            \
            const PaddedPODArray<TYPE>&, const PaddedPODArray<OFFTYPE>&, PaddedPODArray<TYPE>&, \
            PaddedPODArray<OFFTYPE>&, const IColumn::Filter&, ssize_t);                         \
    template void filter_arrays_impl_only_data<TYPE, OFFTYPE>(                                  \
            const PaddedPODArray<TYPE>&, const PaddedPODArray<OFFTYPE>&, PaddedPODArray<TYPE>&, \
            const IColumn::Filter&, ssize_t);

INSTANTIATE(UInt8, IColumn::Offset)
INSTANTIATE(UInt8, ColumnArray::Offset64)
INSTANTIATE(UInt16, IColumn::Offset)
INSTANTIATE(UInt16, ColumnArray::Offset64)
INSTANTIATE(UInt32, IColumn::Offset)
INSTANTIATE(UInt32, ColumnArray::Offset64)
INSTANTIATE(UInt64, IColumn::Offset)
INSTANTIATE(UInt64, ColumnArray::Offset64)
INSTANTIATE(Int8, IColumn::Offset)
INSTANTIATE(Int8, ColumnArray::Offset64)
INSTANTIATE(Int16, IColumn::Offset)
INSTANTIATE(Int16, ColumnArray::Offset64)
INSTANTIATE(Int32, IColumn::Offset)
INSTANTIATE(Int32, ColumnArray::Offset64)
INSTANTIATE(Int64, IColumn::Offset)
INSTANTIATE(Int64, ColumnArray::Offset64)
INSTANTIATE(Float32, IColumn::Offset)
INSTANTIATE(Float32, ColumnArray::Offset64)
INSTANTIATE(Float64, IColumn::Offset)
INSTANTIATE(Float64, ColumnArray::Offset64)

#undef INSTANTIATE

} // namespace doris::vectorized
