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

#include "core/data_type_serde/parquet_decode_source.h"

#include <cstring>
#include <limits>
#include <type_traits>

#include "core/column/column_decimal.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "util/simd/parquet_kernels.h"

namespace doris {
namespace {

template <typename ColumnType>
bool try_gather_fixed_width(IColumn& destination, const IColumn& dictionary,
                            const uint32_t* indices, size_t num_values) {
    using ValueType = typename ColumnType::value_type;
    static_assert(std::is_trivially_copyable_v<ValueType>);
    if constexpr (sizeof(ValueType) != 4 && sizeof(ValueType) != 8) {
        return false;
    } else {
        auto* destination_vector = dynamic_cast<ColumnType*>(&destination);
        const auto* dictionary_vector = dynamic_cast<const ColumnType*>(&dictionary);
        if (destination_vector == nullptr || dictionary_vector == nullptr) {
            return false;
        }
        // The direct strategy is chosen only while the typed dictionary is cache-resident. Keep
        // the existing generic insertion path for tiny batches where gather setup cannot amortize.
        constexpr size_t SIMD_LANES = sizeof(ValueType) == 4 ? 8 : 4;
        if (num_values < SIMD_LANES) {
            return false;
        }
        auto& destination_data = destination_vector->get_data();
        const auto& dictionary_data = dictionary_vector->get_data();
        const size_t old_size = destination_data.size();
        destination_data.resize(old_size + num_values);
        simd::dictionary_gather(reinterpret_cast<const uint8_t*>(dictionary_data.data()), indices,
                                num_values, sizeof(ValueType),
                                reinterpret_cast<uint8_t*>(destination_data.data() + old_size));
        return true;
    }
}

template <PrimitiveType TYPE>
bool try_gather_vector(IColumn& destination, const IColumn& dictionary, const uint32_t* indices,
                       size_t num_values) {
    return try_gather_fixed_width<ColumnVector<TYPE>>(destination, dictionary, indices, num_values);
}

template <typename Offset>
bool try_gather_strings(IColumn& destination, const IColumn& dictionary, const uint32_t* indices,
                        size_t num_values) {
    auto* destination_string = dynamic_cast<ColumnStr<Offset>*>(&destination);
    const auto* dictionary_string = dynamic_cast<const ColumnStr<Offset>*>(&dictionary);
    if (destination_string == nullptr || dictionary_string == nullptr) {
        return false;
    }

    size_t bytes = 0;
    for (size_t row = 0; row < num_values; ++row) {
        const size_t value_size = dictionary_string->get_data_at(indices[row]).size;
        if (value_size > std::numeric_limits<size_t>::max() - bytes) {
            return false;
        }
        bytes += value_size;
    }
    auto& chars = destination_string->get_chars();
    auto& offsets = destination_string->get_offsets();
    if (bytes > std::numeric_limits<Offset>::max() - chars.size()) {
        return false;
    }
    const size_t old_chars_size = chars.size();
    chars.resize(old_chars_size + bytes);
    offsets.reserve(offsets.size() + num_values);
    size_t output_offset = old_chars_size;
    for (size_t row = 0; row < num_values; ++row) {
        const StringRef value = dictionary_string->get_data_at(indices[row]);
        if (value.size != 0) {
            memcpy(chars.data() + output_offset, value.data, value.size);
        }
        output_offset += value.size;
        offsets.push_back(static_cast<Offset>(output_offset));
    }
    return true;
}

} // namespace

bool try_simd_insert_parquet_dictionary_indices(IColumn& destination, const IColumn& dictionary,
                                                const uint32_t* indices, size_t num_values) {
#define TRY_PARQUET_GATHER(TYPE) \
    if (try_gather_vector<TYPE>(destination, dictionary, indices, num_values)) return true
    TRY_PARQUET_GATHER(TYPE_INT);
    TRY_PARQUET_GATHER(TYPE_BIGINT);
    TRY_PARQUET_GATHER(TYPE_FLOAT);
    TRY_PARQUET_GATHER(TYPE_DOUBLE);
    TRY_PARQUET_GATHER(TYPE_DATE);
    TRY_PARQUET_GATHER(TYPE_DATETIME);
    TRY_PARQUET_GATHER(TYPE_DATEV2);
    TRY_PARQUET_GATHER(TYPE_DATETIMEV2);
    TRY_PARQUET_GATHER(TYPE_TIMESTAMPTZ);
    TRY_PARQUET_GATHER(TYPE_IPV4);
    TRY_PARQUET_GATHER(TYPE_TIMEV2);
    TRY_PARQUET_GATHER(TYPE_UINT32);
    TRY_PARQUET_GATHER(TYPE_UINT64);
#undef TRY_PARQUET_GATHER
    // Keep every 4/8-byte POD column on this path: aliases such as TIMESTAMPTZ and decimals do not
    // participate in the ordinary numeric dispatch and otherwise silently fall back to Field insertion.
    if (try_gather_fixed_width<ColumnDecimal32>(destination, dictionary, indices, num_values) ||
        try_gather_fixed_width<ColumnDecimal64>(destination, dictionary, indices, num_values)) {
        return true;
    }
    // String survivors have variable widths, so pre-size both buffers and copy each selected
    // dictionary slice exactly once instead of routing every id through generic Field insertion.
    if (try_gather_strings<UInt32>(destination, dictionary, indices, num_values) ||
        try_gather_strings<UInt64>(destination, dictionary, indices, num_values)) {
        return true;
    }
    return false;
}

} // namespace doris
