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

#include "core/column/column_vector.h"
#include "util/simd/parquet_kernels.h"

namespace doris {
namespace {

template <PrimitiveType TYPE>
bool try_gather_vector(IColumn& destination, const IColumn& dictionary, const uint32_t* indices,
                       size_t num_values) {
    using ColumnType = ColumnVector<TYPE>;
    using ValueType = typename ColumnType::value_type;
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
    TRY_PARQUET_GATHER(TYPE_IPV4);
    TRY_PARQUET_GATHER(TYPE_TIMEV2);
    TRY_PARQUET_GATHER(TYPE_UINT32);
    TRY_PARQUET_GATHER(TYPE_UINT64);
#undef TRY_PARQUET_GATHER
    return false;
}

} // namespace doris
