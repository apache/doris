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

#pragma once

#include <cstddef>
#include <cstdint>

namespace doris::simd {

// SIMD implementation for expanding data with null handling.
// This is useful for parquet dict encoding where we need to assign
// non-null values to destination positions while skipping nulls.
//
// The basic operation is:
//   for (size_t i = 0; i < count; ++i) {
//       dst_data[i] = src_data[cnt];
//       cnt += !nulls[i];  // nulls[i] == 1 means null
//   }
//
// SIMD versions are provided for 32-bit and 64-bit data types that
// can significantly accelerate this operation (up to 7-10x faster).

namespace Expand {

// SIMD optimized expand_load for 32-bit types (int32_t, float, etc.)
void expand_load_simd(int32_t* dst_data, const int32_t* src_data, const uint8_t* nulls,
                      size_t count);

// SIMD optimized expand_load for 64-bit types (int64_t, double, etc.)
void expand_load_simd(int64_t* dst_data, const int64_t* src_data, const uint8_t* nulls,
                      size_t count);

// Branch implementation (uses a conditional branch per element)
template <class DataType>
void expand_load_branch(DataType* dst_data, const DataType* src_data, const uint8_t* nulls,
                        size_t count) {
    size_t cnt = 0;
    for (size_t i = 0; i < count; ++i) {
        if (!nulls[i]) {
            dst_data[i] = src_data[cnt];
            ++cnt;
        }
    }
}

// Branchless scalar implementation (fallback for non-SIMD types)
template <class DataType>
void expand_load_branchless(DataType* dst_data, const DataType* src_data, const uint8_t* nulls,
                            size_t count) {
    size_t cnt = 0;
    for (size_t i = 0; i < count; ++i) {
        dst_data[i] = src_data[cnt];
        cnt += !nulls[i];
    }
}

// Generic expand_load that dispatches to SIMD or branchless based on data type size.
// Uses SIMD for 32-bit and 64-bit types, falls back to branchless for others.
template <class DataType>
void expand_load(DataType* dst_data, const DataType* src_data, const uint8_t* nulls, size_t count) {
    if constexpr (sizeof(DataType) == 4) {
        expand_load_simd(reinterpret_cast<int32_t*>(dst_data),
                         reinterpret_cast<const int32_t*>(src_data), nulls, count);
    } else if constexpr (sizeof(DataType) == 8) {
        expand_load_simd(reinterpret_cast<int64_t*>(dst_data),
                         reinterpret_cast<const int64_t*>(src_data), nulls, count);
    } else {
        expand_load_branchless(dst_data, src_data, nulls, count);
    }
}

} // namespace Expand

} // namespace doris::simd
