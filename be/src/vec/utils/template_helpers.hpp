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

#include <limits>
#include <variant>

#include "http/http_status.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_complex.h"
#include "vec/columns/column_map.h"
#include "vec/columns/column_object.h"
#include "vec/columns/column_struct.h"
#include "vec/columns/columns_number.h"
#include "vec/data_types/data_type.h"
#include "vec/functions/function.h"

#define NUMERIC_TYPE_TO_COLUMN_TYPE(M) \
    M(UInt8, ColumnUInt8)              \
    M(Int8, ColumnInt8)                \
    M(Int16, ColumnInt16)              \
    M(Int32, ColumnInt32)              \
    M(Int64, ColumnInt64)              \
    M(Int128, ColumnInt128)            \
    M(Float32, ColumnFloat32)          \
    M(Float64, ColumnFloat64)

#define DECIMAL_TYPE_TO_COLUMN_TYPE(M)       \
    M(Decimal32, ColumnDecimal<Decimal32>)   \
    M(Decimal64, ColumnDecimal<Decimal64>)   \
    M(Decimal128, ColumnDecimal<Decimal128>) \
    M(Decimal128I, ColumnDecimal<Decimal128I>)

#define STRING_TYPE_TO_COLUMN_TYPE(M) \
    M(String, ColumnString)           \
    M(JSONB, ColumnString)

#define TIME_TYPE_TO_COLUMN_TYPE(M) \
    M(Date, ColumnInt64)            \
    M(DateTime, ColumnInt64)        \
    M(DateV2, ColumnUInt32)         \
    M(DateTimeV2, ColumnUInt64)

#define COMPLEX_TYPE_TO_COLUMN_TYPE(M) \
    M(Array, ColumnArray)              \
    M(Map, ColumnMap)                  \
    M(Struct, ColumnStruct)            \
    M(VARIANT, ColumnObject)           \
    M(BitMap, ColumnBitmap)            \
    M(HLL, ColumnHLL)

#define TYPE_TO_BASIC_COLUMN_TYPE(M) \
    NUMERIC_TYPE_TO_COLUMN_TYPE(M)   \
    DECIMAL_TYPE_TO_COLUMN_TYPE(M)   \
    STRING_TYPE_TO_COLUMN_TYPE(M)    \
    TIME_TYPE_TO_COLUMN_TYPE(M)

#define TYPE_TO_COLUMN_TYPE(M)   \
    TYPE_TO_BASIC_COLUMN_TYPE(M) \
    COMPLEX_TYPE_TO_COLUMN_TYPE(M)

namespace doris::vectorized {

template <typename LoopType, LoopType start, LoopType end, template <LoopType> typename Reducer>
struct constexpr_loop_match {
    template <typename... TArgs>
    static void run(LoopType target, TArgs&&... args) {
        if constexpr (start <= end) {
            if (start == target) {
                Reducer<start>::run(std::forward<TArgs>(args)...);
            } else {
                if constexpr (start < std::numeric_limits<LoopType>::max()) {
                    constexpr_loop_match<LoopType, start + 1, end, Reducer>::run(
                            target, std::forward<TArgs>(args)...);
                }
            }
        }
    }
};

template <int start, int end, template <int> typename Reducer>
using constexpr_int_match = constexpr_loop_match<int, start, end, Reducer>;

template <template <bool> typename Reducer>
using constexpr_bool_match = constexpr_loop_match<bool, false, true, Reducer>;

// we can't use variadic-parameters, because it will reject alias-templates.
// https://stackoverflow.com/questions/30707011/pack-expansion-for-alias-template
template <typename LoopType, LoopType start, LoopType end,
          template <LoopType, LoopType> typename Reducer,
          template <template <LoopType> typename> typename InnerMatch>
struct constexpr_2_loop_match {
    template <LoopType matched>
    using InnerReducer = Reducer<start, matched>;

    template <typename... TArgs>
    static void run(LoopType target, TArgs&&... args) {
        if constexpr (start <= end) {
            if (start == target) {
                InnerMatch<InnerReducer>::run(std::forward<TArgs>(args)...);
            } else {
                if constexpr (start < std::numeric_limits<LoopType>::max()) {
                    constexpr_2_loop_match<LoopType, start + 1, end, Reducer, InnerMatch>::run(
                            target, std::forward<TArgs>(args)...);
                }
            }
        }
    }
};

template <typename LoopType, LoopType start, LoopType end,
          template <LoopType, LoopType, LoopType> typename Reducer,
          template <template <LoopType, LoopType> typename> typename InnerMatch>
struct constexpr_3_loop_match {
    template <LoopType matched, LoopType matched_next>
    using InnerReducer = Reducer<start, matched, matched_next>;

    template <typename... TArgs>
    static void run(LoopType target, TArgs&&... args) {
        if constexpr (start <= end) {
            if (start == target) {
                InnerMatch<InnerReducer>::run(std::forward<TArgs>(args)...);
            } else {
                if constexpr (start < std::numeric_limits<LoopType>::max()) {
                    constexpr_3_loop_match<LoopType, start + 1, end, Reducer, InnerMatch>::run(
                            target, std::forward<TArgs>(args)...);
                }
            }
        }
    }
};

std::variant<std::false_type, std::true_type> inline make_bool_variant(bool condition) {
    if (condition) {
        return std::true_type {};
    } else {
        return std::false_type {};
    }
}

} // namespace  doris::vectorized
