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

#include "http/http_status.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column_complex.h"
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

#define DECIMAL_TYPE_TO_COLUMN_TYPE(M)     \
    M(Decimal32, ColumnDecimal<Decimal32>) \
    M(Decimal64, ColumnDecimal<Decimal64>) \
    M(Decimal128, ColumnDecimal<Decimal128>)

#define STRING_TYPE_TO_COLUMN_TYPE(M) M(String, ColumnString)

#define TIME_TYPE_TO_COLUMN_TYPE(M) \
    M(Date, ColumnInt64)            \
    M(DateTime, ColumnInt64)

#define COMPLEX_TYPE_TO_COLUMN_TYPE(M) \
    M(BitMap, ColumnBitmap)            \
    M(HLL, ColumnHLL)

#define TYPE_TO_COLUMN_TYPE(M)     \
    NUMERIC_TYPE_TO_COLUMN_TYPE(M) \
    DECIMAL_TYPE_TO_COLUMN_TYPE(M) \
    STRING_TYPE_TO_COLUMN_TYPE(M)  \
    TIME_TYPE_TO_COLUMN_TYPE(M)    \
    COMPLEX_TYPE_TO_COLUMN_TYPE(M)

namespace doris::vectorized {

template <template <typename> typename ClassTemplate, typename... TArgs>
IAggregateFunction* create_class_with_type(const IDataType& argument_type, TArgs&&... args) {
    WhichDataType which(argument_type);
#define DISPATCH(TYPE, COLUMN_TYPE)   \
    if (which.idx == TypeIndex::TYPE) \
        return new ClassTemplate<COLUMN_TYPE>(std::forward<TArgs>(args)...);
    TYPE_TO_COLUMN_TYPE(DISPATCH)
#undef DISPATCH
    return nullptr;
}

// We can use template lambda function in C++20, but now just use static function
#define CONSTEXPR_LOOP_MATCH_DECLARE(EXECUTE)                                               \
    template <int start, int end, template <int> typename Object, typename... TArgs>        \
    static void constexpr_loop_match(int target, TArgs&&... args) {                         \
        if constexpr (start < end) {                                                        \
            if (start == target) {                                                          \
                EXECUTE<Object<start>>(std::forward<TArgs>(args)...);                       \
            } else {                                                                        \
                constexpr_loop_match<start + 1, end, Object>(target,                        \
                                                             std::forward<TArgs>(args)...); \
            }                                                                               \
        }                                                                                   \
    }

} // namespace  doris::vectorized
