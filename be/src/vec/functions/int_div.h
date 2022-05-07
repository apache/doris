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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Functions/IntDiv.h
// and modified by Doris

#pragma once

#include <libdivide.h>

#include <type_traits>

#include "vec/columns/column_decimal.h"
#include "vec/columns/column_nullable.h"
#include "vec/core/types.h"
#include "vec/data_types/number_traits.h"
#include "vec/functions/function_binary_arithmetic.h"

namespace doris::vectorized {

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wsign-compare"
#pragma GCC diagnostic pop

template <typename A, typename B>
struct DivideIntegralImpl {
    using ResultType = typename NumberTraits::ResultOfIntegerDivision<A, B>::Type;
    using Traits = NumberTraits::BinaryOperatorTraits<A, B>;

    template <typename Result = ResultType>
    static void apply(const typename Traits::ArrayA& a, B b,
                      typename ColumnVector<Result>::Container& c,
                      typename Traits::ArrayNull& null_map) {
        size_t size = c.size();
        UInt8 is_null = b == 0;
        memset(null_map.data(), is_null, size);

        if (!is_null) {
            if constexpr (!std::is_floating_point_v<A> && !std::is_same_v<A, Int128> &&
                          !std::is_same_v<A, Int8> && !std::is_same_v<A, UInt8>) {
                for (size_t i = 0; i < size; i++) {
                    c[i] = a[i] / libdivide::divider<A>(b);
                }
            } else {
                for (size_t i = 0; i < size; i++) {
                    c[i] = a[i] / b;
                }
            }
        }
    }

    template <typename Result = ResultType>
    static inline Result apply(A a, B b, UInt8& is_null) {
        is_null = b == 0;
        return a / (b + is_null);
    }
};

} // namespace doris::vectorized
