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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Functions/divide.cpp
// and modified by Doris

#include "vec/functions/function_binary_arithmetic.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {

static const DecimalV2Value one(1, 0);

template <typename A, typename B>
struct DivideFloatingImpl {
    using ResultType = typename NumberTraits::ResultOfFloatingPointDivision<A, B>::Type;
    using Traits = NumberTraits::BinaryOperatorTraits<A, B>;

    static const constexpr bool allow_decimal = true;

    template <typename Result = ResultType>
    static void apply(const typename Traits::ArrayA& a, B b,
                      typename ColumnVector<Result>::Container& c,
                      typename Traits::ArrayNull& null_map) {
        size_t size = c.size();
        UInt8 is_null = b == 0;
        memset(null_map.data(), is_null, size);

        if (!is_null) {
            for (size_t i = 0; i < size; i++) {
                c[i] = (double)a[i] / (double)b;
            }
        }
    }

    template <typename Result = DecimalV2Value>
    static inline DecimalV2Value apply(DecimalV2Value a, DecimalV2Value b, UInt8& is_null) {
        is_null = b.is_zero();
        return a / (is_null ? one : b);
    }

    template <typename Result = ResultType>
    static inline Result apply(A a, B b, UInt8& is_null) {
        is_null = b == 0;
        return static_cast<Result>(a) / (b + is_null);
    }
};

struct NameDivide {
    static constexpr auto name = "divide";
};
using FunctionDivide = FunctionBinaryArithmetic<DivideFloatingImpl, NameDivide, true>;

void register_function_divide(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionDivide>();
}

} // namespace doris::vectorized
