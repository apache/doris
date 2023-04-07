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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Functions/Multiply.cpp
// and modified by Doris

#include "runtime/decimalv2_value.h"
#include "vec/columns/column_decimal.h"
#include "vec/common/arithmetic_overflow.h"
#include "vec/functions/function_binary_arithmetic.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {

template <typename A, typename B>
struct MultiplyImpl {
    using ResultType = typename NumberTraits::ResultOfAdditionMultiplication<A, B>::Type;
    static const constexpr bool allow_decimal = true;

    template <typename Result = ResultType>
    static inline Result apply(A a, B b) {
        return static_cast<Result>(a) * b;
    }

    template <typename Result = DecimalV2Value>
    static inline DecimalV2Value apply(const DecimalV2Value& a, const DecimalV2Value& b) {
        return a * b;
    }

    static void vector_vector(const ColumnDecimal128::Container::value_type* __restrict a,
                              const ColumnDecimal128::Container::value_type* __restrict b,
                              ColumnDecimal128::Container::value_type* c, size_t size) {
        int8 sgn[size];

        for (int i = 0; i < size; i++) {
            sgn[i] = ((DecimalV2Value(a[i]).value() > 0) && (DecimalV2Value(b[i]).value() > 0)) ||
                                     ((DecimalV2Value(a[i]).value() < 0) &&
                                      (DecimalV2Value(b[i]).value() < 0))
                             ? 1
                     : ((DecimalV2Value(a[i]).value() == 0) || (DecimalV2Value(b[i]).value() == 0))
                             ? 0
                             : -1;
        }

        for (int i = 0; i < size; i++) {
            c[i] = (DecimalV2Value(a[i]).value() * DecimalV2Value(b[i]).value() - sgn[i]) /
                           DecimalV2Value::ONE_BILLION +
                   sgn[i];
        }
    }

    /// Apply operation and check overflow. It's used for Decimal operations. @returns true if overflowed, false otherwise.
    template <typename Result = ResultType>
    static inline bool apply(A a, B b, Result& c) {
        return common::mul_overflow(static_cast<Result>(a), b, c);
    }
};

struct NameMultiply {
    static constexpr auto name = "multiply";
};
using FunctionMultiply = FunctionBinaryArithmetic<MultiplyImpl, NameMultiply, false>;

void register_function_multiply(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionMultiply>();
}

} // namespace doris::vectorized
