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

#include <stddef.h>

#include <utility>

#include "gutil/integral_types.h"
#include "runtime/decimalv2_value.h"
#include "vec/columns/columns_number.h"
#include "vec/common/arithmetic_overflow.h"
#include "vec/core/types.h"
#include "vec/data_types/number_traits.h"
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

    /*
    select 999999999999999999999999999 * 999999999999999999999999999;
    999999999999999999999999998000000000.000000000000000001 54 digits
    */
    template <bool check_overflow>
    static void vector_vector(const ColumnDecimal128V2::Container::value_type* __restrict a,
                              const ColumnDecimal128V2::Container::value_type* __restrict b,
                              ColumnDecimal128V2::Container::value_type* c, size_t size) {
        auto sng_uptr = std::unique_ptr<int8[]>(new int8[size]);
        int8* sgn = sng_uptr.get();
        auto max = DecimalV2Value::get_max_decimal();
        auto min = DecimalV2Value::get_min_decimal();

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
            if constexpr (check_overflow) {
                int128_t i128_mul_result;
                if (common::mul_overflow(DecimalV2Value(a[i]).value(), DecimalV2Value(b[i]).value(),
                                         i128_mul_result)) {
                    THROW_DECIMAL_BINARY_OP_OVERFLOW_EXCEPTION(
                            DecimalV2Value(a[i]).to_string(), "multiply",
                            DecimalV2Value(b[i]).to_string(),
                            DecimalV2Value(i128_mul_result).to_string(), "decimalv2");
                }
                c[i] = (i128_mul_result - sgn[i]) / DecimalV2Value::ONE_BILLION + sgn[i];
                if (c[i].value > max.value() || c[i].value < min.value()) {
                    THROW_DECIMAL_BINARY_OP_OVERFLOW_EXCEPTION(
                            DecimalV2Value(a[i]).to_string(), "multiply",
                            DecimalV2Value(b[i]).to_string(),
                            DecimalV2Value(i128_mul_result).to_string(), "decimalv2");
                }
            } else {
                c[i] = (DecimalV2Value(a[i]).value() * DecimalV2Value(b[i]).value() - sgn[i]) /
                               DecimalV2Value::ONE_BILLION +
                       sgn[i];
            }
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
