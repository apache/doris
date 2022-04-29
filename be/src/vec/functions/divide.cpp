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

#include "vec/functions/function_binary_arithmetic_to_null_type.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {

static const DecimalV2Value one(1, 0);

template <typename A, typename B>
struct DivideFloatingImpl {
    using ResultType = typename NumberTraits::ResultOfFloatingPointDivision<A, B>::Type;
    static const constexpr bool allow_decimal = true;

    template <typename Result = DecimalV2Value>
    static inline DecimalV2Value apply(DecimalV2Value a, DecimalV2Value b, NullMap& null_map,
                                       size_t index) {
        null_map[index] = b.is_zero();
        return a / (b.is_zero() ? one : b);
    }

    template <typename Result = ResultType>
    static inline Result apply(A a, B b, NullMap& null_map, size_t index) {
        null_map[index] = b == 0;
        return static_cast<Result>(a) / (b + (b == 0));
    }
};

struct NameDivide {
    static constexpr auto name = "divide";
};
using FunctionDivide = FunctionBinaryArithmeticToNullType<DivideFloatingImpl, NameDivide>;

void register_function_divide(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionDivide>();
}

} // namespace doris::vectorized
