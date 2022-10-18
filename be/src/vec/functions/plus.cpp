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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Functions/Plus.cpp
// and modified by Doris

#include "vec/common/arithmetic_overflow.h"
#include "vec/functions/function_binary_arithmetic.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {

template <typename A, typename B>
struct PlusImpl {
    using ResultType = typename NumberTraits::ResultOfAdditionMultiplication<A, B>::Type;
    static const constexpr bool allow_decimal = true;

    template <typename Result = ResultType>
    static inline Result apply(A a, B b) {
        /// Next everywhere, static_cast - so that there is no wrong result in expressions of the form Int64 c = UInt32(a) * Int32(-1).
        return static_cast<Result>(a) + b;
    }

    template <typename Result = DecimalV2Value>
    static inline DecimalV2Value apply(DecimalV2Value a, DecimalV2Value b) {
        return DecimalV2Value(a.value() + b.value());
    }

    /// Apply operation and check overflow. It's used for Deciamal operations. @returns true if overflowed, false otherwise.
    template <typename Result = ResultType>
    static inline bool apply(A a, B b, Result& c) {
        return common::add_overflow(static_cast<Result>(a), b, c);
    }
};

struct NamePlus {
    static constexpr auto name = "add";
};
using FunctionPlus = FunctionBinaryArithmetic<PlusImpl, NamePlus, false>;

void register_function_plus(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionPlus>();
}
} // namespace doris::vectorized
