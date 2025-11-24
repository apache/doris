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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Functions/Minus.cpp
// and modified by Doris

#include "vec/functions/binary_arithmetic.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"
template <PrimitiveType TypeA, PrimitiveType TypeB>
struct MinusDecimalImpl {
    static_assert(is_decimal(TypeA) && is_decimal(TypeB));
    static_assert((TypeA == TYPE_DECIMALV2 && TypeB == TYPE_DECIMALV2) ||
                  (TypeA != TYPE_DECIMALV2 && TypeB != TYPE_DECIMALV2));

    static constexpr auto name = "subtract";
    static constexpr PrimitiveType PTypeA = TypeA;
    static constexpr PrimitiveType PTypeB = TypeA;
    using ArgNativeTypeA = typename PrimitiveTypeTraits<TypeA>::CppNativeType;
    using ArgNativeTypeB = typename PrimitiveTypeTraits<TypeB>::CppNativeType;

    template <PrimitiveType Result>
        requires(is_decimal(Result) && Result != TYPE_DECIMALV2)
    static inline typename PrimitiveTypeTraits<Result>::CppNativeType apply(ArgNativeTypeA a,
                                                                            ArgNativeTypeB b) {
        return static_cast<typename PrimitiveTypeTraits<Result>::CppNativeType>(
                static_cast<typename PrimitiveTypeTraits<Result>::CppNativeType>(a) - b);
    }

    static inline DecimalV2Value apply(const DecimalV2Value& a, const DecimalV2Value& b) {
        return DecimalV2Value(a.value() - b.value());
    }

    /// Apply operation and check overflow. It's used for Decimal operations. @returns true if overflowed, false otherwise.
    template <PrimitiveType Result>
        requires(is_decimal(Result) && Result != TYPE_DECIMALV2)
    static inline bool apply(ArgNativeTypeA a, ArgNativeTypeB b,
                             typename PrimitiveTypeTraits<Result>::CppNativeType& c) {
        return common::sub_overflow(
                static_cast<typename PrimitiveTypeTraits<Result>::CppNativeType>(a),
                static_cast<typename PrimitiveTypeTraits<Result>::CppNativeType>(b), c);
    }
};

template <PrimitiveType Type>
struct MinusImpl {
    static constexpr auto name = "subtract";
    static constexpr PrimitiveType PType = Type;
    using Arg = typename PrimitiveTypeTraits<Type>::ColumnItemType;
    NO_SANITIZE_UNDEFINED static inline Arg apply(Arg a, Arg b) { return a - b; }
};

void register_function_minus(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionPlusMinus<
            PlusMinusDecimalImpl<MinusDecimalImpl<TYPE_DECIMALV2, TYPE_DECIMALV2>>>>();

    factory.register_function<FunctionPlusMinus<
            PlusMinusDecimalImpl<MinusDecimalImpl<TYPE_DECIMAL32, TYPE_DECIMAL32>>>>();
    factory.register_function<FunctionPlusMinus<
            PlusMinusDecimalImpl<MinusDecimalImpl<TYPE_DECIMAL32, TYPE_DECIMAL64>>>>();
    factory.register_function<FunctionPlusMinus<
            PlusMinusDecimalImpl<MinusDecimalImpl<TYPE_DECIMAL32, TYPE_DECIMAL128I>>>>();
    factory.register_function<FunctionPlusMinus<
            PlusMinusDecimalImpl<MinusDecimalImpl<TYPE_DECIMAL32, TYPE_DECIMAL256>>>>();

    factory.register_function<FunctionPlusMinus<
            PlusMinusDecimalImpl<MinusDecimalImpl<TYPE_DECIMAL64, TYPE_DECIMAL32>>>>();
    factory.register_function<FunctionPlusMinus<
            PlusMinusDecimalImpl<MinusDecimalImpl<TYPE_DECIMAL64, TYPE_DECIMAL64>>>>();
    factory.register_function<FunctionPlusMinus<
            PlusMinusDecimalImpl<MinusDecimalImpl<TYPE_DECIMAL64, TYPE_DECIMAL128I>>>>();
    factory.register_function<FunctionPlusMinus<
            PlusMinusDecimalImpl<MinusDecimalImpl<TYPE_DECIMAL64, TYPE_DECIMAL256>>>>();

    factory.register_function<FunctionPlusMinus<
            PlusMinusDecimalImpl<MinusDecimalImpl<TYPE_DECIMAL128I, TYPE_DECIMAL32>>>>();
    factory.register_function<FunctionPlusMinus<
            PlusMinusDecimalImpl<MinusDecimalImpl<TYPE_DECIMAL128I, TYPE_DECIMAL64>>>>();
    factory.register_function<FunctionPlusMinus<
            PlusMinusDecimalImpl<MinusDecimalImpl<TYPE_DECIMAL128I, TYPE_DECIMAL128I>>>>();
    factory.register_function<FunctionPlusMinus<
            PlusMinusDecimalImpl<MinusDecimalImpl<TYPE_DECIMAL128I, TYPE_DECIMAL256>>>>();

    factory.register_function<FunctionPlusMinus<
            PlusMinusDecimalImpl<MinusDecimalImpl<TYPE_DECIMAL256, TYPE_DECIMAL32>>>>();
    factory.register_function<FunctionPlusMinus<
            PlusMinusDecimalImpl<MinusDecimalImpl<TYPE_DECIMAL256, TYPE_DECIMAL64>>>>();
    factory.register_function<FunctionPlusMinus<
            PlusMinusDecimalImpl<MinusDecimalImpl<TYPE_DECIMAL256, TYPE_DECIMAL128I>>>>();
    factory.register_function<FunctionPlusMinus<
            PlusMinusDecimalImpl<MinusDecimalImpl<TYPE_DECIMAL256, TYPE_DECIMAL256>>>>();

    factory.register_function<FunctionPlusMinus<PlusMinusIntegralImpl<MinusImpl<TYPE_TINYINT>>>>();
    factory.register_function<FunctionPlusMinus<PlusMinusIntegralImpl<MinusImpl<TYPE_SMALLINT>>>>();
    factory.register_function<FunctionPlusMinus<PlusMinusIntegralImpl<MinusImpl<TYPE_INT>>>>();
    factory.register_function<FunctionPlusMinus<PlusMinusIntegralImpl<MinusImpl<TYPE_BIGINT>>>>();
    factory.register_function<FunctionPlusMinus<PlusMinusIntegralImpl<MinusImpl<TYPE_LARGEINT>>>>();
    factory.register_function<FunctionPlusMinus<PlusMinusIntegralImpl<MinusImpl<TYPE_DOUBLE>>>>();
    factory.register_function<FunctionPlusMinus<PlusMinusIntegralImpl<MinusImpl<TYPE_FLOAT>>>>();
}
#include "common/compile_check_end.h"
} // namespace doris::vectorized
