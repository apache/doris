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

#ifdef __SSE2__
#define LIBDIVIDE_SSE2 1
#endif

#include <libdivide.h>

#include "common/status.h"
#include "vec/functions/function_binary_arithmetic.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {

template <typename A, typename B>
struct ModuloImpl {
    using ResultType = typename NumberTraits::ResultOfModulo<A, B>::Type;

    template <typename Result = ResultType>
    static inline Result apply(A a, B b) {
        throw_if_division_leads_to_fpe(typename NumberTraits::ToInteger<A>::Type(a),
                                       typename NumberTraits::ToInteger<B>::Type(b));
        return typename NumberTraits::ToInteger<A>::Type(a) %
               typename NumberTraits::ToInteger<B>::Type(b);
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = false; /// don't know how to throw from LLVM IR
#endif
};

template <typename A, typename B>
struct ModuloByConstantImpl : BinaryOperationImplBase<A, B, ModuloImpl<A, B>> {
    using ResultType = typename ModuloImpl<A, B>::ResultType;

    static void vector_constant(const PaddedPODArray<A>& a, B b, PaddedPODArray<ResultType>& c) {
        if (UNLIKELY(b == 0))
            throw Exception("Division by zero", TStatusCode::VEC_ILLEGAL_DIVISION);

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wsign-compare"

        if (UNLIKELY((std::is_signed_v<B> && b == -1) || b == 1)) {
            size_t size = a.size();
            for (size_t i = 0; i < size; ++i) c[i] = 0;
            return;
        }

#pragma GCC diagnostic pop

        libdivide::divider<A> divider(b);

        /// Here we failed to make the SSE variant from libdivide give an advantage.
        size_t size = a.size();
        for (size_t i = 0; i < size; ++i)
            c[i] = a[i] -
                   (a[i] / divider) *
                           b; /// NOTE: perhaps, the division semantics with the remainder of negative numbers is not preserved.
    }
};

/** Specializations are specified for dividing numbers of the type UInt64 and UInt32 by the numbers of the same sign.
  * Can be expanded to all possible combinations, but more code is needed.
  */

template <>
struct BinaryOperationImpl<UInt64, UInt8, ModuloImpl<UInt64, UInt8>>
        : ModuloByConstantImpl<UInt64, UInt8> {};
template <>
struct BinaryOperationImpl<UInt64, UInt16, ModuloImpl<UInt64, UInt16>>
        : ModuloByConstantImpl<UInt64, UInt16> {};
template <>
struct BinaryOperationImpl<UInt64, UInt32, ModuloImpl<UInt64, UInt32>>
        : ModuloByConstantImpl<UInt64, UInt32> {};
template <>
struct BinaryOperationImpl<UInt64, UInt64, ModuloImpl<UInt64, UInt64>>
        : ModuloByConstantImpl<UInt64, UInt64> {};

template <>
struct BinaryOperationImpl<UInt32, UInt8, ModuloImpl<UInt32, UInt8>>
        : ModuloByConstantImpl<UInt32, UInt8> {};
template <>
struct BinaryOperationImpl<UInt32, UInt16, ModuloImpl<UInt32, UInt16>>
        : ModuloByConstantImpl<UInt32, UInt16> {};
template <>
struct BinaryOperationImpl<UInt32, UInt32, ModuloImpl<UInt32, UInt32>>
        : ModuloByConstantImpl<UInt32, UInt32> {};
template <>
struct BinaryOperationImpl<UInt32, UInt64, ModuloImpl<UInt32, UInt64>>
        : ModuloByConstantImpl<UInt32, UInt64> {};

template <>
struct BinaryOperationImpl<Int64, Int8, ModuloImpl<Int64, Int8>>
        : ModuloByConstantImpl<Int64, Int8> {};
template <>
struct BinaryOperationImpl<Int64, Int16, ModuloImpl<Int64, Int16>>
        : ModuloByConstantImpl<Int64, Int16> {};
template <>
struct BinaryOperationImpl<Int64, Int32, ModuloImpl<Int64, Int32>>
        : ModuloByConstantImpl<Int64, Int32> {};
template <>
struct BinaryOperationImpl<Int64, Int64, ModuloImpl<Int64, Int64>>
        : ModuloByConstantImpl<Int64, Int64> {};

template <>
struct BinaryOperationImpl<Int32, Int8, ModuloImpl<Int32, Int8>>
        : ModuloByConstantImpl<Int32, Int8> {};
template <>
struct BinaryOperationImpl<Int32, Int16, ModuloImpl<Int32, Int16>>
        : ModuloByConstantImpl<Int32, Int16> {};
template <>
struct BinaryOperationImpl<Int32, Int32, ModuloImpl<Int32, Int32>>
        : ModuloByConstantImpl<Int32, Int32> {};
template <>
struct BinaryOperationImpl<Int32, Int64, ModuloImpl<Int32, Int64>>
        : ModuloByConstantImpl<Int32, Int64> {};

struct NameModulo {
    static constexpr auto name = "mod";
};
using FunctionModulo = FunctionBinaryArithmetic<ModuloImpl, NameModulo, false>;

void register_function_modulo(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionModulo>();
}

} // namespace doris::vectorized
