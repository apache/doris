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

#include "vec/functions/int_div.h"

#include <libdivide.h>

#include "vec/functions/function_binary_arithmetic.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {

/// Optimizations for integer division by a constant.

template <typename A, typename B>
struct DivideIntegralByConstantImpl : BinaryOperationImplBase<A, B, DivideIntegralImpl<A, B>> {
    using ResultType = typename DivideIntegralImpl<A, B>::ResultType;

    static void vector_constant(const PaddedPODArray<A>& a, B b, PaddedPODArray<ResultType>& c) {
        if (UNLIKELY(b == 0)) {
            throw Exception("Division by zero", TStatusCode::VEC_ILLEGAL_DIVISION);
        }

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wsign-compare"

        if (UNLIKELY(std::is_signed_v<B> && b == -1)) {
            size_t size = a.size();
            for (size_t i = 0; i < size; ++i) c[i] = -c[i];
            return;
        }

#pragma GCC diagnostic pop

        libdivide::divider<A> divider(b);

        size_t size = a.size();
        const A* a_pos = a.data();
        const A* a_end = a_pos + size;
        ResultType* c_pos = c.data();

#ifdef __SSE2__
        static constexpr size_t values_per_sse_register = 16 / sizeof(A);
        const A* a_end_sse = a_pos + size / values_per_sse_register * values_per_sse_register;

        while (a_pos < a_end_sse) {
            _mm_storeu_si128(reinterpret_cast<__m128i*>(c_pos),
                             _mm_loadu_si128(reinterpret_cast<const __m128i*>(a_pos)) / divider);

            a_pos += values_per_sse_register;
            c_pos += values_per_sse_register;
        }
#endif

        while (a_pos < a_end) {
            *c_pos = *a_pos / divider;
            ++a_pos;
            ++c_pos;
        }
    }
};

/** Specializations are specified for dividing numbers of the type UInt64 and UInt32 by the numbers of the same sign.
  * Can be expanded to all possible combinations, but more code is needed.
  */

template <>
struct BinaryOperationImpl<UInt64, UInt8, DivideIntegralImpl<UInt64, UInt8>>
        : DivideIntegralByConstantImpl<UInt64, UInt8> {};
template <>
struct BinaryOperationImpl<UInt64, UInt16, DivideIntegralImpl<UInt64, UInt16>>
        : DivideIntegralByConstantImpl<UInt64, UInt16> {};
template <>
struct BinaryOperationImpl<UInt64, UInt32, DivideIntegralImpl<UInt64, UInt32>>
        : DivideIntegralByConstantImpl<UInt64, UInt32> {};
template <>
struct BinaryOperationImpl<UInt64, UInt64, DivideIntegralImpl<UInt64, UInt64>>
        : DivideIntegralByConstantImpl<UInt64, UInt64> {};

template <>
struct BinaryOperationImpl<UInt32, UInt8, DivideIntegralImpl<UInt32, UInt8>>
        : DivideIntegralByConstantImpl<UInt32, UInt8> {};
template <>
struct BinaryOperationImpl<UInt32, UInt16, DivideIntegralImpl<UInt32, UInt16>>
        : DivideIntegralByConstantImpl<UInt32, UInt16> {};
template <>
struct BinaryOperationImpl<UInt32, UInt32, DivideIntegralImpl<UInt32, UInt32>>
        : DivideIntegralByConstantImpl<UInt32, UInt32> {};
template <>
struct BinaryOperationImpl<UInt32, UInt64, DivideIntegralImpl<UInt32, UInt64>>
        : DivideIntegralByConstantImpl<UInt32, UInt64> {};

template <>
struct BinaryOperationImpl<Int64, Int8, DivideIntegralImpl<Int64, Int8>>
        : DivideIntegralByConstantImpl<Int64, Int8> {};
template <>
struct BinaryOperationImpl<Int64, Int16, DivideIntegralImpl<Int64, Int16>>
        : DivideIntegralByConstantImpl<Int64, Int16> {};
template <>
struct BinaryOperationImpl<Int64, Int32, DivideIntegralImpl<Int64, Int32>>
        : DivideIntegralByConstantImpl<Int64, Int32> {};
template <>
struct BinaryOperationImpl<Int64, Int64, DivideIntegralImpl<Int64, Int64>>
        : DivideIntegralByConstantImpl<Int64, Int64> {};

template <>
struct BinaryOperationImpl<Int32, Int8, DivideIntegralImpl<Int32, Int8>>
        : DivideIntegralByConstantImpl<Int32, Int8> {};
template <>
struct BinaryOperationImpl<Int32, Int16, DivideIntegralImpl<Int32, Int16>>
        : DivideIntegralByConstantImpl<Int32, Int16> {};
template <>
struct BinaryOperationImpl<Int32, Int32, DivideIntegralImpl<Int32, Int32>>
        : DivideIntegralByConstantImpl<Int32, Int32> {};
template <>
struct BinaryOperationImpl<Int32, Int64, DivideIntegralImpl<Int32, Int64>>
        : DivideIntegralByConstantImpl<Int32, Int64> {};

struct NameIntDiv {
    static constexpr auto name = "int_divide";
};
using FunctionIntDiv = FunctionBinaryArithmetic<DivideIntegralImpl, NameIntDiv, false>;

void register_function_int_div(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionIntDiv>();
}

} // namespace doris::vectorized
