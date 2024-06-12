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

#include <bit>
#include <bitset>
#include <cstdint>
#include <exception>
#include <stdexcept>
#include <type_traits>

#include "common/compiler_util.h"
#include "common/exception.h"
#include "common/logging.h"
#include "common/status.h"
#include "gutil/integral_types.h"
#include "util/radix_sort.h"
#include "vec/core/types.h"
#include "vec/functions/function_binary_arithmetic.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {

struct NameBitShiftLeft {
    static constexpr auto name = "bit_shift_left";
};

struct NameBitShiftRight {
    static constexpr auto name = "bit_shift_right";
};

template <typename A, typename B>
struct BitShiftLeftImpl {
    using ResultType = typename NumberTraits::ResultOfBit<A, B>::Type;

    template <typename Result = ResultType>
    static inline Result apply(A a, B b) {
        if constexpr (!std::is_same_v<A, Int64> || !std::is_same_v<B, Int8>) {
            throw Exception(ErrorCode::NOT_FOUND,
                            "bit_shift_left only supports [BIGINT, TINYINT] as operator");
        } else {
            // return zero if b < 0, keep consistent with mysql
            // cast to unsigned so that we can do logical shift by default, keep consistent with mysql
            if (UNLIKELY(b >= 64 || b < 0)) {
                return 0;
            }
            return static_cast<typename std::make_unsigned<A>::type>(a) << static_cast<Result>(b);
        }
    }
};

template <typename A, typename B>
struct BitShiftRightImpl {
    using ResultType = typename NumberTraits::ResultOfBit<A, B>::Type;

    template <typename Result = ResultType>
    static inline Result apply(A a, B b) {
        if constexpr (!std::is_same_v<A, Int64> || !std::is_same_v<B, Int8>) {
            throw Exception(ErrorCode::NOT_FOUND,
                            "bit_shift_right only supports [BIGINT, TINYINT] as operator");
        } else {
            // return zero if b < 0, keep consistent with mysql
            // cast to unsigned so that we can do logical shift by default, keep consistent with mysql
            if (UNLIKELY(b >= 64 || b < 0)) {
                return 0;
            }

            return static_cast<typename std::make_unsigned<A>::type>(a) >> static_cast<Result>(b);
        }
    }
};

using FunctionBitShiftLeft = FunctionBinaryArithmetic<BitShiftLeftImpl, NameBitShiftLeft, false>;
using FunctionBitShiftRight = FunctionBinaryArithmetic<BitShiftRightImpl, NameBitShiftRight, false>;

void register_function_bit_shift(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionBitShiftLeft>();
    factory.register_function<FunctionBitShiftRight>();
}

} // namespace doris::vectorized
