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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Functions/Modulo.cpp
// and modified by Doris

#include <string.h>

#include <cmath>
#include <memory>
#include <utility>

#include "runtime/decimalv2_value.h"
#include "vec/columns/column_vector.h"
#include "vec/core/types.h"
#include "vec/data_types/number_traits.h"
#include "vec/functions/function_binary_arithmetic.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {

template <typename A, typename B>
inline void throw_if_division_leads_to_FPE(A a, B b) {
    // http://avva.livejournal.com/2548306.html
    // (-9223372036854775808 % -1) will cause coredump directly, so check this case to throw exception, or maybe could return 0 as result
    if constexpr (std::is_signed_v<A> && std::is_signed_v<B>) {
        if (b == -1 && a == std::numeric_limits<A>::min()) {
            throw Exception(ErrorCode::INVALID_ARGUMENT,
                            "Division of minimal signed number by minus one is an undefined "
                            "behavior, {} % {}. ",
                            a, b);
        }
    }
}

template <typename A, typename B>
struct ModuloImpl {
    using ResultType = typename NumberTraits::ResultOfModulo<A, B>::Type;
    using Traits = NumberTraits::BinaryOperatorTraits<A, B>;

    template <typename Result = ResultType>
    static void apply(const typename Traits::ArrayA& a, B b,
                      typename ColumnVector<Result>::Container& c,
                      typename Traits::ArrayNull& null_map) {
        size_t size = c.size();
        UInt8 is_null = b == 0;
        memset(null_map.data(), is_null, sizeof(UInt8) * size);

        if (!is_null) {
            for (size_t i = 0; i < size; i++) {
                if constexpr (std::is_floating_point_v<ResultType>) {
                    c[i] = std::fmod((double)a[i], (double)b);
                } else {
                    throw_if_division_leads_to_FPE(a[i], b);
                    c[i] = a[i] % b;
                }
            }
        }
    }

    template <typename Result = ResultType>
    static inline Result apply(A a, B b, UInt8& is_null) {
        is_null = b == 0;
        b += is_null;

        if constexpr (std::is_floating_point_v<Result>) {
            return std::fmod((double)a, (double)b);
        } else {
            throw_if_division_leads_to_FPE(a, b);
            return a % b;
        }
    }

    template <typename Result = DecimalV2Value>
    static inline DecimalV2Value apply(DecimalV2Value a, DecimalV2Value b, UInt8& is_null) {
        is_null = b == DecimalV2Value(0);
        return a % (b + DecimalV2Value(is_null));
    }
};

template <typename A, typename B>
struct PModuloImpl {
    using ResultType = typename NumberTraits::ResultOfModulo<A, B>::Type;
    using Traits = NumberTraits::BinaryOperatorTraits<A, B>;

    template <typename Result = ResultType>
    static void apply(const typename Traits::ArrayA& a, B b,
                      typename ColumnVector<Result>::Container& c,
                      typename Traits::ArrayNull& null_map) {
        size_t size = c.size();
        UInt8 is_null = b == 0;
        memset(null_map.data(), is_null, size);

        if (!is_null) {
            for (size_t i = 0; i < size; i++) {
                if constexpr (std::is_floating_point_v<ResultType>) {
                    c[i] = std::fmod(std::fmod((double)a[i], (double)b) + (double)b, double(b));
                } else {
                    throw_if_division_leads_to_FPE(a[i], b);
                    c[i] = (a[i] % b + b) % b;
                }
            }
        }
    }

    template <typename Result = ResultType>
    static inline Result apply(A a, B b, UInt8& is_null) {
        is_null = b == 0;
        b += is_null;

        if constexpr (std::is_floating_point_v<Result>) {
            return std::fmod(std::fmod((double)a, (double)b) + (double)b, (double)b);
        } else {
            throw_if_division_leads_to_FPE(a, b);
            return (a % b + b) % b;
        }
    }

    template <typename Result = DecimalV2Value>
    static inline DecimalV2Value apply(DecimalV2Value a, DecimalV2Value b, UInt8& is_null) {
        is_null = b == DecimalV2Value(0);
        b += DecimalV2Value(is_null);
        return (a % b + b) % b;
    }
};

struct NameModulo {
    static constexpr auto name = "mod";
};
struct NamePModulo {
    static constexpr auto name = "pmod";
};

using FunctionModulo = FunctionBinaryArithmetic<ModuloImpl, NameModulo, true>;
using FunctionPModulo = FunctionBinaryArithmetic<PModuloImpl, NamePModulo, true>;

void register_function_modulo(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionModulo>();
    factory.register_function<FunctionPModulo>();
    factory.register_alias("mod", "fmod");
}

} // namespace doris::vectorized
