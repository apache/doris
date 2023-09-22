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

#include <stdint.h>
#include <string.h>

#include <boost/iterator/iterator_facade.hpp>
// IWYU pragma: no_include <bits/std_abs.h>
#include <algorithm>
#include <cmath>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>

#include "common/status.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
#include "vec/columns/columns_number.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"
#include "vec/data_types/number_traits.h"
#include "vec/functions/function_binary_arithmetic.h"
#include "vec/functions/function_const.h"
#include "vec/functions/function_math_unary.h"
#include "vec/functions/function_math_unary_to_null_type.h"
#include "vec/functions/function_string.h"
#include "vec/functions/function_totype.h"
#include "vec/functions/function_unary_arithmetic.h"
#include "vec/functions/round.h"
#include "vec/functions/simple_function_factory.h"

namespace doris {
namespace vectorized {
struct LnImpl;
struct Log10Impl;
struct Log2Impl;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {
struct AcosName {
    static constexpr auto name = "acos";
};
using FunctionAcos = FunctionMathUnary<UnaryFunctionVectorized<AcosName, std::acos>>;

struct AsinName {
    static constexpr auto name = "asin";
};
using FunctionAsin = FunctionMathUnary<UnaryFunctionVectorized<AsinName, std::asin>>;

struct AtanName {
    static constexpr auto name = "atan";
};
using FunctionAtan = FunctionMathUnary<UnaryFunctionVectorized<AtanName, std::atan>>;

struct CosName {
    static constexpr auto name = "cos";
};
using FunctionCos = FunctionMathUnary<UnaryFunctionVectorized<CosName, std::cos>>;

struct EImpl {
    static constexpr auto name = "e";
    static constexpr double value = 2.7182818284590452353602874713526624977572470;
};
using FunctionE = FunctionMathConstFloat64<EImpl>;

struct PiImpl {
    static constexpr auto name = "pi";
    static constexpr double value = 3.1415926535897932384626433832795028841971693;
};
using FunctionPi = FunctionMathConstFloat64<PiImpl>;

struct ExpName {
    static constexpr auto name = "exp";
};
using FunctionExp = FunctionMathUnary<UnaryFunctionVectorized<ExpName, std::exp>>;

#define LOG_FUNCTION_IMPL(CLASS, NAME, FUNC)                          \
    struct CLASS##Impl {                                              \
        using Type = DataTypeFloat64;                                 \
        using RetType = Float64;                                      \
        static constexpr auto name = #NAME;                           \
        template <typename T, typename U>                             \
        static void execute(const T* src, U* dst, UInt8& null_flag) { \
            null_flag = src[0] <= 0;                                  \
            dst[0] = static_cast<U>(FUNC((double)src[0]));            \
        }                                                             \
    };                                                                \
    using Function##CLASS = FunctionMathUnaryToNullType<CLASS##Impl>;

LOG_FUNCTION_IMPL(Log10, log10, std::log10);
LOG_FUNCTION_IMPL(Log2, log2, std::log2);
LOG_FUNCTION_IMPL(Ln, ln, std::log);

struct LogName {
    static constexpr auto name = "log";
};

template <typename A, typename B>
struct LogImpl {
    using ResultType = Float64;
    using Traits = NumberTraits::BinaryOperatorTraits<A, B>;

    static const constexpr bool allow_decimal = false;
    static constexpr double EPSILON = 1e-9;

    template <typename Result = ResultType>
    static void apply(const typename Traits::ArrayA& a, B b,
                      typename ColumnVector<Result>::Container& c,
                      typename Traits::ArrayNull& null_map) {
        size_t size = c.size();
        UInt8 is_null = b <= 0;
        memset(null_map.data(), is_null, size);

        if (!is_null) {
            for (size_t i = 0; i < size; i++) {
                if (a[i] <= 0 || std::fabs(a[i] - 1.0) < EPSILON) {
                    null_map[i] = 1;
                } else {
                    c[i] = static_cast<Float64>(std::log(static_cast<Float64>(b)) /
                                                std::log(static_cast<Float64>(a[i])));
                }
            }
        }
    }

    template <typename Result>
    static inline Result apply(A a, B b, UInt8& is_null) {
        is_null = a <= 0 || b <= 0 || std::fabs(a - 1.0) < EPSILON;
        return static_cast<Float64>(std::log(static_cast<Float64>(b)) /
                                    std::log(static_cast<Float64>(a)));
    }
};
using FunctionLog = FunctionBinaryArithmetic<LogImpl, LogName, true>;

template <typename A>
struct SignImpl {
    using ResultType = Int8;
    static inline ResultType apply(A a) {
        if constexpr (IsDecimalNumber<A> || std::is_floating_point_v<A>)
            return static_cast<ResultType>(a < A(0) ? -1 : a == A(0) ? 0 : 1);
        else if constexpr (std::is_signed_v<A>)
            return static_cast<ResultType>(a < 0 ? -1 : a == 0 ? 0 : 1);
        else if constexpr (std::is_unsigned_v<A>)
            return static_cast<ResultType>(a == 0 ? 0 : 1);
    }
};

struct NameSign {
    static constexpr auto name = "sign";
};
using FunctionSign = FunctionUnaryArithmetic<SignImpl, NameSign, false>;

template <typename A>
struct AbsImpl {
    using ResultType =
            std::conditional_t<IsDecimalNumber<A>, A, typename NumberTraits::ResultOfAbs<A>::Type>;

    static inline ResultType apply(A a) {
        if constexpr (IsDecimalNumber<A>)
            return a < A(0) ? A(-a) : a;
        else if constexpr (std::is_integral_v<A> && std::is_signed_v<A>)
            return a < A(0) ? static_cast<ResultType>(~a) + 1 : a;
        else if constexpr (std::is_integral_v<A> && std::is_unsigned_v<A>)
            return static_cast<ResultType>(a);
        else if constexpr (std::is_floating_point_v<A>)
            return static_cast<ResultType>(std::abs(a));
    }
};

struct NameAbs {
    static constexpr auto name = "abs";
};

using FunctionAbs = FunctionUnaryArithmetic<AbsImpl, NameAbs, false>;

template <typename A>
struct NegativeImpl {
    using ResultType = A;

    static inline ResultType apply(A a) { return -a; }
};

struct NameNegative {
    static constexpr auto name = "negative";
};

using FunctionNegative = FunctionUnaryArithmetic<NegativeImpl, NameNegative, false>;

template <typename A>
struct PositiveImpl {
    using ResultType = A;

    static inline ResultType apply(A a) { return static_cast<ResultType>(a); }
};

struct NamePositive {
    static constexpr auto name = "positive";
};

using FunctionPositive = FunctionUnaryArithmetic<PositiveImpl, NamePositive, false>;

struct SinName {
    static constexpr auto name = "sin";
};
using FunctionSin = FunctionMathUnary<UnaryFunctionVectorized<SinName, std::sin>>;

struct SqrtName {
    static constexpr auto name = "sqrt";
};
using FunctionSqrt = FunctionMathUnary<UnaryFunctionVectorized<SqrtName, std::sqrt>>;

struct CbrtName {
    static constexpr auto name = "cbrt";
};
using FunctionCbrt = FunctionMathUnary<UnaryFunctionVectorized<CbrtName, std::cbrt>>;

struct TanName {
    static constexpr auto name = "tan";
};
using FunctionTan = FunctionMathUnary<UnaryFunctionVectorized<TanName, std::tan>>;

template <typename A>
struct RadiansImpl {
    using ResultType = A;

    static inline ResultType apply(A a) {
        return static_cast<ResultType>(a * PiImpl::value / 180.0);
    }
};

struct NameRadians {
    static constexpr auto name = "radians";
};

using FunctionRadians = FunctionUnaryArithmetic<RadiansImpl, NameRadians, false>;

template <typename A>
struct DegreesImpl {
    using ResultType = A;

    static inline ResultType apply(A a) {
        return static_cast<ResultType>(a * 180.0 / PiImpl::value);
    }
};

struct NameDegrees {
    static constexpr auto name = "degrees";
};

using FunctionDegrees = FunctionUnaryArithmetic<DegreesImpl, NameDegrees, false>;

struct NameBin {
    static constexpr auto name = "bin";
};
struct BinImpl {
    using ReturnType = DataTypeString;
    static constexpr auto TYPE_INDEX = TypeIndex::Int64;
    using Type = Int64;
    using ReturnColumnType = ColumnString;

    static std::string bin_impl(Int64 value) {
        uint64_t n = static_cast<uint64_t>(value);
        const size_t max_bits = sizeof(uint64_t) * 8;
        char result[max_bits];
        uint32_t index = max_bits;
        do {
            result[--index] = '0' + (n & 1);
        } while (n >>= 1);
        return std::string(result + index, max_bits - index);
    }

    static Status vector(const ColumnInt64::Container& data, ColumnString::Chars& res_data,
                         ColumnString::Offsets& res_offsets) {
        res_offsets.resize(data.size());
        size_t input_size = res_offsets.size();

        for (size_t i = 0; i < input_size; ++i) {
            StringOP::push_value_string(bin_impl(data[i]), i, res_data, res_offsets);
        }
        return Status::OK();
    }
};

using FunctionBin = FunctionUnaryToType<BinImpl, NameBin>;

template <typename A, typename B>
struct PowImpl {
    using ResultType = double;
    static const constexpr bool allow_decimal = false;

    template <typename type>
    static inline double apply(A a, B b) {
        /// Next everywhere, static_cast - so that there is no wrong result in expressions of the form Int64 c = UInt32(a) * Int32(-1).
        return std::pow((double)a, (double)b);
    }
};
struct PowName {
    static constexpr auto name = "pow";
};
using FunctionPow = FunctionBinaryArithmetic<PowImpl, PowName, false>;

struct TruncateName {
    static constexpr auto name = "truncate";
};

struct CeilName {
    static constexpr auto name = "ceil";
};

struct FloorName {
    static constexpr auto name = "floor";
};

struct RoundName {
    static constexpr auto name = "round";
};

struct RoundBankersName {
    static constexpr auto name = "round_bankers";
};

/// round(double,int32)-->double
/// key_str:roundFloat64Int32
template <typename Name>
struct DoubleRoundTwoImpl {
    static constexpr auto name = Name::name;

    static DataTypes get_variadic_argument_types() {
        return {std::make_shared<vectorized::DataTypeFloat64>(),
                std::make_shared<vectorized::DataTypeInt32>()};
    }
};

template <typename Name>
struct DoubleRoundOneImpl {
    static constexpr auto name = Name::name;

    static DataTypes get_variadic_argument_types() {
        return {std::make_shared<vectorized::DataTypeFloat64>()};
    }
};

template <typename Name>
struct DecimalRoundTwoImpl {
    static constexpr auto name = Name::name;

    static DataTypes get_variadic_argument_types() {
        return {std::make_shared<vectorized::DataTypeDecimal<Decimal32>>(9, 0),
                std::make_shared<vectorized::DataTypeInt32>()};
    }
};

template <typename Name>
struct DecimalRoundOneImpl {
    static constexpr auto name = Name::name;

    static DataTypes get_variadic_argument_types() {
        return {std::make_shared<vectorized::DataTypeDecimal<Decimal32>>(9, 0)};
    }
};

// TODO: Now math may cause one thread compile time too long, because the function in math
// so mush. Split it to speed up compile time in the future
void register_function_math(SimpleFunctionFactory& factory) {
#define REGISTER_ROUND_FUNCTIONS(IMPL)                                                           \
    factory.register_function<                                                                   \
            FunctionRounding<IMPL<RoundName>, RoundingMode::Round, TieBreakingMode::Auto>>();    \
    factory.register_function<                                                                   \
            FunctionRounding<IMPL<FloorName>, RoundingMode::Floor, TieBreakingMode::Auto>>();    \
    factory.register_function<                                                                   \
            FunctionRounding<IMPL<CeilName>, RoundingMode::Ceil, TieBreakingMode::Auto>>();      \
    factory.register_function<                                                                   \
            FunctionRounding<IMPL<TruncateName>, RoundingMode::Trunc, TieBreakingMode::Auto>>(); \
    factory.register_function<FunctionRounding<IMPL<RoundBankersName>, RoundingMode::Round,      \
                                               TieBreakingMode::Bankers>>();

    REGISTER_ROUND_FUNCTIONS(DecimalRoundOneImpl)
    REGISTER_ROUND_FUNCTIONS(DecimalRoundTwoImpl)
    REGISTER_ROUND_FUNCTIONS(DoubleRoundOneImpl)
    REGISTER_ROUND_FUNCTIONS(DoubleRoundTwoImpl)
    factory.register_alias("round", "dround");
    factory.register_function<FunctionAcos>();
    factory.register_function<FunctionAsin>();
    factory.register_function<FunctionAtan>();
    factory.register_function<FunctionCos>();
    factory.register_alias("ceil", "dceil");
    factory.register_alias("ceil", "ceiling");
    factory.register_function<FunctionE>();
    factory.register_function<FunctionLn>();
    factory.register_alias("ln", "dlog1");
    factory.register_function<FunctionLog>();
    factory.register_function<FunctionLog2>();
    factory.register_function<FunctionLog10>();
    factory.register_alias("log10", "dlog10");
    factory.register_function<FunctionPi>();
    factory.register_function<FunctionSign>();
    factory.register_function<FunctionAbs>();
    factory.register_function<FunctionNegative>();
    factory.register_function<FunctionPositive>();
    factory.register_function<FunctionSin>();
    factory.register_function<FunctionSqrt>();
    factory.register_alias("sqrt", "dsqrt");
    factory.register_function<FunctionCbrt>();
    factory.register_function<FunctionTan>();
    factory.register_alias("floor", "dfloor");
    factory.register_function<FunctionPow>();
    factory.register_alias("pow", "power");
    factory.register_alias("pow", "dpow");
    factory.register_alias("pow", "fpow");
    factory.register_function<FunctionExp>();
    factory.register_alias("exp", "dexp");
    factory.register_function<FunctionRadians>();
    factory.register_function<FunctionDegrees>();
    factory.register_function<FunctionBin>();
}
} // namespace doris::vectorized
