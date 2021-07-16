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

#include "vec/common/field_visitors.h"
#include "vec/data_types/number_traits.h"
#include "vec/functions/function_const.h"
#include "vec/functions/function_math_binary_float64.h"
#include "vec/functions/function_math_unary.h"
#include "vec/functions/function_unary_arithmetic.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {
namespace {

const double log_10[] = {
        1e000, 1e001, 1e002, 1e003, 1e004, 1e005, 1e006, 1e007, 1e008, 1e009, 1e010, 1e011, 1e012,
        1e013, 1e014, 1e015, 1e016, 1e017, 1e018, 1e019, 1e020, 1e021, 1e022, 1e023, 1e024, 1e025,
        1e026, 1e027, 1e028, 1e029, 1e030, 1e031, 1e032, 1e033, 1e034, 1e035, 1e036, 1e037, 1e038,
        1e039, 1e040, 1e041, 1e042, 1e043, 1e044, 1e045, 1e046, 1e047, 1e048, 1e049, 1e050, 1e051,
        1e052, 1e053, 1e054, 1e055, 1e056, 1e057, 1e058, 1e059, 1e060, 1e061, 1e062, 1e063, 1e064,
        1e065, 1e066, 1e067, 1e068, 1e069, 1e070, 1e071, 1e072, 1e073, 1e074, 1e075, 1e076, 1e077,
        1e078, 1e079, 1e080, 1e081, 1e082, 1e083, 1e084, 1e085, 1e086, 1e087, 1e088, 1e089, 1e090,
        1e091, 1e092, 1e093, 1e094, 1e095, 1e096, 1e097, 1e098, 1e099, 1e100, 1e101, 1e102, 1e103,
        1e104, 1e105, 1e106, 1e107, 1e108, 1e109, 1e110, 1e111, 1e112, 1e113, 1e114, 1e115, 1e116,
        1e117, 1e118, 1e119, 1e120, 1e121, 1e122, 1e123, 1e124, 1e125, 1e126, 1e127, 1e128, 1e129,
        1e130, 1e131, 1e132, 1e133, 1e134, 1e135, 1e136, 1e137, 1e138, 1e139, 1e140, 1e141, 1e142,
        1e143, 1e144, 1e145, 1e146, 1e147, 1e148, 1e149, 1e150, 1e151, 1e152, 1e153, 1e154, 1e155,
        1e156, 1e157, 1e158, 1e159, 1e160, 1e161, 1e162, 1e163, 1e164, 1e165, 1e166, 1e167, 1e168,
        1e169, 1e170, 1e171, 1e172, 1e173, 1e174, 1e175, 1e176, 1e177, 1e178, 1e179, 1e180, 1e181,
        1e182, 1e183, 1e184, 1e185, 1e186, 1e187, 1e188, 1e189, 1e190, 1e191, 1e192, 1e193, 1e194,
        1e195, 1e196, 1e197, 1e198, 1e199, 1e200, 1e201, 1e202, 1e203, 1e204, 1e205, 1e206, 1e207,
        1e208, 1e209, 1e210, 1e211, 1e212, 1e213, 1e214, 1e215, 1e216, 1e217, 1e218, 1e219, 1e220,
        1e221, 1e222, 1e223, 1e224, 1e225, 1e226, 1e227, 1e228, 1e229, 1e230, 1e231, 1e232, 1e233,
        1e234, 1e235, 1e236, 1e237, 1e238, 1e239, 1e240, 1e241, 1e242, 1e243, 1e244, 1e245, 1e246,
        1e247, 1e248, 1e249, 1e250, 1e251, 1e252, 1e253, 1e254, 1e255, 1e256, 1e257, 1e258, 1e259,
        1e260, 1e261, 1e262, 1e263, 1e264, 1e265, 1e266, 1e267, 1e268, 1e269, 1e270, 1e271, 1e272,
        1e273, 1e274, 1e275, 1e276, 1e277, 1e278, 1e279, 1e280, 1e281, 1e282, 1e283, 1e284, 1e285,
        1e286, 1e287, 1e288, 1e289, 1e290, 1e291, 1e292, 1e293, 1e294, 1e295, 1e296, 1e297, 1e298,
        1e299, 1e300, 1e301, 1e302, 1e303, 1e304, 1e305, 1e306, 1e307, 1e308};

#define ARRAY_ELEMENTS(A) ((uint64_t)(sizeof(A) / sizeof(A[0])))

double my_double_round(double value, int64_t dec, bool dec_unsigned, bool truncate) {
    bool dec_negative = (dec < 0) && !dec_unsigned;
    uint64_t abs_dec = dec_negative ? -dec : dec;
    /*
       tmp2 is here to avoid return the value with 80 bit precision
       This will fix that the test round(0.1,1) = round(0.1,1) is true
       Tagging with volatile is no guarantee, it may still be optimized away...
       */
    volatile double tmp2 = 0.0;

    double tmp =
            (abs_dec < ARRAY_ELEMENTS(log_10) ? log_10[abs_dec] : std::pow(10.0, (double)abs_dec));

    // Pre-compute these, to avoid optimizing away e.g. 'floor(v/tmp) * tmp'.
    volatile double value_div_tmp = value / tmp;
    volatile double value_mul_tmp = value * tmp;

    if (dec_negative && std::isinf(tmp)) {
        tmp2 = 0.0;
    } else if (!dec_negative && std::isinf(value_mul_tmp)) {
        tmp2 = value;
    } else if (truncate) {
        if (value >= 0.0) {
            tmp2 = dec < 0 ? std::floor(value_div_tmp) * tmp : std::floor(value_mul_tmp) / tmp;
        } else {
            tmp2 = dec < 0 ? std::ceil(value_div_tmp) * tmp : std::ceil(value_mul_tmp) / tmp;
        }
    } else {
        tmp2 = dec < 0 ? std::rint(value_div_tmp) * tmp : std::rint(value_mul_tmp) / tmp;
    }

    return tmp2;
}
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

struct LnName {
    static constexpr auto name = "ln";
};
using FunctionLn = FunctionMathUnary<UnaryFunctionVectorized<LnName, std::log>>;

struct Log2Name {
    static constexpr auto name = "log2";
};
using FunctionLog2 = FunctionMathUnary<UnaryFunctionVectorized<Log2Name, std::log2>>;

struct Log10Name {
    static constexpr auto name = "log10";
};
using FunctionLog10 = FunctionMathUnary<UnaryFunctionVectorized<Log10Name, std::log10>>;

struct LogName {
    static constexpr auto name = "log";
};

template <typename Name>
struct LogImpl {
    static constexpr auto name = LogName::name;
    static constexpr auto rows_per_iteration = 1;

    template <typename T1, typename T2>
    static void execute(const T1* src_left, const T2* src_right, Float64* dst) {
        dst[0] = static_cast<Float64>(std::log(static_cast<Float64>(src_left[0])) /
                                      std::log(static_cast<Float64>(src_right[0])));
    }
};
using FunctionLog = FunctionMathBinaryFloat64<LogImpl<LogName>>;

struct CeilName {
    static constexpr auto name = "ceil";
};
using FunctionCeil = FunctionMathUnary<UnaryFunctionVectorized<CeilName, std::ceil>>;

template <typename A>
struct SignImpl {
    using ResultType = Float32;
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
            return a < 0 ? A(-a) : a;
        else if constexpr (std::is_integral_v<A> && std::is_signed_v<A>)
            return a < 0 ? static_cast<ResultType>(~a) + 1 : a;
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

    static inline ResultType apply(A a) {
        if constexpr (IsDecimalNumber<A>)
            return a > 0 ? A(-a) : a;
        else if constexpr (std::is_integral_v<A> && std::is_signed_v<A>)
            return a > 0 ? static_cast<ResultType>(~a) + 1 : a;
        else if constexpr (std::is_integral_v<A> && std::is_unsigned_v<A>)
            return static_cast<ResultType>(-a);
        else if constexpr (std::is_floating_point_v<A>)
            return static_cast<ResultType>(-std::abs(a));
    }
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

struct TanName {
    static constexpr auto name = "tan";
};
using FunctionTan = FunctionMathUnary<UnaryFunctionVectorized<TanName, std::tan>>;

struct FloorName {
    static constexpr auto name = "floor";
};
using FunctionFloor = FunctionMathUnary<UnaryFunctionVectorized<FloorName, std::floor>>;

struct RoundName {
    static constexpr auto name = "round";
};
using FunctionRound = FunctionMathUnary<UnaryFunctionVectorized<RoundName, std::round>>;

struct PowName {
    static constexpr auto name = "pow";
};
using FunctionPow = FunctionMathBinaryFloat64<BinaryFunctionVectorized<PowName, std::pow>>;

struct TruncateName {
    static constexpr auto name = "truncate";
};

template <typename Name>
struct TruncateImpl {
    static constexpr auto rows_per_iteration = 1;
    static constexpr auto name = TruncateName::name;

    template <typename T1, typename T2>
    static void execute(const T1* src_left, const T2* src_right, Float64* dst) {
        dst[0] = static_cast<Float64>(my_double_round(
                static_cast<Float64>(src_left[0]), static_cast<Int32>(src_right[0]), false, true));
    }
};
using FunctionTruncate = FunctionMathBinaryFloat64<TruncateImpl<TruncateName>>;

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

} // namespace
void register_function_math(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionAcos>();
    factory.register_function<FunctionAsin>();
    factory.register_function<FunctionAtan>();
    factory.register_function<FunctionCos>();
    factory.register_function<FunctionCeil>();
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
    factory.register_function<FunctionTan>();
    factory.register_function<FunctionFloor>();
    factory.register_alias("floor", "dfloor");
    factory.register_function<FunctionRound>();
    factory.register_function<FunctionPow>();
    factory.register_alias("pow", "power");
    factory.register_alias("pow", "dpow");
    factory.register_alias("pow", "fpow");
    factory.register_function<FunctionExp>();
    factory.register_alias("exp", "dexp");
    factory.register_function<FunctionTruncate>();
    factory.register_function<FunctionRadians>();
    factory.register_function<FunctionDegrees>();
}
} // namespace doris::vectorized