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

#include <cstdint>
#include <cstring>

// IWYU pragma: no_include <bits/std_abs.h>
#include <dlfcn.h>

#include <cmath>
#include <string>
#include <type_traits>

#include "common/status.h"
#include "vec/columns/column.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_string.h"
#include "vec/data_types/number_traits.h"
#include "vec/functions/function_const.h"
#include "vec/functions/function_math_log.h"
#include "vec/functions/function_math_unary.h"
#include "vec/functions/function_math_unary_alway_nullable.h"
#include "vec/functions/function_totype.h"
#include "vec/functions/function_unary_arithmetic.h"
#include "vec/functions/simple_function_factory.h"
#include "vec/utils/stringop_substring.h"

namespace doris::vectorized {

struct AcosName {
    static constexpr auto name = "acos";
    // https://dev.mysql.com/doc/refman/8.4/en/mathematical-functions.html#function_acos
    static constexpr bool is_invalid_input(Float64 x) { return x < -1 || x > 1; }
};
using FunctionAcos =
        FunctionMathUnaryAlwayNullable<UnaryFunctionPlainAlwayNullable<AcosName, std::acos>>;

struct AcoshName {
    static constexpr auto name = "acosh";
    static constexpr bool is_invalid_input(Float64 x) { return x < 1; }
};
using FunctionAcosh =
        FunctionMathUnaryAlwayNullable<UnaryFunctionPlainAlwayNullable<AcoshName, std::acosh>>;

struct AsinName {
    static constexpr auto name = "asin";
    // https://dev.mysql.com/doc/refman/8.4/en/mathematical-functions.html#function_asin
    static constexpr bool is_invalid_input(Float64 x) { return x < -1 || x > 1; }
};
using FunctionAsin =
        FunctionMathUnaryAlwayNullable<UnaryFunctionPlainAlwayNullable<AsinName, std::asin>>;

struct AsinhName {
    static constexpr auto name = "asinh";
};
using FunctionAsinh = FunctionMathUnary<UnaryFunctionPlain<AsinhName, std::asinh>>;

struct AtanName {
    static constexpr auto name = "atan";
};
using FunctionAtan = FunctionMathUnary<UnaryFunctionPlain<AtanName, std::atan>>;

struct AtanhName {
    static constexpr auto name = "atanh";
    static constexpr bool is_invalid_input(Float64 x) { return x <= -1 || x >= 1; }
};
using FunctionAtanh =
        FunctionMathUnaryAlwayNullable<UnaryFunctionPlainAlwayNullable<AtanhName, std::atanh>>;

struct CosName {
    static constexpr auto name = "cos";
};
using FunctionCos = FunctionMathUnary<UnaryFunctionPlain<CosName, std::cos>>;

struct CoshName {
    static constexpr auto name = "cosh";
};
using FunctionCosh = FunctionMathUnary<UnaryFunctionPlain<CoshName, std::cosh>>;

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
using FunctionExp = FunctionMathUnary<UnaryFunctionPlain<ExpName, std::exp>>;

template <typename A>
struct SignImpl {
    static constexpr PrimitiveType ResultType = TYPE_TINYINT;
    static inline UInt8 apply(A a) {
        if constexpr (IsDecimalNumber<A> || std::is_floating_point_v<A>) {
            return static_cast<UInt8>(a < A(0) ? -1 : a == A(0) ? 0 : 1);
        } else if constexpr (IsSignedV<A>) {
            return static_cast<UInt8>(a < 0 ? -1 : a == 0 ? 0 : 1);
        } else if constexpr (IsUnsignedV<A>) {
            return static_cast<UInt8>(a == 0 ? 0 : 1);
        }
    }
};

struct NameSign {
    static constexpr auto name = "sign";
};
using FunctionSign = FunctionUnaryArithmetic<SignImpl, NameSign>;

template <typename A>
struct AbsImpl {
    static constexpr PrimitiveType ResultType = NumberTraits::ResultOfAbs<A>::Type;

    static inline typename PrimitiveTypeTraits<ResultType>::ColumnItemType apply(A a) {
        if constexpr (IsDecimalNumber<A>) {
            return a < A(0) ? A(-a) : a;
        } else if constexpr (IsIntegralV<A> && IsSignedV<A>) {
            return a < A(0) ? static_cast<typename PrimitiveTypeTraits<ResultType>::ColumnItemType>(
                                      ~a) +
                                      1
                            : a;
        } else if constexpr (IsIntegralV<A> && IsUnsignedV<A>) {
            return static_cast<typename PrimitiveTypeTraits<ResultType>::ColumnItemType>(a);
        } else if constexpr (std::is_floating_point_v<A>) {
            return static_cast<typename PrimitiveTypeTraits<ResultType>::ColumnItemType>(
                    std::abs(a));
        }
    }
};

struct NameAbs {
    static constexpr auto name = "abs";
};

template <typename A>
struct ResultOfUnaryFunc;

template <>
struct ResultOfUnaryFunc<UInt8> {
    static constexpr PrimitiveType ResultType = TYPE_BOOLEAN;
};

template <>
struct ResultOfUnaryFunc<Int8> {
    static constexpr PrimitiveType ResultType = TYPE_TINYINT;
};

template <>
struct ResultOfUnaryFunc<Int16> {
    static constexpr PrimitiveType ResultType = TYPE_SMALLINT;
};

template <>
struct ResultOfUnaryFunc<Int32> {
    static constexpr PrimitiveType ResultType = TYPE_INT;
};

template <>
struct ResultOfUnaryFunc<Int64> {
    static constexpr PrimitiveType ResultType = TYPE_BIGINT;
};

template <>
struct ResultOfUnaryFunc<Int128> {
    static constexpr PrimitiveType ResultType = TYPE_LARGEINT;
};

template <>
struct ResultOfUnaryFunc<Decimal32> {
    static constexpr PrimitiveType ResultType = TYPE_DECIMAL32;
};

template <>
struct ResultOfUnaryFunc<Decimal64> {
    static constexpr PrimitiveType ResultType = TYPE_DECIMAL64;
};

template <>
struct ResultOfUnaryFunc<Decimal128V3> {
    static constexpr PrimitiveType ResultType = TYPE_DECIMAL128I;
};

template <>
struct ResultOfUnaryFunc<Decimal128V2> {
    static constexpr PrimitiveType ResultType = TYPE_DECIMALV2;
};

template <>
struct ResultOfUnaryFunc<Decimal256> {
    static constexpr PrimitiveType ResultType = TYPE_DECIMAL256;
};

template <>
struct ResultOfUnaryFunc<float> {
    static constexpr PrimitiveType ResultType = TYPE_FLOAT;
};

template <>
struct ResultOfUnaryFunc<double> {
    static constexpr PrimitiveType ResultType = TYPE_DOUBLE;
};

using FunctionAbs = FunctionUnaryArithmetic<AbsImpl, NameAbs>;

template <typename A>
struct NegativeImpl {
    static constexpr PrimitiveType ResultType = ResultOfUnaryFunc<A>::ResultType;

    static inline typename PrimitiveTypeTraits<ResultType>::ColumnItemType apply(A a) { return -a; }
};

struct NameNegative {
    static constexpr auto name = "negative";
};

using FunctionNegative = FunctionUnaryArithmetic<NegativeImpl, NameNegative>;

template <typename A>
struct PositiveImpl {
    static constexpr PrimitiveType ResultType = ResultOfUnaryFunc<A>::ResultType;

    static inline typename PrimitiveTypeTraits<ResultType>::ColumnItemType apply(A a) {
        return static_cast<typename PrimitiveTypeTraits<ResultType>::ColumnItemType>(a);
    }
};

struct NamePositive {
    static constexpr auto name = "positive";
};

using FunctionPositive = FunctionUnaryArithmetic<PositiveImpl, NamePositive>;

struct SinName {
    static constexpr auto name = "sin";
};
using FunctionSin = FunctionMathUnary<UnaryFunctionPlain<SinName, std::sin>>;

struct SinhName {
    static constexpr auto name = "sinh";
};
using FunctionSinh = FunctionMathUnary<UnaryFunctionPlain<SinhName, std::sinh>>;

struct SqrtName {
    static constexpr auto name = "sqrt";
    // https://dev.mysql.com/doc/refman/8.4/en/mathematical-functions.html#function_sqrt
    static constexpr bool is_invalid_input(Float64 x) { return x < 0; }
};
using FunctionSqrt =
        FunctionMathUnaryAlwayNullable<UnaryFunctionPlainAlwayNullable<SqrtName, std::sqrt>>;

struct CbrtName {
    static constexpr auto name = "cbrt";
};
using FunctionCbrt = FunctionMathUnary<UnaryFunctionPlain<CbrtName, std::cbrt>>;

struct TanName {
    static constexpr auto name = "tan";
};
using FunctionTan = FunctionMathUnary<UnaryFunctionPlain<TanName, std::tan>>;

struct TanhName {
    static constexpr auto name = "tanh";
};
using FunctionTanh = FunctionMathUnary<UnaryFunctionPlain<TanhName, std::tanh>>;

struct CotName {
    static constexpr auto name = "cot";
};
double cot(double x) {
    return 1.0 / std::tan(x);
}
using FunctionCot = FunctionMathUnary<UnaryFunctionPlain<CotName, cot>>;

struct SecName {
    static constexpr auto name = "sec";
};
double sec(double x) {
    return 1.0 / std::cos(x);
}
using FunctionSec = FunctionMathUnary<UnaryFunctionPlain<SecName, sec>>;

struct CscName {
    static constexpr auto name = "csc";
};
double csc(double x) {
    return 1.0 / std::sin(x);
}
using FunctionCosec = FunctionMathUnary<UnaryFunctionPlain<CscName, csc>>;

template <typename A>
struct RadiansImpl {
    static constexpr PrimitiveType ResultType = ResultOfUnaryFunc<A>::ResultType;

    static inline typename PrimitiveTypeTraits<ResultType>::ColumnItemType apply(A a) {
        return static_cast<typename PrimitiveTypeTraits<ResultType>::ColumnItemType>(
                a * PiImpl::value / 180.0);
    }
};

struct NameRadians {
    static constexpr auto name = "radians";
};

using FunctionRadians = FunctionUnaryArithmetic<RadiansImpl, NameRadians>;

template <typename A>
struct DegreesImpl {
    static constexpr PrimitiveType ResultType = ResultOfUnaryFunc<A>::ResultType;

    static inline typename PrimitiveTypeTraits<ResultType>::ColumnItemType apply(A a) {
        return static_cast<typename PrimitiveTypeTraits<ResultType>::ColumnItemType>(a * 180.0 /
                                                                                     PiImpl::value);
    }
};

struct NameDegrees {
    static constexpr auto name = "degrees";
};

using FunctionDegrees = FunctionUnaryArithmetic<DegreesImpl, NameDegrees>;

struct NameBin {
    static constexpr auto name = "bin";
};
struct BinImpl {
    using ReturnType = DataTypeString;
    static constexpr auto PrimitiveTypeImpl = PrimitiveType::TYPE_BIGINT;
    using Type = Int64;
    using ReturnColumnType = ColumnString;

    static std::string bin_impl(Int64 value) {
        auto n = static_cast<uint64_t>(value);
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

struct PowImpl {
    static constexpr PrimitiveType type = TYPE_DOUBLE;
    static constexpr auto name = "pow";
    static constexpr bool need_replace_null_data_to_default = true;
    static constexpr bool is_nullable = false;
    static inline double apply(double a, double b) { return std::pow(a, b); }
};
struct LogImpl {
    static constexpr PrimitiveType type = TYPE_DOUBLE;
    static constexpr auto name = "log";
    static constexpr bool need_replace_null_data_to_default = false;
    static constexpr bool is_nullable = true;
    static constexpr double EPSILON = 1e-9;
    static inline double apply(double a, double b, UInt8& is_null) {
        is_null = a <= 0 || b <= 0 || std::fabs(a - 1.0) < EPSILON;
        return std::log(b) / std::log(a);
    }
};
struct Atan2Impl {
    static constexpr PrimitiveType type = TYPE_DOUBLE;
    static constexpr auto name = "atan2";
    static constexpr bool need_replace_null_data_to_default = false;
    static constexpr bool is_nullable = false;
    static inline double apply(double a, double b) { return std::atan2(a, b); }
};

template <typename Impl>
class FunctionMathBinary : public IFunction {
public:
    using cpp_type = typename PrimitiveTypeTraits<Impl::type>::CppType;
    using column_type = typename PrimitiveTypeTraits<Impl::type>::ColumnType;

    static constexpr auto name = Impl::name;
    static constexpr bool has_variadic_argument =
            !std::is_void_v<decltype(has_variadic_argument_types(std::declval<Impl>()))>;

    String get_name() const override { return name; }

    static FunctionPtr create() { return std::make_shared<FunctionMathBinary<Impl>>(); }

    bool is_variadic() const override { return has_variadic_argument; }

    DataTypes get_variadic_argument_types_impl() const override {
        if constexpr (has_variadic_argument) {
            return Impl::get_variadic_argument_types();
        }
        return {};
    }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        auto res = std::make_shared<DataTypeNumber<Impl::type>>();
        return Impl::is_nullable ? make_nullable(res) : res;
    }
    bool need_replace_null_data_to_default() const override {
        return Impl::need_replace_null_data_to_default;
    }

    size_t get_number_of_arguments() const override { return 2; }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        auto& column_left = block.get_by_position(arguments[0]).column;
        auto& column_right = block.get_by_position(arguments[1]).column;
        bool is_const_left = is_column_const(*column_left);
        bool is_const_right = is_column_const(*column_right);

        ColumnPtr column_result = nullptr;
        if (is_const_left && is_const_right) {
            column_result = constant_constant(column_left, column_right);
        } else if (is_const_left) {
            column_result = constant_vector(column_left, column_right);
        } else if (is_const_right) {
            column_result = vector_constant(column_left, column_right);
        } else {
            column_result = vector_vector(column_left, column_right);
        }
        block.replace_by_position(result, std::move(column_result));

        return Status::OK();
    }

private:
    ColumnPtr constant_constant(ColumnPtr column_left, ColumnPtr column_right) const {
        const auto* column_left_ptr = assert_cast<const ColumnConst*>(column_left.get());
        const auto* column_right_ptr = assert_cast<const ColumnConst*>(column_right.get());
        ColumnPtr column_result = nullptr;

        auto res = column_type::create(1);
        if constexpr (Impl::is_nullable) {
            auto null_map = ColumnUInt8::create(1, 0);
            res->get_element(0) = Impl::apply(column_left_ptr->template get_value<cpp_type>(),
                                              column_right_ptr->template get_value<cpp_type>(),
                                              null_map->get_element(0));
            column_result = ColumnNullable::create(std::move(res), std::move(null_map));
        } else {
            res->get_element(0) = Impl::apply(column_left_ptr->template get_value<cpp_type>(),
                                              column_right_ptr->template get_value<cpp_type>());
            column_result = std::move(res);
        }

        return ColumnConst::create(std::move(column_result), column_left->size());
    }

    ColumnPtr vector_constant(ColumnPtr column_left, ColumnPtr column_right) const {
        const auto* column_right_ptr = assert_cast<const ColumnConst*>(column_right.get());
        const auto* column_left_ptr = assert_cast<const column_type*>(column_left.get());
        auto column_result = column_type::create(column_left->size());

        if constexpr (Impl::is_nullable) {
            auto null_map = ColumnUInt8::create(column_left->size(), 0);
            auto& a = column_left_ptr->get_data();
            auto& c = column_result->get_data();
            auto& n = null_map->get_data();
            size_t size = a.size();
            for (size_t i = 0; i < size; ++i) {
                c[i] = Impl::apply(a[i], column_right_ptr->template get_value<cpp_type>(), n[i]);
            }
            return ColumnNullable::create(std::move(column_result), std::move(null_map));
        } else {
            auto& a = column_left_ptr->get_data();
            auto& c = column_result->get_data();
            size_t size = a.size();
            for (size_t i = 0; i < size; ++i) {
                c[i] = Impl::apply(a[i], column_right_ptr->template get_value<cpp_type>());
            }
            return column_result;
        }
    }

    ColumnPtr constant_vector(ColumnPtr column_left, ColumnPtr column_right) const {
        const auto* column_left_ptr = assert_cast<const ColumnConst*>(column_left.get());

        const auto* column_right_ptr = assert_cast<const column_type*>(column_right.get());
        auto column_result = column_type::create(column_right->size());

        if constexpr (Impl::is_nullable) {
            auto null_map = ColumnUInt8::create(column_right->size(), 0);
            auto& b = column_right_ptr->get_data();
            auto& c = column_result->get_data();
            auto& n = null_map->get_data();
            size_t size = b.size();
            for (size_t i = 0; i < size; ++i) {
                c[i] = Impl::apply(column_left_ptr->template get_value<cpp_type>(), b[i], n[i]);
            }
            return ColumnNullable::create(std::move(column_result), std::move(null_map));
        } else {
            auto& b = column_right_ptr->get_data();
            auto& c = column_result->get_data();
            size_t size = b.size();
            for (size_t i = 0; i < size; ++i) {
                c[i] = Impl::apply(column_left_ptr->template get_value<cpp_type>(), b[i]);
            }
            return column_result;
        }
    }

    ColumnPtr vector_vector(ColumnPtr column_left, ColumnPtr column_right) const {
        const auto* column_left_ptr = assert_cast<const column_type*>(column_left->get_ptr().get());
        const auto* column_right_ptr =
                assert_cast<const column_type*>(column_right->get_ptr().get());

        auto column_result = column_type::create(column_left->size());

        if constexpr (Impl::is_nullable) {
            auto null_map = ColumnUInt8::create(column_result->size(), 0);
            auto& a = column_left_ptr->get_data();
            auto& b = column_right_ptr->get_data();
            auto& c = column_result->get_data();
            auto& n = null_map->get_data();
            size_t size = a.size();
            for (size_t i = 0; i < size; ++i) {
                c[i] = Impl::apply(a[i], b[i], n[i]);
            }
            return ColumnNullable::create(std::move(column_result), std::move(null_map));
        } else {
            auto& a = column_left_ptr->get_data();
            auto& b = column_right_ptr->get_data();
            auto& c = column_result->get_data();
            size_t size = a.size();
            for (size_t i = 0; i < size; ++i) {
                c[i] = Impl::apply(a[i], b[i]);
            }
            return column_result;
        }
    }
};

class FunctionNormalCdf : public IFunction {
public:
    static constexpr auto name = "normal_cdf";

    String get_name() const override { return name; }

    static FunctionPtr create() { return std::make_shared<FunctionNormalCdf>(); }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return make_nullable(std::make_shared<DataTypeFloat64>());
    }

    DataTypes get_variadic_argument_types_impl() const override {
        return {std::make_shared<DataTypeFloat64>(), std::make_shared<DataTypeFloat64>(),
                std::make_shared<DataTypeFloat64>()};
    }
    size_t get_number_of_arguments() const override { return 3; }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        auto result_column = ColumnFloat64::create(input_rows_count);
        auto result_null_map_column = ColumnUInt8::create(input_rows_count, 0);

        auto& result_data = result_column->get_data();
        NullMap& result_null_map =
                assert_cast<ColumnUInt8*>(result_null_map_column.get())->get_data();

        ColumnPtr argument_columns[3];
        bool col_const[3];
        size_t argument_size = arguments.size();
        for (int i = 0; i < argument_size; ++i) {
            argument_columns[i] = block.get_by_position(arguments[i]).column;
            col_const[i] = is_column_const(*argument_columns[i]);
            if (col_const[i]) {
                argument_columns[i] =
                        static_cast<const ColumnConst&>(*argument_columns[i]).get_data_column_ptr();
            }
        }

        const auto* mean_col = assert_cast<const ColumnFloat64*>(argument_columns[0].get());
        const auto* sd_col = assert_cast<const ColumnFloat64*>(argument_columns[1].get());
        const auto* value_col = assert_cast<const ColumnFloat64*>(argument_columns[2].get());

        result_column->reserve(input_rows_count);
        for (size_t i = 0; i < input_rows_count; ++i) {
            double mean = mean_col->get_element(index_check_const(i, col_const[0]));
            double sd = sd_col->get_element(index_check_const(i, col_const[1]));
            double v = value_col->get_element(index_check_const(i, col_const[2]));

            if (!check_argument(sd)) [[unlikely]] {
                result_null_map[i] = true;
                continue;
            }
            result_data[i] = calculate_cell(mean, sd, v);
        }

        block.get_by_position(result).column =
                ColumnNullable::create(std::move(result_column), std::move(result_null_map_column));
        return Status::OK();
    }

    static bool check_argument(double sd) { return sd > 0; }
    static double calculate_cell(double mean, double sd, double v) {
#ifdef __APPLE__
        const double sqrt2 = std::sqrt(2);
#else
        constexpr double sqrt2 = std::numbers::sqrt2;
#endif

        return 0.5 * (std::erf((v - mean) / (sd * sqrt2)) + 1);
    }
};

template <typename A>
struct SignBitImpl {
    static constexpr PrimitiveType ResultType = TYPE_BOOLEAN;

    static inline bool apply(A a) { return std::signbit(static_cast<Float64>(a)); }
};

struct NameSignBit {
    static constexpr auto name = "signbit";
};

using FunctionSignBit = FunctionUnaryArithmetic<SignBitImpl, NameSignBit>;

double EvenImpl(double a) {
    double mag = std::abs(a);
    double even_mag = 2 * std::ceil(mag / 2);
    return std::copysign(even_mag, a);
}

struct NameEven {
    static constexpr auto name = "even";
};

using FunctionEven = FunctionMathUnary<UnaryFunctionPlain<NameEven, EvenImpl>>;

template <PrimitiveType A>
struct GcdImpl {
    using cpp_type = typename PrimitiveTypeTraits<A>::CppType;

    static constexpr PrimitiveType type = A;
    static constexpr auto name = "gcd";
    static constexpr bool need_replace_null_data_to_default = false;
    static constexpr bool is_nullable = false;

    static DataTypes get_variadic_argument_types() {
        using datatype = typename PrimitiveTypeTraits<A>::DataType;
        return {std::make_shared<datatype>(), std::make_shared<datatype>()};
    }

    static inline cpp_type apply(cpp_type a, cpp_type b) {
        return static_cast<cpp_type>(std::gcd(a, b));
    }
};

template <PrimitiveType A>
struct LcmImpl {
    using cpp_type = typename PrimitiveTypeTraits<A>::CppType;

    static constexpr PrimitiveType type = A;
    static constexpr auto name = "lcm";
    static constexpr bool need_replace_null_data_to_default = false;
    static constexpr bool is_nullable = false;

    static DataTypes get_variadic_argument_types() {
        using datatype = typename PrimitiveTypeTraits<A>::DataType;
        return {std::make_shared<datatype>(), std::make_shared<datatype>()};
    }

    static inline cpp_type apply(cpp_type a, cpp_type b) {
        return static_cast<cpp_type>(std::lcm(a, b));
    }
};

enum class FloatPointNumberJudgmentType {
    IsNan = 0,
    IsInf,
};

struct ImplIsNan {
    static constexpr auto name = "isnan";
    static constexpr FloatPointNumberJudgmentType type = FloatPointNumberJudgmentType::IsNan;
};

struct ImplIsInf {
    static constexpr auto name = "isinf";
    static constexpr FloatPointNumberJudgmentType type = FloatPointNumberJudgmentType::IsInf;
};

template <typename Impl>
class FunctionFloatingPointNumberJudgment : public IFunction {
public:
    using IFunction::execute;

    static constexpr auto name = Impl::name;
    static FunctionPtr create() { return std::make_shared<FunctionFloatingPointNumberJudgment>(); }

private:
    String get_name() const override { return name; }
    size_t get_number_of_arguments() const override { return 1; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return std::make_shared<DataTypeBool>();
    }

    void execute_impl_with_type(const auto* input_column,
                                DataTypeBool::ColumnType::Container& output, size_t size) const {
        for (int i = 0; i < size; i++) {
            auto value = input_column->get_element(i);
            if constexpr (Impl::type == FloatPointNumberJudgmentType::IsNan) {
                output[i] = std::isnan(value);
            } else if constexpr (Impl::type == FloatPointNumberJudgmentType::IsInf) {
                output[i] = std::isinf(value);
            }
        }
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        auto dst = DataTypeBool::ColumnType::create();
        auto& dst_data = dst->get_data();
        dst_data.resize(input_rows_count);
        const auto* column = block.get_by_position(arguments[0]).column.get();
        if (const auto* col_f64 = check_and_get_column<ColumnFloat64>(column)) {
            execute_impl_with_type(col_f64, dst_data, input_rows_count);
        } else if (const auto* col_f32 = check_and_get_column<ColumnFloat32>(column)) {
            execute_impl_with_type(col_f32, dst_data, input_rows_count);
        } else {
            return Status::InvalidArgument("Unsupported column type  {} for function {}",
                                           column->get_name(), get_name());
        }
        block.replace_by_position(result, std::move(dst));
        return Status::OK();
    }
};

using FunctionIsNan = FunctionFloatingPointNumberJudgment<ImplIsNan>;
using FunctionIsInf = FunctionFloatingPointNumberJudgment<ImplIsInf>;

void register_function_math(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionAcos>();
    factory.register_function<FunctionAcosh>();
    factory.register_function<FunctionAsin>();
    factory.register_function<FunctionAsinh>();
    factory.register_function<FunctionAtan>();
    factory.register_function<FunctionAtanh>();
    factory.register_function<FunctionMathBinary<LogImpl>>();
    factory.register_function<FunctionMathBinary<PowImpl>>();
    factory.register_function<FunctionMathBinary<Atan2Impl>>();
    factory.register_function<FunctionCos>();
    factory.register_function<FunctionCosh>();
    factory.register_function<FunctionE>();
    factory.register_alias("ln", "dlog1");
    factory.register_function<FunctionMathLog<ImplLn>>();
    factory.register_function<FunctionMathLog<ImplLog2>>();
    factory.register_function<FunctionMathLog<ImplLog10>>();
    factory.register_alias("log10", "dlog10");
    factory.register_function<FunctionPi>();
    factory.register_function<FunctionSign>();
    factory.register_function<FunctionAbs>();
    factory.register_function<FunctionNegative>();
    factory.register_function<FunctionPositive>();
    factory.register_function<FunctionSin>();
    factory.register_function<FunctionSinh>();
    factory.register_function<FunctionSqrt>();
    factory.register_alias("sqrt", "dsqrt");
    factory.register_function<FunctionCbrt>();
    factory.register_function<FunctionTan>();
    factory.register_function<FunctionTanh>();
    factory.register_function<FunctionCot>();
    factory.register_function<FunctionSec>();
    factory.register_function<FunctionCosec>();
    factory.register_alias("pow", "power");
    factory.register_alias("pow", "dpow");
    factory.register_alias("pow", "fpow");
    factory.register_function<FunctionExp>();
    factory.register_alias("exp", "dexp");
    factory.register_function<FunctionRadians>();
    factory.register_function<FunctionDegrees>();
    factory.register_function<FunctionBin>();
    factory.register_function<FunctionNormalCdf>();
    factory.register_function<FunctionSignBit>();
    factory.register_function<FunctionEven>();
    factory.register_function<FunctionMathBinary<GcdImpl<TYPE_TINYINT>>>();
    factory.register_function<FunctionMathBinary<GcdImpl<TYPE_SMALLINT>>>();
    factory.register_function<FunctionMathBinary<GcdImpl<TYPE_INT>>>();
    factory.register_function<FunctionMathBinary<GcdImpl<TYPE_BIGINT>>>();
    factory.register_function<FunctionMathBinary<GcdImpl<TYPE_LARGEINT>>>();
    factory.register_function<FunctionMathBinary<LcmImpl<TYPE_SMALLINT>>>();
    factory.register_function<FunctionMathBinary<LcmImpl<TYPE_INT>>>();
    factory.register_function<FunctionMathBinary<LcmImpl<TYPE_BIGINT>>>();
    factory.register_function<FunctionMathBinary<LcmImpl<TYPE_LARGEINT>>>();
    factory.register_function<FunctionIsNan>();
    factory.register_function<FunctionIsInf>();
}
} // namespace doris::vectorized
