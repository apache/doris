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

#pragma once

#include <fmt/format.h>

#include <cmath>
#include <cstddef>
#include <cstring>
#include <string>
#include <type_traits>

#include "common/status.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/block/column_numbers.h"
#include "core/column/column_const.h"
#include "core/column/column_decimal.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_decimal.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/define_primitive_type.h"
#include "core/data_type/primitive_type.h"
#include "core/string_ref.h"
#include "core/types.h"
#include "core/value/decimalv2_value.h"
#include "exec/common/int_exp.h"
#include "exec/common/stringop_substring.h"
#include "exec/common/template_helpers.hpp"
#include "exprs/function/function.h"
#include "exprs/function/function_helpers.h"
#include "exprs/function_context.h"
#include "exprs/math_functions.h"

namespace doris {
#include "common/compile_check_avoid_begin.h"

// ----------------------------------------------------------------------
// SimpleItoaWithCommas()
//    Description: converts an integer to a string.
//    Puts commas every 3 spaces.
//    Faster than printf("%d")?
//
//    Return value: string
// ----------------------------------------------------------------------
template <typename T>
char* SimpleItoaWithCommas(T i, char* buffer, int32_t buffer_size) {
    char* p = buffer + buffer_size;
    // Need to use unsigned T instead of T to correctly handle
    std::make_unsigned_t<T> n = i;
    if (i < 0) {
        n = 0 - n;
    }
    *--p = '0' + n % 10; // this case deals with the number "0"
    n /= 10;
    while (n) {
        *--p = '0' + n % 10;
        n /= 10;
        if (n == 0) {
            break;
        }

        *--p = '0' + n % 10;
        n /= 10;
        if (n == 0) {
            break;
        }

        *--p = ',';
        *--p = '0' + n % 10;
        n /= 10;
        // For this unrolling, we check if n == 0 in the main while loop
    }
    if (i < 0) {
        *--p = '-';
    }
    return p;
}

namespace MoneyFormat {

constexpr size_t MAX_FORMAT_LEN_DEC32() {
    // Decimal(9, 0)
    // Double the size to avoid some unexpected bug.
    return 2 * (1 + 9 + (9 / 3) + 3);
}

constexpr size_t MAX_FORMAT_LEN_DEC64() {
    // Decimal(18, 0)
    // Double the size to avoid some unexpected bug.
    return 2 * (1 + 18 + (18 / 3) + 3);
}

constexpr size_t MAX_FORMAT_LEN_DEC128V2() {
    // DecimalV2 has at most 27 digits
    // Double the size to avoid some unexpected bug.
    return 2 * (1 + 27 + (27 / 3) + 3);
}

constexpr size_t MAX_FORMAT_LEN_DEC128V3() {
    // Decimal(38, 0)
    // Double the size to avoid some unexpected bug.
    return 2 * (1 + 39 + (39 / 3) + 3);
}

constexpr size_t MAX_FORMAT_LEN_INT64() {
    // INT_MIN = -9223372036854775807
    // Double the size to avoid some unexpected bug.
    return 2 * (1 + 20 + (20 / 3) + 3);
}

constexpr size_t MAX_FORMAT_LEN_INT128() {
    // INT128_MIN = -170141183460469231731687303715884105728
    return 2 * (1 + 39 + (39 / 3) + 3);
}

template <typename T, size_t N>
StringRef do_money_format(FunctionContext* context, UInt32 scale, T int_value, T frac_value) {
    static_assert(std::is_integral<T>::value);
    const bool is_negative = int_value < 0 || frac_value < 0;

    // do round to frac_part
    // magic number 2: since we need to round frac_part to 2 digits
    if (scale > 2) {
        DCHECK(scale <= 38);
        // do rounding, so we need to reserve 3 digits.
        auto multiplier = common::exp10_i128(std::abs(static_cast<int>(scale - 3)));
        // do devide first to avoid overflow
        // after round frac_value will be positive by design.
        frac_value = std::abs(static_cast<int>(frac_value / multiplier)) + 5;
        frac_value /= 10;
    } else if (scale < 2) {
        DCHECK(frac_value < 100);
        // since scale <= 2, overflow is impossiable
        frac_value = frac_value * common::exp10_i32(2 - scale);
    }

    if (frac_value == 100) {
        if (is_negative) {
            int_value -= 1;
        } else {
            int_value += 1;
        }
        frac_value = 0;
    }

    bool append_sign_manually = false;
    if (is_negative && int_value == 0) {
        // when int_value is 0, result of SimpleItoaWithCommas will contains just zero
        // for Decimal like -0.1234, this will leads to problem, because negative sign is discarded.
        // this is why we introduce argument append_sing_manually.
        append_sign_manually = true;
    }

    char local[N];
    char* p = SimpleItoaWithCommas<T>(int_value, local, sizeof(local));
    const Int32 integer_str_len = N - (p - local);
    const Int32 frac_str_len = 2;
    const Int32 whole_decimal_str_len =
            (append_sign_manually ? 1 : 0) + integer_str_len + 1 + frac_str_len;

    StringRef result = context->create_temp_string_val(whole_decimal_str_len);
    // Modify a string passed via stringref
    char* result_data = const_cast<char*>(result.data);

    if (append_sign_manually) {
        memset(result_data, '-', 1);
    }

    memcpy(result_data + (append_sign_manually ? 1 : 0), p, integer_str_len);
    *(result_data + whole_decimal_str_len - 3) = '.';
    *(result_data + whole_decimal_str_len - 2) = '0' + std::abs(static_cast<int>(frac_value / 10));
    *(result_data + whole_decimal_str_len - 1) = '0' + std::abs(static_cast<int>(frac_value % 10));
    return result;
};

// Note string value must be valid decimal string which contains two digits after the decimal point
static StringRef do_money_format(FunctionContext* context, const std::string& value) {
    bool is_positive = (value[0] != '-');
    int32_t result_len = value.size() + (value.size() - (is_positive ? 4 : 5)) / 3;
    StringRef result = context->create_temp_string_val(result_len);
    // Modify a string passed via stringref
    char* result_data = const_cast<char*>(result.data);
    if (!is_positive) {
        *result_data = '-';
    }
    for (int i = value.size() - 4, j = result_len - 4; i >= 0; i = i - 3) {
        *(result_data + j) = *(value.data() + i);
        if (i - 1 < 0) {
            break;
        }
        *(result_data + j - 1) = *(value.data() + i - 1);
        if (i - 2 < 0) {
            break;
        }
        *(result_data + j - 2) = *(value.data() + i - 2);
        if (j - 3 > 1 || (j - 3 == 1 && is_positive)) {
            *(result_data + j - 3) = ',';
            j -= 4;
        } else {
            j -= 3;
        }
    }
    memcpy(result_data + result_len - 3, value.data() + value.size() - 3, 3);
    return result;
};

} // namespace MoneyFormat

namespace FormatRound {

constexpr size_t MAX_FORMAT_LEN_DEC32() {
    // Decimal(9, 0)
    // Double the size to avoid some unexpected bug.
    return 2 * (1 + 9 + (9 / 3) + 3);
}

constexpr size_t MAX_FORMAT_LEN_DEC64() {
    // Decimal(18, 0)
    // Double the size to avoid some unexpected bug.
    return 2 * (1 + 18 + (18 / 3) + 3);
}

constexpr size_t MAX_FORMAT_LEN_DEC128V2() {
    // DecimalV2 has at most 27 digits
    // Double the size to avoid some unexpected bug.
    return 2 * (1 + 27 + (27 / 3) + 3);
}

constexpr size_t MAX_FORMAT_LEN_DEC128V3() {
    // Decimal(38, 0)
    // Double the size to avoid some unexpected bug.
    return 2 * (1 + 39 + (39 / 3) + 3);
}

constexpr size_t MAX_FORMAT_LEN_INT64() {
    // INT_MIN = -9223372036854775807
    // Double the size to avoid some unexpected bug.
    return 2 * (1 + 20 + (20 / 3) + 3);
}

constexpr size_t MAX_FORMAT_LEN_INT128() {
    // INT128_MIN = -170141183460469231731687303715884105728
    return 2 * (1 + 39 + (39 / 3) + 3);
}

template <typename T, size_t N>
StringRef do_format_round(FunctionContext* context, UInt32 scale, T int_value, T frac_value,
                          Int32 decimal_places) {
    static_assert(std::is_integral<T>::value);
    const bool is_negative = int_value < 0 || frac_value < 0;

    // do round to frac_part based on decimal_places
    if (scale > decimal_places && decimal_places > 0) {
        DCHECK(scale <= 38);
        // do rounding, so we need to reserve decimal_places + 1 digits
        auto multiplier =
                common::exp10_i128(std::abs(static_cast<int>(scale - (decimal_places + 1))));
        // do divide first to avoid overflow
        // after round frac_value will be positive by design
        frac_value = std::abs(static_cast<int>(frac_value / multiplier)) + 5;
        frac_value /= 10;
    } else if (scale < decimal_places && decimal_places > 0) {
        // since scale <= decimal_places, overflow is impossible
        frac_value = frac_value * common::exp10_i32(decimal_places - scale);
    }

    // Calculate power of 10 for decimal_places
    T decimal_power = common::exp10_i32(decimal_places);
    if (frac_value == decimal_power) {
        if (is_negative) {
            int_value -= 1;
        } else {
            int_value += 1;
        }
        frac_value = 0;
    }

    bool append_sign_manually = false;
    if (is_negative && int_value == 0) {
        append_sign_manually = true;
    }

    char local[N];
    char* p = SimpleItoaWithCommas<T>(int_value, local, sizeof(local));
    const Int32 integer_str_len = N - (p - local);
    const Int32 frac_str_len = decimal_places;
    const Int32 whole_decimal_str_len = (append_sign_manually ? 1 : 0) + integer_str_len +
                                        (decimal_places > 0 ? 1 : 0) + frac_str_len;

    StringRef result = context->create_temp_string_val(whole_decimal_str_len);
    // Modify a string passed via stringref
    char* result_data = const_cast<char*>(result.data);

    if (append_sign_manually) {
        memset(result_data, '-', 1);
    }

    memcpy(result_data + (append_sign_manually ? 1 : 0), p, integer_str_len);
    if (decimal_places > 0) {
        *(result_data + whole_decimal_str_len - (frac_str_len + 1)) = '.';
    }

    // Convert fractional part to string with proper padding
    T remaining_frac = std::abs(static_cast<int>(frac_value));
    for (int i = 0; i <= decimal_places - 1; ++i) {
        *(result_data + whole_decimal_str_len - 1 - i) = '0' + (remaining_frac % 10);
        remaining_frac /= 10;
    }
    return result;
}

} // namespace FormatRound

template <typename Impl>
class FunctionMoneyFormat : public IFunction {
public:
    static constexpr auto name = "money_format";
    static FunctionPtr create() { return std::make_shared<FunctionMoneyFormat>(); }
    String get_name() const override { return name; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        if (arguments.size() != 1) {
            throw doris::Exception(ErrorCode::INVALID_ARGUMENT,
                                   "Function {} requires exactly 1 argument", name);
        }

        return std::make_shared<DataTypeString>();
    }
    DataTypes get_variadic_argument_types_impl() const override {
        return Impl::get_variadic_argument_types();
    }
    size_t get_number_of_arguments() const override { return 1; }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        auto res_column = ColumnString::create();
        ColumnPtr argument_column = block.get_by_position(arguments[0]).column;

        auto result_column = assert_cast<ColumnString*>(res_column.get());

        Impl::execute(context, result_column, argument_column, input_rows_count);

        block.replace_by_position(result, std::move(res_column));
        return Status::OK();
    }
};

struct MoneyFormatDoubleImpl {
    static DataTypes get_variadic_argument_types() { return {std::make_shared<DataTypeFloat64>()}; }

    static void execute(FunctionContext* context, ColumnString* result_column,
                        const ColumnPtr col_ptr, size_t input_rows_count) {
        const auto* data_column = assert_cast<const ColumnFloat64*>(col_ptr.get());
        // when scale is above 38, we will go here
        for (size_t i = 0; i < input_rows_count; i++) {
            // round to 2 decimal places
            double value =
                    MathFunctions::my_double_round(data_column->get_element(i), 2, false, false);
            StringRef str = MoneyFormat::do_money_format(context, fmt::format("{:.2f}", value));
            result_column->insert_data(str.data, str.size);
        }
    }
};

struct MoneyFormatInt64Impl {
    static DataTypes get_variadic_argument_types() { return {std::make_shared<DataTypeInt64>()}; }

    static void execute(FunctionContext* context, ColumnString* result_column,
                        const ColumnPtr col_ptr, size_t input_rows_count) {
        const auto* data_column = assert_cast<const ColumnInt64*>(col_ptr.get());
        for (size_t i = 0; i < input_rows_count; i++) {
            Int64 value = data_column->get_element(i);
            StringRef str =
                    MoneyFormat::do_money_format<Int64, MoneyFormat::MAX_FORMAT_LEN_INT64()>(
                            context, 0, value, 0);
            result_column->insert_data(str.data, str.size);
        }
    }
};

struct MoneyFormatInt128Impl {
    static DataTypes get_variadic_argument_types() { return {std::make_shared<DataTypeInt128>()}; }

    static void execute(FunctionContext* context, ColumnString* result_column,
                        const ColumnPtr col_ptr, size_t input_rows_count) {
        const auto* data_column = assert_cast<const ColumnInt128*>(col_ptr.get());
        // SELECT money_format(170141183460469231731687303715884105728/*INT128_MAX + 1*/) will
        // get "170,141,183,460,469,231,731,687,303,715,884,105,727.00" in doris,
        // see https://github.com/apache/doris/blob/788abf2d7c3c7c2d57487a9608e889e7662d5fb2/be/src/vec/data_types/data_type_number_base.cpp#L124
        for (size_t i = 0; i < input_rows_count; i++) {
            Int128 value = data_column->get_element(i);
            StringRef str =
                    MoneyFormat::do_money_format<Int128, MoneyFormat::MAX_FORMAT_LEN_INT128()>(
                            context, 0, value, 0);
            result_column->insert_data(str.data, str.size);
        }
    }
};

template <PrimitiveType Type>
struct MoneyFormatDecimalImpl {
    static DataTypes get_variadic_argument_types() {
        return {std::make_shared<typename PrimitiveTypeTraits<Type>::DataType>()};
    }

    static void execute(FunctionContext* context, ColumnString* result_column, ColumnPtr col_ptr,
                        size_t input_rows_count) {
        if (auto* decimalv2_column = check_and_get_column<ColumnDecimal128V2>(*col_ptr)) {
            for (size_t i = 0; i < input_rows_count; i++) {
                const auto& value = decimalv2_column->get_element(i);
                // unified_frac_value has 3 digits
                auto unified_frac_value = value.frac_value() / 1000000;
                StringRef str =
                        MoneyFormat::do_money_format<Int128,
                                                     MoneyFormat::MAX_FORMAT_LEN_DEC128V2()>(
                                context, 3, value.int_value(), unified_frac_value);

                result_column->insert_data(str.data, str.size);
            }
        } else if (auto* decimal32_column = check_and_get_column<ColumnDecimal32>(*col_ptr)) {
            const UInt32 scale = decimal32_column->get_scale();
            for (size_t i = 0; i < input_rows_count; i++) {
                const Int32& frac_part = decimal32_column->get_fractional_part(i);
                const Int32& whole_part = decimal32_column->get_intergral_part(i);
                StringRef str =
                        MoneyFormat::do_money_format<Int64, MoneyFormat::MAX_FORMAT_LEN_DEC32()>(
                                context, scale, static_cast<Int64>(whole_part),
                                static_cast<Int64>(frac_part));

                result_column->insert_data(str.data, str.size);
            }
        } else if (auto* decimal64_column = check_and_get_column<ColumnDecimal64>(*col_ptr)) {
            const UInt32 scale = decimal64_column->get_scale();
            for (size_t i = 0; i < input_rows_count; i++) {
                const Int64& frac_part = decimal64_column->get_fractional_part(i);
                const Int64& whole_part = decimal64_column->get_intergral_part(i);

                StringRef str =
                        MoneyFormat::do_money_format<Int64, MoneyFormat::MAX_FORMAT_LEN_DEC64()>(
                                context, scale, whole_part, frac_part);

                result_column->insert_data(str.data, str.size);
            }
        } else if (auto* decimal128_column = check_and_get_column<ColumnDecimal128V3>(*col_ptr)) {
            const UInt32 scale = decimal128_column->get_scale();
            for (size_t i = 0; i < input_rows_count; i++) {
                const Int128& frac_part = decimal128_column->get_fractional_part(i);
                const Int128& whole_part = decimal128_column->get_intergral_part(i);

                StringRef str =
                        MoneyFormat::do_money_format<Int128,
                                                     MoneyFormat::MAX_FORMAT_LEN_DEC128V3()>(
                                context, scale, whole_part, frac_part);

                result_column->insert_data(str.data, str.size);
            }
        } else {
            throw doris::Exception(ErrorCode::INVALID_ARGUMENT,
                                   "Not supported input argument type {}", col_ptr->get_name());
        }
        // TODO: decimal256
        /* else if (auto* decimal256_column =
                           check_and_get_column<ColumnDecimal<Decimal256>>(*col_ptr)) {
            const UInt32 scale = decimal256_column->get_scale();
            const auto multiplier =
                    scale > 2 ? common::exp10_i32(scale - 2) : common::exp10_i32(2 - scale);
            for (size_t i = 0; i < input_rows_count; i++) {
                Decimal256 frac_part = decimal256_column->get_fractional_part(i);
                if (scale > 2) {
                    int delta = ((frac_part % multiplier) << 1) > multiplier;
                    frac_part = Decimal256(frac_part / multiplier + delta);
                } else if (scale < 2) {
                    frac_part = Decimal256(frac_part * multiplier);
                }

                StringRef str = MoneyFormat::do_money_format<int64_t, 26>(
                        context, decimal256_column->get_intergral_part(i), frac_part);

                result_column->insert_data(str.data, str.size);
            }
        }*/
    }
};

template <typename Impl>
class FunctionStringFormatRound : public IFunction {
public:
    static constexpr auto name = "format_round";
    static FunctionPtr create() { return std::make_shared<FunctionStringFormatRound>(); }
    String get_name() const override { return name; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        if (arguments.size() != 2) {
            throw doris::Exception(ErrorCode::INVALID_ARGUMENT,
                                   "Function {} requires exactly 2 argument", name);
        }
        return std::make_shared<DataTypeString>();
    }
    DataTypes get_variadic_argument_types_impl() const override {
        return Impl::get_variadic_argument_types();
    }
    size_t get_number_of_arguments() const override { return 2; }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        auto res_column = ColumnString::create();
        ColumnPtr argument_column =
                block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
        ColumnPtr argument_column_2;
        bool is_const;
        std::tie(argument_column_2, is_const) =
                unpack_if_const(block.get_by_position(arguments[1]).column);
        auto* result_column = assert_cast<ColumnString*>(res_column.get());

        if (is_const) {
            RETURN_IF_ERROR(Impl::template execute<true>(context, result_column, argument_column,
                                                         argument_column_2, input_rows_count));
        } else {
            RETURN_IF_ERROR(Impl::template execute<false>(context, result_column, argument_column,
                                                          argument_column_2, input_rows_count));
        }

        block.replace_by_position(result, std::move(res_column));
        return Status::OK();
    }
};

struct FormatRoundDoubleImpl {
    static DataTypes get_variadic_argument_types() {
        return {std::make_shared<DataTypeFloat64>(), std::make_shared<DataTypeInt32>()};
    }

    static std::string add_thousands_separator(const std::string& formatted_num) {
        //  Find the position of the decimal point
        size_t dot_pos = formatted_num.find('.');
        if (dot_pos == std::string::npos) {
            dot_pos = formatted_num.size();
        }

        // Handle the integer part
        int start = (formatted_num[0] == '-') ? 1 : 0;
        int digit_count = dot_pos - start;

        // There is no need to add commas.
        if (digit_count <= 3) {
            return formatted_num;
        }

        std::string result;

        if (start == 1) result += '-';

        // Add the integer part (with comma)
        int first_group = digit_count % 3;
        if (first_group == 0) first_group = 3;
        result.append(formatted_num, start, first_group);

        for (size_t i = start + first_group; i < dot_pos; i += 3) {
            result += ',';
            result.append(formatted_num, i, 3);
        }

        // Add the decimal part (keep as it is)
        if (dot_pos != formatted_num.size()) {
            result.append(formatted_num, dot_pos);
        }

        return result;
    }

    template <bool is_const>
    static Status execute(FunctionContext* context, ColumnString* result_column,
                          const ColumnPtr col_ptr, ColumnPtr decimal_places_col_ptr,
                          size_t input_rows_count) {
        const auto& arg_column_data_2 =
                assert_cast<const ColumnInt32*>(decimal_places_col_ptr.get())->get_data();
        const auto* data_column = assert_cast<const ColumnFloat64*>(col_ptr.get());
        // when scale is above 38, we will go here
        for (size_t i = 0; i < input_rows_count; i++) {
            int32_t decimal_places = arg_column_data_2[index_check_const<is_const>(i)];
            if (decimal_places < 0 || decimal_places > 1024) {
                return Status::InvalidArgument(
                        "The second argument is {}, it should be in range [0, 1024].",
                        decimal_places);
            }
            // round to `decimal_places` decimal places
            double value = MathFunctions::my_double_round(data_column->get_element(i),
                                                          decimal_places, false, false);
            std::string formatted_value = fmt::format("{:.{}f}", value, decimal_places);
            if (std::isfinite(value)) {
                result_column->insert_value(add_thousands_separator(formatted_value));
            } else {
                // if value is not finite, we just insert the original formatted value
                // e.g. "inf", "-inf", "nan"
                result_column->insert_value(formatted_value);
            }
        }
        return Status::OK();
    }
};

struct FormatRoundInt64Impl {
    static DataTypes get_variadic_argument_types() {
        return {std::make_shared<DataTypeInt64>(), std::make_shared<DataTypeInt32>()};
    }

    template <bool is_const>
    static Status execute(FunctionContext* context, ColumnString* result_column,
                          const ColumnPtr col_ptr, ColumnPtr decimal_places_col_ptr,
                          size_t input_rows_count) {
        const auto* data_column = assert_cast<const ColumnInt64*>(col_ptr.get());
        const auto& arg_column_data_2 =
                assert_cast<const ColumnInt32*>(decimal_places_col_ptr.get())->get_data();
        for (size_t i = 0; i < input_rows_count; i++) {
            int32_t decimal_places = arg_column_data_2[index_check_const<is_const>(i)];
            if (decimal_places < 0 || decimal_places > 1024) {
                return Status::InvalidArgument(
                        "The second argument is {}, it should be in range [0, 1024].",
                        decimal_places);
            }
            Int64 value = data_column->get_element(i);
            StringRef str =
                    FormatRound::do_format_round<Int64, FormatRound::MAX_FORMAT_LEN_INT64()>(
                            context, 0, value, 0, decimal_places);
            result_column->insert_data(str.data, str.size);
        }
        return Status::OK();
    }
};

struct FormatRoundInt128Impl {
    static DataTypes get_variadic_argument_types() {
        return {std::make_shared<DataTypeInt128>(), std::make_shared<DataTypeInt32>()};
    }

    template <bool is_const>
    static Status execute(FunctionContext* context, ColumnString* result_column,
                          const ColumnPtr col_ptr, ColumnPtr decimal_places_col_ptr,
                          size_t input_rows_count) {
        const auto* data_column = assert_cast<const ColumnInt128*>(col_ptr.get());
        const auto& arg_column_data_2 =
                assert_cast<const ColumnInt32*>(decimal_places_col_ptr.get())->get_data();
        // SELECT money_format(170141183460469231731687303715884105728/*INT128_MAX + 1*/) will
        // get "170,141,183,460,469,231,731,687,303,715,884,105,727.00" in doris,
        // see https://github.com/apache/doris/blob/788abf2d7c3c7c2d57487a9608e889e7662d5fb2/be/src/vec/data_types/data_type_number_base.cpp#L124
        for (size_t i = 0; i < input_rows_count; i++) {
            int32_t decimal_places = arg_column_data_2[index_check_const<is_const>(i)];
            if (decimal_places < 0 || decimal_places > 1024) {
                return Status::InvalidArgument(
                        "The second argument is {}, it should be in range [0, 1024].",
                        decimal_places);
            }
            Int128 value = data_column->get_element(i);
            StringRef str =
                    FormatRound::do_format_round<Int128, FormatRound::MAX_FORMAT_LEN_INT128()>(
                            context, 0, value, 0, decimal_places);
            result_column->insert_data(str.data, str.size);
        }
        return Status::OK();
    }
};

template <PrimitiveType Type>
struct FormatRoundDecimalImpl {
    static DataTypes get_variadic_argument_types() {
        return {std::make_shared<typename PrimitiveTypeTraits<Type>::DataType>(),
                std::make_shared<DataTypeInt32>()};
    }

    template <bool is_const>
    static Status execute(FunctionContext* context, ColumnString* result_column, ColumnPtr col_ptr,
                          ColumnPtr decimal_places_col_ptr, size_t input_rows_count) {
        const auto& arg_column_data_2 =
                assert_cast<const ColumnInt32*>(decimal_places_col_ptr.get())->get_data();
        if (const auto* decimalv2_column = check_and_get_column<ColumnDecimal128V2>(*col_ptr)) {
            for (size_t i = 0; i < input_rows_count; i++) {
                int32_t decimal_places = arg_column_data_2[index_check_const<is_const>(i)];
                if (decimal_places < 0 || decimal_places > 1024) {
                    return Status::InvalidArgument(
                            "The second argument is {}, it should be in range [0, 1024].",
                            decimal_places);
                }
                const auto& value = decimalv2_column->get_element(i);
                // unified_frac_value has 3 digits
                auto unified_frac_value = value.frac_value() / 1000000;
                StringRef str =
                        FormatRound::do_format_round<Int128,
                                                     FormatRound::MAX_FORMAT_LEN_DEC128V2()>(
                                context, 3, value.int_value(), unified_frac_value, decimal_places);

                result_column->insert_data(str.data, str.size);
            }
        } else if (const auto* decimal32_column = check_and_get_column<ColumnDecimal32>(*col_ptr)) {
            const UInt32 scale = decimal32_column->get_scale();
            for (size_t i = 0; i < input_rows_count; i++) {
                int32_t decimal_places = arg_column_data_2[index_check_const<is_const>(i)];
                if (decimal_places < 0 || decimal_places > 1024) {
                    return Status::InvalidArgument(
                            "The second argument is {}, it should be in range [0, 1024].",
                            decimal_places);
                }
                const Int32& frac_part = decimal32_column->get_fractional_part(i);
                const Int32& whole_part = decimal32_column->get_intergral_part(i);
                StringRef str =
                        FormatRound::do_format_round<Int64, FormatRound::MAX_FORMAT_LEN_DEC32()>(
                                context, scale, static_cast<Int64>(whole_part),
                                static_cast<Int64>(frac_part), decimal_places);

                result_column->insert_data(str.data, str.size);
            }
        } else if (const auto* decimal64_column = check_and_get_column<ColumnDecimal64>(*col_ptr)) {
            const UInt32 scale = decimal64_column->get_scale();
            for (size_t i = 0; i < input_rows_count; i++) {
                int32_t decimal_places = arg_column_data_2[index_check_const<is_const>(i)];
                if (decimal_places < 0 || decimal_places > 1024) {
                    return Status::InvalidArgument(
                            "The second argument is {}, it should be in range [0, 1024].",
                            decimal_places);
                }
                const Int64& frac_part = decimal64_column->get_fractional_part(i);
                const Int64& whole_part = decimal64_column->get_intergral_part(i);

                StringRef str =
                        FormatRound::do_format_round<Int64, FormatRound::MAX_FORMAT_LEN_DEC64()>(
                                context, scale, whole_part, frac_part, decimal_places);

                result_column->insert_data(str.data, str.size);
            }
        } else if (const auto* decimal128_column =
                           check_and_get_column<ColumnDecimal128V3>(*col_ptr)) {
            const UInt32 scale = decimal128_column->get_scale();
            for (size_t i = 0; i < input_rows_count; i++) {
                int32_t decimal_places = arg_column_data_2[index_check_const<is_const>(i)];
                if (decimal_places < 0 || decimal_places > 1024) {
                    return Status::InvalidArgument(
                            "The second argument is {}, it should be in range [0, 1024].",
                            decimal_places);
                }
                const Int128& frac_part = decimal128_column->get_fractional_part(i);
                const Int128& whole_part = decimal128_column->get_intergral_part(i);

                StringRef str =
                        FormatRound::do_format_round<Int128,
                                                     FormatRound::MAX_FORMAT_LEN_DEC128V3()>(
                                context, scale, whole_part, frac_part, decimal_places);

                result_column->insert_data(str.data, str.size);
            }
        } else {
            return Status::InternalError("Not supported input argument type {}",
                                         col_ptr->get_name());
        }
        return Status::OK();
    }
};

#include "common/compile_check_avoid_end.h"
} // namespace doris
