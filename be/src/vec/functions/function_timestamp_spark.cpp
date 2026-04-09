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

#include <cctz/time_zone.h>

#include <cmath>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>

#include "common/status.h"
#include "runtime/define_primitive_type.h"
#include "runtime/runtime_state.h"
#include "util/binary_cast.hpp"
#include "util/string_parser.hpp"
#include "util/timezone_utils.h"
#include "vec/columns/column_decimal.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
#include "vec/common/assert_cast.h"
#include "vec/common/int_exp.h"
#include "vec/core/block.h"
#include "vec/core/column_numbers.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_date_or_datetime_v2.h"
#include "vec/data_types/data_type_date_time.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_string.h"
#include "vec/exprs/function_context.h"
#include "vec/functions/function.h"
#include "vec/functions/simple_function_factory.h"
#include "vec/runtime/vdatetime_value.h"

namespace doris::vectorized {

#include "common/compile_check_begin.h"

/**
 * Spark-compatible timestamp function.
 *
 * Accepts string, integer, float, and decimal inputs:
 * - String: parsed with Spark-compatible rules (only '-' date separator,
 *   truncates fractional seconds beyond 6 digits, supports timezone offsets).
 * - Integer/Float/Double/Decimal: treated as Unix epoch seconds, converted
 *   to DateTimeV2 in the session timezone.
 *
 * Key differences from Doris's standard timestamp():
 * 1. Only accepts '-' as date separator (rejects '/', '_', '.')
 * 2. Truncates sub-microsecond fractional seconds (no rounding)
 * 3. Accepts string input directly (no implicit cast to DateTimeV2)
 * 4. Returns NULL for negative numeric values and invalid string formats
 */
class FunctionTimestampSpark : public IFunction {
public:
    static constexpr auto name = "timestamp_spark";

    static FunctionPtr create() { return std::make_shared<FunctionTimestampSpark>(); }

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 1; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return make_nullable(std::make_shared<DataTypeDateTimeV2>(static_cast<UInt32>(6)));
    }

    bool use_default_implementation_for_nulls() const override { return true; }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        const auto& arg_column = block.get_by_position(arguments[0]).column;
        const cctz::time_zone& local_tz = context->state()->timezone_obj();

        auto col_result = ColumnDateTimeV2::create(input_rows_count);
        auto null_map = ColumnUInt8::create(input_rows_count, 0);

        auto& result_data = col_result->get_data();
        auto& null_data = null_map->get_data();

        // Dispatch once based on the argument's PrimitiveType to avoid repeated dynamic casts.
        const auto& arg_type = block.get_by_position(arguments[0]).type;
        PrimitiveType ptype = remove_nullable(arg_type)->get_primitive_type();

        switch (ptype) {
        case TYPE_TINYINT:
            execute_int_type<ColumnInt8>(arg_column.get(), result_data, null_data, input_rows_count,
                                         local_tz);
            break;
        case TYPE_SMALLINT:
            execute_int_type<ColumnInt16>(arg_column.get(), result_data, null_data,
                                          input_rows_count, local_tz);
            break;
        case TYPE_INT:
            execute_int_type<ColumnInt32>(arg_column.get(), result_data, null_data,
                                          input_rows_count, local_tz);
            break;
        case TYPE_BIGINT:
            execute_int_type<ColumnInt64>(arg_column.get(), result_data, null_data,
                                          input_rows_count, local_tz);
            break;
        case TYPE_FLOAT:
            execute_float_type<ColumnFloat32>(arg_column.get(), result_data, null_data,
                                              input_rows_count, local_tz);
            break;
        case TYPE_DOUBLE:
            execute_float_type<ColumnFloat64>(arg_column.get(), result_data, null_data,
                                              input_rows_count, local_tz);
            break;
        case TYPE_DECIMAL32:
            execute_decimal_type<ColumnDecimal32>(arg_column.get(), result_data, null_data,
                                                  input_rows_count, local_tz);
            break;
        case TYPE_DECIMAL64:
            execute_decimal_type<ColumnDecimal64>(arg_column.get(), result_data, null_data,
                                                  input_rows_count, local_tz);
            break;
        case TYPE_DECIMAL128I:
            execute_decimal_type<ColumnDecimal128V3>(arg_column.get(), result_data, null_data,
                                                     input_rows_count, local_tz);
            break;
        case TYPE_VARCHAR:
        case TYPE_CHAR:
        case TYPE_STRING:
            execute_string_type(arg_column.get(), result_data, null_data, input_rows_count,
                                local_tz);
            break;
        default:
            return Status::InvalidArgument(
                    "timestamp_spark requires string, integer, float, or decimal input");
        }

        block.get_by_position(result).column =
                ColumnNullable::create(std::move(col_result), std::move(null_map));
        return Status::OK();
    }

private:
    /** Convert integer values to DateTimeV2 by treating them as Unix epoch seconds. */
    template <typename IntColumnType>
    static void execute_int_type(const IColumn* column, ColumnDateTimeV2::Container& result_data,
                                 ColumnUInt8::Container& null_data, size_t input_rows_count,
                                 const cctz::time_zone& local_tz) {
        const auto& int_data = assert_cast<const IntColumnType&>(*column).get_data();
        for (size_t i = 0; i < input_rows_count; ++i) {
            int64_t val = static_cast<int64_t>(int_data[i]);
            if (val < 0) {
                null_data[i] = 1;
                result_data[i] = binary_cast<DateV2Value<DateTimeV2ValueType>, UInt64>(
                        DateV2Value<DateTimeV2ValueType>());
            } else {
                DateV2Value<DateTimeV2ValueType> dt_value;
                dt_value.from_unixtime(val, local_tz);
                if (!dt_value.is_valid_date()) {
                    null_data[i] = 1;
                    result_data[i] = binary_cast<DateV2Value<DateTimeV2ValueType>, UInt64>(
                            DateV2Value<DateTimeV2ValueType>());
                } else {
                    result_data[i] = dt_value.to_date_int_val();
                    null_data[i] = 0;
                }
            }
        }
    }

    /** Convert float/double values to DateTimeV2 by treating them as Unix epoch seconds
     *  with fractional part as microseconds. NaN, Inf, and negative values yield NULL. */
    template <typename FloatColumnType>
    static void execute_float_type(const IColumn* column, ColumnDateTimeV2::Container& result_data,
                                   ColumnUInt8::Container& null_data, size_t input_rows_count,
                                   const cctz::time_zone& local_tz) {
        const auto& float_data = assert_cast<const FloatColumnType&>(*column).get_data();
        for (size_t i = 0; i < input_rows_count; ++i) {
            double val = static_cast<double>(float_data[i]);
            if (val < 0 || std::isnan(val) || std::isinf(val)) {
                null_data[i] = 1;
                result_data[i] = binary_cast<DateV2Value<DateTimeV2ValueType>, UInt64>(
                        DateV2Value<DateTimeV2ValueType>());
            } else {
                int64_t seconds = static_cast<int64_t>(val);
                double frac = val - static_cast<double>(seconds);
                int32_t microseconds = static_cast<int32_t>(frac * 1000000);
                DateV2Value<DateTimeV2ValueType> dt_value;
                // Convert microseconds to nanoseconds for from_unixtime (scale=6)
                dt_value.from_unixtime(seconds,
                                       static_cast<int32_t>(microseconds * common::exp10_i32(3)),
                                       local_tz, 6);
                if (!dt_value.is_valid_date()) {
                    null_data[i] = 1;
                    result_data[i] = binary_cast<DateV2Value<DateTimeV2ValueType>, UInt64>(
                            DateV2Value<DateTimeV2ValueType>());
                } else {
                    result_data[i] = dt_value.to_date_int_val();
                    null_data[i] = 0;
                }
            }
        }
    }

    /** Convert decimal values to DateTimeV2 by splitting into integer (epoch seconds)
     *  and fractional (microseconds) parts. Fractional digits beyond 6 are truncated. */
    template <typename DecimalColumnType>
    static void execute_decimal_type(const IColumn* column,
                                     ColumnDateTimeV2::Container& result_data,
                                     ColumnUInt8::Container& null_data, size_t input_rows_count,
                                     const cctz::time_zone& local_tz) {
        const auto& col_decimal = assert_cast<const DecimalColumnType&>(*column);
        for (size_t i = 0; i < input_rows_count; ++i) {
            auto integer_part = col_decimal.get_intergral_part(i);
            auto fractional_part = col_decimal.get_fractional_part(i);
            int64_t seconds = static_cast<int64_t>(integer_part);
            if (seconds < 0) {
                null_data[i] = 1;
                result_data[i] = binary_cast<DateV2Value<DateTimeV2ValueType>, UInt64>(
                        DateV2Value<DateTimeV2ValueType>());
            } else {
                // Scale fractional part to microseconds, then to nanoseconds
                int32_t scale = static_cast<int32_t>(col_decimal.get_scale());
                int64_t frac = static_cast<int64_t>(fractional_part);
                // Normalize fraction to 6 digits (microseconds)
                if (scale < 6) {
                    frac *= common::exp10_i32(6 - scale);
                } else if (scale > 6) {
                    // Truncate (Spark-compatible, no rounding)
                    for (int s = scale; s > 6; --s) {
                        frac /= 10;
                    }
                }
                int32_t microseconds = static_cast<int32_t>(frac);
                DateV2Value<DateTimeV2ValueType> dt_value;
                // Convert microseconds to nanoseconds for from_unixtime (scale=6)
                dt_value.from_unixtime(seconds,
                                       static_cast<int32_t>(microseconds * common::exp10_i32(3)),
                                       local_tz, 6);
                if (!dt_value.is_valid_date()) {
                    null_data[i] = 1;
                    result_data[i] = binary_cast<DateV2Value<DateTimeV2ValueType>, UInt64>(
                            DateV2Value<DateTimeV2ValueType>());
                } else {
                    result_data[i] = dt_value.to_date_int_val();
                    null_data[i] = 0;
                }
            }
        }
    }

    /** Parse string values into DateTimeV2 using Spark-compatible rules.
     *  Supports timezone offsets; converts to session local timezone when present. */
    static void execute_string_type(const IColumn* column, ColumnDateTimeV2::Container& result_data,
                                    ColumnUInt8::Container& null_data, size_t input_rows_count,
                                    const cctz::time_zone& local_tz) {
        const auto& col_string = assert_cast<const ColumnString&>(*column);
        for (size_t i = 0; i < input_rows_count; ++i) {
            auto str_ref = col_string.get_data_at(i);
            std::string_view str(str_ref.data, str_ref.size);

            DateV2Value<DateTimeV2ValueType> dt_value;
            bool has_tz = false;
            cctz::time_zone parsed_tz;
            if (!parse_spark_timestamp(str, dt_value, has_tz, parsed_tz)) {
                null_data[i] = 1;
                result_data[i] = binary_cast<DateV2Value<DateTimeV2ValueType>, UInt64>(
                        DateV2Value<DateTimeV2ValueType>());
            } else {
                if (has_tz) {
                    // Convert from parsed timezone to session local timezone
                    cctz::civil_second cs(dt_value.year(), dt_value.month(), dt_value.day(),
                                          dt_value.hour(), dt_value.minute(), dt_value.second());
                    auto tp = cctz::convert(cs, parsed_tz);
                    auto local_cs = cctz::convert(tp, local_tz);
                    dt_value.unchecked_set_time_unit<TimeUnit::YEAR>(
                            static_cast<uint32_t>(local_cs.year()));
                    dt_value.unchecked_set_time_unit<TimeUnit::MONTH>(
                            static_cast<uint32_t>(local_cs.month()));
                    dt_value.unchecked_set_time_unit<TimeUnit::DAY>(
                            static_cast<uint32_t>(local_cs.day()));
                    dt_value.unchecked_set_time_unit<TimeUnit::HOUR>(
                            static_cast<uint32_t>(local_cs.hour()));
                    dt_value.unchecked_set_time_unit<TimeUnit::MINUTE>(
                            static_cast<uint32_t>(local_cs.minute()));
                    dt_value.unchecked_set_time_unit<TimeUnit::SECOND>(
                            static_cast<uint32_t>(local_cs.second()));
                }
                result_data[i] = dt_value.to_date_int_val();
                null_data[i] = 0;
            }
        }
    }

    /**
     * Parse a Spark-compatible timestamp string.
     *
     * Accepted formats:
     *   YYYY-MM-DD
     *   YYYY-MM-DD[T| ]HH:MM:SS[.ffffff][Z|+HH:MM|-HH:MM]
     *
     * Rules:
     * - Only '-' as date separator; only 'T' or space as datetime separator.
     * - Fractional seconds beyond 6 digits are truncated (not rounded).
     * - Leading/trailing whitespace is stripped; all other trailing content is rejected.
     * - Returns false (NULL) on any parse failure or invalid date/time values.
     */
    static bool parse_spark_timestamp(std::string_view str,
                                      DateV2Value<DateTimeV2ValueType>& result, bool& has_tz,
                                      cctz::time_zone& parsed_tz) {
        has_tz = false;

        if (str.empty()) {
            return false;
        }

        const char* ptr = str.data();
        const char* end = ptr + str.size();

        // Skip leading whitespace
        while (ptr < end && (*ptr == ' ' || *ptr == '\t')) {
            ++ptr;
        }

        if (ptr >= end) {
            return false;
        }

        // Parse year (4 digits required)
        uint32_t year = 0;
        if (!consume_digit<uint32_t, 4>(ptr, end, year)) {
            return false;
        }

        // Expect '-'
        if (ptr >= end || *ptr != '-') {
            return false;
        }
        ++ptr;

        // Parse month (2 digits required)
        uint32_t month = 0;
        if (!consume_digit<uint32_t, 2>(ptr, end, month)) {
            return false;
        }

        // Expect '-'
        if (ptr >= end || *ptr != '-') {
            return false;
        }
        ++ptr;

        // Parse day (2 digits required)
        uint32_t day = 0;
        if (!consume_digit<uint32_t, 2>(ptr, end, day)) {
            return false;
        }

        // Set date part
        if (!result.set_time_unit<TimeUnit::YEAR>(year) ||
            !result.set_time_unit<TimeUnit::MONTH>(month) ||
            !result.set_time_unit<TimeUnit::DAY>(day)) {
            return false;
        }

        // Check if there's a time part
        if (ptr >= end) {
            // Date only, set time to 00:00:00.000000
            result.unchecked_set_time_unit<TimeUnit::HOUR>(0);
            result.unchecked_set_time_unit<TimeUnit::MINUTE>(0);
            result.unchecked_set_time_unit<TimeUnit::SECOND>(0);
            result.unchecked_set_time_unit<TimeUnit::MICROSECOND>(0);
            return true;
        }

        // Expect 'T' or space as datetime separator
        if (*ptr != 'T' && *ptr != ' ') {
            return false;
        }
        ++ptr;

        if (ptr >= end) {
            return false;
        }

        // Parse hour (2 digits required)
        uint32_t hour = 0;
        if (!consume_digit<uint32_t, 2>(ptr, end, hour)) {
            return false;
        }

        // Expect ':'
        if (ptr >= end || *ptr != ':') {
            return false;
        }
        ++ptr;

        // Parse minute (2 digits required)
        uint32_t minute = 0;
        if (!consume_digit<uint32_t, 2>(ptr, end, minute)) {
            return false;
        }

        // Expect ':'
        if (ptr >= end || *ptr != ':') {
            return false;
        }
        ++ptr;

        // Parse second (2 digits required)
        uint32_t second = 0;
        if (!consume_digit<uint32_t, 2>(ptr, end, second)) {
            return false;
        }

        // Set time part
        if (!result.set_time_unit<TimeUnit::HOUR>(hour) ||
            !result.set_time_unit<TimeUnit::MINUTE>(minute) ||
            !result.set_time_unit<TimeUnit::SECOND>(second)) {
            return false;
        }

        // Parse fractional seconds if present
        if (ptr < end && *ptr == '.') {
            ++ptr;

            const char* frac_start = ptr;

            // Count fractional digits (up to 9), but only the first 6 are used
            int digit_count = 0;
            while (ptr < end && is_numeric_ascii(*ptr) && digit_count < 9) {
                ++ptr;
                ++digit_count;
            }

            if (digit_count == 0) {
                return false; // '.' must be followed by digits
            }

            // Parse up to 6 digits (microseconds) and TRUNCATE (not round)
            uint32_t microseconds = 0;
            int parse_count = std::min(digit_count, 6);

            for (int i = 0; i < parse_count; ++i) {
                microseconds = microseconds * 10 + (frac_start[i] - '0');
            }

            // Scale to microseconds if we parsed fewer than 6 digits
            for (int i = parse_count; i < 6; ++i) {
                microseconds *= 10;
            }

            result.unchecked_set_time_unit<TimeUnit::MICROSECOND>(microseconds);
        } else {
            result.unchecked_set_time_unit<TimeUnit::MICROSECOND>(0);
        }

        // Skip any remaining fractional digits beyond 9
        while (ptr < end && is_numeric_ascii(*ptr)) {
            ++ptr;
        }

        // Parse timezone offset if present
        if (ptr < end) {
            if (*ptr == 'Z' || *ptr == 'z') {
                ++ptr;
                parsed_tz = cctz::utc_time_zone();
                has_tz = true;
            } else if (*ptr == '+' || *ptr == '-') {
                if (!parse_tz_offset(ptr, end, parsed_tz)) {
                    return false;
                }
                has_tz = true;
            }
        }

        // Skip trailing whitespace
        while (ptr < end && (*ptr == ' ' || *ptr == '\t')) {
            ++ptr;
        }

        // Reject any remaining unparsed content
        if (ptr != end) {
            return false;
        }

        return true;
    }

    /** Parse timezone offset (+HH:MM, -HH:MM, colon optional). Advances ptr past the offset. */
    static bool parse_tz_offset(const char*& ptr, const char* end, cctz::time_zone& tz) {
        if (ptr >= end) {
            return false;
        }

        bool positive = (*ptr == '+');
        ++ptr;

        // Parse hour (1-2 digits)
        if (ptr >= end || !is_numeric_ascii(*ptr)) {
            return false;
        }
        uint32_t tz_hour = 0;
        if (ptr + 1 < end && is_numeric_ascii(*(ptr + 1))) {
            tz_hour =
                    static_cast<uint32_t>(ptr[0] - '0') * 10 + static_cast<uint32_t>(ptr[1] - '0');
            ptr += 2;
        } else {
            tz_hour = static_cast<uint32_t>(*ptr - '0');
            ptr += 1;
        }

        if ((!positive && tz_hour > 12) || (positive && tz_hour > 14)) {
            return false;
        }

        // Optional colon
        if (ptr < end && *ptr == ':') {
            ++ptr;
        }

        // Parse minute (2 digits, optional)
        uint32_t tz_minute = 0;
        if (ptr + 1 < end && is_numeric_ascii(*ptr) && is_numeric_ascii(*(ptr + 1))) {
            tz_minute =
                    static_cast<uint32_t>(ptr[0] - '0') * 10 + static_cast<uint32_t>(ptr[1] - '0');
            ptr += 2;
        }

        if (tz_minute != 0 && tz_minute != 30 && tz_minute != 45) {
            return false;
        }
        if (tz_hour == 14 && tz_minute != 0) {
            return false;
        }

        int offset_seconds = static_cast<int>(tz_hour * 3600 + tz_minute * 60);
        if (!positive) {
            offset_seconds = -offset_seconds;
        }
        tz = cctz::fixed_time_zone(cctz::seconds(offset_seconds));
        return true;
    }
};

void register_function_timestamp_spark(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionTimestampSpark>();
}

#include "common/compile_check_end.h"

} // namespace doris::vectorized
