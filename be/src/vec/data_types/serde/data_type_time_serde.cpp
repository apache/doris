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

#include "data_type_time_serde.h"

#include "runtime/primitive_type.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_number.h"
#include "vec/runtime/time_value.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"

Status DataTypeTimeV2SerDe::write_column_to_mysql(const IColumn& column,
                                                  MysqlRowBuffer<true>& row_buffer, int64_t row_idx,
                                                  bool col_const,
                                                  const FormatOptions& options) const {
    return _write_column_to_mysql(column, row_buffer, row_idx, col_const, options);
}
Status DataTypeTimeV2SerDe::write_column_to_mysql(const IColumn& column,
                                                  MysqlRowBuffer<false>& row_buffer,
                                                  int64_t row_idx, bool col_const,
                                                  const FormatOptions& options) const {
    return _write_column_to_mysql(column, row_buffer, row_idx, col_const, options);
}
template <bool is_binary_format>
Status DataTypeTimeV2SerDe::_write_column_to_mysql(const IColumn& column,
                                                   MysqlRowBuffer<is_binary_format>& result,
                                                   int64_t row_idx, bool col_const,
                                                   const FormatOptions& options) const {
    const auto& data = assert_cast<const ColumnTimeV2&>(column).get_data();
    const auto col_index = index_check_const(row_idx, col_const);
    // _nesting_level >= 2 means this time is in complex type
    // and we should add double quotes
    if (_nesting_level >= 2 && options.wrapper_len > 0) {
        if (UNLIKELY(0 != result.push_string(options.nested_string_wrapper, options.wrapper_len))) {
            return Status::InternalError("pack mysql buffer failed.");
        }
    }
    if (UNLIKELY(0 != result.push_timev2(data[col_index], scale))) {
        return Status::InternalError("pack mysql buffer failed.");
    }
    if (_nesting_level >= 2 && options.wrapper_len > 0) {
        if (UNLIKELY(0 != result.push_string(options.nested_string_wrapper, options.wrapper_len))) {
            return Status::InternalError("pack mysql buffer failed.");
        }
    }
    return Status::OK();
}

// NOLINTBEGIN(readability-function-size)
// NOLINTBEGIN(readability-function-cognitive-complexity)
Status DataTypeTimeV2SerDe::from_string_batch(const ColumnString& col_str, ColumnNullable& col_res,
                                              const FormatOptions& options) const {
    auto& col_data = assert_cast<ColumnTimeV2&>(col_res.get_nested_column());
    auto& col_nullmap = assert_cast<ColumnBool&>(col_res.get_null_map_column());
    size_t row = col_str.size();
    col_res.resize(row);

    for (size_t i = 0; i < row; ++i) {
        auto str = col_str.get_element(i);
        TimeValue::TimeType res;
        Status st = _from_string_strict_mode(str, res);
        if (st.is<ErrorCode::INVALID_ARGUMENT>()) {
            st = _from_string(str, res);
        }
        if (st.ok()) {
            col_nullmap.get_data()[i] = false;
            col_data.get_data()[i] = res;
        } else if (st.is<ErrorCode::INVALID_ARGUMENT>()) {
            // if still invalid, set null
            col_nullmap.get_data()[i] = true;
            col_data.get_data()[i] = 0;
        } else {
            // some internal error
            return st;
        }
    }
    return Status::OK();
}

Status DataTypeTimeV2SerDe::from_string_strict_mode_batch(const ColumnString& col_str,
                                                          IColumn& col_res,
                                                          const FormatOptions& options) const {
    size_t row = col_str.size();
    col_res.resize(row);
    auto& col_data = assert_cast<ColumnTimeV2&>(col_res);

    for (size_t i = 0; i < row; ++i) {
        auto str = col_str.get_element(i);
        TimeValue::TimeType res;
        RETURN_IF_ERROR(_from_string_strict_mode(str, res));
        col_data.get_data()[i] = res;
    }
    return Status::OK();
}

/**
<time> ::= ("+" | "-")? (<colon-format> | <numeric-format>)

<colon-format> ::= <hour> ":" <minute> (":" <second> (<microsecond>)?)?
<hour> ::= <digit>+
<minute> ::= <digit>{1,2}
<second> ::= <digit>{1,2}

<numeric-format> ::= <digit>+ (<microsecond>)?

<microsecond> ::= "." <digit>*

<digit> ::= "0" | "1" | "2" | "3" | "4" | "5" | "6" | "7" | "8" | "9"
*/
Status DataTypeTimeV2SerDe::_from_string(const std::string& str, double& res) const {
    const char* ptr = str.data();
    const char* end = ptr + str.size();

    // skip leading whitespace
    static_cast<void>(skip_any_whitespace(ptr, end));
    if (ptr == end) {
        return Status::InvalidArgument("empty time string");
    }

    // check sign
    bool negative = false;
    if (*ptr == '+') {
        ++ptr;
    } else if (*ptr == '-') {
        negative = true;
        ++ptr;
    }

    if (ptr == end) {
        return Status::InvalidArgument("empty time value after sign");
    }

    // Two possible formats: colon-format or numeric-format
    uint32_t hour = 0, minute = 0, second = 0;
    uint32_t microsecond = 0;

    // Check if we have colon format by looking ahead
    const char* temp = ptr;
    static_cast<void>(skip_any_digit(temp, end));
    bool colon_format = (temp < end && *temp == ':');

    if (colon_format) {
        // Parse hour
        const auto* start = ptr;
        static_cast<void>(skip_any_digit(ptr, end));
        if (ptr == start) {
            return Status::InvalidArgument("no digits in hour part");
        }

        StringParser::ParseResult success;
        hour = StringParser::string_to_int_internal<uint32_t, true>(start, (int)(ptr - start),
                                                                    &success);
        DCHECK(success == StringParser::PARSE_SUCCESS);

        // Check and consume colon
        RETURN_IF_ERROR(assert_within_bound(ptr, end, 0));
        if (*ptr != ':') {
            return Status::InvalidArgument("expected ':' after hour");
        }
        ++ptr;

        // Parse minute (1 or 2 digits)
        RETURN_IF_ERROR((consume_digit<uint32_t, 1, 2>(ptr, end, minute)));
        RETURN_INVALID_ARG_IF_NOT(minute < 60, "invalid minute {}", minute);

        // Check if we have seconds
        if (ptr < end && *ptr == ':') {
            ++ptr;

            // Parse second (1 or 2 digits)
            RETURN_IF_ERROR((consume_digit<uint32_t, 1, 2>(ptr, end, second)));
            RETURN_INVALID_ARG_IF_NOT(second < 60, "invalid second {}", second);

            // Check if we have microseconds
            if (ptr < end && *ptr == '.') {
                ++ptr;

                const auto* ms_start = ptr;
                static_cast<void>(skip_any_digit(ptr, end));
                auto length = ptr - ms_start;

                if (length > 0) {
                    auto ms = StringParser::string_to_uint_greedy_no_overflow<uint32_t>(
                            ms_start, std::min<int>((int)length, 6), &success);
                    if (success != StringParser::PARSE_SUCCESS) [[unlikely]] {
                        return Status::InvalidArgument(
                                "invalid fractional part in datetime string '{}'",
                                std::string {start, ptr});
                    }

                    if (length > 6) {
                        // Round off to at most 6 digits
                        if (auto remainder = *(ms_start + 6) - '0'; remainder >= 5) {
                            ms++;
                            DCHECK(ms <= 1000000);
                            if (ms == 1000000) {
                                // overflow, round up to next second
                                second++;
                                if (second == 60) {
                                    second = 0;
                                    minute++;
                                    if (minute == 60) {
                                        minute = 0;
                                        hour++;
                                    }
                                }
                                ms = 0;
                            }
                        }
                        microsecond = ms;
                    } else {
                        microsecond = (uint32_t)ms * common::exp10_i32(6 - (int)length);
                    }
                }
            }
        }
    } else {
        // numeric-format
        const auto* start = ptr;
        static_cast<void>(skip_any_digit(ptr, end));
        if (ptr == start) {
            return Status::InvalidArgument("no digits in numeric time format");
        }

        StringParser::ParseResult success;
        auto numeric_value = StringParser::string_to_int_internal<uint32_t, true>(
                start, (int)(ptr - start), &success);
        DCHECK(success == StringParser::PARSE_SUCCESS);

        // Convert the number to HHMMSS format
        if (numeric_value < 10000) {
            // SS or S format
            second = numeric_value;
            RETURN_INVALID_ARG_IF_NOT(second < 60, "invalid second {}", second);
        } else if (numeric_value < 10000 * 100) {
            // MMSS format
            minute = numeric_value / 100;
            second = numeric_value % 100;
            RETURN_INVALID_ARG_IF_NOT(minute < 60, "invalid minute {}", minute);
            RETURN_INVALID_ARG_IF_NOT(second < 60, "invalid second {}", second);
        } else {
            // HHMMSS format
            hour = numeric_value / 10000;
            minute = (numeric_value / 100) % 100;
            second = numeric_value % 100;
            RETURN_INVALID_ARG_IF_NOT(minute < 60, "invalid minute {}", minute);
            RETURN_INVALID_ARG_IF_NOT(second < 60, "invalid second {}", second);
        }

        // Check if we have microseconds
        if (ptr < end && *ptr == '.') {
            ++ptr;

            const auto* ms_start = ptr;
            static_cast<void>(skip_any_digit(ptr, end));
            auto length = ptr - ms_start;

            if (length > 0) {
                auto ms =
                        StringParser::string_to_int_internal<uint32_t, true>(ms_start, 6, &success);
                DCHECK(success == StringParser::PARSE_SUCCESS);

                if (length > 6) {
                    // Round off to at most 6 digits
                    if (auto remainder = *(ms_start + 6) - '0'; remainder >= 5) {
                        ms++;
                        DCHECK(ms <= 1000000);
                        if (ms == 1000000) {
                            // overflow, round up to next second
                            second++;
                            if (second == 60) {
                                second = 0;
                                minute++;
                                if (minute == 60) {
                                    minute = 0;
                                    hour++;
                                }
                            }
                            ms = 0;
                        }
                    }
                    microsecond = ms;
                } else {
                    microsecond = (uint32_t)ms * common::exp10_i32(6 - (int)length);
                }
            }
        }
    }

    // Skip trailing whitespace
    static_cast<void>(skip_any_whitespace(ptr, end));
    RETURN_INVALID_ARG_IF_NOT(ptr == end,
                              "invalid time string '{}', extra characters after parsing",
                              std::string {ptr, end});

    // Convert to TimeValue's internal storage format (microseconds since 00:00:00)
    res = (negative ? -1 : 1) * TimeValue::make_time(hour, minute, second, microsecond);
    RETURN_INVALID_ARG_IF_NOT(TimeValue::valid(res), "invalid time value: {}:{}:{}.{}", hour,
                              minute, second, microsecond);
    return Status::OK();
}

/**
<time> ::= ("+" | "-")? (<colon-format> | <numeric-format>)

<colon-format> ::= <hour> ":" <minute> (":" <second> (<microsecond>)?)?
<hour> ::= <digit>+
<minute> ::= <digit>{1,2}
<second> ::= <digit>{1,2}

<numeric-format> ::= <digit>+ (<microsecond>)?

<microsecond> ::= "." <digit>*

<digit> ::= "0" | "1" | "2" | "3" | "4" | "5" | "6" | "7" | "8" | "9"
*/
Status DataTypeTimeV2SerDe::_from_string_strict_mode(const std::string& str, double& res) const {
    const char* ptr = str.data();
    const char* end = ptr + str.size();

    // No whitespace skipping in strict mode
    if (ptr == end) {
        return Status::InvalidArgument("empty time string");
    }

    // check sign
    bool negative = false;
    if (*ptr == '+') {
        ++ptr;
    } else if (*ptr == '-') {
        negative = true;
        ++ptr;
    }

    if (ptr == end) {
        return Status::InvalidArgument("empty time value after sign");
    }

    // Two possible formats: colon-format or numeric-format
    uint32_t hour = 0, minute = 0, second = 0;
    uint32_t microsecond = 0;

    // Check if we have colon format by looking ahead
    const char* temp = ptr;
    RETURN_IF_ERROR(skip_any_digit(temp, end));
    bool colon_format = (temp < end && *temp == ':');

    if (colon_format) {
        // Parse hour
        const auto* start = ptr;
        RETURN_IF_ERROR(skip_any_digit(ptr, end));
        if (ptr == start) {
            return Status::InvalidArgument("no digits in hour part");
        }

        StringParser::ParseResult success;
        hour = StringParser::string_to_int_internal<uint32_t, true>(start, (int)(ptr - start),
                                                                    &success);
        DCHECK(success == StringParser::PARSE_SUCCESS);

        // Check and consume colon
        RETURN_IF_ERROR(assert_within_bound(ptr, end, 0));
        if (*ptr != ':') {
            return Status::InvalidArgument("expected ':' after hour");
        }
        ++ptr;

        // Parse minute (1 or 2 digits)
        RETURN_IF_ERROR((consume_digit<uint32_t, 1, 2>(ptr, end, minute)));
        RETURN_INVALID_ARG_IF_NOT(minute < 60, "invalid minute {}", minute);

        // Check if we have seconds
        if (ptr < end && *ptr == ':') {
            ++ptr;

            // Parse second (1 or 2 digits)
            RETURN_IF_ERROR((consume_digit<uint32_t, 1, 2>(ptr, end, second)));
            RETURN_INVALID_ARG_IF_NOT(second < 60, "invalid second {}", second);

            // Check if we have microseconds
            if (ptr < end && *ptr == '.') {
                ++ptr;

                const auto* ms_start = ptr;
                RETURN_IF_ERROR(skip_any_digit(ptr, end));
                auto length = ptr - ms_start;

                if (length > 0) {
                    auto ms = StringParser::string_to_uint_greedy_no_overflow<uint32_t>(
                            ms_start, std::min<int>((int)length, 6), &success);
                    if (success != StringParser::PARSE_SUCCESS) [[unlikely]] {
                        return Status::InvalidArgument(
                                "invalid fractional part in datetime string '{}'",
                                std::string {start, ptr});
                    }

                    if (length > 6) {
                        // Round off to at most 6 digits
                        if (auto remainder = *(ms_start + 6) - '0'; remainder >= 5) {
                            ms++;
                            DCHECK(ms <= 1000000);
                            if (ms == 1000000) {
                                // overflow, round up to next second
                                second++;
                                if (second == 60) {
                                    second = 0;
                                    minute++;
                                    if (minute == 60) {
                                        minute = 0;
                                        hour++;
                                    }
                                }
                                ms = 0;
                            }
                        }
                        microsecond = ms;
                    } else {
                        microsecond = (uint32_t)ms * common::exp10_i32(6 - (int)length);
                    }
                }
            }
        }
    } else {
        // numeric-format
        const auto* start = ptr;
        RETURN_IF_ERROR(skip_any_digit(ptr, end));
        if (ptr == start) {
            return Status::InvalidArgument("no digits in numeric time format");
        }

        StringParser::ParseResult success;
        auto numeric_value = StringParser::string_to_int_internal<uint32_t, true>(
                start, (int)(ptr - start), &success);
        DCHECK(success == StringParser::PARSE_SUCCESS);

        // Convert the number to HHMMSS format
        if (numeric_value < 10000) {
            // SS or S format
            second = numeric_value;
            RETURN_INVALID_ARG_IF_NOT(second < 60, "invalid second {}", second);
        } else if (numeric_value < 10000 * 100) {
            // MMSS format
            minute = numeric_value / 100;
            second = numeric_value % 100;
            RETURN_INVALID_ARG_IF_NOT(minute < 60, "invalid minute {}", minute);
            RETURN_INVALID_ARG_IF_NOT(second < 60, "invalid second {}", second);
        } else {
            // HHMMSS format
            hour = numeric_value / 10000;
            minute = (numeric_value / 100) % 100;
            second = numeric_value % 100;
            RETURN_INVALID_ARG_IF_NOT(minute < 60, "invalid minute {}", minute);
            RETURN_INVALID_ARG_IF_NOT(second < 60, "invalid second {}", second);
        }

        // Check if we have microseconds
        if (ptr < end && *ptr == '.') {
            ++ptr;

            const auto* ms_start = ptr;
            RETURN_IF_ERROR(skip_any_digit(ptr, end));
            auto length = ptr - ms_start;

            if (length > 0) {
                auto ms = StringParser::string_to_uint_greedy_no_overflow<uint32_t>(
                        ms_start, std::min<int>((int)length, 6), &success);
                if (success != StringParser::PARSE_SUCCESS) [[unlikely]] {
                    return Status::InvalidArgument(
                            "invalid fractional part in datetime string '{}'",
                            std::string {start, ptr});
                }

                if (length > 6) {
                    // Round off to at most 6 digits
                    if (auto remainder = *(ms_start + 6) - '0'; remainder >= 5) {
                        ms++;
                        DCHECK(ms <= 1000000);
                        if (ms == 1000000) {
                            // overflow, round up to next second
                            second++;
                            if (second == 60) {
                                second = 0;
                                minute++;
                                if (minute == 60) {
                                    minute = 0;
                                    hour++;
                                }
                            }
                            ms = 0;
                        }
                    }
                    microsecond = ms;
                } else {
                    microsecond = (uint32_t)ms * common::exp10_i32(6 - (int)length);
                }
            }
        }
    }

    // No trailing characters allowed in strict mode
    RETURN_INVALID_ARG_IF_NOT(ptr == end,
                              "invalid time string '{}', extra characters after parsing",
                              std::string {ptr, end});

    // Convert to TimeValue's internal storage format (microseconds since 00:00:00)
    res = (negative ? -1 : 1) * TimeValue::make_time(hour, minute, second, microsecond);
    RETURN_INVALID_ARG_IF_NOT(TimeValue::valid(res), "invalid time value: {}:{}:{}.{}", hour,
                              minute, second, microsecond);
    return Status::OK();
}

static Status from_int(int64_t int_val, int length, double& val) {
    if (length >= 1 && length <= 7) {
        bool negative = int_val < 0;
        uint64_t uint_val = negative ? -int_val : int_val;

        int hour = int(uint_val / 10000);
        int minute = (uint_val / 100) % 100;
        int second = uint_val % 100;
        val = (negative ? -1 : 1) * TimeValue::make_time(hour, minute, second);
        RETURN_INVALID_ARG_IF_NOT(TimeValue::valid(val), "invalid time value: {}:{}:{}", hour,
                                  minute, second);
    } else [[unlikely]] {
        return Status::InvalidArgument("invalid digits for time: {}", int_val);
    }
    return Status::OK();
}

static void init_microsecond(int64_t frac_part, uint32_t float_scale, double& val) {
    // normalize the fractional part to microseconds(6 digits)
    if (float_scale > 0) {
        if (float_scale > 6) {
            int ms = int(frac_part / common::exp10_i64(float_scale - 6));
            // if scale > 6, we need to round the fractional part
            int digit7 = frac_part % common::exp10_i32(6) / common::exp10_i32(float_scale - 6);
            if (digit7 >= 5) {
                ms++;
                DCHECK(ms <= 1000000);
                if (ms == 1000000) {
                    // overflow, round up to next second
                    val += TimeValue::ONE_SECOND_MICROSECONDS;
                    ms = 0;
                }
            }
            val = TimeValue::init_unsigned_microsecond(val, ms);
        } else { // scale <= 6
            val = TimeValue::init_unsigned_microsecond(
                    val, (uint32_t)frac_part * common::exp10_i32(6 - (int)float_scale));
        }
    }
}

template <typename IntDataType>
Status DataTypeTimeV2SerDe::from_int_batch(const IntDataType::ColumnType& int_col,
                                           ColumnNullable& target_col) const {
    auto& col_data = assert_cast<ColumnTimeV2&>(target_col.get_nested_column());
    auto& col_nullmap = assert_cast<ColumnBool&>(target_col.get_null_map_column());
    col_data.resize(int_col.size());
    col_nullmap.resize(int_col.size());

    for (size_t i = 0; i < int_col.size(); ++i) {
        auto int_val = (int64_t)int_col.get_element(i);
        int length = common::count_digits_fast(int_val);

        double val;
        if (auto st = from_int(int_val, length, val); st.ok()) [[likely]] {
            col_data.get_data()[i] = val;
            col_nullmap.get_data()[i] = false;
        } else if (st.is<ErrorCode::INVALID_ARGUMENT>()) {
            col_nullmap.get_data()[i] = true;
        } else {
            return st;
        }
    }
    return Status::OK();
}

template <typename IntDataType>
Status DataTypeTimeV2SerDe::from_int_strict_mode_batch(const IntDataType::ColumnType& int_col,
                                                       IColumn& target_col) const {
    auto& col_data = assert_cast<ColumnTimeV2&>(target_col);
    col_data.resize(int_col.size());

    for (size_t i = 0; i < int_col.size(); ++i) {
        auto int_val = (int64_t)int_col.get_element(i);
        int length = common::count_digits_fast(int_val);

        double val;
        RETURN_IF_ERROR(from_int(int_val, length, val));
        col_data.get_data()[i] = val;
    }
    return Status::OK();
}

template <typename FloatDataType>
Status DataTypeTimeV2SerDe::from_float_batch(const FloatDataType::ColumnType& float_col,
                                             ColumnNullable& target_col) const {
    auto& col_data = assert_cast<ColumnTimeV2&>(target_col.get_nested_column());
    auto& col_nullmap = assert_cast<ColumnBool&>(target_col.get_null_map_column());
    col_data.resize(float_col.size());
    col_nullmap.resize(float_col.size());

    for (size_t i = 0; i < float_col.size(); ++i) {
        double float_value = float_col.get_data()[i];
        if (std::isnan(float_value) || std::isinf(float_value) ||
            float_value >= (double)std::numeric_limits<int64_t>::max()) {
            col_nullmap.get_data()[i] = true;
            continue;
        }
        auto int_part = static_cast<int64_t>(float_value);
        int length = common::count_digits_fast(int_part);

        double val;
        if (auto st = from_int(int_part, length, val); st.ok()) [[likely]] {
            int ms_part_7 = (float_value - (double)int_part) * common::exp10_i32(7);
            init_microsecond(ms_part_7, 7, val);

            col_data.get_data()[i] = val;
            col_nullmap.get_data()[i] = false;
        } else if (st.is<ErrorCode::INVALID_ARGUMENT>()) {
            col_nullmap.get_data()[i] = true;
        } else {
            return st;
        }
    }
    return Status::OK();
}

template <typename FloatDataType>
Status DataTypeTimeV2SerDe::from_float_strict_mode_batch(const FloatDataType::ColumnType& float_col,
                                                         IColumn& target_col) const {
    auto& col_data = assert_cast<ColumnTimeV2&>(target_col);
    col_data.resize(float_col.size());

    for (size_t i = 0; i < float_col.size(); ++i) {
        double float_value = float_col.get_data()[i];
        if (std::isnan(float_value) || std::isinf(float_value) ||
            float_value >= (double)std::numeric_limits<int64_t>::max()) {
            return Status::InvalidArgument("invalid float value for time: {}", float_value);
        }
        auto int_part = static_cast<int64_t>(float_value);
        int length = common::count_digits_fast(int_part);

        double val;
        RETURN_IF_ERROR(from_int(int_part, length, val));

        int ms_part_7 = (float_value - (double)int_part) * common::exp10_i32(7);
        init_microsecond(ms_part_7, 7, val);

        col_data.get_data()[i] = val;
    }
    return Status::OK();
}

template <typename DecimalDataType>
Status DataTypeTimeV2SerDe::from_decimal_batch(const DecimalDataType::ColumnType& decimal_col,
                                               ColumnNullable& target_col) const {
    auto& col_data = assert_cast<ColumnTimeV2&>(target_col.get_nested_column());
    auto& col_nullmap = assert_cast<ColumnBool&>(target_col.get_null_map_column());
    col_data.resize(decimal_col.size());
    col_nullmap.resize(decimal_col.size());

    for (size_t i = 0; i < decimal_col.size(); ++i) {
        auto int_part = (int64_t)decimal_col.get_intergral_part(i);
        int length = common::count_digits_fast(int_part);

        double val;
        if (auto st = from_int(int_part, length, val); st.ok()) [[likely]] {
            init_microsecond((int64_t)decimal_col.get_fractional_part(i), decimal_col.get_scale(),
                             val);

            col_data.get_data()[i] = val;
            col_nullmap.get_data()[i] = false;
        } else if (st.is<ErrorCode::INVALID_ARGUMENT>()) {
            col_nullmap.get_data()[i] = true;
        } else {
            return st;
        }
    }
    return Status::OK();
}

template <typename DecimalDataType>
Status DataTypeTimeV2SerDe::from_decimal_strict_mode_batch(
        const DecimalDataType::ColumnType& decimal_col, IColumn& target_col) const {
    auto& col_data = assert_cast<ColumnTimeV2&>(target_col);
    col_data.resize(decimal_col.size());

    for (size_t i = 0; i < decimal_col.size(); ++i) {
        auto int_part = (int64_t)decimal_col.get_intergral_part(i);
        int length = common::count_digits_fast(int_part);

        double val;
        RETURN_IF_ERROR(from_int(int_part, length, val));
        init_microsecond((int64_t)decimal_col.get_fractional_part(i), decimal_col.get_scale(), val);

        col_data.get_data()[i] = val;
    }
    return Status::OK();
}
// NOLINTEND(readability-function-cognitive-complexity)
// NOLINTEND(readability-function-size)

// instantiation of template functions
template Status DataTypeTimeV2SerDe::from_int_batch<DataTypeInt8>(
        const DataTypeInt8::ColumnType& int_col, ColumnNullable& target_col) const;
template Status DataTypeTimeV2SerDe::from_int_batch<DataTypeInt16>(
        const DataTypeInt16::ColumnType& int_col, ColumnNullable& target_col) const;
template Status DataTypeTimeV2SerDe::from_int_batch<DataTypeInt32>(
        const DataTypeInt32::ColumnType& int_col, ColumnNullable& target_col) const;
template Status DataTypeTimeV2SerDe::from_int_batch<DataTypeInt64>(
        const DataTypeInt64::ColumnType& int_col, ColumnNullable& target_col) const;
template Status DataTypeTimeV2SerDe::from_int_batch<DataTypeInt128>(
        const DataTypeInt128::ColumnType& int_col, ColumnNullable& target_col) const;
template Status DataTypeTimeV2SerDe::from_int_strict_mode_batch<DataTypeInt8>(
        const DataTypeInt8::ColumnType& int_col, IColumn& target_col) const;
template Status DataTypeTimeV2SerDe::from_int_strict_mode_batch<DataTypeInt16>(
        const DataTypeInt16::ColumnType& int_col, IColumn& target_col) const;
template Status DataTypeTimeV2SerDe::from_int_strict_mode_batch<DataTypeInt32>(
        const DataTypeInt32::ColumnType& int_col, IColumn& target_col) const;
template Status DataTypeTimeV2SerDe::from_int_strict_mode_batch<DataTypeInt64>(
        const DataTypeInt64::ColumnType& int_col, IColumn& target_col) const;
template Status DataTypeTimeV2SerDe::from_int_strict_mode_batch<DataTypeInt128>(
        const DataTypeInt128::ColumnType& int_col, IColumn& target_col) const;
template Status DataTypeTimeV2SerDe::from_float_batch<DataTypeFloat32>(
        const DataTypeFloat32::ColumnType& float_col, ColumnNullable& target_col) const;
template Status DataTypeTimeV2SerDe::from_float_batch<DataTypeFloat64>(
        const DataTypeFloat64::ColumnType& float_col, ColumnNullable& target_col) const;
template Status DataTypeTimeV2SerDe::from_float_strict_mode_batch<DataTypeFloat32>(
        const DataTypeFloat32::ColumnType& float_col, IColumn& target_col) const;
template Status DataTypeTimeV2SerDe::from_float_strict_mode_batch<DataTypeFloat64>(
        const DataTypeFloat64::ColumnType& float_col, IColumn& target_col) const;
template Status DataTypeTimeV2SerDe::from_decimal_batch<DataTypeDecimal32>(
        const DataTypeDecimal32::ColumnType& decimal_col, ColumnNullable& target_col) const;
template Status DataTypeTimeV2SerDe::from_decimal_batch<DataTypeDecimal64>(
        const DataTypeDecimal64::ColumnType& decimal_col, ColumnNullable& target_col) const;
template Status DataTypeTimeV2SerDe::from_decimal_batch<DataTypeDecimalV2>(
        const DataTypeDecimalV2::ColumnType& decimal_col, ColumnNullable& target_col) const;
template Status DataTypeTimeV2SerDe::from_decimal_batch<DataTypeDecimal128>(
        const DataTypeDecimal128::ColumnType& decimal_col, ColumnNullable& target_col) const;
template Status DataTypeTimeV2SerDe::from_decimal_batch<DataTypeDecimal256>(
        const DataTypeDecimal256::ColumnType& decimal_col, ColumnNullable& target_col) const;
template Status DataTypeTimeV2SerDe::from_decimal_strict_mode_batch<DataTypeDecimal32>(
        const DataTypeDecimal32::ColumnType& decimal_col, IColumn& target_col) const;
template Status DataTypeTimeV2SerDe::from_decimal_strict_mode_batch<DataTypeDecimal64>(
        const DataTypeDecimal64::ColumnType& decimal_col, IColumn& target_col) const;
template Status DataTypeTimeV2SerDe::from_decimal_strict_mode_batch<DataTypeDecimalV2>(
        const DataTypeDecimalV2::ColumnType& decimal_col, IColumn& target_col) const;
template Status DataTypeTimeV2SerDe::from_decimal_strict_mode_batch<DataTypeDecimal128>(
        const DataTypeDecimal128::ColumnType& decimal_col, IColumn& target_col) const;
template Status DataTypeTimeV2SerDe::from_decimal_strict_mode_batch<DataTypeDecimal256>(
        const DataTypeDecimal256::ColumnType& decimal_col, IColumn& target_col) const;

} // namespace doris::vectorized
