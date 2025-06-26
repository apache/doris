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
Status DataTypeTimeV2SerDe::_from_string(const std::string& str, TimeValue::TimeType& res) const {
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
                    auto ms = StringParser::string_to_int_internal<uint32_t, true>(ms_start, 6,
                                                                                   &success);
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
    res = (double)TimeValue::limit_with_bound(
            (negative ? -1 : 1) * double(hour * 3600 + minute * 60 + second) +
            (double)microsecond / TimeValue::ONE_SECOND_MICROSECONDS);

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
Status DataTypeTimeV2SerDe::_from_string_strict_mode(const std::string& str,
                                                     TimeValue::TimeType& res) const {
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
                    auto ms = StringParser::string_to_int_internal<uint32_t, true>(
                            ms_start, std::min<int>((int)length, 6), &success);
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
                auto ms = StringParser::string_to_int_internal<uint32_t, true>(
                        ms_start, std::min<int>((int)length, 6), &success);
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

    // No trailing characters allowed in strict mode
    RETURN_INVALID_ARG_IF_NOT(ptr == end,
                              "invalid time string '{}', extra characters after parsing",
                              std::string {ptr, end});

    // Convert to TimeValue's internal storage format (microseconds since 00:00:00)
    res = (double)TimeValue::limit_with_bound(
            (negative ? -1 : 1) * double(hour * 3600 + minute * 60 + second) +
            (double)microsecond / TimeValue::ONE_SECOND_MICROSECONDS);

    return Status::OK();
}
} // namespace doris::vectorized
