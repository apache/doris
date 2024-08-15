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

#include <cmath>
#include <string>

#include "runtime/define_primitive_type.h"
#include "runtime/primitive_type.h"
#include "util/date_func.h"
#include "util/string_parser.hpp"
#include "vec/runtime/vdatetime_value.h"

namespace doris {

/// TODO:  Due to the "Time type is not supported for OLAP table" issue, a lot of basic content is missing.It will be supplemented later.
class TimeValue {
    using TimeType = typename PrimitiveTypeTraits<TYPE_TIMEV2>::CppType;
    constexpr static int64_t microsecond = 1000 * 1000;

public:
    static double to_time(int64_t hour, int64_t minute, int64_t second, int64_t micro = 0) {
        return hour * 3600 * microsecond + minute * 60 * microsecond + second * microsecond + micro;
    }

    static std::string to_string(TimeType time, int scale) {
        return timev2_to_buffer_from_double(time, scale);
    }

    // Cast from string
    // Some examples of conversions.
    // '300' -> 00:03:00 '20:23' ->  20:23:00 '20:23:24' -> 20:23:24
    static bool try_parse_time_from_string(char* s, size_t len, TimeType& x,
                                           const cctz::time_zone& local_time_zone) {
        if (_try_as_time_from_string(s, len, x)) {
            return true;
        } else {
            if (DateV2Value<DateTimeV2ValueType> dv {}; dv.from_date_str(s, len, local_time_zone)) {
                // can be parse as a datetime
                x = to_time(dv.hour(), dv.minute(), dv.second(), dv.microsecond());
                return true;
            }
            return false;
        }
    }
    // Cast from number
    template <typename T>
    static bool try_parse_time_from_number(T from, TimeType& x) {
        int64 time = static_cast<int64>(from);
        int64 seconds = int64(time / 100);
        int64 hour = 0, minute = 0, second = 0;
        second = int64(time - 100 * seconds);
        time /= 100;
        seconds = int64(time / 100);
        minute = int64(time - 100 * seconds);
        hour = seconds;
        if (minute >= 60 || second >= 60) {
            return false;
        }
        x = to_time(hour, minute, second);
        return true;
    }

private:
    template <typename T>
    static bool _try_as_time_from_string(char* s, size_t len, T& x) {
        char* first_char = s;
        char* end_char = s + len;
        int64_t hour = 0, minute = 0, second = 0, micro = 0;
        auto parse_from_str_to_int = [](char* begin, size_t len, auto& num) {
            StringParser::ParseResult parse_result = StringParser::PARSE_SUCCESS;
            auto int_value = StringParser::string_to_unsigned_int<uint64_t>(
                    reinterpret_cast<char*>(begin), len, &parse_result);
            if (UNLIKELY(parse_result != StringParser::PARSE_SUCCESS)) {
                return false;
            }
            num = int_value;
            return true;
        };
        auto parse_micro = [&](char* begin, int64_t len, int64_t& num) {
            // '20:21:23.1234' -> 20:21:23.123400
            // '20:21:23.12345678' -> 20:21:23.123457
            int64_t micro_num = 0;
            if (!parse_from_str_to_int(begin, len, micro_num)) {
                return false;
            }
            // 123 -> 123000.0
            // 1234567 -> 123456.7
            double microe = micro_num * std::pow(10, 6 - len);
            num = static_cast<int64_t>(std::ceil(microe));
            return true;
        };
        if (char* first_colon {nullptr};
            (first_colon = (char*)memchr(first_char, ':', len)) != nullptr) {
            if (char* second_colon {nullptr};
                (second_colon = (char*)memchr(first_colon + 1, ':', end_char - first_colon - 1)) !=
                nullptr) {
                if (char* third_colon {nullptr};
                    (third_colon = (char*)memchr(second_colon + 1, '.',
                                                 end_char - second_colon - 1)) != nullptr) {
                    // '20:21:23.123' -> 20:21:23.123
                    // parse hour
                    if (!parse_from_str_to_int(first_char, first_colon - first_char, hour)) {
                        return false;
                    }
                    // parse minute
                    if (!parse_from_str_to_int(first_colon + 1, second_colon - first_colon - 1,
                                               minute)) {
                        return false;
                    }
                    // parse second
                    if (!parse_from_str_to_int(second_colon + 1, third_colon - second_colon - 1,
                                               second)) {
                        return false;
                    }
                    // parse micro
                    if (!parse_micro(third_colon + 1, end_char - third_colon - 1, micro)) {
                        return false;
                    }
                } else {
                    // '20:23:24' -> 20:23:24
                    // parse hour
                    if (!parse_from_str_to_int(first_char, first_colon - first_char, hour)) {
                        return false;
                    }
                    // parse minute
                    if (!parse_from_str_to_int(first_colon + 1, second_colon - first_colon - 1,
                                               minute)) {
                        return false;
                    }
                    // parse second
                    if (!parse_from_str_to_int(second_colon + 1, end_char - second_colon - 1,
                                               second)) {
                        return false;
                    }
                }
            } else {
                // '20:23' -> 20:23:00
                // parse hour
                if (!parse_from_str_to_int(first_char, first_colon - first_char, hour)) {
                    return false;
                }
                // parse minute
                if (!parse_from_str_to_int(first_colon + 1, end_char - first_colon - 1, minute)) {
                    return false;
                }
            }
        } else {
            // '2000' -> 00:20:00
            // no colon ,so try to parse as a number
            size_t from {};
            if (!parse_from_str_to_int(first_char, len, from)) {
                return false;
            }
            return try_parse_time_from_number(from, x);
        }
        // minute second must be < 60
        if (minute >= 60 || second >= 60) {
            return false;
        }
        x = to_time(hour, minute, second, micro);
        return true;
    }
};

} // namespace doris
