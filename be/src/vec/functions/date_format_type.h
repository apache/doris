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

#include <algorithm>
#include <charconv>
#include <variant>

#include "vec/common/string_ref.h"

namespace doris::vectorized::time_format_type {
#include "common/compile_check_begin.h"
// Used to optimize commonly used date formats.

inline StringRef rewrite_specific_format(const char* raw_str, size_t str_size) {
    const static std::string specific_format_strs[3] = {"yyyyMMdd", "yyyy-MM-dd",
                                                        "yyyy-MM-dd HH:mm:ss"};
    const static std::string specific_format_rewrite[3] = {"%Y%m%d", "%Y-%m-%d",
                                                           "%Y-%m-%d %H:%i:%s"};
    for (int i = 0; i < 3; i++) {
        const StringRef specific_format {specific_format_strs[i].data(),
                                         specific_format_strs[i].size()};
        if (specific_format == StringRef {raw_str, str_size}) {
            return {specific_format_rewrite[i].data(), specific_format_rewrite[i].size()};
        }
    }
    return {raw_str, str_size};
}

template <typename T>
void put_year(T y, char* buf, int& i) {
    int t = y / 100;
    buf[i++] = cast_set<char, int, false>(t / 10 + '0');
    buf[i++] = cast_set<char, int, false>(t % 10 + '0');

    t = y % 100;
    buf[i++] = cast_set<char, int, false>(t / 10 + '0');
    buf[i++] = cast_set<char, int, false>(t % 10 + '0');
}

template <typename T>
void put_two_digits(T m, char* buf, int& i) {
    buf[i++] = cast_set<char, int, false>(m / 10 + '0');
    buf[i++] = cast_set<char, int, false>(m % 10 + '0');
}

// UserDefinedImpl indicates that no specific optimization has been applied, and the general logic is used for processing.
struct UserDefinedImpl {
    // Pre-allocated size for performance.
    constexpr static size_t row_size = 26;
};

struct yyyyMMddImpl {
    constexpr static size_t row_size = 8;
    template <typename DateType>
    size_t static date_to_str(const DateType& date_value, char* buf) {
        int i = 0;
        put_year(date_value.year(), buf, i);
        put_two_digits(date_value.month(), buf, i);
        put_two_digits(date_value.day(), buf, i);
        return i;
    }
};

struct yyyy_MM_ddImpl {
    constexpr static size_t row_size = 10;
    template <typename DateType>
    size_t static date_to_str(const DateType& date_value, char* buf) {
        int i = 0;
        put_year(date_value.year(), buf, i);
        buf[i++] = '-';
        put_two_digits(date_value.month(), buf, i);
        buf[i++] = '-';
        put_two_digits(date_value.day(), buf, i);
        return i;
    }
};

struct yyyy_MM_dd_HH_mm_ssImpl {
    constexpr static size_t row_size = 19;
    template <typename DateType>
    size_t static date_to_str(const DateType& date_value, char* buf) {
        int i = 0;
        put_year(date_value.year(), buf, i);
        buf[i++] = '-';
        put_two_digits(date_value.month(), buf, i);
        buf[i++] = '-';
        put_two_digits(date_value.day(), buf, i);
        buf[i++] = ' ';
        put_two_digits(date_value.hour(), buf, i);
        buf[i++] = ':';
        put_two_digits(date_value.minute(), buf, i);
        buf[i++] = ':';
        put_two_digits(date_value.second(), buf, i);
        return i;
    }
};

struct yyyy_MM_dd_HH_mm_ss_SSSSSSImpl {
    constexpr static size_t row_size = 26;
    size_t static date_to_str(const DateV2Value<DateTimeV2ValueType>& date_value, char* buf) {
        int i = 0;
        put_year(date_value.year(), buf, i);
        buf[i++] = '-';
        put_two_digits(date_value.month(), buf, i);
        buf[i++] = '-';
        put_two_digits(date_value.day(), buf, i);
        buf[i++] = ' ';
        put_two_digits(date_value.hour(), buf, i);
        buf[i++] = ':';
        put_two_digits(date_value.minute(), buf, i);
        buf[i++] = ':';
        put_two_digits(date_value.second(), buf, i);
        buf[i++] = '.';
        int length = common::count_digits_fast(date_value.microsecond());
        std::fill(buf + i, buf + i + 6 - length, '0');
        std::to_chars(buf + i + 6 - length, buf + i + 6, date_value.microsecond());
        return i + 6;
    }
};

struct yyyy_MMImpl {
    constexpr static size_t row_size = 7;
    template <typename DateType>
    size_t static date_to_str(const DateType& date_value, char* buf) {
        int i = 0;
        put_year(date_value.year(), buf, i);
        buf[i++] = '-';
        put_two_digits(date_value.month(), buf, i);
        return i;
    }
};
struct yyyyMMImpl {
    constexpr static size_t row_size = 6;
    template <typename DateType>
    size_t static date_to_str(const DateType& date_value, char* buf) {
        int i = 0;
        put_year(date_value.year(), buf, i);
        put_two_digits(date_value.month(), buf, i);
        return i;
    }
};

struct yyyyImpl {
    constexpr static size_t row_size = 4;
    template <typename DateType>
    size_t static date_to_str(const DateType& date_value, char* buf) {
        int i = 0;
        put_year(date_value.year(), buf, i);
        return i;
    }
};

using FormatImplVariant = std::variant<UserDefinedImpl, yyyyMMddImpl, yyyy_MM_ddImpl,
                                       yyyy_MM_dd_HH_mm_ssImpl, yyyy_MMImpl, yyyyMMImpl, yyyyImpl>;

const static std::string DEFAULT_FORMAT = "yyyy-MM-dd HH:mm:ss";
const static std::string DEFAULT_FORMAT_DECIMAL = "%Y-%m-%d %H:%i:%s.%f";
const static auto DEFAULT_IMPL = yyyy_MM_dd_HH_mm_ssImpl {};
// this type hasn't be put into FormatImplVariant. we will use special judgment to handle it.
const static auto DEFAULT_IMPL_DECIMAL = yyyy_MM_dd_HH_mm_ss_SSSSSSImpl {};
inline FormatImplVariant string_to_impl(const std::string& format) {
    if (format == "yyyyMMdd" || format == "%Y%m%d") {
        return yyyyMMddImpl {};
    } else if (format == "yyyy-MM-dd" || format == "%Y-%m-%d") {
        return yyyy_MM_ddImpl {};
    } else if (format == DEFAULT_FORMAT || format == "%Y-%m-%d %H:%i:%s") {
        return yyyy_MM_dd_HH_mm_ssImpl {};
    } else if (format == "yyyy-MM") {
        return yyyy_MMImpl {};
    } else if (format == "yyyyMM") {
        return yyyyMMImpl {};
    } else if (format == "yyyy") {
        return yyyyImpl {};
    } else {
        return UserDefinedImpl {};
    }
}
#include "common/compile_check_end.h"
} // namespace doris::vectorized::time_format_type
