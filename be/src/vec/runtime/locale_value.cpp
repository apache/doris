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

#include "locale_value.h"

#include <cctype>
#include <cstddef>
#include <cstdint>
#include <cstring>

namespace doris::vectorized {
namespace {

constexpr size_t kMonthsPerYear = 12;
constexpr size_t kDaysPerWeek = 7;

// The name arrays keep the trailing nullptr sentinel so the data layout mirrors
// the MySQL TYPELIB-based representation.
const char* month_names_en_US[kMonthsPerYear + 1] = {
        "January", "February",  "March",   "April",    "May",      "June", "July",
        "August",  "September", "October", "November", "December", nullptr};
const char* ab_month_names_en_US[kMonthsPerYear + 1] = {"Jan", "Feb", "Mar",  "Apr", "May",
                                                        "Jun", "Jul", "Aug",  "Sep", "Oct",
                                                        "Nov", "Dec", nullptr};
const char* day_names_en_US[kDaysPerWeek + 1] = {"Monday", "Tuesday",  "Wednesday", "Thursday",
                                                 "Friday", "Saturday", "Sunday",    nullptr};
const char* ab_day_names_en_US[kDaysPerWeek + 1] = {"Mon", "Tue", "Wed", "Thu",
                                                    "Fri", "Sat", "Sun", nullptr};

LocaleLib locale_lib_month_names_en_US {
        .count = kMonthsPerYear,
        .name = "month_names",
        .type_name = month_names_en_US,
};
LocaleLib locale_lib_ab_month_names_en_US {
        .count = kMonthsPerYear,
        .name = "ab_month_names",
        .type_name = ab_month_names_en_US,
};
LocaleLib locale_lib_day_names_en_US {
        .count = kDaysPerWeek,
        .name = "day_names",
        .type_name = day_names_en_US,
};
LocaleLib locale_lib_ab_day_names_en_US {
        .count = kDaysPerWeek,
        .name = "ab_day_names",
        .type_name = ab_day_names_en_US,
};

inline bool iequals(const char* lhs, std::size_t lhs_len, const char* rhs) {
    if (lhs == nullptr || rhs == nullptr) {
        return false;
    }
    const std::size_t rhs_len = std::strlen(rhs);
    if (lhs_len != rhs_len) {
        return false;
    }
    for (std::size_t i = 0; i < rhs_len; ++i) {
        const auto l = static_cast<unsigned char>(lhs[i]);
        const auto r = static_cast<unsigned char>(rhs[i]);
        if (std::tolower(l) != std::tolower(r)) {
            return false;
        }
    }
    return true;
}

} // namespace

LocaleValue locale_en_US {
        .number = 0,
        .name = "en_US",
        .description = "English - United States",
        .is_ascii = true,
        .month_names = &locale_lib_month_names_en_US,
        .ab_month_names = &locale_lib_ab_month_names_en_US,
        .day_names = &locale_lib_day_names_en_US,
        .ab_day_names = &locale_lib_ab_day_names_en_US,
};

LocaleValue* locale_values[] = {&locale_en_US, nullptr};

LocaleValue* default_locale_value = &locale_en_US;

LocaleValue* locale_value_by_name(const char* name, std::size_t length) {
    if (name == nullptr) {
        return nullptr;
    }
    for (LocaleValue** cursor = locale_values; *cursor != nullptr; ++cursor) {
        if (iequals(name, length, (*cursor)->name)) {
            return *cursor;
        }
    }
    return nullptr;
}

LocaleValue* locale_value_by_number(std::uint32_t number) {
    for (LocaleValue** cursor = locale_values; *cursor != nullptr; ++cursor) {
        if ((*cursor)->number == number) {
            return *cursor;
        }
    }
    return nullptr;
}

} // namespace doris::vectorized
