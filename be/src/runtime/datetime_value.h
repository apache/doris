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

#include <re2/re2.h>
#include <stdint.h>

// IWYU pragma: no_include <bits/chrono.h>
#include <chrono>
#include <cstddef>
#include <iostream>

#include "cctz/civil_time.h"
#include "cctz/time_zone.h"
#include "udf/udf.h"
#include "util/hash_util.hpp"
#include "util/timezone_utils.h"
#include "vec/runtime/vdatetime_value.h"

namespace doris {

enum TimeUnit {
    MICROSECOND,
    SECOND,
    MINUTE,
    HOUR,
    DAY,
    WEEK,
    MONTH,
    QUARTER,
    YEAR,
    SECOND_MICROSECOND,
    MINUTE_MICROSECOND,
    MINUTE_SECOND,
    HOUR_MICROSECOND,
    HOUR_SECOND,
    HOUR_MINUTE,
    DAY_MICROSECOND,
    DAY_SECOND,
    DAY_MINUTE,
    DAY_HOUR,
    YEAR_MONTH
};

struct TimeInterval {
    int64_t year;
    int64_t month;
    int64_t day;
    int64_t hour;
    int64_t minute;
    int64_t second;
    int64_t microsecond;
    bool is_neg;

    TimeInterval()
            : year(0),
              month(0),
              day(0),
              hour(0),
              minute(0),
              second(0),
              microsecond(0),
              is_neg(false) {}

    TimeInterval(TimeUnit unit, int64_t count, bool is_neg_param)
            : year(0),
              month(0),
              day(0),
              hour(0),
              minute(0),
              second(0),
              microsecond(0),
              is_neg(is_neg_param) {
        switch (unit) {
        case YEAR:
            year = count;
            break;
        case MONTH:
            month = count;
            break;
        case WEEK:
            day = 7 * count;
            break;
        case DAY:
            day = count;
            break;
        case HOUR:
            hour = count;
            break;
        case MINUTE:
            minute = count;
            break;
        case SECOND:
            second = count;
            break;
        case MICROSECOND:
            microsecond = count;
            break;
        default:
            break;
        }
    }
};

enum TimeType { TIME_TIME = 1, TIME_DATE = 2, TIME_DATETIME = 3 };

// 9999-99-99 99:99:99.999999; 26 + 1('\0')
const int MAX_DTVALUE_STR_LEN = 27;

constexpr size_t const_length(const char* str) {
    return (str == nullptr || *str == 0) ? 0 : const_length(str + 1) + 1;
}
} // namespace doris
