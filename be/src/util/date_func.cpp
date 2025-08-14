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

#include "util/date_func.h"

#include <fmt/compile.h>
#include <fmt/format.h>
#include <glog/logging.h>

#include <cstring>
#include <ctime>

#include "vec/runtime/time_value.h"
#include "vec/runtime/vdatetime_value.h"

namespace doris {

VecDateTimeValue timestamp_from_datetime(const std::string& datetime_str) {
    tm time_tm;
    char* res = strptime(datetime_str.c_str(), "%Y-%m-%d %H:%M:%S", &time_tm);

    uint64_t value = 0;
    if (nullptr != res) {
        value = ((time_tm.tm_year + 1900) * 10000L + (time_tm.tm_mon + 1) * 100L +
                 time_tm.tm_mday) *
                        1000000L +
                time_tm.tm_hour * 10000L + time_tm.tm_min * 100L + time_tm.tm_sec;
    } else {
        // 1400 - 01 - 01
        value = 14000101000000;
    }

    return VecDateTimeValue::create_from_olap_datetime(value);
}

VecDateTimeValue timestamp_from_date(const std::string& date_str) {
    tm time_tm;
    char* res = strptime(date_str.c_str(), "%Y-%m-%d", &time_tm);

    uint32_t value = 0;
    if (nullptr != res) {
        value = (uint32_t)((time_tm.tm_year + 1900) * 16 * 32 + (time_tm.tm_mon + 1) * 32 +
                           time_tm.tm_mday);
    } else {
        LOG(WARNING) << "Invalid date string: " << date_str;
        // 1400 - 01 - 01
        value = 716833;
    }

    return VecDateTimeValue::create_from_olap_date(value);
}

DateV2Value<DateV2ValueType> timestamp_from_date_v2(const std::string& date_str) {
    tm time_tm;
    char* res = strptime(date_str.c_str(), "%Y-%m-%d", &time_tm);

    uint32_t value = 0;
    if (nullptr != res) {
        value = ((time_tm.tm_year + 1900) << 9) | ((time_tm.tm_mon + 1) << 5) | time_tm.tm_mday;
    } else {
        value = MIN_DATE_V2;
    }

    return DateV2Value<DateV2ValueType>::create_from_olap_date(value);
}

DateV2Value<DateTimeV2ValueType> timestamp_from_datetime_v2(const std::string& date_str) {
    DateV2Value<DateTimeV2ValueType> val;
    std::string date_format = "%Y-%m-%d %H:%i:%s.%f";
    val.from_date_format_str(date_format.data(), date_format.size(), date_str.data(),
                             date_str.size());
    return val;
}

//FIXME: try to remove or refactor all those time input/output functions.
int32_t timev2_to_buffer_from_double(double time, char* buffer, int scale) {
    static int pow10[7] = {1, 10, 100, 1000, 10000, 100000, 1000000};

    char* begin = buffer;
    if (time < 0) {
        time = -time;
        *buffer++ = '-';
    }
    auto m_time = (int64_t)TimeValue::limit_with_bound(time);
    int64_t hour = m_time / ((int64_t)3600 * 1000 * 1000);
    if (hour >= 100) {
        buffer = fmt::format_to(buffer, FMT_COMPILE("{}"), hour);
    } else {
        *buffer++ = (char)('0' + (hour / 10));
        *buffer++ = (char)('0' + (hour % 10));
    }
    *buffer++ = ':';
    m_time %= (int64_t)3600 * 1000 * 1000;
    int64_t minute = m_time / (60 * 1000 * 1000);
    *buffer++ = (char)('0' + (minute / 10));
    *buffer++ = (char)('0' + (minute % 10));
    *buffer++ = ':';
    m_time %= 60 * 1000 * 1000;
    int32_t second = m_time / (1000 * 1000);
    *buffer++ = (char)('0' + (second / 10));
    *buffer++ = (char)('0' + (second % 10));
    m_time %= 1000 * 1000;
    if (scale == 0) {
        return buffer - begin;
    }
    *buffer++ = '.';
    memset(buffer, '0', scale);
    buffer += scale;
    int32_t micosecond = m_time % (1000 * 1000);
    micosecond /= pow10[6 - scale];
    auto* it = buffer - 1;
    while (micosecond) {
        *it = (char)('0' + (micosecond % 10));
        micosecond /= 10;
        it--;
    }
    return buffer - begin;
}

std::string timev2_to_buffer_from_double(double time, int scale) {
    fmt::memory_buffer buffer;
    if (time < 0) {
        time = -time;
        fmt::format_to(buffer, "-");
    }
    auto m_time = TimeValue::limit_with_bound(time);
    auto hour = TimeValue::hour(m_time);
    if (hour >= 100) {
        fmt::format_to(buffer, fmt::format("{}", hour));
    } else {
        fmt::format_to(buffer, fmt::format("{:02d}", hour));
    }
    auto minute = TimeValue::minute(m_time);
    auto second = TimeValue::second(m_time);
    auto micosecond = TimeValue::microsecond(m_time);
    micosecond /= common::exp10_i32(6 - scale);
    switch (scale) {
    case 0:
        fmt::format_to(buffer, fmt::format(FMT_COMPILE(":{:02d}:{:02d}"), minute, second));
        break;
    case 1:
        fmt::format_to(buffer, fmt::format(FMT_COMPILE(":{:02d}:{:02d}.{:01d}"), minute, second,
                                           micosecond));
        break;
    case 2:
        fmt::format_to(buffer, fmt::format(FMT_COMPILE(":{:02d}:{:02d}.{:02d}"), minute, second,
                                           micosecond));
        break;
    case 3:
        fmt::format_to(buffer, fmt::format(FMT_COMPILE(":{:02d}:{:02d}.{:03d}"), minute, second,
                                           micosecond));
        break;
    case 4:
        fmt::format_to(buffer, fmt::format(FMT_COMPILE(":{:02d}:{:02d}.{:04d}"), minute, second,
                                           micosecond));
        break;
    case 5:
        fmt::format_to(buffer, fmt::format(FMT_COMPILE(":{:02d}:{:02d}.{:05d}"), minute, second,
                                           micosecond));
        break;
    case 6:
        fmt::format_to(buffer, fmt::format(FMT_COMPILE(":{:02d}:{:02d}.{:06d}"), minute, second,
                                           micosecond));
        break;
    }

    return fmt::to_string(buffer);
}
} // namespace doris
