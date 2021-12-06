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

#include <iomanip>

namespace doris {

uint64_t timestamp_from_datetime(const std::string& datetime_str) {
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

    return value;
}

uint24_t timestamp_from_date(const std::string& date_str) {
    tm time_tm;
    char* res = strptime(date_str.c_str(), "%Y-%m-%d", &time_tm);

    int value = 0;
    if (nullptr != res) {
        value = (time_tm.tm_year + 1900) * 16 * 32 + (time_tm.tm_mon + 1) * 32 + time_tm.tm_mday;
    } else {
        // 1400 - 01 - 01
        value = 716833;
    }

    return uint24_t(value);
}
// refer to https://dev.mysql.com/doc/refman/5.7/en/time.html
// the time value between '-838:59:59' and '838:59:59'
int32_t time_to_buffer_from_double(double time, char* buffer) {
    char* begin = buffer;
    if (time < 0) {
        time = -time;
        *buffer++ = '-';
    }
    if (time > 3020399) {
        time = 3020399;
    }
    int64_t hour = (int64_t)(time / 3600);
    if (hour >= 100) {
        auto f = fmt::format_int(hour);
        memcpy(buffer, f.data(), f.size());
        buffer = buffer + f.size();
    } else {
        *buffer++ = (char)('0' + (hour / 10));
        *buffer++ = (char)('0' + (hour % 10));
    }
    *buffer++ = ':';
    int32_t minute = ((int32_t)(time / 60)) % 60;
    *buffer++ = (char)('0' + (minute / 10));
    *buffer++ = (char)('0' + (minute % 10));
    *buffer++ = ':';
    int32_t second = ((int32_t)time) % 60;
    *buffer++ = (char)('0' + (second / 10));
    *buffer++ = (char)('0' + (second % 10));
    return buffer - begin;
}

} // namespace doris
