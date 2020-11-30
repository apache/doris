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
    if (NULL != res) {
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
    if (NULL != res) {
        value = (time_tm.tm_year + 1900) * 16 * 32 + (time_tm.tm_mon + 1) * 32 + time_tm.tm_mday;
    } else {
        // 1400 - 01 - 01
        value = 716833;
    }

    return uint24_t(value);
}

std::string time_str_from_double(double time) {
    std::stringstream time_ss;
    if (time < 0) {
        time_ss << "-";
        time = -time;
    }
    int64_t hour = time / 60 / 60;
    int minute = ((int64_t)(time / 60)) % 60;
    int second = ((int64_t)time) % 60;

    time_ss << std::setw(2) << std::setfill('0') << hour << ":" << std::setw(2) << std::setfill('0')
            << minute << ":" << std::setw(2) << std::setfill('0') << second;
    return time_ss.str();
}

} // namespace doris
