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

#include <cstdint>

#include "common/config.h"
#include "vec/runtime/vdatetime_value.h"

namespace doris::vectorized {

inline PURE uint32_t complete_4digit_year(uint32_t year) {
    if (year < 70) {
        return year + 2000; // 00-69 -> 2000-2069
    } else {
        return year + 1900; // 70-99 -> 1970-1999
    }
}

// return true if we set the date value in this way.
template <typename T>
inline bool try_convert_set_zero_date(T& date_val, uint32_t year, uint32_t month, uint32_t day) {
    if (config::allow_zero_date && year == 0 && month == 0 && day == 0) {
        date_val.template unchecked_set_time_unit<TimeUnit::YEAR>(0);
        date_val.template unchecked_set_time_unit<TimeUnit::MONTH>(1);
        date_val.template unchecked_set_time_unit<TimeUnit::DAY>(1);
        return true;
    }
    return false;
}
} // namespace doris::vectorized
