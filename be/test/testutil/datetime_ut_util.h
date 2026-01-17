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

#include "vec/runtime/timestamptz_value.h"
#include "vec/runtime/vdatetime_value.h"
namespace doris {
inline auto make_datetime(int year, int month, int day, int hour, int minute, int second,
                          int microsecond) {
    DateV2Value<DateTimeV2ValueType> dt;
    dt.unchecked_set_time(year, month, day, hour, minute, second, microsecond);
    return dt;
}

inline auto make_timestamptz(int year, int month, int day, int hour, int minute, int second,
                             int microsecond) {
    DateV2Value<DateTimeV2ValueType> dt;
    dt.unchecked_set_time(year, month, day, hour, minute, second, microsecond);
    return TimestampTzValue(dt.to_date_int_val());
}

} // namespace doris