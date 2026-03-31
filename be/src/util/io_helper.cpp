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
// This file is copied from
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Functions/registerFunctionsComparison.cpp
// and modified by Doris

#include "util/io_helper.h"

#include "core/binary_cast.hpp"
#include "exprs/function/cast/cast_to_date_or_datetime_impl.hpp"
#include "exprs/function/cast/cast_to_datetimev2_impl.hpp"
#include "exprs/function/cast/cast_to_datev2_impl.hpp"

namespace doris {
bool read_date_text_impl(VecDateTimeValue& x, const StringRef& buf) {
    CastParameters params;
    auto ans = CastToDateOrDatetime::from_string_non_strict_mode<DatelikeTargetType::DATE>(
            buf, x, nullptr, params);
    x.cast_to_date();
    return ans;
}

bool read_datetime_text_impl(VecDateTimeValue& x, const StringRef& buf) {
    CastParameters params;
    auto ans = CastToDateOrDatetime::from_string_non_strict_mode<DatelikeTargetType::DATE_TIME>(
            buf, x, nullptr, params);
    x.to_datetime();
    return ans;
}

bool read_date_text_impl(Int64& x, const StringRef& buf, const cctz::time_zone& local_time_zone) {
    auto dv = binary_cast<Int64, VecDateTimeValue>(x);
    CastParameters params;
    auto ans = CastToDateOrDatetime::from_string_non_strict_mode<DatelikeTargetType::DATE>(
            buf, dv, &local_time_zone, params);
    dv.cast_to_date();
    x = binary_cast<VecDateTimeValue, Int64>(dv);
    return ans;
}

bool read_datetime_text_impl(Int64& x, const StringRef& buf,
                             const cctz::time_zone& local_time_zone) {
    auto dv = binary_cast<Int64, VecDateTimeValue>(x);
    CastParameters params;
    auto ans = CastToDateOrDatetime::from_string_non_strict_mode<DatelikeTargetType::DATE_TIME>(
            buf, dv, &local_time_zone, params);
    dv.to_datetime();
    x = binary_cast<VecDateTimeValue, Int64>(dv);
    return ans;
}

bool read_date_v2_text_impl(DateV2Value<DateV2ValueType>& x, const StringRef& buf) {
    CastParameters params;
    return CastToDateV2::from_string_non_strict_mode(buf, x, nullptr, params);
}

bool read_date_v2_text_impl(DateV2Value<DateV2ValueType>& x, const StringRef& buf,
                            const cctz::time_zone& local_time_zone) {
    CastParameters params;
    return CastToDateV2::from_string_non_strict_mode(buf, x, &local_time_zone, params);
}

bool read_datetime_v2_text_impl(DateV2Value<DateTimeV2ValueType>& x, const StringRef& buf,
                                UInt32 scale) {
    CastParameters params;
    return CastToDatetimeV2::from_string_non_strict_mode(buf, x, nullptr, scale, params);
}

bool read_datetime_v2_text_impl(DateV2Value<DateTimeV2ValueType>& x, const StringRef& buf,
                                const cctz::time_zone& local_time_zone, UInt32 scale) {
    CastParameters params;
    return CastToDatetimeV2::from_string_non_strict_mode(buf, x, &local_time_zone, scale, params);
}

} // namespace doris
