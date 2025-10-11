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

#include "timestamptz_value.h"

#include "common/status.h"
#include "vec/functions/cast/cast_to_datetimev2_impl.hpp"

namespace doris {

bool TimestampTzValue::from_string(const StringRef& str, const cctz::time_zone* local_time_zone,
                                   vectorized::CastParameters& params) {
    using namespace vectorized;
    if (params.is_strict) {
        return CastToDatetimeV2::from_string_strict_mode<true, DataTimeCastEnumType::TIMESTAMP_TZ>(
                str, _utc_dt, local_time_zone, 6, params);
    } else {
        // This from_string implementation is derived from:
        /*
        static inline bool from_string_non_strict_mode(const StringRef& str,
                                                   DateV2Value<DateTimeV2ValueType>& res,
                                                   const cctz::time_zone* local_time_zone,
                                                   uint32_t to_scale, CastParameters& params) {
        return CastToDatetimeV2::from_string_strict_mode<false>(str, res, local_time_zone, to_scale,
                                                                params) ||
               CastToDatetimeV2::from_string_non_strict_mode_impl(str, res, local_time_zone,
                                                                  to_scale, params);
    }
        */
        return CastToDatetimeV2::from_string_strict_mode<false, DataTimeCastEnumType::TIMESTAMP_TZ>(
                       str, _utc_dt, local_time_zone, 6, params) ||
               CastToDatetimeV2::from_string_non_strict_mode_impl<
                       DataTimeCastEnumType::TIMESTAMP_TZ>(str, _utc_dt, local_time_zone, 6,
                                                           params);
    }
}

std::string TimestampTzValue::to_string(const cctz::time_zone& tz) const {
    cctz::civil_second utc_cs(_utc_dt.year(), _utc_dt.month(), _utc_dt.day(), _utc_dt.hour(),
                              _utc_dt.minute(), _utc_dt.second());

    cctz::time_point<cctz::seconds> cur_tz_time = cctz::convert(utc_cs, cctz::utc_time_zone());

    auto lookup_result = tz.lookup(cur_tz_time);

    cctz::civil_second civ = lookup_result.cs;
    auto time_offset = lookup_result.offset;

    int offset_hours = time_offset / 3600;
    int offset_mins = (std::abs(time_offset) % 3600) / 60;

    char buf[48];

    /// TODO: We could directly use datetime's to_string here. In the future,
    /// when we support a function like 'show datetime with timezone',
    /// we can reuse this implementation.
    snprintf(buf, sizeof(buf), "%04ld-%02d-%02d %02d:%02d:%02d.%06d %c%02d:%02d", civ.year(),
             civ.month(), civ.day(), civ.hour(), civ.minute(), civ.second(), _utc_dt.microsecond(),
             (offset_hours >= 0 ? '+' : '-'), std::abs(offset_hours), offset_mins);

    return {buf};
}

bool TimestampTzValue::from_datetime(const DateV2Value<DateTimeV2ValueType>& dt,
                                     const cctz::time_zone& local_time_zone) {
    cctz::civil_second local_cs(dt.year(), dt.month(), dt.day(), dt.hour(), dt.minute(),
                                dt.second());
    cctz::time_point<cctz::seconds> local_tp = cctz::convert(local_cs, local_time_zone);

    auto utc_cs = cctz::convert(local_tp, cctz::utc_time_zone());

    DateV2Value<DateTimeV2ValueType> utc_dt;

    return _utc_dt.check_range_and_set_time((uint16_t)utc_cs.year(), (uint8_t)utc_cs.month(),
                                            (uint8_t)utc_cs.day(), (uint8_t)utc_cs.hour(),
                                            (uint8_t)utc_cs.minute(), (uint8_t)utc_cs.second(),
                                            dt.microsecond());
}

bool TimestampTzValue::to_datetime(DateV2Value<DateTimeV2ValueType>& dt,
                                   const cctz::time_zone& local_time_zone) const {
    cctz::civil_second utc_cs(_utc_dt.year(), _utc_dt.month(), _utc_dt.day(), _utc_dt.hour(),
                              _utc_dt.minute(), _utc_dt.second());

    cctz::time_point<cctz::seconds> cur_tz_time = cctz::convert(utc_cs, cctz::utc_time_zone());
    auto local_cs = cctz::convert(cur_tz_time, local_time_zone);
    return dt.check_range_and_set_time((uint16_t)local_cs.year(), (uint8_t)local_cs.month(),
                                       (uint8_t)local_cs.day(), (uint8_t)local_cs.hour(),
                                       (uint8_t)local_cs.minute(), (uint8_t)local_cs.second(),
                                       _utc_dt.microsecond());
}

} // namespace doris
