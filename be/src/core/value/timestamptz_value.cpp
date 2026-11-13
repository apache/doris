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

#include "core/value/timestamptz_value.h"

#include "common/exception.h"
#include "exprs/function/cast/cast_to_timestamptz_impl.hpp"

namespace doris {

const TimestampTzValue TimestampTzValue::FIRST_DAY =
        TimestampTzValue(DateV2Value<DateTimeV2ValueType>::FIRST_DAY.to_date_int_val());

bool TimestampTzValue::from_string(const StringRef& str, const cctz::time_zone* local_time_zone,
                                   CastParameters& params, uint32_t to_scale) {
    return CastToTimestampTz::from_string(str, *this, params, local_time_zone, to_scale);
}

std::string TimestampTzValue::to_string(const cctz::time_zone& tz, int scale) const {
    char buffer[64];
    const int len = to_buffer(buffer, tz, scale);
    return {buffer, static_cast<size_t>(len)};
}

int TimestampTzValue::to_buffer(char* buffer, const cctz::time_zone& tz, int scale) const {
    cctz::civil_second utc_cs(_utc_dt.year(), _utc_dt.month(), _utc_dt.day(), _utc_dt.hour(),
                              _utc_dt.minute(), _utc_dt.second());

    cctz::time_point<cctz::seconds> cur_tz_time = cctz::convert(utc_cs, cctz::utc_time_zone());

    auto lookup_result = tz.lookup(cur_tz_time);

    cctz::civil_second civ = lookup_result.cs;
    // UTC storage bounds do not guarantee a representable session-local year. Reject
    // overflow before DateTimeV2 formatting could produce an offset-only wire value.
    if (civ.year() < 0 || civ.year() > 9999) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "TIMESTAMPTZ local year is outside [0, 9999]: year={}, timezone={}",
                        civ.year(), tz.name());
    }
    auto time_offset = lookup_result.offset;

    bool is_negative_offset = time_offset < 0;
    int abs_offset = std::abs(time_offset);
    int offset_hours = abs_offset / 3600;
    int offset_mins = (abs_offset % 3600) / 60;

    /// TODO: We could directly use datetime's to_string here. In the future,
    /// when we support a function like 'show datetime with timezone',
    /// we can reuse this implementation.

    DateV2Value<DateTimeV2ValueType> tmp_dt;
    tmp_dt.unchecked_set_time((uint16_t)civ.year(), (uint8_t)civ.month(), (uint8_t)civ.day(),
                              (uint8_t)civ.hour(), (uint8_t)civ.minute(), (uint8_t)civ.second(),
                              _utc_dt.microsecond());

    int len = tmp_dt.to_buffer(buffer, scale);
    // timezone +03:00
    // buffer[len++] = ' ';
    buffer[len++] = (is_negative_offset ? '-' : '+');
    buffer[len++] = static_cast<char>('0' + offset_hours / 10);
    buffer[len++] = '0' + offset_hours % 10;
    buffer[len++] = ':';
    buffer[len++] = static_cast<char>('0' + offset_mins / 10);
    buffer[len++] = '0' + offset_mins % 10;
    // Historical zones can have sub-minute offsets. Dropping their seconds changes the
    // instant represented by the client-visible wall clock and offset when read back.
    const int offset_seconds = abs_offset % 60;
    if (offset_seconds != 0) {
        buffer[len++] = ':';
        buffer[len++] = static_cast<char>('0' + offset_seconds / 10);
        buffer[len++] = static_cast<char>('0' + offset_seconds % 10);
    }
    return len;
}

bool TimestampTzValue::date_add_days(int64_t days, const cctz::time_zone& local_time_zone) {
    // Predicate movement treats a zero interval as an infallible identity, including
    // UTC year 0000/9999 whose local display year can be outside [0, 9999].
    if (days == 0) {
        return true;
    }
    const cctz::civil_second utc_cs(year(), month(), day(), hour(), minute(), second());
    const auto local = local_time_zone.lookup(cctz::convert(utc_cs, cctz::utc_time_zone()));
    const auto target = cctz::civil_day(local.cs) + days;
    // UTC boundaries and a day delta can produce an unrepresentable local year.
    if (local.cs.year() < 0 || local.cs.year() > 9999 || target.year() < 0 ||
        target.year() > 9999) {
        return false;
    }
    // Match Java's civil calendar, which permits February 29 in year 0. For example,
    // UTC 0000-02-28 20:00 at +08:00 passes through that local date before subtracting
    // a day. Validate the final UTC value rather than this local intermediate as DateV2.
    DateV2Value<DateTimeV2ValueType> local_dt;
    local_dt.unchecked_set_time(
            static_cast<uint16_t>(target.year()), static_cast<uint8_t>(target.month()),
            static_cast<uint8_t>(target.day()), static_cast<uint8_t>(local.cs.hour()),
            static_cast<uint8_t>(local.cs.minute()), static_cast<uint8_t>(local.cs.second()),
            microsecond());
    // Preserve the input offset when the target repeats, e.g. the second autumn 01:30.
    convert_local_to_utc(local_time_zone, local_dt, local.offset);
    // A valid local result can still fall outside the supported UTC year range.
    return is_valid_date();
}

bool TimestampTzValue::from_datetime(const DateV2Value<DateTimeV2ValueType>& origin_dt,
                                     const cctz::time_zone& local_time_zone, int dt_scale,
                                     int tz_scale) {
    PrimitiveTypeTraits<TYPE_DATETIMEV2>::CppType dt_value;

    PROPAGATE_FALSE(
            transform_date_scale(tz_scale, dt_scale, dt_value, origin_dt.to_date_int_val()));

    DateV2Value<DateTimeV2ValueType> dt {dt_value};

    cctz::civil_second local_cs(dt.year(), dt.month(), dt.day(), dt.hour(), dt.minute(),
                                dt.second());
    cctz::time_point<cctz::seconds> local_tp = cctz::convert(local_cs, local_time_zone);

    auto utc_cs = cctz::convert(local_tp, cctz::utc_time_zone());

    return _utc_dt.check_range_and_set_time((uint16_t)utc_cs.year(), (uint8_t)utc_cs.month(),
                                            (uint8_t)utc_cs.day(), (uint8_t)utc_cs.hour(),
                                            (uint8_t)utc_cs.minute(), (uint8_t)utc_cs.second(),
                                            dt.microsecond());
}

bool TimestampTzValue::to_datetime(DateV2Value<DateTimeV2ValueType>& dt,
                                   const cctz::time_zone& local_time_zone, int dt_scale,
                                   int tz_scale) const {
    PrimitiveTypeTraits<TYPE_DATETIMEV2>::CppType dt_value;

    PROPAGATE_FALSE(transform_date_scale(dt_scale, tz_scale, dt_value, _utc_dt.to_date_int_val()));

    dt = DateV2Value<DateTimeV2ValueType> {dt_value};

    cctz::civil_second utc_cs(dt.year(), dt.month(), dt.day(), dt.hour(), dt.minute(), dt.second());

    cctz::time_point<cctz::seconds> cur_tz_time = cctz::convert(utc_cs, cctz::utc_time_zone());
    auto local_cs = cctz::convert(cur_tz_time, local_time_zone);
    return dt.check_range_and_set_time((uint16_t)local_cs.year(), (uint8_t)local_cs.month(),
                                       (uint8_t)local_cs.day(), (uint8_t)local_cs.hour(),
                                       (uint8_t)local_cs.minute(), (uint8_t)local_cs.second(),
                                       dt.microsecond());
}

void TimestampTzValue::convert_utc_to_local(const cctz::time_zone& local_time_zone,
                                            DateV2Value<DateTimeV2ValueType>& dt) const {
    cctz::civil_second utc_cs(_utc_dt.year(), _utc_dt.month(), _utc_dt.day(), _utc_dt.hour(),
                              _utc_dt.minute(), _utc_dt.second());
    cctz::time_point<cctz::seconds> utc_tp = cctz::convert(utc_cs, cctz::utc_time_zone());
    auto local_cs = cctz::convert(utc_tp, local_time_zone);

    dt.unchecked_set_time((uint16_t)local_cs.year(), (uint8_t)local_cs.month(),
                          (uint8_t)local_cs.day(), (uint8_t)local_cs.hour(),
                          (uint8_t)local_cs.minute(), (uint8_t)local_cs.second(),
                          _utc_dt.microsecond());
}

void TimestampTzValue::convert_local_to_utc(const cctz::time_zone& local_time_zone,
                                            const DateV2Value<DateTimeV2ValueType>& dt) {
    cctz::civil_second local_cs(dt.year(), dt.month(), dt.day(), dt.hour(), dt.minute(),
                                dt.second());
    cctz::time_point<cctz::seconds> local_tp = cctz::convert(local_cs, local_time_zone);
    auto utc_cs = cctz::convert(local_tp, cctz::utc_time_zone());

    _utc_dt.unchecked_set_time((uint16_t)utc_cs.year(), (uint8_t)utc_cs.month(),
                               (uint8_t)utc_cs.day(), (uint8_t)utc_cs.hour(),
                               (uint8_t)utc_cs.minute(), (uint8_t)utc_cs.second(),
                               dt.microsecond());
}

int TimestampTzValue::utc_offset(const cctz::time_zone& local_time_zone) const {
    cctz::civil_second utc_cs(_utc_dt.year(), _utc_dt.month(), _utc_dt.day(), _utc_dt.hour(),
                              _utc_dt.minute(), _utc_dt.second());
    cctz::time_point<cctz::seconds> utc_tp = cctz::convert(utc_cs, cctz::utc_time_zone());
    return local_time_zone.lookup(utc_tp).offset;
}

void TimestampTzValue::convert_local_to_utc(const cctz::time_zone& local_time_zone,
                                            const DateV2Value<DateTimeV2ValueType>& dt,
                                            int preferred_offset) {
    cctz::civil_second local_cs(dt.year(), dt.month(), dt.day(), dt.hour(), dt.minute(),
                                dt.second());
    const auto lookup = local_time_zone.lookup(local_cs);
    // Use the pre-transition offset to shift gap input by the gap length, e.g. New York's
    // 2024-03-10 02:30 becomes 03:30. The preferred offset applies only to an overlap.
    auto local_tp = lookup.pre;

    if (lookup.kind == cctz::time_zone::civil_lookup::REPEATED) {
        const auto pre_offset = local_time_zone.lookup(lookup.pre).offset;
        const auto post_offset = local_time_zone.lookup(lookup.post).offset;
        if (preferred_offset == pre_offset) {
            local_tp = lookup.pre;
        } else if (preferred_offset == post_offset) {
            local_tp = lookup.post;
        }
    }

    auto utc_cs = cctz::convert(local_tp, cctz::utc_time_zone());
    _utc_dt.unchecked_set_time((uint16_t)utc_cs.year(), (uint8_t)utc_cs.month(),
                               (uint8_t)utc_cs.day(), (uint8_t)utc_cs.hour(),
                               (uint8_t)utc_cs.minute(), (uint8_t)utc_cs.second(),
                               dt.microsecond());
}

} // namespace doris
