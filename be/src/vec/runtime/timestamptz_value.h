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

#include <cctz/time_zone.h>
#include <sys/types.h>

#include <cstdint>
#include <functional>
#include <string>

#include "common/status.h"
#include "vec/runtime/vdatetime_value.h"

namespace doris {

struct StringRef;

namespace vectorized {
struct CastParameters;
}

// TIMESTAMPTZ can be understood as a DATETIME type with timezone conversion functionality.
// Doris automatically handles timezone conversions internally.
// The storage format of TIMESTAMPTZ is the same as DATETIMEV2, both are 8-byte integers
// representing microseconds from 0001-01-01 00:00:00.000000 to 9999-12-31 23:59:59.999999.
// TIMESTAMPTZ does not store timezone information; conversions are performed during read and write
// operations according to the specified timezone.
// This requires that both reading and writing operations need a timezone parameter.
// Therefore, we implement a separate TIMESTAMPTZ type to prevent misuse.

class TimestampTzValue {
public:
    using underlying_value = uint64_t;
    const static TimestampTzValue DEFAULT_VALUE;

    explicit TimestampTzValue(underlying_value u64) : _utc_dt(u64) {}

    TimestampTzValue() : _utc_dt(MIN_DATETIME_V2) {}

    TimestampTzValue(const DateV2Value<DateTimeV2ValueType>& dt) : _utc_dt(dt) {}

    DateV2Value<DateTimeV2ValueType> utc_dt() const { return _utc_dt; }

    // Returns an integer value for storage in a column
    underlying_value to_date_int_val() const { return _utc_dt.to_date_int_val(); }

    // Outputs a string representation with timezone information in the format +03:00
    std::string to_string(const cctz::time_zone& local_time_zone, int scale = 6) const;

    // Parses a string, CastParameters can control whether strict mode is used
    bool from_string(const StringRef& str, const cctz::time_zone* local_time_zone,
                     vectorized::CastParameters& params, uint32_t to_scale);

    // Converts from a datetime value
    bool from_datetime(const DateV2Value<DateTimeV2ValueType>& dt,
                       const cctz::time_zone& local_time_zone, int dt_scale, int tz_scale);

    // Converts to a datetime value
    bool to_datetime(DateV2Value<DateTimeV2ValueType>& dt, const cctz::time_zone& local_time_zone,
                     int dt_scale, int tz_scale) const;

    // Default column value (since the default value 0 for UInt64 is not a valid datetime)
    static underlying_value default_column_value() { return MIN_DATETIME_V2; }

    // Check if the datetime part is valid
    bool is_valid_date() const { return _utc_dt.is_valid_date(); }

    uint16_t year() const { return _utc_dt.year(); }
    uint8_t month() const { return _utc_dt.month(); }
    uint8_t day() const { return _utc_dt.day(); }
    uint8_t hour() const { return _utc_dt.hour(); }
    uint8_t minute() const { return _utc_dt.minute(); }
    uint8_t second() const { return _utc_dt.second(); }
    uint32_t microsecond() const { return _utc_dt.microsecond(); }
    int64_t daynr() const { return _utc_dt.daynr(); }
    int quarter() const { return _utc_dt.quarter(); }

    // Methods needed for time rounding
    int64_t datetime_diff_in_seconds(const TimestampTzValue& other) const {
        return _utc_dt.datetime_diff_in_seconds(other._utc_dt);
    }

    template <TimeUnit unit>
    bool date_set_interval(const TimeInterval& interval) {
        return _utc_dt.date_set_interval<unit>(interval);
    }

    template <TimeUnit unit>
    void unchecked_set_time_unit(uint32_t value) {
        _utc_dt.unchecked_set_time_unit<unit>(value);
    }

    void unchecked_set_time(uint16_t year, uint8_t month, uint8_t day, uint8_t hour, uint8_t minute,
                            uint8_t second, uint32_t microsecond = 0) {
        _utc_dt.unchecked_set_time(year, month, day, hour, minute, second, microsecond);
    }

    bool check_range_and_set_time(uint16_t year, uint8_t month, uint8_t day, uint8_t hour,
                                  uint8_t minute, uint8_t second, uint32_t microsecond = 0) {
        return _utc_dt.check_range_and_set_time(year, month, day, hour, minute, second,
                                                microsecond);
    }

    void set_int_val(underlying_value value) { _utc_dt.set_int_val(value); }

    // Special constant for first day
    static const TimestampTzValue FIRST_DAY;

    template <TimeUnit unit, bool need_check = true>
    bool date_add_interval(const TimeInterval& interval) {
        return _utc_dt.date_add_interval<unit, need_check>(interval);
    }

    // truncate datetime to specified unit
    template <TimeUnit unit>
    bool datetime_trunc() {
        return _utc_dt.datetime_trunc<unit>();
    }

    void from_unixtime(int64_t timestamp, const cctz::time_zone& ctz) {
        _utc_dt.from_unixtime(timestamp, ctz);
    }

    void set_microsecond(uint64_t microsecond) { _utc_dt.set_microsecond(microsecond); }

    void unix_timestamp(int64_t* timestamp, const cctz::time_zone& ctz) const {
        _utc_dt.unix_timestamp(timestamp, ctz);
    }

    // Convert UTC time to local time based on the given timezone
    void convert_utc_to_local(const cctz::time_zone& local_time_zone,
                              DateV2Value<DateTimeV2ValueType>& dt) const;

    // Convert local time to UTC time based on the given timezone
    void convert_local_to_utc(const cctz::time_zone& local_time_zone,
                              const DateV2Value<DateTimeV2ValueType>& dt);

    TimestampTzValue& operator++() {
        ++_utc_dt;
        return *this;
    }

    TimestampTzValue& operator--() {
        --_utc_dt;
        return *this;
    }

    TimestampTzValue& operator+=(int64_t rhs) {
        _utc_dt += rhs;
        return *this;
    }

    TimestampTzValue& operator-=(int64_t rhs) {
        _utc_dt -= rhs;
        return *this;
    }

    bool operator==(const TimestampTzValue& rhs) const { return _utc_dt == rhs._utc_dt; }

    bool operator!=(const TimestampTzValue& rhs) const { return _utc_dt != rhs._utc_dt; }

    bool operator<(const TimestampTzValue& rhs) const { return _utc_dt < rhs._utc_dt; }

    bool operator<=(const TimestampTzValue& rhs) const { return _utc_dt <= rhs._utc_dt; }

    bool operator>(const TimestampTzValue& rhs) const { return _utc_dt > rhs._utc_dt; }

    bool operator>=(const TimestampTzValue& rhs) const { return _utc_dt >= rhs._utc_dt; }

private:
    DateV2Value<DateTimeV2ValueType> _utc_dt;
};
inline const TimestampTzValue TimestampTzValue::DEFAULT_VALUE =
        TimestampTzValue(DateV2Value<DateTimeV2ValueType>::DEFAULT_VALUE.to_date_int_val());

} // namespace doris

template <>
struct std::hash<doris::TimestampTzValue> {
    size_t operator()(const doris::TimestampTzValue& v) const {
        auto int_val = v.to_date_int_val();
        return doris::HashUtil::hash(&int_val, sizeof(int_val), 0);
    }
};