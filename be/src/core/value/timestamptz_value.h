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
#include "core/value/vdatetime_value.h"

namespace doris {

struct StringRef;

struct CastParameters;

// TIMESTAMPTZ can be understood as a DATETIME type with timezone conversion functionality.
// Doris automatically handles timezone conversions internally.
// The storage format of TIMESTAMPTZ is the same as DATETIMEV2, both are 8-byte integers
// representing microseconds from 0001-01-01 00:00:00.000000 to 9999-12-31 23:59:59.999999.
// TIMESTAMPTZ does not store timezone information; conversions are performed during read and write
// operations according to the specified timezone.
// This requires that both reading and writing operations need a timezone parameter.
// Therefore, we implement a separate TIMESTAMPTZ type to prevent misuse.
//
// TimestampTzValue inherits from DateV2ValueBase<DateTimeV2ValueType> which provides all
// date/time computation methods (date_add_interval, datetime_trunc, from_unixtime, etc.)
// without delegation. TimestampTzValue adds only timezone-aware methods.

class TimestampTzValue : public DateV2ValueBase<DateTimeV2ValueType> {
    using Base = DateV2ValueBase<DateTimeV2ValueType>;

public:
    using underlying_value = uint64_t;
    const static TimestampTzValue DEFAULT_VALUE;
    static const TimestampTzValue FIRST_DAY;

    TimestampTzValue() : Base(MIN_DATETIME_V2) {}

    explicit TimestampTzValue(underlying_value u64) : Base(u64) {}

    TimestampTzValue(const DateV2Value<DateTimeV2ValueType>& dt) // NOLINT: implicit from DateTimeV2
            : Base(dt.to_date_int_val()) {}

    // Return a DateTimeV2 copy of the UTC datetime value (for backward compatibility)
    DateV2Value<DateTimeV2ValueType> utc_dt() const {
        return DateTimeV2(to_date_int_val());
    }

    // Bring base class to_string(char*) into scope (needed by operator<< and other generic code)
    using Base::to_string;

    // Outputs a string representation with timezone information in the format +03:00
    std::string to_string(const cctz::time_zone& local_time_zone, int scale = 6) const;

    // Parses a string, CastParameters can control whether strict mode is used
    bool from_string(const StringRef& str, const cctz::time_zone* local_time_zone,
                     CastParameters& params, uint32_t to_scale);

    // Converts from a datetime value
    bool from_datetime(const DateV2Value<DateTimeV2ValueType>& dt,
                       const cctz::time_zone& local_time_zone, int dt_scale, int tz_scale);

    // Converts to a datetime value
    bool to_datetime(DateV2Value<DateTimeV2ValueType>& dt, const cctz::time_zone& local_time_zone,
                     int dt_scale, int tz_scale) const;

    // Default column value (since the default value 0 for UInt64 is not a valid datetime)
    static underlying_value default_column_value() { return MIN_DATETIME_V2; }

    // Convert UTC time to local time based on the given timezone
    void convert_utc_to_local(const cctz::time_zone& local_time_zone,
                              DateV2Value<DateTimeV2ValueType>& dt) const;

    // Convert local time to UTC time based on the given timezone
    void convert_local_to_utc(const cctz::time_zone& local_time_zone,
                              const DateV2Value<DateTimeV2ValueType>& dt);

    // Override self-returning operators to return TimestampTzValue&
    TimestampTzValue& operator+=(int64_t rhs) {
        Base::operator+=(rhs);
        return *this;
    }
    TimestampTzValue& operator-=(int64_t rhs) { return *this += -rhs; }
    TimestampTzValue& operator++() { return *this += 1; }
    TimestampTzValue& operator--() { return *this += -1; }
};
inline const TimestampTzValue TimestampTzValue::DEFAULT_VALUE =
        TimestampTzValue(DateV2Value<DateTimeV2ValueType>::DEFAULT_VALUE.to_date_int_val());

static_assert(std::is_trivially_destructible_v<TimestampTzValue>,
              "TimestampTzValue must be trivially destructible");
static_assert(std::is_trivially_copyable_v<TimestampTzValue>,
              "TimestampTzValue must be trivially copyable");
static_assert(sizeof(TimestampTzValue) == sizeof(uint64_t),
              "TimestampTzValue must be 8 bytes");

} // namespace doris

template <>
struct std::hash<doris::TimestampTzValue> {
    size_t operator()(const doris::TimestampTzValue& v) const {
        auto int_val = v.to_date_int_val();
        return doris::HashUtil::hash(&int_val, sizeof(int_val), 0);
    }
};