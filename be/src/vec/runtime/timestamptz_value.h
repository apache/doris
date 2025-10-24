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
#include <string>

#include "common/status.h"
#include "vec/common/string_ref.h"
#include "vec/runtime/vdatetime_value.h"

namespace doris {

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
    explicit TimestampTzValue(underlying_value u64) : _utc_dt(u64) {}

    TimestampTzValue() : _utc_dt(MIN_DATETIME_V2) {}

    // Returns an integer value for storage in a column
    underlying_value value() const { return _utc_dt.to_date_int_val(); }

    // Outputs a string representation with timezone information in the format +03:00
    std::string to_string(const cctz::time_zone& local_time_zone, int scale = 6) const;

    // Parses a string, CastParameters can control whether strict mode is used
    bool from_string(const StringRef& str, const cctz::time_zone* local_time_zone,
                     vectorized::CastParameters& params);

    // Converts from a datetime value
    bool from_datetime(const DateV2Value<DateTimeV2ValueType>& dt,
                       const cctz::time_zone& local_time_zone, int dt_scale, int tz_scale);

    // Converts to a datetime value
    bool to_datetime(DateV2Value<DateTimeV2ValueType>& dt, const cctz::time_zone& local_time_zone,
                     int dt_scale, int tz_scale) const;

    // Default column value (since the default value 0 for UInt64 is not a valid datetime)
    static underlying_value default_column_value() { return MIN_DATETIME_V2; }

private:
    DateV2Value<DateTimeV2ValueType> _utc_dt;
};

// for ut test
#ifdef BE_TEST
inline auto make_datetime(int year, int month, int day, int hour, int minute, int second,
                          int microsecond) {
    DateV2Value<DateTimeV2ValueType> dt;
    dt.unchecked_set_time(year, month, day, hour, minute, second, microsecond);
    return dt.to_date_int_val();
}
#endif
} // namespace doris
