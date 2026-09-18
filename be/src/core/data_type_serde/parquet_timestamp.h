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
#include <limits>

#include "common/status.h"
#include "core/data_type_serde/parquet_decode_source.h"

namespace doris {

#pragma pack(1)
struct ParquetInt96Timestamp {
    int64_t nanos_of_day;
    int32_t julian_day;
};
#pragma pack()
static_assert(sizeof(ParquetInt96Timestamp) == 12);

// Doris DATETIMEV2 and TIMESTAMPTZ both start at 0000-01-01 00:00:00 (`MIN_DATE_V2`) and end at
// 9999-12-31 23:59:59.999999. Parquet timestamps count from 1970-01-01 in the proleptic Gregorian
// calendar, where year 0 IS a leap year, so 0000-01-01 is 366 days below 0001-01-01 rather than
// 365. Pinning the lower bound at 0001-01-01 made the reader stricter than the type it
// materializes into: every year-zero value Doris itself writes came back as a conversion failure.
inline constexpr int64_t MIN_DORIS_TIMESTAMP_MICROS = -62167219200000000LL; // 0000-01-01 00:00:00
inline constexpr int64_t MAX_DORIS_TIMESTAMP_MICROS =
        253402300799999999LL; // 9999-12-31 23:59:59.999999

// A UTC-adjusted Parquet timestamp is an instant, not a civil value: which Doris DATETIME it
// becomes depends on the reader's timezone, so the raw instant must not be measured against the
// civil range. A local 0001-01-01 00:00:00 in Asia/Shanghai is an instant *below* the civil
// minimum, and a local 9999-12-31 23:59:59 in America/New_York is one *above* the maximum; both
// are representable and must survive. What stays here is only a coarse guard keeping the value in
// the domain where the cctz conversion and the narrowing to a `uint16_t` year behave. No timezone
// offset has ever exceeded 16 hours, so one day of slack admits every instant that can still land
// inside the civil range. The exact range is enforced per target type after the conversion:
// `epoch_days_to_daynr()` for a civil timestamp, `is_valid_date()` for an instant.
inline constexpr int64_t DORIS_TIMESTAMP_OFFSET_SLACK_MICROS = 86400000000LL;
inline constexpr int64_t MIN_DORIS_INSTANT_MICROS =
        MIN_DORIS_TIMESTAMP_MICROS - DORIS_TIMESTAMP_OFFSET_SLACK_MICROS;
inline constexpr int64_t MAX_DORIS_INSTANT_MICROS =
        MAX_DORIS_TIMESTAMP_MICROS + DORIS_TIMESTAMP_OFFSET_SLACK_MICROS;

inline Status validate_parquet_timestamp_micros(int64_t timestamp_micros) {
    if (timestamp_micros < MIN_DORIS_INSTANT_MICROS ||
        timestamp_micros > MAX_DORIS_INSTANT_MICROS) {
        return Status::DataQualityError(
                "Parquet timestamp is outside the Doris 0000-9999 range: micros={}",
                timestamp_micros);
    }
    return Status::OK();
}

inline Status parquet_timestamp_micros(ParquetTimeUnit unit, int64_t value, int64_t* result) {
    if (unit == ParquetTimeUnit::MILLIS) {
        // Validate in the source unit before scaling; signed overflow could otherwise turn an
        // invalid file value into an in-range timestamp that survives later range checks.
        if (value > std::numeric_limits<int64_t>::max() / 1000 ||
            value < std::numeric_limits<int64_t>::min() / 1000) {
            return Status::DataQualityError("Parquet timestamp overflows microseconds");
        }
        *result = value * 1000;
    } else if (unit == ParquetTimeUnit::NANOS) {
        *result = value / 1000;
        // Epoch-relative negative timestamps must round toward the preceding representable
        // microsecond; C++ division truncates toward zero and would move pre-epoch values forward.
        if (value < 0 && value % 1000 != 0) {
            --*result;
        }
    } else {
        *result = value;
    }
    return validate_parquet_timestamp_micros(*result);
}

inline Status parquet_int96_timestamp_micros(const ParquetInt96Timestamp& value, int64_t* result) {
    static constexpr int32_t JULIAN_EPOCH_OFFSET_DAYS = 2440588;
    static constexpr int64_t MICROS_IN_DAY = 86400000000LL;
    static constexpr int64_t NANOS_IN_DAY = 86400000000000LL;
    static constexpr int64_t NANOS_PER_MICROSECOND = 1000;

    // INT96 nanos is a time-of-day, not a signed duration. Validate it before widened day
    // arithmetic so corrupt input cannot wrap into a plausible Doris timestamp.
    if (value.nanos_of_day < 0 || value.nanos_of_day >= NANOS_IN_DAY) {
        return Status::DataQualityError("Invalid Parquet INT96 nanos-of-day: {}",
                                        value.nanos_of_day);
    }
    const __int128 days = static_cast<int64_t>(value.julian_day) - JULIAN_EPOCH_OFFSET_DAYS;
    const __int128 timestamp_micros =
            days * MICROS_IN_DAY + value.nanos_of_day / NANOS_PER_MICROSECOND;
    if (timestamp_micros < MIN_DORIS_INSTANT_MICROS ||
        timestamp_micros > MAX_DORIS_INSTANT_MICROS) {
        return Status::DataQualityError(
                "Parquet INT96 timestamp is outside the Doris 0000-9999 range");
    }
    *result = static_cast<int64_t>(timestamp_micros);
    return Status::OK();
}

} // namespace doris
