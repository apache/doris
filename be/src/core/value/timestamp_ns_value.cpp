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

#include "core/value/timestamp_ns_value.h"

#include <cctz/time_zone.h>

#include "exec/common/int_exp.h"

namespace doris {

DateV2Value<DateTimeV2ValueType> TimeStampNsValue::to_datetime() const {
    // epoch_seconds() is floor-divided, so set_microsecond() always receives the first six digits
    // of a non-negative fractional second even for timestamps before the epoch.
    DateV2Value<DateTimeV2ValueType> value;
    value.from_unixtime(epoch_seconds(), cctz::utc_time_zone());
    value.set_microsecond(microsecond());
    return value;
}

bool TimeStampNsValue::from_datetime(const DateV2Value<DateTimeV2ValueType>& value,
                                     uint16_t nanosecond_remainder) {
    DORIS_CHECK_LE(nanosecond_remainder, 999);
    int64_t seconds = 0;
    value.unix_timestamp(&seconds, cctz::utc_time_zone());
    // The civil adapter and remainder form one exact epoch-nanosecond value. Int128 is required:
    // valid boundary dates overflow Int64 during the intermediate seconds-to-nanoseconds product.
    const __int128 epoch_nanos = static_cast<__int128>(seconds) * NANOS_PER_SECOND +
                                 static_cast<__int128>(value.microsecond()) * 1000 +
                                 nanosecond_remainder;
    if (epoch_nanos < std::numeric_limits<int64_t>::min() ||
        epoch_nanos > std::numeric_limits<int64_t>::max()) {
        return false;
    }
    _epoch_nanos = static_cast<int64_t>(epoch_nanos);
    return true;
}

int32_t TimeStampNsValue::to_buffer(char* buffer, int scale) const {
    DORIS_CHECK_GE(scale, 0);
    DORIS_CHECK_LE(scale, 9);
    const auto value = to_datetime();
    const int32_t base_length = value.to_buffer(buffer, 0);
    if (scale == 0) {
        return base_length;
    }
    buffer[base_length] = '.';
    uint32_t nanos = nanosecond();
    // Consume the fraction from most significant to least significant digit. Deliberately do not
    // round here: type normalization happens while parsing/casting, before a value is stored.
    for (int i = 0; i < scale; ++i) {
        buffer[base_length + 1 + i] =
                static_cast<char>('0' + nanos / static_cast<uint32_t>(int_exp10(8 - i)));
        nanos %= static_cast<uint32_t>(int_exp10(8 - i));
    }
    return base_length + 1 + scale;
}

} // namespace doris
