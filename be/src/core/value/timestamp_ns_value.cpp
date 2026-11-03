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

#include "core/type_limit.h"
#include "exec/common/int_exp.h"

namespace doris {

TimeStampNsValue type_limit<TimeStampNsValue>::min() {
    return TimeStampNsValue(std::numeric_limits<int64_t>::min());
}

TimeStampNsValue type_limit<TimeStampNsValue>::max() {
    return TimeStampNsValue(std::numeric_limits<int64_t>::max());
}

DateV2Value<DateV2ValueType> TimeStampNsValue::to_date() const {
    DateV2Value<DateV2ValueType> value;
    [[maybe_unused]] const bool valid = value.get_date_from_daynr(static_cast<uint64_t>(daynr()));
    DCHECK(valid);
    return value;
}

DateV2Value<DateTimeV2ValueType> TimeStampNsValue::to_datetime() const {
    int64_t days = _epoch_nanos / NANOS_PER_DAY;
    int64_t nanos_of_day = _epoch_nanos % NANOS_PER_DAY;
    if (nanos_of_day < 0) {
        nanos_of_day += NANOS_PER_DAY;
        --days;
    }

    DateV2Value<DateTimeV2ValueType> value;
    [[maybe_unused]] const bool valid =
            value.get_date_from_daynr(static_cast<uint64_t>(UNIX_EPOCH_DAYNR + days));
    DCHECK(valid);

    int64_t seconds = nanos_of_day / NANOS_PER_SECOND;
    const auto hour = static_cast<uint8_t>(seconds / 3600);
    seconds %= 3600;
    const auto minute = static_cast<uint8_t>(seconds / 60);
    const auto second = static_cast<uint16_t>(seconds % 60);
    const auto microsecond =
            static_cast<uint32_t>(nanos_of_day % NANOS_PER_SECOND / NANOS_PER_MICROSECOND);
    value.unchecked_set_time(value.year(), value.month(), value.day(), hour, minute, second,
                             microsecond);
    return value;
}

bool TimeStampNsValue::from_datetime(const DateV2Value<DateTimeV2ValueType>& value,
                                     uint16_t nanosecond_remainder) {
    DORIS_CHECK_LE(nanosecond_remainder, 999);
    const int64_t seconds =
            (value.daynr() - UNIX_EPOCH_DAYNR) * SECONDS_PER_DAY + value.time_part_to_seconds();
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

int32_t TimeStampNsValue::to_buffer(char* buffer) const {
    const auto value = to_datetime();
    const int32_t base_length = value.to_buffer(buffer, 0);
    buffer[base_length] = '.';
    uint32_t nanos = nanosecond();
    // Consume the fraction from most significant to least significant digit. Deliberately do not
    // round here: type normalization happens while parsing/casting, before a value is stored.
    for (int i = 0; i < FRACTIONAL_DIGITS; ++i) {
        const auto divisor = static_cast<uint32_t>(int_exp10(FRACTIONAL_DIGITS - 1 - i));
        buffer[base_length + 1 + i] = static_cast<char>('0' + nanos / divisor);
        nanos %= divisor;
    }
    return base_length + 1 + FRACTIONAL_DIGITS;
}

} // namespace doris
