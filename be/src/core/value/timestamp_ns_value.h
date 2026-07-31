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

#include "core/value/vdatetime_value.h"

namespace doris {

// TIMESTAMP_NS uses a signed Int64 nanosecond offset from the Unix epoch. UTC is used only as a
// stable mapping between timezone-naive civil fields and the physical integer representation.
class TimeStampNsValue {
public:
    static constexpr int64_t NANOS_PER_SECOND = 1000000000;
    static constexpr int64_t NANOS_PER_MILLISECOND = 1000000;
    static constexpr int64_t NANOS_PER_MICROSECOND = 1000;

    constexpr TimeStampNsValue() = default;
    explicit constexpr TimeStampNsValue(int64_t epoch_nanos) : _epoch_nanos(epoch_nanos) {}

    constexpr int64_t to_date_int_val() const { return _epoch_nanos; }
    constexpr int64_t epoch_nanos() const { return _epoch_nanos; }

    int64_t epoch_seconds() const {
        int64_t seconds = _epoch_nanos / NANOS_PER_SECOND;
        if (_epoch_nanos % NANOS_PER_SECOND < 0) {
            --seconds;
        }
        return seconds;
    }

    uint32_t nanosecond() const {
        int64_t nanos = _epoch_nanos % NANOS_PER_SECOND;
        if (nanos < 0) {
            nanos += NANOS_PER_SECOND;
        }
        return static_cast<uint32_t>(nanos);
    }

    uint32_t microsecond() const { return nanosecond() / 1000; }
    uint16_t nanosecond_remainder() const { return static_cast<uint16_t>(nanosecond() % 1000); }

    DateV2Value<DateTimeV2ValueType> to_datetime() const;
    bool from_datetime(const DateV2Value<DateTimeV2ValueType>& value,
                       uint16_t nanosecond_remainder = 0);

    uint16_t year() const { return to_datetime().year(); }
    uint8_t month() const { return to_datetime().month(); }
    uint8_t day() const { return to_datetime().day(); }
    uint8_t hour() const { return to_datetime().hour(); }
    uint8_t minute() const { return to_datetime().minute(); }
    uint8_t second() const { return to_datetime().second(); }
    int quarter() const { return to_datetime().quarter(); }
    int64_t daynr() const { return to_datetime().daynr(); }
    uint16_t year_of_week() const { return to_datetime().year_of_week(); }
    uint8_t week(uint8_t mode) const { return to_datetime().week(mode); }
    uint32_t year_week(uint8_t mode) const { return to_datetime().year_week(mode); }
    int day_of_year() const { return to_datetime().day_of_year(); }
    int day_of_week() const { return to_datetime().day_of_week(); }
    uint8_t weekday() const { return to_datetime().weekday(); }
    int64_t time_part_to_seconds() const { return to_datetime().time_part_to_seconds(); }
    int64_t time_part_to_microsecond() const {
        return time_part_to_seconds() * 1000000 + microsecond();
    }

    template <typename RHS>
    int64_t time_part_diff_in_ms(const RHS& rhs) const {
        return time_part_to_microsecond() - rhs.time_part_to_microsecond();
    }

    template <typename RHS>
    int64_t datetime_diff_in_seconds(const RHS& rhs) const {
        return (daynr() - rhs.daynr()) * SECOND_PER_HOUR * HOUR_PER_DAY + time_part_to_seconds() -
               rhs.time_part_to_seconds();
    }

    template <typename RHS>
    int64_t datetime_diff_in_microseconds(const RHS& rhs) const {
        if constexpr (std::is_same_v<std::remove_cvref_t<RHS>, TimeStampNsValue>) {
            return static_cast<int64_t>((static_cast<__int128>(_epoch_nanos) - rhs._epoch_nanos) /
                                        1000);
        }
        return (daynr() - rhs.daynr()) * HOUR_PER_DAY * SECOND_PER_HOUR * MS_PER_SECOND +
               time_part_diff_in_ms(rhs);
    }

    template <typename RHS>
    int32_t date_diff_in_days(const RHS& rhs) const {
        return static_cast<int32_t>(daynr() - rhs.daynr());
    }

    int32_t date_diff_in_days_round_to_zero_by_time(const auto& rhs) const {
        int32_t days = date_diff_in_days(rhs);
        const int64_t time_diff = time_part_diff_in_ms(rhs);
        if (days > 0 && time_diff < 0) {
            --days;
        } else if (days < 0 && time_diff > 0) {
            ++days;
        }
        return days;
    }

    int64_t datetime_diff_in_seconds_round_to_zero_by_ms(const auto& rhs) const {
        int64_t seconds = datetime_diff_in_seconds(rhs);
        const int64_t fraction_diff = static_cast<int64_t>(nanosecond()) - rhs.nanosecond();
        if (seconds > 0 && fraction_diff < 0) {
            --seconds;
        } else if (seconds < 0 && fraction_diff > 0) {
            ++seconds;
        }
        return seconds;
    }

    const char* day_name_with_locale(const char* const* day_names) const {
        return to_datetime().day_name_with_locale(day_names);
    }
    const char* month_name_with_locale(const char* const* month_names) const {
        return to_datetime().month_name_with_locale(month_names);
    }
    bool to_format_string_conservative(const char* format, size_t len, char* to,
                                       size_t max_valid_length) const {
        return to_datetime().to_format_string_conservative(format, len, to, max_valid_length);
    }

    bool is_valid_date() const { return true; }

    template <TimeUnit unit>
    bool date_add_interval(const TimeInterval& interval) {
        auto value = to_datetime();
        const uint16_t remainder = nanosecond_remainder();
        if (!value.date_add_interval<unit>(interval)) {
            return false;
        }
        return from_datetime(value, remainder);
    }

    template <TimeUnit unit>
    bool datetime_trunc() {
        auto value = to_datetime();
        if (!value.template datetime_trunc<unit>()) {
            return false;
        }
        return from_datetime(value);
    }

    int32_t to_buffer(char* buffer, int scale = 9) const;
    char* to_string(char* buffer, int scale = 9) const {
        const int32_t length = to_buffer(buffer, scale);
        buffer[length] = '\0';
        return buffer + length + 1;
    }
    std::string to_string(int scale = 9) const {
        char buffer[40];
        const int32_t length = to_buffer(buffer, scale);
        return {buffer, static_cast<size_t>(length)};
    }

    auto operator<=>(const TimeStampNsValue&) const = default;

    TimeStampNsValue& operator+=(int64_t seconds) {
        int64_t delta = 0;
        DORIS_CHECK(!__builtin_mul_overflow(seconds, NANOS_PER_SECOND, &delta));
        DORIS_CHECK(!__builtin_add_overflow(_epoch_nanos, delta, &_epoch_nanos));
        return *this;
    }

    TimeStampNsValue& operator-=(int64_t seconds) {
        DORIS_CHECK_NE(seconds, std::numeric_limits<int64_t>::min());
        return *this += -seconds;
    }

    uint32_t hash(int seed) const {
        return HashUtil::hash(&_epoch_nanos, sizeof(_epoch_nanos), seed);
    }

private:
    int64_t _epoch_nanos = 0;
};

static_assert(sizeof(TimeStampNsValue) == sizeof(int64_t));
static_assert(std::is_trivially_copyable_v<TimeStampNsValue>);

template <TimeUnit UNIT>
int64_t datetime_diff(const TimeStampNsValue& ts_value1, const TimeStampNsValue& ts_value2) {
    const auto time_key = [](const TimeStampNsValue& value, bool include_month) {
        int64_t result = include_month ? value.month() : 0;
        result = result * 32 + value.day();
        result = result * 24 + value.hour();
        result = result * 60 + value.minute();
        result = result * 60 + value.second();
        return result * TimeStampNsValue::NANOS_PER_SECOND + value.nanosecond();
    };
    if constexpr (UNIT == YEAR) {
        int year = ts_value2.year() - ts_value1.year();
        const int64_t remainder1 = time_key(ts_value1, true);
        const int64_t remainder2 = time_key(ts_value2, true);
        if (year > 0) {
            year -= remainder2 < remainder1;
        } else if (year < 0) {
            year += remainder2 > remainder1;
        }
        return year;
    } else if constexpr (UNIT == QUARTER || UNIT == MONTH) {
        int month = (ts_value2.year() - ts_value1.year()) * 12 +
                    (ts_value2.month() - ts_value1.month());
        const int64_t remainder1 = time_key(ts_value1, false);
        const int64_t remainder2 = time_key(ts_value2, false);
        if (month > 0) {
            month -= remainder2 < remainder1;
        } else if (month < 0) {
            month += remainder2 > remainder1;
        }
        return UNIT == QUARTER ? month / 3 : month;
    } else if constexpr (UNIT == WEEK || UNIT == DAY) {
        int64_t day = ts_value2.daynr() - ts_value1.daynr();
        const int64_t time1 =
                ts_value1.time_part_to_seconds() * TimeStampNsValue::NANOS_PER_SECOND +
                ts_value1.nanosecond();
        const int64_t time2 =
                ts_value2.time_part_to_seconds() * TimeStampNsValue::NANOS_PER_SECOND +
                ts_value2.nanosecond();
        if (day > 0) {
            day -= time2 < time1;
        } else if (day < 0) {
            day += time2 > time1;
        }
        return UNIT == WEEK ? day / 7 : day;
    } else {
        constexpr int64_t divisor = UNIT == HOUR     ? 3600 * TimeStampNsValue::NANOS_PER_SECOND
                                    : UNIT == MINUTE ? 60 * TimeStampNsValue::NANOS_PER_SECOND
                                    : UNIT == SECOND ? TimeStampNsValue::NANOS_PER_SECOND
                                    : UNIT == MILLISECOND ? 1000000
                                                          : 1000;
        static_assert(UNIT == HOUR || UNIT == MINUTE || UNIT == SECOND || UNIT == MILLISECOND ||
                              UNIT == MICROSECOND,
                      "Unsupported TimeUnit for datetime_diff");
        return static_cast<int64_t>(
                (static_cast<__int128>(ts_value2.epoch_nanos()) - ts_value1.epoch_nanos()) /
                divisor);
    }
}

} // namespace doris

template <>
struct std::hash<doris::TimeStampNsValue> {
    size_t operator()(const doris::TimeStampNsValue& value) const {
        return std::hash<int64_t> {}(value.epoch_nanos());
    }
};
