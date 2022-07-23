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

#include <re2/re2.h>
#include <stdint.h>

#include <chrono>
#include <cstddef>
#include <iostream>

#include "cctz/civil_time.h"
#include "cctz/time_zone.h"
#include "udf/udf.h"
#include "util/hash_util.hpp"
#include "util/timezone_utils.h"
#include "vec/runtime/vdatetime_value.h"

namespace doris {

enum TimeUnit {
    MICROSECOND,
    SECOND,
    MINUTE,
    HOUR,
    DAY,
    WEEK,
    MONTH,
    QUARTER,
    YEAR,
    SECOND_MICROSECOND,
    MINUTE_MICROSECOND,
    MINUTE_SECOND,
    HOUR_MICROSECOND,
    HOUR_SECOND,
    HOUR_MINUTE,
    DAY_MICROSECOND,
    DAY_SECOND,
    DAY_MINUTE,
    DAY_HOUR,
    YEAR_MONTH
};

struct TimeInterval {
    int64_t year;
    int64_t month;
    int64_t day;
    int64_t hour;
    int64_t minute;
    int64_t second;
    int64_t microsecond;
    bool is_neg;

    TimeInterval()
            : year(0),
              month(0),
              day(0),
              hour(0),
              minute(0),
              second(0),
              microsecond(0),
              is_neg(false) {}

    TimeInterval(TimeUnit unit, int64_t count, bool is_neg_param)
            : year(0),
              month(0),
              day(0),
              hour(0),
              minute(0),
              second(0),
              microsecond(0),
              is_neg(is_neg_param) {
        switch (unit) {
        case YEAR:
            year = count;
            break;
        case MONTH:
            month = count;
            break;
        case WEEK:
            day = 7 * count;
            break;
        case DAY:
            day = count;
            break;
        case HOUR:
            hour = count;
            break;
        case MINUTE:
            minute = count;
            break;
        case SECOND:
            second = count;
            break;
        case MICROSECOND:
            microsecond = count;
            break;
        default:
            break;
        }
    }
};

enum TimeType { TIME_TIME = 1, TIME_DATE = 2, TIME_DATETIME = 3 };

// Used to compute week
const int WEEK_MONDAY_FIRST = 1;
const int WEEK_YEAR = 2;
const int WEEK_FIRST_WEEKDAY = 4;

// 9999-99-99 99:99:99.999999; 26 + 1('\0')
const int MAX_DTVALUE_STR_LEN = 27;

const int DATE_MAX_DAYNR = 3652424;
// two-digit years < this are 20..; >= this are 19..
const int YY_PART_YEAR = 70;

// Limits of time value
const int TIME_MAX_HOUR = 838;
const int TIME_MAX_MINUTE = 59;
const int TIME_MAX_SECOND = 59;
const int TIME_MAX_VALUE = 10000 * TIME_MAX_HOUR + 100 * TIME_MAX_MINUTE + TIME_MAX_SECOND;
const int TIME_MAX_VALUE_SECONDS = 3600 * TIME_MAX_HOUR + 60 * TIME_MAX_MINUTE + TIME_MAX_SECOND;

constexpr size_t const_length(const char* str) {
    return (str == nullptr || *str == 0) ? 0 : const_length(str + 1) + 1;
}

constexpr size_t max_char_length(const char* const* name, size_t end) {
    size_t res = 0;
    for (int i = 0; i < end; ++i) {
        res = std::max(const_length(name[i]), res);
    }
    return res;
}

static constexpr const char* s_month_name[] = {
        "",     "January", "February",  "March",   "April",    "May",      "June",
        "July", "August",  "September", "October", "November", "December", nullptr};

static constexpr const char* s_day_name[] = {"Monday", "Tuesday",  "Wednesday", "Thursday",
                                             "Friday", "Saturday", "Sunday",    nullptr};

static constexpr size_t MAX_DAY_NAME_LEN = max_char_length(s_day_name, std::size(s_day_name));
static constexpr size_t MAX_MONTH_NAME_LEN = max_char_length(s_month_name, std::size(s_month_name));

uint8_t mysql_week_mode(uint32_t mode);

class DateTimeValue {
public:
    // Constructor
    DateTimeValue()
            : _neg(0),
              _type(TIME_DATETIME),
              _hour(0),
              _minute(0),
              _second(0),
              _year(0),
              _month(0),
              _day(0),
              _microsecond(0) {}

    explicit DateTimeValue(int64_t t) { from_date_int64(t); }

    void set_time(uint32_t year, uint32_t month, uint32_t day, uint32_t hour, uint32_t minute,
                  uint32_t second, uint32_t microsecond);

    // Converted from Olap Date or Datetime
    bool from_olap_datetime(uint64_t datetime) {
        _neg = 0;
        _type = TIME_DATETIME;
        uint64_t date = datetime / 1000000;
        uint64_t time = datetime % 1000000;

        auto [year, month, day, hour, minute, second, microsecond] =
                std::tuple {0, 0, 0, 0, 0, 0, 0};
        year = date / 10000;
        date %= 10000;
        month = date / 100;
        day = date % 100;
        hour = time / 10000;
        time %= 10000;
        minute = time / 100;
        second = time % 100;
        microsecond = 0;

        return check_range_and_set_time(year, month, day, hour, minute, second, microsecond, _type);
    }

    uint64_t to_olap_datetime() const {
        uint64_t date_val = _year * 10000 + _month * 100 + _day;
        uint64_t time_val = _hour * 10000 + _minute * 100 + _second;
        return date_val * 1000000 + time_val;
    }

    bool from_olap_date(uint64_t date) {
        _neg = 0;
        _type = TIME_DATE;

        auto [year, month, day, hour, minute, second, microsecond] =
                std::tuple {0, 0, 0, 0, 0, 0, 0};

        day = date & 0x1f;
        date >>= 5;
        month = date & 0x0f;
        date >>= 4;
        year = date;

        return check_range_and_set_time(year, month, day, hour, minute, second, microsecond, _type);
    }

    uint64_t to_olap_date() const {
        uint64_t val;
        val = _year;
        val <<= 4;
        val |= _month;
        val <<= 5;
        val |= _day;
        return val;
    }

    bool from_date_format_str(const char* format, int format_len, const char* value,
                              int value_len) {
        memset(this, 0, sizeof(*this));
        return from_date_format_str(format, format_len, value, value_len, nullptr);
    }

    operator int64_t() const { return to_int64(); }

    // Given days since 0000-01-01, construct the datetime value.
    bool from_date_daynr(uint64_t);

    // Construct Date/Datetime type value from string.
    // At least the following formats are recogniced (based on number of digits)
    // 'YYMMDD', 'YYYYMMDD', 'YYMMDDHHMMSS', 'YYYYMMDDHHMMSS'
    // 'YY-MM-DD', 'YYYY-MM-DD', 'YY-MM-DD HH.MM.SS'
    // 'YYYYMMDDTHHMMSS'
    bool from_date_str(const char* str, int len);

    // Construct Date/Datetime type value from int64_t value.
    // Return true if convert success. Otherwise return false.
    bool from_date_int64(int64_t value);

    // Construct time type value from int64_t value.
    // Return true if convert success. Otherwise return false.
    bool from_time_int64(int64_t value);

    // Convert this value to string
    // this will check type to decide which format to convert
    // TIME:  format 'hh:mm:ss.xxxxxx'
    // DATE:  format 'YYYY-MM-DD'
    // DATETIME:  format 'YYYY-MM-DD hh:mm:ss.xxxxxx'
    int32_t to_buffer(char* buffer) const;

    char* to_string(char* to) const;

    // Convert this datetime value to string by the format string
    bool to_format_string(const char* format, int len, char* to) const;

    // compute the length of data format pattern
    static int compute_format_len(const char* format, int len);

    // Return true if range or date is invalid
    static bool check_range(uint32_t year, uint32_t month, uint32_t day, uint32_t hour,
                            uint32_t minute, uint32_t second, uint32_t microsecond, uint16_t type);

    static bool check_date(uint32_t year, uint32_t month, uint32_t day);

    // compute the diff between two datetime value
    template <TimeUnit unit>
    static int64_t datetime_diff(const DateTimeValue& ts_value1, const DateTimeValue& ts_value2) {
        switch (unit) {
        case YEAR: {
            int year = (ts_value2.year() - ts_value1.year());
            if (year > 0) {
                year -= (ts_value2.to_datetime_int64() % 10000000000 -
                         ts_value1.to_datetime_int64() % 10000000000) < 0;
            } else if (year < 0) {
                year += (ts_value2.to_datetime_int64() % 10000000000 -
                         ts_value1.to_datetime_int64() % 10000000000) > 0;
            }
            return year;
        }
        case MONTH: {
            int month = (ts_value2.year() - ts_value1.year()) * 12 +
                        (ts_value2.month() - ts_value1.month());
            if (month > 0) {
                month -= (ts_value2.to_datetime_int64() % 100000000 -
                          ts_value1.to_datetime_int64() % 100000000) < 0;
            } else if (month < 0) {
                month += (ts_value2.to_datetime_int64() % 100000000 -
                          ts_value1.to_datetime_int64() % 100000000) > 0;
            }
            return month;
        }
        case WEEK: {
            int day = ts_value2.daynr() - ts_value1.daynr();
            if (day > 0) {
                day -= ts_value2.time_part_diff(ts_value1) < 0;
            } else if (day < 0) {
                day += ts_value2.time_part_diff(ts_value1) > 0;
            }
            return day / 7;
        }
        case DAY: {
            int day = ts_value2.daynr() - ts_value1.daynr();
            if (day > 0) {
                day -= ts_value2.time_part_diff(ts_value1) < 0;
            } else if (day < 0) {
                day += ts_value2.time_part_diff(ts_value1) > 0;
            }
            return day;
        }
        case HOUR: {
            int64_t second = ts_value2.second_diff(ts_value1);
            int64_t hour = second / 60 / 60;
            return hour;
        }
        case MINUTE: {
            int64_t second = ts_value2.second_diff(ts_value1);
            int64_t minute = second / 60;
            return minute;
        }
        case SECOND: {
            int64_t second = ts_value2.second_diff(ts_value1);
            return second;
        }
        }
        // Rethink the default return value
        return 0;
    }

    // Convert this value to uint64_t
    // Will check its type
    int64_t to_int64() const;

    bool check_range_and_set_time(uint32_t year, uint32_t month, uint32_t day, uint32_t hour,
                                  uint32_t minute, uint32_t second, uint32_t microsecond,
                                  uint16_t type) {
        if (check_range(year, month, day, hour, minute, second, microsecond, type)) {
            return false;
        }
        set_time(year, month, day, hour, minute, second, microsecond);
        return true;
    };

    uint64_t daynr() const { return calc_daynr(_year, _month, _day); }

    // Calculate how many days since 0000-01-01
    // 0000-01-01 is 1st B.C.
    static uint64_t calc_daynr(uint32_t year, uint32_t month, uint32_t day);

    static uint8_t calc_weekday(uint64_t daynr, bool);

    int year() const { return _year; }
    int month() const { return _month; }
    int quarter() const { return (_month - 1) / 3 + 1; }
    int day() const { return _day; }
    int hour() const { return _hour; }
    int minute() const { return _minute; }
    int second() const { return _second; }
    int microsecond() const { return _microsecond; }
    int neg() const { return _neg; }

    bool check_loss_accuracy_cast_to_date() {
        auto loss_accuracy = _hour != 0 || _minute != 0 || _second != 0 || _microsecond != 0;
        cast_to_date();
        return loss_accuracy;
    }

    void cast_to_date() {
        _hour = 0;
        _minute = 0;
        _second = 0;
        _microsecond = 0;
        _type = TIME_DATE;
    }

    void cast_to_time() {
        _year = 0;
        _month = 0;
        _day = 0;
        _type = TIME_TIME;
    }

    void to_datetime() { _type = TIME_DATETIME; }

    // Weekday, from 0(Mon) to 6(Sun)
    uint8_t weekday() const { return calc_weekday(daynr(), false); }
    auto day_of_week() const { return (weekday() + 1) % 7 + 1; }

    // The bits in week_format has the following meaning:
    // WEEK_MONDAY_FIRST (0)
    //  If not set:
    //      Sunday is first day of week
    //  If set:
    //      Monday is first day of week
    //
    // WEEK_YEAR (1)
    //  If not set:
    //      Week is in range 0-53
    //      Week 0 is returned for the last week of the previous year (for
    //      a date at start of january) In this case one can get 53 for the
    //      first week of next year.  This flag ensures that the week is
    //      relevant for the given year. Note that this flag is only
    //      relevant if WEEK_JANUARY is not set.
    //  If set:
    //      Week is in range 1-53.
    //      In this case one may get week 53 for a date in January (when
    //      the week is that last week of previous year) and week 1 for a
    //      date in December.
    //
    // WEEK_FIRST_WEEKDAY (2)
    //  If not set
    //      Weeks are numbered according to ISO 8601:1988
    //  If set
    //      The week that contains the first 'first-day-of-week' is week 1.
    //
    // ISO 8601:1988 means that
    //      if the week containing January 1 has
    //      four or more days in the new year, then it is week 1;
    //      Otherwise it is the last week of the previous year, and the
    //      next week is week 1.
    uint8_t week(uint8_t) const;

    uint32_t year_week(uint8_t mode) const;

    // Add interval
    bool date_add_interval(const TimeInterval& interval, TimeUnit unit);

    //unix_timestamp is called with a timezone argument,
    //it returns seconds of the value of date literal since '1970-01-01 00:00:00' UTC
    bool unix_timestamp(int64_t* timestamp, const std::string& timezone) const;
    bool unix_timestamp(int64_t* timestamp, const cctz::time_zone& ctz) const;

    //construct datetime_value from timestamp and timezone
    //timestamp is an internal timestamp value representing seconds since '1970-01-01 00:00:00' UTC
    bool from_unixtime(int64_t, const std::string& timezone);
    bool from_unixtime(int64_t, const cctz::time_zone& ctz);

    bool operator==(const DateTimeValue& other) const {
        // NOTE: This is not same with MySQL.
        // MySQL convert both to int with left value type and then compare
        // We think all fields equals.
        int64_t v1 = to_int64_datetime_packed();
        int64_t v2 = other.to_int64_datetime_packed();
        return v1 == v2;
    }

    bool operator!=(const DateTimeValue& other) const { return !(*this == other); }

    // Now, we don't support TIME_TIME type,
    bool operator<=(const DateTimeValue& other) const { return !(*this > other); }

    bool operator>=(const DateTimeValue& other) const { return !(*this < other); }

    bool operator<(const DateTimeValue& other) const {
        int64_t v1 = to_int64_datetime_packed();
        int64_t v2 = other.to_int64_datetime_packed();
        return v1 < v2;
    }

    bool operator>(const DateTimeValue& other) const {
        int64_t v1 = to_int64_datetime_packed();
        int64_t v2 = other.to_int64_datetime_packed();
        return v1 > v2;
    }

    const char* month_name() const;

    const char* day_name() const;

    DateTimeValue& operator++() {
        switch (_type) {
        case TIME_DATE: {
            TimeInterval interval(DAY, 1, false);
            date_add_interval(interval, DAY);
            break;
        }
        case TIME_DATETIME: {
            TimeInterval interval(SECOND, 1, false);
            date_add_interval(interval, SECOND);
            break;
        }
        case TIME_TIME: {
            TimeInterval interval(SECOND, 1, false);
            date_add_interval(interval, SECOND);
            break;
        }
        }
        return *this;
    }

    void to_datetime_val(doris_udf::DateTimeVal* tv) const {
        tv->packed_time = to_int64_datetime_packed();
        tv->type = _type;
    }

    static DateTimeValue from_datetime_val(const doris_udf::DateTimeVal& tv) {
        DateTimeValue value;
        value.from_packed_time(tv.packed_time);
        if (tv.type == TIME_DATE) {
            value.cast_to_date();
        }
        return value;
    }

    inline uint32_t hash(int seed) const { return HashUtil::hash(this, sizeof(*this), seed); }

    int day_of_year() const { return daynr() - calc_daynr(_year, 1, 1) + 1; }

    // TODO(zhaochun): local time ???
    static DateTimeValue local_time();

    std::string debug_string() const {
        char buf[64];
        char* end = to_string(buf);
        return std::string(buf, end - buf);
    }

    static DateTimeValue datetime_min_value() {
        static DateTimeValue _s_min_datetime_value(0, TIME_DATETIME, 0, 0, 0, 0, 0, 1, 1);
        return _s_min_datetime_value;
    }

    static DateTimeValue datetime_max_value() {
        static DateTimeValue _s_max_datetime_value(0, TIME_DATETIME, 23, 59, 59, 0, 9999, 12, 31);
        return _s_max_datetime_value;
    }

    int64_t second_diff(const DateTimeValue& rhs) const {
        int day_diff = daynr() - rhs.daynr();
        int time_diff = (hour() * 3600 + minute() * 60 + second()) -
                        (rhs.hour() * 3600 + rhs.minute() * 60 + rhs.second());
        return day_diff * 3600 * 24 + time_diff;
    }

    int64_t time_part_diff(const DateTimeValue& rhs) const {
        int time_diff = (hour() * 3600 + minute() * 60 + second()) -
                        (rhs.hour() * 3600 + rhs.minute() * 60 + rhs.second());
        return time_diff;
    }

    void set_type(int type);

    int type() const { return _type; }

    bool is_valid_date() const {
        return !check_range(_year, _month, _day, _hour, _minute, _second, _microsecond, _type) &&
               _month > 0 && _day > 0;
    }

private:
    // Used to make sure sizeof DateTimeValue
    friend class UnusedClass;
    friend void doris::vectorized::VecDateTimeValue::convert_vec_dt_to_dt(DateTimeValue* dt);
    friend void doris::vectorized::VecDateTimeValue::convert_dt_to_vec_dt(DateTimeValue* dt);
    friend void doris::vectorized::DateV2Value<
            doris::vectorized::DateV2ValueType>::convert_date_v2_to_dt(DateTimeValue* dt);
    friend void doris::vectorized::DateV2Value<
            doris::vectorized::DateV2ValueType>::convert_dt_to_date_v2(DateTimeValue* dt);
    friend void doris::vectorized::DateV2Value<
            doris::vectorized::DateTimeV2ValueType>::convert_date_v2_to_dt(DateTimeValue* dt);
    friend void doris::vectorized::DateV2Value<
            doris::vectorized::DateTimeV2ValueType>::convert_dt_to_date_v2(DateTimeValue* dt);

    void from_packed_time(int64_t packed_time) {
        _microsecond = packed_time % (1LL << 24);
        int64_t ymdhms = packed_time >> 24;
        int64_t ymd = ymdhms >> 17;
        int64_t hms = ymdhms % (1 << 17);

        _day = ymd % (1 << 5);
        int64_t ym = ymd >> 5;
        _month = ym % 13;
        _year = ym / 13;
        _year %= 10000;
        _second = hms % (1 << 6);
        _minute = (hms >> 6) % (1 << 6);
        _hour = (hms >> 12);
        _neg = 0;
        _type = TIME_DATETIME;
    }

    int64_t make_packed_time(int64_t time, int64_t second_part) const {
        return (time << 24) + second_part;
    }

    // To compatible with MySQL
    int64_t to_int64_datetime_packed() const {
        int64_t ymd = ((_year * 13 + _month) << 5) | _day;
        int64_t hms = (_hour << 12) | (_minute << 6) | _second;
        int64_t tmp = make_packed_time(((ymd << 17) | hms), _microsecond);
        return _neg ? -tmp : tmp;
    }

    int64_t to_int64_date_packed() const {
        int64_t ymd = ((_year * 13 + _month) << 5) | _day;
        int64_t tmp = make_packed_time(ymd << 17, 0);
        return _neg ? -tmp : tmp;
    }

    // Used to construct from int value
    int64_t standardize_timevalue(int64_t value);

    // Used to convert to a string.
    char* append_date_buffer(char* to) const;
    char* append_time_buffer(char* to) const;
    char* to_datetime_buffer(char* to) const;
    char* to_date_buffer(char* to) const;
    char* to_time_buffer(char* to) const;

    // Used to convert to uint64_t
    int64_t to_datetime_int64() const;
    int64_t to_date_int64() const;
    int64_t to_time_int64() const;

    static uint8_t calc_week(const DateTimeValue& value, uint8_t mode, uint32_t* year);

    // This is private function which modify date but modify `_type`
    bool get_date_from_daynr(uint64_t);

    // Helper to set max, min, zero
    void set_zero(int type);
    void set_max_time(bool neg);

    bool from_date_format_str(const char* format, int format_len, const char* value, int value_len,
                              const char** sub_val_end);

    // NOTICE: it's dangerous if you want to modify the memory structure of datetime
    // which will cause problem in serialization/deserialization of RowBatch.
    // 1 bits for neg. 3 bits for type. 12bit for hour
    uint16_t _neg : 1;  // Used for time value.
    uint16_t _type : 3; // Which type of this value.
    uint16_t _hour : 12;
    uint8_t _minute;
    uint8_t _second;
    uint16_t _year;
    uint8_t _month;
    uint8_t _day;
    // TODO(zc): used for nothing
    uint64_t _microsecond;

    DateTimeValue(uint8_t neg, uint8_t type, uint8_t hour, uint8_t minute, uint8_t second,
                  uint32_t microsecond, uint16_t year, uint8_t month, uint8_t day)
            : _neg(neg),
              _type(type),
              _hour(hour),
              _minute(minute),
              _second(second),
              _year(year),
              _month(month),
              _day(day),
              _microsecond(microsecond) {}

    // RE2 obj is thread safe
    static RE2 time_zone_offset_format_reg;
};

// only support DATE - DATE (no support DATETIME - DATETIME)
std::size_t operator-(const DateTimeValue& v1, const DateTimeValue& v2);

std::ostream& operator<<(std::ostream& os, const DateTimeValue& value);

std::size_t hash_value(DateTimeValue const& value);

} // namespace doris

namespace std {
template <>
struct hash<doris::DateTimeValue> {
    size_t operator()(const doris::DateTimeValue& v) const { return doris::hash_value(v); }
};
} // namespace std
