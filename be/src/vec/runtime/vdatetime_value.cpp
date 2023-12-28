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

#include "vec/runtime/vdatetime_value.h"

#include <cctz/civil_time.h>
#include <cctz/time_zone.h>
#include <ctype.h>
#include <glog/logging.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
// IWYU pragma: no_include <bits/chrono.h>
#include <algorithm>
#include <cctype>
#include <chrono> // IWYU pragma: keep
// IWYU pragma: no_include <bits/std_abs.h>
#include <cmath>
#include <exception>
#include <string>
#include <string_view>

#include "common/compiler_util.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/status.h"
#include "util/timezone_utils.h"
#include "vec/common/int_exp.h"

namespace doris {

static constexpr int s_days_in_month[13] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};

static const char* s_ab_month_name[] = {"",    "Jan", "Feb", "Mar", "Apr", "May", "Jun",
                                        "Jul", "Aug", "Sep", "Oct", "Nov", "Dec", nullptr};

static const char* s_ab_day_name[] = {"Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun", nullptr};

uint8_t mysql_week_mode(uint32_t mode) {
    mode &= 7;
    if (!(mode & WEEK_MONDAY_FIRST)) {
        mode ^= WEEK_FIRST_WEEKDAY;
    }
    return mode;
}

static bool time_zone_begins(const char* ptr, const char* end) {
    return *ptr == '+' || (*ptr == '-' && ptr + 3 < end && *(ptr + 3) == ':') ||
           (isalpha(*ptr) && *ptr != 'T');
}

bool VecDateTimeValue::check_range(uint32_t year, uint32_t month, uint32_t day, uint32_t hour,
                                   uint32_t minute, uint32_t second, uint16_t type) {
    bool time = hour > (type == TIME_TIME ? TIME_MAX_HOUR : 23) || minute > 59 || second > 59;
    if (type == TIME_TIME) {
        return time;
    } else {
        return time || check_date(year, month, day);
    }
}

bool VecDateTimeValue::check_date(uint32_t year, uint32_t month, uint32_t day) {
    if (month == 2 && day == 29 && doris::is_leap(year)) return false;
    if (year > 9999 || month == 0 || month > 12 || day > s_days_in_month[month] || day == 0) {
        return true;
    }
    return false;
}

// The interval format is that with no delimiters
// YYYY-MM-DD HH-MM-DD.FFFFFF AM in default format
// 0    1  2  3  4  5  6      7
bool VecDateTimeValue::from_date_str(const char* date_str, int len) {
    return from_date_str_base(date_str, len, nullptr);
}
//parse timezone to get offset
bool VecDateTimeValue::from_date_str(const char* date_str, int len,
                                     const cctz::time_zone& local_time_zone) {
    return from_date_str_base(date_str, len, &local_time_zone);
}

bool VecDateTimeValue::from_date_str_base(const char* date_str, int len,
                                          const cctz::time_zone* local_time_zone) {
    const char* ptr = date_str;
    const char* end = date_str + len;
    // ONLY 2, 6 can follow by a space
    const static int allow_space_mask = 4 | 64;
    const static int MAX_DATE_PARTS = 8;
    uint32_t date_val[MAX_DATE_PARTS];
    int32_t date_len[MAX_DATE_PARTS];

    _neg = false;
    // Skip space character
    while (ptr < end && isspace(*ptr)) {
        ptr++;
    }
    if (ptr == end || !isdigit(*ptr)) {
        return false;
    }
    // Fix year length
    const char* pos = ptr;
    while (pos < end && (isdigit(*pos) || *pos == 'T')) {
        pos++;
    }
    int year_len = 4;
    int digits = pos - ptr;
    bool is_interval_format = false;
    bool has_bar = false;

    // Compatible with MySQL.
    // For YYYYMMDD/YYYYMMDDHHMMSS is 4 digits years
    if (pos == end || *pos == '.') {
        if (digits == 4 || digits == 8 || digits >= 14) {
            year_len = 4;
        } else {
            year_len = 2;
        }
        is_interval_format = true;
    }

    int field_idx = 0;
    int field_len = year_len;
    long sec_offset = 0;
    while (ptr < end && isdigit(*ptr) && field_idx < MAX_DATE_PARTS - 1) {
        const char* start = ptr;
        int temp_val = 0;
        bool scan_to_delim = (!is_interval_format) && (field_idx != 6);
        while (ptr < end && isdigit(*ptr) && (scan_to_delim || field_len--)) {
            temp_val = temp_val * 10 + (*ptr++ - '0');
        }
        // Impossible
        if (temp_val > 999999L) {
            return false;
        }
        date_val[field_idx] = temp_val;
        date_len[field_idx] = ptr - start;
        field_len = 2;

        if (ptr == end) {
            field_idx++;
            break;
        }

        // timezone
        if (UNLIKELY((field_idx > 2 ||
                      !has_bar) /*dont treat xxxx-xx-xx:xx:xx as xxxx-xx(-xx:xx:xx)*/
                     && time_zone_begins(ptr, end))) {
            if (local_time_zone == nullptr) {
                return false;
            }
            auto get_tz_offset = [&](const std::string& str_tz,
                                     const cctz::time_zone* local_time_zone) -> long {
                cctz::time_zone given_tz {};
                if (!TimezoneUtils::find_cctz_time_zone(str_tz, given_tz)) {
                    throw Exception {ErrorCode::INVALID_ARGUMENT, ""};
                }
                auto given = cctz::convert(cctz::civil_second {}, given_tz);
                auto local = cctz::convert(cctz::civil_second {}, *local_time_zone);
                // these two values is absolute time. so they are negative. need to use (-local) - (-given)
                return std::chrono::duration_cast<std::chrono::seconds>(given - local).count();
            };
            try {
                sec_offset = get_tz_offset(std::string {ptr, end},
                                           local_time_zone); // use the whole remain string
            } catch ([[maybe_unused]] Exception& e) {
                return false; // invalid format
            }
            field_idx++;
            break;
        }

        if (field_idx == 2 && *ptr == 'T') {
            // YYYYMMDDTHHMMDD, skip 'T' and continue
            ptr++;
            field_idx++;
            continue;
        }

        // Second part
        if (field_idx == 5) {
            if (*ptr == '.') {
                ptr++;
                field_len = 6;
            } else if (isdigit(*ptr)) {
                field_idx++;
                break;
            }
            field_idx++;
            continue;
        }
        // escape separator
        while (ptr < end && (ispunct(*ptr) || isspace(*ptr))) {
            if (isspace(*ptr)) {
                if (((1 << field_idx) & allow_space_mask) == 0) {
                    return false;
                }
            }
            if (*ptr == '-') {
                has_bar = true;
            }
            ptr++;
        }
        field_idx++;
    }
    int num_field = field_idx;
    if (num_field <= 3) {
        _type = TIME_DATE;
    } else {
        _type = TIME_DATETIME;
    }
    if (!is_interval_format) {
        year_len = date_len[0];
    }
    for (; field_idx < MAX_DATE_PARTS; ++field_idx) {
        date_len[field_idx] = 0;
        date_val[field_idx] = 0;
    }

    if (year_len == 2) {
        if (date_val[0] < YY_PART_YEAR) {
            date_val[0] += 2000;
        } else {
            date_val[0] += 1900;
        }
    }

    if (num_field < 3) {
        return false;
    }
    if (!check_range_and_set_time(date_val[0], date_val[1], date_val[2], date_val[3], date_val[4],
                                  date_val[5], _type)) {
        return false;
    }
    return sec_offset ? date_add_interval<TimeUnit::SECOND>(
                                TimeInterval {TimeUnit::SECOND, sec_offset, false})
                      : true;
}

// [0, 101) invalid
// [101, (YY_PART_YEAR - 1) * 10000 + 1231] for two digits year 2000 ~ 2069
// [(YY_PART_YEAR - 1) * 10000 + 1231, YY_PART_YEAR * 10000L + 101) invalid
// [YY_PART_YEAR * 10000L + 101, 991231] for two digits year 1970 ~1999
// (991231, 10000101) invalid, because support 1000-01-01
// [10000101, 99991231] for four digits year date value.
// (99991231, 101000000) invalid, NOTE below this is datetime vale hh:mm:ss must exist.
// [101000000, (YY_PART_YEAR - 1)##1231235959] two digits year datetime value
// ((YY_PART_YEAR - 1)##1231235959, YY_PART_YEAR##0101000000) invalid
// ((YY_PART_YEAR)##1231235959, 99991231235959] two digits year datetime value 1970 ~ 1999
// (999991231235959, ~) valid
int64_t VecDateTimeValue::standardize_timevalue(int64_t value) {
    _type = TIME_DATE;
    if (value <= 0) {
        return 0;
    }
    if (value >= 10000101000000L) {
        // 9999-99-99 99:99:99
        if (value > 99999999999999L) {
            return 0;
        }

        // between 1000-01-01 00:00:00L and 9999-99-99 99:99:99
        // all digits exist.
        _type = TIME_DATETIME;
        return value;
    }
    // 2000-01-01
    if (value < 101) {
        return 0;
    }
    // two digits  year. 2000 ~ 2069
    if (value <= (YY_PART_YEAR - 1) * 10000L + 1231L) {
        return (value + 20000000L) * 1000000L;
    }
    // two digits year, invalid date
    if (value < YY_PART_YEAR * 10000L + 101) {
        return 0;
    }
    // two digits year. 1970 ~ 1999
    if (value <= 991231L) {
        return (value + 19000000L) * 1000000L;
    }
    // TODO(zhaochun): Don't allow year betwen 1000-01-01
    if (value < 10000101) {
        return 0;
    }
    // four digits years without hour.
    if (value <= 99991231L) {
        return value * 1000000L;
    }
    // below 0000-01-01
    if (value < 101000000) {
        return 0;
    }

    // below is with datetime, must have hh:mm:ss
    _type = TIME_DATETIME;
    // 2000 ~ 2069
    if (value <= (YY_PART_YEAR - 1) * 10000000000L + 1231235959L) {
        return value + 20000000000000L;
    }
    if (value < YY_PART_YEAR * 10000000000L + 101000000L) {
        return 0;
    }
    // 1970 ~ 1999
    if (value <= 991231235959L) {
        return value + 19000000000000L;
    }
    return value;
}

bool VecDateTimeValue::from_date_int64(int64_t value) {
    _neg = false;
    value = standardize_timevalue(value);
    if (value <= 0) {
        return false;
    }
    uint64_t date = value / 1000000;
    uint64_t time = value % 1000000;

    auto [year, month, day, hour, minute, second] = std::tuple {0, 0, 0, 0, 0, 0};
    year = date / 10000;
    date %= 10000;
    month = date / 100;
    day = date % 100;
    hour = time / 10000;
    time %= 10000;
    minute = time / 100;
    second = time % 100;

    return check_range_and_set_time(year, month, day, hour, minute, second, _type);
}

void VecDateTimeValue::set_zero(int type) {
    memset(this, 0, sizeof(*this));
    _type = type;
}

void VecDateTimeValue::set_type(int type) {
    _type = type;
    if (type == TIME_DATE) {
        _hour = 0;
        _minute = 0;
        _second = 0;
    }
}

void VecDateTimeValue::set_max_time(bool neg) {
    set_zero(TIME_TIME);
    _hour = static_cast<uint8_t>(TIME_MAX_HOUR);
    _minute = TIME_MAX_MINUTE;
    _second = TIME_MAX_SECOND;
    _neg = neg;
}

bool VecDateTimeValue::from_time_int64(int64_t value) {
    _type = TIME_TIME;
    if (value > TIME_MAX_VALUE) {
        // 0001-01-01 00:00:00 to convert to a datetime
        if (value > 10000000000L) {
            if (from_date_int64(value)) {
                return true;
            }
        }
        set_max_time(false);
        return false;
    } else if (value < -1 * TIME_MAX_VALUE) {
        set_max_time(true);
        return false;
    }
    if (value < 0) {
        _neg = 1;
        value = -value;
    }
    _hour = value / 10000;
    value %= 10000;
    _minute = value / 100;
    if (_minute > TIME_MAX_MINUTE) {
        return false;
    }
    _second = value % 100;
    if (_second > TIME_MAX_SECOND) {
        return false;
    }
    return true;
}

char* VecDateTimeValue::append_date_buffer(char* to) const {
    uint32_t temp;
    // Year
    temp = _year / 100;
    *to++ = (char)('0' + (temp / 10));
    *to++ = (char)('0' + (temp % 10));
    temp = _year % 100;
    *to++ = (char)('0' + (temp / 10));
    *to++ = (char)('0' + (temp % 10));
    *to++ = '-';
    // Month
    *to++ = (char)('0' + (_month / 10));
    *to++ = (char)('0' + (_month % 10));
    *to++ = '-';
    // Day
    *to++ = (char)('0' + (_day / 10));
    *to++ = (char)('0' + (_day % 10));
    return to;
}

char* VecDateTimeValue::append_time_buffer(char* to) const {
    if (_neg) {
        *to++ = '-';
    }
    // Hour
    uint32_t temp = _hour;
    if (temp >= 100) {
        *to++ = (char)('0' + (temp / 100));
        temp %= 100;
    }
    *to++ = (char)('0' + (temp / 10));
    *to++ = (char)('0' + (temp % 10));
    *to++ = ':';
    // Minute
    *to++ = (char)('0' + (_minute / 10));
    *to++ = (char)('0' + (_minute % 10));
    *to++ = ':';
    /* Second */
    *to++ = (char)('0' + (_second / 10));
    *to++ = (char)('0' + (_second % 10));
    return to;
}

char* VecDateTimeValue::to_datetime_buffer(char* to) const {
    to = append_date_buffer(to);
    *to++ = ' ';
    return append_time_buffer(to);
}

char* VecDateTimeValue::to_date_buffer(char* to) const {
    return append_date_buffer(to);
}

char* VecDateTimeValue::to_time_buffer(char* to) const {
    return append_time_buffer(to);
}

int32_t VecDateTimeValue::to_buffer(char* buffer) const {
    switch (_type) {
    case TIME_TIME:
        return to_time_buffer(buffer) - buffer;
    case TIME_DATE:
        return to_date_buffer(buffer) - buffer;
    case TIME_DATETIME:
        return to_datetime_buffer(buffer) - buffer;
    default:
        break;
    }
    return 0;
}

char* VecDateTimeValue::to_string(char* to) const {
    int len = to_buffer(to);
    *(to + len) = '\0';
    return to + len + 1;
}

int64_t VecDateTimeValue::to_datetime_int64() const {
    return (_year * 10000L + _month * 100 + _day) * 1000000L + _hour * 10000 + _minute * 100 +
           _second;
}

int64_t VecDateTimeValue::to_date_int64() const {
    return _year * 10000 + _month * 100 + _day;
}

int64_t VecDateTimeValue::to_time_int64() const {
    int sign = _neg == 0 ? 1 : -1;
    return sign * (_hour * 10000 + _minute * 100 + _second);
}

int64_t VecDateTimeValue::to_int64() const {
    switch (_type) {
    case TIME_TIME:
        return to_time_int64();
    case TIME_DATE:
        return to_date_int64();
    case TIME_DATETIME:
        return to_datetime_int64();
    default:
        return 0;
    }
}

bool VecDateTimeValue::get_date_from_daynr(uint64_t daynr) {
    if (daynr <= 0 || daynr > DATE_MAX_DAYNR) {
        return false;
    }

    auto [year, month, day] = std::tuple {0, 0, 0};
    year = daynr / 365;
    uint32_t days_befor_year = 0;
    while (daynr < (days_befor_year = doris::calc_daynr(year, 1, 1))) {
        year--;
    }
    uint32_t days_of_year = daynr - days_befor_year + 1;
    int leap_day = 0;
    if (doris::is_leap(year)) {
        if (days_of_year > 31 + 28) {
            days_of_year--;
            if (days_of_year == 31 + 28) {
                leap_day = 1;
            }
        }
    }
    month = 1;
    while (days_of_year > s_days_in_month[month]) {
        days_of_year -= s_days_in_month[month];
        month++;
    }
    day = days_of_year + leap_day;

    if (check_range(year, month, day, 0, 0, 0, _type)) {
        return false;
    }
    set_time(year, month, day, _hour, _minute, _second);
    return true;
}

bool VecDateTimeValue::from_date_daynr(uint64_t daynr) {
    _neg = false;
    if (!get_date_from_daynr(daynr)) {
        return false;
    }
    _hour = 0;
    _minute = 0;
    _second = 0;
    _type = TIME_DATE;
    return true;
}

static char* int_to_str(uint64_t val, char* to) {
    char buf[64];
    char* ptr = buf;
    // Use do/while for 0 value
    do {
        *ptr++ = '0' + (val % 10);
        val /= 10;
    } while (val);

    while (ptr > buf) {
        *to++ = *--ptr;
    }

    return to;
}

static char* append_string(const char* from, char* to) {
    while (*from) {
        *to++ = *from++;
    }
    return to;
}

static char* append_with_prefix(const char* str, int str_len, char prefix, int full_len, char* to) {
    int len = (str_len > full_len) ? str_len : full_len;
    len -= str_len;
    while (len-- > 0) {
        // push prefix;
        *to++ = prefix;
    }
    while (str_len-- > 0) {
        *to++ = *str++;
    }

    return to;
}

int VecDateTimeValue::compute_format_len(const char* format, int len) {
    int size = 0;
    const char* ptr = format;
    const char* end = format + len;

    while (ptr < end) {
        if (*ptr != '%' || (ptr + 1) < end) {
            size++;
            ptr++;
            continue;
        }
        switch (*++ptr) {
        case 'M':
        case 'W':
            size += 10;
            break;
        case 'D':
        case 'Y':
        case 'x':
        case 'X':
            size += 4;
            break;
        case 'a':
        case 'b':
            size += 10;
            break;
        case 'j':
            size += 3;
            break;
        case 'u':
        case 'U':
        case 'v':
        case 'V':
        case 'y':
        case 'm':
        case 'd':
        case 'h':
        case 'i':
        case 'I':
        case 'l':
        case 'p':
        case 'S':
        case 's':
        case 'c':
        case 'e':
            size += 2;
            break;
        case 'k':
        case 'H':
            size += 7;
            break;
        case 'r':
            size += 11;
            break;
        case 'T':
            size += 8;
            break;
        case 'f':
            size += 6;
            break;
        case 'w':
        case '%':
        default:
            size++;
            break;
        }
    }
    return size;
}

static const char digits100[201] =
        "00010203040506070809"
        "10111213141516171819"
        "20212223242526272829"
        "30313233343536373839"
        "40414243444546474849"
        "50515253545556575859"
        "60616263646566676869"
        "70717273747576777879"
        "80818283848586878889"
        "90919293949596979899";

char* write_two_digits_to_string(int number, char* dst) {
    memcpy(dst, &digits100[number * 2], 2);
    return dst + 2;
}

char* write_four_digits_to_string(int number, char* dst) {
    memcpy(dst, &digits100[(number / 100) * 2], 2);
    memcpy(dst + 2, &digits100[(number % 100) * 2], 2);
    return dst + 4;
}

bool VecDateTimeValue::to_format_string(const char* format, int len, char* to) const {
    if (check_range(_year, _month, _day, _hour, _minute, _second, _type)) {
        return false;
    }
    char buf[64];
    char* cursor = buf;
    char* pos = NULL;
    const char* ptr = format;
    const char* end = format + len;
    char ch = '\0';

    while (ptr < end) {
        if (*ptr != '%' || (ptr + 1) == end) {
            *to++ = *ptr++;
            continue;
        }
        // Skip '%'
        ptr++;
        switch (ch = *ptr++) {
        case 'y':
            // Year, numeric (two digits)
            to = write_two_digits_to_string(_year % 100, to);
            cursor += 2;
            pos = cursor;
            break;
        case 'Y':
            // Year, numeric, four digits
            to = write_four_digits_to_string(_year, to);
            cursor += 4;
            pos = cursor;
            break;
        case 'd':
            // Day of month (00...31)
            to = write_two_digits_to_string(_day, to);
            cursor += 2;
            pos = cursor;
            break;
        case 'H':
            to = write_two_digits_to_string(_hour, to);
            cursor += 2;
            pos = cursor;
            break;
        case 'i':
            // Minutes, numeric (00..59)
            to = write_two_digits_to_string(_minute, to);
            cursor += 2;
            pos = cursor;
            break;
        case 'm':
            to = write_two_digits_to_string(_month, to);
            cursor += 2;
            pos = cursor;
            break;
        case 'h':
        case 'I':
            // Hour (01..12)
            to = write_two_digits_to_string((_hour % 24 + 11) % 12 + 1, to);
            cursor += 2;
            pos = cursor;
            break;
        case 's':
        case 'S':
            // Seconds (00..59)
            to = write_two_digits_to_string(_second, to);
            cursor += 2;
            pos = cursor;
            break;
        case 'a':
            // Abbreviated weekday name
            if (_type == TIME_TIME || (_year == 0 && _month == 0)) {
                return false;
            }
            to = append_string(s_ab_day_name[weekday()], to);
            break;
        case 'b':
            // Abbreviated month name
            if (_month == 0) {
                return false;
            }
            to = append_string(s_ab_month_name[_month], to);
            break;
        case 'c':
            // Month, numeric (0...12)
            pos = int_to_str(_month, cursor);
            to = append_with_prefix(cursor, pos - cursor, '0', 1, to);
            break;
        case 'D':
            // Day of the month with English suffix (0th, 1st, ...)
            pos = int_to_str(_day, cursor);
            to = append_with_prefix(cursor, pos - cursor, '0', 1, to);
            if (_day >= 10 && _day <= 19) {
                to = append_string("th", to);
            } else {
                switch (_day % 10) {
                case 1:
                    to = append_string("st", to);
                    break;
                case 2:
                    to = append_string("nd", to);
                    break;
                case 3:
                    to = append_string("rd", to);
                    break;
                default:
                    to = append_string("th", to);
                    break;
                }
            }
            break;
        case 'e':
            // Day of the month, numeric (0..31)
            pos = int_to_str(_day, cursor);
            to = append_with_prefix(cursor, pos - cursor, '0', 1, to);
            break;
        case 'f':
            // Microseconds (000000..999999)
            pos = int_to_str(0, cursor);
            to = append_with_prefix(cursor, pos - cursor, '0', 6, to);
            break;
        case 'j':
            // Day of year (001..366)
            pos = int_to_str(daynr() - doris::calc_daynr(_year, 1, 1) + 1, cursor);
            to = append_with_prefix(cursor, pos - cursor, '0', 3, to);
            break;
        case 'k':
            // Hour (0..23)
            pos = int_to_str(_hour, cursor);
            to = append_with_prefix(cursor, pos - cursor, '0', 1, to);
            break;
        case 'l':
            // Hour (1..12)
            pos = int_to_str((_hour % 24 + 11) % 12 + 1, cursor);
            to = append_with_prefix(cursor, pos - cursor, '0', 1, to);
            break;
        case 'M':
            // Month name (January..December)
            if (_month == 0) {
                return false;
            }
            to = append_string(s_month_name[_month], to);
            break;
        case 'p':
            // AM or PM
            if ((_hour % 24) >= 12) {
                to = append_string("PM", to);
            } else {
                to = append_string("AM", to);
            }
            break;
        case 'r':
            // Time, 12-hour (hh:mm:ss followed by AM or PM)
            *to++ = (char)('0' + (((_hour + 11) % 12 + 1) / 10));
            *to++ = (char)('0' + (((_hour + 11) % 12 + 1) % 10));
            *to++ = ':';
            // Minute
            *to++ = (char)('0' + (_minute / 10));
            *to++ = (char)('0' + (_minute % 10));
            *to++ = ':';
            /* Second */
            *to++ = (char)('0' + (_second / 10));
            *to++ = (char)('0' + (_second % 10));
            if ((_hour % 24) >= 12) {
                to = append_string(" PM", to);
            } else {
                to = append_string(" AM", to);
            }
            break;
        case 'T':
            // Time, 24-hour (hh:mm:ss)
            *to++ = (char)('0' + ((_hour % 24) / 10));
            *to++ = (char)('0' + ((_hour % 24) % 10));
            *to++ = ':';
            // Minute
            *to++ = (char)('0' + (_minute / 10));
            *to++ = (char)('0' + (_minute % 10));
            *to++ = ':';
            /* Second */
            *to++ = (char)('0' + (_second / 10));
            *to++ = (char)('0' + (_second % 10));
            break;
        case 'u':
            // Week (00..53), where Monday is the first day of the week;
            // WEEK() mode 1
            if (_type == TIME_TIME) {
                return false;
            }
            to = write_two_digits_to_string(week(mysql_week_mode(1)), to);
            cursor += 2;
            pos = cursor;
            break;
        case 'U':
            // Week (00..53), where Sunday is the first day of the week;
            // WEEK() mode 0
            if (_type == TIME_TIME) {
                return false;
            }
            to = write_two_digits_to_string(week(mysql_week_mode(0)), to);
            cursor += 2;
            pos = cursor;
            break;
        case 'v':
            // Week (01..53), where Monday is the first day of the week;
            // WEEK() mode 3; used with %x
            if (_type == TIME_TIME) {
                return false;
            }
            to = write_two_digits_to_string(week(mysql_week_mode(3)), to);
            cursor += 2;
            pos = cursor;
            break;
        case 'V':
            // Week (01..53), where Sunday is the first day of the week;
            // WEEK() mode 2; used with %X
            if (_type == TIME_TIME) {
                return false;
            }
            to = write_two_digits_to_string(week(mysql_week_mode(2)), to);
            cursor += 2;
            pos = cursor;
            break;
        case 'w':
            // Day of the week (0=Sunday..6=Saturday)
            if (_type == TIME_TIME || (_month == 0 && _year == 0)) {
                return false;
            }
            pos = int_to_str(doris::calc_weekday(daynr(), true), cursor);
            to = append_with_prefix(cursor, pos - cursor, '0', 1, to);
            break;
        case 'W':
            // Weekday name (Sunday..Saturday)
            to = append_string(s_day_name[weekday()], to);
            break;
        case 'x': {
            // Year for the week, where Monday is the first day of the week,
            // numeric, four digits; used with %v
            if (_type == TIME_TIME) {
                return false;
            }
            uint32_t year = 0;
            calc_week(*this, mysql_week_mode(3), &year, true);
            to = write_four_digits_to_string(year, to);
            cursor += 4;
            pos = cursor;
            break;
        }
        case 'X': {
            // Year for the week where Sunday is the first day of the week,
            // numeric, four digits; used with %V
            if (_type == TIME_TIME) {
                return false;
            }
            uint32_t year = 0;
            calc_week(*this, mysql_week_mode(2), &year);
            to = write_four_digits_to_string(year, to);
            cursor += 4;
            pos = cursor;
            break;
        }
        default:
            *to++ = ch;
            break;
        }
    }
    *to++ = '\0';
    return true;
}

uint8_t VecDateTimeValue::calc_week(const VecDateTimeValue& value, uint8_t mode, uint32_t* year,
                                    bool disable_lut) {
    // mode=3 is used for week_of_year()
    if (config::enable_time_lut && !disable_lut && mode == 3 && value._year >= 1950 &&
        value._year < 2030) {
        return doris::TimeLUT::GetImplement()
                ->week_of_year_table[value._year - doris::LUT_START_YEAR][value._month - 1]
                                    [value._day - 1];
    }
    // mode=4 is used for week()
    if (config::enable_time_lut && !disable_lut && mode == 4 && value._year >= 1950 &&
        value._year < 2030) {
        return doris::TimeLUT::GetImplement()
                ->week_table[value._year - doris::LUT_START_YEAR][value._month - 1][value._day - 1];
    }
    // not covered by pre calculated dates, calculate at runtime
    bool monday_first = mode & WEEK_MONDAY_FIRST;
    bool week_year = mode & WEEK_YEAR;
    bool first_weekday = mode & WEEK_FIRST_WEEKDAY;
    uint64_t day_nr = value.daynr();
    uint64_t daynr_first_day = doris::calc_daynr(value._year, 1, 1);
    uint8_t weekday_first_day = doris::calc_weekday(daynr_first_day, !monday_first);

    int days = 0;
    *year = value._year;

    // Check weather the first days of this year belongs to last year
    if (value._month == 1 && value._day <= (7 - weekday_first_day)) {
        if (!week_year && ((first_weekday && weekday_first_day != 0) ||
                           (!first_weekday && weekday_first_day > 3))) {
            return 0;
        }
        (*year)--;
        week_year = true;
        daynr_first_day -= (days = doris::calc_days_in_year(*year));
        weekday_first_day = (weekday_first_day + 53 * 7 - days) % 7;
    }

    // How many days since first week
    if ((first_weekday && weekday_first_day != 0) || (!first_weekday && weekday_first_day > 3)) {
        // days in new year belongs to last year.
        days = day_nr - (daynr_first_day + (7 - weekday_first_day));
    } else {
        // days in new year belongs to this year.
        days = day_nr - (daynr_first_day - weekday_first_day);
    }

    if (week_year && days >= 52 * 7) {
        weekday_first_day = (weekday_first_day + doris::calc_days_in_year(*year)) % 7;
        if ((first_weekday && weekday_first_day == 0) ||
            (!first_weekday && weekday_first_day <= 3)) {
            // Belong to next year.
            (*year)++;
            return 1;
        }
    }

    return days / 7 + 1;
}

uint8_t VecDateTimeValue::week(uint8_t mode) const {
    uint32_t year = 0;
    return calc_week(*this, mode, &year);
}

uint32_t VecDateTimeValue::year_week(uint8_t mode) const {
    // mode=4 is used for yearweek()
    if (config::enable_time_lut && mode == 4 && _year >= 1950 && _year < 2030) {
        return doris::TimeLUT::GetImplement()->year_week_table[_year - 1950][_month - 1][_day - 1];
    }

    // not covered by year_week_table, calculate at runtime
    uint32_t year = 0;
    // The range of the week in the year_week is 1-53, so the mode WEEK_YEAR is always true.
    uint8_t week = calc_week(*this, mode | 2, &year, true);
    // When the mode WEEK_FIRST_WEEKDAY is not set,
    // the week in which the last three days of the year fall may belong to the following year.
    if (week == 53 && day() >= 29 && !(mode & 4)) {
        uint8_t monday_first = mode & WEEK_MONDAY_FIRST;
        uint64_t daynr_of_last_day = doris::calc_daynr(_year, 12, 31);
        uint8_t weekday_of_last_day = doris::calc_weekday(daynr_of_last_day, !monday_first);

        if (weekday_of_last_day - monday_first < 2) {
            ++year;
            week = 1;
        }
    }
    return year * 100 + week;
}

template <typename T>
bool VecDateTimeValue::operator>=(const DateV2Value<T>& other) const {
    int64_t ts1;
    int64_t ts2;
    this->unix_timestamp(&ts1, TimezoneUtils::default_time_zone);
    other.unix_timestamp(&ts2, TimezoneUtils::default_time_zone);
    return ts1 >= ts2;
}

template <typename T>
bool VecDateTimeValue::operator<=(const DateV2Value<T>& other) const {
    int64_t ts1;
    int64_t ts2;
    this->unix_timestamp(&ts1, TimezoneUtils::default_time_zone);
    other.unix_timestamp(&ts2, TimezoneUtils::default_time_zone);
    return ts1 <= ts2;
}

template <typename T>
bool VecDateTimeValue::operator>(const DateV2Value<T>& other) const {
    int64_t ts1;
    int64_t ts2;
    this->unix_timestamp(&ts1, TimezoneUtils::default_time_zone);
    other.unix_timestamp(&ts2, TimezoneUtils::default_time_zone);
    return ts1 > ts2;
}

template <typename T>
bool VecDateTimeValue::operator<(const DateV2Value<T>& other) const {
    int64_t ts1;
    int64_t ts2;
    this->unix_timestamp(&ts1, TimezoneUtils::default_time_zone);
    other.unix_timestamp(&ts2, TimezoneUtils::default_time_zone);
    return ts1 < ts2;
}

template <typename T>
bool VecDateTimeValue::operator==(const DateV2Value<T>& other) const {
    int64_t ts1;
    int64_t ts2;
    this->unix_timestamp(&ts1, TimezoneUtils::default_time_zone);
    other.unix_timestamp(&ts2, TimezoneUtils::default_time_zone);
    return ts1 == ts2;
}

// TODO(zhaochun): Think endptr is NULL
// Return true if convert to a integer success. Otherwise false.
static bool str_to_int64(const char* ptr, const char** endptr, int64_t* ret) {
    const static uint64_t MAX_NEGATIVE_NUMBER = 0x8000000000000000;
    const static uint64_t ULONGLONG_MAX = ~0;
    const static uint64_t LFACTOR2 = 100000000000ULL;
    const char* end = *endptr;
    uint64_t cutoff_1 = 0;
    uint64_t cutoff_2 = 0;
    uint64_t cutoff_3 = 0;
    // Skip space
    while (ptr < end && (*ptr == ' ' || *ptr == '\t')) {
        ptr++;
    }
    if (ptr >= end) {
        return false;
    }
    // Sign
    bool neg = false;
    if (*ptr == '-') {
        neg = true;
        ptr++;
        cutoff_1 = MAX_NEGATIVE_NUMBER / LFACTOR2;
        cutoff_2 = (MAX_NEGATIVE_NUMBER % LFACTOR2) / 100;
        cutoff_3 = (MAX_NEGATIVE_NUMBER % LFACTOR2) % 100;
    } else {
        if (*ptr == '+') {
            ptr++;
        }
        cutoff_1 = ULONGLONG_MAX / LFACTOR2;
        cutoff_2 = (ULONGLONG_MAX % LFACTOR2) / 100;
        cutoff_3 = (ULONGLONG_MAX % LFACTOR2) % 100;
    }
    if (ptr >= end) {
        return false;
    }
    // a valid input should at least contains one digit
    if (!isdigit(*ptr)) {
        return false;
    }
    // Skip '0'
    while (ptr < end && *ptr == '0') {
        ptr++;
    }
    const char* n_end = ptr + 9;
    if (n_end > end) {
        n_end = end;
    }
    uint64_t value_1 = 0;
    while (ptr < n_end && isdigit(*ptr)) {
        value_1 *= 10;
        value_1 += *ptr++ - '0';
    }
    if (ptr == end || !isdigit(*ptr)) {
        *endptr = ptr;
        *ret = neg ? -value_1 : value_1;
        return true;
    }
    // TODO
    uint64_t value_2 = 0;
    uint64_t value_3 = 0;

    // Check overflow.
    if (value_1 > cutoff_1 ||
        (value_1 == cutoff_1 &&
         (value_2 > cutoff_2 || (value_2 == cutoff_2 && value_3 > cutoff_3)))) {
        return false;
    }
    return true;
}

static int min(int a, int b) {
    return a < b ? a : b;
}

static int find_in_lib(const char* lib[], const char* str, const char* end) {
    int pos = 0;
    int find_count = 0;
    int find_pos = 0;
    for (; lib[pos] != NULL; ++pos) {
        const char* i = str;
        const char* j = lib[pos];
        while (i < end && *j) {
            if (toupper(*i) != toupper(*j)) {
                break;
            }
            ++i;
            ++j;
        }
        if (i == end) {
            if (*j == '\0') {
                return pos;
            } else {
                find_count++;
                find_pos = pos;
            }
        }
    }
    return find_count == 1 ? find_pos : -1;
}

static int check_word(const char* lib[], const char* str, const char* end, const char** endptr) {
    const char* ptr = str;
    while (ptr < end && isalpha(*ptr)) {
        ptr++;
    }
    int pos = find_in_lib(lib, str, ptr);
    if (pos >= 0) {
        *endptr = ptr;
    }
    return pos;
}

// this method is exactly same as fromDateFormatStr() in DateLiteral.java in FE
// change this method should also change that.
bool VecDateTimeValue::from_date_format_str(const char* format, int format_len, const char* value,
                                            int value_len, const char** sub_val_end) {
    const char* ptr = format;
    const char* end = format + format_len;
    const char* val = value;
    const char* val_end = value + value_len;

    bool date_part_used = false;
    bool time_part_used = false;
    bool frac_part_used = false;
    bool already_set_time_part = false;

    int day_part = 0;
    int weekday = -1;
    int yearday = -1;
    int week_num = -1;

    bool strict_week_number = false;
    bool sunday_first = false;
    bool strict_week_number_year_type = false;
    int strict_week_number_year = -1;
    bool usa_time = false;

    auto [year, month, day, hour, minute, second] = std::tuple {0, 0, 0, 0, 0, 0};
    while (ptr < end && val < val_end) {
        // Skip space character
        while (val < val_end && isspace(*val)) {
            val++;
        }
        if (val >= val_end) {
            break;
        }
        // Check switch
        if (*ptr == '%' && ptr + 1 < end) {
            const char* tmp = NULL;
            int64_t int_value = 0;
            ptr++;
            switch (*ptr++) {
                // Year
            case 'y':
                // Year, numeric (two digits)
                tmp = val + min(2, val_end - val);
                if (!str_to_int64(val, &tmp, &int_value)) {
                    return false;
                }
                int_value += int_value >= 70 ? 1900 : 2000;
                year = int_value;
                val = tmp;
                date_part_used = true;
                break;
            case 'Y':
                // Year, numeric, four digits
                tmp = val + min(4, val_end - val);
                if (!str_to_int64(val, &tmp, &int_value)) {
                    return false;
                }
                if (tmp - val <= 2) {
                    int_value += int_value >= 70 ? 1900 : 2000;
                }
                year = int_value;
                val = tmp;
                date_part_used = true;
                break;
                // Month
            case 'm':
            case 'c':
                tmp = val + min(2, val_end - val);
                if (!str_to_int64(val, &tmp, &int_value)) {
                    return false;
                }
                month = int_value;
                val = tmp;
                date_part_used = true;
                break;
            case 'M':
                int_value = check_word(const_cast<const char**>(s_month_name), val, val_end, &val);
                if (int_value < 0) {
                    return false;
                }
                month = int_value;
                break;
            case 'b':
                int_value = check_word(s_ab_month_name, val, val_end, &val);
                if (int_value < 0) {
                    return false;
                }
                month = int_value;
                break;
                // Day
            case 'd':
            case 'e':
                tmp = val + min(2, val_end - val);
                if (!str_to_int64(val, &tmp, &int_value)) {
                    return false;
                }
                day = int_value;
                val = tmp;
                date_part_used = true;
                break;
            case 'D':
                tmp = val + min(2, val_end - val);
                if (!str_to_int64(val, &tmp, &int_value)) {
                    return false;
                }
                day = int_value;
                val = tmp + min(2, val_end - tmp);
                date_part_used = true;
                break;
                // Hour
            case 'h':
            case 'I':
            case 'l':
                usa_time = true;
                // Fall through
            case 'k':
            case 'H':
                tmp = val + min(2, val_end - val);
                if (!str_to_int64(val, &tmp, &int_value)) {
                    return false;
                }
                hour = int_value;
                val = tmp;
                time_part_used = true;
                break;
                // Minute
            case 'i':
                tmp = val + min(2, val_end - val);
                if (!str_to_int64(val, &tmp, &int_value)) {
                    return false;
                }
                minute = int_value;
                val = tmp;
                time_part_used = true;
                break;
                // Second
            case 's':
            case 'S':
                tmp = val + min(2, val_end - val);
                if (!str_to_int64(val, &tmp, &int_value)) {
                    return false;
                }
                second = int_value;
                val = tmp;
                time_part_used = true;
                break;
                // Micro second
            case 'f':
                // _microsecond is removed, but need to eat this val
                tmp = val + min(6, val_end - val);
                if (!str_to_int64(val, &tmp, &int_value)) {
                    return false;
                }
                val = tmp;
                break;
                // AM/PM
            case 'p':
                if ((val_end - val) < 2 || toupper(*(val + 1)) != 'M' || !usa_time) {
                    return false;
                }
                if (toupper(*val) == 'P') {
                    // PM
                    day_part = 12;
                }
                time_part_used = true;
                val += 2;
                break;
                // Weekday
            case 'W':
                int_value = check_word(const_cast<const char**>(s_day_name), val, val_end, &val);
                if (int_value < 0) {
                    return false;
                }
                int_value++;
                weekday = int_value;
                date_part_used = true;
                break;
            case 'a':
                int_value = check_word(s_ab_day_name, val, val_end, &val);
                if (int_value < 0) {
                    return false;
                }
                int_value++;
                weekday = int_value;
                date_part_used = true;
                break;
            case 'w':
                tmp = val + min(1, val_end - val);
                if (!str_to_int64(val, &tmp, &int_value)) {
                    return false;
                }
                if (int_value >= 7) {
                    return false;
                }
                if (int_value == 0) {
                    int_value = 7;
                }
                weekday = int_value;
                val = tmp;
                date_part_used = true;
                break;
            case 'j':
                tmp = val + min(3, val_end - val);
                if (!str_to_int64(val, &tmp, &int_value)) {
                    return false;
                }
                yearday = int_value;
                val = tmp;
                date_part_used = true;
                break;
            case 'u':
            case 'v':
            case 'U':
            case 'V':
                sunday_first = (*(ptr - 1) == 'U' || *(ptr - 1) == 'V');
                // Used to check if there is %x or %X
                strict_week_number = (*(ptr - 1) == 'V' || *(ptr - 1) == 'v');
                tmp = val + min(2, val_end - val);
                if (!str_to_int64(val, &tmp, &int_value)) {
                    return false;
                }
                week_num = int_value;
                if (week_num > 53 || (strict_week_number && week_num == 0)) {
                    return false;
                }
                val = tmp;
                date_part_used = true;
                break;
                // strict week number, must be used with %V or %v
            case 'x':
            case 'X':
                strict_week_number_year_type = (*(ptr - 1) == 'X');
                tmp = val + min(4, val_end - val);
                if (!str_to_int64(val, &tmp, &int_value)) {
                    return false;
                }
                strict_week_number_year = int_value;
                val = tmp;
                date_part_used = true;
                break;
            case 'r': {
                VecDateTimeValue tmp_val;
                if (!tmp_val.from_date_format_str("%I:%i:%S %p", 11, val, val_end - val, &tmp)) {
                    return false;
                }
                this->_hour = tmp_val._hour;
                this->_minute = tmp_val._minute;
                this->_second = tmp_val._second;
                val = tmp;
                time_part_used = true;
                already_set_time_part = true;
                break;
            }
            case 'T': {
                VecDateTimeValue tmp_val;
                if (!tmp_val.from_date_format_str("%H:%i:%S", 8, val, val_end - val, &tmp)) {
                    return false;
                }
                this->_hour = tmp_val._hour;
                this->_minute = tmp_val._minute;
                this->_second = tmp_val._second;
                time_part_used = true;
                already_set_time_part = true;
                val = tmp;
                break;
            }
            case '.':
                while (val < val_end && ispunct(*val)) {
                    val++;
                }
                break;
            case '@':
                while (val < val_end && isalpha(*val)) {
                    val++;
                }
                break;
            case '#':
                while (val < val_end && isdigit(*val)) {
                    val++;
                }
                break;
            case '%': // %%, escape the %
                if ('%' != *val) {
                    return false;
                }
                val++;
                break;
            default:
                return false;
            }
        } else if (!isspace(*ptr)) {
            if (*ptr != *val) {
                return false;
            }
            ptr++;
            val++;
        } else {
            ptr++;
        }
    }

    // continue to iterate pattern if has
    // to find out if it has time part.
    while (ptr < end) {
        if (*ptr == '%' && ptr + 1 < end) {
            ptr++;
            switch (*ptr++) {
            case 'H':
            case 'h':
            case 'I':
            case 'i':
            case 'k':
            case 'l':
            case 'r':
            case 's':
            case 'S':
            case 'p':
            case 'T':
                time_part_used = true;
                break;
            default:
                break;
            }
        } else {
            ptr++;
        }
    }

    if (usa_time) {
        if (hour > 12 || hour < 1) {
            return false;
        }
        hour = (hour % 12) + day_part;
    }
    if (sub_val_end) {
        *sub_val_end = val;
    }

    // Compute timestamp type
    if (frac_part_used) {
        if (date_part_used) {
            _type = TIME_DATETIME;
        } else {
            _type = TIME_TIME;
        }
    } else {
        if (date_part_used) {
            if (time_part_used) {
                _type = TIME_DATETIME;
            } else {
                _type = TIME_DATE;
            }
        } else {
            _type = TIME_TIME;
        }
    }

    _neg = false;

    // Year day
    if (yearday > 0) {
        uint64_t days = doris::calc_daynr(year, 1, 1) + yearday - 1;
        if (!get_date_from_daynr(days)) {
            return false;
        }
    }
    // weekday
    if (week_num >= 0 && weekday > 0) {
        // Check
        if ((strict_week_number &&
             (strict_week_number_year < 0 || strict_week_number_year_type != sunday_first)) ||
            (!strict_week_number && strict_week_number_year >= 0)) {
            return false;
        }
        uint64_t days =
                doris::calc_daynr(strict_week_number ? strict_week_number_year : year, 1, 1);

        uint8_t weekday_b = doris::calc_weekday(days, sunday_first);

        if (sunday_first) {
            days += ((weekday_b == 0) ? 0 : 7) - weekday_b + (week_num - 1) * 7 + weekday % 7;
        } else {
            days += ((weekday_b <= 3) ? 0 : 7) - weekday_b + (week_num - 1) * 7 + weekday - 1;
        }
        if (!get_date_from_daynr(days)) {
            return false;
        }
    }
    // 1. already_set_date_part means _year, _month, _day be set, so we only set time part
    // 2. already_set_time_part means _hour, _minute, _second, _microsecond be set,
    //    so we only need to set date part
    // 3. if both are true, means all part of date_time be set, no need check_range_and_set_time
    bool already_set_date_part = yearday > 0 || (week_num >= 0 && weekday > 0);
    if (already_set_date_part && already_set_time_part) return true;
    if (already_set_date_part)
        return check_range_and_set_time(_year, _month, _day, hour, minute, second, _type);
    if (already_set_time_part)
        return check_range_and_set_time(year, month, day, _hour, _minute, _second, _type);

    return check_range_and_set_time(year, month, day, hour, minute, second, _type);
}

template <TimeUnit unit, bool need_check>
bool VecDateTimeValue::date_add_interval(const TimeInterval& interval) {
    if constexpr (need_check) {
        if (!is_valid_date()) return false;
    }

    int sign = interval.is_neg ? -1 : 1;

    if constexpr ((unit == SECOND) || (unit == MINUTE) || (unit == HOUR) ||
                  (unit == SECOND_MICROSECOND) || (unit == MINUTE_MICROSECOND) ||
                  (unit == MINUTE_SECOND) || (unit == HOUR_MICROSECOND) || (unit == HOUR_SECOND) ||
                  (unit == HOUR_MINUTE) || (unit == DAY_MICROSECOND) || (unit == DAY_SECOND) ||
                  (unit == DAY_MINUTE) || (unit == DAY_HOUR)) {
        // This may change the day information

        int64_t seconds = (_day - 1) * 86400L + _hour * 3600L + _minute * 60 + _second +
                          sign * (interval.day * 86400 + interval.hour * 3600 +
                                  interval.minute * 60 + interval.second);
        int64_t days = seconds / 86400;
        seconds %= 86400L;
        if (seconds < 0) {
            seconds += 86400L;
            days--;
        }
        _second = seconds % 60;
        _minute = (seconds / 60) % 60;
        _hour = seconds / 3600;
        int64_t day_nr = doris::calc_daynr(_year, _month, 1) + days;
        if (!get_date_from_daynr(day_nr)) {
            return false;
        }
        if (_second || _minute || _hour) {
            _type = TIME_DATETIME;
        }
    } else if constexpr ((unit == DAY) || (unit == WEEK)) {
        // This only change day information, not change second information
        int64_t day_nr = daynr() + interval.day * sign;
        if (!get_date_from_daynr(day_nr)) {
            return false;
        }
    } else if constexpr (unit == YEAR) {
        // This only change year information
        _year += sign * interval.year;
        if (_year > 9999) {
            return false;
        }
        if (_month == 2 && _day == 29 && !doris::is_leap(_year)) {
            _day = 28;
        }
    } else if constexpr (unit == QUARTER || unit == MONTH || unit == YEAR_MONTH) {
        // This will change month and year information, maybe date.
        int64_t months = _year * 12 + _month - 1 + sign * (12 * interval.year + interval.month);
        _year = months / 12;
        if (months < 0) {
            return false;
        }
        if (_year > MAX_YEAR) {
            return false;
        }
        _month = (months % 12) + 1;
        if (_day > s_days_in_month[_month]) {
            _day = s_days_in_month[_month];
            if (_month == 2 && doris::is_leap(_year)) {
                _day++;
            }
        }
    }

    return true;
}

template <TimeUnit unit>
bool VecDateTimeValue::date_set_interval(const TimeInterval& interval) {
    static_assert(
            (unit == YEAR) || (unit == MONTH) || (unit == DAY) || (unit == HOUR) ||
                    (unit == MINUTE) || (unit == SECOND),
            "date_set_interval function now only support YEAR MONTH DAY HOUR MINUTE SECOND type");
    if constexpr ((unit == SECOND) || (unit == MINUTE) || (unit == HOUR)) {
        // This may change the day information
        int64_t seconds = interval.day * 86400L + interval.hour * 3600 + interval.minute * 60 +
                          interval.second;
        int64_t days = seconds / 86400;
        seconds %= 86400L;
        _second = seconds % 60;
        _minute = (seconds / 60) % 60;
        _hour = seconds / 3600;
        int64_t day_nr = doris::calc_daynr(_year, _month, 1) + days;
        if (!get_date_from_daynr(day_nr)) {
            return false;
        }
        if (_second || _minute || _hour) {
            _type = TIME_DATETIME;
        }
    } else if constexpr ((unit == DAY)) {
        // This only change day information, not change second information
        int64_t day_nr = interval.day;
        if (!get_date_from_daynr(day_nr)) {
            return false;
        }
    } else if constexpr (unit == YEAR) {
        // This only change year information
        _year = interval.year;
        _day = 1;
        _month = 1;
    } else if constexpr (unit == MONTH) {
        // This will change month and year information, maybe date.
        int64_t months = 12 * interval.year + interval.month;
        _year = months / 12;
        _day = 1;
        _month = (months % 12) + 1;
    }
    return true;
}

bool VecDateTimeValue::unix_timestamp(int64_t* timestamp, const std::string& timezone) const {
    cctz::time_zone ctz;
    if (!TimezoneUtils::find_cctz_time_zone(timezone, ctz)) {
        return false;
    }
    return unix_timestamp(timestamp, ctz);
}

bool VecDateTimeValue::unix_timestamp(int64_t* timestamp, const cctz::time_zone& ctz) const {
    const auto tp =
            cctz::convert(cctz::civil_second(_year, _month, _day, _hour, _minute, _second), ctz);
    *timestamp = tp.time_since_epoch().count();
    return true;
}

bool VecDateTimeValue::from_unixtime(int64_t timestamp, const std::string& timezone) {
    cctz::time_zone ctz;
    if (!TimezoneUtils::find_cctz_time_zone(timezone, ctz)) {
        return false;
    }
    from_unixtime(timestamp, ctz);
    return true;
}

void VecDateTimeValue::from_unixtime(int64_t timestamp, const cctz::time_zone& ctz) {
    static const cctz::time_point<cctz::sys_seconds> epoch =
            std::chrono::time_point_cast<cctz::sys_seconds>(
                    std::chrono::system_clock::from_time_t(0));
    cctz::time_point<cctz::sys_seconds> t = epoch + cctz::seconds(timestamp);

    const auto tp = cctz::convert(t, ctz);

    // there's no overflow check since it's hot path

    _neg = 0;
    _type = TIME_DATETIME;
    _year = tp.year();
    _month = tp.month();
    _day = tp.day();
    _hour = tp.hour();
    _minute = tp.minute();
    _second = tp.second();
}

const char* VecDateTimeValue::month_name() const {
    if (_month < 1 || _month > 12) {
        return NULL;
    }
    return s_month_name[_month];
}

const char* VecDateTimeValue::day_name() const {
    int day = weekday();
    if (day < 0 || day >= 7) {
        return NULL;
    }
    return s_day_name[day];
}

VecDateTimeValue VecDateTimeValue::local_time() {
    VecDateTimeValue value;
    value.from_unixtime(time(NULL), TimezoneUtils::default_time_zone);
    return value;
}

void VecDateTimeValue::set_time(uint32_t year, uint32_t month, uint32_t day, uint32_t hour,
                                uint32_t minute, uint32_t second) {
    _year = year;
    _month = month;
    _day = day;
    _hour = hour;
    _minute = minute;
    _second = second;
}

template <TimeUnit unit>
bool VecDateTimeValue::datetime_trunc() {
    if (!is_valid_date()) {
        return false;
    }
    switch (unit) {
    case SECOND: {
        break;
    }
    case MINUTE: {
        _second = 0;
        break;
    }
    case HOUR: {
        _second = 0;
        _minute = 0;
        break;
    }
    case DAY: {
        _second = 0;
        _minute = 0;
        _hour = 0;
        break;
    }
    case WEEK: {
        _second = 0;
        _minute = 0;
        _hour = 0;
        TimeInterval interval(DAY, weekday(), true);
        date_add_interval<DAY>(interval);
        break;
    }
    case MONTH: {
        _second = 0;
        _minute = 0;
        _hour = 0;
        _day = 1;
        break;
    }
    case QUARTER: {
        _second = 0;
        _minute = 0;
        _hour = 0;
        _day = 1;
        if (_month <= 3) {
            _month = 1;
        } else if (_month <= 6) {
            _month = 4;
        } else if (_month <= 9) {
            _month = 7;
        } else {
            _month = 10;
        }
        break;
    }
    case YEAR: {
        _second = 0;
        _minute = 0;
        _hour = 0;
        _day = 1;
        _month = 1;
        break;
    }
    default:
        return false;
    }
    return true;
}

template <typename T>
void VecDateTimeValue::create_from_date_v2(DateV2Value<T>& value, TimeType type) {
    if constexpr (std::is_same_v<T, DateV2ValueType>) {
        this->set_time(value.year(), value.month(), value.day(), 0, 0, 0);
    } else {
        this->set_time(value.year(), value.month(), value.day(), value.hour(), value.minute(),
                       value.second());
    }
    this->set_type(type);
    this->_neg = 0;
}

template <typename T>
void VecDateTimeValue::create_from_date_v2(DateV2Value<T>&& value, TimeType type) {
    DateV2Value<T> v = value;
    create_from_date_v2(v, type);
}

std::ostream& operator<<(std::ostream& os, const VecDateTimeValue& value) {
    char buf[64];
    value.to_string(buf);
    return os << buf;
}

// NOTE:
//  only support DATE - DATE (no support DATETIME - DATETIME)
std::size_t operator-(const VecDateTimeValue& v1, const VecDateTimeValue& v2) {
    return v1.daynr() - v2.daynr();
}

template <typename T>
std::size_t operator-(const DateV2Value<T>& v1, const VecDateTimeValue& v2) {
    return v1.daynr() - v2.daynr();
}

template <typename T>
std::size_t operator-(const VecDateTimeValue& v1, const DateV2Value<T>& v2) {
    return v1.daynr() - v2.daynr();
}

std::size_t hash_value(VecDateTimeValue const& value) {
    return HashUtil::hash(&value, sizeof(VecDateTimeValue), 0);
}

template <typename T>
bool DateV2Value<T>::is_invalid(uint32_t year, uint32_t month, uint32_t day, uint8_t hour,
                                uint8_t minute, uint8_t second, uint32_t microsecond,
                                bool only_time_part) {
    if (hour > 24 || minute >= 60 || second >= 60 || microsecond > 999999) {
        return true;
    }
    if (only_time_part) {
        return false;
    }
    if (year < MIN_YEAR || year > MAX_YEAR) {
        return true;
    }
    if (month == 2 && day == 29 && doris::is_leap(year)) return false;
    if (month == 0 || month > 12 || day > s_days_in_month[month] || day == 0) {
        return true;
    }
    return false;
}

template <typename T>
void DateV2Value<T>::format_datetime(uint32_t* date_val, bool* carry_bits) const {
    // ms
    DCHECK(date_val[6] < 1000000L);
    // hour, minute, second
    for (size_t i = 5; i > 2; i--) {
        if (date_val[i] == MAX_TIME_PART_VALUE[i - 3] + 1 && carry_bits[i + 1]) {
            date_val[i] = 0;
            date_val[i - 1] += 1;
            carry_bits[i] = true;
        }
    }
    // day
    if (date_val[1] == 2 && doris::is_leap(date_val[0])) {
        if (date_val[2] == 30 && carry_bits[3]) {
            date_val[2] = 1;
            date_val[1] += 1;
            carry_bits[2] = true;
        }
    } else if (date_val[2] == s_days_in_month[date_val[1]] + 1 && carry_bits[3]) {
        date_val[2] = 1;
        date_val[1] += 1;
        carry_bits[2] = true;
    }
    // month
    if (date_val[1] == 13 && carry_bits[2]) {
        date_val[1] = 1;
        date_val[0] += 1;
    }
}

// The interval format is that with no delimiters
// YYYY-MM-DD HH-MM-DD.FFFFFF AM in default format
// 0    1  2  3  4  5  6      7
template <typename T>
bool DateV2Value<T>::from_date_str(const char* date_str, int len, int scale /* = -1*/) {
    return from_date_str_base(date_str, len, scale, nullptr);
}
// when we parse
template <typename T>
bool DateV2Value<T>::from_date_str(const char* date_str, int len,
                                   const cctz::time_zone& local_time_zone, int scale /* = -1*/) {
    return from_date_str_base(date_str, len, scale, &local_time_zone);
}
template <typename T>
bool DateV2Value<T>::from_date_str_base(const char* date_str, int len, int scale,
                                        const cctz::time_zone* local_time_zone) {
    const char* ptr = date_str;
    const char* end = date_str + len;
    // ONLY 2, 6 can follow by a space
    const static int allow_space_mask = 4 | 64;
    uint32_t date_val[MAX_DATE_PARTS] = {0};
    int32_t date_len[MAX_DATE_PARTS] = {0};
    bool carry_bits[MAX_DATE_PARTS] = {false};

    // Skip space character
    while (ptr < end && isspace(*ptr)) {
        ptr++;
    }
    if (ptr == end || !isdigit(*ptr)) {
        return false;
    }
    // Fix year length
    const char* pos = ptr;
    while (pos < end && (isdigit(*pos) || *pos == 'T')) {
        pos++;
    }
    int year_len = 4;
    int digits = pos - ptr;
    bool is_interval_format = false;
    bool has_bar = false;

    // Compatible with MySQL.
    // For YYYYMMDD/YYYYMMDDHHMMSS is 4 digits years
    if (pos == end || *pos == '.' ||
        time_zone_begins(pos, end)) { // no delimeter until ./Asia/Z/GMT...
        if (digits == 4 || digits == 8 || digits >= 14) {
            year_len = 4;
        } else {
            year_len = 2;
        }
        is_interval_format = true;
    }

    int field_idx = 0;
    int field_len = year_len;
    long sec_offset = 0;
    while (ptr < end && isdigit(*ptr) && field_idx < MAX_DATE_PARTS) {
        const char* start = ptr;
        int temp_val = 0;
        bool scan_to_delim = (!is_interval_format) && (field_idx != 6);
        while (ptr < end && isdigit(*ptr) && (scan_to_delim || field_len--)) { // field_len <= 6
            temp_val = temp_val * 10 + (*ptr++ - '0');
        }
        if (field_idx == 6) {
            // Microsecond
            const auto ms_part = ptr - start;
            temp_val *= int_exp10(std::max(0L, 6 - ms_part));
            if constexpr (is_datetime) {
                if (scale >= 0) {
                    if (scale == 6 && ms_part > 6) {
                        if (ptr < end && isdigit(*ptr) && *ptr >= '5') {
                            temp_val += 1;
                        }
                    } else {
                        const int divisor = int_exp10(6 - scale);
                        int remainder = temp_val % divisor;
                        temp_val /= divisor;
                        if (scale < 6 && std::abs(remainder) >= (divisor >> 1)) {
                            temp_val += 1;
                        }
                        temp_val *= divisor;
                        if (temp_val == 1000000L) {
                            temp_val = 0;
                            date_val[field_idx - 1] += 1;
                            carry_bits[field_idx] = true;
                        }
                    }
                }
            }
        }
        // Impossible
        if (temp_val > 999999L) {
            return false;
        }
        date_val[field_idx] = temp_val;
        date_len[field_idx] = ptr - start;
        field_len = 2;

        if (ptr == end) {
            field_idx++;
            break;
        }

        // timezone
        if (UNLIKELY((field_idx > 2 ||
                      !has_bar) /*dont treat xxxx-xx-xx:xx:xx as xxxx-xx(-xx:xx:xx)*/
                     && time_zone_begins(ptr, end))) {
            if (local_time_zone == nullptr) {
                return false;
            }
            auto get_tz_offset = [&](const std::string& str_tz,
                                     const cctz::time_zone* local_time_zone) -> long {
                cctz::time_zone given_tz {};
                if (!TimezoneUtils::find_cctz_time_zone(str_tz, given_tz)) {
                    throw Exception {ErrorCode::INVALID_ARGUMENT, ""};
                }
                auto given = cctz::convert(cctz::civil_second {}, given_tz);
                auto local = cctz::convert(cctz::civil_second {}, *local_time_zone);
                // these two values is absolute time. so they are negative. need to use (-local) - (-given)
                return std::chrono::duration_cast<std::chrono::seconds>(given - local).count();
            };
            try {
                sec_offset = get_tz_offset(std::string {ptr, end},
                                           local_time_zone); // use the whole remain string
            } catch ([[maybe_unused]] Exception& e) {
                return false; // invalid format
            }
            field_idx++;
            break;
        }

        if (field_idx == 2 && *ptr == 'T') {
            // YYYYMMDDTHHMMDD, skip 'T' and continue
            ptr++;
            field_idx++;
            continue;
        }

        // Second part
        if (field_idx == 5) {
            if (*ptr == '.') {
                ptr++;
                field_len = 6;
            } else if (isdigit(*ptr)) {
                field_idx++;
                break;
            }
            field_idx++;
            continue;
        }
        // escape separator
        while (ptr < end && (ispunct(*ptr) || isspace(*ptr))) {
            if (isspace(*ptr)) {
                if (((1 << field_idx) & allow_space_mask) == 0) {
                    return false;
                }
            }
            if (*ptr == '-') {
                has_bar = true;
            }
            ptr++;
        }
        field_idx++;
    }
    int num_field = field_idx;
    if (!is_interval_format) {
        year_len = date_len[0];
    }
    for (; field_idx < MAX_DATE_PARTS; ++field_idx) {
        date_val[field_idx] = 0;
    }

    if (year_len == 2) {
        if (date_val[0] < YY_PART_YEAR) {
            date_val[0] += 2000;
        } else {
            date_val[0] += 1900;
        }
    }

    if (num_field < 3) {
        return false;
    }
    if (is_invalid(date_val[0], date_val[1], date_val[2], 0, 0, 0, 0)) {
        return false;
    }
    format_datetime(date_val, carry_bits);
    if (!check_range_and_set_time(date_val[0], date_val[1], date_val[2], date_val[3], date_val[4],
                                  date_val[5], date_val[6])) {
        return false;
    }
    return sec_offset ? date_add_interval<TimeUnit::SECOND>(
                                TimeInterval {TimeUnit::SECOND, sec_offset, false})
                      : true;
}

template <typename T>
void DateV2Value<T>::set_zero() {
    int_val_ = 0;
}

// this method is exactly same as fromDateFormatStr() in DateLiteral.java in FE
// change this method should also change that.
template <typename T>
bool DateV2Value<T>::from_date_format_str(const char* format, int format_len, const char* value,
                                          int value_len, const char** sub_val_end) {
    const char* ptr = format;
    const char* end = format + format_len;
    const char* val = value;
    const char* val_end = value + value_len;

    bool date_part_used = false;
    bool time_part_used = false;
    bool frac_part_used = false;
    bool already_set_time_part = false;

    int day_part = 0;
    int weekday = -1;
    int yearday = -1;
    int week_num = -1;

    bool strict_week_number = false;
    bool sunday_first = false;
    bool strict_week_number_year_type = false;
    int strict_week_number_year = -1;
    bool usa_time = false;

    auto [year, month, day, hour, minute, second, microsecond] = std::tuple {0, 0, 0, 0, 0, 0, 0};
    while (ptr < end && val < val_end) {
        // Skip space character
        while (val < val_end && isspace(*val)) {
            val++;
        }
        if (val >= val_end) {
            break;
        }
        // Check switch
        if (*ptr == '%' && ptr + 1 < end) {
            const char* tmp = NULL;
            int64_t int_value = 0;
            ptr++;
            switch (*ptr++) {
                // Year
            case 'y':
                // Year, numeric (two digits)
                tmp = val + min(2, val_end - val);
                if (!str_to_int64(val, &tmp, &int_value)) {
                    return false;
                }
                int_value += int_value >= 70 ? 1900 : 2000;
                year = int_value;
                val = tmp;
                date_part_used = true;
                break;
            case 'Y':
                // Year, numeric, four digits
                tmp = val + min(4, val_end - val);
                if (!str_to_int64(val, &tmp, &int_value)) {
                    return false;
                }
                if (tmp - val <= 2) {
                    int_value += int_value >= 70 ? 1900 : 2000;
                }
                year = int_value;
                val = tmp;
                date_part_used = true;
                break;
                // Month
            case 'm':
            case 'c':
                tmp = val + min(2, val_end - val);
                if (!str_to_int64(val, &tmp, &int_value)) {
                    return false;
                }
                month = int_value;
                val = tmp;
                date_part_used = true;
                break;
            case 'M':
                int_value = check_word(const_cast<const char**>(s_month_name), val, val_end, &val);
                if (int_value < 0) {
                    return false;
                }
                month = int_value;
                break;
            case 'b':
                int_value = check_word(s_ab_month_name, val, val_end, &val);
                if (int_value < 0) {
                    return false;
                }
                month = int_value;
                break;
                // Day
            case 'd':
            case 'e':
                tmp = val + min(2, val_end - val);
                if (!str_to_int64(val, &tmp, &int_value)) {
                    return false;
                }
                day = int_value;
                val = tmp;
                date_part_used = true;
                break;
            case 'D':
                tmp = val + min(2, val_end - val);
                if (!str_to_int64(val, &tmp, &int_value)) {
                    return false;
                }
                day = int_value;
                val = tmp + min(2, val_end - tmp);
                date_part_used = true;
                break;
                // Hour
            case 'h':
            case 'I':
            case 'l':
                usa_time = true;
                // Fall through
            case 'k':
            case 'H':
                tmp = val + min(2, val_end - val);
                if (!str_to_int64(val, &tmp, &int_value)) {
                    return false;
                }
                hour = int_value;
                val = tmp;
                time_part_used = true;
                break;
                // Minute
            case 'i':
                tmp = val + min(2, val_end - val);
                if (!str_to_int64(val, &tmp, &int_value)) {
                    return false;
                }
                minute = int_value;
                val = tmp;
                time_part_used = true;
                break;
                // Second
            case 's':
            case 'S':
                tmp = val + min(2, val_end - val);
                if (!str_to_int64(val, &tmp, &int_value)) {
                    return false;
                }
                second = int_value;
                val = tmp;
                time_part_used = true;
                break;
            // Micro second
            case 'f':
                tmp = val;
                // when there's still something to the end, fix the scale of ms.
                while (tmp < val_end && isdigit(*tmp)) {
                    tmp++;
                }

                if (tmp - val > 6) {
                    const char* tmp2 = val + 6;
                    if (!str_to_int64(val, &tmp2, &int_value)) {
                        return false;
                    }
                } else {
                    if (!str_to_int64(val, &tmp, &int_value)) {
                        return false;
                    }
                }
                if constexpr (is_datetime) {
                    microsecond = int_value * int_exp10(6 - min(6, tmp - val));
                    frac_part_used = true;
                }
                val = tmp;
                time_part_used = true;
                break;
                // AM/PM
            case 'p':
                if ((val_end - val) < 2 || toupper(*(val + 1)) != 'M' || !usa_time) {
                    return false;
                }
                if (toupper(*val) == 'P') {
                    // PM
                    day_part = 12;
                }
                time_part_used = true;
                val += 2;
                break;
                // Weekday
            case 'W':
                int_value = check_word(const_cast<const char**>(s_day_name), val, val_end, &val);
                if (int_value < 0) {
                    return false;
                }
                int_value++;
                weekday = int_value;
                date_part_used = true;
                break;
            case 'a':
                int_value = check_word(s_ab_day_name, val, val_end, &val);
                if (int_value < 0) {
                    return false;
                }
                int_value++;
                weekday = int_value;
                date_part_used = true;
                break;
            case 'w':
                tmp = val + min(1, val_end - val);
                if (!str_to_int64(val, &tmp, &int_value)) {
                    return false;
                }
                if (int_value >= 7) {
                    return false;
                }
                if (int_value == 0) {
                    int_value = 7;
                }
                weekday = int_value;
                val = tmp;
                date_part_used = true;
                break;
            case 'j':
                tmp = val + min(3, val_end - val);
                if (!str_to_int64(val, &tmp, &int_value)) {
                    return false;
                }
                yearday = int_value;
                val = tmp;
                date_part_used = true;
                break;
            case 'u':
            case 'v':
            case 'U':
            case 'V':
                sunday_first = (*(ptr - 1) == 'U' || *(ptr - 1) == 'V');
                // Used to check if there is %x or %X
                strict_week_number = (*(ptr - 1) == 'V' || *(ptr - 1) == 'v');
                tmp = val + min(2, val_end - val);
                if (!str_to_int64(val, &tmp, &int_value)) {
                    return false;
                }
                week_num = int_value;
                if (week_num > 53 || (strict_week_number && week_num == 0)) {
                    return false;
                }
                val = tmp;
                date_part_used = true;
                break;
                // strict week number, must be used with %V or %v
            case 'x':
            case 'X':
                strict_week_number_year_type = (*(ptr - 1) == 'X');
                tmp = val + min(4, val_end - val);
                if (!str_to_int64(val, &tmp, &int_value)) {
                    return false;
                }
                strict_week_number_year = int_value;
                val = tmp;
                date_part_used = true;
                break;
            case 'r': {
                if constexpr (is_datetime) {
                    DateV2Value<DateTimeV2ValueType> tmp_val;
                    if (!tmp_val.from_date_format_str("%I:%i:%S %p", 11, val, val_end - val,
                                                      &tmp)) {
                        return false;
                    }
                    this->date_v2_value_.hour_ = tmp_val.hour();
                    this->date_v2_value_.minute_ = tmp_val.minute();
                    this->date_v2_value_.second_ = tmp_val.second();
                    val = tmp;
                    time_part_used = true;
                    already_set_time_part = true;
                    break;
                } else {
                    return false;
                }
            }
            case 'T': {
                if constexpr (is_datetime) {
                    DateV2Value<DateTimeV2ValueType> tmp_val;
                    if (!tmp_val.from_date_format_str("%H:%i:%S", 8, val, val_end - val, &tmp)) {
                        return false;
                    }
                    this->date_v2_value_.hour_ = tmp_val.hour();
                    this->date_v2_value_.minute_ = tmp_val.minute();
                    this->date_v2_value_.second_ = tmp_val.second();
                    time_part_used = true;
                    already_set_time_part = true;
                    val = tmp;
                    break;
                } else {
                    return false;
                }
            }
            case '.':
                while (val < val_end && ispunct(*val)) {
                    val++;
                }
                break;
            case '@':
                while (val < val_end && isalpha(*val)) {
                    val++;
                }
                break;
            case '#':
                while (val < val_end && isdigit(*val)) {
                    val++;
                }
                break;
            case '%': // %%, escape the %
                if ('%' != *val) {
                    return false;
                }
                val++;
                break;
            default:
                return false;
            }
        } else if (!isspace(*ptr)) {
            if (*ptr != *val) {
                return false;
            }
            ptr++;
            val++;
        } else {
            ptr++;
        }
    }

    // continue to iterate pattern if has
    // to find out if it has time part.
    while (ptr < end) {
        if (*ptr == '%' && ptr + 1 < end) {
            ptr++;
            switch (*ptr++) {
            case 'H':
            case 'h':
            case 'I':
            case 'i':
            case 'k':
            case 'l':
            case 'r':
            case 's':
            case 'f':
            case 'S':
            case 'p':
            case 'T':
                time_part_used = true;
                break;
            default:
                break;
            }
        } else {
            ptr++;
        }
    }

    if (usa_time) {
        if (hour > 12 || hour < 1) {
            return false;
        }
        hour = (hour % 12) + day_part;
    }
    if (sub_val_end) {
        *sub_val_end = val;
    }

    // Compute timestamp type
    if (frac_part_used) {
        if constexpr (!is_datetime) {
            LOG(WARNING) << "Microsecond is not allowed for date type!";
            return false;
        }
    } else if (time_part_used) {
        if constexpr (!is_datetime) {
            LOG(WARNING) << "Time part is not allowed for date type!";
            return false;
        }
    }

    // Year day
    if (yearday > 0) {
        uint64_t days = doris::calc_daynr(year, 1, 1) + yearday - 1;
        if (!get_date_from_daynr(days)) {
            return false;
        }
    }
    // weekday
    if (week_num >= 0 && weekday > 0) {
        // Check
        if ((strict_week_number &&
             (strict_week_number_year < 0 || strict_week_number_year_type != sunday_first)) ||
            (!strict_week_number && strict_week_number_year >= 0)) {
            return false;
        }
        uint64_t days =
                doris::calc_daynr(strict_week_number ? strict_week_number_year : year, 1, 1);

        uint8_t weekday_b = doris::calc_weekday(days, sunday_first);

        if (sunday_first) {
            days += ((weekday_b == 0) ? 0 : 7) - weekday_b + (week_num - 1) * 7 + weekday % 7;
        } else {
            days += ((weekday_b <= 3) ? 0 : 7) - weekday_b + (week_num - 1) * 7 + weekday - 1;
        }
        if (!get_date_from_daynr(days)) {
            return false;
        }
    }
    // 1. already_set_date_part means _year, _month, _day be set, so we only set time part
    // 2. already_set_time_part means _hour, _minute, _second, _microsecond be set,
    //    so we only need to set date part
    // 3. if both are true, means all part of date_time be set, no need check_range_and_set_time
    bool already_set_date_part = yearday > 0 || (week_num >= 0 && weekday > 0);
    if (already_set_date_part && already_set_time_part) return true;
    if (already_set_date_part) {
        if constexpr (is_datetime) {
            return check_range_and_set_time(date_v2_value_.year_, date_v2_value_.month_,
                                            date_v2_value_.day_, hour, minute, second, microsecond);
        } else {
            return check_range_and_set_time(date_v2_value_.year_, date_v2_value_.month_,
                                            date_v2_value_.day_, 0, 0, 0, 0);
        }
    }
    if (already_set_time_part) {
        if constexpr (is_datetime) {
            return check_range_and_set_time(year, month, day, date_v2_value_.hour_,
                                            date_v2_value_.minute_, date_v2_value_.second_,
                                            microsecond);
        } else {
            return check_range_and_set_time(year, month, day, 0, 0, 0, 0);
        }
    }
    if constexpr (is_datetime) {
        return check_range_and_set_time(year, month, day, hour, minute, second, microsecond,
                                        time_part_used && !date_part_used);
    } else {
        return check_range_and_set_time(year, month, day, 0, 0, 0, 0);
    }
}

template <typename T>
int32_t DateV2Value<T>::to_buffer(char* buffer, int scale) const {
    // if this is an invalid date, write nothing(instead of 0000-00-00) to output string, or else
    // it will cause problem for null DataTypeDateV2 value in cast function,
    // e.g. cast(cast(null_date as char) as date)
    if (!is_valid_date()) {
        return 0;
    }
    char* start = buffer;
    uint32_t temp;
    // Year
    temp = date_v2_value_.year_ / 100;
    *buffer++ = (char)('0' + (temp / 10));
    *buffer++ = (char)('0' + (temp % 10));
    temp = date_v2_value_.year_ % 100;
    *buffer++ = (char)('0' + (temp / 10));
    *buffer++ = (char)('0' + (temp % 10));
    *buffer++ = '-';
    // Month
    *buffer++ = (char)('0' + (date_v2_value_.month_ / 10));
    *buffer++ = (char)('0' + (date_v2_value_.month_ % 10));
    *buffer++ = '-';
    // Day
    *buffer++ = (char)('0' + (date_v2_value_.day_ / 10));
    *buffer++ = (char)('0' + (date_v2_value_.day_ % 10));
    if constexpr (is_datetime) {
        *buffer++ = ' ';
        // Hour
        temp = date_v2_value_.hour_;
        if (temp >= 100) {
            *buffer++ = (char)('0' + (temp / 100));
            temp %= 100;
        }
        *buffer++ = (char)('0' + (temp / 10));
        *buffer++ = (char)('0' + (temp % 10));
        *buffer++ = ':';
        // Minute
        *buffer++ = (char)('0' + (date_v2_value_.minute_ / 10));
        *buffer++ = (char)('0' + (date_v2_value_.minute_ % 10));
        *buffer++ = ':';
        /* Second */
        *buffer++ = (char)('0' + (date_v2_value_.second_ / 10));
        *buffer++ = (char)('0' + (date_v2_value_.second_ % 10));
        if (scale < 0 && date_v2_value_.microsecond_ > 0) {
            *buffer++ = '.';
            /* Microsecond */
            uint32_t ms = date_v2_value_.microsecond_;
            int ms_width = scale == -1 ? 6 : std::min(6, scale);
            for (int i = 0; i < ms_width; i++) {
                *buffer++ = (char)('0' + (ms / int_exp10(5 - i)));
                ms %= (uint32_t)int_exp10(5 - i);
            }
        } else if (scale > 0) {
            *buffer++ = '.';
            /* Microsecond */
            uint32_t ms = date_v2_value_.microsecond_;
            int ms_width = std::min(6, scale);
            for (int i = 0; i < ms_width; i++) {
                *buffer++ = (char)('0' + (ms / int_exp10(5 - i)));
                ms %= (uint32_t)int_exp10(5 - i);
            }
        }
    }
    return buffer - start;
}

template <typename T>
char* DateV2Value<T>::to_string(char* to, int scale) const {
    int len = to_buffer(to, scale);
    *(to + len) = '\0';
    return to + len + 1;
}

template <typename T>
typename DateV2Value<T>::underlying_value DateV2Value<T>::to_date_int_val() const {
    return int_val_;
}
// [1900-01-01, 2039-12-31]
static std::array<DateV2Value<DateV2ValueType>, date_day_offset_dict::DICT_DAYS>
        DATE_DAY_OFFSET_ITEMS;
// [1900-01-01, 2039-12-31]
static std::array<std::array<std::array<int, 31>, 12>, 140> DATE_DAY_OFFSET_DICT;

static bool DATE_DAY_OFFSET_ITEMS_INIT = false;

date_day_offset_dict date_day_offset_dict::instance = date_day_offset_dict();

date_day_offset_dict& date_day_offset_dict::get() {
    return instance;
}

bool date_day_offset_dict::get_dict_init() {
    return DATE_DAY_OFFSET_ITEMS_INIT;
}

date_day_offset_dict::date_day_offset_dict() {
    DateV2Value<DateV2ValueType> d;
    // Init days before epoch.
    d.set_time(1969, 12, 31, 0, 0, 0, 0);
    for (int i = 0; i < DAY_BEFORE_EPOCH; ++i) {
        DATE_DAY_OFFSET_ITEMS[DAY_BEFORE_EPOCH - i - 1] = d;
        DATE_DAY_OFFSET_DICT[d.year() - START_YEAR][d.month() - 1][d.day() - 1] =
                calc_daynr(d.year(), d.month(), d.day());
        d -= 1;
    }
    // Init epoch day.
    d.set_time(1970, 1, 1, 0, 0, 0, 0);
    DATE_DAY_OFFSET_ITEMS[DAY_BEFORE_EPOCH] = d;
    DATE_DAY_OFFSET_DICT[d.year() - START_YEAR][d.month() - 1][d.day() - 1] =
            calc_daynr(d.year(), d.month(), d.day());
    d += 1;

    // Init days after epoch.
    for (int i = 0; i < DAY_AFTER_EPOCH; ++i) {
        DATE_DAY_OFFSET_ITEMS[DAY_BEFORE_EPOCH + 1 + i] = d;
        DATE_DAY_OFFSET_DICT[d.year() - START_YEAR][d.month() - 1][d.day() - 1] =
                calc_daynr(d.year(), d.month(), d.day());
        d += 1;
    }

    DATE_DAY_OFFSET_ITEMS_INIT = true;
}

DateV2Value<DateV2ValueType> date_day_offset_dict::operator[](int day) const {
    int index = day + DAY_BEFORE_EPOCH;
    if (LIKELY(index >= 0 && index < DICT_DAYS)) {
        return DATE_DAY_OFFSET_ITEMS[index];
    } else {
        DateV2Value<DateV2ValueType> d = DATE_DAY_OFFSET_ITEMS[0];
        return d += index;
    }
}

int date_day_offset_dict::daynr(int year, int month, int day) const {
    return DATE_DAY_OFFSET_DICT[year - START_YEAR][month - 1][day - 1];
}

template <typename T>
uint32_t DateV2Value<T>::set_date_uint32(uint32_t int_val) {
    union DateV2UInt32Union {
        DateV2Value<T> dt;
        uint32_t ui32;
        ~DateV2UInt32Union() {}
    };
    DateV2UInt32Union conv = {.ui32 = int_val};
    if (is_invalid(conv.dt.year(), conv.dt.month(), conv.dt.day(), 0, 0, 0, 0)) {
        return 0;
    }
    this->set_time(conv.dt.year(), conv.dt.month(), conv.dt.day(), 0, 0, 0, 0);

    return int_val;
}

template <typename T>
uint64_t DateV2Value<T>::set_datetime_uint64(uint64_t int_val) {
    union DateTimeV2UInt64Union {
        DateV2Value<T> dt;
        uint64_t ui64;
        ~DateTimeV2UInt64Union() {}
    };
    DateTimeV2UInt64Union conv = {.ui64 = int_val};
    if (is_invalid(conv.dt.year(), conv.dt.month(), conv.dt.day(), conv.dt.hour(), conv.dt.minute(),
                   conv.dt.second(), conv.dt.microsecond())) {
        return 0;
    }
    this->set_time(conv.dt.year(), conv.dt.month(), conv.dt.day(), conv.dt.hour(), conv.dt.minute(),
                   conv.dt.second(), conv.dt.microsecond());

    return int_val;
}

template <typename T>
uint8_t DateV2Value<T>::week(uint8_t mode) const {
    uint16_t year = 0;
    return calc_week(this->daynr(), this->year(), this->month(), this->day(), mode, &year);
}

template <typename T>
uint32_t DateV2Value<T>::year_week(uint8_t mode) const {
    if (config::enable_time_lut && mode == 4 && this->year() >= 1950 && this->year() < 2030) {
        return doris::TimeLUT::GetImplement()
                ->year_week_table[this->year() - 1950][this->month() - 1][this->day() - 1];
    }
    uint16_t year = 0;
    // The range of the week in the year_week is 1-53, so the mode WEEK_YEAR is always true.
    uint8_t week = calc_week(this->daynr(), this->year(), this->month(), this->day(), mode | 2,
                             &year, true);
    // When the mode WEEK_FIRST_WEEKDAY is not set,
    // the week in which the last three days of the year fall may belong to the following year.
    if (week == 53 && day() >= 29 && !(mode & 4)) {
        uint8_t monday_first = mode & WEEK_MONDAY_FIRST;
        uint64_t daynr_of_last_day = doris::calc_daynr(this->year(), 12, 31);
        uint8_t weekday_of_last_day = doris::calc_weekday(daynr_of_last_day, !monday_first);

        if (weekday_of_last_day - monday_first < 2) {
            ++year;
            week = 1;
        }
    }
    return year * 100 + week;
}

template <typename T>
bool DateV2Value<T>::get_date_from_daynr(uint64_t daynr) {
    if (daynr <= 0 || daynr > DATE_MAX_DAYNR) {
        return false;
    }
    auto [year, month, day] = std::tuple {0, 0, 0};

    if (date_day_offset_dict::can_speed_up_daynr_to_date(daynr) &&
        LIKELY(date_day_offset_dict::get_dict_init())) {
        auto dt = date_day_offset_dict::get()[date_day_offset_dict::get_offset_by_daynr(daynr)];
        year = dt.year();
        month = dt.month();
        day = dt.day();
    } else {
        year = daynr / 365;
        uint32_t days_befor_year = 0;
        while (daynr < (days_befor_year = doris::calc_daynr(year, 1, 1))) {
            year--;
        }
        uint32_t days_of_year = daynr - days_befor_year + 1;
        int leap_day = 0;
        if (doris::is_leap(year)) {
            if (days_of_year > 31 + 28) {
                days_of_year--;
                if (days_of_year == 31 + 28) {
                    leap_day = 1;
                }
            }
        }
        month = 1;
        while (days_of_year > s_days_in_month[month]) {
            days_of_year -= s_days_in_month[month];
            month++;
        }
        day = days_of_year + leap_day;

        if (is_invalid(year, month, day, this->hour(), this->minute(), this->second(),
                       this->microsecond())) {
            return false;
        }
    }

    set_time(year, month, day, this->hour(), this->minute(), this->second(), this->microsecond());
    return true;
}

template <typename T>
template <TimeUnit unit, typename TO>
bool DateV2Value<T>::date_add_interval(const TimeInterval& interval, DateV2Value<TO>& to_value) {
    if (!is_valid_date()) return false;

    int sign = interval.is_neg ? -1 : 1;

    if constexpr ((unit == MICROSECOND) || (unit == MILLISECOND) || (unit == SECOND) ||
                  (unit == MINUTE) || (unit == HOUR) || (unit == SECOND_MICROSECOND) ||
                  (unit == MINUTE_MICROSECOND) || (unit == MINUTE_SECOND) ||
                  (unit == HOUR_MICROSECOND) || (unit == HOUR_SECOND) || (unit == HOUR_MINUTE) ||
                  (unit == DAY_MICROSECOND) || (unit == DAY_SECOND) || (unit == DAY_MINUTE) ||
                  (unit == DAY_HOUR) || (unit == DAY) || (unit == WEEK)) {
        // This may change the day information
        constexpr int64_t microseconds_in_one_second = 1000000L;
        int64_t microseconds = this->microsecond() + sign * interval.microsecond +
                               sign * interval.millisecond * 1000L;
        int64_t extra_second = microseconds / microseconds_in_one_second;
        microseconds -= extra_second * microseconds_in_one_second;

        int64_t seconds = (this->day() - 1) * 86400L + this->hour() * 3600L + this->minute() * 60 +
                          this->second() +
                          sign * (interval.day * 86400 + interval.hour * 3600 +
                                  interval.minute * 60 + interval.second) +
                          extra_second;
        if (microseconds < 0) {
            seconds--;
            microseconds += microseconds_in_one_second;
        }
        int64_t days = seconds / 86400;
        seconds %= 86400L;
        if (seconds < 0) {
            seconds += 86400L;
            days--;
        }
        int64_t day_nr = doris::calc_daynr(this->year(), this->month(), 1) + days;
        if (!to_value.get_date_from_daynr(day_nr)) {
            return false;
        }
        to_value.set_time(seconds / 3600, (seconds / 60) % 60, seconds % 60, microseconds);
    } else if constexpr (unit == YEAR) {
        // This only change year information
        to_value.template set_time_unit<TimeUnit::YEAR>(date_v2_value_.year_ + interval.year);
        if (to_value.year() > 9999) {
            return false;
        }
        if (date_v2_value_.month_ == 2 && date_v2_value_.day_ == 29 &&
            !doris::is_leap(to_value.year())) {
            to_value.template set_time_unit<TimeUnit::DAY>(28);
        }
    } else if constexpr (unit == QUARTER || unit == MONTH || unit == YEAR_MONTH) {
        // This will change month and year information, maybe date.
        int64_t months = date_v2_value_.year_ * 12 + date_v2_value_.month_ - 1 +
                         12 * interval.year + interval.month;
        to_value.template set_time_unit<TimeUnit::YEAR>(months / 12);
        if (months < 0) {
            return false;
        }
        if (to_value.year() > MAX_YEAR) {
            return false;
        }
        to_value.template set_time_unit<TimeUnit::MONTH>((months % 12) + 1);
        if (date_v2_value_.day_ > s_days_in_month[to_value.month()]) {
            date_v2_value_.day_ = s_days_in_month[to_value.month()];
            if (to_value.month() == 2 && doris::is_leap(to_value.year())) {
                to_value.template set_time_unit<TimeUnit::DAY>(date_v2_value_.day_ + 1);
            }
        }
    }
    return true;
}

template <typename T>
template <TimeUnit unit, bool need_check>
bool DateV2Value<T>::date_add_interval(const TimeInterval& interval) {
    if constexpr (need_check) {
        if (!is_valid_date()) return false;
    }

    int sign = interval.is_neg ? -1 : 1;

    if constexpr ((unit == MICROSECOND) || (unit == MILLISECOND) || (unit == SECOND) ||
                  (unit == MINUTE) || (unit == HOUR) || (unit == SECOND_MICROSECOND) ||
                  (unit == MINUTE_MICROSECOND) || (unit == MINUTE_SECOND) ||
                  (unit == HOUR_MICROSECOND) || (unit == HOUR_SECOND) || (unit == HOUR_MINUTE) ||
                  (unit == DAY_MICROSECOND) || (unit == DAY_SECOND) || (unit == DAY_MINUTE) ||
                  (unit == DAY_HOUR) || (unit == DAY) || (unit == WEEK)) {
        // This may change the day information
        constexpr int64_t microseconds_in_one_second = 1000000L;
        int64_t microseconds = this->microsecond() + sign * interval.microsecond +
                               sign * interval.millisecond * 1000L;
        int64_t extra_second = microseconds / microseconds_in_one_second;
        microseconds -= extra_second * microseconds_in_one_second;

        int64_t seconds = (this->day() - 1) * 86400L + this->hour() * 3600L + this->minute() * 60 +
                          this->second() +
                          sign * (interval.day * 86400 + interval.hour * 3600 +
                                  interval.minute * 60 + interval.second) +
                          extra_second;
        if (microseconds < 0) {
            seconds--;
            microseconds += microseconds_in_one_second;
        }
        int64_t days = seconds / 86400;
        seconds %= 86400L;
        if (seconds < 0) {
            seconds += 86400L;
            days--;
        }
        int64_t day_nr = doris::calc_daynr(this->year(), this->month(), 1) + days;
        if (!this->get_date_from_daynr(day_nr)) {
            return false;
        }
        if constexpr (is_datetime) {
            this->set_time(seconds / 3600, (seconds / 60) % 60, seconds % 60, microseconds);
        }
    } else if constexpr (unit == YEAR) {
        // This only change year information
        this->template set_time_unit<TimeUnit::YEAR>(date_v2_value_.year_ + interval.year);
        if (this->year() > 9999) {
            return false;
        }
        if (date_v2_value_.month_ == 2 && date_v2_value_.day_ == 29 &&
            !doris::is_leap(this->year())) {
            this->template set_time_unit<TimeUnit::DAY>(28);
        }
    } else if constexpr (unit == QUARTER || unit == MONTH || unit == YEAR_MONTH) {
        // This will change month and year information, maybe date.
        int64_t months = date_v2_value_.year_ * 12 + date_v2_value_.month_ - 1 +
                         12 * interval.year + interval.month;
        this->template set_time_unit<TimeUnit::YEAR>(months / 12);
        if (months < 0) {
            return false;
        }
        if (this->year() > MAX_YEAR) {
            return false;
        }
        this->template set_time_unit<TimeUnit::MONTH>((months % 12) + 1);
        if (date_v2_value_.day_ > s_days_in_month[this->month()]) {
            date_v2_value_.day_ = s_days_in_month[this->month()];
            if (this->month() == 2 && doris::is_leap(this->year())) {
                this->template set_time_unit<TimeUnit::DAY>(date_v2_value_.day_ + 1);
            }
        }
    }
    return true;
}

template <typename T>
template <TimeUnit unit>
bool DateV2Value<T>::date_set_interval(const TimeInterval& interval) {
    static_assert(
            (unit == YEAR) || (unit == MONTH) || (unit == DAY) || (unit == HOUR) ||
                    (unit == MINUTE) || (unit == SECOND),
            "date_set_interval function now only support YEAR MONTH DAY HOUR MINUTE SECOND type");
    if constexpr ((unit == SECOND) || (unit == MINUTE) || (unit == HOUR) || (unit == DAY)) {
        set_zero();
        // This may change the day information
        int64_t seconds = (interval.day * 86400 + interval.hour * 3600 + interval.minute * 60 +
                           interval.second);
        int64_t days = seconds / 86400;
        seconds %= 86400L;
        if (!this->get_date_from_daynr(days)) {
            return false;
        }
        if constexpr (is_datetime) {
            this->set_time(seconds / 3600, (seconds / 60) % 60, seconds % 60, 0);
        }
    } else if constexpr (unit == YEAR) {
        this->set_time(0, 1, 1, 0, 0, 0, 0);
        this->template set_time_unit<TimeUnit::YEAR>(interval.year);
    } else if constexpr (unit == MONTH) {
        // This will change month and year information, maybe date.
        this->set_time(0, 1, 1, 0, 0, 0, 0);
        int64_t months = 12 * interval.year + interval.month;
        this->template set_time_unit<TimeUnit::YEAR>(months / 12);
        this->template set_time_unit<TimeUnit::MONTH>((months % 12) + 1);
    }
    return true;
}

template <typename T>
template <TimeUnit unit>
bool DateV2Value<T>::datetime_trunc() {
    if constexpr (is_datetime) {
        if (!is_valid_date()) {
            return false;
        }
        switch (unit) {
        case SECOND: {
            date_v2_value_.microsecond_ = 0;
            break;
        }
        case MINUTE: {
            date_v2_value_.microsecond_ = 0;
            date_v2_value_.second_ = 0;
            break;
        }
        case HOUR: {
            date_v2_value_.microsecond_ = 0;
            date_v2_value_.second_ = 0;
            date_v2_value_.minute_ = 0;
            break;
        }
        case DAY: {
            date_v2_value_.microsecond_ = 0;
            date_v2_value_.second_ = 0;
            date_v2_value_.minute_ = 0;
            date_v2_value_.hour_ = 0;
            break;
        }
        case WEEK: {
            date_v2_value_.microsecond_ = 0;
            date_v2_value_.second_ = 0;
            date_v2_value_.minute_ = 0;
            date_v2_value_.hour_ = 0;
            TimeInterval interval(DAY, weekday(), true);
            date_add_interval<DAY>(interval);
            break;
        }
        case MONTH: {
            date_v2_value_.microsecond_ = 0;
            date_v2_value_.second_ = 0;
            date_v2_value_.minute_ = 0;
            date_v2_value_.hour_ = 0;
            date_v2_value_.day_ = 1;
            break;
        }
        case QUARTER: {
            date_v2_value_.microsecond_ = 0;
            date_v2_value_.second_ = 0;
            date_v2_value_.minute_ = 0;
            date_v2_value_.hour_ = 0;
            date_v2_value_.day_ = 1;
            if (date_v2_value_.month_ <= 3) {
                date_v2_value_.month_ = 1;
            } else if (date_v2_value_.month_ <= 6) {
                date_v2_value_.month_ = 4;
            } else if (date_v2_value_.month_ <= 9) {
                date_v2_value_.month_ = 7;
            } else {
                date_v2_value_.month_ = 10;
            }
            break;
        }
        case YEAR: {
            date_v2_value_.microsecond_ = 0;
            date_v2_value_.second_ = 0;
            date_v2_value_.minute_ = 0;
            date_v2_value_.hour_ = 0;
            date_v2_value_.day_ = 1;
            date_v2_value_.month_ = 1;
            break;
        }
        default:
            return false;
        }
    } else { // is_datev2
        if (!is_valid_date()) {
            return false;
        }
        switch (unit) {
        case SECOND:
        case MINUTE:
        case HOUR:
        case DAY:
            break;
        case WEEK: {
            TimeInterval interval(DAY, weekday(), true);
            date_add_interval<DAY>(interval);
            break;
        }
        case MONTH: {
            date_v2_value_.day_ = 1;
            break;
        }
        case QUARTER: {
            date_v2_value_.day_ = 1;
            if (date_v2_value_.month_ <= 3) {
                date_v2_value_.month_ = 1;
            } else if (date_v2_value_.month_ <= 6) {
                date_v2_value_.month_ = 4;
            } else if (date_v2_value_.month_ <= 9) {
                date_v2_value_.month_ = 7;
            } else {
                date_v2_value_.month_ = 10;
            }
            break;
        }
        case YEAR: {
            date_v2_value_.day_ = 1;
            date_v2_value_.month_ = 1;
            break;
        }
        default:
            return false;
        }
    }
    return true;
}

template <typename T>
bool DateV2Value<T>::unix_timestamp(int64_t* timestamp, const std::string& timezone) const {
    cctz::time_zone ctz;
    if (!TimezoneUtils::find_cctz_time_zone(timezone, ctz)) {
        return false;
    }
    return unix_timestamp(timestamp, ctz);
}

template <typename T>
bool DateV2Value<T>::unix_timestamp(int64_t* timestamp, const cctz::time_zone& ctz) const {
    if constexpr (is_datetime) {
        const auto tp =
                cctz::convert(cctz::civil_second(date_v2_value_.year_, date_v2_value_.month_,
                                                 date_v2_value_.day_, date_v2_value_.hour_,
                                                 date_v2_value_.minute_, date_v2_value_.second_),
                              ctz);
        *timestamp = tp.time_since_epoch().count();
        return true;
    } else {
        const auto tp =
                cctz::convert(cctz::civil_second(date_v2_value_.year_, date_v2_value_.month_,
                                                 date_v2_value_.day_, 0, 0, 0),
                              ctz);
        *timestamp = tp.time_since_epoch().count();
        return true;
    }
}

template <typename T>
bool DateV2Value<T>::unix_timestamp(std::pair<int64_t, int64_t>* timestamp,
                                    const std::string& timezone) const {
    cctz::time_zone ctz;
    if (!TimezoneUtils::find_cctz_time_zone(timezone, ctz)) {
        return false;
    }
    return unix_timestamp(timestamp, ctz);
}

template <typename T>
bool DateV2Value<T>::unix_timestamp(std::pair<int64_t, int64_t>* timestamp,
                                    const cctz::time_zone& ctz) const {
    DCHECK(is_datetime) << "Function unix_timestamp with double_t timestamp only support "
                           "datetimev2 value type.";
    if constexpr (is_datetime) {
        const auto tp =
                cctz::convert(cctz::civil_second(date_v2_value_.year_, date_v2_value_.month_,
                                                 date_v2_value_.day_, date_v2_value_.hour_,
                                                 date_v2_value_.minute_, date_v2_value_.second_),
                              ctz);
        timestamp->first = tp.time_since_epoch().count();
        timestamp->second = date_v2_value_.microsecond_;
    } else {
    }
    return true;
}

template <typename T>
bool DateV2Value<T>::from_unixtime(int64_t timestamp, const std::string& timezone) {
    cctz::time_zone ctz;
    if (!TimezoneUtils::find_cctz_time_zone(timezone, ctz)) {
        return false;
    }
    from_unixtime(timestamp, ctz);
    return true;
}

template <typename T>
void DateV2Value<T>::from_unixtime(int64_t timestamp, const cctz::time_zone& ctz) {
    static const cctz::time_point<cctz::sys_seconds> epoch =
            std::chrono::time_point_cast<cctz::sys_seconds>(
                    std::chrono::system_clock::from_time_t(0));
    cctz::time_point<cctz::sys_seconds> t = epoch + cctz::seconds(timestamp);
    const auto tp = cctz::convert(t, ctz);

    // there's no overflow check since it's hot path

    set_time(tp.year(), tp.month(), tp.day(), tp.hour(), tp.minute(), tp.second(), 0);
}

template <typename T>
bool DateV2Value<T>::from_unixtime(std::pair<int64_t, int64_t> timestamp,
                                   const std::string& timezone) {
    cctz::time_zone ctz;
    if (!TimezoneUtils::find_cctz_time_zone(timezone, ctz)) {
        return false;
    }
    from_unixtime(timestamp, ctz);
    return true;
}

template <typename T>
void DateV2Value<T>::from_unixtime(std::pair<int64_t, int64_t> timestamp,
                                   const cctz::time_zone& ctz) {
    static const cctz::time_point<cctz::sys_seconds> epoch =
            std::chrono::time_point_cast<cctz::sys_seconds>(
                    std::chrono::system_clock::from_time_t(0));
    cctz::time_point<cctz::sys_seconds> t = epoch + cctz::seconds(timestamp.first);

    const auto tp = cctz::convert(t, ctz);

    set_time(tp.year(), tp.month(), tp.day(), tp.hour(), tp.minute(), tp.second(),
             timestamp.second);
}

template <typename T>
bool DateV2Value<T>::from_unixtime(int64_t timestamp, int32_t nano_seconds,
                                   const std::string& timezone, const int scale) {
    cctz::time_zone ctz;
    if (!TimezoneUtils::find_cctz_time_zone(timezone, ctz)) {
        return false;
    }
    from_unixtime(timestamp, nano_seconds, ctz, scale);
    return true;
}

template <typename T>
void DateV2Value<T>::from_unixtime(int64_t timestamp, int32_t nano_seconds,
                                   const cctz::time_zone& ctz, int scale) {
    static const cctz::time_point<cctz::sys_seconds> epoch =
            std::chrono::time_point_cast<cctz::sys_seconds>(
                    std::chrono::system_clock::from_time_t(0));
    cctz::time_point<cctz::sys_seconds> t = epoch + cctz::seconds(timestamp);

    const auto tp = cctz::convert(t, ctz);

    if (scale > 6) [[unlikely]] {
        scale = 6;
    }

    set_time(tp.year(), tp.month(), tp.day(), tp.hour(), tp.minute(), tp.second(),
             nano_seconds / int_exp10(9 - scale) * int_exp10(6 - scale));
}

template <typename T>
const char* DateV2Value<T>::month_name() const {
    if (date_v2_value_.month_ < 1 || date_v2_value_.month_ > 12) {
        return nullptr;
    }
    return s_month_name[date_v2_value_.month_];
}

template <typename T>
const char* DateV2Value<T>::day_name() const {
    int day = weekday();
    if (day < 0 || day >= 7) {
        return nullptr;
    }
    return s_day_name[day];
}

template <typename T>
void DateV2Value<T>::set_time(uint16_t year, uint8_t month, uint8_t day, uint8_t hour,
                              uint8_t minute, uint8_t second, uint32_t microsecond) {
    date_v2_value_.year_ = year;
    date_v2_value_.month_ = month;
    date_v2_value_.day_ = day;
    if constexpr (is_datetime) {
        date_v2_value_.hour_ = hour;
        date_v2_value_.minute_ = minute;
        date_v2_value_.second_ = second;
        date_v2_value_.microsecond_ = microsecond;
    }
}

template <typename T>
void DateV2Value<T>::set_time(uint8_t hour, uint8_t minute, uint8_t second, uint32_t microsecond) {
    if constexpr (is_datetime) {
        date_v2_value_.hour_ = hour;
        date_v2_value_.minute_ = minute;
        date_v2_value_.second_ = second;
        date_v2_value_.microsecond_ = microsecond;
    } else {
        LOG(FATAL) << "Invalid operation 'set_time' for date!";
    }
}

template <typename T>
void DateV2Value<T>::set_microsecond(uint32_t microsecond) {
    if constexpr (is_datetime) {
        date_v2_value_.microsecond_ = microsecond;
    } else {
        LOG(FATAL) << "Invalid operation 'set_microsecond' for date!";
    }
}

template <typename T>
bool DateV2Value<T>::to_format_string(const char* format, int len, char* to) const {
    if (is_invalid(year(), month(), day(), hour(), minute(), second(), microsecond())) {
        return false;
    }
    char buf[64];
    char* pos = nullptr;
    char* cursor = buf;
    const char* ptr = format;
    const char* end = format + len;
    char ch = '\0';

    while (ptr < end) {
        if (*ptr != '%' || (ptr + 1) == end) {
            *to++ = *ptr++;
            continue;
        }
        // Skip '%'
        ptr++;
        switch (ch = *ptr++) {
        case 'y':
            // Year, numeric (two digits)
            to = write_two_digits_to_string(this->year() % 100, to);
            cursor += 2;
            pos = cursor;
            break;
        case 'Y':
            // Year, numeric, four digits
            to = write_four_digits_to_string(this->year(), to);
            cursor += 4;
            pos = cursor;
            break;
        case 'd':
            // Day of month (00...31)
            to = write_two_digits_to_string(this->day(), to);
            cursor += 2;
            pos = cursor;
            break;
        case 'H':
            to = write_two_digits_to_string(this->hour(), to);
            cursor += 2;
            pos = cursor;
            break;
        case 'i':
            // Minutes, numeric (00..59)
            to = write_two_digits_to_string(this->minute(), to);
            cursor += 2;
            pos = cursor;
            break;
        case 'm':
            to = write_two_digits_to_string(this->month(), to);
            cursor += 2;
            pos = cursor;
            break;
        case 'h':
        case 'I':
            // Hour (01..12)
            to = write_two_digits_to_string((this->hour() % 24 + 11) % 12 + 1, to);
            cursor += 2;
            pos = cursor;
            break;
        case 's':
        case 'S':
            // Seconds (00..59)
            to = write_two_digits_to_string(this->second(), to);
            cursor += 2;
            pos = cursor;
            break;
        case 'a':
            // Abbreviated weekday name
            if (this->year() == 0 && this->month() == 0) {
                return false;
            }
            to = append_string(s_ab_day_name[weekday()], to);
            break;
        case 'b':
            // Abbreviated month name
            if (this->month() == 0) {
                return false;
            }
            to = append_string(s_ab_month_name[this->month()], to);
            break;
        case 'c':
            // Month, numeric (0...12)
            pos = int_to_str(this->month(), cursor);
            to = append_with_prefix(cursor, pos - cursor, '0', 1, to);
            break;
        case 'D':
            // Day of the month with English suffix (0th, 1st, ...)
            pos = int_to_str(this->day(), cursor);
            to = append_with_prefix(cursor, pos - cursor, '0', 1, to);
            if (this->day() >= 10 && this->day() <= 19) {
                to = append_string("th", to);
            } else {
                switch (this->day() % 10) {
                case 1:
                    to = append_string("st", to);
                    break;
                case 2:
                    to = append_string("nd", to);
                    break;
                case 3:
                    to = append_string("rd", to);
                    break;
                default:
                    to = append_string("th", to);
                    break;
                }
            }
            break;
        case 'e':
            // Day of the month, numeric (0..31)
            pos = int_to_str(this->day(), cursor);
            to = append_with_prefix(cursor, pos - cursor, '0', 1, to);
            break;
        case 'f':
            // Microseconds (000000..999999)
            pos = int_to_str(this->microsecond(), cursor);
            to = append_with_prefix(cursor, pos - cursor, '0', 6, to);
            break;
        case 'j':
            // Day of year (001..366)
            pos = int_to_str(daynr() - doris::calc_daynr(this->year(), 1, 1) + 1, cursor);
            to = append_with_prefix(cursor, pos - cursor, '0', 3, to);
            break;
        case 'k':
            // Hour (0..23)
            pos = int_to_str(this->hour(), cursor);
            to = append_with_prefix(cursor, pos - cursor, '0', 1, to);
            break;
        case 'l':
            // Hour (1..12)
            pos = int_to_str((this->hour() % 24 + 11) % 12 + 1, cursor);
            to = append_with_prefix(cursor, pos - cursor, '0', 1, to);
            break;
        case 'M':
            // Month name (January..December)
            if (this->month() == 0) {
                return false;
            }
            to = append_string(s_month_name[this->month()], to);
            break;
        case 'p':
            // AM or PM
            if ((this->hour() % 24) >= 12) {
                to = append_string("PM", to);
            } else {
                to = append_string("AM", to);
            }
            break;
        case 'r': {
            // Time, 12-hour (hh:mm:ss followed by AM or PM)
            *to++ = (char)('0' + (((this->hour() + 11) % 12 + 1) / 10));
            *to++ = (char)('0' + (((this->hour() + 11) % 12 + 1) % 10));
            *to++ = ':';
            // Minute
            *to++ = (char)('0' + (this->minute() / 10));
            *to++ = (char)('0' + (this->minute() % 10));
            *to++ = ':';
            /* Second */
            *to++ = (char)('0' + (this->second() / 10));
            *to++ = (char)('0' + (this->second() % 10));
            if ((this->hour() % 24) >= 12) {
                to = append_string(" PM", to);
            } else {
                to = append_string(" AM", to);
            }
            break;
        }
        case 'T': {
            // Time, 24-hour (hh:mm:ss)
            *to++ = (char)('0' + ((this->hour() % 24) / 10));
            *to++ = (char)('0' + ((this->hour() % 24) % 10));
            *to++ = ':';
            // Minute
            *to++ = (char)('0' + (this->minute() / 10));
            *to++ = (char)('0' + (this->minute() % 10));
            *to++ = ':';
            /* Second */
            *to++ = (char)('0' + (this->second() / 10));
            *to++ = (char)('0' + (this->second() % 10));
            break;
        }
        case 'u':
            // Week (00..53), where Monday is the first day of the week;
            // WEEK() mode 1
            to = write_two_digits_to_string(week(mysql_week_mode(1)), to);
            cursor += 2;
            pos = cursor;
            break;
        case 'U':
            // Week (00..53), where Sunday is the first day of the week;
            // WEEK() mode 0
            to = write_two_digits_to_string(week(mysql_week_mode(0)), to);
            cursor += 2;
            pos = cursor;
            break;
        case 'v':
            // Week (01..53), where Monday is the first day of the week;
            // WEEK() mode 3; used with %x
            to = write_two_digits_to_string(week(mysql_week_mode(3)), to);
            cursor += 2;
            pos = cursor;
            break;
        case 'V':
            // Week (01..53), where Sunday is the first day of the week;
            // WEEK() mode 2; used with %X
            to = write_two_digits_to_string(week(mysql_week_mode(2)), to);
            cursor += 2;
            pos = cursor;
            break;
        case 'w':
            // Day of the week (0=Sunday..6=Saturday)
            if (this->month() == 0 && this->year() == 0) {
                return false;
            }
            pos = int_to_str(doris::calc_weekday(daynr(), true), cursor);
            to = append_with_prefix(cursor, pos - cursor, '0', 1, to);
            break;
        case 'W':
            // Weekday name (Sunday..Saturday)
            to = append_string(s_day_name[weekday()], to);
            break;
        case 'x': {
            // Year for the week, where Monday is the first day of the week,
            // numeric, four digits; used with %v
            uint16_t year = 0;
            calc_week(this->daynr(), this->year(), this->month(), this->day(), mysql_week_mode(3),
                      &year, true);
            to = write_four_digits_to_string(year, to);
            cursor += 4;
            pos = cursor;
            break;
        }
        case 'X': {
            // Year for the week where Sunday is the first day of the week,
            // numeric, four digits; used with %V
            uint16_t year = 0;
            calc_week(this->daynr(), this->year(), this->month(), this->day(), mysql_week_mode(2),
                      &year);
            to = write_four_digits_to_string(year, to);
            cursor += 4;
            pos = cursor;
            break;
        }
        default:
            *to++ = ch;
            break;
        }
    }
    *to++ = '\0';
    return true;
}

template <typename T>
bool DateV2Value<T>::from_date(uint32_t value) {
    DCHECK(!is_datetime);
    if (value < MIN_DATE_V2 || value > MAX_DATE_V2) {
        return false;
    }

    return set_date_uint32(value);
}

template <typename T>
bool DateV2Value<T>::from_datetime(uint64_t value) {
    DCHECK(is_datetime);
    if (value < MIN_DATETIME_V2 || value > MAX_DATETIME_V2) {
        return false;
    }

    return set_datetime_uint64(value);
}

template <typename T>
int64_t DateV2Value<T>::standardize_timevalue(int64_t value) {
    if (value <= 0) {
        return 0;
    }
    if (value >= 10000101000000L) {
        // 9999-99-99 99:99:99
        if (value > 99999999999999L) {
            return 0;
        }

        // between 1000-01-01 00:00:00L and 9999-99-99 99:99:99
        // all digits exist.
        return value;
    }
    // 2000-01-01
    if (value < 101) {
        return 0;
    }
    // two digits  year. 2000 ~ 2069
    if (value <= (YY_PART_YEAR - 1) * 10000L + 1231L) {
        return (value + 20000000L) * 1000000L;
    }
    // two digits year, invalid date
    if (value < YY_PART_YEAR * 10000L + 101) {
        return 0;
    }
    // two digits year. 1970 ~ 1999
    if (value <= 991231L) {
        return (value + 19000000L) * 1000000L;
    }
    if (value < 10000101) {
        return 0;
    }
    // four digits years without hour.
    if (value <= 99991231L) {
        return value * 1000000L;
    }
    // below 0000-01-01
    if (value < 101000000) {
        return 0;
    }

    // below is with datetime, must have hh:mm:ss
    // 2000 ~ 2069
    if (value <= (YY_PART_YEAR - 1) * 10000000000L + 1231235959L) {
        return value + 20000000000000L;
    }
    if (value < YY_PART_YEAR * 10000000000L + 101000000L) {
        return 0;
    }
    // 1970 ~ 1999
    if (value <= 991231235959L) {
        return value + 19000000000000L;
    }
    return value;
}

template <typename T>
bool DateV2Value<T>::from_date_int64(int64_t value) {
    value = standardize_timevalue(value);
    if (value <= 0) {
        return false;
    }
    uint64_t date = value / 1000000;

    auto [year, month, day, hour, minute, second] = std::tuple {0, 0, 0, 0, 0, 0};
    year = date / 10000;
    date %= 10000;
    month = date / 100;
    day = date % 100;

    if constexpr (is_datetime) {
        uint64_t time = value % 1000000;
        hour = time / 10000;
        time %= 10000;
        minute = time / 100;
        second = time % 100;
        return check_range_and_set_time(year, month, day, hour, minute, second, 0);
    } else {
        return check_range_and_set_time(year, month, day, 0, 0, 0, 0);
    }
}

template <typename T>
uint8_t DateV2Value<T>::calc_week(const uint32_t& day_nr, const uint16_t& year,
                                  const uint8_t& month, const uint8_t& day, uint8_t mode,
                                  uint16_t* to_year, bool disable_lut) {
    if (config::enable_time_lut && !disable_lut && mode == 3 && year >= 1950 && year < 2030) {
        return doris::TimeLUT::GetImplement()
                ->week_of_year_table[year - doris::LUT_START_YEAR][month - 1][day - 1];
    }
    // mode=4 is used for week()
    if (config::enable_time_lut && !disable_lut && mode == 4 && year >= 1950 && year < 2030) {
        return doris::TimeLUT::GetImplement()
                ->week_table[year - doris::LUT_START_YEAR][month - 1][day - 1];
    }
    bool monday_first = mode & WEEK_MONDAY_FIRST;
    bool week_year = mode & WEEK_YEAR;
    bool first_weekday = mode & WEEK_FIRST_WEEKDAY;
    uint64_t daynr_first_day = doris::calc_daynr(year, 1, 1);
    uint8_t weekday_first_day = doris::calc_weekday(daynr_first_day, !monday_first);

    int days = 0;
    *to_year = year;

    // Check weather the first days of this year belongs to last year
    if (month == 1 && day <= (7 - weekday_first_day)) {
        if (!week_year && ((first_weekday && weekday_first_day != 0) ||
                           (!first_weekday && weekday_first_day > 3))) {
            return 0;
        }
        (*to_year)--;
        week_year = true;
        daynr_first_day -= (days = doris::calc_days_in_year(*to_year));
        weekday_first_day = (weekday_first_day + 53 * 7 - days) % 7;
    }

    // How many days since first week
    if ((first_weekday && weekday_first_day != 0) || (!first_weekday && weekday_first_day > 3)) {
        // days in new year belongs to last year.
        days = day_nr - (daynr_first_day + (7 - weekday_first_day));
    } else {
        // days in new year belongs to this year.
        days = day_nr - (daynr_first_day - weekday_first_day);
    }

    if (week_year && days >= 52 * 7) {
        weekday_first_day = (weekday_first_day + doris::calc_days_in_year(*to_year)) % 7;
        if ((first_weekday && weekday_first_day == 0) ||
            (!first_weekday && weekday_first_day <= 3)) {
            // Belong to next year.
            (*to_year)++;
            return 1;
        }
    }

    return days / 7 + 1;
}

template <typename T>
std::ostream& operator<<(std::ostream& os, const DateV2Value<T>& value) {
    char buf[30];
    value.to_string(buf);
    return os << buf;
}

// NOTE:
//  only support DATE - DATE (no support DATETIME - DATETIME)
template <typename T0, typename T1>
std::size_t operator-(const DateV2Value<T0>& v1, const DateV2Value<T1>& v2) {
    return v1.daynr() - v2.daynr();
}

template <typename T>
std::size_t hash_value(DateV2Value<T> const& value) {
    return HashUtil::hash(&value, sizeof(DateV2Value<T>), 0);
}

template class DateV2Value<DateV2ValueType>;
template class DateV2Value<DateTimeV2ValueType>;

template std::size_t hash_value<DateV2ValueType>(DateV2Value<DateV2ValueType> const& value);
template std::size_t hash_value<DateTimeV2ValueType>(DateV2Value<DateTimeV2ValueType> const& value);

template std::ostream& operator<<(std::ostream& os, const DateV2Value<DateV2ValueType>& value);
template std::ostream& operator<<(std::ostream& os, const DateV2Value<DateTimeV2ValueType>& value);

template std::size_t operator-(const VecDateTimeValue& v1, const DateV2Value<DateV2ValueType>& v2);
template std::size_t operator-(const VecDateTimeValue& v1,
                               const DateV2Value<DateTimeV2ValueType>& v2);

template std::size_t operator-(const DateV2Value<DateV2ValueType>& v1, const VecDateTimeValue& v2);
template std::size_t operator-(const DateV2Value<DateTimeV2ValueType>& v1,
                               const VecDateTimeValue& v2);

template std::size_t operator-(const DateV2Value<DateV2ValueType>& v1,
                               const DateV2Value<DateV2ValueType>& v2);
template std::size_t operator-(const DateV2Value<DateV2ValueType>& v1,
                               const DateV2Value<DateTimeV2ValueType>& v2);
template std::size_t operator-(const DateV2Value<DateTimeV2ValueType>& v1,
                               const DateV2Value<DateV2ValueType>& v2);
template std::size_t operator-(const DateV2Value<DateTimeV2ValueType>& v1,
                               const DateV2Value<DateTimeV2ValueType>& v2);

template void VecDateTimeValue::create_from_date_v2<DateV2ValueType>(
        DateV2Value<DateV2ValueType>& value, TimeType type);
template void VecDateTimeValue::create_from_date_v2<DateV2ValueType>(
        DateV2Value<DateV2ValueType>&& value, TimeType type);
template void VecDateTimeValue::create_from_date_v2<DateTimeV2ValueType>(
        DateV2Value<DateTimeV2ValueType>& value, TimeType type);
template void VecDateTimeValue::create_from_date_v2<DateTimeV2ValueType>(
        DateV2Value<DateTimeV2ValueType>&& value, TimeType type);

template int64_t VecDateTimeValue::second_diff<DateV2Value<DateV2ValueType>>(
        const DateV2Value<DateV2ValueType>& rhs) const;
template int64_t VecDateTimeValue::second_diff<DateV2Value<DateTimeV2ValueType>>(
        const DateV2Value<DateTimeV2ValueType>& rhs) const;

#define DELARE_DATE_ADD_INTERVAL(DateValueType1, DateValueType2)                                   \
    template bool                                                                                  \
    DateV2Value<DateValueType1>::date_add_interval<TimeUnit::MICROSECOND, DateValueType2>(         \
            TimeInterval const&, DateV2Value<DateValueType2>&);                                    \
    template bool                                                                                  \
    DateV2Value<DateValueType1>::date_add_interval<TimeUnit::MILLISECOND, DateValueType2>(         \
            TimeInterval const&, DateV2Value<DateValueType2>&);                                    \
    template bool                                                                                  \
    DateV2Value<DateValueType1>::date_add_interval<TimeUnit::SECOND, DateValueType2>(              \
            TimeInterval const&, DateV2Value<DateValueType2>&);                                    \
    template bool                                                                                  \
    DateV2Value<DateValueType1>::date_add_interval<TimeUnit::MINUTE, DateValueType2>(              \
            TimeInterval const&, DateV2Value<DateValueType2>&);                                    \
    template bool DateV2Value<DateValueType1>::date_add_interval<TimeUnit::HOUR, DateValueType2>(  \
            TimeInterval const&, DateV2Value<DateValueType2>&);                                    \
    template bool DateV2Value<DateValueType1>::date_add_interval<TimeUnit::DAY, DateValueType2>(   \
            TimeInterval const&, DateV2Value<DateValueType2>&);                                    \
    template bool DateV2Value<DateValueType1>::date_add_interval<TimeUnit::MONTH, DateValueType2>( \
            TimeInterval const&, DateV2Value<DateValueType2>&);                                    \
    template bool DateV2Value<DateValueType1>::date_add_interval<TimeUnit::YEAR, DateValueType2>(  \
            TimeInterval const&, DateV2Value<DateValueType2>&);                                    \
    template bool                                                                                  \
    DateV2Value<DateValueType1>::date_add_interval<TimeUnit::QUARTER, DateValueType2>(             \
            TimeInterval const&, DateV2Value<DateValueType2>&);                                    \
    template bool DateV2Value<DateValueType1>::date_add_interval<TimeUnit::WEEK, DateValueType2>(  \
            TimeInterval const&, DateV2Value<DateValueType2>&);

DELARE_DATE_ADD_INTERVAL(DateV2ValueType, DateV2ValueType)
DELARE_DATE_ADD_INTERVAL(DateV2ValueType, DateTimeV2ValueType)
DELARE_DATE_ADD_INTERVAL(DateTimeV2ValueType, DateV2ValueType)
DELARE_DATE_ADD_INTERVAL(DateTimeV2ValueType, DateTimeV2ValueType)

template bool VecDateTimeValue::date_add_interval<TimeUnit::SECOND>(const TimeInterval& interval);
template bool VecDateTimeValue::date_add_interval<TimeUnit::MINUTE>(const TimeInterval& interval);
template bool VecDateTimeValue::date_add_interval<TimeUnit::HOUR>(const TimeInterval& interval);
template bool VecDateTimeValue::date_add_interval<TimeUnit::DAY>(const TimeInterval& interval);
template bool VecDateTimeValue::date_add_interval<TimeUnit::MONTH>(const TimeInterval& interval);
template bool VecDateTimeValue::date_add_interval<TimeUnit::YEAR>(const TimeInterval& interval);
template bool VecDateTimeValue::date_add_interval<TimeUnit::QUARTER>(const TimeInterval& interval);
template bool VecDateTimeValue::date_add_interval<TimeUnit::WEEK>(const TimeInterval& interval);

template bool VecDateTimeValue::date_add_interval<TimeUnit::SECOND, false>(
        const TimeInterval& interval);
template bool VecDateTimeValue::date_add_interval<TimeUnit::MINUTE, false>(
        const TimeInterval& interval);
template bool VecDateTimeValue::date_add_interval<TimeUnit::HOUR, false>(
        const TimeInterval& interval);
template bool VecDateTimeValue::date_add_interval<TimeUnit::DAY, false>(
        const TimeInterval& interval);
template bool VecDateTimeValue::date_add_interval<TimeUnit::MONTH, false>(
        const TimeInterval& interval);
template bool VecDateTimeValue::date_add_interval<TimeUnit::YEAR, false>(
        const TimeInterval& interval);
template bool VecDateTimeValue::date_add_interval<TimeUnit::QUARTER, false>(
        const TimeInterval& interval);
template bool VecDateTimeValue::date_add_interval<TimeUnit::WEEK, false>(
        const TimeInterval& interval);

template bool DateV2Value<DateV2ValueType>::date_add_interval<TimeUnit::MICROSECOND>(
        const TimeInterval& interval);
template bool DateV2Value<DateV2ValueType>::date_add_interval<TimeUnit::MILLISECOND>(
        const TimeInterval& interval);
template bool DateV2Value<DateV2ValueType>::date_add_interval<TimeUnit::SECOND>(
        const TimeInterval& interval);
template bool DateV2Value<DateV2ValueType>::date_add_interval<TimeUnit::MINUTE>(
        const TimeInterval& interval);
template bool DateV2Value<DateV2ValueType>::date_add_interval<TimeUnit::HOUR>(
        const TimeInterval& interval);
template bool DateV2Value<DateV2ValueType>::date_add_interval<TimeUnit::DAY>(
        const TimeInterval& interval);
template bool DateV2Value<DateV2ValueType>::date_add_interval<TimeUnit::MONTH>(
        const TimeInterval& interval);
template bool DateV2Value<DateV2ValueType>::date_add_interval<TimeUnit::YEAR>(
        const TimeInterval& interval);
template bool DateV2Value<DateV2ValueType>::date_add_interval<TimeUnit::QUARTER>(
        const TimeInterval& interval);
template bool DateV2Value<DateV2ValueType>::date_add_interval<TimeUnit::WEEK>(
        const TimeInterval& interval);

template bool DateV2Value<DateTimeV2ValueType>::date_add_interval<TimeUnit::MICROSECOND>(
        const TimeInterval& interval);
template bool DateV2Value<DateTimeV2ValueType>::date_add_interval<TimeUnit::MILLISECOND>(
        const TimeInterval& interval);
template bool DateV2Value<DateTimeV2ValueType>::date_add_interval<TimeUnit::SECOND>(
        const TimeInterval& interval);
template bool DateV2Value<DateTimeV2ValueType>::date_add_interval<TimeUnit::MINUTE>(
        const TimeInterval& interval);
template bool DateV2Value<DateTimeV2ValueType>::date_add_interval<TimeUnit::HOUR>(
        const TimeInterval& interval);
template bool DateV2Value<DateTimeV2ValueType>::date_add_interval<TimeUnit::DAY>(
        const TimeInterval& interval);
template bool DateV2Value<DateTimeV2ValueType>::date_add_interval<TimeUnit::MONTH>(
        const TimeInterval& interval);
template bool DateV2Value<DateTimeV2ValueType>::date_add_interval<TimeUnit::YEAR>(
        const TimeInterval& interval);
template bool DateV2Value<DateTimeV2ValueType>::date_add_interval<TimeUnit::QUARTER>(
        const TimeInterval& interval);
template bool DateV2Value<DateTimeV2ValueType>::date_add_interval<TimeUnit::WEEK>(
        const TimeInterval& interval);

template bool DateV2Value<DateV2ValueType>::date_add_interval<TimeUnit::MICROSECOND, false>(
        const TimeInterval& interval);
template bool DateV2Value<DateV2ValueType>::date_add_interval<TimeUnit::SECOND, false>(
        const TimeInterval& interval);
template bool DateV2Value<DateV2ValueType>::date_add_interval<TimeUnit::MINUTE, false>(
        const TimeInterval& interval);
template bool DateV2Value<DateV2ValueType>::date_add_interval<TimeUnit::HOUR, false>(
        const TimeInterval& interval);
template bool DateV2Value<DateV2ValueType>::date_add_interval<TimeUnit::DAY, false>(
        const TimeInterval& interval);
template bool DateV2Value<DateV2ValueType>::date_add_interval<TimeUnit::MONTH, false>(
        const TimeInterval& interval);
template bool DateV2Value<DateV2ValueType>::date_add_interval<TimeUnit::YEAR, false>(
        const TimeInterval& interval);
template bool DateV2Value<DateV2ValueType>::date_add_interval<TimeUnit::QUARTER, false>(
        const TimeInterval& interval);
template bool DateV2Value<DateV2ValueType>::date_add_interval<TimeUnit::WEEK, false>(
        const TimeInterval& interval);

template bool DateV2Value<DateTimeV2ValueType>::date_add_interval<TimeUnit::MICROSECOND, false>(
        const TimeInterval& interval);
template bool DateV2Value<DateTimeV2ValueType>::date_add_interval<TimeUnit::SECOND, false>(
        const TimeInterval& interval);
template bool DateV2Value<DateTimeV2ValueType>::date_add_interval<TimeUnit::MINUTE, false>(
        const TimeInterval& interval);
template bool DateV2Value<DateTimeV2ValueType>::date_add_interval<TimeUnit::HOUR, false>(
        const TimeInterval& interval);
template bool DateV2Value<DateTimeV2ValueType>::date_add_interval<TimeUnit::DAY, false>(
        const TimeInterval& interval);
template bool DateV2Value<DateTimeV2ValueType>::date_add_interval<TimeUnit::MONTH, false>(
        const TimeInterval& interval);
template bool DateV2Value<DateTimeV2ValueType>::date_add_interval<TimeUnit::YEAR, false>(
        const TimeInterval& interval);
template bool DateV2Value<DateTimeV2ValueType>::date_add_interval<TimeUnit::QUARTER, false>(
        const TimeInterval& interval);
template bool DateV2Value<DateTimeV2ValueType>::date_add_interval<TimeUnit::WEEK, false>(
        const TimeInterval& interval);

template bool VecDateTimeValue::date_set_interval<TimeUnit::SECOND>(const TimeInterval& interval);
template bool VecDateTimeValue::date_set_interval<TimeUnit::MINUTE>(const TimeInterval& interval);
template bool VecDateTimeValue::date_set_interval<TimeUnit::HOUR>(const TimeInterval& interval);
template bool VecDateTimeValue::date_set_interval<TimeUnit::DAY>(const TimeInterval& interval);
template bool VecDateTimeValue::date_set_interval<TimeUnit::MONTH>(const TimeInterval& interval);
template bool VecDateTimeValue::date_set_interval<TimeUnit::YEAR>(const TimeInterval& interval);

template bool DateV2Value<DateV2ValueType>::date_set_interval<TimeUnit::SECOND>(
        const TimeInterval& interval);
template bool DateV2Value<DateV2ValueType>::date_set_interval<TimeUnit::MINUTE>(
        const TimeInterval& interval);
template bool DateV2Value<DateV2ValueType>::date_set_interval<TimeUnit::HOUR>(
        const TimeInterval& interval);
template bool DateV2Value<DateV2ValueType>::date_set_interval<TimeUnit::DAY>(
        const TimeInterval& interval);
template bool DateV2Value<DateV2ValueType>::date_set_interval<TimeUnit::MONTH>(
        const TimeInterval& interval);
template bool DateV2Value<DateV2ValueType>::date_set_interval<TimeUnit::YEAR>(
        const TimeInterval& interval);

template bool DateV2Value<DateTimeV2ValueType>::date_set_interval<TimeUnit::SECOND>(
        const TimeInterval& interval);
template bool DateV2Value<DateTimeV2ValueType>::date_set_interval<TimeUnit::MINUTE>(
        const TimeInterval& interval);
template bool DateV2Value<DateTimeV2ValueType>::date_set_interval<TimeUnit::HOUR>(
        const TimeInterval& interval);
template bool DateV2Value<DateTimeV2ValueType>::date_set_interval<TimeUnit::DAY>(
        const TimeInterval& interval);
template bool DateV2Value<DateTimeV2ValueType>::date_set_interval<TimeUnit::MONTH>(
        const TimeInterval& interval);
template bool DateV2Value<DateTimeV2ValueType>::date_set_interval<TimeUnit::YEAR>(
        const TimeInterval& interval);

template bool VecDateTimeValue::datetime_trunc<TimeUnit::SECOND>();
template bool VecDateTimeValue::datetime_trunc<TimeUnit::MINUTE>();
template bool VecDateTimeValue::datetime_trunc<TimeUnit::HOUR>();
template bool VecDateTimeValue::datetime_trunc<TimeUnit::DAY>();
template bool VecDateTimeValue::datetime_trunc<TimeUnit::MONTH>();
template bool VecDateTimeValue::datetime_trunc<TimeUnit::YEAR>();
template bool VecDateTimeValue::datetime_trunc<TimeUnit::QUARTER>();
template bool VecDateTimeValue::datetime_trunc<TimeUnit::WEEK>();

template bool DateV2Value<DateV2ValueType>::datetime_trunc<TimeUnit::SECOND>();
template bool DateV2Value<DateV2ValueType>::datetime_trunc<TimeUnit::MINUTE>();
template bool DateV2Value<DateV2ValueType>::datetime_trunc<TimeUnit::HOUR>();
template bool DateV2Value<DateV2ValueType>::datetime_trunc<TimeUnit::DAY>();
template bool DateV2Value<DateV2ValueType>::datetime_trunc<TimeUnit::MONTH>();
template bool DateV2Value<DateV2ValueType>::datetime_trunc<TimeUnit::YEAR>();
template bool DateV2Value<DateV2ValueType>::datetime_trunc<TimeUnit::QUARTER>();
template bool DateV2Value<DateV2ValueType>::datetime_trunc<TimeUnit::WEEK>();

template bool DateV2Value<DateTimeV2ValueType>::datetime_trunc<TimeUnit::MICROSECOND>();
template bool DateV2Value<DateTimeV2ValueType>::datetime_trunc<TimeUnit::SECOND>();
template bool DateV2Value<DateTimeV2ValueType>::datetime_trunc<TimeUnit::MINUTE>();
template bool DateV2Value<DateTimeV2ValueType>::datetime_trunc<TimeUnit::HOUR>();
template bool DateV2Value<DateTimeV2ValueType>::datetime_trunc<TimeUnit::DAY>();
template bool DateV2Value<DateTimeV2ValueType>::datetime_trunc<TimeUnit::MONTH>();
template bool DateV2Value<DateTimeV2ValueType>::datetime_trunc<TimeUnit::YEAR>();
template bool DateV2Value<DateTimeV2ValueType>::datetime_trunc<TimeUnit::QUARTER>();
template bool DateV2Value<DateTimeV2ValueType>::datetime_trunc<TimeUnit::WEEK>();

} // namespace doris
