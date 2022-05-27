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

#include "runtime/datetime_value.h"

#include <ctype.h>
#include <string.h>
#include <time.h>

#include <limits>
#include <sstream>

#include "common/logging.h"
#include "util/timezone_utils.h"

namespace doris {
const uint64_t log_10_int[] = {1,           10,           100,           1000,
                               10000UL,     100000UL,     1000000UL,     10000000UL,
                               100000000UL, 1000000000UL, 10000000000UL, 100000000000UL};

static int s_days_in_month[13] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};

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

static bool is_leap(uint32_t year) {
    return ((year % 4) == 0) && ((year % 100 != 0) || ((year % 400) == 0 && year));
}

static uint32_t calc_days_in_year(uint32_t year) {
    return is_leap(year) ? 366 : 365;
}

RE2 DateTimeValue::time_zone_offset_format_reg("^[+-]{1}\\d{2}\\:\\d{2}$");

bool DateTimeValue::check_range(uint32_t year, uint32_t month, uint32_t day, uint32_t hour,
                                uint32_t minute, uint32_t second, uint32_t microsecond,
                                uint16_t type) {
    bool time = hour > (type == TIME_TIME ? TIME_MAX_HOUR : 23) || minute > 59 || second > 59 ||
                microsecond > 999999;
    return time || check_date(year, month, day);
}

bool DateTimeValue::check_date(uint32_t year, uint32_t month, uint32_t day) {
    if (month != 0 && month <= 12 && day > s_days_in_month[month]) {
        // Feb 29 in leap year is valid.
        if (!(month == 2 && day == 29 && is_leap(year))) return true;
    }
    return year > 9999 || month > 12 || day > 31;
}

// The interval format is that with no delimiters
// YYYY-MM-DD HH-MM-DD.FFFFFF AM in default format
// 0    1  2  3  4  5  6      7
bool DateTimeValue::from_date_str(const char* date_str, int len) {
    const char* ptr = date_str;
    const char* end = date_str + len;
    // ONLY 2, 6 can follow by a sapce
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

    if (date_val[6] && date_len[6] < 6) {
        date_val[6] *= log_10_int[6 - date_len[6]];
    }
    if (year_len == 2) {
        if (date_val[0] < YY_PART_YEAR) {
            date_val[0] += 2000;
        } else {
            date_val[0] += 1900;
        }
    }

    if (num_field < 3) return false;
    return check_range_and_set_time(date_val[0], date_val[1], date_val[2], date_val[3], date_val[4],
                                    date_val[5], date_val[6], _type);
}

// [0, 101) invalid
// [101, (YY_PART_YEAR - 1) * 10000 + 1231] for two digits year 2000 ~ 2069
// [(YY_PART_YEAR - 1) * 10000 + 1231, YY_PART_YEAR * 10000L + 101) invalid
// [YY_PART_YEAR * 10000L + 101, 991231] for two digits year 1970 ~1999
// (991231, 10000101) invalid, because support 1000-01-01
// [10000101, 99991231] for four digits year date value.
// (99991231, 101000000) invalid, NOTE below this is datetime vaule hh:mm:ss must exist.
// [101000000, (YY_PART_YEAR - 1)##1231235959] two digits year datetime value
// ((YY_PART_YEAR - 1)##1231235959, YY_PART_YEAR##0101000000) invalid
// ((YY_PART_YEAR)##1231235959, 99991231235959] two digits year datetime value 1970 ~ 1999
// (999991231235959, ~) valid
int64_t DateTimeValue::standardize_timevalue(int64_t value) {
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

bool DateTimeValue::from_date_int64(int64_t value) {
    _neg = false;
    value = standardize_timevalue(value);
    if (value <= 0) {
        return false;
    }
    uint64_t date = value / 1000000;
    uint64_t time = value % 1000000;

    auto [year, month, day, hour, minute, second, microsecond] = std::tuple {0, 0, 0, 0, 0, 0, 0};
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

void DateTimeValue::set_zero(int type) {
    memset(this, 0, sizeof(*this));
    _type = type;
}

void DateTimeValue::set_type(int type) {
    _type = type;
}

void DateTimeValue::set_max_time(bool neg) {
    set_zero(TIME_TIME);
    DCHECK(TIME_MAX_HOUR >= std::numeric_limits<uint8_t>::min() &&
           TIME_MAX_HOUR <= std::numeric_limits<uint8_t>::max())
            << "TIME_MAX_HOUR overflow:" << TIME_MAX_HOUR;
    _hour = static_cast<uint8_t>(TIME_MAX_HOUR);
    _minute = TIME_MAX_MINUTE;
    _second = TIME_MAX_SECOND;
    _neg = neg;
}

bool DateTimeValue::from_time_int64(int64_t value) {
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

char* DateTimeValue::append_date_buffer(char* to) const {
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

char* DateTimeValue::append_time_buffer(char* to) const {
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
    if (_microsecond > 0) {
        *to++ = '.';
        uint32_t first = _microsecond / 10000;
        uint32_t second = (_microsecond % 10000) / 100;
        uint32_t third = _microsecond % 100;
        *to++ = (char)('0' + first / 10);
        *to++ = (char)('0' + first % 10);
        *to++ = (char)('0' + second / 10);
        *to++ = (char)('0' + second % 10);
        *to++ = (char)('0' + third / 10);
        *to++ = (char)('0' + third % 10);
    }
    return to;
}

char* DateTimeValue::to_datetime_buffer(char* to) const {
    to = append_date_buffer(to);
    *to++ = ' ';
    return append_time_buffer(to);
}

char* DateTimeValue::to_date_buffer(char* to) const {
    return append_date_buffer(to);
}

char* DateTimeValue::to_time_buffer(char* to) const {
    return append_time_buffer(to);
}

int32_t DateTimeValue::to_buffer(char* buffer) const {
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

char* DateTimeValue::to_string(char* to) const {
    int len = to_buffer(to);
    *(to + len) = '\0';
    return to + len + 1;
}

int64_t DateTimeValue::to_datetime_int64() const {
    return (_year * 10000L + _month * 100 + _day) * 1000000L + _hour * 10000 + _minute * 100 +
           _second;
}

int64_t DateTimeValue::to_date_int64() const {
    return _year * 10000 + _month * 100 + _day;
}

int64_t DateTimeValue::to_time_int64() const {
    int sign = _neg == 0 ? 1 : -1;
    return sign * (_hour * 10000 + _minute * 100 + _second);
}

int64_t DateTimeValue::to_int64() const {
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

bool DateTimeValue::get_date_from_daynr(uint64_t daynr) {
    if (daynr <= 0 || daynr > DATE_MAX_DAYNR) {
        return false;
    }

    auto [year, month, day] = std::tuple {0, 0, 0};
    year = daynr / 365;
    uint32_t days_befor_year = 0;
    while (daynr < (days_befor_year = calc_daynr(year, 1, 1))) {
        year--;
    }
    uint32_t days_of_year = daynr - days_befor_year + 1;
    int leap_day = 0;
    if (is_leap(year)) {
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

    if (check_range(year, month, day, 0, 0, 0, 0, _type)) {
        return false;
    }
    set_time(year, month, day, _hour, _minute, _second, _microsecond);
    return true;
}

bool DateTimeValue::from_date_daynr(uint64_t daynr) {
    _neg = false;
    if (!get_date_from_daynr(daynr)) {
        return false;
    }
    _hour = 0;
    _minute = 0;
    _second = 0;
    _microsecond = 0;
    _type = TIME_DATE;
    return true;
}

// Following code is stolen from MySQL.
uint64_t DateTimeValue::calc_daynr(uint32_t year, uint32_t month, uint32_t day) {
    uint64_t delsum = 0;
    int y = year;

    if (year == 0 && month == 0) {
        return 0;
    }

    /* Cast to int to be able to handle month == 0 */
    delsum = 365 * y + 31 * (month - 1) + day;
    if (month <= 2) {
        // No leap year
        y--;
    } else {
        // This is great!!!
        // 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
        // 0, 0, 3, 3, 4, 4, 5, 5, 5,  6,  7,  8
        delsum -= (month * 4 + 23) / 10;
    }
    // Every 400 year has 97 leap year, 100, 200, 300 are not leap year.
    return delsum + y / 4 - y / 100 + y / 400;
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

int DateTimeValue::compute_format_len(const char* format, int len) {
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

bool DateTimeValue::to_format_string(const char* format, int len, char* to) const {
    char buf[64];
    char* pos = nullptr;
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
            pos = int_to_str(_month, buf);
            to = append_with_prefix(buf, pos - buf, '0', 1, to);
            break;
        case 'd':
            // Day of month (00...31)
            pos = int_to_str(_day, buf);
            to = append_with_prefix(buf, pos - buf, '0', 2, to);
            break;
        case 'D':
            // Day of the month with English suffix (0th, 1st, ...)
            pos = int_to_str(_day, buf);
            to = append_with_prefix(buf, pos - buf, '0', 1, to);
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
            pos = int_to_str(_day, buf);
            to = append_with_prefix(buf, pos - buf, '0', 1, to);
            break;
        case 'f':
            // Microseconds (000000..999999)
            pos = int_to_str(_microsecond, buf);
            to = append_with_prefix(buf, pos - buf, '0', 6, to);
            break;
        case 'h':
        case 'I':
            // Hour (01..12)
            pos = int_to_str((_hour % 24 + 11) % 12 + 1, buf);
            to = append_with_prefix(buf, pos - buf, '0', 2, to);
            break;
        case 'H':
            // Hour (00..23)
            pos = int_to_str(_hour, buf);
            to = append_with_prefix(buf, pos - buf, '0', 2, to);
            break;
        case 'i':
            // Minutes, numeric (00..59)
            pos = int_to_str(_minute, buf);
            to = append_with_prefix(buf, pos - buf, '0', 2, to);
            break;
        case 'j':
            // Day of year (001..366)
            pos = int_to_str(daynr() - calc_daynr(_year, 1, 1) + 1, buf);
            to = append_with_prefix(buf, pos - buf, '0', 3, to);
            break;
        case 'k':
            // Hour (0..23)
            pos = int_to_str(_hour, buf);
            to = append_with_prefix(buf, pos - buf, '0', 1, to);
            break;
        case 'l':
            // Hour (1..12)
            pos = int_to_str((_hour % 24 + 11) % 12 + 1, buf);
            to = append_with_prefix(buf, pos - buf, '0', 1, to);
            break;
        case 'm':
            // Month, numeric (00..12)
            pos = int_to_str(_month, buf);
            to = append_with_prefix(buf, pos - buf, '0', 2, to);
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
        case 's':
        case 'S':
            // Seconds (00..59)
            pos = int_to_str(_second, buf);
            to = append_with_prefix(buf, pos - buf, '0', 2, to);
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
            pos = int_to_str(week(mysql_week_mode(1)), buf);
            to = append_with_prefix(buf, pos - buf, '0', 2, to);
            break;
        case 'U':
            // Week (00..53), where Sunday is the first day of the week;
            // WEEK() mode 0
            if (_type == TIME_TIME) {
                return false;
            }
            pos = int_to_str(week(mysql_week_mode(0)), buf);
            to = append_with_prefix(buf, pos - buf, '0', 2, to);
            break;
        case 'v':
            // Week (01..53), where Monday is the first day of the week;
            // WEEK() mode 3; used with %x
            if (_type == TIME_TIME) {
                return false;
            }
            pos = int_to_str(week(mysql_week_mode(3)), buf);
            to = append_with_prefix(buf, pos - buf, '0', 2, to);
            break;
        case 'V':
            // Week (01..53), where Sunday is the first day of the week;
            // WEEK() mode 2; used with %X
            if (_type == TIME_TIME) {
                return false;
            }
            pos = int_to_str(week(mysql_week_mode(2)), buf);
            to = append_with_prefix(buf, pos - buf, '0', 2, to);
            break;
        case 'w':
            // Day of the week (0=Sunday..6=Saturday)
            if (_type == TIME_TIME || (_month == 0 && _year == 0)) {
                return false;
            }
            pos = int_to_str(calc_weekday(daynr(), true), buf);
            to = append_with_prefix(buf, pos - buf, '0', 1, to);
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
            calc_week(*this, mysql_week_mode(3), &year);
            pos = int_to_str(year, buf);
            to = append_with_prefix(buf, pos - buf, '0', 4, to);
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
            pos = int_to_str(year, buf);
            to = append_with_prefix(buf, pos - buf, '0', 4, to);
            break;
        }
        case 'y':
            // Year, numeric (two digits)
            pos = int_to_str(_year % 100, buf);
            to = append_with_prefix(buf, pos - buf, '0', 2, to);
            break;
        case 'Y':
            // Year, numeric, four digits
            pos = int_to_str(_year, buf);
            to = append_with_prefix(buf, pos - buf, '0', 4, to);
            break;
        default:
            *to++ = ch;
            break;
        }
    }
    *to++ = '\0';
    return true;
}

uint8_t DateTimeValue::calc_week(const DateTimeValue& value, uint8_t mode, uint32_t* year) {
    bool monday_first = mode & WEEK_MONDAY_FIRST;
    bool week_year = mode & WEEK_YEAR;
    bool first_weekday = mode & WEEK_FIRST_WEEKDAY;
    uint64_t day_nr = value.daynr();
    uint64_t daynr_first_day = calc_daynr(value._year, 1, 1);
    uint8_t weekday_first_day = calc_weekday(daynr_first_day, !monday_first);

    int days = 0;
    *year = value._year;

    // Check wether the first days of this year belongs to last year
    if (value._month == 1 && value._day <= (7 - weekday_first_day)) {
        if (!week_year && ((first_weekday && weekday_first_day != 0) ||
                           (!first_weekday && weekday_first_day > 3))) {
            return 0;
        }
        (*year)--;
        week_year = true;
        daynr_first_day -= (days = calc_days_in_year(*year));
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
        weekday_first_day = (weekday_first_day + calc_days_in_year(*year)) % 7;
        if ((first_weekday && weekday_first_day == 0) ||
            (!first_weekday && weekday_first_day <= 3)) {
            // Belong to next year.
            (*year)++;
            return 1;
        }
    }

    return days / 7 + 1;
}

uint8_t DateTimeValue::week(uint8_t mode) const {
    uint32_t year = 0;
    return calc_week(*this, mode, &year);
}

uint32_t DateTimeValue::year_week(uint8_t mode) const {
    uint32_t year = 0;
    // The range of the week in the year_week is 1-53, so the mode WEEK_YEAR is always true.
    uint8_t week = calc_week(*this, mode | 2, &year);
    // When the mode WEEK_FIRST_WEEKDAY is not set,
    // the week in which the last three days of the year fall may belong to the following year.
    if (week == 53 && day() >= 29 && !(mode & 4)) {
        uint8_t monday_first = mode & WEEK_MONDAY_FIRST;
        uint64_t daynr_of_last_day = calc_daynr(_year, 12, 31);
        uint8_t weekday_of_last_day = calc_weekday(daynr_of_last_day, !monday_first);

        if (weekday_of_last_day - monday_first < 2) {
            ++year;
            week = 1;
        }
    }
    return year * 100 + week;
}

uint8_t DateTimeValue::calc_weekday(uint64_t day_nr, bool is_sunday_first_day) {
    return (day_nr + 5L + (is_sunday_first_day ? 1L : 0L)) % 7;
}

// TODO(zhaochun): Think endptr is nullptr
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
    for (; lib[pos] != nullptr; ++pos) {
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
bool DateTimeValue::from_date_format_str(const char* format, int format_len, const char* value,
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
            const char* tmp = nullptr;
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
                tmp = val + min(6, val_end - val);
                if (!str_to_int64(val, &tmp, &int_value)) {
                    return false;
                }
                int_value *= log_10_int[6 - (tmp - val)];
                microsecond = int_value;
                val = tmp;
                frac_part_used = true;
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
            case 'r':
                if (!from_date_format_str("%I:%i:%S %p", 11, val, val_end - val, &tmp)) {
                    return false;
                }
                val = tmp;
                time_part_used = true;
                already_set_time_part = true;
                break;
            case 'T':
                if (!from_date_format_str("%H:%i:%S", 8, val, val_end - val, &tmp)) {
                    return false;
                }
                time_part_used = true;
                already_set_time_part = true;
                val = tmp;
                break;
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
        uint64_t days = calc_daynr(year, 1, 1) + yearday - 1;
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
        uint64_t days = calc_daynr(strict_week_number ? strict_week_number_year : year, 1, 1);

        uint8_t weekday_b = calc_weekday(days, sunday_first);

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
    //    so we only neet to set date part
    // 3. if both are true, means all part of date_time be set, no need check_range_and_set_time
    bool already_set_date_part = yearday > 0 || (week_num >= 0 && weekday > 0);
    if (already_set_date_part && already_set_time_part) return true;
    if (already_set_date_part)
        return check_range_and_set_time(_year, _month, _day, hour, minute, second, microsecond,
                                        _type);
    if (already_set_time_part)
        return check_range_and_set_time(year, month, day, _hour, _minute, _second, _microsecond,
                                        _type);

    return check_range_and_set_time(year, month, day, hour, minute, second, microsecond, _type);
}

bool DateTimeValue::date_add_interval(const TimeInterval& interval, TimeUnit unit) {
    if (!is_valid_date()) return false;

    int sign = interval.is_neg ? -1 : 1;
    switch (unit) {
    case MICROSECOND:
    case SECOND:
    case MINUTE:
    case HOUR:
    case SECOND_MICROSECOND:
    case MINUTE_MICROSECOND:
    case MINUTE_SECOND:
    case HOUR_MICROSECOND:
    case HOUR_SECOND:
    case HOUR_MINUTE:
    case DAY_MICROSECOND:
    case DAY_SECOND:
    case DAY_MINUTE:
    case DAY_HOUR: {
        // This may change the day information
        int64_t microseconds = _microsecond + sign * interval.microsecond;
        int64_t extra_second = microseconds / 1000000L;
        microseconds %= 1000000L;

        int64_t seconds = (_day - 1) * 86400L + _hour * 3600L + _minute * 60 + _second +
                          sign * (interval.day * 86400 + interval.hour * 3600 +
                                  interval.minute * 60 + interval.second) +
                          extra_second;
        if (microseconds < 0) {
            seconds--;
            microseconds += 1000000L;
        }
        int64_t days = seconds / 86400;
        seconds %= 86400L;
        if (seconds < 0) {
            seconds += 86400L;
            days--;
        }
        _microsecond = microseconds;
        _second = seconds % 60;
        _minute = (seconds / 60) % 60;
        _hour = seconds / 3600;
        int64_t day_nr = calc_daynr(_year, _month, 1) + days;
        if (!get_date_from_daynr(day_nr)) {
            return false;
        }
        _type = TIME_DATETIME;
        break;
    }
    case DAY:
    case WEEK: {
        // This only change day information, not change second information
        int64_t day_nr = daynr() + interval.day * sign;
        if (!get_date_from_daynr(day_nr)) {
            return false;
        }
        break;
    }
    case YEAR: {
        // This only change year information
        _year += sign * interval.year;
        if (_year > 9999) {
            return false;
        }
        if (_month == 2 && _day == 29 && !is_leap(_year)) {
            _day = 28;
        }
        break;
    }
    case MONTH:
    case QUARTER:
    case YEAR_MONTH: {
        // This will change month and year information, maybe date.
        int64_t months = _year * 12 + _month - 1 + sign * (12 * interval.year + interval.month);
        _year = months / 12;
        if (_year > 9999) {
            return false;
        }
        _month = (months % 12) + 1;
        if (_day > s_days_in_month[_month]) {
            _day = s_days_in_month[_month];
            if (_month == 2 && is_leap(_year)) {
                _day++;
            }
        }
        break;
    }
    }
    return true;
}

bool DateTimeValue::unix_timestamp(int64_t* timestamp, const std::string& timezone) const {
    cctz::time_zone ctz;
    if (!TimezoneUtils::find_cctz_time_zone(timezone, ctz)) {
        return false;
    }
    return unix_timestamp(timestamp, ctz);
}

bool DateTimeValue::unix_timestamp(int64_t* timestamp, const cctz::time_zone& ctz) const {
    const auto tp =
            cctz::convert(cctz::civil_second(_year, _month, _day, _hour, _minute, _second), ctz);
    *timestamp = tp.time_since_epoch().count();
    return true;
}

bool DateTimeValue::from_unixtime(int64_t timestamp, const std::string& timezone) {
    cctz::time_zone ctz;
    if (!TimezoneUtils::find_cctz_time_zone(timezone, ctz)) {
        return false;
    }
    return from_unixtime(timestamp, ctz);
}

bool DateTimeValue::from_unixtime(int64_t timestamp, const cctz::time_zone& ctz) {
    static const cctz::time_point<cctz::sys_seconds> epoch =
            std::chrono::time_point_cast<cctz::sys_seconds>(
                    std::chrono::system_clock::from_time_t(0));
    cctz::time_point<cctz::sys_seconds> t = epoch + cctz::seconds(timestamp);

    const auto tp = cctz::convert(t, ctz);

    _neg = 0;
    _type = TIME_DATETIME;
    _year = tp.year();
    _month = tp.month();
    _day = tp.day();
    _hour = tp.hour();
    _minute = tp.minute();
    _second = tp.second();
    _microsecond = 0;

    return true;
}

const char* DateTimeValue::month_name() const {
    if (_month < 1 || _month > 12) {
        return nullptr;
    }
    return s_month_name[_month];
}

const char* DateTimeValue::day_name() const {
    int day = weekday();
    if (day < 0 || day >= 7) {
        return nullptr;
    }
    return s_day_name[day];
}

DateTimeValue DateTimeValue::local_time() {
    DateTimeValue value;
    value.from_unixtime(time(nullptr), TimezoneUtils::default_time_zone);
    return value;
}

void DateTimeValue::set_time(uint32_t year, uint32_t month, uint32_t day, uint32_t hour,
                             uint32_t minute, uint32_t second, uint32_t microsecond) {
    _year = year;
    _month = month;
    _day = day;
    _hour = hour;
    _minute = minute;
    _second = second;
    _microsecond = microsecond;
}

std::ostream& operator<<(std::ostream& os, const DateTimeValue& value) {
    char buf[64];
    value.to_string(buf);
    return os << buf;
}

// NOTE:
//  only support DATE - DATE (no support DATETIME - DATETIME)
std::size_t operator-(const DateTimeValue& v1, const DateTimeValue& v2) {
    return v1.daynr() - v2.daynr();
}

std::size_t hash_value(DateTimeValue const& value) {
    return HashUtil::hash(&value, sizeof(DateTimeValue), 0);
}

} // namespace doris
