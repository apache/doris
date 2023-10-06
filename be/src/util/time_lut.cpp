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
//

#include "util/time_lut.h"

#include "vec/runtime/vdatetime_value.h"

namespace doris {
TimeLUTImpl::TimeLUTImpl() {
    init_time_lut();
}

void TimeLUTImpl::init_time_lut() {
    for (uint32_t y = LUT_START_YEAR; y < LUT_END_YEAR; y++) {
        uint16_t tmp_year = 0;
        for (uint8_t m = 0; m < NUM_MONTHS; m++) {
            for (uint8_t i = 0; i < NUM_DAYS; i++) {
                week_table[y - LUT_START_YEAR][m][i] =
                        calc_week(y, m + 1, i + 1, false, false, true, &tmp_year);
                week_of_year_table[y - LUT_START_YEAR][m][i] =
                        calc_week(y, m + 1, i + 1, true, true, false, &tmp_year);
                year_week_table[y - LUT_START_YEAR][m][i] = year_week(y, m + 1, i + 1);
            }
        }
    }
}

uint8_t calc_week(uint16_t year, uint8_t month, uint8_t day, bool monday_first, bool week_year,
                  bool first_weekday, uint16_t* to_year) {
    uint64_t day_nr = calc_daynr(year, month, day);
    uint64_t daynr_first_day = calc_daynr(year, 1, 1);
    uint8_t weekday_first_day = calc_weekday(daynr_first_day, !monday_first);

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
        daynr_first_day -= (days = calc_days_in_year(*to_year));
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
        weekday_first_day = (weekday_first_day + calc_days_in_year(*to_year)) % 7;
        if ((first_weekday && weekday_first_day == 0) ||
            (!first_weekday && weekday_first_day <= 3)) {
            // Belong to next year.
            (*to_year)++;
            return 1;
        }
    }

    return days / 7 + 1;
}

uint32_t calc_days_in_year(uint32_t year) {
    return is_leap(year) ? 366 : 365;
}

bool is_leap(uint32_t year) {
    return ((year % 4) == 0) && ((year % 100 != 0) || ((year % 400) == 0 && year));
}

uint8_t calc_weekday(uint64_t day_nr, bool is_sunday_first_day) {
    return (day_nr + 5L + (is_sunday_first_day ? 1L : 0L)) % 7;
}

uint32_t calc_daynr(uint16_t year, uint8_t month, uint8_t day) {
    // date_day_offet_dict range from [1900-01-01, 2039-10-24]
    if (vectorized::date_day_offset_dict::can_speed_up_calc_daynr(year) &&
        LIKELY(vectorized::date_day_offset_dict::get_dict_init())) {
        return vectorized::date_day_offset_dict::get().daynr(year, month, day);
    }

    uint32_t delsum = 0;
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

uint32_t year_week(uint16_t yy, uint8_t month, uint8_t day) {
    //not covered by year_week_table, calculate at runtime
    uint16_t to_year = 0;
    // The range of the week in the year_week is 1-53, so the mode WEEK_YEAR is always true.
    uint8_t week = calc_week(yy, month, day, false, true, true, &to_year);
    return to_year * 100 + week;
}

} // namespace doris
