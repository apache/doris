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

#include <util/time_lut.h>

namespace doris {

static uint32_t calc_daynr(uint16_t year, uint8_t month, uint8_t day) {
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

uint8_t calc_week(uint16_t yy, uint8_t mm, uint8_t dd, bool monday_first, bool week_year) {
    if (mm == 0 || dd == 0) return 0;
    uint64_t day_nr = calc_daynr(yy, mm, dd);
    uint64_t daynr_first_day = calc_daynr(yy, 1, 1);
    uint8_t weekday_first_day = calc_weekday(daynr_first_day, !monday_first);

    int days = 0;
    uint16_t year = yy;

    // Check wether the first days of this year belongs to last year
    if (mm == 1 && dd <= (7 - weekday_first_day)) {
        if (weekday_first_day != 0) {
            return 0;
        }
        year--;
        week_year = true;
        daynr_first_day -= (days = calc_days_in_year(year));
        weekday_first_day = (weekday_first_day + 53 * 7 - days) % 7;
    }

    // How many days since first week
    if (weekday_first_day != 0) {
        // days in new year belongs to last year.
        days = day_nr - (daynr_first_day + (7 - weekday_first_day));
    } else {
        // days in new year belongs to this year.
        days = day_nr - (daynr_first_day - weekday_first_day);
    }

    if (week_year && days >= 52 * 7) {
        weekday_first_day = (weekday_first_day + calc_days_in_year(year)) % 7;
        if (weekday_first_day == 0) {
            // Belong to next year.
            return 1;
        }
    }
    return days / 7 + 1;
}

} // namespace doris
