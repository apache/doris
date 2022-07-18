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

#pragma once

#include <stdint.h>

namespace doris {

static constexpr uint32_t LUT_START_YEAR = 1950;
static constexpr uint32_t LUT_END_YEAR = 2030;

static constexpr uint32_t NUM_MONTHS = 12;
static constexpr uint32_t NUM_DAYS = 31;

static int8_t week_of_year_table[LUT_END_YEAR - LUT_START_YEAR][NUM_MONTHS][NUM_DAYS];

static int8_t year_week_table[LUT_END_YEAR - LUT_START_YEAR][NUM_MONTHS][NUM_DAYS];

static int8_t week_table[LUT_END_YEAR - LUT_START_YEAR][NUM_MONTHS][NUM_DAYS];

static uint32_t calc_daynr(uint16_t year, uint8_t month, uint8_t day);

static uint8_t calc_weekday(uint64_t day_nr, bool is_sunday_first_day) {
    return (day_nr + 5L + (is_sunday_first_day ? 1L : 0L)) % 7;
}

bool is_leap(uint32_t year) {
    return ((year % 4) == 0) && ((year % 100 != 0) || ((year % 400) == 0 && year));
}

uint32_t calc_days_in_year(uint32_t year) {
    return is_leap(year) ? 366 : 365;
}

uint8_t calc_week(uint16_t yy, uint8_t mm, uint8_t dd, bool monday_first, bool week_year);

void init_time_lut() {
    for (uint32_t y = LUT_START_YEAR; y < LUT_END_YEAR; y++) {
        for (uint8_t m = 0; m < NUM_MONTHS; m++) {
            for (uint8_t i = 0; i < NUM_DAYS; i++) {
                week_table[y - LUT_START_YEAR][m][i] = calc_week(y, m + 1, i + 1, false, false);
                week_of_year_table[y - LUT_START_YEAR][m][i] =
                        calc_week(y, m + 1, i + 1, true, true);
                year_week_table[y - LUT_START_YEAR][m][i] = calc_week(y, m + 1, i + 1, false, true);
            }
        }
    }
}

} // namespace doris
