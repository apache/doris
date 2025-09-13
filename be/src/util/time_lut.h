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

#include <atomic>
#include <cstdint>

namespace doris {

constexpr uint16_t LUT_START_YEAR = 1950;
constexpr uint16_t LUT_END_YEAR = 2030;

constexpr uint8_t NUM_MONTHS = 12;
constexpr uint8_t NUM_DAYS = 31;

uint32_t year_week(uint16_t yy, uint8_t month, uint8_t day);

uint8_t calc_weekday(uint64_t day_nr, bool is_sunday_first_day);

inline bool is_leap(uint32_t year) {
    // 0000(BC01) is a leap year
    return ((year % 4) == 0) && (year % 100 != 0 || (year % 400) == 0);
}

uint16_t calc_days_in_year(uint32_t year);

uint8_t calc_week(uint16_t yy, uint8_t month, uint8_t day, bool monday_first, bool week_year,
                  bool first_weekday, uint16_t* to_year);

class TimeLUTImpl {
public:
    int8_t week_of_year_table[LUT_END_YEAR - LUT_START_YEAR][NUM_MONTHS][NUM_DAYS];

    uint32_t year_week_table[LUT_END_YEAR - LUT_START_YEAR][NUM_MONTHS][NUM_DAYS];

    int8_t week_table[LUT_END_YEAR - LUT_START_YEAR][NUM_MONTHS][NUM_DAYS];

private:
    friend class TimeLUT;

    TimeLUTImpl();
    void init_time_lut();
};

class TimeLUT {
public:
    static const TimeLUTImpl* GetImplement() {
        static TimeLUT time_lut;
        return time_lut._impl.load();
    }

protected:
    TimeLUT() { _impl.store(new TimeLUTImpl()); }

private:
    std::atomic<const TimeLUTImpl*> _impl;
};

} // namespace doris
