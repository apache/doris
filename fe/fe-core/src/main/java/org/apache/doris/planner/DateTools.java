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

package org.apache.doris.planner;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;

public class DateTools {
    // format string DateTime  And Full Zero for hour,minute,second
    public static LocalDateTime formatDateTimeAndFullZero(String datetime, DateTimeFormatter formatter) {
        TemporalAccessor temporal = formatter.parse(datetime);
        int year = temporal.isSupported(ChronoField.YEAR)
                ? temporal.get(ChronoField.YEAR) : 0;
        int month = temporal.isSupported(ChronoField.MONTH_OF_YEAR)
                ? temporal.get(ChronoField.MONTH_OF_YEAR) : 1;
        int day = temporal.isSupported(ChronoField.DAY_OF_MONTH)
                ? temporal.get(ChronoField.DAY_OF_MONTH) : 1;
        int hour = temporal.isSupported(ChronoField.HOUR_OF_DAY)
                ? temporal.get(ChronoField.HOUR_OF_DAY) : 0;
        int minute = temporal.isSupported(ChronoField.MINUTE_OF_HOUR)
                ? temporal.get(ChronoField.MINUTE_OF_HOUR) : 0;
        int second = temporal.isSupported(ChronoField.SECOND_OF_MINUTE)
                ? temporal.get(ChronoField.SECOND_OF_MINUTE) : 0;
        int milliSecond = temporal.isSupported(ChronoField.MILLI_OF_SECOND)
                ? temporal.get(ChronoField.MILLI_OF_SECOND) : 0;
        return LocalDateTime.of(LocalDate.of(year, month, day),
                LocalTime.of(hour, minute, second, milliSecond * 1000000));
    }
}
