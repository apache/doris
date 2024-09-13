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

package org.apache.doris.nereids.trees.expressions.functions.executable;

import org.apache.doris.nereids.trees.expressions.ExecFunction;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.literal.DateLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.DateV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

/**
 * executable function:
 * date_add/sub, years/months/week/days/hours/minutes/seconds_add/sub, datediff
 */
public class DateTimeArithmetic {
    /**
     * datetime arithmetic function date-add.
     */
    @ExecFunction(name = "date_add")
    public static Expression dateAdd(DateLiteral date, IntegerLiteral day) {
        return daysAdd(date, day);
    }

    @ExecFunction(name = "date_add")
    public static Expression dateAdd(DateTimeLiteral date, IntegerLiteral day) {
        return daysAdd(date, day);
    }

    @ExecFunction(name = "date_add")
    public static Expression dateAdd(DateV2Literal date, IntegerLiteral day) {
        return daysAdd(date, day);
    }

    @ExecFunction(name = "date_add")
    public static Expression dateAdd(DateTimeV2Literal date, IntegerLiteral day) {
        return daysAdd(date, day);
    }

    /**
     * datetime arithmetic function date-sub.
     */
    @ExecFunction(name = "date_sub")
    public static Expression dateSub(DateLiteral date, IntegerLiteral day) {
        return dateAdd(date, new IntegerLiteral(-day.getValue()));
    }

    @ExecFunction(name = "date_sub")
    public static Expression dateSub(DateTimeLiteral date, IntegerLiteral day) {
        return dateAdd(date, new IntegerLiteral(-day.getValue()));
    }

    @ExecFunction(name = "date_sub")
    public static Expression dateSub(DateV2Literal date, IntegerLiteral day) {
        return dateAdd(date, new IntegerLiteral(-day.getValue()));
    }

    @ExecFunction(name = "date_sub")
    public static Expression dateSub(DateTimeV2Literal date, IntegerLiteral day) {
        return dateAdd(date, new IntegerLiteral(-day.getValue()));
    }

    /**
     * datetime arithmetic function years-add.
     */
    @ExecFunction(name = "years_add")
    public static Expression yearsAdd(DateLiteral date, IntegerLiteral year) {
        return date.plusYears(year.getValue());
    }

    @ExecFunction(name = "years_add")
    public static Expression yearsAdd(DateTimeLiteral date, IntegerLiteral year) {
        return date.plusYears(year.getValue());
    }

    @ExecFunction(name = "years_add")
    public static Expression yearsAdd(DateV2Literal date, IntegerLiteral year) {
        return date.plusYears(year.getValue());
    }

    @ExecFunction(name = "years_add")
    public static Expression yearsAdd(DateTimeV2Literal date, IntegerLiteral year) {
        return date.plusYears(year.getValue());
    }

    /**
     * datetime arithmetic function months-add.
     */
    @ExecFunction(name = "months_add")
    public static Expression monthsAdd(DateLiteral date, IntegerLiteral month) {
        return date.plusMonths(month.getValue());
    }

    @ExecFunction(name = "months_add")
    public static Expression monthsAdd(DateTimeLiteral date, IntegerLiteral month) {
        return date.plusMonths(month.getValue());
    }

    @ExecFunction(name = "months_add")
    public static Expression monthsAdd(DateV2Literal date, IntegerLiteral month) {
        return date.plusMonths(month.getValue());
    }

    @ExecFunction(name = "months_add")
    public static Expression monthsAdd(DateTimeV2Literal date, IntegerLiteral month) {
        return date.plusMonths(month.getValue());
    }

    /**
     * datetime arithmetic function weeks-add.
     */
    @ExecFunction(name = "weeks_add")
    public static Expression weeksAdd(DateLiteral date, IntegerLiteral weeks) {
        return date.plusWeeks(weeks.getValue());
    }

    @ExecFunction(name = "weeks_add")
    public static Expression weeksAdd(DateTimeLiteral date, IntegerLiteral weeks) {
        return date.plusWeeks(weeks.getValue());
    }

    @ExecFunction(name = "weeks_add")
    public static Expression weeksAdd(DateV2Literal date, IntegerLiteral weeks) {
        return date.plusWeeks(weeks.getValue());
    }

    @ExecFunction(name = "weeks_add")
    public static Expression weeksAdd(DateTimeV2Literal date, IntegerLiteral weeks) {
        return date.plusWeeks(weeks.getValue());
    }

    /**
     * datetime arithmetic function days-add.
     */
    @ExecFunction(name = "days_add")
    public static Expression daysAdd(DateLiteral date, IntegerLiteral day) {
        return date.plusDays(day.getValue());
    }

    @ExecFunction(name = "days_add")
    public static Expression daysAdd(DateTimeLiteral date, IntegerLiteral day) {
        return date.plusDays(day.getValue());
    }

    @ExecFunction(name = "days_add")
    public static Expression daysAdd(DateV2Literal date, IntegerLiteral day) {
        return date.plusDays(day.getValue());
    }

    @ExecFunction(name = "days_add")
    public static Expression daysAdd(DateTimeV2Literal date, IntegerLiteral day) {
        return date.plusDays(day.getValue());
    }

    /**
     * datetime arithmetic function hours-add.
     */
    @ExecFunction(name = "hours_add")
    public static Expression hoursAdd(DateTimeLiteral date, IntegerLiteral hour) {
        return date.plusHours(hour.getValue());
    }

    @ExecFunction(name = "hours_add")
    public static Expression hoursAdd(DateTimeV2Literal date, IntegerLiteral hour) {
        return date.plusHours(hour.getValue());
    }

    /**
     * datetime arithmetic function minutes-add.
     */
    @ExecFunction(name = "minutes_add")
    public static Expression minutesAdd(DateTimeLiteral date, IntegerLiteral minute) {
        return date.plusMinutes(minute.getValue());
    }

    @ExecFunction(name = "minutes_add")
    public static Expression minutesAdd(DateTimeV2Literal date, IntegerLiteral minute) {
        return date.plusMinutes(minute.getValue());
    }

    /**
     * datetime arithmetic function seconds-add.
     */
    @ExecFunction(name = "seconds_add")
    public static Expression secondsAdd(DateTimeLiteral date, IntegerLiteral second) {
        return date.plusSeconds(second.getValue());
    }

    @ExecFunction(name = "seconds_add")
    public static Expression secondsAdd(DateTimeV2Literal date, IntegerLiteral second) {
        return date.plusSeconds(second.getValue());
    }

    /**
     * datetime arithmetic function microseconds-add.
     */
    @ExecFunction(name = "microseconds_add")
    public static Expression microSecondsAdd(DateTimeV2Literal date, IntegerLiteral microSecond) {
        return date.plusMicroSeconds(microSecond.getValue());
    }

    /**
     * datetime arithmetic function microseconds_sub.
     */
    @ExecFunction(name = "microseconds_sub")
    public static Expression microSecondsSub(DateTimeV2Literal date, IntegerLiteral microSecond) {
        return date.plusMicroSeconds(-microSecond.getValue());
    }

    /**
     * datetime arithmetic function milliseconds_add.
     */
    @ExecFunction(name = "milliseconds_add")
    public static Expression milliSecondsAdd(DateTimeV2Literal date, IntegerLiteral milliSecond) {
        return date.plusMilliSeconds(milliSecond.getValue());
    }

    /**
     * datetime arithmetic function milliseconds_sub.
     */
    @ExecFunction(name = "milliseconds_sub")
    public static Expression milliSecondsSub(DateTimeV2Literal date, IntegerLiteral milliSecond) {
        return date.plusMilliSeconds(-milliSecond.getValue());
    }

    /**
     * datetime arithmetic function years-sub.
     */
    @ExecFunction(name = "years_sub")
    public static Expression yearsSub(DateLiteral date, IntegerLiteral year) {
        return yearsAdd(date, new IntegerLiteral(-year.getValue()));
    }

    @ExecFunction(name = "years_sub")
    public static Expression yearsSub(DateTimeLiteral date, IntegerLiteral year) {
        return yearsAdd(date, new IntegerLiteral(-year.getValue()));
    }

    @ExecFunction(name = "years_sub")
    public static Expression yearsSub(DateV2Literal date, IntegerLiteral year) {
        return yearsAdd(date, new IntegerLiteral(-year.getValue()));
    }

    @ExecFunction(name = "years_sub")
    public static Expression yearsSub(DateTimeV2Literal date, IntegerLiteral year) {
        return yearsAdd(date, new IntegerLiteral(-year.getValue()));
    }

    /**
     * datetime arithmetic function months-sub
     */
    @ExecFunction(name = "months_sub")
    public static Expression monthsSub(DateLiteral date, IntegerLiteral month) {
        return monthsAdd(date, new IntegerLiteral(-month.getValue()));
    }

    @ExecFunction(name = "months_sub")
    public static Expression monthsSub(DateTimeLiteral date, IntegerLiteral month) {
        return monthsAdd(date, new IntegerLiteral(-month.getValue()));
    }

    @ExecFunction(name = "months_sub")
    public static Expression monthsSub(DateV2Literal date, IntegerLiteral month) {
        return monthsAdd(date, new IntegerLiteral(-month.getValue()));
    }

    @ExecFunction(name = "months_sub")
    public static Expression monthsSub(DateTimeV2Literal date, IntegerLiteral month) {
        return monthsAdd(date, new IntegerLiteral(-month.getValue()));
    }

    /**
     * datetime arithmetic function weeks-sub.
     */
    @ExecFunction(name = "weeks_sub")
    public static Expression weeksSub(DateLiteral date, IntegerLiteral weeks) {
        return date.plusWeeks(-weeks.getValue());
    }

    @ExecFunction(name = "weeks_sub")
    public static Expression weeksSub(DateTimeLiteral date, IntegerLiteral weeks) {
        return date.plusWeeks(-weeks.getValue());
    }

    @ExecFunction(name = "weeks_sub")
    public static Expression weeksSub(DateV2Literal date, IntegerLiteral weeks) {
        return date.plusWeeks(-weeks.getValue());
    }

    @ExecFunction(name = "weeks_sub")
    public static Expression weeksSub(DateTimeV2Literal date, IntegerLiteral weeks) {
        return date.plusWeeks(-weeks.getValue());
    }

    /**
     * datetime arithmetic function days-sub
     */
    @ExecFunction(name = "days_sub")
    public static Expression daysSub(DateLiteral date, IntegerLiteral day) {
        return daysAdd(date, new IntegerLiteral(-day.getValue()));
    }

    @ExecFunction(name = "days_sub")
    public static Expression daysSub(DateTimeLiteral date, IntegerLiteral day) {
        return daysAdd(date, new IntegerLiteral(-day.getValue()));
    }

    @ExecFunction(name = "days_sub")
    public static Expression daysSub(DateV2Literal date, IntegerLiteral day) {
        return daysAdd(date, new IntegerLiteral(-day.getValue()));
    }

    @ExecFunction(name = "days_sub")
    public static Expression daysSub(DateTimeV2Literal date, IntegerLiteral day) {
        return daysAdd(date, new IntegerLiteral(-day.getValue()));
    }

    /**
     * datetime arithmetic function hours-sub
     */
    @ExecFunction(name = "hours_sub")
    public static Expression hoursSub(DateTimeLiteral date, IntegerLiteral hour) {
        return hoursAdd(date, new IntegerLiteral(-hour.getValue()));
    }

    @ExecFunction(name = "hours_sub")
    public static Expression hoursSub(DateTimeV2Literal date, IntegerLiteral hour) {
        return hoursAdd(date, new IntegerLiteral(-hour.getValue()));
    }

    /**
     * datetime arithmetic function minutes-sub
     */
    @ExecFunction(name = "minutes_sub")
    public static Expression minutesSub(DateTimeLiteral date, IntegerLiteral minute) {
        return minutesAdd(date, new IntegerLiteral(-minute.getValue()));
    }

    @ExecFunction(name = "minutes_sub")
    public static Expression minutesSub(DateTimeV2Literal date, IntegerLiteral minute) {
        return minutesAdd(date, new IntegerLiteral(-minute.getValue()));
    }

    /**
     * datetime arithmetic function seconds-sub
     */
    @ExecFunction(name = "seconds_sub")
    public static Expression secondsSub(DateTimeLiteral date, IntegerLiteral second) {
        return secondsAdd(date, new IntegerLiteral(-second.getValue()));
    }

    @ExecFunction(name = "seconds_sub")
    public static Expression secondsSub(DateTimeV2Literal date, IntegerLiteral second) {
        return secondsAdd(date, new IntegerLiteral(-second.getValue()));
    }

    /**
     * datetime arithmetic function datediff
     */
    @ExecFunction(name = "datediff")
    public static Expression dateDiff(DateTimeLiteral date1, DateTimeLiteral date2) {
        return new IntegerLiteral(dateDiff(date1.toJavaDateType(), date2.toJavaDateType()));
    }

    @ExecFunction(name = "datediff")
    public static Expression dateDiff(DateV2Literal date1, DateV2Literal date2) {
        return new IntegerLiteral(dateDiff(date1.toJavaDateType(), date2.toJavaDateType()));
    }

    @ExecFunction(name = "datediff")
    public static Expression dateDiff(DateV2Literal date1, DateTimeV2Literal date2) {
        return new IntegerLiteral(dateDiff(date1.toJavaDateType(), date2.toJavaDateType()));
    }

    @ExecFunction(name = "datediff")
    public static Expression dateDiff(DateTimeV2Literal date1, DateV2Literal date2) {
        return new IntegerLiteral(dateDiff(date1.toJavaDateType(), date2.toJavaDateType()));
    }

    @ExecFunction(name = "datediff")
    public static Expression dateDiff(DateTimeV2Literal date1, DateTimeV2Literal date2) {
        return new IntegerLiteral(dateDiff(date1.toJavaDateType(), date2.toJavaDateType()));
    }

    private static int dateDiff(LocalDateTime date1, LocalDateTime date2) {
        return ((int) ChronoUnit.DAYS.between(date2.toLocalDate(), date1.toLocalDate()));
    }
}
