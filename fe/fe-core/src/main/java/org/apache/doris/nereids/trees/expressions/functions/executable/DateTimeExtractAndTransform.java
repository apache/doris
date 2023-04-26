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

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.ExecFunction;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.DateV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.util.DateUtils;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;

/**
 * executable function:
 * year, quarter, month, week, dayOfYear, dayOfweek, dayOfMonth, hour, minute, second
 */
public class DateTimeExtractAndTransform {
    /**
     * datetime arithmetic function date-v2
     */
    @ExecFunction(name = "datev2", argTypes = {"DATETIMEV2"}, returnType = "DATEV2")
    public static DateV2Literal dateV2(DateTimeV2Literal dateTime) {
        return new DateV2Literal(dateTime.getYear(), dateTime.getMonth(), dateTime.getDay());
    }

    /**
     * Executable datetime extract year
     */
    @ExecFunction(name = "year", argTypes = {"DATE"}, returnType = "INT")
    public static IntegerLiteral year(DateLiteral date) {
        return new IntegerLiteral(((int) date.getYear()));
    }

    @ExecFunction(name = "year", argTypes = {"DATETIME"}, returnType = "INT")
    public static IntegerLiteral year(DateTimeLiteral date) {
        return new IntegerLiteral(((int) date.getYear()));
    }

    @ExecFunction(name = "year", argTypes = {"DATEV2"}, returnType = "INT")
    public static IntegerLiteral year(DateV2Literal date) {
        return new IntegerLiteral(((int) date.getYear()));
    }

    @ExecFunction(name = "year", argTypes = {"DATETIMEV2"}, returnType = "INT")
    public static IntegerLiteral year(DateTimeV2Literal date) {
        return new IntegerLiteral(((int) date.getYear()));
    }

    /**
     * Executable datetime extract quarter
     */
    @ExecFunction(name = "quarter", argTypes = {"DATE"}, returnType = "INT")
    public static IntegerLiteral quarter(DateLiteral date) {
        return new IntegerLiteral(((int) date.getMonth() - 1) / 3 + 1);
    }

    @ExecFunction(name = "quarter", argTypes = {"DATETIME"}, returnType = "INT")
    public static IntegerLiteral quarter(DateTimeLiteral date) {
        return new IntegerLiteral(((int) date.getMonth() - 1) / 3 + 1);
    }

    @ExecFunction(name = "quarter", argTypes = {"DATEV2"}, returnType = "INT")
    public static IntegerLiteral quarter(DateV2Literal date) {
        return new IntegerLiteral(((int) date.getMonth() - 1) / 3 + 1);
    }

    @ExecFunction(name = "quarter", argTypes = {"DATETIMEV2"}, returnType = "INT")
    public static IntegerLiteral quarter(DateTimeV2Literal date) {
        return new IntegerLiteral(((int) date.getMonth() - 1) / 3 + 1);
    }

    /**
     * Executable datetime extract month
     */
    @ExecFunction(name = "month", argTypes = {"DATE"}, returnType = "INT")
    public static IntegerLiteral month(DateLiteral date) {
        return new IntegerLiteral(((int) date.getMonth()));
    }

    @ExecFunction(name = "month", argTypes = {"DATETIME"}, returnType = "INT")
    public static IntegerLiteral month(DateTimeLiteral date) {
        return new IntegerLiteral(((int) date.getMonth()));
    }

    @ExecFunction(name = "month", argTypes = {"DATEV2"}, returnType = "INT")
    public static IntegerLiteral month(DateV2Literal date) {
        return new IntegerLiteral(((int) date.getMonth()));
    }

    @ExecFunction(name = "month", argTypes = {"DATETIMEV2"}, returnType = "INT")
    public static IntegerLiteral month(DateTimeV2Literal date) {
        return new IntegerLiteral(((int) date.getMonth()));
    }

    /**
     * Executable datetime extract day
     */
    @ExecFunction(name = "day", argTypes = {"DATE"}, returnType = "INT")
    public static IntegerLiteral day(DateLiteral date) {
        return new IntegerLiteral(((int) date.getDay()));
    }

    @ExecFunction(name = "day", argTypes = {"DATETIME"}, returnType = "INT")
    public static IntegerLiteral day(DateTimeLiteral date) {
        return new IntegerLiteral(((int) date.getDay()));
    }

    @ExecFunction(name = "day", argTypes = {"DATEV2"}, returnType = "INT")
    public static IntegerLiteral day(DateV2Literal date) {
        return new IntegerLiteral(((int) date.getDay()));
    }

    @ExecFunction(name = "day", argTypes = {"DATETIMEV2"}, returnType = "INT")
    public static IntegerLiteral day(DateTimeV2Literal date) {
        return new IntegerLiteral(((int) date.getDay()));
    }

    /**
     * Executable datetime extract hour
     */
    @ExecFunction(name = "hour", argTypes = {"DATETIME"}, returnType = "INT")
    public static IntegerLiteral hour(DateTimeLiteral date) {
        return new IntegerLiteral(((int) date.getHour()));
    }

    @ExecFunction(name = "hour", argTypes = {"DATETIMEV2"}, returnType = "INT")
    public static IntegerLiteral hour(DateTimeV2Literal date) {
        return new IntegerLiteral(((int) date.getHour()));
    }

    /**
     * Executable datetime extract hour
     */
    @ExecFunction(name = "minute", argTypes = {"DATETIME"}, returnType = "INT")
    public static IntegerLiteral minute(DateTimeLiteral date) {
        return new IntegerLiteral(((int) date.getMinute()));
    }

    @ExecFunction(name = "minute", argTypes = {"DATETIMEV2"}, returnType = "INT")
    public static IntegerLiteral minute(DateTimeV2Literal date) {
        return new IntegerLiteral(((int) date.getMinute()));
    }

    /**
     * Executable datetime extract hour
     */
    @ExecFunction(name = "second", argTypes = {"DATETIME"}, returnType = "INT")
    public static IntegerLiteral second(DateTimeLiteral date) {
        return new IntegerLiteral(((int) date.getSecond()));
    }

    @ExecFunction(name = "second", argTypes = {"DATETIMEV2"}, returnType = "INT")
    public static IntegerLiteral second(DateTimeV2Literal date) {
        return new IntegerLiteral(((int) date.getSecond()));
    }

    /**
     * Executable datetime extract dayofyear
     */
    @ExecFunction(name = "dayofyear", argTypes = {"DATE"}, returnType = "INT")
    public static IntegerLiteral dayOfYear(DateLiteral date) {
        return new IntegerLiteral(date.toJavaDateType().getDayOfYear());
    }

    @ExecFunction(name = "dayofyear", argTypes = {"DATETIME"}, returnType = "INT")
    public static IntegerLiteral dayOfYear(DateTimeLiteral date) {
        return new IntegerLiteral(date.toJavaDateType().getDayOfYear());
    }

    @ExecFunction(name = "dayofyear", argTypes = {"DATEV2"}, returnType = "INT")
    public static IntegerLiteral dayOfYear(DateV2Literal date) {
        return new IntegerLiteral(date.toJavaDateType().getDayOfYear());
    }

    @ExecFunction(name = "dayofyear", argTypes = {"DATETIMEV2"}, returnType = "INT")
    public static IntegerLiteral dayOfYear(DateTimeV2Literal date) {
        return new IntegerLiteral(date.toJavaDateType().getDayOfYear());
    }

    /**
     * Executable datetime extract dayofmonth
     */
    @ExecFunction(name = "dayofmonth", argTypes = {"DATE"}, returnType = "INT")
    public static IntegerLiteral dayOfMonth(DateLiteral date) {
        return new IntegerLiteral(date.toJavaDateType().getDayOfMonth());
    }

    @ExecFunction(name = "dayofmonth", argTypes = {"DATETIME"}, returnType = "INT")
    public static IntegerLiteral dayOfMonth(DateTimeLiteral date) {
        return new IntegerLiteral(date.toJavaDateType().getDayOfMonth());
    }

    @ExecFunction(name = "dayofmonth", argTypes = {"DATEV2"}, returnType = "INT")
    public static IntegerLiteral dayOfMonth(DateV2Literal date) {
        return new IntegerLiteral(date.toJavaDateType().getDayOfMonth());
    }

    @ExecFunction(name = "dayofmonth", argTypes = {"DATETIMEV2"}, returnType = "INT")
    public static IntegerLiteral dayOfMonth(DateTimeV2Literal date) {
        return new IntegerLiteral(date.toJavaDateType().getDayOfMonth());
    }

    /**
     * Executable datetime extract dayofweek
     */
    @ExecFunction(name = "dayofweek", argTypes = {"DATE"}, returnType = "INT")
    public static IntegerLiteral dayOfWeek(DateLiteral date) {
        return new IntegerLiteral(date.toJavaDateType().getDayOfWeek().getValue() % 7 + 1);
    }

    @ExecFunction(name = "dayofweek", argTypes = {"DATETIME"}, returnType = "INT")
    public static IntegerLiteral dayOfWeek(DateTimeLiteral date) {
        return new IntegerLiteral(date.toJavaDateType().getDayOfWeek().getValue() % 7 + 1);
    }

    @ExecFunction(name = "dayofweek", argTypes = {"DATEV2"}, returnType = "INT")
    public static IntegerLiteral dayOfWeek(DateV2Literal date) {
        return new IntegerLiteral(date.toJavaDateType().getDayOfWeek().getValue() % 7 + 1);
    }

    @ExecFunction(name = "dayofweek", argTypes = {"DATETIMEV2"}, returnType = "INT")
    public static IntegerLiteral dayOfWeek(DateTimeV2Literal date) {
        return new IntegerLiteral(date.toJavaDateType().getDayOfWeek().getValue() % 7 + 1);
    }

    private static int distanceToFirstDayOfWeek(LocalDateTime dateTime) {
        return dateTime.getDayOfWeek().getValue() - 1;
    }

    private static LocalDateTime firstDayOfWeek(LocalDateTime dateTime) {
        return dateTime.plusDays(-distanceToFirstDayOfWeek(dateTime));
    }

    /**
     * datetime arithmetic function date-format
     */
    @ExecFunction(name = "date_format", argTypes = {"DATE", "VARCHAR"}, returnType = "VARCHAR")
    public static VarcharLiteral dateFormat(DateLiteral date, VarcharLiteral format) {
        return new VarcharLiteral(DateUtils.formatBuilder(format.getValue()).toFormatter().format(
                java.time.LocalDate.of(((int) date.getYear()), ((int) date.getMonth()), ((int) date.getDay()))));
    }

    @ExecFunction(name = "date_format", argTypes = {"DATETIME", "VARCHAR"}, returnType = "VARCHAR")
    public static VarcharLiteral dateFormat(DateTimeLiteral date, VarcharLiteral format) {
        return new VarcharLiteral(DateUtils.formatBuilder(format.getValue()).toFormatter().format(
                java.time.LocalDateTime.of(((int) date.getYear()), ((int) date.getMonth()), ((int) date.getDay()),
                        ((int) date.getHour()), ((int) date.getMinute()), ((int) date.getSecond()))));
    }

    @ExecFunction(name = "date_format", argTypes = {"DATEV2", "VARCHAR"}, returnType = "VARCHAR")
    public static VarcharLiteral dateFormat(DateV2Literal date, VarcharLiteral format) {
        return new VarcharLiteral(DateUtils.formatBuilder(format.getValue()).toFormatter().format(
                java.time.LocalDate.of(((int) date.getYear()), ((int) date.getMonth()), ((int) date.getDay()))));
    }

    @ExecFunction(name = "date_format", argTypes = {"DATETIMEV2", "VARCHAR"}, returnType = "VARCHAR")
    public static VarcharLiteral dateFormat(DateTimeV2Literal date, VarcharLiteral format) {
        return new VarcharLiteral(DateUtils.formatBuilder(format.getValue()).toFormatter().format(
                java.time.LocalDateTime.of(((int) date.getYear()), ((int) date.getMonth()), ((int) date.getDay()),
                        ((int) date.getHour()), ((int) date.getMinute()), ((int) date.getSecond()))));
    }

    /**
     * datetime arithmetic function date
     */
    @ExecFunction(name = "date", argTypes = {"DATETIME"}, returnType = "DATE")
    public static DateLiteral date(DateTimeLiteral dateTime) throws AnalysisException {
        return new DateLiteral(dateTime.getYear(), dateTime.getMonth(), dateTime.getDay());
    }

    @ExecFunction(name = "date", argTypes = {"DATETIMEV2"}, returnType = "DATE")
    public static DateLiteral date(DateTimeV2Literal dateTime) throws AnalysisException {
        return new DateLiteral(dateTime.getYear(), dateTime.getMonth(), dateTime.getDay());
    }

    /**
     * datetime arithmetic function date-trunc
     */
    @ExecFunction(name = "date_trunc", argTypes = {"DATETIME", "VARCHAR"}, returnType = "DATETIME")
    public static DateTimeLiteral dateTrunc(DateTimeLiteral date, VarcharLiteral trunc) {
        return DateTimeLiteral.fromJavaDateType(dateTruncHelper(date.toJavaDateType(), trunc.getValue()));
    }

    @ExecFunction(name = "date_trunc", argTypes = {"DATETIMEV2", "VARCHAR"}, returnType = "DATETIMEV2")
    public static DateTimeV2Literal dateTrunc(DateTimeV2Literal date, VarcharLiteral trunc) {
        return DateTimeV2Literal.fromJavaDateType(dateTruncHelper(date.toJavaDateType(), trunc.getValue()));
    }

    private static LocalDateTime dateTruncHelper(LocalDateTime dateTime, String trunc) {
        int year = dateTime.getYear();
        int month = dateTime.getMonthValue();
        int day = dateTime.getDayOfMonth();
        int hour = dateTime.getHour();
        int minute = dateTime.getMinute();
        int second = dateTime.getSecond();
        switch (trunc.toLowerCase()) {
            case "year":
                month = 0;
            case "quarter": // CHECKSTYLE IGNORE THIS LINE
                month = ((month - 1) / 3) * 3 + 1;
            case "month": // CHECKSTYLE IGNORE THIS LINE
                day = 1;
                break;
            case "week":
                LocalDateTime firstDayOfWeek = firstDayOfWeek(dateTime);
                year = firstDayOfWeek.getYear();
                month = firstDayOfWeek.getMonthValue();
                day = firstDayOfWeek.getDayOfMonth();
            default: // CHECKSTYLE IGNORE THIS LINE
                break;
        }
        switch (trunc.toLowerCase()) {
            case "year":
            case "quarter":
            case "month":
            case "week":
            case "day": // CHECKSTYLE IGNORE THIS LINE
                hour = 0;
            case "hour": // CHECKSTYLE IGNORE THIS LINE
                minute = 0;
            case "minute": // CHECKSTYLE IGNORE THIS LINE
                second = 0;
            default: // CHECKSTYLE IGNORE THIS LINE
        }
        return LocalDateTime.of(year, month, day, hour, minute, second);
    }

    @ExecFunction(name = "from_days", argTypes = {"INT"}, returnType = "DATE")
    public static DateLiteral fromDays(IntegerLiteral n) {
        return DateLiteral.fromJavaDateType(LocalDateTime.of(0, 1, 1, 0, 0, 0).plusDays(n.getValue()));
    }

    @ExecFunction(name = "last_day", argTypes = {"DATE"}, returnType = "DATE")
    public static DateLiteral lastDay(DateLiteral date) {
        LocalDateTime nextMonthFirstDay = LocalDateTime.of((int) date.getYear(), (int) date.getMonth(), 1,
                0, 0, 0).plusMonths(1);
        return DateLiteral.fromJavaDateType(nextMonthFirstDay.minusDays(1));
    }

    @ExecFunction(name = "last_day", argTypes = {"DATETIME"}, returnType = "DATE")
    public static DateLiteral lastDay(DateTimeLiteral date) {
        LocalDateTime nextMonthFirstDay = LocalDateTime.of((int) date.getYear(), (int) date.getMonth(), 1,
                0, 0, 0).plusMonths(1);
        return DateLiteral.fromJavaDateType(nextMonthFirstDay.minusDays(1));
    }

    @ExecFunction(name = "last_day", argTypes = {"DATEV2"}, returnType = "DATEV2")
    public static DateV2Literal lastDay(DateV2Literal date) {
        LocalDateTime nextMonthFirstDay = LocalDateTime.of((int) date.getYear(), (int) date.getMonth(), 1,
                0, 0, 0).plusMonths(1);
        return DateV2Literal.fromJavaDateType(nextMonthFirstDay.minusDays(1));
    }

    @ExecFunction(name = "last_day", argTypes = {"DATETIMEV2"}, returnType = "DATEV2")
    public static DateV2Literal lastDay(DateTimeV2Literal date) {
        LocalDateTime nextMonthFirstDay = LocalDateTime.of((int) date.getYear(), (int) date.getMonth(), 1,
                0, 0, 0).plusMonths(1);
        return DateV2Literal.fromJavaDateType(nextMonthFirstDay.minusDays(1));
    }

    /**
     * datetime transformation function: to_monday
     */
    @ExecFunction(name = "to_monday", argTypes = {"DATE"}, returnType = "DATE")
    public static DateLiteral toMonday(DateLiteral date) {
        return DateLiteral.fromJavaDateType(toMonday(date.toJavaDateType()));
    }

    @ExecFunction(name = "to_monday", argTypes = {"DATETIME"}, returnType = "DATE")
    public static DateLiteral toMonday(DateTimeLiteral date) {
        return DateLiteral.fromJavaDateType(toMonday(date.toJavaDateType()));
    }

    @ExecFunction(name = "to_monday", argTypes = {"DATEV2"}, returnType = "DATEV2")
    public static DateV2Literal toMonday(DateV2Literal date) {
        return DateV2Literal.fromJavaDateType(toMonday(date.toJavaDateType()));
    }

    @ExecFunction(name = "to_monday", argTypes = {"DATETIMEV2"}, returnType = "DATEV2")
    public static DateV2Literal toMonday(DateTimeV2Literal date) {
        return DateV2Literal.fromJavaDateType(toMonday(date.toJavaDateType()));
    }

    private static LocalDateTime toMonday(LocalDateTime dateTime) {
        LocalDateTime specialUpperBound = LocalDateTime.of(1970, 1, 4, 0, 0, 0);
        LocalDateTime specialLowerBound = LocalDateTime.of(1970, 1, 1, 0, 0, 0);
        if (dateTime.isAfter(specialUpperBound) || dateTime.isBefore(specialLowerBound)) {
            return dateTime.plusDays(-dateTime.getDayOfWeek().getValue() + 1);
        }
        return specialLowerBound;
    }

    /**
     * date transformation function: from_unixtime
     */
    @ExecFunction(name = "from_unixtime", argTypes = {"INT"}, returnType = "VARCHAR")
    public static VarcharLiteral fromUnixTime(BigIntLiteral second) {
        if (second.getValue() < 0 || second.getValue() >= 253402271999L) {
            return null;
        }
        return fromUnixTime(second, new VarcharLiteral("%Y-%m-%d %H:%i:%s"));
    }

    /**
     * date transformation function: from_unixtime
     */
    @ExecFunction(name = "from_unixtime", argTypes = {"INT", "VARCHAR"}, returnType = "VARCHAR")
    public static VarcharLiteral fromUnixTime(BigIntLiteral second, VarcharLiteral format) {
        if (second.getValue() < 0 || second.getValue() >= 253402271999L) {
            return null;
        }
        ZonedDateTime dateTime = LocalDateTime.of(1970, 1, 1, 0, 0, 0)
                .plusSeconds(second.getValue())
                .atZone(ZoneId.of("UTC+0"))
                .toOffsetDateTime()
                .atZoneSameInstant(ZoneId.systemDefault());
        return dateFormat(new DateTimeLiteral(dateTime.getYear(), dateTime.getMonthValue(),
                        dateTime.getDayOfMonth(), dateTime.getHour(), dateTime.getMinute(), dateTime.getSecond()),
                format);
    }

    /**
     * date transformation function: unix_timestamp
     */
    @ExecFunction(name = "unix_timestamp", argTypes = {}, returnType = "INT")
    public static IntegerLiteral unixTimestamp() {
        return new IntegerLiteral(getTimestamp(LocalDateTime.now()));
    }

    /**
     * date transformation function: unix_timestamp
     */
    @ExecFunction(name = "unix_timestamp", argTypes = {"VARCHAR"}, returnType = "INT")
    public static IntegerLiteral unixTimestamp(VarcharLiteral date) {
        return new IntegerLiteral(getTimestamp(LocalDateTime.parse(date.getValue(),
                DateUtils.formatBuilder("%Y-%m-%d %H:%i:%s").toFormatter())));
    }

    /**
     * date transformation function: unix_timestamp
     */
    @ExecFunction(name = "unix_timestamp", argTypes = {"VARCHAR", "VARCHAR"}, returnType = "INT")
    public static IntegerLiteral unixTimestamp(VarcharLiteral date, VarcharLiteral format) {
        return new IntegerLiteral(getTimestamp(LocalDateTime.parse(date.getValue(),
                DateUtils.formatBuilder(format.getValue()).toFormatter())));
    }

    private static Integer getTimestamp(LocalDateTime dateTime) {
        LocalDateTime specialUpperBound = LocalDateTime.of(2038, 1, 19, 3, 14, 7);
        LocalDateTime specialLowerBound = LocalDateTime.of(1970, 1, 1, 0, 0, 0);
        if (dateTime.isBefore(specialLowerBound) || dateTime.isAfter(specialUpperBound)) {
            return 0;
        }
        return ((int) Duration.between(specialLowerBound, dateTime).getSeconds());
    }

    /**
     * date transformation function: utc_timestamp
     */
    @ExecFunction(name = "utc_timestamp", argTypes = {}, returnType = "INT")
    public static DateTimeLiteral utcTimestamp() {
        return DateTimeLiteral.fromJavaDateType(LocalDateTime.now());
    }

    /**
     * date transformation function: to_date
     */
    @ExecFunction(name = "to_date", argTypes = {"DATETIME"}, returnType = "DATE")
    public static DateLiteral toDate(DateTimeLiteral date) {
        return new DateLiteral(date.getYear(), date.getMonth(), date.getDay());
    }

    @ExecFunction(name = "to_date", argTypes = {"DATETIMEV2"}, returnType = "DATE")
    public static DateLiteral toDate(DateTimeV2Literal date) {
        return new DateLiteral(date.getYear(), date.getMonth(), date.getDay());
    }

    /**
     * date transformation function: to_days
     */
    @ExecFunction(name = "to_days", argTypes = {"DATETIME"}, returnType = "INT")
    public static IntegerLiteral toDays(DateTimeLiteral date) {
        return new IntegerLiteral(((int) Duration.between(
                LocalDateTime.of(0, 1, 1, 0, 0, 0), date.toJavaDateType()).toDays()));
    }

    @ExecFunction(name = "to_days", argTypes = {"DATETIMEV2"}, returnType = "INT")
    public static IntegerLiteral toDays(DateTimeV2Literal date) {
        return new IntegerLiteral(((int) Duration.between(
                LocalDateTime.of(0, 1, 1, 0, 0, 0), date.toJavaDateType()).toDays()));
    }

    /**
     * date transformation function: makedate
     */
    @ExecFunction(name = "makedate", argTypes = {"INT, INT"}, returnType = "DATE")
    public static DateLiteral makeDate(IntegerLiteral year, IntegerLiteral dayOfYear) {
        return DateLiteral.fromJavaDateType(LocalDateTime.of(year.getValue(), 1, 1, 0, 0, 0)
                .plusDays(dayOfYear.getValue() - 1));
    }

    /**
     * date transformation function: str_to_date
     */
    @ExecFunction(name = "str_to_date", argTypes = {"VARCHAR, VARCHAR"}, returnType = "DATETIME")
    public static DateTimeLiteral strToDate(VarcharLiteral str, VarcharLiteral format) {
        return DateTimeLiteral.fromJavaDateType(DateUtils.getTime(DateUtils.formatBuilder(format.getValue())
                        .toFormatter(), str.getValue()));
    }
}
