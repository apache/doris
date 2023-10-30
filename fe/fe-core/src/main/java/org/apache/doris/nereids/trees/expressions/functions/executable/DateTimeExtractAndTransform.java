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
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.DateV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.util.DateUtils;

import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

/**
 * executable function:
 * year, quarter, month, week, dayOfYear, dayOfweek, dayOfMonth, hour, minute, second, microsecond
 */
public class DateTimeExtractAndTransform {
    /**
     * datetime arithmetic function date-v2
     */
    @ExecFunction(name = "datev2", argTypes = {"DATETIMEV2"}, returnType = "DATEV2")
    public static Expression dateV2(DateTimeV2Literal dateTime) {
        return new DateV2Literal(dateTime.getYear(), dateTime.getMonth(), dateTime.getDay());
    }

    /**
     * Executable datetime extract year
     */
    @ExecFunction(name = "year", argTypes = {"DATE"}, returnType = "INT")
    public static Expression year(DateLiteral date) {
        return new IntegerLiteral(((int) date.getYear()));
    }

    @ExecFunction(name = "year", argTypes = {"DATETIME"}, returnType = "INT")
    public static Expression year(DateTimeLiteral date) {
        return new IntegerLiteral(((int) date.getYear()));
    }

    @ExecFunction(name = "year", argTypes = {"DATEV2"}, returnType = "INT")
    public static Expression year(DateV2Literal date) {
        return new IntegerLiteral(((int) date.getYear()));
    }

    @ExecFunction(name = "year", argTypes = {"DATETIMEV2"}, returnType = "INT")
    public static Expression year(DateTimeV2Literal date) {
        return new IntegerLiteral(((int) date.getYear()));
    }

    /**
     * Executable datetime extract quarter
     */
    @ExecFunction(name = "quarter", argTypes = {"DATE"}, returnType = "INT")
    public static Expression quarter(DateLiteral date) {
        return new IntegerLiteral(((int) date.getMonth() - 1) / 3 + 1);
    }

    @ExecFunction(name = "quarter", argTypes = {"DATETIME"}, returnType = "INT")
    public static Expression quarter(DateTimeLiteral date) {
        return new IntegerLiteral(((int) date.getMonth() - 1) / 3 + 1);
    }

    @ExecFunction(name = "quarter", argTypes = {"DATEV2"}, returnType = "INT")
    public static Expression quarter(DateV2Literal date) {
        return new IntegerLiteral(((int) date.getMonth() - 1) / 3 + 1);
    }

    @ExecFunction(name = "quarter", argTypes = {"DATETIMEV2"}, returnType = "INT")
    public static Expression quarter(DateTimeV2Literal date) {
        return new IntegerLiteral(((int) date.getMonth() - 1) / 3 + 1);
    }

    /**
     * Executable datetime extract month
     */
    @ExecFunction(name = "month", argTypes = {"DATE"}, returnType = "INT")
    public static Expression month(DateLiteral date) {
        return new IntegerLiteral(((int) date.getMonth()));
    }

    @ExecFunction(name = "month", argTypes = {"DATETIME"}, returnType = "INT")
    public static Expression month(DateTimeLiteral date) {
        return new IntegerLiteral(((int) date.getMonth()));
    }

    @ExecFunction(name = "month", argTypes = {"DATEV2"}, returnType = "INT")
    public static Expression month(DateV2Literal date) {
        return new IntegerLiteral(((int) date.getMonth()));
    }

    @ExecFunction(name = "month", argTypes = {"DATETIMEV2"}, returnType = "INT")
    public static Expression month(DateTimeV2Literal date) {
        return new IntegerLiteral(((int) date.getMonth()));
    }

    /**
     * Executable datetime extract day
     */
    @ExecFunction(name = "day", argTypes = {"DATE"}, returnType = "INT")
    public static Expression day(DateLiteral date) {
        return new IntegerLiteral(((int) date.getDay()));
    }

    @ExecFunction(name = "day", argTypes = {"DATETIME"}, returnType = "INT")
    public static Expression day(DateTimeLiteral date) {
        return new IntegerLiteral(((int) date.getDay()));
    }

    @ExecFunction(name = "day", argTypes = {"DATEV2"}, returnType = "INT")
    public static Expression day(DateV2Literal date) {
        return new IntegerLiteral(((int) date.getDay()));
    }

    @ExecFunction(name = "day", argTypes = {"DATETIMEV2"}, returnType = "INT")
    public static Expression day(DateTimeV2Literal date) {
        return new IntegerLiteral(((int) date.getDay()));
    }

    /**
     * Executable datetime extract hour
     */
    @ExecFunction(name = "hour", argTypes = {"DATETIME"}, returnType = "INT")
    public static Expression hour(DateTimeLiteral date) {
        return new IntegerLiteral(((int) date.getHour()));
    }

    @ExecFunction(name = "hour", argTypes = {"DATETIMEV2"}, returnType = "INT")
    public static Expression hour(DateTimeV2Literal date) {
        return new IntegerLiteral(((int) date.getHour()));
    }

    /**
     * Executable datetime extract hour
     */
    @ExecFunction(name = "minute", argTypes = {"DATETIME"}, returnType = "INT")
    public static Expression minute(DateTimeLiteral date) {
        return new IntegerLiteral(((int) date.getMinute()));
    }

    @ExecFunction(name = "minute", argTypes = {"DATETIMEV2"}, returnType = "INT")
    public static Expression minute(DateTimeV2Literal date) {
        return new IntegerLiteral(((int) date.getMinute()));
    }

    /**
     * Executable datetime extract second
     */
    @ExecFunction(name = "second", argTypes = {"DATETIME"}, returnType = "INT")
    public static Expression second(DateTimeLiteral date) {
        return new IntegerLiteral(((int) date.getSecond()));
    }

    @ExecFunction(name = "second", argTypes = {"DATETIMEV2"}, returnType = "INT")
    public static Expression second(DateTimeV2Literal date) {
        return new IntegerLiteral(((int) date.getSecond()));
    }

    /**
     * Executable datetime extract microsecond
     */
    @ExecFunction(name = "microsecond", argTypes = {"DATETIMEV2"}, returnType = "INT")
    public static Expression microsecond(DateTimeV2Literal date) {
        return new IntegerLiteral(((int) date.getMicroSecond()));
    }

    /**
     * Executable datetime extract dayofyear
     */
    @ExecFunction(name = "dayofyear", argTypes = {"DATE"}, returnType = "INT")
    public static Expression dayOfYear(DateLiteral date) {
        return new IntegerLiteral(date.toJavaDateType().getDayOfYear());
    }

    @ExecFunction(name = "dayofyear", argTypes = {"DATETIME"}, returnType = "INT")
    public static Expression dayOfYear(DateTimeLiteral date) {
        return new IntegerLiteral(date.toJavaDateType().getDayOfYear());
    }

    @ExecFunction(name = "dayofyear", argTypes = {"DATEV2"}, returnType = "INT")
    public static Expression dayOfYear(DateV2Literal date) {
        return new IntegerLiteral(date.toJavaDateType().getDayOfYear());
    }

    @ExecFunction(name = "dayofyear", argTypes = {"DATETIMEV2"}, returnType = "INT")
    public static Expression dayOfYear(DateTimeV2Literal date) {
        return new IntegerLiteral(date.toJavaDateType().getDayOfYear());
    }

    /**
     * Executable datetime extract dayofmonth
     */
    @ExecFunction(name = "dayofmonth", argTypes = {"DATE"}, returnType = "INT")
    public static Expression dayOfMonth(DateLiteral date) {
        return new IntegerLiteral(date.toJavaDateType().getDayOfMonth());
    }

    @ExecFunction(name = "dayofmonth", argTypes = {"DATETIME"}, returnType = "INT")
    public static Expression dayOfMonth(DateTimeLiteral date) {
        return new IntegerLiteral(date.toJavaDateType().getDayOfMonth());
    }

    @ExecFunction(name = "dayofmonth", argTypes = {"DATEV2"}, returnType = "INT")
    public static Expression dayOfMonth(DateV2Literal date) {
        return new IntegerLiteral(date.toJavaDateType().getDayOfMonth());
    }

    @ExecFunction(name = "dayofmonth", argTypes = {"DATETIMEV2"}, returnType = "INT")
    public static Expression dayOfMonth(DateTimeV2Literal date) {
        return new IntegerLiteral(date.toJavaDateType().getDayOfMonth());
    }

    /**
     * Executable datetime extract dayofweek
     */
    @ExecFunction(name = "dayofweek", argTypes = {"DATE"}, returnType = "INT")
    public static Expression dayOfWeek(DateLiteral date) {
        return new IntegerLiteral(date.toJavaDateType().getDayOfWeek().getValue() % 7 + 1);
    }

    @ExecFunction(name = "dayofweek", argTypes = {"DATETIME"}, returnType = "INT")
    public static Expression dayOfWeek(DateTimeLiteral date) {
        return new IntegerLiteral(date.toJavaDateType().getDayOfWeek().getValue() % 7 + 1);
    }

    @ExecFunction(name = "dayofweek", argTypes = {"DATEV2"}, returnType = "INT")
    public static Expression dayOfWeek(DateV2Literal date) {
        return new IntegerLiteral(date.toJavaDateType().getDayOfWeek().getValue() % 7 + 1);
    }

    @ExecFunction(name = "dayofweek", argTypes = {"DATETIMEV2"}, returnType = "INT")
    public static Expression dayOfWeek(DateTimeV2Literal date) {
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
    public static Expression dateFormat(DateLiteral date, VarcharLiteral format) {
        return new VarcharLiteral(DateUtils.formatBuilder(format.getValue()).toFormatter().format(
                java.time.LocalDate.of(((int) date.getYear()), ((int) date.getMonth()), ((int) date.getDay()))));
    }

    @ExecFunction(name = "date_format", argTypes = {"DATETIME", "VARCHAR"}, returnType = "VARCHAR")
    public static Expression dateFormat(DateTimeLiteral date, VarcharLiteral format) {
        return new VarcharLiteral(DateUtils.formatBuilder(format.getValue()).toFormatter().format(
                java.time.LocalDateTime.of(((int) date.getYear()), ((int) date.getMonth()), ((int) date.getDay()),
                        ((int) date.getHour()), ((int) date.getMinute()), ((int) date.getSecond()))));
    }

    @ExecFunction(name = "date_format", argTypes = {"DATEV2", "VARCHAR"}, returnType = "VARCHAR")
    public static Expression dateFormat(DateV2Literal date, VarcharLiteral format) {
        return new VarcharLiteral(DateUtils.formatBuilder(format.getValue()).toFormatter().format(
                java.time.LocalDate.of(((int) date.getYear()), ((int) date.getMonth()), ((int) date.getDay()))));
    }

    @ExecFunction(name = "date_format", argTypes = {"DATETIMEV2", "VARCHAR"}, returnType = "VARCHAR")
    public static Expression dateFormat(DateTimeV2Literal date, VarcharLiteral format) {
        return new VarcharLiteral(DateUtils.formatBuilder(format.getValue()).toFormatter().format(
                java.time.LocalDateTime.of(((int) date.getYear()), ((int) date.getMonth()), ((int) date.getDay()),
                        ((int) date.getHour()), ((int) date.getMinute()), ((int) date.getSecond()))));
    }

    /**
     * datetime arithmetic function date
     */
    @ExecFunction(name = "date", argTypes = {"DATETIME"}, returnType = "DATE")
    public static Expression date(DateTimeLiteral dateTime) throws AnalysisException {
        return new DateLiteral(dateTime.getYear(), dateTime.getMonth(), dateTime.getDay());
    }

    @ExecFunction(name = "date", argTypes = {"DATETIMEV2"}, returnType = "DATEV2")
    public static Expression date(DateTimeV2Literal dateTime) throws AnalysisException {
        return new DateV2Literal(dateTime.getYear(), dateTime.getMonth(), dateTime.getDay());
    }

    /**
     * datetime arithmetic function date-trunc
     */
    @ExecFunction(name = "date_trunc", argTypes = {"DATETIME", "VARCHAR"}, returnType = "DATETIME")
    public static Expression dateTrunc(DateTimeLiteral date, VarcharLiteral trunc) {
        return DateTimeLiteral.fromJavaDateType(dateTruncHelper(date.toJavaDateType(), trunc.getValue()));
    }

    @ExecFunction(name = "date_trunc", argTypes = {"DATETIMEV2", "VARCHAR"}, returnType = "DATETIMEV2")
    public static Expression dateTrunc(DateTimeV2Literal date, VarcharLiteral trunc) {
        return DateTimeV2Literal.fromJavaDateType(dateTruncHelper(date.toJavaDateType(), trunc.getValue()));
    }

    @ExecFunction(name = "date_trunc", argTypes = { "DATE", "VARCHAR" }, returnType = "DATE")
    public static Expression dateTrunc(DateLiteral date, VarcharLiteral trunc) {
        return DateLiteral.fromJavaDateType(dateTruncHelper(date.toJavaDateType(), trunc.getValue()));
    }

    @ExecFunction(name = "date_trunc", argTypes = { "DATEV2", "VARCHAR" }, returnType = "DATEV2")
    public static Expression dateTrunc(DateV2Literal date, VarcharLiteral trunc) {
        return DateV2Literal.fromJavaDateType(dateTruncHelper(date.toJavaDateType(), trunc.getValue()));
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

    /**
     * from_days.
     */
    @ExecFunction(name = "from_days", argTypes = {"INT"}, returnType = "DATE")
    public static Expression fromDays(IntegerLiteral n) {
        // doris treat 0000AD as ordinary year but java LocalDateTime treat it as lunar year.
        LocalDateTime res = LocalDateTime.of(0, 1, 1, 0, 0, 0)
                .plusDays(n.getValue());
        if (res.isBefore(LocalDateTime.of(0, 3, 1, 0, 0, 0))) {
            res = res.plusDays(-1);
        }
        return DateLiteral.fromJavaDateType(res);
    }

    @ExecFunction(name = "last_day", argTypes = {"DATE"}, returnType = "DATE")
    public static Expression lastDay(DateLiteral date) {
        LocalDateTime nextMonthFirstDay = LocalDateTime.of((int) date.getYear(), (int) date.getMonth(), 1,
                0, 0, 0).plusMonths(1);
        return DateLiteral.fromJavaDateType(nextMonthFirstDay.minusDays(1));
    }

    @ExecFunction(name = "last_day", argTypes = {"DATETIME"}, returnType = "DATE")
    public static Expression lastDay(DateTimeLiteral date) {
        LocalDateTime nextMonthFirstDay = LocalDateTime.of((int) date.getYear(), (int) date.getMonth(), 1,
                0, 0, 0).plusMonths(1);
        return DateLiteral.fromJavaDateType(nextMonthFirstDay.minusDays(1));
    }

    @ExecFunction(name = "last_day", argTypes = {"DATEV2"}, returnType = "DATEV2")
    public static Expression lastDay(DateV2Literal date) {
        LocalDateTime nextMonthFirstDay = LocalDateTime.of((int) date.getYear(), (int) date.getMonth(), 1,
                0, 0, 0).plusMonths(1);
        return DateV2Literal.fromJavaDateType(nextMonthFirstDay.minusDays(1));
    }

    @ExecFunction(name = "last_day", argTypes = {"DATETIMEV2"}, returnType = "DATEV2")
    public static Expression lastDay(DateTimeV2Literal date) {
        LocalDateTime nextMonthFirstDay = LocalDateTime.of((int) date.getYear(), (int) date.getMonth(), 1,
                0, 0, 0).plusMonths(1);
        return DateV2Literal.fromJavaDateType(nextMonthFirstDay.minusDays(1));
    }

    /**
     * datetime transformation function: to_monday
     */
    @ExecFunction(name = "to_monday", argTypes = {"DATE"}, returnType = "DATE")
    public static Expression toMonday(DateLiteral date) {
        return DateLiteral.fromJavaDateType(toMonday(date.toJavaDateType()));
    }

    @ExecFunction(name = "to_monday", argTypes = {"DATETIME"}, returnType = "DATE")
    public static Expression toMonday(DateTimeLiteral date) {
        return DateLiteral.fromJavaDateType(toMonday(date.toJavaDateType()));
    }

    @ExecFunction(name = "to_monday", argTypes = {"DATEV2"}, returnType = "DATEV2")
    public static Expression toMonday(DateV2Literal date) {
        return DateV2Literal.fromJavaDateType(toMonday(date.toJavaDateType()));
    }

    @ExecFunction(name = "to_monday", argTypes = {"DATETIMEV2"}, returnType = "DATEV2")
    public static Expression toMonday(DateTimeV2Literal date) {
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
    @ExecFunction(name = "from_unixtime", argTypes = {"BIGINT"}, returnType = "VARCHAR")
    public static Expression fromUnixTime(BigIntLiteral second) {
        // 32536771199L is max valid timestamp of mysql from_unix_time
        if (second.getValue() < 0 || second.getValue() > 32536771199L) {
            return null;
        }
        return fromUnixTime(second, new VarcharLiteral("%Y-%m-%d %H:%i:%s"));
    }

    /**
     * date transformation function: from_unixtime
     */
    @ExecFunction(name = "from_unixtime", argTypes = {"BIGINT", "VARCHAR"}, returnType = "VARCHAR")
    public static Expression fromUnixTime(BigIntLiteral second, VarcharLiteral format) {
        if (second.getValue() < 0 || second.getValue() > 32536771199L) {
            return null;
        }

        ZonedDateTime dateTime = LocalDateTime.of(1970, 1, 1, 0, 0, 0)
                .plusSeconds(second.getValue())
                .atZone(ZoneId.of("UTC+0"))
                .toOffsetDateTime()
                .atZoneSameInstant(DateUtils.getTimeZone());
        return dateFormat(new DateTimeLiteral(dateTime.getYear(), dateTime.getMonthValue(),
                        dateTime.getDayOfMonth(), dateTime.getHour(), dateTime.getMinute(), dateTime.getSecond()),
                format);
    }

    /**
     * date transformation function: unix_timestamp
     */
    @ExecFunction(name = "unix_timestamp", argTypes = {"DATE"}, returnType = "INT")
    public static Expression unixTimestamp(DateLiteral date) {
        return new IntegerLiteral(getTimestamp(date.toJavaDateType()));
    }

    @ExecFunction(name = "unix_timestamp", argTypes = {"DATETIME"}, returnType = "INT")
    public static Expression unixTimestamp(DateTimeLiteral date) {
        return new IntegerLiteral(getTimestamp(date.toJavaDateType()));
    }

    @ExecFunction(name = "unix_timestamp", argTypes = {"DATEV2"}, returnType = "INT")
    public static Expression unixTimestamp(DateV2Literal date) {
        return new IntegerLiteral(getTimestamp(date.toJavaDateType()));
    }

    @ExecFunction(name = "unix_timestamp", argTypes = {"DATETIMEV2"}, returnType = "INT")
    public static Expression unixTimestamp(DateTimeV2Literal date) {
        return new IntegerLiteral(getTimestamp(date.toJavaDateType()));
    }

    /**
     * date transformation function: unix_timestamp
     */
    @ExecFunction(name = "unix_timestamp", argTypes = {"VARCHAR", "VARCHAR"}, returnType = "INT")
    public static Expression unixTimestamp(VarcharLiteral date, VarcharLiteral format) {
        DateTimeFormatter formatter = DateUtils.formatBuilder(format.getValue()).toFormatter();
        LocalDateTime dateObj;
        try {
            dateObj = LocalDateTime.parse(date.getValue(), formatter);
        } catch (DateTimeParseException e) {
            // means the date string doesn't contain time fields.
            dateObj = LocalDate.parse(date.getValue(), formatter).atStartOfDay();
        }
        return new IntegerLiteral(getTimestamp(dateObj));
    }

    private static Integer getTimestamp(LocalDateTime dateTime) {
        LocalDateTime specialUpperBound = LocalDateTime.of(2038, 1, 19, 3, 14, 7);
        LocalDateTime specialLowerBound = LocalDateTime.of(1970, 1, 1, 0, 0, 0);
        if (dateTime.isBefore(specialLowerBound) || dateTime.isAfter(specialUpperBound)) {
            return 0;
        }
        return ((int) Duration.between(
                specialLowerBound,
                dateTime.atZone(DateUtils.getTimeZone())
                        .toOffsetDateTime().atZoneSameInstant(ZoneId.of("UTC+0"))
                        .toLocalDateTime()).getSeconds());
    }

    /**
     * date transformation function: to_date
     */
    @ExecFunction(name = "to_date", argTypes = {"DATETIME"}, returnType = "DATE")
    public static Expression toDate(DateTimeLiteral date) {
        return new DateLiteral(date.getYear(), date.getMonth(), date.getDay());
    }

    @ExecFunction(name = "to_date", argTypes = {"DATETIMEV2"}, returnType = "DATEV2")
    public static Expression toDate(DateTimeV2Literal date) {
        return new DateV2Literal(date.getYear(), date.getMonth(), date.getDay());
    }

    /**
     * date transformation function: to_days
     */
    @ExecFunction(name = "to_days", argTypes = {"DATE"}, returnType = "INT")
    public static Expression toDays(DateLiteral date) {
        return new IntegerLiteral(((int) Duration.between(
                LocalDateTime.of(0, 1, 1, 0, 0, 0), date.toJavaDateType()).toDays()));
    }

    @ExecFunction(name = "to_days", argTypes = {"DATETIME"}, returnType = "INT")
    public static Expression toDays(DateTimeLiteral date) {
        return new IntegerLiteral(((int) Duration.between(
                LocalDateTime.of(0, 1, 1, 0, 0, 0), date.toJavaDateType()).toDays()));
    }

    @ExecFunction(name = "to_days", argTypes = {"DATEV2"}, returnType = "INT")
    public static Expression toDays(DateV2Literal date) {
        return new IntegerLiteral(((int) Duration.between(
                LocalDateTime.of(0, 1, 1, 0, 0, 0), date.toJavaDateType()).toDays()));
    }

    @ExecFunction(name = "to_days", argTypes = {"DATETIMEV2"}, returnType = "INT")
    public static Expression toDays(DateTimeV2Literal date) {
        return new IntegerLiteral(((int) Duration.between(
                LocalDateTime.of(0, 1, 1, 0, 0, 0), date.toJavaDateType()).toDays()));
    }

    /**
     * date transformation function: makedate
     */
    @ExecFunction(name = "makedate", argTypes = {"INT", "INT"}, returnType = "DATE")
    public static Expression makeDate(IntegerLiteral year, IntegerLiteral dayOfYear) {
        return DateLiteral.fromJavaDateType(LocalDateTime.of(year.getValue(), 1, 1, 0, 0, 0)
                .plusDays(dayOfYear.getValue() - 1));
    }

    /**
     * date transformation function: str_to_date
     */
    @ExecFunction(name = "str_to_date", argTypes = {"VARCHAR, VARCHAR"}, returnType = "DATETIME")
    public static Expression strToDate(VarcharLiteral str, VarcharLiteral format) {
        return DateTimeLiteral.fromJavaDateType(DateUtils.getTime(DateUtils.formatBuilder(format.getValue())
                        .toFormatter(), str.getValue()));
    }

    @ExecFunction(name = "timestamp", argTypes = {"DATETIME"}, returnType = "DATETIME")
    public static Expression timestamp(DateTimeLiteral datetime) {
        return datetime;
    }

    @ExecFunction(name = "timestamp", argTypes = {"DATETIMEV2"}, returnType = "DATETIMEV2")
    public static Expression timestamp(DateTimeV2Literal datetime) {
        return datetime;
    }
}
