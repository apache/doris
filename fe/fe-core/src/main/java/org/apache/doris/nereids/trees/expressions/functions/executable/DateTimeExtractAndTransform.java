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

import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.ExecFunction;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.DateV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.DecimalV3Literal;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.SmallIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLikeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.DateType;
import org.apache.doris.nereids.types.DateV2Type;
import org.apache.doris.nereids.types.DecimalV3Type;
import org.apache.doris.nereids.types.VarcharType;
import org.apache.doris.nereids.util.DateUtils;

import java.math.BigDecimal;
import java.time.DayOfWeek;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.format.ResolverStyle;
import java.time.format.TextStyle;
import java.time.temporal.ChronoUnit;
import java.time.temporal.WeekFields;
import java.util.Locale;

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
    @ExecFunction(name = "year", argTypes = {"DATE"}, returnType = "SMALLINT")
    public static Expression year(DateLiteral date) {
        return new SmallIntLiteral(((short) date.getYear()));
    }

    @ExecFunction(name = "year", argTypes = {"DATETIME"}, returnType = "SMALLINT")
    public static Expression year(DateTimeLiteral date) {
        return new SmallIntLiteral(((short) date.getYear()));
    }

    @ExecFunction(name = "year", argTypes = {"DATEV2"}, returnType = "SMALLINT")
    public static Expression year(DateV2Literal date) {
        return new SmallIntLiteral(((short) date.getYear()));
    }

    @ExecFunction(name = "year", argTypes = {"DATETIMEV2"}, returnType = "SMALLINT")
    public static Expression year(DateTimeV2Literal date) {
        return new SmallIntLiteral(((short) date.getYear()));
    }

    /**
     * Executable datetime extract quarter
     */
    @ExecFunction(name = "quarter", argTypes = {"DATE"}, returnType = "TINYINT")
    public static Expression quarter(DateLiteral date) {
        return new TinyIntLiteral((byte) (((byte) date.getMonth() - 1) / 3 + 1));
    }

    @ExecFunction(name = "quarter", argTypes = {"DATETIME"}, returnType = "TINYINT")
    public static Expression quarter(DateTimeLiteral date) {
        return new TinyIntLiteral((byte) ((date.getMonth() - 1) / 3 + 1));
    }

    @ExecFunction(name = "quarter", argTypes = {"DATEV2"}, returnType = "TINYINT")
    public static Expression quarter(DateV2Literal date) {
        return new TinyIntLiteral((byte) ((date.getMonth() - 1) / 3 + 1));
    }

    @ExecFunction(name = "quarter", argTypes = {"DATETIMEV2"}, returnType = "TINYINT")
    public static Expression quarter(DateTimeV2Literal date) {
        return new TinyIntLiteral((byte) ((date.getMonth() - 1) / 3 + 1));
    }

    /**
     * Executable datetime extract month
     */
    @ExecFunction(name = "month", argTypes = {"DATE"}, returnType = "TINYINT")
    public static Expression month(DateLiteral date) {
        return new TinyIntLiteral((byte) date.getMonth());
    }

    @ExecFunction(name = "month", argTypes = {"DATETIME"}, returnType = "TINYINT")
    public static Expression month(DateTimeLiteral date) {
        return new TinyIntLiteral((byte) date.getMonth());
    }

    @ExecFunction(name = "month", argTypes = {"DATEV2"}, returnType = "TINYINT")
    public static Expression month(DateV2Literal date) {
        return new TinyIntLiteral((byte) date.getMonth());
    }

    @ExecFunction(name = "month", argTypes = {"DATETIMEV2"}, returnType = "TINYINT")
    public static Expression month(DateTimeV2Literal date) {
        return new TinyIntLiteral((byte) date.getMonth());
    }

    /**
     * Executable datetime extract day
     */
    @ExecFunction(name = "day", argTypes = {"DATE"}, returnType = "TINYINT")
    public static Expression day(DateLiteral date) {
        return new TinyIntLiteral((byte) date.getDay());
    }

    @ExecFunction(name = "day", argTypes = {"DATETIME"}, returnType = "TINYINT")
    public static Expression day(DateTimeLiteral date) {
        return new TinyIntLiteral((byte) date.getDay());
    }

    @ExecFunction(name = "day", argTypes = {"DATEV2"}, returnType = "TINYINT")
    public static Expression day(DateV2Literal date) {
        return new TinyIntLiteral((byte) date.getDay());
    }

    @ExecFunction(name = "day", argTypes = {"DATETIMEV2"}, returnType = "TINYINT")
    public static Expression day(DateTimeV2Literal date) {
        return new TinyIntLiteral((byte) date.getDay());
    }

    /**
     * Executable datetime extract hour
     */
    @ExecFunction(name = "hour", argTypes = {"DATETIME"}, returnType = "TINYINT")
    public static Expression hour(DateTimeLiteral date) {
        return new TinyIntLiteral(((byte) date.getHour()));
    }

    @ExecFunction(name = "hour", argTypes = {"DATETIMEV2"}, returnType = "TINYINT")
    public static Expression hour(DateTimeV2Literal date) {
        return new TinyIntLiteral(((byte) date.getHour()));
    }

    /**
     * Executable datetime extract hour
     */
    @ExecFunction(name = "minute", argTypes = {"DATETIME"}, returnType = "TINYINT")
    public static Expression minute(DateTimeLiteral date) {
        return new TinyIntLiteral(((byte) date.getMinute()));
    }

    @ExecFunction(name = "minute", argTypes = {"DATETIMEV2"}, returnType = "TINYINT")
    public static Expression minute(DateTimeV2Literal date) {
        return new TinyIntLiteral(((byte) date.getMinute()));
    }

    /**
     * Executable datetime extract second
     */
    @ExecFunction(name = "second", argTypes = {"DATETIME"}, returnType = "TINYINT")
    public static Expression second(DateTimeLiteral date) {
        return new TinyIntLiteral(((byte) date.getSecond()));
    }

    @ExecFunction(name = "second", argTypes = {"DATETIMEV2"}, returnType = "TINYINT")
    public static Expression second(DateTimeV2Literal date) {
        return new TinyIntLiteral(((byte) date.getSecond()));
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
    @ExecFunction(name = "dayofyear", argTypes = {"DATE"}, returnType = "SMALLINT")
    public static Expression dayOfYear(DateLiteral date) {
        return new SmallIntLiteral((short) date.getDayOfYear());
    }

    @ExecFunction(name = "dayofyear", argTypes = {"DATETIME"}, returnType = "SMALLINT")
    public static Expression dayOfYear(DateTimeLiteral date) {
        return new SmallIntLiteral((short) date.getDayOfYear());
    }

    @ExecFunction(name = "dayofyear", argTypes = {"DATEV2"}, returnType = "SMALLINT")
    public static Expression dayOfYear(DateV2Literal date) {
        return new SmallIntLiteral((short) date.getDayOfYear());
    }

    @ExecFunction(name = "dayofyear", argTypes = {"DATETIMEV2"}, returnType = "SMALLINT")
    public static Expression dayOfYear(DateTimeV2Literal date) {
        return new SmallIntLiteral((short) date.getDayOfYear());
    }

    /**
     * Executable datetime extract dayofmonth
     */
    @ExecFunction(name = "dayofmonth", argTypes = {"DATE"}, returnType = "TINYINT")
    public static Expression dayOfMonth(DateLiteral date) {
        return new TinyIntLiteral((byte) date.toJavaDateType().getDayOfMonth());
    }

    @ExecFunction(name = "dayofmonth", argTypes = {"DATETIME"}, returnType = "TINYINT")
    public static Expression dayOfMonth(DateTimeLiteral date) {
        return new TinyIntLiteral((byte) date.toJavaDateType().getDayOfMonth());
    }

    @ExecFunction(name = "dayofmonth", argTypes = {"DATEV2"}, returnType = "TINYINT")
    public static Expression dayOfMonth(DateV2Literal date) {
        return new TinyIntLiteral((byte) date.toJavaDateType().getDayOfMonth());
    }

    @ExecFunction(name = "dayofmonth", argTypes = {"DATETIMEV2"}, returnType = "TINYINT")
    public static Expression dayOfMonth(DateTimeV2Literal date) {
        return new TinyIntLiteral((byte) date.toJavaDateType().getDayOfMonth());
    }

    /**
     * Executable datetime extract dayofweek
     */
    @ExecFunction(name = "dayofweek", argTypes = {"DATE"}, returnType = "TINYINT")
    public static Expression dayOfWeek(DateLiteral date) {
        return new TinyIntLiteral((byte) (date.getDayOfWeek() % 7 + 1));
    }

    @ExecFunction(name = "dayofweek", argTypes = {"DATETIME"}, returnType = "TINYINT")
    public static Expression dayOfWeek(DateTimeLiteral date) {
        return new TinyIntLiteral((byte) (date.getDayOfWeek() % 7 + 1));
    }

    @ExecFunction(name = "dayofweek", argTypes = {"DATEV2"}, returnType = "TINYINT")
    public static Expression dayOfWeek(DateV2Literal date) {
        return new TinyIntLiteral((byte) (date.getDayOfWeek() % 7 + 1));
    }

    @ExecFunction(name = "dayofweek", argTypes = {"DATETIMEV2"}, returnType = "TINYINT")
    public static Expression dayOfWeek(DateTimeV2Literal date) {
        return new TinyIntLiteral((byte) (date.getDayOfWeek() % 7 + 1));
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
    public static Expression dateFormat(DateLiteral date, StringLikeLiteral format) {
        return new VarcharLiteral(DateUtils.formatBuilder(format.getValue()).toFormatter().format(
                java.time.LocalDate.of(((int) date.getYear()), ((int) date.getMonth()), ((int) date.getDay()))));
    }

    @ExecFunction(name = "date_format", argTypes = {"DATETIME", "VARCHAR"}, returnType = "VARCHAR")
    public static Expression dateFormat(DateTimeLiteral date, StringLikeLiteral format) {
        return new VarcharLiteral(DateUtils.formatBuilder(format.getValue()).toFormatter().format(
                java.time.LocalDateTime.of(((int) date.getYear()), ((int) date.getMonth()), ((int) date.getDay()),
                        ((int) date.getHour()), ((int) date.getMinute()), ((int) date.getSecond()))));
    }

    @ExecFunction(name = "date_format", argTypes = {"DATEV2", "VARCHAR"}, returnType = "VARCHAR")
    public static Expression dateFormat(DateV2Literal date, StringLikeLiteral format) {
        return new VarcharLiteral(DateUtils.formatBuilder(format.getValue()).toFormatter().format(
                java.time.LocalDate.of(((int) date.getYear()), ((int) date.getMonth()), ((int) date.getDay()))));
    }

    @ExecFunction(name = "date_format", argTypes = {"DATETIMEV2", "VARCHAR"}, returnType = "VARCHAR")
    public static Expression dateFormat(DateTimeV2Literal date, StringLikeLiteral format) {
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
    public static Expression dateTrunc(DateTimeLiteral date, StringLikeLiteral trunc) {
        return DateTimeLiteral.fromJavaDateType(dateTruncHelper(date.toJavaDateType(), trunc.getValue()));
    }

    @ExecFunction(name = "date_trunc", argTypes = {"DATETIMEV2", "VARCHAR"}, returnType = "DATETIMEV2")
    public static Expression dateTrunc(DateTimeV2Literal date, StringLikeLiteral trunc) {
        return DateTimeV2Literal.fromJavaDateType(dateTruncHelper(date.toJavaDateType(), trunc.getValue()));
    }

    @ExecFunction(name = "date_trunc", argTypes = {"DATE", "VARCHAR"}, returnType = "DATE")
    public static Expression dateTrunc(DateLiteral date, StringLikeLiteral trunc) {
        return DateLiteral.fromJavaDateType(dateTruncHelper(date.toJavaDateType(), trunc.getValue()));
    }

    @ExecFunction(name = "date_trunc", argTypes = {"DATEV2", "VARCHAR"}, returnType = "DATEV2")
    public static Expression dateTrunc(DateV2Literal date, StringLikeLiteral trunc) {
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
    @ExecFunction(name = "from_days", argTypes = {"INT"}, returnType = "DATEV2")
    public static Expression fromDays(IntegerLiteral n) {
        // doris treat 0000AD as ordinary year but java LocalDateTime treat it as lunar year.
        LocalDateTime res = LocalDateTime.of(0, 1, 1, 0, 0, 0)
                .plusDays(n.getValue());
        if (res.isBefore(LocalDateTime.of(0, 3, 1, 0, 0, 0))) {
            res = res.plusDays(-1);
        }
        return DateV2Literal.fromJavaDateType(res);
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
        return fromUnixTime(second, new VarcharLiteral("%Y-%m-%d %H:%i:%s"));
    }

    /**
     * date transformation function: from_unixtime
     */
    @ExecFunction(name = "from_unixtime", argTypes = {"BIGINT", "VARCHAR"}, returnType = "VARCHAR")
    public static Expression fromUnixTime(BigIntLiteral second, StringLikeLiteral format) {
        // 32536771199L is max valid timestamp of mysql from_unix_time
        if (second.getValue() < 0 || second.getValue() > 32536771199L) {
            return new NullLiteral(VarcharType.SYSTEM_DEFAULT);
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
        return new IntegerLiteral(Integer.parseInt(getTimestamp(date.toJavaDateType())));
    }

    @ExecFunction(name = "unix_timestamp", argTypes = {"DATETIME"}, returnType = "INT")
    public static Expression unixTimestamp(DateTimeLiteral date) {
        return new IntegerLiteral(Integer.parseInt(getTimestamp(date.toJavaDateType())));
    }

    @ExecFunction(name = "unix_timestamp", argTypes = {"DATEV2"}, returnType = "INT")
    public static Expression unixTimestamp(DateV2Literal date) {
        return new IntegerLiteral(Integer.parseInt(getTimestamp(date.toJavaDateType())));
    }

    /**
     * date transformation function: unix_timestamp
     */
    @ExecFunction(name = "unix_timestamp", argTypes = {"DATETIMEV2"}, returnType = "DECIMALV3")
    public static Expression unixTimestamp(DateTimeV2Literal date) {
        if (date.getMicroSecond() == 0) {
            return new DecimalV3Literal(DecimalV3Type.createDecimalV3TypeLooseCheck(10, 0),
                    new BigDecimal(getTimestamp(date.toJavaDateType())));
        }
        int scale = date.getDataType().getScale();
        return new DecimalV3Literal(DecimalV3Type.createDecimalV3TypeLooseCheck(10 + scale, scale),
                new BigDecimal(getTimestamp(date.toJavaDateType())));
    }

    /**
     * date transformation function: unix_timestamp
     */
    @ExecFunction(name = "unix_timestamp", argTypes = {"VARCHAR", "VARCHAR"}, returnType = "DECIMALV3")
    public static Expression unixTimestamp(StringLikeLiteral date, StringLikeLiteral format) {
        DateTimeFormatter formatter = DateUtils.formatBuilder(format.getValue()).toFormatter();
        LocalDateTime dateObj;
        try {
            dateObj = LocalDateTime.parse(date.getValue(), formatter);
        } catch (DateTimeParseException e) {
            // means the date string doesn't contain time fields.
            dateObj = LocalDate.parse(date.getValue(), formatter).atStartOfDay();
        }
        return new DecimalV3Literal(DecimalV3Type.createDecimalV3TypeLooseCheck(16, 6),
                new BigDecimal(getTimestamp(dateObj)));
    }

    private static String getTimestamp(LocalDateTime dateTime) {
        LocalDateTime specialUpperBound = LocalDateTime.of(2038, 1, 19, 3, 14, 7);
        LocalDateTime specialLowerBound = LocalDateTime.of(1970, 1, 1, 0, 0, 0);
        if (dateTime.isBefore(specialLowerBound) || dateTime.isAfter(specialUpperBound)) {
            return "0";
        }
        Duration duration = Duration.between(
                specialLowerBound,
                dateTime.atZone(DateUtils.getTimeZone())
                        .toOffsetDateTime().atZoneSameInstant(ZoneId.of("UTC+0"))
                        .toLocalDateTime());
        if (duration.getNano() == 0) {
            return String.valueOf(duration.getSeconds());
        } else {
            return duration.getSeconds() + "." + (duration.getNano() / 1000);
        }
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
        int day = dayOfYear.getValue();
        return day > 0 ? DateLiteral.fromJavaDateType(LocalDateTime.of(year.getValue(), 1, 1, 0, 0, 0)
                .plusDays(day - 1)) : new NullLiteral(DateType.INSTANCE);
    }

    /**
     * date transformation function: str_to_date
     */
    @ExecFunction(name = "str_to_date", argTypes = {"VARCHAR", "VARCHAR"}, returnType = "DATETIMEV2")
    public static Expression strToDate(StringLikeLiteral str, StringLikeLiteral format) {
        if (org.apache.doris.analysis.DateLiteral.hasTimePart(format.getStringValue())) {
            DataType returnType = DataType.fromCatalogType(ScalarType.getDefaultDateType(Type.DATETIME));
            if (returnType instanceof DateTimeV2Type) {
                return DateTimeV2Literal.fromJavaDateType(DateUtils.getTime(DateUtils.formatBuilder(format.getValue())
                        .toFormatter(), str.getValue()));
            } else {
                return DateTimeLiteral.fromJavaDateType(DateUtils.getTime(DateUtils.formatBuilder(format.getValue())
                        .toFormatter(), str.getValue()));
            }
        } else {
            DataType returnType = DataType.fromCatalogType(ScalarType.getDefaultDateType(Type.DATE));
            if (returnType instanceof DateV2Type) {
                return DateV2Literal.fromJavaDateType(DateUtils.getTime(DateUtils.formatBuilder(format.getValue())
                        .toFormatter(), str.getValue()));
            } else {
                return DateLiteral.fromJavaDateType(DateUtils.getTime(DateUtils.formatBuilder(format.getValue())
                        .toFormatter(), str.getValue()));
            }
        }
    }

    @ExecFunction(name = "timestamp", argTypes = {"DATETIME"}, returnType = "DATETIME")
    public static Expression timestamp(DateTimeLiteral datetime) {
        return datetime;
    }

    @ExecFunction(name = "timestamp", argTypes = {"DATETIMEV2"}, returnType = "DATETIMEV2")
    public static Expression timestamp(DateTimeV2Literal datetime) {
        return datetime;
    }

    /**
     * convert_tz
     */
    @ExecFunction(name = "convert_tz", argTypes = {"DATETIMEV2", "VARCHAR", "VARCHAR"}, returnType = "DATETIMEV2")
    public static Expression convertTz(DateTimeV2Literal datetime, StringLikeLiteral fromTz, StringLikeLiteral toTz) {
        DateTimeFormatter zoneFormatter = new DateTimeFormatterBuilder()
                .parseCaseInsensitive()
                .appendZoneOrOffsetId()
                .toFormatter()
                .withResolverStyle(ResolverStyle.STRICT);
        ZoneId fromZone = ZoneId.from(zoneFormatter.parse(fromTz.getStringValue()));
        ZoneId toZone = ZoneId.from(zoneFormatter.parse(toTz.getStringValue()));

        LocalDateTime localDateTime = datetime.toJavaDateType();
        ZonedDateTime resultDateTime = localDateTime.atZone(fromZone).withZoneSameInstant(toZone);
        return DateTimeV2Literal.fromJavaDateType(resultDateTime.toLocalDateTime(), datetime.getDataType().getScale());
    }

    @ExecFunction(name = "weekday", argTypes = {"DATE"}, returnType = "TINYINT")
    public static Expression weekDay(DateLiteral date) {
        return new TinyIntLiteral((byte) ((date.toJavaDateType().getDayOfWeek().getValue() + 6) % 7));
    }

    @ExecFunction(name = "weekday", argTypes = {"DATETIME"}, returnType = "TINYINT")
    public static Expression weekDay(DateTimeLiteral date) {
        return new TinyIntLiteral((byte) ((date.toJavaDateType().getDayOfWeek().getValue() + 6) % 7));
    }

    @ExecFunction(name = "weekday", argTypes = {"DATEV2"}, returnType = "TINYINT")
    public static Expression weekDay(DateV2Literal date) {
        return new TinyIntLiteral((byte) ((date.toJavaDateType().getDayOfWeek().getValue() + 6) % 7));
    }

    @ExecFunction(name = "weekday", argTypes = {"DATETIMEV2"}, returnType = "TINYINT")
    public static Expression weekDay(DateTimeV2Literal date) {
        return new TinyIntLiteral((byte) ((date.toJavaDateType().getDayOfWeek().getValue() + 6) % 7));
    }

    @ExecFunction(name = "week", argTypes = {"DATETIMEV2"}, returnType = "TINYINT")
    public static Expression week(DateTimeV2Literal dateTime) {
        return week(dateTime.toJavaDateType(), 0);
    }

    @ExecFunction(name = "week", argTypes = {"DATETIMEV2", "INT"}, returnType = "TINYINT")
    public static Expression week(DateTimeV2Literal dateTime, IntegerLiteral mode) {
        return week(dateTime.toJavaDateType(), mode.getIntValue());
    }

    @ExecFunction(name = "week", argTypes = {"DATETIME"}, returnType = "TINYINT")
    public static Expression week(DateTimeLiteral dateTime) {
        return week(dateTime.toJavaDateType(), 0);
    }

    @ExecFunction(name = "week", argTypes = {"DATETIME", "INT"}, returnType = "TINYINT")
    public static Expression week(DateTimeLiteral dateTime, IntegerLiteral mode) {
        return week(dateTime.toJavaDateType(), mode.getIntValue());
    }

    @ExecFunction(name = "week", argTypes = {"DATEV2"}, returnType = "TINYINT")
    public static Expression week(DateV2Literal date) {
        return week(date.toJavaDateType(), 0);
    }

    @ExecFunction(name = "week", argTypes = {"DATEV2", "INT"}, returnType = "TINYINT")
    public static Expression week(DateV2Literal date, IntegerLiteral mode) {
        return week(date.toJavaDateType(), mode.getIntValue());
    }

    /**
     * the impl of function week(date/datetime, mode)
     */
    public static Expression week(LocalDateTime localDateTime, int mode) {
        final byte[] resultOfFirstDayBC1 = new byte[] { 1, 0, 1, 52, 1, 0, 1, 52 };
        if (isSpecificDate(localDateTime) && mode >= 0 && mode <= 7) { // 0000-01-01/02
            if (localDateTime.getDayOfMonth() == 1) {
                return new TinyIntLiteral(resultOfFirstDayBC1[mode]);
            } else { // 0001-01-02
                return new TinyIntLiteral((byte) 1);
            }
        }

        switch (mode) {
            case 0: {
                return new TinyIntLiteral(
                        (byte) localDateTime.get(WeekFields.of(DayOfWeek.SUNDAY, 7).weekOfYear()));
            }
            case 1: {
                return new TinyIntLiteral((byte) localDateTime.get(WeekFields.ISO.weekOfYear()));
            }
            case 2: {
                return new TinyIntLiteral(
                        (byte) localDateTime.get(WeekFields.of(DayOfWeek.SUNDAY, 7).weekOfWeekBasedYear()));
            }
            case 3: {
                return new TinyIntLiteral(
                        (byte) localDateTime.get(WeekFields.ISO.weekOfWeekBasedYear()));
            }
            case 4: {
                return new TinyIntLiteral((byte) localDateTime
                        .get(WeekFields.of(DayOfWeek.SUNDAY, 4).weekOfYear()));
            }
            case 5: {
                return new TinyIntLiteral((byte) localDateTime
                        .get(WeekFields.of(DayOfWeek.MONDAY, 7).weekOfYear()));
            }
            case 6: {
                return new TinyIntLiteral((byte) localDateTime
                        .get(WeekFields.of(DayOfWeek.SUNDAY, 4).weekOfWeekBasedYear()));
            }
            case 7: {
                return new TinyIntLiteral((byte) localDateTime
                        .get(WeekFields.of(DayOfWeek.MONDAY, 7).weekOfWeekBasedYear()));
            }
            default: {
                throw new AnalysisException(
                        String.format("unknown mode %d in week function", mode));
            }
        }
    }

    /**
     * 0000-01-01/02 are specific dates, sometime need handle them alone.
     */
    private static boolean isSpecificDate(LocalDateTime localDateTime) {
        return localDateTime.getYear() == 0 && localDateTime.getMonthValue() == 1
                && (localDateTime.getDayOfMonth() == 1 || localDateTime.getDayOfMonth() == 2);
    }

    @ExecFunction(name = "yearweek", argTypes = {"DATEV2", "INT"}, returnType = "INT")
    public static Expression yearWeek(DateV2Literal date, IntegerLiteral mode) {
        return yearWeek(date.toJavaDateType(), mode.getIntValue());
    }

    @ExecFunction(name = "yearweek", argTypes = {"DATETIMEV2", "INT"}, returnType = "INT")
    public static Expression yearWeek(DateTimeV2Literal dateTime, IntegerLiteral mode) {
        return yearWeek(dateTime.toJavaDateType(), mode.getIntValue());
    }

    @ExecFunction(name = "yearweek", argTypes = {"DATETIME", "INT"}, returnType = "INT")
    public static Expression yearWeek(DateTimeLiteral dateTime, IntegerLiteral mode) {
        return yearWeek(dateTime.toJavaDateType(), mode.getIntValue());
    }

    @ExecFunction(name = "yearweek", argTypes = {"DATEV2"}, returnType = "INT")
    public static Expression yearWeek(DateV2Literal date) {
        return yearWeek(date.toJavaDateType(), 0);
    }

    @ExecFunction(name = "yearweek", argTypes = {"DATETIMEV2"}, returnType = "INT")
    public static Expression yearWeek(DateTimeV2Literal dateTime) {
        return yearWeek(dateTime.toJavaDateType(), 0);
    }

    @ExecFunction(name = "yearweek", argTypes = {"DATETIME"}, returnType = "INT")
    public static Expression yearWeek(DateTimeLiteral dateTime) {
        return yearWeek(dateTime.toJavaDateType(), 0);
    }

    /**
     * the impl of function yearWeek(date/datetime, mode)
     */
    public static Expression yearWeek(LocalDateTime localDateTime, int mode) {
        if (localDateTime.getYear() == 0) {
            return week(localDateTime, mode);
        }

        switch (mode) {
            case 0: {
                return new IntegerLiteral(
                        localDateTime.get(WeekFields.of(DayOfWeek.SUNDAY, 7).weekBasedYear()) * 100
                                + localDateTime.get(
                                WeekFields.of(DayOfWeek.SUNDAY, 7).weekOfWeekBasedYear()));
            }
            case 1: {
                return new IntegerLiteral(localDateTime.get(WeekFields.ISO.weekBasedYear()) * 100
                        + localDateTime.get(WeekFields.ISO.weekOfWeekBasedYear()));
            }
            case 2: {
                return new IntegerLiteral(
                        localDateTime.get(WeekFields.of(DayOfWeek.SUNDAY, 7).weekBasedYear()) * 100
                                + localDateTime.get(
                                WeekFields.of(DayOfWeek.SUNDAY, 7).weekOfWeekBasedYear()));
            }
            case 3: {
                return new IntegerLiteral(localDateTime.get(WeekFields.ISO.weekBasedYear()) * 100
                        + localDateTime.get(WeekFields.ISO.weekOfWeekBasedYear()));
            }
            case 4: {
                return new IntegerLiteral(
                        localDateTime.get(WeekFields.of(DayOfWeek.SUNDAY, 4).weekBasedYear()) * 100
                                + localDateTime
                                .get(WeekFields.of(DayOfWeek.SUNDAY, 4).weekOfWeekBasedYear()));
            }
            case 5: {
                return new IntegerLiteral(
                        localDateTime.get(WeekFields.of(DayOfWeek.MONDAY, 7).weekBasedYear()) * 100
                                + localDateTime
                                .get(WeekFields.of(DayOfWeek.MONDAY, 7).weekOfWeekBasedYear()));
            }
            case 6: {
                return new IntegerLiteral(
                        localDateTime.get(WeekFields.of(DayOfWeek.SUNDAY, 4).weekBasedYear()) * 100
                                + localDateTime.get(
                                WeekFields.of(DayOfWeek.SUNDAY, 4).weekOfWeekBasedYear()));
            }
            case 7: {
                return new IntegerLiteral(
                        localDateTime.get(WeekFields.of(DayOfWeek.MONDAY, 7).weekBasedYear()) * 100
                                + localDateTime.get(
                                WeekFields.of(DayOfWeek.MONDAY, 7).weekOfWeekBasedYear()));
            }
            default: {
                throw new AnalysisException(
                        String.format("unknown mode %d in yearweek function", mode));
            }
        }
    }

    /**
     * weekofyear
     */
    @ExecFunction(name = "weekofyear", argTypes = {"DATETIMEV2"}, returnType = "TINYINT")
    public static Expression weekOfYear(DateTimeV2Literal dateTime) {
        if (dateTime.getYear() == 0 && dateTime.getDayOfWeek() == 1) {
            if (dateTime.getMonth() == 1 && dateTime.getDay() == 2) {
                return new TinyIntLiteral((byte) 1);
            }
            return new TinyIntLiteral(
                    (byte) (dateTime.toJavaDateType().get(WeekFields.ISO.weekOfWeekBasedYear()) + 1));
        }
        return new TinyIntLiteral((byte) dateTime.toJavaDateType().get(WeekFields.ISO.weekOfWeekBasedYear()));
    }

    /**
     * weekofyear
     */
    @ExecFunction(name = "weekofyear", argTypes = {"DATETIME"}, returnType = "TINYINT")
    public static Expression weekOfYear(DateTimeLiteral dateTime) {
        if (dateTime.getYear() == 0 && dateTime.getDayOfWeek() == 1) {
            if (dateTime.getMonth() == 1 && dateTime.getDay() == 2) {
                return new TinyIntLiteral((byte) 1);
            }
            return new TinyIntLiteral(
                    (byte) (dateTime.toJavaDateType().get(WeekFields.ISO.weekOfWeekBasedYear()) + 1));
        }
        return new TinyIntLiteral((byte) dateTime.toJavaDateType().get(WeekFields.ISO.weekOfWeekBasedYear()));
    }

    /**
     * weekofyear
     */
    @ExecFunction(name = "weekofyear", argTypes = {"DATEV2"}, returnType = "TINYINT")
    public static Expression weekOfYear(DateV2Literal date) {
        if (date.getYear() == 0 && date.getDayOfWeek() == 1) {
            if (date.getMonth() == 1 && date.getDay() == 2) {
                return new TinyIntLiteral((byte) 1);
            }
            return new TinyIntLiteral((byte) (date.toJavaDateType().get(WeekFields.ISO.weekOfWeekBasedYear()) + 1));
        }
        return new TinyIntLiteral((byte) date.toJavaDateType().get(WeekFields.ISO.weekOfWeekBasedYear()));
    }

    @ExecFunction(name = "dayname", argTypes = {"DATETIMEV2"}, returnType = "VARCHAR")
    public static Expression dayName(DateTimeV2Literal dateTime) {
        return new VarcharLiteral(dateTime.toJavaDateType().getDayOfWeek().getDisplayName(TextStyle.FULL,
                Locale.getDefault()));
    }

    @ExecFunction(name = "dayname", argTypes = {"DATETIME"}, returnType = "VARCHAR")
    public static Expression dayName(DateTimeLiteral dateTime) {
        return new VarcharLiteral(dateTime.toJavaDateType().getDayOfWeek().getDisplayName(TextStyle.FULL,
                Locale.getDefault()));
    }

    @ExecFunction(name = "dayname", argTypes = {"DATEV2"}, returnType = "VARCHAR")
    public static Expression dayName(DateV2Literal date) {
        return new VarcharLiteral(date.toJavaDateType().getDayOfWeek().getDisplayName(TextStyle.FULL,
                Locale.getDefault()));
    }

    @ExecFunction(name = "monthname", argTypes = {"DATETIMEV2"}, returnType = "VARCHAR")
    public static Expression monthName(DateTimeV2Literal dateTime) {
        return new VarcharLiteral(dateTime.toJavaDateType().getMonth().getDisplayName(TextStyle.FULL,
                Locale.getDefault()));
    }

    @ExecFunction(name = "monthname", argTypes = {"DATETIME"}, returnType = "VARCHAR")
    public static Expression monthName(DateTimeLiteral dateTime) {
        return new VarcharLiteral(dateTime.toJavaDateType().getMonth().getDisplayName(TextStyle.FULL,
                Locale.getDefault()));
    }

    @ExecFunction(name = "monthname", argTypes = {"DATEV2"}, returnType = "VARCHAR")
    public static Expression monthName(DateV2Literal date) {
        return new VarcharLiteral(date.toJavaDateType().getMonth().getDisplayName(TextStyle.FULL,
                Locale.getDefault()));
    }

    @ExecFunction(name = "from_second", argTypes = {"BIGINT"}, returnType = "DATETIMEV2")
    public static Expression fromSecond(BigIntLiteral second) {
        return fromMicroSecond(second.getValue() * 1000 * 1000);
    }

    @ExecFunction(name = "from_millisecond", argTypes = {"BIGINT"}, returnType = "DATETIMEV2")
    public static Expression fromMilliSecond(BigIntLiteral milliSecond) {
        return fromMicroSecond(milliSecond.getValue() * 1000);
    }

    @ExecFunction(name = "from_microsecond", argTypes = {"BIGINT"}, returnType = "DATETIMEV2")
    public static Expression fromMicroSecond(BigIntLiteral microSecond) {
        return fromMicroSecond(microSecond.getValue());
    }

    private static Expression fromMicroSecond(long microSecond) {
        if (microSecond < 0 || microSecond > 253402271999999999L) {
            return new NullLiteral(DateTimeV2Type.SYSTEM_DEFAULT);
        }
        LocalDateTime dateTime = LocalDateTime.ofInstant(
                Instant.ofEpochMilli(microSecond / 1000).plusNanos(microSecond % 1000 * 1000),
                DateUtils.getTimeZone());
        return new DateTimeV2Literal(DateTimeV2Type.MAX, dateTime.getYear(),
                dateTime.getMonthValue(), dateTime.getDayOfMonth(), dateTime.getHour(),
                dateTime.getMinute(), dateTime.getSecond(), dateTime.getNano() / 1000);
    }

    @ExecFunction(name = "microseconds_diff", argTypes = {"DATETIMEV2", "DATETIMEV2"}, returnType = "BIGINT")
    public static Expression microsecondsDiff(DateTimeV2Literal t1, DateTimeV2Literal t2) {
        return new BigIntLiteral(ChronoUnit.MICROS.between(t2.toJavaDateType(), t1.toJavaDateType()));
    }

    @ExecFunction(name = "milliseconds_diff", argTypes = {"DATETIMEV2", "DATETIMEV2"}, returnType = "BIGINT")
    public static Expression millisecondsDiff(DateTimeV2Literal t1, DateTimeV2Literal t2) {
        return new BigIntLiteral(ChronoUnit.MILLIS.between(t2.toJavaDateType(), t1.toJavaDateType()));
    }

    @ExecFunction(name = "seconds_diff", argTypes = {"DATETIMEV2", "DATETIMEV2"}, returnType = "BIGINT")
    public static Expression secondsDiff(DateTimeV2Literal t1, DateTimeV2Literal t2) {
        return new BigIntLiteral(ChronoUnit.SECONDS.between(t2.toJavaDateType(), t1.toJavaDateType()));
    }

    @ExecFunction(name = "seconds_diff", argTypes = {"DATETIMEV2", "DATEV2"}, returnType = "BIGINT")
    public static Expression secondsDiff(DateTimeV2Literal t1, DateV2Literal t2) {
        return new BigIntLiteral(ChronoUnit.SECONDS.between(t2.toJavaDateType(), t1.toJavaDateType()));
    }

    @ExecFunction(name = "seconds_diff", argTypes = {"DATEV2", "DATETIMEV2"}, returnType = "BIGINT")
    public static Expression secondsDiff(DateV2Literal t1, DateTimeV2Literal t2) {
        return new BigIntLiteral(ChronoUnit.SECONDS.between(t2.toJavaDateType(), t1.toJavaDateType()));
    }

    @ExecFunction(name = "seconds_diff", argTypes = {"DATEV2", "DATEV2"}, returnType = "BIGINT")
    public static Expression secondsDiff(DateV2Literal t1, DateV2Literal t2) {
        return new BigIntLiteral(ChronoUnit.SECONDS.between(t2.toJavaDateType(), t1.toJavaDateType()));
    }

    @ExecFunction(name = "seconds_diff", argTypes = {"DATETIME", "DATETIME"}, returnType = "BIGINT")
    public static Expression secondsDiff(DateTimeLiteral t1, DateTimeLiteral t2) {
        return new BigIntLiteral(ChronoUnit.SECONDS.between(t2.toJavaDateType(), t1.toJavaDateType()));
    }

    @ExecFunction(name = "minutes_diff", argTypes = {"DATETIMEV2", "DATETIMEV2"}, returnType = "BIGINT")
    public static Expression minutesDiff(DateTimeV2Literal t1, DateTimeV2Literal t2) {
        return new BigIntLiteral(ChronoUnit.MINUTES.between(t2.toJavaDateType(), t1.toJavaDateType()));
    }

    @ExecFunction(name = "minutes_diff", argTypes = {"DATETIMEV2", "DATEV2"}, returnType = "BIGINT")
    public static Expression minutesDiff(DateTimeV2Literal t1, DateV2Literal t2) {
        return new BigIntLiteral(ChronoUnit.MINUTES.between(t2.toJavaDateType(), t1.toJavaDateType()));
    }

    @ExecFunction(name = "minutes_diff", argTypes = {"DATEV2", "DATETIMEV2"}, returnType = "BIGINT")
    public static Expression minutesDiff(DateV2Literal t1, DateTimeV2Literal t2) {
        return new BigIntLiteral(ChronoUnit.MINUTES.between(t2.toJavaDateType(), t1.toJavaDateType()));
    }

    @ExecFunction(name = "minutes_diff", argTypes = {"DATEV2", "DATEV2"}, returnType = "BIGINT")
    public static Expression minutesDiff(DateV2Literal t1, DateV2Literal t2) {
        return new BigIntLiteral(ChronoUnit.MINUTES.between(t2.toJavaDateType(), t1.toJavaDateType()));
    }

    @ExecFunction(name = "minutes_diff", argTypes = {"DATETIME", "DATETIME"}, returnType = "BIGINT")
    public static Expression minutesDiff(DateTimeLiteral t1, DateTimeLiteral t2) {
        return new BigIntLiteral(ChronoUnit.MINUTES.between(t2.toJavaDateType(), t1.toJavaDateType()));
    }

    @ExecFunction(name = "hours_diff", argTypes = {"DATETIMEV2", "DATETIMEV2"}, returnType = "BIGINT")
    public static Expression hoursDiff(DateTimeV2Literal t1, DateTimeV2Literal t2) {
        return new BigIntLiteral(ChronoUnit.HOURS.between(t2.toJavaDateType(), t1.toJavaDateType()));
    }

    @ExecFunction(name = "hours_diff", argTypes = {"DATETIMEV2", "DATEV2"}, returnType = "BIGINT")
    public static Expression hoursDiff(DateTimeV2Literal t1, DateV2Literal t2) {
        return new BigIntLiteral(ChronoUnit.HOURS.between(t2.toJavaDateType(), t1.toJavaDateType()));
    }

    @ExecFunction(name = "hours_diff", argTypes = {"DATEV2", "DATETIMEV2"}, returnType = "BIGINT")
    public static Expression hoursDiff(DateV2Literal t1, DateTimeV2Literal t2) {
        return new BigIntLiteral(ChronoUnit.HOURS.between(t2.toJavaDateType(), t1.toJavaDateType()));
    }

    @ExecFunction(name = "hours_diff", argTypes = {"DATEV2", "DATEV2"}, returnType = "BIGINT")
    public static Expression hoursDiff(DateV2Literal t1, DateV2Literal t2) {
        return new BigIntLiteral(ChronoUnit.HOURS.between(t2.toJavaDateType(), t1.toJavaDateType()));
    }

    @ExecFunction(name = "hours_diff", argTypes = {"DATETIME", "DATETIME"}, returnType = "BIGINT")
    public static Expression hoursDiff(DateTimeLiteral t1, DateTimeLiteral t2) {
        return new BigIntLiteral(ChronoUnit.HOURS.between(t2.toJavaDateType(), t1.toJavaDateType()));
    }

    @ExecFunction(name = "days_diff", argTypes = {"DATETIMEV2", "DATETIMEV2"}, returnType = "BIGINT")
    public static Expression daysDiff(DateTimeV2Literal t1, DateTimeV2Literal t2) {
        return new BigIntLiteral(ChronoUnit.DAYS.between(t2.toJavaDateType(), t1.toJavaDateType()));
    }

    @ExecFunction(name = "days_diff", argTypes = {"DATETIMEV2", "DATEV2"}, returnType = "BIGINT")
    public static Expression daysDiff(DateTimeV2Literal t1, DateV2Literal t2) {
        return new BigIntLiteral(ChronoUnit.DAYS.between(t2.toJavaDateType(), t1.toJavaDateType()));
    }

    @ExecFunction(name = "days_diff", argTypes = {"DATEV2", "DATETIMEV2"}, returnType = "BIGINT")
    public static Expression daysDiff(DateV2Literal t1, DateTimeV2Literal t2) {
        return new BigIntLiteral(ChronoUnit.DAYS.between(t2.toJavaDateType(), t1.toJavaDateType()));
    }

    @ExecFunction(name = "days_diff", argTypes = {"DATEV2", "DATEV2"}, returnType = "BIGINT")
    public static Expression daysDiff(DateV2Literal t1, DateV2Literal t2) {
        return new BigIntLiteral(ChronoUnit.DAYS.between(t2.toJavaDateType(), t1.toJavaDateType()));
    }

    @ExecFunction(name = "days_diff", argTypes = {"DATETIME", "DATETIME"}, returnType = "BIGINT")
    public static Expression daysDiff(DateTimeLiteral t1, DateTimeLiteral t2) {
        return new BigIntLiteral(ChronoUnit.DAYS.between(t2.toJavaDateType(), t1.toJavaDateType()));
    }

    @ExecFunction(name = "weeks_diff", argTypes = {"DATETIMEV2", "DATETIMEV2"}, returnType = "BIGINT")
    public static Expression weeksDiff(DateTimeV2Literal t1, DateTimeV2Literal t2) {
        return new BigIntLiteral(ChronoUnit.WEEKS.between(t2.toJavaDateType(), t1.toJavaDateType()));
    }

    @ExecFunction(name = "weeks_diff", argTypes = {"DATETIMEV2", "DATEV2"}, returnType = "BIGINT")
    public static Expression weeksDiff(DateTimeV2Literal t1, DateV2Literal t2) {
        return new BigIntLiteral(ChronoUnit.WEEKS.between(t2.toJavaDateType(), t1.toJavaDateType()));
    }

    @ExecFunction(name = "weeks_diff", argTypes = {"DATEV2", "DATETIMEV2"}, returnType = "BIGINT")
    public static Expression weeksDiff(DateV2Literal t1, DateTimeV2Literal t2) {
        return new BigIntLiteral(ChronoUnit.WEEKS.between(t2.toJavaDateType(), t1.toJavaDateType()));
    }

    @ExecFunction(name = "weeks_diff", argTypes = {"DATEV2", "DATEV2"}, returnType = "BIGINT")
    public static Expression weeksDiff(DateV2Literal t1, DateV2Literal t2) {
        return new BigIntLiteral(ChronoUnit.WEEKS.between(t2.toJavaDateType(), t1.toJavaDateType()));
    }

    @ExecFunction(name = "weeks_diff", argTypes = {"DATETIME", "DATETIME"}, returnType = "BIGINT")
    public static Expression weeksDiff(DateTimeLiteral t1, DateTimeLiteral t2) {
        return new BigIntLiteral(ChronoUnit.WEEKS.between(t2.toJavaDateType(), t1.toJavaDateType()));
    }

    @ExecFunction(name = "months_diff", argTypes = {"DATETIMEV2", "DATETIMEV2"}, returnType = "BIGINT")
    public static Expression monthsDiff(DateTimeV2Literal t1, DateTimeV2Literal t2) {
        return new BigIntLiteral(ChronoUnit.MONTHS.between(t2.toJavaDateType(), t1.toJavaDateType()));
    }

    @ExecFunction(name = "months_diff", argTypes = {"DATETIMEV2", "DATEV2"}, returnType = "BIGINT")
    public static Expression monthsDiff(DateTimeV2Literal t1, DateV2Literal t2) {
        return new BigIntLiteral(ChronoUnit.MONTHS.between(t2.toJavaDateType(), t1.toJavaDateType()));
    }

    @ExecFunction(name = "months_diff", argTypes = {"DATEV2", "DATETIMEV2"}, returnType = "BIGINT")
    public static Expression monthsDiff(DateV2Literal t1, DateTimeV2Literal t2) {
        return new BigIntLiteral(ChronoUnit.MONTHS.between(t2.toJavaDateType(), t1.toJavaDateType()));
    }

    @ExecFunction(name = "months_diff", argTypes = {"DATEV2", "DATEV2"}, returnType = "BIGINT")
    public static Expression monthsDiff(DateV2Literal t1, DateV2Literal t2) {
        return new BigIntLiteral(ChronoUnit.MONTHS.between(t2.toJavaDateType(), t1.toJavaDateType()));
    }

    @ExecFunction(name = "months_diff", argTypes = {"DATETIME", "DATETIME"}, returnType = "BIGINT")
    public static Expression monthsDiff(DateTimeLiteral t1, DateTimeLiteral t2) {
        return new BigIntLiteral(ChronoUnit.MONTHS.between(t2.toJavaDateType(), t1.toJavaDateType()));
    }

    @ExecFunction(name = "years_diff", argTypes = {"DATETIMEV2", "DATETIMEV2"}, returnType = "BIGINT")
    public static Expression yearsDiff(DateTimeV2Literal t1, DateTimeV2Literal t2) {
        return new BigIntLiteral(ChronoUnit.YEARS.between(t2.toJavaDateType(), t1.toJavaDateType()));
    }

    @ExecFunction(name = "years_diff", argTypes = {"DATETIMEV2", "DATEV2"}, returnType = "BIGINT")
    public static Expression yearsDiff(DateTimeV2Literal t1, DateV2Literal t2) {
        return new BigIntLiteral(ChronoUnit.YEARS.between(t2.toJavaDateType(), t1.toJavaDateType()));
    }

    @ExecFunction(name = "years_diff", argTypes = {"DATEV2", "DATETIMEV2"}, returnType = "BIGINT")
    public static Expression yearsDiff(DateV2Literal t1, DateTimeV2Literal t2) {
        return new BigIntLiteral(ChronoUnit.YEARS.between(t2.toJavaDateType(), t1.toJavaDateType()));
    }

    @ExecFunction(name = "years_diff", argTypes = {"DATEV2", "DATEV2"}, returnType = "BIGINT")
    public static Expression yearsDiff(DateV2Literal t1, DateV2Literal t2) {
        return new BigIntLiteral(ChronoUnit.YEARS.between(t2.toJavaDateType(), t1.toJavaDateType()));
    }

    @ExecFunction(name = "years_diff", argTypes = {"DATETIME", "DATETIME"}, returnType = "BIGINT")
    public static Expression yearsDiff(DateTimeLiteral t1, DateTimeLiteral t2) {
        return new BigIntLiteral(ChronoUnit.YEARS.between(t2.toJavaDateType(), t1.toJavaDateType()));
    }
}
