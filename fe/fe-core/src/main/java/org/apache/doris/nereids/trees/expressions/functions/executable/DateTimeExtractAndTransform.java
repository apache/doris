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
import org.apache.doris.nereids.rules.expression.rules.SupportJavaDateFormatter;
import org.apache.doris.nereids.trees.expressions.ExecFunction;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.scalar.FromMicrosecond;
import org.apache.doris.nereids.trees.expressions.functions.scalar.FromMillisecond;
import org.apache.doris.nereids.trees.expressions.functions.scalar.FromSecond;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.DateV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.DecimalLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DecimalV3Literal;
import org.apache.doris.nereids.trees.expressions.literal.DoubleLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.SmallIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLikeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.expressions.literal.TimeV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.DateV2Type;
import org.apache.doris.nereids.types.DecimalV3Type;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.TimeV2Type;
import org.apache.doris.nereids.util.DateTimeFormatterUtils;
import org.apache.doris.nereids.util.DateUtils;
import org.apache.doris.qe.ConnectContext;

import org.apache.commons.lang3.StringUtils;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.DayOfWeek;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.YearMonth;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.format.ResolverStyle;
import java.time.format.TextStyle;
import java.time.temporal.ChronoUnit;
import java.time.temporal.WeekFields;
import java.util.HashMap;
import java.util.Locale;

/**
 * executable function:
 * year, quarter, month, week, dayOfYear, dayOfweek, dayOfMonth, hour, minute,
 * second, microsecond
 */
public class DateTimeExtractAndTransform {

    private static final HashMap<String, Integer> DAY_OF_WEEK = new HashMap<>();

    static {
        DAY_OF_WEEK.put("MO", 1);
        DAY_OF_WEEK.put("MON", 1);
        DAY_OF_WEEK.put("MONDAY", 1);
        DAY_OF_WEEK.put("TU", 2);
        DAY_OF_WEEK.put("TUE", 2);
        DAY_OF_WEEK.put("TUESDAY", 2);
        DAY_OF_WEEK.put("WE", 3);
        DAY_OF_WEEK.put("WED", 3);
        DAY_OF_WEEK.put("WEDNESDAY", 3);
        DAY_OF_WEEK.put("TH", 4);
        DAY_OF_WEEK.put("THU", 4);
        DAY_OF_WEEK.put("THURSDAY", 4);
        DAY_OF_WEEK.put("FR", 5);
        DAY_OF_WEEK.put("FRI", 5);
        DAY_OF_WEEK.put("FRIDAY", 5);
        DAY_OF_WEEK.put("SA", 6);
        DAY_OF_WEEK.put("SAT", 6);
        DAY_OF_WEEK.put("SATURDAY", 6);
        DAY_OF_WEEK.put("SU", 7);
        DAY_OF_WEEK.put("SUN", 7);
        DAY_OF_WEEK.put("SUNDAY", 7);
    }

    // Maximum valid timestamp value (UTC 9999-12-31 23:59:59 - 24 * 3600 for all timezones)
    private static final long TIMESTAMP_VALID_MAX = 32536771199L;

    /**
     * datetime arithmetic function date-v2
     */
    @ExecFunction(name = "datev2")
    public static Expression dateV2(DateTimeV2Literal dateTime) {
        return new DateV2Literal(dateTime.getYear(), dateTime.getMonth(), dateTime.getDay());
    }

    /**
     * Executable datetime extract year
     */
    @ExecFunction(name = "year")
    public static Expression year(DateV2Literal date) {
        return new SmallIntLiteral(((short) date.getYear()));
    }

    @ExecFunction(name = "year")
    public static Expression year(DateTimeV2Literal date) {
        return new SmallIntLiteral(((short) date.getYear()));
    }


    /**
     * Executable datetime extract century
     */
    @ExecFunction(name = "century")
    public static Expression century(DateV2Literal date) {
        return new SmallIntLiteral((short) ((date.getYear() - 1) / 100 + 1));
    }

    @ExecFunction(name = "century")
    public static Expression century(DateTimeV2Literal date) {
        return new SmallIntLiteral((short) ((date.getYear() - 1) / 100 + 1));
    }


    /**
     * Executable datetime extract quarter
     */
    @ExecFunction(name = "quarter")
    public static Expression quarter(DateV2Literal date) {
        return new TinyIntLiteral((byte) ((date.getMonth() - 1) / 3 + 1));
    }

    @ExecFunction(name = "quarter")
    public static Expression quarter(DateTimeV2Literal date) {
        return new TinyIntLiteral((byte) ((date.getMonth() - 1) / 3 + 1));
    }

    /**
     * Executable datetime extract month
     */
    @ExecFunction(name = "month")
    public static Expression month(DateV2Literal date) {
        return new TinyIntLiteral((byte) date.getMonth());
    }

    @ExecFunction(name = "month")
    public static Expression month(DateTimeV2Literal date) {
        return new TinyIntLiteral((byte) date.getMonth());
    }

    /**
     * Executable datetime extract day
     */
    @ExecFunction(name = "day")
    public static Expression day(DateV2Literal date) {
        return new TinyIntLiteral((byte) date.getDay());
    }

    @ExecFunction(name = "day")
    public static Expression day(DateTimeV2Literal date) {
        return new TinyIntLiteral((byte) date.getDay());
    }

    /**
     * Executable datetime extract hour
     */
    @ExecFunction(name = "hour")
    public static Expression hour(DateTimeV2Literal date) {
        return new TinyIntLiteral(((byte) date.getHour()));
    }

    /**
     * Executable datetime extract hour
     */
    @ExecFunction(name = "minute")
    public static Expression minute(DateTimeV2Literal date) {
        return new TinyIntLiteral(((byte) date.getMinute()));
    }

    /**
     * Executable datetime extract second
     */
    @ExecFunction(name = "second")
    public static Expression second(DateTimeV2Literal date) {
        return new TinyIntLiteral(((byte) date.getSecond()));
    }

    /**
     * Executable time extract second
     */
    @ExecFunction(name = "second")
    public static Expression second(TimeV2Literal time) {
        return new TinyIntLiteral(((byte) time.getSecond()));
    }

    /**
     * Executable datetime extract microsecond
     */
    @ExecFunction(name = "microsecond")
    public static Expression microsecond(DateTimeV2Literal date) {
        return new IntegerLiteral(((int) date.getMicroSecond()));
    }

    /**
     * Executable time extract microsecond
     */
    @ExecFunction(name = "microsecond")
    public static Expression microsecond(TimeV2Literal time) {
        return new IntegerLiteral(((int) time.getMicroSecond()));
    }

    /**
     * Executable datetime extract dayofyear
     */
    @ExecFunction(name = "dayofyear")
    public static Expression dayOfYear(DateV2Literal date) {
        return new SmallIntLiteral((short) date.getDayOfYear());
    }

    @ExecFunction(name = "dayofyear")
    public static Expression dayOfYear(DateTimeV2Literal date) {
        return new SmallIntLiteral((short) date.getDayOfYear());
    }

    /**
     * Executable datetime extract dayofmonth
     */
    @ExecFunction(name = "dayofmonth")
    public static Expression dayOfMonth(DateV2Literal date) {
        return new TinyIntLiteral((byte) date.toJavaDateType().getDayOfMonth());
    }

    @ExecFunction(name = "dayofmonth")
    public static Expression dayOfMonth(DateTimeV2Literal date) {
        return new TinyIntLiteral((byte) date.toJavaDateType().getDayOfMonth());
    }

    /**
     * Executable datetime extract dayofweek
     */
    @ExecFunction(name = "dayofweek")
    public static Expression dayOfWeek(DateV2Literal date) {
        return new TinyIntLiteral((byte) (date.getDayOfWeek() % 7 + 1));
    }

    @ExecFunction(name = "dayofweek")
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
    @ExecFunction(name = "date_format")
    public static Expression dateFormat(DateTimeV2Literal date, StringLikeLiteral format) {
        if (StringUtils.trim(format.getValue()).length() > 128) {
            throw new AnalysisException("The length of format string in date_format() function should not be greater"
                    + " than 128.");
        }
        format = (StringLikeLiteral) SupportJavaDateFormatter.translateJavaFormatter(format);
        return new VarcharLiteral(DateTimeFormatterUtils.toFormatStringConservative(date, format, false));
    }

    /**
     * time_format constant folding for time literal.
     */
    @ExecFunction(name = "time_format")
    public static Expression timeFormat(TimeV2Literal time, StringLikeLiteral format) {
        if (StringUtils.trim(format.getValue()).length() > 128) {
            throw new AnalysisException("The length of format string in time_format() function should not be greater"
                    + " than 128.");
        }
        return new VarcharLiteral(DateTimeFormatterUtils.toFormatStringConservative(time, format));
    }

    /**
     * time_format constant folding for datetimev2 literal.
     */
    @ExecFunction(name = "time_format")
    public static Expression timeFormat(DateTimeV2Literal dateTime, StringLikeLiteral format) {
        if (StringUtils.trim(format.getValue()).length() > 128) {
            throw new AnalysisException("The length of format string in time_format() function should not be greater"
                    + " than 128.");
        }
        return new VarcharLiteral(DateTimeFormatterUtils.toFormatStringConservative(dateTime, format, true));
    }

    private static String padTwo(int value) {
        return value < 10 ? "0" + value : Integer.toString(value);
    }

    private static String padMicro(int micro) {
        String s = Integer.toString(micro);
        int len = s.length();
        return len >= 6 ? s : "000000".substring(len) + s;
    }

    @ExecFunction(name = "year_month")
    public static Expression yearMonth(DateTimeV2Literal dateTime) {
        LocalDateTime t = dateTime.toJavaDateType();
        return new VarcharLiteral(t.getYear() + "-" + padTwo(t.getMonthValue()));
    }

    @ExecFunction(name = "day_hour")
    public static Expression dayHour(DateTimeV2Literal dateTime) {
        LocalDateTime t = dateTime.toJavaDateType();
        return new VarcharLiteral(padTwo(t.getDayOfMonth()) + " " + padTwo(t.getHour()));
    }

    @ExecFunction(name = "day_minute")
    public static Expression dayMinute(DateTimeV2Literal dateTime) {
        LocalDateTime t = dateTime.toJavaDateType();
        return new VarcharLiteral(padTwo(t.getDayOfMonth()) + " " + padTwo(t.getHour()) + ":"
                + padTwo(t.getMinute()));
    }

    @ExecFunction(name = "day_second")
    public static Expression daySecond(DateTimeV2Literal dateTime) {
        LocalDateTime t = dateTime.toJavaDateType();
        return new VarcharLiteral(padTwo(t.getDayOfMonth()) + " " + padTwo(t.getHour()) + ":"
                + padTwo(t.getMinute()) + ":" + padTwo(t.getSecond()));
    }

    @ExecFunction(name = "day_microsecond")
    public static Expression dayMicrosecond(DateTimeV2Literal dateTime) {
        LocalDateTime t = dateTime.toJavaDateType();
        return new VarcharLiteral(padTwo(t.getDayOfMonth()) + " " + padTwo(t.getHour()) + ":"
                + padTwo(t.getMinute()) + ":" + padTwo(t.getSecond()) + "."
                + padMicro(t.getNano() / 1000));
    }

    @ExecFunction(name = "hour_minute")
    public static Expression hourMinute(DateTimeV2Literal dateTime) {
        LocalDateTime t = dateTime.toJavaDateType();
        return new VarcharLiteral(padTwo(t.getHour()) + ":" + padTwo(t.getMinute()));
    }

    @ExecFunction(name = "hour_second")
    public static Expression hourSecond(DateTimeV2Literal dateTime) {
        LocalDateTime t = dateTime.toJavaDateType();
        return new VarcharLiteral(padTwo(t.getHour()) + ":" + padTwo(t.getMinute()) + ":"
                + padTwo(t.getSecond()));
    }

    @ExecFunction(name = "hour_microsecond")
    public static Expression hourMicrosecond(DateTimeV2Literal dateTime) {
        LocalDateTime t = dateTime.toJavaDateType();
        return new VarcharLiteral(padTwo(t.getHour()) + ":" + padTwo(t.getMinute()) + ":"
                + padTwo(t.getSecond()) + "." + padMicro(t.getNano() / 1000));
    }

    @ExecFunction(name = "minute_second")
    public static Expression minuteSecond(DateTimeV2Literal dateTime) {
        LocalDateTime t = dateTime.toJavaDateType();
        return new VarcharLiteral(padTwo(t.getMinute()) + ":" + padTwo(t.getSecond()));
    }

    @ExecFunction(name = "minute_microsecond")
    public static Expression minuteMicrosecond(DateTimeV2Literal dateTime) {
        LocalDateTime t = dateTime.toJavaDateType();
        return new VarcharLiteral(padTwo(t.getMinute()) + ":" + padTwo(t.getSecond()) + "."
                + padMicro(t.getNano() / 1000));
    }

    @ExecFunction(name = "second_microsecond")
    public static Expression secondMicrosecond(DateTimeV2Literal dateTime) {
        LocalDateTime t = dateTime.toJavaDateType();
        return new VarcharLiteral(padTwo(t.getSecond()) + "." + padMicro(t.getNano() / 1000));
    }

    /**
     * datetime arithmetic function date
     */
    @ExecFunction(name = "date")
    public static Expression date(DateTimeV2Literal dateTime) throws AnalysisException {
        return new DateV2Literal(dateTime.getYear(), dateTime.getMonth(), dateTime.getDay());
    }

    /**
     * datetime arithmetic function date-trunc
     */
    @ExecFunction(name = "date_trunc")
    public static Expression dateTrunc(DateTimeV2Literal date, StringLikeLiteral trunc) {
        return DateTimeV2Literal.fromJavaDateType(
                dateTruncHelper(date.toJavaDateType(), trunc.getValue()), date.getScale());
    }

    @ExecFunction(name = "date_trunc")
    public static Expression dateTrunc(DateV2Literal date, StringLikeLiteral trunc) {
        return DateV2Literal.fromJavaDateType(dateTruncHelper(date.toJavaDateType(), trunc.getValue()));
    }

    @ExecFunction(name = "date_trunc")
    public static Expression dateTrunc(StringLikeLiteral trunc, DateTimeV2Literal date) {
        return DateTimeV2Literal.fromJavaDateType(
                dateTruncHelper(date.toJavaDateType(), trunc.getValue()), date.getScale());
    }

    @ExecFunction(name = "date_trunc")
    public static Expression dateTrunc(StringLikeLiteral trunc, DateV2Literal date) {
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
    @ExecFunction(name = "from_days")
    public static Expression fromDays(IntegerLiteral n) {
        // doris treat 0000AD as ordinary year but java LocalDateTime treat it as lunar year.
        LocalDateTime res = LocalDateTime.of(0, 1, 1, 0, 0, 0)
                .plusDays(n.getValue());
        if (res.isBefore(LocalDateTime.of(0, 3, 1, 0, 0, 0))) {
            res = res.plusDays(-1);
        }
        return DateV2Literal.fromJavaDateType(res);
    }

    @ExecFunction(name = "last_day")
    public static Expression lastDay(DateV2Literal date) {
        LocalDateTime nextMonthFirstDay = LocalDateTime.of((int) date.getYear(), (int) date.getMonth(), 1,
                0, 0, 0).plusMonths(1);
        return DateV2Literal.fromJavaDateType(nextMonthFirstDay.minusDays(1));
    }

    @ExecFunction(name = "last_day")
    public static Expression lastDay(DateTimeV2Literal date) {
        LocalDateTime nextMonthFirstDay = LocalDateTime.of((int) date.getYear(), (int) date.getMonth(), 1,
                0, 0, 0).plusMonths(1);
        return DateV2Literal.fromJavaDateType(nextMonthFirstDay.minusDays(1));
    }

    /**
     * datetime transformation function: to_monday
     */
    @ExecFunction(name = "to_monday")
    public static Expression toMonday(DateV2Literal date) {
        return DateV2Literal.fromJavaDateType(toMonday(date.toJavaDateType()));
    }

    @ExecFunction(name = "to_monday")
    public static Expression toMonday(DateTimeV2Literal date) {
        return DateV2Literal.fromJavaDateType(toMonday(date.toJavaDateType()));
    }

    private static LocalDateTime toMonday(LocalDateTime dateTime) {
        LocalDateTime specialUpperBound = LocalDateTime.of(1970, 1, 4, 23, 59, 59, 999_999_999);
        LocalDateTime specialLowerBound = LocalDateTime.of(1970, 1, 1, 0, 0, 0);
        if (dateTime.isAfter(specialUpperBound) || dateTime.isBefore(specialLowerBound)) {
            return dateTime.plusDays(-dateTime.getDayOfWeek().getValue() + 1);
        }
        return specialLowerBound;
    }

    /**
     * date transformation function: from_unixtime
     */
    @ExecFunction(name = "from_unixtime")
    public static Expression fromUnixTime(BigIntLiteral second) {
        return fromUnixTime(second, new VarcharLiteral("%Y-%m-%d %H:%i:%s"));
    }

    @ExecFunction(name = "from_unixtime")
    public static Expression fromUnixTime(DecimalLiteral second) {
        return fromUnixTime(second, new VarcharLiteral("%Y-%m-%d %H:%i:%s.%f"));
    }

    @ExecFunction(name = "from_unixtime")
    public static Expression fromUnixTime(DecimalV3Literal second) {
        return fromUnixTime(second, new VarcharLiteral("%Y-%m-%d %H:%i:%s.%f"));
    }

    /**
     * date transformation function: from_unixtime
     */
    @ExecFunction(name = "from_unixtime")
    public static Expression fromUnixTime(BigIntLiteral second, StringLikeLiteral format) {
        format = (StringLikeLiteral) SupportJavaDateFormatter.translateJavaFormatter(format);

        if (second.getValue() < 0) {
            throw new AnalysisException("Operation from_unixtime of " + second.getValue() + " out of range");
        }

        ZonedDateTime dateTime = LocalDateTime.of(1970, 1, 1, 0, 0, 0)
                .plusSeconds(second.getValue())
                .atZone(ZoneId.of("UTC+0"))
                .toOffsetDateTime()
                .atZoneSameInstant(DateUtils.getTimeZone());
        DateTimeV2Literal datetime = new DateTimeV2Literal(dateTime.getYear(), dateTime.getMonthValue(),
                dateTime.getDayOfMonth(), dateTime.getHour(), dateTime.getMinute(), dateTime.getSecond());
        if (datetime.checkRange()) {
            throw new AnalysisException("Operation from_unixtime of " + second.getValue() + " out of range");
        }
        return dateFormat(datetime, format);
    }

    /**
     * date transformation function: from_unixtime
     */
    @ExecFunction(name = "from_unixtime")
    public static Expression fromUnixTime(DecimalLiteral second, StringLikeLiteral format) {
        return fromUnixTime(second.getValue(), format);
    }

    /**
     * date transformation function: from_unixtime
     */
    @ExecFunction(name = "from_unixtime")
    public static Expression fromUnixTime(DecimalV3Literal second, StringLikeLiteral format) {
        return fromUnixTime(second.getValue(), format);
    }

    private static Expression fromUnixTime(BigDecimal second, StringLikeLiteral format) {
        if (second.signum() < 0) {
            throw new AnalysisException("Operation from_unixtime of " + second + " out of range");
        }
        format = (StringLikeLiteral) SupportJavaDateFormatter.translateJavaFormatter(format);
        BigDecimal microSeconds = second.movePointRight(second.scale()).setScale(0, RoundingMode.DOWN);
        ZonedDateTime dateTime = LocalDateTime.of(1970, 1, 1, 0, 0, 0)
                .plus(microSeconds.longValue(), ChronoUnit.MICROS)
                .atZone(ZoneId.of("UTC+0"))
                .toOffsetDateTime()
                .atZoneSameInstant(DateUtils.getTimeZone());
        DateTimeV2Literal datetime = new DateTimeV2Literal(DateTimeV2Type.of(6), dateTime.getYear(),
                dateTime.getMonthValue(),
                dateTime.getDayOfMonth(), dateTime.getHour(), dateTime.getMinute(), dateTime.getSecond(),
                dateTime.getNano() / 1000);
        if (datetime.checkRange()) {
            throw new AnalysisException("Operation from_unixtime of " + second + " out of range");
        }
        return dateFormat(datetime, format);
    }

    /**
     * date transformation function: unix_timestamp
     */
    @ExecFunction(name = "unix_timestamp")
    public static Expression unixTimestamp(DateV2Literal date) {
        return new BigIntLiteral(Integer.parseInt(getTimestamp(date.toJavaDateType())));
    }

    /**
     * date transformation function: unix_timestamp
     */
    @ExecFunction(name = "unix_timestamp")
    public static Expression unixTimestamp(DateTimeV2Literal date) {
        int scale = date.getDataType().getScale();
        return new DecimalV3Literal(DecimalV3Type.createDecimalV3TypeLooseCheck(12 + scale, scale),
                new BigDecimal(getTimestamp(date.toJavaDateType())));
    }

    /**
     * date transformation function: unix_timestamp
     */
    @ExecFunction(name = "unix_timestamp")
    public static Expression unixTimestamp(StringLikeLiteral date, StringLikeLiteral format) {
        format = (StringLikeLiteral) SupportJavaDateFormatter.translateJavaFormatter(format);
        DateTimeFormatter formatter = DateUtils.dateTimeFormatter(format.getValue());
        LocalDateTime dateObj;
        try {
            dateObj = LocalDateTime.parse(date.getValue(), formatter);
        } catch (DateTimeParseException e) {
            // means the date string doesn't contain time fields.
            dateObj = LocalDate.parse(date.getValue(), formatter).atStartOfDay();
        }
        return new DecimalV3Literal(DecimalV3Type.createDecimalV3TypeLooseCheck(18, 6),
                new BigDecimal(getTimestamp(dateObj)));
    }

    private static String getTimestamp(LocalDateTime dateTime) {
        LocalDateTime specialLowerBound = LocalDateTime.of(1970, 1, 1, 0, 0, 0);
        dateTime = dateTime.atZone(DateUtils.getTimeZone())
                        .toOffsetDateTime().atZoneSameInstant(ZoneId.of("UTC+0"))
                        .toLocalDateTime();
        if (dateTime.isBefore(specialLowerBound)) {
            return "0";
        }
        Duration duration = Duration.between(
                specialLowerBound,
                dateTime
                );
        if (duration.getNano() == 0) {
            return String.valueOf(duration.getSeconds());
        } else {
            return duration.getSeconds() + "." + String.format("%06d", duration.getNano() / 1000);
        }
    }

    /**
     * date transformation function: to_date
     */
    @ExecFunction(name = "to_date")
    public static Expression toDate(DateTimeV2Literal date) {
        return new DateV2Literal(date.getYear(), date.getMonth(), date.getDay());
    }

    /**
     * date transformation function: to_days
     */
    @ExecFunction(name = "to_days")
    public static Expression toDays(DateV2Literal date) {
        return new IntegerLiteral((int) calcDayNumber(date.getYear(), date.getMonth(), date.getDay()));
    }

    @ExecFunction(name = "to_days")
    public static Expression toDays(DateTimeV2Literal date) {
        return new IntegerLiteral((int) calcDayNumber(date.getYear(), date.getMonth(), date.getDay()));
    }

    /**
     * date transformation function: to_seconds
     */
    @ExecFunction(name = "to_seconds")
    public static Expression toSeconds(DateV2Literal date) {
        return new BigIntLiteral(calcDayNumber(date.getYear(), date.getMonth(), date.getDay()) * 86400L);
    }

    @ExecFunction(name = "to_seconds")
    public static Expression toSeconds(DateTimeV2Literal date) {
        return new BigIntLiteral(calcDayNumber(date.getYear(), date.getMonth(), date.getDay()) * 86400L
                                    + date.getHour() * 3600L + date.getMinute() * 60L + date.getSecond());
    }

    // Java Duration cannot represent days before 0000-01-01, so using it would turn
    // TO_DAYS('0000-01-01') into the diff between that date and itself (0).
    // We use BE's arithmetic instead so 0000-01-01 returns 1 as expected.
    // Previous FE logic often matched BE only because Java treats year 0 as leap
    // making TO_DAYS('0000-02-29') fold to 59.
    // While BE/MySQL consider year 0 common, so:
    // TO_DAYS('0000-02-28') == 59 and TO_DAYS('0000-02-29') == NULL. After
    // 0000-03-01 the two implementations naturally align again.
    private static long calcDayNumber(long year, long month, long day) {
        if (year == 0 && month == 0) {
            return 0;
        }
        if (year == 0 && month == 1 && day == 1) {
            return 1;
        }

        long y = year;
        long delsum = 365L * y + 31L * (month - 1) + day;
        if (month <= 2) {
            y -= 1;
        } else {
            delsum -= (month * 4 + 23) / 10;
        }
        return delsum + y / 4 - y / 100 + y / 400;
    }

    /**
     * date transformation function: makedate
     */
    @ExecFunction(name = "makedate")
    public static Expression makeDate(IntegerLiteral year, IntegerLiteral dayOfYear) {
        int yearValue = year.getValue();
        int dayValue = dayOfYear.getValue();
        if (yearValue < 0 || yearValue > 9999 || dayValue <= 0) {
            throw new AnalysisException("Operation makedate of " + yearValue + ", " + dayValue + " out of range");
        }
        return dayValue > 0
                ? DateV2Literal.fromJavaDateType(LocalDateTime.of(yearValue, 1, 1, 0, 0, 0).plusDays(dayValue - 1))
                : new NullLiteral(DateV2Type.INSTANCE);
    }

    /**
     * time transformation function: maketime
     */
    @ExecFunction(name = "maketime")
    public static Expression makeTime(BigIntLiteral hour, BigIntLiteral minute, DoubleLiteral second) {
        long hourValue = hour.getValue();
        long minuteValue = minute.getValue();
        double secondValue = second.getValue();

        if (minuteValue < 0 || minuteValue >= 60 || secondValue < 0 || secondValue >= 60) {
            return new NullLiteral(TimeV2Type.SYSTEM_DEFAULT);
        }
        if (Math.abs(hourValue) > 838) {
            hourValue = hourValue > 0 ? 838 : -838;
            minuteValue = 59;
            secondValue = 59;
        } else if (Math.abs(hourValue) == 838 && secondValue > 59) {
            secondValue = 59;
        }

        double totalSeconds = Math.abs(hourValue) * 3600 + minuteValue * 60
                + Math.round(secondValue * 1000000.0) / 1000000.0;
        if (hourValue < 0) {
            totalSeconds = -totalSeconds;
        }
        return new TimeV2Literal(totalSeconds);
    }

    /**
     * date transformation function: str_to_date
     */
    @ExecFunction(name = "str_to_date")
    public static Expression strToDate(StringLikeLiteral str, StringLikeLiteral format) {
        format = (StringLikeLiteral) SupportJavaDateFormatter.translateJavaFormatter(format);
        if (org.apache.doris.analysis.DateLiteral.hasTimePart(format.getStringValue())) {
            boolean hasMicroPart = org.apache.doris.analysis.DateLiteral.hasMicroSecondPart(format.getStringValue());
            return DateTimeV2Literal.fromJavaDateType(
                    DateUtils.getTime(DateUtils.dateTimeFormatter(format.getValue()), str.getValue()),
                    hasMicroPart ? 6 : 0);
        } else {
            return DateV2Literal.fromJavaDateType(
                    DateUtils.getTime(DateUtils.dateTimeFormatter(format.getValue()), str.getValue()));
        }
    }

    @ExecFunction(name = "timestamp")
    public static Expression timestamp(DateTimeV2Literal datetime) {
        return datetime;
    }

    /**
     * date transformation function: convert_tz
     */
    @ExecFunction(name = "convert_tz")
    public static Expression convertTz(DateTimeV2Literal datetime, StringLikeLiteral fromTz, StringLikeLiteral toTz) {
        // Validate timezone offset ranges before parsing
        validateTimezoneOffset(fromTz.getStringValue());
        validateTimezoneOffset(toTz.getStringValue());

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

    private static void validateTimezoneOffset(String timezone) {
        // Pattern to match offset format like +HH:MM or -HH:MM
        if (timezone.matches("^[+-]\\d{2}:\\d{2}$")) {
            boolean positive = timezone.charAt(0) == '+';
            int hour = Integer.parseInt(timezone.substring(1, 3));
            int minute = Integer.parseInt(timezone.substring(4, 6));

            if (!positive && hour > 12) {
                throw new AnalysisException("Invalid timezone offset: " + timezone
                        + ". Timezone offsets must be between -12:00 and +14:00");
            } else if (positive && hour > 14) {
                throw new AnalysisException("Invalid timezone offset: " + timezone
                        + ". Timezone offsets must be between -12:00 and +14:00");
            }

            if (minute != 0 && minute != 15 && minute != 30 && minute != 45) {
                throw new AnalysisException("Invalid timezone offset: " + timezone
                        + ". Minute part should be 00, 15, 30, or 45");
            }
        }
    }

    @ExecFunction(name = "weekday")
    public static Expression weekDay(DateV2Literal date) {
        return new TinyIntLiteral((byte) ((date.toJavaDateType().getDayOfWeek().getValue() + 6) % 7));
    }

    @ExecFunction(name = "weekday")
    public static Expression weekDay(DateTimeV2Literal date) {
        return new TinyIntLiteral((byte) ((date.toJavaDateType().getDayOfWeek().getValue() + 6) % 7));
    }

    @ExecFunction(name = "week")
    public static Expression week(DateTimeV2Literal dateTime) {
        return week(dateTime.toJavaDateType(), 0);
    }

    @ExecFunction(name = "week")
    public static Expression week(DateTimeV2Literal dateTime, IntegerLiteral mode) {
        return week(dateTime.toJavaDateType(), mode.getIntValue());
    }

    @ExecFunction(name = "week")
    public static Expression week(DateV2Literal date) {
        return week(date.toJavaDateType(), 0);
    }

    @ExecFunction(name = "week")
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

    @ExecFunction(name = "yearweek")
    public static Expression yearWeek(DateV2Literal date, IntegerLiteral mode) {
        return yearWeek(date.toJavaDateType(), mode.getIntValue());
    }

    @ExecFunction(name = "yearweek")
    public static Expression yearWeek(DateTimeV2Literal dateTime, IntegerLiteral mode) {
        return yearWeek(dateTime.toJavaDateType(), mode.getIntValue());
    }

    @ExecFunction(name = "yearweek")
    public static Expression yearWeek(DateV2Literal date) {
        return yearWeek(date.toJavaDateType(), 0);
    }

    @ExecFunction(name = "yearweek")
    public static Expression yearWeek(DateTimeV2Literal dateTime) {
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
    @ExecFunction(name = "weekofyear")
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
    @ExecFunction(name = "weekofyear")
    public static Expression weekOfYear(DateV2Literal date) {
        if (date.getYear() == 0 && date.getDayOfWeek() == 1) {
            if (date.getMonth() == 1 && date.getDay() == 2) {
                return new TinyIntLiteral((byte) 1);
            }
            return new TinyIntLiteral((byte) (date.toJavaDateType().get(WeekFields.ISO.weekOfWeekBasedYear()) + 1));
        }
        return new TinyIntLiteral((byte) date.toJavaDateType().get(WeekFields.ISO.weekOfWeekBasedYear()));
    }

    /**
     * Get locale from session variable lc_time_names, fallback to default if not available
     */
    private static Locale getSessionLocale() {
        ConnectContext ctx = ConnectContext.get();
        if (ctx != null && ctx.getSessionVariable() != null) {
            String lcTimeNames = ctx.getSessionVariable().getLcTimeNames();
            if (lcTimeNames != null && !lcTimeNames.isEmpty()) {
                String[] parts = lcTimeNames.split("_");
                return new Locale(parts[0], parts[1]);
            }
        }
        return Locale.getDefault();
    }

    @ExecFunction(name = "dayname")
    public static Expression dayName(DateTimeV2Literal dateTime) {
        return new VarcharLiteral(dateTime.toJavaDateType().getDayOfWeek().getDisplayName(TextStyle.FULL,
                getSessionLocale()));
    }

    @ExecFunction(name = "dayname")
    public static Expression dayName(DateV2Literal date) {
        return new VarcharLiteral(date.toJavaDateType().getDayOfWeek().getDisplayName(TextStyle.FULL,
                getSessionLocale()));
    }

    @ExecFunction(name = "monthname")
    public static Expression monthName(DateTimeV2Literal dateTime) {
        return new VarcharLiteral(dateTime.toJavaDateType().getMonth().getDisplayName(TextStyle.FULL,
                getSessionLocale()));
    }

    @ExecFunction(name = "monthname")
    public static Expression monthName(DateV2Literal date) {
        return new VarcharLiteral(date.toJavaDateType().getMonth().getDisplayName(TextStyle.FULL,
                getSessionLocale()));
    }

    @ExecFunction(name = "from_second")
    public static Expression fromSecond(BigIntLiteral second) {
        return fromMicroSecond(second.getValue() * 1000 * 1000, FromSecond.RESULT_SCALE);
    }

    @ExecFunction(name = "from_millisecond")
    public static Expression fromMilliSecond(BigIntLiteral milliSecond) {
        return fromMicroSecond(milliSecond.getValue() * 1000, FromMillisecond.RESULT_SCALE);
    }

    @ExecFunction(name = "from_microsecond")
    public static Expression fromMicroSecond(BigIntLiteral microSecond) {
        return fromMicroSecond(microSecond.getValue(), FromMicrosecond.RESULT_SCALE);
    }

    private static Expression fromMicroSecond(long microSecond, int scale) {
        if (microSecond < 0 || microSecond > 253402271999999999L) {
            throw new AnalysisException("Operation from_microsecond of " + microSecond + " out of range");
        }
        LocalDateTime dateTime = LocalDateTime.ofInstant(
                Instant.ofEpochMilli(microSecond / 1000).plusNanos(microSecond % 1000 * 1000),
                DateUtils.getTimeZone());
        return new DateTimeV2Literal(DateTimeV2Type.of(scale), dateTime.getYear(),
                dateTime.getMonthValue(), dateTime.getDayOfMonth(), dateTime.getHour(),
                dateTime.getMinute(), dateTime.getSecond(), dateTime.getNano() / 1000);
    }

    @ExecFunction(name = "microseconds_diff")
    public static Expression microsecondsDiff(DateTimeV2Literal t1, DateTimeV2Literal t2) {
        return new BigIntLiteral(ChronoUnit.MICROS.between(t2.toJavaDateType(), t1.toJavaDateType()));
    }

    @ExecFunction(name = "milliseconds_diff")
    public static Expression millisecondsDiff(DateTimeV2Literal t1, DateTimeV2Literal t2) {
        return new BigIntLiteral(ChronoUnit.MILLIS.between(t2.toJavaDateType(), t1.toJavaDateType()));
    }

    @ExecFunction(name = "seconds_diff")
    public static Expression secondsDiff(DateTimeV2Literal t1, DateTimeV2Literal t2) {
        return new BigIntLiteral(ChronoUnit.SECONDS.between(t2.toJavaDateType(), t1.toJavaDateType()));
    }

    @ExecFunction(name = "seconds_diff")
    public static Expression secondsDiff(DateTimeV2Literal t1, DateV2Literal t2) {
        return new BigIntLiteral(ChronoUnit.SECONDS.between(t2.toJavaDateType(), t1.toJavaDateType()));
    }

    @ExecFunction(name = "seconds_diff")
    public static Expression secondsDiff(DateV2Literal t1, DateTimeV2Literal t2) {
        return new BigIntLiteral(ChronoUnit.SECONDS.between(t2.toJavaDateType(), t1.toJavaDateType()));
    }

    @ExecFunction(name = "seconds_diff")
    public static Expression secondsDiff(DateV2Literal t1, DateV2Literal t2) {
        return new BigIntLiteral(ChronoUnit.SECONDS.between(t2.toJavaDateType(), t1.toJavaDateType()));
    }

    @ExecFunction(name = "minutes_diff")
    public static Expression minutesDiff(DateTimeV2Literal t1, DateTimeV2Literal t2) {
        return new BigIntLiteral(ChronoUnit.MINUTES.between(t2.toJavaDateType(), t1.toJavaDateType()));
    }

    @ExecFunction(name = "minutes_diff")
    public static Expression minutesDiff(DateTimeV2Literal t1, DateV2Literal t2) {
        return new BigIntLiteral(ChronoUnit.MINUTES.between(t2.toJavaDateType(), t1.toJavaDateType()));
    }

    @ExecFunction(name = "minutes_diff")
    public static Expression minutesDiff(DateV2Literal t1, DateTimeV2Literal t2) {
        return new BigIntLiteral(ChronoUnit.MINUTES.between(t2.toJavaDateType(), t1.toJavaDateType()));
    }

    @ExecFunction(name = "minutes_diff")
    public static Expression minutesDiff(DateV2Literal t1, DateV2Literal t2) {
        return new BigIntLiteral(ChronoUnit.MINUTES.between(t2.toJavaDateType(), t1.toJavaDateType()));
    }

    @ExecFunction(name = "hours_diff")
    public static Expression hoursDiff(DateTimeV2Literal t1, DateTimeV2Literal t2) {
        return new BigIntLiteral(ChronoUnit.HOURS.between(t2.toJavaDateType(), t1.toJavaDateType()));
    }

    @ExecFunction(name = "hours_diff")
    public static Expression hoursDiff(DateTimeV2Literal t1, DateV2Literal t2) {
        return new BigIntLiteral(ChronoUnit.HOURS.between(t2.toJavaDateType(), t1.toJavaDateType()));
    }

    @ExecFunction(name = "hours_diff")
    public static Expression hoursDiff(DateV2Literal t1, DateTimeV2Literal t2) {
        return new BigIntLiteral(ChronoUnit.HOURS.between(t2.toJavaDateType(), t1.toJavaDateType()));
    }

    @ExecFunction(name = "hours_diff")
    public static Expression hoursDiff(DateV2Literal t1, DateV2Literal t2) {
        return new BigIntLiteral(ChronoUnit.HOURS.between(t2.toJavaDateType(), t1.toJavaDateType()));
    }

    @ExecFunction(name = "days_diff")
    public static Expression daysDiff(DateTimeV2Literal t1, DateTimeV2Literal t2) {
        return new BigIntLiteral(ChronoUnit.DAYS.between(t2.toJavaDateType(), t1.toJavaDateType()));
    }

    @ExecFunction(name = "days_diff")
    public static Expression daysDiff(DateTimeV2Literal t1, DateV2Literal t2) {
        return new BigIntLiteral(ChronoUnit.DAYS.between(t2.toJavaDateType(), t1.toJavaDateType()));
    }

    @ExecFunction(name = "days_diff")
    public static Expression daysDiff(DateV2Literal t1, DateTimeV2Literal t2) {
        return new BigIntLiteral(ChronoUnit.DAYS.between(t2.toJavaDateType(), t1.toJavaDateType()));
    }

    @ExecFunction(name = "days_diff")
    public static Expression daysDiff(DateV2Literal t1, DateV2Literal t2) {
        return new BigIntLiteral(ChronoUnit.DAYS.between(t2.toJavaDateType(), t1.toJavaDateType()));
    }

    @ExecFunction(name = "weeks_diff")
    public static Expression weeksDiff(DateTimeV2Literal t1, DateTimeV2Literal t2) {
        return new BigIntLiteral(ChronoUnit.WEEKS.between(t2.toJavaDateType(), t1.toJavaDateType()));
    }

    @ExecFunction(name = "weeks_diff")
    public static Expression weeksDiff(DateTimeV2Literal t1, DateV2Literal t2) {
        return new BigIntLiteral(ChronoUnit.WEEKS.between(t2.toJavaDateType(), t1.toJavaDateType()));
    }

    @ExecFunction(name = "weeks_diff")
    public static Expression weeksDiff(DateV2Literal t1, DateTimeV2Literal t2) {
        return new BigIntLiteral(ChronoUnit.WEEKS.between(t2.toJavaDateType(), t1.toJavaDateType()));
    }

    @ExecFunction(name = "weeks_diff")
    public static Expression weeksDiff(DateV2Literal t1, DateV2Literal t2) {
        return new BigIntLiteral(ChronoUnit.WEEKS.between(t2.toJavaDateType(), t1.toJavaDateType()));
    }

    @ExecFunction(name = "months_diff")
    public static Expression monthsDiff(DateTimeV2Literal t1, DateTimeV2Literal t2) {
        return new BigIntLiteral(ChronoUnit.MONTHS.between(t2.toJavaDateType(), t1.toJavaDateType()));
    }

    @ExecFunction(name = "months_diff")
    public static Expression monthsDiff(DateTimeV2Literal t1, DateV2Literal t2) {
        return new BigIntLiteral(ChronoUnit.MONTHS.between(t2.toJavaDateType(), t1.toJavaDateType()));
    }

    @ExecFunction(name = "months_diff")
    public static Expression monthsDiff(DateV2Literal t1, DateTimeV2Literal t2) {
        return new BigIntLiteral(ChronoUnit.MONTHS.between(t2.toJavaDateType(), t1.toJavaDateType()));
    }

    @ExecFunction(name = "months_diff")
    public static Expression monthsDiff(DateV2Literal t1, DateV2Literal t2) {
        return new BigIntLiteral(ChronoUnit.MONTHS.between(t2.toJavaDateType(), t1.toJavaDateType()));
    }

    @ExecFunction(name = "quarters_diff")
    public static Expression quartersDiff(DateTimeV2Literal t1, DateTimeV2Literal t2) {
        return new BigIntLiteral(ChronoUnit.MONTHS.between(t2.toJavaDateType(), t1.toJavaDateType()) / 3);
    }

    @ExecFunction(name = "quarters_diff")
    public static Expression quartersDiff(DateV2Literal t1, DateV2Literal t2) {
        return new BigIntLiteral(ChronoUnit.MONTHS.between(t2.toJavaDateType(), t1.toJavaDateType()) / 3);
    }

    @ExecFunction(name = "years_diff")
    public static Expression yearsDiff(DateTimeV2Literal t1, DateTimeV2Literal t2) {
        return new BigIntLiteral(ChronoUnit.YEARS.between(t2.toJavaDateType(), t1.toJavaDateType()));
    }

    @ExecFunction(name = "years_diff")
    public static Expression yearsDiff(DateTimeV2Literal t1, DateV2Literal t2) {
        return new BigIntLiteral(ChronoUnit.YEARS.between(t2.toJavaDateType(), t1.toJavaDateType()));
    }

    @ExecFunction(name = "years_diff")
    public static Expression yearsDiff(DateV2Literal t1, DateTimeV2Literal t2) {
        return new BigIntLiteral(ChronoUnit.YEARS.between(t2.toJavaDateType(), t1.toJavaDateType()));
    }

    @ExecFunction(name = "years_diff")
    public static Expression yearsDiff(DateV2Literal t1, DateV2Literal t2) {
        return new BigIntLiteral(ChronoUnit.YEARS.between(t2.toJavaDateType(), t1.toJavaDateType()));
    }

    /**
     * months_between(date1, date2, round_off)
     */
    @ExecFunction(name = "months_between")
    public static Expression monthsBetween(DateV2Literal t1, DateV2Literal t2, BooleanLiteral roundOff) {
        long yearBetween = t1.getYear() - t2.getYear();
        long monthBetween = t1.getMonth() - t2.getMonth();
        int daysInMonth1 = YearMonth.of((int) t1.getYear(), (int) t1.getMonth()).lengthOfMonth();
        int daysInMonth2 = YearMonth.of((int) t2.getYear(), (int) t2.getMonth()).lengthOfMonth();
        double dayBetween = 0;
        if (t1.getDay() == daysInMonth1 && t2.getDay() == daysInMonth2) {
            dayBetween = 0;
        } else {
            dayBetween = (t1.getDay() - t2.getDay()) / 31.0;
        }
        double result = yearBetween * 12 + monthBetween + dayBetween;
        // rounded to 8 digits unless roundOff=false.
        if (roundOff.getValue()) {
            result = new BigDecimal(result).setScale(8, RoundingMode.HALF_UP).doubleValue();
        }
        return new DoubleLiteral(result);
    }

    private static int getDayOfWeek(String day) {
        Integer dayOfWeek = DAY_OF_WEEK.get(day.toUpperCase());
        if (dayOfWeek == null) {
            return 0;
        }
        return dayOfWeek;
    }

    /**
     * date arithmetic function next_day
     */
    @ExecFunction(name = "next_day")
    public static Expression nextDay(DateV2Literal date, StringLiteral day) {
        int dayOfWeek = getDayOfWeek(day.getValue());
        if (dayOfWeek == 0) {
            throw new RuntimeException("Invalid day of week: " + day.getValue());
        }
        int daysToAdd = (dayOfWeek - date.getDayOfWeek() + 7) % 7;
        daysToAdd = daysToAdd == 0 ? 7 : daysToAdd;
        return date.plusDays(daysToAdd);
    }

    /**
     * date arithmetic function previous_day
     */
    @ExecFunction(name = "previous_day")
    public static Expression previousDay(DateV2Literal date, StringLiteral day) {
        int dayOfWeek = getDayOfWeek(day.getValue());
        if (dayOfWeek == 0) {
            throw new RuntimeException("Invalid day of week: " + day.getValue());
        }
        int daysToSub = (date.getDayOfWeek() - dayOfWeek + 7) % 7;
        daysToSub = daysToSub == 0 ? 7 : daysToSub;
        return date.plusDays(-daysToSub);
    }

    /**
     * date transform function sec_to_time
     */
    @ExecFunction(name = "sec_to_time")
    public static Expression secToTime(IntegerLiteral sec) {
        return new TimeV2Literal((double) sec.getValue() * 1000000, 0);
    }

    /**
     * date transform function sec_to_time
     */
    @ExecFunction(name = "sec_to_time")
    public static Expression secToTime(DoubleLiteral sec) {
        return new TimeV2Literal(sec.getValue() * 1000000);
    }

    /**
     * get_format function for constant folding
     */
    @ExecFunction(name = "get_format")
    public static Expression getFormat(StringLikeLiteral type, StringLikeLiteral format) {
        String typeStr = type.getValue();
        String formatStr = format.getValue().toUpperCase();

        String result = null;

        switch (typeStr) {
            case "DATE":
                switch (formatStr) {
                    case "USA":
                        result = "%m.%d.%Y";
                        break;
                    case "JIS":
                    case "ISO":
                        result = "%Y-%m-%d";
                        break;
                    case "EUR":
                        result = "%d.%m.%Y";
                        break;
                    case "INTERNAL":
                        result = "%Y%m%d";
                        break;
                    default:
                        break;
                }
                break;
            case "DATETIME":
                switch (formatStr) {
                    case "USA":
                        result = "%Y-%m-%d %H.%i.%s";
                        break;
                    case "JIS":
                    case "ISO":
                        result = "%Y-%m-%d %H:%i:%s";
                        break;
                    case "EUR":
                        result = "%Y-%m-%d %H.%i.%s";
                        break;
                    case "INTERNAL":
                        result = "%Y%m%d%H%i%s";
                        break;
                    default:
                        break;
                }
                break;
            case "TIME":
                switch (formatStr) {
                    case "USA":
                        result = "%h:%i:%s %p";
                        break;
                    case "JIS":
                    case "ISO":
                        result = "%H:%i:%s";
                        break;
                    case "EUR":
                        result = "%H.%i.%s";
                        break;
                    case "INTERNAL":
                        result = "%H%i%s";
                        break;
                    default:
                        break;
                }
                break;
            default:
                break;
        }

        if (result == null) {
            return new NullLiteral(StringType.INSTANCE);
        }

        return new VarcharLiteral(result);
    }

    /**
     * date transform function period_add
     */
    @ExecFunction(name = "period_add")
    public static Expression periodAdd(BigIntLiteral period, BigIntLiteral months) {
        return new BigIntLiteral(convertMonthToPeriod(
                checkAndConvertPeriodToMonth(period.getValue()) + months.getValue()));
    }

    /**
     * date transform function period_diff
     */
    @ExecFunction(name = "period_diff")
    public static Expression periodDiff(BigIntLiteral period1, BigIntLiteral period2) {
        return new BigIntLiteral(checkAndConvertPeriodToMonth(
                period1.getValue()) - checkAndConvertPeriodToMonth(period2.getValue()));
    }

    private static void validatePeriod(long period) {
        if (period <= 0 || (period % 100) == 0 || (period % 100) > 12) {
            throw new AnalysisException("Period function got invalid period: " + period);
        }
    }

    private static long checkAndConvertPeriodToMonth(long period) {
        validatePeriod(period);
        long year = period / 100;
        if (year < 100) {
            year += (year >= 70) ? 1900 : 2000;
        }
        return year * 12L + (period % 100) - 1;
    }

    private static long convertMonthToPeriod(long month) {
        long year = month / 12;
        if (year < 100) {
            year += (year >= 70) ? 1900 : 2000;
        }
        return year * 100 + month % 12 + 1;
    }

    /**
     * date extract function hour_from_unixtime
     */
    @ExecFunction(name = "hour_from_unixtime")
    public static Expression hourFromUnixtime(BigIntLiteral unixTime) {
        long epochSecond = unixTime.getValue();
        if (epochSecond < 0 || epochSecond > TIMESTAMP_VALID_MAX) {
            throw new AnalysisException("Function hour_from_unixtime out of range(between 0 and "
                            + TIMESTAMP_VALID_MAX + "): " + epochSecond);
        }

        ZoneId timeZone = DateUtils.getTimeZone();
        ZonedDateTime zonedDateTime = Instant.ofEpochSecond(epochSecond).atZone(timeZone);
        return new TinyIntLiteral((byte) zonedDateTime.getHour());
    }

    /**
     * date extract function minute_from_unixtime
     */
    @ExecFunction(name = "minute_from_unixtime")
    public static Expression minuteFromUnixtime(BigIntLiteral unixTime) {
        long localTime = unixTime.getValue();
        if (localTime < 0 || localTime > TIMESTAMP_VALID_MAX) {
            throw new AnalysisException("Function minute_from_unixtime out of range(between 0 and "
                    + TIMESTAMP_VALID_MAX + "): " + localTime);
        }

        localTime = localTime - (localTime / 3600) * 3600;

        byte minute = (byte) (localTime / 60);
        return new TinyIntLiteral(minute);
    }

    /**
     * date extract function second_from_unixtime
     */
    @ExecFunction(name = "second_from_unixtime")
    public static Expression secondFromUnixtime(BigIntLiteral unixTime) {
        long localTime = unixTime.getValue();
        if (localTime < 0 || localTime > TIMESTAMP_VALID_MAX) {
            throw new AnalysisException("Function second_from_unixtime out of range(between 0 and "
                    + TIMESTAMP_VALID_MAX + "): " + localTime);
        }

        long remainder;
        if (localTime >= 0) {
            remainder = localTime % 60;
        } else {
            remainder = localTime % 60;
            if (remainder < 0) {
                remainder += 60;
            }
        }
        return new TinyIntLiteral((byte) remainder);
    }

    /**
     * date extract function microsecond_from_unixtime
     */
    @ExecFunction(name = "microsecond_from_unixtime")
    public static Expression microsecondFromUnixtime(DecimalV3Literal unixTime) {
        BigDecimal value = unixTime.getValue();

        long seconds = value.longValue();
        if (seconds < 0 || seconds > TIMESTAMP_VALID_MAX) {
            throw new AnalysisException("Function microsecond_from_unixtime out of range(between 0 and "
                    + TIMESTAMP_VALID_MAX + "): " + seconds);
        }

        DecimalV3Type dataType = (DecimalV3Type) unixTime.getDataType();
        int scale = dataType.getScale();

        BigDecimal fractional = value.remainder(BigDecimal.ONE);
        long fraction = fractional.movePointRight(scale).longValue();

        if (scale < 6) {
            fraction *= (long) Math.pow(10, 6 - scale);
        }
        return new IntegerLiteral((int) fraction);
    }
}
