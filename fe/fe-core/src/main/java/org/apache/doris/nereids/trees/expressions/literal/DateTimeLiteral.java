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

package org.apache.doris.nereids.trees.expressions.literal;

import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.catalog.Type;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.exceptions.CastException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DateTimeType;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.TimeStampNsType;
import org.apache.doris.nereids.types.TimeStampTzType;
import org.apache.doris.nereids.types.TimeV2Type;
import org.apache.doris.nereids.types.coercion.DateLikeType;
import org.apache.doris.nereids.util.DateUtils;

import com.google.common.base.Preconditions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.math.BigInteger;
import java.time.DateTimeException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalQueries;
import java.util.Objects;

/**
 * date time literal.
 */
public class DateTimeLiteral extends DateLiteral {
    public static final DateTimeLiteral MIN_DATETIME = new DateTimeLiteral(0000, 1, 1, 0, 0, 0);
    public static final DateTimeLiteral MAX_DATETIME = new DateTimeLiteral(9999, 12, 31, 23, 59, 59);
    protected static final int MAX_MICROSECOND = 999999;
    protected static final int MAX_NANOSECOND = 999999999;

    private static final Logger LOG = LogManager.getLogger(DateTimeLiteral.class);

    protected long hour;
    protected long minute;
    protected long second;
    protected long microSecond;
    protected long nanoSecond;

    public DateTimeLiteral(String s) {
        this(DateTimeType.INSTANCE, s);
    }

    protected DateTimeLiteral(DateLikeType dataType, String s) {
        super(dataType);
        init(s);
    }

    /**
     * C'tor data time literal.
     */
    public DateTimeLiteral(long year, long month, long day, long hour, long minute, long second) {
        this(DateTimeType.INSTANCE, year, month, day, hour, minute, second);
    }

    /**
     * C'tor data time literal.
     */
    public DateTimeLiteral(DateLikeType dataType, long year, long month, long day,
            long hour, long minute, long second) {
        this(dataType, year, month, day, hour, minute, second, 0L);
    }

    /**
     * C'tor data time literal.
     */
    public DateTimeLiteral(DateLikeType dataType, long year, long month, long day,
            long hour, long minute, long second, long microSecond) {
        super(dataType);
        this.hour = hour;
        this.minute = minute;
        this.second = second;
        this.microSecond = microSecond;
        this.nanoSecond = microSecond * 1000;
        this.year = year;
        this.month = month;
        this.day = day;
    }

    @Override
    public boolean isMidnight() {
        return hour == 0 && minute == 0 && second == 0 && nanoSecond == 0;
    }

    /**
     * determine scale by datetime string
     */
    public static int determineScale(String s) {
        if (!s.contains("-") && !s.contains(":")) {
            return 0;
        }
        // means basic format with timezone
        if (s.indexOf("-") == s.lastIndexOf("-") && s.indexOf(":") == s.lastIndexOf(":")) {
            return 0;
        }
        s = normalize(s).get();
        if (s.length() <= 19 || s.charAt(19) != '.') {
            return 0;
        }
        // from index 19 find the index of first char which is not digit
        int scale = 0;
        for (int i = 20; i < s.length(); i++) {
            if (!Character.isDigit(s.charAt(i))) {
                break;
            }
            scale++;
        }
        // trim the tailing zero
        for (int i = 19 + scale; i >= 19; i--) {
            if (s.charAt(i) != '0') {
                break;
            }
            scale--;
        }
        return scale;
    }

    /** parseDateTimeLiteral */
    public static Result<DateTimeLiteral, AnalysisException> parseDateTimeLiteral(String s, boolean isV2) {
        Result<TemporalAccessor, AnalysisException> parseResult = parseDateTime(s);
        if (parseResult.isError()) {
            return parseResult.cast();
        }

        TemporalAccessor temporal = parseResult.get();
        long year = DateUtils.getOrDefault(temporal, ChronoField.YEAR);
        long month = DateUtils.getOrDefault(temporal, ChronoField.MONTH_OF_YEAR);
        long day = DateUtils.getOrDefault(temporal, ChronoField.DAY_OF_MONTH);
        long hour = DateUtils.getOrDefault(temporal, ChronoField.HOUR_OF_DAY);
        long minute = DateUtils.getOrDefault(temporal, ChronoField.MINUTE_OF_HOUR);
        long second = DateUtils.getOrDefault(temporal, ChronoField.SECOND_OF_MINUTE);

        ZoneId zoneId = temporal.query(TemporalQueries.zone());
        if (zoneId != null) {
            LocalDateTime localDateTime = DateUtils.convertTimeZone(year, month, day, hour, minute, second,
                    zoneId, DateUtils.getTimeZone());
            year = localDateTime.getYear();
            month = localDateTime.getMonthValue();
            day = localDateTime.getDayOfMonth();
            hour = localDateTime.getHour();
            minute = localDateTime.getMinute();
            second = localDateTime.getSecond();
        }

        long nanoSecond = DateUtils.getOrDefault(temporal, ChronoField.NANO_OF_SECOND);
        DateTimeV2Type dateTimeV2Type = isV2 ? DateTimeV2Type.forTypeFromString(s) : null;
        long microSecond = nanoSecond / 100L;
        // Microseconds have 7 digits.
        long sevenDigit = microSecond % 10;
        microSecond = microSecond / 10;
        if (sevenDigit >= 5 && isV2) {
            DateTimeV2Literal tempLiteral = new DateTimeV2Literal(year, month, day, hour, minute, second, microSecond);
            DateTimeV2Literal result = (DateTimeV2Literal) tempLiteral.plusMicroSeconds(1);
            second = result.second;
            minute = result.minute;
            hour = result.hour;
            day = result.day;
            month = result.month;
            year = result.year;
            microSecond = result.microSecond;
        }

        if (checkRange(year, month, day) || checkDate(year, month, day)) {
            return Result.err(() -> new AnalysisException("datetime literal [" + s + "] is out of range"));
        }

        if (isV2) {
            return Result.ok(DateTimeV2Literal.create(dateTimeV2Type, year, month, day, hour, minute, second,
                    microSecond));
        } else {
            return Result.ok(new DateTimeLiteral(DateTimeType.INSTANCE, year, month, day, hour, minute, second));
        }
    }

    protected void roundMicroSecond(int scale) {
        Preconditions.checkArgument(scale >= 0 && scale <= DateTimeV2Type.MAX_SCALE,
                "invalid datetime v2 scale: %s", scale);
        long factor = (long) Math.pow(10, DateUtils.NANOSECOND_SCALE - scale);
        this.nanoSecond = (this.nanoSecond + factor / 2) / factor * factor;
        if (this.nanoSecond >= 1000000000) {
            LocalDateTime localDateTime = LocalDateTime.of((int) year, (int) month, (int) day,
                    (int) hour, (int) minute, (int) second).plusSeconds(1);
            this.year = localDateTime.getYear();
            this.month = localDateTime.getMonthValue();
            this.day = localDateTime.getDayOfMonth();
            this.hour = localDateTime.getHour();
            this.minute = localDateTime.getMinute();
            this.second = localDateTime.getSecond();
            this.nanoSecond -= 1000000000;
        }
        this.microSecond = this.nanoSecond / 1000;
        if (checkRange() || checkDate(year, month, day)) {
            // may fallback to legacy planner. make sure the behaviour of rounding is same.
            throw new AnalysisException("datetime literal [" + toString() + "] is out of range");
        }
    }

    protected void init(String s) throws AnalysisException {
        TemporalAccessor temporal = parseDateTime(s).get();

        year = DateUtils.getOrDefault(temporal, ChronoField.YEAR);
        month = DateUtils.getOrDefault(temporal, ChronoField.MONTH_OF_YEAR);
        day = DateUtils.getOrDefault(temporal, ChronoField.DAY_OF_MONTH);
        hour = DateUtils.getOrDefault(temporal, ChronoField.HOUR_OF_DAY);
        minute = DateUtils.getOrDefault(temporal, ChronoField.MINUTE_OF_HOUR);
        second = DateUtils.getOrDefault(temporal, ChronoField.SECOND_OF_MINUTE);

        ZoneId zoneId = temporal.query(TemporalQueries.zone());
        if (zoneId != null) {
            ZoneId targetZone = this.dataType instanceof TimeStampTzType ? ZoneId.of("UTC") : DateUtils.getTimeZone();
            LocalDateTime localDateTime = DateUtils.convertTimeZone(year, month, day, hour, minute, second,
                    zoneId, targetZone);
            this.year = localDateTime.getYear();
            this.month = localDateTime.getMonthValue();
            this.day = localDateTime.getDayOfMonth();
            this.hour = localDateTime.getHour();
            this.minute = localDateTime.getMinute();
            this.second = localDateTime.getSecond();
        }

        nanoSecond = DateUtils.getOrDefault(temporal, ChronoField.NANO_OF_SECOND);
        microSecond = nanoSecond / 100L;
        // Microseconds have 7 digits.
        long sevenDigit = microSecond % 10;
        microSecond = microSecond / 10;
        nanoSecond = microSecond * 1000;
        if (sevenDigit >= 5) {
            DateTimeLiteral result = this.plusMicroSeconds(1);
            this.second = result.second;
            this.minute = result.minute;
            this.hour = result.hour;
            this.day = result.day;
            this.month = result.month;
            this.year = result.year;
            this.microSecond = result.microSecond;
            this.nanoSecond = result.nanoSecond;
        }

        if (checkRange(year, month, day) || checkDate(year, month, day)) {
            throw new AnalysisException("datetime literal [" + s + "] is out of range");
        }
    }

    // When performing addition or subtraction with MicroSeconds, the precision must be set to 6 to display it
    // completely. use multiplyExact to be aware of multiplication overflow possibility.
    public DateTimeLiteral plusMicroSeconds(long microSeconds) {
        return fromJavaDateType(toJavaDateType().plusNanos(Math.multiplyExact(microSeconds, 1000L)), 6);
    }

    public boolean checkRange() {
        return checkRange(year, month, day) || hour > MAX_DATETIME.getHour() || minute > MAX_DATETIME.getMinute()
                || second > MAX_DATETIME.getSecond() || nanoSecond > MAX_NANOSECOND;
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitDateTimeLiteral(this, context);
    }

    @Override
    public Long getValue() {
        return (year * 10000 + month * 100 + day) * 1000000L + hour * 10000 + minute * 100 + second;
    }

    public long timePartToMicroSecond() {
        return ((hour * 60L + minute) * 60L + second) * 1000L * 1000L + microSecond;
    }

    @Override
    public long getTimePartInNanoseconds() {
        return ((hour * 60L + minute) * 60L + second) * 1_000_000_000L + nanoSecond;
    }

    @Override
    public long getFractionalSecondInNanoseconds() {
        return nanoSecond;
    }

    @Override
    public double getDouble() {
        return (double) getValue();
    }

    @Override
    public String computeToSql() {
        return "'" + getStringValue() + "'";
    }

    @Override
    public String toString() {
        return getStringValue();
    }

    @Override
    public String getStringValue() {
        return DateUtils.formatDateTime(year, month, day, hour, minute, second, nanoSecond, 0);
    }

    @Override
    protected String castValueToString() {
        return getStringValue();
    }

    @Override
    protected Expression uncheckedCastTo(DataType targetType) throws AnalysisException {
        if (this.dataType.equals(targetType)) {
            return this;
        }
        if (targetType.isIntegralType()) {
            if (targetType.isBigIntType()) {
                return new BigIntLiteral(getValue());
            } else if (targetType.isLargeIntType()) {
                return new LargeIntLiteral(new BigInteger(String.valueOf(getValue())));
            } else {
                throw new AnalysisException("DateTime can not cast to " + targetType);
            }
        } else if (targetType.isDateV2Type()) {
            return new DateV2Literal(year, month, day);
        } else if (targetType.isDateType()) {
            return new DateLiteral(year, month, day);
        } else if (targetType.isDateTimeV2Type()) {
            // High scale datetime to low scale datetime may overflow.
            try {
                DateTimeV2Type dateTimeV2Type = (DateTimeV2Type) targetType;
                int scale = dateTimeV2Type.getScale();
                long factor = (long) Math.pow(10, DateUtils.NANOSECOND_SCALE - scale);
                LocalDateTime roundedDateTime = LocalDateTime.of(
                                (int) year, (int) month, (int) day,
                                (int) hour, (int) minute, (int) second, (int) nanoSecond)
                        .plusNanos(factor / 2);
                if (dateTimeV2Type instanceof TimeStampNsType) {
                    return TimeStampNsLiteral.fromJavaDateType(roundedDateTime);
                }
                return DateTimeV2Literal.fromJavaDateType(roundedDateTime, dateTimeV2Type);
            } catch (AnalysisException | DateTimeException e) {
                throw new CastException(e.getMessage(), e);
            }
        } else if (targetType.isTimeType()) {
            int scale = ((TimeV2Type) targetType).getScale();
            long factor = (long) Math.pow(10, DateUtils.NANOSECOND_SCALE - scale);
            long roundedNanoSecond = (nanoSecond + factor / 2) / factor * factor;
            long totalNanoSecond = ((hour * 60 + minute) * 60 + second) * 1000000000L
                    + roundedNanoSecond;
            int resultHour = (int) (totalNanoSecond / 1000000000L / 60 / 60);
            int resultMinute = (int) (totalNanoSecond / 1000000000L / 60 % 60);
            int resultSecond = (int) (totalNanoSecond / 1000000000L % 60);
            int resultMicroSecond = (int) (totalNanoSecond % 1000000000L / 1000);
            return new TimeV2Literal(resultHour, resultMinute, resultSecond, resultMicroSecond, scale, false);
        } else if (targetType.isFloatType()) {
            return new FloatLiteral(getValue());
        } else if (targetType.isDoubleType()) {
            return new DoubleLiteral(getValue());
        }
        return super.uncheckedCastTo(targetType);
    }

    @Override
    public LiteralExpr toLegacyLiteral() {
        return new org.apache.doris.analysis.DateLiteral(year, month, day, hour, minute, second, Type.DATETIME);
    }

    public Expression plusDays(long days) {
        return fromJavaDateType(toJavaDateType().plusDays(days), 0);
    }

    public Expression plusMonths(long months) {
        return fromJavaDateType(toJavaDateType().plusMonths(months), 0);
    }

    public Expression plusWeeks(long weeks) {
        return fromJavaDateType(toJavaDateType().plusWeeks(weeks), 0);
    }

    public Expression plusYears(long years) {
        return fromJavaDateType(toJavaDateType().plusYears(years), 0);
    }

    public Expression plusHours(long hours) {
        return fromJavaDateType(toJavaDateType().plusHours(hours), 0);
    }

    public Expression plusMinutes(long minutes) {
        return fromJavaDateType(toJavaDateType().plusMinutes(minutes), 0);
    }

    public Expression plusSeconds(long seconds) {
        return fromJavaDateType(toJavaDateType().plusSeconds(seconds), 0);
    }

    public long getHour() {
        return hour;
    }

    public long getMinute() {
        return minute;
    }

    public long getSecond() {
        return second;
    }

    public long getMicroSecond() {
        return microSecond;
    }

    public long getNanoSecond() {
        return nanoSecond;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DateTimeLiteral other = (DateTimeLiteral) o;
        return Objects.equals(getValue(), other.getValue());
    }

    public LocalDateTime toJavaDateType() {
        return LocalDateTime.of(((int) getYear()), ((int) getMonth()), ((int) getDay()),
                ((int) getHour()), ((int) getMinute()), ((int) getSecond()), (int) getNanoSecond());
    }

    public static DateTimeLiteral fromJavaDateType(LocalDateTime dateTime, int precision) {
        if (isDateOutOfRange(dateTime)) {
            throw new AnalysisException("datetime out of range: " + dateTime.toString());
        }
        return new DateTimeLiteral(dateTime.getYear(), dateTime.getMonthValue(), dateTime.getDayOfMonth(),
                        dateTime.getHour(), dateTime.getMinute(), dateTime.getSecond());
    }
}
