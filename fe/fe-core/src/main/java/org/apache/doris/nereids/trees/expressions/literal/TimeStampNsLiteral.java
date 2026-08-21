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
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DateTimeType;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.TimeStampNsType;
import org.apache.doris.nereids.types.TimeStampTzType;
import org.apache.doris.nereids.types.TimeV2Type;
import org.apache.doris.nereids.util.DateUtils;

import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalQueries;
import java.util.Objects;

/** Literal for the fixed nanosecond-precision TIMESTAMP_NS type. */
public final class TimeStampNsLiteral extends DateLiteral {
    private static final long NANOS_PER_SECOND = 1_000_000_000L;
    private static final long MAX_NANOSECOND = NANOS_PER_SECOND - 1;
    private static final LocalDateTime MIN_VALUE
            = LocalDateTime.of(1677, 9, 21, 0, 12, 43, 145224192);
    private static final LocalDateTime MAX_VALUE
            = LocalDateTime.of(2262, 4, 11, 23, 47, 16, 854775807);

    private final long hour;
    private final long minute;
    private final long second;
    private final long nanosecond;

    public TimeStampNsLiteral(String value) {
        this(parse(value));
    }

    /** Construct a TIMESTAMP_NS literal from civil datetime fields. */
    public TimeStampNsLiteral(long year, long month, long day, long hour, long minute, long second,
            long nanosecond) {
        super(TimeStampNsType.INSTANCE, year, month, day);
        this.hour = hour;
        this.minute = minute;
        this.second = second;
        this.nanosecond = nanosecond;
        if (checkRange()) {
            throw new AnalysisException("timestamp_ns literal [" + toString()
                    + "] is outside Int64 epoch nanosecond range");
        }
    }

    private TimeStampNsLiteral(LocalDateTime value) {
        this(value.getYear(), value.getMonthValue(), value.getDayOfMonth(),
                value.getHour(), value.getMinute(), value.getSecond(), value.getNano());
    }

    private static LocalDateTime parse(String value) {
        TemporalAccessor temporal = parseDateTime(value, DateUtils.NANOSECOND_SCALE + 1).get();
        long year = DateUtils.getOrDefault(temporal, ChronoField.YEAR);
        long month = DateUtils.getOrDefault(temporal, ChronoField.MONTH_OF_YEAR);
        long day = DateUtils.getOrDefault(temporal, ChronoField.DAY_OF_MONTH);
        long hour = DateUtils.getOrDefault(temporal, ChronoField.HOUR_OF_DAY);
        long minute = DateUtils.getOrDefault(temporal, ChronoField.MINUTE_OF_HOUR);
        long second = DateUtils.getOrDefault(temporal, ChronoField.SECOND_OF_MINUTE);

        LocalDateTime result = LocalDateTime.of((int) year, (int) month, (int) day,
                (int) hour, (int) minute, (int) second,
                DateUtils.getOrDefault(temporal, ChronoField.NANO_OF_SECOND));
        if (DateUtils.getNanosecondGuardDigit(value) >= 5) {
            result = result.plusNanos(1);
        }

        ZoneId zoneId = temporal.query(TemporalQueries.zone());
        if (zoneId != null) {
            Instant instant = DateUtils.convertLocalToInstant(result, zoneId);
            result = LocalDateTime.ofInstant(instant, DateUtils.getTimeZone());
        }
        return result;
    }

    public static TimeStampNsLiteral getMinValue() {
        return new TimeStampNsLiteral(MIN_VALUE);
    }

    public static TimeStampNsLiteral getMaxValue() {
        return new TimeStampNsLiteral(MAX_VALUE);
    }

    public static TimeStampNsLiteral createEndOfDay(long year, long month, long day) {
        return new TimeStampNsLiteral(year, month, day, 23, 59, 59, MAX_NANOSECOND);
    }

    public static TimeStampNsLiteral fromJavaDateType(LocalDateTime dateTime) {
        return new TimeStampNsLiteral(dateTime);
    }

    /** Return whether a civil datetime is representable as a signed epoch-nanosecond value. */
    public static boolean isInRange(LocalDateTime value) {
        return !value.isBefore(MIN_VALUE) && !value.isAfter(MAX_VALUE);
    }

    /** Return whether the civil fields are invalid or outside the signed epoch-nanosecond range. */
    public boolean checkRange() {
        if (checkRange(year, month, day) || month < 1 || day < 1 || checkDate(year, month, day)
                || hour < 0 || hour > 23 || minute < 0 || minute > 59
                || second < 0 || second > 59 || nanosecond < 0 || nanosecond > MAX_NANOSECOND) {
            return true;
        }
        return !isInRange(toJavaDateType());
    }

    @Override
    public boolean isMidnight() {
        return hour == 0 && minute == 0 && second == 0 && nanosecond == 0;
    }

    @Override
    public TimeStampNsType getDataType() {
        return TimeStampNsType.INSTANCE;
    }

    @Override
    public Long getValue() {
        return (year * 10000 + month * 100 + day) * 1000000L + hour * 10000 + minute * 100 + second;
    }

    @Override
    public long getTimePartInNanoseconds() {
        return ((hour * 60L + minute) * 60L + second) * NANOS_PER_SECOND + nanosecond;
    }

    @Override
    public long getFractionalSecondInNanoseconds() {
        return nanosecond;
    }

    @Override
    public double getDouble() {
        return getValue() + nanosecond / (double) NANOS_PER_SECOND;
    }

    @Override
    public String getStringValue() {
        return DateUtils.formatDateTime(year, month, day, hour, minute, second,
                nanosecond, TimeStampNsType.SCALE);
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitTimeStampNsLiteral(this, context);
    }

    @Override
    public LiteralExpr toLegacyLiteral() {
        return new org.apache.doris.analysis.TimeStampNsLiteral(
                year, month, day, hour, minute, second, nanosecond);
    }

    public DateTimeV2Literal roundFloorToDateTimeV2(int scale) {
        long factor = (long) Math.pow(10, DateUtils.NANOSECOND_SCALE - scale);
        LocalDateTime rounded = toJavaDateType().withNano((int) (nanosecond / factor * factor));
        return DateTimeV2Literal.fromJavaDateType(rounded, scale);
    }

    public DateTimeV2Literal roundCeilingToDateTimeV2(int scale) {
        long factor = (long) Math.pow(10, DateUtils.NANOSECOND_SCALE - scale);
        long remainder = nanosecond % factor;
        LocalDateTime rounded = remainder == 0 ? toJavaDateType()
                : toJavaDateType().plusNanos(factor - remainder);
        return DateTimeV2Literal.fromJavaDateType(rounded, scale);
    }

    @Override
    public LocalDateTime toJavaDateType() {
        return LocalDateTime.of((int) year, (int) month, (int) day,
                (int) hour, (int) minute, (int) second, (int) nanosecond);
    }

    @Override
    public TimeStampNsLiteral plusDays(long days) {
        return fromJavaDateType(toJavaDateType().plusDays(days));
    }

    @Override
    public TimeStampNsLiteral plusMonths(long months) {
        return fromJavaDateType(toJavaDateType().plusMonths(months));
    }

    @Override
    public TimeStampNsLiteral plusWeeks(long weeks) {
        return fromJavaDateType(toJavaDateType().plusWeeks(weeks));
    }

    @Override
    public TimeStampNsLiteral plusYears(long years) {
        return fromJavaDateType(toJavaDateType().plusYears(years));
    }

    public TimeStampNsLiteral plusHours(long hours) {
        return fromJavaDateType(toJavaDateType().plusHours(hours));
    }

    public TimeStampNsLiteral plusMinutes(long minutes) {
        return fromJavaDateType(toJavaDateType().plusMinutes(minutes));
    }

    public TimeStampNsLiteral plusSeconds(long seconds) {
        return fromJavaDateType(toJavaDateType().plusSeconds(seconds));
    }

    public TimeStampNsLiteral plusMicroSeconds(long microSeconds) {
        return fromJavaDateType(toJavaDateType().plusNanos(Math.multiplyExact(microSeconds, 1000L)));
    }

    public TimeStampNsLiteral plusMilliSeconds(long milliSeconds) {
        return plusMicroSeconds(Math.multiplyExact(milliSeconds, 1000L));
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
        return nanosecond / 1000;
    }

    public long getNanoSecond() {
        return nanosecond;
    }

    public int getScale() {
        return TimeStampNsType.SCALE;
    }

    @Override
    protected Expression uncheckedCastTo(DataType targetType) throws AnalysisException {
        if (getDataType().equals(targetType)) {
            return this;
        }
        if (targetType.isBigIntType()) {
            return new BigIntLiteral(getValue());
        }
        if (targetType.isLargeIntType()) {
            return new LargeIntLiteral(new BigInteger(String.valueOf(getValue())));
        }
        if (targetType.isDateType()) {
            return new DateLiteral(year, month, day);
        }
        if (targetType.isDateV2Type()) {
            return new DateV2Literal(year, month, day);
        }
        if (targetType.isDateTimeType()) {
            return new DateTimeLiteral((DateTimeType) targetType,
                    year, month, day, hour, minute, second, 0);
        }
        if (targetType instanceof DateTimeV2Type) {
            DateTimeV2Type dateTimeV2Type = (DateTimeV2Type) targetType;
            LocalDateTime rounded = roundToScale(dateTimeV2Type.getScale());
            return DateTimeV2Literal.fromJavaDateType(rounded, dateTimeV2Type.getScale());
        }
        if (targetType.isTimeType()) {
            int scale = ((TimeV2Type) targetType).getScale();
            long factor = (long) Math.pow(10, DateUtils.NANOSECOND_SCALE - scale);
            long timeNanos = getTimePartInNanoseconds() + factor / 2;
            int resultHour = (int) (timeNanos / NANOS_PER_SECOND / 60 / 60);
            int resultMinute = (int) (timeNanos / NANOS_PER_SECOND / 60 % 60);
            int resultSecond = (int) (timeNanos / NANOS_PER_SECOND % 60);
            int resultMicroSecond = (int) (timeNanos % NANOS_PER_SECOND / 1000 / (factor / 1000)
                    * (factor / 1000));
            return new TimeV2Literal(resultHour, resultMinute, resultSecond,
                    resultMicroSecond, scale, false);
        }
        if (targetType.isTimeStampTzType()) {
            int scale = ((TimeStampTzType) targetType).getScale();
            LocalDateTime rounded = roundToScale(scale);
            Instant instant = DateTimeLiteral.convertLocalToInstantPreservingFraction(
                    rounded, DateUtils.getTimeZone());
            LocalDateTime utc = LocalDateTime.ofInstant(instant, ZoneOffset.UTC);
            return new TimestampTzLiteral((TimeStampTzType) targetType,
                    utc.getYear(), utc.getMonthValue(), utc.getDayOfMonth(), utc.getHour(),
                    utc.getMinute(), utc.getSecond(), utc.getNano() / 1000);
        }
        if (targetType.isFloatType()) {
            return new FloatLiteral(getValue());
        }
        if (targetType.isDoubleType()) {
            return new DoubleLiteral(getValue());
        }
        if (targetType.isIntegralType()) {
            throw new AnalysisException("TimestampNs can not cast to " + targetType);
        }
        return super.uncheckedCastTo(targetType);
    }

    private LocalDateTime roundToScale(int scale) {
        long factor = (long) Math.pow(10, DateUtils.NANOSECOND_SCALE - scale);
        LocalDateTime rounded = toJavaDateType().plusNanos(factor / 2);
        return rounded.withNano((int) (rounded.getNano() / factor * factor));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof TimeStampNsLiteral)) {
            return false;
        }
        TimeStampNsLiteral literal = (TimeStampNsLiteral) o;
        return Objects.equals(getValue(), literal.getValue()) && nanosecond == literal.nanosecond;
    }

    @Override
    protected int computeHashCode() {
        return Objects.hash(getValue(), nanosecond);
    }
}
