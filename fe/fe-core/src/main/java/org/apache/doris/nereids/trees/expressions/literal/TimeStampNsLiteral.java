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
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.TimeStampNsType;
import org.apache.doris.nereids.util.DateUtils;

import java.time.LocalDateTime;
import java.time.ZoneId;
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
        TemporalAccessor temporal = parseDateTime(value).get();
        long year = DateUtils.getOrDefault(temporal, ChronoField.YEAR);
        long month = DateUtils.getOrDefault(temporal, ChronoField.MONTH_OF_YEAR);
        long day = DateUtils.getOrDefault(temporal, ChronoField.DAY_OF_MONTH);
        long hour = DateUtils.getOrDefault(temporal, ChronoField.HOUR_OF_DAY);
        long minute = DateUtils.getOrDefault(temporal, ChronoField.MINUTE_OF_HOUR);
        long second = DateUtils.getOrDefault(temporal, ChronoField.SECOND_OF_MINUTE);

        ZoneId zoneId = temporal.query(TemporalQueries.zone());
        if (zoneId != null) {
            LocalDateTime converted = DateUtils.convertTimeZone(
                    year, month, day, hour, minute, second, zoneId, DateUtils.getTimeZone());
            year = converted.getYear();
            month = converted.getMonthValue();
            day = converted.getDayOfMonth();
            hour = converted.getHour();
            minute = converted.getMinute();
            second = converted.getSecond();
        }

        LocalDateTime result = LocalDateTime.of((int) year, (int) month, (int) day,
                (int) hour, (int) minute, (int) second,
                DateUtils.getOrDefault(temporal, ChronoField.NANO_OF_SECOND));
        if (DateUtils.getNanosecondGuardDigit(value) >= 5) {
            result = result.plusNanos(1);
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

    /** Return whether the civil fields are invalid or outside the signed epoch-nanosecond range. */
    public boolean checkRange() {
        if (checkRange(year, month, day) || month < 1 || day < 1 || checkDate(year, month, day)
                || hour < 0 || hour > 23 || minute < 0 || minute > 59
                || second < 0 || second > 59 || nanosecond < 0 || nanosecond > MAX_NANOSECOND) {
            return true;
        }
        LocalDateTime value = toJavaDateType();
        return value.isBefore(MIN_VALUE) || value.isAfter(MAX_VALUE);
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
