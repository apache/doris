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

package org.apache.doris.analysis;

import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;

import com.google.common.base.Preconditions;
import com.google.gson.annotations.SerializedName;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.DateTimeException;
import java.time.LocalDateTime;
import java.time.Year;
import java.time.ZoneOffset;

/**
 * Legacy literal for the fixed nanosecond-precision TIMESTAMP_NS type.
 *
 * <p>TIMESTAMP_NS owns its value and range logic instead of inheriting the unrelated calendar
 * encodings and microsecond rules in {@link DateLiteral}.</p>
 */
public final class TimeStampNsLiteral extends LiteralExpr {
    private static final long NANOSECONDS_PER_SECOND = 1_000_000_000L;
    private static final long MAX_NANOSECOND = NANOSECONDS_PER_SECOND - 1;
    private static final int MIN_YEAR = 1677;
    private static final int MAX_YEAR = 2262;
    private static final LocalDateTime MIN_VALUE
            = LocalDateTime.of(MIN_YEAR, 9, 21, 0, 12, 43, 145224192);
    private static final LocalDateTime MAX_VALUE
            = LocalDateTime.of(MAX_YEAR, 4, 11, 23, 47, 16, 854775807);

    @SerializedName("y")
    private long year;
    @SerializedName("m")
    private long month;
    @SerializedName("d")
    private long day;
    @SerializedName("h")
    private long hour;
    @SerializedName("M")
    private long minute;
    @SerializedName("s")
    private long second;
    @SerializedName("ns")
    private long nanosecond;
    @SerializedName("inf")
    private boolean isMinInfinity;

    public TimeStampNsLiteral() {
        type = Type.TIMESTAMP_NS;
        nullable = false;
    }

    public TimeStampNsLiteral(boolean isMax) {
        this(isMax ? MAX_VALUE : MIN_VALUE);
        isMinInfinity = !isMax;
    }

    public TimeStampNsLiteral(long year, long month, long day, long hour, long minute, long second,
            long nanosecond) {
        this();
        this.year = year;
        this.month = month;
        this.day = day;
        this.hour = hour;
        this.minute = minute;
        this.second = second;
        this.nanosecond = nanosecond;
        this.isMinInfinity = false;
    }

    public TimeStampNsLiteral(LocalDateTime value) {
        this(value.getYear(), value.getMonthValue(), value.getDayOfMonth(), value.getHour(),
                value.getMinute(), value.getSecond(), value.getNano());
    }

    private TimeStampNsLiteral(TimeStampNsLiteral other) {
        super(other);
        year = other.year;
        month = other.month;
        day = other.day;
        hour = other.hour;
        minute = other.minute;
        second = other.second;
        nanosecond = other.nanosecond;
        isMinInfinity = other.isMinInfinity;
        type = Type.TIMESTAMP_NS;
    }

    public static TimeStampNsLiteral createMinValue() {
        return new TimeStampNsLiteral(false);
    }

    @Override
    public Expr clone() {
        return new TimeStampNsLiteral(this);
    }

    @Override
    public boolean isMinValue() {
        return isMinInfinity;
    }

    @Override
    public Object getRealValue() {
        LocalDateTime value = toLocalDateTime();
        return BigInteger.valueOf(value.toEpochSecond(ZoneOffset.UTC))
                .multiply(BigInteger.valueOf(NANOSECONDS_PER_SECOND))
                .add(BigInteger.valueOf(nanosecond))
                .longValueExact();
    }

    @Override
    public ByteBuffer getHashValue(PrimitiveType primitiveType) {
        Preconditions.checkArgument(primitiveType == PrimitiveType.TIMESTAMP_NS,
                "Expected TIMESTAMP_NS hash type, but got %s", primitiveType);
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES).order(ByteOrder.LITTLE_ENDIAN);
        buffer.putLong((long) getRealValue());
        return buffer;
    }

    @Override
    public int compareLiteral(LiteralExpr expr) {
        if (expr instanceof PlaceHolderExpr) {
            return compareLiteral(((PlaceHolderExpr) expr).getLiteral());
        }
        if (expr instanceof NullLiteral) {
            return 1;
        }
        if (expr == MaxLiteral.MAX_VALUE) {
            return -1;
        }
        if (expr instanceof TimeStampNsLiteral) {
            return compareDateTime((TimeStampNsLiteral) expr);
        }
        if (expr instanceof DateLiteral) {
            if (isMinValue()) {
                return -1;
            }
            DateLiteral other = (DateLiteral) expr;
            int result = compareDateTime(other.getYear(), other.getMonth(), other.getDay(),
                    other.getHour(), other.getMinute(), other.getSecond(), other.getMicrosecond() * 1000);
            if (result == 0 && other.isDateType()) {
                // DateLiteral orders DATE/DATEV2 before datetime-like literals on the same day.
                // Preserve that ordering and comparison symmetry across the two literal classes.
                return 1;
            }
            return result;
        }
        return Integer.signum(getStringValue().compareTo(expr.getStringValue()));
    }

    private int compareDateTime(TimeStampNsLiteral other) {
        if (isMinValue() != other.isMinValue()) {
            return isMinValue() ? -1 : 1;
        }
        return compareDateTime(other.year, other.month, other.day, other.hour, other.minute,
                other.second, other.nanosecond);
    }

    private int compareDateTime(long otherYear, long otherMonth, long otherDay, long otherHour,
            long otherMinute, long otherSecond, long otherNanosecond) {
        int result = Long.compare(year, otherYear);
        if (result == 0) {
            result = Long.compare(month, otherMonth);
        }
        if (result == 0) {
            result = Long.compare(day, otherDay);
        }
        if (result == 0) {
            result = Long.compare(hour, otherHour);
        }
        if (result == 0) {
            result = Long.compare(minute, otherMinute);
        }
        if (result == 0) {
            result = Long.compare(second, otherSecond);
        }
        if (result == 0) {
            result = Long.compare(nanosecond, otherNanosecond);
        }
        return result;
    }

    @Override
    public String getStringValue() {
        return String.format("%04d-%02d-%02d %02d:%02d:%02d.%09d",
                year, month, day, hour, minute, second, nanosecond);
    }

    public void roundFloor(int newScale) {
        Preconditions.checkArgument(newScale == ScalarType.TIMESTAMP_NS_SCALE,
                "TIMESTAMP_NS has fixed scale %s, but got %s",
                ScalarType.TIMESTAMP_NS_SCALE, newScale);
    }

    @Override
    public void checkValueValid() throws AnalysisException {
        if (year < MIN_YEAR || year > MAX_YEAR) {
            throw new AnalysisException("TimeStampNsLiteral has invalid year value: " + year);
        }
        if (month < 1 || month > 12) {
            throw new AnalysisException("TimeStampNsLiteral has invalid month value: " + month);
        }
        if (day < 1 || day > daysInMonth(year, month)) {
            throw new AnalysisException("TimeStampNsLiteral has invalid day value: " + day);
        }
        if (hour < 0 || hour > 23) {
            throw new AnalysisException("TimeStampNsLiteral has invalid hour value: " + hour);
        }
        if (minute < 0 || minute > 59) {
            throw new AnalysisException("TimeStampNsLiteral has invalid minute value: " + minute);
        }
        if (second < 0 || second > 59) {
            throw new AnalysisException("TimeStampNsLiteral has invalid second value: " + second);
        }
        if (nanosecond < 0 || nanosecond > MAX_NANOSECOND) {
            throw new AnalysisException("TimeStampNsLiteral has invalid nanosecond value: " + nanosecond);
        }
        if (checkRange()) {
            throw new AnalysisException("TimeStampNsLiteral is outside Int64 epoch nanosecond range: "
                    + getStringValue());
        }
    }

    boolean checkRange() {
        if (year < MIN_YEAR || year > MAX_YEAR || month < 1 || month > 12
                || hour < 0 || hour > 23 || minute < 0 || minute > 59
                || second < 0 || second > 59) {
            return true;
        }
        if (day < 1 || day > daysInMonth(year, month)) {
            return true;
        }
        if (nanosecond < 0 || nanosecond > MAX_NANOSECOND) {
            return true;
        }
        return compareBoundary(MIN_VALUE) < 0 || compareBoundary(MAX_VALUE) > 0;
    }

    private int compareBoundary(LocalDateTime boundary) {
        return compareDateTime(boundary.getYear(), boundary.getMonthValue(), boundary.getDayOfMonth(),
                boundary.getHour(), boundary.getMinute(), boundary.getSecond(), boundary.getNano());
    }

    private static int daysInMonth(long year, long month) {
        switch ((int) month) {
            case 2:
                return Year.isLeap(year) ? 29 : 28;
            case 4:
            case 6:
            case 9:
            case 11:
                return 30;
            default:
                return 31;
        }
    }

    public LocalDateTime toLocalDateTime() {
        try {
            return LocalDateTime.of((int) year, (int) month, (int) day, (int) hour,
                    (int) minute, (int) second, (int) nanosecond);
        } catch (DateTimeException e) {
            throw new IllegalStateException("Invalid TIMESTAMP_NS literal: " + getStringValue(), e);
        }
    }

    public TimeStampNsLiteral plusYears(long years) {
        return new TimeStampNsLiteral(toLocalDateTime().plusYears(years));
    }

    public TimeStampNsLiteral plusMonths(long months) {
        return new TimeStampNsLiteral(toLocalDateTime().plusMonths(months));
    }

    public TimeStampNsLiteral plusDays(long days) {
        return new TimeStampNsLiteral(toLocalDateTime().plusDays(days));
    }

    public TimeStampNsLiteral plusHours(long hours) {
        return new TimeStampNsLiteral(toLocalDateTime().plusHours(hours));
    }

    public TimeStampNsLiteral plusMinutes(long minutes) {
        return new TimeStampNsLiteral(toLocalDateTime().plusMinutes(minutes));
    }

    public TimeStampNsLiteral plusSeconds(long seconds) {
        return new TimeStampNsLiteral(toLocalDateTime().plusSeconds(seconds));
    }

    public long getYear() {
        return year;
    }

    public long getMonth() {
        return month;
    }

    public long getDay() {
        return day;
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

    public long getMicrosecond() {
        return nanosecond / 1000;
    }

    public long getNanosecond() {
        return nanosecond;
    }

    @Override
    public long getLongValue() {
        return (year * 10000 + month * 100 + day) * 1000000L + hour * 10000 + minute * 100 + second;
    }

    @Override
    public double getDoubleValue() {
        return getLongValue();
    }

    public double getDoubleValueAsDateTime() {
        return getDoubleValue();
    }

    @Override
    public int hashCode() {
        int legacyHash = Long.hashCode(getLongValue());
        if (nanosecond % 1000 == 0) {
            return isMinValue() ? 31 * legacyHash + 1 : legacyHash;
        }
        int hash = 31 * legacyHash + Long.hashCode(nanosecond);
        return isMinValue() ? 31 * hash + 1 : hash;
    }

    @Override
    public <R, C> R accept(ExprVisitor<R, C> visitor, C context) {
        return visitor.visitTimeStampNsLiteral(this, context);
    }
}
