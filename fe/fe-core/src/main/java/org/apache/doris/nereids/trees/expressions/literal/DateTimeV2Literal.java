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
import org.apache.doris.nereids.exceptions.NotSupportedException;
import org.apache.doris.nereids.exceptions.UnboundException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DateTimeType;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.TimeStampNsType;
import org.apache.doris.nereids.types.TimeStampTzType;
import org.apache.doris.nereids.util.DateUtils;

import com.google.common.base.Preconditions;

import java.time.LocalDateTime;
import java.util.Objects;

/**
 * date time v2 literal for nereids
 */
public class DateTimeV2Literal extends DateTimeLiteral {

    public static final DateTimeV2Literal USE_IN_FLOOR_CEIL
            = new DateTimeV2Literal(0001L, 01L, 01L, 0L, 0L, 0L, 0L);

    public DateTimeV2Literal(String s) {
        this(DateTimeV2Type.forTypeFromString(s), s);
    }

    public DateTimeV2Literal(DateTimeV2Type dateType, String s) {
        super(requireDateTimeV2Type(dateType), s);
        roundMicroSecond(dateType.getScale());
    }

    public DateTimeV2Literal(long year, long month, long day, long hour, long minute, long second) {
        super(DateTimeV2Type.SYSTEM_DEFAULT, year, month, day, hour, minute, second, 0);
    }

    public DateTimeV2Literal(long year, long month, long day, long hour, long minute, long second, long microSecond) {
        super(DateTimeV2Type.SYSTEM_DEFAULT, year, month, day, hour, minute, second, microSecond);
    }

    public DateTimeV2Literal(DateTimeV2Type dateType,
            long year, long month, long day, long hour, long minute, long second, long microSecond) {
        super(requireDateTimeV2Type(dateType), year, month, day, hour, minute, second, microSecond);
        roundMicroSecond(dateType.getScale());
    }

    private static DateTimeV2Type requireDateTimeV2Type(DateTimeV2Type dateType) {
        Preconditions.checkArgument(!(dateType instanceof TimeStampNsType),
                "Use TimeStampNsLiteral for TIMESTAMP_NS values");
        return dateType;
    }

    /** Create a DATETIMEV2 literal. TIMESTAMP_NS has its own literal factory. */
    public static DateTimeV2Literal create(DateTimeV2Type dateType, String value) {
        return new DateTimeV2Literal(dateType, value);
    }

    /** Create a DATETIMEV2 literal. TIMESTAMP_NS has its own literal factory. */
    public static DateTimeV2Literal create(DateTimeV2Type dateType,
            long year, long month, long day, long hour, long minute, long second, long fractionalSecond) {
        return new DateTimeV2Literal(dateType, year, month, day, hour, minute, second, fractionalSecond);
    }

    /** Create the last value of a date for a DATETIMEV2 type. */
    public static DateTimeV2Literal createEndOfDay(DateTimeV2Type dateType, long year, long month, long day) {
        long microSecond = 0;
        for (int i = 0; i < DateTimeV2Type.MAX_SCALE; i++) {
            microSecond = microSecond * 10 + (i < dateType.getScale() ? 9 : 0);
        }
        return new DateTimeV2Literal(dateType, year, month, day, 23, 59, 59, microSecond);
    }

    /** Date difference rounded toward zero by time part. */
    public static long dateDiffInDaysRoundToZeroByTime(DateLiteral lhs, DateLiteral rhs) {
        long days = DateV2Literal.dateDiffInDays(lhs, rhs);
        long nanoSecondDiff = timePartToNanoSecond(lhs) - timePartToNanoSecond(rhs);
        if (days > 0 && nanoSecondDiff < 0) {
            days--;
        } else if (days < 0 && nanoSecondDiff > 0) {
            days++;
        }
        return days;
    }

    /** Datetime difference in seconds rounded toward zero by microsecond part. */
    public static long datetimeDiffInSecondsRoundToZeroByMicroSecond(DateLiteral lhs, DateLiteral rhs) {
        return datetimeDiffInMicroSeconds(lhs, rhs) / 1000L / 1000L;
    }

    /** Datetime difference in microseconds. */
    public static long datetimeDiffInMicroSeconds(DateLiteral lhs, DateLiteral rhs) {
        long secondDiff = DateV2Literal.dateDiffInDays(lhs, rhs) * 24L * 60L * 60L
                + timePartToSecond(lhs) - timePartToSecond(rhs);
        long nanoDiff = fractionalNanoSecond(lhs) - fractionalNanoSecond(rhs);
        long result = secondDiff * 1000000L + nanoDiff / 1000L;
        if (secondDiff > 0 && nanoDiff < 0 && nanoDiff % 1000 != 0) {
            result--;
        } else if (secondDiff < 0 && nanoDiff > 0 && nanoDiff % 1000 != 0) {
            result++;
        }
        return result;
    }

    private static long timePartToNanoSecond(DateLiteral date) {
        return date.getTimePartInNanoseconds();
    }

    private static long timePartToSecond(DateLiteral date) {
        return date.getTimePartInNanoseconds() / 1_000_000_000L;
    }

    private static long fractionalNanoSecond(DateLiteral date) {
        return date.getFractionalSecondInNanoseconds();
    }

    @Override
    public DateTimeV2Type getDataType() throws UnboundException {
        return (DateTimeV2Type) super.getDataType();
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitDateTimeV2Literal(this, context);
    }

    @Override
    public LiteralExpr toLegacyLiteral() {
        return new org.apache.doris.analysis.DateLiteral(year, month, day, hour, minute, second, microSecond,
                getDataType().toCatalogDataType());
    }

    @Override
    public double getDouble() {
        return super.getDouble() + nanoSecond / 1000000000.0;
    }

    @Override
    public String toString() {
        return getStringValue();
    }

    @Override
    public String getStringValue() {
        int scale = getDataType().getScale();
        return DateUtils.formatDateTime(year, month, day, hour, minute, second, nanoSecond, scale);
    }

    @Override
    protected Expression uncheckedCastTo(DataType targetType) throws AnalysisException {
        if (this.dataType.equals(targetType)) {
            return this;
        }
        if (targetType.isDateTimeType()) {
            return new DateTimeLiteral((DateTimeType) targetType,
                    year, month, day, hour, minute, second, microSecond);
        }
        if (targetType.isTimeStampTzType()) {
            return TimestampTzLiteral.fromSessionTimeZone((TimeStampTzType) targetType, this);
        }
        return super.uncheckedCastTo(targetType);
    }

    public Expression plusDays(long days) {
        return fromJavaDateType(toJavaDateType().plusDays(days), getDataType());
    }

    /**
     * plusDaySecond
     */
    public Expression plusDaySecond(VarcharLiteral daySecond) {
        String stringValue = daySecond.getStringValue().trim();

        if (!stringValue.matches("[0-9:\\-\\s]+")) {
            throw new NotSupportedException("Invalid time format");
        }

        String[] split = stringValue.split("\\s+");
        if (split.length != 2) {
            throw new NotSupportedException("Invalid time format");
        }

        String day = split[0];
        String[] hourMinuteSecond = split[1].split(":");

        if (hourMinuteSecond.length != 3) {
            throw new NotSupportedException("Invalid time format");
        }

        try {
            long days = Long.parseLong(day);
            boolean dayPositive = days >= 0;

            long hours = Long.parseLong(hourMinuteSecond[0]);
            long minutes = Long.parseLong(hourMinuteSecond[1]);
            long seconds = Long.parseLong(hourMinuteSecond[2]);

            if (dayPositive) {
                hours = Math.abs(hours);
                minutes = Math.abs(minutes);
                seconds = Math.abs(seconds);
            } else {
                hours = -Math.abs(hours);
                minutes = -Math.abs(minutes);
                seconds = -Math.abs(seconds);
            }

            return fromJavaDateType(toJavaDateType()
                .plusDays(days)
                .plusHours(hours)
                .plusMinutes(minutes)
                .plusSeconds(seconds), getDataType());
        } catch (NumberFormatException e) {
            throw new NotSupportedException("Invalid time format");
        }
    }

    public Expression plusMonths(long months) {
        return fromJavaDateType(toJavaDateType().plusMonths(months), getDataType());
    }

    public Expression plusWeeks(long weeks) {
        return fromJavaDateType(toJavaDateType().plusWeeks(weeks), getDataType());
    }

    public Expression plusYears(long years) {
        return fromJavaDateType(toJavaDateType().plusYears(years), getDataType());
    }

    public Expression plusHours(long hours) {
        return fromJavaDateType(toJavaDateType().plusHours(hours), getDataType());
    }

    public Expression plusMinutes(long minutes) {
        return fromJavaDateType(toJavaDateType().plusMinutes(minutes), getDataType());
    }

    public Expression plusSeconds(long seconds) {
        return fromJavaDateType(toJavaDateType().plusSeconds(seconds), getDataType());
    }

    /**
     * plusDaySecond
     */
    public Expression plusDayHour(VarcharLiteral dayHour) {
        String stringValue = dayHour.getStringValue().trim();

        if (!stringValue.matches("[0-9\\-\\s]+")) {
            throw new NotSupportedException("Invalid time format");
        }

        String[] split = stringValue.split("\\s+");
        if (split.length != 2) {
            throw new NotSupportedException("Invalid time format");
        }

        String day = split[0];
        String hour = split[1];

        try {
            long days = Long.parseLong(day);
            boolean dayPositive = days >= 0;

            long hours = Long.parseLong(hour);

            if (dayPositive) {
                hours = Math.abs(hours);
            } else {
                hours = -Math.abs(hours);
            }

            return fromJavaDateType(toJavaDateType()
                .plusDays(days)
                .plusHours(hours), getDataType());
        } catch (NumberFormatException e) {
            throw new NotSupportedException("Invalid time format");
        }
    }

    /**
     * plusMinuteSecond
     */
    public Expression plusMinuteSecond(VarcharLiteral minuteSecond) {
        String stringValue = minuteSecond.getStringValue().trim();

        if (!stringValue.matches("[0-9\\-:\\s]+")) {
            throw new NotSupportedException("Invalid time format");
        }

        String[] split = stringValue.split(":");
        if (split.length != 2) {
            throw new NotSupportedException("Invalid time format");
        }

        String minute = split[0].trim();
        String second = split[1].trim();

        try {
            long minutes = Long.parseLong(minute);
            boolean minutePositive = minutes >= 0;

            long seconds = Long.parseLong(second);

            if (minutePositive) {
                seconds = Math.abs(seconds);
            } else {
                seconds = -Math.abs(seconds);
            }

            return fromJavaDateType(toJavaDateType()
                .plusMinutes(minutes)
                .plusSeconds(seconds), getDataType());
        } catch (NumberFormatException e) {
            throw new NotSupportedException("Invalid time format");
        }
    }

    /**
     * plusSecondMicrosecond
     */
    public Expression plusSecondMicrosecond(VarcharLiteral secondMicrosecond) {
        String stringValue = secondMicrosecond.getStringValue().trim();

        if (!stringValue.matches("[0-9\\-\\.\\s]+")) {
            throw new NotSupportedException("Invalid time format");
        }

        String[] split = stringValue.split("\\.");
        if (split.length != 2) {
            throw new NotSupportedException("Invalid time format");
        }

        String second = split[0].trim();
        String microsecond = split[1].trim();

        try {
            long seconds = Long.parseLong(second);
            boolean secondPositive = seconds >= 0;

            long microseconds = Long.parseLong(microsecond);
            int microsecondLen = microsecond.startsWith("-") ? microsecond.length() - 1 : microsecond.length();
            if (microsecondLen < 6) {
                microseconds *= Math.pow(10, 6 - microsecondLen);
            }

            if (secondPositive) {
                microseconds = Math.abs(microseconds);
            } else {
                microseconds = -Math.abs(microseconds);
            }

            return fromJavaDateType(toJavaDateType()
                .plusSeconds(seconds)
                .plusNanos(Math.multiplyExact(microseconds, 1000L)), getDataType());
        } catch (NumberFormatException e) {
            throw new NotSupportedException("Invalid time format");
        }
    }

    // When performing addition or subtraction with MicroSeconds, the precision must be set to 6 to display it
    // completely. use multiplyExact to be aware of multiplication overflow possibility.
    public DateTimeV2Literal plusMicroSeconds(long microSeconds) {
        return fromJavaDateType(
                toJavaDateType().plusNanos(Math.multiplyExact(microSeconds, 1000L)),
                DateTimeV2Type.MAX);
    }

    public Expression plusMilliSeconds(long microSeconds) {
        return plusMicroSeconds(Math.multiplyExact(microSeconds, 1000L));
    }

    public int getScale() {
        return ((DateTimeV2Type) dataType).getScale();
    }

    public DateTimeV2Type commonType(DateTimeV2Literal other) {
        return DateTimeV2Type.getWiderDatetimeV2Type(getDataType(), other.getDataType());
    }

    /**
     * roundCeiling
     */
    public DateTimeV2Literal roundCeiling(int newScale) {
        DateTimeV2Type targetType = DateTimeV2Type.of(newScale);
        int targetScale = targetType.getScale();
        long factor = (long) Math.pow(10, DateUtils.NANOSECOND_SCALE - targetScale);
        long remain = nanoSecond % factor;
        long newNanoSecond = nanoSecond;
        long newSecond = second;
        long newMinute = minute;
        long newHour = hour;
        long newDay = day;
        long newMonth = month;
        long newYear = year;
        if (remain != 0) {
            newNanoSecond = (nanoSecond + factor) / factor * factor;
        }
        if (newNanoSecond > MAX_NANOSECOND) {
            newNanoSecond = 0;
            Expression plus1Second = this.plusSeconds(1);
            if (plus1Second.isNullLiteral()) {
                throw new AnalysisException("round ceil datetime literal (" + toString() + ", "
                        + newScale + ") is out of range");
            }
            DateTimeV2Literal result = (DateTimeV2Literal) plus1Second;
            newSecond = result.second;
            newMinute = result.minute;
            newHour = result.hour;
            newDay = result.day;
            newMonth = result.month;
            newYear = result.year;
        }
        return create(targetType, newYear, newMonth, newDay,
                newHour, newMinute, newSecond, newNanoSecond / 1000);
    }

    /** Round down the fractional second to the requested normalized datetime scale. */
    public DateTimeV2Literal roundFloor(int newScale) {
        DateTimeV2Type targetType = DateTimeV2Type.of(newScale);
        int targetScale = targetType.getScale();
        long factor = (long) Math.pow(10, DateUtils.NANOSECOND_SCALE - targetScale);
        long newNanoSecond = nanoSecond / factor * factor;
        return create(targetType, year, month, day, hour, minute, second,
                newNanoSecond / 1000);
    }

    public static Expression fromJavaDateType(LocalDateTime dateTime) {
        return fromJavaDateType(dateTime, 6);
    }

    /**
     * convert java LocalDateTime object to DateTimeV2Literal object.
     */
    public static DateTimeV2Literal fromJavaDateType(LocalDateTime dateTime, int precision) {
        return fromJavaDateType(dateTime, DateTimeV2Type.of(precision));
    }

    /** Convert a Java datetime to the explicit DATETIMEV2 target type. */
    public static DateTimeV2Literal fromJavaDateType(LocalDateTime dateTime, DateTimeV2Type targetType) {
        requireDateTimeV2Type(targetType);
        int targetScale = targetType.getScale();
        long factor = (long) Math.pow(10, DateUtils.NANOSECOND_SCALE - targetScale);
        if (isDateOutOfRange(dateTime)) {
            throw new AnalysisException("datetime out of range" + dateTime.toString());
        }
        long nanoSecond = dateTime.getNano() / factor * factor;
        return create(targetType, dateTime.getYear(),
                        dateTime.getMonthValue(), dateTime.getDayOfMonth(), dateTime.getHour(),
                        dateTime.getMinute(), dateTime.getSecond(), nanoSecond / 1000);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof DateTimeV2Literal)) {
            return false;
        }
        DateTimeV2Literal literal = (DateTimeV2Literal) o;
        return Objects.equals(getValue(), literal.getValue())
                && Objects.equals(dataType, literal.dataType)
                && Objects.equals(nanoSecond, literal.nanoSecond);
    }
}
