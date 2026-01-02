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
import org.apache.doris.nereids.trees.expressions.functions.executable.DateTimeExtractAndTransform;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.TimeStampTzType;
import org.apache.doris.qe.ConnectContext;

import java.time.LocalDateTime;
import java.util.Objects;

/**
 * timstamptz literal for nereids
 */
public class TimestampTzLiteral extends DateTimeLiteral {

    public static final TimestampTzLiteral USE_IN_FLOOR_CEIL
            = new TimestampTzLiteral(0001L, 01L, 01L, 0L, 0L, 0L, 0L);

    public TimestampTzLiteral(String s) {
        this(TimeStampTzType.forTypeFromString(s), s);
    }

    public TimestampTzLiteral(TimeStampTzType dateType, String s) {
        super(dateType, s);
        roundMicroSecond(dateType.getScale());
    }

    public TimestampTzLiteral(long year, long month, long day, long hour, long minute, long second) {
        super(TimeStampTzType.SYSTEM_DEFAULT, year, month, day, hour, minute, second, 0);
    }

    public TimestampTzLiteral(long year, long month, long day, long hour, long minute, long second, long microSecond) {
        super(TimeStampTzType.forTypeFromMicroSeconds(microSecond),
                year, month, day, hour, minute, second, microSecond);
    }

    public TimestampTzLiteral(TimeStampTzType dateType,
            long year, long month, long day, long hour, long minute, long second, long microSecond) {
        super(dateType, year, month, day, hour, minute, second, microSecond);
        roundMicroSecond(dateType.getScale());
    }

    @Override
    public TimeStampTzType getDataType() throws UnboundException {
        return (TimeStampTzType) super.getDataType();
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitTimestampTzLiteral(this, context);
    }

    @Override
    public LiteralExpr toLegacyLiteral() {
        return new org.apache.doris.analysis.DateLiteral(year, month, day, hour, minute, second, microSecond,
                getDataType().toCatalogDataType());
    }

    @Override
    public double getDouble() {
        return super.getDouble() + microSecond / 1000000.0;
    }

    @Override
    public String toString() {
        return getStringValue();
    }

    @Override
    public String getStringValue() {
        int scale = getDataType().getScale();
        if (scale <= 0) {
            return super.getStringValue();
        }

        if (0 <= year && year <= 9999 && 0 <= month && month <= 99 && 0 <= day && day <= 99
                && 0 <= hour && hour <= 99 && 0 <= minute && minute <= 99 && 0 <= second && second <= 99
                && 0 <= microSecond && microSecond <= MAX_MICROSECOND) {
            char[] format = new char[] {
                    '0', '0', '0', '0', '-', '0', '0', '-', '0', '0', ' ', '0', '0', ':', '0', '0', ':', '0', '0',
                    '.', '0', '0', '0', '0', '0', '0'};
            int offset = 3;
            long year = this.year;
            while (year > 0) {
                format[offset--] = (char) ('0' + (year % 10));
                year /= 10;
            }

            offset = 6;
            long month = this.month;
            while (month > 0) {
                format[offset--] = (char) ('0' + (month % 10));
                month /= 10;
            }

            offset = 9;
            long day = this.day;
            while (day > 0) {
                format[offset--] = (char) ('0' + (day % 10));
                day /= 10;
            }

            offset = 12;
            long hour = this.hour;
            while (hour > 0) {
                format[offset--] = (char) ('0' + (hour % 10));
                hour /= 10;
            }

            offset = 15;
            long minute = this.minute;
            while (minute > 0) {
                format[offset--] = (char) ('0' + (minute % 10));
                minute /= 10;
            }

            offset = 18;
            long second = this.second;
            while (second > 0) {
                format[offset--] = (char) ('0' + (second % 10));
                second /= 10;
            }

            offset = 19 + scale;
            long microSecond = (int) (this.microSecond / Math.pow(10, TimeStampTzType.MAX_SCALE - scale));
            while (microSecond > 0) {
                format[offset--] = (char) ('0' + (microSecond % 10));
                microSecond /= 10;
            }
            return String.valueOf(format, 0, 20 + scale);
        }

        return String.format("%04d-%02d-%02d %02d:%02d:%02d"
                        + (scale > 0 ? ".%0" + scale + "d" : ""),
                year, month, day, hour, minute, second,
                (int) (microSecond / Math.pow(10, TimeStampTzType.MAX_SCALE - scale)));
    }

    @Override
    protected Expression uncheckedCastTo(DataType targetType) throws AnalysisException {
        if (this.dataType.equals(targetType)) {
            return this;
        }
        if (targetType.isTimeStampTzType()) {
            return new TimestampTzLiteral((TimeStampTzType) targetType,
                    year, month, day, hour, minute, second, microSecond);
        } else if (targetType.isDateTimeV2Type()) {
            DateTimeV2Literal dtV2Lit = new DateTimeV2Literal((DateTimeV2Type) targetType,
                    year, month, day, hour, minute, second, microSecond);
            dtV2Lit = (DateTimeV2Literal) (DateTimeExtractAndTransform.convertTz(
                    dtV2Lit,
                    new StringLiteral("UTC"),
                    new StringLiteral(ConnectContext.get().getSessionVariable().timeZone)));
            return dtV2Lit;
        }
        throw new AnalysisException(String.format("Cast from %s to %s not supported", this, targetType));
    }

    public Expression plusDays(long days) {
        return fromJavaDateType(toJavaDateType().plusDays(days), getDataType().getScale());
    }

    /**
     * plusDayHour
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
                .plusHours(hours), getDataType().getScale());
        } catch (NumberFormatException e) {
            return new NullLiteral(dataType);
        }
    }

    /**
     * plusDaySecond
     */
    public Expression plusDaySecond(VarcharLiteral daySecond) {
        String stringValue = daySecond.getStringValue().trim();

        if (!stringValue.matches("[0-9:\\-\\s]+")) {
            return new NullLiteral(dataType);
        }

        String[] split = stringValue.split("\\s+");
        if (split.length != 2) {
            return new NullLiteral(dataType);
        }

        String day = split[0];
        String[] hourMinuteSecond = split[1].split(":");

        if (hourMinuteSecond.length != 3) {
            return new NullLiteral(dataType);
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
                .plusSeconds(seconds), getDataType().getScale());
        } catch (NumberFormatException e) {
            return new NullLiteral(dataType);
        }
    }

    /**
     * plusMinuteSecond
     */
    public Expression plusMinuteSecond(VarcharLiteral minuteSecond) {
        String stringValue = minuteSecond.getStringValue().trim();

        if (!stringValue.matches("[0-9\\-:\\s]+")) {
            return new NullLiteral(dataType);
        }

        String[] split = stringValue.split(":");
        if (split.length != 2) {
            return new NullLiteral(dataType);
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
                .plusSeconds(seconds), getDataType().getScale());
        } catch (NumberFormatException e) {
            return new NullLiteral(dataType);
        }
    }

    /**
     * plusSecondMicrosecond
     */
    public Expression plusSecondMicrosecond(VarcharLiteral secondMicrosecond) {
        String stringValue = secondMicrosecond.getStringValue().trim();

        if (!stringValue.matches("[0-9\\-\\.\\s]+")) {
            return new NullLiteral(dataType);
        }

        String[] split = stringValue.split("\\.");
        if (split.length != 2) {
            return new NullLiteral(dataType);
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
                .plusNanos(Math.multiplyExact(microseconds, 1000L)), getDataType().getScale());
        } catch (NumberFormatException e) {
            return new NullLiteral(dataType);
        }
    }

    public Expression plusMonths(long months) {
        return fromJavaDateType(toJavaDateType().plusMonths(months), getDataType().getScale());
    }

    public Expression plusWeeks(long weeks) {
        return fromJavaDateType(toJavaDateType().plusWeeks(weeks), getDataType().getScale());
    }

    public Expression plusYears(long years) {
        return fromJavaDateType(toJavaDateType().plusYears(years), getDataType().getScale());
    }

    public Expression plusHours(long hours) {
        return fromJavaDateType(toJavaDateType().plusHours(hours), getDataType().getScale());
    }

    public Expression plusMinutes(long minutes) {
        return fromJavaDateType(toJavaDateType().plusMinutes(minutes), getDataType().getScale());
    }

    public Expression plusSeconds(long seconds) {
        return fromJavaDateType(toJavaDateType().plusSeconds(seconds), getDataType().getScale());
    }

    // When performing addition or subtraction with MicroSeconds, the precision must be set to 6 to display it
    // completely. use multiplyExact to be aware of multiplication overflow possibility.
    public Expression plusMicroSeconds(long microSeconds) {
        return fromJavaDateType(toJavaDateType().plusNanos(Math.multiplyExact(microSeconds, 1000L)), 6);
    }

    public Expression plusMilliSeconds(long microSeconds) {
        return plusMicroSeconds(Math.multiplyExact(microSeconds, 1000L));
    }

    public int getScale() {
        return ((TimeStampTzType) dataType).getScale();
    }

    public int commonScale(TimestampTzLiteral other) {
        return (int) Math.max(getScale(), other.getScale());
    }

    /**
     * roundCeiling
     */
    public TimestampTzLiteral roundCeiling(int newScale) {
        long remain = Double.valueOf(microSecond % (Math.pow(10, 6 - newScale))).longValue();
        long newMicroSecond = microSecond;
        long newSecond = second;
        long newMinute = minute;
        long newHour = hour;
        long newDay = day;
        long newMonth = month;
        long newYear = year;
        if (remain != 0) {
            newMicroSecond = Double
                    .valueOf((microSecond + (int) (Math.pow(10, 6 - newScale)))
                            / (int) (Math.pow(10, 6 - newScale)) * (Math.pow(10, 6 - newScale)))
                    .longValue();
        }
        if (newMicroSecond > MAX_MICROSECOND) {
            newMicroSecond %= newMicroSecond;
            Expression plus1Second = this.plusSeconds(1);
            if (plus1Second.isNullLiteral()) {
                throw new AnalysisException("round ceil datetime literal (" + toString() + ", "
                        + newScale + ") is out of range");
            }
            TimestampTzLiteral result = (TimestampTzLiteral) plus1Second;
            newSecond = result.second;
            newMinute = result.minute;
            newHour = result.hour;
            newDay = result.day;
            newMonth = result.month;
            newYear = result.year;
        }
        return new TimestampTzLiteral(TimeStampTzType.of(newScale), newYear, newMonth, newDay,
                newHour, newMinute, newSecond, newMicroSecond);
    }

    public TimestampTzLiteral roundFloor(int newScale) {
        return new TimestampTzLiteral(TimeStampTzType.of(newScale), year, month, day, hour, minute, second,
                microSecond / (int) Math.pow(10, 6 - newScale) * (int) Math.pow(10, 6 - newScale));
    }

    public static Expression fromJavaDateType(LocalDateTime dateTime) {
        return fromJavaDateType(dateTime, 6);
    }

    /**
     * convert java LocalDateTime object to TimeStampTzTypeLiteral object.
     */
    public static Expression fromJavaDateType(LocalDateTime dateTime, int precision) {
        long value = (long) Math.pow(10, TimeStampTzType.MAX_SCALE - precision);
        if (isDateOutOfRange(dateTime)) {
            throw new AnalysisException("datetime out of range" + dateTime.toString());
        }
        return new TimestampTzLiteral(TimeStampTzType.of(precision), dateTime.getYear(),
                        dateTime.getMonthValue(), dateTime.getDayOfMonth(), dateTime.getHour(),
                        dateTime.getMinute(), dateTime.getSecond(),
                        (dateTime.getNano() / 1000) / value * value);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        TimestampTzLiteral literal = (TimestampTzLiteral) o;
        return Objects.equals(dataType, literal.dataType) && Objects.equals(microSecond, literal.microSecond);
    }
}
