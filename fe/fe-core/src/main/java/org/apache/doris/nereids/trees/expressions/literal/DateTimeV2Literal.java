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
import org.apache.doris.nereids.exceptions.UnboundException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.util.DateUtils;
import org.apache.doris.nereids.util.StandardDateFormat;

import com.google.common.base.Preconditions;

import java.time.LocalDateTime;
import java.util.Objects;

/**
 * date time v2 literal for nereids
 */
public class DateTimeV2Literal extends DateTimeLiteral {

    public DateTimeV2Literal(String s) {
        this(DateTimeV2Type.forTypeFromString(s), s);
    }

    public DateTimeV2Literal(DateTimeV2Type dateType, String s) {
        super(dateType, s);
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
        super(dateType, year, month, day, hour, minute, second, microSecond);
        roundMicroSecond(dateType.getScale());
    }

    private void roundMicroSecond(int scale) {
        Preconditions.checkArgument(scale >= 0 && scale <= DateTimeV2Type.MAX_SCALE,
                "invalid datetime v2 scale: %s", scale);
        double factor = Math.pow(10, 6 - scale);

        this.microSecond = Math.round(this.microSecond / factor) * (int) factor;

        if (this.microSecond >= 1000000) {
            LocalDateTime localDateTime = DateUtils.getTime(StandardDateFormat.DATE_TIME_FORMATTER_TO_MICRO_SECOND,
                    getStringValue()).plusSeconds(1);
            this.year = localDateTime.getYear();
            this.month = localDateTime.getMonthValue();
            this.day = localDateTime.getDayOfMonth();
            this.hour = localDateTime.getHour();
            this.minute = localDateTime.getMinute();
            this.second = localDateTime.getSecond();
            this.microSecond -= 1000000;
        }
        if (checkRange() || checkDate(year, month, day)) {
            // may fallback to legacy planner. make sure the behaviour of rounding is same.
            throw new AnalysisException("datetime literal [" + toString() + "] is out of range");
        }
    }

    public String getFullMicroSecondValue() {
        return String.format("%04d-%02d-%02d %02d:%02d:%02d.%06d",
                year, month, day, hour, minute, second, microSecond);
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
            long microSecond = (int) (this.microSecond / Math.pow(10, DateTimeV2Type.MAX_SCALE - scale));
            while (microSecond > 0) {
                format[offset--] = (char) ('0' + (microSecond % 10));
                microSecond /= 10;
            }
            return String.valueOf(format, 0, 20 + scale);
        }

        return String.format("%04d-%02d-%02d %02d:%02d:%02d"
                        + (scale > 0 ? ".%0" + scale + "d" : ""),
                year, month, day, hour, minute, second,
                (int) (microSecond / Math.pow(10, DateTimeV2Type.MAX_SCALE - scale)));
    }

    public String getMicrosecondString() {
        if (microSecond == 0) {
            return "0";
        }
        return String.format("%0" + getDataType().getScale() + "d",
                (int) (microSecond / Math.pow(10, DateTimeV2Type.MAX_SCALE - getDataType().getScale())));
    }

    public Expression plusDays(long days) {
        return fromJavaDateType(toJavaDateType().plusDays(days), getDataType().getScale());
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

    // When performing addition or subtraction with MicroSeconds, the precision must
    // be set to 6 to display it completely.
    public Expression plusMicroSeconds(long microSeconds) {
        return fromJavaDateType(toJavaDateType().plusNanos(microSeconds * 1000L), 6);
    }

    public Expression plusMilliSeconds(long microSeconds) {
        return plusMicroSeconds(microSeconds * 1000L);
    }

    /**
     * roundCeiling
     */
    public DateTimeV2Literal roundCeiling(int newScale) {
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
            DateTimeV2Literal result = (DateTimeV2Literal) plus1Second;
            newSecond = result.second;
            newMinute = result.minute;
            newHour = result.hour;
            newDay = result.day;
            newMonth = result.month;
            newYear = result.year;
        }
        return new DateTimeV2Literal(DateTimeV2Type.of(newScale), newYear, newMonth, newDay,
                newHour, newMinute, newSecond, newMicroSecond);
    }

    public DateTimeV2Literal roundFloor(int newScale) {
        return new DateTimeV2Literal(DateTimeV2Type.of(newScale), year, month, day, hour, minute, second,
                microSecond / (int) Math.pow(10, 6 - newScale) * (int) Math.pow(10, 6 - newScale));
    }

    public static Expression fromJavaDateType(LocalDateTime dateTime) {
        return fromJavaDateType(dateTime, 6);
    }

    /**
     * convert java LocalDateTime object to DateTimeV2Literal object.
     */
    public static Expression fromJavaDateType(LocalDateTime dateTime, int precision) {
        long value = (long) Math.pow(10, DateTimeV2Type.MAX_SCALE - precision);
        return isDateOutOfRange(dateTime)
                ? new NullLiteral(DateTimeV2Type.of(precision))
                : new DateTimeV2Literal(DateTimeV2Type.of(precision), dateTime.getYear(),
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
        DateTimeV2Literal literal = (DateTimeV2Literal) o;
        return Objects.equals(dataType, literal.dataType) && Objects.equals(microSecond, literal.microSecond);
    }
}
