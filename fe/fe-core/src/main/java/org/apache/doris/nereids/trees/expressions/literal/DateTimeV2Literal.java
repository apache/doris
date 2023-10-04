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
import org.apache.doris.nereids.exceptions.UnboundException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.util.DateUtils;
import org.apache.doris.nereids.util.StandardDateFormat;

import com.google.common.base.Preconditions;

import java.time.LocalDateTime;

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
    public String toString() {
        return getStringValue();
    }

    @Override
    public String getStringValue() {
        return String.format("%04d-%02d-%02d %02d:%02d:%02d"
                        + (getDataType().getScale() > 0 ? ".%0" + getDataType().getScale() + "d" : ""),
                year, month, day, hour, minute, second,
                (int) (microSecond / Math.pow(10, DateTimeV2Type.MAX_SCALE - getDataType().getScale())));
    }

    @Override
    public Expression plusYears(long years) {
        return fromJavaDateType(
                DateUtils.getTime(StandardDateFormat.DATE_TIME_FORMATTER_TO_MICRO_SECOND, getStringValue())
                        .plusYears(years), getDataType().getScale());
    }

    @Override
    public Expression plusMonths(long months) {
        return fromJavaDateType(
                DateUtils.getTime(StandardDateFormat.DATE_TIME_FORMATTER_TO_MICRO_SECOND, getStringValue())
                        .plusMonths(months), getDataType().getScale());
    }

    @Override
    public Expression plusDays(long days) {
        return fromJavaDateType(
                DateUtils.getTime(StandardDateFormat.DATE_TIME_FORMATTER_TO_MICRO_SECOND, getStringValue())
                        .plusDays(days), getDataType().getScale());
    }

    @Override
    public Expression plusHours(long hours) {
        return fromJavaDateType(
                DateUtils.getTime(StandardDateFormat.DATE_TIME_FORMATTER_TO_MICRO_SECOND, getStringValue())
                        .plusHours(hours), getDataType().getScale());
    }

    @Override
    public Expression plusMinutes(long minutes) {
        return fromJavaDateType(
                DateUtils.getTime(StandardDateFormat.DATE_TIME_FORMATTER_TO_MICRO_SECOND, getStringValue())
                        .plusMinutes(minutes), getDataType().getScale());
    }

    @Override
    public Expression plusSeconds(long seconds) {
        return fromJavaDateType(
                DateUtils.getTime(StandardDateFormat.DATE_TIME_FORMATTER_TO_MICRO_SECOND, getStringValue())
                        .plusSeconds(seconds), getDataType().getScale());
    }

    public Expression plusMicroSeconds(long microSeconds) {
        return fromJavaDateType(
                DateUtils.getTime(StandardDateFormat.DATE_TIME_FORMATTER_TO_MICRO_SECOND, getStringValue())
                        .plusNanos(microSeconds * 1000L), getDataType().getScale());
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
                    .valueOf((microSecond + (Math.pow(10, 6 - newScale)))
                            / (int) (Math.pow(10, 6 - newScale)) * (Math.pow(10, 6 - newScale)))
                    .longValue();
        }
        if (newMicroSecond > MAX_MICROSECOND) {
            newMicroSecond %= newMicroSecond;
            DateTimeV2Literal result = (DateTimeV2Literal) this.plusSeconds(1);
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
        // use roundMicroSecond in constructor
        return new DateTimeV2Literal(DateTimeV2Type.of(newScale), year, month, day, hour, minute, second, microSecond);
    }

    public static Expression fromJavaDateType(LocalDateTime dateTime) {
        return fromJavaDateType(dateTime, 0);
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
}
