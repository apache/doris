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
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.util.DateUtils;

import java.time.LocalDateTime;

/**
 * date time v2 literal for nereids
 */
public class DateTimeV2Literal extends DateTimeLiteral {

    public DateTimeV2Literal(String s) {
        this(DateTimeV2Type.MAX, s);
    }

    public DateTimeV2Literal(DateTimeV2Type dateType, String s) {
        super(dateType, s);
    }

    public DateTimeV2Literal(long year, long month, long day, long hour, long minute, long second) {
        super(DateTimeV2Type.SYSTEM_DEFAULT, year, month, day, hour, minute, second, 0);
    }

    public DateTimeV2Literal(long year, long month, long day, long hour, long minute, long second, long microSecond) {
        super(DateTimeV2Type.SYSTEM_DEFAULT, year, month, day, hour, minute, second, microSecond);
    }

    public DateTimeV2Literal(DateTimeV2Type dataType,
            long year, long month, long day, long hour, long minute, long second, long microSecond) {
        super(dataType, year, month, day, hour, minute, second, microSecond);
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
    public DateTimeV2Literal plusYears(int years) {
        return fromJavaDateType(DateUtils.getTime(DATE_TIME_FORMATTER_TO_MICRO_SECOND, getStringValue())
                .plusYears(years));
    }

    @Override
    public DateTimeV2Literal plusMonths(int months) {
        return fromJavaDateType(DateUtils.getTime(DATE_TIME_FORMATTER_TO_MICRO_SECOND, getStringValue())
                .plusMonths(months));
    }

    @Override
    public DateTimeV2Literal plusDays(int days) {
        return fromJavaDateType(DateUtils.getTime(DATE_TIME_FORMATTER_TO_MICRO_SECOND, getStringValue())
                .plusDays(days));
    }

    @Override
    public DateTimeV2Literal plusHours(int hours) {
        return fromJavaDateType(DateUtils.getTime(DATE_TIME_FORMATTER_TO_MICRO_SECOND, getStringValue())
                .plusHours(hours));
    }

    @Override
    public DateTimeV2Literal plusMinutes(int minutes) {
        return fromJavaDateType(DateUtils.getTime(DATE_TIME_FORMATTER_TO_MICRO_SECOND, getStringValue())
                .plusMinutes(minutes));
    }

    @Override
    public DateTimeV2Literal plusSeconds(long seconds) {
        return fromJavaDateType(DateUtils.getTime(DATE_TIME_FORMATTER_TO_MICRO_SECOND, getStringValue())
                .plusSeconds(seconds));
    }

    public DateTimeV2Literal plusMicroSeconds(int microSeconds) {
        return fromJavaDateType(DateUtils.getTime(DATE_TIME_FORMATTER_TO_MICRO_SECOND, getStringValue())
                .plusNanos(microSeconds * 1000L));
    }

    public static DateTimeV2Literal fromJavaDateType(LocalDateTime dateTime) {
        return fromJavaDateType(dateTime, 0);
    }

    public static DateTimeV2Literal fromJavaDateType(LocalDateTime dateTime, int precision) {
        return isDateOutOfRange(dateTime) ? null
                : new DateTimeV2Literal(DateTimeV2Type.of(precision),
                        dateTime.getYear(), dateTime.getMonthValue(), dateTime.getDayOfMonth(),
                        dateTime.getHour(), dateTime.getMinute(), dateTime.getSecond(),
                        dateTime.getNano() / (long) Math.pow(10, 9 - precision));
    }
}
