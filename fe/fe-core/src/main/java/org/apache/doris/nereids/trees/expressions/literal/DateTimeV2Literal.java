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

import org.apache.doris.nereids.exceptions.UnboundException;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.util.DateUtils;

import java.time.LocalDateTime;

/**
 * date time v2 literal for nereids
 */
public class DateTimeV2Literal extends DateTimeLiteral {

    public DateTimeV2Literal(String s) {
        super(DateTimeV2Type.MAX, s);
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
    public String toString() {
        return String.format("%04d-%02d-%02d %02d:%02d:%02d.%06d", year, month, day, hour, minute, second, microSecond);
    }

    @Override
    public String getStringValue() {
        return String.format("%04d-%02d-%02d %02d:%02d:%02d.%06d", year, month, day, hour, minute, second, microSecond);
    }

    @Override
    public DateTimeV2Literal plusYears(int years) {
        LocalDateTime d = DateUtils.getTime(DATE_TIME_FORMATTER_TO_MICRO_SECOND, getStringValue()).plusYears(years);
        return new DateTimeV2Literal(this.getDataType(), d.getYear(), d.getMonthValue(), d.getDayOfMonth(),
                d.getHour(), d.getMinute(), d.getSecond(), d.getNano() / 1000L);
    }

    @Override
    public DateTimeV2Literal plusMonths(int months) {
        LocalDateTime d = DateUtils.getTime(DATE_TIME_FORMATTER_TO_MICRO_SECOND, getStringValue()).plusMonths(months);
        return new DateTimeV2Literal(this.getDataType(), d.getYear(), d.getMonthValue(), d.getDayOfMonth(),
                d.getHour(), d.getMinute(), d.getSecond(), d.getNano() / 1000L);
    }

    @Override
    public DateTimeV2Literal plusDays(int days) {
        LocalDateTime d = DateUtils.getTime(DATE_TIME_FORMATTER_TO_MICRO_SECOND, getStringValue()).plusDays(days);
        return new DateTimeV2Literal(this.getDataType(), d.getYear(), d.getMonthValue(), d.getDayOfMonth(),
                d.getHour(), d.getMinute(), d.getSecond(), d.getNano() / 1000L);
    }

    @Override
    public DateTimeV2Literal plusHours(int hours) {
        LocalDateTime d = DateUtils.getTime(DATE_TIME_FORMATTER_TO_MICRO_SECOND, getStringValue()).plusHours(hours);
        return new DateTimeV2Literal(this.getDataType(), d.getYear(), d.getMonthValue(), d.getDayOfMonth(),
                d.getHour(), d.getMinute(), d.getSecond(), d.getNano() / 1000L);
    }

    @Override
    public DateTimeV2Literal plusMinutes(int minutes) {
        LocalDateTime d = DateUtils.getTime(DATE_TIME_FORMATTER_TO_MICRO_SECOND, getStringValue()).plusMinutes(minutes);
        return new DateTimeV2Literal(this.getDataType(), d.getYear(), d.getMonthValue(), d.getDayOfMonth(),
                d.getHour(), d.getMinute(), d.getSecond(), d.getNano() / 1000L);
    }

    @Override
    public DateTimeV2Literal plusSeconds(int seconds) {
        LocalDateTime d = DateUtils.getTime(DATE_TIME_FORMATTER_TO_MICRO_SECOND, getStringValue()).plusSeconds(seconds);
        return new DateTimeV2Literal(this.getDataType(), d.getYear(), d.getMonthValue(), d.getDayOfMonth(),
                d.getHour(), d.getMinute(), d.getSecond(), d.getNano() / 1000L);
    }
}
