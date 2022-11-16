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

import org.joda.time.LocalDateTime;

/**
 * date time v2 literal for nereids
 */
public class DateTimeV2Literal extends DateTimeLiteral {

    public DateTimeV2Literal(String s) {
        super(DateTimeV2Type.INSTANCE, s);
    }

    public DateTimeV2Literal(long year, long month, long day, long hour, long minute, long second) {
        super(DateTimeV2Type.INSTANCE, year, month, day, hour, minute, second);
    }

    public DateTimeV2Literal(DateTimeV2Type dataType,
            long year, long month, long day, long hour, long minute, long second) {
        super(dataType, year, month, day, hour, minute, second);
    }

    @Override
    public DateTimeV2Type getDataType() throws UnboundException {
        return (DateTimeV2Type) super.getDataType();
    }

    @Override
    public DateTimeV2Literal plusYears(int years) {
        LocalDateTime d = LocalDateTime.parse(getStringValue(), DATE_TIME_FORMATTER).plusYears(years);
        return new DateTimeV2Literal(this.getDataType(), d.getYear(), d.getMonthOfYear(), d.getDayOfMonth(),
                d.getHourOfDay(), d.getMinuteOfHour(), d.getSecondOfMinute());
    }

    @Override
    public DateTimeV2Literal plusMonths(int months) {
        LocalDateTime d = LocalDateTime.parse(getStringValue(), DATE_TIME_FORMATTER).plusMonths(months);
        return new DateTimeV2Literal(this.getDataType(), d.getYear(), d.getMonthOfYear(), d.getDayOfMonth(),
                d.getHourOfDay(), d.getMinuteOfHour(), d.getSecondOfMinute());
    }

    @Override
    public DateTimeLiteral plusDays(int days) {
        LocalDateTime d = LocalDateTime.parse(getStringValue(), DATE_TIME_FORMATTER).plusDays(days);
        return new DateTimeV2Literal(this.getDataType(), d.getYear(), d.getMonthOfYear(), d.getDayOfMonth(),
                d.getHourOfDay(), d.getMinuteOfHour(), d.getSecondOfMinute());
    }

    @Override
    public DateTimeV2Literal plusHours(int hours) {
        LocalDateTime d = LocalDateTime.parse(getStringValue(), DATE_TIME_FORMATTER).plusHours(hours);
        return new DateTimeV2Literal(this.getDataType(), d.getYear(), d.getMonthOfYear(), d.getDayOfMonth(),
                d.getHourOfDay(), d.getMinuteOfHour(), d.getSecondOfMinute());
    }

    @Override
    public DateTimeV2Literal plusMinutes(int minutes) {
        LocalDateTime d = LocalDateTime.parse(getStringValue(), DATE_TIME_FORMATTER).plusMinutes(minutes);
        return new DateTimeV2Literal(this.getDataType(), d.getYear(), d.getMonthOfYear(), d.getDayOfMonth(),
                d.getHourOfDay(), d.getMinuteOfHour(), d.getSecondOfMinute());
    }

    @Override
    public DateTimeV2Literal plusSeconds(int seconds) {
        LocalDateTime d = LocalDateTime.parse(getStringValue(), DATE_TIME_FORMATTER).plusSeconds(seconds);
        return new DateTimeV2Literal(this.getDataType(), d.getYear(), d.getMonthOfYear(), d.getDayOfMonth(),
                d.getHourOfDay(), d.getMinuteOfHour(), d.getSecondOfMinute());
    }
}
