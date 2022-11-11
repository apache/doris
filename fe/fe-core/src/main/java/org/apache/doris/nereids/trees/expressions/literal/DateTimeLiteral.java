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
import org.apache.doris.catalog.Type;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.DateTimeType;
import org.apache.doris.nereids.types.coercion.DateLikeType;
import org.apache.doris.nereids.util.DateUtils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.joda.time.LocalDateTime;
import org.joda.time.format.DateTimeFormatter;

import java.util.Objects;

/**
 * date time literal.
 */
public class DateTimeLiteral extends DateLiteral {

    protected static final int DATETIME_TO_MINUTE_STRING_LENGTH = 16;
    protected static final int DATETIME_TO_HOUR_STRING_LENGTH = 13;
    protected static final int DATETIME_DEFAULT_STRING_LENGTH = 10;
    protected static DateTimeFormatter DATE_TIME_DEFAULT_FORMATTER = null;
    protected static DateTimeFormatter DATE_TIME_FORMATTER = null;
    protected static DateTimeFormatter DATE_TIME_FORMATTER_TO_HOUR = null;
    protected static DateTimeFormatter DATE_TIME_FORMATTER_TO_MINUTE = null;
    protected static DateTimeFormatter DATE_TIME_FORMATTER_TWO_DIGIT = null;

    private static final Logger LOG = LogManager.getLogger(DateTimeLiteral.class);

    protected long hour;
    protected long minute;
    protected long second;

    static {
        try {
            DATE_TIME_FORMATTER = DateUtils.formatBuilder("%Y-%m-%d %H:%i:%s").toFormatter();
            DATE_TIME_FORMATTER_TO_HOUR = DateUtils.formatBuilder("%Y-%m-%d %H").toFormatter();
            DATE_TIME_FORMATTER_TO_MINUTE = DateUtils.formatBuilder("%Y-%m-%d %H:%i").toFormatter();
            DATE_TIME_FORMATTER_TWO_DIGIT = DateUtils.formatBuilder("%y-%m-%d %H:%i:%s").toFormatter();
            DATE_TIME_DEFAULT_FORMATTER = DateUtils.formatBuilder("%Y-%m-%d").toFormatter();
        } catch (AnalysisException e) {
            LOG.error("invalid date format", e);
            System.exit(-1);
        }
    }

    public DateTimeLiteral(String s) {
        this(DateTimeType.INSTANCE, s);
    }

    protected DateTimeLiteral(DateLikeType dataType, String s) {
        super(dataType);
        init(s);
    }

    /**
     * C'tor data time literal.
     */
    public DateTimeLiteral(long year, long month, long day, long hour, long minute, long second) {
        this(DateTimeType.INSTANCE, year, month, day, hour, minute, second);
    }

    /**
     * C'tor data time literal.
     */
    public DateTimeLiteral(DateLikeType dataType, long year, long month, long day,
            long hour, long minute, long second) {
        super(dataType);
        this.hour = hour;
        this.minute = minute;
        this.second = second;
        this.year = year;
        this.month = month;
        this.day = day;
    }

    @Override
    protected void init(String s) throws AnalysisException {
        try {
            LocalDateTime dateTime;
            if (s.split("-")[0].length() == 2) {
                dateTime = DATE_TIME_FORMATTER_TWO_DIGIT.parseLocalDateTime(s);
            } else {
                if (s.length() == DATETIME_TO_MINUTE_STRING_LENGTH) {
                    dateTime = DATE_TIME_FORMATTER_TO_MINUTE.parseLocalDateTime(s);
                } else if (s.length() == DATETIME_TO_HOUR_STRING_LENGTH) {
                    dateTime = DATE_TIME_FORMATTER_TO_HOUR.parseLocalDateTime(s);
                } else if (s.length() == DATETIME_DEFAULT_STRING_LENGTH) {
                    dateTime = DATE_TIME_DEFAULT_FORMATTER.parseLocalDateTime(s);
                } else {
                    dateTime = DATE_TIME_FORMATTER.parseLocalDateTime(s);
                }
            }
            year = dateTime.getYear();
            month = dateTime.getMonthOfYear();
            day = dateTime.getDayOfMonth();
            hour = dateTime.getHourOfDay();
            minute = dateTime.getMinuteOfHour();
            second = dateTime.getSecondOfMinute();
        } catch (Exception ex) {
            throw new AnalysisException("date time literal [" + s + "] is invalid");
        }
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitDateTimeLiteral(this, context);
    }

    @Override
    public Long getValue() {
        return (year * 10000 + month * 100 + day) * 1000000L + hour * 10000 + minute * 100 + second;
    }

    @Override
    public String toSql() {
        return toString();
    }

    @Override
    public String toString() {
        return String.format("%04d-%02d-%02d %02d:%02d:%02d", year, month, day, hour, minute, second);
    }

    @Override
    public String getStringValue() {
        return String.format("%04d-%02d-%02d %02d:%02d:%02d", year, month, day, hour, minute, second);
    }

    @Override
    public LiteralExpr toLegacyLiteral() {
        return new org.apache.doris.analysis.DateLiteral(year, month, day, hour, minute, second, Type.DATETIME);
    }

    public DateTimeLiteral plusDays(int days) {
        LocalDateTime d = LocalDateTime.parse(getStringValue(), DATE_TIME_FORMATTER).plusDays(days);
        return new DateTimeLiteral(d.getYear(), d.getMonthOfYear(), d.getDayOfMonth(),
                d.getHourOfDay(), d.getMinuteOfHour(), d.getSecondOfMinute());
    }

    public DateTimeLiteral plusYears(int years) {
        LocalDateTime d = LocalDateTime.parse(getStringValue(), DATE_TIME_FORMATTER).plusYears(years);
        return new DateTimeLiteral(d.getYear(), d.getMonthOfYear(), d.getDayOfMonth(),
                d.getHourOfDay(), d.getMinuteOfHour(), d.getSecondOfMinute());
    }

    public DateTimeLiteral plusMonths(int months) {
        LocalDateTime d = LocalDateTime.parse(getStringValue(), DATE_TIME_FORMATTER).plusMonths(months);
        return new DateTimeLiteral(d.getYear(), d.getMonthOfYear(), d.getDayOfMonth(),
                d.getHourOfDay(), d.getMinuteOfHour(), d.getSecondOfMinute());
    }

    public DateTimeLiteral plusHours(int hours) {
        LocalDateTime d = LocalDateTime.parse(getStringValue(), DATE_TIME_FORMATTER).plusHours(hours);
        return new DateTimeLiteral(d.getYear(), d.getMonthOfYear(), d.getDayOfMonth(),
                d.getHourOfDay(), d.getMinuteOfHour(), d.getSecondOfMinute());
    }

    public DateTimeLiteral plusMinutes(int minutes) {
        LocalDateTime d = LocalDateTime.parse(getStringValue(), DATE_TIME_FORMATTER).plusMinutes(minutes);
        return new DateTimeLiteral(d.getYear(), d.getMonthOfYear(), d.getDayOfMonth(),
                d.getHourOfDay(), d.getMinuteOfHour(), d.getSecondOfMinute());
    }

    public DateTimeLiteral plusSeconds(int seconds) {
        LocalDateTime d = LocalDateTime.parse(getStringValue(), DATE_TIME_FORMATTER).plusSeconds(seconds);
        return new DateTimeLiteral(d.getYear(), d.getMonthOfYear(), d.getDayOfMonth(),
                d.getHourOfDay(), d.getMinuteOfHour(), d.getSecondOfMinute());
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

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DateTimeLiteral other = (DateTimeLiteral) o;
        return Objects.equals(getValue(), other.getValue());
    }
}
