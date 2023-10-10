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
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.DateTimeType;
import org.apache.doris.nereids.types.coercion.DateLikeType;
import org.apache.doris.nereids.util.DateUtils;
import org.apache.doris.nereids.util.StandardDateFormat;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalQueries;
import java.util.Objects;

/**
 * date time literal.
 */
public class DateTimeLiteral extends DateLiteral {
    protected static final int MAX_MICROSECOND = 999999;

    private static final DateTimeLiteral MIN_DATETIME = new DateTimeLiteral(0000, 1, 1, 0, 0, 0);
    private static final DateTimeLiteral MAX_DATETIME = new DateTimeLiteral(9999, 12, 31, 23, 59, 59);

    private static final Logger LOG = LogManager.getLogger(DateTimeLiteral.class);

    protected long hour;
    protected long minute;
    protected long second;
    protected long microSecond;

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
        this(dataType, year, month, day, hour, minute, second, 0L);
    }

    /**
     * C'tor data time literal.
     */
    public DateTimeLiteral(DateLikeType dataType, long year, long month, long day,
            long hour, long minute, long second, long microSecond) {
        super(dataType);
        this.hour = hour;
        this.minute = minute;
        this.second = second;
        this.microSecond = microSecond;
        this.year = year;
        this.month = month;
        this.day = day;
    }

    public boolean isMidnight() {
        return hour == 0 && minute == 0 && second == 0 && microSecond == 0;
    }

    /**
     * determine scale by datetime string
     */
    public static int determineScale(String s) {
        if (!s.contains("-") && !s.contains(":")) {
            return 0;
        }
        s = normalize(s);
        if (s.length() <= 19 || s.charAt(19) != '.') {
            return 0;
        }
        // from index 19 find the index of first char which is not digit
        int scale = 0;
        for (int i = 20; i < s.length(); i++) {
            if (!Character.isDigit(s.charAt(i))) {
                break;
            }
            scale++;
        }
        // trim the tailing zero
        for (int i = 19 + scale; i >= 19; i--) {
            if (s.charAt(i) != '0') {
                break;
            }
            scale--;
        }
        return scale;
    }

    @Override
    protected void init(String s) throws AnalysisException {
        TemporalAccessor temporal = parse(s);

        year = DateUtils.getOrDefault(temporal, ChronoField.YEAR);
        month = DateUtils.getOrDefault(temporal, ChronoField.MONTH_OF_YEAR);
        day = DateUtils.getOrDefault(temporal, ChronoField.DAY_OF_MONTH);
        hour = DateUtils.getOrDefault(temporal, ChronoField.HOUR_OF_DAY);
        minute = DateUtils.getOrDefault(temporal, ChronoField.MINUTE_OF_HOUR);
        second = DateUtils.getOrDefault(temporal, ChronoField.SECOND_OF_MINUTE);
        microSecond = DateUtils.getOrDefault(temporal, ChronoField.MICRO_OF_SECOND);

        ZoneId zoneId = temporal.query(TemporalQueries.zone());
        if (zoneId != null) {
            int offset = DateUtils.getTimeZone().getRules().getOffset(Instant.now()).getTotalSeconds()
                    - zoneId.getRules().getOffset(Instant.now()).getTotalSeconds();
            if (offset != 0) {
                DateTimeLiteral result = (DateTimeLiteral) this.plusSeconds(offset);
                this.second = result.second;
                this.minute = result.minute;
                this.hour = result.hour;
                this.day = result.day;
                this.month = result.month;
                this.year = result.year;
            }
        }

        if (checkRange() || checkDate()) {
            throw new AnalysisException("datetime literal [" + s + "] is out of range");
        }
    }

    @Override
    protected boolean checkRange() {
        return super.checkRange() || hour > MAX_DATETIME.getHour() || minute > MAX_DATETIME.getMinute()
                || second > MAX_DATETIME.getSecond() || microSecond > MAX_MICROSECOND;
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
    public double getDouble() {
        return (double) getValue();
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

    public Expression plusYears(long years) {
        return fromJavaDateType(
                DateUtils.getTime(StandardDateFormat.DATE_TIME_FORMATTER, getStringValue()).plusYears(years));
    }

    public Expression plusMonths(long months) {
        return fromJavaDateType(
                DateUtils.getTime(StandardDateFormat.DATE_TIME_FORMATTER, getStringValue()).plusMonths(months));
    }

    public Expression plusWeeks(long weeks) {
        return fromJavaDateType(
                DateUtils.getTime(StandardDateFormat.DATE_TIME_FORMATTER, getStringValue()).plusWeeks(weeks));
    }

    public Expression plusDays(long days) {
        return fromJavaDateType(
                DateUtils.getTime(StandardDateFormat.DATE_TIME_FORMATTER, getStringValue()).plusDays(days));
    }

    public Expression plusHours(long hours) {
        return fromJavaDateType(
                DateUtils.getTime(StandardDateFormat.DATE_TIME_FORMATTER, getStringValue()).plusHours(hours));
    }

    public Expression plusMinutes(long minutes) {
        return fromJavaDateType(
                DateUtils.getTime(StandardDateFormat.DATE_TIME_FORMATTER, getStringValue()).plusMinutes(minutes));
    }

    public Expression plusSeconds(long seconds) {
        return fromJavaDateType(
                DateUtils.getTime(StandardDateFormat.DATE_TIME_FORMATTER, getStringValue()).plusSeconds(seconds));
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
        return microSecond;
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

    public LocalDateTime toJavaDateType() {
        return LocalDateTime.of(((int) getYear()), ((int) getMonth()), ((int) getDay()),
                ((int) getHour()), ((int) getMinute()), ((int) getSecond()));
    }

    public static Expression fromJavaDateType(LocalDateTime dateTime) {
        return isDateOutOfRange(dateTime)
                ? new NullLiteral(DateTimeType.INSTANCE)
                : new DateTimeLiteral(dateTime.getYear(), dateTime.getMonthValue(), dateTime.getDayOfMonth(),
                        dateTime.getHour(), dateTime.getMinute(), dateTime.getSecond());
    }
}
