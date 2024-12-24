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
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.coercion.DateLikeType;
import org.apache.doris.nereids.util.DateUtils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
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
        // means basic format with timezone
        if (s.indexOf("-") == s.lastIndexOf("-") && s.indexOf(":") == s.lastIndexOf(":")) {
            return 0;
        }
        s = normalize(s).get();
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

    /** parseDateTimeLiteral */
    public static Result<DateTimeLiteral, AnalysisException> parseDateTimeLiteral(String s, boolean isV2) {
        Result<TemporalAccessor, AnalysisException> parseResult = parseDateTime(s);
        if (parseResult.isError()) {
            return parseResult.cast();
        }

        TemporalAccessor temporal = parseResult.get();
        long year = DateUtils.getOrDefault(temporal, ChronoField.YEAR);
        long month = DateUtils.getOrDefault(temporal, ChronoField.MONTH_OF_YEAR);
        long day = DateUtils.getOrDefault(temporal, ChronoField.DAY_OF_MONTH);
        long hour = DateUtils.getOrDefault(temporal, ChronoField.HOUR_OF_DAY);
        long minute = DateUtils.getOrDefault(temporal, ChronoField.MINUTE_OF_HOUR);
        long second = DateUtils.getOrDefault(temporal, ChronoField.SECOND_OF_MINUTE);

        ZoneId zoneId = temporal.query(TemporalQueries.zone());
        if (zoneId != null) {
            // get correct DST of that time.
            Instant thatTime = ZonedDateTime
                    .of((int) year, (int) month, (int) day, (int) hour, (int) minute, (int) second, 0, zoneId)
                    .toInstant();

            int offset = DateUtils.getTimeZone().getRules().getOffset(thatTime).getTotalSeconds()
                    - zoneId.getRules().getOffset(thatTime).getTotalSeconds();
            if (offset != 0) {
                DateTimeLiteral tempLiteral = new DateTimeLiteral(year, month, day, hour, minute, second);
                DateTimeLiteral result = (DateTimeLiteral) tempLiteral.plusSeconds(offset);
                second = result.second;
                minute = result.minute;
                hour = result.hour;
                day = result.day;
                month = result.month;
                year = result.year;
            }
        }

        long microSecond = DateUtils.getOrDefault(temporal, ChronoField.NANO_OF_SECOND) / 100L;
        // Microseconds have 7 digits.
        long sevenDigit = microSecond % 10;
        microSecond = microSecond / 10;
        if (sevenDigit >= 5 && isV2) {
            DateTimeV2Literal tempLiteral = new DateTimeV2Literal(year, month, day, hour, minute, second, microSecond);
            DateTimeV2Literal result = (DateTimeV2Literal) tempLiteral.plusMicroSeconds(1);
            second = result.second;
            minute = result.minute;
            hour = result.hour;
            day = result.day;
            month = result.month;
            year = result.year;
            microSecond = result.microSecond;
        }

        if (checkRange(year, month, day) || checkDate(year, month, day)) {
            return Result.err(() -> new AnalysisException("datetime literal [" + s + "] is out of range"));
        }

        if (isV2) {
            DateTimeV2Type type = DateTimeV2Type.forTypeFromString(s);
            return Result.ok(new DateTimeV2Literal(type, year, month, day, hour, minute, second, microSecond));
        } else {
            return Result.ok(new DateTimeLiteral(DateTimeType.INSTANCE, year, month, day, hour, minute, second));
        }
    }

    protected void init(String s) throws AnalysisException {
        // TODO: check and do fast parse like fastParseDate
        TemporalAccessor temporal = parseDateTime(s).get();

        year = DateUtils.getOrDefault(temporal, ChronoField.YEAR);
        month = DateUtils.getOrDefault(temporal, ChronoField.MONTH_OF_YEAR);
        day = DateUtils.getOrDefault(temporal, ChronoField.DAY_OF_MONTH);
        hour = DateUtils.getOrDefault(temporal, ChronoField.HOUR_OF_DAY);
        minute = DateUtils.getOrDefault(temporal, ChronoField.MINUTE_OF_HOUR);
        second = DateUtils.getOrDefault(temporal, ChronoField.SECOND_OF_MINUTE);

        ZoneId zoneId = temporal.query(TemporalQueries.zone());
        if (zoneId != null) {
            // get correct DST of that time.
            Instant thatTime = ZonedDateTime
                    .of((int) year, (int) month, (int) day, (int) hour, (int) minute, (int) second, 0, zoneId)
                    .toInstant();

            int offset = DateUtils.getTimeZone().getRules().getOffset(thatTime).getTotalSeconds()
                    - zoneId.getRules().getOffset(thatTime).getTotalSeconds();
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

        microSecond = DateUtils.getOrDefault(temporal, ChronoField.NANO_OF_SECOND) / 100L;
        // Microseconds have 7 digits.
        long sevenDigit = microSecond % 10;
        microSecond = microSecond / 10;
        if (sevenDigit >= 5 && this instanceof DateTimeV2Literal) {
            DateTimeV2Literal result = (DateTimeV2Literal) ((DateTimeV2Literal) this).plusMicroSeconds(1);
            this.second = result.second;
            this.minute = result.minute;
            this.hour = result.hour;
            this.day = result.day;
            this.month = result.month;
            this.year = result.year;
            this.microSecond = result.microSecond;
        }

        if (checkRange(year, month, day) || checkDate(year, month, day)) {
            throw new AnalysisException("datetime literal [" + s + "] is out of range");
        }
    }

    protected boolean checkRange() {
        return checkRange(year, month, day) || hour > MAX_DATETIME.getHour() || minute > MAX_DATETIME.getMinute()
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
    public String computeToSql() {
        return "'" + getStringValue() + "'";
    }

    @Override
    public String toString() {
        return getStringValue();
    }

    @Override
    public String getStringValue() {
        if (0 <= year && year <= 9999 && 0 <= month && month <= 99 && 0 <= day && day <= 99
                && 0 <= hour && hour <= 99 && 0 <= minute && minute <= 99 && 0 <= second && second <= 99) {
            char[] format = new char[] {
                    '0', '0', '0', '0', '-', '0', '0', '-', '0', '0', ' ', '0', '0', ':', '0', '0', ':', '0', '0'};
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
            return String.valueOf(format);
        }
        return String.format("%04d-%02d-%02d %02d:%02d:%02d", year, month, day, hour, minute, second);
    }

    @Override
    public LiteralExpr toLegacyLiteral() {
        return new org.apache.doris.analysis.DateLiteral(year, month, day, hour, minute, second, Type.DATETIME);
    }

    public Expression plusDays(long days) {
        return fromJavaDateType(toJavaDateType().plusDays(days));
    }

    public Expression plusMonths(long months) {
        return fromJavaDateType(toJavaDateType().plusMonths(months));
    }

    public Expression plusWeeks(long weeks) {
        return fromJavaDateType(toJavaDateType().plusWeeks(weeks));
    }

    public Expression plusYears(long years) {
        return fromJavaDateType(toJavaDateType().plusYears(years));
    }

    public Expression plusHours(long hours) {
        return fromJavaDateType(toJavaDateType().plusHours(hours));
    }

    public Expression plusMinutes(long minutes) {
        return fromJavaDateType(toJavaDateType().plusMinutes(minutes));
    }

    public Expression plusSeconds(long seconds) {
        return fromJavaDateType(toJavaDateType().plusSeconds(seconds));
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
                ((int) getHour()), ((int) getMinute()), ((int) getSecond()), (int) getMicroSecond() * 1000);
    }

    public static Expression fromJavaDateType(LocalDateTime dateTime) {
        return isDateOutOfRange(dateTime)
                ? new NullLiteral(DateTimeType.INSTANCE)
                : new DateTimeLiteral(dateTime.getYear(), dateTime.getMonthValue(), dateTime.getDayOfMonth(),
                        dateTime.getHour(), dateTime.getMinute(), dateTime.getSecond());
    }
}
