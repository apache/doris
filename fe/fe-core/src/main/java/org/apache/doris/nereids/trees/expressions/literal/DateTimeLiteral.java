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

import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.format.ResolverStyle;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * date time literal.
 */
public class DateTimeLiteral extends DateLiteral {
    protected static DateTimeFormatter DATE_TIME_FORMATTER_TO_HOUR = null;
    protected static DateTimeFormatter DATE_TIME_FORMATTER_TO_MINUTE = null;
    protected static DateTimeFormatter DATE_TIME_FORMATTER_TWO_DIGIT = null;
    protected static DateTimeFormatter DATETIMEKEY_FORMATTER = null;
    protected static DateTimeFormatter DATE_TIME_FORMATTER_TO_MICRO_SECOND = null;
    protected static List<DateTimeFormatter> formatterList = null;
    protected static final int MAX_MICROSECOND = 999999;

    private static final DateTimeLiteral MIN_DATETIME = new DateTimeLiteral(0000, 1, 1, 0, 0, 0);
    private static final DateTimeLiteral MAX_DATETIME = new DateTimeLiteral(9999, 12, 31, 23, 59, 59);

    private static final Logger LOG = LogManager.getLogger(DateTimeLiteral.class);

    protected long hour;
    protected long minute;
    protected long second;
    protected long microSecond;

    static {
        try {
            DATE_TIME_FORMATTER = DateUtils.formatBuilder("%Y-%m-%d %H:%i:%s")
                    .toFormatter().withResolverStyle(ResolverStyle.STRICT);
            DATE_TIME_FORMATTER_TO_HOUR = DateUtils.formatBuilder("%Y-%m-%d %H")
                    .toFormatter().withResolverStyle(ResolverStyle.STRICT);
            DATE_TIME_FORMATTER_TO_MINUTE = DateUtils.formatBuilder("%Y-%m-%d %H:%i")
                    .toFormatter().withResolverStyle(ResolverStyle.STRICT);
            DATE_TIME_FORMATTER_TWO_DIGIT = DateUtils.formatBuilder("%y-%m-%d %H:%i:%s")
                    .toFormatter().withResolverStyle(ResolverStyle.STRICT);

            DATETIMEKEY_FORMATTER = DateUtils.formatBuilder("%Y%m%d%H%i%s")
                    .toFormatter().withResolverStyle(ResolverStyle.STRICT);

            DATE_TIME_FORMATTER_TO_MICRO_SECOND = new DateTimeFormatterBuilder()
                    .appendPattern("uuuu-MM-dd HH:mm:ss")
                    .appendFraction(ChronoField.MICRO_OF_SECOND, 0, 6, true)
                    .toFormatter()
                    .withResolverStyle(ResolverStyle.STRICT);

            formatterList = Lists.newArrayList(
                    DateUtils.formatBuilder("%Y%m%d").appendLiteral('T').appendPattern("HHmmss")
                            .appendFraction(ChronoField.MICRO_OF_SECOND, 0, 6, true)
                            .toFormatter().withResolverStyle(ResolverStyle.STRICT),
                    DateUtils.formatBuilder("%Y%m%d").appendLiteral('T').appendPattern("HHmmss")
                            .appendFraction(ChronoField.MICRO_OF_SECOND, 0, 6, false)
                            .toFormatter().withResolverStyle(ResolverStyle.STRICT),
                    DateUtils.formatBuilder("%Y%m%d%H%i%s")
                            .appendFraction(ChronoField.MICRO_OF_SECOND, 0, 6, true)
                            .toFormatter().withResolverStyle(ResolverStyle.STRICT),
                    DateUtils.formatBuilder("%Y%m%d%H%i%s")
                            .appendFraction(ChronoField.MICRO_OF_SECOND, 0, 6, false)
                            .toFormatter().withResolverStyle(ResolverStyle.STRICT),
                    DATETIMEKEY_FORMATTER, DATEKEY_FORMATTER);
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

    @Override
    protected void init(String s) throws AnalysisException {
        try {
            TemporalAccessor dateTime = null;
            if (!s.contains("-")) {
                // handle format like 20210106, but should not handle 2021-1-6
                boolean parsed = false;
                for (DateTimeFormatter formatter : formatterList) {
                    try {
                        dateTime = formatter.parse(s);
                        parsed = true;
                        break;
                    } catch (DateTimeParseException ex) {
                        // ignore
                    }
                }
                if (!parsed) {
                    throw new AnalysisException("datetime literal [" + s + "] is invalid");
                }
            } else {
                String[] datePart = s.contains(" ") ? s.split(" ")[0].split("-") : s.split("-");
                DateTimeFormatterBuilder builder = new DateTimeFormatterBuilder();
                if (datePart.length != 3) {
                    throw new AnalysisException("datetime literal [" + s + "] is invalid");
                }
                for (int i = 0; i < datePart.length; i++) {
                    switch (i) {
                        case 0:
                            if (datePart[i].length() == 2) {
                                // If year is represented by two digits, number bigger than 70 will be prefixed
                                // with 19 otherwise 20. e.g. 69 -> 2069, 70 -> 1970.
                                builder.appendValueReduced(ChronoField.YEAR, 2, 2, 1970);
                            } else {
                                builder.appendPattern(String.join("", Collections.nCopies(datePart[i].length(), "u")));
                            }
                            break;
                        case 1:
                            builder.appendPattern(String.join("", Collections.nCopies(datePart[i].length(), "M")));
                            break;
                        case 2:
                            builder.appendPattern(String.join("", Collections.nCopies(datePart[i].length(), "d")));
                            break;
                        default:
                            throw new AnalysisException("two many parts in date format " + s);
                    }
                    if (i < datePart.length - 1) {
                        builder.appendLiteral("-");
                    }
                }
                if (s.contains(" ")) {
                    builder.appendLiteral(" ");
                }
                String[] timePart = s.contains(" ") ? s.split(" ")[1].split(":") : new String[]{};
                for (int i = 0; i < timePart.length; i++) {
                    switch (i) {
                        case 0:
                            builder.appendPattern(String.join("", Collections.nCopies(timePart[i].length(), "H")));
                            break;
                        case 1:
                            builder.appendPattern(String.join("", Collections.nCopies(timePart[i].length(), "m")));
                            break;
                        case 2:
                            builder.appendPattern(String.join("", Collections.nCopies(timePart[i].contains(".")
                                    ? timePart[i].split("\\.")[0].length() : timePart[i].length(), "s")));
                            if (timePart[i].contains(".")) {
                                builder.appendFraction(ChronoField.MICRO_OF_SECOND, 0, 6, true);
                            }
                            break;
                        default:
                            throw new AnalysisException("too many parts in time format " + s);
                    }
                    if (i < timePart.length - 1) {
                        builder.appendLiteral(":");
                    }
                }
                // The default resolver style is 'SMART', which parses "2022-06-31" as "2022-06-30"
                // and does not throw an exception. 'STRICT' is used here.
                DateTimeFormatter formatter = builder.toFormatter().withResolverStyle(ResolverStyle.STRICT);
                dateTime = formatter.parse(s);
            }

            year = DateUtils.getOrDefault(dateTime, ChronoField.YEAR);
            month = DateUtils.getOrDefault(dateTime, ChronoField.MONTH_OF_YEAR);
            day = DateUtils.getOrDefault(dateTime, ChronoField.DAY_OF_MONTH);
            hour = DateUtils.getOrDefault(dateTime, ChronoField.HOUR_OF_DAY);
            minute = DateUtils.getOrDefault(dateTime, ChronoField.MINUTE_OF_HOUR);
            second = DateUtils.getOrDefault(dateTime, ChronoField.SECOND_OF_MINUTE);
            microSecond = DateUtils.getOrDefault(dateTime, ChronoField.MICRO_OF_SECOND);

        } catch (Exception ex) {
            throw new AnalysisException("datetime literal [" + s + "] is invalid");
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

    public Expression plusYears(int years) {
        return fromJavaDateType(DateUtils.getTime(DATE_TIME_FORMATTER, getStringValue()).plusYears(years));
    }

    public Expression plusMonths(int months) {
        return fromJavaDateType(DateUtils.getTime(DATE_TIME_FORMATTER, getStringValue()).plusMonths(months));
    }

    public Expression plusDays(int days) {
        return fromJavaDateType(DateUtils.getTime(DATE_TIME_FORMATTER, getStringValue()).plusDays(days));
    }

    public Expression plusHours(int hours) {
        return fromJavaDateType(DateUtils.getTime(DATE_TIME_FORMATTER, getStringValue()).plusHours(hours));
    }

    public Expression plusMinutes(int minutes) {
        return fromJavaDateType(DateUtils.getTime(DATE_TIME_FORMATTER, getStringValue()).plusMinutes(minutes));
    }

    public Expression plusSeconds(long seconds) {
        return fromJavaDateType(DateUtils.getTime(DATE_TIME_FORMATTER, getStringValue()).plusSeconds(seconds));
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
