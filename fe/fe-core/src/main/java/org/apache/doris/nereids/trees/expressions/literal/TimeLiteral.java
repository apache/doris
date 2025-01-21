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
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.TimeType;
import org.apache.doris.nereids.util.DateTimeFormatterUtils;
import org.apache.doris.nereids.util.DateUtils;

import java.time.DateTimeException;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;

/**
 * Time literal in Nereids.
 */
public class TimeLiteral extends Literal {
    public static final String JAVA_TIME_FORMAT = "HH:mm:ss";

    private static final LocalDateTime START_OF_A_DAY = LocalDateTime.of(0, 1, 1, 0, 0, 0);
    private static final LocalDateTime END_OF_A_DAY = LocalDateTime.of(9999, 12, 31, 23, 59, 59, 999999000);
    private static final TimeLiteral MIN_TIME = new TimeLiteral(-838, -59, -59);
    private static final TimeLiteral MAX_TIME = new TimeLiteral(838, 59, 59);

    protected long hour;
    protected long minute;
    protected long second;

    public TimeLiteral(String s) throws AnalysisException {
        this(TimeType.INSTANCE, s);
    }

    protected TimeLiteral(TimeType dataType, String s) throws AnalysisException {
        super(dataType);
        init(s);
    }

    /**
     * C'tor time literal.
     */
    public TimeLiteral(long hour, long minute, long second) {
        this(TimeType.INSTANCE, hour, minute, second);
    }

    /**
     * C'tor for time type.
     */
    public TimeLiteral(TimeType dataType, long hour, long minute, long second) {
        super(dataType);
        this.hour = hour;
        this.minute = minute;
        this.second = second;
    }

    /** parseTime */
    public static Result<TemporalAccessor, ? extends Exception> parseTime(String s) {
        String originalString = s;
        try {
            Integer hour;
            Integer minute;
            Integer second;
            if (s.length() == 8) {
                hour = readNextInt(s, 0, 2);
                minute = readNextInt(s, 3, 2);
                second = readNextInt(s, 6, 2);
            } else if (s.length() == 9) {
                hour = readNextInt(s, 0, 3);
                minute = readNextInt(s, 4, 2);
                second = readNextInt(s, 7, 2);
            } else {
                TemporalAccessor time;
                time = DateTimeFormatterUtils.TIME_FORMATTER.parse(s);
                return Result.ok(time);
            }

            return Result.ok(LocalTime.of(hour, minute, second));
        } catch (DateTimeException e) {
            return Result.err(() ->
                    new DateTimeException("time literal [" + originalString + "] is invalid", e)
            );
        }
    }

    protected void init(String s) throws AnalysisException {
        TemporalAccessor time = parseTime(s).get();
        hour = DateUtils.getOrDefault(time, ChronoField.HOUR_OF_DAY);
        minute = DateUtils.getOrDefault(time, ChronoField.MINUTE_OF_HOUR);
        second = DateUtils.getOrDefault(time, ChronoField.SECOND_OF_MINUTE);

        if (checkTime(time) || checkRange(hour, minute, second)) {
            throw new AnalysisException("time literal [" + s + "] is out of range");
        }
    }

    protected static boolean checkRange(long hour, long minute, long second) {
        return hour > MAX_TIME.getHour() || minute > MAX_TIME.getMinute() || second > MAX_TIME.getSecond();
    }

    private static boolean checkTime(TemporalAccessor time) {
        return DateUtils.getOrDefault(time, ChronoField.HOUR_OF_DAY) != 0
                || DateUtils.getOrDefault(time, ChronoField.MINUTE_OF_HOUR) != 0
                || DateUtils.getOrDefault(time, ChronoField.SECOND_OF_MINUTE) != 0
                || DateUtils.getOrDefault(time, ChronoField.MICRO_OF_SECOND) != 0;
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
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitTimeLiteral(this, context);
    }

    @Override
    public LiteralExpr toLegacyLiteral() {
        return new org.apache.doris.analysis.TimeLiteral(hour, minute, second);
    }

    @Override
    public String toString() {
        long h = Math.max(Math.min(hour, MAX_TIME.getHour()), MIN_TIME.getHour());
        long m = Math.max(Math.min(minute, MAX_TIME.getMinute()), MIN_TIME.getMinute());
        long s = Math.max(Math.min(second, MAX_TIME.getSecond()), MIN_TIME.getSecond());

        if (h > 99 || h < -99) {
            return String.format("%03d:%02d:%02d", h, m, s);
        }
        return String.format("%02d:%02d:%02d", h, m, s);
    }

    @Override
    public Long getValue() {
        return hour * 10000 + minute * 100 + second;
    }

    public static boolean isDateOutOfRange(LocalDateTime dateTime) {
        return dateTime == null || dateTime.isBefore(START_OF_A_DAY) || dateTime.isAfter(END_OF_A_DAY);
    }

    public static Expression fromJavaDateType(LocalDateTime dateTime) {
        if (isDateOutOfRange(dateTime)) {
            throw new AnalysisException("datetime out of range: " + dateTime.toString());
        }
        return new TimeLiteral(dateTime.getHour(), dateTime.getMinute(), dateTime.getSecond());
    }

    private static Integer readNextInt(String str, int offset, int readLength) {
        int value = 0;
        int realReadLength = 0;
        for (int i = offset; i < str.length(); i++) {
            char c = str.charAt(i);
            if ('0' <= c && c <= '9') {
                realReadLength++;
                value = value * 10 + (c - '0');
            } else {
                break;
            }
        }
        return readLength == realReadLength ? value : null;
    }
}
