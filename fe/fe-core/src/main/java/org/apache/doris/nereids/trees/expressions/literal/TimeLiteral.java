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
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.TimeType;
import org.apache.doris.nereids.util.DateTimeFormatterUtils;
import org.apache.doris.nereids.util.DateUtils;

import com.google.common.collect.ImmutableSet;

import java.time.DateTimeException;
import java.time.LocalTime;
import java.time.LocalDateTime;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.Set;

/**
 * Time literal in Nereids.
 */
public class TimeLiteral extends Literal {
    public static final String JAVA_TIME_FORMAT = "HH:mm:ss";

    public static final Set<Character> punctuations = ImmutableSet.of('!', '@', '#', '$', '%', '^', '&', '*', '(', ')',
    '-', '+', '=', '_', '{', '}', '[', ']', '|', '\\', ':', ';', '"', '\'', '<', '>', ',', '.', '?', '/', '~',
    '`');

    private static final LocalDateTime START_OF_A_DAY = LocalDateTime.of(0, 1, 1, 0, 0, 0);
    private static final LocalDateTime END_OF_A_DAY = LocalDateTime.of(9999, 12, 31, 23, 59, 59, 999999000);
    public static final TimeLiteral MIN_TIME = new TimeLiteral(0, 0, 0);
    public static final TimeLiteral MAX_TIME = new TimeLiteral(23, 59, 59);

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

    private static boolean isPunctuation(char c) {
        return punctuations.contains(c);
    }

    static Result<String, AnalysisException> normalize(String s) {
        boolean containsPunctuation = false;
        for (int i = 0; i < s.length(); i++) {
            if (isPunctuation(s.charAt(i))) {
                containsPunctuation = true;
                break;
            }
        }
        StringBuilder sb = new StringBuilder();

        if (!containsPunctuation) {
            int len = s.length();
            if (len == 1) {
                sb.append("00:00:0");
                sb.append(s);
            } else if (len == 2) {
                sb.append("00:00:");
                sb.append(s);
            } else if (len == 3) {
                sb.append("00:0");
                sb.append(s.substring(0, 1));
                sb.append(":");
                sb.append(s.substring(1, 3));
            } else if (len == 4) {
                sb.append("00:");
                sb.append(s.substring(0, 2));
                sb.append(":");
                sb.append(s.substring(2, 4));
            } else {
                sb.append(s.substring(0, len - 4));
                sb.append(":");
                sb.append(s.substring(len - 4, len - 2));
                sb.append(":");
                sb.append(s.substring(len - 2, len));
            }
            return Result.ok(sb.toString());
        }

        int partNumber = 0;
        for (int i = 0; i < s.length() && partNumber < 3; i++) {
            if (isPunctuation(s.charAt(i))) {
                partNumber++;
                if (partNumber != 3) {
                    sb.append(":");
                }
            } else {
                sb.append(s.charAt(i));
            }
        }
        return Result.ok(sb.toString());
    }

    /** parseTime */
    public static Result<TemporalAccessor, ? extends Exception> parseTime(String s) {
        String originalString = s;
        try {
            if (s.length() == 8) {
                if (s.charAt(2) == ':' && s.charAt(5) == ':') {
                    TemporalAccessor time = fastParseTime(s);
                    if (time != null) {
                        return Result.ok(time);
                    }
                }
            }

            TemporalAccessor time;

            s = s.trim();
            Result<String, AnalysisException> normalizeResult = normalize(s);
            if (normalizeResult.isError()) {
                return normalizeResult.cast();
            }
            s = normalizeResult.get();

            time = DateTimeFormatterUtils.TIME_FORMATTER.parse(s);

            return Result.ok(time);
        } catch (DateTimeException e) {
            return Result.err(() ->
                    new DateTimeException("time literal [" + originalString + "] is invalid", e)
            );
        } catch (Exception ex) {
            return Result.err(() -> new AnalysisException("time literal [" + originalString + "] is invalid"));
        }
    }

    private static TemporalAccessor fastParseTime(String time) {
        Integer hour = readNextInt(time, 0, 2);
        Integer minute = readNextInt(time, 3, 2);
        Integer second = readNextInt(time, 6, 2);

        if (hour != null && minute != null && second != null) {
            return LocalTime.of(hour, minute, second);
        } else {
            return null;
        }
    }

    protected void init(String s) throws AnalysisException {
        TemporalAccessor time = parseTime(s).get();
        hour = DateUtils.getOrDefault(time, ChronoField.HOUR_OF_DAY);
        minute = DateUtils.getOrDefault(time, ChronoField.MINUTE_OF_HOUR);
        second = DateUtils.getOrDefault(time, ChronoField.SECOND_OF_MINUTE);

        if (checkTime(time) || checkRange(hour, minute, second) || checkTime(hour, minute, second)) {
            throw new AnalysisException("time literal [" + s + "] is out of range");
        }
    }

    protected static boolean checkRange(long hour, long minute, long second) {
        return hour > MAX_TIME.getHour() || minute > MAX_TIME.getMinute() || second > MAX_TIME.getSecond();
    }

    protected static boolean checkTime(long hour, long minute, long second) {
        if (hour < 24 && minute < 60 && second < 60) {
            return true;
        }
        return false;
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
        return String.format("%02d:%02d:%02d", hour, minute, second);
    }

    @Override
    public Long getValue() {
        return (hour * 60 + minute * 60 + second) * 1000000L;
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

    public static boolean isDateOutOfRange(LocalDateTime dateTime) {
        return dateTime == null || dateTime.isBefore(START_OF_A_DAY) || dateTime.isAfter(END_OF_A_DAY);
    }

    public static Expression fromJavaDateType(LocalDateTime dateTime) {
        if (isDateOutOfRange(dateTime)) {
            throw new AnalysisException("datetime out of range: " + dateTime.toString());
        }
        return new TimeLiteral(dateTime.getHour(), dateTime.getMinute(), dateTime.getSecond());
    }
}
