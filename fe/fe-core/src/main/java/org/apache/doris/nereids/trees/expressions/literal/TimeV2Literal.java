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
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.TimeStampNsType;
import org.apache.doris.nereids.types.TimeV2Type;
import org.apache.doris.nereids.util.DateUtils;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDateTime;

/**
 * Time literal in Nereids.
 */
public class TimeV2Literal extends Literal {
    private static final LocalDateTime START_OF_A_DAY = LocalDateTime.of(0, 1, 1, 0, 0, 0);
    private static final LocalDateTime END_OF_A_DAY = LocalDateTime.of(9999, 12, 31, 23, 59, 59, 999999999);
    private static final TimeV2Literal MIN_VALUE = new TimeV2Literal(838, 59, 59, 0, 9, true);
    private static final TimeV2Literal MAX_VALUE = new TimeV2Literal(838, 59, 59, 0, 9, false);

    protected int hour;
    protected int minute;
    protected int second;
    protected int microsecond;
    protected int nanosecondRemainder;
    protected boolean negative;

    public TimeV2Literal(TimeV2Type dataType, String s) {
        super(dataType);
        init(s);
    }

    public TimeV2Literal(String s) {
        super(TimeV2Type.forTypeFromString(s));
        init(s);
    }

    /**
     * C'tor time literal.
     */
    public TimeV2Literal(double value) throws AnalysisException {
        super(TimeV2Type.of(6));
        init(value, 6);
    }

    /**
     * C'tor time literal with confirmed scale.
     */
    public TimeV2Literal(double value, int scale) throws AnalysisException {
        super(TimeV2Type.of(scale));
        init(value, scale);
    }

    /**
     * C'tor for time type.
     */
    // for -00:... so we need explicite negative
    public TimeV2Literal(int hour, int minute, int second, int microsecond, int scale, boolean negative)
            throws AnalysisException {
        this(hour, minute, second, (long) microsecond * 1000, scale, negative);
    }

    private TimeV2Literal(int hour, int minute, int second, long nanosecond, int scale, boolean negative)
            throws AnalysisException {
        super(TimeV2Type.of(scale));
        this.hour = hour;
        this.minute = minute;
        this.second = second;
        int factor = (int) Math.pow(10, TimeV2Type.MAX_SCALE - scale);
        int roundedNanosecond = (int) (nanosecond / factor * factor);
        this.microsecond = roundedNanosecond / 1000;
        this.nanosecondRemainder = roundedNanosecond % 1000;
        this.negative = negative;
        if (checkRange(this.hour, this.minute, this.second, roundedNanosecond)) {
            throw new AnalysisException(
                    "time literal is out of range [-838:59:59.999999999, 838:59:59.999999999]");
        }
    }

    /** Create a TIMEV2 literal from a full nanosecond-of-second value. */
    public static TimeV2Literal fromNanosecond(int hour, int minute, int second, int nanosecond,
            int scale, boolean negative) {
        return new TimeV2Literal(hour, minute, second, (long) nanosecond, scale, negative);
    }

    protected static String normalize(String s) {
        // remove suffix/prefix ' '
        s = s.trim();
        // just a number
        if (!s.contains(":")) {
            String tail = "";
            if (s.contains(".")) {
                tail = s.substring(s.indexOf("."));
                s = s.substring(0, s.indexOf("."));
            }
            int len = s.length();
            if (len == 1) {
                s = "00:00:0" + s;
            } else if (len == 2) {
                s = "00:00:" + s;
            } else if (len == 3) {
                s = "00:0" + s.charAt(0) + ":" + s.substring(1);
            } else if (len == 4) {
                s = "00:" + s.substring(0, 2) + ":" + s.substring(2);
            } else {
                // minute and second must be 2 digits. others put in front as hour
                s = s.substring(0, len - 4) + ":" + s.substring(len - 4, len - 2) + ":" + s.substring(len - 2);
            }
            return s + tail;
        }
        // s maybe just contail 1 ":" like "12:00" so append a ":00" to the end
        if (s.indexOf(':') == s.lastIndexOf(':')) {
            s = s + ":00";
        }
        return s;
    }

    /**
     * parse time string and avoid throw exception directly for better performance.
     */
    public static Result<TimeV2Literal, AnalysisException> parseTimeLiteral(String s) {
        try {
            int scale = determineScale(s);
            return Result.ok(new TimeV2Literal(TimeV2Type.of(scale), s));
        } catch (AnalysisException e) {
            return Result.err(() -> e);
        }
    }

    private void init(double value, int scale) {
        if (value > (double) MAX_VALUE.getValue() || value < (double) MIN_VALUE.getValue()) {
            throw new AnalysisException("The value " + value + " is out of range, expect value range is ["
                + (double) MIN_VALUE.getValue() + ", " + (double) MAX_VALUE.getValue() + "]");
        }
        this.negative = value < 0;
        long v = Math.round(Math.abs(value) * 1000);
        long factor = (long) Math.pow(10, TimeV2Type.MAX_SCALE - scale);
        v = (v + factor / 2) / factor * factor;
        int nanosecond = (int) (v % 1000000000);
        this.microsecond = nanosecond / 1000;
        this.nanosecondRemainder = nanosecond % 1000;
        v /= 1000000000;
        this.second = (int) (v % 60);
        v /= 60;
        this.minute = (int) (v % 60);
        v /= 60;
        this.hour = (int) v;
    }

    // should like be/src/vec/runtime/time_value.h timev2_to_double_from_str
    protected void init(String s) throws AnalysisException {
        s = normalize(s);
        if (s.charAt(0) == '-') {
            negative = true;
            s = s.substring(1);
        } else if (s.charAt(0) == '+') {
            s = s.substring(1);
        }
        // start parse string
        String[] parts = s.split(":");
        if (parts.length != 3) {
            throw new AnalysisException("Invalid format, must have 3 parts separated by ':'");
        }
        try {
            hour = Integer.parseInt(parts[0]);
        } catch (NumberFormatException e) {
            throw new AnalysisException("Invalid hour format", e);
        }

        try {
            minute = Integer.parseInt(parts[1]);
        } catch (NumberFormatException e) {
            throw new AnalysisException("Invalid minute format", e);
        }
        int scale = ((TimeV2Type) dataType).getScale();
        // if parts[2] is 60.000 it will cause judge feed execute error
        if (parts[2].startsWith("60")) {
            throw new AnalysisException("second out of range");
        }
        long scaledSecond;
        try {
            scaledSecond = new BigDecimal(parts[2]).movePointRight(scale)
                    .setScale(0, RoundingMode.HALF_UP).longValueExact();
        } catch (ArithmeticException | NumberFormatException e) {
            throw new AnalysisException("Invalid second format", e);
        }
        long secondInNanoseconds = scaledSecond
                * (long) Math.pow(10, TimeV2Type.MAX_SCALE - scale);
        second = (int) (secondInNanoseconds / 1000000000);
        int nanosecond = (int) (secondInNanoseconds % 1000000000);
        microsecond = nanosecond / 1000;
        nanosecondRemainder = nanosecond % 1000;
        if (second == 60) {
            minute += 1;
            second -= 60;
            if (minute == 60) {
                hour += 1;
                minute -= 60;
            }
        }

        if (checkRange(hour, minute, second, nanosecond)) {
            throw new AnalysisException("time literal [" + s + "] is out of range");
        }
    }

    protected static boolean checkRange(double hour, int minute, int second, int nanosecond) {
        return hour > 838 || minute > 59 || second > 59 || nanosecond > 999999999
                || minute < 0 || second < 0 || nanosecond < 0;
    }

    /**
     * determine scale by time string. didn't check if the string is valid.
     */
    public static int determineScale(String s) {
        // find point
        s = normalize(s);
        int pointIndex = s.indexOf('.');
        if (pointIndex < 0) {
            return 0; // no point, scale is 0
        }
        String microPart = s.substring(pointIndex + 1);
        int len = microPart.length();
        while (len > 0 && microPart.charAt(len - 1) == '0') {
            len--; // remove trailing zeros
        }
        return Math.min(len, TimeV2Type.MAX_SCALE);
    }

    public int getHour() {
        return hour;
    }

    public int getMinute() {
        return minute;
    }

    public int getSecond() {
        return second;
    }

    public int getMicroSecond() {
        return microsecond;
    }

    public int getNanoSecond() {
        return microsecond * 1000 + nanosecondRemainder;
    }

    public long getValueInNanoseconds() {
        long value = ((hour * 60L + minute) * 60L + second) * 1000000000L + getNanoSecond();
        return negative ? -value : value;
    }

    @Override
    protected Expression uncheckedCastTo(DataType targetType) throws AnalysisException {
        long nanosecondValue = getValueInNanoseconds();
        DateTimeV2Literal time = (DateTimeV2Literal) DateTimeV2Literal.fromJavaDateType(LocalDateTime
                .now(DateUtils.getTimeZone()).withHour(0).withMinute(0).withSecond(0).withNano(0)
                        .plusNanos(nanosecondValue),
                Math.min(((TimeV2Type) dataType).getScale(), 6));
        if (targetType.isDateType()) {
            return new DateLiteral(time.getYear(), time.getMonth(), time.getDay());
        } else if (targetType.isDateV2Type()) {
            return new DateV2Literal(time.getYear(), time.getMonth(), time.getDay());
        } else if (targetType.isDateTimeType()) {
            return new DateTimeLiteral(time.getYear(), time.getMonth(), time.getDay(), time.getHour(), time.getMinute(),
                    time.getSecond());
        } else if (targetType instanceof TimeStampNsType) {
            LocalDateTime timestamp = LocalDateTime.now(DateUtils.getTimeZone())
                    .withHour(0).withMinute(0).withSecond(0).withNano(0).plusNanos(nanosecondValue);
            return new TimeStampNsLiteral(timestamp.getYear(), timestamp.getMonthValue(),
                    timestamp.getDayOfMonth(), timestamp.getHour(), timestamp.getMinute(),
                    timestamp.getSecond(), timestamp.getNano());
        } else if (targetType.isDateTimeV2Type()) {
            return time;
        }
        return super.uncheckedCastTo(targetType);
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitTimeV2Literal(this, context);
    }

    @Override
    public LiteralExpr toLegacyLiteral() {
        int scale = ((TimeV2Type) dataType).getScale();
        return org.apache.doris.analysis.TimeV2Literal.fromNanosecond(
                hour, minute, second, getNanoSecond(), scale, negative);
    }

    @Override
    public String getStringValue() {
        StringBuilder sb = new StringBuilder();
        if (negative) {
            sb.append("-");
        }
        if (hour > 99) {
            sb.append(String.format("%03d:%02d:%02d", hour, minute, second));
        } else {
            sb.append(String.format("%02d:%02d:%02d", hour, minute, second));
        }
        int scale = ((TimeV2Type) dataType).getScale();
        if (scale > 0) {
            sb.append(String.format(".%0" + scale + "d",
                    getNanoSecond() / (int) Math.pow(10, TimeV2Type.MAX_SCALE - scale)));
        }
        return sb.toString();
    }

    @Override
    protected String castValueToString() {
        return getStringValue();
    }

    public static boolean isDateOutOfRange(LocalDateTime dateTime) {
        return dateTime == null || dateTime.isBefore(START_OF_A_DAY) || dateTime.isAfter(END_OF_A_DAY);
    }

    public static Expression fromJavaDateType(LocalDateTime dateTime) {
        if (isDateOutOfRange(dateTime)) {
            throw new AnalysisException("datetime out of range: " + dateTime.toString());
        }
        return new TimeV2Literal(dateTime.getHour(), dateTime.getMinute(), dateTime.getSecond(), 0, 0, false);
    }

    /**
     *  construct with precision
     */
    public static Expression fromJavaDateType(LocalDateTime dateTime, int precision) {
        if (isDateOutOfRange(dateTime)) {
            throw new AnalysisException("datetime out of range" + dateTime.toString());
        }
        int value = (int) Math.pow(10, TimeV2Type.MAX_SCALE - precision);
        return TimeV2Literal.fromNanosecond(dateTime.getHour(), dateTime.getMinute(), dateTime.getSecond(),
                dateTime.getNano() / value * value, precision, false);
    }

    public LocalDateTime toJavaDateType() {
        return LocalDateTime.of(0, 1, 1, ((int) getHour()), ((int) getMinute()), ((int) getSecond()),
                getNanoSecond());
    }

    public Expression plusMicroSeconds(long microSeconds) {
        return fromJavaDateType(toJavaDateType().plusNanos(Math.multiplyExact(microSeconds, 1000L)));
    }

    /** Add nanoseconds and retain the requested TIMEV2 precision. */
    public Expression plusNanoSeconds(long nanoSeconds, int precision) {
        long result = Math.addExact(getValueInNanoseconds(), nanoSeconds);
        long maxValue = MAX_VALUE.getValueInNanoseconds();
        result = Math.max(-maxValue, Math.min(maxValue, result));
        boolean resultNegative = result < 0;
        long absolute = Math.abs(result);
        int resultNanosecond = (int) (absolute % 1000000000L);
        long totalSeconds = absolute / 1000000000L;
        int resultSecond = (int) (totalSeconds % 60);
        int resultMinute = (int) (totalSeconds / 60 % 60);
        int resultHour = (int) (totalSeconds / 3600);
        return TimeV2Literal.fromNanosecond(resultHour, resultMinute, resultSecond,
                resultNanosecond, precision, resultNegative);
    }

    public Expression plusMilliSeconds(long milliSeconds) {
        return plusMicroSeconds(Math.multiplyExact(milliSeconds, 1000L));
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

    @Override
    public Object getValue() {
        if (negative) {
            return (((double) (-hour * 60) - minute) * 60 - second) * 1000000
                    - microsecond - nanosecondRemainder / 1000.0;
        }
        return (((double) (hour * 60) + minute) * 60 + second) * 1000000
                + microsecond + nanosecondRemainder / 1000.0;
    }

    @Override
    public String computeToSql() {
        return "'" + getStringValue() + "'";
    }

    public boolean isNegative() {
        return negative;
    }
}
