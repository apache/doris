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
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.TimeV2Type;

import java.time.LocalDateTime;

/**
 * Time literal in Nereids.
 */
public class TimeV2Literal extends Literal {
    private static final LocalDateTime START_OF_A_DAY = LocalDateTime.of(0, 1, 1, 0, 0, 0);
    private static final LocalDateTime END_OF_A_DAY = LocalDateTime.of(9999, 12, 31, 23, 59, 59, 999999000);
    // part min max store every part of time's min max value
    private static final TimeV2Literal PART_MIN = new TimeV2Literal(-838, 0, 0, 0, 0);
    private static final TimeV2Literal PART_MAX = new TimeV2Literal(838, 59, 59, 999999, 6);

    protected long hour;
    protected long minute;
    protected long second;
    protected long microsecond;
    protected boolean negative;

    public TimeV2Literal(TimeV2Type dataType, String s) {
        super(dataType);
        init(s);
    }

    /**
     * C'tor time literal.
     */
    public TimeV2Literal(double value) throws AnalysisException {
        super(TimeV2Type.of(6));
        if (value > (double) PART_MAX.getValue() || value < -(double) PART_MAX.getValue()) {
            throw new AnalysisException("The value " + value + " is out of range, expect value range is ["
                    + (-(double) PART_MAX.getValue()) + ", " + PART_MAX.getValue() + "]");
        }
        this.negative = 1.0 / value < 0;
        long v = (long) Math.abs(value);
        this.microsecond = (long) (v % 1000000);
        v /= 1000000;
        this.second = (long) (v % 60);
        v /= 60;
        this.minute = (long) (v % 60);
        v /= 60;
        this.hour = (long) v;
    }

    /**
     * C'tor for time type.
     */
    public TimeV2Literal(long hour, long minute, long second, long microsecond, int scale) throws AnalysisException {
        super(TimeV2Type.of(scale));
        this.hour = Math.abs(hour);
        this.minute = minute;
        this.second = second;
        this.microsecond = (long) (microsecond / Math.pow(10, 6 - scale)) * (long) Math.pow(10, 6 - scale);
        while (microsecond != 0 && this.microsecond < 100000) {
            this.microsecond *= 10;
        }
        this.negative = hour < 0;
        if (checkRange(this.hour, this.minute, this.second, this.microsecond) || scale > 6 || scale < 0) {
            throw new AnalysisException("time literal is out of range [-838:59:59.999999, 838:59:59.999999]");
        }
    }

    protected String normalize(String s) {
        // remove suffix/prefix ' '
        s = s.trim();
        if (s.charAt(0) == '-') {
            s = s.substring(1);
            negative = true;
        } else if (s.charAt(0) == '+') {
            s = s.substring(1);
            negative = false;
        }
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

    // should like be/src/vec/runtime/time_value.h timev2_to_double_from_str
    protected void init(String s) throws AnalysisException {
        s = normalize(s);
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
        double secPart;
        try {
            secPart = Double.parseDouble(parts[2]);
        } catch (NumberFormatException e) {
            throw new AnalysisException("Invalid second format", e);
        }
        secPart = secPart * (long) Math.pow(10, scale);
        secPart = Math.round(secPart);
        secPart = (long) secPart * (long) Math.pow(10, 6 - scale);
        second = (long) secPart / 1000000;
        if (scale != 0) {
            microsecond = (long) secPart % 1000000;
            if (second == 60) {
                minute += 1;
                second -= 60;
                if (minute == 60) {
                    hour += 1;
                    minute -= 60;
                }
            }
        } else {
            microsecond = 0;
        }

        if (checkRange(hour, minute, second, microsecond)) {
            throw new AnalysisException("time literal [" + s + "] is out of range");
        }
    }

    protected static boolean checkRange(double hour, long minute, long second, long microsecond) {
        return hour > 838 || minute > 59 || second > 59 || hour < 0 || minute < 0 || second < 0
                || microsecond > 999999 || microsecond < 0;
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
        long scale = ((TimeV2Type) dataType).getScale();
        return (long) (microsecond / Math.pow(10, 6 - scale)) * (long) Math.pow(10, 6 - scale);
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitTimeV2Literal(this, context);
    }

    @Override
    public LiteralExpr toLegacyLiteral() {
        int scale = ((TimeV2Type) dataType).getScale();
        return new org.apache.doris.analysis.TimeV2Literal(getStringValue(), ScalarType.createTimeV2Type(scale));
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
        // why re caculate microsecond? example:
        // the microsecond is 001000, it will parsed to 1000
        // the scale is 3, we need make sure it not become start with 1
        int scale = ((TimeV2Type) dataType).getScale();
        if (scale > 0) {
            sb.append(String.format(".%0" + scale + "d", microsecond / (long) Math.pow(10, 6 - scale)));
        }
        return sb.toString();
    }

    public static boolean isDateOutOfRange(LocalDateTime dateTime) {
        return dateTime == null || dateTime.isBefore(START_OF_A_DAY) || dateTime.isAfter(END_OF_A_DAY);
    }

    public static Expression fromJavaDateType(LocalDateTime dateTime) {
        if (isDateOutOfRange(dateTime)) {
            throw new AnalysisException("datetime out of range: " + dateTime.toString());
        }
        return new TimeV2Literal(dateTime.getHour(), dateTime.getMinute(), dateTime.getSecond(), 0, 0);
    }

    public LocalDateTime toJavaDateType() {
        return LocalDateTime.of(0, 1, 1, ((int) getHour()), ((int) getMinute()), ((int) getSecond()),
                (int) getMicroSecond() * 1000);
    }

    @Override
    public Object getValue() {
        if (negative) {
            return (((double) (-hour * 60) - minute) * 60 - second) * 1000000 - microsecond;
        }
        return (((double) (hour * 60) + minute) * 60 + second) * 1000000 + microsecond;
    }

    @Override
    public String computeToSql() {
        return "'" + getStringValue() + "'";
    }
}
