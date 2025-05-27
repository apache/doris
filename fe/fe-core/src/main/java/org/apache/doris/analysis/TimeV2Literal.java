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

package org.apache.doris.analysis;

import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.thrift.TExprNode;
import org.apache.doris.thrift.TExprNodeType;
import org.apache.doris.thrift.TTimeV2Literal;

public class TimeV2Literal extends LiteralExpr {
    // part min max store every part of time's min max value
    public static final TimeV2Literal PART_MIN = new TimeV2Literal(-838, 0, 0, 0, 0);
    public static final TimeV2Literal PART_MAX = new TimeV2Literal(838, 59, 59, 999999, 6);

    protected long hour;
    protected long minute;
    protected long second;
    protected long microsecond;
    protected boolean negative;

    /**
     * C'tor forcing type, e.g., due to implicit cast
     */
    // for restore
    public TimeV2Literal() {
        this.type = Type.TIMEV2;
        this.hour = 0;
        this.minute = 0;
        this.second = 0;
        this.microsecond = 0;
        this.negative = false;
    }

    public TimeV2Literal(double value) throws AnalysisException {
        super();
        if (value > (double) PART_MAX.getValue() || value < -(double) PART_MAX.getValue()) {
            throw new AnalysisException("The value " + value + " is out of range, expect value range is ["
                    + (-PART_MAX.getValue()) + ", " + PART_MAX.getValue() + "]");
        }
        this.type = ScalarType.createTimeV2Type(6);
        this.negative = 1.0 / value < 0;
        long v = (long) Math.abs(value);
        this.microsecond = (long) (v % 1000000);
        v /= 1000000;
        this.second = (long) (v % 60);
        v /= 60;
        this.minute = (long) (v % 60);
        v /= 60;
        this.hour = (long) v;
        analysisDone();
    }

    public TimeV2Literal(long hour, long minute, long second, long microsecond, int scale) throws AnalysisException {
        super();
        this.type = ScalarType.createTimeV2Type(scale);
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
        analysisDone();
    }

    public TimeV2Literal(String s, ScalarType type) {
        super();
        this.type = type;
        init(s);
        analysisDone();
    }

    protected TimeV2Literal(TimeV2Literal other) {
        super(other);
        this.type = other.type;
        this.hour = other.getHour();
        this.minute = other.getMinute();
        this.second = other.getSecond();
        this.microsecond = other.getMicroSecond();
        this.negative = other.isNegative();
    }

    @Override
    public Expr clone() {
        return new TimeV2Literal(this);
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
        int scale = ((ScalarType) type).getScalarScale();
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

    @Override
    protected String toSqlImpl() {
        return "\"" + getStringValue() + "\"";
    }

    @Override
    protected void toThrift(TExprNode msg) {
        msg.node_type = TExprNodeType.TIMEV2_LITERAL;
        msg.timev2_literal = new TTimeV2Literal(getValue());
    }

    @Override
    public boolean isMinValue() {
        return getValue() == -PART_MAX.getValue();
    }

    @Override
    public int compareLiteral(LiteralExpr expr) {
        return 0;
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
        int scale = ((ScalarType) type).getScalarScale();
        if (scale > 0) {
            sb.append(String.format(".%0" + scale + "d", microsecond / (long) Math.pow(10, 6 - scale)));
        }
        return sb.toString();
    }

    protected static boolean checkRange(long hour, long minute, long second, long microsecond) {
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
        return microsecond;
    }

    public boolean isNegative() {
        return negative;
    }

    public double getValue() {
        if (negative) {
            return (((double) (-hour * 60) - minute) * 60 - second) * 1000000 - microsecond;
        }
        return (((double) (hour * 60) + minute) * 60 + second) * 1000000 + microsecond;
    }
}
