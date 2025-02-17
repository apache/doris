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
import org.apache.doris.common.FormatOptions;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.thrift.TExprNode;
import org.apache.doris.thrift.TExprNodeType;
import org.apache.doris.thrift.TTimeV2Literal;

public class TimeV2Literal extends LiteralExpr {

    public static final TimeV2Literal MIN_TIME = new TimeV2Literal(-838, 0, 0, 0, 0);
    public static final TimeV2Literal MAX_TIME = new TimeV2Literal(838, 59, 59, 999999, 6);

    // The time may start at -0. To keep this negative sign need to use a float number.
    protected double hour;
    protected long minute;
    protected long second;
    protected long microsecond;

    /**
     * C'tor forcing type, e.g., due to implicit cast
     */
    // for restore
    private TimeV2Literal() {
        this.type = Type.TIMEV2;
        this.hour = 0;
        this.minute = 0;
        this.second = 0;
        this.microsecond = 0;
    }

    public TimeV2Literal(double value) {
        super();
        boolean sign = 1.0 / value < 0;
        this.type = ScalarType.createTimeV2Type(6);
        long v = (long) Math.abs(value);
        this.microsecond = v % 1000000;
        v /= 1000000;
        this.second = v % 60;
        v /= 60;
        this.minute = v % 60;
        v /= 60;
        this.hour = v * (sign ? -1 : 1);
        analysisDone();
    }

    public TimeV2Literal(double hour, long minute, long second) {
        super();
        this.type = Type.TIMEV2;
        this.hour = hour;
        this.minute = minute;
        this.second = second;
        this.microsecond = 0;
        analysisDone();
    }

    public TimeV2Literal(double hour, long minute, long second, long microsecond, long scale) {
        super();
        this.type = ScalarType.createTimeV2Type((int) scale);
        this.hour = hour;
        this.minute = minute;
        this.second = second;
        this.microsecond = microsecond;
        while (microsecond != 0 && this.microsecond < 100000) {
            this.microsecond *= 10;
        }
        analysisDone();
    }

    public TimeV2Literal(String s) throws AnalysisException {
        super();
        init(s);
        analysisDone();
    }

    protected TimeV2Literal(TimeV2Literal other) {
        super(other);
        this.type = ScalarType.createTimeV2Type(((ScalarType) other.type).getScalarScale());
        this.hour = other.getHour();
        this.minute = other.getMinute();
        this.second = other.getSecond();
        this.microsecond = other.getMicroSecond();
    }

    @Override
    public Expr clone() {
        return new TimeV2Literal(this);
    }

    protected String normalize(String s) {
        // remove suffix/prefix ' '
        s = s.trim();
        if (!s.contains(":")) {
            boolean sign = false;
            String tail = "";
            if (s.charAt(0) == '-') {
                s = s.substring(1);
                sign = true;
            }
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
            if (sign) {
                s = '-' + s;
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
        int scale = 0;
        try {
            hour = Float.parseFloat(parts[0]);
        } catch (NumberFormatException e) {
            throw new AnalysisException("Invalid hour format", e);
        }

        try {
            minute = Long.parseLong(parts[1]);
        } catch (NumberFormatException e) {
            throw new AnalysisException("Invalid minute format", e);
        }

        String[] secondParts = parts[2].split("\\.");
        if (secondParts.length > 2) {
            throw new AnalysisException("Invalid second format");
        }

        try {
            second = Long.parseLong(secondParts[0]);
        } catch (NumberFormatException e) {
            throw new AnalysisException("Invalid second format", e);
        }

        if (secondParts.length == 2) {
            String microStr = secondParts[1];
            scale = microStr.length();

            if (scale > 6) {
                microStr = microStr.substring(0, 6);
            }

            StringBuilder sb = new StringBuilder(microStr);
            while (sb.length() < 6) {
                sb.append('0');
            }

            try {
                microsecond = Long.parseLong(sb.toString());
            } catch (NumberFormatException e) {
                throw new AnalysisException("Invalid microsecond format", e);
            }
        } else {
            microsecond = 0;
            scale = 0;
        }

        this.type = ScalarType.createTimeV2Type(scale);
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
        return hour == MIN_TIME.getHour() && minute == 59 && second == 59 && microsecond == 999999;
    }

    @Override
    public int compareLiteral(LiteralExpr expr) {
        return 0;
    }

    @Override
    public String getStringValue() {
        double h = Math.max(Math.min(hour, MAX_TIME.getHour()), MIN_TIME.getHour());
        long m = Math.max(Math.min(minute, MAX_TIME.getMinute()), MIN_TIME.getMinute());
        long s = Math.max(Math.min(second, MAX_TIME.getSecond()), MIN_TIME.getSecond());
        long ms = Math.max(Math.min(getMicroSecond(), MAX_TIME.getMicroSecond()), MIN_TIME.getMicroSecond());

        StringBuilder sb = new StringBuilder();
        if (h > 99 || 1.0 / h < 0) {
            sb.append(String.format("%03.0f:%02d:%02d", h, m, s));
        } else {
            sb.append(String.format("%02.0f:%02d:%02d", h, m, s));
        }
        switch (((ScalarType) type).getScalarScale()) {
            case 1:
                sb.append(String.format(".%01d", ms));
                break;
            case 2:
                sb.append(String.format(".%02d", ms));
                break;
            case 3:
                sb.append(String.format(".%03d", ms));
                break;
            case 4:
                sb.append(String.format(".%04d", ms));
                break;
            case 5:
                sb.append(String.format(".%05d", ms));
                break;
            case 6:
                sb.append(String.format(".%06d", ms));
                break;
            default:
                break;
        }
        return sb.toString();
    }

    @Override
    public String getStringValueForArray(FormatOptions options) {
        return options.getNestedStringWrapper() + getStringValue() + options.getNestedStringWrapper();
    }

    protected static boolean checkRange(double hour, long minute, long second, long microsecond) {
        return hour > MAX_TIME.getHour() || minute > MAX_TIME.getMinute() || second > MAX_TIME.getSecond()
                || hour < MIN_TIME.getHour() || minute < MIN_TIME.getMinute() || second < MIN_TIME.getSecond()
                || microsecond < MIN_TIME.getMicroSecond() || microsecond > MAX_TIME.getMicroSecond();
    }

    public double getHour() {
        return hour;
    }

    public long getMinute() {
        return minute;
    }

    public long getSecond() {
        return second;
    }

    public long getMicroSecond() {
        int scale = ((ScalarType) type).getScalarScale();
        return (long) (microsecond / Math.pow(10, 6 - scale)) * (long) Math.pow(10, 6 - scale);
    }

    public double getValue() {
        if (1.0 / hour < 0) {
            return (((hour * 60) - minute) * 60 - second) * 1000000 - microsecond;
        }
        return (((hour * 60) + minute) * 60 + second) * 1000000 + microsecond;
    }
}
