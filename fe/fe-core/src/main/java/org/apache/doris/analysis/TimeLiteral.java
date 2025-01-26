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
import org.apache.doris.thrift.TTimeLiteral;

public class TimeLiteral extends LiteralExpr {

    public static final TimeLiteral MIN_TIME = new TimeLiteral(0, 0, 0, 0, 0);
    public static final TimeLiteral MAX_TIME = new TimeLiteral(838, 59, 59, 999999, 6);

    protected long hour;
    protected long minute;
    protected long second;
    protected long microsecond;

    /**
     * C'tor forcing type, e.g., due to implicit cast
     */
    // for restore
    private TimeLiteral() {
        this.type = Type.TIMEV2;
        this.hour = 0;
        this.minute = 0;
        this.second = 0;
        this.microsecond = 0;
    }

    public TimeLiteral(long hour, long minute, long second) {
        super();
        this.type = Type.TIMEV2;
        this.hour = hour;
        this.minute = minute;
        this.second = second;
        this.microsecond = 0;
        analysisDone();
    }

    public TimeLiteral(long hour, long minute, long second, long microsecond, long scale) {
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

    public TimeLiteral(String s) throws AnalysisException {
        super();
        init(s);
        analysisDone();
    }

    protected TimeLiteral(TimeLiteral other) {
        super(other);
        this.type = ScalarType.createTimeV2Type(((ScalarType) other.type).getScalarScale());
        this.hour = other.getHour();
        this.minute = other.getMinute();
        this.second = other.getSecond();
        this.microsecond = other.getMicroSecond();
    }

    @Override
    public Expr clone() {
        return new TimeLiteral(this);
    }

    protected void init(String s) throws AnalysisException {
        if (!s.contains(":")) {
            int len = s.length();
            s = s.substring(0, len - 4) + ":" + s.substring(len - 4, len - 2) + ":" + s.substring(len - 2);
        }
        if (s.indexOf(':') == s.lastIndexOf(':')) {
            s = s + ":00";
        }
        String[] parts = s.split(":");
        if (parts.length != 3) {
            throw new AnalysisException("Invalid format, must have 3 parts separated by ':'");
        }
        int scale = 0;
        try {
            hour = Long.parseLong(parts[0]);
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
            microsecond = 0L;
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
        msg.node_type = TExprNodeType.TIME_LITERAL;
        msg.time_literal = new TTimeLiteral(getStringValue());
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
        long h = Math.max(Math.min(hour, MAX_TIME.getHour()), MIN_TIME.getHour());
        long m = Math.max(Math.min(minute, MAX_TIME.getMinute()), MIN_TIME.getMinute());
        long s = Math.max(Math.min(second, MAX_TIME.getSecond()), MIN_TIME.getSecond());
        long ms = Math.max(Math.min(getMicroSecond(), MAX_TIME.getMicroSecond()), MIN_TIME.getMicroSecond());

        StringBuilder sb = new StringBuilder();
        if (h > 99) {
            sb.append(String.format("%03d:%02d:%02d", h, m, s));
        } else {
            sb.append(String.format("%02d:%02d:%02d", h, m, s));
        }
        switch (((ScalarType) type).getScalarScale()) {
            case 1:
                sb.append(String.format(".%1d", ms));
                break;
            case 2:
                sb.append(String.format(".%2d", ms));
                break;
            case 3:
                sb.append(String.format(".%3d", ms));
                break;
            case 4:
                sb.append(String.format(".%4d", ms));
                break;
            case 5:
                sb.append(String.format(".%5d", ms));
                break;
            case 6:
                sb.append(String.format(".%6d", ms));
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

    protected static boolean checkRange(long hour, long minute, long second, long microsecond) {
        return hour > MAX_TIME.getHour() || minute > MAX_TIME.getMinute() || second > MAX_TIME.getSecond()
                || hour < MIN_TIME.getHour() || minute < MIN_TIME.getMinute() || second < MIN_TIME.getSecond()
                || microsecond > MIN_TIME.getMicroSecond() || microsecond > MAX_TIME.getMicroSecond();
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
        return (long) (microsecond / Math.pow(10, 6 - ((ScalarType) type).getScalarScale()));
    }

}
