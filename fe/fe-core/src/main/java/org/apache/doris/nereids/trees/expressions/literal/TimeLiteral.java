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
import org.apache.doris.nereids.types.TimeV2Type;

import java.time.LocalDateTime;

/**
 * Time literal in Nereids.
 */
public class TimeLiteral extends Literal {
    private static final LocalDateTime START_OF_A_DAY = LocalDateTime.of(0, 1, 1, 0, 0, 0);
    private static final LocalDateTime END_OF_A_DAY = LocalDateTime.of(9999, 12, 31, 23, 59, 59, 999999000);
    private static final TimeLiteral MIN_TIME = new TimeLiteral(-838, 0, 0, 0, 0);
    private static final TimeLiteral MAX_TIME = new TimeLiteral(838, 59, 59, 999999, 6);

    protected long hour;
    protected long minute;
    protected long second;
    protected long microsecond;
    protected long scale;

    public TimeLiteral(String s) throws AnalysisException {
        this(TimeV2Type.INSTANCE, s);
    }

    protected TimeLiteral(TimeV2Type dataType, String s) throws AnalysisException {
        super(dataType);
        init(s);
    }

    /**
     * C'tor time literal.
     */
    public TimeLiteral(long hour, long minute, long second) {
        this(TimeV2Type.INSTANCE, hour, minute, second);
    }

    /**
     * C'tor for time type.
     */
    public TimeLiteral(TimeV2Type dataType, long hour, long minute, long second) {
        super(dataType);
        this.hour = hour;
        this.minute = minute;
        this.second = second;
        this.microsecond = 0;
        this.scale = 0;
    }

    public TimeLiteral(long hour, long minute, long second, long microsecond, int scale) {
        super(TimeV2Type.INSTANCE);
        this.hour = hour;
        this.minute = minute;
        this.second = second;
        this.microsecond = microsecond;
        while (microsecond != 0 && this.microsecond < 100000) {
            this.microsecond *= 10;
        }
        this.scale = scale;
    }

    protected void init(String s) throws AnalysisException {
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

        if (checkRange(hour, minute, second, microsecond)) {
            throw new AnalysisException("time literal [" + s + "] is out of range");
        }
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
        return (long) (microsecond / Math.pow(10, 6 - scale));
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitTimeLiteral(this, context);
    }

    @Override
    public LiteralExpr toLegacyLiteral() {
        return new org.apache.doris.analysis.TimeLiteral(hour, minute, second, microsecond, scale);
    }

    @Override
    public String getStringValue() {
        long h = Math.max(Math.min(hour, MAX_TIME.getHour()), MIN_TIME.getHour());
        long m = Math.max(Math.min(minute, MAX_TIME.getMinute()), MIN_TIME.getMinute());
        long s = Math.max(Math.min(second, MAX_TIME.getSecond()), MIN_TIME.getSecond());
        long ms = Math.max(Math.min(getMicroSecond(), MAX_TIME.getMicroSecond()), MIN_TIME.getMicroSecond());

        StringBuilder sb = new StringBuilder();
        if (h > 99 || h < -99) {
            sb.append(String.format("%03d:%02d:%02d", h, m, s));
        } else {
            sb.append(String.format("%02d:%02d:%02d", h, m, s));
        }
        switch ((int) scale) {
            case 0:
                break;
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
        return new TimeLiteral(dateTime.getHour(), dateTime.getMinute(), dateTime.getSecond());
    }

    @Override
    public Object getValue() {
        return (((hour * 60) + minute * 60) + second) * 1000000 + microsecond;
    }
}
