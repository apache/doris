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

import org.apache.doris.common.FormatOptions;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.literal.Result;
import org.apache.doris.nereids.util.DateUtils;
import org.apache.doris.thrift.TExprNode;
import org.apache.doris.thrift.TExprNodeType;
import org.apache.doris.thrift.TTimeLiteral;

import java.lang.Math;
import java.time.DateTimeException;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;

public class TimeLiteral extends LiteralExpr {

    public static final TimeLiteral MIN_TIME = new TimeLiteral(-838, -59, -59);
    public static final TimeLiteral MAX_TIME = new TimeLiteral(838, 59, 59);

    protected long hour;
    protected long minute;
    protected long second;

    /**
     * C'tor forcing type, e.g., due to implicit cast
     */
    // for restore
    private TimeLiteral() {
        this.hour = 0;
        this.minute = 0;
        this.second = 0;
    }

    public TimeLiteral(long hour, long minute, long second) {
        super();
        this.hour = Math.min(hour, MAX_TIME.getHour());
        this.minute = Math.min(minute, MAX_TIME.getMinute());
        this.second = Math.min(second, MAX_TIME.getSecond());
        analysisDone();
    }

    public TimeLiteral(String s) throws AnalysisException {
        super();
        init(s);
        analysisDone();
    }

    protected TimeLiteral(TimeLiteral other) {
        super(other);
        this.hour = other.getHour();
        this.minute = other.getMinute();
        this.second = other.getSecond();
    }

    @Override
    public Expr clone() {
        return new TimeLiteral(this);
    }

    /** parseTime */
    public static Result<TemporalAccessor, ? extends Exception> parseTime(String s) {
        try {
            TemporalAccessor time;

            time = DateTimeFormatterUtils.TIME_FORMATTER.parse(s);

            return Result.ok(time);
        } catch (DateTimeException e) {
            return Result.err(() ->
                    new DateTimeException("time literal [" + s + "] is invalid", e)
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
        return hour == MIN_TIME.getHour() && minute == MIN_TIME.getMinute() && second == MIN_TIME.getSecond();
    }

    @Override
    public int compareLiteral(LiteralExpr expr) {
        return 0;
    }

    @Override
    public String getStringValue() {
        if (hour > 99) {
            return String.format("%03d:%02d:%02d", hour, minute, second);
        }
        return String.format("%02d:%02d:%02d", hour, minute, second);
    }

    @Override
    public String getStringValueForArray(FormatOptions options) {
        return options.getNestedStringWrapper() + getStringValue() + options.getNestedStringWrapper();
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
}
