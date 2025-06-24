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
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.catalog.Type;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.thrift.TExprNode;
import org.apache.doris.thrift.TExprNodeType;
import org.apache.doris.thrift.TTimeV2Literal;

public class TimeV2Literal extends LiteralExpr {
    public static final TimeV2Literal MIN_VALUE = new TimeV2Literal(838, 59, 59, 999999, 6, true);
    public static final TimeV2Literal MAX_VALUE = new TimeV2Literal(838, 59, 59, 999999, 6, false);

    protected int hour;
    protected int minute;
    protected int second;
    protected int microsecond;
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

    // for -00:... so we need explicite negative
    public TimeV2Literal(int hour, int minute, int second, int microsecond, int scale, boolean negative)
            throws AnalysisException {
        super();
        this.type = ScalarType.createTimeV2Type(scale);
        this.hour = hour;
        this.minute = minute;
        this.second = second;
        this.microsecond = microsecond / (int) Math.pow(10, 6 - scale) * (int) Math.pow(10, 6 - scale);
        this.negative = negative;
        if (checkRange(this.hour, this.minute, this.second, this.microsecond) || scale > 6 || scale < 0) {
            throw new AnalysisException("time literal is out of range [-838:59:59.999999, 838:59:59.999999]");
        }
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

    @Override
    protected String toSqlImpl() {
        return "\"" + getStringValue() + "\"";
    }

    @Override
    protected String toSqlImpl(boolean disableTableName, boolean needExternalSql, TableType tableType, TableIf table) {
        return "\"" + getStringValue() + "\"";
    }

    @Override
    protected void toThrift(TExprNode msg) {
        msg.node_type = TExprNodeType.TIMEV2_LITERAL;
        msg.timev2_literal = new TTimeV2Literal(getValue());
    }

    @Override
    public boolean isMinValue() {
        return getValue() == MIN_VALUE.getValue();
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
            sb.append(String.format(".%0" + scale + "d", microsecond / (int) Math.pow(10, 6 - scale)));
        }
        return sb.toString();
    }

    protected static boolean checkRange(int hour, int minute, int second, int microsecond) {
        return hour > 838 || minute > 59 || second > 59 || microsecond > 999999 || minute < 0 || second < 0
                || microsecond < 0;
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
