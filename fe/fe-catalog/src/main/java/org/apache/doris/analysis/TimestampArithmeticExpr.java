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

import org.apache.doris.analysis.ArithmeticExpr.Operator;
import org.apache.doris.catalog.Function;
import org.apache.doris.catalog.Function.NullableMode;
import org.apache.doris.catalog.FunctionName;
import org.apache.doris.catalog.Type;

import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Describes the addition and subtraction of time units from timestamps.
 * Arithmetic expressions on timestamps are syntactic sugar.
 * They are executed as function call exprs in the BE.
 */
public class TimestampArithmeticExpr extends Expr {
    private static final Map<String, TimeUnit> TIME_UNITS_MAP = new HashMap<String, TimeUnit>();

    static {
        for (TimeUnit timeUnit : TimeUnit.values()) {
            TIME_UNITS_MAP.put(timeUnit.toString(), timeUnit);
        }
    }

    // Set for function call-like arithmetic.
    @SerializedName("funcn")
    private String funcName;
    // Keep the original string passed in the c'tor to resolve
    // ambiguities with other uses of IDENT during query parsing.
    @SerializedName("tui")
    private String timeUnitIdent;
    // Indicates an expr where the interval comes first, e.g., 'interval b year + a'.
    @SerializedName("if")
    private boolean intervalFirst;
    @SerializedName("op")
    private ArithmeticExpr.Operator op;
    @SerializedName("tu")
    private TimeUnit timeUnit;

    private TimestampArithmeticExpr() {
        // use for serde only
    }

    // only used in JDBC function push down
    public TimestampArithmeticExpr(String funcName, Expr e1, Expr e2, String timeUnitIdent) {
        this(funcName, null, e1, e2, timeUnitIdent, Type.DATETIMEV2, false);
    }

    /**
     * used for Nereids ONLY.
     * C'tor for function-call like arithmetic, e.g., 'date_add(a, interval b year)'.
     *
     * @param funcName timestamp arithmetic function name, used for all function except ADD and SUBTRACT.
     * @param e1 non interval literal child of this function
     * @param e2 interval literal child of this function
     * @param timeUnitIdent interval time unit, could be 'year', 'month', 'day', 'hour', 'minute', 'second'.
     * @param dataType the return data type of this expression.
     * @param nullable result is nullable if true
     */
    public TimestampArithmeticExpr(String funcName, ArithmeticExpr.Operator op,
            Expr e1, Expr e2, String timeUnitIdent, Type dataType, boolean nullable) {
        this.funcName = funcName;
        this.timeUnitIdent = timeUnitIdent;
        this.timeUnit = TIME_UNITS_MAP.get(timeUnitIdent.toUpperCase(Locale.ROOT));
        this.op = op;
        this.intervalFirst = false;
        children.add(e1);
        children.add(e2);
        this.type = dataType;
        fn = new Function(new FunctionName(funcName.toLowerCase(Locale.ROOT)),
                Lists.newArrayList(e1.getType(), e2.getType()), dataType,
                false, true, NullableMode.DEPEND_ON_ARGUMENT);
        this.nullable = nullable;
    }

    protected TimestampArithmeticExpr(TimestampArithmeticExpr other) {
        super(other);
        funcName = other.funcName;
        op = other.op;
        timeUnitIdent = other.timeUnitIdent;
        timeUnit = other.timeUnit;
        intervalFirst = other.intervalFirst;
    }

    @Override
    public Expr clone() {
        return new TimestampArithmeticExpr(this);
    }

    public String getFuncName() {
        return funcName;
    }

    public String getTimeUnitIdent() {
        return timeUnitIdent;
    }

    public boolean isIntervalFirst() {
        return intervalFirst;
    }

    public Operator getOp() {
        return op;
    }

    public TimeUnit getTimeUnit() {
        return timeUnit;
    }

    @Override
    public <R, C> R accept(ExprVisitor<R, C> visitor, C context) {
        return visitor.visitTimestampArithmeticExpr(this, context);
    }

    // Time units supported in timestamp arithmetic.
    public enum TimeUnit {
        YEAR("YEAR"),                               // YEARS
        MONTH("MONTH"),                             // MONTHS
        WEEK("WEEK"),                               // WEEKS
        DAY("DAY"),                                 // DAYS
        HOUR("HOUR"),                               // HOURS
        MINUTE("MINUTE"),                           // MINUTES
        SECOND("SECOND"),                           // SECONDS
        MICROSECOND("MICROSECOND"),                 // MICROSECONDS
        SECOND_MICROSECOND("SECOND_MICROSECOND"),   // 'SECONDS.MICROSECONDS'
        MINUTE_MICROSECOND("MINUTE_MICROSECOND"),   // 'MINUTES:SECONDS.MICROSECONDS'
        MINUTE_SECOND("MINUTE_SECOND"),             // 'MINUTES:SECONDS'
        HOUR_MICROSECOND("HOUR_MICROSECOND"),       // 'HOURS:MINUTES:SECONDS.MICROSECONDS'
        HOUR_SECOND("HOUR_SECOND"),                 // 'HOURS:MINUTES:SECONDS'
        HOUR_MINUTE("HOUR_MINUTE"),                 // 'HOURS:MINUTES'
        DAY_MICROSECOND("DAY_MICROSECOND"),         // 'DAYS HOURS:MINUTES:SECONDS.MICROSECONDS'
        DAY_SECOND("DAY_SECOND"),                   // 'DAYS HOURS:MINUTES:SECONDS'
        DAY_MINUTE("DAY_MINUTE"),                   // 'DAYS HOURS:MINUTES'
        DAY_HOUR("DAY_HOUR"),                       // 'DAYS HOURS'
        YEAR_MONTH("YEAR_MONTH");                   // 'YEARS-MONTHS'

        private final String description;

        TimeUnit(String description) {
            this.description = description;
        }

        @Override
        public String toString() {
            return description;
        }
    }
}
