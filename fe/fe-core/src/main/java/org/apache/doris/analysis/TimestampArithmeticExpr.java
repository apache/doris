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
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.thrift.TExprNode;
import org.apache.doris.thrift.TExprNodeType;
import org.apache.doris.thrift.TExprOpcode;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Describes the addition and subtraction of time units from timestamps.
 * Arithmetic expressions on timestamps are syntactic sugar.
 * They are executed as function call exprs in the BE.
 */
public class TimestampArithmeticExpr extends Expr {
    private static final Logger LOG = LogManager.getLogger(TimestampArithmeticExpr.class);
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

    // C'tor for function-call like arithmetic, e.g., 'date_add(a, interval b year)'.
    public TimestampArithmeticExpr(String funcName, Expr e1, Expr e2, String timeUnitIdent) {
        this.funcName = funcName;
        this.timeUnitIdent = timeUnitIdent;
        this.intervalFirst = false;
        children.add(e1);
        children.add(e2);
    }

    // C'tor for non-function-call like arithmetic, e.g., 'a + interval b year'.
    // e1 always refers to the timestamp to be added/subtracted from, and e2
    // to the time value (even in the interval-first case).
    public TimestampArithmeticExpr(ArithmeticExpr.Operator op, Expr e1, Expr e2,
                                   String timeUnitIdent, boolean intervalFirst) {
        Preconditions.checkState(op == Operator.ADD || op == Operator.SUBTRACT);
        this.funcName = null;
        this.op = op;
        this.timeUnitIdent = timeUnitIdent;
        this.intervalFirst = intervalFirst;
        children.add(e1);
        children.add(e2);
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
     */
    public TimestampArithmeticExpr(String funcName, ArithmeticExpr.Operator op,
            Expr e1, Expr e2, String timeUnitIdent, Type dataType, NullableMode nullableMode) {
        this.funcName = funcName;
        this.timeUnitIdent = timeUnitIdent;
        this.timeUnit = TIME_UNITS_MAP.get(timeUnitIdent.toUpperCase(Locale.ROOT));
        this.op = op;
        this.intervalFirst = false;
        children.add(e1);
        children.add(e2);
        this.type = dataType;
        fn = new Function(new FunctionName(funcName.toLowerCase(Locale.ROOT)),
                Lists.newArrayList(e1.getType(), e2.getType()), dataType, false, true, nullableMode);
        try {
            opcode = getOpCode();
        } catch (AnalysisException e) {
            throw new RuntimeException(e);
        }

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

    @Override
    protected void toThrift(TExprNode msg) {
        msg.node_type = TExprNodeType.COMPUTE_FUNCTION_CALL;
        msg.setOpcode(opcode);
    }

    private TExprOpcode getOpCode() throws AnalysisException {
        // Select appropriate opcode based on op and timeUnit.
        switch (timeUnit) {
            case YEAR: {
                if (op == Operator.ADD) {
                    return TExprOpcode.TIMESTAMP_YEARS_ADD;
                } else {
                    return TExprOpcode.TIMESTAMP_YEARS_SUB;
                }
            }
            case MONTH: {
                if (op == Operator.ADD) {
                    return TExprOpcode.TIMESTAMP_MONTHS_ADD;
                } else {
                    return TExprOpcode.TIMESTAMP_MONTHS_SUB;
                }
            }
            case WEEK: {
                if (op == Operator.ADD) {
                    return TExprOpcode.TIMESTAMP_WEEKS_ADD;
                } else {
                    return TExprOpcode.TIMESTAMP_WEEKS_SUB;
                }
            }
            case DAY: {
                if (op == Operator.ADD) {
                    return TExprOpcode.TIMESTAMP_DAYS_ADD;
                } else {
                    return TExprOpcode.TIMESTAMP_DAYS_SUB;
                }
            }
            case HOUR: {
                if (op == Operator.ADD) {
                    return TExprOpcode.TIMESTAMP_HOURS_ADD;
                } else {
                    return TExprOpcode.TIMESTAMP_HOURS_SUB;
                }
            }
            case MINUTE: {
                if (op == Operator.ADD) {
                    return TExprOpcode.TIMESTAMP_MINUTES_ADD;
                } else {
                    return TExprOpcode.TIMESTAMP_MINUTES_SUB;
                }
            }
            case SECOND: {
                if (op == Operator.ADD) {
                    return TExprOpcode.TIMESTAMP_SECONDS_ADD;
                } else {
                    return TExprOpcode.TIMESTAMP_SECONDS_SUB;
                }
            }
            default: {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_BAD_TIMEUNIT, timeUnit);
            }
        }
        return null;
    }

    @Override
    public String toSqlImpl() {
        StringBuilder strBuilder = new StringBuilder();
        if (funcName != null) {
            if (funcName.equalsIgnoreCase("TIMESTAMPDIFF") || funcName.equalsIgnoreCase("TIMESTAMPADD")) {
                strBuilder.append(funcName).append("(");
                strBuilder.append(timeUnitIdent).append(", ");
                strBuilder.append(getChild(1).toSql()).append(", ");
                strBuilder.append(getChild(0).toSql()).append(")");
                return strBuilder.toString();
            }
            // Function-call like version.
            strBuilder.append(funcName).append("(");
            strBuilder.append(getChild(0).toSql()).append(", ");
            strBuilder.append("INTERVAL ");
            strBuilder.append(getChild(1).toSql());
            strBuilder.append(" ").append(timeUnitIdent);
            strBuilder.append(")");
            return strBuilder.toString();
        }
        if (intervalFirst) {
            // Non-function-call like version with interval as first operand.
            strBuilder.append("INTERVAL ");
            strBuilder.append(getChild(1).toSql() + " ");
            strBuilder.append(timeUnitIdent);
            strBuilder.append(" ").append(op.toString()).append(" ");
            strBuilder.append(getChild(0).toSql());
        } else {
            // Non-function-call like version with interval as second operand.
            strBuilder.append(getChild(0).toSql());
            strBuilder.append(" " + op.toString() + " ");
            strBuilder.append("INTERVAL ");
            strBuilder.append(getChild(1).toSql() + " ");
            strBuilder.append(timeUnitIdent);
        }
        return strBuilder.toString();
    }

    @Override
    public String toSqlImpl(boolean disableTableName, boolean needExternalSql, TableType tableType,
            TableIf table) {
        StringBuilder strBuilder = new StringBuilder();
        if (funcName != null) {
            if (funcName.equalsIgnoreCase("TIMESTAMPDIFF") || funcName.equalsIgnoreCase("TIMESTAMPADD")) {
                strBuilder.append(funcName).append("(");
                strBuilder.append(timeUnitIdent).append(", ");
                strBuilder.append(getChild(1).toSql(disableTableName, needExternalSql, tableType, table)).append(", ");
                strBuilder.append(getChild(0).toSql(disableTableName, needExternalSql, tableType, table)).append(")");
                return strBuilder.toString();
            }
            // Function-call like version.
            strBuilder.append(funcName).append("(");
            strBuilder.append(getChild(0).toSql(disableTableName, needExternalSql, tableType, table)).append(", ");
            strBuilder.append("INTERVAL ");
            strBuilder.append(getChild(1).toSql(disableTableName, needExternalSql, tableType, table));
            strBuilder.append(" ").append(timeUnitIdent);
            strBuilder.append(")");
            return strBuilder.toString();
        }
        if (intervalFirst) {
            // Non-function-call like version with interval as first operand.
            strBuilder.append("INTERVAL ");
            strBuilder.append(getChild(1).toSql(disableTableName, needExternalSql, tableType, table) + " ");
            strBuilder.append(timeUnitIdent);
            strBuilder.append(" ").append(op.toString()).append(" ");
            strBuilder.append(getChild(0).toSql(disableTableName, needExternalSql, tableType, table));
        } else {
            // Non-function-call like version with interval as second operand.
            strBuilder.append(getChild(0).toSql(disableTableName, needExternalSql, tableType, table));
            strBuilder.append(" " + op.toString() + " ");
            strBuilder.append("INTERVAL ");
            strBuilder.append(getChild(1).toSql(disableTableName, needExternalSql, tableType, table) + " ");
            strBuilder.append(timeUnitIdent);
        }
        return strBuilder.toString();
    }

    @Override
    public String toDigestImpl() {
        StringBuilder strBuilder = new StringBuilder();
        if (funcName != null) {
            if (funcName.equalsIgnoreCase("TIMESTAMPDIFF") || funcName.equalsIgnoreCase("TIMESTAMPADD")) {
                strBuilder.append(funcName).append("(");
                strBuilder.append(timeUnitIdent).append(", ");
                strBuilder.append(getChild(1).toDigest()).append(", ");
                strBuilder.append(getChild(0).toDigest()).append(")");
                return strBuilder.toString();
            }
            // Function-call like version.
            strBuilder.append(funcName).append("(");
            strBuilder.append(getChild(0).toDigest()).append(", ");
            strBuilder.append("INTERVAL ");
            strBuilder.append(getChild(1).toDigest());
            strBuilder.append(" ").append(timeUnitIdent);
            strBuilder.append(")");
            return strBuilder.toString();
        }
        if (intervalFirst) {
            // Non-function-call like version with interval as first operand.
            strBuilder.append("INTERVAL ");
            strBuilder.append(getChild(1).toDigest() + " ");
            strBuilder.append(timeUnitIdent);
            strBuilder.append(" ").append(op.toString()).append(" ");
            strBuilder.append(getChild(0).toDigest());
        } else {
            // Non-function-call like version with interval as second operand.
            strBuilder.append(getChild(0).toDigest());
            strBuilder.append(" " + op.toString() + " ");
            strBuilder.append("INTERVAL ");
            strBuilder.append(getChild(1).toDigest() + " ");
            strBuilder.append(timeUnitIdent);
        }
        return strBuilder.toString();
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

        public boolean isDateTime() {
            if (this == HOUR || this == MINUTE || this == SECOND || this == MICROSECOND) {
                return true;
            }
            return false;
        }

        @Override
        public String toString() {
            return description;
        }
    }
}
