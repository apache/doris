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
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
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

    private Type fixType() {
        PrimitiveType t1 = getChild(0).getType().getPrimitiveType();
        if (t1 == PrimitiveType.DATETIME) {
            return Type.DATETIME;
        }
        if (t1 == PrimitiveType.DATE) {
            return Type.DATE;
        }
        if (t1 == PrimitiveType.DATETIMEV2) {
            return Type.DATETIMEV2;
        }
        if (t1 == PrimitiveType.DATEV2) {
            return Type.DATEV2;
        }
        // could try cast to date first, then cast to datetime
        if (t1 == PrimitiveType.VARCHAR || t1 == PrimitiveType.STRING) {
            Expr expr = getChild(0);
            if ((expr instanceof StringLiteral) && ((StringLiteral) expr).canConvertToDateType(Type.DATEV2)) {
                try {
                    setChild(0, new DateLiteral(((StringLiteral) expr).getValue(), Type.DATEV2));
                } catch (AnalysisException e) {
                    return Type.INVALID;
                }
                return Type.DATEV2;
            }
        }
        if (PrimitiveType.isImplicitCast(t1, PrimitiveType.DATETIME)) {
            if (Config.enable_date_conversion) {
                if (t1 == PrimitiveType.NULL_TYPE) {
                    getChild(0).type = Type.DATETIMEV2_WITH_MAX_SCALAR;
                }
                return Type.DATETIMEV2_WITH_MAX_SCALAR;
            }
            if (t1 == PrimitiveType.NULL_TYPE) {
                getChild(0).type = Type.DATETIME;
            }
            return Type.DATETIME;
        }
        return Type.INVALID;
    }

    @Override
    public void analyzeImpl(Analyzer analyzer) throws AnalysisException {
        // Check if name of function call is date_sub or date_add.
        String funcOpName;
        if (funcName != null && funcName.equalsIgnoreCase("TIMESTAMPDIFF")) {
            timeUnit = TIME_UNITS_MAP.get(timeUnitIdent.toUpperCase());
            if (timeUnit == null) {
                throw new AnalysisException("Invalid time unit '" + timeUnitIdent
                        + "' in timestamp arithmetic expression '" + toSql() + "'.");
            }
            Type dateType = fixType();
            if (dateType.isDate() && timeUnit.isDateTime()) {
                dateType = ScalarType.getDefaultDateType(Type.DATETIME);
            }
            // The first child must return a timestamp or null.
            if (!getChild(0).getType().isDateType() && !getChild(0).getType().isNull()) {
                if (!dateType.isValid()) {
                    throw new AnalysisException("Operand '" + getChild(0).toSql()
                            + "' of timestamp arithmetic expression '" + toSql() + "' returns type '"
                            + getChild(0).getType() + "'. Expected type 'TIMESTAMP/DATE/DATETIME'.");
                }
                castChild(dateType, 0);
            }

            // The first child must return a timestamp or null.
            if (!getChild(1).getType().isDateType() && !getChild(1).getType().isNull()) {
                if (!dateType.isValid()) {
                    throw new AnalysisException("Operand '" + getChild(1).toSql()
                            + "' of timestamp arithmetic expression '" + toSql() + "' returns type '"
                            + getChild(1).getType() + "'. Expected type 'TIMESTAMP/DATE/DATETIME'.");
                }
                castChild(dateType, 1);
            }

            type = Type.BIGINT;
            opcode = getOpCode();
            funcOpName = String.format("%sS_%s", timeUnit, "DIFF");
        } else {
            if (funcName != null) {
                if (funcName.toUpperCase().equals("DATE_ADD")
                        || funcName.toUpperCase().equals("DAYS_ADD")
                        || funcName.toUpperCase().equals("ADDDATE")
                        || funcName.toUpperCase().equals("TIMESTAMPADD")) {
                    op = ArithmeticExpr.Operator.ADD;
                } else if (funcName.toUpperCase().equals("DATE_SUB")
                        || funcName.toUpperCase().equals("DAYS_SUB")
                        || funcName.toUpperCase().equals("SUBDATE")) {
                    op = ArithmeticExpr.Operator.SUBTRACT;
                } else {
                    throw new AnalysisException("Encountered function name '" + funcName
                            + "' in timestamp arithmetic expression '" + toSql() + "'. "
                            + "Expected function name 'DATE_ADD/DAYS_ADD/ADDDATE/TIMESTAMPADD'"
                            + "or 'DATE_SUB/DAYS_SUB/SUBDATE");
                }
            }

            timeUnit = TIME_UNITS_MAP.get(timeUnitIdent.toUpperCase());
            if (timeUnit == null) {
                throw new AnalysisException("Invalid time unit '" + timeUnitIdent
                        + "' in timestamp arithmetic expression '" + toSql() + "'.");
            }

            Type dateType = fixType();
            if (dateType.isDate() && timeUnit.isDateTime()) {
                dateType = Type.DATETIME;
            }
            if (dateType.isDateV2() && timeUnit.isDateTime()) {
                dateType = Type.DATETIMEV2;
            }
            // The first child must return a timestamp or null.
            if (!getChild(0).getType().isDateType() && !getChild(0).getType().isNull()) {
                if (!dateType.isValid()) {
                    throw new AnalysisException("Operand '" + getChild(0).toSql()
                            + "' of timestamp arithmetic expression '" + toSql() + "' returns type '"
                            + getChild(0).getType() + "'. Expected type 'TIMESTAMP/DATE/DATETIME'.");
                }
                castChild(dateType, 0);
            }

            if (!getChild(1).getType().isScalarType()) {
                throw new AnalysisException(
                        "the second argument must be a scalar type. but it is " + getChild(1).toSql());
            }

            // The second child must be of type 'INT' or castable to it.
            if (!getChild(1).getType().isScalarType(PrimitiveType.INT)) {
                if (!ScalarType.canCastTo((ScalarType) getChild(1).getType(), Type.INT)) {
                    throw new AnalysisException("Operand '" + getChild(1).toSql()
                            + "' of timestamp arithmetic expression '" + toSql() + "' returns type '"
                            + getChild(1).getType() + "' which is incompatible with expected type 'INT'.");
                }
                castChild(Type.INT, 1);
            }

            type = dateType;
            opcode = getOpCode();
            funcOpName = String.format("%sS_%s", timeUnit,
                    (op == ArithmeticExpr.Operator.ADD) ? "ADD" : "SUB");
        }

        Type[] childrenTypes = collectChildReturnTypes();
        fn = getBuiltinFunction(funcOpName.toLowerCase(), childrenTypes,
                Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
        Preconditions.checkArgument(fn != null);
        Type[] argTypes = fn.getArgs();
        if (argTypes.length > 0) {
            // Implicitly cast all the children to match the function if necessary
            for (int i = 0; i < childrenTypes.length; ++i) {
                // For varargs, we must compare with the last type in callArgs.argTypes.
                int ix = Math.min(argTypes.length - 1, i);
                if (!childrenTypes[i].matchesType(argTypes[ix]) && !(
                        childrenTypes[i].isDateOrDateTime() && argTypes[ix].isDateOrDateTime())) {
                    uncheckedCastChild(argTypes[ix], i);
                }
            }
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("fn is {} name is {}", fn, funcOpName);
        }
    }

    @Override
    protected void toThrift(TExprNode msg) {
        msg.node_type = TExprNodeType.COMPUTE_FUNCTION_CALL;
        msg.setOpcode(opcode);
    }

    public ArithmeticExpr.Operator getOp() {
        return op;
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
