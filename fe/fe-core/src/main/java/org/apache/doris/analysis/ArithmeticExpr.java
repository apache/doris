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
// This file is copied from
// https://github.com/apache/impala/blob/branch-2.9.0/fe/src/main/java/org/apache/impala/ArithmeticExpr.java
// and modified by Doris

package org.apache.doris.analysis;

import org.apache.doris.catalog.Function;
import org.apache.doris.catalog.FunctionSet;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarFunction;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.util.VectorizedUtil;
import org.apache.doris.thrift.TExprNode;
import org.apache.doris.thrift.TExprNodeType;
import org.apache.doris.thrift.TExprOpcode;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class ArithmeticExpr extends Expr {
    private static final Logger LOG = LogManager.getLogger(ArithmeticExpr.class);

    enum OperatorPosition {
        BINARY_INFIX,
        UNARY_PREFIX,
        UNARY_POSTFIX,
    }

    public enum Operator {
        MULTIPLY("*", "multiply", OperatorPosition.BINARY_INFIX, TExprOpcode.MULTIPLY),
        DIVIDE("/", "divide", OperatorPosition.BINARY_INFIX, TExprOpcode.DIVIDE),
        MOD("%", "mod", OperatorPosition.BINARY_INFIX, TExprOpcode.MOD),
        INT_DIVIDE("DIV", "int_divide", OperatorPosition.BINARY_INFIX, TExprOpcode.INT_DIVIDE),
        ADD("+", "add", OperatorPosition.BINARY_INFIX, TExprOpcode.ADD),
        SUBTRACT("-", "subtract", OperatorPosition.BINARY_INFIX, TExprOpcode.SUBTRACT),
        BITAND("&", "bitand", OperatorPosition.BINARY_INFIX, TExprOpcode.BITAND),
        BITOR("|", "bitor", OperatorPosition.BINARY_INFIX, TExprOpcode.BITOR),
        BITXOR("^", "bitxor", OperatorPosition.BINARY_INFIX, TExprOpcode.BITXOR),
        BITNOT("~", "bitnot", OperatorPosition.UNARY_PREFIX, TExprOpcode.BITNOT),
        FACTORIAL("!", "factorial", OperatorPosition.UNARY_POSTFIX, TExprOpcode.FACTORIAL);

        private final String description;
        private final String name;
        private final OperatorPosition pos;
        private final TExprOpcode opcode;

        Operator(String description, String name, OperatorPosition pos, TExprOpcode opcode) {
            this.description = description;
            this.name = name;
            this.pos = pos;
            this.opcode = opcode;
        }

        @Override
        public String toString() {
            return description;
        }

        public String getName() {
            return name;
        }

        public OperatorPosition getPos() {
            return pos;
        }

        public TExprOpcode getOpcode() {
            return opcode;
        }

        public boolean isUnary() {
            return pos == OperatorPosition.UNARY_PREFIX
                    || pos == OperatorPosition.UNARY_POSTFIX;
        }

        public boolean isBinary() {
            return pos == OperatorPosition.BINARY_INFIX;
        }
    }

    public static void initBuiltins(FunctionSet functionSet) {
        for (Type t : Type.getNumericTypes()) {
            functionSet.addBuiltin(ScalarFunction.createBuiltinOperator(
                    Operator.MULTIPLY.getName(), Lists.newArrayList(t, t), t));
            functionSet.addBuiltin(ScalarFunction.createBuiltinOperator(
                    Operator.ADD.getName(), Lists.newArrayList(t, t), t));
            functionSet.addBuiltin(ScalarFunction.createBuiltinOperator(
                    Operator.SUBTRACT.getName(), Lists.newArrayList(t, t), t));
        }
        functionSet.addBuiltin(ScalarFunction.createBuiltinOperator(
                Operator.DIVIDE.getName(),
                Lists.<Type>newArrayList(Type.DOUBLE, Type.DOUBLE),
                Type.DOUBLE, Function.NullableMode.ALWAYS_NULLABLE));
        functionSet.addBuiltin(ScalarFunction.createBuiltinOperator(
                Operator.DIVIDE.getName(),
                Lists.<Type>newArrayList(Type.MAX_DECIMALV2_TYPE, Type.MAX_DECIMALV2_TYPE),
                Type.MAX_DECIMALV2_TYPE, Function.NullableMode.ALWAYS_NULLABLE));
        functionSet.addBuiltin(ScalarFunction.createBuiltinOperator(
                Operator.DIVIDE.getName(),
                Lists.<Type>newArrayList(Type.DECIMAL32, Type.DECIMAL32),
                Type.DECIMAL32, Function.NullableMode.ALWAYS_NULLABLE));
        functionSet.addBuiltin(ScalarFunction.createBuiltinOperator(
                Operator.DIVIDE.getName(),
                Lists.<Type>newArrayList(Type.DECIMAL64, Type.DECIMAL64),
                Type.DECIMAL64, Function.NullableMode.ALWAYS_NULLABLE));
        functionSet.addBuiltin(ScalarFunction.createBuiltinOperator(
                Operator.DIVIDE.getName(),
                Lists.<Type>newArrayList(Type.DECIMAL128, Type.DECIMAL128),
                Type.DECIMAL128, Function.NullableMode.ALWAYS_NULLABLE));

        // MOD(), FACTORIAL(), BITAND(), BITOR(), BITXOR(), and BITNOT() are registered as
        // builtins, see palo_functions.py
        for (Type t : Type.getIntegerTypes()) {
            functionSet.addBuiltin(ScalarFunction.createBuiltinOperator(
                    Operator.INT_DIVIDE.getName(), Lists.newArrayList(t, t),
                    t, Function.NullableMode.ALWAYS_NULLABLE));
        }

        // init vec build function
        for (int i = 0; i < Type.getNumericTypes().size(); i++) {
            Type t1 = Type.getNumericTypes().get(i);
            for (int j = 0; j < Type.getNumericTypes().size(); j++) {
                Type t2 = Type.getNumericTypes().get(j);

                functionSet.addBuiltin(ScalarFunction.createVecBuiltinOperator(
                        Operator.MULTIPLY.getName(), Lists.newArrayList(t1, t2),
                        Type.getNextNumType(Type.getAssignmentCompatibleType(t1, t2, false))));
                functionSet.addBuiltin(ScalarFunction.createVecBuiltinOperator(
                        Operator.ADD.getName(), Lists.newArrayList(t1, t2),
                        Type.getNextNumType(Type.getAssignmentCompatibleType(t1, t2, false))));
                functionSet.addBuiltin(ScalarFunction.createVecBuiltinOperator(
                        Operator.SUBTRACT.getName(), Lists.newArrayList(t1, t2),
                        Type.getNextNumType(Type.getAssignmentCompatibleType(t1, t2, false))));
            }
        }

        functionSet.addBuiltin(ScalarFunction.createVecBuiltinOperator(
                Operator.DIVIDE.getName(),
                Lists.<Type>newArrayList(Type.DOUBLE, Type.DOUBLE),
                Type.DOUBLE, Function.NullableMode.ALWAYS_NULLABLE));
        functionSet.addBuiltin(ScalarFunction.createVecBuiltinOperator(
                Operator.DIVIDE.getName(),
                Lists.<Type>newArrayList(Type.MAX_DECIMALV2_TYPE, Type.MAX_DECIMALV2_TYPE),
                Type.MAX_DECIMALV2_TYPE, Function.NullableMode.ALWAYS_NULLABLE));
        functionSet.addBuiltin(ScalarFunction.createVecBuiltinOperator(
                Operator.DIVIDE.getName(),
                Lists.<Type>newArrayList(Type.DECIMAL32, Type.DECIMAL32),
                Type.DECIMAL32, Function.NullableMode.ALWAYS_NULLABLE));
        functionSet.addBuiltin(ScalarFunction.createVecBuiltinOperator(
                Operator.DIVIDE.getName(),
                Lists.<Type>newArrayList(Type.DECIMAL64, Type.DECIMAL64),
                Type.DECIMAL64, Function.NullableMode.ALWAYS_NULLABLE));
        functionSet.addBuiltin(ScalarFunction.createVecBuiltinOperator(
                Operator.DIVIDE.getName(),
                Lists.<Type>newArrayList(Type.DECIMAL128, Type.DECIMAL128),
                Type.DECIMAL128, Function.NullableMode.ALWAYS_NULLABLE));

        functionSet.addBuiltin(ScalarFunction.createVecBuiltinOperator(
                Operator.MOD.getName(),
                Lists.<Type>newArrayList(Type.FLOAT, Type.FLOAT),
                Type.FLOAT, Function.NullableMode.ALWAYS_NULLABLE));
        functionSet.addBuiltin(ScalarFunction.createVecBuiltinOperator(
                Operator.MOD.getName(),
                Lists.<Type>newArrayList(Type.DOUBLE, Type.DOUBLE),
                Type.DOUBLE, Function.NullableMode.ALWAYS_NULLABLE));
        functionSet.addBuiltin(ScalarFunction.createVecBuiltinOperator(
                Operator.MOD.getName(),
                Lists.<Type>newArrayList(Type.MAX_DECIMALV2_TYPE, Type.MAX_DECIMALV2_TYPE),
                Type.MAX_DECIMALV2_TYPE, Function.NullableMode.ALWAYS_NULLABLE));
        functionSet.addBuiltin(ScalarFunction.createVecBuiltinOperator(
                Operator.MOD.getName(),
                Lists.<Type>newArrayList(Type.DECIMAL32, Type.DECIMAL32),
                Type.DECIMAL32, Function.NullableMode.ALWAYS_NULLABLE));
        functionSet.addBuiltin(ScalarFunction.createVecBuiltinOperator(
                Operator.MOD.getName(),
                Lists.<Type>newArrayList(Type.DECIMAL64, Type.DECIMAL64),
                Type.DECIMAL64, Function.NullableMode.ALWAYS_NULLABLE));
        functionSet.addBuiltin(ScalarFunction.createVecBuiltinOperator(
                Operator.MOD.getName(),
                Lists.<Type>newArrayList(Type.DECIMAL128, Type.DECIMAL128),
                Type.DECIMAL128, Function.NullableMode.ALWAYS_NULLABLE));

        for (int i = 0; i < Type.getIntegerTypes().size(); i++) {
            Type t1 = Type.getIntegerTypes().get(i);
            for (int j = 0; j < Type.getIntegerTypes().size(); j++) {
                Type t2 = Type.getIntegerTypes().get(j);

                functionSet.addBuiltin(ScalarFunction.createVecBuiltinOperator(
                        Operator.INT_DIVIDE.getName(), Lists.newArrayList(t1, t2),
                        Type.getAssignmentCompatibleType(t1, t2, false),
                        Function.NullableMode.ALWAYS_NULLABLE));
                functionSet.addBuiltin(ScalarFunction.createVecBuiltinOperator(
                        Operator.MOD.getName(), Lists.newArrayList(t1, t2),
                        Type.getAssignmentCompatibleType(t1, t2, false),
                        Function.NullableMode.ALWAYS_NULLABLE));
            }
        }
    }

    private final Operator op;

    public ArithmeticExpr(Operator op, Expr e1, Expr e2) {
        super();
        this.op = op;
        Preconditions.checkNotNull(e1);
        children.add(e1);
        Preconditions.checkArgument(
                op == Operator.BITNOT && e2 == null || op != Operator.BITNOT && e2 != null);
        if (e2 != null) {
            children.add(e2);
        }
    }

    /**
     * Copy c'tor used in clone().
     */
    protected ArithmeticExpr(ArithmeticExpr other) {
        super(other);
        this.op = other.op;
    }

    @Override
    public String toString() {
        return toSql();
    }

    @Override
    public Expr clone() {
        return new ArithmeticExpr(this);
    }

    @Override
    public String toSqlImpl() {
        if (children.size() == 1) {
            return op.toString() + " " + getChild(0).toSql();
        } else {
            return getChild(0).toSql() + " " + op.toString() + " " + getChild(1).toSql();
        }
    }

    @Override
    public String toDigestImpl() {
        if (children.size() == 1) {
            return op.toString() + " " + getChild(0).toDigest();
        } else {
            return getChild(0).toDigest() + " " + op.toString() + " " + getChild(1).toDigest();
        }
    }

    @Override
    protected void toThrift(TExprNode msg) {
        msg.node_type = TExprNodeType.ARITHMETIC_EXPR;
        if (!(type.isDecimalV2() || type.isDecimalV3())) {
            msg.setOpcode(op.getOpcode());
            msg.setOutputColumn(outputColumn);
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (!super.equals(obj)) {
            return false;
        }
        return ((ArithmeticExpr) obj).opcode == opcode;
    }

    @Override
    public void computeOutputColumn(Analyzer analyzer) {
        super.computeOutputColumn(analyzer);

        List<TupleId> tupleIds = Lists.newArrayList();
        getIds(tupleIds, null);
        Preconditions.checkArgument(tupleIds.size() == 1);

        // for (Expr child : children) {
        //     if (child.getOutputColumn() > analyzer.getTupleDesc(tupleIds.get(0)).getSlots().size()) {
        //     }
        // }
    }

    private Type findCommonType(Type t1, Type t2) {
        PrimitiveType pt1 = t1.getPrimitiveType();
        PrimitiveType pt2 = t2.getPrimitiveType();

        if (pt1 == PrimitiveType.DOUBLE || pt2 == PrimitiveType.DOUBLE) {
            return Type.DOUBLE;
        } else if (pt1 == PrimitiveType.DECIMALV2 || pt2 == PrimitiveType.DECIMALV2) {
            return Type.MAX_DECIMALV2_TYPE;
        } else if (pt1 == PrimitiveType.DECIMAL32 || pt2 == PrimitiveType.DECIMAL32) {
            return Type.DECIMAL32;
        } else if (pt1 == PrimitiveType.DECIMAL64 || pt2 == PrimitiveType.DECIMAL64) {
            return Type.DECIMAL64;
        } else if (pt1 == PrimitiveType.DECIMAL128 || pt2 == PrimitiveType.DECIMAL128) {
            return Type.DECIMAL128;
        } else if (pt1 == PrimitiveType.LARGEINT || pt2 == PrimitiveType.LARGEINT) {
            return Type.LARGEINT;
        } else {
            if (pt1 != PrimitiveType.BIGINT && pt2 != PrimitiveType.BIGINT) {
                return Type.INVALID;
            }
            return Type.BIGINT;
        }
    }

    private boolean castIfHaveSameType(Type t1, Type t2, Type target) throws AnalysisException {
        if (t1 == target || t2 == target) {
            castChild(target, 0);
            castChild(target, 1);
            return true;
        }
        return false;
    }

    private void castUpperInteger(Type t1, Type t2) throws AnalysisException {
        if (!t1.isIntegerType() || !t2.isIntegerType()) {
            return;
        }
        if (castIfHaveSameType(t1, t2, Type.BIGINT)) {
            return;
        }
        if (castIfHaveSameType(t1, t2, Type.INT)) {
            return;
        }
        if (castIfHaveSameType(t1, t2, Type.SMALLINT)) {
            return;
        }
        if (castIfHaveSameType(t1, t2, Type.TINYINT)) {
            return;
        }
    }

    private void analyzeNoneDecimalOp(Type t1, Type t2) throws AnalysisException {
        Type commonType;
        switch (op) {
            case MULTIPLY:
            case ADD:
            case SUBTRACT:
                if (t1.isDecimalV2() || t2.isDecimalV2()) {
                    castBinaryOp(findCommonType(t1, t2));
                }
                if (isConstant()) {
                    castUpperInteger(t1, t2);
                }
                break;
            case MOD:
                if (t1.isDecimalV2() || t2.isDecimalV2()) {
                    castBinaryOp(findCommonType(t1, t2));
                } else if ((t1.isFloatingPointType() || t2.isFloatingPointType()) && !t1.equals(t2)) {
                    castBinaryOp(Type.DOUBLE);
                }
                break;
            case INT_DIVIDE:
                if (!t1.isFixedPointType() || !t2.isFloatingPointType()) {
                    castBinaryOp(Type.BIGINT);
                }
                break;
            case DIVIDE:
                t1 = getChild(0).getType().getNumResultType();
                t2 = getChild(1).getType().getNumResultType();
                commonType = findCommonType(t1, t2);
                if (commonType.getPrimitiveType() == PrimitiveType.BIGINT
                        || commonType.getPrimitiveType() == PrimitiveType.LARGEINT) {
                    commonType = Type.DOUBLE;
                }
                castBinaryOp(commonType);
                break;
            case BITAND:
            case BITOR:
            case BITXOR:
                if (t1 == Type.BOOLEAN && t2 == Type.BOOLEAN) {
                    t1 = Type.TINYINT;
                    t2 = Type.TINYINT;
                }
                commonType = Type.getAssignmentCompatibleType(t1, t2, false);
                if (commonType.getPrimitiveType().ordinal() > PrimitiveType.LARGEINT.ordinal()) {
                    commonType = Type.BIGINT;
                }
                type = castBinaryOp(commonType);
                break;
            default:
                Preconditions.checkState(false,
                        "Unknown arithmetic operation " + op.toString() + " in: " + this.toSql());
                break;
        }
    }

    /**
     * Convert integer type to decimal type.
     */
    public static Type convertIntToDecimalV3Type(Type type) throws AnalysisException {
        if (type.isLargeIntType()) {
            return ScalarType.createDecimalType(ScalarType.MAX_DECIMAL128_PRECISION, 0);
        } else if (type.isBigIntType()) {
            return ScalarType.createDecimalType(ScalarType.MAX_DECIMAL64_PRECISION, 0);
        } else if (type.isInteger32Type()) {
            return ScalarType.createDecimalType(ScalarType.MAX_DECIMAL32_PRECISION, 0);
        } else {
            Preconditions.checkState(false,
                    "Implicit converting to decimal for arithmetic operations only support integer");
            return Type.INVALID;
        }
    }

    private void analyzeDecimalV3Op(Type t1, Type t2) throws AnalysisException {
        Type t1TargetType = t1;
        Type t2TargetType = t2;
        switch (op) {
            case MULTIPLY:
            case ADD:
            case SUBTRACT:
            case MOD:
            case DIVIDE:
                if (t1.isFloatingPointType() || t2.isFloatingPointType()) {
                    castBinaryOp(type.DOUBLE);
                    break;
                }
                if (t1.isFixedPointType()) {
                    t1TargetType = convertIntToDecimalV3Type(t1);
                    castChild(t1TargetType, 0);
                }
                if (t2.isFixedPointType()) {
                    t2TargetType = convertIntToDecimalV3Type(t2);
                    castChild(t2TargetType, 1);
                }
                final int t1Precision = ((ScalarType) t1TargetType).getScalarPrecision();
                final int t2Precision = ((ScalarType) t2TargetType).getScalarPrecision();
                final int t1Scale = ((ScalarType) t1TargetType).getScalarScale();
                final int t2Scale = ((ScalarType) t2TargetType).getScalarScale();
                final int precision = Math.max(t1Precision, t2Precision);
                int scale = Math.max(t1Scale, t2Scale);
                if (op == Operator.MULTIPLY) {
                    scale = t1Scale + t2Scale;
                }
                if (op == Operator.DIVIDE) {
                    scale = t1Scale;
                }
                type = ScalarType.createWiderDecimalV3Type(precision, scale);
                break;
            case INT_DIVIDE:
                if (!t1.isFixedPointType() || !t2.isFloatingPointType()) {
                    castBinaryOp(Type.BIGINT);
                }
                break;
            case BITAND:
            case BITOR:
            case BITXOR:
                type = castBinaryOp(Type.BIGINT);
                break;
            case BITNOT:
            case FACTORIAL:
                break;
            default:
                Preconditions.checkState(false,
                        "Unknown arithmetic operation " + op.toString() + " in: " + this.toSql());
                break;
        }
    }

    @Override
    public void analyzeImpl(Analyzer analyzer) throws AnalysisException {
        if (VectorizedUtil.isVectorized()) {
            // bitnot is the only unary op, deal with it here
            if (op == Operator.BITNOT) {
                Type t = getChild(0).getType();
                if (t.getPrimitiveType().ordinal() > PrimitiveType.LARGEINT.ordinal()) {
                    type = Type.BIGINT;
                    castChild(type, 0);
                } else {
                    type = t;
                }
                fn = getBuiltinFunction(op.getName(), collectChildReturnTypes(), Function.CompareMode.IS_SUPERTYPE_OF);
                if (fn == null) {
                    Preconditions.checkState(false, String.format("No match for op with operand types", toSql()));
                }
                return;
            }
            analyzeSubqueryInChildren();
            // if children has subquery, it will be rewritten and reanalyzed in the future.
            if (contains(Subquery.class)) {
                return;
            }

            Type t1 = getChild(0).getType();
            Type t2 = getChild(1).getType();

            // Support null operation
            if (t1.isNull() || t2.isNull()) {
                castBinaryOp(t1.isNull() ? t2 : t1);
                t1 = getChild(0).getType();
                t2 = getChild(1).getType();
            }

            // dispose the case t1 and t2 is not numeric type
            if (!t1.isNumericType()) {
                castChild(t1.getNumResultType(), 0);
                t1 = t1.getNumResultType();
            }
            if (!t2.isNumericType()) {
                castChild(t2.getNumResultType(), 1);
                t2 = t2.getNumResultType();
            }

            if (t1.isDecimalV3() || t2.isDecimalV3()) {
                analyzeDecimalV3Op(t1, t2);
            } else {
                analyzeNoneDecimalOp(t1, t2);
            }
            fn = getBuiltinFunction(op.name, Arrays.stream(collectChildReturnTypes()).map(
                    (Type type) -> {
                        if (type.getPrimitiveType() == PrimitiveType.DECIMAL32) {
                            return Type.DECIMAL32;
                        } else if (type.getPrimitiveType() == PrimitiveType.DECIMAL64) {
                            return Type.DECIMAL64;
                        } else if (type.getPrimitiveType() == PrimitiveType.DECIMAL128) {
                            return Type.DECIMAL128;
                        } else if (type.getPrimitiveType() == PrimitiveType.DATETIMEV2) {
                            return Type.DATETIMEV2;
                        }
                        return type;
                    }).toArray(Type[]::new), Function.CompareMode.IS_IDENTICAL);
            if (fn == null) {
                Preconditions.checkState(false, String.format(
                        "No match for vec function '%s' with operand types %s and %s", toSql(), t1, t2));
            }
            if (!type.isValid()) {
                type = fn.getReturnType();
            }
        } else {
            // bitnot is the only unary op, deal with it here
            if (op == Operator.BITNOT) {
                type = Type.BIGINT;
                if (getChild(0).getType().getPrimitiveType() != PrimitiveType.BIGINT) {
                    castChild(type, 0);
                }
                fn = getBuiltinFunction(op.getName(), collectChildReturnTypes(), Function.CompareMode.IS_SUPERTYPE_OF);
                if (fn == null) {
                    Preconditions.checkState(false, String.format("No match for op with operand types", toSql()));
                }
                return;
            }
            analyzeSubqueryInChildren();
            // if children has subquery, it will be rewritten and reanalyzed in the future.
            if (contains(Subquery.class)) {
                return;
            }
            Type t1 = getChild(0).getType().getNumResultType();
            Type t2 = getChild(1).getType().getNumResultType();
            // Find result type of this operator
            Type commonType = Type.INVALID;
            String fnName = op.getName();
            switch (op) {
                case MULTIPLY:
                case ADD:
                case SUBTRACT:
                case MOD:
                    // numeric ops must be promoted to highest-resolution type
                    // (otherwise we can't guarantee that a <op> b won't overflow/underflow)
                    commonType = findCommonType(t1, t2);
                    break;
                case DIVIDE:
                    commonType = findCommonType(t1, t2);
                    if (commonType.getPrimitiveType() == PrimitiveType.BIGINT
                            || commonType.getPrimitiveType() == PrimitiveType.LARGEINT) {
                        commonType = Type.DOUBLE;
                    }
                    break;
                case INT_DIVIDE:
                case BITAND:
                case BITOR:
                case BITXOR:
                    // Must be bigint
                    commonType = Type.BIGINT;
                    break;
                default:
                    // the programmer forgot to deal with a case
                    Preconditions.checkState(false,
                            "Unknown arithmetic operation " + op.toString() + " in: " + this.toSql());
                    break;
            }
            type = castBinaryOp(commonType);
            fn = getBuiltinFunction(fnName, collectChildReturnTypes(), Function.CompareMode.IS_IDENTICAL);
            if (fn == null) {
                Preconditions.checkState(false, String.format(
                        "No match for '%s' with operand types %s and %s", toSql(), t1, t2));
            }
        }
    }

    public void analyzeSubqueryInChildren() throws AnalysisException {
        for (Expr child : children) {
            if (child instanceof Subquery) {
                Subquery subquery = (Subquery) child;
                if (!subquery.returnsScalarColumn()) {
                    String msg = "Subquery of arithmetic expr must return a single column: " + child.toSql();
                    throw new AnalysisException(msg);
                }
                /**
                 * Situation: The expr is a binary predicate and the type of subquery is not scalar type.
                 * Add assert: The stmt of subquery is added an assert condition (return error if row count > 1).
                 * Input params:
                 *     expr: 0.9*(select k1 from t2)
                 *     subquery stmt: select k1 from t2
                 * Output params:
                 *     new expr: 0.9 * (select k1 from t2 (assert row count: return error if row count > 1 ))
                 *     subquery stmt: select k1 from t2 (assert row count: return error if row count > 1 )
                 */
                if (!subquery.getType().isScalarType()) {
                    subquery.getStatement().setAssertNumRowsElement(1, AssertNumRowsElement.Assertion.LE);
                }
            }
        }
    }

    @Override
    public int hashCode() {
        return 31 * super.hashCode() + Objects.hashCode(op);
    }

    @Override
    public void finalizeImplForNereids() throws AnalysisException {
        if (op == Operator.BITNOT) {
            fn = getBuiltinFunction(op.getName(), collectChildReturnTypes(), Function.CompareMode.IS_SUPERTYPE_OF);
        } else {
            fn = getBuiltinFunction(op.name, collectChildReturnTypes(), Function.CompareMode.IS_IDENTICAL);
        }
        if (fn == null) {
            Preconditions.checkState(false, String.format("No match for op with operand types. %s", toSql()));
        }
        type = fn.getReturnType();
    }
}
