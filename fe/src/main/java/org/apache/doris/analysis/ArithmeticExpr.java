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

import org.apache.doris.catalog.Function;
import org.apache.doris.catalog.FunctionSet;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarFunction;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.thrift.TExprNode;
import org.apache.doris.thrift.TExprNodeType;
import org.apache.doris.thrift.TExprOpcode;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
                Type.DOUBLE));
        functionSet.addBuiltin(ScalarFunction.createBuiltinOperator(
                Operator.DIVIDE.getName(),
                Lists.<Type>newArrayList(Type.DECIMAL, Type.DECIMAL),
                Type.DECIMAL));
        functionSet.addBuiltin(ScalarFunction.createBuiltinOperator(
                Operator.DIVIDE.getName(),
                Lists.<Type>newArrayList(Type.DECIMALV2, Type.DECIMALV2),
                Type.DECIMALV2));

        // MOD(), FACTORIAL(), BITAND(), BITOR(), BITXOR(), and BITNOT() are registered as
        // builtins, see palo_functions.py
        for (Type t : Type.getIntegerTypes()) {
            functionSet.addBuiltin(ScalarFunction.createBuiltinOperator(
                    Operator.INT_DIVIDE.getName(), Lists.newArrayList(t, t), t));
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
    protected void toThrift(TExprNode msg) {
        msg.node_type = TExprNodeType.ARITHMETIC_EXPR;
        if (!type.isDecimal() && !type.isDecimalV2()) {
            msg.setOpcode(op.getOpcode());
            msg.setOutput_column(outputColumn);
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
            return Type.DECIMALV2;
        } else if (pt1 == PrimitiveType.DECIMAL || pt2 == PrimitiveType.DECIMAL) {
            return Type.DECIMAL;
        } else if (pt1 == PrimitiveType.LARGEINT || pt2 == PrimitiveType.LARGEINT) {
            return Type.LARGEINT;
        } else {
            if (pt1 != PrimitiveType.BIGINT && pt2 != PrimitiveType.BIGINT) {
                return Type.INVALID;
            }
            return Type.BIGINT;
        }
    }

    @Override
    public void analyzeImpl(Analyzer analyzer) throws AnalysisException {
        // bitnot is the only unary op, deal with it here
        if (op == Operator.BITNOT) {
            type = Type.BIGINT;
            if (getChild(0).getType().getPrimitiveType() != PrimitiveType.BIGINT) {
                castChild(type, 0);
            }
            fn = getBuiltinFunction(
                    analyzer, op.getName(), collectChildReturnTypes(), Function.CompareMode.IS_SUPERTYPE_OF);
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
        fn = getBuiltinFunction(analyzer, fnName, collectChildReturnTypes(),
                Function.CompareMode.IS_IDENTICAL);
        if (fn == null) {
            Preconditions.checkState(false, String.format(
                    "No match for '%s' with operand types %s and %s", toSql(), t1, t2));
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
                    subquery.getStatement().setAssertNumRowsElement(1);
                }
            }
        }
    }

    @Override
    public int hashCode() {
        return 31 * super.hashCode() + Objects.hashCode(op);
    }
}
