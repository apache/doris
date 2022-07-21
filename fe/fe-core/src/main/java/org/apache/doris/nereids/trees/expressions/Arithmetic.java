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

package org.apache.doris.nereids.trees.expressions;


import org.apache.doris.analysis.ArithmeticExpr;
import org.apache.doris.analysis.ArithmeticExpr.Operator;
import org.apache.doris.nereids.exceptions.UnboundException;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.DataType;

import java.util.Objects;

/**
 * All arithmetic operator.
 */
public abstract class Arithmetic extends Expression {

    enum OperatorPosition {
        BINARY_INFIX,
        UNARY_PREFIX,
        UNARY_POSTFIX,
    }

    /**
     * All counts as expressions.
     */
    @SuppressWarnings("checkstyle:RegexpSingleline")
    public enum ArithmeticOperator {
        MULTIPLY("*", "multiply",
                Arithmetic.OperatorPosition.BINARY_INFIX, Operator.MULTIPLY),
        DIVIDE("/", "divide",
                Arithmetic.OperatorPosition.BINARY_INFIX, Operator.DIVIDE),
        MOD("%", "mod",
                Arithmetic.OperatorPosition.BINARY_INFIX, Operator.MOD),
        ADD("+", "add",
                Arithmetic.OperatorPosition.BINARY_INFIX, Operator.ADD),
        SUBTRACT("-", "subtract",
                Arithmetic.OperatorPosition.BINARY_INFIX, Operator.SUBTRACT),
        //TODO: The following functions will be added later.
        BITAND("&", "bitand",
                Arithmetic.OperatorPosition.BINARY_INFIX, Operator.BITAND),
        BITOR("|", "bitor",
                Arithmetic.OperatorPosition.BINARY_INFIX, Operator.BITOR),
        BITXOR("^", "bitxor",
                Arithmetic.OperatorPosition.BINARY_INFIX, Operator.BITXOR),
        BITNOT("~", "bitnot",
                Arithmetic.OperatorPosition.UNARY_PREFIX, Operator.BITNOT);

        private final String description;
        private final String name;
        private final Arithmetic.OperatorPosition pos;
        private final ArithmeticExpr.Operator staleOp;

        ArithmeticOperator(String description,
                String name,
                Arithmetic.OperatorPosition pos,
                ArithmeticExpr.Operator staleOp) {
            this.description = description;
            this.name = name;
            this.pos = pos;
            this.staleOp = staleOp;
        }

        @Override
        public String toString() {
            return description;
        }

        public String getName() {
            return name;
        }

        public Arithmetic.OperatorPosition getPos() {
            return pos;
        }

        public Operator getStaleOp() {
            return staleOp;
        }

        public boolean isUnary() {
            return pos == Arithmetic.OperatorPosition.UNARY_PREFIX
                    || pos == Arithmetic.OperatorPosition.UNARY_POSTFIX;
        }

        public boolean isBinary() {
            return pos == Arithmetic.OperatorPosition.BINARY_INFIX;
        }
    }

    private final ArithmeticOperator op;

    public Arithmetic(ArithmeticOperator op, Expression... children) {
        super(genNodeType(op), children);
        this.op = op;
    }

    public ArithmeticOperator getArithmeticOperator() {
        return op;
    }

    private static ExpressionType genNodeType(ArithmeticOperator op) {
        switch (op) {
            case MULTIPLY:
                return ExpressionType.MULTIPLY;
            case DIVIDE:
                return ExpressionType.DIVIDE;
            case MOD:
                return ExpressionType.MOD;
            case ADD:
                return ExpressionType.ADD;
            case SUBTRACT:
                return ExpressionType.SUBTRACT;
            case BITAND:
                return ExpressionType.BITAND;
            case BITOR:
                return ExpressionType.BITOR;
            case BITXOR:
                return ExpressionType.BITXOR;
            case BITNOT:
                return ExpressionType.NOT;
            default:
                return null;
        }
    }

    @Override
    public DataType getDataType() {
        // TODO: split Unary and Binary arithmetic
        int arity = arity();
        if (arity == 1) {
            return child(0).getDataType();
        } else if (arity == 2) {
            // TODO: binary arithmetic
            return child(0).getDataType();
        } else {
            return super.getDataType();
        }
    }

    @Override
    public boolean nullable() throws UnboundException {
        if (op.isUnary()) {
            return child(0).nullable();
        } else {
            return child(0).nullable() || child(1).nullable();
        }
    }

    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitArithmetic(this, context);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        Arithmetic that = (Arithmetic) o;
        return op == that.op;
    }

    @Override
    public int hashCode() {
        return Objects.hash(op);
    }

    @Override
    public String toString() {
        return toSql();
    }
}
