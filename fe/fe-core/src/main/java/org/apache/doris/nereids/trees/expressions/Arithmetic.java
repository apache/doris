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
import org.apache.doris.nereids.trees.NodeType;

/**
 * All arithmetic operator.
 */
public class Arithmetic extends Expression {

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

    public ArithmeticOperator getArithOperator() {
        return op;
    }

    private static NodeType genNodeType(ArithmeticOperator op) {
        switch (op) {
            case MULTIPLY:
                return NodeType.MULTIPLY;
            case DIVIDE:
                return NodeType.DIVIDE;
            case MOD:
                return NodeType.MOD;
            case ADD:
                return NodeType.ADD;
            case SUBTRACT:
                return NodeType.SUBTRACT;
            case BITAND:
                return NodeType.BITAND;
            case BITOR:
                return NodeType.BITOR;
            case BITXOR:
                return NodeType.BITXOR;
            case BITNOT:
                return NodeType.NOT;
            default:
                return null;
        }
    }

    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitArithmetic(this, context);
    }

    @Override
    public String sql() {
        return null;
    }
}
