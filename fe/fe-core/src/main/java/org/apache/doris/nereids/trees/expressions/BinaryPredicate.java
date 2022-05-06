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

import org.apache.doris.common.AnalysisException;
import org.apache.doris.nereids.exceptions.UnboundException;
import org.apache.doris.nereids.trees.NodeType;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.types.DataType;

/**
 * Binary predicate expression.
 */
public class BinaryPredicate extends Expression {
    private final Operator operator;

    /**
     * Operator for binary predicate.
     */
    public enum Operator {
        EQ("="),
        NSEQ("<=>"),
        LT("<"),
        GT(">"),
        LE("<="),
        GE(">="),
        ;

        private final String operand;

        Operator(String operand) {
            this.operand = operand;
        }

        /**
         * Translate expression op in Nereids to legacy one in Doris.
         *
         * @param operator expression operator in Nereids
         * @return legacy expression operator in Doris
         * @throws AnalysisException throw exception when operator cannot be recognized
         */
        public static org.apache.doris.analysis.BinaryPredicate.Operator toExprOp(Operator operator)
                throws AnalysisException {
            switch (operator) {
                case EQ:
                    return org.apache.doris.analysis.BinaryPredicate.Operator.EQ;
                case GE:
                    return org.apache.doris.analysis.BinaryPredicate.Operator.GE;
                case GT:
                    return org.apache.doris.analysis.BinaryPredicate.Operator.GT;
                case LE:
                    return org.apache.doris.analysis.BinaryPredicate.Operator.LE;
                case LT:
                    return org.apache.doris.analysis.BinaryPredicate.Operator.LT;
                case NSEQ:
                    return org.apache.doris.analysis.BinaryPredicate.Operator.EQ_FOR_NULL;
                default:
                    throw new AnalysisException("Not support operator: " + operator.name());
            }
        }
    }

    /**
     * Constructor of BinaryPredicate.
     *
     * @param left left child of binary predicate
     * @param right right child of binary predicate
     * @param operator operator of binary predicate
     */
    public BinaryPredicate(Expression left, Expression right, Operator operator) {
        super(NodeType.BINARY_PREDICATE);
        this.operator = operator;
        addChild(left);
        addChild(right);
    }

    public Expression left() {
        return getChild(0);
    }

    public Expression right() {
        return getChild(1);
    }

    public Operator getOperator() {
        return operator;
    }

    @Override
    public boolean nullable() throws UnboundException {
        if (operator == Operator.NSEQ) {
            return false;
        } else {
            return left().nullable() || right().nullable();
        }
    }

    @Override
    public DataType getDataType() throws UnboundException {
        return BooleanType.INSTANCE;
    }

    @Override
    public String sql() {
        return null;
    }

    @Override
    public String toString() {
        return "(" + left() + " " + operator.operand + " " + right() + ")";
    }
}
