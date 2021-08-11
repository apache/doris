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

import com.google.common.collect.Lists;
import org.apache.doris.catalog.FunctionSet;
import org.apache.doris.catalog.ScalarFunction;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.thrift.TExprNode;
import org.apache.doris.thrift.TExprNodeType;
import org.apache.doris.thrift.TExprOpcode;

import com.google.common.base.Preconditions;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Objects;

/**
 * &&, ||, ! predicates.
 */
public class CompoundPredicate extends Predicate {
    private final static Logger LOG = LogManager.getLogger(CompoundPredicate.class);
    private final Operator op;

    public static void initBuiltins(FunctionSet functionSet) {
        functionSet.addBuiltinBothScalaAndVectorized(ScalarFunction.createBuiltinOperator(
                Operator.AND.toString(), Lists.newArrayList(Type.BOOLEAN, Type.BOOLEAN), Type.BOOLEAN));
        functionSet.addBuiltinBothScalaAndVectorized(ScalarFunction.createBuiltinOperator(
                Operator.OR.toString(), Lists.newArrayList(Type.BOOLEAN, Type.BOOLEAN), Type.BOOLEAN));
        functionSet.addBuiltinBothScalaAndVectorized(ScalarFunction.createBuiltinOperator(
                Operator.NOT.toString(), Lists.newArrayList(Type.BOOLEAN), Type.BOOLEAN));
    }

    public CompoundPredicate(Operator op, Expr e1, Expr e2) {
        super();
        this.op = op;
        Preconditions.checkNotNull(e1);
        children.add(e1);
        Preconditions.checkArgument(
          op == Operator.NOT && e2 == null || op != Operator.NOT && e2 != null);
        if (e2 != null) {
            children.add(e2);
        }
    }

    protected CompoundPredicate(CompoundPredicate other) {
        super(other);
        op = other.op;
    }

    @Override
    public Expr clone() {
        return new CompoundPredicate(this);
    }

    public Operator getOp() {
        return op;
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj) && ((CompoundPredicate) obj).op == op;
    }

    @Override
    public String toSqlImpl() {
        if (children.size() == 1) {
            Preconditions.checkState(op == Operator.NOT);
            return "NOT " + getChild(0).toSql();
        } else {
            return getChild(0).toSql() + " " + op.toString() + " " + getChild(1).toSql();
        }
    }

    @Override
    protected void toThrift(TExprNode msg) {
        msg.node_type = TExprNodeType.COMPOUND_PRED;
        msg.setOpcode(op.toThrift());
    }

    @Override
    public boolean isVectorized() {
        return false;
    }

    @Override
    public void analyzeImpl(Analyzer analyzer) throws AnalysisException {
        super.analyzeImpl(analyzer);

        // Check that children are predicates.
        for (Expr e : children) {
            if (!e.getType().equals(Type.BOOLEAN) && !e.getType().isNull()) {
                throw new AnalysisException(String.format(
                  "Operand '%s' part of predicate " + "'%s' should return type 'BOOLEAN' but " +
                    "returns type '%s'.",
                  e.toSql(), toSql(), e.getType()));
            }
        }

        if (getChild(0).selectivity == -1 || children.size() == 2 && getChild(
          1).selectivity == -1) {
            // give up if we're missing an input
            selectivity = -1;
            return;
        }

        switch (op) {
            case AND:
                selectivity = getChild(0).selectivity * getChild(1).selectivity;
                break;
            case OR:
                selectivity = getChild(0).selectivity + getChild(1).selectivity - getChild(
                  0).selectivity * getChild(1).selectivity;
                break;
            case NOT:
                selectivity = 1.0 - getChild(0).selectivity;
                break;
        }
        selectivity = Math.max(0.0, Math.min(1.0, selectivity));
        if (LOG.isDebugEnabled()) {
            LOG.debug(toSql() + " selectivity: " + Double.toString(selectivity));
        }
    }

    public enum Operator {
        AND("AND", TExprOpcode.COMPOUND_AND),
        OR("OR", TExprOpcode.COMPOUND_OR),
        NOT("NOT", TExprOpcode.COMPOUND_NOT);

        private final String      description;
        private final TExprOpcode thriftOp;

        Operator(String description, TExprOpcode thriftOp) {
            this.description = description;
            this.thriftOp = thriftOp;
        }

        @Override
        public String toString() {
            return description;
        }

        public TExprOpcode toThrift() {
            return thriftOp;
        }
    }

    /**
     * Negates a CompoundPredicate.
     */
    @Override
    public Expr negate() {
        if (op == Operator.NOT) return getChild(0);
        Expr negatedLeft = getChild(0).negate();
        Expr negatedRight = getChild(1).negate();
        Operator newOp = (op == Operator.OR) ? Operator.AND : Operator.OR;
        return new CompoundPredicate(newOp, negatedLeft, negatedRight);
    }

    // Create an AND predicate between two exprs, 'lhs' and 'rhs'. If
    // 'rhs' is null, simply return 'lhs'.
    public static Expr createConjunction(Expr lhs, Expr rhs) {
        if (rhs == null) {
            return lhs;
        }
        return new CompoundPredicate(Operator.AND, rhs, lhs);
    }

    /**
     * Creates a conjunctive predicate from a list of exprs.
     */
    public static Expr createConjunctivePredicate(List<Expr> conjuncts) {
        Expr conjunctivePred = null;
        for (Expr expr: conjuncts) {
          if (conjunctivePred == null) {
            conjunctivePred = expr;
            continue;
          }
          conjunctivePred = new CompoundPredicate(CompoundPredicate.Operator.AND,
              expr, conjunctivePred);
        }
        return conjunctivePred;
    }

   @Override
    public Expr getResultValue() throws AnalysisException {
        recursiveResetChildrenResult();
        boolean compoundResult = false;
        if (op == Operator.NOT) {
            final Expr childValue = getChild(0);
            if(!(childValue instanceof BoolLiteral)) {
                return this;
            }
            final BoolLiteral boolChild = (BoolLiteral)childValue;
            compoundResult = !boolChild.getValue();
        } else {
            final Expr leftChildValue = getChild(0);
            final Expr rightChildValue = getChild(1);
            if(!(leftChildValue instanceof BoolLiteral)
                    || !(rightChildValue instanceof BoolLiteral)) {
                return this;
            }
            final BoolLiteral leftBoolValue = (BoolLiteral)leftChildValue;
            final BoolLiteral rightBoolValue = (BoolLiteral)rightChildValue;
            switch (op) {
                case AND:
                    compoundResult = leftBoolValue.getValue() && rightBoolValue.getValue();
                    break;
                case OR:
                    compoundResult = leftBoolValue.getValue() || rightBoolValue.getValue();
                    break;
                default:
                    Preconditions.checkState(false, "No defined binary operator.");
            }
        }
        return new BoolLiteral(compoundResult);
    }

    @Override
    public int hashCode() {
        return 31 * super.hashCode() + Objects.hashCode(op);
    }

    @Override
    public boolean isNullable() {
        return hasNullableChild();
    }
}
