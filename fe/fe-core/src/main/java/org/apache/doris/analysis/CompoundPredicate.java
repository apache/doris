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
// https://github.com/apache/impala/blob/branch-2.9.0/fe/src/main/java/org/apache/impala/CompoundPredicate.java
// and modified by Doris

package org.apache.doris.analysis;

import org.apache.doris.catalog.FunctionSet;
import org.apache.doris.catalog.ScalarFunction;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.thrift.TExprNode;
import org.apache.doris.thrift.TExprNodeType;
import org.apache.doris.thrift.TExprOpcode;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Objects;

/**
 * &&, ||, ! predicates.
 */
public class CompoundPredicate extends Predicate {
    private static final Logger LOG = LogManager.getLogger(CompoundPredicate.class);
    @SerializedName("op")
    private Operator op;

    public static void initBuiltins(FunctionSet functionSet) {
        functionSet.addBuiltinBothScalaAndVectorized(ScalarFunction.createBuiltinOperator(
                Operator.AND.toString(), Lists.newArrayList(Type.BOOLEAN, Type.BOOLEAN), Type.BOOLEAN));
        functionSet.addBuiltinBothScalaAndVectorized(ScalarFunction.createBuiltinOperator(
                Operator.OR.toString(), Lists.newArrayList(Type.BOOLEAN, Type.BOOLEAN), Type.BOOLEAN));
        functionSet.addBuiltinBothScalaAndVectorized(ScalarFunction.createBuiltinOperator(
                Operator.NOT.toString(), Lists.newArrayList(Type.BOOLEAN), Type.BOOLEAN));
    }

    private CompoundPredicate() {
        // use for serde only
    }

    public CompoundPredicate(Operator op, Expr e1, Expr e2) {
        super();
        this.op = op;
        Preconditions.checkNotNull(e1);
        children.add(e1);
        Preconditions.checkArgument(op == Operator.NOT && e2 == null || op != Operator.NOT && e2 != null);
        if (e2 != null) {
            children.add(e2);
        }
        printSqlInParens = true;
    }

    protected CompoundPredicate(CompoundPredicate other) {
        super(other);
        op = other.op;
        printSqlInParens = true;
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
    public String toSqlImpl(boolean disableTableName, boolean needExternalSql, TableType tableType,
            TableIf table) {
        if (children.size() == 1) {
            Preconditions.checkState(op == Operator.NOT);
            return "NOT " + getChild(0).toSql(disableTableName, needExternalSql, tableType, table);
        } else {
            return getChild(0).toSql(disableTableName, needExternalSql, tableType, table) + " " + op.toString() + " "
                    + getChild(1).toSql(disableTableName, needExternalSql, tableType, table);
        }
    }

    @Override
    public String toDigestImpl() {
        if (children.size() == 1) {
            return "NOT " + getChild(0).toDigest();
        } else {
            return getChild(0).toDigest() + " " + op.toString() + " " + getChild(1).toDigest();
        }
    }

    @Override
    protected void toThrift(TExprNode msg) {
        msg.node_type = TExprNodeType.COMPOUND_PRED;
        msg.setOpcode(op.toThrift());
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
        if (op == Operator.NOT) {
            return getChild(0);
        }
        Expr negatedLeft = getChild(0).negate();
        Expr negatedRight = getChild(1).negate();
        Operator newOp = (op == Operator.OR) ? Operator.AND : Operator.OR;
        return new CompoundPredicate(newOp, negatedLeft, negatedRight);
    }

    @Override
    public Expr getResultValue(boolean forPushDownPredicatesToView) throws AnalysisException {
        recursiveResetChildrenResult(forPushDownPredicatesToView);
        boolean compoundResult = false;
        if (op == Operator.NOT) {
            final Expr childValue = getChild(0);
            if (!(childValue instanceof BoolLiteral)) {
                return this;
            }
            final BoolLiteral boolChild = (BoolLiteral) childValue;
            compoundResult = !boolChild.getValue();
        } else {
            final Expr leftChildValue = getChild(0);
            final Expr rightChildValue = getChild(1);
            if (!(leftChildValue instanceof BoolLiteral)
                    || !(rightChildValue instanceof BoolLiteral)) {
                return this;
            }
            final BoolLiteral leftBoolValue = (BoolLiteral) leftChildValue;
            final BoolLiteral rightBoolValue = (BoolLiteral) rightChildValue;
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

    @Override
    public String toString() {
        return toSqlImpl();
    }

    @Override
    public Expr replaceSubPredicate(Expr subExpr) {
        if (toSqlWithoutTbl().equals(subExpr.toSqlWithoutTbl())) {
            return null;
        }
        if (op.equals(Operator.AND)) {
            Expr lhs = children.get(0);
            Expr rhs = children.get(1);
            if (lhs.replaceSubPredicate(subExpr) == null) {
                return rhs;
            }
            if (rhs.replaceSubPredicate(subExpr) == null) {
                return lhs;
            }
        }
        return this;
    }
}
