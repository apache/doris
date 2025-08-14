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
// https://github.com/apache/impala/blob/branch-2.9.0/fe/src/main/java/org/apache/impala/TupleIsNullPredicate.java
// and modified by Doris

package org.apache.doris.analysis;

import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.thrift.TExprNode;
import org.apache.doris.thrift.TExprNodeType;
import org.apache.doris.thrift.TNullSide;
import org.apache.doris.thrift.TTupleIsNullPredicate;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Objects;

/**
 * Internal expr that returns true if all of the given tuples are NULL, otherwise false.
 * Used to make exprs originating from an inline view nullable in an outer join.
 * The given tupleIds must be materialized and nullable at the appropriate PlanNode.
 */
public class TupleIsNullPredicate extends Predicate {
    private List<TupleId> tupleIds = Lists.newArrayList();
    // Only effective in vectorized exec engine to mark null side,
    // can set null in origin exec engine
    private TNullSide nullSide = null;

    public TupleIsNullPredicate(List<TupleId> tupleIds, TNullSide nullSide) {
        Preconditions.checkState(tupleIds != null && (!tupleIds.isEmpty() || nullSide != null));
        this.tupleIds.addAll(tupleIds);
        this.nullSide = nullSide;
    }

    protected TupleIsNullPredicate(TupleIsNullPredicate other) {
        super(other);
        tupleIds.addAll(other.tupleIds);
        nullSide = other.nullSide;
    }

    @Override
    protected boolean isConstantImpl() {
        return false;
    }

    @Override
    public boolean isBoundByTupleIds(List<TupleId> tids) {
        for (TupleId tid : tids) {
            if (tupleIds.contains(tid)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public Expr clone() {
        return new TupleIsNullPredicate(this);
    }

    @Override
    protected void toThrift(TExprNode msg) {
        msg.node_type = TExprNodeType.TUPLE_IS_NULL_PRED;
        msg.tuple_is_null_pred = new TTupleIsNullPredicate();
        msg.tuple_is_null_pred.setTupleIds(Lists.newArrayList());
        for (TupleId tid : tupleIds) {
            msg.tuple_is_null_pred.addToTupleIds(tid.asInt());
        }
        if (nullSide != null) {
            msg.tuple_is_null_pred.setNullSide(nullSide);
        }
    }

    public List<TupleId> getTupleIds() {
        return tupleIds;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode());
    }

    @Override
    public boolean equals(Object o) {
        if (!super.equals(o)) {
            return false;
        }
        if (!(o instanceof TupleIsNullPredicate)) {
            return false;
        }
        TupleIsNullPredicate other = (TupleIsNullPredicate) o;
        return other.tupleIds.containsAll(tupleIds)
                && tupleIds.containsAll(other.tupleIds);
    }

    @Override
    public String toSqlImpl() {
        return "TupleIsNull(" + Joiner.on(",").join(tupleIds) + ")";
    }

    @Override
    public String toSqlImpl(boolean disableTableName, boolean needExternalSql, TableType tableType,
            TableIf table) {
        return "TupleIsNull(" + Joiner.on(",").join(tupleIds) + ")";
    }

    /**
     * Recursive function that replaces all 'IF(TupleIsNull(), NULL, e)' exprs in
     * 'expr' with e and returns the modified expr.
     */
    public static Expr unwrapExpr(Expr expr) {
        if (expr instanceof FunctionCallExpr) {
            FunctionCallExpr fnCallExpr = (FunctionCallExpr) expr;
            List<Expr> params = fnCallExpr.getParams().exprs();
            if (fnCallExpr.getFnName().getFunction().equals("if") && params.get(0) instanceof TupleIsNullPredicate
                    && Expr.IS_NULL_LITERAL.apply(params.get(1))) {
                return unwrapExpr(params.get(2));
            }
        }
        for (int i = 0; i < expr.getChildren().size(); ++i) {
            expr.setChild(i, unwrapExpr(expr.getChild(i)));
        }
        return expr;
    }

    @Override
    public boolean isNullable() {
        return false;
    }

    @Override
    public boolean supportSerializable() {
        return false;
    }
}
