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

import com.google.common.base.Joiner;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;
import org.apache.doris.thrift.TExprNode;
import org.apache.doris.thrift.TExprNodeType;
import org.apache.doris.thrift.TTupleIsNullPredicate;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import java.util.List;

/**
 * Internal expr that returns true if all of the given tuples are NULL, otherwise false.
 * Used to make exprs originating from an inline view nullable in an outer join.
 * The given tupleIds must be materialized and nullable at the appropriate PlanNode.
 */
public class TupleIsNullPredicate extends Predicate {

    private final List<TupleId> tupleIds = Lists.newArrayList();

    public TupleIsNullPredicate(List<TupleId> tupleIds) {
        Preconditions.checkState(tupleIds != null && !tupleIds.isEmpty());
        this.tupleIds.addAll(tupleIds);
    }

    protected TupleIsNullPredicate(TupleIsNullPredicate other) {
        super(other);
        tupleIds.addAll(other.tupleIds);
    }

    @Override
    protected void analyzeImpl(Analyzer analyzer) throws AnalysisException {
        super.analyzeImpl(analyzer);
    }

    @Override
    protected boolean isConstantImpl() {
        return false;
    }

    @Override
    public boolean isBoundByTupleIds(List<TupleId> tids) {
        for (TupleId tid : tids) {
            if (tupleIds.contains(tid)) return true;
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
        for (TupleId tid : tupleIds) {
            msg.tuple_is_null_pred.addToTupleIds(tid.asInt());
        }
    }

    public List<TupleId> getTupleIds() {
        return tupleIds;
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

    /**
     * Makes each input expr nullable, if necessary, by wrapping it as follows:
     * IF(TupleIsNull(tids), NULL, expr)
     * <p>
     * The given tids must be materialized. The given inputExprs are expected to be bound
     * by tids once fully substituted against base tables. However, inputExprs may not yet
     * be fully substituted at this point.
     * <p>
     * Returns a new list with the nullable exprs.
     */
    public static List<Expr> wrapExprs(List<Expr> inputExprs,
                                       List<TupleId> tids, Analyzer analyzer) throws UserException {
        // Assert that all tids are materialized.
        for (TupleId tid : tids) {
            TupleDescriptor tupleDesc = analyzer.getTupleDesc(tid);
            Preconditions.checkState(tupleDesc.getIsMaterialized());
        }
        // Perform the wrapping.
        List<Expr> result = Lists.newArrayListWithCapacity(inputExprs.size());
        for (Expr e : inputExprs) {
            result.add(wrapExpr(e, tids, analyzer));
        }
        return result;
    }

    /**
     * Returns a new analyzed conditional expr 'IF(TupleIsNull(tids), NULL, expr)',
     * if required to make expr nullable. Otherwise, returns expr.
     */
    public static Expr wrapExpr(Expr expr, List<TupleId> tids, Analyzer analyzer)
            throws UserException {
        if (!requiresNullWrapping(expr, analyzer)) {
            return expr;
        }
        List<Expr> params = Lists.newArrayList();
        params.add(new TupleIsNullPredicate(tids));
        params.add(new NullLiteral());
        params.add(expr);
        Expr ifExpr = new FunctionCallExpr("if", params);
        ifExpr.analyzeNoThrow(analyzer);
        // The type of function which is different from the type of expr will return the incorrect result in query.
        // Example:
        //   the type of expr is date
        //   the type of function is int
        //   So, the upper fragment will receive a int value instead of date while the result expr is date.
        // If there is no cast function, the result of query will be incorrect.
        if (expr.getType().getPrimitiveType() != ifExpr.getType().getPrimitiveType()) {
            ifExpr = ifExpr.uncheckedCastTo(expr.getType());
        }
        return ifExpr;
    }

    /**
     * Returns true if the given expr evaluates to a non-NULL value if all its contained
     * SlotRefs evaluate to NULL, false otherwise.
     * Throws an InternalException if expr evaluation in the BE failed.
     */
    private static boolean requiresNullWrapping(Expr expr, Analyzer analyzer) {
        return !expr.getType().isNull();
    }

    @Override
    public String toSqlImpl() {
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
            if (fnCallExpr.getFnName().getFunction().equals("if") &&
                    params.get(0) instanceof TupleIsNullPredicate &&
                    Expr.IS_NULL_LITERAL.apply(params.get(1))) {
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
}
