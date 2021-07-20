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

import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.Reference;

import com.google.common.base.Preconditions;

public abstract class Predicate extends Expr {
    protected boolean isEqJoinConjunct;

    public Predicate() {
        super();
        this.isEqJoinConjunct = false;
    }

    protected Predicate(Predicate other) {
        super(other);
        isEqJoinConjunct = other.isEqJoinConjunct;
    }

    public boolean isEqJoinConjunct() {
        return isEqJoinConjunct;
    }

    public void setIsEqJoinConjunct(boolean v) {
        isEqJoinConjunct = v;
    }

    @Override
    protected void analyzeImpl(Analyzer analyzer) throws AnalysisException {
        type = Type.BOOLEAN;
        // values: true/false/null
        numDistinctValues = 3;
    }

    /**
     * Returns true if one of the children is a slotref (possibly wrapped in a cast)
     * and the other children are all constant. Returns the slotref in 'slotRef' and
     * its child index in 'idx'.
     * This will pick up something like "col = 5", but not "2 * col = 10", which is
     * what we want.
     */
    public boolean isSingleColumnPredicate(Reference<SlotRef> slotRefRef,
      Reference<Integer> idxRef) {
        // find slotref
        SlotRef slotRef = null;
        int i = 0;
        for (; i < children.size(); ++i) {
            slotRef = getChild(i).unwrapSlotRef();
            if (slotRef != null) {
                break;
            }
        }
        if (slotRef == null) {
            return false;
        }

        // make sure everything else is constant
        for (int j = 0; j < children.size(); ++j) {
            if (i == j) {
                continue;
            }
            if (!getChild(j).isConstant()) {
                return false;
            }
        }

        if (slotRefRef != null) {
            slotRefRef.setRef(slotRef);
        }
        if (idxRef != null) {
            idxRef.setRef(Integer.valueOf(i));
        }
        return true;
    }

    public static boolean isEquivalencePredicate(Expr expr) {
        return (expr instanceof BinaryPredicate)
                && ((BinaryPredicate) expr).getOp().isEquivalence();
    }

    public static boolean isUnNullSafeEquivalencePredicate(Expr expr) {
        return (expr instanceof BinaryPredicate)
                && ((BinaryPredicate) expr).getOp().isUnNullSafeEquivalence();
    }

    public static boolean canPushDownPredicate(Expr expr) {
        if (!(expr instanceof Predicate)) {
            return false;
        }

        if (((Predicate) expr).isSingleColumnPredicate(null, null)) {
            if (expr instanceof BinaryPredicate) {
                BinaryPredicate binPredicate = (BinaryPredicate) expr;
                Expr right = binPredicate.getChild(1);

                // because isSingleColumnPredicate
                Preconditions.checkState(right != null);

                // ATTN(cmy): Usually, the BinaryPredicate in the query will be rewritten through ExprRewriteRule,
                // and all SingleColumnPredicate will be rewritten as "column on the left and the constant on the right".
                // So usually the right child is constant.
                //
                // But if there is a subquery in where clause, the planner will equal the subquery to join.
                // During the equal, some auxiliary BinaryPredicate will be automatically generated,
                // and these BinaryPredicates will not go through ExprRewriteRule.
                // As a result, these BinaryPredicates may be as "column on the right and the constant on the left".
                // Example can be found in QueryPlanTest.java -> testJoinPredicateTransitivityWithSubqueryInWhereClause().
                //
                // Because our current planner implementation is very error-prone, so when this happens,
                // we simply assume that these kind of BinaryPredicates cannot be pushed down,
                // to ensure that this change will not affect other query plans.
                if (!right.isConstant()) {
                    return false;
                }

                return right instanceof LiteralExpr;
            }

            if (expr instanceof InPredicate) {
                InPredicate inPredicate = (InPredicate) expr;
                return inPredicate.isLiteralChildren();
            }
        }

        return false;
    }


    /**
     * If predicate is of the form "<slotref> = <slotref>", returns both SlotRefs,
     * otherwise returns null.
     */
    public Pair<SlotId, SlotId> getEqSlots() {
        return null;
    }
}
