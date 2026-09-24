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

package org.apache.doris.nereids.spm.matcher;

import org.apache.doris.nereids.analyzer.UnboundFunction;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.analyzer.UnboundStar;
import org.apache.doris.nereids.spm.SPMPlanTreeSupport;
import org.apache.doris.nereids.spm.placeholder.SpmConstList;
import org.apache.doris.nereids.spm.placeholder.SpmConstVar;
import org.apache.doris.nereids.trees.expressions.Exists;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.InSubquery;
import org.apache.doris.nereids.trees.expressions.ScalarSubquery;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.SubqueryExpr;
import org.apache.doris.nereids.trees.expressions.WindowExpression;
import org.apache.doris.nereids.trees.expressions.WindowFrame;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;

import java.util.Map;
import java.util.Objects;

/**
 * SPMAstCheckVisitor - AST structure matcher (M2).
 *
 * Corresponds to design doc section 6.4. Recursively compares the bindSql expression tree
 * with the user query expression tree, verifying structural consistency level by level,
 * and extracts the actual parameter values for the placeholders during the comparison.
 *
 * Placeholder handling rules:
 *
 * - bind side is a SpmConstVar -> extract the id and record the actual value from the
 *   corresponding position on the user side; the same id appearing more than once must
 *   resolve to the same value.
 * - bind side is InPredicate(expr, [SpmConstList]) -> the whole user IN list becomes the
 *   actual value of that id.
 *
 * Column references are compared by name (design doc 6.4: "SlotRef 比列名"), so
 * "a = 100" never matches "b = 42" even though both are structurally identical.
 */
public class SPMAstCheckVisitor extends ExpressionVisitor<Boolean, SPMAstCheckVisitor.CheckContext> {

    /**
     * Match context: carries the user-side expression and the placeholder value
     * extraction result.
     */
    public static class CheckContext {
        /** The user-side expression corresponding to the current bind-side node. */
        private final Expression userExpr;
        /** Placeholder id -> actual value (extraction result). */
        private final Map<Long, Expression> placeholderValues;

        public CheckContext(Expression userExpr, Map<Long, Expression> placeholderValues) {
            this.userExpr = userExpr;
            this.placeholderValues = placeholderValues;
        }
    }

    /**
     * Match entry point.
     *
     * @param bindExpr          the bindSql-side expression
     * @param userExpr          the user-query-side expression
     * @param placeholderValues the placeholder value extraction result (filled in here)
     * @return whether the structures match
     */
    public boolean checkExpression(Expression bindExpr, Expression userExpr,
            Map<Long, Expression> placeholderValues) {
        return bindExpr.accept(this, new CheckContext(userExpr, placeholderValues));
    }

    @Override
    public Boolean visit(Expression bindExpr, CheckContext context) {
        Expression userExpr = context.userExpr;

        // ===== placeholder: extract the value (do not compare inner children) =====
        if (bindExpr instanceof SpmConstVar) {
            SpmConstVar placeholder = (SpmConstVar) bindExpr;
            return recordPlaceholderValue(placeholder.getId(), userExpr, context);
        }

        // Type mismatch -> no match
        if (bindExpr.getClass() != userExpr.getClass()) {
            return false;
        }

        // Identity fields that live OUTSIDE children() must be compared explicitly:
        // UnboundStar: the REPLACE payload (a leaf with no children - a baseline
        //   captured with "* REPLACE(k + 1 AS k)" must not replay its captured
        //   expression for a query using "* REPLACE(k + 2 AS k)")
        // UnboundFunction: the database qualifier (db1.f must not match db2.f)
        // WindowExpression: the frame (ROWS 1 PRECEDING must not replay as 2 PRECEDING;
        //   WindowFrame is not an expression child, so neither the placeholder
        //   construction nor the generic child comparison sees its bound offsets)
        if (bindExpr instanceof UnboundStar) {
            return Objects.equals(bindExpr.toSql(), userExpr.toSql());
        }
        if (bindExpr instanceof UnboundFunction
                && !Objects.equals(((UnboundFunction) bindExpr).getDbName(),
                        ((UnboundFunction) userExpr).getDbName())) {
            return false;
        }
        if (bindExpr instanceof WindowExpression) {
            WindowFrame bindFrame = ((WindowExpression) bindExpr).getWindowFrame().orElse(null);
            WindowFrame userFrame = ((WindowExpression) userExpr).getWindowFrame().orElse(null);
            if ((bindFrame == null) != (userFrame == null)) {
                return false;
            }
            if (bindFrame != null
                    && !Objects.equals(bindFrame.computeToSql(), userFrame.computeToSql())) {
                return false;
            }
        }

        // Column references are compared by name (a = 100 must not match b = 42)
        if (bindExpr instanceof SlotReference || bindExpr instanceof UnboundSlot) {
            return Objects.equals(slotName(bindExpr), slotName(userExpr));
        }

        // A literal on the bind side that is NOT a placeholder (e.g. it lives inside a
        // plan-node slot not reached by parameterization, such as a sort key that was not
        // parameterized) must equal the user literal: structure equality alone would
        // otherwise let "ORDER BY substr(x, 1, 20)" match "ORDER BY substr(x, 1, 21)"
        // and the rewritten tree would keep the bind-side literal while group-by /
        // project got the user's value.
        if (bindExpr instanceof Literal && userExpr instanceof Literal) {
            return Objects.equals(((Literal) bindExpr).getValue(), ((Literal) userExpr).getValue());
        }

        // Child count mismatch -> no match
        if (bindExpr.children().size() != userExpr.children().size()) {
            return false;
        }
        // Compare children recursively
        for (int i = 0; i < bindExpr.children().size(); i++) {
            Expression bindChild = bindExpr.children().get(i);
            Expression userChild = userExpr.children().get(i);
            if (!bindChild.accept(this, new CheckContext(userChild, context.placeholderValues))) {
                return false;
            }
        }
        return true;
    }

    /**
     * Returns the column name of a slot (SlotReference or UnboundSlot).
     */
    private static String slotName(Expression expr) {
        if (expr instanceof SlotReference) {
            return ((SlotReference) expr).getName();
        }
        if (expr instanceof UnboundSlot) {
            return String.join(".", ((UnboundSlot) expr).getNameParts());
        }
        return expr.toSql();
    }

    // ==================== IN predicate special handling ====================

    @Override
    public Boolean visitInPredicate(InPredicate bindIn, CheckContext context) {
        if (!(context.userExpr instanceof InPredicate)) {
            return false;
        }
        InPredicate userIn = (InPredicate) context.userExpr;

        // The compareExpr must match
        if (!bindIn.getCompareExpr().accept(this,
                new CheckContext(userIn.getCompareExpr(), context.placeholderValues))) {
            return false;
        }

        // bind side has a single SpmConstList option -> the whole user IN list is the
        // value of that id
        if (bindIn.getOptions().size() == 1 && bindIn.getOptions().get(0) instanceof SpmConstList) {
            SpmConstList list = (SpmConstList) bindIn.getOptions().get(0);
            return recordPlaceholderValue(list.getId(), userIn, context);
        }

        // Regular IN: compare the options one by one
        if (bindIn.getOptions().size() != userIn.getOptions().size()) {
            return false;
        }
        for (int i = 0; i < bindIn.getOptions().size(); i++) {
            if (!bindIn.getOptions().get(i).accept(this,
                    new CheckContext(userIn.getOptions().get(i), context.placeholderValues))) {
                return false;
            }
        }
        return true;
    }

    /**
     * Records a placeholder value: records on first occurrence and validates
     * consistency on later occurrences.
     *
     * @param id       the placeholder id
     * @param userExpr the actual user-side value
     * @param context  the match context
     * @return whether it passes
     */
    private boolean recordPlaceholderValue(long id, Expression userExpr, CheckContext context) {
        if (!context.placeholderValues.containsKey(id)) {
            context.placeholderValues.put(id, userExpr);
            return true;
        }
        return Objects.equals(context.placeholderValues.get(id), userExpr);
    }

    // ==================== subquery matching ====================

    /**
     * Subquery comparison: the query plan of the bind side is compared against the
     * user side structurally (its filter/having predicates compared pairwise, which
     * also extracts the values of any placeholders inside the subquery), and the
     * compare expression / NOT flag of InSubquery / Exists are checked as well.
     *
     * The overall query structure (including the subquery) is already guaranteed by
     * the Level 2 full-query digest match, so the per-predicate comparison is the
     * value extraction pass for the constants inside the subquery.
     */
    @Override
    public Boolean visitSubqueryExpr(SubqueryExpr bindSubquery, CheckContext context) {
        Expression userExpr = context.userExpr;
        if (bindSubquery.getClass() != userExpr.getClass()) {
            return false;
        }
        SubqueryExpr userSubquery = (SubqueryExpr) userExpr;

        if (bindSubquery instanceof InSubquery) {
            InSubquery bindIn = (InSubquery) bindSubquery;
            InSubquery userIn = (InSubquery) userSubquery;
            if (bindIn.isNot() != userIn.isNot()) {
                return false;
            }
            if (!bindIn.getCompareExpr().accept(this,
                    new CheckContext(userIn.getCompareExpr(), context.placeholderValues))) {
                return false;
            }
        } else if (bindSubquery instanceof Exists) {
            if (((Exists) bindSubquery).isNot() != ((Exists) userSubquery).isNot()) {
                return false;
            }
        } else if (bindSubquery instanceof ScalarSubquery) {
            // A ScalarSubquery has no compare expression and no NOT flag, so there is
            // nothing extra to check here - its inner subquery predicates are compared
            // below. It is routed to this method through
            // ExpressionVisitor.visitScalarSubquery (default -> visitSubqueryExpr), so a
            // scalar subquery in the SELECT list or in a filter expression takes part in
            // the structural comparison and value extraction like any other subquery.
        }

        // compare the subquery's whole plan tree against the user side (every node's
        // expressions pairwise); this extracts the user values for placeholders anywhere
        // inside the subquery (filters, having, projections, nested subqueries, ...),
        // while LIMIT / OFFSET of the subquery plan are compared exactly (they cannot be
        // merged from the user query afterwards)
        return SPMPlanTreeSupport.checkSubqueryPlan(bindSubquery.getQueryPlan(),
                userSubquery.getQueryPlan(), context.placeholderValues);
    }
}
