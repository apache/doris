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

import org.apache.doris.nereids.spm.SPMPlanTreeSupport;
import org.apache.doris.nereids.spm.placeholder.SPMSubquerySupport;
import org.apache.doris.nereids.spm.placeholder.SpmConstList;
import org.apache.doris.nereids.spm.placeholder.SpmConstVar;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.InSubquery;
import org.apache.doris.nereids.trees.expressions.SubqueryExpr;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * SPMPlaceholderReplacer - placeholder replacer (M2).
 *
 * The core logic of SPMPlanner.replacePlan() in design doc section 6.5. Replaces the
 * placeholders (SpmConstVar / SpmConstList) in planSql with the actual values extracted
 * from the user query:
 *
 * - SpmConstVar -> the user's actual literal
 * - InPredicate(expr, [SpmConstList]) -> the user's actual IN list
 *
 * M2 scope is expression-level replacement (plan-level integration is completed in M3).
 */
public class SPMPlaceholderReplacer extends ExpressionVisitor<Expression, Map<Long, Expression>> {

    /**
     * Replacement entry point.
     *
     * @param expr              the planSql-side expression containing placeholders
     * @param placeholderValues placeholder id -> user actual value (extracted by
     *                          SPMAstCheckVisitor)
     * @return the replaced expression (with no placeholders)
     */
    public Expression replace(Expression expr, Map<Long, Expression> placeholderValues) {
        return expr.accept(this, placeholderValues);
    }

    @Override
    public Expression visit(Expression expr, Map<Long, Expression> placeholderValues) {
        // ===== scalar placeholder -> user actual value =====
        if (expr instanceof SpmConstVar) {
            SpmConstVar placeholder = (SpmConstVar) expr;
            Expression userValue = placeholderValues.get(placeholder.getId());
            return userValue != null ? userValue : expr;
        }
        // default: rebuild children bottom-up
        List<Expression> children = expr.children();
        if (children.isEmpty()) {
            return expr;
        }
        boolean changed = false;
        List<Expression> newChildren = new ArrayList<>(children.size());
        for (Expression child : children) {
            Expression newChild = child.accept(this, placeholderValues);
            newChildren.add(newChild);
            if (newChild != child) {
                changed = true;
            }
        }
        if (!changed) {
            return expr;
        }
        try {
            return expr.withChildren(newChildren);
        } catch (RuntimeException e) {
            return expr;
        }
    }

    // ==================== subquery substitution ====================

    @Override
    public Expression visitSubqueryExpr(SubqueryExpr subqueryExpr, Map<Long, Expression> placeholderValues) {
        // substitute the placeholders in the InSubquery compare expression
        Expression newCompare = subqueryExpr instanceof InSubquery
                ? ((InSubquery) subqueryExpr).getCompareExpr().accept(this, placeholderValues) : null;
        // substitute EVERY placeholder of the subquery's own plan tree (recursively)
        LogicalPlan newPlan = SPMPlanTreeSupport.transform(
                subqueryExpr.getQueryPlan(), expr -> expr.accept(this, placeholderValues));
        if (newPlan == subqueryExpr.getQueryPlan() && newCompare == null) {
            return subqueryExpr;
        }
        Expression rebuilt = SPMSubquerySupport.rebuildSubquery(subqueryExpr, newPlan);
        if (newCompare != null && rebuilt instanceof InSubquery) {
            InSubquery in = (InSubquery) rebuilt;
            return new InSubquery(newCompare, in.getQueryPlan(), in.getCorrelateSlots(),
                    in.getTypeCoercionExpr(), in.isNot());
        }
        return rebuilt;
    }

    // ==================== IN predicate special handling ====================

    @Override
    public Expression visitInPredicate(InPredicate inPredicate, Map<Long, Expression> placeholderValues) {
        // bind side has a single SpmConstList option -> replace with the user's actual
        // IN list
        if (inPredicate.getOptions().size() == 1 && inPredicate.getOptions().get(0) instanceof SpmConstList) {
            SpmConstList list = (SpmConstList) inPredicate.getOptions().get(0);
            Expression userValue = placeholderValues.get(list.getId());
            if (userValue instanceof InPredicate) {
                InPredicate userIn = (InPredicate) userValue;
                // Keep the planSql-side compareExpr shape but replace its inner
                // placeholders (e.g. substr(ca_zip, _spm_const_var(1, 1),
                // _spm_const_var(2, 5))) and replace only the value list
                Expression newCompare = inPredicate.getCompareExpr().accept(this, placeholderValues);
                return new InPredicate(newCompare, userIn.getOptions());
            }
            return inPredicate;
        }
        return super.visitInPredicate(inPredicate, placeholderValues);
    }
}
