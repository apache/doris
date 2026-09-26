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
import org.apache.doris.nereids.spm.SPMPlanTreeSupport;
import org.apache.doris.nereids.spm.placeholder.SPMSubquerySupport;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.InSubquery;
import org.apache.doris.nereids.trees.expressions.SubqueryExpr;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * SPMFrozenTreeReplacer - placeholder replacer for the RE-PARSED frozen plan tree (M3).
 *
 * At rewrite time the frozen planSql - the decompiled optimal plan
 * carrying the placeholder ids, join distribution hints ([BROADCAST] / [SHUFFLE]) and the
 * pushed-down structure - is re-parsed into an unbound logical plan. In that tree a
 * placeholder is NOT an in-memory SpmConstVar / SpmConstList node (those
 * only exist inside the parameterized bind / plan trees) but a raw parsed function call:
 *
 *   _spm_const_var(id) - a scalar placeholder, possibly wrapped by an outer
 *       CAST (the optimizer typed it at CREATE time), e.g.
 *       a > CAST(_spm_const_var(1) AS INT);
 *   _spm_const_list(id) - the single option of an IN predicate, e.g.
 *       x IN (_spm_const_list(3)).
 *
 * This replacer identifies those calls by name + leading integer literal (the id) and
 * substitutes the actual user value (extracted by SPMAstCheckVisitor from the bind tree)
 * - mirroring SR's PlaceholderReplacer which visits FunctionCallExpr and InPredicate.
 */
public class SPMFrozenTreeReplacer extends ExpressionVisitor<Expression, Map<Long, Expression>> {

    /** Function name of the scalar placeholder as printed into the frozen planSql. */
    public static final String CONST_VAR_FUNC = "_spm_const_var";

    /** Function name of the IN-list placeholder as printed into the frozen planSql. */
    public static final String CONST_LIST_FUNC = "_spm_const_list";

    /**
     * Substitutes every placeholder call of the frozen (re-parsed) tree expression with
     * the user value registered under the placeholder id.
     *
     * @param expr              the frozen-tree expression (may contain placeholders)
     * @param placeholderValues placeholder id -> user actual value (extracted from the
     *                          bind-side structural check)
     * @return the substituted expression
     */
    public Expression replace(Expression expr, Map<Long, Expression> placeholderValues) {
        return expr.accept(this, placeholderValues);
    }

    /**
     * Returns whether the expression is a frozen-tree placeholder call
     * (_spm_const_var(id) / _spm_const_list(id)) that was NOT substituted
     * - such an expression must never reach the analyzer (the function is not registered),
     * so a rewritten tree containing one must be rejected by the caller.
     */
    public static boolean isUnsubstitutedPlaceholder(Expression expr) {
        if (expr instanceof UnboundFunction) {
            String name = ((UnboundFunction) expr).getName();
            return CONST_VAR_FUNC.equals(name) || CONST_LIST_FUNC.equals(name);
        }
        return false;
    }

    @Override
    public Expression visit(Expression expr, Map<Long, Expression> placeholderValues) {
        // default: rebuild the children bottom-up (a CAST / arithmetic / comparison /
        // function around a placeholder is preserved, only the placeholder is replaced)
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

    @Override
    public Expression visitUnboundFunction(UnboundFunction function,
            Map<Long, Expression> placeholderValues) {
        // a scalar placeholder call: _spm_const_var(id) -> the user actual value
        if (CONST_VAR_FUNC.equals(function.getName())) {
            Long id = placeholderId(function);
            if (id != null) {
                Expression userValue = placeholderValues.get(id);
                if (userValue != null) {
                    return userValue;
                }
            }
            // no extracted value (plan-only placeholder): leave it for the caller's
            // safety net (isUnsubstitutedPlaceholder) to reject
            return function;
        }
        // a non-placeholder function (e.g. substr(...) whose arguments may still carry
        // placeholders): recurse into the arguments
        return super.visitUnboundFunction(function, placeholderValues);
    }

    @Override
    public Expression visitInPredicate(InPredicate inPredicate,
            Map<Long, Expression> placeholderValues) {
        // _spm_const_list(id) as the single option: replace with the user's actual IN
        // list (the whole user-side IN predicate carries the options). Type coercion
        // wraps the placeholder in a CAST (x IN (CAST(_spm_const_list(id) AS ...))), so
        // the list call must be detected THROUGH the wrapper.
        if (inPredicate.getOptions().size() == 1) {
            UnboundFunction listFn = findConstListCall(inPredicate.getOptions().get(0));
            if (listFn != null) {
                Long id = placeholderId(listFn);
                if (id != null) {
                    Expression userValue = placeholderValues.get(id);
                    if (userValue instanceof InPredicate) {
                        InPredicate userIn = (InPredicate) userValue;
                        // keep the frozen compare expression (substituting any scalar
                        // placeholders still inside it) and adopt the user option list
                        Expression newCompare = inPredicate.getCompareExpr().accept(this,
                                placeholderValues);
                        return new InPredicate(newCompare, userIn.getOptions());
                    }
                }
            }
        }
        return super.visitInPredicate(inPredicate, placeholderValues);
    }

    // ==================== subquery substitution ====================

    @Override
    public Expression visitSubqueryExpr(SubqueryExpr subqueryExpr,
            Map<Long, Expression> placeholderValues) {
        // The generic visit() only walks Expression.children(): a SubqueryExpr's own
        // queryPlan is out-of-band (IN / EXISTS / scalar / residual NOT IN), so its
        // placeholders must be substituted by recursing into the plan explicitly -
        // mirroring SPMPlaceholderReplacer / SPMPlaceholderBuilder. Without this, a
        // residual LEFT NULL_AWARE ANTI JOIN frozen as
        // NOT IN (SELECT ... WHERE ... _spm_const_var(...)) keeps the call inside the
        // subquery plan, the residue scan rejects the replay and a reloaded frozen row
        // has no usable fallback either.
        Expression newCompare = subqueryExpr instanceof InSubquery
                ? ((InSubquery) subqueryExpr).getCompareExpr().accept(this, placeholderValues) : null;
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

    /**
     * Finds the _spm_const_list(id) call under an IN option, looking THROUGH the
     * type-coercion CAST wrapper (x IN (CAST(_spm_const_list(id) AS T))). Returns null
     * when the option is not (a wrapper of) the list placeholder call.
     */
    private static UnboundFunction findConstListCall(Expression option) {
        if (option instanceof UnboundFunction) {
            UnboundFunction fn = (UnboundFunction) option;
            return CONST_LIST_FUNC.equals(fn.getName()) ? fn : null;
        }
        if (option instanceof org.apache.doris.nereids.trees.expressions.Cast
                || option instanceof org.apache.doris.nereids.trees.expressions.TryCast) {
            return option.children().isEmpty() ? null : findConstListCall(option.child(0));
        }
        return null;
    }

    /**
     * Extracts the placeholder id from a parsed placeholder call: the leading argument is
     * an integer literal carrying the id (_spm_const_var(1) -> 1).
     *
     * @return the id, or null when the call does not carry an integer id argument
     */
    private static Long placeholderId(UnboundFunction function) {
        if (function.children().isEmpty()) {
            return null;
        }
        Expression first = function.child(0);
        if (!(first instanceof Literal)) {
            return null;
        }
        Object value = ((Literal) first).getValue();
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        return null;
    }
}
