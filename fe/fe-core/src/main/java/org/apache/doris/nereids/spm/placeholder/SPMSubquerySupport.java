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

package org.apache.doris.nereids.spm.placeholder;

import org.apache.doris.nereids.trees.expressions.Exists;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.InSubquery;
import org.apache.doris.nereids.trees.expressions.ScalarSubquery;
import org.apache.doris.nereids.trees.expressions.SubqueryExpr;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalHaving;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

/**
 * SPMSubquerySupport - helpers to collect and rebuild the filter predicates of a
 * subquery plan tree.
 *
 * Subquery literals (e.g. the 300 in ... HAVING sum(l_quantity) > 300, or the
 * 'EUROPE' inside a scalar subquery) must go through the same placeholder lifecycle as
 * top-level literals: parameterize (SPMPlaceholderBuilder), match/extract
 * (SPMAstCheckVisitor) and substitute (SPMPlaceholderReplacer). Since those literals
 * live inside LOGICAL PLAN nodes (LogicalFilter / LogicalHaving) of the subquery plan
 * (not in the expression children of the SubqueryExpr), the three visitors delegate the
 * plan traversal here:
 *
 * - transformFilters(LogicalPlan, Function) rebuilds every filter/having
 *   predicate of the plan via an expression transform (used for parameterization and
 *   substitution).
 * - collectFilterPredicates(LogicalPlan) returns all filter/having predicates
 *   in DFS order (used for the pairwise bind-vs-user comparison during matching).
 *
 * Nested subqueries inside a predicate are handled by the expression visitors
 * themselves (their visitSubqueryExpr recurses into the nested plan), so this helper
 * only needs to walk the plain plan nodes.
 */
public final class SPMSubquerySupport {

    private SPMSubquerySupport() {
    }

    /**
     * Rebuilds the plan tree applying an expression transform to every filter/having
     * predicate. Plan nodes that cannot be rebuilt (no withChildren) are kept as-is.
     *
     * @param plan      the subquery plan (or any logical plan)
     * @param transform the predicate transform (e.g. parameterize / substitute)
     * @return the rebuilt plan (the original instance when nothing changed)
     */
    public static LogicalPlan transformFilters(LogicalPlan plan,
            Function<Expression, Expression> transform) {
        Plan result = plan.accept(new FilterTransformVisitor(transform), null);
        return result instanceof LogicalPlan ? (LogicalPlan) result : plan;
    }

    /**
     * Rebuilds a SubqueryExpr with a new (parameterized / substituted) query plan,
     * preserving correlateSlots, typeCoercionExpr and the NOT flag.
     *
     * @param subquery the original subquery expression
     * @param newPlan  the rebuilt query plan
     * @return the rebuilt subquery expression
     */
    public static Expression rebuildSubquery(SubqueryExpr subquery, LogicalPlan newPlan) {
        if (subquery instanceof InSubquery) {
            InSubquery in = (InSubquery) subquery;
            return new InSubquery(in.getCompareExpr(), newPlan, in.getCorrelateSlots(),
                    in.getTypeCoercionExpr(), in.isNot());
        } else if (subquery instanceof ScalarSubquery) {
            ScalarSubquery scalar = (ScalarSubquery) subquery;
            return new ScalarSubquery(newPlan, scalar.getCorrelateSlots(),
                    scalar.getTypeCoercionExpr(), scalar.limitOneIsEliminated());
        } else if (subquery instanceof Exists) {
            Exists exists = (Exists) subquery;
            return new Exists(newPlan, exists.getCorrelateSlots(),
                    exists.getTypeCoercionExpr(), exists.isNot());
        }
        return subquery;
    }

    /**
     * Collects the filter/having predicates of the plan tree in DFS order (children of
     * every node are visited first, then the node itself is emitted if it is a
     * LogicalFilter or LogicalHaving).
     *
     * @param plan the subquery plan
     * @return the predicate expressions in DFS order
     */
    public static List<Expression> collectFilterPredicates(LogicalPlan plan) {
        List<Expression> predicates = new ArrayList<>();
        plan.accept(new FilterCollectVisitor(predicates), null);
        return predicates;
    }

    /** Plan visitor that rebuilds filter/having predicates via an expression transform. */
    private static class FilterTransformVisitor extends PlanVisitor<Plan, Void> {
        private final Function<Expression, Expression> transform;

        FilterTransformVisitor(Function<Expression, Expression> transform) {
            this.transform = transform;
        }

        @Override
        public Plan visit(Plan plan, Void context) {
            // 1. rebuild children bottom-up
            List<Plan> children = plan.children();
            boolean childChanged = false;
            List<Plan> newChildren = new ArrayList<>(children.size());
            for (Plan child : children) {
                Plan newChild = child.accept(this, context);
                newChildren.add(newChild);
                if (newChild != child) {
                    childChanged = true;
                }
            }
            Plan current = plan;
            if (childChanged) {
                try {
                    current = plan.withChildren(newChildren);
                } catch (RuntimeException e) {
                    // plan type without withChildren -> keep original children
                    current = plan;
                }
            }
            // 2. transform the predicate of a filter/having node
            if (current instanceof LogicalFilter) {
                LogicalFilter<?> filter = (LogicalFilter<?>) current;
                Expression newPredicate = transform.apply(filter.getPredicate());
                if (newPredicate != filter.getPredicate()) {
                    return new LogicalFilter(Collections.singleton(newPredicate), newPredicate,
                            filter.child());
                }
            } else if (current instanceof LogicalHaving) {
                LogicalHaving<?> having = (LogicalHaving<?>) current;
                Expression newPredicate = transform.apply(having.getPredicate());
                if (newPredicate != having.getPredicate()) {
                    return new LogicalHaving(Collections.singleton(newPredicate), having.child());
                }
            }
            return current;
        }
    }

    /** Plan visitor that collects filter/having predicates in DFS order. */
    private static class FilterCollectVisitor extends PlanVisitor<Void, Void> {
        private final List<Expression> predicates;

        FilterCollectVisitor(List<Expression> predicates) {
            this.predicates = predicates;
        }

        @Override
        public Void visit(Plan plan, Void context) {
            for (Plan child : plan.children()) {
                child.accept(this, context);
            }
            return null;
        }

        @Override
        public Void visitLogicalFilter(LogicalFilter<?> filter, Void context) {
            predicates.add(filter.getPredicate());
            return null;
        }

        @Override
        public Void visitLogicalHaving(LogicalHaving<?> having, Void context) {
            predicates.add(having.getPredicate());
            return null;
        }
    }
}
