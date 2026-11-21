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
import java.util.List;

/**
 * Helpers for rebuilding supported subquery expressions and visiting filter predicates through ordinary plan
 * children. Whole-tree placeholder traversal is implemented by SPMPlanTreeSupport.
 */
public final class SPMSubquerySupport {

    private SPMSubquerySupport() {
    }

    /**
     * Rebuilds IN, scalar and EXISTS subqueries with the supplied plan while preserving their expression metadata.
     * Other subquery expression types are returned unchanged.
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
     * Collects LogicalFilter and LogicalHaving predicates through ordinary plan children in postorder. Nested
     * filters and having nodes are all visited; expression-owned subquery plans are handled separately by the
     * expression renderer.
     */
    public static List<Expression> collectFilterPredicates(LogicalPlan plan) {
        List<Expression> predicates = new ArrayList<>();
        plan.accept(new FilterCollectVisitor(predicates), null);
        return predicates;
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
            filter.child().accept(this, context);
            predicates.add(filter.getPredicate());
            return null;
        }

        @Override
        public Void visitLogicalHaving(LogicalHaving<?> having, Void context) {
            having.child().accept(this, context);
            predicates.add(having.getPredicate());
            return null;
        }
    }
}
