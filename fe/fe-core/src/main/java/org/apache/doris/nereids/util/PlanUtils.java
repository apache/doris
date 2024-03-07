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

package org.apache.doris.nereids.util;

import org.apache.doris.catalog.TableIf;
import org.apache.doris.nereids.trees.expressions.ComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.WindowExpression;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionVisitor;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalCatalogRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalLimit;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Util for plan
 */
public class PlanUtils {
    public static Optional<LogicalFilter<? extends Plan>> filter(Set<Expression> predicates, Plan plan) {
        if (predicates.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(new LogicalFilter<>(predicates, plan));
    }

    public static Plan filterOrSelf(Set<Expression> predicates, Plan plan) {
        return filter(predicates, plan).map(Plan.class::cast).orElse(plan);
    }

    /**
     * normalize comparison predicate on a binary plan to its two sides are corresponding to the child's output.
     */
    public static ComparisonPredicate maybeCommuteComparisonPredicate(ComparisonPredicate expression, Plan left) {
        Set<Slot> slots = expression.left().getInputSlots();
        Set<Slot> leftSlots = left.getOutputSet();
        Set<Slot> buffer = Sets.newHashSet(slots);
        buffer.removeAll(leftSlots);
        return buffer.isEmpty() ? expression : expression.commute();
    }

    public static Optional<LogicalProject<? extends Plan>> project(List<NamedExpression> projects, Plan plan) {
        if (projects.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(new LogicalProject<>(projects, plan));
    }

    public static Plan projectOrSelf(List<NamedExpression> projects, Plan plan) {
        return project(projects, plan).map(Plan.class::cast).orElse(plan);
    }

    public static LogicalAggregate<Plan> distinct(Plan plan) {
        if (plan instanceof LogicalAggregate && ((LogicalAggregate<?>) plan).isDistinct()) {
            return (LogicalAggregate<Plan>) plan;
        } else {
            return new LogicalAggregate<>(ImmutableList.copyOf(plan.getOutput()), false, plan);
        }
    }

    /**
     * merge childProjects with parentProjects
     */
    public static List<NamedExpression> mergeProjections(List<NamedExpression> childProjects,
            List<NamedExpression> parentProjects) {
        Map<Slot, Expression> replaceMap = ExpressionUtils.generateReplaceMap(childProjects);
        return ExpressionUtils.replaceNamedExpressions(parentProjects, replaceMap);
    }

    public static Plan skipProjectFilterLimit(Plan plan) {
        if (plan instanceof LogicalProject && ((LogicalProject<?>) plan).isAllSlots()
                || plan instanceof LogicalFilter || plan instanceof LogicalLimit) {
            return plan.child(0);
        }
        return plan;
    }

    public static Set<LogicalCatalogRelation> getLogicalScanFromRootPlan(LogicalPlan rootPlan) {
        Set<LogicalCatalogRelation> tableSet = new HashSet<>();
        tableSet.addAll((Collection<? extends LogicalCatalogRelation>) rootPlan
                .collect(LogicalCatalogRelation.class::isInstance));
        return tableSet;
    }

    /**
     * get table set from plan root.
     */
    public static ImmutableSet<TableIf> getTableSet(LogicalPlan plan) {
        Set<LogicalCatalogRelation> tableSet = new HashSet<>();
        tableSet.addAll((Collection<? extends LogicalCatalogRelation>) plan
                .collect(LogicalCatalogRelation.class::isInstance));
        ImmutableSet<TableIf> resultSet = tableSet.stream().map(e -> e.getTable())
                .collect(ImmutableSet.toImmutableSet());
        return resultSet;
    }

    /**
     * Check if slot is from the plan.
     */
    public static boolean checkSlotFrom(Plan plan, SlotReference slot) {
        Set<LogicalCatalogRelation> tableSets = PlanUtils.getLogicalScanFromRootPlan((LogicalPlan) plan);
        for (LogicalCatalogRelation table : tableSets) {
            if (table.getOutputExprIds().contains(slot.getExprId())) {
                return true;
            }
        }
        return false;
    }

    /**
     * Check if the expression is a column reference.
     */
    public static boolean isColumnRef(Expression expr) {
        return expr instanceof SlotReference && ((SlotReference) expr).getTable().isPresent()
                && ((SlotReference) expr).getColumn().isPresent()
                && ((SlotReference) expr).getTable().isPresent();
    }

    /**
     * collect non_window_agg_func
     */
    public static class CollectNonWindowedAggFuncs extends DefaultExpressionVisitor<Void, List<AggregateFunction>> {

        public static final CollectNonWindowedAggFuncs INSTANCE = new CollectNonWindowedAggFuncs();

        @Override
        public Void visitWindow(WindowExpression windowExpression, List<AggregateFunction> context) {
            for (Expression child : windowExpression.getExpressionsInWindowSpec()) {
                child.accept(this, context);
            }
            return null;
        }

        @Override
        public Void visitAggregateFunction(AggregateFunction aggregateFunction, List<AggregateFunction> context) {
            context.add(aggregateFunction);
            return null;
        }
    }
}
