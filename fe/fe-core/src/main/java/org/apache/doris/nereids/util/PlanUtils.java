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
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalCatalogRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalLimit;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
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

    public static List<Expression> replaceExpressionByProjections(List<NamedExpression> childProjects,
            List<Expression> targetExpression) {
        Map<Slot, Expression> replaceMap = ExpressionUtils.generateReplaceMap(childProjects);
        return ExpressionUtils.replace(targetExpression, replaceMap);
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
        Set<LogicalCatalogRelation> tableSet = plan.collect(LogicalCatalogRelation.class::isInstance);
        return tableSet.stream()
                .map(LogicalCatalogRelation::getTable)
                .collect(ImmutableSet.<TableIf>toImmutableSet());
    }

    /** fastGetChildrenOutput */
    public static List<Slot> fastGetChildrenOutputs(List<Plan> children) {
        switch (children.size()) {
            case 1: return children.get(0).getOutput();
            case 0: return ImmutableList.of();
            default: {
            }
        }

        int outputNum = 0;
        // child.output is cached by AbstractPlan.logicalProperties,
        // we can compute output num without the overhead of re-compute output
        for (Plan child : children) {
            List<Slot> output = child.getOutput();
            outputNum += output.size();
        }
        // generate output list only copy once and without resize the list
        Builder<Slot> output = ImmutableList.builderWithExpectedSize(outputNum);
        for (Plan child : children) {
            output.addAll(child.getOutput());
        }
        return output.build();
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
        return expr instanceof SlotReference
                && ((SlotReference) expr).getColumn().isPresent()
                && ((SlotReference) expr).getTable().isPresent();
    }

    /**
     * collect non_window_agg_func
     */
    public static class CollectNonWindowedAggFuncs {
        public static List<AggregateFunction> collect(Collection<? extends Expression> expressions) {
            List<AggregateFunction> aggFunctions = Lists.newArrayList();
            for (Expression expression : expressions) {
                doCollect(expression, aggFunctions);
            }
            return aggFunctions;
        }

        public static List<AggregateFunction> collect(Expression expression) {
            List<AggregateFunction> aggFuns = Lists.newArrayList();
            doCollect(expression, aggFuns);
            return aggFuns;
        }

        private static void doCollect(Expression expression, List<AggregateFunction> aggFunctions) {
            expression.foreach(expr -> {
                if (expr instanceof AggregateFunction) {
                    aggFunctions.add((AggregateFunction) expr);
                    return true;
                } else if (expr instanceof WindowExpression) {
                    WindowExpression windowExpression = (WindowExpression) expr;
                    for (Expression exprInWindowsSpec : windowExpression.getExpressionsInWindowSpec()) {
                        doCollect(exprInWindowsSpec, aggFunctions);
                    }
                    return true;
                } else {
                    return false;
                }
            });
        }
    }
}
