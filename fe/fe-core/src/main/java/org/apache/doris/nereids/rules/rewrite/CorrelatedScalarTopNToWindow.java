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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.OrderExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.WindowExpression;
import org.apache.doris.nereids.trees.expressions.functions.window.RowNumber;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalApply;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalLimit;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalSort;
import org.apache.doris.nereids.trees.plans.logical.LogicalSubQueryAlias;
import org.apache.doris.nereids.trees.plans.logical.LogicalWindow;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.PlanUtils;
import org.apache.doris.nereids.util.Utils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Rewrite a correlated scalar subquery with ORDER BY and LIMIT 1 to row_number.
 */
public class CorrelatedScalarTopNToWindow extends OneRewriteRuleFactory {
    @Override
    public Rule build() {
        return logicalApply(any(), logicalLimit(logicalSort()))
                .when(LogicalApply::isScalar)
                .when(LogicalApply::isCorrelated)
                .then(CorrelatedScalarTopNToWindow::rewrite)
                .toRule(RuleType.CORRELATED_SCALAR_TOP_N_TO_WINDOW);
    }

    private static Plan rewrite(LogicalApply<Plan, LogicalLimit<LogicalSort<Plan>>> apply) {
        LogicalLimit<LogicalSort<Plan>> limit = apply.right();
        if (limit.getLimit() != 1 || limit.getOffset() != 0) {
            return null;
        }

        LogicalSort<Plan> sort = limit.child();
        PullUpResult result = pullUpCorrelatedFilter(sort.child(), apply.getCorrelationSlot(),
                ExpressionUtils.getInputSlotSet(sort.getExpressions()));
        if (result == null) {
            return null;
        }

        List<OrderExpression> orderKeys = sort.getOrderKeys().stream()
                .map(OrderExpression::new)
                .collect(Collectors.toList());
        Alias rowNumber = new Alias(new WindowExpression(
                new RowNumber(), result.partitionKeys, orderKeys), "correlated_row_number");
        LogicalWindow<Plan> window = new LogicalWindow<>(ImmutableList.of(rowNumber), result.plan);
        LogicalFilter<Plan> topOne = new LogicalFilter<>(
                ImmutableSet.of(new EqualTo(rowNumber.toSlot(), new BigIntLiteral(1))), window);
        return new LogicalApply<>(apply.getCorrelationSlot(), apply.getSubqueryType(), apply.isNot(),
                apply.getCompareExpr(), apply.getTypeCoercionExpr(),
                ExpressionUtils.optionalAnd(result.correlatedPredicates), apply.getMarkJoinSlotReference(),
                apply.isNeedAddSubOutputToProjects(), apply.isMarkJoinSlotNotNull(), apply.left(), topOne);
    }

    private static PullUpResult pullUpCorrelatedFilter(
            Plan plan, List<Slot> correlationSlots, Set<Slot> requiredSlots) {
        if (plan instanceof LogicalAggregate) {
            return null;
        }
        if (plan instanceof LogicalFilter) {
            LogicalFilter<Plan> filter = (LogicalFilter<Plan>) plan;
            Map<Boolean, List<Expression>> split = Utils.splitCorrelatedConjuncts(
                    filter.getConjuncts(), correlationSlots);
            List<Expression> correlatedPredicates = split.get(true);
            if (!correlatedPredicates.isEmpty()) {
                correlatedPredicates.stream()
                        .filter(predicate -> !(predicate instanceof EqualTo))
                        .forEach(predicate -> {
                            throw new AnalysisException(
                                    "scalar subquery's correlatedPredicates's operator must be EQ");
                        });
                List<Expression> partitionKeys = Utils.getUnCorrelatedExprs(
                        correlatedPredicates, correlationSlots);
                Set<Slot> windowInputSlots = ImmutableSet.<Slot>builder()
                        .addAll(requiredSlots)
                        .addAll(ExpressionUtils.getInputSlotSet(partitionKeys))
                        .build();
                Plan newPlan = PlanUtils.filterOrSelf(
                        ImmutableSet.copyOf(split.get(false)), filter.child());
                return new PullUpResult(newPlan, correlatedPredicates, partitionKeys, windowInputSlots);
            }
        }

        if (!(plan instanceof LogicalProject || plan instanceof LogicalSubQueryAlias
                || plan instanceof LogicalFilter)) {
            throw new AnalysisException("unsupported plan in correlated scalar subquery with top-n: " + plan);
        }

        PullUpResult result = pullUpCorrelatedFilter(plan.child(0), correlationSlots, requiredSlots);
        if (result == null) {
            return null;
        }
        if (plan instanceof LogicalProject) {
            LogicalProject<Plan> project = (LogicalProject<Plan>) plan;
            List<NamedExpression> projects = new ArrayList<>(project.getProjects());
            Set<Slot> outputSlots = projects.stream()
                    .map(NamedExpression::toSlot)
                    .collect(Collectors.toSet());
            for (Slot requiredSlot : result.requiredSlots) {
                if (outputSlots.add(requiredSlot)) {
                    Preconditions.checkState(result.plan.getOutputSet().contains(requiredSlot),
                            "required correlated slot is not output by project child: %s", requiredSlot);
                    projects.add(requiredSlot);
                }
            }
            result.plan = project.withProjectsAndChild(projects, result.plan);
        } else {
            result.plan = plan.withChildren(ImmutableList.of(result.plan));
        }
        Preconditions.checkState(result.plan.getOutputSet().containsAll(result.requiredSlots),
                "required correlated slots are not output: %s", result.requiredSlots);
        return result;
    }

    private static class PullUpResult {
        private Plan plan;
        private final List<Expression> correlatedPredicates;
        private final List<Expression> partitionKeys;
        private final Set<Slot> requiredSlots;

        private PullUpResult(Plan plan, List<Expression> correlatedPredicates,
                List<Expression> partitionKeys, Set<Slot> requiredSlots) {
            this.plan = plan;
            this.correlatedPredicates = correlatedPredicates;
            this.partitionKeys = partitionKeys;
            this.requiredSlots = requiredSlots;
        }
    }
}
