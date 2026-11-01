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

import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalApply;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.PlanUtils;
import org.apache.doris.nereids.util.Utils;

import com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Adjust the order of Project and apply in correlated subqueries.
 *
 * before:
 *              apply
 *         /              \
 * Input(output:b)    Project(output:a)
 *                         |
 *                     Filter(Correlated predicate/UnCorrelated predicate)
 *                         |
 *                       child
 *
 * after:
 *                apply(Correlated predicate)
 *          /               \
 * Input(output:b)         Project(output:a)
 *                           |
 *                         Filter(UnCorrelated predicate)
 *                           |
 *                          child
 */
public class UnCorrelatedApplyProjectFilter extends OneRewriteRuleFactory {
    @Override
    public Rule build() {
        return logicalApply(any(), logicalProject(logicalFilter()))
                .when(LogicalApply::isCorrelated)
                .when(LogicalApply::isIn)
                .then(apply -> {
                    LogicalProject<LogicalFilter<Plan>> project = apply.right();
                    LogicalFilter<Plan> filter = project.child();
                    Set<Expression> conjuncts = filter.getConjuncts();
                    Map<Boolean, List<Expression>> split = Utils.splitCorrelatedConjuncts(
                            conjuncts, apply.getCorrelationSlot());
                    List<Expression> correlatedPredicate = split.get(true);
                    List<Expression> unCorrelatedPredicate = split.get(false);

                    // the representative has experienced the rule and added the correlated predicate to the apply node
                    if (correlatedPredicate.isEmpty()) {
                        return apply;
                    }

                    Plan child = PlanUtils.filterOrSelf(ImmutableSet.copyOf(unCorrelatedPredicate), filter.child());
                    List<NamedExpression> projects = new ArrayList<>();
                    projects.addAll(project.getProjects());
                    ExpressionUtils.collect(correlatedPredicate, SlotReference.class::isInstance).stream()
                            .filter(e -> filter.child().getOutput().contains(e))
                            .filter(e -> !projects.contains(e))
                            .map(NamedExpression.class::cast)
                            .forEach(projects::add);
                    LogicalProject newProject = project.withProjectsAndChild(projects, child);
                    // every predicate which was already pulled into the apply is a condition of the
                    // join which unnests the apply, so it may not be replaced by the predicates of
                    // this filter. for the IN subquery of t1.c1 in (select t3.c1 from (select
                    // t2.c1 from t2 where t2.c1 = t1.c1) t3 where t3.c1 > t1.c1) this rule pulls
                    // t3.c1 > t1.c1 from the filter above the projection and t2.c1 = t1.c1 from
                    // the filter below it, in two applications of the rule: the join needs both
                    // conditions
                    List<Expression> newCorrelationFilter = new ArrayList<>();
                    apply.getCorrelationFilter().map(ExpressionUtils::extractConjunction)
                            .ifPresent(newCorrelationFilter::addAll);
                    newCorrelationFilter.addAll(correlatedPredicate);
                    return new LogicalApply<>(apply.getCorrelationSlot(), apply.getSubqueryType(), apply.isNot(),
                            apply.getCompareExpr(), apply.getTypeCoercionExpr(),
                            ExpressionUtils.optionalAnd(newCorrelationFilter), apply.getMarkJoinSlotReference(),
                            apply.isNeedAddSubOutputToProjects(),
                            apply.isMarkJoinSlotNotNull(), apply.left(), newProject);
                }).toRule(RuleType.UN_CORRELATED_APPLY_PROJECT_FILTER);
    }
}
