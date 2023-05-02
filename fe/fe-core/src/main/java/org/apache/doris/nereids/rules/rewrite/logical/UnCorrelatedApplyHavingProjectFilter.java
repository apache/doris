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

package org.apache.doris.nereids.rules.rewrite.logical;

import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.rewrite.OneRewriteRuleFactory;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalApply;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalHaving;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.PlanUtils;
import org.apache.doris.nereids.util.Utils;

import com.google.common.collect.ImmutableSet;

import java.util.*;

/**
 * Raise the correlated predicate in filter and having predicate into apply
 * before:
 *              apply
 *         /              \
 * Input(output:b)      Having(predicate)
 *                         |
 *                      Project(output:a)
 *                         |
 *                     Filter(Correlated predicate/UnCorrelated predicate)
 *                         |
 *                       child
 *
 * after:
 *             apply(Correlated predicate/Having predicate)
 *          /               \
 * Input(output:b)         Project(output:a)
 *                           |
 *                         Filter(UnCorrelated predicate)
 *                           |
 *                          child
 */
public class UnCorrelatedApplyHavingProjectFilter extends OneRewriteRuleFactory {
    @Override
    public Rule build() {
        return logicalApply(any(), logicalHaving(logicalProject(logicalFilter())))
            .when(LogicalApply::isCorrelated)
            .then(apply -> {
                LogicalHaving<LogicalProject<LogicalFilter<Plan>>> having = apply.right();
                LogicalProject<LogicalFilter<Plan>> project = having.child();
                LogicalFilter<Plan> filter = project.child();
                Set<Expression> conjuncts = filter.getConjuncts();
                Map<Boolean, List<Expression>> split = Utils.splitCorrelatedConjuncts(
                    conjuncts, apply.getCorrelationSlot());
                List<Expression> correlatedPredicate = split.get(true);
                List<Expression> unCorrelatedPredicate = split.get(false);
                Plan child = PlanUtils.filterOrSelf(ImmutableSet.copyOf(unCorrelatedPredicate), filter.child());
                List<NamedExpression> projects = new ArrayList<>();
                projects.addAll(project.getProjects());
                ExpressionUtils.collect(correlatedPredicate, SlotReference.class::isInstance).stream()
                    .filter(e -> filter.child().getOutput().contains(e))
                    .filter(e -> !projects.contains(e))
                    .map(NamedExpression.class::cast)
                    .forEach(projects::add);
                LogicalProject newProject = project.withProjectsAndChild(projects, child);
                return new LogicalApply<>(apply.getCorrelationSlot(), apply.getSubqueryExpr(),
                    ExpressionUtils.optionalAnd(correlatedPredicate),
                    ExpressionUtils.optionalAnd(having.getPredicate()),
                    apply.getMarkJoinSlotReference(),
                    apply.getSubCorrespondingConjunct(), apply.isNeedAddSubOutputToProjects(),
                    apply.left(), newProject);
            }).toRule(RuleType.UN_CORRELATED_APPLY_HAVING_PROJECT_FILTER);
    }
}
