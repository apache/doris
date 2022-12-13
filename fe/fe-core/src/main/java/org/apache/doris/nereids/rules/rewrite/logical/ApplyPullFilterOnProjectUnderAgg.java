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
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalApply;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;

import com.google.common.collect.Lists;

import java.util.List;

/**
 * Swap the order of project and filter under agg in correlated subqueries.
 * <pre>
 * before:
 *              apply
 *         /              \
 * Input(output:b)        agg
 *                         |
 *                  Project(output:a)
 *                         |
 *              Filter(correlated predicate(Input.e = this.f)/Unapply predicate)
 *                          |
 *                         child
 *
 * after:
 *              apply
 *         /              \
 * Input(output:b)        agg
 *                         |
 *              Filter(correlated predicate(Input.e = this.f)/Unapply predicate)
 *                         |
 *                  Project(output:a,this.f, Unapply predicate(slots))
 *                          |
 *                         child
 * </pre>
 */
public class ApplyPullFilterOnProjectUnderAgg extends OneRewriteRuleFactory {
    @Override
    public Rule build() {
        return logicalApply(group(), logicalAggregate(logicalProject(logicalFilter())))
                .when(LogicalApply::isCorrelated).then(apply -> {
                    LogicalAggregate<LogicalProject<LogicalFilter<GroupPlan>>> agg = apply.right();

                    LogicalProject<LogicalFilter<GroupPlan>> project = agg.child();
                    LogicalFilter<GroupPlan> filter = project.child();
                    List<NamedExpression> newProjects = Lists.newArrayList();
                    newProjects.addAll(project.getProjects());
                    filter.child().getOutput().forEach(slot -> {
                        if (!newProjects.contains(slot)) {
                            newProjects.add(slot);
                        }
                    });

                    LogicalProject newProject = new LogicalProject<>(newProjects, filter.child());
                    LogicalFilter newFilter = new LogicalFilter<>(filter.getPredicates(), newProject);
                    LogicalAggregate newAgg = new LogicalAggregate<>(
                            agg.getGroupByExpressions(), agg.getOutputExpressions(), newFilter);
                    return new LogicalApply<>(apply.getCorrelationSlot(), apply.getSubqueryExpr(),
                            apply.getCorrelationFilter(), apply.left(), newAgg);
                }).toRule(RuleType.APPLY_PULL_FILTER_ON_PROJECT_UNDER_AGG);
    }
}
