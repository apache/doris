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
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalApply;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Set;

/**
 * Swap the order of project and filter under agg in correlated subqueries.
 * <pre>
 * before:
 *              apply
 *         /             \
 *  Input(output:b)     Filter(this node's existence depends on having clause's existence)
 *                         |
 *                        agg
 *                         |
 *                  Project(output:a)
 *                         |
 *              Filter(correlated predicate(Input.e = this.f)/Unapply predicate)
 *                          |
 *                         child
 *
 * after:
 *              apply
 *         /             \
 *  Input(output:b)     Filter(this node's existence depends on having clause's existence)
 *                         |
 *                        agg
 *                         |
 *              Filter(correlated predicate(Input.e = this.f)/Unapply predicate)
 *                         |
 *                  Project(output:a,this.f, Unapply predicate(slots))
 *                          |
 *                         child
 * </pre>
 * SQL example: in select t1.c1 from t1 where t1.c2 > (select max(t3.c3) from (select t2.c2 * 2
 * as c3 from t2 where t2.c1 = t1.c1 and t2.c2 > 10) t3) the subquery is an Aggregate above a
 * Project above a Filter, and the Project computes the argument of the aggregation
 * (t2.c2 * 2 as c3), so it cannot be replaced together with the aggregation
 * (UnCorrelatedApplyAggregateFilter only rewrites aggregations whose projections pass through the
 * columns below them). This rule moves the correlated conjunct t2.c1 = t1.c1 above the Project,
 * so that it sits directly below the aggregation, while the Project and the ordinary conjunct
 * t2.c2 > 10 stay below it; afterwards UnCorrelatedApplyAggregateFilter can pull the conjunct
 * into the apply. When the Project only passes through the columns below it (eg. the Project of
 * the derived table in (select t2.c2 from t2 where t2.c1 = t1.c1)), the aggregation rule handles
 * the subquery by itself and this rule does not fire.
 */
public class PullUpCorrelatedFilterUnderApplyAggregateProject implements RewriteRuleFactory {
    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(logicalApply(any(), logicalAggregate(
                logicalProject(logicalFilter()))).when(LogicalApply::isCorrelated).then(
                        PullUpCorrelatedFilterUnderApplyAggregateProject::pullUpCorrelatedFilter)
                        .toRule(RuleType.PULL_UP_CORRELATED_FILTER_UNDER_APPLY_AGGREGATE_PROJECT),
                logicalApply(any(), logicalFilter((logicalAggregate(
                        logicalProject(logicalFilter()))))).when(LogicalApply::isCorrelated).then(
                                PullUpCorrelatedFilterUnderApplyAggregateProject::pullUpCorrelatedFilter)
                                .toRule(RuleType.PULL_UP_CORRELATED_FILTER_UNDER_APPLY_FILTER_AGGREGATE_PROJECT));
    }

    private static LogicalApply<?, ?> pullUpCorrelatedFilter(LogicalApply<?, ?> apply) {
        boolean isRightChildAgg = apply.right() instanceof LogicalAggregate;
        // locate agg node
        LogicalAggregate<LogicalProject<LogicalFilter<Plan>>> agg = isRightChildAgg
                ? (LogicalAggregate<LogicalProject<LogicalFilter<Plan>>>) (apply.right())
                : (LogicalAggregate<LogicalProject<LogicalFilter<Plan>>>) (apply.right().child(0));

        // pull up filter under the project
        LogicalProject<LogicalFilter<Plan>> project = agg.child();
        LogicalFilter<Plan> filter = project.child();
        List<NamedExpression> newProjects = Lists.newArrayList();
        newProjects.addAll(project.getProjects());

        // filter may use all slots from its child, so add all the slots to newProjects
        filter.child().getOutput().forEach(slot -> {
            if (!newProjects.contains(slot)) {
                newProjects.add(slot);
            }
        });

        Set<Slot> correlatedSlots = ExpressionUtils.getInputSlotSet(apply.getCorrelationSlot());
        Set<Expression> pullUpPredicates = Sets.newLinkedHashSet();
        Set<Expression> filterPredicates = Sets.newLinkedHashSet();
        for (Expression conjunct : filter.getConjuncts()) {
            Set<Slot> conjunctSlots = conjunct.getInputSlots();
            if (Sets.intersection(conjunctSlots, correlatedSlots).isEmpty()) {
                filterPredicates.add(conjunct);
            } else {
                pullUpPredicates.add(conjunct);
            }
        }

        LogicalProject<Plan> newProject = project.withProjectsAndChild(newProjects,
                filterPredicates.isEmpty() ? filter.child()
                        : filter.withConjuncts(filterPredicates));
        LogicalFilter<Plan> newFilter = new LogicalFilter<>(pullUpPredicates, newProject);
        LogicalAggregate<Plan> newAgg = agg.withChildren(ImmutableList.of(newFilter));
        return (LogicalApply<?, ?>) (apply.withChildren(apply.left(),
                isRightChildAgg ? newAgg : apply.right().withChildren(newAgg)));
    }
}
