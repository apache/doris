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

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalLimit;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.PlanUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Set;

/**
 * Push down filter through project.
 * input:
 * filter(a>2, b=0) -> project(c+d as a, e as b)
 * output:
 * project(c+d as a, e as b) -> filter(c+d>2, e=0).
 */

public class PushDownFilterThroughProject implements RewriteRuleFactory {
    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                logicalFilter(logicalProject())
                        .whenNot(filter -> ExpressionUtils.containsWindowExpression(filter.child().getProjects()))
                        .then(PushDownFilterThroughProject::pushDownFilterThroughProject)
                        .toRule(RuleType.PUSH_DOWN_FILTER_THROUGH_PROJECT),
                // filter(project(limit)) will change to filter(limit(project)) by PushdownProjectThroughLimit,
                // then we should change filter(limit(project)) to project(filter(limit))
                // TODO maybe we could remove this rule, because translator already support filter(limit(project))
                logicalFilter(logicalLimit(logicalProject()))
                        .whenNot(filter ->
                                ExpressionUtils.containsWindowExpression(filter.child().child().getProjects())
                        )
                        .then(PushDownFilterThroughProject::pushDownFilterThroughLimitProject)
                        .toRule(RuleType.PUSH_DOWN_FILTER_THROUGH_PROJECT_UNDER_LIMIT)
        );
    }

    /** push down Filter through project */
    private static Plan pushDownFilterThroughProject(LogicalFilter<LogicalProject<Plan>> filter) {
        LogicalProject<? extends Plan> project = filter.child();
        Set<Slot> childOutputs = project.getOutputSet();
        // we need run this rule before subquery unnesting
        // therefore the conjuncts may contain slots from outer query
        // we should only push down conjuncts without any outer query's slot,
        // so we split the conjuncts into two parts:
        // splitConjuncts.first -> conjuncts having outer query slots which should NOT be pushed down
        // splitConjuncts.second -> conjuncts without any outer query slots which should be pushed down
        Pair<Set<Expression>, Set<Expression>> splitConjuncts =
                splitConjunctsByChildOutput(filter.getConjuncts(), childOutputs);
        if (splitConjuncts.second.isEmpty()) {
            // all conjuncts contain outer query's slots, no conjunct can be pushed down
            // just return unchanged plan
            return null;
        }
        project = (LogicalProject<? extends Plan>) project.withChildren(new LogicalFilter<>(
                ExpressionUtils.replace(splitConjuncts.second, project.getAliasToProducer()),
                project.child()));
        return PlanUtils.filterOrSelf(splitConjuncts.first, project);
    }

    private static Plan pushDownFilterThroughLimitProject(
            LogicalFilter<LogicalLimit<LogicalProject<Plan>>> filter) {
        LogicalLimit<LogicalProject<Plan>> limit = filter.child();
        LogicalProject<Plan> project = limit.child();
        Set<Slot> childOutputs = project.getOutputSet();
        // split the conjuncts by child's output
        Pair<Set<Expression>, Set<Expression>> splitConjuncts =
                splitConjunctsByChildOutput(filter.getConjuncts(), childOutputs);
        if (splitConjuncts.second.isEmpty()) {
            return null;
        }
        project = project.withProjectsAndChild(project.getProjects(),
                new LogicalFilter<>(
                        ExpressionUtils.replace(splitConjuncts.second,
                                project.getAliasToProducer()),
                        limit.withChildren(project.child())));
        return PlanUtils.filterOrSelf(splitConjuncts.first, project);
    }

    private static Pair<Set<Expression>, Set<Expression>> splitConjunctsByChildOutput(
            Set<Expression> conjuncts, Set<Slot> childOutputs) {
        Set<Expression> pushDownPredicates = Sets.newLinkedHashSet();
        Set<Expression> remainPredicates = Sets.newLinkedHashSet();
        for (Expression conjunct : conjuncts) {
            Set<Slot> conjunctSlots = conjunct.getInputSlots();
            if (childOutputs.containsAll(conjunctSlots)) {
                pushDownPredicates.add(conjunct);
            } else {
                remainPredicates.add(conjunct);
            }
        }
        return Pair.of(remainPredicates, pushDownPredicates);
    }
}
