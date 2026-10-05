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
import org.apache.doris.nereids.rules.expression.rules.OrToIn;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * infer In-predicate from Or
 *
 */
public class InferInPredicateFromOr implements RewriteRuleFactory {

    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                logicalFilter()
                        .when(InferInPredicateFromOr::containsOr)
                        .then(this::rewriteFilterExpression).toRule(RuleType.EXTRACT_IN_PREDICATE_FROM_OR),
                logicalProject()
                        .when(InferInPredicateFromOr::containsOr)
                        .then(this::rewriteProject).toRule(RuleType.EXTRACT_IN_PREDICATE_FROM_OR),
                logicalJoin()
                        .whenNot(LogicalJoin::isMarkJoin)
                        .when(InferInPredicateFromOr::containsOr)
                        .then(this::rewriteJoin).toRule(RuleType.EXTRACT_IN_PREDICATE_FROM_OR)
        );
    }

    private LogicalFilter<Plan> rewriteFilterExpression(LogicalFilter<Plan> filter) {
        Expression rewritten = OrToIn.EXTRACT_MODE_INSTANCE.rewriteTree(filter.getPredicate());
        Set<Expression> set = new LinkedHashSet<>(ExpressionUtils.extractConjunction(rewritten));
        return filter.withConjuncts(set);
    }

    private LogicalProject<Plan> rewriteProject(LogicalProject<Plan> project) {
        List<NamedExpression> newProjections = Lists.newArrayList();
        for (NamedExpression proj : project.getProjects()) {
            if (proj instanceof SlotReference) {
                newProjections.add(proj);
            } else {
                Expression rewritten = OrToIn.REPLACE_MODE_INSTANCE.rewriteTree(proj);
                newProjections.add((NamedExpression) rewritten);
            }
        }
        return project.withProjects(newProjections);
    }

    private LogicalJoin<Plan, Plan> rewriteJoin(LogicalJoin<Plan, Plan> join) {
        if (!join.isMarkJoin()) {
            Expression otherCondition;
            if (join.getOtherJoinConjuncts().isEmpty()) {
                return join;
            } else if (join.getOtherJoinConjuncts().size() == 1) {
                otherCondition = join.getOtherJoinConjuncts().get(0);
            } else {
                otherCondition = new And(join.getOtherJoinConjuncts());
            }
            Expression rewritten = OrToIn.REPLACE_MODE_INSTANCE.rewriteTree(otherCondition);
            join = join.withJoinConjuncts(join.getHashJoinConjuncts(), ExpressionUtils.extractConjunction(rewritten),
                    join.getJoinReorderContext());
        }
        return join;
    }

    private static boolean containsOr(Plan plan) {
        for (Expression expression : plan.getExpressions()) {
            if (expression.containsType(Or.class)) {
                return true;
            }
        }
        return false;
    }
}
