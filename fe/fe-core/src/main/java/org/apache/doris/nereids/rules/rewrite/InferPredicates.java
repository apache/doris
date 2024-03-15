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

import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.visitor.CustomRewriter;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.PlanUtils;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * infer additional predicates for `LogicalFilter` and `LogicalJoin`.
 * <pre>
 * The logic is as follows:
 * 1. poll up bottom predicate then infer additional predicates
 *   for example:
 *   select * from (select * from t1 where t1.id = 1) t join t2 on t.id = t2.id
 *   1. poll up bottom predicate
 *      select * from (select * from t1 where t1.id = 1) t join t2 on t.id = t2.id and t.id = 1
 *   2. infer
 *      select * from (select * from t1 where t1.id = 1) t join t2 on t.id = t2.id and t.id = 1 and t2.id = 1
 *   finally transformed sql:
 *      select * from (select * from t1 where t1.id = 1) t join t2 on t.id = t2.id and t2.id = 1
 * 2. put these predicates into `otherJoinConjuncts` , these predicates are processed in the next
 *   round of predicate push-down
 * </pre>
 */
public class InferPredicates extends DefaultPlanRewriter<JobContext> implements CustomRewriter {
    private final PullUpPredicates pollUpPredicates = new PullUpPredicates();

    @Override
    public Plan rewriteRoot(Plan plan, JobContext jobContext) {
        return plan.accept(this, jobContext);
    }

    @Override
    public Plan visitLogicalJoin(LogicalJoin<? extends Plan, ? extends Plan> join, JobContext context) {
        join = visitChildren(this, join, context);
        if (join.isMarkJoin()) {
            return join;
        }
        Plan left = join.left();
        Plan right = join.right();
        Set<Expression> expressions = getAllExpressions(left, right, join.getOnClauseCondition());
        switch (join.getJoinType()) {
            case INNER_JOIN:
            case CROSS_JOIN:
            case LEFT_SEMI_JOIN:
            case RIGHT_SEMI_JOIN:
                left = inferNewPredicate(left, expressions);
                right = inferNewPredicate(right, expressions);
                break;
            case LEFT_OUTER_JOIN:
            case LEFT_ANTI_JOIN:
                right = inferNewPredicate(right, expressions);
                break;
            case RIGHT_OUTER_JOIN:
            case RIGHT_ANTI_JOIN:
                left = inferNewPredicate(left, expressions);
                break;
            default:
                break;
        }
        if (left != join.left() || right != join.right()) {
            return join.withChildren(left, right);
        } else {
            return join;
        }
    }

    @Override
    public Plan visitLogicalFilter(LogicalFilter<? extends Plan> filter, JobContext context) {
        filter = visitChildren(this, filter, context);
        Set<Expression> filterPredicates = pullUpPredicates(filter);
        filterPredicates.removeAll(pullUpPredicates(filter.child()));
        filter.getConjuncts().forEach(filterPredicates::remove);
        if (!filterPredicates.isEmpty()) {
            filterPredicates.addAll(filter.getConjuncts());
            return new LogicalFilter<>(ImmutableSet.copyOf(filterPredicates), filter.child());
        }
        return filter;
    }

    private Set<Expression> getAllExpressions(Plan left, Plan right, Optional<Expression> condition) {
        Set<Expression> baseExpressions = pullUpPredicates(left);
        baseExpressions.addAll(pullUpPredicates(right));
        condition.ifPresent(on -> baseExpressions.addAll(ExpressionUtils.extractConjunction(on)));
        baseExpressions.addAll(PredicatePropagation.infer(baseExpressions));
        return baseExpressions;
    }

    private Set<Expression> pullUpPredicates(Plan plan) {
        return Sets.newHashSet(plan.accept(pollUpPredicates, null));
    }

    private Plan inferNewPredicate(Plan plan, Set<Expression> expressions) {
        Set<Expression> predicates = expressions.stream()
                .filter(c -> !c.getInputSlots().isEmpty() && plan.getOutputSet().containsAll(c.getInputSlots()))
                .collect(Collectors.toSet());
        predicates.removeAll(plan.accept(pollUpPredicates, null));
        return PlanUtils.filterOrSelf(predicates, plan);
    }
}
