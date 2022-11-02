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

import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.Lists;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * infer additional predicates for `LogicalFilter` and `LogicalJoin`.
 */
public class InferPredicates extends DefaultPlanRewriter<JobContext> {
    PredicatePropagation propagation = new PredicatePropagation();
    PullUpPredicates pollUpPredicates = new PullUpPredicates();

    /**
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
     */
    @Override
    public Plan visitLogicalJoin(LogicalJoin<? extends Plan, ? extends Plan> join, JobContext context) {
        join = (LogicalJoin<? extends Plan, ? extends Plan>) super.visit(join, context);
        Plan left = join.left();
        Plan right = join.right();
        Set<Expression> expressions = getAllExpressions(left, right, join.getOnClauseCondition());
        List<Expression> otherJoinConjuncts = Lists.newArrayList(join.getOtherJoinConjuncts());
        switch (join.getJoinType()) {
            case INNER_JOIN:
            case CROSS_JOIN:
            case LEFT_SEMI_JOIN:
            case RIGHT_SEMI_JOIN:
                otherJoinConjuncts.addAll(inferNewPredicate(left, expressions));
                otherJoinConjuncts.addAll(inferNewPredicate(right, expressions));
                break;
            case LEFT_OUTER_JOIN:
            case LEFT_ANTI_JOIN:
                otherJoinConjuncts.addAll(inferNewPredicate(right, expressions));
                break;
            case RIGHT_OUTER_JOIN:
            case RIGHT_ANTI_JOIN:
                otherJoinConjuncts.addAll(inferNewPredicate(left, expressions));
                break;
            default:
                return join;
        }
        return join.withOtherJoinConjuncts(otherJoinConjuncts);
    }

    /**
     * reference `inferOn`
     */
    @Override
    public Plan visitLogicalFilter(LogicalFilter<? extends Plan> filter, JobContext context) {
        filter = (LogicalFilter<? extends Plan>) super.visit(filter, context);
        Set<Expression> filterPredicates = filter.accept(pollUpPredicates, null);
        Set<Expression> filterChildPredicates = filter.child(0).accept(pollUpPredicates, null);
        filterPredicates.removeAll(filterChildPredicates);
        filter.getConjuncts().forEach(filterPredicates::remove);
        if (!filterPredicates.isEmpty()) {
            filterPredicates.addAll(filter.getConjuncts());
            return new LogicalFilter<>(ExpressionUtils.and(Lists.newArrayList(filterPredicates)), filter.child());
        }
        return filter;
    }

    private Set<Expression> getAllExpressions(Plan left, Plan right, Optional<Expression> condition) {
        Set<Expression> baseExpressions = left.accept(pollUpPredicates, null);
        baseExpressions.addAll(right.accept(pollUpPredicates, null));
        condition.ifPresent(on -> baseExpressions.addAll(ExpressionUtils.extractConjunction(on)));
        baseExpressions.addAll(propagation.infer(baseExpressions));
        return baseExpressions;
    }

    private List<Expression> inferNewPredicate(Plan originalPlan, Set<Expression> expressions) {
        List<Expression> predicates = expressions.stream()
                .filter(c -> !c.getInputSlots().isEmpty() && originalPlan.getOutputSet().containsAll(
                        c.getInputSlots())).collect(Collectors.toList());
        predicates.removeAll(originalPlan.accept(pollUpPredicates, null));
        return predicates;
    }
}

