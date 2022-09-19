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

import org.apache.doris.nereids.pattern.MatchingContext;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.rewrite.RewriteRuleFactory;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * infer additional predicates for `LogicalFilter` and `LogicalJoin`.
 */
public class InferPredicates implements RewriteRuleFactory {
    PredicatePropagation propagation = new PredicatePropagation();
    PullUpPredicates pollUpPredicates = new PullUpPredicates();

    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                inferWhere(),
                inferOn()
        );
    }

    /**
     * reference `inferOn`
     */
    private Rule inferWhere() {
        return logicalFilter(any()).thenApply(ctx -> {
            LogicalFilter<Plan> root = ctx.root;
            Plan filter = getOriginalPlan(ctx, root);
            Set<Expression> filterPredicates = filter.accept(pollUpPredicates, null);
            Set<Expression> filterChildPredicates = filter.child(0).accept(pollUpPredicates, null);
            filterPredicates.removeAll(filterChildPredicates);
            root.getConjuncts().forEach(filterPredicates::remove);
            if (!filterPredicates.isEmpty()) {
                filterPredicates.addAll(root.getConjuncts());
                return new LogicalFilter<>(ExpressionUtils.and(Lists.newArrayList(filterPredicates)), root.child());
            }
            return root;
        }).toRule(RuleType.INFER_PREDICATES_FOR_WHERE);
    }

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
    private Rule inferOn() {
        return logicalJoin(any(), any()).thenApply(ctx -> {
            LogicalJoin<Plan, Plan> root = ctx.root;
            Plan left = getOriginalPlan(ctx, root.left());
            Plan right = getOriginalPlan(ctx, root.right());
            Set<Expression> expressions = getAllExpressions(left, right, root.getOnClauseCondition());
            List<Expression> otherJoinConjuncts = Lists.newArrayList(root.getOtherJoinConjuncts());
            switch (root.getJoinType()) {
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
                    return root;
            }
            return root.withOtherJoinConjuncts(otherJoinConjuncts);
        }).toRule(RuleType.INFER_PREDICATES_FOR_ON);
    }

    private Plan getOriginalPlan(MatchingContext context, Plan patternPlan) {
        patternPlan.getGroupExpression().orElseThrow(() -> new IllegalArgumentException("GroupExpression not exists."));
        return context.cascadesContext.getMemo().copyOut(patternPlan.getGroupExpression().get(), false);
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

