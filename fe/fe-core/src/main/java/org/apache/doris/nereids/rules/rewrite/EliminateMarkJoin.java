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
import org.apache.doris.nereids.rules.expression.ExpressionRewriteContext;
import org.apache.doris.nereids.rules.expression.rules.TrySimplifyPredicateWithMarkJoinSlot;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableList;

import java.util.Set;

/**
 * Eliminate mark join.
 */
public class EliminateMarkJoin extends OneRewriteRuleFactory {

    @Override
    public Rule build() {
        return logicalFilter(logicalJoin().when(
                join -> join.getJoinType().isSemiJoin() && !join.getMarkJoinConjuncts().isEmpty()))
                        .when(filter -> canSimplifyMarkJoin(filter.getConjuncts(), null))
                        .thenApply(ctx -> {
                            LogicalFilter<LogicalJoin<Plan, Plan>> filter = ctx.root;
                            ExpressionRewriteContext rewriteContext = new ExpressionRewriteContext(ctx.cascadesContext);
                            if (canSimplifyMarkJoin(filter.getConjuncts(), rewriteContext)) {
                                return filter.withChildren(eliminateMarkJoin(filter.child()));
                            }
                            return filter;
                        })
                        .toRule(RuleType.ELIMINATE_MARK_JOIN);
    }

    private boolean canSimplifyMarkJoin(Set<Expression> predicates, ExpressionRewriteContext rewriteContext) {
        return ExpressionUtils
                .canInferNotNullForMarkSlot(TrySimplifyPredicateWithMarkJoinSlot.INSTANCE
                        .rewrite(ExpressionUtils.and(predicates), rewriteContext), rewriteContext);
    }

    private LogicalJoin<Plan, Plan> eliminateMarkJoin(LogicalJoin<Plan, Plan> join) {
        ImmutableList.Builder<Expression> newHashConjuncts = ImmutableList.builder();
        newHashConjuncts.addAll(join.getHashJoinConjuncts());
        newHashConjuncts.addAll(join.getMarkJoinConjuncts());
        return join.withJoinConjuncts(newHashConjuncts.build(), join.getOtherJoinConjuncts(),
                ExpressionUtils.EMPTY_CONDITION, join.getJoinReorderContext());
    }
}
