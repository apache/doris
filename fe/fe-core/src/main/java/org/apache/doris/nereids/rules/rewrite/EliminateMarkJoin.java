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
import org.apache.doris.nereids.rules.expression.rules.FoldConstantRule;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

import java.util.Map;
import java.util.Set;

/**
 * Eliminate mark join.
 */
public class EliminateMarkJoin extends OneRewriteRuleFactory {

    @Override
    public Rule build() {
        return logicalFilter(logicalJoin().when(LogicalJoin::isMarkJoin)).then(filter -> {
            if (canFilterNull(filter.getConjuncts())) {
                return filter.withChildren(eliminateMarkJoin(filter.child()));
            } else {
                return null;
            }
        }).toRule(RuleType.ELIMINATE_MARK_JOIN);
    }

    private boolean canFilterNull(Set<Expression> predicates) {
        // the idea is replacing mark join slot to null literal and run FoldConstant rule
        // if the return value is false or null, we can safely change the mark conjunct to hash conjunct
        NullLiteral nullLiteral = new NullLiteral(BooleanType.INSTANCE);
        for (Expression predicate : predicates) {
            Map<Expression, Expression> replaceMap = Maps.newHashMap();
            predicate.getInputSlots().forEach(slot -> replaceMap.put(slot, nullLiteral));
            Expression evalExpr = FoldConstantRule.INSTANCE.rewrite(
                    ExpressionUtils.replace(predicate, replaceMap),
                    new ExpressionRewriteContext(null));
            if (nullLiteral.equals(evalExpr) || BooleanLiteral.FALSE.equals(evalExpr)) {
                return true;
            }
        }
        return false;
    }

    private LogicalJoin<Plan, Plan> eliminateMarkJoin(LogicalJoin<Plan, Plan> join) {
        ImmutableList.Builder<Expression> newHashConjuncts = ImmutableList.builder();
        newHashConjuncts.addAll(join.getHashJoinConjuncts());
        newHashConjuncts.addAll(join.getMarkJoinConjuncts());
        return join.withJoinConjuncts(newHashConjuncts.build(), join.getOtherJoinConjuncts(),
                ExpressionUtils.EMPTY_CONDITION);
    }
}
