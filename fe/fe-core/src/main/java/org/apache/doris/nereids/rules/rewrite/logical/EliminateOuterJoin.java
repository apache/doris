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
import org.apache.doris.nereids.rules.expression.rewrite.ExpressionRewriteContext;
import org.apache.doris.nereids.rules.expression.rewrite.rules.FoldConstantRule;
import org.apache.doris.nereids.rules.rewrite.OneRewriteRuleFactory;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;

/**
 * Eliminate outer join.
 */
public class EliminateOuterJoin extends OneRewriteRuleFactory {

    @Override
    public Rule build() {
        return logicalFilter(
                    logicalJoin().when(join -> join.getJoinType().isOuterJoin())
                ).then(filter -> {
                    LogicalJoin<GroupPlan, GroupPlan> join = filter.child();
                    List<Expression> conjuncts = filter.getConjuncts();
                    List<Expression> leftPredicates = ExpressionUtils.extractCoveredConjunction(conjuncts,
                            join.left().getOutputSet());
                    List<Expression> rightPredicates = ExpressionUtils.extractCoveredConjunction(conjuncts,
                            join.right().getOutputSet());
                    boolean canFilterLeftNull = canFilterNull(leftPredicates);
                    boolean canFilterRightNull = canFilterNull(rightPredicates);
                    JoinType newJoinType = tryEliminateOuterJoin(join.getJoinType(), canFilterLeftNull,
                            canFilterRightNull);
                    if (newJoinType == join.getJoinType()) {
                        return filter;
                    } else {
                        return filter.withChildren(join.withJoinType(newJoinType));
                    }
                }).toRule(RuleType.ELIMINATE_OUTER_JOIN);
    }

    private JoinType tryEliminateOuterJoin(JoinType joinType, boolean canFilterLeftNull, boolean canFilterRightNull) {
        if (joinType.isRightOuterJoin() && canFilterLeftNull) {
            return JoinType.INNER_JOIN;
        }
        if (joinType.isLeftOuterJoin() && canFilterRightNull) {
            return JoinType.INNER_JOIN;
        }
        if (joinType.isFullOuterJoin() && canFilterLeftNull && canFilterRightNull) {
            return JoinType.INNER_JOIN;
        }
        if (joinType.isFullOuterJoin() && canFilterLeftNull) {
            return JoinType.LEFT_OUTER_JOIN;
        }
        if (joinType.isFullOuterJoin() && canFilterRightNull) {
            return JoinType.RIGHT_OUTER_JOIN;
        }
        return joinType;
    }

    private boolean canFilterNull(List<Expression> predicates) {
        Literal nullLiteral = Literal.of(null);
        for (Expression predicate : predicates) {
            Map<Expression, Expression> replaceMap = Maps.newHashMap();
            predicate.getInputSlots().forEach(slot -> replaceMap.put(slot, nullLiteral));
            Expression evalExpr = FoldConstantRule.INSTANCE.rewrite(ExpressionUtils.replace(predicate, replaceMap),
                    new ExpressionRewriteContext(null));
            if (nullLiteral.equals(evalExpr) || BooleanLiteral.FALSE.equals(evalExpr)) {
                return true;
            }
        }
        return false;
    }
}
