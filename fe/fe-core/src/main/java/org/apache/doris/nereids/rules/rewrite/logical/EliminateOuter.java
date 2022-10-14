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
import org.apache.doris.nereids.trees.expressions.ComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableMap;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Eliminate outer.
 */
public class EliminateOuter extends OneRewriteRuleFactory {
    public static EliminateOuter INSTANCE = new EliminateOuter();

    // right nullable
    public static Map<JoinType, JoinType> ELIMINATE_LEFT_MAP = ImmutableMap.of(
            JoinType.LEFT_OUTER_JOIN, JoinType.INNER_JOIN,
            JoinType.FULL_OUTER_JOIN, JoinType.RIGHT_OUTER_JOIN
    );

    // left nullable
    public static Map<JoinType, JoinType> ELIMINATE_RIGHT_MAP = ImmutableMap.of(
            JoinType.RIGHT_OUTER_JOIN, JoinType.INNER_JOIN,
            JoinType.FULL_OUTER_JOIN, JoinType.LEFT_OUTER_JOIN
    );

    @Override
    public Rule build() {
        return logicalFilter(logicalJoin())
                .when(filter -> filter.child().getJoinType().isOuterJoin())
                .then(filter -> {
                    List<Expression> predicates = ExpressionUtils.extractConjunction(filter.getPredicates());
                    Set<Slot> notNullSlots = new HashSet<>();
                    for (Expression predicate : predicates) {
                        // TODO: more case.
                        if (predicate instanceof ComparisonPredicate) {
                            notNullSlots.addAll(predicate.getInputSlots());
                        }
                    }
                    LogicalJoin<GroupPlan, GroupPlan> join = filter.child();
                    JoinType joinType = join.getJoinType();
                    if (!joinType.isLeftOuterJoin() && ExpressionUtils.isIntersecting(join.left().getOutputSet(),
                            notNullSlots)) {
                        joinType = ELIMINATE_RIGHT_MAP.get(joinType);
                    }
                    if (!joinType.isRightOuterJoin() && ExpressionUtils.isIntersecting(join.right().getOutputSet(),
                            notNullSlots)) {
                        joinType = ELIMINATE_LEFT_MAP.get(joinType);
                    }

                    if (joinType == join.getJoinType()) {
                        return null;
                    }

                    return new LogicalFilter<>(filter.getPredicates(),
                            new LogicalJoin<>(joinType,
                                    join.getHashJoinConjuncts(), join.getOtherJoinConjuncts(),
                                    join.left(), join.right()));
                }).toRule(RuleType.ELIMINATE_OUTER);
    }
}
