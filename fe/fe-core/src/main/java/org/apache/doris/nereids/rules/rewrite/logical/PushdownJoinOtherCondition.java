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
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.util.PlanUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Set;

/**
 * Push the other join conditions in LogicalJoin to children.
 */
public class PushdownJoinOtherCondition extends OneRewriteRuleFactory {
    private static final ImmutableList<JoinType> PUSH_DOWN_LEFT_VALID_TYPE = ImmutableList.of(
            JoinType.INNER_JOIN,
            JoinType.LEFT_SEMI_JOIN,
            JoinType.RIGHT_OUTER_JOIN,
            JoinType.RIGHT_ANTI_JOIN,
            JoinType.RIGHT_SEMI_JOIN,
            JoinType.CROSS_JOIN
    );

    private static final ImmutableList<JoinType> PUSH_DOWN_RIGHT_VALID_TYPE = ImmutableList.of(
            JoinType.INNER_JOIN,
            JoinType.LEFT_OUTER_JOIN,
            JoinType.LEFT_ANTI_JOIN,
            JoinType.LEFT_SEMI_JOIN,
            JoinType.RIGHT_SEMI_JOIN,
            JoinType.CROSS_JOIN
    );

    @Override
    public Rule build() {
        return logicalJoin()
                .whenNot(join -> join.getOtherJoinConjuncts().isEmpty())
                .then(join -> {
                    List<Expression> otherJoinConjuncts = join.getOtherJoinConjuncts();
                    List<Expression> remainingOther = Lists.newArrayList();
                    List<Expression> leftConjuncts = Lists.newArrayList();
                    List<Expression> rightConjuncts = Lists.newArrayList();

                    for (Expression otherConjunct : otherJoinConjuncts) {
                        if (PUSH_DOWN_LEFT_VALID_TYPE.contains(join.getJoinType())
                                && allCoveredBy(otherConjunct, join.left().getOutputSet())) {
                            leftConjuncts.add(otherConjunct);
                        } else if (PUSH_DOWN_RIGHT_VALID_TYPE.contains(join.getJoinType())
                                && allCoveredBy(otherConjunct, join.right().getOutputSet())) {
                            rightConjuncts.add(otherConjunct);
                        } else {
                            remainingOther.add(otherConjunct);
                        }
                    }

                    if (leftConjuncts.isEmpty() && rightConjuncts.isEmpty()) {
                        return null;
                    }

                    Plan left = PlanUtils.filterOrSelf(leftConjuncts, join.left());
                    Plan right = PlanUtils.filterOrSelf(rightConjuncts, join.right());

                    return new LogicalJoin<>(join.getJoinType(), join.getHashJoinConjuncts(),
                            remainingOther, left, right);

                }).toRule(RuleType.PUSHDOWN_JOIN_OTHER_CONDITION);
    }

    private boolean allCoveredBy(Expression predicate, Set<Slot> inputSlotSet) {
        return inputSlotSet.containsAll(predicate.getInputSlots());
    }
}
