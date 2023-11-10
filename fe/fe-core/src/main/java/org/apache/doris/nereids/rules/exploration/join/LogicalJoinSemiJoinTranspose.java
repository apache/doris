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

package org.apache.doris.nereids.rules.exploration.join;

import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.exploration.ExplorationRuleFactory;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * LogicalJoin(SemiJoin(A, B), C) -> SemiJoin(LogicalJoin(A, C), B)
 * LogicalJoin(A, SemiJoin(B, C)) -> SemiJoin(LogicalJoin(A, B), C)
 */
public class LogicalJoinSemiJoinTranspose implements ExplorationRuleFactory {

    public static final LogicalJoinSemiJoinTranspose INSTANCE = new LogicalJoinSemiJoinTranspose();

    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                logicalJoin(logicalJoin(), group())
                        .when(topJoin -> (topJoin.left().getJoinType().isLeftSemiOrAntiJoin()
                                && (topJoin.getJoinType().isInnerJoin()
                                || topJoin.getJoinType().isLeftOuterJoin())))
                        .whenNot(topJoin -> topJoin.hasJoinHint() || topJoin.left().hasJoinHint()
                                || topJoin.left().isMarkJoin())
                        .whenNot(LogicalJoin::isMarkJoin)
                        .then(topJoin -> {
                            LogicalJoin<GroupPlan, GroupPlan> bottomJoin = topJoin.left();
                            GroupPlan a = bottomJoin.left();
                            GroupPlan b = bottomJoin.right();
                            GroupPlan c = topJoin.right();

                            Plan newBottomJoin = topJoin.withChildrenNoContext(a, c);
                            return bottomJoin.withChildrenNoContext(newBottomJoin, b);
                        }).toRule(RuleType.LOGICAL_JOIN_LOGICAL_SEMI_JOIN_TRANSPOSE_LEFT),

                logicalJoin(group(), logicalJoin())
                        .when(topJoin -> (topJoin.right().getJoinType().isLeftSemiOrAntiJoin()
                                && (topJoin.getJoinType().isInnerJoin()
                                || topJoin.getJoinType().isRightOuterJoin())))
                        .whenNot(topJoin -> topJoin.hasJoinHint() || topJoin.right().hasJoinHint()
                                || topJoin.right().isMarkJoin())
                        .whenNot(LogicalJoin::isMarkJoin)
                        .then(topJoin -> {
                            LogicalJoin<GroupPlan, GroupPlan> bottomJoin = topJoin.right();
                            GroupPlan a = topJoin.left();
                            GroupPlan b = bottomJoin.left();
                            GroupPlan c = bottomJoin.right();

                            Plan newBottomJoin = topJoin.withChildrenNoContext(a, b);
                            return bottomJoin.withChildrenNoContext(newBottomJoin, c);
                        }).toRule(RuleType.LOGICAL_JOIN_LOGICAL_SEMI_JOIN_TRANSPOSE_RIGHT)
        );
    }
}
