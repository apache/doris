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
import org.apache.doris.nereids.rules.exploration.CBOUtils;
import org.apache.doris.nereids.rules.exploration.ExplorationRuleFactory;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.util.JoinUtils;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * LogicalJoin(SemiJoin(A, B), C) -> SemiJoin(LogicalJoin(A, C), B)
 */
public class LogicalJoinSemiJoinTransposeProject implements ExplorationRuleFactory {

    public static final LogicalJoinSemiJoinTransposeProject INSTANCE = new LogicalJoinSemiJoinTransposeProject();

    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                logicalJoin(logicalProject(logicalJoin()), group())
                        .when(topJoin -> (topJoin.left().child().getJoinType().isLeftSemiOrAntiJoin()
                                && (topJoin.getJoinType().isInnerJoin()
                                || topJoin.getJoinType().isLeftOuterJoin())))
                        .whenNot(topJoin -> topJoin.hasDistributeHint()
                                || topJoin.left().child().hasDistributeHint())
                        .when(join -> join.left().isAllSlots())
                        .then(topJoin -> {
                            LogicalJoin<GroupPlan, GroupPlan> bottomJoin = topJoin.left().child();
                            if (!JoinUtils.checkReorderPrecondition(topJoin, bottomJoin)) {
                                return null;
                            }
                            GroupPlan a = bottomJoin.left();
                            GroupPlan b = bottomJoin.right();
                            GroupPlan c = topJoin.right();

                            // Discard this project, because it is useless.
                            Plan newBottomJoin = topJoin.withChildrenNoContext(a, c, null);
                            Plan newTopJoin = bottomJoin.withChildrenNoContext(newBottomJoin, b, null);
                            return CBOUtils.projectOrSelf(ImmutableList.copyOf(topJoin.getOutput()),
                                    newTopJoin);
                        }).toRule(RuleType.LOGICAL_JOIN_LOGICAL_SEMI_JOIN_TRANSPOSE_LEFT_PROJECT),

                logicalJoin(group(), logicalProject(logicalJoin()))
                        .when(topJoin -> (topJoin.right().child().getJoinType().isLeftSemiOrAntiJoin()
                                && (topJoin.getJoinType().isInnerJoin()
                                || topJoin.getJoinType().isRightOuterJoin())))
                        .whenNot(topJoin -> topJoin.hasDistributeHint()
                                || topJoin.right().child().hasDistributeHint())
                        .when(join -> join.right().isAllSlots())
                        .then(topJoin -> {
                            LogicalJoin<GroupPlan, GroupPlan> bottomJoin = topJoin.right().child();
                            if (!JoinUtils.checkReorderPrecondition(topJoin, bottomJoin)) {
                                return null;
                            }
                            GroupPlan a = topJoin.left();
                            GroupPlan b = bottomJoin.left();
                            GroupPlan c = bottomJoin.right();

                            // Discard this project, because it is useless.
                            Plan newBottomJoin = topJoin.withChildrenNoContext(a, b, null);
                            Plan newTopJoin = bottomJoin.withChildrenNoContext(newBottomJoin, c, null);
                            return CBOUtils.projectOrSelf(ImmutableList.copyOf(topJoin.getOutput()),
                                    newTopJoin);
                        }).toRule(RuleType.LOGICAL_JOIN_LOGICAL_SEMI_JOIN_TRANSPOSE_RIGHT_PROJECT)
        );
    }
}
