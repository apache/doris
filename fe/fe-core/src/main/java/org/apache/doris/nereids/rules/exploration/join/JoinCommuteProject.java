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
import org.apache.doris.nereids.rules.exploration.OneExplorationRuleFactory;
import org.apache.doris.nereids.rules.exploration.join.JoinCommuteHelper.SwapType;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;

/**
 * Project-Join Commute.
 * This rule can prevent double JoinCommute cause dead-loop in Memo.
 */
public class JoinCommuteProject extends OneExplorationRuleFactory {

    public static final JoinCommuteProject LEFT_DEEP = new JoinCommuteProject(SwapType.LEFT_DEEP);
    public static final JoinCommuteProject ZIG_ZAG = new JoinCommuteProject(SwapType.ZIG_ZAG);
    public static final JoinCommuteProject BUSHY = new JoinCommuteProject(SwapType.BUSHY);

    private final SwapType swapType;

    public JoinCommuteProject(SwapType swapType) {
        this.swapType = swapType;
    }

    @Override
    public Rule build() {
        return logicalProject(logicalJoin())
                .when(project -> JoinCommuteHelper.check(swapType, project.child()))
                .then(project -> {
                    LogicalJoin<GroupPlan, GroupPlan> join = project.child();
                    // prevent this join match by JoinCommute.
                    join.getGroupExpression().get().setApplied(RuleType.LOGICAL_JOIN_COMMUTATE);
                    LogicalJoin<GroupPlan, GroupPlan> newJoin = new LogicalJoin<>(
                            join.getJoinType().swap(),
                            join.getHashJoinConjuncts(),
                            join.getOtherJoinCondition(),
                            join.right(), join.left(),
                            join.getJoinReorderContext());
                    newJoin.getJoinReorderContext().setHasCommute(true);
                    if (swapType == SwapType.ZIG_ZAG && JoinCommuteHelper.isNotBottomJoin(join)) {
                        newJoin.getJoinReorderContext().setHasCommuteZigZag(true);
                    }

                    return newJoin;
                }).toRule(RuleType.LOGICAL_JOIN_COMMUTATE);
    }
}
