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
 * Join Commute
 */
public class JoinCommute extends OneExplorationRuleFactory {

    public static final JoinCommute LEFT_DEEP = new JoinCommute(SwapType.LEFT_DEEP);
    public static final JoinCommute ZIG_ZAG = new JoinCommute(SwapType.ZIG_ZAG);
    public static final JoinCommute BUSHY = new JoinCommute(SwapType.BUSHY);

    private final SwapType swapType;

    public JoinCommute(SwapType swapType) {
        this.swapType = swapType;
    }

    @Override
    public Rule build() {
        return logicalJoin()
                .when(join -> JoinCommuteHelper.check(swapType, join))
                .then(join -> {
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
