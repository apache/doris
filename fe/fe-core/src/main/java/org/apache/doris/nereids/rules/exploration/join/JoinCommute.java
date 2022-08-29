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

import org.apache.doris.nereids.annotation.Developing;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.exploration.OneExplorationRuleFactory;
import org.apache.doris.nereids.rules.exploration.join.JoinCommuteHelper.SwapType;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;

/**
 * Join Commute
 */
@Developing
public class JoinCommute extends OneExplorationRuleFactory {

    public static final JoinCommute SWAP_OUTER_COMMUTE_BOTTOM_JOIN = new JoinCommute(true, SwapType.BOTTOM_JOIN);
    public static final JoinCommute SWAP_OUTER_SWAP_ZIG_ZAG = new JoinCommute(true, SwapType.ZIG_ZAG);

    private final boolean swapOuter;
    private final SwapType swapType;

    public JoinCommute(boolean swapOuter) {
        this.swapOuter = swapOuter;
        this.swapType = SwapType.ALL;
    }

    public JoinCommute(boolean swapOuter, SwapType swapType) {
        this.swapOuter = swapOuter;
        this.swapType = swapType;
    }

    @Override
    public Rule build() {
        return innerLogicalJoin().when(JoinCommuteHelper::check).then(join -> {
            // TODO: add project for mapping column output.
            // List<NamedExpression> newOutput = new ArrayList<>(join.getOutput());
            LogicalJoin<GroupPlan, GroupPlan> newJoin = new LogicalJoin<>(
                    join.getJoinType(),
                    join.getHashJoinConjuncts(),
                    join.getOtherJoinCondition(),
                    join.right(), join.left(),
                    join.getJoinReorderContext());
            newJoin.getJoinReorderContext().setHasCommute(true);
            // if (swapType == SwapType.ZIG_ZAG && !isBottomJoin(join)) {
            //     newJoin.getJoinReorderContext().setHasCommuteZigZag(true);
            // }

            // LogicalProject<LogicalJoin> project = new LogicalProject<>(newOutput, newJoin);
            return newJoin;
        }).toRule(RuleType.LOGICAL_JOIN_COMMUTATIVE);
    }
}
