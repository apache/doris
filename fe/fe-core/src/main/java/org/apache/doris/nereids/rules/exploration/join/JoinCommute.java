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
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.util.Utils;

import java.util.List;

/**
 * Join Commute
 */
@Developing
public class JoinCommute extends OneExplorationRuleFactory {

    public static final JoinCommute OUTER_LEFT_DEEP = new JoinCommute(SwapType.LEFT_DEEP);
    public static final JoinCommute OUTER_ZIG_ZAG = new JoinCommute(SwapType.ZIG_ZAG);
    public static final JoinCommute OUTER_BUSHY = new JoinCommute(SwapType.BUSHY);

    private final SwapType swapType;

    public JoinCommute(SwapType swapType) {
        this.swapType = swapType;
    }

    enum SwapType {
        LEFT_DEEP, ZIG_ZAG, BUSHY
    }

    @Override
    public Rule build() {
        return innerLogicalJoin().when(this::check).then(join -> {
            LogicalJoin<GroupPlan, GroupPlan> newJoin = new LogicalJoin<>(
                    join.getJoinType(),
                    join.getHashJoinConjuncts(),
                    join.getOtherJoinCondition(),
                    join.right(), join.left(),
                    join.getJoinReorderContext());
            newJoin.getJoinReorderContext().setHasCommute(true);
            if (swapType == SwapType.ZIG_ZAG && isNotBottomJoin(join)) {
                newJoin.getJoinReorderContext().setHasCommuteZigZag(true);
            }

            return newJoin;
        }).toRule(RuleType.LOGICAL_JOIN_COMMUTATIVE);
    }

    private boolean check(LogicalJoin<GroupPlan, GroupPlan> join) {
        if (swapType == SwapType.LEFT_DEEP && isNotBottomJoin(join)) {
            return false;
        }

        return !join.getJoinReorderContext().isHasCommute() && !join.getJoinReorderContext().isHasExchange();
    }

    private boolean isNotBottomJoin(LogicalJoin<GroupPlan, GroupPlan> join) {
        // TODO: tmp way to judge bottomJoin
        return containJoin(join.left()) || containJoin(join.right());
    }

    private boolean containJoin(GroupPlan groupPlan) {
        List<SlotReference> output = Utils.getOutputSlotReference(groupPlan);
        return output.stream().map(SlotReference::getQualifier).allMatch(output.get(0).getQualifier()::equals);
    }
}
