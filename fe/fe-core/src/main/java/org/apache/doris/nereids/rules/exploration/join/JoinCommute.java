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
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.scalar.BitmapContains;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TRuntimeFilterType;

import java.util.List;

/**
 * Join Commute
 */
public class JoinCommute extends OneExplorationRuleFactory {

    public static final JoinCommute LEFT_DEEP = new JoinCommute(SwapType.LEFT_DEEP, false);
    public static final JoinCommute LEFT_ZIG_ZAG = new JoinCommute(SwapType.LEFT_ZIG_ZAG, false);
    public static final JoinCommute ZIG_ZAG = new JoinCommute(SwapType.ZIG_ZAG, false);
    public static final JoinCommute BUSHY = new JoinCommute(SwapType.BUSHY, false);
    public static final JoinCommute NON_INNER = new JoinCommute(SwapType.BUSHY, true);

    private final SwapType swapType;
    private final boolean justNonInner;

    public JoinCommute(SwapType swapType, boolean justNonInner) {
        this.swapType = swapType;
        this.justNonInner = justNonInner;
    }

    @Override
    public Rule build() {
        return logicalJoin()
                .when(join -> !justNonInner || !join.getJoinType().isInnerJoin())
                .when(join -> checkReorder(join))
                .when(join -> check(swapType, join))
                .whenNot(LogicalJoin::hasJoinHint)
                .whenNot(join -> joinOrderMatchBitmapRuntimeFilterOrder(join))
                .whenNot(LogicalJoin::isMarkJoin)
                .then(join -> {
                    LogicalJoin<Plan, Plan> newJoin = join.withTypeChildren(join.getJoinType().swap(),
                            join.right(), join.left());
                    newJoin.getJoinReorderContext().copyFrom(join.getJoinReorderContext());
                    newJoin.getJoinReorderContext().setHasCommute(true);
                    if (swapType == SwapType.ZIG_ZAG && isNotBottomJoin(join)) {
                        newJoin.getJoinReorderContext().setHasCommuteZigZag(true);
                    }

                    return newJoin;
                }).toRule(RuleType.LOGICAL_JOIN_COMMUTE);
    }

    enum SwapType {
        LEFT_DEEP, ZIG_ZAG, BUSHY,
        LEFT_ZIG_ZAG
    }

    /**
     * Check if commutative law needs to be enforced.
     */
    public static boolean check(SwapType swapType, LogicalJoin<GroupPlan, GroupPlan> join) {
        if (swapType == SwapType.LEFT_DEEP && isNotBottomJoin(join)) {
            return false;
        }

        if (join.getJoinType().isNullAwareLeftAntiJoin()) {
            return false;
        }

        if (swapType == SwapType.LEFT_ZIG_ZAG) {
            double leftRows = join.left().getGroup().getStatistics().getRowCount();
            double rightRows = join.right().getGroup().getStatistics().getRowCount();
            return leftRows <= rightRows && isZigZagJoin(join);
        }

        return true;
    }

    private boolean checkReorder(LogicalJoin<GroupPlan, GroupPlan> join) {
        return !join.getJoinReorderContext().hasCommute()
                && !join.getJoinReorderContext().hasExchange();
    }

    public static boolean isNotBottomJoin(LogicalJoin<GroupPlan, GroupPlan> join) {
        // TODO: tmp way to judge bottomJoin
        return containJoin(join.left()) || containJoin(join.right());
    }

    public static boolean isZigZagJoin(LogicalJoin<GroupPlan, GroupPlan> join) {
        return !containJoin(join.left()) || !containJoin(join.right());
    }

    private static boolean containJoin(GroupPlan groupPlan) {
        // TODO: tmp way to judge containJoin
        List<Slot> output = groupPlan.getOutput();
        return !output.stream().map(Slot::getQualifier).allMatch(output.get(0).getQualifier()::equals);
    }

    /**
     * bitmap runtime filter requires bitmap column on right.
     */
    private boolean joinOrderMatchBitmapRuntimeFilterOrder(LogicalJoin<GroupPlan, GroupPlan> join) {
        if (!ConnectContext.get().getSessionVariable().isRuntimeFilterTypeEnabled(TRuntimeFilterType.BITMAP)) {
            return false;
        }
        for (Expression expr : join.getOtherJoinConjuncts()) {
            if (expr instanceof Not) {
                expr = expr.child(0);
            }
            if (expr instanceof BitmapContains) {
                BitmapContains bitmapContains = (BitmapContains) expr;
                return (join.right().getOutputSet().containsAll(bitmapContains.child(0).getInputSlots())
                        && join.left().getOutputSet().containsAll(bitmapContains.child(1).getInputSlots()));
            }
        }
        return false;
    }
}
