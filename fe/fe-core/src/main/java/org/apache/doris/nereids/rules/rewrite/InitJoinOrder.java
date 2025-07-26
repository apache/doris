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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.rewrite.StatsDerive.DeriveContext;
import org.apache.doris.nereids.trees.plans.AbstractPlan;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Due to the limitation on the data size in the memo, when optimizing large SQL queries, once this
 * limitation is triggered, some subtrees of the plan tree may not undergo optimization. Therefore,
 * we need to set a reasonably good initial join order before optimizing the plan tree.
 */
public class InitJoinOrder extends OneRewriteRuleFactory {
    private static final Logger LOG = LoggerFactory.getLogger(InitJoinOrder.class);
    private static final double SWAP_THRESHOLD = 0.1;
    private final StatsDerive derive = new StatsDerive(false);

    @Override
    public Rule build() {
        return logicalJoin()
                .whenNot(LogicalJoin::isMarkJoin)
                .thenApply(ctx -> {
                    if (ctx.statementContext.getConnectContext().getSessionVariable().isDisableJoinReorder()
                            || !ctx.statementContext.getConnectContext().getSessionVariable().enableInitJoinOrder
                            || ctx.cascadesContext.isLeadingDisableJoinReorder()
                            || ((LogicalJoin<?, ?>) ctx.root).isLeadingJoin()) {
                        return null;
                    }
                    LogicalJoin<? extends Plan, ? extends Plan> join = (LogicalJoin<?, ?>) ctx.root;
                    return swapJoinChildrenIfNeed(join);
                })
                .toRule(RuleType.INIT_JOIN_ORDER);
    }

    private Plan swapJoinChildrenIfNeed(LogicalJoin<? extends Plan, ? extends Plan> join) {
        if (join.getJoinType().isLeftSemiOrAntiJoin()) {
            // TODO: currently, the transform rules for right semi/anti join is not complete,
            //  for example LogicalJoinSemiJoinTransposeProject (tpch 22) only works for left semi/anti join
            //  if we swap left semi/anti to right semi/anti, we lost the opportunity to optimize join order
            return null;
        }
        JoinType swapType = join.getJoinType().swap();
        if (swapType == null) {
            return null;
        }
        AbstractPlan left = (AbstractPlan) join.left();
        AbstractPlan right = (AbstractPlan) join.right();
        if (left.getStats() == null) {
            left.accept(derive, new DeriveContext());
        }
        if (right.getStats() == null) {
            right.accept(derive, new DeriveContext());
        }

        // requires "left.getStats().getRowCount() > 0" to avoid dead loop when negative row count is estimated.
        if (left.getStats().getRowCount() < right.getStats().getRowCount() * SWAP_THRESHOLD
                && left.getStats().getRowCount() > 0) {
            join = join.withTypeChildren(swapType, right, left,
                    join.getJoinReorderContext());
            return join;
        }
        return null;
    }

}
