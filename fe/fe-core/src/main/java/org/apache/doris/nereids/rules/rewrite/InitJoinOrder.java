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
import org.apache.doris.nereids.trees.plans.AbstractPlan;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.qe.ConnectContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Due to the limitation on the data size in the memo, when optimizing large SQL queries, once this
 * limitation is triggered, some subtrees of the plan tree may not undergo optimization. Therefore,
 * we need to set a reasonably good initial join order before optimizing the plan tree.
 */
public class InitJoinOrder extends OneRewriteRuleFactory {
    private static final Logger LOG = LoggerFactory.getLogger(InitJoinOrder.class);

    @Override
    public Rule build() {
        return logicalJoin()
                .whenNot(LogicalJoin::isMarkJoin)
                .when(join -> join.getMutableState("swapJoinChildren") == null)
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
        JoinType swapType = join.getJoinType().swap();
        if (swapType == null) {
            return null;
        }
        AbstractPlan left = (AbstractPlan) join.left();
        AbstractPlan right = (AbstractPlan) join.right();
        if (left.getStats() == null || right.getStats() == null) {
            if (ConnectContext.get().getSessionVariable().feDebug) {
                throw new RuntimeException("missing stats");
            }
            LOG.warn("missing stats: {}", join.treeString());
            return null;
        }
        if (left.getStats().getRowCount() < right.getStats().getRowCount() * 5) {
            join = (LogicalJoin<? extends Plan, ? extends Plan>) join.withChildren(right, left);
            if (join.getJoinType() != swapType) {
                join = join.withJoinType(swapType);
            }
            join.setMutableState("swapJoinChildren", true);
            return join;
        }
        return null;
    }

}
