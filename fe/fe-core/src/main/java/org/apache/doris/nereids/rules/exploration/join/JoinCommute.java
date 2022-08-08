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
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;

/**
 * rule factory for exchange inner join's children.
 */
@Developing
public class JoinCommute extends OneExplorationRuleFactory {

    public static final JoinCommute SWAP_OUTER_COMMUTE_BOTTOM_JOIN = new JoinCommute(true, SwapType.BOTTOM_JOIN);

    private final SwapType swapType;
    private final boolean swapOuter;

    public JoinCommute(boolean swapOuter) {
        this.swapOuter = swapOuter;
        this.swapType = SwapType.ALL;
    }

    public JoinCommute(boolean swapOuter, SwapType swapType) {
        this.swapOuter = swapOuter;
        this.swapType = swapType;
    }

    enum SwapType {
        BOTTOM_JOIN, ZIG_ZAG, ALL
    }

    @Override
    public Rule build() {
        return innerLogicalJoin(any(), any()).then(join -> {
            if (!check(join)) {
                return null;
            }
            boolean isBottomJoin = isBottomJoin(join);
            if (swapType == SwapType.BOTTOM_JOIN && !isBottomJoin) {
                return null;
            }

            LogicalJoin newJoin = new LogicalJoin(
                    join.getJoinType(),
                    join.getCondition(),
                    join.right(), join.left(),
                    join.getJoinReorderContext()
            );
            newJoin.getJoinReorderContext().setHasCommute(true);
            if (swapType == SwapType.ZIG_ZAG && !isBottomJoin) {
                newJoin.getJoinReorderContext().setHasCommuteZigZag(true);
            }

            return newJoin;
        }).toRule(RuleType.LOGICAL_JOIN_COMMUTATIVE);
    }


    private boolean check(LogicalJoin join) {
        if (join.getJoinReorderContext().hasCommute() || join.getJoinReorderContext().hasExchange()) {
            return false;
        }
        return true;
    }

    private boolean isBottomJoin(LogicalJoin join) {
        // TODO: wait for tree model of pattern-match.
        if (join.left() instanceof LogicalProject) {
            LogicalProject project = (LogicalProject) join.left();
            if (project.child() instanceof LogicalJoin) {
                return false;
            }
        }
        if (join.right() instanceof LogicalProject) {
            LogicalProject project = (LogicalProject) join.left();
            if (project.child() instanceof LogicalJoin) {
                return false;
            }
        }
        if (join.left() instanceof LogicalJoin || join.right() instanceof LogicalJoin) {
            return false;
        }
        return true;
    }
}
