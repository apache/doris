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
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;

/**
 * rule factory for exchange inner join's children.
 */
public class JoinCommutative extends OneExplorationRuleFactory {
    private boolean justApplyInnerOuterCrossJoin = false;

    private final SwapType swapType;

    /**
     * If param is true, just apply rule in inner/full-outer/cross join.
     */
    public JoinCommutative(boolean justApplyInnerOuterCrossJoin) {
        this.justApplyInnerOuterCrossJoin = justApplyInnerOuterCrossJoin;
        this.swapType = SwapType.ALL;
    }

    public JoinCommutative(boolean justApplyInnerOuterCrossJoin, SwapType swapType) {
        this.justApplyInnerOuterCrossJoin = justApplyInnerOuterCrossJoin;
        this.swapType = swapType;
    }

    enum SwapType {
        BOTTOM_JOIN, ZIG_ZAG, ALL
    }

    @Override
    public Rule build() {
        return innerLogicalJoin().then(join -> new LogicalJoin(
                join.getJoinType().swap(),
                join.getCondition(),
                join.right(),
                join.left())
        ).toRule(RuleType.LOGICAL_JOIN_COMMUTATIVE);
    }
}
