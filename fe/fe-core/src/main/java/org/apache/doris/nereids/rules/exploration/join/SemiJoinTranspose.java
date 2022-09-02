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

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.exploration.OneExplorationRuleFactory;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;

import com.google.common.collect.ImmutableSet;

import java.util.Set;

/**
 * Rule for SemiJoinTranspose.
 */
public class SemiJoinTranspose extends OneExplorationRuleFactory {

    public static Set<Pair<JoinType, JoinType>> typeSet = ImmutableSet.of(
            Pair.of(JoinType.LEFT_SEMI_JOIN, JoinType.LEFT_SEMI_JOIN),
            Pair.of(JoinType.LEFT_ANTI_JOIN, JoinType.LEFT_ANTI_JOIN),
            Pair.of(JoinType.LEFT_SEMI_JOIN, JoinType.LEFT_ANTI_JOIN),
            Pair.of(JoinType.LEFT_ANTI_JOIN, JoinType.LEFT_SEMI_JOIN));

    /*
     *      topJoin                newTopJoin
     *      /     \                 /     \
     * bottomJoin  C   -->  newBottomJoin  B
     *  /    \                  /    \
     * A      B                A      C
     */
    @Override
    public Rule build() {
        return logicalJoin(logicalJoin(), group()).when(this::check).then(topJoin -> {
            LogicalJoin<GroupPlan, GroupPlan> bottomJoin = topJoin.left();
            GroupPlan a = bottomJoin.left();
            GroupPlan b = bottomJoin.right();
            GroupPlan c = topJoin.right();


            LogicalJoin<GroupPlan, GroupPlan> newBottomJoin = new LogicalJoin<>(topJoin.getJoinType(),
                    topJoin.getHashJoinConjuncts(), topJoin.getOtherJoinCondition(), a, c);
            LogicalJoin<LogicalJoin<GroupPlan, GroupPlan>, GroupPlan> newTopJoin = new LogicalJoin<>(
                    bottomJoin.getJoinType(), bottomJoin.getHashJoinConjuncts(), bottomJoin.getOtherJoinCondition(),
                    newBottomJoin, b);

            return newTopJoin;
        }).toRule(RuleType.LOGICAL_JOIN_L_ASSCOM);
    }

    private boolean check(LogicalJoin<LogicalJoin<GroupPlan, GroupPlan>, GroupPlan> topJoin) {
        return typeSet.contains(Pair.of(topJoin.getJoinType(), topJoin.left().getJoinType()));
    }
}
