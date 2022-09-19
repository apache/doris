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
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;

import com.google.common.collect.ImmutableSet;

import java.util.Set;

/**
 * Rule for change inner join LAsscom (associative and commutive).
 */
public class OuterJoinLAsscom extends OneExplorationRuleFactory {
    public static final OuterJoinLAsscom INSTANCE = new OuterJoinLAsscom();

    // Pair<bottomJoin, topJoin>
    // newBottomJoin Type = topJoin Type, newTopJoin Type = bottomJoin Type
    public static Set<Pair<JoinType, JoinType>> VALID_TYPE_PAIR_SET = ImmutableSet.of(
            Pair.of(JoinType.LEFT_OUTER_JOIN, JoinType.INNER_JOIN),
            Pair.of(JoinType.INNER_JOIN, JoinType.LEFT_OUTER_JOIN),
            Pair.of(JoinType.LEFT_OUTER_JOIN, JoinType.LEFT_OUTER_JOIN));

    /*
     *      topJoin                newTopJoin
     *      /     \                 /     \
     * bottomJoin  C   -->  newBottomJoin  B
     *  /    \                  /    \
     * A      B                A      C
     */
    @Override
    public Rule build() {
        return logicalJoin(logicalJoin(), group())
                .when(topJoin -> check(topJoin, topJoin.left()))
                .when(join -> VALID_TYPE_PAIR_SET.contains(Pair.of(join.left().getJoinType(), join.getJoinType())))
                .then(topJoin -> {
                    JoinLAsscomHelper helper = new JoinLAsscomHelper(topJoin, topJoin.left());
                    return helper.newTopJoin();
                }).toRule(RuleType.LOGICAL_OUTER_JOIN_LASSCOM);
    }

    /**
     * check.
     */
    public static boolean check(LogicalJoin<? extends Plan, GroupPlan> topJoin,
            LogicalJoin<GroupPlan, GroupPlan> bottomJoin) {
        // hasCommute will cause to lack of OuterJoinAssocRule:Left
        return !topJoin.getJoinReorderContext().hasLAsscom()
                && !topJoin.getJoinReorderContext().hasLeftAssociate()
                && !topJoin.getJoinReorderContext().hasRightAssociate()
                && !topJoin.getJoinReorderContext().hasExchange()
                && !bottomJoin.getJoinReorderContext().hasCommute();
    }
}
