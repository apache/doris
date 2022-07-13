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
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;


/**
 * Rule for busy-tree, exchange the children node.
 */
public class JoinExchange extends OneExplorationRuleFactory {
    /*
     *        topJoin                      newTopJoin
     *        /      \                      /      \
     *   leftJoin  rightJoin   -->   newLeftJoin newRightJoin
     *    /    \    /    \            /    \        /    \
     *   A      B  C      D          A      C      B      D
     */
    @Override
    public Rule<Plan> build() {
        return innerLogicalJoin(innerLogicalJoin(), innerLogicalJoin()).then(topJoin -> {
            LogicalJoin<GroupPlan, GroupPlan> leftJoin = topJoin.left();
            LogicalJoin<GroupPlan, GroupPlan> rightJoin = topJoin.right();

            GroupPlan a = leftJoin.left();
            GroupPlan b = leftJoin.right();
            GroupPlan c = rightJoin.left();
            GroupPlan d = rightJoin.right();

            Plan newLeftJoin = new LogicalJoin(leftJoin.getJoinType(), leftJoin.getCondition(), a, c);
            Plan newRightJoin = new LogicalJoin(rightJoin.getJoinType(), rightJoin.getCondition(), b, d);
            return new LogicalJoin(topJoin.getJoinType(), topJoin.getCondition(), newLeftJoin, newRightJoin);
        }).toRule(RuleType.LOGICAL_JOIN_EXCHANGE);
    }
}
