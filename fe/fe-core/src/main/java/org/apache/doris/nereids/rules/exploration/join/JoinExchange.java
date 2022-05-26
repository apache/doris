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

import org.apache.doris.nereids.operators.plans.logical.LogicalJoin;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.exploration.OneExplorationRuleFactory;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalBinary;


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
            LogicalBinary<LogicalJoin, Plan, Plan> leftJoin = topJoin.left();
            LogicalBinary<LogicalJoin, Plan, Plan> rightJoin = topJoin.right();

            Plan a = leftJoin.left();
            Plan b = leftJoin.right();
            Plan c = rightJoin.left();
            Plan d = rightJoin.right();

            Plan newLeftJoin = plan(
                    new LogicalJoin(leftJoin.op.getJoinType(), leftJoin.op.getOnClause()),
                    a, c
            );
            Plan newRightJoin = plan(
                    new LogicalJoin(rightJoin.op.getJoinType(), rightJoin.op.getOnClause()),
                    b, d
            );
            Plan newTopJoin = plan(
                    new LogicalJoin(topJoin.op.getJoinType(), topJoin.op.getOnClause()),
                    newLeftJoin, newRightJoin
            );
            return newTopJoin;
        }).toRule(RuleType.LOGICAL_JOIN_EXCHANGE);
    }
}
