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
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;


/**
 * Rule for change inner join left associative to right.
 */
public class JoinLAsscom extends OneExplorationRuleFactory {
    /*
     *        topJoin                newTopJoin
     *        /     \                 /     \
     *   bottomJoin  C   -->  newBottomJoin  B
     *    /    \                  /    \
     *   A      B                A      C
     */
    @Override
    public Rule<Plan> build() {
        return innerLogicalJoin(innerLogicalJoin(), any()).then(topJoin -> {
            LogicalJoin bottomJoin = topJoin.left();

            Plan c = topJoin.right();
            Plan a = bottomJoin.left();
            Plan b = bottomJoin.right();

            LogicalJoin newBottomJoin =
                    new LogicalJoin(bottomJoin.getJoinType(), bottomJoin.getOnClause(), a, c);
            LogicalJoin newTopJoin =
                    new LogicalJoin(bottomJoin.getJoinType(), topJoin.getOnClause(), newBottomJoin, b);
            return newTopJoin;
        }).toRule(RuleType.LOGICAL_JOIN_L_ASSCOM);
    }
}
