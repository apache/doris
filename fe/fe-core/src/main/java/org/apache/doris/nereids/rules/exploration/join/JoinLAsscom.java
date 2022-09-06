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
import org.apache.doris.nereids.annotation.Developing;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.exploration.OneExplorationRuleFactory;
import org.apache.doris.nereids.rules.exploration.join.JoinReorderCommon.Type;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;

import java.util.function.Predicate;

/**
 * Rule for change inner join LAsscom (associative and commutive).
 */
public class JoinLAsscom extends OneExplorationRuleFactory {
    // for inner-inner
    public static final JoinLAsscom INNER = new JoinLAsscom(Type.INNER);
    // for inner-leftOuter or leftOuter-leftOuter
    public static final JoinLAsscom OUTER = new JoinLAsscom(Type.OUTER);

    private final Predicate<LogicalJoin<LogicalJoin<GroupPlan, GroupPlan>, GroupPlan>> typeChecker;

    private final Type type;

    /**
     * Specify join type.
     */
    public JoinLAsscom(Type type) {
        this.type = type;
        if (type == Type.INNER) {
            typeChecker = join -> join.getJoinType().isInnerJoin() && join.left().getJoinType().isInnerJoin();
        } else {
            typeChecker = join -> JoinLAsscomHelper.outerSet.contains(
                    Pair.of(join.left().getJoinType(), join.getJoinType()));
        }
    }

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
                .when(topJoin -> JoinLAsscomHelper.check(type, topJoin, topJoin.left()))
                .when(typeChecker)
                .then(topJoin -> {
                    JoinLAsscomHelper helper = new JoinLAsscomHelper(topJoin, topJoin.left());
                    if (!helper.initJoinOnCondition()) {
                        return null;
                    }
                    return helper.newTopJoin();
                }).toRule(RuleType.LOGICAL_JOIN_L_ASSCOM);
    }
}
