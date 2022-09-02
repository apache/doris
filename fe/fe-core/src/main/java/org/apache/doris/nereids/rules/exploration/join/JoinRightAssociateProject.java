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
import org.apache.doris.nereids.rules.exploration.join.JoinReorderCommon.Type;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;

import java.util.function.Predicate;

/**
 * Rule for inner join RightAssociate.
 */
public class JoinRightAssociateProject extends OneExplorationRuleFactory {
    public static final JoinRightAssociateProject INNER = new JoinRightAssociateProject(Type.INNER);
    public static final JoinRightAssociateProject OUTER = new JoinRightAssociateProject(Type.OUTER);

    private final Predicate<LogicalJoin<LogicalProject<LogicalJoin<GroupPlan, GroupPlan>>, GroupPlan>> typeChecker;

    /**
     * Specifiy join type.
     */
    public JoinRightAssociateProject(Type type) {
        if (type == Type.INNER) {
            typeChecker = join -> join.getJoinType().isInnerJoin() && join.left().child().getJoinType().isInnerJoin();
        } else {
            typeChecker = join -> JoinRightAssociateHelper.outerSet.contains(
                    Pair.of(join.left().child().getJoinType(), join.getJoinType()));
        }
    }

    /*
     *        topJoin                   newTopJoin
     *        /     \                   /        \
     *    project    C          newLeftProject newRightProject
     *      /            ──►          /            \
     * bottomJoin                newBottomJoin      B
     *    /   \                     /   \
     *   A     B                   A     C
     */
    @Override
    public Rule build() {
        return logicalJoin(logicalProject(logicalJoin()), group())
                .when(JoinRightAssociateHelper::check)
                .when(typeChecker)
                .then(topJoin -> {
                    JoinRightAssociateHelper helper = new JoinRightAssociateHelper(topJoin, topJoin.left().child());
                    helper.initAllProject();
                    if (!helper.initJoinOnCondition()) {
                        return null;
                    }
                    return helper.newTopJoin();
                }).toRule(RuleType.LOGICAL_JOIN_L_ASSCOM);
    }
}
