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

package org.apache.doris.nereids.rules.rewrite.logical;

import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.rewrite.OneRewriteRuleFactory;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalEmptyRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;

/**
 * case 1: inner/cross/semi join
 * before:
 *       join(inner/cross/semi)
 *     /     \
 *   Node    EmptyRelation
 *
 * after:
 *   EmptyRelation
 *
 * case 2: left/right outer join with right/left empty child
 * before:
 *       join(left outer)
 *     /     \
 *   Node    EmptyRelation
 *
 * after:
 *   Node
 *
 * case 3: left/right outer join with left/right empty child
 * before:
 *       join(left outer)
 *     /     \
 * EmptyRel  Node
 *
 * after:
 *   EmptyRel
 *
 * case 4: left/right anti join with right/left empty child
 * before:
 *       join(left anti)
 *     /     \
 *   Node    EmptyRelation
 *
 * after:
 *   Node
 *
 * case 5: left/right anti join with left/right empty child
 * before:
 *       join(left anti)
 *     /     \
 * EmptyRel  Node
 *
 * after:
 *   EmptyRel
 *
 */

public class EliminateJoinByEmptyRelation extends OneRewriteRuleFactory {

    @Override
    public Rule build() {
        return RuleType.ELIMINATE_JOIN_BY_EMPTY_RELATION.build(
                logicalJoin()
                        .when(join -> hasEmptySetChild(join))
                        .then(join -> {
                            if (isInnerSemiCrossJoin(join)) {
                                return new LogicalEmptyRelation(join.getOutput());
                            } else if (join.getJoinType() == JoinType.LEFT_OUTER_JOIN) {
                                //TODO: if right is empty relation
                                if (isLeftEmpty(join)) {
                                    return new LogicalEmptyRelation(join.getOutput());
                                }

                            } else if (join.getJoinType() == JoinType.RIGHT_OUTER_JOIN) {
                                //TODO: if left is empty relation
                                if (isRightEmpty(join)) {
                                    return new LogicalEmptyRelation(join.getOutput());
                                }
                            } else if (join.getJoinType() == JoinType.LEFT_ANTI_JOIN) {
                                if (isRightEmpty(join)) {
                                    return join.left();
                                } else if (isLeftEmpty(join)) {
                                    return new LogicalEmptyRelation(join.getOutput());
                                }
                            } else if (join.getJoinType() == JoinType.RIGHT_ANTI_JOIN) {
                                if (isLeftEmpty(join)) {
                                    return join.right();
                                } else if (isRightEmpty(join)) {
                                    return new LogicalEmptyRelation(join.getOutput());
                                }
                            }
                            return join;
                        })
        );
    }

    private boolean isInnerSemiCrossJoin(LogicalJoin join) {
        JoinType type = join.getJoinType();
        return (type == JoinType.CROSS_JOIN
                || type == JoinType.INNER_JOIN
                || type == JoinType.LEFT_SEMI_JOIN
                || type == JoinType.RIGHT_SEMI_JOIN);
    }

    private boolean hasEmptySetChild(LogicalJoin join) {
        Plan left = ((GroupPlan) join.left()).getGroup().getLogicalExpressions().get(0).getPlan();
        Plan right = ((GroupPlan) join.right()).getGroup().getLogicalExpressions().get(0).getPlan();
        return (left instanceof LogicalEmptyRelation
                || right instanceof LogicalEmptyRelation);
    }

    private boolean isLeftEmpty(LogicalJoin join) {
        Plan left = ((GroupPlan) join.left()).getGroup().getLogicalExpressions().get(0).getPlan();
        return left instanceof LogicalEmptyRelation;
    }

    private boolean isRightEmpty(LogicalJoin join) {
        Plan right = ((GroupPlan) join.right()).getGroup().getLogicalExpressions().get(0).getPlan();
        return right instanceof LogicalEmptyRelation;
    }
}
