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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.plans.logical.LogicalEmptyRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;

import java.util.List;

/**
 * Eliminate Semi/Anti Join which is FALSE or TRUE.
 */
public class EliminateSemiJoin extends OneRewriteRuleFactory {
    @Override
    public Rule build() {
        return logicalJoin()
                .whenNot(LogicalJoin::isMarkJoin)
                .when(join -> join.getJoinType().isSemiOrAntiJoin())
                .when(join -> join.getHashJoinConjuncts().isEmpty())
                .then(join -> {
                    List<Expression> otherJoinConjuncts = join.getOtherJoinConjuncts();
                    if (otherJoinConjuncts.size() == 1
                            && otherJoinConjuncts.get(0).equals(BooleanLiteral.FALSE)) {
                        switch (join.getJoinType()) {
                            case LEFT_SEMI_JOIN:
                            case RIGHT_SEMI_JOIN: {
                                return new LogicalEmptyRelation(
                                        StatementScopeIdGenerator.newRelationId(),
                                        join.getOutput());
                            }
                            case NULL_AWARE_LEFT_ANTI_JOIN:
                            case LEFT_ANTI_JOIN: {
                                return join.left();
                            }
                            case RIGHT_ANTI_JOIN: {
                                return join.right();
                            }
                            default:
                                throw new IllegalStateException("Unexpected join type: " + join.getJoinType());
                        }
                    } else {
                        /*
                         * A left semi join B on true, normally should output all rows from A
                         * A left anti join B on true, normally should output empty set
                         * but things are different if other side table is empty, examples:
                         * A left semi join B on true, if B is empty, should output empty set
                         * A left anti join B on true, if B is empty, should output all rows from A
                         * A right semi join B on true, if A is empty, should output empty set
                         * A right anti join B on true, if A is empty, should output all rows from B
                         * we can see even condition is true, the result depends on if other side table is empty
                         */
                        return null;
                    }
                }).toRule(RuleType.ELIMINATE_SEMI_JOIN);
    }
}
