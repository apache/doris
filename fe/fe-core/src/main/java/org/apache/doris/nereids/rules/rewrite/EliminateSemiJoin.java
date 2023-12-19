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
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.logical.LogicalEmptyRelation;

import java.util.List;

/**
 * Eliminate Semi/Anti Join which is FALSE or TRUE.
 */
public class EliminateSemiJoin extends OneRewriteRuleFactory {
    @Override
    public Rule build() {
        return logicalJoin()
                // right will be converted to left
                .when(join -> join.getJoinType().isLeftSemiOrAntiJoin())
                .when(join -> join.getHashJoinConjuncts().isEmpty())
                .then(join -> {
                    List<Expression> otherJoinConjuncts = join.getOtherJoinConjuncts();
                    JoinType joinType = join.getJoinType();

                    boolean condition;
                    if (otherJoinConjuncts.isEmpty()) {
                        condition = true;
                    } else if (otherJoinConjuncts.size() == 1) {
                        if (otherJoinConjuncts.get(0).equals(BooleanLiteral.TRUE)) {
                            condition = true;
                        } else if (otherJoinConjuncts.get(0).equals(BooleanLiteral.FALSE)) {
                            condition = false;
                        } else {
                            return null;
                        }
                    } else {
                        return null;
                    }
                    if (joinType == JoinType.LEFT_SEMI_JOIN && condition
                            || (joinType == JoinType.LEFT_ANTI_JOIN && !condition)) {
                        return join.left();
                    } else if (joinType == JoinType.LEFT_SEMI_JOIN && !condition
                            || (joinType == JoinType.LEFT_ANTI_JOIN && condition)) {
                        return new LogicalEmptyRelation(StatementScopeIdGenerator.newRelationId(), join.getOutput());
                    } else {
                        throw new IllegalStateException("Unexpected join type: " + joinType);
                    }
                })
                .toRule(RuleType.ELIMINATE_SEMI_JOIN);
    }
}
