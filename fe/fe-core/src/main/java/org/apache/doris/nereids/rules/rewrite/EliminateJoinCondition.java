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
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Eliminate true Condition in Join Condition.
 */
public class EliminateJoinCondition extends OneRewriteRuleFactory {

    @Override
    public Rule build() {
        return logicalJoin().then(join -> {
            List<Expression> hashJoinConjuncts = join.getHashJoinConjuncts().stream()
                    .filter(expression -> !expression.equals(BooleanLiteral.TRUE))
                    .collect(Collectors.toList());
            List<Expression> otherJoinConjuncts = join.getOtherJoinConjuncts().stream()
                    .filter(expression -> !expression.equals(BooleanLiteral.TRUE))
                    .collect(Collectors.toList());
            List<Expression> markJoinConjuncts = join.getMarkJoinConjuncts().stream()
                    .filter(expression -> !expression.equals(BooleanLiteral.TRUE))
                    .collect(Collectors.toList());
            if (hashJoinConjuncts.size() == join.getHashJoinConjuncts().size()
                    && otherJoinConjuncts.size() == join.getOtherJoinConjuncts().size()
                    && markJoinConjuncts.size() == join.getMarkJoinConjuncts().size()) {
                return null;
            }
            return join.withJoinConjuncts(hashJoinConjuncts, otherJoinConjuncts, markJoinConjuncts,
                        join.getJoinReorderContext());
        }).toRule(RuleType.ELIMINATE_JOIN_CONDITION);
    }
}
