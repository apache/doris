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
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.util.JoinUtils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * T1 join T2 on T1.a=T2.a and T1.a=1 and T2.a=1
 * T1.a = T2.a can be eliminated
 */
public class EliminateConstHashJoinCondition extends OneRewriteRuleFactory {
    public static final Logger LOG = LogManager.getLogger(EliminateConstHashJoinCondition.class);

    @Override
    public Rule build() {
        return logicalJoin()
                .when(join -> join.getJoinType().isInnerJoin() || join.getJoinType().isSemiJoin())
                .whenNot(join -> join.isMarkJoin())
                .then(EliminateConstHashJoinCondition::eliminateConstHashJoinCondition)
                .toRule(RuleType.ELIMINATE_CONST_JOIN_CONDITION);
    }

    /**
     * eliminate const hash join condition
     */
    public static Plan eliminateConstHashJoinCondition(LogicalJoin<? extends Plan, ? extends Plan> join) {
        List<Expression> newHashConditions = new ArrayList<>();
        boolean changed = false;
        for (Expression expr : join.getHashJoinConjuncts()) {
            boolean eliminate = false;
            if (expr instanceof EqualTo) {
                if (((EqualTo) expr).left() instanceof SlotReference
                        && ((EqualTo) expr).right() instanceof SlotReference) {
                    EqualTo equal = (EqualTo) JoinUtils.swapEqualToForChildrenOrder((EqualTo) expr,
                            join.left().getOutputSet());
                    Optional<Expression> leftValue = join.left().getLogicalProperties()
                            .getTrait().getUniformValue((Slot) equal.left());

                    Optional<Expression> rightValue = join.right().getLogicalProperties()
                            .getTrait().getUniformValue((Slot) equal.right());
                    if (leftValue != null && rightValue != null) {
                        if (leftValue.isPresent() && rightValue.isPresent()) {
                            if (leftValue.get().equals(rightValue.get())) {
                                eliminate = true;
                                changed = true;
                            }
                        }
                    }
                }
                if (!eliminate) {
                    newHashConditions.add(expr);
                }
            } else {
                // null safe equal
                newHashConditions.add(expr);
            }
        }
        if (changed) {
            LOG.info("EliminateConstHashJoinCondition: " + join.getHashJoinConjuncts() + " -> " + newHashConditions);
            return join.withHashJoinConjuncts(newHashConditions);
        }
        return join;
    }
}
