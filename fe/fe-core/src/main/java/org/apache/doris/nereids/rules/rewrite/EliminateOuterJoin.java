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
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.util.TypeUtils;
import org.apache.doris.nereids.util.Utils;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSet.Builder;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

/**
 * Eliminate outer join.
 */
public class EliminateOuterJoin extends OneRewriteRuleFactory {

    @Override
    public Rule build() {
        return logicalFilter(
                logicalJoin().when(join -> join.getJoinType().isOuterJoin())
        ).then(filter -> {
            LogicalJoin<Plan, Plan> join = filter.child();

            Builder<Expression> conjunctsBuilder = ImmutableSet.builder();
            Set<Slot> notNullSlots = new HashSet<>();
            for (Expression predicate : filter.getConjuncts()) {
                Optional<Slot> notNullSlot = TypeUtils.isNotNull(predicate);
                if (notNullSlot.isPresent()) {
                    notNullSlots.add(notNullSlot.get());
                } else {
                    conjunctsBuilder.add(predicate);
                }
            }
            boolean canFilterLeftNull = Utils.isIntersecting(join.left().getOutputSet(), notNullSlots);
            boolean canFilterRightNull = Utils.isIntersecting(join.right().getOutputSet(), notNullSlots);
            if (!canFilterRightNull && !canFilterLeftNull) {
                return null;
            }

            JoinType newJoinType = tryEliminateOuterJoin(join.getJoinType(), canFilterLeftNull, canFilterRightNull);
            return filter.withChildren(join.withJoinType(newJoinType));
        }).toRule(RuleType.ELIMINATE_OUTER_JOIN);
    }

    private JoinType tryEliminateOuterJoin(JoinType joinType, boolean canFilterLeftNull, boolean canFilterRightNull) {
        if (joinType.isRightOuterJoin() && canFilterLeftNull) {
            return JoinType.INNER_JOIN;
        }
        if (joinType.isLeftOuterJoin() && canFilterRightNull) {
            return JoinType.INNER_JOIN;
        }
        if (joinType.isFullOuterJoin() && canFilterLeftNull && canFilterRightNull) {
            return JoinType.INNER_JOIN;
        }
        if (joinType.isFullOuterJoin() && canFilterLeftNull) {
            return JoinType.LEFT_OUTER_JOIN;
        }
        if (joinType.isFullOuterJoin() && canFilterRightNull) {
            return JoinType.RIGHT_OUTER_JOIN;
        }
        return joinType;
    }
}
