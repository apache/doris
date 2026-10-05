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
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.PlanUtils;

import com.google.common.collect.ImmutableSet;

import java.util.LinkedHashSet;
import java.util.Set;

/**
 * InferNotNull From Join. Like:
 * Join: a inner join b on a.id = b.id
 * ->
 * Join: a inner join b on a.id = b.id.
 * - Filter: a.id is not null
 * - Filter: b.id is not null
 */
public class InferJoinNotNull extends OneRewriteRuleFactory {
    @Override
    public Rule build() {
        // TODO: maybe consider ANTI?
        return logicalJoin(any(), any())
            .when(join -> join.getJoinType().isInnerJoin() || join.getJoinType().isAsofInnerJoin()
                    || join.getJoinType().isSemiJoin())
            .whenNot(LogicalJoin::isMarkJoin)
            .thenApply(ctx -> {
                LogicalJoin<Plan, Plan> join = ctx.root;
                Set<Expression> conjuncts = new LinkedHashSet<>();
                conjuncts.addAll(join.getHashJoinConjuncts());
                conjuncts.addAll(join.getOtherJoinConjuncts());
                Set<Slot> notNullSlots = ExpressionUtils.inferNotNullSlots(
                        conjuncts, ctx.cascadesContext);

                Plan left = join.left();
                Plan right = join.right();
                if (join.getJoinType().isInnerJoin() || join.getJoinType().isAsofInnerJoin()) {
                    Set<Expression> leftNotNull = inferNotNull(notNullSlots, join.left().getOutputSet());
                    Set<Expression> rightNotNull = inferNotNull(notNullSlots, join.right().getOutputSet());
                    left = PlanUtils.filterOrSelf(leftNotNull, join.left());
                    right = PlanUtils.filterOrSelf(rightNotNull, join.right());
                } else if (join.getJoinType() == JoinType.LEFT_SEMI_JOIN) {
                    Set<Expression> leftNotNull = inferNotNull(notNullSlots, join.left().getOutputSet());
                    left = PlanUtils.filterOrSelf(leftNotNull, join.left());
                } else {
                    Set<Expression> rightNotNull = inferNotNull(notNullSlots, join.right().getOutputSet());
                    right = PlanUtils.filterOrSelf(rightNotNull, join.right());
                }

                if (left.equals(join.left()) && right.equals(join.right())) {
                    return null;
                }
                return join.withChildren(left, right);
            }).toRule(RuleType.INFER_JOIN_NOT_NULL);
    }

    private Set<Expression> inferNotNull(Set<Slot> notNullSlots, Set<Slot> outputSlots) {
        ImmutableSet.Builder<Expression> predicates = ImmutableSet.builderWithExpectedSize(notNullSlots.size());
        for (Slot slot : notNullSlots) {
            if (outputSlots.contains(slot)) {
                predicates.add(new Not(new IsNull(slot), true));
            }
        }
        return predicates.build();
    }
}
