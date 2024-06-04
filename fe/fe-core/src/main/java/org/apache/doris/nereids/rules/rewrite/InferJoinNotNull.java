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
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.PlanUtils;

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
            .when(join -> join.getJoinType().isInnerJoin() || join.getJoinType().isSemiJoin())
            .whenNot(LogicalJoin::isMarkJoin)
            .thenApply(ctx -> {
                LogicalJoin<Plan, Plan> join = ctx.root;
                Set<Expression> conjuncts = new LinkedHashSet<>();
                conjuncts.addAll(join.getHashJoinConjuncts());
                conjuncts.addAll(join.getOtherJoinConjuncts());

                Plan left = join.left();
                Plan right = join.right();
                if (join.getJoinType().isInnerJoin()) {
                    Set<Expression> leftNotNull = ExpressionUtils.inferNotNull(
                            conjuncts, join.left().getOutputSet(), ctx.cascadesContext);
                    Set<Expression> rightNotNull = ExpressionUtils.inferNotNull(
                            conjuncts, join.right().getOutputSet(), ctx.cascadesContext);
                    left = PlanUtils.filterOrSelf(leftNotNull, join.left());
                    right = PlanUtils.filterOrSelf(rightNotNull, join.right());
                } else if (join.getJoinType() == JoinType.LEFT_SEMI_JOIN) {
                    Set<Expression> leftNotNull = ExpressionUtils.inferNotNull(
                            conjuncts, join.left().getOutputSet(), ctx.cascadesContext);
                    left = PlanUtils.filterOrSelf(leftNotNull, join.left());
                } else {
                    Set<Expression> rightNotNull = ExpressionUtils.inferNotNull(
                            conjuncts, join.right().getOutputSet(), ctx.cascadesContext);
                    right = PlanUtils.filterOrSelf(rightNotNull, join.right());
                }

                if (left.equals(join.left()) && right.equals(join.right())) {
                    return null;
                }
                return join.withChildren(left, right);
            }).toRule(RuleType.INFER_JOIN_NOT_NULL);
    }
}
