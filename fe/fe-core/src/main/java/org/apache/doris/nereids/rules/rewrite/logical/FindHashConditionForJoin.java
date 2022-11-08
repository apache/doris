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

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.rewrite.OneRewriteRuleFactory;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.util.JoinUtils;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * this rule aims to find a conjunct list from on clause expression, which could
 * be used to build hash-table.
 * <p>
 * For example:
 * A join B on A.x=B.x and A.y>1 and A.x+1=B.x+B.y and A.z=B.z+A.x and (A.z=B.z or A.x=B.x)
 * {A.x=B.x, A.x+1=B.x+B.y} could be used to build hash table,
 * but {A.y>1, A.z=B.z+A.z, (A.z=B.z or A.x=B.x)} are not.
 * <p>
 * CAUTION:
 * This rule must be applied after BindSlotReference
 */
public class FindHashConditionForJoin extends OneRewriteRuleFactory {
    @Override
    public Rule build() {
        return logicalJoin().then(join -> {
            List<Slot> leftSlots = join.left().getOutput();
            List<Slot> rightSlots = join.right().getOutput();
            Pair<List<Expression>, List<Expression>> pair = JoinUtils.extractExpressionForHashTable(leftSlots,
                    rightSlots, join.getOtherJoinConjuncts());

            List<Expression> extractedHashJoinConjuncts = pair.first;
            List<Expression> remainedNonHashJoinConjuncts = pair.second;
            if (extractedHashJoinConjuncts.isEmpty()) {
                return join;
            }

            List<Expression> combinedHashJoinConjuncts = new ImmutableList.Builder<Expression>()
                    .addAll(join.getHashJoinConjuncts())
                    .addAll(extractedHashJoinConjuncts)
                    .build();
            JoinType joinType = join.getJoinType();
            if (joinType == JoinType.CROSS_JOIN && !combinedHashJoinConjuncts.isEmpty()) {
                joinType = JoinType.INNER_JOIN;
            }
            return new LogicalJoin<>(joinType,
                    combinedHashJoinConjuncts,
                    remainedNonHashJoinConjuncts,
                    join.left(), join.right());
        }).toRule(RuleType.FIND_HASH_CONDITION_FOR_JOIN);
    }
}
