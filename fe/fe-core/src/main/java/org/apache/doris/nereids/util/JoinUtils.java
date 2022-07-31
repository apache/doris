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

package org.apache.doris.nereids.util;

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashJoin;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Utils for join
 */
public class JoinUtils {
    public static boolean onlyBroadcast(PhysicalHashJoin join) {
        // Cross-join only can be broadcast join.
        return join.getJoinType().isCrossJoin();
    }

    public static boolean onlyShuffle(PhysicalHashJoin join) {
        return join.getJoinType().isRightJoin() || join.getJoinType().isFullOuterJoin();
    }

    /**
     * Get all equalTo from onClause of join
     */
    public static List<EqualTo> getEqualTo(PhysicalHashJoin<Plan, Plan> join) {
        List<EqualTo> eqConjuncts = Lists.newArrayList();
        if (!join.getCondition().isPresent()) {
            return eqConjuncts;
        }

        List<SlotReference> leftSlots = join.left().getOutput().stream().map(slot -> (SlotReference) slot)
                .collect(Collectors.toList());
        List<SlotReference> rightSlots = join.right().getOutput().stream().map(slot -> (SlotReference) slot)
                .collect(Collectors.toList());

        Expression onCondition = join.getCondition().get();
        List<Expression> conjunctList = ExpressionUtils.extractConjunctive(onCondition);
        for (Expression predicate : conjunctList) {
            if (isEqualTo(leftSlots, rightSlots, predicate)) {
                eqConjuncts.add((EqualTo) predicate);
            }
        }
        return eqConjuncts;
    }

    private static boolean isEqualTo(List<SlotReference> leftSlots, List<SlotReference> rightSlots,
            Expression predicate) {
        if (!(predicate instanceof EqualTo)) {
            return false;
        }

        EqualTo equalTo = (EqualTo) predicate;
        List<SlotReference> leftUsed = equalTo.left().collect(SlotReference.class::isInstance);
        List<SlotReference> rightUsed = equalTo.right().collect(SlotReference.class::isInstance);
        if (leftUsed.isEmpty() || rightUsed.isEmpty()) {
            return false;
        }

        return Utils.equalsIgnoreOrder(leftUsed, leftSlots) || Utils.equalsIgnoreOrder(rightUsed, rightSlots);
    }

    /**
     * Get all used slots from onClause of join.
     * Return pair of left used slots and right used slots.
     */
    public static Pair<List<SlotReference>, List<SlotReference>> getOnClauseUsedSlots(
            PhysicalHashJoin<Plan, Plan> join) {
        Pair<List<SlotReference>, List<SlotReference>> childSlots =
                new Pair<>(Lists.newArrayList(), Lists.newArrayList());

        List<SlotReference> leftSlots = join.left().getOutput().stream().map(slot -> (SlotReference) slot)
                .collect(Collectors.toList());
        List<SlotReference> rightSlots = join.right().getOutput().stream().map(slot -> (SlotReference) slot)
                .collect(Collectors.toList());
        List<EqualTo> equalToList = getEqualTo(join);


        for (EqualTo equalTo : equalToList) {
            List<SlotReference> leftOnSlots = equalTo.left().collect(SlotReference.class::isInstance);
            List<SlotReference> rightOnSlots = equalTo.right().collect(SlotReference.class::isInstance);

            if (new HashSet<>(leftSlots).containsAll(leftOnSlots)
                    && new HashSet<>(rightSlots).containsAll(rightOnSlots)) {
                // TODO: need rethink about `.get(0)`
                childSlots.first.add(leftOnSlots.get(0));
                childSlots.second.add(rightOnSlots.get(0));
            } else if (new HashSet<>(leftSlots).containsAll(rightOnSlots)
                    && new HashSet<>(rightSlots).containsAll(leftOnSlots)) {
                childSlots.first.add(rightOnSlots.get(0));
                childSlots.second.add(leftOnSlots.get(0));
            } else {
                Preconditions.checkState(false, "error");
            }
        }

        return childSlots;
    }
}
