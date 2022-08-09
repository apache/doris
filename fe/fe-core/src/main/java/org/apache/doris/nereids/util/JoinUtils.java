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
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.Join;
import org.apache.doris.nereids.trees.plans.physical.PhysicalJoin;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Utils for join
 */
public class JoinUtils {
    public static boolean onlyBroadcast(PhysicalJoin join) {
        // Cross-join only can be broadcast join.
        return join.getJoinType().isCrossJoin();
    }

    public static boolean onlyShuffle(PhysicalJoin join) {
        return join.getJoinType().isRightJoin() || join.getJoinType().isFullOuterJoin();
    }

    /**
     * Get all equalTo from onClause of join
     */
    public static List<EqualTo> getEqualTo(PhysicalJoin<Plan, Plan> join) {
        List<EqualTo> eqConjuncts = Lists.newArrayList();
        if (!join.getCondition().isPresent()) {
            return eqConjuncts;
        }

        List<SlotReference> leftSlots = Utils.getOutputSlotReference(join.left());
        List<SlotReference> rightSlots = Utils.getOutputSlotReference(join.right());

        Expression onCondition = join.getCondition().get();
        List<Expression> conjunctList = ExpressionUtils.extractConjunction(onCondition);
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

        Set<SlotReference> leftSlotsSet = new HashSet<>(leftSlots);
        Set<SlotReference> rightSlotsSet = new HashSet<>(rightSlots);
        return (leftSlotsSet.containsAll(leftUsed) && rightSlotsSet.containsAll(rightUsed))
                || (leftSlotsSet.containsAll(rightUsed) && rightSlotsSet.containsAll(leftUsed));
    }

    /**
     * Get all used slots from onClause of join.
     * Return pair of left used slots and right used slots.
     */
    public static Pair<List<SlotReference>, List<SlotReference>> getOnClauseUsedSlots(
            PhysicalJoin<Plan, Plan> join) {
        Pair<List<SlotReference>, List<SlotReference>> childSlots =
                new Pair<>(Lists.newArrayList(), Lists.newArrayList());

        List<SlotReference> leftSlots = Utils.getOutputSlotReference(join.left());
        List<SlotReference> rightSlots = Utils.getOutputSlotReference(join.right());
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

        Preconditions.checkState(childSlots.first.size() == childSlots.second.size());
        return childSlots;
    }

    public static boolean shouldNestedLoopJoin(Join join) {
        JoinType joinType = join.getJoinType();
        return (joinType.isInnerJoin() && !join.getCondition().isPresent()) || joinType.isCrossJoin();
    }
}
