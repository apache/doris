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
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.Join;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.physical.AbstractPhysicalJoin;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Utils for join
 */
public class JoinUtils {
    public static boolean onlyBroadcast(AbstractPhysicalJoin join) {
        // Cross-join only can be broadcast join.
        return join.getJoinType().isCrossJoin();
    }

    public static boolean onlyShuffle(AbstractPhysicalJoin join) {
        return join.getJoinType().isRightJoin() || join.getJoinType().isFullOuterJoin();
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

    private static class JoinSlotCoverageChecker {
        HashSet<ExprId> leftExprIds;
        HashSet<ExprId> rightExprIds;

        JoinSlotCoverageChecker(List<SlotReference> left, List<SlotReference> right) {
            leftExprIds = (HashSet<ExprId>) left.stream().map(SlotReference::getExprId).collect(Collectors.toSet());
            rightExprIds = (HashSet<ExprId>) right.stream().map(SlotReference::getExprId).collect(Collectors.toSet());
        }

        boolean isCoveredByLeftSlots(List<SlotReference> slots) {
            return slots.stream().map(SlotReference::getExprId)
                    .allMatch(leftExprIds::contains);
        }

        boolean isCoveredByRightSlots(List<SlotReference> slots) {
            return slots.stream()
                    .map(SlotReference::getExprId)
                    .allMatch(rightExprIds::contains);
        }

        /**
         *  consider following cases:
         *  1# A=1 => not for hash table
         *  2# t1.a=t2.a + t2.b => hash table
         *  3# t1.a=t1.a + t2.b => not for hash table
         *  4# t1.a=t2.a or t1.b=t2.b not for hash table
         *  5# t1.a > 1 not for hash table
         * @param equalTo a conjunct in on clause condition
         * @return true if the equal can be used as hash join condition
         */
        boolean isHashJoinCondition(EqualTo equalTo) {
            List<SlotReference> equalLeft =  equalTo.left().collect(SlotReference.class::isInstance);
            if (equalLeft.isEmpty()) {
                return false;
            }

            List<SlotReference> equalRight = equalTo.right().collect(SlotReference.class::isInstance);
            if (equalRight.isEmpty()) {
                return false;
            }

            List<ExprId> equalLeftExprIds = equalLeft.stream()
                    .map(SlotReference::getExprId).collect(Collectors.toList());

            List<ExprId> equalRightExprIds = equalRight.stream()
                    .map(SlotReference::getExprId).collect(Collectors.toList());
            return leftExprIds.containsAll(equalLeftExprIds) && rightExprIds.containsAll(equalRightExprIds)
                    || leftExprIds.containsAll(equalRightExprIds) && rightExprIds.containsAll(equalLeftExprIds);
        }
    }

    /**
     * collect expressions from on clause, which could be used to build hash table
     * @param join join node
     * @return pair of expressions, for hash table or not.
     */
    public static Pair<List<Expression>, List<Expression>> extractExpressionForHashTable(LogicalJoin join) {
        if (join.getOtherJoinCondition().isPresent()) {
            List<Expression> onExprs = ExpressionUtils.extractConjunction(
                    (Expression) join.getOtherJoinCondition().get());
            List<SlotReference> leftSlots = Utils.getOutputSlotReference(join.left());
            List<SlotReference> rightSlots = Utils.getOutputSlotReference(join.right());
            return extractExpressionForHashTable(leftSlots, rightSlots, onExprs);
        }
        return Pair.of(Lists.newArrayList(), Lists.newArrayList());
    }

    /**
     * extract expression
     * @param leftSlots left child output slots
     * @param rightSlots right child output slots
     * @param onConditions conditions to be split
     * @return pair of hashCondition and otherCondition
     */
    public static Pair<List<Expression>, List<Expression>> extractExpressionForHashTable(List<SlotReference> leftSlots,
            List<SlotReference> rightSlots,
            List<Expression> onConditions) {

        Pair<List<Expression>, List<Expression>> pair = Pair.of(Lists.newArrayList(), Lists.newArrayList());
        JoinSlotCoverageChecker checker = new JoinSlotCoverageChecker(leftSlots, rightSlots);
        Map<Boolean, List<Expression>> mapper = onConditions.stream()
                .collect(Collectors.groupingBy(
                        expr -> (expr instanceof EqualTo) && checker.isHashJoinCondition((EqualTo) expr)));
        if (mapper.containsKey(true)) {
            pair.first = mapper.get(true);
        }
        if (mapper.containsKey(false)) {
            pair.second = mapper.get(false);
        }
        return pair;
    }



    /**
     * Get all used slots from onClause of join.
     * Return pair of left used slots and right used slots.
     */
    public static Pair<List<SlotReference>, List<SlotReference>> getOnClauseUsedSlots(
            AbstractPhysicalJoin<Plan, Plan> join) {
        Pair<List<SlotReference>, List<SlotReference>> childSlots =
                Pair.of(Lists.newArrayList(), Lists.newArrayList());

        List<SlotReference> leftSlots = Utils.getOutputSlotReference(join.left());
        List<SlotReference> rightSlots = Utils.getOutputSlotReference(join.right());
        List<EqualTo> equalToList = join.getHashJoinConjuncts().stream()
                .map(e -> (EqualTo) e).collect(Collectors.toList());
        JoinSlotCoverageChecker checker = new JoinSlotCoverageChecker(leftSlots, rightSlots);
        for (EqualTo equalTo : equalToList) {
            List<SlotReference> leftOnSlots = equalTo.left().collect(SlotReference.class::isInstance);
            List<SlotReference> rightOnSlots = equalTo.right().collect(SlotReference.class::isInstance);

            if (checker.isCoveredByLeftSlots(leftOnSlots)
                    && checker.isCoveredByRightSlots(rightOnSlots)) {
                childSlots.first.addAll(leftOnSlots);
                childSlots.second.addAll(rightOnSlots);
            } else if (checker.isCoveredByLeftSlots(rightOnSlots)
                    && checker.isCoveredByRightSlots(leftOnSlots)) {
                childSlots.first.addAll(rightOnSlots);
                childSlots.second.addAll(leftOnSlots);
            } else {
                Preconditions.checkState(false, "error");
            }
        }

        Preconditions.checkState(childSlots.first.size() == childSlots.second.size());
        return childSlots;
    }

    public static boolean shouldNestedLoopJoin(Join join) {
        JoinType joinType = join.getJoinType();
        return (joinType.isInnerJoin() && join.getHashJoinConjuncts().isEmpty()) || joinType.isCrossJoin();
    }
}
