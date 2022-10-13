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

import org.apache.doris.catalog.ColocateTableIndex;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.properties.DistributionSpec;
import org.apache.doris.nereids.properties.DistributionSpecHash;
import org.apache.doris.nereids.properties.DistributionSpecHash.ShuffleType;
import org.apache.doris.nereids.properties.DistributionSpecReplicated;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.Join;
import org.apache.doris.nereids.trees.plans.physical.AbstractPhysicalJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalDistribute;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Utils for join
 */
public class JoinUtils {
    public static boolean couldShuffle(Join join) {
        // Cross-join only can be broadcast join.
        return !(join.getJoinType().isCrossJoin());
    }

    public static boolean couldBroadcast(Join join) {
        return !(join.getJoinType().isRightJoin() || join.getJoinType().isFullOuterJoin());
    }

    private static final class JoinSlotCoverageChecker {
        Set<ExprId> leftExprIds;
        Set<ExprId> rightExprIds;

        JoinSlotCoverageChecker(List<Slot> left, List<Slot> right) {
            leftExprIds = left.stream().map(Slot::getExprId).collect(Collectors.toSet());
            rightExprIds = right.stream().map(Slot::getExprId).collect(Collectors.toSet());
        }

        JoinSlotCoverageChecker(Set<ExprId> left, Set<ExprId> right) {
            leftExprIds = left;
            rightExprIds = right;
        }

        /**
         * PushDownExpressionInHashConjuncts ensure the "slots" is only one slot.
         */
        boolean isCoveredByLeftSlots(ExprId slot) {
            return leftExprIds.contains(slot);
        }

        boolean isCoveredByRightSlots(ExprId slot) {
            return rightExprIds.contains(slot);
        }

        /**
         * consider following cases:
         * 1# A=1 => not for hash table
         * 2# t1.a=t2.a + t2.b => hash table
         * 3# t1.a=t1.a + t2.b => not for hash table
         * 4# t1.a=t2.a or t1.b=t2.b not for hash table
         * 5# t1.a > 1 not for hash table
         *
         * @param equalTo a conjunct in on clause condition
         * @return true if the equal can be used as hash join condition
         */
        boolean isHashJoinCondition(EqualTo equalTo) {
            Set<Slot> equalLeft = equalTo.left().collect(Slot.class::isInstance);
            if (equalLeft.isEmpty()) {
                return false;
            }

            Set<Slot> equalRight = equalTo.right().collect(Slot.class::isInstance);
            if (equalRight.isEmpty()) {
                return false;
            }

            List<ExprId> equalLeftExprIds = equalLeft.stream()
                    .map(Slot::getExprId).collect(Collectors.toList());

            List<ExprId> equalRightExprIds = equalRight.stream()
                    .map(Slot::getExprId).collect(Collectors.toList());
            return leftExprIds.containsAll(equalLeftExprIds) && rightExprIds.containsAll(equalRightExprIds)
                    || leftExprIds.containsAll(equalRightExprIds) && rightExprIds.containsAll(equalLeftExprIds);
        }
    }

    /**
     * extract expression
     * @param leftSlots left child output slots
     * @param rightSlots right child output slots
     * @param onConditions conditions to be split
     * @return pair of hashCondition and otherCondition
     */
    public static Pair<List<Expression>, List<Expression>> extractExpressionForHashTable(List<Slot> leftSlots,
            List<Slot> rightSlots, List<Expression> onConditions) {
        JoinSlotCoverageChecker checker = new JoinSlotCoverageChecker(leftSlots, rightSlots);
        Map<Boolean, List<Expression>> mapper = onConditions.stream()
                .collect(Collectors.groupingBy(
                        expr -> (expr instanceof EqualTo) && checker.isHashJoinCondition((EqualTo) expr)));
        return Pair.of(
                mapper.getOrDefault(true, ImmutableList.of()),
                mapper.getOrDefault(false, ImmutableList.of())
        );
    }

    /**
     * Get all used slots from onClause of join.
     * Return pair of left used slots and right used slots.
     */
    public static Pair<List<ExprId>, List<ExprId>> getOnClauseUsedSlots(
                AbstractPhysicalJoin<? extends Plan, ? extends Plan> join) {

        List<ExprId> exprIds1 = Lists.newArrayListWithCapacity(join.getHashJoinConjuncts().size());
        List<ExprId> exprIds2 = Lists.newArrayListWithCapacity(join.getHashJoinConjuncts().size());

        JoinSlotCoverageChecker checker = new JoinSlotCoverageChecker(
                join.left().getOutputExprIdSet(),
                join.right().getOutputExprIdSet());

        for (Expression expr : join.getHashJoinConjuncts()) {
            EqualTo equalTo = (EqualTo) expr;
            if (!(equalTo.left() instanceof Slot) || !(equalTo.right() instanceof Slot)) {
                continue;
            }
            ExprId leftExprId = ((Slot) equalTo.left()).getExprId();
            ExprId rightExprId = ((Slot) equalTo.right()).getExprId();

            if (checker.isCoveredByLeftSlots(leftExprId)
                    && checker.isCoveredByRightSlots(rightExprId)) {
                exprIds1.add(leftExprId);
                exprIds2.add(rightExprId);
            } else if (checker.isCoveredByLeftSlots(rightExprId)
                    && checker.isCoveredByRightSlots(leftExprId)) {
                exprIds1.add(rightExprId);
                exprIds2.add(leftExprId);
            } else {
                throw new RuntimeException("Could not generate valid equal on clause slot pairs for join: " + join);
            }
        }
        return Pair.of(exprIds1, exprIds2);
    }

    public static boolean shouldNestedLoopJoin(Join join) {
        JoinType joinType = join.getJoinType();
        return (joinType.isInnerJoin() && join.getHashJoinConjuncts().isEmpty()) || joinType.isCrossJoin();
    }

    /**
     * The left and right child of origin predicates need to be swap sometimes.
     * Case A:
     * select * from t1 join t2 on t2.id=t1.id
     * The left plan node is t1 and the right plan node is t2.
     * The left child of origin predicate is t2.id and the right child of origin predicate is t1.id.
     * In this situation, the children of predicate need to be swap => t1.id=t2.id.
     */
    public static Expression swapEqualToForChildrenOrder(EqualTo equalTo, Set<Slot> leftOutput) {
        if (leftOutput.containsAll(equalTo.left().getInputSlots())) {
            return equalTo;
        } else {
            return equalTo.commute();
        }
    }

    /**
     * return true if we should do broadcast join when translate plan.
     */
    public static boolean shouldBroadcastJoin(AbstractPhysicalJoin<PhysicalPlan, PhysicalPlan> join) {
        PhysicalPlan right = join.right();
        DistributionSpec rightDistributionSpec = right.getPhysicalProperties().getDistributionSpec();
        return rightDistributionSpec instanceof DistributionSpecReplicated;
    }

    /**
     * return true if we should do colocate join when translate plan.
     */
    public static boolean shouldColocateJoin(AbstractPhysicalJoin<PhysicalPlan, PhysicalPlan> join) {
        if (ConnectContext.get() == null
                || ConnectContext.get().getSessionVariable().isDisableColocatePlan()) {
            return false;
        }
        DistributionSpec joinDistributionSpec = join.getPhysicalProperties().getDistributionSpec();
        DistributionSpec leftDistributionSpec = join.left().getPhysicalProperties().getDistributionSpec();
        DistributionSpec rightDistributionSpec = join.right().getPhysicalProperties().getDistributionSpec();
        if (!(leftDistributionSpec instanceof DistributionSpecHash)
                || !(rightDistributionSpec instanceof DistributionSpecHash)
                || !(joinDistributionSpec instanceof DistributionSpecHash)) {
            return false;
        }
        DistributionSpecHash leftHash = (DistributionSpecHash) leftDistributionSpec;
        DistributionSpecHash rightHash = (DistributionSpecHash) rightDistributionSpec;
        DistributionSpecHash joinHash = (DistributionSpecHash) joinDistributionSpec;
        return leftHash.getShuffleType() == ShuffleType.NATURAL
                && rightHash.getShuffleType() == ShuffleType.NATURAL
                && joinHash.getShuffleType() == ShuffleType.NATURAL;
    }

    /**
     * return true if we should do bucket shuffle join when translate plan.
     */
    public static boolean shouldBucketShuffleJoin(AbstractPhysicalJoin<PhysicalPlan, PhysicalPlan> join) {
        if (ConnectContext.get() == null
                || !ConnectContext.get().getSessionVariable().isEnableBucketShuffleJoin()) {
            return false;
        }
        DistributionSpec joinDistributionSpec = join.getPhysicalProperties().getDistributionSpec();
        DistributionSpec leftDistributionSpec = join.left().getPhysicalProperties().getDistributionSpec();
        DistributionSpec rightDistributionSpec = join.right().getPhysicalProperties().getDistributionSpec();
        if (join.left() instanceof PhysicalDistribute) {
            return false;
        }
        if (!(joinDistributionSpec instanceof DistributionSpecHash)
                || !(leftDistributionSpec instanceof DistributionSpecHash)
                || !(rightDistributionSpec instanceof DistributionSpecHash)) {
            return false;
        }
        DistributionSpecHash leftHash = (DistributionSpecHash) leftDistributionSpec;
        // when we plan a bucket shuffle join, the left should not add a distribution enforce.
        // so its shuffle type should be NATURAL(olap scan node or result of colocate join / bucket shuffle join with
        // left child's shuffle type is also NATURAL), or be BUCKETED(result of join / agg).
        if (leftHash.getShuffleType() != ShuffleType.BUCKETED && leftHash.getShuffleType() != ShuffleType.NATURAL) {
            return false;
        }
        // there must use left as required and join as source.
        // Because after property derive upper node's properties is contains lower node
        // if their properties are satisfy.
        return joinDistributionSpec.satisfy(leftDistributionSpec);
    }

    /**
     * could do colocate join with left and right child distribution spec.
     */
    public static boolean couldColocateJoin(DistributionSpecHash leftHashSpec, DistributionSpecHash rightHashSpec) {
        if (ConnectContext.get() == null
                || ConnectContext.get().getSessionVariable().isDisableColocatePlan()) {
            return false;
        }
        if (leftHashSpec.getShuffleType() != ShuffleType.NATURAL
                || rightHashSpec.getShuffleType() != ShuffleType.NATURAL) {
            return false;
        }
        final long leftTableId = leftHashSpec.getTableId();
        final long rightTableId = rightHashSpec.getTableId();
        final Set<Long> leftTablePartitions = leftHashSpec.getPartitionIds();
        final Set<Long> rightTablePartitions = rightHashSpec.getPartitionIds();
        boolean noNeedCheckColocateGroup = (leftTableId == rightTableId)
                && (leftTablePartitions.equals(rightTablePartitions)) && (leftTablePartitions.size() <= 1);
        ColocateTableIndex colocateIndex = Env.getCurrentColocateIndex();
        if (noNeedCheckColocateGroup
                || (colocateIndex.isSameGroup(leftTableId, rightTableId)
                && !colocateIndex.isGroupUnstable(colocateIndex.getGroup(leftTableId)))) {
            return true;
        }
        return false;
    }

    /**
     * replace hashJoinConjuncts by using slots map.
     * TODO: just support `col1=col2`
     */
    public static List<Expression> replaceHashConjuncts(List<Expression> hashJoinConjuncts,
            Map<Slot, Slot> replaceMaps) {
        List<Expression> newHashJoinConjuncts = Lists.newArrayList();
        for (Expression hashJoinConjunct : hashJoinConjuncts) {
            if (!(hashJoinConjunct instanceof EqualTo)) {
                return null;
            }
            EqualTo equalTo = (EqualTo) hashJoinConjunct;
            if (!(equalTo.left() instanceof Slot) || !(equalTo.right() instanceof Slot)) {
                return null;
            }

            Slot leftSlot = (Slot) equalTo.left();
            leftSlot = replaceMaps.getOrDefault(leftSlot, leftSlot);

            Slot rightSlot = (Slot) equalTo.right();
            rightSlot = replaceMaps.getOrDefault(rightSlot, rightSlot);

            if (leftSlot != equalTo.left() || rightSlot != equalTo.right()) {
                newHashJoinConjuncts.add(new EqualTo(leftSlot, rightSlot));
            } else {
                newHashJoinConjuncts.add(hashJoinConjunct);
            }
        }

        return newHashJoinConjuncts;
    }

    /**
     * When project not empty, we add all slots used by hashOnCondition into projects.
     */
    public static void addSlotsUsedByOn(Set<Slot> usedSlots, List<NamedExpression> projects) {
        if (projects.isEmpty()) {
            return;
        }
        Set<ExprId> projectExprIdSet = projects.stream()
                .map(project -> project.getExprId())
                .collect(Collectors.toSet());
        usedSlots.forEach(slot -> {
            if (!projectExprIdSet.contains(slot.getExprId())) {
                projects.add(slot);
            }
        });
    }
}
