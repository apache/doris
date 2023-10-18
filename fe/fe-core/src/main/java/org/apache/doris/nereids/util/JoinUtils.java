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
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.scalar.BitmapContains;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.Join;
import org.apache.doris.nereids.trees.plans.physical.AbstractPhysicalJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalDistribute;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
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
    public static boolean couldShuffle(Join join) {
        // Cross-join and Null-Aware-Left-Anti-Join only can be broadcast join.
        // Because mark join would consider null value from both build and probe side, so must use broadcast join too.
        return !(join.getJoinType().isCrossJoin() || join.getJoinType().isNullAwareLeftAntiJoin() || join.isMarkJoin());
    }

    public static boolean couldBroadcast(Join join) {
        return !(join.getJoinType().isRightJoin() || join.getJoinType().isFullOuterJoin());
    }

    /**
     * check if the row count of the left child in the broadcast join is less than a threshold value.
     */
    public static boolean checkBroadcastJoinStats(PhysicalHashJoin<? extends Plan, ? extends Plan> join) {
        double memLimit = ConnectContext.get().getSessionVariable().getMaxExecMemByte();
        double rowsLimit = ConnectContext.get().getSessionVariable().getBroadcastRowCountLimit();
        double brMemlimit = ConnectContext.get().getSessionVariable().getBroadcastHashtableMemLimitPercentage();
        double datasize = join.getGroupExpression().get().child(1).getStatistics().computeSize();
        double rowCount = join.getGroupExpression().get().child(1).getStatistics().getRowCount();
        return rowCount <= rowsLimit && datasize <= memLimit * brMemlimit;
    }

    /**
     * for a given equation, judge if it can be used as hash join condition
     */
    public static final class JoinSlotCoverageChecker {
        Set<ExprId> leftExprIds;
        Set<ExprId> rightExprIds;

        public JoinSlotCoverageChecker(List<Slot> left, List<Slot> right) {
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
        public boolean isHashJoinCondition(EqualTo equalTo) {
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
     *
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
     * This is used for bitmap runtime filter only.
     * Extract bitmap_contains conjunct:
     * like: bitmap_contains(a, b) and ..., Not(bitmap_contains(a, b)) and ...,
     * where `a` and `b` are from right child and left child, respectively.
     *
     * @return condition for bitmap runtime filter: bitmap_contains
     */
    public static List<Expression> extractBitmapRuntimeFilterConditions(List<Slot> leftSlots,
            List<Slot> rightSlots, List<Expression> onConditions) {
        List<Expression> result = Lists.newArrayList();
        for (Expression expr : onConditions) {
            BitmapContains bitmapContains = null;
            if (expr instanceof Not) {
                List<Expression> notChildren = ExpressionUtils.extractConjunction(expr.child(0));
                if (notChildren.size() == 1 && notChildren.get(0) instanceof BitmapContains) {
                    bitmapContains = (BitmapContains) notChildren.get(0);
                }
            } else if (expr instanceof BitmapContains) {
                bitmapContains = (BitmapContains) expr;
            }
            if (bitmapContains == null) {
                continue;
            }
            //first child in right, second child in left
            if (leftSlots.containsAll(bitmapContains.child(1).collect(Slot.class::isInstance))
                    && rightSlots.containsAll(bitmapContains.child(0).collect(Slot.class::isInstance))) {
                result.add(expr);
            }
        }
        return result;
    }

    public static boolean shouldNestedLoopJoin(Join join) {
        return join.getHashJoinConjuncts().isEmpty();
    }

    public static boolean shouldNestedLoopJoin(JoinType joinType, List<Expression> hashConjuncts) {
        return hashConjuncts.isEmpty();
    }

    /**
     * The left and right child of origin predicates need to swap sometimes.
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
     * return true if we should do bucket shuffle join when translate plan.
     */
    public static boolean shouldBucketShuffleJoin(AbstractPhysicalJoin<PhysicalPlan, PhysicalPlan> join) {
        DistributionSpec rightDistributionSpec = join.right().getPhysicalProperties().getDistributionSpec();
        if (!(rightDistributionSpec instanceof DistributionSpecHash)) {
            return false;
        }
        DistributionSpecHash rightHash = (DistributionSpecHash) rightDistributionSpec;
        return rightHash.getShuffleType() == ShuffleType.STORAGE_BUCKETED;
    }

    /**
     * return true if we should do broadcast join when translate plan.
     */
    public static boolean shouldBroadcastJoin(AbstractPhysicalJoin<PhysicalPlan, PhysicalPlan> join) {
        PhysicalPlan right = join.right();
        if (right instanceof PhysicalDistribute) {
            return ((PhysicalDistribute<?>) right).getDistributionSpec() instanceof DistributionSpecReplicated;
        }
        return false;
    }

    /**
     * return true if we should do colocate join when translate plan.
     */
    public static boolean shouldColocateJoin(AbstractPhysicalJoin<PhysicalPlan, PhysicalPlan> join) {
        if (ConnectContext.get() == null
                || ConnectContext.get().getSessionVariable().isDisableColocatePlan()) {
            return false;
        }
        DistributionSpec leftDistributionSpec = join.left().getPhysicalProperties().getDistributionSpec();
        DistributionSpec rightDistributionSpec = join.right().getPhysicalProperties().getDistributionSpec();
        if (!(leftDistributionSpec instanceof DistributionSpecHash)
                || !(rightDistributionSpec instanceof DistributionSpecHash)) {
            return false;
        }
        return couldColocateJoin((DistributionSpecHash) leftDistributionSpec,
                (DistributionSpecHash) rightDistributionSpec);
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

        // For UT or no partition is selected, getSelectedIndexId() == -1, see selectMaterializedView()
        boolean hitSameIndex = (leftTableId == rightTableId)
                && (leftHashSpec.getSelectedIndexId() != -1 && rightHashSpec.getSelectedIndexId() != -1)
                && (leftHashSpec.getSelectedIndexId() == rightHashSpec.getSelectedIndexId());
        boolean noNeedCheckColocateGroup = hitSameIndex && (leftTablePartitions.equals(rightTablePartitions))
                && (leftTablePartitions.size() <= 1);
        ColocateTableIndex colocateIndex = Env.getCurrentColocateIndex();
        if (noNeedCheckColocateGroup
                || (colocateIndex.isSameGroup(leftTableId, rightTableId)
                && !colocateIndex.isGroupUnstable(colocateIndex.getGroup(leftTableId)))) {
            return true;
        }
        return false;
    }

    public static Set<ExprId> getJoinOutputExprIdSet(Plan left, Plan right) {
        Set<ExprId> joinOutputExprIdSet = new HashSet<>();
        joinOutputExprIdSet.addAll(left.getOutputExprIdSet());
        joinOutputExprIdSet.addAll(right.getOutputExprIdSet());
        return joinOutputExprIdSet;
    }

    private static List<Slot> applyNullable(List<Slot> slots, boolean nullable) {
        return slots.stream().map(o -> o.withNullable(nullable))
                .collect(ImmutableList.toImmutableList());
    }

    /**
     * calculate the output slot of a join operator according join type and its children
     *
     * @param joinType the type of join operator
     * @param left left child
     * @param right right child
     * @return return the output slots
     */
    public static List<Slot> getJoinOutput(JoinType joinType, Plan left, Plan right) {
        switch (joinType) {
            case LEFT_SEMI_JOIN:
            case LEFT_ANTI_JOIN:
            case NULL_AWARE_LEFT_ANTI_JOIN:
                return ImmutableList.copyOf(left.getOutput());
            case RIGHT_SEMI_JOIN:
            case RIGHT_ANTI_JOIN:
                return ImmutableList.copyOf(right.getOutput());
            case LEFT_OUTER_JOIN:
                return ImmutableList.<Slot>builder()
                        .addAll(left.getOutput())
                        .addAll(applyNullable(right.getOutput(), true))
                        .build();
            case RIGHT_OUTER_JOIN:
                return ImmutableList.<Slot>builder()
                        .addAll(applyNullable(left.getOutput(), true))
                        .addAll(right.getOutput())
                        .build();
            case FULL_OUTER_JOIN:
                return ImmutableList.<Slot>builder()
                        .addAll(applyNullable(left.getOutput(), true))
                        .addAll(applyNullable(right.getOutput(), true))
                        .build();
            default:
                return ImmutableList.<Slot>builder()
                        .addAll(left.getOutput())
                        .addAll(right.getOutput())
                        .build();
        }
    }
}
