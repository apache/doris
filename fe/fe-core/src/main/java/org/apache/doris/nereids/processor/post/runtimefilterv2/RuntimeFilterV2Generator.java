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

package org.apache.doris.nereids.processor.post.runtimefilterv2;

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.processor.post.PlanPostProcessor;
import org.apache.doris.nereids.processor.post.RuntimeFilterGenerator;
import org.apache.doris.nereids.trees.expressions.EqualPredicate;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NullSafeEqual;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.AbstractPlan;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalExcept;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalIntersect;
import org.apache.doris.nereids.trees.plans.physical.PhysicalSetOperation;
import org.apache.doris.nereids.util.JoinUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.statistics.ColumnStatistic;
import org.apache.doris.statistics.Statistics;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

/**
 * RuntimeFilterV2Generator
 */
public class RuntimeFilterV2Generator extends PlanPostProcessor {

    public RuntimeFilterV2Generator() {
    }

    @Override
    public Plan visitPhysicalHashJoin(PhysicalHashJoin<? extends Plan, ? extends Plan> join,
            CascadesContext context) {
        // bottom-up traversal to make sure build/probe children processed first
        join.right().accept(this, context);
        join.left().accept(this, context);

        if (RuntimeFilterGenerator.DENIED_JOIN_TYPES.contains(join.getJoinType()) || join.isMarkJoin()) {
            return join;
        }

        boolean buildOnRight = !isBuildOnLeft(join.getJoinType());
        AbstractPlan buildPlan = (AbstractPlan) (buildOnRight ? join.right() : join.left());
        AbstractPlan probePlan = (AbstractPlan) (buildOnRight ? join.left() : join.right());

        if (buildSideTooLarge(buildPlan)) {
            return join;
        }

        for (int idx = 0; idx < join.getHashJoinConjuncts().size(); idx++) {
            Expression conjunct = join.getHashJoinConjuncts().get(idx);
            if (!(conjunct instanceof EqualPredicate)) {
                continue;
            }
            EqualPredicate normalized = JoinUtils.swapEqualToForChildrenOrder(
                    (EqualPredicate) conjunct,
                    buildOnRight ? join.left().getOutputSet() : join.right().getOutputSet());

            if (skipBecauseUniformValue(buildPlan, probePlan, normalized)) {
                continue;
            }

            Expression targetExpression = normalized.left();
            Expression sourceExpression = normalized.right();
            if (targetExpression.getInputSlots().size() != 1
                    || sourceExpression.getInputSlots().size() != 1) {
                continue;
            }
            long buildNdvOrRowCount = estimateBuildNdvOrRowCount(buildPlan, sourceExpression);
            if (conjunct instanceof NullSafeEqual) {
                continue;
            }

            PushDownContext pushDownContext = new PushDownContext(
                    context.getRuntimeFilterV2Context(),
                    join,
                    sourceExpression,
                    buildNdvOrRowCount,
                    idx, false,
                    targetExpression);

            probePlan.accept(PushDownVisitor.INSTANCE, pushDownContext);
        }

        return join;
    }

    @Override
    public Plan visitPhysicalIntersect(PhysicalIntersect intersect, CascadesContext context) {
        computeRuntimeFilterForIntersectAndExcept(intersect, context);
        return visitPhysicalSetOperation(intersect, context);
    }

    @Override
    public Plan visitPhysicalExcept(PhysicalExcept except, CascadesContext context) {
        computeRuntimeFilterForIntersectAndExcept(except, context);
        return visitPhysicalSetOperation(except, context);
    }

    private void computeRuntimeFilterForIntersectAndExcept(PhysicalSetOperation setOp, CascadesContext context) {
        AbstractPlan child0 = (AbstractPlan) setOp.child(0);
        if (child0.getStats() != null
                && ConnectContext.get() != null
                && ConnectContext.get().getSessionVariable() != null
                && child0.getStats().getRowCount()
                < ConnectContext.get().getSessionVariable().runtimeFilterMaxBuildRowCount) {
            for (int slotIdx : chooseSourceSlots(setOp)) {
                Expression sourceExpression = setOp.getRegularChildrenOutputs().get(0).get(slotIdx);
                for (int childId = 1; childId < setOp.children().size(); childId++) {
                    Plan child = setOp.children().get(childId);
                    Expression targetExpression = setOp.getRegularChildrenOutputs().get(childId).get(slotIdx);
                    Statistics stats = child0.getStats();
                    long buildNdvOrRowCount = -1;
                    if (stats != null) {
                        buildNdvOrRowCount = (long) stats.getRowCount();
                        ColumnStatistic colStats = stats.findColumnStatistics(
                                sourceExpression);
                        if (colStats != null && !colStats.isUnKnown) {
                            buildNdvOrRowCount = Math.max(1, (long) colStats.ndv);
                        }
                    }
                    PushDownContext pushDownContext = new PushDownContext(
                            context.getRuntimeFilterV2Context(),
                            setOp,
                            sourceExpression,
                            buildNdvOrRowCount,
                            slotIdx,
                            targetExpression);
                    child.accept(PushDownVisitor.INSTANCE, pushDownContext);
                }
            }
        }
    }

    /**
     *
     * do not use metric data type column (such as array, map, struct, json, bitmap, ...) as runtime filter source
     *
     */
    private List<Integer> chooseSourceSlots(PhysicalSetOperation setOp) {
        List<Slot> output = setOp.getOutput();
        for (int i = 0; i < output.size(); i++) {
            if (!output.get(i).getDataType().isOnlyMetricType()
                    && !setOp.getLogicalProperties().getTrait().getUniformValue(output.get(i)).isPresent()) {
                return ImmutableList.of(i);
            }
        }
        return ImmutableList.of();
    }

    private boolean isBuildOnLeft(JoinType joinType) {
        return joinType.isRightJoin();
    }

    private boolean buildSideTooLarge(AbstractPlan buildPlan) {
        if (buildPlan == null || buildPlan.getStats() == null) {
            return false;
        }
        if (ConnectContext.get() == null || ConnectContext.get().getSessionVariable() == null) {
            return false;
        }
        double rowCount = buildPlan.getStats().getRowCount();
        if (Double.isNaN(rowCount)) {
            return false;
        }
        return rowCount >= ConnectContext.get().getSessionVariable().runtimeFilterMaxBuildRowCount;
    }

    private long estimateBuildNdvOrRowCount(AbstractPlan buildPlan, Expression sourceExpression) {
        long buildNdvOrRowCount = -1;
        if (buildPlan != null && buildPlan.getStats() != null) {
            Statistics stats = buildPlan.getStats();
            buildNdvOrRowCount = (long) stats.getRowCount();
            ColumnStatistic columnStatistic = stats.findColumnStatistics(sourceExpression);
            if (columnStatistic != null && !columnStatistic.isUnKnown) {
                buildNdvOrRowCount = Math.max(1, (long) columnStatistic.ndv);
            }
        }
        return buildNdvOrRowCount;
    }

    private boolean skipBecauseUniformValue(AbstractPlan buildPlan, AbstractPlan probePlan,
            EqualPredicate equalPredicate) {
        if (!(equalPredicate.left() instanceof Slot) || !(equalPredicate.right() instanceof Slot)) {
            return false;
        }
        Slot targetSlot = (Slot) equalPredicate.left();
        Slot sourceSlot = (Slot) equalPredicate.right();
        Optional<Expression> targetUniform = Optional.empty();
        Optional<Expression> sourceUniform = Optional.empty();
        if (probePlan.getOutputSet().contains(targetSlot) && probePlan.getLogicalProperties() != null
                && probePlan.getLogicalProperties().getTrait() != null) {
            targetUniform = probePlan.getLogicalProperties().getTrait().getUniformValue(targetSlot);
        }
        if (buildPlan.getOutputSet().contains(sourceSlot) && buildPlan.getLogicalProperties() != null
                && buildPlan.getLogicalProperties().getTrait() != null) {
            sourceUniform = buildPlan.getLogicalProperties().getTrait().getUniformValue(sourceSlot);
        }
        return targetUniform.isPresent() && sourceUniform.isPresent()
                && targetUniform.get().equals(sourceUniform.get());
    }
}
