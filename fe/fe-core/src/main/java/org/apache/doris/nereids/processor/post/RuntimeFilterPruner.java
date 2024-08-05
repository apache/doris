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

package org.apache.doris.nereids.processor.post;

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.AbstractPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalAssertNumRows;
import org.apache.doris.nereids.trees.plans.physical.PhysicalCTEAnchor;
import org.apache.doris.nereids.trees.plans.physical.PhysicalFilter;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashAggregate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalIntersect;
import org.apache.doris.nereids.trees.plans.physical.PhysicalLimit;
import org.apache.doris.nereids.trees.plans.physical.PhysicalNestedLoopJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalSetOperation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalTopN;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.statistics.ColumnStatistic;
import org.apache.doris.statistics.Statistics;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Set;

/**
 * Doris generates RFs (runtime filter) on Join node to reduce the probe table at scan stage.
 * But some RFs have no effect, because its selectivity is 100%. This pr will remove them.
 * A RF is effective if
 *
 * 1. the build column value range covers part of that of probe column, OR
 * 2. the build column ndv is less than that of probe column, OR
 * 3. the build column's ColumnStats.selectivity < 1, OR
 * 4. the build column is reduced by another RF, which satisfies above criterions.
 *
 * TODO: item 2 is not used since the estimation is not accurate now.
 */
public class RuntimeFilterPruner extends PlanPostProcessor {

    @Override
    public Plan visit(Plan plan, CascadesContext context) {
        if (!plan.children().isEmpty()) {
            Preconditions.checkArgument(plan.children().size() == 1,
                    plan.getClass().getSimpleName()
                    + " has more than one child, needs its own visitor implementation");
            plan.child(0).accept(this, context);
            if (context.getRuntimeFilterContext().isEffectiveSrcNode(plan.child(0))) {
                RuntimeFilterContext.EffectiveSrcType childType = context.getRuntimeFilterContext()
                        .getEffectiveSrcType(plan.child(0));
                context.getRuntimeFilterContext().addEffectiveSrcNode(plan, childType);
            }
        }
        return plan;
    }

    @Override
    public PhysicalSetOperation visitPhysicalSetOperation(PhysicalSetOperation setOperation, CascadesContext context) {
        for (Plan child : setOperation.children()) {
            child.accept(this, context);
        }
        return setOperation;
    }

    @Override
    public PhysicalIntersect visitPhysicalIntersect(PhysicalIntersect intersect, CascadesContext context) {
        for (Plan child : intersect.children()) {
            child.accept(this, context);
        }
        context.getRuntimeFilterContext().addEffectiveSrcNode(intersect, RuntimeFilterContext.EffectiveSrcType.NATIVE);
        return intersect;
    }

    @Override
    public PhysicalNestedLoopJoin<? extends Plan, ? extends Plan> visitPhysicalNestedLoopJoin(
            PhysicalNestedLoopJoin<? extends Plan, ? extends Plan> join,
            CascadesContext context) {
        join.right().accept(this, context);
        join.left().accept(this, context);
        if (context.getRuntimeFilterContext().isEffectiveSrcNode(join.child(0))) {
            RuntimeFilterContext.EffectiveSrcType childType = context.getRuntimeFilterContext()
                    .getEffectiveSrcType(join.child(0));
            context.getRuntimeFilterContext().addEffectiveSrcNode(join, childType);
        }
        return join;
    }

    @Override
    public PhysicalCTEAnchor<? extends Plan, ? extends Plan> visitPhysicalCTEAnchor(
            PhysicalCTEAnchor<? extends Plan, ? extends Plan> cteAnchor,
            CascadesContext context) {
        cteAnchor.child(0).accept(this, context);
        cteAnchor.child(1).accept(this, context);
        return cteAnchor;
    }

    @Override
    public PhysicalTopN<? extends Plan> visitPhysicalTopN(PhysicalTopN<? extends Plan> topN, CascadesContext context) {
        topN.child().accept(this, context);
        context.getRuntimeFilterContext().addEffectiveSrcNode(topN, RuntimeFilterContext.EffectiveSrcType.NATIVE);
        return topN;
    }

    public PhysicalLimit<? extends Plan> visitPhysicalLimit(
            PhysicalLimit<? extends Plan> limit,
            CascadesContext context) {
        limit.child().accept(this, context);
        context.getRuntimeFilterContext().addEffectiveSrcNode(limit, RuntimeFilterContext.EffectiveSrcType.NATIVE);
        return limit;
    }

    @Override
    public PhysicalHashJoin<? extends Plan, ? extends Plan> visitPhysicalHashJoin(
            PhysicalHashJoin<? extends Plan, ? extends Plan> join,
            CascadesContext context) {
        join.right().accept(this, context);
        RuntimeFilterContext rfContext = context.getRuntimeFilterContext();
        if (rfContext.isEffectiveSrcNode(join.right())) {
            boolean enableExpand = false;
            if (ConnectContext.get() != null) {
                enableExpand = ConnectContext.get().getSessionVariable().expandRuntimeFilterByInnerJoin;
            }
            if (enableExpand && rfContext.getEffectiveSrcType(join.right())
                    == RuntimeFilterContext.EffectiveSrcType.REF) {
                RuntimeFilterContext.ExpandRF expand = rfContext.getExpandRfByJoin(join);
                if (expand != null) {
                    Set<ExprId> outputExprIdOfExpandTargets = Sets.newHashSet();
                    outputExprIdOfExpandTargets.addAll(expand.target1.getOutputExprIds());
                    outputExprIdOfExpandTargets.addAll(expand.target2.getOutputExprIds());
                    rfContext.getTargetExprIdByFilterJoin(join)
                            .stream().filter(exprId -> outputExprIdOfExpandTargets.contains(exprId))
                            .forEach(exprId -> rfContext.removeFilters(exprId, join));
                }
            }
            RuntimeFilterContext.EffectiveSrcType childType =
                    rfContext.getEffectiveSrcType(join.right());
            context.getRuntimeFilterContext().addEffectiveSrcNode(join, childType);
        } else {
            List<ExprId> exprIds = rfContext.getTargetExprIdByFilterJoin(join);
            if (exprIds != null && !exprIds.isEmpty()) {
                boolean isEffective = false;
                for (Expression expr : join.getEqualToConjuncts()) {
                    if (isEffectiveRuntimeFilter((EqualTo) expr, join)) {
                        isEffective = true;
                    }
                }
                if (!isEffective) {
                    exprIds.stream().forEach(exprId -> rfContext.removeFilters(exprId, join));
                }
            }
        }
        join.left().accept(this, context);
        if (rfContext.isEffectiveSrcNode(join.left())) {
            RuntimeFilterContext.EffectiveSrcType leftType =
                    rfContext.getEffectiveSrcType(join.left());
            RuntimeFilterContext.EffectiveSrcType rightType =
                    rfContext.getEffectiveSrcType(join.right());
            if (rightType == null
                    || (rightType == RuntimeFilterContext.EffectiveSrcType.REF
                        && leftType == RuntimeFilterContext.EffectiveSrcType.NATIVE)) {
                rfContext.addEffectiveSrcNode(join, leftType);
            }
        }
        return join;
    }

    private boolean isVisibleColumn(Slot slot) {
        if (slot instanceof SlotReference) {
            SlotReference slotReference = (SlotReference) slot;
            if (slotReference.getColumn().isPresent()) {
                return slotReference.getColumn().get().isVisible();
            }
        }
        return true;
    }

    @Override
    public PhysicalFilter visitPhysicalFilter(PhysicalFilter<? extends Plan> filter, CascadesContext context) {
        filter.child().accept(this, context);

        boolean visibleFilter = false;

        for (Expression expr : filter.getExpressions()) {
            for (Slot inputSlot : expr.getInputSlots()) {
                if (isVisibleColumn(inputSlot)) {
                    visibleFilter = true;
                    break;
                }
            }
            if (visibleFilter) {
                break;
            }
        }
        if (visibleFilter) {
            // skip filters like: __DORIS_DELETE_SIGN__ = 0
            context.getRuntimeFilterContext().addEffectiveSrcNode(filter, RuntimeFilterContext.EffectiveSrcType.NATIVE);
        }
        return filter;
    }

    @Override
    public PhysicalRelation visitPhysicalRelation(PhysicalRelation scan, CascadesContext context) {
        RuntimeFilterContext rfCtx = context.getRuntimeFilterContext();
        List<Slot> slots = rfCtx.getTargetListByScan(scan);
        for (Slot slot : slots) {
            //if this scan node is the target of any effective RF, it is effective source
            if (!rfCtx.getTargetExprIdToFilter().get(slot.getExprId()).isEmpty()) {
                context.getRuntimeFilterContext().addEffectiveSrcNode(scan, RuntimeFilterContext.EffectiveSrcType.REF);
                break;
            }
        }
        return scan;
    }

    @Override
    public PhysicalAssertNumRows visitPhysicalAssertNumRows(PhysicalAssertNumRows<? extends Plan> assertNumRows,
            CascadesContext context) {
        assertNumRows.child().accept(this, context);
        context.getRuntimeFilterContext().addEffectiveSrcNode(assertNumRows,
                RuntimeFilterContext.EffectiveSrcType.NATIVE);
        return assertNumRows;
    }

    @Override
    public PhysicalHashAggregate visitPhysicalHashAggregate(PhysicalHashAggregate<? extends Plan> aggregate,
                                                            CascadesContext context) {
        RuntimeFilterContext ctx = context.getRuntimeFilterContext();
        aggregate.child().accept(this, context);
        // q1: A join (select x, sum(y) as z from B group by x) T on A.a = T.x
        // q2: A join (select x, sum(y) as z from B group by x) T on A.a = T.z
        // RF on q1 is not effective, but RF on q2 is. But q1 is a more generous pattern, and hence agg is not
        // regarded as an effective source. Let this RF judge by ndv.
        if (ctx.isEffectiveSrcNode(aggregate.child(0))) {
            RuntimeFilterContext.EffectiveSrcType childType = ctx.getEffectiveSrcType(aggregate.child());
            ctx.addEffectiveSrcNode(aggregate, childType);
        }
        return aggregate;
    }

    /**
     * consider L join R on L.a=R.b
     * runtime-filter: L.a<-R.b is effective,
     * if rf could reduce tuples of L,
     * 1. some L.a distinctive value are not covered by R.b, or
     * 2. if there is a effective RF applied on R
     *
     * TODO: min-max
     * @param equalTo join condition
     * @param join join node
     * @return true if runtime-filter is effective
     */
    private boolean isEffectiveRuntimeFilter(EqualTo equalTo, PhysicalHashJoin join) {
        Statistics leftStats = ((AbstractPlan) join.child(0)).getStats();
        Statistics rightStats = ((AbstractPlan) join.child(1)).getStats();
        Set<Slot> leftSlots = equalTo.child(0).getInputSlots();
        if (leftSlots.size() > 1) {
            return false;
        }
        Set<Slot> rightSlots = equalTo.child(1).getInputSlots();
        if (rightSlots.size() > 1) {
            return false;
        }
        Slot leftSlot = leftSlots.iterator().next();
        Slot rightSlot = rightSlots.iterator().next();
        ColumnStatistic probeColumnStat = leftStats.findColumnStatistics(leftSlot);
        ColumnStatistic buildColumnStat = rightStats.findColumnStatistics(rightSlot);
        //TODO remove these code when we ensure left child if from probe side
        if (probeColumnStat == null || buildColumnStat == null) {
            probeColumnStat = leftStats.findColumnStatistics(rightSlot);
            buildColumnStat = rightStats.findColumnStatistics(leftSlot);
            if (probeColumnStat == null || buildColumnStat == null) {
                return false;
            }
        }

        if (probeColumnStat.isUnKnown || buildColumnStat.isUnKnown) {
            return false;
        }

        double buildNdvInProbeRange = buildColumnStat.ndvIntersection(probeColumnStat);
        return probeColumnStat.ndv > buildNdvInProbeRange * (1 + ColumnStatistic.STATS_ERROR);
    }
}
