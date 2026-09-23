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
import org.apache.doris.nereids.trees.expressions.CTEId;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.AbstractPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalAssertNumRows;
import org.apache.doris.nereids.trees.plans.physical.PhysicalCTEAnchor;
import org.apache.doris.nereids.trees.plans.physical.PhysicalCTEConsumer;
import org.apache.doris.nereids.trees.plans.physical.PhysicalFilter;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashAggregate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalIntersect;
import org.apache.doris.nereids.trees.plans.physical.PhysicalLimit;
import org.apache.doris.nereids.trees.plans.physical.PhysicalNestedLoopJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPartitionTopN;
import org.apache.doris.nereids.trees.plans.physical.PhysicalRecursiveUnion;
import org.apache.doris.nereids.trees.plans.physical.PhysicalRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalSetOperation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalTopN;
import org.apache.doris.nereids.trees.plans.physical.RuntimeFilter;
import org.apache.doris.statistics.model.ColumnStatistic;
import org.apache.doris.statistics.model.Statistics;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Doris generates RFs (runtime filter) on Join node to reduce the probe table at scan stage.
 * But some RFs have no effect, because its selectivity is 100%. This pr will remove them.
 * A RF is effective if
 *
 * 1. the build column value range covers part of that of probe column, OR
 * 2. the build column ndv is less than that of probe column, OR
 * 3. the build column's ColumnStats.selectivity < 1, OR
 * 4. the build column is reduced by another RF, which satisfies above criterions, OR
 * 5. the RF can eliminate whole scan ranges.
 *
 * TODO: item 2 is not used since the estimation is not accurate now.
 */
public class RuntimeFilterPruner extends PlanPostProcessor {

    // Records CTE producers whose subtree is an effective RF source (e.g. contains TopN/Limit/
    // a visible-column filter, i.e. its output is bounded/selective). Keyed by CTEId, filled when
    // visiting the producer side of a CTE anchor, consumed when visiting its consumers.
    private final Map<CTEId, RuntimeFilterContext.EffectiveSrcType> effectiveCteProducers = new HashMap<>();

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
    public PhysicalRecursiveUnion visitPhysicalRecursiveUnion(
            PhysicalRecursiveUnion<? extends Plan, ? extends Plan> recursiveUnion, CascadesContext context) {
        for (Plan child : recursiveUnion.children()) {
            child.accept(this, context);
        }
        return recursiveUnion;
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
        // Visit the producer subtree first: if its root is an effective RF source
        // (bounded/selective output, e.g. TopN/Limit/visible-column filter/global agg),
        // record it so that consumers of this CTE can inherit the effectiveness.
        // Without this, a join whose build side is a CTE consumer of such a producer gets
        // its runtime filters pruned as "ineffective" merely because the consumer's
        // statistics are unknown (always the case for external tables).
        cteAnchor.child(0).accept(this, context);
        RuntimeFilterContext rfCtx = context.getRuntimeFilterContext();
        if (rfCtx.isEffectiveSrcNode(cteAnchor.child(0))) {
            effectiveCteProducers.put(cteAnchor.getCteId(), rfCtx.getEffectiveSrcType(cteAnchor.child(0)));
        }
        cteAnchor.child(1).accept(this, context);
        return cteAnchor;
    }

    @Override
    public PhysicalCTEConsumer visitPhysicalCTEConsumer(PhysicalCTEConsumer consumer, CascadesContext context) {
        RuntimeFilterContext rfCtx = context.getRuntimeFilterContext();
        // Inherit effectiveness recorded from the producer subtree (see visitPhysicalCTEAnchor).
        RuntimeFilterContext.EffectiveSrcType producerType = effectiveCteProducers.get(consumer.getCteId());
        if (producerType != null) {
            rfCtx.addEffectiveSrcNode(consumer, producerType);
        }
        // A consumer is also a relation that can be the target of RFs.
        List<Slot> slots = rfCtx.getTargetListByScan(consumer);
        for (Slot slot : slots) {
            if (!rfCtx.getTargetExprIdToFilter().get(slot.getExprId()).isEmpty()) {
                rfCtx.addEffectiveSrcNode(consumer, RuntimeFilterContext.EffectiveSrcType.REF);
                break;
            }
        }
        return consumer;
    }

    @Override
    public PhysicalPartitionTopN<? extends Plan> visitPhysicalPartitionTopN(
            PhysicalPartitionTopN<? extends Plan> partitionTopN, CascadesContext context) {
        partitionTopN.child().accept(this, context);
        // Same rationale as PhysicalTopN: bounded output (at most partitionLimit rows per group)
        // makes RFs built from it highly selective regardless of statistics.
        context.getRuntimeFilterContext().addEffectiveSrcNode(partitionTopN,
                RuntimeFilterContext.EffectiveSrcType.NATIVE);
        return partitionTopN;
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
            RuntimeFilterContext.EffectiveSrcType childType =
                    rfContext.getEffectiveSrcType(join.right());
            context.getRuntimeFilterContext().addEffectiveSrcNode(join, childType);
        } else {
            List<ExprId> exprIds = rfContext.getTargetExprIdByFilterJoin(join);
            if (exprIds != null && !exprIds.isEmpty()) {
                // Check the row-level filtering effectiveness of runtime filters, not partition/bucket pruning.
                // A filter may still prune whole scan ranges even when this check returns false.
                boolean hasEffectiveRowFilter = false;
                for (Expression expr : join.getEqualToConjuncts()) {
                    if (isEffectiveRuntimeFilter((EqualTo) expr, join)) {
                        hasEffectiveRowFilter = true;
                        break;
                    }
                }
                if (!hasEffectiveRowFilter) {
                    // Scan-range pruning is target-specific. Keep only the filters whose own target
                    // can prune partitions or buckets instead of retaining every filter from the join.
                    for (RuntimeFilter filter : ImmutableList.copyOf(join.getRuntimeFilters())) {
                        if (!filter.canPruneScanRanges()) {
                            rfContext.removeFilter(filter, filter.getTargetSlot().getExprId());
                        }
                    }
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
            if (slotReference.getOriginalColumn().isPresent()) {
                return slotReference.getOriginalColumn().get().isVisible();
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
        // A global aggregate without any group-by key (e.g. the MAX(dt) in
        //   WHERE dt = (SELECT MAX(dt) FROM t))
        // produces exactly ONE output row, so an equi-join RF built from it reduces the probe side
        // to a single value and is always maximally selective -- regardless of column statistics.
        // This is the same "cardinality <= 1" guarantee that PhysicalAssertNumRows provides (and
        // which is treated as an effective source below); a no-group-by global aggregate lets the
        // planner elide the AssertNumRows, so we must recognize the aggregate itself as effective,
        // otherwise the RF gets pruned for tables without stats (e.g. Hive external tables) and the
        // "latest partition" pattern can never benefit from runtime-filter partition pruning.
        if (aggregate.getGroupByExpressions().isEmpty()) {
            aggregate.child(0).accept(this, context);
            ctx.addEffectiveSrcNode(aggregate, RuntimeFilterContext.EffectiveSrcType.NATIVE);
            return aggregate;
        }
        return propagateEffectiveSrc(aggregate, context);
    }

    /**
     * Visit child and propagate effective source type if applicable.
     * Shared by visitPhysicalHashAggregate.
     *
     * Note: agg is not regarded as an effective source itself. For example:
     * q1: A join (select x, sum(y) as z from B group by x) T on A.a = T.x
     * q2: A join (select x, sum(y) as z from B group by x) T on A.a = T.z
     * RF on q1 is not effective, but RF on q2 is. Let RF judge by ndv.
     */
    private <T extends Plan> T propagateEffectiveSrc(T aggregate, CascadesContext context) {
        RuntimeFilterContext ctx = context.getRuntimeFilterContext();
        aggregate.child(0).accept(this, context);
        if (ctx.isEffectiveSrcNode(aggregate.child(0))) {
            RuntimeFilterContext.EffectiveSrcType childType = ctx.getEffectiveSrcType(aggregate.child(0));
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
        if (leftStats == null || rightStats == null) {
            return true;
        }
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
