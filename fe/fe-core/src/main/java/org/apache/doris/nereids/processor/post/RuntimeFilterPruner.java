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
import org.apache.doris.nereids.trees.plans.AbstractPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalAssertNumRows;
import org.apache.doris.nereids.trees.plans.physical.PhysicalDistribute;
import org.apache.doris.nereids.trees.plans.physical.PhysicalFilter;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashAggregate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalLimit;
import org.apache.doris.nereids.trees.plans.physical.PhysicalProject;
import org.apache.doris.nereids.trees.plans.physical.PhysicalQuickSort;
import org.apache.doris.nereids.trees.plans.physical.PhysicalRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalTopN;
import org.apache.doris.statistics.ColumnStatistic;
import org.apache.doris.statistics.Statistics;

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

    // *******************************
    // Physical plans
    // *******************************
    @Override
    public PhysicalHashAggregate visitPhysicalHashAggregate(
            PhysicalHashAggregate<? extends Plan> agg, CascadesContext context) {
        agg.child().accept(this, context);
        context.getRuntimeFilterContext().addEffectiveSrcNode(agg);
        return agg;
    }

    @Override
    public PhysicalQuickSort visitPhysicalQuickSort(PhysicalQuickSort<? extends Plan> sort, CascadesContext context) {
        sort.child().accept(this, context);
        if (context.getRuntimeFilterContext().isEffectiveSrcNode(sort.child())) {
            context.getRuntimeFilterContext().addEffectiveSrcNode(sort);
        }
        return sort;
    }

    @Override
    public PhysicalTopN visitPhysicalTopN(PhysicalTopN<? extends Plan> topN, CascadesContext context) {
        topN.child().accept(this, context);
        context.getRuntimeFilterContext().addEffectiveSrcNode(topN);
        return topN;
    }

    public PhysicalLimit visitPhysicalLimit(PhysicalLimit<? extends Plan> limit, CascadesContext context) {
        limit.child().accept(this, context);
        context.getRuntimeFilterContext().addEffectiveSrcNode(limit);
        return limit;
    }

    @Override
    public PhysicalHashJoin visitPhysicalHashJoin(PhysicalHashJoin<? extends Plan, ? extends Plan> join,
            CascadesContext context) {
        join.right().accept(this, context);
        if (context.getRuntimeFilterContext().isEffectiveSrcNode(join.right())) {
            context.getRuntimeFilterContext().addEffectiveSrcNode(join);
        } else {
            RuntimeFilterContext ctx = context.getRuntimeFilterContext();
            List<ExprId> exprIds = ctx.getTargetExprIdByFilterJoin(join);
            if (exprIds != null && !exprIds.isEmpty()) {
                boolean isEffective = false;
                for (Expression expr : join.getHashJoinConjuncts()) {
                    if (isEffectiveRuntimeFilter((EqualTo) expr, join)) {
                        isEffective = true;
                    }
                }
                if (!isEffective) {
                    exprIds.stream().forEach(exprId -> context.getRuntimeFilterContext().removeFilter(exprId, join));
                }
            }
        }
        join.left().accept(this, context);
        return join;
    }

    @Override
    public PhysicalProject visitPhysicalProject(PhysicalProject<? extends Plan> project, CascadesContext context) {
        project.child().accept(this, context);
        if (context.getRuntimeFilterContext().isEffectiveSrcNode(project.child())) {
            context.getRuntimeFilterContext().addEffectiveSrcNode(project);
        }
        return project;
    }

    @Override
    public PhysicalFilter visitPhysicalFilter(PhysicalFilter<? extends Plan> filter, CascadesContext context) {
        filter.child().accept(this, context);
        context.getRuntimeFilterContext().addEffectiveSrcNode(filter);
        return filter;
    }

    @Override
    public PhysicalRelation visitPhysicalRelation(PhysicalRelation scan, CascadesContext context) {
        RuntimeFilterContext rfCtx = context.getRuntimeFilterContext();
        List<Slot> slots = rfCtx.getTargetOnOlapScanNodeMap().get(scan.getRelationId());
        if (slots != null) {
            for (Slot slot : slots) {
                //if this scan node is the target of any effective RF, it is effective source
                if (!rfCtx.getTargetExprIdToFilter().get(slot.getExprId()).isEmpty()) {
                    context.getRuntimeFilterContext().addEffectiveSrcNode(scan);
                    break;
                }
            }
        }
        return scan;
    }

    // *******************************
    // Physical enforcer
    // *******************************
    public PhysicalDistribute visitPhysicalDistribute(PhysicalDistribute<? extends Plan> distribute,
            CascadesContext context) {
        distribute.child().accept(this, context);
        if (context.getRuntimeFilterContext().isEffectiveSrcNode(distribute.child())) {
            context.getRuntimeFilterContext().addEffectiveSrcNode(distribute);
        }
        return distribute;
    }

    public PhysicalAssertNumRows visitPhysicalAssertNumRows(PhysicalAssertNumRows<? extends Plan> assertNumRows,
            CascadesContext context) {
        assertNumRows.child().accept(this, context);
        return assertNumRows;
    }

    /**
     * consider L join R on L.a=R.b
     * runtime-filter: L.a<-R.b is effective,
     * if R.b.selectivity<1 or b is partly covered by a
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
        //without column statistics, we can not judge if the rf is effective.
        if (probeColumnStat.isUnKnown || buildColumnStat.isUnKnown) {
            return true;
        }
        return buildColumnStat.selectivity < 1
                || probeColumnStat.notEnclosed(buildColumnStat)
                || buildColumnStat.ndv < probeColumnStat.ndv * 0.95;
    }
}
