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

package org.apache.doris.nereids.trees.plans.physical;

import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.AbstractPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.statistics.Statistics;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

/** Olap scan wrapper that replaces deferred columns with a row-id used by the materialize node. */
public class PhysicalLazyMaterializeOlapScan extends PhysicalOlapScan {

    private final PhysicalOlapScan scan;
    private final SlotReference rowId;
    private final List<Slot> lazySlots;

    public PhysicalLazyMaterializeOlapScan(PhysicalOlapScan physicalOlapScan,
            SlotReference rowId, List<Slot> lazySlots) {
        this(physicalOlapScan, rowId, lazySlots, physicalOlapScan.getGroupExpression(), null,
                physicalOlapScan.getPhysicalProperties(), physicalOlapScan.getStats());
    }

    private PhysicalLazyMaterializeOlapScan(PhysicalOlapScan physicalOlapScan,
            SlotReference rowId, List<Slot> lazySlots, Optional<GroupExpression> groupExpression,
            LogicalProperties logicalProperties, PhysicalProperties physicalProperties, Statistics statistics) {
        super(physicalOlapScan.getRelationId(), physicalOlapScan.getTable(), physicalOlapScan.getQualifier(),
                physicalOlapScan.getSelectedIndexId(),
                physicalOlapScan.getSelectedTabletIds(),
                physicalOlapScan.getSelectedPartitionIds(),
                physicalOlapScan.getDistributionSpec(),
                physicalOlapScan.getPreAggStatus(),
                physicalOlapScan.getBaseOutputs(),
                groupExpression,
                logicalProperties,
                physicalProperties,
                statistics,
                physicalOlapScan.getTableSample(),
                physicalOlapScan.getOperativeSlots(),
                physicalOlapScan.getVirtualColumns(),
                physicalOlapScan.getScoreOrderKeys(),
                physicalOlapScan.getScoreLimit(),
                physicalOlapScan.getScoreRangeInfo(),
                physicalOlapScan.getAnnOrderKeys(),
                physicalOlapScan.getAnnLimit()
        );
        this.scan = physicalOlapScan;
        this.rowId = rowId;
        this.lazySlots = ImmutableList.copyOf(lazySlots);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalLazyMaterializeOlapScan(this, context);
    }

    @Override
    public List<Slot> computeOutput() {
        ImmutableList.Builder<Slot> output = ImmutableList.builder();
        for (Slot slot : scan.getOutput()) {
            if (!lazySlots.contains(slot)) {
                output.add(slot);
            }
        }
        return output.add(rowId).build();
    }

    public PhysicalOlapScan getScan() {
        return scan;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("PhysicalLazyMaterializeOlapScan[")
                .append(scan.toString());

        if (!getAppliedRuntimeFilters().isEmpty()) {
            getAppliedRuntimeFilters().forEach(rf -> sb.append(" RF").append(rf.getId().asInt()));
        }
        sb.append("]");
        return sb.toString();
    }

    @Override
    public String shapeInfo() {
        StringBuilder shapeBuilder = new StringBuilder();
        shapeBuilder.append(this.getClass().getSimpleName())
                .append("[").append(scan.table.getName()).append(" lazySlots:")
                .append(ExpressionUtils.slotListShapeInfo(lazySlots))
                .append("]");
        if (!getAppliedRuntimeFilters().isEmpty()) {
            shapeBuilder.append(" apply RFs:");
            getAppliedRuntimeFilters().forEach(rf -> shapeBuilder.append(" RF").append(rf.getId().asInt()));
        }
        return shapeBuilder.toString();
    }

    public SlotReference getRowId() {
        return rowId;
    }

    public List<Slot> getLazySlots() {
        return lazySlots;
    }

    @Override
    public PhysicalLazyMaterializeOlapScan withGroupExpression(Optional<GroupExpression> groupExpression) {
        PhysicalOlapScan copiedScan = scan.withGroupExpression(groupExpression);
        return AbstractPlan.copyWithSameId(this,
                () -> new PhysicalLazyMaterializeOlapScan(copiedScan, rowId, lazySlots,
                        groupExpression, getLogicalProperties(), physicalProperties, statistics));
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        PhysicalOlapScan copiedScan = (PhysicalOlapScan) scan.withGroupExprLogicalPropChildren(
                groupExpression, logicalProperties, children);
        return AbstractPlan.copyWithSameId(this,
                () -> new PhysicalLazyMaterializeOlapScan(copiedScan, rowId, lazySlots,
                        groupExpression, logicalProperties.orElse(null), physicalProperties, statistics));
    }

    @Override
    public PhysicalLazyMaterializeOlapScan withPhysicalPropertiesAndStats(
            PhysicalProperties physicalProperties, Statistics statistics) {
        PhysicalOlapScan copiedScan = scan.withPhysicalPropertiesAndStats(physicalProperties, statistics);
        return AbstractPlan.copyWithSameId(this,
                () -> new PhysicalLazyMaterializeOlapScan(copiedScan, rowId, lazySlots,
                        groupExpression, getLogicalProperties(), physicalProperties, statistics));
    }
}
