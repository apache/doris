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
import org.apache.doris.statistics.Statistics;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

/** File scan wrapper that replaces deferred columns with a row-id used by the materialize node. */
public class PhysicalLazyMaterializeFileScan extends PhysicalFileScan {
    private final PhysicalFileScan scan;
    private final SlotReference rowId;
    private final List<Slot> lazySlots;

    public PhysicalLazyMaterializeFileScan(PhysicalFileScan scan, SlotReference rowId, List<Slot> lazySlots) {
        this(scan, rowId, lazySlots, scan.getGroupExpression(), null,
                scan.getPhysicalProperties(), scan.getStats());
    }

    private PhysicalLazyMaterializeFileScan(PhysicalFileScan scan, SlotReference rowId, List<Slot> lazySlots,
            Optional<GroupExpression> groupExpression, LogicalProperties logicalProperties,
            PhysicalProperties physicalProperties, Statistics statistics) {
        super(scan.getRelationId(), scan.getTable(), scan.getQualifier(), scan.getDistributionSpec(),
                groupExpression, logicalProperties, physicalProperties, statistics,
                scan.getSelectedPartitions(), scan.getTableSample(),
                scan.getTableSnapshot(), scan.getOperativeSlots(),
                scan.getScanParams());
        this.scan = scan;
        this.rowId = rowId;
        this.lazySlots = ImmutableList.copyOf(lazySlots);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalLazyMaterializeFileScan(this, context);
    }

    @Override
    public List<String> getQualifier() {
        return scan.getQualifier();
    }

    @Override
    public List<Slot> computeOutput() {
        ImmutableList.Builder<Slot> outputBuilder = ImmutableList.builder();
        for (Slot slot : scan.getOutput()) {
            if (!lazySlots.contains(slot)) {
                outputBuilder.add(slot);
            }
        }
        return outputBuilder.add(rowId).build();
    }

    public PhysicalFileScan getScan() {
        return scan;
    }

    public SlotReference getRowId() {
        return rowId;
    }

    public List<Slot> getLazySlots() {
        return lazySlots;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("PhysicalLazyMaterializeFileScan[")
                .append(scan.toString());

        if (!getAppliedRuntimeFilters().isEmpty()) {
            getAppliedRuntimeFilters().forEach(rf -> sb.append(" RF").append(rf.getId().asInt()));
        }
        sb.append("]");
        return sb.toString();
    }

    @Override
    public PhysicalLazyMaterializeFileScan withGroupExpression(Optional<GroupExpression> groupExpression) {
        PhysicalFileScan copiedScan = scan.withGroupExpression(groupExpression);
        return AbstractPlan.copyWithSameId(this,
                () -> new PhysicalLazyMaterializeFileScan(copiedScan, rowId, lazySlots,
                        groupExpression, getLogicalProperties(), physicalProperties, statistics));
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        PhysicalFileScan copiedScan = (PhysicalFileScan) scan.withGroupExprLogicalPropChildren(
                groupExpression, logicalProperties, children);
        return AbstractPlan.copyWithSameId(this,
                () -> new PhysicalLazyMaterializeFileScan(copiedScan, rowId, lazySlots,
                        groupExpression, logicalProperties.orElse(null), physicalProperties, statistics));
    }

    @Override
    public PhysicalLazyMaterializeFileScan withPhysicalPropertiesAndStats(
            PhysicalProperties physicalProperties, Statistics statistics) {
        PhysicalFileScan copiedScan = scan.withPhysicalPropertiesAndStats(physicalProperties, statistics);
        return AbstractPlan.copyWithSameId(this,
                () -> new PhysicalLazyMaterializeFileScan(copiedScan, rowId, lazySlots,
                        groupExpression, getLogicalProperties(), physicalProperties, statistics));
    }
}
