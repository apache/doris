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

import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.statistics.Statistics;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * PhysicalLazyMaterializeOlapScan
 */
public class PhysicalLazyMaterializeOlapScan extends PhysicalOlapScan {

    private PhysicalOlapScan scan;
    private SlotReference rowId;
    private final List<Slot> lazySlots;
    private List<Slot> output;

    /**
     * constr
     */

    public PhysicalLazyMaterializeOlapScan(PhysicalOlapScan physicalOlapScan,
            SlotReference rowId, List<Slot> lazySlots) {
        super(physicalOlapScan.getRelationId(), physicalOlapScan.getTable(), physicalOlapScan.getQualifier(),
                physicalOlapScan.getSelectedIndexId(),
                physicalOlapScan.getSelectedTabletIds(),
                physicalOlapScan.getSelectedPartitionIds(),
                physicalOlapScan.getDistributionSpec(),
                physicalOlapScan.getPreAggStatus(),
                physicalOlapScan.getBaseOutputs(),
                physicalOlapScan.getGroupExpression(),
                null,
                physicalOlapScan.getPhysicalProperties(),
                physicalOlapScan.getStats(),
                physicalOlapScan.getTableSample(),
                physicalOlapScan.getOperativeSlots());
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
        if (output == null) {
            output = ImmutableList.<Slot>builder()
                    .addAll(scan.getOperativeSlots())
                    .add(rowId).build();
        }
        return output;
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
            getAppliedRuntimeFilters()
                    .stream().forEach(rf -> shapeBuilder.append(" RF").append(rf.getId().asInt()));
        }
        return shapeBuilder.toString();
    }

    @Override
    public PhysicalLazyMaterializeOlapScan withPhysicalPropertiesAndStats(PhysicalProperties physicalProperties,
            Statistics statistics) {
        return new PhysicalLazyMaterializeOlapScan(scan, rowId, lazySlots);
    }

    public SlotReference getRowId() {
        return rowId;
    }
}
