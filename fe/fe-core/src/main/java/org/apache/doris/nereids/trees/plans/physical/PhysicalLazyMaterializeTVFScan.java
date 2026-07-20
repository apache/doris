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

/** TVF scan wrapper that replaces deferred columns with a row-id used by the materialize node. */
public class PhysicalLazyMaterializeTVFScan extends PhysicalTVFRelation {
    private final PhysicalTVFRelation scan;
    private final SlotReference rowId;
    private final List<Slot> lazySlots;

    public PhysicalLazyMaterializeTVFScan(PhysicalTVFRelation scan, SlotReference rowId, List<Slot> lazySlots) {
        this(scan, rowId, lazySlots, scan.getGroupExpression(), null,
                scan.getPhysicalProperties(), scan.getStats());
    }

    private PhysicalLazyMaterializeTVFScan(PhysicalTVFRelation scan, SlotReference rowId, List<Slot> lazySlots,
            Optional<GroupExpression> groupExpression, LogicalProperties logicalProperties,
            PhysicalProperties physicalProperties, Statistics statistics) {
        super(scan.getRelationId(), scan.getFunction(), scan.getOperativeSlots(),
                groupExpression, logicalProperties, physicalProperties, statistics);
        this.scan = scan;
        this.rowId = rowId;
        this.lazySlots = ImmutableList.copyOf(lazySlots);
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

    @Override
    public List<Slot> getOutput() {
        // PhysicalTVFRelation generates its output from the function schema. Use the wrapped scan's
        // stable slots so repeated output access does not generate new ExprIds for this decorated relation.
        return computeOutput();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("PhysicalLazyMaterializeTVFScan[")
                .append(scan.toString());

        if (!getAppliedRuntimeFilters().isEmpty()) {
            getAppliedRuntimeFilters().forEach(rf -> sb.append(" RF").append(rf.getId().asInt()));
        }
        sb.append("]");
        return sb.toString();
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalLazyMaterializeTVFScan(this, context);
    }

    public PhysicalTVFRelation getScan() {
        return scan;
    }

    public SlotReference getRowId() {
        return rowId;
    }

    public List<Slot> getLazySlots() {
        return lazySlots;
    }

    @Override
    public PhysicalPlan withPhysicalPropertiesAndStats(PhysicalProperties physicalProperties,
            Statistics statistics) {
        PhysicalTVFRelation tvfRelation = new PhysicalTVFRelation(relationId, function, operativeSlots,
                groupExpression, getLogicalProperties(), physicalProperties, statistics);
        return AbstractPlan.copyWithSameId(this, () ->
                new PhysicalLazyMaterializeTVFScan(tvfRelation, rowId, lazySlots,
                        groupExpression, getLogicalProperties(), physicalProperties, statistics));
    }

    @Override
    public PhysicalLazyMaterializeTVFScan withGroupExpression(Optional<GroupExpression> groupExpression) {
        PhysicalTVFRelation tvfRelation = new PhysicalTVFRelation(relationId, function, operativeSlots,
                groupExpression, getLogicalProperties(), physicalProperties, statistics);
        return AbstractPlan.copyWithSameId(this, () ->
                new PhysicalLazyMaterializeTVFScan(tvfRelation, rowId, lazySlots,
                        groupExpression, getLogicalProperties(), physicalProperties, statistics));
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        PhysicalTVFRelation tvfRelation = new PhysicalTVFRelation(relationId, function, operativeSlots,
                groupExpression, logicalProperties.orElse(null), physicalProperties, statistics);
        return AbstractPlan.copyWithSameId(this, () ->
                new PhysicalLazyMaterializeTVFScan(tvfRelation, rowId, lazySlots,
                        groupExpression, logicalProperties.orElse(null), physicalProperties, statistics));
    }
}
