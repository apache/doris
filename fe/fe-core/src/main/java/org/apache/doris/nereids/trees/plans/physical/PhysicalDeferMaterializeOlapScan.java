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

import org.apache.doris.catalog.OlapTable;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.OlapScan;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.statistics.Statistics;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * use for defer materialize top n
 */
public class PhysicalDeferMaterializeOlapScan extends PhysicalCatalogRelation implements OlapScan {

    private final PhysicalOlapScan physicalOlapScan;

    ///////////////////////////////////////////////////////////////////////////
    // Members for defer materialize for top-n opt.
    ///////////////////////////////////////////////////////////////////////////
    private final Set<ExprId> deferMaterializeSlotIds;
    private final SlotReference columnIdSlot;

    public PhysicalDeferMaterializeOlapScan(PhysicalOlapScan physicalOlapScan,
            Set<ExprId> deferMaterializeSlotIds, SlotReference columnIdSlot,
            Optional<GroupExpression> groupExpression, LogicalProperties logicalProperties) {
        this(physicalOlapScan, deferMaterializeSlotIds, columnIdSlot, groupExpression, logicalProperties, null, null);
    }

    /**
     * constructor
     */
    public PhysicalDeferMaterializeOlapScan(PhysicalOlapScan physicalOlapScan,
            Set<ExprId> deferMaterializeSlotIds, SlotReference columnIdSlot,
            Optional<GroupExpression> groupExpression, LogicalProperties logicalProperties,
            PhysicalProperties physicalProperties, Statistics statistics) {
        super(physicalOlapScan.getRelationId(), physicalOlapScan.getType(),
                physicalOlapScan.getTable(), physicalOlapScan.getQualifier(),
                groupExpression, logicalProperties, physicalProperties, statistics,
                ImmutableList.of());
        this.physicalOlapScan = physicalOlapScan;
        this.deferMaterializeSlotIds = deferMaterializeSlotIds;
        this.columnIdSlot = columnIdSlot;
    }

    public PhysicalOlapScan getPhysicalOlapScan() {
        return physicalOlapScan;
    }

    public Set<ExprId> getDeferMaterializeSlotIds() {
        return deferMaterializeSlotIds;
    }

    public SlotReference getColumnIdSlot() {
        return columnIdSlot;
    }

    @Override
    public OlapTable getTable() {
        return physicalOlapScan.getTable();
    }

    @Override
    public long getSelectedIndexId() {
        return physicalOlapScan.getSelectedIndexId();
    }

    @Override
    public List<Long> getSelectedPartitionIds() {
        return physicalOlapScan.getSelectedPartitionIds();
    }

    @Override
    public List<Long> getSelectedTabletIds() {
        return physicalOlapScan.getSelectedTabletIds();
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalDeferMaterializeOlapScan(this, context);
    }

    @Override
    public Plan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new PhysicalDeferMaterializeOlapScan(physicalOlapScan, deferMaterializeSlotIds, columnIdSlot,
                groupExpression, getLogicalProperties(), physicalProperties, statistics);
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        return new PhysicalDeferMaterializeOlapScan(physicalOlapScan, deferMaterializeSlotIds, columnIdSlot,
                groupExpression, logicalProperties.get(), physicalProperties, statistics);
    }

    @Override
    public PhysicalPlan withPhysicalPropertiesAndStats(PhysicalProperties physicalProperties, Statistics statistics) {
        return new PhysicalDeferMaterializeOlapScan(physicalOlapScan, deferMaterializeSlotIds, columnIdSlot,
                groupExpression, getLogicalProperties(), physicalProperties, statistics);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        PhysicalDeferMaterializeOlapScan that = (PhysicalDeferMaterializeOlapScan) o;
        return Objects.equals(physicalOlapScan, that.physicalOlapScan) && Objects.equals(
                deferMaterializeSlotIds, that.deferMaterializeSlotIds) && Objects.equals(columnIdSlot,
                that.columnIdSlot);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), physicalOlapScan, deferMaterializeSlotIds, columnIdSlot);
    }

    @Override
    public String toString() {
        return Utils.toSqlString("PhysicalDeferMaterializeOlapScan[" + id.asInt() + "]",
                "physicalOlapScan", physicalOlapScan,
                "deferMaterializeSlotIds", deferMaterializeSlotIds,
                "columnIdSlot", columnIdSlot
            );
    }
}
