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
import org.apache.doris.nereids.properties.DistributionSpec;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.plans.ObjectId;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.PreAggStatus;
import org.apache.doris.nereids.trees.plans.algebra.OlapScan;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.statistics.Statistics;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Physical olap scan plan.
 */
public class PhysicalOlapScan extends PhysicalRelation implements OlapScan {

    public static final String DEFERRED_MATERIALIZED_SLOTS = "deferred_materialized_slots";

    private final OlapTable olapTable;
    private final DistributionSpec distributionSpec;
    private final long selectedIndexId;
    private final ImmutableList<Long> selectedTabletIds;
    private final ImmutableList<Long> selectedPartitionIds;
    private final PreAggStatus preAggStatus;

    /**
     * Constructor for PhysicalOlapScan.
     */
    public PhysicalOlapScan(ObjectId id, OlapTable olapTable, List<String> qualifier, long selectedIndexId,
            List<Long> selectedTabletIds, List<Long> selectedPartitionIds, DistributionSpec distributionSpec,
            PreAggStatus preAggStatus, Optional<GroupExpression> groupExpression, LogicalProperties logicalProperties) {
        this(id, olapTable, qualifier, selectedIndexId, selectedTabletIds, selectedPartitionIds, distributionSpec,
                preAggStatus, groupExpression, logicalProperties, null, null);
    }

    /**
     * Constructor for PhysicalOlapScan.
     */
    public PhysicalOlapScan(ObjectId id, OlapTable olapTable, List<String> qualifier, long selectedIndexId,
            List<Long> selectedTabletIds, List<Long> selectedPartitionIds, DistributionSpec distributionSpec,
            PreAggStatus preAggStatus, Optional<GroupExpression> groupExpression, LogicalProperties logicalProperties,
            PhysicalProperties physicalProperties, Statistics statistics) {
        super(id, PlanType.PHYSICAL_OLAP_SCAN, qualifier, groupExpression, logicalProperties, physicalProperties,
                statistics);
        this.olapTable = olapTable;
        this.selectedIndexId = selectedIndexId;
        this.selectedTabletIds = ImmutableList.copyOf(selectedTabletIds);
        this.selectedPartitionIds = ImmutableList.copyOf(selectedPartitionIds);
        this.distributionSpec = distributionSpec;
        this.preAggStatus = preAggStatus;
    }

    @Override
    public long getSelectedIndexId() {
        return selectedIndexId;
    }

    @Override
    public List<Long> getSelectedTabletIds() {
        return selectedTabletIds;
    }

    public List<Long> getSelectedPartitionIds() {
        return selectedPartitionIds;
    }

    @Override
    public OlapTable getTable() {
        return olapTable;
    }

    public DistributionSpec getDistributionSpec() {
        return distributionSpec;
    }

    public PreAggStatus getPreAggStatus() {
        return preAggStatus;
    }

    @Override
    public String toString() {
        return Utils.toSqlString("PhysicalOlapScan[" + id.asInt() + "]" + getGroupIdAsString(),
                "qualified", Utils.qualifiedName(qualifier, olapTable.getName()),
                "stats", statistics
        );
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass() || !super.equals(o)) {
            return false;
        }
        PhysicalOlapScan that = ((PhysicalOlapScan) o);
        return Objects.equals(selectedIndexId, that.selectedIndexId)
                && Objects.equals(selectedTabletIds, that.selectedPartitionIds)
                && Objects.equals(selectedPartitionIds, that.selectedPartitionIds)
                && Objects.equals(olapTable, that.olapTable);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, selectedIndexId, selectedPartitionIds, selectedTabletIds, olapTable);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalOlapScan(this, context);
    }

    @Override
    public PhysicalOlapScan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new PhysicalOlapScan(id, olapTable, qualifier, selectedIndexId, selectedTabletIds,
                selectedPartitionIds, distributionSpec, preAggStatus, groupExpression, getLogicalProperties());
    }

    @Override
    public PhysicalOlapScan withLogicalProperties(Optional<LogicalProperties> logicalProperties) {
        return new PhysicalOlapScan(id, olapTable, qualifier, selectedIndexId, selectedTabletIds,
                selectedPartitionIds, distributionSpec, preAggStatus, Optional.empty(),
                logicalProperties.get());
    }

    @Override
    public PhysicalOlapScan withPhysicalPropertiesAndStats(
            PhysicalProperties physicalProperties, Statistics statistics) {
        return new PhysicalOlapScan(id, olapTable, qualifier, selectedIndexId, selectedTabletIds,
                selectedPartitionIds, distributionSpec, preAggStatus, groupExpression,
                getLogicalProperties(), physicalProperties, statistics);
    }

    @Override
    public String shapeInfo() {
        StringBuilder builder = new StringBuilder();
        builder.append(this.getClass().getSimpleName()).append("[").append(olapTable.getName()).append("]");
        return builder.toString();
    }

}
