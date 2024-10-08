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
import org.apache.doris.nereids.trees.TableSample;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.PreAggStatus;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.algebra.OlapScan;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.statistics.Statistics;

import com.google.common.collect.ImmutableList;
import org.json.JSONObject;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Physical olap scan plan.
 */
public class PhysicalOlapScan extends PhysicalCatalogRelation implements OlapScan {

    private final DistributionSpec distributionSpec;
    private final long selectedIndexId;
    private final ImmutableList<Long> selectedTabletIds;
    private final ImmutableList<Long> selectedPartitionIds;
    private final PreAggStatus preAggStatus;
    private final List<Slot> baseOutputs;
    private final Optional<TableSample> tableSample;

    /**
     * Constructor for PhysicalOlapScan.
     */
    public PhysicalOlapScan(RelationId id, OlapTable olapTable, List<String> qualifier, long selectedIndexId,
            List<Long> selectedTabletIds, List<Long> selectedPartitionIds, DistributionSpec distributionSpec,
            PreAggStatus preAggStatus, List<Slot> baseOutputs,
            Optional<GroupExpression> groupExpression, LogicalProperties logicalProperties,
            Optional<TableSample> tableSample) {
        this(id, olapTable, qualifier, selectedIndexId, selectedTabletIds, selectedPartitionIds, distributionSpec,
                preAggStatus, baseOutputs, groupExpression, logicalProperties, null, null,
                tableSample);
    }

    /**
     * Constructor for PhysicalOlapScan.
     */
    public PhysicalOlapScan(RelationId id, OlapTable olapTable, List<String> qualifier, long selectedIndexId,
            List<Long> selectedTabletIds, List<Long> selectedPartitionIds, DistributionSpec distributionSpec,
            PreAggStatus preAggStatus, List<Slot> baseOutputs,
            Optional<GroupExpression> groupExpression, LogicalProperties logicalProperties,
            PhysicalProperties physicalProperties, Statistics statistics, Optional<TableSample> tableSample) {
        super(id, PlanType.PHYSICAL_OLAP_SCAN, olapTable, qualifier,
                groupExpression, logicalProperties, physicalProperties, statistics);
        this.selectedIndexId = selectedIndexId;
        this.selectedTabletIds = ImmutableList.copyOf(selectedTabletIds);
        this.selectedPartitionIds = ImmutableList.copyOf(selectedPartitionIds);
        this.distributionSpec = distributionSpec;
        this.preAggStatus = preAggStatus;
        this.baseOutputs = ImmutableList.copyOf(baseOutputs);
        this.tableSample = tableSample;
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
        return (OlapTable) table;
    }

    public DistributionSpec getDistributionSpec() {
        return distributionSpec;
    }

    public PreAggStatus getPreAggStatus() {
        return preAggStatus;
    }

    public List<Slot> getBaseOutputs() {
        return baseOutputs;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        if (!getAppliedRuntimeFilters().isEmpty()) {
            getAppliedRuntimeFilters().forEach(rf -> builder.append(" RF").append(rf.getId().asInt()));
        }
        String index = "";
        if (selectedIndexId != getTable().getBaseIndexId()) {
            index = "(" + getTable().getIndexNameById(selectedIndexId) + ")";
        }
        String partitions = "";
        int partitionCount = this.table.getPartitionNames().size();
        if (selectedPartitionIds.size() != partitionCount) {
            partitions = " partitions(" + selectedPartitionIds.size() + "/" + partitionCount + ")";
        }
        return Utils.toSqlString("PhysicalOlapScan[" + table.getName() + index + partitions + "]"
                        + getGroupIdWithPrefix(),
                "stats", statistics, "RFs", builder
        );
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
        PhysicalOlapScan olapScan = (PhysicalOlapScan) o;
        return selectedIndexId == olapScan.selectedIndexId && Objects.equals(distributionSpec,
                olapScan.distributionSpec) && Objects.equals(selectedTabletIds, olapScan.selectedTabletIds)
                && Objects.equals(selectedPartitionIds, olapScan.selectedPartitionIds)
                && Objects.equals(preAggStatus, olapScan.preAggStatus) && Objects.equals(baseOutputs,
                olapScan.baseOutputs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), distributionSpec, selectedIndexId, selectedTabletIds,
                selectedPartitionIds,
                preAggStatus, baseOutputs);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalOlapScan(this, context);
    }

    @Override
    public PhysicalOlapScan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new PhysicalOlapScan(relationId, getTable(), qualifier, selectedIndexId, selectedTabletIds,
                selectedPartitionIds, distributionSpec, preAggStatus, baseOutputs,
                groupExpression, getLogicalProperties(), tableSample);
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        return new PhysicalOlapScan(relationId, getTable(), qualifier, selectedIndexId, selectedTabletIds,
                selectedPartitionIds, distributionSpec, preAggStatus, baseOutputs, groupExpression,
                logicalProperties.get(), tableSample);
    }

    @Override
    public PhysicalOlapScan withPhysicalPropertiesAndStats(
            PhysicalProperties physicalProperties, Statistics statistics) {
        return new PhysicalOlapScan(relationId, getTable(), qualifier, selectedIndexId, selectedTabletIds,
                selectedPartitionIds, distributionSpec, preAggStatus, baseOutputs, groupExpression,
                getLogicalProperties(), physicalProperties, statistics, tableSample);
    }

    @Override
    public JSONObject toJson() {
        JSONObject olapScan = super.toJson();
        JSONObject properties = new JSONObject();
        properties.put("OlapTable", ((OlapTable) table).toSimpleJson());
        properties.put("DistributionSpec", distributionSpec.toString());
        properties.put("SelectedIndexId", Long.toString(selectedIndexId));
        properties.put("SelectedTabletIds", selectedTabletIds.toString());
        properties.put("SelectedPartitionIds", selectedPartitionIds.toString());
        properties.put("PreAggStatus", preAggStatus.toString());
        olapScan.put("Properties", properties);
        return olapScan;
    }

    public Optional<TableSample> getTableSample() {
        return tableSample;
    }
}
