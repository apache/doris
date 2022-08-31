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
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Physical olap scan plan.
 */
public class PhysicalOlapScan extends PhysicalRelation {
    private final OlapTable olapTable;
    private final DistributionSpec distributionSpec;
    private final long selectedIndexId;
    private final List<Long> selectedTabletId;
    private final List<Long> selectedPartitionId;


    /**
     * Constructor for PhysicalOlapScan.
     *
     * @param olapTable OlapTable in Doris
     * @param qualifier qualifier of table name
     */
    public PhysicalOlapScan(OlapTable olapTable, List<String> qualifier, long selectedIndexId,
            List<Long> selectedTabletId, List<Long> selectedPartitionId, DistributionSpec distributionSpec,
            Optional<GroupExpression> groupExpression, LogicalProperties logicalProperties) {
        super(PlanType.PHYSICAL_OLAP_SCAN, qualifier, groupExpression, logicalProperties);

        this.olapTable = olapTable;
        this.selectedIndexId = selectedIndexId;
        this.selectedTabletId = selectedTabletId;
        this.selectedPartitionId = selectedPartitionId;
        this.distributionSpec = distributionSpec;
    }

    public long getSelectedIndexId() {
        return selectedIndexId;
    }

    public List<Long> getSelectedTabletId() {
        return selectedTabletId;
    }

    public List<Long> getSelectedPartitionId() {
        return selectedPartitionId;
    }

    public OlapTable getTable() {
        return olapTable;
    }

    public DistributionSpec getDistributionSpec() {
        return distributionSpec;
    }

    @Override
    public String toString() {
        return Utils.toSqlString("PhysicalOlapScan",
                "qualifier", Utils.qualifiedName(qualifier, olapTable.getName()),
                "output", getOutput()
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
        PhysicalOlapScan that = (PhysicalOlapScan) o;
        return selectedIndexId == that.selectedIndexId
                && Objects.equals(selectedTabletId, that.selectedTabletId)
                && Objects.equals(selectedPartitionId, that.selectedPartitionId)
                && Objects.equals(olapTable, that.olapTable);
    }

    @Override
    public int hashCode() {
        return Objects.hash(selectedIndexId, selectedPartitionId, selectedTabletId, olapTable);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalOlapScan(this, context);
    }

    @Override
    public Plan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new PhysicalOlapScan(olapTable, qualifier, selectedIndexId, selectedTabletId, selectedPartitionId,
                distributionSpec, groupExpression, logicalProperties);
    }

    @Override
    public Plan withLogicalProperties(Optional<LogicalProperties> logicalProperties) {
        return new PhysicalOlapScan(olapTable, qualifier, selectedIndexId, selectedTabletId, selectedPartitionId,
                distributionSpec, Optional.empty(), logicalProperties.get());
    }
}
