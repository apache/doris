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
import org.apache.doris.nereids.trees.plans.PreAggStatus;
import org.apache.doris.nereids.trees.plans.PushDownAggOperator;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.statistics.StatsDeriveResult;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Builder for PhysicalOlapScan
 */
public class PhysicalOlapScanBuilder {
    private RelationId id;
    private OlapTable olapTable;
    private List<String> qualifier;
    private long selectedIndexId;
    private List<Long> selectedTabletIds = Collections.emptyList();
    private List<Long> selectedPartitionIds = Collections.emptyList();
    private DistributionSpec distributionSpec;
    private PreAggStatus preAggStatus;
    private PushDownAggOperator pushDownAggOperator;
    private Optional<GroupExpression> groupExpression = Optional.empty();
    private LogicalProperties logicalProperties;
    private PhysicalProperties physicalProperties;
    private StatsDeriveResult statsDeriveResult;

    public PhysicalOlapScanBuilder setId(RelationId id) {
        this.id = id;
        return this;
    }

    public PhysicalOlapScanBuilder setOlapTable(OlapTable olapTable) {
        this.olapTable = olapTable;
        return this;
    }

    public PhysicalOlapScanBuilder setQualifier(List<String> qualifier) {
        this.qualifier = qualifier;
        return this;
    }

    public PhysicalOlapScanBuilder setSelectedIndexId(long selectedIndexId) {
        this.selectedIndexId = selectedIndexId;
        return this;
    }

    public PhysicalOlapScanBuilder setSelectedTabletIds(List<Long> selectedTabletIds) {
        this.selectedTabletIds = selectedTabletIds;
        return this;
    }

    public PhysicalOlapScanBuilder setSelectedPartitionIds(List<Long> selectedPartitionIds) {
        this.selectedPartitionIds = selectedPartitionIds;
        return this;
    }

    public PhysicalOlapScanBuilder setDistributionSpec(DistributionSpec distributionSpec) {
        this.distributionSpec = distributionSpec;
        return this;
    }

    public PhysicalOlapScanBuilder setPreAggStatus(PreAggStatus preAggStatus) {
        this.preAggStatus = preAggStatus;
        return this;
    }

    public PhysicalOlapScanBuilder setPushDownAggOperator(PushDownAggOperator pushDownAggOperator) {
        this.pushDownAggOperator = pushDownAggOperator;
        return this;
    }

    public PhysicalOlapScanBuilder setGroupExpression(Optional<GroupExpression> groupExpression) {
        this.groupExpression = groupExpression;
        return this;
    }

    public PhysicalOlapScanBuilder setLogicalProperties(LogicalProperties logicalProperties) {
        this.logicalProperties = logicalProperties;
        return this;
    }

    public PhysicalOlapScanBuilder setPhysicalProperties(PhysicalProperties physicalProperties) {
        this.physicalProperties = physicalProperties;
        return this;
    }

    public PhysicalOlapScanBuilder setStatsDeriveResult(StatsDeriveResult statsDeriveResult) {
        this.statsDeriveResult = statsDeriveResult;
        return this;
    }

    public PhysicalOlapScan build() {
        return new PhysicalOlapScan(id, olapTable, qualifier, selectedIndexId, selectedTabletIds, selectedPartitionIds,
                distributionSpec, preAggStatus, pushDownAggOperator, groupExpression, logicalProperties,
                physicalProperties, statsDeriveResult);
    }
}
