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

package org.apache.doris.nereids.trees.plans.logical;

import org.apache.doris.catalog.Table;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.trees.plans.PreAggStatus;
import org.apache.doris.nereids.trees.plans.PushDownAggOperator;
import org.apache.doris.nereids.trees.plans.RelationId;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Builder for LogicalOlapScan
 */
public class LogicalOlapScanBuilder {
    private RelationId id;
    private Table table;
    private List<String> qualifier = Collections.emptyList();
    private Optional<GroupExpression> groupExpression = Optional.empty();
    private Optional<LogicalProperties> logicalProperties = Optional.empty();
    private List<Long> selectedPartitionIds = Collections.emptyList();
    private boolean partitionPruned;
    private List<Long> selectedTabletIds = Collections.emptyList();
    private boolean tabletPruned;
    private List<Long> candidateIndexIds = Collections.emptyList();
    private boolean indexSelected;
    private PreAggStatus preAggStatus = PreAggStatus.on();
    private boolean aggPushed;
    private PushDownAggOperator pushDownAggOperator = PushDownAggOperator.NONE;
    private List<Long> partitions = Collections.emptyList();

    public LogicalOlapScanBuilder() {
    }

    /**
     * Builder based on logicalOlapScan
     */
    public LogicalOlapScanBuilder(LogicalOlapScan logicalOlapScan) {
        this.id = logicalOlapScan.getId();
        this.table = logicalOlapScan.getTable();
        this.qualifier = logicalOlapScan.getQualifier();
        this.groupExpression = logicalOlapScan.getGroupExpression();
        this.logicalProperties = Optional.of(logicalOlapScan.getLogicalProperties());
        this.selectedPartitionIds = logicalOlapScan.getSelectedPartitionIds();
        this.partitionPruned = logicalOlapScan.isPartitionPruned();
        this.selectedTabletIds = logicalOlapScan.getSelectedTabletIds();
        this.tabletPruned = logicalOlapScan.isTabletPruned();
        this.candidateIndexIds = logicalOlapScan.getCandidateIndexIds();
        this.indexSelected = logicalOlapScan.isIndexSelected();
        this.preAggStatus = logicalOlapScan.getPreAggStatus();
        this.aggPushed = logicalOlapScan.isAggPushed();
        this.pushDownAggOperator = logicalOlapScan.getPushDownAggOperator();
        this.partitions = logicalOlapScan.getPartitions();
    }

    public LogicalOlapScanBuilder setId(RelationId id) {
        this.id = id;
        return this;
    }

    public LogicalOlapScanBuilder setTable(Table table) {
        this.table = table;
        return this;
    }

    public LogicalOlapScanBuilder setQualifier(List<String> qualifier) {
        this.qualifier = qualifier;
        return this;
    }

    public LogicalOlapScanBuilder setGroupExpression(Optional<GroupExpression> groupExpression) {
        this.groupExpression = groupExpression;
        return this;
    }

    public LogicalOlapScanBuilder setLogicalProperties(Optional<LogicalProperties> logicalProperties) {
        this.logicalProperties = logicalProperties;
        return this;
    }

    public LogicalOlapScanBuilder setSelectedPartitionIds(List<Long> selectedPartitionIds) {
        this.selectedPartitionIds = selectedPartitionIds;
        return this;
    }

    public LogicalOlapScanBuilder setPartitionPruned(boolean partitionPruned) {
        this.partitionPruned = partitionPruned;
        return this;
    }

    public LogicalOlapScanBuilder setSelectedTabletIds(List<Long> selectedTabletIds) {
        this.selectedTabletIds = selectedTabletIds;
        return this;
    }

    public LogicalOlapScanBuilder setTabletPruned(boolean tabletPruned) {
        this.tabletPruned = tabletPruned;
        return this;
    }

    public LogicalOlapScanBuilder setCandidateIndexIds(List<Long> candidateIndexIds) {
        this.candidateIndexIds = candidateIndexIds;
        return this;
    }

    public LogicalOlapScanBuilder setIndexSelected(boolean indexSelected) {
        this.indexSelected = indexSelected;
        return this;
    }

    public LogicalOlapScanBuilder setPreAggStatus(PreAggStatus preAggStatus) {
        this.preAggStatus = preAggStatus;
        return this;
    }

    public LogicalOlapScanBuilder setAggPushed(boolean aggPushed) {
        this.aggPushed = aggPushed;
        return this;
    }

    public LogicalOlapScanBuilder setPushDownAggOperator(PushDownAggOperator pushDownAggOperator) {
        this.pushDownAggOperator = pushDownAggOperator;
        return this;
    }

    public LogicalOlapScanBuilder setPartitions(List<Long> partitions) {
        this.partitions = partitions;
        return this;
    }

    public LogicalOlapScan build() {
        return new LogicalOlapScan(id, table, qualifier, groupExpression, logicalProperties, selectedPartitionIds,
                partitionPruned, selectedTabletIds, tabletPruned, candidateIndexIds, indexSelected, preAggStatus,
                aggPushed, pushDownAggOperator, partitions);
    }
}
