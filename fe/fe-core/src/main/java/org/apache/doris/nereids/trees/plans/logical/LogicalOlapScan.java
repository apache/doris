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

import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Table;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Logical OlapScan.
 */
public class LogicalOlapScan extends LogicalRelation {

    private final long selectedIndexId;
    private final List<Long> selectedTabletId;
    private final boolean partitionPruned;

    public LogicalOlapScan(RelationId id, OlapTable table) {
        this(id, table, ImmutableList.of());
    }

    public LogicalOlapScan(RelationId id, OlapTable table, List<String> qualifier) {
        this(id, table, qualifier, Optional.empty(), Optional.empty(),
                table.getPartitionIds(), false);
    }

    public LogicalOlapScan(RelationId id, Table table, List<String> qualifier) {
        this(id, table, qualifier, Optional.empty(), Optional.empty(),
                ((OlapTable) table).getPartitionIds(), false);
    }

    /**
     * Constructor for LogicalOlapScan.
     *
     * @param table Doris table
     * @param qualifier table name qualifier
     */
    public LogicalOlapScan(RelationId id, Table table, List<String> qualifier,
            Optional<GroupExpression> groupExpression, Optional<LogicalProperties> logicalProperties,
            List<Long> selectedPartitionIdList, boolean partitionPruned) {
        super(id, PlanType.LOGICAL_OLAP_SCAN, table, qualifier,
                groupExpression, logicalProperties, selectedPartitionIdList);
        this.selectedIndexId = getTable().getBaseIndexId();
        this.selectedTabletId = Lists.newArrayList();
        for (Partition partition : getTable().getAllPartitions()) {
            selectedTabletId.addAll(partition.getBaseIndex().getTabletIdsInOrder());
        }
        this.partitionPruned = partitionPruned;
    }

    @Override
    public OlapTable getTable() {
        Preconditions.checkArgument(table instanceof OlapTable);
        return (OlapTable) table;
    }

    @Override
    public String toString() {
        return Utils.toSqlString("LogicalOlapScan",
                "qualified", qualifiedName(),
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
        LogicalOlapScan that = ((LogicalOlapScan) o);
        return this.getId().equals(that.getId())
                && Objects.equals(this.selectedTabletId, that.selectedTabletId)
                && Objects.equals(this.selectedIndexId, that.selectedIndexId)
                && Objects.equals(this.table, that.table);
    }

    @Override
    public Plan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new LogicalOlapScan(getId(), table, qualifier, groupExpression,
                Optional.of(getLogicalProperties()), selectedPartitionIds, partitionPruned);
    }

    @Override
    public LogicalOlapScan withLogicalProperties(Optional<LogicalProperties> logicalProperties) {
        return new LogicalOlapScan(getId(), table, qualifier, Optional.empty(),
                logicalProperties, selectedPartitionIds, partitionPruned);
    }

    public LogicalOlapScan withSelectedPartitionId(List<Long> selectedPartitionId) {
        return new LogicalOlapScan(getId(), table, qualifier, Optional.empty(),
                Optional.of(logicalPropertiesSupplier.get()), selectedPartitionId, true);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalOlapScan(this, context);
    }

    public boolean isPartitionPruned() {
        return partitionPruned;
    }

    public List<Long> getSelectedTabletId() {
        return selectedTabletId;
    }

    public long getSelectedIndexId() {
        return selectedIndexId;
    }
}
