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

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.PreAggStatus;
import org.apache.doris.nereids.trees.plans.PushDownAggOperator;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.algebra.CatalogRelation;
import org.apache.doris.nereids.trees.plans.algebra.OlapScan;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.commons.collections.CollectionUtils;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Logical OlapScan.
 */
public class LogicalOlapScan extends LogicalRelation implements CatalogRelation, OlapScan {

    private final long selectedIndexId;
    private final List<Long> selectedTabletIds;
    private final boolean partitionPruned;
    private final boolean tabletPruned;

    private final List<Long> candidateIndexIds;
    private final boolean indexSelected;

    private final PreAggStatus preAggStatus;

    private final boolean aggPushed;
    private final PushDownAggOperator pushDownAggOperator;

    public LogicalOlapScan(RelationId id, OlapTable table) {
        this(id, table, ImmutableList.of());
    }

    public LogicalOlapScan(RelationId id, OlapTable table, List<String> qualifier) {
        this(id, table, qualifier, Optional.empty(), Optional.empty(),
                table.getPartitionIds(), false, ImmutableList.of(), false,
                ImmutableList.of(), false, PreAggStatus.on(), false, PushDownAggOperator.NONE);
    }

    public LogicalOlapScan(RelationId id, Table table, List<String> qualifier) {
        this(id, table, qualifier, Optional.empty(), Optional.empty(),
                ((OlapTable) table).getPartitionIds(), false, ImmutableList.of(), false,
                ImmutableList.of(), false, PreAggStatus.on(), false, PushDownAggOperator.NONE);
    }

    /**
     * Constructor for LogicalOlapScan.
     */
    public LogicalOlapScan(RelationId id, Table table, List<String> qualifier,
            Optional<GroupExpression> groupExpression, Optional<LogicalProperties> logicalProperties,
            List<Long> selectedPartitionIds, boolean partitionPruned,
            List<Long> selectedTabletIds, boolean tabletPruned,
            List<Long> candidateIndexIds, boolean indexSelected, PreAggStatus preAggStatus,
            boolean aggPushed, PushDownAggOperator pushDownAggOperator) {
        super(id, PlanType.LOGICAL_OLAP_SCAN, table, qualifier,
                groupExpression, logicalProperties, selectedPartitionIds);
        // TODO: use CBO manner to select best index id, according to index's statistics info,
        //   revisit this after rollup and materialized view selection are fully supported.
        this.selectedIndexId = CollectionUtils.isEmpty(candidateIndexIds)
                ? getTable().getBaseIndexId() : candidateIndexIds.get(0);
        this.selectedTabletIds = ImmutableList.copyOf(selectedTabletIds);
        this.partitionPruned = partitionPruned;
        this.tabletPruned = tabletPruned;
        this.candidateIndexIds = ImmutableList.copyOf(candidateIndexIds);
        this.indexSelected = indexSelected;
        this.preAggStatus = preAggStatus;
        this.aggPushed = aggPushed;
        this.pushDownAggOperator = pushDownAggOperator;
    }

    @Override
    public OlapTable getTable() {
        Preconditions.checkArgument(table instanceof OlapTable);
        return (OlapTable) table;
    }

    @Override
    public Database getDatabase() throws AnalysisException {
        Preconditions.checkArgument(!qualifier.isEmpty());
        return Env.getCurrentInternalCatalog().getDbOrException(qualifier.get(0),
                s -> new AnalysisException("Database [" + qualifier.get(0) + "] does not exist."));
    }

    @Override
    public String toString() {
        return Utils.toSqlString("LogicalOlapScan",
                "qualified", qualifiedName(),
                "output", getOutput(),
                "candidateIndexIds", candidateIndexIds,
                "selectedIndexId", selectedIndexId,
                "preAgg", preAggStatus,
                "pushAgg", pushDownAggOperator
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
        return Objects.equals(selectedPartitionIds, ((LogicalOlapScan) o).selectedPartitionIds)
                && Objects.equals(candidateIndexIds, ((LogicalOlapScan) o).candidateIndexIds)
                && Objects.equals(selectedTabletIds, ((LogicalOlapScan) o).selectedTabletIds)
                && Objects.equals(pushDownAggOperator, ((LogicalOlapScan) o).pushDownAggOperator);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, selectedPartitionIds, candidateIndexIds, selectedTabletIds, pushDownAggOperator);
    }

    @Override
    public Plan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new LogicalOlapScan(id, table, qualifier, groupExpression, Optional.of(getLogicalProperties()),
                selectedPartitionIds, partitionPruned, selectedTabletIds, tabletPruned,
                candidateIndexIds, indexSelected, preAggStatus, aggPushed, pushDownAggOperator);
    }

    @Override
    public LogicalOlapScan withLogicalProperties(Optional<LogicalProperties> logicalProperties) {
        return new LogicalOlapScan(id, table, qualifier, Optional.empty(), logicalProperties,
                selectedPartitionIds, partitionPruned, selectedTabletIds, tabletPruned,
                candidateIndexIds, indexSelected, preAggStatus, aggPushed, pushDownAggOperator);
    }

    public LogicalOlapScan withSelectedPartitionIds(List<Long> selectedPartitionIds) {
        return new LogicalOlapScan(id, table, qualifier, Optional.empty(), Optional.of(getLogicalProperties()),
                selectedPartitionIds, true, selectedTabletIds, tabletPruned,
                candidateIndexIds, indexSelected, preAggStatus, aggPushed, pushDownAggOperator);
    }

    public LogicalOlapScan withMaterializedIndexSelected(PreAggStatus preAgg, List<Long> candidateIndexIds) {
        return new LogicalOlapScan(id, table, qualifier, Optional.empty(), Optional.of(getLogicalProperties()),
                selectedPartitionIds, partitionPruned, selectedTabletIds, tabletPruned,
                candidateIndexIds, true, preAgg, aggPushed, pushDownAggOperator);
    }

    public LogicalOlapScan withSelectedTabletIds(List<Long> selectedTabletIds) {
        return new LogicalOlapScan(id, table, qualifier, Optional.empty(), Optional.of(getLogicalProperties()),
                selectedPartitionIds, partitionPruned, selectedTabletIds, true,
                candidateIndexIds, indexSelected, preAggStatus, aggPushed, pushDownAggOperator);
    }

    public LogicalOlapScan withPushDownAggregateOperator(PushDownAggOperator pushDownAggOperator) {
        return new LogicalOlapScan(id, table, qualifier, Optional.empty(), Optional.of(getLogicalProperties()),
                selectedPartitionIds, partitionPruned, selectedTabletIds, true,
                candidateIndexIds, indexSelected, preAggStatus, true, pushDownAggOperator);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalOlapScan(this, context);
    }

    public boolean isPartitionPruned() {
        return partitionPruned;
    }

    public boolean isTabletPruned() {
        return tabletPruned;
    }

    public List<Long> getSelectedTabletIds() {
        return selectedTabletIds;
    }

    @Override
    public long getSelectedIndexId() {
        return selectedIndexId;
    }

    public boolean isIndexSelected() {
        return indexSelected;
    }

    public PreAggStatus getPreAggStatus() {
        return preAggStatus;
    }

    public boolean isAggPushed() {
        return aggPushed;
    }

    public PushDownAggOperator getPushDownAggOperator() {
        return pushDownAggOperator;
    }

    @VisibleForTesting
    public Optional<String> getSelectedMaterializedIndexName() {
        return indexSelected ? Optional.ofNullable(((OlapTable) table).getIndexNameById(selectedIndexId))
                : Optional.empty();
    }

    public boolean supportStorageLayerAggregate() {
        KeysType keysType = getTable().getKeysType();
        return keysType == KeysType.AGG_KEYS || keysType == KeysType.DUP_KEYS;
    }
}
