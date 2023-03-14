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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.util.Util;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.ObjectId;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.PreAggStatus;
import org.apache.doris.nereids.trees.plans.algebra.CatalogRelation;
import org.apache.doris.nereids.trees.plans.algebra.OlapScan;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.qe.ConnectContext;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Logical OlapScan.
 */
public class LogicalOlapScan extends LogicalRelation implements CatalogRelation, OlapScan {

    ///////////////////////////////////////////////////////////////////////////
    // Members for materialized index.
    ///////////////////////////////////////////////////////////////////////////

    /**
     * The select materialized index id to read data from.
     */
    private final long selectedIndexId;

    /**
     * Status to indicate materialized index id is selected or not.
     */
    private final boolean indexSelected;

    /**
     * Status to indicate using pre-aggregation or not.
     */
    private final PreAggStatus preAggStatus;

    ///////////////////////////////////////////////////////////////////////////
    // Members for tablet ids.
    ///////////////////////////////////////////////////////////////////////////

    /**
     * Selected tablet ids to read data from.
     */
    private final List<Long> selectedTabletIds;

    /**
     * Status to indicate tablets are pruned or not.
     */
    private final boolean tabletPruned;

    ///////////////////////////////////////////////////////////////////////////
    // Members for partition ids.
    ///////////////////////////////////////////////////////////////////////////
    /**
     * Status to indicate partitions are pruned or not.
     * todo: should be pulled up to base class?
     */
    private final boolean partitionPruned;
    private final List<Long> manuallySpecifiedPartitions;

    private final List<Long> selectedPartitionIds;

    ///////////////////////////////////////////////////////////////////////////
    // Members for hints.
    ///////////////////////////////////////////////////////////////////////////
    private final List<String> hints;

    public LogicalOlapScan(ObjectId id, OlapTable table) {
        this(id, table, ImmutableList.of());
    }

    public LogicalOlapScan(ObjectId id, OlapTable table, List<String> qualifier) {
        this(id, table, qualifier, Optional.empty(), Optional.empty(),
                table.getPartitionIds(), false,
                ImmutableList.of(), false,
                -1, false, PreAggStatus.on(), ImmutableList.of(), ImmutableList.of());
    }

    public LogicalOlapScan(ObjectId id, OlapTable table, List<String> qualifier, List<String> hints) {
        this(id, table, qualifier, Optional.empty(), Optional.empty(),
                table.getPartitionIds(), false,
                ImmutableList.of(), false,
                -1, false, PreAggStatus.on(), ImmutableList.of(), hints);
    }

    public LogicalOlapScan(ObjectId id, OlapTable table, List<String> qualifier, List<Long> specifiedPartitions,
            List<String> hints) {
        this(id, table, qualifier, Optional.empty(), Optional.empty(),
                specifiedPartitions, false, ImmutableList.of(), false,
                -1, false, PreAggStatus.on(), specifiedPartitions, hints);
    }

    public LogicalOlapScan(ObjectId id, Table table, List<String> qualifier) {
        this(id, table, qualifier, Optional.empty(), Optional.empty(),
                ((OlapTable) table).getPartitionIds(), false, ImmutableList.of(), false,
                -1, false, PreAggStatus.on(), ImmutableList.of(), ImmutableList.of());
    }

    /**
     * Constructor for LogicalOlapScan.
     */
    public LogicalOlapScan(ObjectId id, Table table, List<String> qualifier,
            Optional<GroupExpression> groupExpression, Optional<LogicalProperties> logicalProperties,
            List<Long> selectedPartitionIds, boolean partitionPruned,
            List<Long> selectedTabletIds, boolean tabletPruned,
            long selectedIndexId, boolean indexSelected, PreAggStatus preAggStatus, List<Long> partitions,
            List<String> hints) {

        super(id, PlanType.LOGICAL_OLAP_SCAN, table, qualifier,
                groupExpression, logicalProperties);
        this.selectedTabletIds = ImmutableList.copyOf(selectedTabletIds);
        this.partitionPruned = partitionPruned;
        this.tabletPruned = tabletPruned;
        this.selectedIndexId = selectedIndexId <= 0 ? getTable().getBaseIndexId() : selectedIndexId;
        this.indexSelected = indexSelected;
        this.preAggStatus = preAggStatus;
        this.manuallySpecifiedPartitions = ImmutableList.copyOf(partitions);
        this.selectedPartitionIds = ImmutableList.copyOf(
                Objects.requireNonNull(selectedPartitionIds, "selectedPartitionIds can not be null"));
        this.hints = Objects.requireNonNull(hints, "hints can not be null");
    }

    public List<Long> getSelectedPartitionIds() {
        return selectedPartitionIds;
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
                "indexName", getSelectedMaterializedIndexName().orElse("<index_not_selected>"),
                "selectedIndexId", selectedIndexId,
                "preAgg", preAggStatus
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
        return Objects.equals(id, ((LogicalOlapScan) o).id)
                && Objects.equals(selectedPartitionIds, ((LogicalOlapScan) o).selectedPartitionIds)
                && Objects.equals(partitionPruned, ((LogicalOlapScan) o).partitionPruned)
                && Objects.equals(selectedIndexId, ((LogicalOlapScan) o).selectedIndexId)
                && Objects.equals(indexSelected, ((LogicalOlapScan) o).indexSelected)
                && Objects.equals(selectedTabletIds, ((LogicalOlapScan) o).selectedTabletIds)
                && Objects.equals(tabletPruned, ((LogicalOlapScan) o).tabletPruned)
                && Objects.equals(hints, ((LogicalOlapScan) o).hints);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id,
                selectedPartitionIds, partitionPruned,
                selectedIndexId, indexSelected,
                selectedTabletIds, tabletPruned,
                hints);
    }

    @Override
    public LogicalOlapScan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new LogicalOlapScan(id, (Table) table, qualifier, groupExpression, Optional.of(getLogicalProperties()),
                selectedPartitionIds, partitionPruned, selectedTabletIds, tabletPruned,
                selectedIndexId, indexSelected, preAggStatus, manuallySpecifiedPartitions, hints);
    }

    @Override
    public LogicalOlapScan withLogicalProperties(Optional<LogicalProperties> logicalProperties) {
        return new LogicalOlapScan(id, (Table) table, qualifier, Optional.empty(), logicalProperties,
                selectedPartitionIds, partitionPruned, selectedTabletIds, tabletPruned,
                selectedIndexId, indexSelected, preAggStatus, manuallySpecifiedPartitions, hints);
    }

    public LogicalOlapScan withSelectedPartitionIds(List<Long> selectedPartitionIds) {
        return new LogicalOlapScan(id, (Table) table, qualifier, Optional.empty(), Optional.of(getLogicalProperties()),
                selectedPartitionIds, true, selectedTabletIds, tabletPruned,
                selectedIndexId, indexSelected, preAggStatus, manuallySpecifiedPartitions, hints);
    }

    public LogicalOlapScan withMaterializedIndexSelected(PreAggStatus preAgg, long indexId) {
        return new LogicalOlapScan(id, (Table) table, qualifier, Optional.empty(), Optional.of(getLogicalProperties()),
                selectedPartitionIds, partitionPruned, selectedTabletIds, tabletPruned,
                indexId, true, preAgg, manuallySpecifiedPartitions, hints);
    }

    public LogicalOlapScan withSelectedTabletIds(List<Long> selectedTabletIds) {
        return new LogicalOlapScan(id, (Table) table, qualifier, Optional.empty(), Optional.of(getLogicalProperties()),
                selectedPartitionIds, partitionPruned, selectedTabletIds, true,
                selectedIndexId, indexSelected, preAggStatus, manuallySpecifiedPartitions, hints);
    }

    public LogicalOlapScan withPreAggStatus(PreAggStatus preAggStatus) {
        return new LogicalOlapScan(id, (Table) table, qualifier, Optional.empty(), Optional.of(getLogicalProperties()),
                selectedPartitionIds, partitionPruned, selectedTabletIds, true,
                selectedIndexId, indexSelected, preAggStatus, manuallySpecifiedPartitions, hints);
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

    @VisibleForTesting
    public Optional<String> getSelectedMaterializedIndexName() {
        return indexSelected ? Optional.ofNullable(((OlapTable) table).getIndexNameById(selectedIndexId))
                : Optional.empty();
    }

    @Override
    public List<Slot> computeOutput() {
        List<Column> otherColumns = new ArrayList<>();
        if (!Util.showHiddenColumns() && getTable().hasDeleteSign()
                && !ConnectContext.get().getSessionVariable()
                .skipDeleteSign()) {
            otherColumns.add(getTable().getDeleteSignColumn());
        }
        return Stream.concat(table.getBaseSchema().stream(), otherColumns.stream())
                .map(col -> SlotReference.fromColumn(col, qualified()))
                .collect(ImmutableList.toImmutableList());
    }

    @Override
    public List<Slot> computeNonUserVisibleOutput() {
        // if (getTable().getIndexIdToMeta().size() == 1) {
        //     return ImmutableList.of();
        // }
        Set<String> baseSchemaColNames = table.getBaseSchema().stream()
                .map(Column::getName)
                .collect(Collectors.toSet());

        OlapTable olapTable = (OlapTable) table;
        // extra columns in materialized index, such as `mv_bitmap_union_xxx`
        return olapTable.getVisibleIndexIdToMeta().values()
                .stream()
                .filter(index -> index.getIndexId() != ((OlapTable) table).getBaseIndexId())
                .flatMap(index -> index.getSchema()
                        .stream()
                        .filter(col -> !baseSchemaColNames.contains(col.getName()))
                )
                .map(col -> SlotReference.fromColumn(col, qualified()))
                .collect(ImmutableList.toImmutableList());
    }

    public List<Long> getManuallySpecifiedPartitions() {
        return manuallySpecifiedPartitions;
    }

    public List<String> getHints() {
        return hints;
    }
}
