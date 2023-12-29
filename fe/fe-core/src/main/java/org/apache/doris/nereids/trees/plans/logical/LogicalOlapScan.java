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
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.rules.rewrite.mv.AbstractSelectMaterializedIndexRule;
import org.apache.doris.nereids.trees.TableSample;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.PreAggStatus;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.algebra.OlapScan;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Logical OlapScan.
 */
public class LogicalOlapScan extends LogicalCatalogRelation implements OlapScan {

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

    /**
     * When the SlotReference is generated through fromColumn,
     * the exprId will be generated incrementally,
     * causing the slotId of the base to change when the output is recalculated.
     * This structure is responsible for storing the generated SlotReference
     */
    private final Map<Pair<Long, String>, Slot> cacheSlotWithSlotName;

    ///////////////////////////////////////////////////////////////////////////
    // Members for tablet ids.
    ///////////////////////////////////////////////////////////////////////////

    /**
     * Selected tablet ids to read data from.
     */
    private final List<Long> selectedTabletIds;

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

    private final Optional<TableSample> tableSample;

    public LogicalOlapScan(RelationId id, OlapTable table) {
        this(id, table, ImmutableList.of());
    }

    public LogicalOlapScan(RelationId id, OlapTable table, List<String> qualifier) {
        this(id, table, qualifier, Optional.empty(), Optional.empty(),
                table.getPartitionIds(), false,
                ImmutableList.of(),
                -1, false, PreAggStatus.on(), ImmutableList.of(), ImmutableList.of(),
                Maps.newHashMap(), Optional.empty());
    }

    public LogicalOlapScan(RelationId id, OlapTable table, List<String> qualifier, List<Long> tabletIds,
            List<String> hints, Optional<TableSample> tableSample) {
        this(id, table, qualifier, Optional.empty(), Optional.empty(),
                table.getPartitionIds(), false, tabletIds,
                -1, false, PreAggStatus.on(), ImmutableList.of(), hints, Maps.newHashMap(),
                tableSample);
    }

    public LogicalOlapScan(RelationId id, OlapTable table, List<String> qualifier, List<Long> specifiedPartitions,
            List<Long> tabletIds, List<String> hints, Optional<TableSample> tableSample) {
        this(id, table, qualifier, Optional.empty(), Optional.empty(),
                // must use specifiedPartitions here for prune partition by sql like 'select * from t partition p1'
                specifiedPartitions, false, tabletIds,
                -1, false, PreAggStatus.on(), specifiedPartitions, hints, Maps.newHashMap(),
                tableSample);
    }

    /**
     * Constructor for LogicalOlapScan.
     */
    public LogicalOlapScan(RelationId id, Table table, List<String> qualifier,
            Optional<GroupExpression> groupExpression, Optional<LogicalProperties> logicalProperties,
            List<Long> selectedPartitionIds, boolean partitionPruned,
            List<Long> selectedTabletIds, long selectedIndexId, boolean indexSelected,
            PreAggStatus preAggStatus, List<Long> specifiedPartitions,
            List<String> hints, Map<Pair<Long, String>, Slot> cacheSlotWithSlotName,
            Optional<TableSample> tableSample) {
        super(id, PlanType.LOGICAL_OLAP_SCAN, table, qualifier,
                groupExpression, logicalProperties);
        Preconditions.checkArgument(selectedPartitionIds != null,
                "selectedPartitionIds can not be null");
        this.selectedTabletIds = ImmutableList.copyOf(selectedTabletIds);
        this.partitionPruned = partitionPruned;
        this.selectedIndexId = selectedIndexId <= 0 ? getTable().getBaseIndexId() : selectedIndexId;
        this.indexSelected = indexSelected;
        this.preAggStatus = preAggStatus;
        this.manuallySpecifiedPartitions = ImmutableList.copyOf(specifiedPartitions);
        this.selectedPartitionIds = selectedPartitionIds.stream()
                .filter(partitionId -> this.getTable().getPartition(partitionId) != null)
                .collect(Collectors.toList());
        this.hints = Objects.requireNonNull(hints, "hints can not be null");
        this.cacheSlotWithSlotName = Objects.requireNonNull(cacheSlotWithSlotName,
                "mvNameToSlot can not be null");
        this.tableSample = tableSample;
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
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        LogicalOlapScan that = (LogicalOlapScan) o;
        return selectedIndexId == that.selectedIndexId && indexSelected == that.indexSelected
                && partitionPruned == that.partitionPruned && Objects.equals(preAggStatus, that.preAggStatus)
                && Objects.equals(selectedTabletIds, that.selectedTabletIds)
                && Objects.equals(manuallySpecifiedPartitions, that.manuallySpecifiedPartitions)
                && Objects.equals(selectedPartitionIds, that.selectedPartitionIds)
                && Objects.equals(hints, that.hints)
                && Objects.equals(tableSample, tableSample);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), selectedIndexId, indexSelected, preAggStatus, cacheSlotWithSlotName,
                selectedTabletIds, partitionPruned, manuallySpecifiedPartitions, selectedPartitionIds, hints,
                tableSample);
    }

    @Override
    public LogicalOlapScan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new LogicalOlapScan(relationId, (Table) table, qualifier,
                groupExpression, Optional.of(getLogicalProperties()),
                selectedPartitionIds, partitionPruned, selectedTabletIds,
                selectedIndexId, indexSelected, preAggStatus, manuallySpecifiedPartitions,
                hints, cacheSlotWithSlotName, tableSample);
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        return new LogicalOlapScan(relationId, (Table) table, qualifier, groupExpression, logicalProperties,
                selectedPartitionIds, partitionPruned, selectedTabletIds,
                selectedIndexId, indexSelected, preAggStatus, manuallySpecifiedPartitions,
                hints, cacheSlotWithSlotName, tableSample);
    }

    public LogicalOlapScan withSelectedPartitionIds(List<Long> selectedPartitionIds) {
        return new LogicalOlapScan(relationId, (Table) table, qualifier,
                Optional.empty(), Optional.of(getLogicalProperties()),
                selectedPartitionIds, true, selectedTabletIds,
                selectedIndexId, indexSelected, preAggStatus, manuallySpecifiedPartitions,
                hints, cacheSlotWithSlotName, tableSample);
    }

    public LogicalOlapScan withMaterializedIndexSelected(PreAggStatus preAgg, long indexId) {
        return new LogicalOlapScan(relationId, (Table) table, qualifier,
                Optional.empty(), Optional.of(getLogicalProperties()),
                selectedPartitionIds, partitionPruned, selectedTabletIds,
                indexId, true, preAgg, manuallySpecifiedPartitions, hints, cacheSlotWithSlotName,
                tableSample);
    }

    public LogicalOlapScan withSelectedTabletIds(List<Long> selectedTabletIds) {
        return new LogicalOlapScan(relationId, (Table) table, qualifier,
                Optional.empty(), Optional.of(getLogicalProperties()),
                selectedPartitionIds, partitionPruned, selectedTabletIds,
                selectedIndexId, indexSelected, preAggStatus, manuallySpecifiedPartitions,
                hints, cacheSlotWithSlotName, tableSample);
    }

    public LogicalOlapScan withPreAggStatus(PreAggStatus preAggStatus) {
        return new LogicalOlapScan(relationId, (Table) table, qualifier,
                Optional.empty(), Optional.of(getLogicalProperties()),
                selectedPartitionIds, partitionPruned, selectedTabletIds,
                selectedIndexId, indexSelected, preAggStatus, manuallySpecifiedPartitions,
                hints, cacheSlotWithSlotName, tableSample);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalOlapScan(this, context);
    }

    public boolean isPartitionPruned() {
        return partitionPruned;
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
        if (selectedIndexId != ((OlapTable) table).getBaseIndexId()) {
            return getOutputByIndex(selectedIndexId);
        }
        return table.getBaseSchema(true).stream().map(col -> {
            if (cacheSlotWithSlotName.containsKey(Pair.of(selectedIndexId, col.getName()))) {
                return cacheSlotWithSlotName.get(Pair.of(selectedIndexId, col.getName()));
            }
            Slot slot = SlotReference.fromColumn(col, qualified());
            cacheSlotWithSlotName.put(Pair.of(selectedIndexId, col.getName()), slot);
            return slot;
        }).collect(ImmutableList.toImmutableList());
    }

    @Override
    public Set<RelationId> getInputRelations() {
        Set<RelationId> relationIdSet = Sets.newHashSet();
        relationIdSet.add(relationId);
        return relationIdSet;
    }

    /**
     * Get the slot under the index,
     * and create a new slotReference for the slot that has not appeared in the materialized view.
     */
    public List<Slot> getOutputByIndex(long indexId) {
        OlapTable olapTable = (OlapTable) table;
        // PhysicalStorageLayerAggregateTest has no visible index
        // when we have a partitioned table without any partition, visible index is empty
        if (-1 == indexId || olapTable.getIndexMetaByIndexId(indexId) == null) {
            return olapTable.getIndexMetaByIndexId(indexId).getSchema().stream()
                    .map(s -> generateUniqueSlot(s, indexId == ((OlapTable) table).getBaseIndexId(), indexId))
                    .collect(Collectors.toList());
        }
        return olapTable.getIndexMetaByIndexId(indexId).getSchema().stream()
                .map(s -> generateUniqueSlot(s, indexId == ((OlapTable) table).getBaseIndexId(), indexId))
                .collect(ImmutableList.toImmutableList());
    }

    private Slot generateUniqueSlot(Column column, boolean isBaseIndex, long indexId) {
        String name = isBaseIndex ? column.getName()
                : AbstractSelectMaterializedIndexRule.parseMvColumnToMvName(column.getName(),
                        column.isAggregated() ? Optional.of(column.getAggregationType().toSql()) : Optional.empty());
        if (cacheSlotWithSlotName.containsKey(Pair.of(indexId, name))) {
            return cacheSlotWithSlotName.get(Pair.of(indexId, name));
        }
        Slot slot = SlotReference.fromColumn(column, name, qualified());
        cacheSlotWithSlotName.put(Pair.of(indexId, name), slot);
        return slot;
    }

    public List<Long> getManuallySpecifiedPartitions() {
        return manuallySpecifiedPartitions;
    }

    public List<String> getHints() {
        return hints;
    }

    public Optional<TableSample> getTableSample() {
        return tableSample;
    }
}
