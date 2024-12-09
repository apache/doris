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
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.tuple.Pair;
import org.json.JSONObject;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

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

    /**
     * Selected tablet ids to read data from, this would be set if user query with tablets manually
     * Such as select * from  orders TABLET(100);
     */
    private final List<Long> manuallySpecifiedTabletIds;

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

    private final boolean directMvScan;

    private final Map<String, Set<List<String>>> colToSubPathsMap;
    private final Map<Slot, Map<List<String>, SlotReference>> subPathToSlotMap;

    public LogicalOlapScan(RelationId id, OlapTable table) {
        this(id, table, ImmutableList.of());
    }

    /**
     * LogicalOlapScan construct method
     */
    public LogicalOlapScan(RelationId id, OlapTable table, List<String> qualifier) {
        this(id, table, qualifier, Optional.empty(), Optional.empty(),
                table.getPartitionIds(), false,
                ImmutableList.of(),
                -1, false, PreAggStatus.unset(), ImmutableList.of(), ImmutableList.of(),
                Maps.newHashMap(), Optional.empty(), false, ImmutableMap.of(),
                ImmutableList.of());
    }

    public LogicalOlapScan(RelationId id, OlapTable table, List<String> qualifier, List<Long> tabletIds,
            List<String> hints, Optional<TableSample> tableSample) {
        this(id, table, qualifier, Optional.empty(), Optional.empty(),
                table.getPartitionIds(), false, tabletIds,
                -1, false, PreAggStatus.unset(), ImmutableList.of(), hints, Maps.newHashMap(),
                tableSample, false, ImmutableMap.of(), ImmutableList.of());
    }

    public LogicalOlapScan(RelationId id, OlapTable table, List<String> qualifier, List<Long> specifiedPartitions,
            List<Long> tabletIds, List<String> hints, Optional<TableSample> tableSample) {
        this(id, table, qualifier, Optional.empty(), Optional.empty(),
                // must use specifiedPartitions here for prune partition by sql like 'select * from t partition p1'
                specifiedPartitions, false, tabletIds,
                -1, false, PreAggStatus.unset(), specifiedPartitions, hints, Maps.newHashMap(),
                tableSample, false, ImmutableMap.of(), ImmutableList.of());
    }

    public LogicalOlapScan(RelationId id, OlapTable table, List<String> qualifier, List<Long> tabletIds,
                           List<Long> selectedPartitionIds, long selectedIndexId, PreAggStatus preAggStatus,
                           List<Long> specifiedPartitions, List<String> hints, Optional<TableSample> tableSample) {
        this(id, table, qualifier, Optional.empty(), Optional.empty(),
                selectedPartitionIds, false, tabletIds,
                selectedIndexId, true, preAggStatus,
                specifiedPartitions, hints, Maps.newHashMap(), tableSample, true, ImmutableMap.of(),
                ImmutableList.of());
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
            Optional<TableSample> tableSample, boolean directMvScan,
            Map<String, Set<List<String>>> colToSubPathsMap, List<Long> specifiedTabletIds) {
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
        this.manuallySpecifiedTabletIds = ImmutableList.copyOf(specifiedTabletIds);

        if (selectedPartitionIds.isEmpty()) {
            this.selectedPartitionIds = ImmutableList.of();
        } else {
            Builder<Long> existPartitions
                    = ImmutableList.builderWithExpectedSize(selectedPartitionIds.size());
            for (Long partitionId : selectedPartitionIds) {
                if (((OlapTable) table).getPartition(partitionId) != null) {
                    existPartitions.add(partitionId);
                }
            }
            this.selectedPartitionIds = existPartitions.build();
        }
        this.hints = Objects.requireNonNull(hints, "hints can not be null");
        this.cacheSlotWithSlotName = Objects.requireNonNull(cacheSlotWithSlotName,
                "mvNameToSlot can not be null");
        this.tableSample = tableSample;
        this.directMvScan = directMvScan;
        this.colToSubPathsMap = colToSubPathsMap;
        this.subPathToSlotMap = Maps.newHashMap();
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
                && Objects.equals(manuallySpecifiedTabletIds, that.manuallySpecifiedTabletIds)
                && Objects.equals(selectedPartitionIds, that.selectedPartitionIds)
                && Objects.equals(hints, that.hints)
                && Objects.equals(tableSample, that.tableSample);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), selectedIndexId, indexSelected, preAggStatus, cacheSlotWithSlotName,
                selectedTabletIds, partitionPruned, manuallySpecifiedTabletIds, manuallySpecifiedPartitions,
                selectedPartitionIds, hints, tableSample);
    }

    @Override
    public LogicalOlapScan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new LogicalOlapScan(relationId, (Table) table, qualifier,
                groupExpression, Optional.of(getLogicalProperties()),
                selectedPartitionIds, partitionPruned, selectedTabletIds,
                selectedIndexId, indexSelected, preAggStatus, manuallySpecifiedPartitions,
                hints, cacheSlotWithSlotName, tableSample, directMvScan, colToSubPathsMap, manuallySpecifiedTabletIds);
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        return new LogicalOlapScan(relationId, (Table) table, qualifier, groupExpression, logicalProperties,
                selectedPartitionIds, partitionPruned, selectedTabletIds,
                selectedIndexId, indexSelected, preAggStatus, manuallySpecifiedPartitions,
                hints, cacheSlotWithSlotName, tableSample, directMvScan, colToSubPathsMap, manuallySpecifiedTabletIds);
    }

    public LogicalOlapScan withSelectedPartitionIds(List<Long> selectedPartitionIds) {
        return new LogicalOlapScan(relationId, (Table) table, qualifier,
                Optional.empty(), Optional.of(getLogicalProperties()),
                selectedPartitionIds, true, selectedTabletIds,
                selectedIndexId, indexSelected, preAggStatus, manuallySpecifiedPartitions,
                hints, cacheSlotWithSlotName, tableSample, directMvScan, colToSubPathsMap, manuallySpecifiedTabletIds);
    }

    public LogicalOlapScan withMaterializedIndexSelected(long indexId) {
        return new LogicalOlapScan(relationId, (Table) table, qualifier,
                Optional.empty(), Optional.of(getLogicalProperties()),
                selectedPartitionIds, partitionPruned, selectedTabletIds,
                indexId, true, PreAggStatus.unset(), manuallySpecifiedPartitions, hints, cacheSlotWithSlotName,
                tableSample, directMvScan, colToSubPathsMap, manuallySpecifiedTabletIds);
    }

    public LogicalOlapScan withSelectedTabletIds(List<Long> selectedTabletIds) {
        return new LogicalOlapScan(relationId, (Table) table, qualifier,
                Optional.empty(), Optional.of(getLogicalProperties()),
                selectedPartitionIds, partitionPruned, selectedTabletIds,
                selectedIndexId, indexSelected, preAggStatus, manuallySpecifiedPartitions,
                hints, cacheSlotWithSlotName, tableSample, directMvScan, colToSubPathsMap, manuallySpecifiedTabletIds);
    }

    public LogicalOlapScan withPreAggStatus(PreAggStatus preAggStatus) {
        return new LogicalOlapScan(relationId, (Table) table, qualifier,
                Optional.empty(), Optional.of(getLogicalProperties()),
                selectedPartitionIds, partitionPruned, selectedTabletIds,
                selectedIndexId, indexSelected, preAggStatus, manuallySpecifiedPartitions,
                hints, cacheSlotWithSlotName, tableSample, directMvScan, colToSubPathsMap, manuallySpecifiedTabletIds);
    }

    public LogicalOlapScan withColToSubPathsMap(Map<String, Set<List<String>>> colToSubPathsMap) {
        return new LogicalOlapScan(relationId, (Table) table, qualifier,
                Optional.empty(), Optional.empty(),
                selectedPartitionIds, partitionPruned, selectedTabletIds,
                selectedIndexId, indexSelected, preAggStatus, manuallySpecifiedPartitions,
                hints, cacheSlotWithSlotName, tableSample, directMvScan, colToSubPathsMap, manuallySpecifiedTabletIds);
    }

    public LogicalOlapScan withManuallySpecifiedTabletIds(List<Long> manuallySpecifiedTabletIds) {
        return new LogicalOlapScan(relationId, (Table) table, qualifier,
                Optional.empty(), Optional.of(getLogicalProperties()),
                selectedPartitionIds, partitionPruned, selectedTabletIds,
                selectedIndexId, indexSelected, preAggStatus, manuallySpecifiedPartitions,
                hints, cacheSlotWithSlotName, tableSample, directMvScan, colToSubPathsMap, manuallySpecifiedTabletIds);
    }

    @Override
    public LogicalOlapScan withRelationId(RelationId relationId) {
        // we have to set partitionPruned to false, so that mtmv rewrite can prevent deadlock when rewriting union
        return new LogicalOlapScan(relationId, (Table) table, qualifier,
                Optional.empty(), Optional.empty(),
                selectedPartitionIds, false, selectedTabletIds,
                selectedIndexId, indexSelected, preAggStatus, manuallySpecifiedPartitions,
                hints, Maps.newHashMap(), tableSample, directMvScan, colToSubPathsMap, selectedTabletIds);
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

    public List<Long> getManuallySpecifiedTabletIds() {
        return manuallySpecifiedTabletIds;
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

    public Map<Slot, Map<List<String>, SlotReference>> getSubPathToSlotMap() {
        this.getOutput();
        return subPathToSlotMap;
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
        List<Column> baseSchema = table.getBaseSchema(true);
        List<SlotReference> slotFromColumn = createSlotsVectorized(baseSchema);

        Builder<Slot> slots = ImmutableList.builder();
        for (int i = 0; i < baseSchema.size(); i++) {
            final int index = i;
            Column col = baseSchema.get(i);
            Pair<Long, String> key = Pair.of(selectedIndexId, col.getName());
            Slot slot = cacheSlotWithSlotName.computeIfAbsent(key, k -> slotFromColumn.get(index));
            slots.add(slot);
            if (colToSubPathsMap.containsKey(key.getValue())) {
                for (List<String> subPath : colToSubPathsMap.get(key.getValue())) {
                    if (!subPath.isEmpty()) {
                        SlotReference slotReference = SlotReference.fromColumn(
                                table, baseSchema.get(i), qualified()).withSubPath(subPath);
                        slots.add(slotReference);
                        subPathToSlotMap.computeIfAbsent(slot, k -> Maps.newHashMap())
                                .put(subPath, slotReference);
                    }

                }
            }
        }
        return slots.build();
    }

    /**
     * Get the slot under the index,
     * and create a new slotReference for the slot that has not appeared in the materialized view.
     */
    public List<Slot> getOutputByIndex(long indexId) {
        OlapTable olapTable = (OlapTable) table;
        // PhysicalStorageLayerAggregateTest has no visible index
        // when we have a partitioned table without any partition, visible index is empty
        List<Column> schema = olapTable.getIndexMetaByIndexId(indexId).getSchema();
        List<Slot> slots = Lists.newArrayListWithCapacity(schema.size());
        for (Column c : schema) {
            slots.addAll(generateUniqueSlot(
                    olapTable, c, indexId == ((OlapTable) table).getBaseIndexId(), indexId));
        }
        return slots;
    }

    private List<Slot> generateUniqueSlot(OlapTable table, Column column, boolean isBaseIndex, long indexId) {
        String name = isBaseIndex || directMvScan ? column.getName()
                : AbstractSelectMaterializedIndexRule.parseMvColumnToMvName(column.getName(),
                        column.isAggregated() ? Optional.of(column.getAggregationType().toSql()) : Optional.empty());
        Pair<Long, String> key = Pair.of(indexId, name);
        Slot slot = cacheSlotWithSlotName.computeIfAbsent(key, k ->
                SlotReference.fromColumn(table, column, name, qualified()));
        List<Slot> slots = Lists.newArrayList(slot);
        if (colToSubPathsMap.containsKey(key.getValue())) {
            for (List<String> subPath : colToSubPathsMap.get(key.getValue())) {
                if (!subPath.isEmpty()) {
                    SlotReference slotReference
                            = SlotReference.fromColumn(table, column, name, qualified()).withSubPath(subPath);
                    slots.add(slotReference);
                    subPathToSlotMap.computeIfAbsent(slot, k -> Maps.newHashMap())
                            .put(subPath, slotReference);
                }

            }
        }
        return slots;
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

    public boolean isDirectMvScan() {
        return directMvScan;
    }

    public boolean isPreAggStatusUnSet() {
        return preAggStatus.isUnset();
    }

    private List<SlotReference> createSlotsVectorized(List<Column> columns) {
        List<String> qualified = qualified();
        SlotReference[] slots = new SlotReference[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            slots[i] = SlotReference.fromColumn(table, columns.get(i), qualified);
        }
        return Arrays.asList(slots);
    }

    @Override
    public JSONObject toJson() {
        JSONObject olapScan = super.toJson();
        JSONObject properties = new JSONObject();
        properties.put("OlapTable", table.getName());
        properties.put("SelectedIndexId", Long.toString(selectedIndexId));
        properties.put("PreAggStatus", preAggStatus.toString());
        olapScan.put("Properties", properties);
        return olapScan;
    }
}
