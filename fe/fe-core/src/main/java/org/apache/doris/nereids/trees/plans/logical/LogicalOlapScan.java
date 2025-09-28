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
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.IdGenerator;
import org.apache.doris.mtmv.MTMVCache;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.DataTrait;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.trees.TableSample;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.PreAggStatus;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.algebra.CatalogRelation;
import org.apache.doris.nereids.trees.plans.algebra.OlapScan;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.rpc.RpcException;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
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

    private static final Logger LOG = LogManager.getLogger(LogicalOlapScan.class);

    ///////////////////////////////////////////////////////////////////////////
    // Members for materialized index.
    ///////////////////////////////////////////////////////////////////////////

    /**
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

    private final List<OrderKey> scoreOrderKeys;
    private final Optional<Long> scoreLimit;
    // use for ann push down
    private final List<OrderKey> annOrderKeys;
    private final Optional<Long> annLimit;

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
                ImmutableList.of(), ImmutableList.of(), ImmutableList.of(),
                ImmutableList.of(), Optional.empty(), ImmutableList.of(), Optional.empty());
    }

    public LogicalOlapScan(RelationId id, OlapTable table, List<String> qualifier, List<Long> tabletIds,
            List<String> hints, Optional<TableSample> tableSample, Collection<Slot> operativeSlots) {
        this(id, table, qualifier, Optional.empty(), Optional.empty(),
                table.getPartitionIds(), false, tabletIds,
                -1, false, PreAggStatus.unset(), ImmutableList.of(), hints, Maps.newHashMap(),
                tableSample, false, ImmutableMap.of(), ImmutableList.of(), operativeSlots,
                ImmutableList.of(), ImmutableList.of(), Optional.empty(), ImmutableList.of(), Optional.empty());
    }

    /**
     * constructor.
     */
    public LogicalOlapScan(RelationId id, OlapTable table, List<String> qualifier, List<Long> specifiedPartitions,
            List<Long> tabletIds, List<String> hints, Optional<TableSample> tableSample, List<Slot> operativeSlots) {
        this(id, table, qualifier, Optional.empty(), Optional.empty(),
                // must use specifiedPartitions here for prune partition by sql like 'select * from t partition p1'
                specifiedPartitions, false, tabletIds,
                -1, false, PreAggStatus.unset(), specifiedPartitions, hints, Maps.newHashMap(),
                tableSample, false, ImmutableMap.of(), ImmutableList.of(), operativeSlots,
                ImmutableList.of(), ImmutableList.of(), Optional.empty(),
                ImmutableList.of(), Optional.empty());
    }

    /**
     * constructor.
     */
    public LogicalOlapScan(RelationId id, OlapTable table, List<String> qualifier, List<Long> tabletIds,
                           List<Long> selectedPartitionIds, long selectedIndexId, PreAggStatus preAggStatus,
                           List<Long> specifiedPartitions, List<String> hints, Optional<TableSample> tableSample,
                           Collection<Slot> operativeSlots) {
        this(id, table, qualifier, Optional.empty(), Optional.empty(),
                selectedPartitionIds, false, tabletIds,
                selectedIndexId, true, preAggStatus,
                specifiedPartitions, hints, Maps.newHashMap(), tableSample, true, ImmutableMap.of(),
                ImmutableList.of(), operativeSlots, ImmutableList.of(), ImmutableList.of(), Optional.empty(),
                ImmutableList.of(), Optional.empty());
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
            Map<String, Set<List<String>>> colToSubPathsMap, List<Long> specifiedTabletIds,
            Collection<Slot> operativeSlots, List<NamedExpression> virtualColumns,
            List<OrderKey> scoreOrderKeys, Optional<Long> scoreLimit,
            List<OrderKey> annOrderKeys, Optional<Long> annLimit) {
        super(id, PlanType.LOGICAL_OLAP_SCAN, table, qualifier,
                operativeSlots, virtualColumns, groupExpression, logicalProperties);
        Preconditions.checkArgument(selectedPartitionIds != null,
                "selectedPartitionIds can not be null");
        this.selectedTabletIds = Utils.fastToImmutableList(selectedTabletIds);
        this.partitionPruned = partitionPruned;
        this.selectedIndexId = selectedIndexId <= 0 ? getTable().getBaseIndexId() : selectedIndexId;
        this.indexSelected = indexSelected;
        this.preAggStatus = preAggStatus;
        this.manuallySpecifiedPartitions = Utils.fastToImmutableList(specifiedPartitions);
        this.manuallySpecifiedTabletIds = Utils.fastToImmutableList(specifiedTabletIds);

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
        this.scoreOrderKeys = Utils.fastToImmutableList(scoreOrderKeys);
        this.scoreLimit = scoreLimit;
        this.annOrderKeys = Utils.fastToImmutableList(annOrderKeys);
        this.annLimit = annLimit;
    }

    public List<Long> getSelectedPartitionIds() {
        return selectedPartitionIds;
    }

    @Override
    public String getFingerprint() {
        String partitions = "";
        int partitionCount = this.table.getPartitionNames().size();
        if (selectedPartitionIds.size() != partitionCount) {
            partitions = " partitions(" + selectedPartitionIds.size() + "/" + partitionCount + ")";
        }
        // NOTE: embed version info avoid mismatching under data maintaining
        // TODO: more efficient way to ignore the ignorable data maintaining
        long version = 0;
        try {
            version = getTable().getVisibleVersion();
        } catch (RpcException e) {
            String errMsg = "table " + getTable().getName() + "in cloud getTableVisibleVersion error";
            LOG.warn(errMsg, e);
            throw new IllegalStateException(errMsg);
        }
        return Utils.toSqlString("OlapScan[" + table.getNameWithFullQualifiers() + partitions + "]"
                + "#" + getRelationId() + "@" + version
                + "@" + getTable().getVisibleVersionTime());
    }

    @Override
    public OlapTable getTable() {
        Preconditions.checkArgument(table instanceof OlapTable);
        return (OlapTable) table;
    }

    @Override
    public String toString() {
        return Utils.toSqlStringSkipNull("LogicalOlapScan",
                "qualified", qualifiedName(),
                "indexName", getSelectedMaterializedIndexName().orElse("<index_not_selected>"),
                "selectedIndexId", selectedIndexId,
                "preAgg", preAggStatus,
                "operativeCol", operativeSlots,
                "stats", statistics,
                "virtualColumns", virtualColumns
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
                && Objects.equals(tableSample, that.tableSample)
                && Objects.equals(scoreOrderKeys, that.scoreOrderKeys)
                && Objects.equals(scoreLimit, that.scoreLimit)
                && Objects.equals(annOrderKeys, that.annOrderKeys)
                && Objects.equals(annLimit, that.annLimit);
    }

    @Override
    public int hashCode() {
        return relationId.asInt();
    }

    @Override
    public LogicalOlapScan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new LogicalOlapScan(relationId, (Table) table, qualifier,
                groupExpression, Optional.of(getLogicalProperties()),
                selectedPartitionIds, partitionPruned, selectedTabletIds,
                selectedIndexId, indexSelected, preAggStatus, manuallySpecifiedPartitions,
                hints, cacheSlotWithSlotName, tableSample, directMvScan, colToSubPathsMap, manuallySpecifiedTabletIds,
                operativeSlots, virtualColumns, scoreOrderKeys, scoreLimit, annOrderKeys, annLimit);
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        return new LogicalOlapScan(relationId, (Table) table, qualifier, groupExpression, logicalProperties,
                selectedPartitionIds, partitionPruned, selectedTabletIds,
                selectedIndexId, indexSelected, preAggStatus, manuallySpecifiedPartitions,
                hints, cacheSlotWithSlotName, tableSample, directMvScan, colToSubPathsMap, manuallySpecifiedTabletIds,
                operativeSlots, virtualColumns, scoreOrderKeys, scoreLimit, annOrderKeys, annLimit);
    }

    /**
     * withSelectedPartitionIds
     */
    public LogicalOlapScan withSelectedPartitionIds(List<Long> selectedPartitionIds) {
        return new LogicalOlapScan(relationId, (Table) table, qualifier,
                Optional.empty(), Optional.of(getLogicalProperties()),
                selectedPartitionIds, true, selectedTabletIds,
                selectedIndexId, indexSelected, preAggStatus, manuallySpecifiedPartitions,
                hints, cacheSlotWithSlotName, tableSample, directMvScan, colToSubPathsMap, manuallySpecifiedTabletIds,
                operativeSlots, virtualColumns, scoreOrderKeys, scoreLimit, annOrderKeys, annLimit);
    }

    /**
     * with sync materialized index id.
     * @param indexId materialized index id for scan
     * @return scan with  materialized index id
     */
    public LogicalOlapScan withMaterializedIndexSelected(long indexId) {
        return new LogicalOlapScan(relationId, (Table) table, qualifier,
                Optional.empty(), Optional.of(getLogicalProperties()),
                selectedPartitionIds, partitionPruned, selectedTabletIds,
                indexId, true, PreAggStatus.unset(), manuallySpecifiedPartitions, hints, cacheSlotWithSlotName,
                tableSample, directMvScan, colToSubPathsMap, manuallySpecifiedTabletIds,
                operativeSlots, virtualColumns, scoreOrderKeys, scoreLimit, annOrderKeys, annLimit);
    }

    /**
     * withSelectedTabletIds
     */
    public LogicalOlapScan withSelectedTabletIds(List<Long> selectedTabletIds) {
        return new LogicalOlapScan(relationId, (Table) table, qualifier,
                Optional.empty(), Optional.of(getLogicalProperties()),
                selectedPartitionIds, partitionPruned, selectedTabletIds,
                selectedIndexId, indexSelected, preAggStatus, manuallySpecifiedPartitions,
                hints, cacheSlotWithSlotName, tableSample, directMvScan, colToSubPathsMap, manuallySpecifiedTabletIds,
                operativeSlots, virtualColumns, scoreOrderKeys, scoreLimit, annOrderKeys, annLimit);
    }

    /**
     * withPreAggStatus
     */
    public LogicalOlapScan withPreAggStatus(PreAggStatus preAggStatus) {
        return new LogicalOlapScan(relationId, (Table) table, qualifier,
                Optional.empty(), Optional.of(getLogicalProperties()),
                selectedPartitionIds, partitionPruned, selectedTabletIds,
                selectedIndexId, indexSelected, preAggStatus, manuallySpecifiedPartitions,
                hints, cacheSlotWithSlotName, tableSample, directMvScan, colToSubPathsMap, manuallySpecifiedTabletIds,
                operativeSlots, virtualColumns, scoreOrderKeys, scoreLimit, annOrderKeys, annLimit);
    }

    /**
     * constructor
     */
    public LogicalOlapScan withColToSubPathsMap(Map<String, Set<List<String>>> colToSubPathsMap) {
        return new LogicalOlapScan(relationId, (Table) table, qualifier,
                Optional.empty(), Optional.empty(),
                selectedPartitionIds, partitionPruned, selectedTabletIds,
                selectedIndexId, indexSelected, preAggStatus, manuallySpecifiedPartitions,
                hints, cacheSlotWithSlotName, tableSample, directMvScan, colToSubPathsMap, manuallySpecifiedTabletIds,
                operativeSlots, virtualColumns, scoreOrderKeys, scoreLimit, annOrderKeys, annLimit);
    }

    /**
     * constructor
     */
    public LogicalOlapScan withManuallySpecifiedTabletIds(List<Long> manuallySpecifiedTabletIds) {
        return new LogicalOlapScan(relationId, (Table) table, qualifier,
                Optional.empty(), Optional.of(getLogicalProperties()),
                selectedPartitionIds, partitionPruned, selectedTabletIds,
                selectedIndexId, indexSelected, preAggStatus, manuallySpecifiedPartitions,
                hints, cacheSlotWithSlotName, tableSample, directMvScan, colToSubPathsMap, manuallySpecifiedTabletIds,
                operativeSlots, virtualColumns, scoreOrderKeys, scoreLimit, annOrderKeys, annLimit);
    }

    @Override
    public LogicalOlapScan withRelationId(RelationId relationId) {
        // we have to set partitionPruned to false, so that mtmv rewrite can prevent deadlock when rewriting union
        return new LogicalOlapScan(relationId, (Table) table, qualifier,
                Optional.empty(), Optional.empty(),
                selectedPartitionIds, false, selectedTabletIds,
                selectedIndexId, indexSelected, preAggStatus, manuallySpecifiedPartitions,
                hints, Maps.newHashMap(), tableSample, directMvScan, colToSubPathsMap, selectedTabletIds,
                operativeSlots, virtualColumns, scoreOrderKeys, scoreLimit, annOrderKeys, annLimit);
    }

    /**
     * add virtual column to olap scan.
     * @param virtualColumns generated virtual columns
     * @return scan with virtual columns
     */
    @Override
    public LogicalOlapScan withVirtualColumns(List<NamedExpression> virtualColumns) {
        LogicalProperties logicalProperties = getLogicalProperties();
        List<Slot> output = Lists.newArrayList(logicalProperties.getOutput());
        output.addAll(virtualColumns.stream().map(NamedExpression::toSlot).collect(Collectors.toList()));
        logicalProperties = new LogicalProperties(() -> output, this::computeDataTrait);
        return new LogicalOlapScan(relationId, (Table) table, qualifier,
                groupExpression, Optional.of(logicalProperties),
                selectedPartitionIds, partitionPruned, selectedTabletIds,
                selectedIndexId, indexSelected, preAggStatus, manuallySpecifiedPartitions,
                hints, cacheSlotWithSlotName, tableSample, directMvScan, colToSubPathsMap,
                manuallySpecifiedTabletIds, operativeSlots, virtualColumns, scoreOrderKeys, scoreLimit,
                annOrderKeys, annLimit);
    }

    /**
     * add virtual column to olap scan.
     * @param virtualColumns generated virtual columns
     * @return scan with virtual columns
     */
    public LogicalOlapScan withVirtualColumnsAndTopN(
            List<NamedExpression> virtualColumns,
            List<OrderKey> annOrderKeys,
            Optional<Long> annLimit,
            List<OrderKey> scoreOrderKeys,
            Optional<Long> scoreLimit) {
        LogicalProperties logicalProperties = getLogicalProperties();
        List<Slot> output = Lists.newArrayList(logicalProperties.getOutput());
        output.addAll(virtualColumns.stream().map(NamedExpression::toSlot).collect(Collectors.toList()));
        logicalProperties = new LogicalProperties(() -> output, this::computeDataTrait);
        return new LogicalOlapScan(relationId, (Table) table, qualifier,
                groupExpression, Optional.of(logicalProperties),
                selectedPartitionIds, partitionPruned, selectedTabletIds,
                selectedIndexId, indexSelected, preAggStatus, manuallySpecifiedPartitions,
                hints, cacheSlotWithSlotName, tableSample, directMvScan, colToSubPathsMap,
                manuallySpecifiedTabletIds, operativeSlots, virtualColumns, scoreOrderKeys, scoreLimit,
                annOrderKeys, annLimit);
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
        IdGenerator<ExprId> exprIdGenerator = StatementScopeIdGenerator.getExprIdGenerator();
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
                                exprIdGenerator.getNextId(), table, baseSchema.get(i), qualified()
                        ).withSubPath(subPath);
                        slots.add(slotReference);
                        subPathToSlotMap.computeIfAbsent(slot, k -> Maps.newHashMap())
                                .put(subPath, slotReference);
                    }

                }
            }
        }
        // add virtual slots
        for (NamedExpression virtualColumn : virtualColumns) {
            slots.add(virtualColumn.toSlot());
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
        IdGenerator<ExprId> exprIdGenerator = StatementScopeIdGenerator.getExprIdGenerator();
        for (Column c : schema) {
            slots.addAll(generateUniqueSlot(
                    olapTable, c, indexId == ((OlapTable) table).getBaseIndexId(), indexId, exprIdGenerator
            ));
        }
        // add virtual slots, TODO: maybe wrong, should test virtual column + sync mv
        for (NamedExpression virtualColumn : virtualColumns) {
            slots.add(virtualColumn.toSlot());
        }
        return slots;
    }

    private List<Slot> generateUniqueSlot(OlapTable table, Column column, boolean isBaseIndex, long indexId,
            IdGenerator<ExprId> exprIdIdGenerator) {
        String name = column.getName();
        Pair<Long, String> key = Pair.of(indexId, name);
        Slot slot = cacheSlotWithSlotName.computeIfAbsent(key, k ->
                SlotReference.fromColumn(exprIdIdGenerator.getNextId(), table, column, name, qualified()));
        List<Slot> slots = Lists.newArrayList(slot);
        if (colToSubPathsMap.containsKey(key.getValue())) {
            for (List<String> subPath : colToSubPathsMap.get(key.getValue())) {
                if (!subPath.isEmpty()) {
                    SlotReference slotReference = SlotReference.fromColumn(
                            exprIdIdGenerator.getNextId(), table, column, name, qualified()
                    ).withSubPath(subPath);
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

    public String getQualifierWithRelationId() {
        String fullQualifier = getTable().getNameWithFullQualifiers();
        String relationId = getRelationId().toString();
        return fullQualifier + "#" + relationId;
    }

    public boolean isDirectMvScan() {
        return directMvScan;
    }

    public boolean isPreAggStatusUnSet() {
        return preAggStatus.isUnset();
    }

    public List<OrderKey> getScoreOrderKeys() {
        return scoreOrderKeys;
    }

    public Optional<Long> getScoreLimit() {
        return scoreLimit;
    }

    public List<OrderKey> getAnnOrderKeys() {
        return annOrderKeys;
    }

    public Optional<Long> getAnnLimit() {
        return annLimit;
    }

    private List<SlotReference> createSlotsVectorized(List<Column> columns) {
        List<String> qualified = qualified();
        SlotReference[] slots = new SlotReference[columns.size()];
        IdGenerator<ExprId> exprIdGenerator = StatementScopeIdGenerator.getExprIdGenerator();
        for (int i = 0; i < columns.size(); i++) {
            ExprId nextId = exprIdGenerator.getNextId();
            slots[i] = SlotReference.fromColumn(nextId, table, columns.get(i), qualified);
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

    @Override
    public void computeUnique(DataTrait.Builder builder) {
        super.computeUnique(builder);
        if (this.selectedIndexId != getTable().getBaseIndexId()) {
            /*
                computing unique doesn't work for mv, because mv's key may be different from base table
                and the key can be any expression which is difficult to deduce if it's unique. for example
                base table:
                CREATE TABLE IF NOT EXISTS base(
                    siteid INT(11) NOT NULL,
                    citycode SMALLINT(6) NOT NULL,
                    username VARCHAR(32) NOT NULL,
                    pv BIGINT(20) SUM NOT NULL DEFAULT '0'
                )
                AGGREGATE KEY (siteid,citycode,username)
                DISTRIBUTED BY HASH(siteid) BUCKETS 5 properties("replication_num" = "1");

                case1:
                create mv1:
                create materialized view mv1 as select siteid, sum(pv) from base group by siteid;
                the base table siteid + citycode + username is unique but the mv1's agg key siteid is not unique

                case2:
                create mv2:
                create materialized view mv2 as select citycode * citycode, siteid, username from base;
                the mv2's agg key citycode * citycode is not unique

                for simplicity, we disable unique compute for mv
             */
            return;
        }
        Set<Slot> outputSet = Utils.fastToImmutableSet(getOutputSet());
        if (getTable() instanceof MTMV) {
            MTMV mtmv = (MTMV) getTable();
            MTMVCache cache;
            try {
                cache = mtmv.getOrGenerateCache(ConnectContext.get());
            } catch (Exception e) {
                LOG.warn(String.format("LogicalOlapScan computeUnique fail, mv name is %s", mtmv.getName()), e);
                return;
            }
            // Maybe query specified index, should not calc, such as select count(*) from t1 index col_index
            if (this.getSelectedIndexId() != this.getTable().getBaseIndexId()) {
                return;
            }
            Plan originalPlan = cache.getOriginalFinalPlan();
            builder.addUniqueSlot(originalPlan.getLogicalProperties().getTrait());
            builder.replaceUniqueBy(constructReplaceMap(mtmv));
        } else if (getTable().getKeysType().isAggregationFamily() && !getTable().isRandomDistribution()) {
            // When skipDeleteBitmap is set to true, in the unique model, rows that are replaced due to having the same
            // unique key will also be read. As a result, the uniqueness of the unique key cannot be guaranteed.
            if (ConnectContext.get().getSessionVariable().skipDeleteBitmap
                    && getTable().getKeysType() == KeysType.UNIQUE_KEYS) {
                return;
            }
            ImmutableSet.Builder<Slot> uniqSlots = ImmutableSet.builderWithExpectedSize(outputSet.size());
            for (Slot slot : outputSet) {
                if (!(slot instanceof SlotReference)) {
                    continue;
                }
                SlotReference slotRef = (SlotReference) slot;
                if (slotRef.getOriginalColumn().isPresent() && slotRef.getOriginalColumn().get().isKey()) {
                    uniqSlots.add(slot);
                }
            }
            builder.addUniqueSlot(uniqSlots.build());
        }
    }

    @Override
    public void computeUniform(DataTrait.Builder builder) {
        if (getTable() instanceof MTMV) {
            MTMV mtmv = (MTMV) getTable();
            MTMVCache cache;
            try {
                cache = mtmv.getOrGenerateCache(ConnectContext.get());
            } catch (Exception e) {
                LOG.warn(String.format("LogicalOlapScan computeUniform fail, mv name is %s", mtmv.getName()), e);
                return;
            }
            // Maybe query specified index, should not calc, such as select count(*) from t1 index col_index
            if (this.getSelectedIndexId() != this.getTable().getBaseIndexId()) {
                return;
            }
            Plan originalPlan = cache.getOriginalFinalPlan();
            builder.addUniformSlot(originalPlan.getLogicalProperties().getTrait());
            builder.replaceUniformBy(constructReplaceMap(mtmv));
        }
    }

    @Override
    public void computeEqualSet(DataTrait.Builder builder) {
        if (getTable() instanceof MTMV) {
            MTMV mtmv = (MTMV) getTable();
            MTMVCache cache;
            try {
                cache = mtmv.getOrGenerateCache(ConnectContext.get());
            } catch (Exception e) {
                LOG.warn(String.format("LogicalOlapScan computeEqualSet fail, mv name is %s", mtmv.getName()), e);
                return;
            }
            // Maybe query specified index, should not calc, such as select count(*) from t1 index col_index
            if (this.getSelectedIndexId() != this.getTable().getBaseIndexId()) {
                return;
            }
            Plan originalPlan = cache.getOriginalFinalPlan();
            builder.addEqualSet(originalPlan.getLogicalProperties().getTrait());
            builder.replaceEqualSetBy(constructReplaceMap(mtmv));
        }
    }

    @Override
    public void computeFd(DataTrait.Builder builder) {
        if (getTable() instanceof MTMV) {
            MTMV mtmv = (MTMV) getTable();
            MTMVCache cache;
            try {
                cache = mtmv.getOrGenerateCache(ConnectContext.get());
            } catch (Exception e) {
                LOG.warn(String.format("LogicalOlapScan computeFd fail, mv name is %s", mtmv.getName()), e);
                return;
            }
            // Maybe query specified index, should not calc, such as select count(*) from t1 index col_index
            if (this.getSelectedIndexId() != this.getTable().getBaseIndexId()) {
                return;
            }
            Plan originalPlan = cache.getOriginalFinalPlan();
            builder.addFuncDepsDG(originalPlan.getLogicalProperties().getTrait());
            builder.replaceFuncDepsBy(constructReplaceMap(mtmv));
        }
    }

    @Override
    public CatalogRelation withOperativeSlots(Collection<Slot> operativeSlots) {
        return new LogicalOlapScan(relationId, (Table) table, qualifier,
                groupExpression, Optional.of(getLogicalProperties()),
                selectedPartitionIds, partitionPruned, selectedTabletIds,
                selectedIndexId, indexSelected, preAggStatus, manuallySpecifiedPartitions,
                hints, cacheSlotWithSlotName, tableSample, directMvScan, colToSubPathsMap,
                manuallySpecifiedTabletIds, operativeSlots, virtualColumns, scoreOrderKeys, scoreLimit,
                annOrderKeys, annLimit);
    }

    private Map<Slot, Slot> constructReplaceMap(MTMV mtmv) {
        Map<Slot, Slot> replaceMap = new HashMap<>();
        // Need remove invisible column, and then mapping them
        List<Slot> originOutputs = new ArrayList<>();
        MTMVCache cache;
        try {
            cache = mtmv.getOrGenerateCache(ConnectContext.get());
        } catch (Exception e) {
            LOG.warn(String.format("LogicalOlapScan constructReplaceMap fail, mv name is %s", mtmv.getName()), e);
            return replaceMap;
        }
        for (Slot originSlot : cache.getOriginalFinalPlan().getOutput()) {
            if (!(originSlot instanceof SlotReference) || (((SlotReference) originSlot).isVisible())) {
                originOutputs.add(originSlot);
            }
        }
        List<Slot> targetOutputs = new ArrayList<>();
        for (Slot targeSlot : getOutput()) {
            if (!(targeSlot instanceof SlotReference) || (((SlotReference) targeSlot).isVisible())) {
                targetOutputs.add(targeSlot);
            }
        }
        Preconditions.checkArgument(originOutputs.size() == targetOutputs.size(),
                "constructReplaceMap, the size of originOutputs and targetOutputs should be same");
        for (int i = 0; i < targetOutputs.size(); i++) {
            replaceMap.put(originOutputs.get(i), targetOutputs.get(i));
        }
        return replaceMap;
    }
}
