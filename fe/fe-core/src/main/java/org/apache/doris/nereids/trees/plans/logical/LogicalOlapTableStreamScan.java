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

import org.apache.doris.analysis.TableScanParams;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.constraint.TableIdentifier;
import org.apache.doris.catalog.stream.OlapTableStream;
import org.apache.doris.catalog.stream.OlapTableStreamWrapper;
import org.apache.doris.catalog.stream.StreamReadMode;
import org.apache.doris.common.IdGenerator;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.rules.analysis.BindRelation;
import org.apache.doris.nereids.trees.TableSample;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.plans.AbstractPlan;
import org.apache.doris.nereids.trees.plans.PartitionPrunablePredicate;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PreAggStatus;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.ScoreRangeInfo;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.tuple.Pair;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Logical OlapTableStreamScan
 */
public class LogicalOlapTableStreamScan extends LogicalOlapScan {
    private final StreamReadMode readMode;

    /**
     * LogicalOlapTableStreamScan construct method
     */
    public LogicalOlapTableStreamScan(RelationId id, OlapTable table, List<String> qualifier, List<Long> tabletIds,
                           List<String> hints, Optional<TableSample> tableSample, Collection<Slot> operativeSlots) {
        super(id, table, qualifier, tabletIds, hints, tableSample, operativeSlots);
        this.readMode = StreamReadMode.INCREMENTAL;
    }

    /**
     * LogicalOlapTableStreamScan construct method
     */
    public LogicalOlapTableStreamScan(RelationId id, OlapTable table, List<String> qualifier,
                                             List<Long> specifiedPartitions, List<Long> tabletIds, List<String> hints,
                                             Optional<TableSample> tableSample, List<Slot> operativeSlots) {
        super(id, table, qualifier, specifiedPartitions, tabletIds, hints, tableSample, operativeSlots);
        this.readMode = StreamReadMode.INCREMENTAL;
    }

    /**
     * LogicalOlapTableStreamScan construct method
     */
    public LogicalOlapTableStreamScan(RelationId id, Table table, List<String> qualifier,
                                      Optional<GroupExpression> groupExpression,
                                      Optional<LogicalProperties> logicalProperties,
                                      List<Long> selectedPartitionIds, boolean partitionPruned,
                                      boolean hasPartitionPredicate,
                                      List<Long> selectedTabletIds, long selectedIndexId, boolean indexSelected,
                                      PreAggStatus preAggStatus, List<Long> specifiedPartitions,
                                      List<String> hints, Map<Pair<Long, String>, Slot> cacheSlotWithSlotName,
                                      Optional<List<Slot>> cachedOutput, Optional<TableSample> tableSample,
                                      boolean directMvScan,
                                      Map<String, Set<List<String>>> colToSubPathsMap, List<Long> specifiedTabletIds,
                                      Collection<Slot> operativeSlots, List<NamedExpression> virtualColumns,
                                      List<OrderKey> scoreOrderKeys, Optional<Long> scoreLimit,
                                      Optional<ScoreRangeInfo> scoreRangeInfo,
                                      List<OrderKey> annOrderKeys, Optional<Long> annLimit, String tableAlias,
                                      Optional<PartitionPrunablePredicate> partitionPrunablePredicates,
                                      Optional<TableScanParams> scanParams,
                                      StreamReadMode readMode) {
        super(id, table, qualifier, groupExpression, logicalProperties,
                selectedPartitionIds, partitionPruned, hasPartitionPredicate, selectedTabletIds, selectedIndexId,
                indexSelected, preAggStatus, specifiedPartitions, hints, cacheSlotWithSlotName, cachedOutput,
                tableSample, directMvScan, colToSubPathsMap, specifiedTabletIds, operativeSlots, virtualColumns,
                scoreOrderKeys, scoreLimit, scoreRangeInfo, annOrderKeys, annLimit, tableAlias,
                partitionPrunablePredicates, scanParams);
        this.readMode = Objects.requireNonNull(readMode, "readMode can not be null");
    }

    @Override
    public LogicalOlapTableStreamScan withManuallySpecifiedTabletIds(List<Long> manuallySpecifiedTabletIds) {
        return AbstractPlan.copyWithSameId(this, () ->
                new LogicalOlapTableStreamScan(relationId, (Table) table, qualifier,
                        Optional.empty(), Optional.of(getLogicalProperties()),
                        selectedPartitionIds, partitionPruned, hasPartitionPredicate, selectedTabletIds,
                        selectedIndexId, indexSelected, preAggStatus, manuallySpecifiedPartitions,
                        hints, cacheSlotWithSlotName, cachedOutput, tableSample, directMvScan,
                        colToSubPathsMap, manuallySpecifiedTabletIds, operativeSlots, virtualColumns,
                        scoreOrderKeys, scoreLimit, scoreRangeInfo, annOrderKeys, annLimit, tableAlias,
                        partitionPrunablePredicates, scanParams, readMode));
    }

    @Override
    public List<Slot> computeOutput() {
        if (cachedOutput.isPresent()) {
            return cachedOutput.get();
        }
        // use full schema, and filter hidden columns except IVM row-id during IVM rewrite below
        List<Column> baseSchema = table.getBaseSchema(true);
        List<SlotReference> slotFromColumn = createSlotsVectorized(baseSchema);

        ConnectContext connectContext = ConnectContext.get();
        boolean ivmRewriteEnabled = connectContext != null
                && connectContext.getStatementContext() != null
                && connectContext.getStatementContext().isIvmMTMVRewrite();
        // SNAPSHOT reads on DUP tables expose the base row LSN column, so it stays accessible
        // the same way as on the base table.
        boolean snapshotDupTable = readMode == StreamReadMode.SNAPSHOT
                && table instanceof OlapTable
                && ((OlapTable) table).getKeysType() == KeysType.DUP_KEYS;

        ImmutableList.Builder<Slot> slots = ImmutableList.builder();
        IdGenerator<ExprId> exprIdGenerator = StatementScopeIdGenerator.getExprIdGenerator();
        for (int i = 0; i < baseSchema.size(); i++) {
            // skip binlog before column
            final int index = i;
            Column col = baseSchema.get(i);
            if (col.getName().startsWith(Column.BINLOG_BEFORE_PREFIX)) {
                continue;
            }
            // Keep visible columns, and IVM row-id / row lsn during IVM rewrite.
            // Skip all other hidden columns. For reset, we could use get full schema of
            // base table; otherwise, we only need to get the schema without hidden columns.
            // DUP detail tables always carry __DORIS_ROW_LSN_COL__ (added at create time when
            // row binlog is enabled); IVM needs it as the stable row identity. RESET/SNAPSHOT
            // stream scans are expanded from the base table olap scan, which carries the row
            // lsn column directly. INCREMENTAL stream scans are expanded from the row-binlog
            // (which carries the value as __DORIS_BINLOG_LSN__), so they expose the stream lsn
            // virtual column instead; the IVM delta rewrite maps it back to the row lsn slot.
            boolean ivmRewriteColumn = ivmRewriteEnabled
                    && (Column.IVM_ROW_ID_COL.equals(col.getName())
                            || (Column.ROW_LSN_COL.equals(col.getName()) && !isIncremental()));
            boolean snapshotDupRowLsn = snapshotDupTable && Column.ROW_LSN_COL.equals(col.getName());
            if (!col.isVisible() && !isReset() && !ivmRewriteColumn && !snapshotDupRowLsn) {
                continue;
            }
            Pair<Long, String> key = Pair.of(selectedIndexId, col.getName());
            // INCREMENTAL reads materialize value columns from the base table row-binlog whose
            // after/before value columns are always nullable (see Column.generateAfterValueColumn /
            // generateBeforeValueColumn). SNAPSHOT reads scan the base table directly (DUP tables;
            // MOW normal partitions) but may also rebuild partitions from binlog, so declaring
            // every non-RESET value column nullable is a safe widening that keeps the stream scan
            // output consistent with the plan expanded in NormalizeOlapTableStreamScan; otherwise
            // AdjustNullable reports a not-nullable -> nullable conflict. RESET does a full
            // base-table scan only, so it keeps its original nullability.
            Slot slot = cacheSlotWithSlotName.computeIfAbsent(key, k -> {
                SlotReference slotRef = slotFromColumn.get(index);
                boolean forceNullable = readMode != StreamReadMode.RESET && !baseSchema.get(index).isKey();
                return forceNullable ? slotRef.withNullable(true) : slotRef;
            });
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
        if (isIncremental()) {
            // add stream exclusive virtual columns.
            slots.add(SlotReference.fromColumn(
                    exprIdGenerator.getNextId(), table, Column.STREAM_SEQ_VIRTUAL_COLUMN, qualified()));
            // Only expose stream LSN when the base table stores row LSN, e.g. dup table with binlog.
            if (table instanceof OlapTable && ((OlapTable) table).hasRowLsnColumn()) {
                slots.add(SlotReference.fromColumn(
                        exprIdGenerator.getNextId(), table, Column.STREAM_LSN_VIRTUAL_COLUMN, qualified()));
            }
            slots.add(SlotReference.fromColumn(
                    exprIdGenerator.getNextId(), table, Column.STREAM_CHANGE_TYPE_VIRTUAL_COLUMN, qualified()));
        }
        for (NamedExpression virtualColumn : virtualColumns) {
            slots.add(virtualColumn.toSlot());
        }
        return slots.build();
    }

    /**
     * withSelectedTabletIds
     */
    @Override
    public LogicalOlapTableStreamScan withSelectedTabletIds(List<Long> selectedTabletIds) {
        return AbstractPlan.copyWithSameId(this, () ->
                new LogicalOlapTableStreamScan(relationId, (Table) table, qualifier,
                        Optional.empty(), Optional.of(getLogicalProperties()),
                        selectedPartitionIds, partitionPruned, hasPartitionPredicate, selectedTabletIds,
                        selectedIndexId, indexSelected, preAggStatus, manuallySpecifiedPartitions,
                        hints, cacheSlotWithSlotName, cachedOutput, tableSample, directMvScan, colToSubPathsMap,
                        manuallySpecifiedTabletIds, operativeSlots, virtualColumns, scoreOrderKeys, scoreLimit,
                        scoreRangeInfo, annOrderKeys, annLimit, tableAlias, partitionPrunablePredicates,
                        scanParams, readMode));
    }

    /** withCachedOutput */
    @Override
    public LogicalOlapTableStreamScan withCachedOutput(List<Slot> outputSlots) {
        return AbstractPlan.copyWithSameId(this, () ->
                new LogicalOlapTableStreamScan(relationId, (Table) table, qualifier,
                        groupExpression, Optional.empty(),
                        selectedPartitionIds, partitionPruned, hasPartitionPredicate, selectedTabletIds,
                        selectedIndexId, indexSelected, preAggStatus, manuallySpecifiedPartitions, hints,
                        cacheSlotWithSlotName, Optional.of(outputSlots), tableSample, directMvScan, colToSubPathsMap,
                        manuallySpecifiedTabletIds, operativeSlots, virtualColumns, scoreOrderKeys, scoreLimit,
                        scoreRangeInfo, annOrderKeys, annLimit, tableAlias, partitionPrunablePredicates,
                        scanParams, readMode));
    }

    @Override
    public LogicalOlapTableStreamScan withOperativeSlots(Collection<Slot> operativeSlots) {
        return AbstractPlan.copyWithSameId(this, () ->
                new LogicalOlapTableStreamScan(relationId, (Table) table, qualifier,
                        groupExpression, Optional.of(getLogicalProperties()),
                        selectedPartitionIds, partitionPruned, hasPartitionPredicate, selectedTabletIds,
                        selectedIndexId, indexSelected, preAggStatus, manuallySpecifiedPartitions,
                        hints, cacheSlotWithSlotName, cachedOutput, tableSample, directMvScan, colToSubPathsMap,
                        manuallySpecifiedTabletIds, operativeSlots, virtualColumns, scoreOrderKeys, scoreLimit,
                        scoreRangeInfo, annOrderKeys, annLimit, tableAlias, partitionPrunablePredicates,
                        scanParams, readMode));
    }

    /**
     * withPreAggStatus
     */
    @Override
    public LogicalOlapTableStreamScan withPreAggStatus(PreAggStatus preAggStatus) {
        return AbstractPlan.copyWithSameId(this, () ->
                new LogicalOlapTableStreamScan(relationId, (Table) table, qualifier,
                        Optional.empty(), Optional.of(getLogicalProperties()),
                        selectedPartitionIds, partitionPruned, hasPartitionPredicate, selectedTabletIds,
                        selectedIndexId, indexSelected, preAggStatus, manuallySpecifiedPartitions,
                        hints, cacheSlotWithSlotName, cachedOutput, tableSample, directMvScan,
                        colToSubPathsMap, manuallySpecifiedTabletIds, operativeSlots, virtualColumns,
                        scoreOrderKeys, scoreLimit, scoreRangeInfo, annOrderKeys, annLimit, tableAlias,
                        partitionPrunablePredicates, scanParams, readMode));
    }

    /**
     * withGroupExpression
     */
    @Override
    public LogicalOlapTableStreamScan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return AbstractPlan.copyWithSameId(this, () ->
                new LogicalOlapTableStreamScan(relationId, (Table) table, qualifier,
                        groupExpression, Optional.of(getLogicalProperties()),
                        selectedPartitionIds, partitionPruned, hasPartitionPredicate, selectedTabletIds,
                        selectedIndexId, indexSelected, preAggStatus, manuallySpecifiedPartitions,
                        hints, cacheSlotWithSlotName, cachedOutput, tableSample, directMvScan,
                        colToSubPathsMap, manuallySpecifiedTabletIds, operativeSlots, virtualColumns,
                        scoreOrderKeys, scoreLimit, scoreRangeInfo, annOrderKeys, annLimit, tableAlias,
                        partitionPrunablePredicates, scanParams, readMode));
    }

    /**
     * withGroupExprLogicalPropChildren
     */
    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        return AbstractPlan.copyWithSameId(this, () ->
                new LogicalOlapTableStreamScan(relationId, (Table) table, qualifier,
                        groupExpression, logicalProperties,
                        selectedPartitionIds, partitionPruned, hasPartitionPredicate, selectedTabletIds,
                        selectedIndexId, indexSelected, preAggStatus, manuallySpecifiedPartitions,
                        hints, cacheSlotWithSlotName, cachedOutput, tableSample, directMvScan,
                        colToSubPathsMap, manuallySpecifiedTabletIds, operativeSlots, virtualColumns,
                        scoreOrderKeys, scoreLimit, scoreRangeInfo, annOrderKeys, annLimit, tableAlias,
                        partitionPrunablePredicates, scanParams, readMode));
    }

    /**
     * withSelectedPartitionIds
     */
    @Override
    public LogicalOlapTableStreamScan withSelectedPartitionIds(List<Long> selectedPartitionIdsd) {
        return withSelectedPartitionIds(selectedPartitionIdsd, false);
    }

    /**
     * withSelectedPartitionIds
     */
    @Override
    public LogicalOlapTableStreamScan withSelectedPartitionIds(List<Long> selectedPartitionIds,
                                                               boolean hasPartitionPredicate) {
        return AbstractPlan.copyWithSameId(this, () ->
                new LogicalOlapTableStreamScan(relationId, (Table) table, qualifier,
                        groupExpression, Optional.of(getLogicalProperties()),
                        selectedPartitionIds, true, hasPartitionPredicate, selectedTabletIds,
                        selectedIndexId, indexSelected, preAggStatus, manuallySpecifiedPartitions,
                        hints, cacheSlotWithSlotName, cachedOutput, tableSample, directMvScan,
                        colToSubPathsMap, manuallySpecifiedTabletIds, operativeSlots, virtualColumns,
                        scoreOrderKeys, scoreLimit, scoreRangeInfo, annOrderKeys, annLimit, tableAlias,
                        partitionPrunablePredicates, scanParams, readMode));
    }

    @Override
    public LogicalOlapTableStreamScan withPartitionPruned(boolean partitionPruned) {
        return AbstractPlan.copyWithSameId(this, () ->
                new LogicalOlapTableStreamScan(relationId, (Table) table, qualifier,
                        groupExpression, Optional.of(getLogicalProperties()),
                        selectedPartitionIds, partitionPruned, hasPartitionPredicate, selectedTabletIds,
                        selectedIndexId, indexSelected, preAggStatus, manuallySpecifiedPartitions,
                        hints, cacheSlotWithSlotName, cachedOutput, tableSample, directMvScan,
                        colToSubPathsMap, manuallySpecifiedTabletIds, operativeSlots, virtualColumns,
                        scoreOrderKeys, scoreLimit, scoreRangeInfo, annOrderKeys, annLimit, tableAlias,
                        partitionPrunablePredicates, scanParams, readMode));
    }

    /**
     * Returns a new {@code LogicalOlapScan} carrying the supplied
     * {@link PartitionPrunablePredicate}. It is preserved across all other
     * {@code with*} builders so partition-derived conjuncts can be removed
     * safely after MV rewrite has had a chance to match the plan.
     */
    @Override
    public LogicalOlapTableStreamScan withPartitionPrunablePredicates(
            Optional<PartitionPrunablePredicate> partitionPrunablePredicates) {
        return AbstractPlan.copyWithSameId(this, () ->
                new LogicalOlapTableStreamScan(relationId, (Table) table, qualifier,
                        groupExpression, Optional.of(getLogicalProperties()),
                        selectedPartitionIds, partitionPruned, hasPartitionPredicate, selectedTabletIds,
                        selectedIndexId, indexSelected, preAggStatus, manuallySpecifiedPartitions,
                        hints, cacheSlotWithSlotName, cachedOutput, tableSample, directMvScan,
                        colToSubPathsMap, manuallySpecifiedTabletIds, operativeSlots, virtualColumns,
                        scoreOrderKeys, scoreLimit, scoreRangeInfo, annOrderKeys, annLimit, tableAlias,
                        partitionPrunablePredicates, scanParams, readMode));
    }

    /**
     * with sync materialized index id.
     */
    @Override
    public LogicalOlapTableStreamScan withMaterializedIndexSelected(long indexId) {
        return AbstractPlan.copyWithSameId(this, () ->
                new LogicalOlapTableStreamScan(relationId, (Table) table, qualifier,
                        Optional.empty(), Optional.of(getLogicalProperties()),
                        selectedPartitionIds, partitionPruned, hasPartitionPredicate, selectedTabletIds,
                        indexId, true, PreAggStatus.unset(), manuallySpecifiedPartitions,
                        hints, cacheSlotWithSlotName, cachedOutput, tableSample, directMvScan,
                        colToSubPathsMap, manuallySpecifiedTabletIds, operativeSlots, virtualColumns,
                        scoreOrderKeys, scoreLimit, scoreRangeInfo, annOrderKeys, annLimit, tableAlias,
                        partitionPrunablePredicates, scanParams, readMode));
    }

    /**
     * constructor
     */
    @Override
    public LogicalOlapTableStreamScan withColToSubPathsMap(Map<String, Set<List<String>>> colToSubPathsMap) {
        return AbstractPlan.copyWithSameId(this, () ->
                new LogicalOlapTableStreamScan(relationId, (Table) table, qualifier,
                        Optional.empty(), Optional.empty(),
                        selectedPartitionIds, partitionPruned, hasPartitionPredicate, selectedTabletIds,
                        selectedIndexId, indexSelected, preAggStatus, manuallySpecifiedPartitions,
                        hints, cacheSlotWithSlotName, cachedOutput, tableSample, directMvScan,
                        colToSubPathsMap, manuallySpecifiedTabletIds, operativeSlots, virtualColumns,
                        scoreOrderKeys, scoreLimit, scoreRangeInfo, annOrderKeys, annLimit, tableAlias,
                        partitionPrunablePredicates, scanParams, readMode));
    }

    /** withTableScanParams */
    @Override
    public LogicalOlapTableStreamScan withTableScanParams(TableScanParams scanParams) {
        return AbstractPlan.copyWithSameId(this, () ->
                new LogicalOlapTableStreamScan(relationId, (Table) table, qualifier,
                        groupExpression, Optional.of(getLogicalProperties()),
                        selectedPartitionIds, partitionPruned, hasPartitionPredicate, selectedTabletIds,
                        selectedIndexId, indexSelected, preAggStatus, manuallySpecifiedPartitions,
                        hints, cacheSlotWithSlotName, cachedOutput, tableSample, directMvScan, colToSubPathsMap,
                        manuallySpecifiedTabletIds, operativeSlots, virtualColumns, scoreOrderKeys, scoreLimit,
                        scoreRangeInfo, annOrderKeys, annLimit, tableAlias, partitionPrunablePredicates,
                        Optional.of(scanParams), readMode));
    }

    /**
     * withRelationId
     */
    @Override
    public LogicalOlapTableStreamScan withRelationId(RelationId relationId) {
        return AbstractPlan.copyWithSameId(this, () ->
                new LogicalOlapTableStreamScan(relationId, (Table) table, qualifier,
                        groupExpression, Optional.empty(),
                        selectedPartitionIds, partitionPruned, hasPartitionPredicate, selectedTabletIds,
                        selectedIndexId, indexSelected, preAggStatus, manuallySpecifiedPartitions,
                        hints, cacheSlotWithSlotName, cachedOutput, tableSample, directMvScan, colToSubPathsMap,
                        manuallySpecifiedTabletIds, operativeSlots, virtualColumns, scoreOrderKeys, scoreLimit,
                        scoreRangeInfo, annOrderKeys, annLimit, tableAlias, partitionPrunablePredicates,
                        scanParams, readMode));
    }

    @Override
    public LogicalOlapTableStreamScan withTableAlias(String tableAlias) {
        return AbstractPlan.copyWithSameId(this, () ->
                new LogicalOlapTableStreamScan(relationId, (Table) table, qualifier,
                        Optional.empty(), Optional.of(getLogicalProperties()),
                        selectedPartitionIds, partitionPruned, hasPartitionPredicate, selectedTabletIds,
                        selectedIndexId, indexSelected, preAggStatus, manuallySpecifiedPartitions,
                        hints, cacheSlotWithSlotName, cachedOutput, tableSample, directMvScan,
                        colToSubPathsMap, manuallySpecifiedTabletIds, operativeSlots, virtualColumns,
                        scoreOrderKeys, scoreLimit, scoreRangeInfo, annOrderKeys, annLimit, tableAlias,
                        partitionPrunablePredicates, scanParams, readMode));
    }

    @Override
    public LogicalOlapTableStreamScan withVirtualColumns(List<NamedExpression> virtualColumns) {
        LogicalProperties logicalProperties = getLogicalProperties();
        List<Slot> output = Lists.newArrayList(logicalProperties.getOutput());
        output.addAll(virtualColumns.stream().map(NamedExpression::toSlot).collect(Collectors.toList()));
        LogicalProperties finalLogicalProperties = new LogicalProperties(() -> output, this::computeDataTrait);
        return AbstractPlan.copyWithSameId(this, () ->
                new LogicalOlapTableStreamScan(relationId, (Table) table, qualifier,
                        groupExpression, Optional.of(finalLogicalProperties),
                        selectedPartitionIds, partitionPruned, hasPartitionPredicate, selectedTabletIds,
                        selectedIndexId, indexSelected, preAggStatus, manuallySpecifiedPartitions,
                        hints, cacheSlotWithSlotName, cachedOutput, tableSample, directMvScan,
                        colToSubPathsMap, manuallySpecifiedTabletIds, operativeSlots, virtualColumns,
                        scoreOrderKeys, scoreLimit, scoreRangeInfo, annOrderKeys, annLimit, tableAlias,
                        partitionPrunablePredicates, scanParams,
                        readMode));
    }

    @Override
    public LogicalOlapTableStreamScan appendVirtualColumns(List<NamedExpression> additionalVirtualColumns) {
        LogicalProperties logicalProperties = getLogicalProperties();
        List<Slot> output = Lists.newArrayList(logicalProperties.getOutput());
        output.addAll(additionalVirtualColumns.stream().map(NamedExpression::toSlot)
                .collect(Collectors.toList()));
        logicalProperties = new LogicalProperties(() -> output, this::computeDataTrait);
        List<NamedExpression> mergedVirtualColumns = ImmutableList.<NamedExpression>builder()
                .addAll(this.virtualColumns)
                .addAll(additionalVirtualColumns)
                .build();
        return new LogicalOlapTableStreamScan(relationId, (Table) table, qualifier,
                groupExpression, Optional.of(logicalProperties),
                selectedPartitionIds, partitionPruned, hasPartitionPredicate, selectedTabletIds,
                selectedIndexId, indexSelected, preAggStatus, manuallySpecifiedPartitions,
                hints, cacheSlotWithSlotName, cachedOutput, tableSample, directMvScan, colToSubPathsMap,
                manuallySpecifiedTabletIds, operativeSlots, mergedVirtualColumns, scoreOrderKeys, scoreLimit,
                scoreRangeInfo, annOrderKeys, annLimit, tableAlias, partitionPrunablePredicates, scanParams,
                readMode);
    }

    @Override
    public LogicalOlapTableStreamScan appendVirtualColumnsAndTopN(
            List<NamedExpression> additionalVirtualColumns,
            List<OrderKey> annOrderKeys,
            Optional<Long> annLimit,
            List<OrderKey> scoreOrderKeys,
            Optional<Long> scoreLimit,
            Optional<ScoreRangeInfo> scoreRangeInfo) {
        LogicalProperties logicalProperties = getLogicalProperties();
        List<Slot> output = Lists.newArrayList(logicalProperties.getOutput());
        output.addAll(additionalVirtualColumns.stream().map(NamedExpression::toSlot)
                .collect(Collectors.toList()));
        logicalProperties = new LogicalProperties(() -> output, this::computeDataTrait);
        List<NamedExpression> mergedVirtualColumns = ImmutableList.<NamedExpression>builder()
                .addAll(this.virtualColumns)
                .addAll(additionalVirtualColumns)
                .build();
        return new LogicalOlapTableStreamScan(relationId, (Table) table, qualifier,
                groupExpression, Optional.of(logicalProperties),
                selectedPartitionIds, partitionPruned, hasPartitionPredicate, selectedTabletIds,
                selectedIndexId, indexSelected, preAggStatus, manuallySpecifiedPartitions,
                hints, cacheSlotWithSlotName, cachedOutput, tableSample, directMvScan, colToSubPathsMap,
                manuallySpecifiedTabletIds, operativeSlots, mergedVirtualColumns, scoreOrderKeys, scoreLimit,
                scoreRangeInfo, annOrderKeys, annLimit, tableAlias, partitionPrunablePredicates, scanParams,
                readMode);
    }

    /**
     * withReadMode
     */
    public LogicalOlapTableStreamScan withReadMode(StreamReadMode readMode) {
        return AbstractPlan.copyWithSameId(this, () ->
                new LogicalOlapTableStreamScan(relationId, (Table) table, qualifier,
                        groupExpression, Optional.empty(),
                        selectedPartitionIds, partitionPruned, hasPartitionPredicate, selectedTabletIds,
                        selectedIndexId, indexSelected, preAggStatus, manuallySpecifiedPartitions,
                        hints, cacheSlotWithSlotName, cachedOutput, tableSample, directMvScan, colToSubPathsMap,
                        manuallySpecifiedTabletIds, operativeSlots, virtualColumns, scoreOrderKeys, scoreLimit,
                        scoreRangeInfo, annOrderKeys, annLimit, tableAlias, partitionPrunablePredicates,
                        scanParams, readMode));
    }

    @Override
    protected boolean hasSameTableIdentity(LogicalCatalogRelation other) {
        if (!Utils.isSameClass(this, other)) {
            return false;
        }
        LogicalOlapTableStreamScan that = (LogicalOlapTableStreamScan) other;
        return Objects.equals(getTable().getStreamDbId(), that.getTable().getStreamDbId())
                && Objects.equals(getTable().getStreamId(), that.getTable().getStreamId())
                && new TableIdentifier(getTable().getBaseTable())
                .equals(new TableIdentifier(that.getTable().getBaseTable()));
    }

    @Override
    protected boolean hasSameScanState(LogicalCatalogRelation other) {
        if (!Utils.isSameClass(this, other)) {
            return false;
        }
        LogicalOlapTableStreamScan that = (LogicalOlapTableStreamScan) other;
        return super.hasSameScanState(other)
                && readMode == that.readMode
                && getTable().getStreamKeysType() == that.getTable().getStreamKeysType()
                && Objects.equals(getTable().getOutputUpdateMap(), that.getTable().getOutputUpdateMap());
    }

    @Override
    public LogicalPlan withPreSnapshot(Optional<OlapTableStream> stream) {
        return withReadMode(StreamReadMode.SNAPSHOT);
    }

    @Override
    public LogicalPlan withPostSnapshot() {
        OlapTable baseTable = getTable().getBaseTable();
        LogicalOlapScan scan = new LogicalOlapScan(
                StatementScopeIdGenerator.newRelationId(),
                baseTable,
                qualifier,
                ImmutableList.of(),
                baseTable.getPartitionIds(),
                baseTable.getBaseIndexId(),
                PreAggStatus.unset(),
                ImmutableList.of(),
                ImmutableList.of(),
                Optional.empty(),
                ImmutableList.of());
        return BindRelation.checkAndAddDeleteSignFilter(
                scan, ConnectContext.get(), baseTable);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalOlapTableStreamScan(this, context);
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
        LogicalOlapTableStreamScan that = (LogicalOlapTableStreamScan) o;
        return readMode == that.readMode;
    }

    @Override
    public OlapTableStreamWrapper getTable() {
        return (OlapTableStreamWrapper) super.getTable();
    }

    @Override
    public Optional<StreamReadMode> getStreamReadMode() {
        return Optional.of(readMode);
    }

    @Override
    public String toString() {
        return Utils.toSqlStringSkipNull("LogicalOlapTableStreamScan[" + id.asInt() + "]",
                "qualified", qualifiedName(),
                "alias", tableAlias,
                "indexName", getSelectedMaterializedIndexName().orElse("<index_not_selected>"),
                "selectedIndexId", selectedIndexId,
                "preAgg", preAggStatus,
                "operativeCol", operativeSlots,
                "stats", statistics,
                "virtualColumns", virtualColumns,
                "readMode", readMode);
    }

    public StreamReadMode getReadMode() {
        return readMode;
    }

    public boolean isSnapshot() {
        return readMode == StreamReadMode.SNAPSHOT;
    }

    public boolean isReset() {
        return readMode == StreamReadMode.RESET;
    }

    public boolean isIncremental() {
        return readMode == StreamReadMode.INCREMENTAL;
    }

    @Override
    protected boolean producesDuplicateRows() {
        // INCREMENTAL stream scans return multiple row versions for the
        // same key.  SNAPSHOT and RESET modes read a single consistent
        // snapshot and do not produce duplicate key rows.
        return isIncremental();
    }
}
