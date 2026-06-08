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
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.IdGenerator;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.trees.TableSample;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.trees.plans.AbstractPlan;
import org.apache.doris.nereids.trees.plans.PartitionPrunablePredicate;
import org.apache.doris.nereids.trees.plans.PreAggStatus;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.ScoreRangeInfo;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.tuple.Pair;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Logical OlapTableStreamScan
 */
public class LogicalOlapTableStreamScan extends LogicalOlapScan {
    private final boolean isNormalized;
    private final boolean isIncrementalScan;

    /**
     * LogicalOlapTableStreamScan construct method
     */
    public LogicalOlapTableStreamScan(RelationId id, OlapTable table, List<String> qualifier, List<Long> tabletIds,
                           List<String> hints, Optional<TableSample> tableSample, Collection<Slot> operativeSlots) {
        super(id, table, qualifier, tabletIds, hints, tableSample, operativeSlots);
        this.isNormalized = false;
        this.isIncrementalScan = false;
    }

    /**
     * LogicalOlapTableStreamScan construct method
     */
    public LogicalOlapTableStreamScan(RelationId id, OlapTable table, List<String> qualifier,
                                             List<Long> specifiedPartitions, List<Long> tabletIds, List<String> hints,
                                             Optional<TableSample> tableSample, List<Slot> operativeSlots) {
        super(id, table, qualifier, specifiedPartitions, tabletIds, hints, tableSample, operativeSlots);
        this.isNormalized = false;
        this.isIncrementalScan = false;
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
                                      Boolean isNormalized, boolean isIncrementalScan) {
        super(id, table, qualifier, groupExpression, logicalProperties,
                selectedPartitionIds, partitionPruned, hasPartitionPredicate, selectedTabletIds, selectedIndexId,
                indexSelected, preAggStatus, specifiedPartitions, hints, cacheSlotWithSlotName, cachedOutput,
                tableSample, directMvScan, colToSubPathsMap, specifiedTabletIds, operativeSlots, virtualColumns,
                scoreOrderKeys, scoreLimit, scoreRangeInfo, annOrderKeys, annLimit, tableAlias,
                partitionPrunablePredicates, scanParams);
        this.isNormalized = isNormalized;
        this.isIncrementalScan = isIncrementalScan;
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
                        partitionPrunablePredicates, scanParams, isNormalized, isIncrementalScan));
    }

    @Override
    public List<Slot> computeOutput() {
        if (cachedOutput.isPresent()) {
            return cachedOutput.get();
        }
        List<Column> baseSchema = table.getBaseSchema(false);
        List<SlotReference> slotFromColumn = createSlotsVectorized(baseSchema);

        ImmutableList.Builder<Slot> slots = ImmutableList.builder();
        IdGenerator<ExprId> exprIdGenerator = StatementScopeIdGenerator.getExprIdGenerator();
        for (int i = 0; i < baseSchema.size(); i++) {
            // skip binlog before column
            final int index = i;
            Column col = baseSchema.get(i);
            if (col.getName().startsWith(Column.BINLOG_BEFORE_PREFIX)) {
                continue;
            }
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
        // add stream exclusive virtual columns.
        SlotReference seqColRef = (SlotReference) new Alias(new BigIntLiteral(-1L), Column.STREAM_SEQ_COL)
                .toSlot().withNullable(true);
        slots.add(seqColRef.withColumn(Column.STREAM_SEQ_VIRTUAL_COLUMN));
        SlotReference changeTypeColRef = (SlotReference) new Alias(new VarcharLiteral("APPEND"),
                Column.STREAM_CHANGE_TYPE_COL).toSlot();
        slots.add(changeTypeColRef.withColumn(Column.STREAM_CHANGE_TYPE_VIRTUAL_COLUMN));
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
                        scanParams, isNormalized, isIncrementalScan));
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
                        scanParams, isNormalized, isIncrementalScan));
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
                        scanParams, isNormalized, isIncrementalScan));
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
                        partitionPrunablePredicates, scanParams, isNormalized, isIncrementalScan));
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
                        partitionPrunablePredicates, scanParams, isNormalized, isIncrementalScan));
    }

    /**
     * withNormalized
     */
    public LogicalOlapTableStreamScan withNormalized(boolean isNormalized) {
        return AbstractPlan.copyWithSameId(this, () ->
                new LogicalOlapTableStreamScan(relationId, (Table) table, qualifier,
                        groupExpression, Optional.of(getLogicalProperties()),
                        selectedPartitionIds, partitionPruned, hasPartitionPredicate, selectedTabletIds,
                        selectedIndexId, indexSelected, preAggStatus, manuallySpecifiedPartitions,
                        hints, cacheSlotWithSlotName, cachedOutput, tableSample, directMvScan,
                        colToSubPathsMap, manuallySpecifiedTabletIds, operativeSlots, virtualColumns,
                        scoreOrderKeys, scoreLimit, scoreRangeInfo, annOrderKeys, annLimit, tableAlias,
                        partitionPrunablePredicates, scanParams, isNormalized, isIncrementalScan));
    }

    /**
     * withIncrementalScan
     */
    public LogicalOlapTableStreamScan withIncrementalScan(boolean isIncrementalScan) {
        return AbstractPlan.copyWithSameId(this, () ->
                new LogicalOlapTableStreamScan(relationId, (Table) table, qualifier,
                        groupExpression, Optional.of(getLogicalProperties()),
                        selectedPartitionIds, partitionPruned, hasPartitionPredicate, selectedTabletIds,
                        selectedIndexId, indexSelected, preAggStatus, manuallySpecifiedPartitions,
                        hints, cacheSlotWithSlotName, cachedOutput, tableSample, directMvScan,
                        colToSubPathsMap, manuallySpecifiedTabletIds, operativeSlots, virtualColumns,
                        scoreOrderKeys, scoreLimit, scoreRangeInfo, annOrderKeys, annLimit, tableAlias,
                        partitionPrunablePredicates, scanParams, isNormalized, isIncrementalScan));
    }

    @Override
    public boolean isIncrementalScan() {
        return isIncrementalScan;
    }

    public boolean isNormalized() {
        return isNormalized;
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
                                                               boolean isPartitionPruned) {
        return AbstractPlan.copyWithSameId(this, () ->
                new LogicalOlapTableStreamScan(relationId, (Table) table, qualifier,
                        groupExpression, Optional.of(getLogicalProperties()),
                        selectedPartitionIds, isPartitionPruned, hasPartitionPredicate, selectedTabletIds,
                        selectedIndexId, indexSelected, preAggStatus, manuallySpecifiedPartitions,
                        hints, cacheSlotWithSlotName, cachedOutput, tableSample, directMvScan,
                        colToSubPathsMap, manuallySpecifiedTabletIds, operativeSlots, virtualColumns,
                        scoreOrderKeys, scoreLimit, scoreRangeInfo, annOrderKeys, annLimit, tableAlias,
                        partitionPrunablePredicates, scanParams, isNormalized, isIncrementalScan));
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
                        partitionPrunablePredicates, scanParams, isNormalized, isIncrementalScan));
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
                        Optional.of(scanParams), isNormalized, isIncrementalScan));
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
        return Objects.equals(isNormalized, that.isNormalized)
                && Objects.equals(isIncrementalScan, that.isIncrementalScan);
    }
}
