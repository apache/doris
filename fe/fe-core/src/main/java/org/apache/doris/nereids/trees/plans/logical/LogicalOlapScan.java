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
import org.apache.doris.nereids.trees.plans.AbstractPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.PreAggStatus;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.ScoreRangeInfo;
import org.apache.doris.nereids.trees.plans.algebra.CatalogRelation;
import org.apache.doris.nereids.trees.plans.algebra.OlapPartitionSelection;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Logical OlapScan.
 */
public class LogicalOlapScan extends LogicalCatalogRelation implements OlapScan, SupportPruneNestedColumn {

    private static final Logger LOG = LogManager.getLogger(LogicalOlapScan.class);

    ///////////////////////////////////////////////////////////////////////////
    // Members for materialized index.
    ///////////////////////////////////////////////////////////////////////////

    /**
     * /**
     * The select materialized index id to read data from.
     */
    protected final long selectedIndexId;

    /**
     * Status to indicate materialized index id is selected or not.
     */
    protected final boolean indexSelected;

    /**
     * Status to indicate using pre-aggregation or not.
     */
    protected final PreAggStatus preAggStatus;

    /**
     * When the SlotReference is generated through fromColumn,
     * the exprId will be generated incrementally,
     * causing the slotId of the base to change when the output is recalculated.
     * This structure is responsible for storing the generated SlotReference
     */
    protected final Map<Pair<Long, String>, Slot> cacheSlotWithSlotName;

    /**
     * this is the cache output to overwrite the output, the priority is higher than cacheSlotWithSlotName
     */
    protected final Optional<List<Slot>> cachedOutput;

    ///////////////////////////////////////////////////////////////////////////
    // Members for tablet ids.
    ///////////////////////////////////////////////////////////////////////////

    /**
     * Selected tablet ids to read data from.
     */
    protected final List<Long> selectedTabletIds;

    /**
     * Selected tablet ids to read data from, this would be set if user query with tablets manually
     * Such as select * from  orders TABLET(100);
     */
    protected final List<Long> manuallySpecifiedTabletIds;

    ///////////////////////////////////////////////////////////////////////////
    // Members for partition ids.
    ///////////////////////////////////////////////////////////////////////////
    /**
     * Selected partition state carried by this scan.
     */
    protected final OlapPartitionSelection partitionSelection;

    ///////////////////////////////////////////////////////////////////////////
    // Members for hints.
    ///////////////////////////////////////////////////////////////////////////
    protected final List<String> hints;

    protected final Optional<TableSample> tableSample;

    protected final boolean directMvScan;

    protected final Map<String, Set<List<String>>> colToSubPathsMap;
    protected final Map<Slot, Map<List<String>, SlotReference>> subPathToSlotMap;

    protected final List<OrderKey> scoreOrderKeys;
    protected final Optional<Long> scoreLimit;
    // Score range filter parameters for BM25 range queries like score() > 0.5
    protected final Optional<ScoreRangeInfo> scoreRangeInfo;
    // use for ann push down
    protected final List<OrderKey> annOrderKeys;
    protected final Optional<Long> annLimit;

    protected final Optional<TableScanParams> scanParams;

    public LogicalOlapScan(RelationId id, OlapTable table) {
        this(id, table, ImmutableList.of());
    }

    /**
     * LogicalOlapScan construct method
     */
    public LogicalOlapScan(RelationId id, OlapTable table, List<String> qualifier) {
        this(id, table, qualifier, Optional.empty(), Optional.empty(),
                createPartitionSelection(table, table.getPartitionIds(), false, ImmutableList.of()),
                ImmutableList.of(),
                -1, false, PreAggStatus.unset(), ImmutableList.of(),
                Maps.newHashMap(), Optional.empty(), Optional.empty(), false, ImmutableMap.of(),
                ImmutableList.of(), ImmutableList.of(), ImmutableList.of(),
                ImmutableList.of(), Optional.empty(), Optional.empty(), ImmutableList.of(), Optional.empty(), "");
    }

    /**
     * Constructor for LogicalOlapScan.
     */
    public LogicalOlapScan(RelationId id, OlapTable table, List<String> qualifier, List<Long> tabletIds,
            List<String> hints, Optional<TableSample> tableSample, Collection<Slot> operativeSlots) {
        this(id, table, qualifier, Optional.empty(), Optional.empty(),
                createPartitionSelection(table, table.getPartitionIds(), false, ImmutableList.of()), tabletIds,
                -1, false, PreAggStatus.unset(), hints, Maps.newHashMap(), Optional.empty(),
                tableSample, false, ImmutableMap.of(), ImmutableList.of(), operativeSlots,
                ImmutableList.of(), ImmutableList.of(), Optional.empty(), Optional.empty(),
                ImmutableList.of(), Optional.empty(), "");
    }

    /**
     * constructor.
     */
    public LogicalOlapScan(RelationId id, OlapTable table, List<String> qualifier, List<Long> specifiedPartitions,
            List<Long> tabletIds, List<String> hints, Optional<TableSample> tableSample, List<Slot> operativeSlots) {
        this(id, table, qualifier, Optional.empty(), Optional.empty(),
                // must use specifiedPartitions here for prune partition by sql like 'select * from t partition p1'
                createPartitionSelection(table, specifiedPartitions, false, specifiedPartitions), tabletIds,
                -1, false, PreAggStatus.unset(), hints, Maps.newHashMap(), Optional.empty(),
                tableSample, false, ImmutableMap.of(), ImmutableList.of(), operativeSlots,
                ImmutableList.of(), ImmutableList.of(), Optional.empty(), Optional.empty(),
                ImmutableList.of(), Optional.empty(), "");
    }

    /**
     * constructor.
     */
    public LogicalOlapScan(RelationId id, OlapTable table, List<String> qualifier, List<Long> tabletIds,
            List<Long> selectedPartitionIds, long selectedIndexId, PreAggStatus preAggStatus,
            List<Long> specifiedPartitions, List<String> hints, Optional<TableSample> tableSample,
            Collection<Slot> operativeSlots) {
        this(id, table, qualifier, Optional.empty(), Optional.empty(),
                createPartitionSelection(table, selectedPartitionIds, false, specifiedPartitions), tabletIds,
                selectedIndexId, true, preAggStatus,
                hints, Maps.newHashMap(), Optional.empty(), tableSample, true, ImmutableMap.of(),
                ImmutableList.of(), operativeSlots, ImmutableList.of(), ImmutableList.of(),
                Optional.empty(), Optional.empty(),
                ImmutableList.of(), Optional.empty(), "");
    }

    /**
     * Constructor for LogicalOlapScan.
     */
    public LogicalOlapScan(RelationId id, Table table, List<String> qualifier,
            Optional<GroupExpression> groupExpression, Optional<LogicalProperties> logicalProperties,
            OlapPartitionSelection partitionSelection,
            List<Long> selectedTabletIds, long selectedIndexId, boolean indexSelected,
            PreAggStatus preAggStatus, List<String> hints, Map<Pair<Long, String>, Slot> cacheSlotWithSlotName,
            Optional<List<Slot>> cachedOutput, Optional<TableSample> tableSample, boolean directMvScan,
            Map<String, Set<List<String>>> colToSubPathsMap, List<Long> specifiedTabletIds,
            Collection<Slot> operativeSlots, List<NamedExpression> virtualColumns,
            List<OrderKey> scoreOrderKeys, Optional<Long> scoreLimit, Optional<ScoreRangeInfo> scoreRangeInfo,
            List<OrderKey> annOrderKeys, Optional<Long> annLimit, String tableAlias) {
        this(id, table, qualifier, groupExpression, logicalProperties, partitionSelection,
                selectedTabletIds, selectedIndexId, indexSelected, preAggStatus, hints, cacheSlotWithSlotName,
                cachedOutput, tableSample, directMvScan, colToSubPathsMap, specifiedTabletIds, operativeSlots,
                virtualColumns, scoreOrderKeys, scoreLimit, scoreRangeInfo, annOrderKeys, annLimit, tableAlias,
                Optional.empty());
    }

    /**
     * Constructor for LogicalOlapScan.
     */
    public LogicalOlapScan(RelationId id, Table table, List<String> qualifier,
            Optional<GroupExpression> groupExpression, Optional<LogicalProperties> logicalProperties,
            OlapPartitionSelection partitionSelection,
            List<Long> selectedTabletIds, long selectedIndexId, boolean indexSelected,
            PreAggStatus preAggStatus, List<String> hints, Map<Pair<Long, String>, Slot> cacheSlotWithSlotName,
            Optional<List<Slot>> cachedOutput, Optional<TableSample> tableSample, boolean directMvScan,
            Map<String, Set<List<String>>> colToSubPathsMap, List<Long> specifiedTabletIds,
            Collection<Slot> operativeSlots, List<NamedExpression> virtualColumns,
            List<OrderKey> scoreOrderKeys, Optional<Long> scoreLimit, Optional<ScoreRangeInfo> scoreRangeInfo,
            List<OrderKey> annOrderKeys, Optional<Long> annLimit, String tableAlias,
            Optional<TableScanParams> scanParams) {
        super(id, PlanType.LOGICAL_OLAP_SCAN, table, qualifier,
                operativeSlots, virtualColumns, groupExpression, logicalProperties, tableAlias);
        this.partitionSelection = normalizePartitionSelection(table, partitionSelection);
        this.selectedTabletIds = Utils.fastToImmutableList(selectedTabletIds);
        this.selectedIndexId = selectedIndexId <= 0 ? getTable().getBaseIndexId() : selectedIndexId;
        this.indexSelected = indexSelected;
        this.preAggStatus = preAggStatus;
        this.manuallySpecifiedTabletIds = Utils.fastToImmutableList(specifiedTabletIds);
        this.hints = Objects.requireNonNull(hints, "hints can not be null");
        this.cacheSlotWithSlotName = Objects.requireNonNull(cacheSlotWithSlotName,
                "mvNameToSlot can not be null");
        this.cachedOutput = Objects.requireNonNull(cachedOutput, "cachedOutput can not be null");
        this.tableSample = tableSample;
        this.directMvScan = directMvScan;
        this.colToSubPathsMap = colToSubPathsMap;
        this.subPathToSlotMap = Maps.newHashMap();
        this.scoreOrderKeys = Utils.fastToImmutableList(scoreOrderKeys);
        this.scoreLimit = scoreLimit;
        this.scoreRangeInfo = scoreRangeInfo;
        this.annOrderKeys = Utils.fastToImmutableList(annOrderKeys);
        this.annLimit = annLimit;
        this.scanParams = scanParams;
    }

    /**
     * Constructor for LogicalOlapScan with scanParams.
     */
    public LogicalOlapScan(RelationId id, Table table, List<String> qualifier,
            Optional<GroupExpression> groupExpression, Optional<LogicalProperties> logicalProperties,
            List<Long> selectedPartitionIds, boolean partitionPruned, List<Long> selectedTabletIds,
            long selectedIndexId, boolean indexSelected, PreAggStatus preAggStatus,
            List<Long> specifiedPartitions, List<String> hints,
            Map<Pair<Long, String>, Slot> cacheSlotWithSlotName,
            Optional<TableSample> tableSample, boolean directMvScan,
            Map<String, Set<List<String>>> colToSubPathsMap, List<Long> specifiedTabletIds,
            Optional<TableScanParams> scanParams) {
        this(id, table, qualifier, groupExpression, logicalProperties,
                createPartitionSelection(table, selectedPartitionIds, partitionPruned, specifiedPartitions),
                selectedTabletIds, selectedIndexId, indexSelected, preAggStatus, hints, cacheSlotWithSlotName,
                Optional.empty(), tableSample, directMvScan, colToSubPathsMap, specifiedTabletIds,
                ImmutableList.of(), ImmutableList.of(), ImmutableList.of(), Optional.empty(), Optional.empty(),
                ImmutableList.of(), Optional.empty(), "", scanParams);
    }

    /**
     * Factory method to create a new LogicalOlapScan. Can be overridden by subclasses.
     */
    protected LogicalOlapScan newLogicalOlapScan(RelationId id, Table table, List<String> qualifier,
            Optional<GroupExpression> groupExpression, Optional<LogicalProperties> logicalProperties,
            List<Long> selectedPartitionIds, boolean partitionPruned, List<Long> selectedTabletIds,
            long selectedIndexId, boolean indexSelected, PreAggStatus preAggStatus,
            List<Long> specifiedPartitions, List<String> hints,
            Map<Pair<Long, String>, Slot> cacheSlotWithSlotName,
            Optional<TableSample> tableSample, boolean directMvScan,
            Map<String, Set<List<String>>> colToSubPathsMap, List<Long> specifiedTabletIds,
            Optional<TableScanParams> scanParams) {
        return new LogicalOlapScan(id, table, qualifier, groupExpression, logicalProperties,
                createPartitionSelection(table, selectedPartitionIds, partitionPruned, specifiedPartitions),
                selectedTabletIds, selectedIndexId, indexSelected, preAggStatus, hints, cacheSlotWithSlotName,
                Optional.empty(), tableSample, directMvScan, colToSubPathsMap, specifiedTabletIds,
                ImmutableList.of(), ImmutableList.of(), ImmutableList.of(), Optional.empty(), Optional.empty(),
                ImmutableList.of(), Optional.empty(), "", scanParams);
    }

    private static OlapPartitionSelection createPartitionSelection(Table table, List<Long> selectedPartitionIds,
            boolean partitionPruned, List<Long> manuallySpecifiedPartitions) {
        Preconditions.checkArgument(selectedPartitionIds != null, "selectedPartitionIds can not be null");
        return normalizePartitionSelection(table, new OlapPartitionSelection(selectedPartitionIds, partitionPruned,
                false, manuallySpecifiedPartitions));
    }

    private static OlapPartitionSelection normalizePartitionSelection(Table table,
            OlapPartitionSelection partitionSelection) {
        Objects.requireNonNull(partitionSelection, "partitionSelection can not be null");
        List<Long> selectedPartitionIds = partitionSelection.getSelectedPartitionIds();
        if (selectedPartitionIds.isEmpty()) {
            return partitionSelection;
        }

        Builder<Long> existPartitions = ImmutableList.builderWithExpectedSize(selectedPartitionIds.size());
        for (Long partitionId : selectedPartitionIds) {
            if (((OlapTable) table).getPartition(partitionId) != null) {
                existPartitions.add(partitionId);
            }
        }
        List<Long> filteredPartitionIds = existPartitions.build();
        if (filteredPartitionIds.equals(selectedPartitionIds)) {
            return partitionSelection;
        }
        return partitionSelection.withNarrowedSelectedPartitionIds(filteredPartitionIds);
    }

    @Override
    public OlapPartitionSelection getPartitionSelection() {
        return partitionSelection;
    }

    /**
     * Update selected partitions for this scan.
     */
    public LogicalOlapScan withPartitionSelection(OlapPartitionSelection partitionSelection) {
        return AbstractPlan.copyWithSameId(this, () ->
                new LogicalOlapScan(relationId, (Table) table, qualifier,
                groupExpression, Optional.of(getLogicalProperties()),
                partitionSelection, selectedTabletIds, selectedIndexId, indexSelected, preAggStatus,
                hints, cacheSlotWithSlotName, cachedOutput, tableSample, directMvScan,
                colToSubPathsMap, manuallySpecifiedTabletIds, operativeSlots, virtualColumns,
                scoreOrderKeys, scoreLimit, scoreRangeInfo, annOrderKeys, annLimit, tableAlias, scanParams));
    }

    @Override
    public String getFingerprint() {
        String partitions = "";
        int partitionCount = this.table.getPartitionNames().size();
        List<Long> selectedPartitionIds = partitionSelection.getSelectedPartitionIds();
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
        return Utils.toSqlStringSkipNull("LogicalOlapScan[" + id.asInt() + "]",
                "qualified", qualifiedName(),
                "alias", tableAlias,
                "indexName", getSelectedMaterializedIndexName().orElse("<index_not_selected>"),
                "selectedIndexId", selectedIndexId,
                "preAgg", preAggStatus,
                "operativeCol", operativeSlots,
                "stats", statistics,
                "virtualColumns", virtualColumns);
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
                && Objects.equals(preAggStatus, that.preAggStatus)
                && Objects.equals(partitionSelection, that.partitionSelection)
                && Objects.equals(selectedTabletIds, that.selectedTabletIds)
                && Objects.equals(manuallySpecifiedTabletIds, that.manuallySpecifiedTabletIds)
                && Objects.equals(hints, that.hints)
                && Objects.equals(tableSample, that.tableSample)
                && Objects.equals(scoreOrderKeys, that.scoreOrderKeys)
                && Objects.equals(scoreLimit, that.scoreLimit)
                && Objects.equals(scoreRangeInfo, that.scoreRangeInfo)
                && Objects.equals(annOrderKeys, that.annOrderKeys)
                && Objects.equals(annLimit, that.annLimit)
                && Objects.equals(scanParams, that.scanParams);
    }

    @Override
    public int hashCode() {
        return relationId.asInt();
    }

    @Override
    public LogicalOlapScan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return AbstractPlan.copyWithSameId(this, () ->
                new LogicalOlapScan(relationId, (Table) table, qualifier,
                groupExpression, Optional.of(getLogicalProperties()),
                partitionSelection, selectedTabletIds, selectedIndexId, indexSelected, preAggStatus,
                hints, cacheSlotWithSlotName, cachedOutput, tableSample, directMvScan,
                colToSubPathsMap, manuallySpecifiedTabletIds, operativeSlots, virtualColumns,
                scoreOrderKeys, scoreLimit, scoreRangeInfo, annOrderKeys, annLimit, tableAlias, scanParams));
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        return AbstractPlan.copyWithSameId(this, () ->
                new LogicalOlapScan(relationId, (Table) table, qualifier, groupExpression, logicalProperties,
                partitionSelection, selectedTabletIds, selectedIndexId, indexSelected, preAggStatus,
                hints, cacheSlotWithSlotName, cachedOutput, tableSample, directMvScan, colToSubPathsMap,
                manuallySpecifiedTabletIds, operativeSlots, virtualColumns, scoreOrderKeys, scoreLimit,
                scoreRangeInfo, annOrderKeys, annLimit, tableAlias, scanParams));
    }

    /**
     * Return scan with a new partition prune result.
     */
    public LogicalOlapScan withSelectedPartitionIds(List<Long> selectedPartitionIds) {
        return withSelectedPartitionIds(selectedPartitionIds, false);
    }

    /**
     * Return scan with a new partition prune result.
     */
    public LogicalOlapScan withSelectedPartitionIds(List<Long> selectedPartitionIds,
            boolean hasPartitionConstraint) {
        return withPartitionSelection(partitionSelection.withPartitionPruneResult(
                selectedPartitionIds, hasPartitionConstraint));
    }

    /**
     * with sync materialized index id.
     * @param indexId materialized index id for scan
     * @return scan with materialized index id
     */
    public LogicalOlapScan withMaterializedIndexSelected(long indexId) {
        return AbstractPlan.copyWithSameId(this, () ->
                new LogicalOlapScan(relationId, (Table) table, qualifier,
                Optional.empty(), Optional.of(getLogicalProperties()),
                partitionSelection, selectedTabletIds, indexId, true, PreAggStatus.unset(), hints,
                cacheSlotWithSlotName, cachedOutput, tableSample, directMvScan, colToSubPathsMap,
                manuallySpecifiedTabletIds, operativeSlots, virtualColumns, scoreOrderKeys, scoreLimit, scoreRangeInfo,
                annOrderKeys, annLimit, tableAlias, scanParams));
    }

    /**
     * withSelectedTabletIds
     */
    public LogicalOlapScan withSelectedTabletIds(List<Long> selectedTabletIds) {
        return AbstractPlan.copyWithSameId(this, () ->
                new LogicalOlapScan(relationId, (Table) table, qualifier,
                Optional.empty(), Optional.of(getLogicalProperties()),
                partitionSelection, selectedTabletIds, selectedIndexId, indexSelected, preAggStatus,
                hints, cacheSlotWithSlotName, cachedOutput, tableSample, directMvScan,
                colToSubPathsMap, manuallySpecifiedTabletIds, operativeSlots, virtualColumns, scoreOrderKeys,
                scoreLimit, scoreRangeInfo, annOrderKeys, annLimit, tableAlias, scanParams));
    }

    /**
     * withPreAggStatus
     */
    public LogicalOlapScan withPreAggStatus(PreAggStatus preAggStatus) {
        return AbstractPlan.copyWithSameId(this, () ->
                new LogicalOlapScan(relationId, (Table) table, qualifier,
                Optional.empty(), Optional.of(getLogicalProperties()),
                partitionSelection, selectedTabletIds, selectedIndexId, indexSelected, preAggStatus,
                hints, cacheSlotWithSlotName, cachedOutput, tableSample, directMvScan,
                colToSubPathsMap, manuallySpecifiedTabletIds, operativeSlots, virtualColumns,
                scoreOrderKeys, scoreLimit, scoreRangeInfo, annOrderKeys, annLimit, tableAlias, scanParams));
    }

    /**
     * constructor
     */
    public LogicalOlapScan withColToSubPathsMap(Map<String, Set<List<String>>> colToSubPathsMap) {
        return AbstractPlan.copyWithSameId(this, () ->
                new LogicalOlapScan(relationId, (Table) table, qualifier,
                Optional.empty(), Optional.empty(),
                partitionSelection, selectedTabletIds, selectedIndexId, indexSelected, preAggStatus,
                hints, cacheSlotWithSlotName, cachedOutput, tableSample, directMvScan,
                colToSubPathsMap, manuallySpecifiedTabletIds, operativeSlots, virtualColumns,
                scoreOrderKeys, scoreLimit, scoreRangeInfo, annOrderKeys, annLimit, tableAlias, scanParams));
    }

    /**
     * constructor
     */
    public LogicalOlapScan withManuallySpecifiedTabletIds(List<Long> manuallySpecifiedTabletIds) {
        return AbstractPlan.copyWithSameId(this, () ->
                new LogicalOlapScan(relationId, (Table) table, qualifier,
                Optional.empty(), Optional.of(getLogicalProperties()),
                partitionSelection, selectedTabletIds, selectedIndexId, indexSelected, preAggStatus,
                hints, cacheSlotWithSlotName, cachedOutput, tableSample, directMvScan,
                colToSubPathsMap, manuallySpecifiedTabletIds, operativeSlots, virtualColumns,
                scoreOrderKeys, scoreLimit, scoreRangeInfo, annOrderKeys, annLimit, tableAlias, scanParams));
    }

    @Override
    public LogicalOlapScan withRelationId(RelationId relationId) {
        // we have to set partitionPruned to false, so that mtmv rewrite can prevent deadlock when rewriting union
        return AbstractPlan.copyWithSameId(this, () ->
                new LogicalOlapScan(relationId, (Table) table, qualifier,
                Optional.empty(), Optional.empty(),
                partitionSelection.resetPartitionPruneState(), selectedTabletIds,
                selectedIndexId, indexSelected, preAggStatus,
                hints, Maps.newHashMap(), Optional.empty(), tableSample, directMvScan,
                colToSubPathsMap, manuallySpecifiedTabletIds, operativeSlots, virtualColumns, scoreOrderKeys,
                scoreLimit, scoreRangeInfo, annOrderKeys, annLimit, tableAlias, scanParams));
    }

    @Override
    public LogicalOlapScan withTableAlias(String tableAlias) {
        return AbstractPlan.copyWithSameId(this, () ->
                new LogicalOlapScan(relationId, (Table) table, qualifier,
                Optional.empty(), Optional.of(getLogicalProperties()),
                partitionSelection, selectedTabletIds, selectedIndexId, indexSelected, preAggStatus,
                hints, cacheSlotWithSlotName, cachedOutput, tableSample, directMvScan,
                colToSubPathsMap, manuallySpecifiedTabletIds, operativeSlots, virtualColumns,
                scoreOrderKeys, scoreLimit, scoreRangeInfo, annOrderKeys, annLimit, tableAlias, scanParams));
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
        LogicalProperties finalLogicalProperties = new LogicalProperties(() -> output, this::computeDataTrait);
        return AbstractPlan.copyWithSameId(this, () ->
                new LogicalOlapScan(relationId, (Table) table, qualifier,
                groupExpression, Optional.of(finalLogicalProperties),
                partitionSelection, selectedTabletIds, selectedIndexId, indexSelected, preAggStatus,
                hints, cacheSlotWithSlotName, cachedOutput, tableSample, directMvScan, colToSubPathsMap,
                manuallySpecifiedTabletIds, operativeSlots, virtualColumns, scoreOrderKeys, scoreLimit,
                scoreRangeInfo, annOrderKeys, annLimit, tableAlias, scanParams));
    }

    /**
     * Append additional virtual columns to existing ones.
     * Unlike {@link #withVirtualColumns} which replaces, this merges existing + new.
     */
    public LogicalOlapScan appendVirtualColumns(List<NamedExpression> additionalVirtualColumns) {
        LogicalProperties logicalProperties = getLogicalProperties();
        List<Slot> output = Lists.newArrayList(logicalProperties.getOutput());
        output.addAll(additionalVirtualColumns.stream().map(NamedExpression::toSlot)
                .collect(Collectors.toList()));
        logicalProperties = new LogicalProperties(() -> output, this::computeDataTrait);
        List<NamedExpression> mergedVirtualColumns = ImmutableList.<NamedExpression>builder()
                .addAll(this.virtualColumns)
                .addAll(additionalVirtualColumns)
                .build();
        return new LogicalOlapScan(relationId, (Table) table, qualifier,
                groupExpression, Optional.of(logicalProperties),
                partitionSelection, selectedTabletIds, selectedIndexId, indexSelected, preAggStatus,
                hints, cacheSlotWithSlotName, cachedOutput, tableSample, directMvScan, colToSubPathsMap,
                manuallySpecifiedTabletIds, operativeSlots, mergedVirtualColumns, scoreOrderKeys, scoreLimit,
                scoreRangeInfo, annOrderKeys, annLimit, tableAlias, scanParams);
    }

    /**
     * Append additional virtual columns with topN info.
     * Merges existing virtual columns with the new ones.
     */
    public LogicalOlapScan appendVirtualColumnsAndTopN(
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
        return new LogicalOlapScan(relationId, (Table) table, qualifier,
                groupExpression, Optional.of(logicalProperties),
                partitionSelection, selectedTabletIds, selectedIndexId, indexSelected, preAggStatus,
                hints, cacheSlotWithSlotName, cachedOutput, tableSample, directMvScan, colToSubPathsMap,
                manuallySpecifiedTabletIds, operativeSlots, mergedVirtualColumns, scoreOrderKeys, scoreLimit,
                scoreRangeInfo, annOrderKeys, annLimit, tableAlias, scanParams);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalOlapScan(this, context);
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
        if (cachedOutput.isPresent()) {
            return cachedOutput.get();
        }
        if (selectedIndexId != ((OlapTable) table).getBaseIndexId()) {
            return getOutputByIndex(selectedIndexId);
        }
        List<Column> baseSchema = table.getBaseSchema(true);
        boolean skipBinlogBeforeColumn = scanParams.isPresent() && scanParams.get().incrementalRead();
        List<SlotReference> slotFromColumn = createSlotsVectorized(baseSchema, skipBinlogBeforeColumn);

        Builder<Slot> slots = ImmutableList.builder();
        IdGenerator<ExprId> exprIdGenerator = StatementScopeIdGenerator.getExprIdGenerator();
        for (int i = 0; i < baseSchema.size(); i++) {
            final int index = i;
            Column col = baseSchema.get(i);
            if (skipBinlogBeforeColumn && col.getName().startsWith(Column.BINLOG_BEFORE_PREFIX)) {
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
        // when we have a partitioned table without any partition, visible index is
        // empty
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

    protected List<Slot> generateUniqueSlot(OlapTable table, Column column, boolean isBaseIndex, long indexId,
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

    public Optional<ScoreRangeInfo> getScoreRangeInfo() {
        return scoreRangeInfo;
    }

    private List<SlotReference> createSlotsVectorized(List<Column> columns, boolean skipBinlogBeforeColumn) {
        List<String> qualified = qualified();
        SlotReference[] slots = new SlotReference[columns.size()];
        IdGenerator<ExprId> exprIdGenerator = StatementScopeIdGenerator.getExprIdGenerator();
        for (int i = 0; i < columns.size(); i++) {
            if (skipBinlogBeforeColumn && columns.get(i).getName().startsWith(Column.BINLOG_BEFORE_PREFIX)) {
                continue;
            }
            ExprId nextId = exprIdGenerator.getNextId();
            slots[i] = SlotReference.fromColumn(nextId, table, columns.get(i), qualified);
        }
        return Arrays.asList(slots);
    }

    protected List<SlotReference> createSlotsVectorized(List<Column> columns) {
        return createSlotsVectorized(columns, false);
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
            // When readMorAsDup is enabled, MOR tables are read as DUP, so uniqueness cannot be guaranteed.
            if (getTable().getKeysType() == KeysType.UNIQUE_KEYS
                    && getTable().isMorTable()
                    && ConnectContext.get().getSessionVariable().isReadMorAsDupEnabled(
                        getTable().getQualifiedDbName(), getTable().getName())) {
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
        return AbstractPlan.copyWithSameId(this, () ->
                new LogicalOlapScan(relationId, (Table) table, qualifier,
                groupExpression, Optional.of(getLogicalProperties()),
                partitionSelection, selectedTabletIds, selectedIndexId, indexSelected, preAggStatus,
                hints, cacheSlotWithSlotName, cachedOutput, tableSample, directMvScan, colToSubPathsMap,
                manuallySpecifiedTabletIds, operativeSlots, virtualColumns, scoreOrderKeys, scoreLimit,
                scoreRangeInfo, annOrderKeys, annLimit, tableAlias, scanParams));
    }

    @VisibleForTesting
    Map<Slot, Slot> constructReplaceMap(MTMV mtmv) {
        Map<Slot, Slot> replaceMap = new HashMap<>();
        MTMVCache cache;
        try {
            cache = mtmv.getOrGenerateCache(ConnectContext.get());
        } catch (Exception e) {
            LOG.warn(String.format("LogicalOlapScan constructReplaceMap fail, mv name is %s", mtmv.getName()), e);
            return replaceMap;
        }
        // Get MV plan's visible output slots (ordered, matching MV definition SELECT list).
        // This includes all visible slots: regular columns AND variant subPath columns
        // like payload['issue'] that are real physical columns of the MV.
        List<Slot> mvPlanVisibleOutputs = new ArrayList<>();
        for (Slot slot : cache.getOriginalFinalPlan().getOutput()) {
            if (slot instanceof SlotReference && ((SlotReference) slot).isVisible()) {
                mvPlanVisibleOutputs.add(slot);
            }
        }
        // Get MV table's visible physical columns (ordered).
        // getBaseSchema() returns visible-only columns whose names are derived from the
        // CREATE MV AS SELECT aliases. These names are guaranteed unique per table.
        // Using physical column names as keys (instead of plan slot's originalColumn.getName())
        // correctly handles:
        // - Aliased columns (e.g. SELECT sum_total AS agg3): key is "agg3", not "sum_total"
        // - Self-join MVs: physical column names are unique even if source columns collide
        // - Variant columns (e.g. SELECT payload['issue']): they are physical columns in getBaseSchema()
        List<Column> mvPhysicalColumns = mtmv.getBaseSchema();
        if (mvPlanVisibleOutputs.size() != mvPhysicalColumns.size()) {
            LOG.error("LogicalOlapScan constructReplaceMap: MV plan visible output size {} "
                    + "doesn't match physical column size {} for mv {}",
                    mvPlanVisibleOutputs.size(), mvPhysicalColumns.size(), mtmv.getName());
            // not throw exception here to avoid query failed, compute mv fd should not influence query process
            return Collections.emptyMap();
        }
        // Build mvOutputsMap: the i-th visible plan output corresponds to the i-th physical column.
        Map<List<String>, Slot> mvOutputsMap = new HashMap<>();
        for (int i = 0; i < mvPlanVisibleOutputs.size(); i++) {
            String physicalColName = mvPhysicalColumns.get(i).getName().toLowerCase(Locale.ROOT);
            List<String> key = Lists.newArrayList(physicalColName);
            mvOutputsMap.put(key, mvPlanVisibleOutputs.get(i));
        }
        // Match scan output slots against mvOutputsMap.
        // Scan slot's originalColumn.getName() refers to the MV's physical column, so keys match.
        // Extra subPath slots added by VariantSubPathPruning during query optimization won't
        // match any mvOutputsMap entry (their keys include subPath elements) and are safely skipped.
        for (Slot scanSlot : getOutput()) {
            if (scanSlot instanceof SlotReference && ((SlotReference) scanSlot).isVisible()) {
                SlotReference scanRef = (SlotReference) scanSlot;
                String scanName = scanRef.getOriginalColumn().map(Column::getName).orElse(scanRef.getName());
                List<String> key = Lists.newArrayList(scanName.toLowerCase(Locale.ROOT));
                key.addAll(scanRef.getSubPath());
                Slot mvMappingSlot = mvOutputsMap.get(key);
                if (mvMappingSlot != null) {
                    replaceMap.put(mvMappingSlot, scanSlot);
                }
            }
        }
        // Every MV plan slot must be mapped
        if (mvOutputsMap.size() != replaceMap.size()) {
            LOG.error(String.format("LogicalOlapScan constructReplaceMap size not match,"
                    + "mv name is %s, mvOutputsMap is %s, mv output is %s, scan output is %s",
                    mtmv.getName(), mvOutputsMap,
                    cache.getOriginalFinalPlan().getOutput(), getOutput()));
            // not throw exception here to avoid query failed, compute mv fd should not influence query process
            return Collections.emptyMap();
        }
        return replaceMap;
    }

    /** withCachedOutput */
    public LogicalOlapScan withCachedOutput(List<Slot> outputSlots) {
        return AbstractPlan.copyWithSameId(this, () ->
                new LogicalOlapScan(relationId, (Table) table, qualifier,
                groupExpression, Optional.empty(),
                partitionSelection, selectedTabletIds, selectedIndexId, indexSelected, preAggStatus,
                hints, cacheSlotWithSlotName, Optional.of(outputSlots), tableSample, directMvScan, colToSubPathsMap,
                manuallySpecifiedTabletIds, operativeSlots, virtualColumns, scoreOrderKeys, scoreLimit,
                scoreRangeInfo, annOrderKeys, annLimit, tableAlias, scanParams));
    }

    /** withTableScanParams */
    public LogicalOlapScan withTableScanParams(TableScanParams scanParams) {
        return AbstractPlan.copyWithSameId(this, () ->
                new LogicalOlapScan(relationId, (Table) table, qualifier,
                        groupExpression, Optional.empty(),
                        partitionSelection, selectedTabletIds, selectedIndexId, indexSelected, preAggStatus,
                        hints, cacheSlotWithSlotName, cachedOutput, tableSample, directMvScan, colToSubPathsMap,
                        manuallySpecifiedTabletIds, operativeSlots, virtualColumns, scoreOrderKeys, scoreLimit,
                        scoreRangeInfo, annOrderKeys, annLimit, tableAlias, Optional.of(scanParams)));
    }

    @Override
    public boolean supportPruneNestedColumn() {
        return true;
    }

    public boolean isIncrementalScan() {
        return false;
    }

    public Optional<TableScanParams> getScanParams() {
        return scanParams;
    }
}
