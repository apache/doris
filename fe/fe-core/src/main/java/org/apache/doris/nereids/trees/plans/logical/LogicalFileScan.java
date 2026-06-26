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
import org.apache.doris.analysis.TableSnapshot;
import org.apache.doris.common.IdGenerator;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.datasource.iceberg.IcebergExternalTable;
import org.apache.doris.datasource.iceberg.IcebergSysExternalTable;
import org.apache.doris.datasource.mvcc.MvccUtil;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.trees.TableSample;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.plans.AbstractPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.algebra.ExternalPartitionSelection;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.thrift.TFileFormatType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Logical file scan for external catalog.
 */
public class LogicalFileScan extends LogicalCatalogRelation implements SupportPruneNestedColumn {
    protected final ExternalPartitionSelection partitionSelection;
    protected final Optional<TableSample> tableSample;
    protected final Optional<TableSnapshot> tableSnapshot;
    protected final Optional<TableScanParams> scanParams;
    protected final Optional<List<Slot>> cachedOutputs;

    /**
     * Constructor for LogicalFileScan.
     */
    public LogicalFileScan(RelationId id, ExternalTable table, List<String> qualifier,
            Collection<Slot> operativeSlots,
            Optional<TableSample> tableSample, Optional<TableSnapshot> tableSnapshot,
            Optional<TableScanParams> scanParams, Optional<List<Slot>> cachedOutputs) {
        this(id, table, qualifier,
                table.initSelectedPartitions(MvccUtil.getSnapshotFromContext(table)),
                operativeSlots, ImmutableList.of(),
                tableSample, tableSnapshot,
                scanParams, Optional.empty(), Optional.empty(), "",
                cachedOutputs);
    }

    /**
     * Constructor for LogicalFileScan.
     */
    protected LogicalFileScan(RelationId id, ExternalTable table, List<String> qualifier,
            ExternalPartitionSelection partitionSelection, Collection<Slot> operativeSlots,
            List<NamedExpression> virtualColumns, Optional<TableSample> tableSample,
            Optional<TableSnapshot> tableSnapshot, Optional<TableScanParams> scanParams,
            Optional<GroupExpression> groupExpression, Optional<LogicalProperties> logicalProperties,
            String tableAlias, Optional<List<Slot>> cachedSlots) {
        super(id, PlanType.LOGICAL_FILE_SCAN, table, qualifier, operativeSlots, virtualColumns,
                groupExpression, logicalProperties, tableAlias);
        this.partitionSelection = partitionSelection;
        this.tableSample = tableSample;
        this.tableSnapshot = tableSnapshot;
        this.scanParams = scanParams;
        this.cachedOutputs = cachedSlots;
    }

    protected LogicalFileScan(RelationId id, ExternalTable table, List<String> qualifier,
            ExternalPartitionSelection partitionSelection, Collection<Slot> operativeSlots,
            List<NamedExpression> virtualColumns, Optional<TableSample> tableSample,
            Optional<TableSnapshot> tableSnapshot, Optional<TableScanParams> scanParams,
            Optional<GroupExpression> groupExpression, Optional<LogicalProperties> logicalProperties,
            Optional<List<Slot>> cachedOutputs) {
        this(id, table, qualifier, partitionSelection, operativeSlots, virtualColumns, tableSample, tableSnapshot,
                scanParams, groupExpression, logicalProperties, "", cachedOutputs);
    }

    public ExternalPartitionSelection getPartitionSelection() {
        return partitionSelection;
    }

    public boolean hasPartitionConstraint() {
        return partitionSelection.hasPartitionConstraint;
    }

    public Optional<TableSample> getTableSample() {
        return tableSample;
    }

    public Optional<TableSnapshot> getTableSnapshot() {
        return tableSnapshot;
    }

    public Optional<TableScanParams> getScanParams() {
        return scanParams;
    }

    @Override
    public ExternalTable getTable() {
        Preconditions.checkArgument(table instanceof ExternalTable,
                "LogicalFileScan's table must be ExternalTable, but table is " + table.getClass().getSimpleName());
        return (ExternalTable) table;
    }

    @Override
    public String toString() {
        return Utils.toSqlStringSkipNull("LogicalFileScan[" + id.asInt() + "]",
                "qualified", qualifiedName(),
                "alias", tableAlias,
                "output", getOutput(),
                "operativeCols", operativeSlots,
                "stats", statistics);
    }

    @Override
    public LogicalFileScan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return AbstractPlan.copyWithSameId(this, () ->
                new LogicalFileScan(relationId, (ExternalTable) table, qualifier,
                partitionSelection, operativeSlots, virtualColumns, tableSample, tableSnapshot,
                scanParams, groupExpression, Optional.of(getLogicalProperties()), tableAlias,
                cachedOutputs));
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        return AbstractPlan.copyWithSameId(this, () ->
                new LogicalFileScan(relationId, (ExternalTable) table, qualifier,
                partitionSelection, operativeSlots, virtualColumns, tableSample, tableSnapshot,
                scanParams, groupExpression, logicalProperties, tableAlias, cachedOutputs));
    }

    public LogicalFileScan withPartitionSelection(ExternalPartitionSelection partitionSelection) {
        return AbstractPlan.copyWithSameId(this, () ->
                new LogicalFileScan(relationId, (ExternalTable) table, qualifier,
                partitionSelection, operativeSlots, virtualColumns, tableSample, tableSnapshot,
                scanParams, Optional.empty(), Optional.of(getLogicalProperties()), tableAlias,
                cachedOutputs));
    }

    @Override
    public LogicalFileScan withRelationId(RelationId relationId) {
        return AbstractPlan.copyWithSameId(this, () ->
                new LogicalFileScan(relationId, (ExternalTable) table, qualifier,
                partitionSelection, operativeSlots, virtualColumns, tableSample, tableSnapshot,
                scanParams, Optional.empty(), Optional.empty(), tableAlias, cachedOutputs));
    }

    public LogicalFileScan withTableAlias(String tableAlias) {
        return AbstractPlan.copyWithSameId(this, () ->
                new LogicalFileScan(relationId, (ExternalTable) table, qualifier,
                partitionSelection, operativeSlots, virtualColumns, tableSample, tableSnapshot,
                scanParams, Optional.empty(), Optional.of(getLogicalProperties()), tableAlias,
                cachedOutputs));
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalFileScan(this, context);
    }

    @Override
    public boolean equals(Object o) {
        return super.equals(o) && Objects.equals(partitionSelection, ((LogicalFileScan) o).partitionSelection);
    }

    @Override
    public List<Slot> computeOutput() {
        if (cachedOutputs.isPresent()) {
            return cachedOutputs.get();
        }

        if (table instanceof IcebergExternalTable) {
            // iceberg v3 need append row lineage columns
            return computeIcebergOutput((IcebergExternalTable) table);
        } else {
            return super.computeOutput();
        }
    }

    private List<Slot> computeIcebergOutput(IcebergExternalTable iceTable) {
        IdGenerator<ExprId> exprIdGenerator = StatementScopeIdGenerator.getExprIdGenerator();
        Builder<Slot> slots = ImmutableList.builder();
        table.getFullSchema()
                .stream()
                .map(col -> SlotReference.fromColumn(exprIdGenerator.getNextId(), table, col, qualified()))
                .forEach(slots::add);
        // add virtual slots
        for (NamedExpression virtualColumn : virtualColumns) {
            slots.add(virtualColumn.toSlot());
        }
        return slots.build();
    }

    @Override
    public List<Slot> computeAsteriskOutput() {
        return super.computeAsteriskOutput();
    }

    @Override
    public boolean supportPruneNestedColumn() {
        ExternalTable table = getTable();
        if (table instanceof IcebergExternalTable || table instanceof IcebergSysExternalTable) {
            return true;
        } else if (table instanceof HMSExternalTable) {
            HMSExternalTable hmsTable = (HMSExternalTable) table;
            if (hmsTable.getDlaType() == HMSExternalTable.DLAType.HUDI) {
                // Don't prune nested column for HUDI table for now, because HUDI table
                // may have some issues when pruning nested column.
                return false;
            }
            try {
                ConnectContext connectContext = ConnectContext.get();
                SessionVariable sessionVariable = connectContext.getSessionVariable();
                TFileFormatType fileFormatType = ((HMSExternalTable) table).getFileFormatType(sessionVariable);
                switch (fileFormatType) {
                    case FORMAT_PARQUET:
                    case FORMAT_ORC:
                        return true;
                    default:
                        return false;
                }
            } catch (Throwable t) {
                // ignore and not prune
            }
        }
        return false;
    }

    @Override
    public LogicalFileScan withOperativeSlots(Collection<Slot> operativeSlots) {
        return AbstractPlan.copyWithSameId(this, () ->
                new LogicalFileScan(relationId, (ExternalTable) table, qualifier,
                partitionSelection, operativeSlots, virtualColumns, tableSample, tableSnapshot,
                scanParams, groupExpression, Optional.of(getLogicalProperties()), tableAlias, cachedOutputs));
    }

    public LogicalFileScan withCachedOutput(List<Slot> cachedOutputs) {
        return AbstractPlan.copyWithSameId(this, () ->
                new LogicalFileScan(relationId, (ExternalTable) table, qualifier,
                partitionSelection, operativeSlots, virtualColumns, tableSample, tableSnapshot,
                scanParams, groupExpression, Optional.empty(), tableAlias, Optional.of(cachedOutputs)));
    }

    @Override
    public List<Slot> getOperativeSlots() {
        return operativeSlots;
    }
}
