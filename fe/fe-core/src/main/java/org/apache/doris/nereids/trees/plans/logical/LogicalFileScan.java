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
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.common.IdGenerator;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.datasource.iceberg.IcebergExternalTable;
import org.apache.doris.datasource.iceberg.IcebergSysExternalTable;
import org.apache.doris.datasource.mvcc.MvccSnapshot;
import org.apache.doris.datasource.mvcc.MvccTable;
import org.apache.doris.datasource.mvcc.MvccUtil;
import org.apache.doris.datasource.paimon.PaimonExternalTable;
import org.apache.doris.datasource.paimon.PaimonSysExternalTable;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.trees.TableSample;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.plans.AnnTopNInfo;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.thrift.TFileFormatType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.ImmutableMap;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Logical file scan for external catalog.
 */
public class LogicalFileScan extends LogicalCatalogRelation implements SupportPruneNestedColumn {
    protected final SelectedPartitions selectedPartitions;
    protected final Optional<TableSample> tableSample;
    protected final Optional<TableSnapshot> tableSnapshot;
    protected final Optional<TableScanParams> scanParams;
    protected final Optional<List<Slot>> cachedOutputs;
    protected final Optional<List<Column>> relationSchema;
    protected final Optional<MvccSnapshot> relationSnapshot;
    // Used for primary-key vector (ANN) top-N push down into the Paimon scan.
    // Carries the query vector, the indexed vector column name, and the retrieval
    // limit (already user_limit + user_offset) down to PaimonScanNode.
    protected final Optional<AnnTopNInfo> annTopN;

    /**
     * Constructor for LogicalFileScan.
     */
    public LogicalFileScan(RelationId id, ExternalTable table, List<String> qualifier,
            Collection<Slot> operativeSlots,
            Optional<TableSample> tableSample, Optional<TableSnapshot> tableSnapshot,
            Optional<TableScanParams> scanParams, Optional<List<Slot>> cachedOutputs) {
        this(id, table, qualifier, operativeSlots, tableSample, tableSnapshot, scanParams, cachedOutputs,
                MvccUtil.getSnapshotFromContext(table));
    }

    /**
     * Constructor for a relation whose concrete snapshot was resolved during binding.
     */
    public LogicalFileScan(RelationId id, ExternalTable table, List<String> qualifier,
            Collection<Slot> operativeSlots,
            Optional<TableSample> tableSample, Optional<TableSnapshot> tableSnapshot,
            Optional<TableScanParams> scanParams, Optional<List<Slot>> cachedOutputs,
            Optional<MvccSnapshot> relationSnapshot) {
        this(id, table, qualifier,
                initialSelectedPartitions(table, scanParams, relationSnapshot),
                operativeSlots, ImmutableList.of(),
                tableSample, tableSnapshot,
                scanParams, Optional.empty(), Optional.empty(),
                cachedOutputs, captureRelationSchema(table, scanParams, relationSnapshot), relationSnapshot);
    }

    /**
     * Constructor for LogicalFileScan.
     */
    protected LogicalFileScan(RelationId id, ExternalTable table, List<String> qualifier,
            SelectedPartitions selectedPartitions, Collection<Slot> operativeSlots,
            List<NamedExpression> virtualColumns, Optional<TableSample> tableSample,
            Optional<TableSnapshot> tableSnapshot, Optional<TableScanParams> scanParams,
            Optional<GroupExpression> groupExpression, Optional<LogicalProperties> logicalProperties,
            Optional<List<Slot>> cachedSlots) {
        this(id, table, qualifier, selectedPartitions, operativeSlots, virtualColumns, tableSample, tableSnapshot,
                scanParams, groupExpression, logicalProperties, cachedSlots, Optional.empty());
    }

    /**
     * Constructor for LogicalFileScan.
     */
    protected LogicalFileScan(RelationId id, ExternalTable table, List<String> qualifier,
            SelectedPartitions selectedPartitions, Collection<Slot> operativeSlots,
            List<NamedExpression> virtualColumns, Optional<TableSample> tableSample,
            Optional<TableSnapshot> tableSnapshot, Optional<TableScanParams> scanParams,
            Optional<GroupExpression> groupExpression, Optional<LogicalProperties> logicalProperties,
            Optional<List<Slot>> cachedSlots, Optional<List<Column>> relationSchema) {
        this(id, table, qualifier, selectedPartitions, operativeSlots, virtualColumns, tableSample, tableSnapshot,
                scanParams, groupExpression, logicalProperties, cachedSlots, relationSchema,
                MvccUtil.getSnapshotFromContext(table), Optional.empty());
    }

    protected LogicalFileScan(RelationId id, ExternalTable table, List<String> qualifier,
            SelectedPartitions selectedPartitions, Collection<Slot> operativeSlots,
            List<NamedExpression> virtualColumns, Optional<TableSample> tableSample,
            Optional<TableSnapshot> tableSnapshot, Optional<TableScanParams> scanParams,
            Optional<GroupExpression> groupExpression, Optional<LogicalProperties> logicalProperties,
            Optional<List<Slot>> cachedSlots, Optional<List<Column>> relationSchema,
            Optional<MvccSnapshot> relationSnapshot) {
        this(id, table, qualifier, selectedPartitions, operativeSlots, virtualColumns, tableSample, tableSnapshot,
                scanParams, groupExpression, logicalProperties, cachedSlots, relationSchema,
                relationSnapshot, Optional.empty());
    }

    protected LogicalFileScan(RelationId id, ExternalTable table, List<String> qualifier,
            SelectedPartitions selectedPartitions, Collection<Slot> operativeSlots,
            List<NamedExpression> virtualColumns, Optional<TableSample> tableSample,
            Optional<TableSnapshot> tableSnapshot, Optional<TableScanParams> scanParams,
            Optional<GroupExpression> groupExpression, Optional<LogicalProperties> logicalProperties,
            Optional<List<Slot>> cachedSlots, Optional<List<Column>> relationSchema,
            Optional<MvccSnapshot> relationSnapshot, Optional<AnnTopNInfo> annTopN) {
        super(id, PlanType.LOGICAL_FILE_SCAN, table, qualifier, operativeSlots, virtualColumns,
                groupExpression, logicalProperties);
        this.selectedPartitions = selectedPartitions;
        this.tableSample = tableSample;
        this.tableSnapshot = tableSnapshot;
        this.scanParams = scanParams;
        this.cachedOutputs = cachedSlots;
        this.relationSchema = relationSchema;
        this.relationSnapshot = relationSnapshot;
        this.annTopN = annTopN;
    }

    private static SelectedPartitions initialSelectedPartitions(
            ExternalTable table, Optional<TableScanParams> scanParams,
            Optional<MvccSnapshot> relationSnapshot) {
        if ((table instanceof PaimonExternalTable || table instanceof PaimonSysExternalTable)
                && scanParams.isPresent() && scanParams.get().isOptions()) {
            // A relation-scoped historical snapshot cannot reuse partitions cached for the
            // statement-level latest snapshot; Paimon will prune its selected snapshot instead.
            return SelectedPartitions.NOT_PRUNED;
        }
        return table.initSelectedPartitions(relationSnapshot);
    }

    private static Optional<List<Column>> captureRelationSchema(
            ExternalTable table, Optional<TableScanParams> scanParams,
            Optional<MvccSnapshot> relationSnapshot) {
        if (scanParams.isPresent() && scanParams.get().isOptions()) {
            if (table instanceof PaimonExternalTable) {
                return Optional.of(ImmutableList.copyOf(
                        ((PaimonExternalTable) table).getFullSchema(scanParams.get())));
            }
            if (table instanceof PaimonSysExternalTable) {
                return Optional.of(ImmutableList.copyOf(
                        ((PaimonSysExternalTable) table).getFullSchema(
                                scanParams.get(), relationSnapshot)));
            }
        }
        return captureRelationSchema(table, relationSnapshot);
    }

    protected static Optional<List<Column>> captureRelationSchema(
            ExternalTable table, Optional<MvccSnapshot> relationSnapshot) {
        if (!(table instanceof MvccTable)) {
            return Optional.empty();
        }
        // Pin columns while this relation's snapshot is current, but create slots lazily to
        // preserve statement-wide ExprId allocation order used by materialized-view rewrites.
        return Optional.of(ImmutableList.copyOf(table.getFullSchema(relationSnapshot)));
    }

    public SelectedPartitions getSelectedPartitions() {
        return selectedPartitions;
    }

    public boolean hasPartitionPredicate() {
        return selectedPartitions.hasPartitionPredicate;
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

    public Optional<MvccSnapshot> getRelationSnapshot() {
        return relationSnapshot;
    }

    @Override
    public ExternalTable getTable() {
        Preconditions.checkArgument(table instanceof ExternalTable,
                "LogicalFileScan's table must be ExternalTable, but table is " + table.getClass().getSimpleName());
        return (ExternalTable) table;
    }

    @Override
    public String toString() {
        return Utils.toSqlStringSkipNull("LogicalFileScan",
                "qualified", qualifiedName(),
                "output", getOutput(),
                "operativeCols", operativeSlots,
                "stats", statistics
        );
    }

    @Override
    public LogicalFileScan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new LogicalFileScan(relationId, (ExternalTable) table, qualifier,
                selectedPartitions, operativeSlots, virtualColumns, tableSample, tableSnapshot,
                scanParams, groupExpression, Optional.of(getLogicalProperties()),
                cachedOutputs, relationSchema, relationSnapshot, annTopN);
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        return new LogicalFileScan(relationId, (ExternalTable) table, qualifier,
                selectedPartitions, operativeSlots, virtualColumns, tableSample, tableSnapshot,
                scanParams, groupExpression, logicalProperties, cachedOutputs,
                relationSchema, relationSnapshot, annTopN);
    }

    public LogicalFileScan withSelectedPartitions(SelectedPartitions selectedPartitions) {
        return new LogicalFileScan(relationId, (ExternalTable) table, qualifier,
                selectedPartitions, operativeSlots, virtualColumns, tableSample, tableSnapshot,
                scanParams, Optional.empty(), Optional.of(getLogicalProperties()),
                cachedOutputs, relationSchema, relationSnapshot, annTopN);
    }

    @Override
    public LogicalFileScan withRelationId(RelationId relationId) {
        return new LogicalFileScan(relationId, (ExternalTable) table, qualifier,
                selectedPartitions, operativeSlots, virtualColumns, tableSample, tableSnapshot,
                scanParams, Optional.empty(), Optional.empty(), cachedOutputs,
                relationSchema, relationSnapshot, annTopN);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalFileScan(this, context);
    }

    @Override
    public boolean equals(Object o) {
        return super.equals(o) && Objects.equals(selectedPartitions, ((LogicalFileScan) o).selectedPartitions)
                && Objects.equals(annTopN, ((LogicalFileScan) o).annTopN);
    }

    @Override
    protected boolean hasSameScanState(LogicalCatalogRelation other) {
        if (!Utils.isSameClass(this, other)) {
            return false;
        }
        LogicalFileScan that = (LogicalFileScan) other;
        return Objects.equals(selectedPartitions, that.selectedPartitions)
                && Objects.equals(tableSample, that.tableSample)
                && hasSameSnapshot(tableSnapshot, that.tableSnapshot)
                && hasSameScanParams(scanParams, that.scanParams)
                && hasSameResolvedSnapshot(relationSnapshot, that.relationSnapshot)
                && Objects.equals(annTopN, that.annTopN);
    }

    private static boolean hasSameResolvedSnapshot(
            Optional<MvccSnapshot> left, Optional<MvccSnapshot> right) {
        return left.isPresent() == right.isPresent()
                && (!left.isPresent() || left.get().isSameSnapshot(right.get()));
    }

    @Override
    public List<Slot> computeOutput() {
        if (cachedOutputs.isPresent()) {
            return cachedOutputs.get();
        }

        if (relationSchema.isPresent()) {
            return computeOutput(relationSchema.get());
        }

        if (table instanceof IcebergExternalTable) {
            // iceberg v3 need append row lineage columns
            return computeIcebergOutput();
        } else if (scanParams.isPresent() && scanParams.get().isOptions()
                && (table instanceof PaimonExternalTable || table instanceof PaimonSysExternalTable)) {
            List<Column> schema = table instanceof PaimonSysExternalTable
                    ? ((PaimonSysExternalTable) table).getFullSchema(scanParams.get())
                    : ((PaimonExternalTable) table).getFullSchema(scanParams.get());
            return computeOutput(schema);
        } else {
            return super.computeOutput();
        }
    }

    private List<Slot> computeOutput(List<Column> schema) {
        IdGenerator<ExprId> exprIdGenerator = StatementScopeIdGenerator.getExprIdGenerator();
        Builder<Slot> slots = ImmutableList.builder();
        schema.stream()
                .map(col -> SlotReference.fromColumn(exprIdGenerator.getNextId(), table, col, qualified()))
                .forEach(slots::add);
        // add virtual slots
        for (NamedExpression virtualColumn : virtualColumns) {
            slots.add(virtualColumn.toSlot());
        }
        return slots.build();
    }

    private List<Slot> computeIcebergOutput() {
        return computeOutput(table.getFullSchema());
    }

    @Override
    public List<Slot> computeAsteriskOutput() {
        return super.computeAsteriskOutput();
    }

    @Override
    public boolean supportPruneNestedColumn() {
        ExternalTable table = getTable();
        if (table instanceof IcebergExternalTable || table instanceof IcebergSysExternalTable
                || table instanceof PaimonExternalTable || table instanceof PaimonSysExternalTable) {
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

    private boolean hasSameSnapshot(Optional<TableSnapshot> left, Optional<TableSnapshot> right) {
        if (!left.isPresent() || !right.isPresent()) {
            return left.isPresent() == right.isPresent();
        }
        return left.get().getType() == right.get().getType()
                && Objects.equals(left.get().getValue(), right.get().getValue());
    }

    private boolean hasSameScanParams(Optional<TableScanParams> left, Optional<TableScanParams> right) {
        if (!left.isPresent() || !right.isPresent()) {
            return left.isPresent() == right.isPresent();
        }
        return Objects.equals(left.get().getParamType(), right.get().getParamType())
                && Objects.equals(left.get().getMapParams(), right.get().getMapParams())
                && Objects.equals(left.get().getListParams(), right.get().getListParams());
    }

    /**
     * SelectedPartitions contains the selected partitions and the total partition number.
     * Mainly for hive table partition pruning.
     */
    public static class SelectedPartitions {
        // NOT_PRUNED means the Nereids planner does not handle the partition pruning.
        // This can be treated as the initial value of SelectedPartitions.
        // Or used to indicate that the partition pruning is not processed.
        public static SelectedPartitions NOT_PRUNED = new SelectedPartitions(0, ImmutableMap.of(), false, false);
        /**
         * total partition number
         */
        public final long totalPartitionNum;
        /**
         * partition name -> partition item
         */
        public final Map<String, PartitionItem> selectedPartitions;
        /**
         * true means the result is after partition pruning
         * false means the partition pruning is not processed.
         */
        public final boolean isPruned;

        /**
         * true means the pruning logic found a usable partition predicate.
         */
        public final boolean hasPartitionPredicate;

        /**
         * Constructor for SelectedPartitions.
         */
        public SelectedPartitions(long totalPartitionNum, Map<String, PartitionItem> selectedPartitions,
                boolean isPruned) {
            this(totalPartitionNum, selectedPartitions, isPruned, false);
        }

        /**
         * Constructor for SelectedPartitions.
         */
        public SelectedPartitions(long totalPartitionNum, Map<String, PartitionItem> selectedPartitions,
                boolean isPruned, boolean hasPartitionPredicate) {
            this.totalPartitionNum = totalPartitionNum;
            this.selectedPartitions = ImmutableMap.copyOf(Objects.requireNonNull(selectedPartitions,
                    "selectedPartitions is null"));
            this.isPruned = isPruned;
            this.hasPartitionPredicate = hasPartitionPredicate;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            SelectedPartitions that = (SelectedPartitions) o;
            return isPruned == that.isPruned
                    && hasPartitionPredicate == that.hasPartitionPredicate
                    && Objects.equals(
                    selectedPartitions.keySet(), that.selectedPartitions.keySet());
        }

        @Override
        public int hashCode() {
            return Objects.hash(selectedPartitions, isPruned, hasPartitionPredicate);
        }
    }

    @Override
    public LogicalFileScan withOperativeSlots(Collection<Slot> operativeSlots) {
        return new LogicalFileScan(relationId, (ExternalTable) table, qualifier,
                selectedPartitions, operativeSlots, virtualColumns, tableSample, tableSnapshot,
                scanParams, groupExpression, Optional.of(getLogicalProperties()),
                cachedOutputs, relationSchema, relationSnapshot, annTopN);
    }

    public LogicalFileScan withCachedOutput(List<Slot> cachedOutputs) {
        return new LogicalFileScan(relationId, (ExternalTable) table, qualifier,
                selectedPartitions, operativeSlots, virtualColumns, tableSample, tableSnapshot,
                scanParams, groupExpression, Optional.empty(), Optional.of(cachedOutputs),
                relationSchema, relationSnapshot, annTopN);
    }

    @Override
    public List<Slot> getOperativeSlots() {
        return operativeSlots;
    }

    public Optional<AnnTopNInfo> getAnnTopN() {
        return annTopN;
    }

    /**
     * Push primary-key vector (ANN) top-N info into the Paimon scan. Appends a
     * reader-produced {@code __paimon_search_score_to_dis} virtual column to the scan
     * output and records the query vector / indexed column / retrieval limit.
     *
     * <p>The new output is derived from the OLD output plus the distance slot, rather
     * than by letting {@code computeLogicalProperties()} recompute it. That is not an
     * optimization -- it is required for correctness. Passing {@code Optional.empty()}
     * for logicalProperties routes the output through
     * {@link LogicalCatalogRelation#computeOutput()}, which mints a FRESH ExprId for
     * every base column ({@code exprIdGenerator.getNextId()}, no per-column reuse
     * cache). The caller (PushDownVectorTopNIntoPaimonScan) reparents the EXISTING
     * projections onto this new scan, so those projections would then reference slots
     * the scan no longer produces -- e.g. project reads {@code id#10005} while the scan
     * outputs {@code id#10010}. Verified by the exprId-stability assertion in
     * PushDownVectorTopNIntoPaimonScanTest.
     *
     * <p>Same shape as {@code LogicalOlapScan.appendVirtualColumnsAndTopN}, for the same
     * reason. Because the output is supplied explicitly, {@code cachedOutputs} can be
     * forwarded too: it only feeds {@code computeOutput()}, which is now unreachable,
     * and dropping it would lose an upstream rewrite.
     *
     * @param scoreVirtualColumn the {@code __paimon_search_score_to_dis} output column
     * @param annTopN the query vector, indexed column name, and retrieval limit
     */
    public LogicalFileScan withVectorTopN(
            NamedExpression scoreVirtualColumn,
            AnnTopNInfo annTopN) {
        List<NamedExpression> mergedVirtualColumns = ImmutableList.<NamedExpression>builder()
                .addAll(virtualColumns)
                .add(scoreVirtualColumn)
                .build();
        LogicalProperties oldProperties = getLogicalProperties();
        List<Slot> newOutput = ImmutableList.<Slot>builder()
                .addAll(oldProperties.getOutput())
                .add(scoreVirtualColumn.toSlot())
                .build();
        // The asterisk output is deliberately NOT extended: `select *` must not surface
        // an internal distance column. Forwarded as a supplier rather than a value so
        // that this rewrite does not force a computation it never reads -- the asterisk
        // output only matters during binding, long before this rule runs.
        LogicalProperties newProperties = new LogicalProperties(
                () -> newOutput, oldProperties::getAsteriskOutput, this::computeDataTrait);
        return new LogicalFileScan(relationId, (ExternalTable) table, qualifier,
                selectedPartitions, operativeSlots, mergedVirtualColumns, tableSample, tableSnapshot,
                scanParams, groupExpression, Optional.of(newProperties), cachedOutputs,
                relationSchema, relationSnapshot, Optional.of(annTopN));
    }
}
