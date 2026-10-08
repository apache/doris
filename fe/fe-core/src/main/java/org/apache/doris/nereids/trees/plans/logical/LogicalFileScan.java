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
import org.apache.doris.datasource.mvcc.MvccUtil;
import org.apache.doris.datasource.plugin.PluginDrivenExternalTable;
import org.apache.doris.datasource.plugin.PluginDrivenSysExternalTable;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.rules.expression.rules.SortedPartitionRanges;
import org.apache.doris.nereids.trees.TableSample;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.plans.AbstractPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.Utils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Logical file scan for external catalog.
 */
public class LogicalFileScan extends LogicalCatalogRelation implements SupportPruneNestedColumn {
    protected final SelectedPartitions selectedPartitions;
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
                // This reference's OWN version, not the ambient one: the selectors are right here as ctor
                // params, and the blind lookup degrades to LATEST once the table is pinned at two versions.
                table.initSelectedPartitions(
                        MvccUtil.getSnapshotFromContext(table, tableSnapshot, scanParams)),
                operativeSlots, ImmutableList.of(),
                tableSample, tableSnapshot,
                scanParams, Optional.empty(), Optional.empty(), "",
                cachedOutputs);
    }

    /**
     * Constructor for LogicalFileScan.
     */
    protected LogicalFileScan(RelationId id, ExternalTable table, List<String> qualifier,
            SelectedPartitions selectedPartitions, Collection<Slot> operativeSlots,
            List<NamedExpression> virtualColumns, Optional<TableSample> tableSample,
            Optional<TableSnapshot> tableSnapshot, Optional<TableScanParams> scanParams,
            Optional<GroupExpression> groupExpression, Optional<LogicalProperties> logicalProperties,
            String tableAlias, Optional<List<Slot>> cachedSlots) {
        super(id, PlanType.LOGICAL_FILE_SCAN, table, qualifier, operativeSlots, virtualColumns,
                groupExpression, logicalProperties, tableAlias);
        this.selectedPartitions = selectedPartitions;
        this.tableSample = tableSample;
        this.tableSnapshot = tableSnapshot;
        this.scanParams = scanParams;
        this.cachedOutputs = cachedSlots;
    }

    protected LogicalFileScan(RelationId id, ExternalTable table, List<String> qualifier,
            SelectedPartitions selectedPartitions, Collection<Slot> operativeSlots,
            List<NamedExpression> virtualColumns, Optional<TableSample> tableSample,
            Optional<TableSnapshot> tableSnapshot, Optional<TableScanParams> scanParams,
            Optional<GroupExpression> groupExpression, Optional<LogicalProperties> logicalProperties,
            Optional<List<Slot>> cachedOutputs) {
        this(id, table, qualifier, selectedPartitions, operativeSlots, virtualColumns, tableSample, tableSnapshot,
                scanParams, groupExpression, logicalProperties, "", cachedOutputs);
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
                selectedPartitions, operativeSlots, virtualColumns, tableSample, tableSnapshot,
                scanParams, groupExpression, Optional.of(getLogicalProperties()), tableAlias,
                cachedOutputs));
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        return AbstractPlan.copyWithSameId(this, () ->
                new LogicalFileScan(relationId, (ExternalTable) table, qualifier,
                selectedPartitions, operativeSlots, virtualColumns, tableSample, tableSnapshot,
                scanParams, groupExpression, logicalProperties, tableAlias, cachedOutputs));
    }

    public LogicalFileScan withSelectedPartitions(SelectedPartitions selectedPartitions) {
        return AbstractPlan.copyWithSameId(this, () ->
                new LogicalFileScan(relationId, (ExternalTable) table, qualifier,
                selectedPartitions, operativeSlots, virtualColumns, tableSample, tableSnapshot,
                scanParams, Optional.empty(), Optional.of(getLogicalProperties()), tableAlias,
                cachedOutputs));
    }

    @Override
    public LogicalFileScan withRelationId(RelationId relationId) {
        return AbstractPlan.copyWithSameId(this, () ->
                new LogicalFileScan(relationId, (ExternalTable) table, qualifier,
                selectedPartitions, operativeSlots, virtualColumns, tableSample, tableSnapshot,
                scanParams, Optional.empty(), Optional.empty(), tableAlias, cachedOutputs));
    }

    public LogicalFileScan withTableAlias(String tableAlias) {
        return AbstractPlan.copyWithSameId(this, () ->
                new LogicalFileScan(relationId, (ExternalTable) table, qualifier,
                selectedPartitions, operativeSlots, virtualColumns, tableSample, tableSnapshot,
                scanParams, Optional.empty(), Optional.of(getLogicalProperties()), tableAlias,
                cachedOutputs));
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalFileScan(this, context);
    }

    @Override
    public boolean equals(Object o) {
        return super.equals(o) && Objects.equals(selectedPartitions, ((LogicalFileScan) o).selectedPartitions);
    }

    @Override
    protected boolean hasSameScanState(LogicalCatalogRelation other) {
        if (!Utils.isSameClass(this, other)) {
            return false;
        }
        LogicalFileScan that = (LogicalFileScan) other;
        return selectedPartitions.hasSameSelection(that.selectedPartitions)
                && Objects.equals(tableSample, that.tableSample)
                && hasSameSnapshot(tableSnapshot, that.tableSnapshot)
                && hasSameScanParams(scanParams, that.scanParams);
    }

    @Override
    public List<Slot> computeOutput() {
        if (cachedOutputs.isPresent()) {
            return cachedOutputs.get();
        }

        if (table instanceof PluginDrivenExternalTable) {
            // SPI-driven tables: schema is fetched via ConnectorMetadata.getTableSchema()
            // (see PluginDrivenExternalTable.initSchema). Use getFullSchema() so any
            // hidden/metadata columns the connector exposes are reachable.
            return computePluginDrivenOutput();
        }
        return super.computeOutput();
    }

    private List<Slot> computePluginDrivenOutput() {
        IdGenerator<ExprId> exprIdGenerator = StatementScopeIdGenerator.getExprIdGenerator();
        Builder<Slot> slots = ImmutableList.builder();
        pluginDrivenSchemaAtThisVersion()
                .stream()
                .map(col -> SlotReference.fromColumn(exprIdGenerator.getNextId(), table, col, qualified()))
                .forEach(slots::add);
        for (NamedExpression virtualColumn : virtualColumns) {
            slots.add(virtualColumn.toSlot());
        }
        return slots.build();
    }

    /**
     * The plugin table's schema AS OF THIS reference's own version. {@code tableSnapshot}/{@code scanParams}
     * are final fields set in the ctor, so they are available even though {@code computeOutput()} is
     * evaluated lazily ({@code AbstractPlan.logicalPropertiesSupplier}) -- and the version-aware lookup is
     * key-exact, so the answer does not depend on how many versions the statement pins or on when this runs.
     * The version-BLIND {@code getFullSchema()} would degrade to LATEST once this table is pinned at two
     * versions (e.g. {@code t@tag(a) JOIN t@tag(b)}), binding a schema NO reference asked for and making the
     * scan-time guard fire on a column the query never referenced.
     */
    private List<Column> pluginDrivenSchemaAtThisVersion() {
        if (table instanceof PluginDrivenSysExternalTable) {
            // A SYSTEM table resolves its own pin: it is not an MvccTable, and BindRelation returns from
            // handleMetaTable BEFORE loadSnapshots, so the context lookup is empty by construction and the
            // schema would silently degrade to LATEST -- binding a since-renamed column as missing and a
            // since-retyped one at the WRONG TYPE, while the scan reads the pinned snapshot. The pin is
            // resolved off the SOURCE table and memoized there, so this and the scan node share one answer.
            return ((PluginDrivenSysExternalTable) table).getFullSchemaAt(tableSnapshot, scanParams);
        }
        return getTable().getFullSchema(MvccUtil.getSnapshotFromContext(table, tableSnapshot, scanParams));
    }

    @Override
    public List<Slot> computeAsteriskOutput() {
        return super.computeAsteriskOutput();
    }

    @Override
    public boolean supportPruneNestedColumn() {
        ExternalTable table = getTable();
        if (table instanceof PluginDrivenExternalTable) {
            // Post-flip plugin-driven tables (e.g. iceberg as PluginDrivenMvccExternalTable) declare
            // nested-column prune via ConnectorCapability; the legacy exact-class IcebergExternalTable arm
            // below is dead for them. Field ids are NOT required here: a connector addressed by name
            // (paimon, fluss) declares SUPPORTS_NESTED_COLUMN_PRUNE alone, and only the separate
            // SUPPORTS_FIELD_ID_ACCESS_PATH lets SlotTypeReplacer rewrite the paths to ids.
            return ((PluginDrivenExternalTable) table).supportsNestedColumnPrune();
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
        public static SelectedPartitions NOT_PRUNED = new SelectedPartitions(0, ImmutableMap.of(), false, false,
                Optional.empty());
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
         * sorted partition ranges for binary search filtering.
         * Frozen at construction time to ensure consistency with selectedPartitions.
         * Empty if binary search is not applicable (e.g., default partition only).
         */
        public final Optional<SortedPartitionRanges<String>> sortedPartitionRanges;

        private final List<Slot> partitionSlots;
        private final Set<Expression> prunableConjuncts;

        /**
         * Constructor for SelectedPartitions.
         */
        public SelectedPartitions(long totalPartitionNum, Map<String, PartitionItem> selectedPartitions,
                boolean isPruned) {
            this(totalPartitionNum, selectedPartitions, isPruned, false, Optional.empty());
        }

        /**
         * Constructor for SelectedPartitions.
         */
        public SelectedPartitions(long totalPartitionNum, Map<String, PartitionItem> selectedPartitions,
                boolean isPruned, boolean hasPartitionPredicate) {
            this(totalPartitionNum, selectedPartitions, isPruned, hasPartitionPredicate, Optional.empty());
        }

        /**
         * Constructor for SelectedPartitions with sorted partition ranges.
         */
        public SelectedPartitions(long totalPartitionNum, Map<String, PartitionItem> selectedPartitions,
                boolean isPruned, boolean hasPartitionPredicate,
                Optional<SortedPartitionRanges<String>> sortedPartitionRanges) {
            this(totalPartitionNum, selectedPartitions, isPruned, hasPartitionPredicate, sortedPartitionRanges,
                    ImmutableList.of(), ImmutableSet.of());
        }

        private SelectedPartitions(long totalPartitionNum, Map<String, PartitionItem> selectedPartitions,
                boolean isPruned, boolean hasPartitionPredicate,
                Optional<SortedPartitionRanges<String>> sortedPartitionRanges,
                List<Slot> partitionSlots, Set<Expression> prunableConjuncts) {
            this.totalPartitionNum = totalPartitionNum;
            this.selectedPartitions = ImmutableMap.copyOf(Objects.requireNonNull(selectedPartitions,
                    "selectedPartitions is null"));
            this.isPruned = isPruned;
            this.hasPartitionPredicate = hasPartitionPredicate;
            this.sortedPartitionRanges = Objects.requireNonNull(sortedPartitionRanges,
                    "sortedPartitionRanges is null");
            this.partitionSlots = ImmutableList.copyOf(Objects.requireNonNull(partitionSlots,
                    "partitionSlots is null"));
            this.prunableConjuncts = ImmutableSet.copyOf(Objects.requireNonNull(prunableConjuncts,
                    "prunableConjuncts is null"));
            Preconditions.checkArgument(isPruned || prunableConjuncts.isEmpty(),
                    "prunable conjuncts require a pruned partition state");
        }

        /**
         * Returns a fresh partition-pruning result derived from this selection. The sorted ranges are cleared
         * because they describe the pre-pruning partition map, while the predicate proof is frozen with the
         * surviving partition set.
         */
        public SelectedPartitions withPruneResult(Map<String, PartitionItem> selectedPartitions,
                boolean hasPartitionPredicate, List<Slot> partitionSlots,
                Set<Expression> prunableConjuncts) {
            Preconditions.checkState(!isPruned, "partition pruning has already been applied");
            Preconditions.checkArgument(this.selectedPartitions.keySet().containsAll(selectedPartitions.keySet()),
                    "selected partitions must be a subset of the current partition snapshot");
            return new SelectedPartitions(totalPartitionNum, selectedPartitions, true, hasPartitionPredicate,
                    Optional.empty(), partitionSlots, prunableConjuncts);
        }

        public boolean hasPruningProof() {
            return !prunableConjuncts.isEmpty();
        }

        /** Rebind the pruning proof to a new output namespace. */
        public SelectedPartitions rebindPruningProof(List<Slot> output) {
            if (!hasPruningProof()) {
                return this;
            }
            Map<String, Slot> outputSlotsByName = new HashMap<>(output.size());
            for (Slot slot : output) {
                outputSlotsByName.put(slot.getName().toLowerCase(Locale.ROOT), slot);
            }
            Map<Expression, Expression> replacements = new HashMap<>(partitionSlots.size());
            ImmutableList.Builder<Slot> reboundSlots =
                    ImmutableList.builderWithExpectedSize(partitionSlots.size());
            for (Slot partitionSlot : partitionSlots) {
                Slot reboundSlot = outputSlotsByName.get(partitionSlot.getName().toLowerCase(Locale.ROOT));
                Preconditions.checkState(reboundSlot != null,
                        "Can not find output slot for prunable partition slot: %s", partitionSlot.getName());
                reboundSlots.add(reboundSlot);
                if (!partitionSlot.equals(reboundSlot)) {
                    replacements.put(partitionSlot, reboundSlot);
                }
            }
            if (replacements.isEmpty()) {
                return this;
            }
            ImmutableSet.Builder<Expression> reboundConjuncts =
                    ImmutableSet.builderWithExpectedSize(prunableConjuncts.size());
            for (Expression conjunct : prunableConjuncts) {
                reboundConjuncts.add(ExpressionUtils.replace(conjunct, replacements));
            }
            return new SelectedPartitions(totalPartitionNum, selectedPartitions, isPruned, hasPartitionPredicate,
                    sortedPartitionRanges, reboundSlots.build(), reboundConjuncts.build());
        }

        public Set<Expression> getPrunableConjuncts() {
            return prunableConjuncts;
        }

        /** Compare partition-selection state independently of the pruning proof's output-slot namespace. */
        private boolean hasSameSelection(SelectedPartitions other) {
            return totalPartitionNum == other.totalPartitionNum
                    && isPruned == other.isPruned
                    && hasPartitionPredicate == other.hasPartitionPredicate
                    && selectedPartitions.keySet().equals(other.selectedPartitions.keySet())
                    && sortedPartitionRanges.isPresent() == other.sortedPartitionRanges.isPresent();
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
            return hasSameSelection(that)
                    && Objects.equals(partitionSlots, that.partitionSlots)
                    && Objects.equals(prunableConjuncts, that.prunableConjuncts);
        }

        @Override
        public int hashCode() {
            return Objects.hash(totalPartitionNum, selectedPartitions.keySet(), isPruned, hasPartitionPredicate,
                    sortedPartitionRanges.isPresent(), partitionSlots, prunableConjuncts);
        }
    }

    @Override
    public LogicalFileScan withOperativeSlots(Collection<Slot> operativeSlots) {
        return AbstractPlan.copyWithSameId(this, () ->
                new LogicalFileScan(relationId, (ExternalTable) table, qualifier,
                selectedPartitions, operativeSlots, virtualColumns, tableSample, tableSnapshot,
                scanParams, groupExpression, Optional.of(getLogicalProperties()), tableAlias, cachedOutputs));
    }

    public LogicalFileScan withCachedOutput(List<Slot> cachedOutputs) {
        SelectedPartitions reboundPartitions = selectedPartitions.rebindPruningProof(cachedOutputs);
        return AbstractPlan.copyWithSameId(this, () ->
                new LogicalFileScan(relationId, (ExternalTable) table, qualifier,
                reboundPartitions, operativeSlots, virtualColumns, tableSample, tableSnapshot,
                scanParams, groupExpression, Optional.empty(), tableAlias, Optional.of(cachedOutputs)));
    }

    /** Rebind a copied scan's partition-pruning proof to this scan's output slots. */
    public LogicalFileScan withReboundPartitionPruningProofFrom(LogicalFileScan source) {
        Preconditions.checkArgument(getTable().getId() == source.getTable().getId(),
                "partition-pruning proof can only be rebound between scans of the same table");
        return withSelectedPartitions(source.selectedPartitions.rebindPruningProof(getOutput()));
    }

    @Override
    public List<Slot> getOperativeSlots() {
        return operativeSlots;
    }
}
