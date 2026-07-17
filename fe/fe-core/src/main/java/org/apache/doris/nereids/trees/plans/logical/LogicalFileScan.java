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
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.common.IdGenerator;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.mvcc.MvccUtil;
import org.apache.doris.datasource.plugin.PluginDrivenExternalTable;
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
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;

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
        // Resolve the schema AS OF THIS reference's own version. tableSnapshot/scanParams are final fields
        // set in the ctor, so they are available even though computeOutput() is evaluated lazily
        // (AbstractPlan.logicalPropertiesSupplier) -- and the version-aware lookup is key-exact, so the
        // answer does not depend on how many versions the statement pins or on when this runs. The
        // version-BLIND getFullSchema() would degrade to LATEST once this table is pinned at two versions
        // (e.g. t@tag(a) JOIN t@tag(b)), binding a schema NO reference asked for and making the scan-time
        // guard fire on a column the query never referenced.
        getTable().getFullSchema(MvccUtil.getSnapshotFromContext(table, tableSnapshot, scanParams))
                .stream()
                .map(col -> SlotReference.fromColumn(exprIdGenerator.getNextId(), table, col, qualified()))
                .forEach(slots::add);
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
        if (table instanceof PluginDrivenExternalTable) {
            // Post-flip plugin-driven tables (e.g. iceberg as PluginDrivenMvccExternalTable) declare
            // nested-column prune via ConnectorCapability; the legacy exact-class IcebergExternalTable arm
            // below is dead for them. Only enabled when the connector also carries nested field ids (see
            // SUPPORTS_NESTED_COLUMN_PRUNE / SlotTypeReplacer), else nested leaves would read NULL.
            return ((PluginDrivenExternalTable) table).supportsNestedColumnPrune();
        }
        return false;
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
        return AbstractPlan.copyWithSameId(this, () ->
                new LogicalFileScan(relationId, (ExternalTable) table, qualifier,
                selectedPartitions, operativeSlots, virtualColumns, tableSample, tableSnapshot,
                scanParams, groupExpression, Optional.of(getLogicalProperties()), tableAlias, cachedOutputs));
    }

    public LogicalFileScan withCachedOutput(List<Slot> cachedOutputs) {
        return AbstractPlan.copyWithSameId(this, () ->
                new LogicalFileScan(relationId, (ExternalTable) table, qualifier,
                selectedPartitions, operativeSlots, virtualColumns, tableSample, tableSnapshot,
                scanParams, groupExpression, Optional.empty(), tableAlias, Optional.of(cachedOutputs)));
    }

    @Override
    public List<Slot> getOperativeSlots() {
        return operativeSlots;
    }
}
