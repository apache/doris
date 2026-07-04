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

package org.apache.doris.nereids.trees.plans.physical;

import org.apache.doris.catalog.Column;
import org.apache.doris.connector.api.Connector;
import org.apache.doris.connector.api.ConnectorMetadata;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.api.write.ConnectorWritePartitionField;
import org.apache.doris.connector.api.write.ConnectorWritePartitionSpec;
import org.apache.doris.connector.api.write.ConnectorWritePlanProvider;
import org.apache.doris.datasource.ExternalDatabase;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.PluginDrivenExternalCatalog;
import org.apache.doris.datasource.PluginDrivenExternalTable;
import org.apache.doris.datasource.iceberg.IcebergMergeOperation;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.DistributionSpecHash.ShuffleType;
import org.apache.doris.nereids.properties.DistributionSpecMerge;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.delete.DeleteCommandContext;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.statistics.Statistics;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;

/**
 * Physical Iceberg Merge Sink for UPDATE operations.
 * This sink is responsible for writing position delete files and data files.
 */
public class PhysicalIcebergMergeSink<CHILD_TYPE extends Plan> extends PhysicalBaseExternalTableSink<CHILD_TYPE> {
    private final DeleteCommandContext deleteContext;

    /**
     * Constructor
     */
    public PhysicalIcebergMergeSink(ExternalDatabase database,
                                    ExternalTable targetTable,
                                    List<Column> cols,
                                    List<NamedExpression> outputExprs,
                                    DeleteCommandContext deleteContext,
                                    Optional<GroupExpression> groupExpression,
                                    LogicalProperties logicalProperties,
                                    CHILD_TYPE child) {
        this(database, targetTable, cols, outputExprs, deleteContext, groupExpression, logicalProperties,
                PhysicalProperties.GATHER, null, child);
    }

    /**
     * Constructor
     */
    public PhysicalIcebergMergeSink(ExternalDatabase database,
                                    ExternalTable targetTable,
                                    List<Column> cols,
                                    List<NamedExpression> outputExprs,
                                    DeleteCommandContext deleteContext,
                                    Optional<GroupExpression> groupExpression,
                                    LogicalProperties logicalProperties,
                                    PhysicalProperties physicalProperties,
                                    Statistics statistics,
                                    CHILD_TYPE child) {
        super(PlanType.PHYSICAL_ICEBERG_MERGE_SINK, database, targetTable, cols, outputExprs, groupExpression,
                logicalProperties, physicalProperties, statistics, child);
        this.deleteContext = Objects.requireNonNull(
                deleteContext, "deleteContext != null in PhysicalIcebergMergeSink");
    }

    public DeleteCommandContext getDeleteContext() {
        return deleteContext;
    }

    @Override
    public Plan withChildren(List<Plan> children) {
        return new PhysicalIcebergMergeSink<>(
                database, targetTable,
                cols, outputExprs, deleteContext, groupExpression,
                getLogicalProperties(), physicalProperties, statistics, children.get(0));
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalIcebergMergeSink(this, context);
    }

    @Override
    public Plan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new PhysicalIcebergMergeSink<>(
                database, targetTable, cols, outputExprs,
                deleteContext, groupExpression, getLogicalProperties(), child());
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
                                                 Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        return new PhysicalIcebergMergeSink<>(
                database, targetTable, cols, outputExprs,
                deleteContext, groupExpression, logicalProperties.get(), children.get(0));
    }

    @Override
    public PhysicalPlan withPhysicalPropertiesAndStats(PhysicalProperties physicalProperties, Statistics statistics) {
        return new PhysicalIcebergMergeSink<>(
                database, targetTable, cols, outputExprs,
                deleteContext, groupExpression, getLogicalProperties(), physicalProperties, statistics, child());
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
        PhysicalIcebergMergeSink<?> that = (PhysicalIcebergMergeSink<?>) o;
        return Objects.equals(deleteContext, that.deleteContext);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), deleteContext);
    }

    /**
     * Get output physical properties.
     */
    @Override
    public PhysicalProperties getRequirePhysicalProperties() {
        ExprId rowIdExprId = null;
        ExprId operationExprId = null;
        Map<String, ExprId> nameToExprId = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        List<Slot> outputSlots = child().getOutput();
        for (Slot slot : outputSlots) {
            String name = slot.getName();
            if (operationExprId == null && IcebergMergeOperation.OPERATION_COLUMN.equalsIgnoreCase(name)) {
                operationExprId = slot.getExprId();
            }
            if (rowIdExprId == null && Column.ICEBERG_ROWID_COL.equalsIgnoreCase(name)) {
                rowIdExprId = slot.getExprId();
            }
            nameToExprId.put(name, slot.getExprId());
        }

        ConnectContext ctx = ConnectContext.get();
        if (ctx == null || !ctx.getSessionVariable().isEnableIcebergMergePartitioning()) {
            if (rowIdExprId != null) {
                return PhysicalProperties.createHash(ImmutableList.of(rowIdExprId), ShuffleType.REQUIRE);
            }
            return PhysicalProperties.GATHER;
        }

        if (rowIdExprId == null || operationExprId == null) {
            return PhysicalProperties.GATHER;
        }

        List<ExprId> insertPartitionExprIds = new ArrayList<>();
        List<DistributionSpecMerge.IcebergPartitionField> insertPartitionFields = new ArrayList<>();
        Integer partitionSpecId = null;
        List<Column> partitionColumns = targetTable.getPartitionColumns(Optional.empty());
        Map<String, ExprId> columnExprIdMap = buildColumnExprIdMap(outputSlots, nameToExprId);
        boolean insertExprsOk = false;
        if (!partitionColumns.isEmpty()) {
            insertExprsOk = buildInsertPartitionExprIds(insertPartitionExprIds, partitionColumns, columnExprIdMap);
        }
        InsertPartitionFieldResult fieldResult = getIcebergPartitioning(
                insertPartitionFields, targetTable, columnExprIdMap);
        boolean insertFieldsOk = fieldResult.success;
        boolean hasNonIdentity = fieldResult.hasNonIdentity;
        if (insertFieldsOk) {
            partitionSpecId = fieldResult.partitionSpecId;
        }

        boolean insertRandom = !(insertExprsOk || insertFieldsOk);
        if (!insertFieldsOk && hasNonIdentity) {
            insertRandom = true;
            insertPartitionExprIds.clear();
        }
        if (insertRandom) {
            insertPartitionExprIds.clear();
            insertPartitionFields.clear();
        }

        return new PhysicalProperties(new DistributionSpecMerge(
                operationExprId,
                insertPartitionExprIds,
                ImmutableList.of(rowIdExprId),
                insertRandom,
                insertPartitionFields,
                partitionSpecId));
    }

    private boolean buildInsertPartitionExprIds(List<ExprId> insertPartitionExprIds,
                                                List<Column> partitionColumns,
                                                Map<String, ExprId> columnExprIdMap) {
        for (Column column : partitionColumns) {
            ExprId exprId = columnExprIdMap.get(column.getName());
            if (exprId == null) {
                insertPartitionExprIds.clear();
                return false;
            }
            insertPartitionExprIds.add(exprId);
        }
        return insertPartitionExprIds.size() == partitionColumns.size();
    }

    private Map<String, ExprId> buildColumnExprIdMap(List<Slot> outputSlots,
                                                     Map<String, ExprId> nameToExprId) {
        List<Column> visibleColumns = new ArrayList<>();
        for (Column column : cols) {
            if (column.isVisible()) {
                visibleColumns.add(column);
            }
        }
        List<Slot> dataSlots = getDataSlots(outputSlots);
        if (dataSlots.size() == visibleColumns.size()) {
            Map<String, ExprId> columnExprIdMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
            for (int i = 0; i < visibleColumns.size(); i++) {
                columnExprIdMap.put(visibleColumns.get(i).getName(), dataSlots.get(i).getExprId());
            }
            return columnExprIdMap;
        }
        return nameToExprId;
    }

    private List<Slot> getDataSlots(List<Slot> outputSlots) {
        List<Slot> dataSlots = new ArrayList<>();
        for (Slot slot : outputSlots) {
            String name = slot.getName();
            if (IcebergMergeOperation.OPERATION_COLUMN.equalsIgnoreCase(name)) {
                continue;
            }
            if (Column.ICEBERG_ROWID_COL.equalsIgnoreCase(name)) {
                continue;
            }
            dataSlots.add(slot);
        }
        return dataSlots;
    }

    /**
     * Partition-field resolution for the merge-write distribution: asks the connector for its
     * engine-neutral {@link ConnectorWritePartitionSpec} and reconstructs the legacy result via
     * {@link #reconstructPartitionFields}, preserving the three legacy parities (hard-fail clear on an
     * unresolvable source column, the non-identity pre-pass over all fields, and the spec-id carry).
     * Routes entirely through neutral connector SPI (no {@code instanceof Iceberg*}, no native types).
     */
    private InsertPartitionFieldResult getIcebergPartitioning(
            List<DistributionSpecMerge.IcebergPartitionField> insertPartitionFields,
            ExternalTable table,
            Map<String, ExprId> columnExprIdMap) {
        return buildInsertPartitionFieldsFromConnector(
                insertPartitionFields, (PluginDrivenExternalTable) table, columnExprIdMap);
    }

    /**
     * Post-flip arm of {@link #getIcebergPartitioning}: fetches the connector's engine-neutral
     * {@link ConnectorWritePartitionSpec} via the same canonical access path as
     * {@code PhysicalPlanTranslator.visitPhysicalConnectorTableSink}, then reconstructs the partition
     * fields. A {@code null} write-plan provider or an unresolvable table handle degrades to the
     * non-partitioned result (false, GATHER/random fallback), never an exception — matching the legacy
     * native walk, which only ever returns result objects from inside the distribution derivation.
     */
    private InsertPartitionFieldResult buildInsertPartitionFieldsFromConnector(
            List<DistributionSpecMerge.IcebergPartitionField> insertPartitionFields,
            PluginDrivenExternalTable table,
            Map<String, ExprId> columnExprIdMap) {
        PluginDrivenExternalCatalog catalog = (PluginDrivenExternalCatalog) table.getCatalog();
        Connector connector = catalog.getConnector();
        ConnectorWritePlanProvider writePlanProvider = connector.getWritePlanProvider();
        if (writePlanProvider == null) {
            return new InsertPartitionFieldResult(false, false, null);
        }
        ConnectorSession session = catalog.buildConnectorSession();
        ConnectorMetadata metadata = connector.getMetadata(session);
        ConnectorTableHandle handle = metadata.getTableHandle(
                session, table.getRemoteDbName(), table.getRemoteName()).orElse(null);
        if (handle == null) {
            return new InsertPartitionFieldResult(false, false, null);
        }
        ConnectorWritePartitionSpec spec = writePlanProvider.getWritePartitioning(session, handle);
        return reconstructPartitionFields(insertPartitionFields, spec, columnExprIdMap);
    }

    /**
     * Reconstructs the legacy {@link InsertPartitionFieldResult} from a connector's engine-neutral
     * {@link ConnectorWritePartitionSpec}, byte-for-byte equivalent to the retired native
     * {@code PartitionSpec} walk. Pure (no native types, no I/O) so the three parities are
     * pinned deterministically:
     * <ul>
     *   <li><b>P1 hard-fail clear:</b> a field with a {@code null} source column name, or one whose name
     *       does not resolve to a bound expr id, clears the accumulated fields and returns
     *       {@code success=false} — short-circuited <em>before</em> constructing the field, since the
     *       {@link DistributionSpecMerge.IcebergPartitionField} ctor requires a non-null expr id;</li>
     *   <li><b>P2 non-identity pre-pass:</b> {@code hasNonIdentity} is computed over <em>all</em> fields
     *       from the transform string ({@code !"identity".equals}) independently of resolvability,
     *       matching legacy {@code field.transform().isIdentity()} (only {@code Identity.toString()} is
     *       {@code "identity"}); it gates the caller's random fallback;</li>
     *   <li><b>spec-id carry:</b> the spec id is returned on every partitioned outcome (success or
     *       hard-fail), {@code null} only when unpartitioned.</li>
     * </ul>
     * A {@code null} spec means the connector reported the target unpartitioned (mirroring legacy
     * {@code spec().isPartitioned()}), yielding {@code (false, false, null)}.
     */
    static InsertPartitionFieldResult reconstructPartitionFields(
            List<DistributionSpecMerge.IcebergPartitionField> insertPartitionFields,
            ConnectorWritePartitionSpec spec,
            Map<String, ExprId> columnExprIdMap) {
        if (spec == null) {
            return new InsertPartitionFieldResult(false, false, null);
        }
        List<ConnectorWritePartitionField> fields = spec.getFields();
        boolean hasNonIdentity = false;
        for (ConnectorWritePartitionField field : fields) {
            if (!"identity".equals(field.getTransform())) {
                hasNonIdentity = true;
                break;
            }
        }
        for (ConnectorWritePartitionField field : fields) {
            String sourceColumnName = field.getSourceColumnName();
            if (sourceColumnName == null) {
                insertPartitionFields.clear();
                return new InsertPartitionFieldResult(false, hasNonIdentity, spec.getSpecId());
            }
            ExprId exprId = columnExprIdMap.get(sourceColumnName);
            if (exprId == null) {
                insertPartitionFields.clear();
                return new InsertPartitionFieldResult(false, hasNonIdentity, spec.getSpecId());
            }
            insertPartitionFields.add(new DistributionSpecMerge.IcebergPartitionField(
                    field.getTransform(), exprId, field.getTransformParam(),
                    field.getFieldName(), field.getSourceId()));
        }
        if (insertPartitionFields.isEmpty()) {
            return new InsertPartitionFieldResult(false, hasNonIdentity, spec.getSpecId());
        }
        return new InsertPartitionFieldResult(true, hasNonIdentity, spec.getSpecId());
    }

    // Package-private (not private) so the same-package parity test can assert on the reconstructed
    // result of {@link #reconstructPartitionFields} directly, without driving the full distribution.
    static class InsertPartitionFieldResult {
        final boolean success;
        final boolean hasNonIdentity;
        final Integer partitionSpecId;

        InsertPartitionFieldResult(boolean success, boolean hasNonIdentity, Integer partitionSpecId) {
            this.success = success;
            this.hasNonIdentity = hasNonIdentity;
            this.partitionSpecId = partitionSpecId;
        }
    }
}
