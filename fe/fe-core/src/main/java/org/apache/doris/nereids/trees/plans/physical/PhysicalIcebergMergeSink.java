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
import org.apache.doris.datasource.iceberg.IcebergExternalDatabase;
import org.apache.doris.datasource.iceberg.IcebergExternalTable;
import org.apache.doris.datasource.iceberg.IcebergMergeOperation;
import org.apache.doris.datasource.iceberg.IcebergWriteSchemaContext;
import org.apache.doris.nereids.memo.GroupExpression;
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
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.types.Types;

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
    private final boolean writesDataFiles;
    private final Optional<IcebergWriteSchemaContext> writeSchemaContext;
    private final boolean requireMergeCardinalityCheck;
    private final Table targetIcebergTable;

    /**
     * Constructor
     */
    public PhysicalIcebergMergeSink(IcebergExternalDatabase database,
                                    IcebergExternalTable targetTable,
                                    Table targetIcebergTable,
                                    List<Column> cols,
                                    List<NamedExpression> outputExprs,
                                    DeleteCommandContext deleteContext,
                                    boolean writesDataFiles,
                                    boolean requireMergeCardinalityCheck,
                                    Optional<GroupExpression> groupExpression,
                                    LogicalProperties logicalProperties,
                                    CHILD_TYPE child) {
        this(database, targetTable, targetIcebergTable, cols, outputExprs, deleteContext,
                writesDataFiles, requireMergeCardinalityCheck,
                groupExpression, logicalProperties,
                PhysicalProperties.GATHER, null, Optional.empty(), child);
    }

    /**
     * Constructor
     */
    public PhysicalIcebergMergeSink(IcebergExternalDatabase database,
                                    IcebergExternalTable targetTable,
                                    Table targetIcebergTable,
                                    List<Column> cols,
                                    List<NamedExpression> outputExprs,
                                    DeleteCommandContext deleteContext,
                                    boolean writesDataFiles,
                                    boolean requireMergeCardinalityCheck,
                                    Optional<GroupExpression> groupExpression,
                                    LogicalProperties logicalProperties,
                                    PhysicalProperties physicalProperties,
                                    Statistics statistics,
                                    CHILD_TYPE child) {
        this(database, targetTable, targetIcebergTable, cols, outputExprs,
                deleteContext, writesDataFiles, requireMergeCardinalityCheck,
                groupExpression, logicalProperties,
                physicalProperties, statistics, Optional.empty(), child);
    }

    /** Constructor with a statement-pinned Iceberg write schema. */
    public PhysicalIcebergMergeSink(IcebergExternalDatabase database,
                                    IcebergExternalTable targetTable,
                                    Table targetIcebergTable,
                                    List<Column> cols,
                                    List<NamedExpression> outputExprs,
                                    DeleteCommandContext deleteContext,
                                    boolean writesDataFiles,
                                    boolean requireMergeCardinalityCheck,
                                    Optional<GroupExpression> groupExpression,
                                    LogicalProperties logicalProperties,
                                    PhysicalProperties physicalProperties,
                                    Statistics statistics,
                                    Optional<IcebergWriteSchemaContext> writeSchemaContext,
                                    CHILD_TYPE child) {
        super(PlanType.PHYSICAL_ICEBERG_MERGE_SINK, database, targetTable, cols, outputExprs, groupExpression,
                logicalProperties, physicalProperties, statistics, child);
        this.deleteContext = Objects.requireNonNull(
                deleteContext, "deleteContext != null in PhysicalIcebergMergeSink");
        this.writesDataFiles = writesDataFiles;
        this.writeSchemaContext = Objects.requireNonNull(
                writeSchemaContext, "writeSchemaContext should not be null");
        this.requireMergeCardinalityCheck = requireMergeCardinalityCheck;
        this.targetIcebergTable = Objects.requireNonNull(
                targetIcebergTable, "targetIcebergTable != null in PhysicalIcebergMergeSink");
    }

    public DeleteCommandContext getDeleteContext() {
        return deleteContext;
    }

    public boolean isWritesDataFiles() {
        return writesDataFiles;
    }

    public Optional<IcebergWriteSchemaContext> getWriteSchemaContext() {
        return writeSchemaContext;
    }

    public boolean isRequireMergeCardinalityCheck() {
        return requireMergeCardinalityCheck;
    }

    public Table getTargetIcebergTable() {
        return targetIcebergTable;
    }

    @Override
    public Plan withChildren(List<Plan> children) {
        return new PhysicalIcebergMergeSink<>(
                (IcebergExternalDatabase) database, (IcebergExternalTable) targetTable,
                targetIcebergTable, cols, outputExprs, deleteContext, writesDataFiles,
                requireMergeCardinalityCheck,
                groupExpression,
                getLogicalProperties(), physicalProperties, statistics, writeSchemaContext, children.get(0));
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalIcebergMergeSink(this, context);
    }

    @Override
    public Plan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new PhysicalIcebergMergeSink<>(
                (IcebergExternalDatabase) database, (IcebergExternalTable) targetTable,
                targetIcebergTable, cols, outputExprs, deleteContext, writesDataFiles,
                requireMergeCardinalityCheck,
                groupExpression, getLogicalProperties(), PhysicalProperties.GATHER, null,
                writeSchemaContext, child());
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
                                                 Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        return new PhysicalIcebergMergeSink<>(
                (IcebergExternalDatabase) database, (IcebergExternalTable) targetTable,
                targetIcebergTable, cols, outputExprs, deleteContext, writesDataFiles,
                requireMergeCardinalityCheck,
                groupExpression, logicalProperties.get(), PhysicalProperties.GATHER, null,
                writeSchemaContext, children.get(0));
    }

    @Override
    public PhysicalPlan withPhysicalPropertiesAndStats(PhysicalProperties physicalProperties, Statistics statistics) {
        return new PhysicalIcebergMergeSink<>(
                (IcebergExternalDatabase) database, (IcebergExternalTable) targetTable,
                targetIcebergTable, cols, outputExprs, deleteContext, writesDataFiles,
                requireMergeCardinalityCheck,
                groupExpression, getLogicalProperties(),
                physicalProperties, statistics, writeSchemaContext, child());
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
        return Objects.equals(deleteContext, that.deleteContext)
                && writesDataFiles == that.writesDataFiles
                && requireMergeCardinalityCheck == that.requireMergeCardinalityCheck
                && Objects.equals(targetIcebergTable, that.targetIcebergTable)
                && Objects.equals(writeSchemaContext, that.writeSchemaContext);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), deleteContext, targetIcebergTable, writesDataFiles,
                requireMergeCardinalityCheck, writeSchemaContext);
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
            if (rowIdExprId != null && operationExprId != null) {
                // Route only delete images by row ID; unmatched inserts have NULL row IDs and must
                // remain distributable instead of collapsing onto one exchange channel.
                return new PhysicalProperties(new DistributionSpecMerge(
                        operationExprId,
                        ImmutableList.of(),
                        ImmutableList.of(rowIdExprId),
                        true,
                        ImmutableList.of(),
                        null));
            }
            return PhysicalProperties.GATHER;
        }

        if (rowIdExprId == null || operationExprId == null) {
            return PhysicalProperties.GATHER;
        }

        List<ExprId> insertPartitionExprIds = new ArrayList<>();
        List<DistributionSpecMerge.IcebergPartitionField> insertPartitionFields = new ArrayList<>();
        Integer partitionSpecId = null;
        // Distribution and writer serialization must read the same retained spec/schema.
        List<Column> partitionColumns = getRetainedPartitionColumns();
        Map<String, ExprId> columnExprIdMap = buildColumnExprIdMap(outputSlots, nameToExprId);
        boolean insertExprsOk = false;
        if (!partitionColumns.isEmpty()) {
            insertExprsOk = buildInsertPartitionExprIds(insertPartitionExprIds, partitionColumns, columnExprIdMap);
        }
        InsertPartitionFieldResult fieldResult = buildInsertPartitionFields(
                insertPartitionFields, targetIcebergTable, columnExprIdMap);
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

    private InsertPartitionFieldResult buildInsertPartitionFields(
            List<DistributionSpecMerge.IcebergPartitionField> insertPartitionFields,
            Table table,
            Map<String, ExprId> columnExprIdMap) {
        PartitionSpec spec = table.spec();
        if (spec == null || !spec.isPartitioned()) {
            return new InsertPartitionFieldResult(false, false, null);
        }
        Schema schema = table.schema();
        boolean hasNonIdentity = false;
        for (PartitionField field : spec.fields()) {
            if (!field.transform().isIdentity()) {
                hasNonIdentity = true;
                break;
            }
        }
        if (schema == null) {
            return new InsertPartitionFieldResult(false, hasNonIdentity, spec.specId());
        }
        for (PartitionField field : spec.fields()) {
            Types.NestedField sourceField = schema.findField(field.sourceId());
            if (sourceField == null) {
                insertPartitionFields.clear();
                return new InsertPartitionFieldResult(false, hasNonIdentity, spec.specId());
            }
            ExprId exprId = columnExprIdMap.get(sourceField.name());
            if (exprId == null) {
                insertPartitionFields.clear();
                return new InsertPartitionFieldResult(false, hasNonIdentity, spec.specId());
            }
            String transform = field.transform().toString();
            Integer param = parseTransformParam(transform);
            insertPartitionFields.add(new DistributionSpecMerge.IcebergPartitionField(
                    transform, exprId, param, field.name(), field.sourceId()));
        }
        if (insertPartitionFields.isEmpty()) {
            return new InsertPartitionFieldResult(false, hasNonIdentity, spec.specId());
        }
        return new InsertPartitionFieldResult(true, hasNonIdentity, spec.specId());
    }

    private List<Column> getRetainedPartitionColumns() {
        Map<String, Column> columnsByName = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (Column column : cols) {
            columnsByName.put(column.getName(), column);
        }
        List<Column> partitionColumns = new ArrayList<>();
        Schema schema = targetIcebergTable.schema();
        for (PartitionField field : targetIcebergTable.spec().fields()) {
            // Transformed fields are encoded through insertPartitionFields; treating their source
            // columns as identity keys would route rows by a different partitioning invariant.
            if (!field.transform().isIdentity()) {
                continue;
            }
            Types.NestedField sourceField = schema.findField(field.sourceId());
            if (sourceField != null && columnsByName.containsKey(sourceField.name())) {
                partitionColumns.add(columnsByName.get(sourceField.name()));
            }
        }
        return partitionColumns;
    }

    private Integer parseTransformParam(String transform) {
        int start = transform.indexOf('[');
        int end = transform.indexOf(']');
        if (start < 0 || end <= start) {
            return null;
        }
        try {
            return Integer.parseInt(transform.substring(start + 1, end));
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static class InsertPartitionFieldResult {
        private final boolean success;
        private final boolean hasNonIdentity;
        private final Integer partitionSpecId;

        private InsertPartitionFieldResult(boolean success, boolean hasNonIdentity, Integer partitionSpecId) {
            this.success = success;
            this.hasNonIdentity = hasNonIdentity;
            this.partitionSpecId = partitionSpecId;
        }
    }
}
