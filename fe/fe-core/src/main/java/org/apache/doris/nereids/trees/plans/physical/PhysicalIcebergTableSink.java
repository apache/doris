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
import org.apache.doris.datasource.iceberg.IcebergWriteSchemaContext;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.DistributionSpecHiveTableSinkHashPartitioned;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.statistics.Statistics;

import com.google.common.base.Preconditions;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.types.Types;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/** physical iceberg sink */
public class PhysicalIcebergTableSink<CHILD_TYPE extends Plan> extends PhysicalBaseExternalTableSink<CHILD_TYPE> {
    private final Table targetIcebergTable;
    private final Optional<IcebergWriteSchemaContext> writeSchemaContext;

    /**
     * constructor
     */
    public PhysicalIcebergTableSink(IcebergExternalDatabase database,
                                    IcebergExternalTable targetTable,
                                    Table targetIcebergTable,
                                    List<Column> cols,
                                    List<NamedExpression> outputExprs,
                                    Optional<GroupExpression> groupExpression,
                                    LogicalProperties logicalProperties,
                                    CHILD_TYPE child) {
        this(database, targetTable, targetIcebergTable, cols, outputExprs, groupExpression, logicalProperties,
                PhysicalProperties.GATHER, null, Optional.empty(), child);
    }

    /**
     * constructor
     */
    public PhysicalIcebergTableSink(IcebergExternalDatabase database,
                                    IcebergExternalTable targetTable,
                                    Table targetIcebergTable,
                                    List<Column> cols,
                                    List<NamedExpression> outputExprs,
                                    Optional<GroupExpression> groupExpression,
                                    LogicalProperties logicalProperties,
                                    PhysicalProperties physicalProperties,
                                    Statistics statistics,
                                    CHILD_TYPE child) {
        this(database, targetTable, targetIcebergTable, cols, outputExprs, groupExpression, logicalProperties,
                physicalProperties, statistics, Optional.empty(), child);
    }

    /** Constructor with a statement-pinned Iceberg write schema. */
    public PhysicalIcebergTableSink(IcebergExternalDatabase database,
                                    IcebergExternalTable targetTable,
                                    Table targetIcebergTable,
                                    List<Column> cols,
                                    List<NamedExpression> outputExprs,
                                    Optional<GroupExpression> groupExpression,
                                    LogicalProperties logicalProperties,
                                    PhysicalProperties physicalProperties,
                                    Statistics statistics,
                                    Optional<IcebergWriteSchemaContext> writeSchemaContext,
                                    CHILD_TYPE child) {
        super(PlanType.PHYSICAL_ICEBERG_TABLE_SINK, database, targetTable, cols, outputExprs, groupExpression,
                logicalProperties, physicalProperties, statistics, child);
        this.targetIcebergTable = Objects.requireNonNull(
                targetIcebergTable, "targetIcebergTable != null in PhysicalIcebergTableSink");
        this.writeSchemaContext = Objects.requireNonNull(
                writeSchemaContext, "writeSchemaContext should not be null");
    }

    @Override
    public Plan withChildren(List<Plan> children) {
        return new PhysicalIcebergTableSink<>(
                (IcebergExternalDatabase) database, (IcebergExternalTable) targetTable,
                targetIcebergTable, cols, outputExprs, groupExpression,
                getLogicalProperties(), physicalProperties, statistics, writeSchemaContext, children.get(0));
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalIcebergTableSink(this, context);
    }

    @Override
    public Plan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new PhysicalIcebergTableSink<>(
                (IcebergExternalDatabase) database, (IcebergExternalTable) targetTable,
                targetIcebergTable, cols, outputExprs,
                groupExpression, getLogicalProperties(), PhysicalProperties.GATHER, null,
                writeSchemaContext, child());
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
                                                 Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        return new PhysicalIcebergTableSink<>(
                (IcebergExternalDatabase) database, (IcebergExternalTable) targetTable,
                targetIcebergTable, cols, outputExprs,
                groupExpression, logicalProperties.get(), PhysicalProperties.GATHER, null,
                writeSchemaContext, children.get(0));
    }

    @Override
    public PhysicalPlan withPhysicalPropertiesAndStats(PhysicalProperties physicalProperties, Statistics statistics) {
        return new PhysicalIcebergTableSink<>(
                (IcebergExternalDatabase) database, (IcebergExternalTable) targetTable,
                targetIcebergTable, cols, outputExprs,
                groupExpression, getLogicalProperties(), physicalProperties, statistics,
                writeSchemaContext, child());
    }

    public Table getTargetIcebergTable() {
        return targetIcebergTable;
    }

    public Optional<IcebergWriteSchemaContext> getWriteSchemaContext() {
        return writeSchemaContext;
    }

    /**
     * get output physical properties
     */
    @Override
    public PhysicalProperties getRequirePhysicalProperties() {
        // For Iceberg rewrite operations with small data volume,
        // use GATHER distribution to collect data to a single node
        // This helps minimize the number of output files
        ConnectContext connectContext = ConnectContext.get();
        if (connectContext != null && connectContext.getStatementContext() != null
                && connectContext.getStatementContext().isUseGatherForIcebergRewrite()) {
            return PhysicalProperties.GATHER;
        }

        PartitionSpec partitionSpec = writeSchemaContext
                .map(IcebergWriteSchemaContext::getPartitionSpec)
                .orElse(targetIcebergTable.spec());
        Schema schema = writeSchemaContext
                .map(IcebergWriteSchemaContext::getSchema)
                .orElse(targetIcebergTable.schema());
        Set<String> partitionNames = new java.util.TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        for (PartitionField field : partitionSpec.fields()) {
            Types.NestedField sourceField = schema.findField(field.sourceId());
            Preconditions.checkState(sourceField != null,
                    "Iceberg partition source field %s is absent from the pinned write schema",
                    field.sourceId());
            partitionNames.add(sourceField.name());
        }
        if (!partitionNames.isEmpty()) {
            List<Integer> columnIdx = new ArrayList<>();
            List<Column> fullSchema = writeSchemaContext
                    .map(IcebergWriteSchemaContext::getColumns)
                    .orElse(cols);
            for (int i = 0; i < fullSchema.size(); i++) {
                Column column = fullSchema.get(i);
                if (partitionNames.contains(column.getName())) {
                    columnIdx.add(i);
                }
            }
            // mapping partition id
            List<ExprId> exprIds = columnIdx.stream()
                    .map(idx -> child().getOutput().get(idx).getExprId())
                    .collect(Collectors.toList());
            Preconditions.checkState(exprIds.size() == partitionNames.size(),
                    "Iceberg partition columns %s are not present in the pinned sink schema",
                    partitionNames);
            DistributionSpecHiveTableSinkHashPartitioned shuffleInfo
                    = new DistributionSpecHiveTableSinkHashPartitioned();
            shuffleInfo.setOutputColExprIds(exprIds);
            return new PhysicalProperties(shuffleInfo);
        }
        return PhysicalProperties.SINK_RANDOM_PARTITIONED;
    }
}
