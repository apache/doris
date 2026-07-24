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

import org.apache.doris.catalog.Column;
import org.apache.doris.datasource.iceberg.IcebergExternalDatabase;
import org.apache.doris.datasource.iceberg.IcebergExternalTable;
import org.apache.doris.datasource.iceberg.IcebergWriteSchemaContext;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.PropagateFuncDeps;
import org.apache.doris.nereids.trees.plans.algebra.Sink;
import org.apache.doris.nereids.trees.plans.commands.delete.DeleteCommandContext;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Logical Iceberg Merge Sink for UPDATE operations.
 * This sink is responsible for routing rows to position delete and data insert.
 */
public class LogicalIcebergMergeSink<CHILD_TYPE extends Plan> extends LogicalTableSink<CHILD_TYPE>
        implements Sink, PropagateFuncDeps {
    private final IcebergExternalDatabase database;
    private final IcebergExternalTable targetTable;
    private final DeleteCommandContext deleteContext;
    private final Optional<IcebergWriteSchemaContext> writeSchemaContext;

    /**
     * Constructor
     */
    public LogicalIcebergMergeSink(IcebergExternalDatabase database,
                                   IcebergExternalTable targetTable,
                                   List<Column> cols,
                                   List<NamedExpression> outputExprs,
                                   DeleteCommandContext deleteContext,
                                   Optional<GroupExpression> groupExpression,
                                   Optional<LogicalProperties> logicalProperties,
                                   CHILD_TYPE child) {
        this(database, targetTable, cols, outputExprs, deleteContext, groupExpression,
                logicalProperties, Optional.empty(), child);
    }

    /** Constructor with a statement-pinned Iceberg write schema. */
    public LogicalIcebergMergeSink(IcebergExternalDatabase database,
                                   IcebergExternalTable targetTable,
                                   List<Column> cols,
                                   List<NamedExpression> outputExprs,
                                   DeleteCommandContext deleteContext,
                                   Optional<GroupExpression> groupExpression,
                                   Optional<LogicalProperties> logicalProperties,
                                   Optional<IcebergWriteSchemaContext> writeSchemaContext,
                                   CHILD_TYPE child) {
        super(PlanType.LOGICAL_ICEBERG_MERGE_SINK, outputExprs, groupExpression, logicalProperties, cols, child);
        this.database = Objects.requireNonNull(database, "database != null in LogicalIcebergMergeSink");
        this.targetTable = Objects.requireNonNull(targetTable, "targetTable != null in LogicalIcebergMergeSink");
        this.deleteContext = Objects.requireNonNull(deleteContext, "deleteContext != null in LogicalIcebergMergeSink");
        this.writeSchemaContext = Objects.requireNonNull(
                writeSchemaContext, "writeSchemaContext should not be null");
    }

    public Plan withChildAndUpdateOutput(Plan child) {
        List<NamedExpression> output = child.getOutput().stream()
                .map(NamedExpression.class::cast)
                .collect(ImmutableList.toImmutableList());
        return new LogicalIcebergMergeSink<>(database, targetTable, cols, output,
                deleteContext, Optional.empty(), Optional.empty(), writeSchemaContext, child);
    }

    @Override
    public Plan withChildren(List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1, "LogicalIcebergMergeSink only accepts one child");
        return new LogicalIcebergMergeSink<>(database, targetTable, cols, outputExprs,
                deleteContext, Optional.empty(), Optional.empty(), writeSchemaContext, children.get(0));
    }

    public LogicalIcebergMergeSink<CHILD_TYPE> withOutputExprs(List<NamedExpression> outputExprs) {
        return new LogicalIcebergMergeSink<>(database, targetTable, cols, outputExprs,
                deleteContext, Optional.empty(), Optional.empty(), writeSchemaContext, child());
    }

    public IcebergExternalDatabase getDatabase() {
        return database;
    }

    public IcebergExternalTable getTargetTable() {
        return targetTable;
    }

    public DeleteCommandContext getDeleteContext() {
        return deleteContext;
    }

    public Optional<IcebergWriteSchemaContext> getWriteSchemaContext() {
        return writeSchemaContext;
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
        LogicalIcebergMergeSink<?> that = (LogicalIcebergMergeSink<?>) o;
        return Objects.equals(database, that.database)
                && Objects.equals(targetTable, that.targetTable)
                && Objects.equals(deleteContext, that.deleteContext)
                && Objects.equals(cols, that.cols)
                && Objects.equals(writeSchemaContext, that.writeSchemaContext);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), database, targetTable, cols, deleteContext, writeSchemaContext);
    }

    @Override
    public String toString() {
        return Utils.toSqlString("LogicalIcebergMergeSink[" + id.asInt() + "]",
                "outputExprs", outputExprs,
                "database", database.getFullName(),
                "targetTable", targetTable.getName(),
                "cols", cols,
                "deleteFileType", deleteContext.getDeleteFileType());
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalIcebergMergeSink(this, context);
    }

    @Override
    public Plan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new LogicalIcebergMergeSink<>(database, targetTable, cols, outputExprs,
                deleteContext, groupExpression, Optional.of(getLogicalProperties()), writeSchemaContext, child());
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        return new LogicalIcebergMergeSink<>(database, targetTable, cols, outputExprs,
                deleteContext, groupExpression, logicalProperties, writeSchemaContext, children.get(0));
    }
}
