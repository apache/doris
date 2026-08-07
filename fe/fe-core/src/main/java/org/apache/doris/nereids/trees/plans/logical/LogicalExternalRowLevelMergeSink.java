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
import org.apache.doris.datasource.ExternalDatabase;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.PropagateFuncDeps;
import org.apache.doris.nereids.trees.plans.algebra.Sink;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Logical external row-level merge sink for UPDATE/MERGE operations.
 * This sink is responsible for routing rows to position delete and data insert.
 */
public class LogicalExternalRowLevelMergeSink<CHILD_TYPE extends Plan> extends LogicalTableSink<CHILD_TYPE>
        implements Sink, PropagateFuncDeps {
    private final ExternalDatabase database;
    private final ExternalTable targetTable;
    private final String boundWriteMetadataIdentity;
    // True for SQL MERGE INTO, false for UPDATE. MERGE must reject a target row matched by more than one
    // source row (SQL cardinality rule), which the BE sink can only do when the plan keeps the merge
    // distribution; UPDATE has no such rule. Read by RequestPropertyDeriver (which otherwise drops the
    // distribution when enable_strict_consistency_dml is off) and threaded onto the connector write handle.
    private final boolean requireMergeCardinalityCheck;

    /**
     * Constructor.
     *
     * <p>{@code database}/{@code targetTable} are typed to the generic {@link ExternalDatabase}/
     * {@link ExternalTable} (not concrete iceberg types): the synthesis passes a
     * {@code PluginDrivenExternalTable} for the iceberg table. Every consumer ({@code ExplainCommand}, the
     * implementation rule, the translator) only uses the generic {@code getId()}/schema API.</p>
     */
    public LogicalExternalRowLevelMergeSink(ExternalDatabase database,
                                   ExternalTable targetTable,
                                   List<Column> cols,
                                   List<NamedExpression> outputExprs,
                                   boolean requireMergeCardinalityCheck,
                                   Optional<GroupExpression> groupExpression,
                                   Optional<LogicalProperties> logicalProperties,
                                   CHILD_TYPE child) {
        this(database, targetTable, null, cols, outputExprs, requireMergeCardinalityCheck,
                groupExpression, logicalProperties, child);
    }

    /** Builds a row-level sink bound to the same remote generation as its target columns. */
    public LogicalExternalRowLevelMergeSink(ExternalDatabase database,
                                   ExternalTable targetTable,
                                   String boundWriteMetadataIdentity,
                                   List<Column> cols,
                                   List<NamedExpression> outputExprs,
                                   boolean requireMergeCardinalityCheck,
                                   Optional<GroupExpression> groupExpression,
                                   Optional<LogicalProperties> logicalProperties,
                                   CHILD_TYPE child) {
        super(PlanType.LOGICAL_EXTERNAL_ROW_LEVEL_MERGE_SINK, outputExprs, groupExpression, logicalProperties,
                cols, child);
        this.database = Objects.requireNonNull(database,
                "database != null in LogicalExternalRowLevelMergeSink");
        this.targetTable = Objects.requireNonNull(targetTable,
                "targetTable != null in LogicalExternalRowLevelMergeSink");
        this.boundWriteMetadataIdentity = boundWriteMetadataIdentity;
        this.requireMergeCardinalityCheck = requireMergeCardinalityCheck;
    }

    public Plan withChildAndUpdateOutput(Plan child) {
        List<NamedExpression> output = child.getOutput().stream()
                .map(NamedExpression.class::cast)
                .collect(ImmutableList.toImmutableList());
        return new LogicalExternalRowLevelMergeSink<>(database, targetTable, boundWriteMetadataIdentity, cols, output,
                requireMergeCardinalityCheck, Optional.empty(), Optional.empty(), child);
    }

    @Override
    public Plan withChildren(List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1, "LogicalExternalRowLevelMergeSink only accepts one child");
        return new LogicalExternalRowLevelMergeSink<>(database, targetTable, boundWriteMetadataIdentity,
                cols, outputExprs,
                requireMergeCardinalityCheck, Optional.empty(), Optional.empty(), children.get(0));
    }

    public LogicalExternalRowLevelMergeSink<CHILD_TYPE> withOutputExprs(List<NamedExpression> outputExprs) {
        return new LogicalExternalRowLevelMergeSink<>(database, targetTable, boundWriteMetadataIdentity,
                cols, outputExprs,
                requireMergeCardinalityCheck, Optional.empty(), Optional.empty(), child());
    }

    public ExternalDatabase getDatabase() {
        return database;
    }

    public ExternalTable getTargetTable() {
        return targetTable;
    }

    public String getBoundWriteMetadataIdentity() {
        return boundWriteMetadataIdentity;
    }

    public boolean isRequireMergeCardinalityCheck() {
        return requireMergeCardinalityCheck;
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
        LogicalExternalRowLevelMergeSink<?> that = (LogicalExternalRowLevelMergeSink<?>) o;
        return Objects.equals(database, that.database)
                && Objects.equals(targetTable, that.targetTable)
                && Objects.equals(boundWriteMetadataIdentity, that.boundWriteMetadataIdentity)
                && Objects.equals(cols, that.cols)
                && requireMergeCardinalityCheck == that.requireMergeCardinalityCheck;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), database, targetTable, boundWriteMetadataIdentity, cols,
                requireMergeCardinalityCheck);
    }

    @Override
    public String toString() {
        return Utils.toSqlString("LogicalExternalRowLevelMergeSink[" + id.asInt() + "]",
                "outputExprs", outputExprs,
                "database", database.getFullName(),
                "targetTable", targetTable.getName(),
                "cols", cols,
                "requireMergeCardinalityCheck", requireMergeCardinalityCheck);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalExternalRowLevelMergeSink(this, context);
    }

    @Override
    public Plan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new LogicalExternalRowLevelMergeSink<>(database, targetTable, boundWriteMetadataIdentity,
                cols, outputExprs,
                requireMergeCardinalityCheck, groupExpression, Optional.of(getLogicalProperties()), child());
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        return new LogicalExternalRowLevelMergeSink<>(database, targetTable, boundWriteMetadataIdentity,
                cols, outputExprs,
                requireMergeCardinalityCheck, groupExpression, logicalProperties, children.get(0));
    }
}
