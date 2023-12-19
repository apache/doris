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
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.PropagateFuncDeps;
import org.apache.doris.nereids.trees.plans.algebra.Sink;
import org.apache.doris.nereids.trees.plans.commands.info.DMLCommandType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * logical olap table sink for insert command
 */
public class LogicalOlapTableSink<CHILD_TYPE extends Plan> extends LogicalSink<CHILD_TYPE>
        implements Sink, PropagateFuncDeps {
    // bound data sink
    private final Database database;
    private final OlapTable targetTable;
    private final List<Column> cols;
    private final List<Long> partitionIds;
    private final boolean isPartialUpdate;
    private final DMLCommandType dmlCommandType;

    public LogicalOlapTableSink(Database database, OlapTable targetTable, List<Column> cols, List<Long> partitionIds,
            List<NamedExpression> outputExprs, boolean isPartialUpdate, DMLCommandType dmlCommandType,
            CHILD_TYPE child) {
        this(database, targetTable, cols, partitionIds, outputExprs, isPartialUpdate, dmlCommandType,
                Optional.empty(), Optional.empty(), child);
    }

    /**
     * constructor
     */
    public LogicalOlapTableSink(Database database, OlapTable targetTable, List<Column> cols,
            List<Long> partitionIds, List<NamedExpression> outputExprs, boolean isPartialUpdate,
            DMLCommandType dmlCommandType, Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, CHILD_TYPE child) {
        super(PlanType.LOGICAL_OLAP_TABLE_SINK, outputExprs, groupExpression, logicalProperties, child);
        this.database = Objects.requireNonNull(database, "database != null in LogicalOlapTableSink");
        this.targetTable = Objects.requireNonNull(targetTable, "targetTable != null in LogicalOlapTableSink");
        this.cols = Utils.copyRequiredList(cols);
        this.isPartialUpdate = isPartialUpdate;
        this.dmlCommandType = dmlCommandType;
        this.partitionIds = Utils.copyRequiredList(partitionIds);
    }

    public Plan withChildAndUpdateOutput(Plan child) {
        List<NamedExpression> output = child.getOutput().stream()
                .map(NamedExpression.class::cast)
                .collect(ImmutableList.toImmutableList());
        return new LogicalOlapTableSink<>(database, targetTable, cols, partitionIds, output, isPartialUpdate,
                dmlCommandType, Optional.empty(), Optional.empty(), child);
    }

    @Override
    public Plan withChildren(List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1, "LogicalOlapTableSink only accepts one child");
        return new LogicalOlapTableSink<>(database, targetTable, cols, partitionIds, outputExprs, isPartialUpdate,
                dmlCommandType, Optional.empty(), Optional.empty(), children.get(0));
    }

    public Database getDatabase() {
        return database;
    }

    public OlapTable getTargetTable() {
        return targetTable;
    }

    public List<Column> getCols() {
        return cols;
    }

    public List<Long> getPartitionIds() {
        return partitionIds;
    }

    public boolean isPartialUpdate() {
        return isPartialUpdate;
    }

    public DMLCommandType getDmlCommandType() {
        return dmlCommandType;
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
        LogicalOlapTableSink<?> that = (LogicalOlapTableSink<?>) o;
        return isPartialUpdate == that.isPartialUpdate && dmlCommandType == that.dmlCommandType
                && Objects.equals(database, that.database)
                && Objects.equals(targetTable, that.targetTable) && Objects.equals(cols, that.cols)
                && Objects.equals(partitionIds, that.partitionIds);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), database, targetTable, cols, partitionIds,
                isPartialUpdate, dmlCommandType);
    }

    @Override
    public String toString() {
        return Utils.toSqlString("LogicalOlapTableSink[" + id.asInt() + "]",
                "outputExprs", outputExprs,
                "database", database.getFullName(),
                "targetTable", targetTable.getName(),
                "cols", cols,
                "partitionIds", partitionIds,
                "isPartialUpdate", isPartialUpdate,
                "dmlCommandType", dmlCommandType
        );
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalOlapTableSink(this, context);
    }

    @Override
    public Plan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new LogicalOlapTableSink<>(database, targetTable, cols, partitionIds, outputExprs, isPartialUpdate,
                dmlCommandType, groupExpression, Optional.of(getLogicalProperties()), child());
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        return new LogicalOlapTableSink<>(database, targetTable, cols, partitionIds, outputExprs, isPartialUpdate,
                dmlCommandType, groupExpression, logicalProperties, children.get(0));
    }
}
