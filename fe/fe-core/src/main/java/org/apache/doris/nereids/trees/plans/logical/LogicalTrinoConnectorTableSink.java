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
import org.apache.doris.datasource.trinoconnector.TrinoConnectorExternalDatabase;
import org.apache.doris.datasource.trinoconnector.TrinoConnectorExternalTable;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.info.DMLCommandType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * logical trino connector table sink for insert command
 */
public class LogicalTrinoConnectorTableSink<CHILD_TYPE extends Plan> extends LogicalTableSink<CHILD_TYPE> {
    // bound data sink
    private final TrinoConnectorExternalDatabase database;
    private final TrinoConnectorExternalTable targetTable;
    private final DMLCommandType dmlCommandType;

    /**
     * constructor
     */
    public LogicalTrinoConnectorTableSink(TrinoConnectorExternalDatabase database,
                                          TrinoConnectorExternalTable targetTable,
                                          List<Column> cols,
                                          List<NamedExpression> outputExprs,
                                          DMLCommandType dmlCommandType,
                                          Optional<GroupExpression> groupExpression,
                                          Optional<LogicalProperties> logicalProperties,
                                          CHILD_TYPE child) {
        super(PlanType.LOGICAL_TRINO_CONNECTOR_TABLE_SINK, outputExprs, groupExpression, logicalProperties, cols,
                child);
        this.database = Objects.requireNonNull(database, "database != null in LogicalTrinoConnectorTableSink");
        this.targetTable = Objects.requireNonNull(targetTable, "targetTable != null in LogicalTrinoConnectorTableSink");
        this.dmlCommandType = dmlCommandType;
    }

    public Plan withChildAndUpdateOutput(Plan child) {
        List<NamedExpression> output = child.getOutput().stream()
                .map(NamedExpression.class::cast)
                .collect(ImmutableList.toImmutableList());
        return new LogicalTrinoConnectorTableSink<>(database, targetTable, cols, output,
                dmlCommandType, Optional.empty(), Optional.empty(), child);
    }

    @Override
    public Plan withChildren(List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1, "LogicalTrinoConnectorTableSink only accepts one child");
        return new LogicalTrinoConnectorTableSink<>(database, targetTable, cols, outputExprs,
                dmlCommandType, Optional.empty(), Optional.empty(), children.get(0));
    }

    @Override
    public LogicalSink<CHILD_TYPE> withOutputExprs(List<NamedExpression> outputExprs) {
        return new LogicalTrinoConnectorTableSink<>(database, targetTable, cols, outputExprs,
                dmlCommandType, Optional.empty(), Optional.empty(), child());
    }

    public TrinoConnectorExternalDatabase getDatabase() {
        return database;
    }

    public TrinoConnectorExternalTable getTargetTable() {
        return targetTable;
    }

    public DMLCommandType getDmlCommandType() {
        return dmlCommandType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof LogicalTrinoConnectorTableSink)) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        LogicalTrinoConnectorTableSink<?> that = (LogicalTrinoConnectorTableSink<?>) o;
        return dmlCommandType == that.dmlCommandType
                && Objects.equals(database, that.database)
                && Objects.equals(targetTable, that.targetTable) && Objects.equals(cols, that.cols);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), database, targetTable, dmlCommandType);
    }

    @Override
    public String toString() {
        return Utils.toSqlString("LogicalTrinoConnectorTableSink[" + id.asInt() + "]",
                "outputExprs", outputExprs,
                "database", database.getFullName(),
                "targetTable", targetTable.getName(),
                "cols", cols,
                "dmlCommandType", dmlCommandType
        );
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalTrinoConnectorTableSink(this, context);
    }

    @Override
    public Plan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new LogicalTrinoConnectorTableSink<>(database, targetTable, cols, outputExprs,
                dmlCommandType, groupExpression, Optional.of(getLogicalProperties()), child());
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        return new LogicalTrinoConnectorTableSink<>(database, targetTable, cols, outputExprs,
                dmlCommandType, groupExpression, logicalProperties, children.get(0));
    }
}
