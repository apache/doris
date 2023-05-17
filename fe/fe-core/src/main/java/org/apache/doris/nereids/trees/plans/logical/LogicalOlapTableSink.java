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
import org.apache.doris.nereids.properties.DistributionSpecHash;
import org.apache.doris.nereids.properties.DistributionSpecHash.ShuffleType;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * logical olap table sink for insert command
 */
public class LogicalOlapTableSink<CHILD_TYPE extends Plan> extends LogicalUnary<CHILD_TYPE> {
    // bound data sink
    private Database database;
    private OlapTable targetTable;
    private List<Long> partitionIds;
    private List<Column> cols;

    public LogicalOlapTableSink(Database database, OlapTable targetTable, List<Column> cols, List<Long> partitionIds,
            CHILD_TYPE child) {
        this(database, targetTable, cols, partitionIds, Optional.empty(), Optional.empty(), child);
    }

    public LogicalOlapTableSink(Database database, OlapTable targetTable, List<Column> cols, List<Long> partitionIds,
            Optional<GroupExpression> groupExpression, Optional<LogicalProperties> logicalProperties,
            CHILD_TYPE child) {
        super(PlanType.LOGICAL_OLAP_TABLE_SINK, groupExpression, logicalProperties, child);
        this.database = Preconditions.checkNotNull(database, "database != null in LogicalOlapTableSink");
        this.targetTable = Preconditions.checkNotNull(targetTable, "targetTable != null in LogicalOlapTableSink");
        this.partitionIds = partitionIds;
    }

    @Override
    public Plan withChildren(List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1, "LogicalOlapTableSink only accepts one child");
        return new LogicalOlapTableSink<>(database, targetTable, cols, partitionIds, groupExpression,
                Optional.of(getLogicalProperties()), children.get(0));
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

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LogicalOlapTableSink<?> sink = (LogicalOlapTableSink<?>) o;
        return Objects.equals(database, sink.database)
                && Objects.equals(targetTable, sink.targetTable)
                && Objects.equals(partitionIds, sink.partitionIds)
                && Objects.equals(cols, sink.cols);
    }

    @Override
    public int hashCode() {
        return Objects.hash(database, targetTable, partitionIds, cols);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalOlapTableSink(this, context);
    }

    @Override
    public List<? extends Expression> getExpressions() {
        return ImmutableList.of();
    }

    @Override
    public Plan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new LogicalOlapTableSink<>(database, targetTable, cols, partitionIds, groupExpression,
                Optional.of(getLogicalProperties()), child());
    }

    @Override
    public Plan withLogicalProperties(Optional<LogicalProperties> logicalProperties) {
        return new LogicalOlapTableSink<>(database, targetTable, cols, partitionIds, groupExpression,
                logicalProperties, child());
    }

    @Override
    public List<Slot> computeOutput() {
        return child().computeOutput();
    }

    /**
     * calculate PhysicalProperties.
     */
    public PhysicalProperties calculatePhysicalProperties() {
        // it will be used at set physical properties.

        List<ExprId> exprIds = getOutput().subList(0, targetTable.getKeysNum()).stream()
                .map(NamedExpression::getExprId).collect(Collectors.toList());
        return PhysicalProperties.createHash(new DistributionSpecHash(exprIds, ShuffleType.NATURAL));
    }
}
