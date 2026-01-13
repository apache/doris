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
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.DistributionSpecHash.ShuffleType;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.delete.DeleteCommandContext;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.statistics.Statistics;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Physical Iceberg Delete Sink for DELETE operations.
 * This sink is responsible for writing position delete files.
 */
public class PhysicalIcebergDeleteSink<CHILD_TYPE extends Plan> extends PhysicalBaseExternalTableSink<CHILD_TYPE> {
    private final DeleteCommandContext deleteContext;

    /**
     * Constructor
     */
    public PhysicalIcebergDeleteSink(IcebergExternalDatabase database,
                                    IcebergExternalTable targetTable,
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
    public PhysicalIcebergDeleteSink(IcebergExternalDatabase database,
                                    IcebergExternalTable targetTable,
                                    List<Column> cols,
                                    List<NamedExpression> outputExprs,
                                    DeleteCommandContext deleteContext,
                                    Optional<GroupExpression> groupExpression,
                                    LogicalProperties logicalProperties,
                                    PhysicalProperties physicalProperties,
                                    Statistics statistics,
                                    CHILD_TYPE child) {
        super(PlanType.PHYSICAL_ICEBERG_DELETE_SINK, database, targetTable, cols, outputExprs, groupExpression,
                logicalProperties, physicalProperties, statistics, child);
        this.deleteContext = Objects.requireNonNull(
                deleteContext, "deleteContext != null in PhysicalIcebergDeleteSink");
    }

    public DeleteCommandContext getDeleteContext() {
        return deleteContext;
    }

    @Override
    public Plan withChildren(List<Plan> children) {
        return new PhysicalIcebergDeleteSink<>(
                (IcebergExternalDatabase) database, (IcebergExternalTable) targetTable,
                cols, outputExprs, deleteContext, groupExpression,
                getLogicalProperties(), physicalProperties, statistics, children.get(0));
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalIcebergDeleteSink(this, context);
    }

    @Override
    public Plan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new PhysicalIcebergDeleteSink<>(
                (IcebergExternalDatabase) database, (IcebergExternalTable) targetTable, cols, outputExprs,
                deleteContext, groupExpression, getLogicalProperties(), child());
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
                                                 Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        return new PhysicalIcebergDeleteSink<>(
                (IcebergExternalDatabase) database, (IcebergExternalTable) targetTable, cols, outputExprs,
                deleteContext, groupExpression, logicalProperties.get(), children.get(0));
    }

    @Override
    public PhysicalPlan withPhysicalPropertiesAndStats(PhysicalProperties physicalProperties, Statistics statistics) {
        return new PhysicalIcebergDeleteSink<>(
                (IcebergExternalDatabase) database, (IcebergExternalTable) targetTable, cols, outputExprs,
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
        PhysicalIcebergDeleteSink<?> that = (PhysicalIcebergDeleteSink<?>) o;
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
        for (Slot slot : child().getOutput()) {
            String name = slot.getName();
            if (rowIdExprId == null && Column.ICEBERG_ROWID_COL.equalsIgnoreCase(name)) {
                rowIdExprId = slot.getExprId();
            }
        }

        if (rowIdExprId != null) {
            return PhysicalProperties.createHash(ImmutableList.of(rowIdExprId), ShuffleType.REQUIRE);
        }
        return PhysicalProperties.GATHER;
    }
}
