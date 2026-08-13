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
import org.apache.doris.datasource.ExternalWriteDistributionPlan;
import org.apache.doris.datasource.iceberg.IcebergExternalDatabase;
import org.apache.doris.datasource.iceberg.IcebergExternalTable;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.DistributionSpecHash.ShuffleType;
import org.apache.doris.nereids.properties.DistributionSpecHiveTableSinkHashPartitioned;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.statistics.Statistics;

import org.apache.iceberg.Table;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** physical iceberg sink */
public class PhysicalIcebergTableSink<CHILD_TYPE extends Plan> extends PhysicalBaseExternalTableSink<CHILD_TYPE> {
    private final Table targetIcebergTable;
    private final ExternalWriteDistributionPlan writeDistributionPlan;

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
                ExternalWriteDistributionPlan.singleWriter("Iceberg write distribution is not planned"),
                PhysicalProperties.GATHER, null, child);
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
                                    ExternalWriteDistributionPlan writeDistributionPlan,
                                    PhysicalProperties physicalProperties,
                                    Statistics statistics,
                                    CHILD_TYPE child) {
        super(PlanType.PHYSICAL_ICEBERG_TABLE_SINK, database, targetTable, cols, outputExprs, groupExpression,
                logicalProperties, physicalProperties, statistics, child);
        this.targetIcebergTable = Objects.requireNonNull(
                targetIcebergTable, "targetIcebergTable != null in PhysicalIcebergTableSink");
        this.writeDistributionPlan = Objects.requireNonNull(
                writeDistributionPlan, "writeDistributionPlan != null in PhysicalIcebergTableSink");
    }

    @Override
    public Plan withChildren(List<Plan> children) {
        return new PhysicalIcebergTableSink<>(
                (IcebergExternalDatabase) database, (IcebergExternalTable) targetTable,
                targetIcebergTable, cols, outputExprs, groupExpression,
                getLogicalProperties(), writeDistributionPlan,
                physicalProperties, statistics, children.get(0));
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalIcebergTableSink(this, context);
    }

    @Override
    public Plan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new PhysicalIcebergTableSink<>(
                (IcebergExternalDatabase) database, (IcebergExternalTable) targetTable,
                targetIcebergTable, cols, outputExprs, groupExpression, getLogicalProperties(),
                writeDistributionPlan, PhysicalProperties.GATHER, null, child());
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
                                                 Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        return new PhysicalIcebergTableSink<>(
                (IcebergExternalDatabase) database, (IcebergExternalTable) targetTable,
                targetIcebergTable, cols, outputExprs, groupExpression, logicalProperties.get(),
                writeDistributionPlan, PhysicalProperties.GATHER, null, children.get(0));
    }

    @Override
    public PhysicalPlan withPhysicalPropertiesAndStats(PhysicalProperties physicalProperties, Statistics statistics) {
        return new PhysicalIcebergTableSink<>(
                (IcebergExternalDatabase) database, (IcebergExternalTable) targetTable,
                targetIcebergTable, cols, outputExprs, groupExpression, getLogicalProperties(),
                writeDistributionPlan, physicalProperties, statistics, child());
    }

    public Table getTargetIcebergTable() {
        return targetIcebergTable;
    }

    public ExternalWriteDistributionPlan getWriteDistributionPlan() {
        return writeDistributionPlan;
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

        if (writeDistributionPlan.isSingleWriter()) {
            return PhysicalProperties.GATHER;
        }
        if (writeDistributionPlan.isRandom()) {
            return PhysicalProperties.SINK_RANDOM_PARTITIONED;
        }
        if (writeDistributionPlan.isAdaptiveHash()) {
            DistributionSpecHiveTableSinkHashPartitioned distributionSpec =
                    new DistributionSpecHiveTableSinkHashPartitioned();
            distributionSpec.setOutputColExprIds(writeDistributionPlan.getRoutingExprIds());
            return new PhysicalProperties(distributionSpec);
        }
        return PhysicalProperties.createHash(
                writeDistributionPlan.getRoutingExprIds(), ShuffleType.REQUIRE);
    }
}
