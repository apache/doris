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
import org.apache.doris.datasource.ExternalDatabase;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.PluginDrivenExternalTable;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.plans.AbstractPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.statistics.Statistics;

import java.util.List;
import java.util.Optional;

/**
 * Physical table sink for plugin-driven connector catalogs.
 */
public class PhysicalConnectorTableSink<CHILD_TYPE extends Plan> extends PhysicalBaseExternalTableSink<CHILD_TYPE> {

    /**
     * constructor
     */
    public PhysicalConnectorTableSink(ExternalDatabase database,
                                      ExternalTable targetTable,
                                      List<Column> cols,
                                      List<NamedExpression> outputExprs,
                                      Optional<GroupExpression> groupExpression,
                                      LogicalProperties logicalProperties,
                                      CHILD_TYPE child) {
        this(database, targetTable, cols, outputExprs, groupExpression, logicalProperties,
                PhysicalProperties.GATHER, null, child);
    }

    /**
     * constructor
     */
    public PhysicalConnectorTableSink(ExternalDatabase database,
                                      ExternalTable targetTable,
                                      List<Column> cols,
                                      List<NamedExpression> outputExprs,
                                      Optional<GroupExpression> groupExpression,
                                      LogicalProperties logicalProperties,
                                      PhysicalProperties physicalProperties,
                                      Statistics statistics,
                                      CHILD_TYPE child) {
        super(PlanType.PHYSICAL_CONNECTOR_TABLE_SINK, database, targetTable, cols, outputExprs, groupExpression,
                logicalProperties, physicalProperties, statistics, child);
    }

    @Override
    public Plan withChildren(List<Plan> children) {
        return AbstractPlan.copyWithSameId(this, () -> new PhysicalConnectorTableSink<>(
                (ExternalDatabase) database, (ExternalTable) targetTable, cols, outputExprs, groupExpression,
                getLogicalProperties(), physicalProperties, statistics, children.get(0)));
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalConnectorTableSink(this, context);
    }

    @Override
    public Plan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return AbstractPlan.copyWithSameId(this, () -> new PhysicalConnectorTableSink<>(
                (ExternalDatabase) database, (ExternalTable) targetTable, cols, outputExprs,
                groupExpression, getLogicalProperties(), child()));
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
                                                 Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        return AbstractPlan.copyWithSameId(this, () -> new PhysicalConnectorTableSink<>(
                (ExternalDatabase) database, (ExternalTable) targetTable, cols, outputExprs,
                groupExpression, logicalProperties.get(), children.get(0)));
    }

    @Override
    public PhysicalPlan withPhysicalPropertiesAndStats(PhysicalProperties physicalProperties, Statistics statistics) {
        return AbstractPlan.copyWithSameId(this, () -> new PhysicalConnectorTableSink<>(
                (ExternalDatabase) database, (ExternalTable) targetTable, cols, outputExprs,
                groupExpression, getLogicalProperties(), physicalProperties, statistics, child()));
    }

    /**
     * Get required physical properties for sink distribution.
     *
     * <p>Connectors that declare {@code SUPPORTS_PARALLEL_WRITE} capability
     * (e.g., Hive, Iceberg) use random partitioned distribution for parallel writers.
     * All other connectors (e.g., JDBC, ES) default to GATHER (single writer)
     * for transactional safety.</p>
     */
    @Override
    public PhysicalProperties getRequirePhysicalProperties() {
        if (targetTable instanceof PluginDrivenExternalTable
                && ((PluginDrivenExternalTable) targetTable).supportsParallelWrite()) {
            return PhysicalProperties.SINK_RANDOM_PARTITIONED;
        }
        return PhysicalProperties.GATHER;
    }
}
