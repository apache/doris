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
import org.apache.doris.datasource.paimon.PaimonExternalDatabase;
import org.apache.doris.datasource.paimon.PaimonWriteTarget;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.DistributionSpecHash.ShuffleType;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.statistics.Statistics;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Physical Paimon table sink.
 */
public class PhysicalPaimonTableSink<CHILD_TYPE extends Plan>
        extends PhysicalBaseExternalTableSink<CHILD_TYPE> {
    private final PaimonWriteTarget writeTarget;
    private final ExternalWriteDistributionPlan writeDistributionPlan;

    public PhysicalPaimonTableSink(PaimonExternalDatabase database,
                                    PaimonWriteTarget writeTarget,
                                    List<Column> cols,
                                    List<NamedExpression> outputExprs,
                                    Optional<GroupExpression> groupExpression,
                                    LogicalProperties logicalProperties,
                                    CHILD_TYPE child) {
        this(database, writeTarget, cols, outputExprs, groupExpression, logicalProperties,
                ExternalWriteDistributionPlan.singleWriter("Paimon write distribution is not planned"),
                PhysicalProperties.GATHER, null, child);
    }

    public PhysicalPaimonTableSink(PaimonExternalDatabase database,
                                    PaimonWriteTarget writeTarget,
                                    List<Column> cols,
                                    List<NamedExpression> outputExprs,
                                    Optional<GroupExpression> groupExpression,
                                    LogicalProperties logicalProperties,
                                    ExternalWriteDistributionPlan writeDistributionPlan,
                                    PhysicalProperties physicalProperties,
                                    Statistics statistics,
                                    CHILD_TYPE child) {
        super(PlanType.PHYSICAL_PAIMON_TABLE_SINK, database, writeTarget.getDorisTable(), cols, outputExprs,
                groupExpression, logicalProperties, physicalProperties, statistics, child);
        this.writeTarget = writeTarget;
        this.writeDistributionPlan = Objects.requireNonNull(
                writeDistributionPlan, "writeDistributionPlan != null");
    }

    @Override
    public Plan withChildren(List<Plan> children) {
        return new PhysicalPaimonTableSink<>(
                (PaimonExternalDatabase) database, writeTarget, cols, outputExprs, groupExpression,
                getLogicalProperties(), writeDistributionPlan,
                physicalProperties, statistics, children.get(0));
    }

    @Override
    public Plan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new PhysicalPaimonTableSink<>(
                (PaimonExternalDatabase) database, writeTarget, cols, outputExprs, groupExpression,
                getLogicalProperties(), writeDistributionPlan,
                physicalProperties, statistics, child());
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        return new PhysicalPaimonTableSink<>(
                (PaimonExternalDatabase) database, writeTarget, cols, outputExprs, groupExpression,
                logicalProperties.get(), writeDistributionPlan,
                physicalProperties, statistics, children.get(0));
    }

    @Override
    public PhysicalPaimonTableSink<Plan> withPhysicalPropertiesAndStats(
            PhysicalProperties physicalProperties, Statistics stats) {
        return new PhysicalPaimonTableSink<>(
                (PaimonExternalDatabase) database, writeTarget, cols, outputExprs, groupExpression,
                getLogicalProperties(), writeDistributionPlan, physicalProperties, stats, child());
    }

    @Override
    public PhysicalProperties getRequirePhysicalProperties() {
        if (writeDistributionPlan.isSingleWriter()) {
            return PhysicalProperties.GATHER;
        }
        return PhysicalProperties.createHash(
                writeDistributionPlan.getRoutingExprIds(), ShuffleType.REQUIRE);
    }

    public PaimonWriteTarget getWriteTarget() {
        return writeTarget;
    }

    public ExternalWriteDistributionPlan getWriteDistributionPlan() {
        return writeDistributionPlan;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalPaimonTableSink(this, context);
    }
}
