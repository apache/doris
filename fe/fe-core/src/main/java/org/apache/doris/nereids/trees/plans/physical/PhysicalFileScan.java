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

import org.apache.doris.catalog.external.ExternalTable;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.DistributionSpec;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.TableSample;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan.SelectedPartitions;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.statistics.Statistics;

import lombok.Getter;

import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Physical file scan for external catalog.
 */
public class PhysicalFileScan extends PhysicalCatalogRelation {

    private final DistributionSpec distributionSpec;
    @Getter
    private final Set<Expression> conjuncts;
    @Getter
    private final SelectedPartitions selectedPartitions;
    @Getter
    private final Optional<TableSample> tableSample;

    /**
     * Constructor for PhysicalFileScan.
     */
    public PhysicalFileScan(RelationId id, ExternalTable table, List<String> qualifier,
            DistributionSpec distributionSpec, Optional<GroupExpression> groupExpression,
            LogicalProperties logicalProperties, Set<Expression> conjuncts,
            SelectedPartitions selectedPartitions, Optional<TableSample> tableSample) {
        super(id, PlanType.PHYSICAL_FILE_SCAN, table, qualifier, groupExpression, logicalProperties);
        this.distributionSpec = distributionSpec;
        this.conjuncts = conjuncts;
        this.selectedPartitions = selectedPartitions;
        this.tableSample = tableSample;
    }

    /**
     * Constructor for PhysicalFileScan.
     */
    public PhysicalFileScan(RelationId id, ExternalTable table, List<String> qualifier,
            DistributionSpec distributionSpec, Optional<GroupExpression> groupExpression,
            LogicalProperties logicalProperties, PhysicalProperties physicalProperties,
            Statistics statistics, Set<Expression> conjuncts, SelectedPartitions selectedPartitions,
            Optional<TableSample> tableSample) {
        super(id, PlanType.PHYSICAL_FILE_SCAN, table, qualifier, groupExpression, logicalProperties,
                physicalProperties, statistics);
        this.distributionSpec = distributionSpec;
        this.conjuncts = conjuncts;
        this.selectedPartitions = selectedPartitions;
        this.tableSample = tableSample;
    }

    @Override
    public String toString() {
        return Utils.toSqlString("PhysicalFileScan",
                "qualified", Utils.qualifiedName(qualifier, table.getName()),
                "output", getOutput(),
                "stats", statistics,
                "conjuncts", conjuncts,
                "selected partitions num",
                selectedPartitions.isPruned ? selectedPartitions.selectedPartitions.size() : "unknown"
        );
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalFileScan(this, context);
    }

    @Override
    public PhysicalFileScan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new PhysicalFileScan(relationId, getTable(), qualifier, distributionSpec,
                groupExpression, getLogicalProperties(), conjuncts, selectedPartitions, tableSample);
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        return new PhysicalFileScan(relationId, getTable(), qualifier, distributionSpec,
                groupExpression, logicalProperties.get(), conjuncts, selectedPartitions, tableSample);
    }

    @Override
    public ExternalTable getTable() {
        return (ExternalTable) table;
    }

    @Override
    public PhysicalFileScan withPhysicalPropertiesAndStats(PhysicalProperties physicalProperties,
                                                       Statistics statistics) {
        return new PhysicalFileScan(relationId, getTable(), qualifier, distributionSpec,
                groupExpression, getLogicalProperties(), physicalProperties, statistics, conjuncts,
                selectedPartitions, tableSample);
    }
}
