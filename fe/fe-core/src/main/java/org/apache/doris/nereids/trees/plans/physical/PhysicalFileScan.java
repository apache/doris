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

import org.apache.doris.analysis.TableSnapshot;
import org.apache.doris.datasource.ExternalTable;
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

import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Physical file scan for external catalog.
 */
public class PhysicalFileScan extends PhysicalCatalogRelation {

    protected final DistributionSpec distributionSpec;
    protected final Set<Expression> conjuncts;
    protected final SelectedPartitions selectedPartitions;
    protected final Optional<TableSample> tableSample;
    protected final Optional<TableSnapshot> tableSnapshot;

    /**
     * Constructor for PhysicalFileScan.
     */
    public PhysicalFileScan(RelationId id, ExternalTable table, List<String> qualifier,
            DistributionSpec distributionSpec, Optional<GroupExpression> groupExpression,
            LogicalProperties logicalProperties, Set<Expression> conjuncts,
            SelectedPartitions selectedPartitions, Optional<TableSample> tableSample,
            Optional<TableSnapshot> tableSnapshot) {
        this(id, PlanType.PHYSICAL_FILE_SCAN, table, qualifier, distributionSpec, groupExpression,
                logicalProperties, conjuncts, selectedPartitions, tableSample, tableSnapshot);
    }

    /**
     * Constructor for PhysicalFileScan.
     */
    public PhysicalFileScan(RelationId id, ExternalTable table, List<String> qualifier,
            DistributionSpec distributionSpec, Optional<GroupExpression> groupExpression,
            LogicalProperties logicalProperties, PhysicalProperties physicalProperties,
            Statistics statistics, Set<Expression> conjuncts, SelectedPartitions selectedPartitions,
            Optional<TableSample> tableSample, Optional<TableSnapshot> tableSnapshot) {
        this(id, PlanType.PHYSICAL_FILE_SCAN, table, qualifier, distributionSpec, groupExpression,
                logicalProperties, physicalProperties, statistics, conjuncts, selectedPartitions, tableSample,
                tableSnapshot);
    }

    /**
     * For hudi file scan to specified PlanTye
     */
    protected PhysicalFileScan(RelationId id, PlanType type, ExternalTable table, List<String> qualifier,
            DistributionSpec distributionSpec, Optional<GroupExpression> groupExpression,
            LogicalProperties logicalProperties, Set<Expression> conjuncts,
            SelectedPartitions selectedPartitions, Optional<TableSample> tableSample,
            Optional<TableSnapshot> tableSnapshot) {
        super(id, type, table, qualifier, groupExpression, logicalProperties);
        this.distributionSpec = distributionSpec;
        this.conjuncts = conjuncts;
        this.selectedPartitions = selectedPartitions;
        this.tableSample = tableSample;
        this.tableSnapshot = tableSnapshot;
    }

    protected PhysicalFileScan(RelationId id, PlanType type, ExternalTable table, List<String> qualifier,
            DistributionSpec distributionSpec, Optional<GroupExpression> groupExpression,
            LogicalProperties logicalProperties, PhysicalProperties physicalProperties,
            Statistics statistics, Set<Expression> conjuncts, SelectedPartitions selectedPartitions,
            Optional<TableSample> tableSample, Optional<TableSnapshot> tableSnapshot) {
        super(id, type, table, qualifier, groupExpression, logicalProperties,
                physicalProperties, statistics);
        this.distributionSpec = distributionSpec;
        this.conjuncts = conjuncts;
        this.selectedPartitions = selectedPartitions;
        this.tableSample = tableSample;
        this.tableSnapshot = tableSnapshot;
    }

    public DistributionSpec getDistributionSpec() {
        return distributionSpec;
    }

    public Set<Expression> getConjuncts() {
        return conjuncts;
    }

    public SelectedPartitions getSelectedPartitions() {
        return selectedPartitions;
    }

    public Optional<TableSample> getTableSample() {
        return tableSample;
    }

    public Optional<TableSnapshot> getTableSnapshot() {
        return tableSnapshot;
    }

    @Override
    public String toString() {
        return Utils.toSqlString("PhysicalFileScan[" + table.getName() + "]" + getGroupIdWithPrefix(),
            "stats", statistics,
                "qualified", Utils.qualifiedName(qualifier, table.getName()),
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
                groupExpression, getLogicalProperties(), conjuncts, selectedPartitions, tableSample, tableSnapshot);
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        return new PhysicalFileScan(relationId, getTable(), qualifier, distributionSpec,
                groupExpression, logicalProperties.get(), conjuncts, selectedPartitions, tableSample, tableSnapshot);
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
                selectedPartitions, tableSample, tableSnapshot);
    }
}
