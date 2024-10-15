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

import org.apache.doris.analysis.TableScanParams;
import org.apache.doris.analysis.TableSnapshot;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.hudi.source.IncrementalRelation;
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
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Physical Hudi scan for Hudi table.
 */
public class PhysicalHudiScan extends PhysicalFileScan {

    // for hudi incremental read
    private final Optional<TableScanParams> scanParams;
    private final Optional<IncrementalRelation> incrementalRelation;

    /**
     * Constructor for PhysicalHudiScan.
     */
    public PhysicalHudiScan(RelationId id, ExternalTable table, List<String> qualifier,
            DistributionSpec distributionSpec, Optional<GroupExpression> groupExpression,
            LogicalProperties logicalProperties, Set<Expression> conjuncts,
            SelectedPartitions selectedPartitions, Optional<TableSample> tableSample,
            Optional<TableSnapshot> tableSnapshot,
            Optional<TableScanParams> scanParams, Optional<IncrementalRelation> incrementalRelation) {
        super(id, PlanType.PHYSICAL_HUDI_SCAN, table, qualifier, distributionSpec, groupExpression, logicalProperties,
                conjuncts, selectedPartitions, tableSample, tableSnapshot);
        Objects.requireNonNull(scanParams, "scanParams should not null");
        Objects.requireNonNull(incrementalRelation, "incrementalRelation should not null");
        this.scanParams = scanParams;
        this.incrementalRelation = incrementalRelation;
    }

    /**
     * Constructor for PhysicalHudiScan.
     */
    public PhysicalHudiScan(RelationId id, ExternalTable table, List<String> qualifier,
            DistributionSpec distributionSpec, Optional<GroupExpression> groupExpression,
            LogicalProperties logicalProperties, PhysicalProperties physicalProperties,
            Statistics statistics, Set<Expression> conjuncts, SelectedPartitions selectedPartitions,
            Optional<TableSample> tableSample, Optional<TableSnapshot> tableSnapshot,
            Optional<TableScanParams> scanParams, Optional<IncrementalRelation> incrementalRelation) {
        super(id, PlanType.PHYSICAL_HUDI_SCAN, table, qualifier, distributionSpec, groupExpression, logicalProperties,
                physicalProperties, statistics, conjuncts, selectedPartitions, tableSample, tableSnapshot);
        this.scanParams = scanParams;
        this.incrementalRelation = incrementalRelation;
    }

    public Optional<TableScanParams> getScanParams() {
        return scanParams;
    }

    public Optional<IncrementalRelation> getIncrementalRelation() {
        return incrementalRelation;
    }

    @Override
    public PhysicalHudiScan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new PhysicalHudiScan(relationId, getTable(), qualifier, distributionSpec,
                groupExpression, getLogicalProperties(), conjuncts, selectedPartitions, tableSample, tableSnapshot,
                scanParams, incrementalRelation);
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        return new PhysicalHudiScan(relationId, getTable(), qualifier, distributionSpec,
                groupExpression, logicalProperties.get(), conjuncts, selectedPartitions, tableSample, tableSnapshot,
                scanParams, incrementalRelation);
    }

    @Override
    public PhysicalHudiScan withPhysicalPropertiesAndStats(PhysicalProperties physicalProperties,
            Statistics statistics) {
        return new PhysicalHudiScan(relationId, getTable(), qualifier, distributionSpec,
                groupExpression, getLogicalProperties(), physicalProperties, statistics, conjuncts,
                selectedPartitions, tableSample, tableSnapshot,
                scanParams, incrementalRelation);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalHudiScan(this, context);
    }

    @Override
    public String toString() {
        return Utils.toSqlString("PhysicalHudiScan",
                "qualified", Utils.qualifiedName(qualifier, table.getName()),
                "output", getOutput(),
                "stats", statistics,
                "conjuncts", conjuncts,
                "selected partitions num",
                selectedPartitions.isPruned ? selectedPartitions.selectedPartitions.size() : "unknown",
                "isIncremental", incrementalRelation.isPresent()
        );
    }
}
