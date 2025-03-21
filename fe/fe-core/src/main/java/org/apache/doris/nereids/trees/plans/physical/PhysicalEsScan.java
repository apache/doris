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

import org.apache.doris.catalog.TableIf;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.DistributionSpec;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.statistics.Statistics;

import java.util.List;
import java.util.Optional;

/**
 * Physical es scan for external catalog.
 */
public class PhysicalEsScan extends PhysicalCatalogRelation {

    private final DistributionSpec distributionSpec;

    /**
     * Constructor for PhysicalEsScan.
     */
    public PhysicalEsScan(RelationId id, TableIf table, List<String> qualifier,
            DistributionSpec distributionSpec, Optional<GroupExpression> groupExpression,
            LogicalProperties logicalProperties) {
        super(id, PlanType.PHYSICAL_ES_SCAN, table, qualifier, groupExpression, logicalProperties);
        this.distributionSpec = distributionSpec;
    }

    /**
     * Constructor for PhysicalEsScan.
     */
    public PhysicalEsScan(RelationId id, TableIf table, List<String> qualifier,
            DistributionSpec distributionSpec, Optional<GroupExpression> groupExpression,
            LogicalProperties logicalProperties, PhysicalProperties physicalProperties, Statistics statistics) {
        super(id, PlanType.PHYSICAL_ES_SCAN, table, qualifier, groupExpression, logicalProperties,
                physicalProperties, statistics);
        this.distributionSpec = distributionSpec;
    }

    @Override
    public String toString() {
        return Utils.toSqlString("PhysicalEsScan",
            "qualified", Utils.qualifiedName(qualifier, table.getName()),
            "output", getOutput(),
            "stats", statistics
        );
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalEsScan(this, context);
    }

    @Override
    public PhysicalEsScan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new PhysicalEsScan(relationId, getTable(), qualifier, distributionSpec,
                groupExpression, getLogicalProperties());
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        return new PhysicalEsScan(relationId, getTable(), qualifier, distributionSpec,
                groupExpression, logicalProperties.get());
    }

    @Override
    public PhysicalEsScan withPhysicalPropertiesAndStats(PhysicalProperties physicalProperties,
                                                           Statistics statsDeriveResult) {
        return new PhysicalEsScan(relationId, getTable(), qualifier, distributionSpec,
                groupExpression, getLogicalProperties(), physicalProperties, statsDeriveResult);
    }
}
