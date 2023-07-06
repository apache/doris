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
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.ObjectId;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.statistics.Statistics;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Physical file scan for external catalog.
 */
public class PhysicalFileScan extends PhysicalRelation {

    private final ExternalTable table;
    private final DistributionSpec distributionSpec;
    private final Set<Expression> conjuncts;

    /**
     * Constructor for PhysicalFileScan.
     */
    public PhysicalFileScan(ObjectId id, ExternalTable table, List<String> qualifier,
                            DistributionSpec distributionSpec, Optional<GroupExpression> groupExpression,
                            LogicalProperties logicalProperties, Set<Expression> conjuncts) {
        super(id, PlanType.PHYSICAL_FILE_SCAN, qualifier, groupExpression, logicalProperties);
        this.table = table;
        this.distributionSpec = distributionSpec;
        this.conjuncts = conjuncts;
    }

    /**
     * Constructor for PhysicalFileScan.
     */
    public PhysicalFileScan(ObjectId id, ExternalTable table, List<String> qualifier,
                            DistributionSpec distributionSpec, Optional<GroupExpression> groupExpression,
                            LogicalProperties logicalProperties, PhysicalProperties physicalProperties,
                            Statistics statistics, Set<Expression> conjuncts) {
        super(id, PlanType.PHYSICAL_FILE_SCAN, qualifier, groupExpression, logicalProperties,
                physicalProperties, statistics);
        this.table = table;
        this.distributionSpec = distributionSpec;
        this.conjuncts = conjuncts;
    }

    @Override
    public String toString() {
        return Utils.toSqlString("PhysicalFileScan",
            "qualified", Utils.qualifiedName(qualifier, table.getName()),
            "output", getOutput(),
            "stats", statistics
        );
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass() || !super.equals(o)) {
            return false;
        }
        PhysicalFileScan that = ((PhysicalFileScan) o);
        return Objects.equals(table, that.table);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, table);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalFileScan(this, context);
    }

    @Override
    public PhysicalFileScan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new PhysicalFileScan(id, table, qualifier, distributionSpec,
            groupExpression, getLogicalProperties(), conjuncts);
    }

    @Override
    public PhysicalFileScan withLogicalProperties(Optional<LogicalProperties> logicalProperties) {
        return new PhysicalFileScan(id, table, qualifier, distributionSpec,
            groupExpression, logicalProperties.get(), conjuncts);
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        return new PhysicalFileScan(id, table, qualifier, distributionSpec,
                groupExpression, logicalProperties.get(), conjuncts);
    }

    @Override
    public ExternalTable getTable() {
        return table;
    }

    @Override
    public PhysicalFileScan withPhysicalPropertiesAndStats(PhysicalProperties physicalProperties,
                                                       Statistics statistics) {
        return new PhysicalFileScan(id, table, qualifier, distributionSpec, groupExpression, getLogicalProperties(),
            physicalProperties, statistics, conjuncts);
    }

    public Set<Expression> getConjuncts() {
        return this.conjuncts;
    }
}
