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
import org.apache.doris.nereids.trees.plans.ObjectId;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.statistics.StatsDeriveResult;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Physical jdbc scan for external catalog.
 */
public class PhysicalJdbcScan extends PhysicalRelation {

    private final ExternalTable table;
    private final DistributionSpec distributionSpec;

    /**
     * Constructor for PhysicalJdbcScan.
     */
    public PhysicalJdbcScan(ObjectId id, ExternalTable table, List<String> qualifier,
                            DistributionSpec distributionSpec, Optional<GroupExpression> groupExpression,
                            LogicalProperties logicalProperties) {
        super(id, PlanType.PHYSICAL_JDBC_SCAN, qualifier, groupExpression, logicalProperties);
        this.table = table;
        this.distributionSpec = distributionSpec;
    }

    /**
     * Constructor for PhysicalJdbcScan.
     */
    public PhysicalJdbcScan(ObjectId id, ExternalTable table, List<String> qualifier,
                            DistributionSpec distributionSpec, Optional<GroupExpression> groupExpression,
                            LogicalProperties logicalProperties, PhysicalProperties physicalProperties,
                            StatsDeriveResult statsDeriveResult) {
        super(id, PlanType.PHYSICAL_JDBC_SCAN, qualifier, groupExpression, logicalProperties,
                physicalProperties, statsDeriveResult);
        this.table = table;
        this.distributionSpec = distributionSpec;
    }

    @Override
    public String toString() {
        return Utils.toSqlString("PhysicalJdbcScan",
            "qualified", Utils.qualifiedName(qualifier, table.getName()),
            "output", getOutput(),
            "stats", statsDeriveResult
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
        PhysicalJdbcScan that = ((PhysicalJdbcScan) o);
        return Objects.equals(table, that.table);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, table);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalJdbcScan(this, context);
    }

    @Override
    public PhysicalJdbcScan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new PhysicalJdbcScan(id, table, qualifier, distributionSpec, groupExpression, getLogicalProperties());
    }

    @Override
    public PhysicalJdbcScan withLogicalProperties(Optional<LogicalProperties> logicalProperties) {
        return new PhysicalJdbcScan(id, table, qualifier, distributionSpec, groupExpression, logicalProperties.get());
    }

    @Override
    public ExternalTable getTable() {
        return table;
    }

    @Override
    public PhysicalJdbcScan withPhysicalPropertiesAndStats(PhysicalProperties physicalProperties,
                                                           StatsDeriveResult statsDeriveResult) {
        return new PhysicalJdbcScan(id, table, qualifier, distributionSpec, groupExpression, getLogicalProperties(),
            physicalProperties, statsDeriveResult);
    }
}
