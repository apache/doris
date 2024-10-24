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

package org.apache.doris.nereids.trees.plans.logical;

import org.apache.doris.catalog.TableIf;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * LogicalTestScan.
 *
 * only for ut
 */
public class LogicalTestScan extends LogicalCatalogRelation {
    public LogicalTestScan(RelationId relationId, TableIf table, List<String> qualifier) {
        this(relationId, table, qualifier, Optional.empty(), Optional.empty());
    }

    private LogicalTestScan(RelationId relationId, TableIf table, List<String> qualifier,
               Optional<GroupExpression> groupExpression, Optional<LogicalProperties> logicalProperties) {
        super(relationId, PlanType.LOGICAL_TEST_SCAN, table, qualifier, groupExpression, logicalProperties);
    }

    @Override
    public String toString() {
        return Utils.toSqlString("LogicalTestScan",
                "qualified", qualifiedName(),
                "output", getOutput()
        );
    }

    @Override
    public boolean equals(Object o) {
        return super.equals(o) && Objects.equals(table, ((LogicalTestScan) table).getTable());
    }

    @Override
    public Plan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new LogicalTestScan(relationId, table, qualifier,
                groupExpression, Optional.ofNullable(getLogicalProperties()));
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        return new LogicalTestScan(relationId, table, qualifier, groupExpression, logicalProperties);
    }

    @Override
    public LogicalTestScan withRelationId(RelationId relationId) {
        throw new RuntimeException("should not call LogicalTestScan's withRelationId method");
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalTestScan(this, context);
    }
}
