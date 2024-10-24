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

import org.apache.doris.catalog.JdbcTable;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;

import com.google.common.base.Preconditions;

import java.util.List;
import java.util.Optional;

/**
 * Logical scan for external jdbc catalog and jdbc table.
 */
public class LogicalJdbcScan extends LogicalCatalogRelation {

    /**
     * Constructor for LogicalJdbcScan.
     */
    public LogicalJdbcScan(RelationId id, TableIf table, List<String> qualifier,
                           Optional<GroupExpression> groupExpression,
                           Optional<LogicalProperties> logicalProperties) {
        super(id, PlanType.LOGICAL_JDBC_SCAN, table, qualifier, groupExpression, logicalProperties);
    }

    public LogicalJdbcScan(RelationId id, TableIf table, List<String> qualifier) {
        this(id, table, qualifier, Optional.empty(), Optional.empty());
    }

    @Override
    public TableIf getTable() {
        Preconditions.checkArgument(table instanceof ExternalTable || table instanceof JdbcTable,
                String.format("Table %s is neither ExternalTable nor JdbcTable", table.getName()));
        return table;
    }

    @Override
    public String toString() {
        return Utils.toSqlString("LogicalJdbcScan",
            "qualified", qualifiedName(),
            "output", getOutput()
        );
    }

    @Override
    public LogicalJdbcScan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new LogicalJdbcScan(relationId, table, qualifier, groupExpression,
            Optional.of(getLogicalProperties()));
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        return new LogicalJdbcScan(relationId, table, qualifier, groupExpression, logicalProperties);
    }

    @Override
    public LogicalJdbcScan withRelationId(RelationId relationId) {
        return new LogicalJdbcScan(relationId, table, qualifier, Optional.empty(), Optional.empty());
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalJdbcScan(this, context);
    }
}
