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

import org.apache.doris.catalog.OdbcTable;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Logical scan for external odbc table.
 */
public class LogicalOdbcScan extends LogicalExternalRelation {

    public LogicalOdbcScan(RelationId id, TableIf table, List<String> qualifier,
            Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties,
            Set<Expression> conjuncts) {
        super(id, PlanType.LOGICAL_ODBC_SCAN, table, qualifier, conjuncts, groupExpression, logicalProperties);
    }

    public LogicalOdbcScan(RelationId id, TableIf table, List<String> qualifier) {
        this(id, table, qualifier, Optional.empty(), Optional.empty(), ImmutableSet.of());
    }

    @Override
    public TableIf getTable() {
        Preconditions.checkArgument(table instanceof OdbcTable,
                String.format("Table %s is not OdbcTable", table.getName()));
        return table;
    }

    @Override
    public String toString() {
        return Utils.toSqlString("LogicalOdbcScan",
                "qualified", qualifiedName(),
                "output", getOutput()
        );
    }

    @Override
    public LogicalOdbcScan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new LogicalOdbcScan(relationId, table, qualifier, groupExpression,
                Optional.of(getLogicalProperties()), conjuncts);
    }

    @Override
    public LogicalOdbcScan withConjuncts(Set<Expression> conjuncts) {
        return new LogicalOdbcScan(relationId, table, qualifier, Optional.empty(),
                Optional.of(getLogicalProperties()), conjuncts);
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        return new LogicalOdbcScan(relationId, table, qualifier, groupExpression, logicalProperties, conjuncts);
    }

    @Override
    public LogicalOdbcScan withRelationId(RelationId relationId) {
        return new LogicalOdbcScan(relationId, table, qualifier, Optional.empty(), Optional.empty(), conjuncts);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalOdbcScan(this, context);
    }
}
