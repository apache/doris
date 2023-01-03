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

import org.apache.doris.catalog.SchemaTable;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.algebra.Scan;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

/**
 * LogicalSchemaScan.
 */
public class LogicalSchemaScan extends LogicalRelation implements Scan {
    public LogicalSchemaScan(RelationId id,
            TableIf table,
            List<String> qualifier) {
        super(id, PlanType.LOGICAL_SCHEMA_SCAN, table, qualifier);
    }

    public LogicalSchemaScan(RelationId id,
            TableIf table,
            List<String> qualifier, Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties) {
        super(id, PlanType.LOGICAL_SCHEMA_SCAN, table, qualifier, groupExpression, logicalProperties);
    }

    @Override
    public SchemaTable getTable() {
        return (SchemaTable) table;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalSchemaScan(this, context);
    }

    @Override
    public List<Slot> computeNonUserVisibleOutput() {
        SchemaTable schemaTable = getTable();
        return schemaTable.getBaseSchema().stream()
                .map(col -> SlotReference.fromColumn(col, qualified()))
                .collect(ImmutableList.toImmutableList());
    }

    @Override
    public Plan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new LogicalSchemaScan(id, table, qualifier, groupExpression, Optional.of(getLogicalProperties()));
    }

    @Override
    public Plan withLogicalProperties(Optional<LogicalProperties> logicalProperties) {
        return new LogicalSchemaScan(id, table, qualifier, groupExpression, logicalProperties);
    }

    @Override
    public boolean equals(Object o) {
        return super.equals(o);
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public String toString() {
        return Utils.toSqlString("LogicalSchemaScan");
    }
}
