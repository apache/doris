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
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * LogicalSchemaScan.
 */
public class LogicalSchemaScan extends LogicalCatalogRelation {

    private final boolean filterPushed;
    private final Optional<String> schemaCatalog;
    private final Optional<String> schemaDatabase;
    private final Optional<String> schemaTable;

    public LogicalSchemaScan(RelationId id, TableIf table, List<String> qualifier) {
        this(id, table, qualifier, false,
                Optional.empty(), Optional.empty(), Optional.empty(), ImmutableList.of(),
                Optional.empty(), Optional.empty());
    }

    /**
     * Constructs a LogicalSchemaScan with the specified parameters.
     *
     * @param id Unique identifier for this relation
     * @param table The table interface representing the underlying data source
     * @param qualifier The qualifier list representing the path to this table
     * @param filterPushed Whether filter has been pushed down to this scan operation
     * @param schemaCatalog Optional catalog name in the schema
     * @param schemaDatabase Optional database name in the schema
     * @param schemaTable Optional table name in the schema
     * @param virtualColumns List of virtual columns to be included in the scan
     * @param groupExpression Optional group expression for memo representation
     * @param logicalProperties Optional logical properties for this plan node
     */
    public LogicalSchemaScan(RelationId id, TableIf table, List<String> qualifier, boolean filterPushed,
            Optional<String> schemaCatalog, Optional<String> schemaDatabase, Optional<String> schemaTable,
            List<NamedExpression> virtualColumns,
            Optional<GroupExpression> groupExpression, Optional<LogicalProperties> logicalProperties) {
        super(id, PlanType.LOGICAL_SCHEMA_SCAN, table, qualifier, ImmutableList.of(), virtualColumns,
                groupExpression, logicalProperties);
        this.filterPushed = filterPushed;
        this.schemaCatalog = schemaCatalog;
        this.schemaDatabase = schemaDatabase;
        this.schemaTable = schemaTable;
    }

    public boolean isFilterPushed() {
        return filterPushed;
    }

    public Optional<String> getSchemaCatalog() {
        return schemaCatalog;
    }

    public Optional<String> getSchemaDatabase() {
        return schemaDatabase;
    }

    public Optional<String> getSchemaTable() {
        return schemaTable;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalSchemaScan(this, context);
    }

    @Override
    public Plan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new LogicalSchemaScan(relationId, table, qualifier, filterPushed,
                schemaCatalog, schemaDatabase, schemaTable, virtualColumns,
                groupExpression, Optional.of(getLogicalProperties()));
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        return new LogicalSchemaScan(relationId, table, qualifier, filterPushed,
                schemaCatalog, schemaDatabase, schemaTable, virtualColumns, groupExpression, logicalProperties);
    }

    @Override
    public LogicalSchemaScan withRelationId(RelationId relationId) {
        return new LogicalSchemaScan(relationId, table, qualifier, filterPushed,
                schemaCatalog, schemaDatabase, schemaTable, virtualColumns, Optional.empty(), Optional.empty());
    }

    public LogicalSchemaScan withSchemaIdentifier(Optional<String> schemaCatalog, Optional<String> schemaDatabase,
            Optional<String> schemaTable) {
        return new LogicalSchemaScan(relationId, table, qualifier, true, schemaCatalog, schemaDatabase, schemaTable,
                virtualColumns, Optional.empty(), Optional.of(getLogicalProperties()));
    }

    @Override
    public String toString() {
        return Utils.toSqlString("LogicalSchemaScan");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        LogicalSchemaScan that = (LogicalSchemaScan) o;
        return Objects.equals(schemaCatalog, that.schemaCatalog)
            && Objects.equals(schemaDatabase, that.schemaDatabase)
            && Objects.equals(schemaTable, that.schemaTable)
            && filterPushed == that.filterPushed;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), schemaCatalog, schemaDatabase, schemaTable, filterPushed);
    }
}
