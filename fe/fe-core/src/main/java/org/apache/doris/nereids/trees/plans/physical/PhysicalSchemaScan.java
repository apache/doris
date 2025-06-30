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

import jdk.internal.org.objectweb.asm.tree.analysis.AnalyzerException;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.statistics.Statistics;

import com.google.common.collect.ImmutableList;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * PhysicalSchemaScan.
 */
public class PhysicalSchemaScan extends PhysicalCatalogRelation {
    // if the schema table only root user readable, we need insert into this set
    public static final Set<String> BACKEND_TABLE_ONLY_ROOT_READABLE = new HashSet<>();

    private final Optional<String> schemaCatalog;
    private final Optional<String> schemaDatabase;
    private final Optional<String> schemaTable;

    static {
        BACKEND_TABLE_ONLY_ROOT_READABLE.add("backend_configuration");
    }

    public PhysicalSchemaScan(RelationId id, TableIf table, List<String> qualifier,
            Optional<GroupExpression> groupExpression, LogicalProperties logicalProperties,
            Optional<String> schemaCatalog, Optional<String> schemaDatabase, Optional<String> schemaTable) {
        super(id, PlanType.PHYSICAL_SCHEMA_SCAN, table, qualifier, groupExpression, logicalProperties,
                ImmutableList.of());
        this.schemaCatalog = schemaCatalog;
        this.schemaDatabase = schemaDatabase;
        this.schemaTable = schemaTable;
        if (!checkUserAccessPermission(table.getName())) {
            throw new AnalyzerException(
                    "Access denied; you need (at least one of) the ADMIN privilege(s) for this operation");
        }
    }

    public PhysicalSchemaScan(RelationId id, TableIf table, List<String> qualifier,
            Optional<GroupExpression> groupExpression, LogicalProperties logicalProperties,
            PhysicalProperties physicalProperties, Statistics statistics,
            Optional<String> schemaCatalog, Optional<String> schemaDatabase, Optional<String> schemaTable) {
        super(id, PlanType.PHYSICAL_SCHEMA_SCAN, table, qualifier, groupExpression,
                logicalProperties, physicalProperties, statistics, ImmutableList.of());
        this.schemaCatalog = schemaCatalog;
        this.schemaDatabase = schemaDatabase;
        this.schemaTable = schemaTable;
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
    public TableIf getTable() {
        return table;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalSchemaScan(this, context);
    }

    @Override
    public Plan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new PhysicalSchemaScan(relationId, getTable(), qualifier,
                groupExpression, getLogicalProperties(), physicalProperties, statistics,
                schemaCatalog, schemaDatabase, schemaTable);
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        return new PhysicalSchemaScan(relationId, getTable(), qualifier,
                groupExpression, logicalProperties.get(), physicalProperties, statistics,
                schemaCatalog, schemaDatabase, schemaTable);
    }

    @Override
    public PhysicalPlan withPhysicalPropertiesAndStats(PhysicalProperties physicalProperties,
            Statistics statistics) {
        return new PhysicalSchemaScan(relationId, getTable(), qualifier,
                groupExpression, getLogicalProperties(), physicalProperties, statistics,
                schemaCatalog, schemaDatabase, schemaTable);
    }

    @Override
    public String toString() {
        return Utils.toSqlString("PhysicalSchemaScan");
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
        PhysicalSchemaScan that = (PhysicalSchemaScan) o;
        return Objects.equals(schemaCatalog, that.schemaCatalog)
            && Objects.equals(schemaDatabase, that.schemaDatabase)
            && Objects.equals(schemaTable, that.schemaTable);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), schemaCatalog, schemaDatabase, schemaTable);
    }

    @Override
    public boolean canPushDownRuntimeFilter() {
        // currently be doesn't support schema scan rf
        return false;
    }

    private boolean checkUserAccessPermission(String tableName) {
        if (BACKEND_TABLE_ONLY_ROOT_READABLE.contains(tableName.toLowerCase()) && Env.getCurrentEnv().getAccessManager()
                .checkGlobalPriv(ConnectContext.get().getCurrentUserIdentity(), PrivPredicate.ADMIN)) {
            return true;
        }
        return false;
    }
}
