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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.constraint.Constraint;
import org.apache.doris.catalog.constraint.PrimaryKeyConstraint;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.ExplainCommand.ExplainLevel;
import org.apache.doris.nereids.trees.plans.logical.LogicalCatalogRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Set;

/**
 * drop constraint command
 */
public class DropConstraintCommand extends Command implements ForwardWithSync {

    public static final Logger LOG = LogManager.getLogger(DropConstraintCommand.class);
    private final String name;
    private final LogicalPlan plan;

    /**
     * constructor
     */
    public DropConstraintCommand(String name, LogicalPlan plan) {
        super(PlanType.DROP_CONSTRAINT_COMMAND);
        this.name = name;
        this.plan = plan;
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        TableIf table = extractTable(ctx, plan);
        checkAlterPriv(ctx, table);
        List<TableIf> cascadeTables = ImmutableList.of();
        Constraint constraint;
        table.readLock();
        try {
            constraint = table.getConstraintsMapUnsafe().get(name);
            if (constraint == null) {
                throw new AnalysisException(
                        String.format("Unknown constraint %s on table %s.", name, table.getName()));
            }
            if (constraint instanceof PrimaryKeyConstraint) {
                // The reverse references are updated under the primary table's write lock.
                cascadeTables = ((PrimaryKeyConstraint) constraint).getForeignTables();
            }
        } finally {
            table.readUnlock();
        }
        // Dropping a primary key also deletes the foreign keys of its referencing tables.
        // Authorize the complete snapshot before any constraint is modified.
        for (TableIf cascadeTable : cascadeTables) {
            checkAlterPriv(ctx, cascadeTable);
        }
        Set<TableIf> authorizedCascadeTables = ImmutableSet.copyOf(cascadeTables);
        table.writeLock();
        try {
            // Authorization runs outside the table lock. A concurrent ADD/DROP must not replace
            // the authorized constraint or add an unauthorized cascade target before mutation.
            if (table.getConstraintsMapUnsafe().get(name) != constraint
                    || (constraint instanceof PrimaryKeyConstraint
                    && !authorizedCascadeTables.containsAll(((PrimaryKeyConstraint) constraint).getForeignTables()))) {
                throw new AnalysisException("Constraint " + name + " on table " + table.getName()
                        + " changed while checking privileges; retry the statement");
            }
            table.dropConstraint(name, false);
        } finally {
            table.writeUnlock();
        }
    }

    private void checkAlterPriv(ConnectContext ctx, TableIf table)
            throws org.apache.doris.common.AnalysisException {
        if (!Env.getCurrentEnv().getAccessManager().checkTblPriv(ctx, table.getDatabase().getCatalog().getName(),
                table.getDatabase().getFullName(), table.getName(), PrivPredicate.ALTER)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "ALTER",
                    ctx.getQualifiedUser(), ctx.getRemoteIP(),
                    table.getDatabase().getFullName() + ": " + table.getName());
        }
    }

    private TableIf extractTable(ConnectContext ctx, LogicalPlan plan) {
        NereidsPlanner planner = new NereidsPlanner(ctx.getStatementContext());
        Plan analyzedPlan = planner.planWithLock(plan, PhysicalProperties.ANY, ExplainLevel.ANALYZED_PLAN);
        Set<LogicalCatalogRelation> logicalCatalogRelationSet = analyzedPlan
                .collect(LogicalCatalogRelation.class::isInstance);
        if (logicalCatalogRelationSet.size() != 1) {
            throw new AnalysisException("Can not found table when dropping constraint");
        }
        LogicalCatalogRelation catalogRelation = logicalCatalogRelationSet.iterator().next();
        return catalogRelation.getTable();
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitDropConstraintCommand(this, context);
    }
}
