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
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.info.TableNameInfoUtils;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.analyzer.UnboundRelation;
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
        TableNameInfo tableNameInfo;
        try {
            TableIf table = extractTable(ctx, plan);
            tableNameInfo = TableNameInfoUtils.fromCatalogDb(
                    table.getDatabase().getCatalog(), table.getDatabase(), table);
        } catch (Exception e) {
            // Table may no longer exist (e.g., external table deleted by another system).
            // Fall back to extracting the table name from the unresolved plan.
            LOG.warn("Table resolution failed for dropping constraint {}, "
                    + "falling back to name-based lookup: {}", name, e.getMessage());
            tableNameInfo = extractTableNameFromPlan(ctx);
        }
        Env.getCurrentEnv().getConstraintManager().dropConstraint(
                tableNameInfo, name, false);
    }

    private TableNameInfo extractTableNameFromPlan(ConnectContext ctx) {
        if (!(plan instanceof UnboundRelation)) {
            throw new AnalysisException(
                    "Cannot resolve table for dropping constraint " + name);
        }
        UnboundRelation unbound = (UnboundRelation) plan;
        List<String> parts = unbound.getNameParts();
        String ctl = ctx.getCurrentCatalog() != null
                ? ctx.getCurrentCatalog().getName()
                : "internal";
        String db = ctx.getDatabase();
        // Fill in default catalog/db from connect context if not specified
        if (parts.size() == 1) {
            return new TableNameInfo(ctl, db, parts.get(0));
        } else if (parts.size() == 2) {
            return new TableNameInfo(ctl, parts.get(0), parts.get(1));
        }
        return new TableNameInfo(parts);
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
