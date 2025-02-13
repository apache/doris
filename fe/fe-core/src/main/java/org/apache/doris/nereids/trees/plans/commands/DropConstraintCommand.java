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

import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.constraint.Constraint;
import org.apache.doris.catalog.constraint.PrimaryKeyConstraint;
import org.apache.doris.common.util.MetaLockUtils;
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

import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Comparator;
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
        List<TableIf> tables = Lists.newArrayList(table);
        table.readLock();
        try {
            Constraint constraint = table.getConstraintsMapUnsafe().get(name);
            if (constraint == null) {
                throw new AnalysisException(
                        String.format("Unknown constraint %s on table %s.", name, table.getName()));
            }
            if (constraint instanceof PrimaryKeyConstraint) {
                tables.addAll(((PrimaryKeyConstraint) constraint).getForeignTables());
            }
        } finally {
            table.readUnlock();
        }
        tables.sort((Comparator.comparing(TableIf::getId)));
        MetaLockUtils.writeLockTables(tables);
        try {
            table.dropConstraint(name, false);
        } finally {
            MetaLockUtils.writeUnlockTables(tables);
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
