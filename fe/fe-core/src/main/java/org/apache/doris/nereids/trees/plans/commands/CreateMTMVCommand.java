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

import org.apache.doris.analysis.ColumnDef;
import org.apache.doris.analysis.CreateMultiTableMaterializedViewStmt;
import org.apache.doris.analysis.MVRefreshInfo.BuildMode;
import org.apache.doris.analysis.MVRefreshInfo.RefreshMethod;
import org.apache.doris.analysis.MVRefreshTriggerInfo;
import org.apache.doris.analysis.TypeDef;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.Config;
import org.apache.doris.common.UserException;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Objects;

/**
 * create multi table materialized view
 */
public class CreateMTMVCommand extends Command implements ForwardWithSync {

    public static final Logger LOG = LogManager.getLogger(CreateMTMVCommand.class);

    private final LogicalPlan logicalQuery;
    private final String querySql;
    private String originSql;

    //    private final String mvName;
    private final BuildMode buildMode;
    private final RefreshMethod refreshMethod;
    private final MVRefreshTriggerInfo refreshTriggerInfo;
    private final List<TableIf> baseTables = Lists.newArrayList();
    private final List<ColumnDef> columnDefs = Lists.newArrayList();

    /**
     * constructor
     */
    public CreateMTMVCommand(LogicalPlan logicalQuery, BuildMode buildMode, RefreshMethod refreshMethod,
            MVRefreshTriggerInfo refreshTriggerInfo, String querySql) {
        super(PlanType.CREATE_MTMV_COMMAND);
        this.logicalQuery = Objects.requireNonNull(logicalQuery,
                "logicalQuery cannot be null in CreateMTMVCommand");
        this.buildMode = Objects.requireNonNull(buildMode,
                "buildMode cannot be null in CreateMTMVCommand");
        this.refreshMethod = Objects.requireNonNull(refreshMethod,
                "refreshMethod cannot be null in CreateMTMVCommand");
        this.refreshTriggerInfo = Objects.requireNonNull(refreshTriggerInfo,
                "refreshTriggerInfo cannot be null in CreateMTMVCommand");
        this.querySql = Objects.requireNonNull(querySql,
                "querySql cannot be null in CreateMTMVCommand");
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        LogicalPlanAdapter logicalPlanAdapter = new LogicalPlanAdapter(logicalQuery, ctx.getStatementContext());
        NereidsPlanner planner = new NereidsPlanner(ctx.getStatementContext());
        planner.plan(logicalPlanAdapter, ctx.getSessionVariable().toThrift());
        getColumns(planner);
        getBaseTables(planner);
        // TODO: 2023/9/7 check
        originSql = logicalPlanAdapter.toSql();
        if (!Config.enable_mtmv) {
            throw new UserException("Multi table materialized view was not graduated."
                    + " You should set `enable_mtmv = true` in fe to enabled it manually.");
        }
        Env.getCurrentEnv().createMultiTableMaterializedView(transferCommandToStmt());
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitCreateMTMVCommand(this, context);
    }

    private CreateMultiTableMaterializedViewStmt transferCommandToStmt() {
        return null;
    }

    private void getBaseTables(NereidsPlanner planner) {
        for (ScanNode scanNode : planner.getScanNodes()) {
            baseTables.add(scanNode.getTupleDesc().getTable());
        }
    }

    private void getColumns(NereidsPlanner planner) {
        List<Slot> slots = planner.getOptimizedPlan().getOutput();
        for (Slot slot : slots) {
            ColumnDef def = new ColumnDef(slot.getName(), new TypeDef(slot.getDataType().toCatalogDataType()));
            columnDefs.add(def);
        }
    }
}
