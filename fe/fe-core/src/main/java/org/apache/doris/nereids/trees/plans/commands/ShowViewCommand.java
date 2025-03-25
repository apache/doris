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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.View;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.util.Util;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.info.TableNameInfo;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.PlanUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.stream.Collectors;

/**
 * ShowViewCommand
 */
public class ShowViewCommand extends ShowCommand {
    private static final ShowResultSetMetaData META_DATA = ShowResultSetMetaData.builder()
            .addColumn(new Column("View", ScalarType.createVarchar(30)))
            .addColumn(new Column("Create View", ScalarType.createVarchar(65535)))
            .build();

    private final String db;
    private final TableNameInfo tbl;
    private List<View> matchViews = Lists.newArrayList();

    public ShowViewCommand(String db, TableNameInfo tbl) {
        super(PlanType.SHOW_VIEW_COMMAND);
        this.db = db;
        this.tbl = tbl;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitShowViewCommand(this, context);
    }

    private void validate(ConnectContext ctx) throws Exception {
        if (!Strings.isNullOrEmpty(db)) {
            // if user specify the `from db`, overwrite the db in `tbl` with this db.
            // for example:
            //      show view from db1.tbl1 from db2;
            // will be rewrote to:
            //      show view from db2.tbl1;
            // this act same as in MySQL
            tbl.setDb(db);
        }
        tbl.analyze(ctx);
        // disallow external catalog
        Util.prohibitExternalCatalog(tbl.getCtl(), this.getClass().getSimpleName());

        String dbName = tbl.getDb();
        if (!Env.getCurrentEnv().getAccessManager().checkTblPriv(
                ConnectContext.get(), tbl.getCtl(), dbName, tbl.getTbl(), PrivPredicate.SHOW)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "SHOW VIEW",
                    ConnectContext.get().getQualifiedUser(),
                    ConnectContext.get().getRemoteIP(),
                    dbName + ": " + tbl.getTbl());
        }

        Database database = Env.getCurrentInternalCatalog().getDbOrAnalysisException(dbName);
        database.getOlapTableOrAnalysisException(tbl.getTbl());
        for (Table table : database.getViewsOnIdOrder()) {
            View view = (View) table;
            // get table refs instead of get tables because it don't need to check table's validity
            List<TableNameInfo> tableNameInfos = getTableNames(ctx, view.getInlineViewDef());
            for (TableNameInfo tableNameInfo : tableNameInfos) {
                tableNameInfo.analyze(ctx);
                if (tableNameInfo.equals(tbl)) {
                    matchViews.add(view);
                }
            }
        }
    }

    private List<TableNameInfo> getTableNames(ConnectContext ctx, String sql) {
        LogicalPlan unboundMvPlan = new NereidsParser().parseSingle(sql);
        StatementContext statementContext = new StatementContext(ctx,
                new OriginStatement(sql, 0));
        NereidsPlanner planner = new NereidsPlanner(statementContext);
        if (ctx.getStatementContext() == null) {
            ctx.setStatementContext(statementContext);
        }
        planner.planWithLock(unboundMvPlan, PhysicalProperties.ANY, ExplainCommand.ExplainLevel.ANALYZED_PLAN);
        LogicalPlan logicalPlan = (LogicalPlan) planner.getCascadesContext().getRewritePlan();

        return PlanUtils.getLogicalScanFromRootPlan(logicalPlan).stream()
                .map(plan -> new TableNameInfo(plan.getTable().getFullQualifiers())).collect(Collectors.toList());
    }

    @Override
    public ShowResultSet doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {
        validate(ctx);
        List<List<String>> rows = Lists.newArrayList();
        for (View view : matchViews) {
            view.readLock();
            try {
                List<String> createViewStmt = Lists.newArrayList();
                Env.getDdlStmt(view, createViewStmt, null, null, false, true /* hide password */, -1L);
                if (!createViewStmt.isEmpty()) {
                    rows.add(Lists.newArrayList(view.getName(), createViewStmt.get(0)));
                }
            } finally {
                view.readUnlock();
            }
        }
        return new ShowResultSet(META_DATA, rows);
    }
}
