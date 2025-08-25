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

import org.apache.doris.analysis.StmtType;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.info.TableNameInfo;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.persist.CleanQueryStatsInfo;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import org.apache.commons.lang3.StringUtils;

/**
 * CLEAN ALL QUERY STATS;
 * CLEAN DATABASE QUERY STATS FROM db;
 * CLEAN TABLE QUERY STATS FROM db.table;
 */
public class CleanQueryStatsCommand extends Command implements ForwardWithSync {
    private String dbName;
    private TableNameInfo tableNameInfo;
    private Scope scope;

    /**
     * CLEAN ALL QUERY STATS
     */
    public CleanQueryStatsCommand() {
        super(PlanType.CLEAN_QUERY_STATS_COMMAND);
        this.scope = Scope.ALL;
        this.tableNameInfo = null;
        this.dbName = null;
    }

    /**
     * CLEAN DATABASE QUERY STATS FROM db;
     */
    public CleanQueryStatsCommand(String dbName) {
        super(PlanType.CLEAN_QUERY_STATS_COMMAND);
        this.dbName = dbName;
        this.tableNameInfo = null;
        this.scope = Scope.DB;
    }

    /**
     * CLEAN TABLE QUERY STATS FROM db.table;
     */
    public CleanQueryStatsCommand(TableNameInfo tableNameInfo) {
        super(PlanType.CLEAN_QUERY_STATS_COMMAND);
        this.dbName = null;
        this.tableNameInfo = tableNameInfo;
        this.scope = Scope.TABLE;
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        validate(ctx);
        handleCleanQueryStatsCommand(ctx);
    }

    /**
     * validate
     */
    public void validate(ConnectContext ctx) throws AnalysisException {
        switch (scope) {
            case ALL:
                if (!Env.getCurrentEnv().getAccessManager()
                        .checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
                    ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR,
                            "CLEAN ALL QUERY STATS");
                }
                break;
            case DB:
                if (StringUtils.isEmpty(dbName)) {
                    dbName = ctx.getDatabase();
                }
                if (StringUtils.isEmpty(dbName)) {
                    ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_DB_ERROR);
                }

                Env.getCurrentEnv().getCurrentCatalog().getDbOrAnalysisException(dbName);
                if (!Env.getCurrentEnv().getAccessManager()
                        .checkDbPriv(ConnectContext.get(), ctx.getCurrentCatalog().getName(), dbName,
                            PrivPredicate.ALTER)) {
                    ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR,
                            "CLEAN DATABASE QUERY STATS FOR " + ClusterNamespace.getNameFromFullName(dbName));
                }
                break;
            case TABLE:
                tableNameInfo.analyze(ctx);
                dbName = tableNameInfo.getDb();
                if (StringUtils.isEmpty(dbName)) {
                    ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_DB_ERROR);
                }
                DatabaseIf db = Env.getCurrentEnv().getCurrentCatalog().getDbOrAnalysisException(dbName);
                db.getTableOrAnalysisException(tableNameInfo.getTbl());
                if (!Env.getCurrentEnv().getAccessManager()
                        .checkTblPriv(ConnectContext.get(), tableNameInfo.getCtl(), dbName, tableNameInfo.getTbl(),
                            PrivPredicate.ALTER)) {
                    ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR,
                            "CLEAN TABLE QUERY STATS FROM " + tableNameInfo);
                }
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + scope);
        }
    }

    private void handleCleanQueryStatsCommand(ConnectContext ctx) throws DdlException {
        CleanQueryStatsInfo cleanQueryStatsInfo;
        Env env = ctx.getEnv();
        switch (scope) {
            case ALL:
                cleanQueryStatsInfo = new CleanQueryStatsInfo(
                        Scope.ALL, env.getCurrentCatalog().getName(), null, null);
                break;
            case DB:
                cleanQueryStatsInfo = new CleanQueryStatsInfo(
                        Scope.DB, env.getCurrentCatalog().getName(), tableNameInfo.getDb(), null);
                break;
            case TABLE:
                cleanQueryStatsInfo = new CleanQueryStatsInfo(
                        Scope.TABLE, env.getCurrentCatalog().getName(), tableNameInfo.getDb(), tableNameInfo.getTbl());
                break;
            default:
                throw new DdlException("Unknown scope: " + scope);
        }
        env.cleanQueryStats(cleanQueryStatsInfo);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitCleanQueryStatsCommand(this, context);
    }

    /**
     * Scope of clean query stats
     */
    public enum Scope {
        ALL, DB, TABLE
    }

    @Override
    public StmtType stmtType() {
        return StmtType.CLEAN;
    }
}
