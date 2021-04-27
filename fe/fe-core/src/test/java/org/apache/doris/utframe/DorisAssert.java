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

package org.apache.doris.utframe;

import org.apache.doris.alter.AlterJobV2;
import org.apache.doris.analysis.AlterTableStmt;
import org.apache.doris.analysis.CreateDbStmt;
import org.apache.doris.analysis.CreateMaterializedViewStmt;
import org.apache.doris.analysis.CreateTableStmt;
import org.apache.doris.analysis.DropTableStmt;
import org.apache.doris.analysis.ExplainOptions;
import org.apache.doris.analysis.SqlParser;
import org.apache.doris.analysis.SqlScanner;
import org.apache.doris.analysis.StatementBase;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.util.SqlParserUtils;
import org.apache.doris.planner.Planner;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.QueryState;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.system.SystemInfoService;

import org.apache.doris.qe.SessionVariable;

import org.apache.commons.lang.StringUtils;
import org.junit.Assert;

import java.io.IOException;
import java.io.StringReader;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class DorisAssert {

    private ConnectContext ctx;

    public DorisAssert() throws IOException {
        this.ctx = UtFrameUtils.createDefaultCtx();
    }

    public DorisAssert withEnableMV() {
        ctx.getSessionVariable().setTestMaterializedView(true);
        Config.enable_materialized_view = true;
        return this;
    }

    public DorisAssert withDatabase(String dbName) throws Exception {
        CreateDbStmt createDbStmt =
                (CreateDbStmt) UtFrameUtils.parseAndAnalyzeStmt("create database " + dbName + ";", ctx);
        Catalog.getCurrentCatalog().createDb(createDbStmt);
        return this;
    }

    public DorisAssert useDatabase(String dbName) {
        ctx.setDatabase(ClusterNamespace.getFullName(SystemInfoService.DEFAULT_CLUSTER, dbName));
        return this;
    }

    public DorisAssert withoutUseDatabase() {
        ctx.setDatabase("");
        return this;
    }

    public DorisAssert withTable(String sql) throws Exception {
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, ctx);
        Catalog.getCurrentCatalog().createTable(createTableStmt);
        return this;
    }

    public DorisAssert dropTable(String tableName) throws Exception {
        DropTableStmt dropTableStmt =
                (DropTableStmt) UtFrameUtils.parseAndAnalyzeStmt("drop table " + tableName + ";", ctx);
        Catalog.getCurrentCatalog().dropTable(dropTableStmt);
        return this;
    }

    // Add materialized view to the schema
    public DorisAssert withMaterializedView(String sql) throws Exception {
        CreateMaterializedViewStmt createMaterializedViewStmt =
                (CreateMaterializedViewStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, ctx);
        Catalog.getCurrentCatalog().createMaterializedView(createMaterializedViewStmt);
        checkAlterJob();
        // waiting table state to normal
        Thread.sleep(100);
        return this;
    }

    // Add rollup
    public DorisAssert withRollup(String sql) throws Exception {
        AlterTableStmt alterTableStmt = (AlterTableStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, ctx);
        Catalog.getCurrentCatalog().alterTable(alterTableStmt);
        checkAlterJob();
        // waiting table state to normal
        Thread.sleep(100);
        return this;
    }

    public sessionVariable getSessionVariable() {
        return ctx.getSessionVariable();
    }

    private void checkAlterJob() throws InterruptedException {
        // check alter job
        Map<Long, AlterJobV2> alterJobs = Catalog.getCurrentCatalog().getRollupHandler().getAlterJobsV2();
        for (AlterJobV2 alterJobV2 : alterJobs.values()) {
            while (!alterJobV2.getJobState().isFinalState()) {
                System.out.println("alter job " + alterJobV2.getDbId()
                        + " is running. state: " + alterJobV2.getJobState());
                Thread.sleep(100);
            }
            System.out.println("alter job " + alterJobV2.getDbId() + " is done. state: " + alterJobV2.getJobState());
            Assert.assertEquals(AlterJobV2.JobState.FINISHED, alterJobV2.getJobState());
        }
    }

    public QueryAssert query(String sql) {
        return new QueryAssert(ctx, sql);
    }

    public class QueryAssert {
        private ConnectContext connectContext;
        private String sql;

        public QueryAssert(ConnectContext connectContext, String sql) {
            this.connectContext = connectContext;
            this.sql = sql;
        }

        public void explainContains(String... keywords) throws Exception {
            Assert.assertTrue(Stream.of(keywords).allMatch(explainQuery()::contains));
        }

        public void explainContains(String keywords, int count) throws Exception {
            Assert.assertEquals(StringUtils.countMatches(explainQuery(), keywords), count);
        }

        public void explainWithout(String s) throws Exception {
            Assert.assertFalse(explainQuery().contains(s));
        }

        public String explainQuery() throws Exception {
            return internalExecute("explain " + sql);
        }

        private String internalExecute(String sql) throws Exception {
            StmtExecutor stmtExecutor = new StmtExecutor(connectContext, sql);
            stmtExecutor.execute();
            QueryState queryState = connectContext.getState();
            if (queryState.getStateType() == QueryState.MysqlStateType.ERR) {
                switch (queryState.getErrType()) {
                    case ANALYSIS_ERR:
                        throw new AnalysisException(queryState.getErrorMessage());
                    case OTHER_ERR:
                    default:
                        throw new Exception(queryState.getErrorMessage());
                }
            }
            Planner planner = stmtExecutor.planner();
            String explainString = planner.getExplainString(planner.getFragments(), new ExplainOptions(false, false));
            System.out.println(explainString);
            return explainString;
        }

        public Planner internalExecuteOneAndGetPlan() throws Exception{
            SqlScanner input = new SqlScanner(new StringReader(sql), ctx.getSessionVariable().getSqlMode());
            SqlParser parser = new SqlParser(input);
            List<StatementBase> stmts =  SqlParserUtils.getMultiStmts(parser);
            StmtExecutor stmtExecutor = new StmtExecutor(connectContext, stmts.get(0));
            stmtExecutor.execute();

            return stmtExecutor.planner();
        }
    }
}
