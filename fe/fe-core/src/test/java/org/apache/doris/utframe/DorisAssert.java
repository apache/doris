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
import org.apache.doris.analysis.ExplainOptions;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.commands.CreateDatabaseCommand;
import org.apache.doris.nereids.trees.plans.commands.CreateMaterializedViewCommand;
import org.apache.doris.nereids.trees.plans.commands.CreateTableCommand;
import org.apache.doris.nereids.trees.plans.commands.CreateViewCommand;
import org.apache.doris.nereids.trees.plans.commands.DropDatabaseCommand;
import org.apache.doris.nereids.trees.plans.commands.DropTableCommand;
import org.apache.doris.nereids.trees.plans.commands.DropViewCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.planner.Planner;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.QueryState;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.qe.StmtExecutor;

import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;

import java.io.IOException;
import java.util.Map;
import java.util.stream.Stream;

/**
 * This class is deprecated.
 * If you want to start a FE server in unit test, please let your
 * test class extend {@link TestWithFeService}.
 */
@Deprecated
public class DorisAssert {

    private ConnectContext ctx;

    public DorisAssert() throws IOException {
        this.ctx = UtFrameUtils.createDefaultCtx();
    }

    public DorisAssert(ConnectContext ctx) {
        this.ctx = ctx;
    }

    public DorisAssert withDatabase(String dbName) throws Exception {
        String createDbStmtStr = "create database " + dbName;
        NereidsParser nereidsParser = new NereidsParser();
        LogicalPlan logicalPlan = nereidsParser.parseSingle(createDbStmtStr);
        StmtExecutor stmtExecutor = new StmtExecutor(ctx, createDbStmtStr);
        if (logicalPlan instanceof CreateDatabaseCommand) {
            ((CreateDatabaseCommand) logicalPlan).run(ctx, stmtExecutor);
        }
        return this;
    }

    public DorisAssert useDatabase(String dbName) {
        ctx.setDatabase(dbName);
        return this;
    }

    public DorisAssert withoutUseDatabase() {
        ctx.setDatabase("");
        return this;
    }

    public DorisAssert withTable(String sql) throws Exception {
        NereidsParser nereidsParser = new NereidsParser();
        LogicalPlan parsed = nereidsParser.parseSingle(sql);
        StmtExecutor stmtExecutor = new StmtExecutor(ctx, sql);
        if (parsed instanceof CreateTableCommand) {
            ((CreateTableCommand) parsed).run(ctx, stmtExecutor);
        }
        return this;
    }

    public DorisAssert dropTable(String tableName) throws Exception {
        return dropTable(tableName, false);
    }

    public DorisAssert dropTable(String tableName, boolean isForce) throws Exception {
        NereidsParser nereidsParser = new NereidsParser();
        LogicalPlan logicalPlan = nereidsParser.parseSingle("drop table " + tableName + (isForce ? " force" : "") + ";");
        if (logicalPlan instanceof DropTableCommand) {
            ((DropTableCommand) logicalPlan).run(ctx, null);
        }

        return this;
    }

    public DorisAssert withView(String sql) throws Exception {
        NereidsParser nereidsParser = new NereidsParser();
        LogicalPlan parsed = nereidsParser.parseSingle(sql);
        StmtExecutor stmtExecutor = new StmtExecutor(ctx, sql);
        if (parsed instanceof CreateViewCommand) {
            ((CreateViewCommand) parsed).run(ctx, stmtExecutor);
        }
        return this;
    }

    public DorisAssert dropView(String tableName) throws Exception {
        NereidsParser nereidsParser = new NereidsParser();
        LogicalPlan logicalPlan = nereidsParser.parseSingle("drop view " + tableName + ";");
        if (logicalPlan instanceof DropViewCommand) {
            ((DropViewCommand) logicalPlan).run(ctx, null);
        }

        return this;
    }

    public DorisAssert dropDB(String dbName) throws Exception {
        String sql = "drop database " + dbName;

        NereidsParser nereidsParser = new NereidsParser();
        LogicalPlan logicalPlan = nereidsParser.parseSingle(sql);
        if (logicalPlan instanceof DropDatabaseCommand) {
            ((DropDatabaseCommand) logicalPlan).run(ctx, null);
        }
        return this;
    }

    // Add materialized view to the schema
    public DorisAssert withMaterializedView(String sql) throws Exception {
        NereidsParser nereidsParser = new NereidsParser();
        LogicalPlan parsed = nereidsParser.parseSingle(sql);
        StmtExecutor stmtExecutor = new StmtExecutor(ctx, sql);
        if (parsed instanceof CreateMaterializedViewCommand) {
            ((CreateMaterializedViewCommand) parsed).run(ctx, stmtExecutor);
        }
        checkAlterJob();
        // waiting table state to normal
        Thread.sleep(100);
        return this;
    }

    public SessionVariable getSessionVariable() {
        return ctx.getSessionVariable();
    }

    private void checkAlterJob() throws InterruptedException {
        // check alter job
        Map<Long, AlterJobV2> alterJobs = Env.getCurrentEnv().getMaterializedViewHandler().getAlterJobsV2();
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
            this.connectContext.getState().setIsQuery(true);
            this.sql = sql;
        }

        public void explainContains(String... keywords) throws Exception {
            Assert.assertTrue(explainQuery(), Stream.of(keywords).allMatch(explainQuery()::contains));
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
            connectContext.setExecutor(stmtExecutor);
            ConnectContext.get().setExecutor(stmtExecutor);
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
            String explainString = planner.getExplainString(new ExplainOptions(false, false, false));
            System.out.println(explainString);
            return explainString;
        }
    }
}
