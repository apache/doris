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

package org.apache.doris.qe;

import org.apache.doris.analysis.AccessTestUtil;
import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.DdlStmt;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.KillStmt;
import org.apache.doris.analysis.QueryStmt;
import org.apache.doris.analysis.RedirectStatus;
import org.apache.doris.analysis.SetStmt;
import org.apache.doris.analysis.ShowAuthorStmt;
import org.apache.doris.analysis.ShowStmt;
import org.apache.doris.analysis.SqlParser;
import org.apache.doris.analysis.StatementBase;
import org.apache.doris.analysis.UseStmt;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.mysql.MysqlChannel;
import org.apache.doris.mysql.MysqlSerializer;
import org.apache.doris.planner.OriginalPlanner;
import org.apache.doris.qe.ConnectContext.ConnectType;
import org.apache.doris.rewrite.ExprRewriter;
import org.apache.doris.service.FrontendOptions;
import org.apache.doris.thrift.TQueryOptions;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java_cup.runtime.Symbol;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.SortedMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class StmtExecutorTest {
    private ConnectContext ctx;
    private QueryState state;
    private ConnectScheduler scheduler;
    @Mocked
    private MysqlChannel channel = null;

    @BeforeClass
    public static void start() {
        MetricRepo.init();
        try {
            FrontendOptions.init();
        } catch (UnknownHostException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    @Before
    public void setUp() throws IOException {
        state = new QueryState();
        scheduler = new ConnectScheduler(10);
        ctx = new ConnectContext();

        SessionVariable sessionVariable = new SessionVariable();
        MysqlSerializer serializer = MysqlSerializer.newInstance();
        Env env = AccessTestUtil.fetchAdminCatalog();

        new Expectations(channel) {
            {
                channel.sendOnePacket((ByteBuffer) any);
                minTimes = 0;

                channel.reset();
                minTimes = 0;

                channel.getSerializer();
                minTimes = 0;
                result = serializer;
            }
        };

        new Expectations(ctx) {
            {
                ctx.getMysqlChannel();
                minTimes = 0;
                result = channel;

                ctx.getEnv();
                minTimes = 0;
                result = env;

                ctx.getState();
                minTimes = 0;
                result = state;

                ctx.getConnectScheduler();
                minTimes = 0;
                result = scheduler;

                ctx.getConnectionId();
                minTimes = 0;
                result = 1;

                ctx.getQualifiedUser();
                minTimes = 0;
                result = "testUser";

                ctx.getForwardedStmtId();
                minTimes = 0;
                result = 123L;

                ctx.setKilled();
                minTimes = 0;

                ctx.updateReturnRows(anyInt);
                minTimes = 0;

                ctx.setQueryId((TUniqueId) any);
                minTimes = 0;

                ctx.queryId();
                minTimes = 0;
                result = new TUniqueId();

                ctx.getStartTime();
                minTimes = 0;
                result = 0L;

                ctx.getDatabase();
                minTimes = 0;
                result = "testDb";

                ctx.getSessionVariable();
                minTimes = 0;
                result = sessionVariable;

                ctx.setStmtId(anyLong);
                minTimes = 0;

                ctx.getStmtId();
                minTimes = 0;
                result = 1L;
            }
        };
    }

    @Test
    public void testSelect(@Mocked QueryStmt queryStmt,
                           @Mocked SqlParser parser,
                           @Mocked OriginalPlanner planner,
                           @Mocked Coordinator coordinator) throws Exception {
        Env env = Env.getCurrentEnv();
        Deencapsulation.setField(env, "canRead", new AtomicBoolean(true));

        new Expectations() {
            {
                queryStmt.analyze((Analyzer) any);
                minTimes = 0;

                queryStmt.getColLabels();
                minTimes = 0;
                result = Lists.<String>newArrayList();

                queryStmt.getResultExprs();
                minTimes = 0;
                result = Lists.<Expr>newArrayList();

                queryStmt.isExplain();
                minTimes = 0;
                result = false;

                queryStmt.getTables((Analyzer) any, anyBoolean, (SortedMap) any, Sets.newHashSet());
                minTimes = 0;

                queryStmt.getRedirectStatus();
                minTimes = 0;
                result = RedirectStatus.NO_FORWARD;

                queryStmt.rewriteExprs((ExprRewriter) any);
                minTimes = 0;

                Symbol symbol = new Symbol(0, Lists.newArrayList(queryStmt));
                parser.parse();
                minTimes = 0;
                result = symbol;

                planner.plan((QueryStmt) any, (TQueryOptions) any);
                minTimes = 0;

                // mock coordinator
                coordinator.exec();
                minTimes = 0;

                coordinator.getNext();
                minTimes = 0;
                result = new RowBatch();

                coordinator.getJobId();
                minTimes = 0;
                result = -1L;

                Env.getCurrentEnv();
                minTimes = 0;
                result = env;
            }
        };

        StmtExecutor stmtExecutor = new StmtExecutor(ctx, "");
        stmtExecutor.execute();

        Assert.assertEquals(QueryState.MysqlStateType.EOF, state.getStateType());
    }

    @Test
    public void testShow(@Mocked ShowStmt showStmt, @Mocked SqlParser parser,
            @Mocked ShowExecutor executor) throws Exception {
        new Expectations() {
            {
                showStmt.analyze((Analyzer) any);
                minTimes = 0;

                showStmt.getRedirectStatus();
                minTimes = 0;
                result = RedirectStatus.NO_FORWARD;

                showStmt.toSelectStmt((Analyzer) any);
                minTimes = 0;
                result = null;

                Symbol symbol = new Symbol(0, Lists.newArrayList(showStmt));
                parser.parse();
                minTimes = 0;
                result = symbol;

                // mock show
                List<List<String>> rows = Lists.newArrayList();
                rows.add(Lists.newArrayList("abc", "bcd"));
                executor.execute();
                minTimes = 0;
                result = new ShowResultSet(new ShowAuthorStmt().getMetaData(), rows);
            }
        };

        StmtExecutor stmtExecutor = new StmtExecutor(ctx, "");
        stmtExecutor.execute();

        Assert.assertEquals(QueryState.MysqlStateType.EOF, state.getStateType());
    }

    @Test
    public void testShowNull(@Mocked ShowStmt showStmt, @Mocked SqlParser parser,
            @Mocked ShowExecutor executor) throws Exception {
        new Expectations() {
            {
                showStmt.analyze((Analyzer) any);
                minTimes = 0;

                showStmt.getRedirectStatus();
                minTimes = 0;
                result = RedirectStatus.NO_FORWARD;

                showStmt.toSelectStmt((Analyzer) any);
                minTimes = 0;
                result = null;

                Symbol symbol = new Symbol(0, Lists.newArrayList(showStmt));
                parser.parse();
                minTimes = 0;
                result = symbol;

                // mock show
                List<List<String>> rows = Lists.newArrayList();
                rows.add(Lists.newArrayList("abc", "bcd"));
                executor.execute();
                minTimes = 0;
                result = null;
            }
        };

        StmtExecutor stmtExecutor = new StmtExecutor(ctx, "");
        stmtExecutor.execute();

        Assert.assertEquals(QueryState.MysqlStateType.OK, state.getStateType());
    }

    @Test
    public void testKill(@Mocked KillStmt killStmt, @Mocked SqlParser parser) throws Exception {
        new Expectations() {
            {
                killStmt.analyze((Analyzer) any);
                minTimes = 0;

                killStmt.getConnectionId();
                minTimes = 0;
                result = 1L;

                killStmt.getRedirectStatus();
                minTimes = 0;
                result = RedirectStatus.NO_FORWARD;

                Symbol symbol = new Symbol(0, Lists.newArrayList(killStmt));
                parser.parse();
                minTimes = 0;
                result = symbol;
            }
        };

        new Expectations(scheduler) {
            {
                // suicide
                scheduler.getContext(1);
                result = ctx;
            }
        };

        StmtExecutor stmtExecutor = new StmtExecutor(ctx, "");
        stmtExecutor.execute();

        Assert.assertEquals(QueryState.MysqlStateType.OK, state.getStateType());
    }

    @Test
    public void testKillOtherFail(@Mocked KillStmt killStmt, @Mocked SqlParser parser,
            @Mocked ConnectContext killCtx) throws Exception {
        Env killEnv = AccessTestUtil.fetchAdminCatalog();

        new Expectations() {
            {
                killStmt.analyze((Analyzer) any);
                minTimes = 0;

                killStmt.getConnectionId();
                minTimes = 0;
                result = 1L;

                killStmt.isConnectionKill();
                minTimes = 0;
                result = true;

                killStmt.getRedirectStatus();
                minTimes = 0;
                result = RedirectStatus.NO_FORWARD;

                Symbol symbol = new Symbol(0, Lists.newArrayList(killStmt));
                parser.parse();
                minTimes = 0;
                result = symbol;

                killCtx.getEnv();
                minTimes = 0;
                result = killEnv;

                killCtx.getQualifiedUser();
                minTimes = 0;
                result = "blockUser";

                killCtx.kill(true);
                minTimes = 0;

                killCtx.getConnectType();
                minTimes = 0;
                result = ConnectType.MYSQL;

                ConnectContext.get();
                minTimes = 0;
                result = ctx;
            }
        };

        new Expectations(scheduler) {
            {
                // suicide
                scheduler.getContext(1);
                result = killCtx;
            }
        };

        StmtExecutor stmtExecutor = new StmtExecutor(ctx, "");
        stmtExecutor.execute();

        Assert.assertEquals(QueryState.MysqlStateType.ERR, state.getStateType());
    }

    @Test
    public void testKillOther(@Mocked KillStmt killStmt, @Mocked SqlParser parser,
            @Mocked ConnectContext killCtx) throws Exception {
        Env killEnv = AccessTestUtil.fetchAdminCatalog();
        new Expectations() {
            {
                killStmt.analyze((Analyzer) any);
                minTimes = 0;

                killStmt.getConnectionId();
                minTimes = 0;
                result = 1;

                killStmt.isConnectionKill();
                minTimes = 0;
                result = true;

                killStmt.getRedirectStatus();
                minTimes = 0;
                result = RedirectStatus.NO_FORWARD;

                Symbol symbol = new Symbol(0, Lists.newArrayList(killStmt));
                parser.parse();
                minTimes = 0;
                result = symbol;

                killCtx.getEnv();
                minTimes = 0;
                result = killEnv;

                killCtx.getQualifiedUser();
                minTimes = 0;
                result = "killUser";

                killCtx.kill(true);
                minTimes = 0;

                killCtx.getConnectType();
                minTimes = 0;
                result = ConnectType.MYSQL;

                ConnectContext.get();
                minTimes = 0;
                result = ctx;
            }
        };

        new Expectations(scheduler) {
            {
                // suicide
                scheduler.getContext(1);
                result = killCtx;
            }
        };

        StmtExecutor stmtExecutor = new StmtExecutor(ctx, "");
        stmtExecutor.execute();

        Assert.assertEquals(QueryState.MysqlStateType.ERR, state.getStateType());
    }

    @Test
    public void testKillNoCtx(@Mocked KillStmt killStmt, @Mocked SqlParser parser) throws Exception {
        new Expectations() {
            {
                killStmt.analyze((Analyzer) any);
                minTimes = 0;

                killStmt.getConnectionId();
                minTimes = 0;
                result = 1;

                killStmt.getRedirectStatus();
                minTimes = 0;
                result = RedirectStatus.NO_FORWARD;

                Symbol symbol = new Symbol(0, Lists.newArrayList(killStmt));
                parser.parse();
                minTimes = 0;
                result = symbol;
            }
        };

        new Expectations(scheduler) {
            {
                scheduler.getContext(1);
                result = null;
            }
        };

        StmtExecutor stmtExecutor = new StmtExecutor(ctx, "");
        stmtExecutor.execute();

        Assert.assertEquals(QueryState.MysqlStateType.ERR, state.getStateType());
    }

    @Test
    public void testSet(@Mocked SetStmt setStmt, @Mocked SqlParser parser,
            @Mocked SetExecutor executor) throws Exception {
        new Expectations() {
            {
                setStmt.analyze((Analyzer) any);
                minTimes = 0;

                setStmt.getRedirectStatus();
                minTimes = 0;
                result = RedirectStatus.NO_FORWARD;

                Symbol symbol = new Symbol(0, Lists.newArrayList(setStmt));
                parser.parse();
                minTimes = 0;
                result = symbol;

                // Mock set
                executor.execute();
                minTimes = 0;
            }
        };

        StmtExecutor stmtExecutor = new StmtExecutor(ctx, "");
        stmtExecutor.execute();

        Assert.assertEquals(QueryState.MysqlStateType.OK, state.getStateType());
    }

    @Test
    public void testStmtWithUserInfo(@Mocked StatementBase stmt, @Mocked ConnectContext context) throws Exception {
        StmtExecutor stmtExecutor = new StmtExecutor(ctx, stmt);
        Deencapsulation.setField(stmtExecutor, "parsedStmt", null);
        Deencapsulation.setField(stmtExecutor, "originStmt", new OriginStatement("show databases;", 1));
        stmtExecutor.execute();
        StatementBase newstmt = Deencapsulation.getField(stmtExecutor, "parsedStmt");
        Assert.assertNotNull(newstmt.getUserInfo());
    }

    @Test
    public void testSetFail(@Mocked SetStmt setStmt, @Mocked SqlParser parser,
            @Mocked SetExecutor executor) throws Exception {
        new Expectations() {
            {
                setStmt.analyze((Analyzer) any);
                minTimes = 0;

                setStmt.getRedirectStatus();
                minTimes = 0;
                result = RedirectStatus.NO_FORWARD;

                Symbol symbol = new Symbol(0, Lists.newArrayList(setStmt));
                parser.parse();
                minTimes = 0;
                result = symbol;

                // Mock set
                executor.execute();
                minTimes = 0;
                result = new DdlException("failed");
            }
        };

        StmtExecutor stmtExecutor = new StmtExecutor(ctx, "");
        stmtExecutor.execute();

        Assert.assertEquals(QueryState.MysqlStateType.ERR, state.getStateType());
    }

    @Test
    public void testDdl(@Mocked DdlStmt ddlStmt, @Mocked SqlParser parser) throws Exception {
        new Expectations() {
            {
                ddlStmt.analyze((Analyzer) any);
                minTimes = 0;

                ddlStmt.getRedirectStatus();
                minTimes = 0;
                result = RedirectStatus.NO_FORWARD;

                Symbol symbol = new Symbol(0, Lists.newArrayList(ddlStmt));
                parser.parse();
                minTimes = 0;
                result = symbol;
            }
        };

        DdlExecutor ddlExecutor = new DdlExecutor();
        new Expectations(ddlExecutor) {
            {
                // Mock ddl
                DdlExecutor.execute((Env) any, (DdlStmt) any);
                minTimes = 0;
            }
        };

        StmtExecutor executor = new StmtExecutor(ctx, "");
        executor.execute();

        Assert.assertEquals(QueryState.MysqlStateType.OK, state.getStateType());
    }

    @Test
    public void testDdlFail(@Mocked DdlStmt ddlStmt, @Mocked SqlParser parser) throws Exception {
        new Expectations() {
            {
                ddlStmt.analyze((Analyzer) any);
                minTimes = 0;

                ddlStmt.getRedirectStatus();
                minTimes = 0;
                result = RedirectStatus.NO_FORWARD;

                Symbol symbol = new Symbol(0, Lists.newArrayList(ddlStmt));
                parser.parse();
                minTimes = 0;
                result = symbol;
            }
        };

        DdlExecutor ddlExecutor = new DdlExecutor();
        new Expectations(ddlExecutor) {
            {
                // Mock ddl
                DdlExecutor.execute((Env) any, (DdlStmt) any);
                minTimes = 0;
                result = new DdlException("ddl fail");
            }
        };

        StmtExecutor executor = new StmtExecutor(ctx, "");
        executor.execute();

        Assert.assertEquals(QueryState.MysqlStateType.ERR, state.getStateType());
    }

    @Test
    public void testDdlFail2(@Mocked DdlStmt ddlStmt, @Mocked SqlParser parser) throws Exception {
        new Expectations() {
            {
                ddlStmt.analyze((Analyzer) any);
                minTimes = 0;

                ddlStmt.getRedirectStatus();
                minTimes = 0;
                result = RedirectStatus.NO_FORWARD;

                Symbol symbol = new Symbol(0, Lists.newArrayList(ddlStmt));
                parser.parse();
                minTimes = 0;
                result = symbol;
            }
        };

        DdlExecutor ddlExecutor = new DdlExecutor();
        new Expectations(ddlExecutor) {
            {
                // Mock ddl
                DdlExecutor.execute((Env) any, (DdlStmt) any);
                minTimes = 0;
                result = new Exception("bug");
            }
        };

        StmtExecutor executor = new StmtExecutor(ctx, "");
        executor.execute();

        Assert.assertEquals(QueryState.MysqlStateType.ERR, state.getStateType());
    }

    @Test
    public void testUse(@Mocked UseStmt useStmt, @Mocked SqlParser parser) throws Exception {
        new Expectations() {
            {
                useStmt.analyze((Analyzer) any);
                minTimes = 0;

                useStmt.getDatabase();
                minTimes = 0;
                result = "testDb";

                useStmt.getRedirectStatus();
                minTimes = 0;
                result = RedirectStatus.NO_FORWARD;

                Symbol symbol = new Symbol(0, Lists.newArrayList(useStmt));
                parser.parse();
                minTimes = 0;
                result = symbol;
            }
        };

        StmtExecutor executor = new StmtExecutor(ctx, "");
        executor.execute();

        Assert.assertEquals(QueryState.MysqlStateType.OK, state.getStateType());
    }

    @Test
    public void testUseFail(@Mocked UseStmt useStmt, @Mocked SqlParser parser) throws Exception {
        new Expectations() {
            {
                useStmt.analyze((Analyzer) any);
                minTimes = 0;

                useStmt.getDatabase();
                minTimes = 0;
                result = "blockDb";

                useStmt.getRedirectStatus();
                minTimes = 0;
                result = RedirectStatus.NO_FORWARD;

                Symbol symbol = new Symbol(0, Lists.newArrayList(useStmt));
                parser.parse();
                minTimes = 0;
                result = symbol;
            }
        };

        StmtExecutor executor = new StmtExecutor(ctx, "");
        executor.execute();

        Assert.assertEquals(QueryState.MysqlStateType.ERR, state.getStateType());
    }

    @Test
    public void testUseWithCatalog(@Mocked UseStmt useStmt, @Mocked SqlParser parser) throws Exception {
        new Expectations() {
            {
                useStmt.analyze((Analyzer) any);
                minTimes = 0;

                useStmt.getDatabase();
                minTimes = 0;
                result = "testDb";

                useStmt.getRedirectStatus();
                minTimes = 0;
                result = RedirectStatus.NO_FORWARD;

                useStmt.getCatalogName();
                minTimes = 0;
                result = InternalCatalog.INTERNAL_CATALOG_NAME;

                Symbol symbol = new Symbol(0, Lists.newArrayList(useStmt));
                parser.parse();
                minTimes = 0;
                result = symbol;
            }
        };

        StmtExecutor executor = new StmtExecutor(ctx, "");
        executor.execute();

        Assert.assertEquals(QueryState.MysqlStateType.OK, state.getStateType());
    }

    @Test
    public void testUseWithCatalogFail(@Mocked UseStmt useStmt, @Mocked SqlParser parser) throws Exception {
        new Expectations() {
            {
                useStmt.analyze((Analyzer) any);
                minTimes = 0;

                useStmt.getDatabase();
                minTimes = 0;
                result = "blockDb";

                useStmt.getRedirectStatus();
                minTimes = 0;
                result = RedirectStatus.NO_FORWARD;

                useStmt.getCatalogName();
                minTimes = 0;
                result = "testcatalog";

                Symbol symbol = new Symbol(0, Lists.newArrayList(useStmt));
                parser.parse();
                minTimes = 0;
                result = symbol;
            }
        };

        StmtExecutor executor = new StmtExecutor(ctx, "");
        executor.execute();

        Assert.assertEquals(QueryState.MysqlStateType.ERR, state.getStateType());
    }
}
