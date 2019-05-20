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
import org.apache.doris.analysis.SqlScanner;
import org.apache.doris.analysis.UseStmt;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.util.RuntimeProfile;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.mysql.MysqlChannel;
import org.apache.doris.mysql.MysqlSerializer;
import org.apache.doris.planner.Planner;
import org.apache.doris.rewrite.ExprRewriter;
import org.apache.doris.service.FrontendOptions;
import org.apache.doris.thrift.TQueryOptions;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.collect.Lists;

import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.SortedMap;

import java_cup.runtime.Symbol;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"org.apache.log4j.*", "javax.management.*"})
@PrepareForTest({StmtExecutor.class, DdlExecutor.class, Catalog.class})
public class StmtExecutorTest {
    private ConnectContext ctx;
    private QueryState state;
    private ConnectScheduler scheduler;

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

        MysqlChannel channel = EasyMock.createMock(MysqlChannel.class);
        channel.sendOnePacket(EasyMock.isA(ByteBuffer.class));
        EasyMock.expectLastCall().anyTimes();
        channel.reset();
        EasyMock.expectLastCall().anyTimes();
        EasyMock.replay(channel);

        scheduler = EasyMock.createMock(ConnectScheduler.class);

        ctx = EasyMock.createMock(ConnectContext.class);
        EasyMock.expect(ctx.getMysqlChannel()).andReturn(channel).anyTimes();
        EasyMock.expect(ctx.getSerializer()).andReturn(MysqlSerializer.newInstance()).anyTimes();
        EasyMock.expect(ctx.getCatalog()).andReturn(AccessTestUtil.fetchAdminCatalog()).anyTimes();
        EasyMock.expect(ctx.getState()).andReturn(state).anyTimes();
        EasyMock.expect(ctx.getConnectScheduler()).andReturn(scheduler).anyTimes();
        EasyMock.expect(ctx.getConnectionId()).andReturn(1).anyTimes();
        EasyMock.expect(ctx.getQualifiedUser()).andReturn("testUser").anyTimes();
        ctx.setKilled();
        EasyMock.expectLastCall().anyTimes();
        ctx.updateReturnRows(EasyMock.anyInt());
        EasyMock.expectLastCall().anyTimes();
        ctx.setQueryId(EasyMock.isA(TUniqueId.class));
        EasyMock.expectLastCall().anyTimes();
        EasyMock.expect(ctx.queryId()).andReturn(new TUniqueId()).anyTimes();
        EasyMock.expect(ctx.getStartTime()).andReturn(0L).anyTimes();
        EasyMock.expect(ctx.getDatabase()).andReturn("testDb").anyTimes();
        SessionVariable sessionVariable = new SessionVariable();
        EasyMock.expect(ctx.getSessionVariable()).andReturn(sessionVariable).anyTimes();
        ctx.setStmtId(EasyMock.anyLong());
        EasyMock.expectLastCall().anyTimes();
        EasyMock.expect(ctx.getStmtId()).andReturn(1L).anyTimes();
        EasyMock.replay(ctx);
    }

    @Test
    public void testSelect() throws Exception {
        QueryStmt queryStmt = EasyMock.createMock(QueryStmt.class);
        queryStmt.analyze(EasyMock.isA(Analyzer.class));
        EasyMock.expectLastCall().anyTimes();
        EasyMock.expect(queryStmt.getColLabels()).andReturn(Lists.<String>newArrayList()).anyTimes();
        EasyMock.expect(queryStmt.getResultExprs()).andReturn(Lists.<Expr>newArrayList()).anyTimes();
        EasyMock.expect(queryStmt.isExplain()).andReturn(false).anyTimes();
        queryStmt.getDbs(EasyMock.isA(Analyzer.class), EasyMock.isA(SortedMap.class));
        EasyMock.expectLastCall().anyTimes();
        EasyMock.expect(queryStmt.getRedirectStatus()).andReturn(RedirectStatus.NO_FORWARD).anyTimes();
        queryStmt.rewriteExprs(EasyMock.isA(ExprRewriter.class));
        EasyMock.expectLastCall().anyTimes();
        EasyMock.replay(queryStmt);

        Symbol symbol = new Symbol(0, queryStmt);
        SqlParser parser = EasyMock.createMock(SqlParser.class);
        EasyMock.expect(parser.parse()).andReturn(symbol).anyTimes();
        EasyMock.replay(parser);

        PowerMock.expectNew(SqlParser.class, EasyMock.isA(SqlScanner.class)).andReturn(parser);
        PowerMock.replay(SqlParser.class);

        // mock planner
        Planner planner = EasyMock.createMock(Planner.class);
        planner.plan(EasyMock.isA(QueryStmt.class), EasyMock.isA(Analyzer.class), EasyMock.isA(TQueryOptions.class));
        EasyMock.expectLastCall().anyTimes();
        EasyMock.replay(planner);

        PowerMock.expectNew(Planner.class).andReturn(planner).anyTimes();
        PowerMock.replay(Planner.class);

        // mock coordinator
        Coordinator cood = EasyMock.createMock(Coordinator.class);
        cood.exec();
        EasyMock.expectLastCall().anyTimes();
        cood.endProfile();
        EasyMock.expectLastCall().anyTimes();
        EasyMock.expect(cood.getQueryProfile()).andReturn(new RuntimeProfile()).anyTimes();
        EasyMock.expect(cood.getNext()).andReturn(new RowBatch()).anyTimes();
        EasyMock.expect(cood.getJobId()).andReturn(-1L).anyTimes();
        EasyMock.replay(cood);
        PowerMock.expectNew(Coordinator.class, EasyMock.isA(ConnectContext.class),
                EasyMock.isA(Analyzer.class), EasyMock.isA(Planner.class))
                .andReturn(cood).anyTimes();
        PowerMock.replay(Coordinator.class);

        Catalog catalog = Catalog.getInstance();
        Field field = catalog.getClass().getDeclaredField("canRead");
        field.setAccessible(true);
        field.setBoolean(catalog, true);

        PowerMock.mockStatic(Catalog.class);
        EasyMock.expect(Catalog.getInstance()).andReturn(catalog).anyTimes();
        PowerMock.replay(Catalog.class);

        StmtExecutor stmtExecutor = new StmtExecutor(ctx, "");
        stmtExecutor.execute();

        Assert.assertEquals(QueryState.MysqlStateType.EOF, state.getStateType());
    }

    @Test
    public void testShow() throws Exception {
        ShowStmt showStmt = EasyMock.createMock(ShowStmt.class);
        showStmt.analyze(EasyMock.isA(Analyzer.class));
        EasyMock.expectLastCall().anyTimes();
        EasyMock.expect(showStmt.getRedirectStatus()).andReturn(RedirectStatus.NO_FORWARD).anyTimes();
        EasyMock.expect(showStmt.toSelectStmt(EasyMock.isA(Analyzer.class))).andReturn(null).anyTimes();
        EasyMock.replay(showStmt);

        Symbol symbol = new Symbol(0, showStmt);
        SqlParser parser = EasyMock.createMock(SqlParser.class);
        EasyMock.expect(parser.parse()).andReturn(symbol).anyTimes();
        EasyMock.replay(parser);

        PowerMock.expectNew(SqlParser.class, EasyMock.isA(SqlScanner.class)).andReturn(parser);
        PowerMock.replay(SqlParser.class);

        // mock show
        List<List<String>> rows = Lists.newArrayList();
        rows.add(Lists.newArrayList("abc", "bcd"));
        ShowExecutor executor = EasyMock.createMock(ShowExecutor.class);
        EasyMock.expect(executor.execute()).andReturn(new ShowResultSet(new ShowAuthorStmt().getMetaData(), rows))
                .anyTimes();
        EasyMock.replay(executor);
        PowerMock.expectNew(ShowExecutor.class, EasyMock.isA(ConnectContext.class), EasyMock.isA(ShowStmt.class))
                .andReturn(executor).anyTimes();
        PowerMock.replay(ShowExecutor.class);

        StmtExecutor stmtExecutor = new StmtExecutor(ctx, "");
        stmtExecutor.execute();

        Assert.assertEquals(QueryState.MysqlStateType.EOF, state.getStateType());
    }

    @Test
    public void testShowNull() throws Exception {
        ShowStmt showStmt = EasyMock.createMock(ShowStmt.class);
        showStmt.analyze(EasyMock.isA(Analyzer.class));
        EasyMock.expectLastCall().anyTimes();
        EasyMock.expect(showStmt.getRedirectStatus()).andReturn(RedirectStatus.NO_FORWARD).anyTimes();
        EasyMock.expect(showStmt.toSelectStmt(EasyMock.isA(Analyzer.class))).andReturn(null).anyTimes();
        EasyMock.replay(showStmt);

        Symbol symbol = new Symbol(0, showStmt);
        SqlParser parser = EasyMock.createMock(SqlParser.class);
        EasyMock.expect(parser.parse()).andReturn(symbol).anyTimes();
        EasyMock.replay(parser);

        PowerMock.expectNew(SqlParser.class, EasyMock.isA(SqlScanner.class)).andReturn(parser);
        PowerMock.replay(SqlParser.class);

        // mock show
        List<List<String>> rows = Lists.newArrayList();
        rows.add(Lists.newArrayList("abc", "bcd"));
        ShowExecutor executor = EasyMock.createMock(ShowExecutor.class);
        EasyMock.expect(executor.execute()).andReturn(null).anyTimes();
        EasyMock.replay(executor);
        PowerMock.expectNew(ShowExecutor.class, EasyMock.isA(ConnectContext.class), EasyMock.isA(ShowStmt.class))
                .andReturn(executor).anyTimes();
        PowerMock.replay(ShowExecutor.class);

        StmtExecutor stmtExecutor = new StmtExecutor(ctx, "");
        stmtExecutor.execute();

        Assert.assertEquals(QueryState.MysqlStateType.OK, state.getStateType());
    }

    @Test
    public void testKill() throws Exception {
        KillStmt killStmt = EasyMock.createMock(KillStmt.class);
        killStmt.analyze(EasyMock.isA(Analyzer.class));
        EasyMock.expectLastCall().anyTimes();
        EasyMock.expect(killStmt.getConnectionId()).andReturn(1L).anyTimes();
        EasyMock.expect(killStmt.getRedirectStatus()).andReturn(RedirectStatus.NO_FORWARD).anyTimes();
        EasyMock.replay(killStmt);

        Symbol symbol = new Symbol(0, killStmt);
        SqlParser parser = EasyMock.createMock(SqlParser.class);
        EasyMock.expect(parser.parse()).andReturn(symbol).anyTimes();
        EasyMock.replay(parser);

        PowerMock.expectNew(SqlParser.class, EasyMock.isA(SqlScanner.class)).andReturn(parser);
        PowerMock.replay(SqlParser.class);

        // suicide
        EasyMock.expect(scheduler.getContext(1L)).andReturn(ctx);
        EasyMock.replay(scheduler);

        StmtExecutor stmtExecutor = new StmtExecutor(ctx, "");
        stmtExecutor.execute();

        Assert.assertEquals(QueryState.MysqlStateType.OK, state.getStateType());
    }

    @Test
    public void testKillOtherFail() throws Exception {
        KillStmt killStmt = EasyMock.createMock(KillStmt.class);
        killStmt.analyze(EasyMock.isA(Analyzer.class));
        EasyMock.expectLastCall().anyTimes();
        EasyMock.expect(killStmt.getConnectionId()).andReturn(1L).anyTimes();
        EasyMock.expect(killStmt.isConnectionKill()).andReturn(true).anyTimes();
        EasyMock.expect(killStmt.getRedirectStatus()).andReturn(RedirectStatus.NO_FORWARD).anyTimes();
        EasyMock.replay(killStmt);

        Symbol symbol = new Symbol(0, killStmt);
        SqlParser parser = EasyMock.createMock(SqlParser.class);
        EasyMock.expect(parser.parse()).andReturn(symbol).anyTimes();
        EasyMock.replay(parser);

        PowerMock.expectNew(SqlParser.class, EasyMock.isA(SqlScanner.class)).andReturn(parser);
        PowerMock.replay(SqlParser.class);

        ConnectContext killCtx = EasyMock.createMock(ConnectContext.class);
        EasyMock.expect(killCtx.getCatalog()).andReturn(AccessTestUtil.fetchAdminCatalog()).anyTimes();
        EasyMock.expect(killCtx.getQualifiedUser()).andReturn("blockUser").anyTimes();
        killCtx.kill(true);
        EasyMock.expectLastCall().anyTimes();
        EasyMock.replay(killCtx);
        // suicide
        EasyMock.expect(scheduler.getContext(1L)).andReturn(killCtx);
        EasyMock.replay(scheduler);

        StmtExecutor stmtExecutor = new StmtExecutor(ctx, "");
        stmtExecutor.execute();

        Assert.assertEquals(QueryState.MysqlStateType.ERR, state.getStateType());
    }

    @Test
    public void testKillOther() throws Exception {
        KillStmt killStmt = EasyMock.createMock(KillStmt.class);
        killStmt.analyze(EasyMock.isA(Analyzer.class));
        EasyMock.expectLastCall().anyTimes();
        EasyMock.expect(killStmt.getConnectionId()).andReturn(1L).anyTimes();
        EasyMock.expect(killStmt.isConnectionKill()).andReturn(true).anyTimes();
        EasyMock.expect(killStmt.getRedirectStatus()).andReturn(RedirectStatus.NO_FORWARD).anyTimes();
        EasyMock.replay(killStmt);

        Symbol symbol = new Symbol(0, killStmt);
        SqlParser parser = EasyMock.createMock(SqlParser.class);
        EasyMock.expect(parser.parse()).andReturn(symbol).anyTimes();
        EasyMock.replay(parser);

        PowerMock.expectNew(SqlParser.class, EasyMock.isA(SqlScanner.class)).andReturn(parser);
        PowerMock.replay(SqlParser.class);

        ConnectContext killCtx = EasyMock.createMock(ConnectContext.class);
        EasyMock.expect(killCtx.getCatalog()).andReturn(AccessTestUtil.fetchAdminCatalog()).anyTimes();
        EasyMock.expect(killCtx.getQualifiedUser()).andReturn("killUser").anyTimes();
        killCtx.kill(true);
        EasyMock.expectLastCall().anyTimes();
        EasyMock.replay(killCtx);
        // suicide
        EasyMock.expect(scheduler.getContext(1L)).andReturn(killCtx);
        EasyMock.replay(scheduler);

        StmtExecutor stmtExecutor = new StmtExecutor(ctx, "");
        stmtExecutor.execute();

        Assert.assertEquals(QueryState.MysqlStateType.ERR, state.getStateType());
    }

    @Test
    public void testKillNoCtx() throws Exception {
        KillStmt killStmt = EasyMock.createMock(KillStmt.class);
        killStmt.analyze(EasyMock.isA(Analyzer.class));
        EasyMock.expectLastCall().anyTimes();
        EasyMock.expect(killStmt.getConnectionId()).andReturn(1L).anyTimes();
        EasyMock.expect(killStmt.getRedirectStatus()).andReturn(RedirectStatus.NO_FORWARD).anyTimes();
        EasyMock.replay(killStmt);

        Symbol symbol = new Symbol(0, killStmt);
        SqlParser parser = EasyMock.createMock(SqlParser.class);
        EasyMock.expect(parser.parse()).andReturn(symbol).anyTimes();
        EasyMock.replay(parser);

        PowerMock.expectNew(SqlParser.class, EasyMock.isA(SqlScanner.class)).andReturn(parser);
        PowerMock.replay(SqlParser.class);

        // suicide
        EasyMock.expect(scheduler.getContext(1L)).andReturn(null);
        EasyMock.replay(scheduler);

        StmtExecutor stmtExecutor = new StmtExecutor(ctx, "");
        stmtExecutor.execute();

        Assert.assertEquals(QueryState.MysqlStateType.ERR, state.getStateType());
    }

    @Test
    public void testSet() throws Exception {
        SetStmt setStmt = EasyMock.createMock(SetStmt.class);
        setStmt.analyze(EasyMock.isA(Analyzer.class));
        EasyMock.expectLastCall().anyTimes();
        EasyMock.expect(setStmt.getRedirectStatus()).andReturn(RedirectStatus.NO_FORWARD).anyTimes();
        EasyMock.replay(setStmt);

        Symbol symbol = new Symbol(0, setStmt);
        SqlParser parser = EasyMock.createMock(SqlParser.class);
        EasyMock.expect(parser.parse()).andReturn(symbol).anyTimes();
        EasyMock.replay(parser);

        PowerMock.expectNew(SqlParser.class, EasyMock.isA(SqlScanner.class)).andReturn(parser);
        PowerMock.replay(SqlParser.class);

        // Mock set
        SetExecutor executor = EasyMock.createMock(SetExecutor.class);
        executor.execute();
        EasyMock.expectLastCall().anyTimes();
        EasyMock.replay(executor);

        PowerMock.expectNew(SetExecutor.class, EasyMock.isA(ConnectContext.class), EasyMock.isA(SetStmt.class))
                .andReturn(executor).anyTimes();
        PowerMock.replay(SetExecutor.class);

        StmtExecutor stmtExecutor = new StmtExecutor(ctx, "");
        stmtExecutor.execute();

        Assert.assertEquals(QueryState.MysqlStateType.OK, state.getStateType());
    }

    @Test
    public void testSetFail() throws Exception {
        SetStmt setStmt = EasyMock.createMock(SetStmt.class);
        setStmt.analyze(EasyMock.isA(Analyzer.class));
        EasyMock.expectLastCall().anyTimes();
        EasyMock.expect(setStmt.getRedirectStatus()).andReturn(RedirectStatus.NO_FORWARD).anyTimes();
        EasyMock.replay(setStmt);

        Symbol symbol = new Symbol(0, setStmt);
        SqlParser parser = EasyMock.createMock(SqlParser.class);
        EasyMock.expect(parser.parse()).andReturn(symbol).anyTimes();
        EasyMock.replay(parser);

        PowerMock.expectNew(SqlParser.class, EasyMock.isA(SqlScanner.class)).andReturn(parser);
        PowerMock.replay(SqlParser.class);

        // Mock set
        SetExecutor executor = EasyMock.createMock(SetExecutor.class);
        executor.execute();
        EasyMock.expectLastCall().andThrow(new DdlException("failed.")).anyTimes();
        EasyMock.replay(executor);

        PowerMock.expectNew(SetExecutor.class, EasyMock.isA(ConnectContext.class), EasyMock.isA(SetStmt.class))
                .andReturn(executor).anyTimes();
        PowerMock.replay(SetExecutor.class);

        StmtExecutor stmtExecutor = new StmtExecutor(ctx, "");
        stmtExecutor.execute();

        Assert.assertEquals(QueryState.MysqlStateType.ERR, state.getStateType());
    }

    @Test
    public void testDdl() throws Exception {
        DdlStmt ddlStmt = EasyMock.createMock(DdlStmt.class);
        ddlStmt.analyze(EasyMock.isA(Analyzer.class));
        EasyMock.expectLastCall().anyTimes();
        EasyMock.expect(ddlStmt.getRedirectStatus()).andReturn(RedirectStatus.NO_FORWARD).anyTimes();
        EasyMock.replay(ddlStmt);

        Symbol symbol = new Symbol(0, ddlStmt);
        SqlParser parser = EasyMock.createMock(SqlParser.class);
        EasyMock.expect(parser.parse()).andReturn(symbol).anyTimes();
        EasyMock.replay(parser);

        PowerMock.expectNew(SqlParser.class, EasyMock.isA(SqlScanner.class)).andReturn(parser);
        PowerMock.replay(SqlParser.class);

        // Mock ddl
        PowerMock.mockStatic(DdlExecutor.class);
        DdlExecutor.execute(EasyMock.isA(Catalog.class), EasyMock.isA(DdlStmt.class), EasyMock.anyString());
        EasyMock.expectLastCall().anyTimes();
        PowerMock.replay(DdlExecutor.class);

        StmtExecutor executor = new StmtExecutor(ctx, "");
        executor.execute();

        Assert.assertEquals(QueryState.MysqlStateType.OK, state.getStateType());
    }

    @Test
    public void testDdlFail() throws Exception {
        DdlStmt ddlStmt = EasyMock.createMock(DdlStmt.class);
        ddlStmt.analyze(EasyMock.isA(Analyzer.class));
        EasyMock.expectLastCall().anyTimes();
        EasyMock.expect(ddlStmt.getRedirectStatus()).andReturn(RedirectStatus.NO_FORWARD).anyTimes();
        EasyMock.replay(ddlStmt);

        Symbol symbol = new Symbol(0, ddlStmt);
        SqlParser parser = EasyMock.createMock(SqlParser.class);
        EasyMock.expect(parser.parse()).andReturn(symbol).anyTimes();
        EasyMock.replay(parser);

        PowerMock.expectNew(SqlParser.class, EasyMock.isA(SqlScanner.class)).andReturn(parser);
        PowerMock.replay(SqlParser.class);

        // Mock ddl
        PowerMock.mockStatic(DdlExecutor.class);
        DdlExecutor.execute(EasyMock.isA(Catalog.class), EasyMock.isA(DdlStmt.class), EasyMock.anyString());
        EasyMock.expectLastCall().andThrow(new DdlException("ddl fail"));
        PowerMock.replay(DdlExecutor.class);

        StmtExecutor executor = new StmtExecutor(ctx, "");
        executor.execute();

        Assert.assertEquals(QueryState.MysqlStateType.ERR, state.getStateType());
    }

    @Test
    public void testDdlFail2() throws Exception {
        DdlStmt ddlStmt = EasyMock.createMock(DdlStmt.class);
        ddlStmt.analyze(EasyMock.isA(Analyzer.class));
        EasyMock.expectLastCall().anyTimes();
        EasyMock.expect(ddlStmt.getRedirectStatus()).andReturn(RedirectStatus.NO_FORWARD).anyTimes();
        EasyMock.replay(ddlStmt);

        Symbol symbol = new Symbol(0, ddlStmt);
        SqlParser parser = EasyMock.createMock(SqlParser.class);
        EasyMock.expect(parser.parse()).andReturn(symbol).anyTimes();
        EasyMock.replay(parser);

        PowerMock.expectNew(SqlParser.class, EasyMock.isA(SqlScanner.class)).andReturn(parser);
        PowerMock.replay(SqlParser.class);

        // Mock ddl
        PowerMock.mockStatic(DdlExecutor.class);
        DdlExecutor.execute(EasyMock.isA(Catalog.class), EasyMock.isA(DdlStmt.class), EasyMock.anyString());
        EasyMock.expectLastCall().andThrow(new Exception("bug"));
        PowerMock.replay(DdlExecutor.class);

        StmtExecutor executor = new StmtExecutor(ctx, "");
        executor.execute();

        Assert.assertEquals(QueryState.MysqlStateType.ERR, state.getStateType());
    }

    @Test
    public void testUse() throws Exception {
        UseStmt useStmt = EasyMock.createMock(UseStmt.class);
        useStmt.analyze(EasyMock.isA(Analyzer.class));
        EasyMock.expectLastCall().anyTimes();
        EasyMock.expect(useStmt.getDatabase()).andReturn("testDb").anyTimes();
        EasyMock.expect(useStmt.getRedirectStatus()).andReturn(RedirectStatus.NO_FORWARD).anyTimes();
        EasyMock.expect(useStmt.getClusterName()).andReturn("testCluster").anyTimes();
        EasyMock.replay(useStmt);

        Symbol symbol = new Symbol(0, useStmt);
        SqlParser parser = EasyMock.createMock(SqlParser.class);
        EasyMock.expect(parser.parse()).andReturn(symbol).anyTimes();
        EasyMock.replay(parser);

        StmtExecutor executor = new StmtExecutor(ctx, "");
        executor.execute();

        Assert.assertEquals(QueryState.MysqlStateType.OK, state.getStateType());
    }

    @Test
    public void testUseFail() throws Exception {
        UseStmt useStmt = EasyMock.createMock(UseStmt.class);
        useStmt.analyze(EasyMock.isA(Analyzer.class));
        EasyMock.expectLastCall().anyTimes();
        EasyMock.expect(useStmt.getDatabase()).andReturn("blockDb").anyTimes();
        EasyMock.expect(useStmt.getRedirectStatus()).andReturn(RedirectStatus.NO_FORWARD).anyTimes();
        EasyMock.expect(useStmt.getClusterName()).andReturn("testCluster").anyTimes();
        EasyMock.replay(useStmt);

        Symbol symbol = new Symbol(0, useStmt);
        SqlParser parser = EasyMock.createMock(SqlParser.class);
        EasyMock.expect(parser.parse()).andReturn(symbol).anyTimes();
        EasyMock.replay(parser);

        PowerMock.expectNew(SqlParser.class, EasyMock.isA(SqlScanner.class)).andReturn(parser);
        PowerMock.replay(SqlParser.class);

        StmtExecutor executor = new StmtExecutor(ctx, "");
        executor.execute();

        Assert.assertEquals(QueryState.MysqlStateType.ERR, state.getStateType());
    }
}

