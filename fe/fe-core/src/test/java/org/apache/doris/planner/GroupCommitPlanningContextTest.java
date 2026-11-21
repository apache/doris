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

package org.apache.doris.planner;

import org.apache.doris.blockrule.SqlBlockRuleMgr;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.EnvFactory;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.commands.PrepareCommand;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertIntoTableCommand;
import org.apache.doris.proto.InternalService.PGroupCommitInsertResponse;
import org.apache.doris.proto.Types.PStatus;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.qe.PreparedStatementContext;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.transaction.TransactionEntry;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.List;

class GroupCommitPlanningContextTest extends TestWithFeService {
    @Override
    protected void runBeforeAll() throws Exception {
        FeConstants.runningUnitTest = true;
        createDatabase("group_commit_context");
        createTable("create table group_commit_context.target (k int, v int) "
                + "duplicate key(k) distributed by hash(k) buckets 1 properties('replication_num'='1')");
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void planningUsesCurrentExecutionAndPreservesSession(boolean failAfterPlanning) throws Exception {
        String sql = "insert into group_commit_context.target values (1, 2)";
        InsertIntoTableCommand command = Mockito.spy((InsertIntoTableCommand) new NereidsParser().parseSingle(sql));
        String previousGroupCommit = connectContext.getSessionVariable().groupCommit;
        connectContext.getSessionVariable().groupCommit = "sync_mode";
        connectContext.setGroupCommit(true);
        TUniqueId queryId = new TUniqueId(31, 41);
        TUniqueId loadId = new TUniqueId(51, 61);
        connectContext.setQueryId(queryId);
        connectContext.setLoadId(loadId);
        connectContext.setSqlHash("current-execute-hash");
        connectContext.setStartTime();
        connectContext.getState().reset();
        StmtExecutor executor = new StmtExecutor(connectContext, sql);
        StatementContext statement = connectContext.getStatementContext();
        TransactionEntry transaction = connectContext.getTxnEntry();
        PrepareCommand prepare = new PrepareCommand("stmt", command, List.of(), new OriginStatement(sql, 0));
        PreparedStatementContext prepared = new PreparedStatementContext(prepare, connectContext, statement, sql);
        RuntimeException failure = new IllegalStateException("group commit plan creation failed");
        SqlBlockRuleMgr originalRules = Env.getCurrentEnv().getSqlBlockRuleMgr();
        SqlBlockRuleMgr rules = Mockito.spy(originalRules);
        Deencapsulation.setField(Env.getCurrentEnv(), "sqlBlockRuleMgr", rules);
        EnvFactory factory = Mockito.spy(EnvFactory.getInstance());
        PGroupCommitInsertResponse response = PGroupCommitInsertResponse.newBuilder()
                .setStatus(PStatus.newBuilder().setStatusCode(TStatusCode.OK.getValue()))
                .setLabel("group_commit_context").setTxnId(7).setLoadedRows(1).build();
        try (MockedStatic<EnvFactory> factories = Mockito.mockStatic(EnvFactory.class)) {
            factories.when(EnvFactory::getInstance).thenReturn(factory);
            Mockito.doAnswer(call -> {
                if (failAfterPlanning) {
                    throw failure;
                }
                GroupCommitPlanner planner = Mockito.spy((GroupCommitPlanner) call.callRealMethod());
                Mockito.doReturn(response).when(planner).executeGroupCommitInsert(
                        Mockito.same(connectContext), Mockito.anyList());
                return planner;
            }).when(factory).createGroupCommitPlanner(
                    Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(), Mockito.anyString());
            if (failAfterPlanning) {
                Assertions.assertSame(failure, Assertions.assertThrows(RuntimeException.class,
                        () -> GroupCommitPlanner.executeGroupCommitInsert(
                                connectContext, executor, prepared, statement)));
                Assertions.assertTrue(prepared.groupCommitPlanner.isEmpty());
            } else {
                GroupCommitPlanner.executeGroupCommitInsert(connectContext, executor, prepared, statement);
                GroupCommitPlanner cached = prepared.groupCommitPlanner.orElseThrow();
                GroupCommitPlanner.executeGroupCommitInsert(connectContext, executor, prepared, statement);
                Assertions.assertSame(cached, prepared.groupCommitPlanner.orElseThrow());
                Assertions.assertEquals(1, connectContext.getState().getAffectedRows());
                Mockito.verify(cached, Mockito.times(2)).executeGroupCommitInsert(
                        Mockito.same(connectContext), Mockito.anyList());
            }
            Mockito.verify(command).initPlan(connectContext, executor, false);
            Assertions.assertNotNull(executor.planner());
            Assertions.assertSame(executor, connectContext.getExecutor());
            Assertions.assertSame(statement, connectContext.getStatementContext());
            Assertions.assertSame(statement, ((NereidsPlanner) executor.planner()).getStatementContext());
            Mockito.verify(rules).matchSql(sql, "current-execute-hash", connectContext.getQualifiedUser());
            Assertions.assertEquals(queryId, connectContext.queryId());
            Assertions.assertEquals(loadId, connectContext.getLoadId());
            Assertions.assertSame(transaction, connectContext.getTxnEntry());
            Assertions.assertFalse(connectContext.isClosingOrClosed());
        } finally {
            Deencapsulation.setField(Env.getCurrentEnv(), "sqlBlockRuleMgr", originalRules);
            statement.close();
            connectContext.getSessionVariable().groupCommit = previousGroupCommit;
            connectContext.setGroupCommit(false);
            connectContext.setStatementContext(null);
            connectContext.setExecutor(null);
        }
    }
}
