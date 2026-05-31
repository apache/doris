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

package org.apache.doris.nereids.trees.plans.commands.insert;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.EnvFactory;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.Status;
import org.apache.doris.common.profile.ExecutionProfile;
import org.apache.doris.common.profile.Profile;
import org.apache.doris.common.profile.SummaryProfile;
import org.apache.doris.job.manager.JobManager;
import org.apache.doris.job.manager.StreamingTaskManager;
import org.apache.doris.load.EtlJobType;
import org.apache.doris.load.loadv2.LoadManager;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.Coordinator;
import org.apache.doris.qe.InsertResult;
import org.apache.doris.qe.QueryState.MysqlStateType;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.transaction.GlobalTransactionMgrIface;
import org.apache.doris.transaction.TransactionState;
import org.apache.doris.transaction.TransactionStatus;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Optional;

/**
 * Tests for publish-timeout behaviors in {@link OlapInsertExecutor}.
 */
class OlapInsertExecutorTest {

    @AfterEach
    void tearDown() {
        ConnectContext.remove();
    }

    @Test
    void testExecuteSingleInsertPublishTimeoutReturnErrorKeepsCommittedAccounting() throws Exception {
        ConnectContext ctx = createExecutorContext();
        ctx.getSessionVariable().setInsertVisibleTimeoutReturnMode(
                SessionVariable.INSERT_VISIBLE_TIMEOUT_RETURN_MODE_ERROR);

        Coordinator coordinator = createCoordinator();
        GlobalTransactionMgrIface txnMgr = Mockito.mock(GlobalTransactionMgrIface.class);
        TransactionState txnState = Mockito.mock(TransactionState.class);
        LoadManager loadManager = Mockito.mock(LoadManager.class);
        Env currentEnv = createCurrentEnv(loadManager);
        StmtExecutor stmtExecutor = createStmtExecutor();

        try (MockedStatic<EnvFactory> envFactoryMock = Mockito.mockStatic(EnvFactory.class);
                MockedStatic<Env> envMock = Mockito.mockStatic(Env.class)) {
            prepareFactoryMocks(envFactoryMock, envMock, coordinator, txnMgr, txnState, currentEnv);
            ctx.setEnv(currentEnv);

            Mockito.when(txnMgr.commitAndPublishTransaction(
                    Mockito.any(), Mockito.anyList(), Mockito.anyLong(), Mockito.anyList(), Mockito.anyLong(),
                    Mockito.isNull())).thenReturn(false);

            OlapInsertExecutor executor = createExecutor(ctx);
            executor.txnId = 10001L;
            executor.executeSingleInsert(stmtExecutor);

            Assertions.assertEquals(TransactionStatus.COMMITTED, executor.txnStatus);
            Assertions.assertEquals(MysqlStateType.ERR, ctx.getState().getStateType());
            Assertions.assertTrue(ctx.getState().getErrorMessage().contains(
                    "transaction commit successfully, BUT data did not become visible within "
                            + "insert_visible_timeout_ms and will be visible later."));

            InsertResult insertResult = ctx.getInsertResult();
            Assertions.assertNotNull(insertResult);
            Assertions.assertEquals(TransactionStatus.COMMITTED, insertResult.txnStatus);
            Assertions.assertEquals(12L, insertResult.loadedRows);
            Assertions.assertEquals(1L, insertResult.filteredRows);
            Assertions.assertEquals(12L, ctx.getReturnRows());

            Mockito.verify(loadManager).recordFinishedLoadJob(Mockito.eq("label_test"), Mockito.eq(10001L),
                    Mockito.eq("test_db"), Mockito.eq(2L), Mockito.eq(EtlJobType.INSERT), Mockito.anyLong(),
                    Mockito.eq(""), Mockito.isNull(), Mockito.isNull(), Mockito.eq(UserIdentity.ROOT),
                    Mockito.anyLong());
            Mockito.verify(txnMgr, Mockito.never()).abortTransaction(Mockito.anyLong(), Mockito.anyLong(),
                    Mockito.anyString());
        }
    }

    @Test
    void testPublishTimeoutCommittedModeReturnsOk() throws Exception {
        ConnectContext ctx = createExecutorContext();
        Coordinator coordinator = createCoordinator();
        GlobalTransactionMgrIface txnMgr = Mockito.mock(GlobalTransactionMgrIface.class);
        TransactionState txnState = Mockito.mock(TransactionState.class);
        LoadManager loadManager = Mockito.mock(LoadManager.class);
        Env currentEnv = createCurrentEnv(loadManager);
        StmtExecutor stmtExecutor = createStmtExecutor();

        try (MockedStatic<EnvFactory> envFactoryMock = Mockito.mockStatic(EnvFactory.class);
                MockedStatic<Env> envMock = Mockito.mockStatic(Env.class)) {
            prepareFactoryMocks(envFactoryMock, envMock, coordinator, txnMgr, txnState, currentEnv);
            ctx.setEnv(currentEnv);

            Mockito.when(txnMgr.commitAndPublishTransaction(
                    Mockito.any(), Mockito.anyList(), Mockito.anyLong(), Mockito.anyList(), Mockito.anyLong(),
                    Mockito.isNull())).thenReturn(false);

            OlapInsertExecutor executor = createExecutor(ctx);
            executor.txnId = 10002L;
            executor.executeSingleInsert(stmtExecutor);

            Assertions.assertEquals(TransactionStatus.COMMITTED, executor.txnStatus);
            Assertions.assertEquals(MysqlStateType.OK, ctx.getState().getStateType());
            Assertions.assertTrue(ctx.getState().getInfoMessage().contains("'status':'COMMITTED'"));

            InsertResult insertResult = ctx.getInsertResult();
            Assertions.assertNotNull(insertResult);
            Assertions.assertEquals(TransactionStatus.COMMITTED, insertResult.txnStatus);
            Assertions.assertEquals(12L, insertResult.loadedRows);
            Assertions.assertEquals(1L, insertResult.filteredRows);
            Assertions.assertEquals(12L, ctx.getReturnRows());

            Mockito.verify(txnMgr, Mockito.never()).abortTransaction(Mockito.anyLong(), Mockito.anyLong(),
                    Mockito.anyString());
        }
    }

    // Build a fresh context per case so insertResult and QueryState do not leak between tests.
    private ConnectContext createExecutorContext() {
        ConnectContext ctx = new ConnectContext();
        ctx.setThreadLocalInfo();
        ctx.setCurrentUserIdentity(UserIdentity.ROOT);
        ctx.setQueryId(new TUniqueId(1, 2));
        // Disable strict insert mode because this test intentionally keeps one filtered row.
        ctx.getSessionVariable().setEnableInsertStrict(false);
        ctx.getState().reset();
        ctx.resetReturnRows();
        return ctx;
    }

    // Prepare the mocked coordinator so the executor can run its completion logic without real execution.
    private Coordinator createCoordinator() throws Exception {
        Coordinator coordinator = Mockito.mock(Coordinator.class);
        Mockito.when(coordinator.join(Mockito.anyInt())).thenReturn(true);
        Mockito.when(coordinator.isDone()).thenReturn(true);
        Mockito.when(coordinator.getExecStatus()).thenReturn(new Status(TStatusCode.OK, ""));
        Mockito.when(coordinator.getCommitInfos()).thenReturn(Lists.newArrayList());
        Mockito.when(coordinator.getTrackingUrl()).thenReturn(null);
        Mockito.when(coordinator.getFirstErrorMsg()).thenReturn(null);
        Mockito.when(coordinator.getExecutionProfile()).thenReturn(Mockito.mock(ExecutionProfile.class));
        Mockito.when(coordinator.getLoadCounters()).thenReturn(ImmutableMap.of(
                "dpp.norm.ALL", "12",
                "dpp.abnorm.ALL", "1"));
        return coordinator;
    }

    // Use a mocked executor so executeSingleInsert can run the real control flow without a full query setup.
    private StmtExecutor createStmtExecutor() {
        StmtExecutor stmtExecutor = Mockito.mock(StmtExecutor.class);
        Mockito.when(stmtExecutor.getProfile()).thenReturn(Mockito.mock(Profile.class));
        Mockito.when(stmtExecutor.getSummaryProfile()).thenReturn(Mockito.mock(SummaryProfile.class));
        Mockito.when(stmtExecutor.getOriginStmtInString()).thenReturn("insert into test_tbl select 1");
        Mockito.when(stmtExecutor.getParsedStmt()).thenReturn(null);
        Mockito.when(stmtExecutor.isProfileSafeStmt()).thenReturn(false);
        return stmtExecutor;
    }

    // Provide the job-manager chain needed by master-side setTxnCallbackId().
    private Env createCurrentEnv(LoadManager loadManager) {
        Env currentEnv = Mockito.mock(Env.class);
        JobManager<?, ?> jobManager = Mockito.mock(JobManager.class);
        StreamingTaskManager streamingTaskManager = Mockito.mock(StreamingTaskManager.class);
        Mockito.when(currentEnv.getLoadManager()).thenReturn(loadManager);
        Mockito.when(currentEnv.getJobManager()).thenReturn(jobManager);
        Mockito.when(jobManager.getStreamingTaskManager()).thenReturn(streamingTaskManager);
        Mockito.when(streamingTaskManager.getStreamingInsertTaskById(Mockito.anyLong())).thenReturn(null);
        return currentEnv;
    }

    // Create an executor with mocked table metadata because this test only validates timeout result handling.
    private OlapInsertExecutor createExecutor(ConnectContext ctx) {
        Database database = Mockito.mock(Database.class);
        Mockito.when(database.getFullName()).thenReturn("test_db");
        Mockito.when(database.getId()).thenReturn(1L);

        Table table = Mockito.mock(Table.class);
        Mockito.when(table.getDatabase()).thenReturn(database);
        Mockito.when(table.getName()).thenReturn("test_tbl");
        Mockito.when(table.getId()).thenReturn(2L);

        return new OlapInsertExecutor(ctx, table, "label_test", Mockito.mock(NereidsPlanner.class),
                Optional.empty(), false, 0L);
    }

    // Redirect coordinator creation and transaction access to mocks so the test stays deterministic.
    private void prepareFactoryMocks(MockedStatic<EnvFactory> envFactoryMock, MockedStatic<Env> envMock,
            Coordinator coordinator, GlobalTransactionMgrIface txnMgr, TransactionState txnState, Env currentEnv) {
        EnvFactory envFactory = Mockito.mock(EnvFactory.class);
        envFactoryMock.when(EnvFactory::getInstance).thenReturn(envFactory);
        Mockito.when(envFactory.createCoordinator(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.anyLong()))
                .thenReturn(coordinator);

        envMock.when(Env::getCurrentGlobalTransactionMgr).thenReturn(txnMgr);
        envMock.when(Env::getCurrentEnv).thenReturn(currentEnv);
        Mockito.when(txnMgr.getTransactionState(Mockito.anyLong(), Mockito.anyLong())).thenReturn(txnState);
    }
}
