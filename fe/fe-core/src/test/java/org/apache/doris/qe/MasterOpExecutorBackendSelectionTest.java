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

import org.apache.doris.catalog.Env;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.LoadException;
import org.apache.doris.resource.BackendSelection;
import org.apache.doris.resource.BackendSelectionManager;
import org.apache.doris.resource.spi.BackendSelectionProvider;
import org.apache.doris.system.SystemInfoService.HostInfo;
import org.apache.doris.thrift.TGroupCommitInfo;
import org.apache.doris.thrift.TMasterOpRequest;
import org.apache.doris.thrift.TMasterOpResult;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TUniqueId;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.List;
import java.util.Set;

public class MasterOpExecutorBackendSelectionTest {

    @AfterEach
    public void resetBackendSelectionProvider() {
        BackendSelectionManager.resetProviderForTest();
    }

    @Test
    public void testGroupCommitPlanRequiresExplicitMasterValidation() throws Exception {
        ConnectContext context = mockConnectContext();
        Env env = context.getEnv();
        Mockito.when(env.getSelfNode()).thenReturn(new HostInfo("127.0.0.1", 9010));
        TMasterOpResult result = new TMasterOpResult().setMaxJournalId(200L);
        TestingMasterOpExecutor executor = new TestingMasterOpExecutor(context, result);
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            Assertions.assertThrows(LoadException.class,
                    () -> executor.syncJournalAndGetGroupCommitTableStatus("db", "tbl", 5000));
            result.setStatusCode(TStatusCode.OK.getValue());
            Mockito.when(env.getReplayedJournalId()).thenReturn(200L);
            executor.syncJournalAndGetGroupCommitTableStatus("db", "tbl", 5000);
            JournalObservable journal = env.getJournalObservable();
            Mockito.verifyNoInteractions(journal);
            Mockito.when(env.getReplayedJournalId()).thenReturn(199L);
            executor.syncJournalAndGetGroupCommitTableStatus("db", "tbl", 5000);
            Mockito.verify(journal).waitOn(Mockito.eq(200L),
                    Mockito.intThat(timeout -> timeout > 0 && timeout <= 5000));
            Mockito.doThrow(new DdlException("replay timeout")).when(journal)
                    .waitOn(Mockito.eq(200L), Mockito.anyInt());
            Assertions.assertThrows(DdlException.class,
                    () -> executor.syncJournalAndGetGroupCommitTableStatus("db", "tbl", 5000));
            Assertions.assertTrue(executor.capturedRequest.isSyncJournalOnly());
            Assertions.assertEquals("db", executor.capturedRequest.getDb());
            Assertions.assertEquals("tbl", executor.capturedRequest.getGroupCommitInfo().getTableName());
            Assertions.assertTrue(executor.capturedRequest.getGroupCommitInfo().isCheckTableBlocked());
            Assertions.assertEquals(5000, executor.thriftTimeoutMs);
            result.setMaxJournalId(-1L);
            Assertions.assertThrows(LoadException.class,
                    () -> executor.syncJournalAndGetGroupCommitTableStatus("db", "tbl", 5000));
            result.setMaxJournalId(201L).setStatusCode(TStatusCode.ANALYSIS_ERROR.getValue());
            TMasterOpResult validation = executor.syncJournalAndGetGroupCommitTableStatus("db", "tbl", 5000);
            Mockito.verify(journal).waitOn(Mockito.eq(201L), Mockito.anyInt());
            LoadException noMessage = Assertions.assertThrows(LoadException.class,
                    () -> MasterOpExecutor.checkGroupCommitResult(validation, "Group Commit planning"));
            Assertions.assertTrue(noMessage.getMessage().contains("status code: " + result.getStatusCode()));
            result.setStatusCode(TStatusCode.ANALYSIS_ERROR.getValue())
                    .setErrMessage("insert table 42 is blocked on schema change");
            LoadException blocked = Assertions.assertThrows(LoadException.class,
                    () -> MasterOpExecutor.checkGroupCommitResult(validation, "Group Commit planning"));
            Assertions.assertTrue(blocked.getMessage().contains("insert table 42 is blocked on schema change"));
        }
    }

    @Test
    public void testGroupCommitValidationReportsRpcFailure() throws Exception {
        ConnectContext context = mockConnectContext();
        Env env = context.getEnv();
        Mockito.when(env.getSelfNode()).thenReturn(new HostInfo("127.0.0.1", 9010));
        Exception unavailable = new Exception("catalog not ready");
        Mockito.doThrow(unavailable).when(env).checkReadyOrThrow();
        MasterOpExecutor executor = new MasterOpExecutor(context);
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            LoadException failure = Assertions.assertThrows(
                    LoadException.class,
                    () -> executor.syncJournalAndGetGroupCommitTableStatus("db", "tbl", 5000));
            Assertions.assertSame(unavailable, failure.getCause());
        }
    }

    @Test
    public void testGroupCommitLoadBackendChecksForwardResultStatusCode() throws Exception {
        TMasterOpResult result = new TMasterOpResult();
        result.setStatusCode(1);
        ConnectContext context = mockConnectContext();
        Env env = context.getEnv();
        Mockito.when(env.getSelfNode()).thenReturn(new HostInfo("127.0.0.1", 9010));
        MasterOpExecutor executor = new TestingMasterOpExecutor(context, result);

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);

            LoadException exception = Assertions.assertThrows(LoadException.class,
                    () -> executor.getGroupCommitLoadBeId(1L, ""));

            Assertions.assertTrue(exception.getMessage().contains("status code: 1"));
        }
    }

    @Test
    public void testGroupCommitLoadBackendDoesNotWaitJournalReplay() throws Exception {
        TMasterOpResult result = new TMasterOpResult();
        result.setGroupCommitLoadBeId(123L);
        result.setMaxJournalId(456L);
        Env env = Mockito.mock(Env.class);
        JournalObservable journalObservable = Mockito.mock(JournalObservable.class);
        ConnectContext context = Mockito.mock(ConnectContext.class);
        Mockito.when(context.getEnv()).thenReturn(env);
        Mockito.when(context.getExecTimeoutS()).thenReturn(1);
        Mockito.when(env.getMasterHost()).thenReturn("127.0.0.1");
        Mockito.when(env.getMasterRpcPort()).thenReturn(9010);
        Mockito.when(env.getJournalObservable()).thenReturn(journalObservable);
        Mockito.when(env.getSelfNode()).thenReturn(new HostInfo("127.0.0.1", 9010));
        MasterOpExecutor executor = new TestingMasterOpExecutor(context, result);

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);

            Assertions.assertEquals(123L, executor.getGroupCommitLoadBeId(1L, ""));

            Mockito.verify(journalObservable, Mockito.never()).waitOn(Mockito.anyLong(), Mockito.anyInt());
        }
    }

    @Test
    public void testGroupCommitLoadBackendRequestAlwaysSignalsErrorResultCapability() throws Exception {
        TMasterOpResult result = new TMasterOpResult();
        result.setGroupCommitLoadBeId(123L);
        ConnectContext context = mockConnectContext();
        Env env = context.getEnv();
        Mockito.when(env.getSelfNode()).thenReturn(new HostInfo("127.0.0.1", 9010));
        TestingMasterOpExecutor executor = new TestingMasterOpExecutor(context, result);

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);

            executor.getGroupCommitLoadBeId(1L, "");

            Assertions.assertTrue(executor.capturedRequest.getGroupCommitInfo().isSupportsSelectionErrorResult());
        }
    }

    @Test
    public void testGroupCommitUpdateLoadDataDoesNotWaitJournalReplay() throws Exception {
        TMasterOpResult result = new TMasterOpResult();
        result.setMaxJournalId(456L);
        Env env = Mockito.mock(Env.class);
        JournalObservable journalObservable = Mockito.mock(JournalObservable.class);
        ConnectContext context = Mockito.mock(ConnectContext.class);
        Mockito.when(context.getEnv()).thenReturn(env);
        Mockito.when(context.getExecTimeoutS()).thenReturn(1);
        Mockito.when(env.getMasterHost()).thenReturn("127.0.0.1");
        Mockito.when(env.getMasterRpcPort()).thenReturn(9010);
        Mockito.when(env.getJournalObservable()).thenReturn(journalObservable);
        Mockito.when(env.getSelfNode()).thenReturn(new HostInfo("127.0.0.1", 9010));
        MasterOpExecutor executor = new TestingMasterOpExecutor(context, result);

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);

            executor.updateLoadData(1L, 2L);

            Mockito.verify(journalObservable, Mockito.never()).waitOn(Mockito.anyLong(), Mockito.anyInt());
        }
    }

    @Test
    public void testForwardResultExposesAuditBackendIds() throws Exception {
        TMasterOpResult result = new TMasterOpResult();
        result.setMaxJournalId(0L);
        result.setAuditStatisticsBackendIds(List.of(10001L, 10002L));
        ConnectContext context = mockConnectContext();
        Env env = context.getEnv();
        Mockito.when(env.getSelfNode()).thenReturn(new HostInfo("127.0.0.1", 9010));
        TestingMasterOpExecutor executor = new TestingMasterOpExecutor(context, result);

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);

            executor.installForwardResult();

            Assertions.assertEquals(Set.of(10001L, 10002L), executor.getAuditStatisticsBackendIds());
        }
    }

    @Test
    public void testCancelDoesNotEraseForwardedAuditBackendIds() throws Exception {
        TMasterOpResult queryResult = new TMasterOpResult();
        queryResult.setAuditStatisticsBackendIds(List.of(10001L, 10002L));
        ConnectContext context = mockConnectContext();
        Mockito.when(context.queryId()).thenReturn(new TUniqueId(1L, 2L));
        Mockito.when(context.getDatabase()).thenReturn("db");
        Mockito.when(context.getQualifiedUser()).thenReturn("user");
        Env env = context.getEnv();
        Mockito.when(env.getSelfNode()).thenReturn(new HostInfo("127.0.0.1", 9010));
        TestingMasterOpExecutor executor = new TestingMasterOpExecutor(context, queryResult);
        executor.cancelResult = new TMasterOpResult();
        executor.installForwardResult();

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            executor.cancel();
        }

        Assertions.assertEquals(Set.of(10001L, 10002L), executor.getAuditStatisticsBackendIds());
    }

    @Test
    public void testDisabledLoadSelectionDoesNotPopulateForwardedInfo() {
        ConnectContext context = new ConnectContext();
        TGroupCommitInfo info = new TGroupCommitInfo();
        DisabledLoadSelectionPolicy policy = new DisabledLoadSelectionPolicy();

        BackendSelectionManager.setProviderForTest(policy);

        MasterOpExecutor.setGroupCommitLoadSelectionHint(info, context);

        Assertions.assertFalse(info.isSetLoadSelectionPreferredKey());
        Assertions.assertFalse(info.isSetLoadSelectionMode());
        Assertions.assertEquals(0, policy.getLoadSelectionHintCalls);
    }

    @Test
    public void testEnabledLoadSelectionPopulatesForwardedInfo() {
        ConnectContext context = new ConnectContext();
        TGroupCommitInfo info = new TGroupCommitInfo();
        EnabledLoadSelectionPolicy policy = new EnabledLoadSelectionPolicy();

        BackendSelectionManager.setProviderForTest(policy);

        MasterOpExecutor.setGroupCommitLoadSelectionHint(info, context);

        Assertions.assertEquals("key_a", info.getLoadSelectionPreferredKey());
        Assertions.assertEquals(BackendSelection.Mode.PREFER.name(), info.getLoadSelectionMode());
        Assertions.assertEquals(1, policy.getLoadSelectionHintCalls);
    }

    private static final class DisabledLoadSelectionPolicy implements BackendSelectionProvider {
        private int getLoadSelectionHintCalls;

        @Override
        public boolean isLoadSelectionEnabled(ConnectContext context) {
            return false;
        }

        @Override
        public BackendSelection.SelectionHint getLoadSelectionHint(ConnectContext context) {
            getLoadSelectionHintCalls++;
            throw new AssertionError("load selection decision should not be resolved when disabled");
        }
    }

    private static final class EnabledLoadSelectionPolicy implements BackendSelectionProvider {
        private int getLoadSelectionHintCalls;

        @Override
        public boolean isLoadSelectionEnabled(ConnectContext context) {
            return true;
        }

        @Override
        public BackendSelection.SelectionHint getLoadSelectionHint(ConnectContext context) {
            getLoadSelectionHintCalls++;
            return new BackendSelection.SelectionHint("key_a",
                    BackendSelection.Mode.PREFER, "test");
        }
    }

    private ConnectContext mockConnectContext() {
        Env env = Mockito.mock(Env.class);
        ConnectContext context = Mockito.mock(ConnectContext.class);
        Mockito.when(context.getEnv()).thenReturn(env);
        Mockito.when(context.getExecTimeoutS()).thenReturn(1);
        Mockito.when(env.getMasterHost()).thenReturn("127.0.0.1");
        Mockito.when(env.getMasterRpcPort()).thenReturn(9010);
        Mockito.when(env.getJournalObservable()).thenReturn(Mockito.mock(JournalObservable.class));
        return context;
    }

    private static final class TestingMasterOpExecutor extends MasterOpExecutor {
        private final TMasterOpResult forwardResult;
        private TMasterOpResult cancelResult;
        private TMasterOpRequest capturedRequest;

        private TestingMasterOpExecutor(ConnectContext context, TMasterOpResult forwardResult) {
            super(context);
            this.forwardResult = forwardResult;
        }

        @Override
        protected TMasterOpResult forward(TMasterOpRequest params) {
            capturedRequest = params;
            return params.isCancelQeury() && cancelResult != null ? cancelResult : forwardResult;
        }

        private void installForwardResult() {
            result = forwardResult;
        }
    }
}
