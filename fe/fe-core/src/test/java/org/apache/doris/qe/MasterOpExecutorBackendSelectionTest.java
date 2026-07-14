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
import org.apache.doris.common.LoadException;
import org.apache.doris.resource.BackendSelection;
import org.apache.doris.resource.BackendSelectionPolicy;
import org.apache.doris.resource.BackendSelectionPolicyFactory;
import org.apache.doris.system.SystemInfoService.HostInfo;
import org.apache.doris.thrift.TGroupCommitInfo;
import org.apache.doris.thrift.TMasterOpRequest;
import org.apache.doris.thrift.TMasterOpResult;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

public class MasterOpExecutorBackendSelectionTest {
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

            LoadException exception = Assert.assertThrows(LoadException.class,
                    () -> executor.getGroupCommitLoadBeId(1L, ""));

            Assert.assertTrue(exception.getMessage().contains("status code: 1"));
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

            Assert.assertEquals(123L, executor.getGroupCommitLoadBeId(1L, ""));

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

            Assert.assertTrue(executor.capturedRequest.getGroupCommitInfo().isSupportsSelectionErrorResult());
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
    public void testDisabledLoadSelectionDoesNotPopulateForwardedInfo() {
        ConnectContext context = new ConnectContext();
        TGroupCommitInfo info = new TGroupCommitInfo();
        DisabledLoadSelectionPolicy policy = new DisabledLoadSelectionPolicy();

        try (MockedStatic<BackendSelectionPolicyFactory> mockedFactory =
                Mockito.mockStatic(BackendSelectionPolicyFactory.class)) {
            mockedFactory.when(BackendSelectionPolicyFactory::get).thenReturn(policy);

            MasterOpExecutor.setGroupCommitLoadSelectionHint(info, context);

            Assert.assertFalse(info.isSetLoadSelectionPreferredKey());
            Assert.assertFalse(info.isSetLoadSelectionMode());
            Assert.assertEquals(0, policy.getLoadSelectionHintCalls);
        }
    }

    @Test
    public void testEnabledLoadSelectionPopulatesForwardedInfo() {
        ConnectContext context = new ConnectContext();
        TGroupCommitInfo info = new TGroupCommitInfo();
        EnabledLoadSelectionPolicy policy = new EnabledLoadSelectionPolicy();

        try (MockedStatic<BackendSelectionPolicyFactory> mockedFactory =
                Mockito.mockStatic(BackendSelectionPolicyFactory.class)) {
            mockedFactory.when(BackendSelectionPolicyFactory::get).thenReturn(policy);

            MasterOpExecutor.setGroupCommitLoadSelectionHint(info, context);

            Assert.assertEquals("key_a", info.getLoadSelectionPreferredKey());
            Assert.assertEquals(BackendSelection.Mode.PREFER.name(), info.getLoadSelectionMode());
            Assert.assertEquals(1, policy.getLoadSelectionHintCalls);
        }
    }

    private static final class DisabledLoadSelectionPolicy implements BackendSelectionPolicy {
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

    private static final class EnabledLoadSelectionPolicy implements BackendSelectionPolicy {
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
        private TMasterOpRequest capturedRequest;

        private TestingMasterOpExecutor(ConnectContext context, TMasterOpResult forwardResult) {
            super(context);
            this.forwardResult = forwardResult;
        }

        @Override
        protected TMasterOpResult forward(TMasterOpRequest params) {
            capturedRequest = params;
            return forwardResult;
        }
    }
}
