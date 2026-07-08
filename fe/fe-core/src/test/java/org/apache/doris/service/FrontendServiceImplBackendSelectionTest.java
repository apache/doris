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

package org.apache.doris.service;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.LoadException;
import org.apache.doris.load.GroupCommitManager;
import org.apache.doris.mysql.authenticate.TestLogAppender;
import org.apache.doris.resource.BackendSelection;
import org.apache.doris.resource.BackendSelectionPolicy;
import org.apache.doris.resource.BackendSelectionPolicyFactory;
import org.apache.doris.thrift.TGroupCommitInfo;
import org.apache.doris.thrift.TMasterOpRequest;
import org.apache.doris.thrift.TMasterOpResult;

import org.apache.logging.log4j.Level;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.Method;

public class FrontendServiceImplBackendSelectionTest {
    @Test
    public void testUnsetForwardedGroupCommitSelectionDoesNotResolveDecision() {
        TGroupCommitInfo info = new TGroupCommitInfo();
        ForwardedLoadSelectionPolicy policy = new ForwardedLoadSelectionPolicy();

        try (MockedStatic<BackendSelectionPolicyFactory> mockedFactory =
                Mockito.mockStatic(BackendSelectionPolicyFactory.class)) {
            mockedFactory.when(BackendSelectionPolicyFactory::get).thenReturn(policy);

            Assert.assertNull(FrontendServiceImpl.forwardedGroupCommitLoadSelectionHint(info));
            Assert.assertEquals(0, policy.forwardedLoadSelectionHintCalls);
        }
    }

    @Test
    public void testForwardedGroupCommitSelectionUsesSetFields() {
        TGroupCommitInfo info = new TGroupCommitInfo();
        info.setLoadSelectionPreferredKey("key_a");
        info.setLoadSelectionMode(BackendSelection.Mode.PREFER.name());
        ForwardedLoadSelectionPolicy policy = new ForwardedLoadSelectionPolicy();

        try (MockedStatic<BackendSelectionPolicyFactory> mockedFactory =
                Mockito.mockStatic(BackendSelectionPolicyFactory.class)) {
            mockedFactory.when(BackendSelectionPolicyFactory::get).thenReturn(policy);

            Assert.assertSame(policy.decision, FrontendServiceImpl.forwardedGroupCommitLoadSelectionHint(info));
            Assert.assertEquals(1, policy.forwardedLoadSelectionHintCalls);
            Assert.assertEquals("key_a", policy.preferredKey);
            Assert.assertEquals(BackendSelection.Mode.PREFER.name(), policy.mode);
        }
    }

    @Test
    public void testGroupCommitLoadBackendResultDoesNotSetMaxJournalId() throws Exception {
        Env env = Mockito.mock(Env.class);
        GroupCommitManager manager = Mockito.mock(GroupCommitManager.class);
        Mockito.when(env.getGroupCommitManager()).thenReturn(manager);
        Mockito.when(manager.selectBackendForGroupCommitInternal(Mockito.eq(10L), Mockito.eq("cluster_a"),
                Mockito.<BackendSelection.SelectionHint>isNull())).thenReturn(123L);
        TGroupCommitInfo info = new TGroupCommitInfo();
        info.setGroupCommitLoadTableId(10L);
        info.setCluster("cluster_a");
        FrontendServiceImpl service = new FrontendServiceImpl(Mockito.mock(ExecuteEnv.class));

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);

            TMasterOpResult result = invokeHandleGroupCommitLoadBeId(service, info);

            Assert.assertEquals(123L, result.getGroupCommitLoadBeId());
            Assert.assertFalse(result.isSetMaxJournalId());
        }
    }

    @Test
    public void testGroupCommitLoadBackendSelectionFailureLogsWarn() throws Exception {
        Env env = Mockito.mock(Env.class);
        GroupCommitManager manager = Mockito.mock(GroupCommitManager.class);
        Mockito.when(env.getGroupCommitManager()).thenReturn(manager);
        Mockito.when(manager.selectBackendForGroupCommitInternal(Mockito.eq(10L), Mockito.eq("cluster_a"),
                Mockito.<BackendSelection.SelectionHint>isNull())).thenThrow(new LoadException("no backend"));
        TGroupCommitInfo info = new TGroupCommitInfo();
        info.setGroupCommitLoadTableId(10L);
        info.setCluster("cluster_a");
        FrontendServiceImpl service = new FrontendServiceImpl(Mockito.mock(ExecuteEnv.class));

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class);
                TestLogAppender appender = TestLogAppender.attach(FrontendServiceImpl.class, Level.WARN)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);

            TMasterOpResult result = invokeHandleGroupCommitLoadBeId(service, info);

            Assert.assertEquals(1, result.getStatusCode());
            Assert.assertTrue(result.getErrMessage().contains("no backend"));
            Assert.assertTrue(appender.contains(Level.WARN,
                    "failed to select backend for forwarded group commit load, tableId=10, cluster=cluster_a"));
        }
    }

    @Test
    public void testGroupCommitUpdateLoadDataResultDoesNotSetMaxJournalId() throws Exception {
        Env env = Mockito.mock(Env.class);
        GroupCommitManager manager = Mockito.mock(GroupCommitManager.class);
        Mockito.when(env.getGroupCommitManager()).thenReturn(manager);
        TGroupCommitInfo info = new TGroupCommitInfo();
        info.setUpdateLoadData(true);
        info.setTableId(10L);
        info.setReceiveData(20L);
        TMasterOpRequest request = new TMasterOpRequest();
        request.setGroupCommitInfo(info);
        FrontendServiceImpl service = new FrontendServiceImpl(Mockito.mock(ExecuteEnv.class));

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);

            TMasterOpResult result = invokeHandleForwardShortcut(service, request);

            Assert.assertFalse(result.isSetMaxJournalId());
            Mockito.verify(manager).updateLoadData(10L, 20L);
        }
    }

    private static TMasterOpResult invokeHandleGroupCommitLoadBeId(
            FrontendServiceImpl service, TGroupCommitInfo info) throws Exception {
        Method method = FrontendServiceImpl.class.getDeclaredMethod(
                "handleGroupCommitLoadBeId", TGroupCommitInfo.class);
        method.setAccessible(true);
        return (TMasterOpResult) method.invoke(service, info);
    }

    private static TMasterOpResult invokeHandleForwardShortcut(
            FrontendServiceImpl service, TMasterOpRequest request) throws Exception {
        Method method = FrontendServiceImpl.class.getDeclaredMethod(
                "handleForwardShortcut", TMasterOpRequest.class);
        method.setAccessible(true);
        return (TMasterOpResult) method.invoke(service, request);
    }

    private static final class ForwardedLoadSelectionPolicy implements BackendSelectionPolicy {
        private final BackendSelection.SelectionHint decision =
                new BackendSelection.SelectionHint("key_a", BackendSelection.Mode.PREFER, "test");
        private int forwardedLoadSelectionHintCalls;
        private String preferredKey;
        private String mode;

        @Override
        public BackendSelection.SelectionHint getForwardedLoadSelectionHint(String preferredKey, String mode) {
            forwardedLoadSelectionHintCalls++;
            this.preferredKey = preferredKey;
            this.mode = mode;
            return decision;
        }
    }
}
