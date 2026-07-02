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
import org.apache.doris.resource.ResourceGroupAffinity;
import org.apache.doris.resource.ResourceGroupAffinityPolicy;
import org.apache.doris.resource.ResourceGroupAffinityPolicyFactory;
import org.apache.doris.system.SystemInfoService.HostInfo;
import org.apache.doris.thrift.TGroupCommitInfo;
import org.apache.doris.thrift.TMasterOpRequest;
import org.apache.doris.thrift.TMasterOpResult;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

public class MasterOpExecutorAffinityTest {
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
    public void testDisabledLoadAffinityDoesNotPopulateForwardedInfo() {
        ConnectContext context = new ConnectContext();
        TGroupCommitInfo info = new TGroupCommitInfo();
        DisabledLoadAffinityPolicy policy = new DisabledLoadAffinityPolicy();

        try (MockedStatic<ResourceGroupAffinityPolicyFactory> mockedFactory =
                Mockito.mockStatic(ResourceGroupAffinityPolicyFactory.class)) {
            mockedFactory.when(ResourceGroupAffinityPolicyFactory::get).thenReturn(policy);

            MasterOpExecutor.setGroupCommitLoadAffinity(info, context);

            Assert.assertFalse(info.isSetLoadAffinityPreferredGroup());
            Assert.assertFalse(info.isSetLoadAffinityPolicy());
            Assert.assertEquals(0, policy.decideForLoadCalls);
        }
    }

    @Test
    public void testEnabledLoadAffinityPopulatesForwardedInfo() {
        ConnectContext context = new ConnectContext();
        TGroupCommitInfo info = new TGroupCommitInfo();
        EnabledLoadAffinityPolicy policy = new EnabledLoadAffinityPolicy();

        try (MockedStatic<ResourceGroupAffinityPolicyFactory> mockedFactory =
                Mockito.mockStatic(ResourceGroupAffinityPolicyFactory.class)) {
            mockedFactory.when(ResourceGroupAffinityPolicyFactory::get).thenReturn(policy);

            MasterOpExecutor.setGroupCommitLoadAffinity(info, context);

            Assert.assertEquals("rg_a", info.getLoadAffinityPreferredGroup());
            Assert.assertEquals(ResourceGroupAffinity.Policy.PREFER_LOCAL.name(), info.getLoadAffinityPolicy());
            Assert.assertEquals(1, policy.decideForLoadCalls);
        }
    }

    private static final class DisabledLoadAffinityPolicy implements ResourceGroupAffinityPolicy {
        private int decideForLoadCalls;

        @Override
        public boolean isLoadAffinityEnabled(ConnectContext context) {
            return false;
        }

        @Override
        public ResourceGroupAffinity.AffinityDecision decideForLoad(ConnectContext context) {
            decideForLoadCalls++;
            throw new AssertionError("load affinity decision should not be resolved when disabled");
        }
    }

    private static final class EnabledLoadAffinityPolicy implements ResourceGroupAffinityPolicy {
        private int decideForLoadCalls;

        @Override
        public boolean isLoadAffinityEnabled(ConnectContext context) {
            return true;
        }

        @Override
        public ResourceGroupAffinity.AffinityDecision decideForLoad(ConnectContext context) {
            decideForLoadCalls++;
            return new ResourceGroupAffinity.AffinityDecision("rg_a",
                    ResourceGroupAffinity.Policy.PREFER_LOCAL, "test");
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

        private TestingMasterOpExecutor(ConnectContext context, TMasterOpResult forwardResult) {
            super(context);
            this.forwardResult = forwardResult;
        }

        @Override
        protected TMasterOpResult forward(TMasterOpRequest params) {
            return forwardResult;
        }
    }
}
