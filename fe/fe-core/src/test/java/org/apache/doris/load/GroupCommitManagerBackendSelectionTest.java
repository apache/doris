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

package org.apache.doris.load;

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.resource.BackendSelection;
import org.apache.doris.resource.BackendSelectionPolicy;
import org.apache.doris.resource.BackendSelectionPolicyFactory;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;

import com.google.common.cache.Cache;
import com.google.common.collect.ImmutableMap;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.List;

public class GroupCommitManagerBackendSelectionTest {
    private final String originalCloudUniqueId = Config.cloud_unique_id;
    private final String originalDeployMode = Config.deploy_mode;

    @After
    public void tearDown() {
        Config.cloud_unique_id = originalCloudUniqueId;
        Config.deploy_mode = originalDeployMode;
    }

    @Test
    public void testDisabledLoadSelectionDoesNotResolveDecision() {
        ConnectContext context = new ConnectContext();
        DisabledLoadSelectionPolicy policy = new DisabledLoadSelectionPolicy();

        try (MockedStatic<BackendSelectionPolicyFactory> mockedFactory =
                Mockito.mockStatic(BackendSelectionPolicyFactory.class)) {
            mockedFactory.when(BackendSelectionPolicyFactory::get).thenReturn(policy);

            Assert.assertNull(GroupCommitManager.getGroupCommitLoadSelectionHint(context));
            Assert.assertEquals(0, policy.getLoadSelectionHintCalls);
        }
    }

    @Test
    public void testEffectiveLoadSelectionCacheUsesSelectionKey() throws Exception {
        Config.cloud_unique_id = "";
        Config.deploy_mode = "";
        long tableId = 10001L;
        GroupCommitManager manager = new GroupCommitManager();
        CountingLoadSelectionPolicy policy = new CountingLoadSelectionPolicy();
        Env env = mockEnv(tableId);
        SystemInfoService systemInfoService = mockSystemInfoService();

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class);
                MockedStatic<BackendSelectionPolicyFactory> mockedFactory =
                        Mockito.mockStatic(BackendSelectionPolicyFactory.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            mockedEnv.when(Env::getCurrentSystemInfo).thenReturn(systemInfoService);
            mockedFactory.when(BackendSelectionPolicyFactory::get).thenReturn(policy);

            long firstBackendId = manager.selectBackendForGroupCommitInternal(tableId, "", policy.decision);
            long secondBackendId = manager.selectBackendForGroupCommitInternal(tableId, "", policy.decision);
            long otherBackendId = manager.selectBackendForGroupCommitInternal(tableId, "", policy.otherDecision);
            long cachedOtherBackendId = manager.selectBackendForGroupCommitInternal(
                    tableId, "", policy.otherDecision);

            Assert.assertEquals(1L, firstBackendId);
            Assert.assertEquals(1L, secondBackendId);
            Assert.assertEquals(1L, otherBackendId);
            Assert.assertEquals(1L, cachedOtherBackendId);
            Assert.assertEquals(2, policy.orderLoadCandidatesCalls);
        }
    }

    @Test
    public void testRequiredLoadSelectionDoesNotReuseCachedBackend() throws Exception {
        Config.cloud_unique_id = "";
        Config.deploy_mode = "";
        long tableId = 10005L;
        GroupCommitManager manager = new GroupCommitManager();
        RequiredLoadSelectionPolicy policy = new RequiredLoadSelectionPolicy();
        Env env = mockEnv(tableId);
        SystemInfoService systemInfoService = mockSystemInfoService();

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class);
                MockedStatic<BackendSelectionPolicyFactory> mockedFactory =
                        Mockito.mockStatic(BackendSelectionPolicyFactory.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            mockedEnv.when(Env::getCurrentSystemInfo).thenReturn(systemInfoService);
            mockedFactory.when(BackendSelectionPolicyFactory::get).thenReturn(policy);

            Assert.assertEquals(1L, manager.selectBackendForGroupCommitInternal(tableId, "", policy.decision));
            Assert.assertEquals(1L, manager.selectBackendForGroupCommitInternal(tableId, "", policy.decision));
            Assert.assertEquals(2, policy.partitionCalls);
        }
    }

    @Test
    public void testNullSelectionHintDoesNotReachPolicyPreferencePredicate() throws Exception {
        Config.cloud_unique_id = "";
        Config.deploy_mode = "";
        long tableId = 10004L;
        GroupCommitManager manager = new GroupCommitManager();
        NullRejectingLoadSelectionPolicy policy = new NullRejectingLoadSelectionPolicy();
        Env env = mockEnv(tableId);
        SystemInfoService systemInfoService = mockSystemInfoService();

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class);
                MockedStatic<BackendSelectionPolicyFactory> mockedFactory =
                        Mockito.mockStatic(BackendSelectionPolicyFactory.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            mockedEnv.when(Env::getCurrentSystemInfo).thenReturn(systemInfoService);
            mockedFactory.when(BackendSelectionPolicyFactory::get).thenReturn(policy);

            Assert.assertEquals(1L, manager.selectBackendForGroupCommitInternal(tableId, "", null));
        }
    }

    @Test
    public void testPreferenceKeyedBackendCacheStaysBounded() throws Exception {
        GroupCommitManager manager = new GroupCommitManager();
        Field field = GroupCommitManager.class.getDeclaredField("tableToBeMap");
        field.setAccessible(true);
        @SuppressWarnings("unchecked")
        Cache<String, Long> cache = (Cache<String, Long>) field.get(manager);

        for (int i = 0; i < 20000; i++) {
            cache.put("10001#key_" + i + "#PREFER", 1L);
        }
        cache.cleanUp();

        Assert.assertTrue("cache must stay bounded, size=" + cache.size(), cache.size() <= 10000);
    }

    @Test
    public void testCloudGroupCommitIgnoresLoadSelectionDecision() throws Exception {
        Config.cloud_unique_id = "cloud_id";
        Config.deploy_mode = "cloud";
        long tableId = 10003L;
        String cluster = "cluster_a";
        GroupCommitManager manager = new GroupCommitManager();
        CountingLoadSelectionPolicy policy = new CountingLoadSelectionPolicy();
        Env env = mockEnv(tableId);
        CloudSystemInfoService cloudSystemInfoService = Mockito.mock(CloudSystemInfoService.class);
        Backend backend = newBackend(1L);
        backend.setCloudClusterName(cluster);
        Mockito.when(cloudSystemInfoService.getCloudIdToBackend(cluster))
                .thenReturn(ImmutableMap.of(backend.getId(), backend));
        Mockito.when(cloudSystemInfoService.getBackend(backend.getId())).thenReturn(backend);

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class);
                MockedStatic<BackendSelectionPolicyFactory> mockedFactory =
                        Mockito.mockStatic(BackendSelectionPolicyFactory.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            mockedEnv.when(Env::getCurrentSystemInfo).thenReturn(cloudSystemInfoService);
            mockedFactory.when(BackendSelectionPolicyFactory::get).thenReturn(policy);

            long firstBackendId = manager.selectBackendForGroupCommitInternal(tableId, cluster, policy.decision);
            long secondBackendId = manager.selectBackendForGroupCommitInternal(tableId, cluster, policy.decision);

            Assert.assertEquals(1L, firstBackendId);
            Assert.assertEquals(1L, secondBackendId);
            Assert.assertEquals(0, policy.orderLoadCandidatesCalls);
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

    private static final class CountingLoadSelectionPolicy implements BackendSelectionPolicy {
        private final BackendSelection.SelectionHint decision =
                new BackendSelection.SelectionHint("key_a", BackendSelection.Mode.PREFER, "test");
        private final BackendSelection.SelectionHint otherDecision =
                new BackendSelection.SelectionHint("key_b", BackendSelection.Mode.PREFER, "test");
        private int orderLoadCandidatesCalls;

        @Override
        public boolean hasLoadSelectionPreference(BackendSelection.SelectionHint decision) {
            return decision != null && !decision.getPreferredKey().isEmpty();
        }

        @Override
        public List<Backend> orderLoadCandidates(BackendSelection.SelectionHint decision,
                List<Backend> candidates) {
            orderLoadCandidatesCalls++;
            return candidates;
        }
    }

    private static final class NullRejectingLoadSelectionPolicy implements BackendSelectionPolicy {
        @Override
        public boolean hasLoadSelectionPreference(BackendSelection.SelectionHint decision) {
            throw new AssertionError("null selection hints must be handled by BackendSelectionService");
        }
    }

    private static final class RequiredLoadSelectionPolicy implements BackendSelectionPolicy {
        private final BackendSelection.SelectionHint decision =
                new BackendSelection.SelectionHint("key_a", BackendSelection.Mode.REQUIRE, "test");
        private int partitionCalls;

        @Override
        public BackendSelection.CandidateSelection<Backend> partitionRequiredLoadCandidates(
                BackendSelection.SelectionHint decision, List<Backend> candidates) {
            partitionCalls++;
            return new BackendSelection.CandidateSelection<>(candidates, java.util.Collections.emptyList());
        }
    }

    private Env mockEnv(long tableId) {
        Env env = Mockito.mock(Env.class);
        InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
        OlapTable table = Mockito.mock(OlapTable.class);
        Mockito.when(env.getInternalCatalog()).thenReturn(catalog);
        Mockito.when(catalog.getTableByTableId(tableId)).thenReturn(table);
        Mockito.when(table.getGroupCommitDataBytes()).thenReturn(1024 * 1024);
        Mockito.when(table.getGroupCommitIntervalMs()).thenReturn(1000);
        return env;
    }

    private SystemInfoService mockSystemInfoService() throws AnalysisException {
        SystemInfoService systemInfoService = Mockito.mock(SystemInfoService.class);
        Backend backend = newBackend(1L);
        Mockito.when(systemInfoService.getAllBackendsByAllCluster())
                .thenReturn(ImmutableMap.of(backend.getId(), backend));
        Mockito.when(systemInfoService.getBackend(backend.getId())).thenReturn(backend);
        return systemInfoService;
    }

    private Backend newBackend(long backendId) {
        Backend backend = new Backend(backendId, "127.0.0.1", 9050);
        backend.setAlive(true);
        return backend;
    }
}
