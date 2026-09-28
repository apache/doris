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
import org.apache.doris.common.Config;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.system.Backend;

import com.google.common.collect.ImmutableMap;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.concurrent.atomic.AtomicReference;

public class GroupCommitManagerTest {
    private static final long TABLE_ID = 100L;
    private static final String VIRTUAL_CLUSTER = "virtual_cluster";
    private static final String PHYSICAL_CLUSTER_A = "physical_cluster_a";
    private static final String PHYSICAL_CLUSTER_B = "physical_cluster_b";
    private static final long BACKEND_A_ID = 10001L;
    private static final long BACKEND_B_ID = 10002L;

    private String originalCloudUniqueId;
    private String originalDeployMode;
    private Env currentEnv;
    private InternalCatalog internalCatalog;
    private OlapTable table;
    private CloudSystemInfoService systemInfoService;

    @Before
    public void setUp() {
        originalCloudUniqueId = Config.cloud_unique_id;
        originalDeployMode = Config.deploy_mode;
        Config.cloud_unique_id = "test_cloud_unique_id";

        currentEnv = Mockito.mock(Env.class);
        internalCatalog = Mockito.mock(InternalCatalog.class);
        table = Mockito.mock(OlapTable.class);
        systemInfoService = Mockito.mock(CloudSystemInfoService.class);

        Mockito.when(currentEnv.getInternalCatalog()).thenReturn(internalCatalog);
        Mockito.when(internalCatalog.getTableByTableId(TABLE_ID)).thenReturn(table);
        Mockito.when(table.getGroupCommitDataBytes()).thenReturn(1024);
        Mockito.when(table.getGroupCommitIntervalMs()).thenReturn(1000);
    }

    @After
    public void tearDown() {
        Config.cloud_unique_id = originalCloudUniqueId;
        Config.deploy_mode = originalDeployMode;
    }

    @Test
    public void testVirtualComputeGroupUsesActiveBackendsForCacheAndFailover() throws Exception {
        Backend backendA = createBackend(BACKEND_A_ID, PHYSICAL_CLUSTER_A);
        Backend backendB = createBackend(BACKEND_B_ID, PHYSICAL_CLUSTER_B);
        AtomicReference<ImmutableMap<Long, Backend>> activeBackends =
                new AtomicReference<>(ImmutableMap.of(BACKEND_A_ID, backendA));

        Mockito.when(systemInfoService.getCloudIdToBackend(VIRTUAL_CLUSTER))
                .thenAnswer(invocation -> activeBackends.get());
        Mockito.when(systemInfoService.getBackendInCurrentCluster(
                Mockito.eq(VIRTUAL_CLUSTER), Mockito.anyLong()))
                .thenAnswer(invocation -> activeBackends.get().get(invocation.getArgument(1, Long.class)));

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(currentEnv);
            mockedEnv.when(Env::getCurrentSystemInfo).thenReturn(systemInfoService);

            GroupCommitManager manager = new GroupCommitManager();
            Assert.assertEquals(BACKEND_A_ID,
                    manager.selectBackendForGroupCommitInternal(TABLE_ID, VIRTUAL_CLUSTER));
            Mockito.verify(systemInfoService, Mockito.never()).getPhysicalCluster(Mockito.anyString());
            Mockito.verify(systemInfoService, Mockito.never()).getBackend(Mockito.anyLong());

            Mockito.clearInvocations(systemInfoService);
            Assert.assertEquals(BACKEND_A_ID,
                    manager.selectBackendForGroupCommitInternal(TABLE_ID, VIRTUAL_CLUSTER));
            Mockito.verify(systemInfoService).getBackendInCurrentCluster(VIRTUAL_CLUSTER, BACKEND_A_ID);
            Mockito.verify(systemInfoService, Mockito.never()).getCloudIdToBackend(Mockito.anyString());
            Mockito.verify(systemInfoService, Mockito.never()).getPhysicalCluster(Mockito.anyString());
            Mockito.verify(systemInfoService, Mockito.never()).getBackend(Mockito.anyLong());

            Mockito.clearInvocations(systemInfoService);
            activeBackends.set(ImmutableMap.of(BACKEND_B_ID, backendB));

            Assert.assertEquals(BACKEND_B_ID,
                    manager.selectBackendForGroupCommitInternal(TABLE_ID, VIRTUAL_CLUSTER));
            Mockito.verify(systemInfoService).getBackendInCurrentCluster(VIRTUAL_CLUSTER, BACKEND_A_ID);
            Mockito.verify(systemInfoService).getCloudIdToBackend(VIRTUAL_CLUSTER);
        }

        Mockito.verify(systemInfoService, Mockito.never()).getPhysicalCluster(Mockito.anyString());
        Mockito.verify(systemInfoService, Mockito.never()).getBackend(Mockito.anyLong());
    }

    @Test
    public void testLoadDisabledCachedBackendIsReplacedFromActiveBackends() throws Exception {
        Backend backendA1 = createBackend(BACKEND_A_ID, PHYSICAL_CLUSTER_A);
        Backend backendA2 = createBackend(BACKEND_B_ID, PHYSICAL_CLUSTER_A);
        AtomicReference<ImmutableMap<Long, Backend>> activeBackends =
                new AtomicReference<>(ImmutableMap.of(BACKEND_A_ID, backendA1));

        Mockito.when(systemInfoService.getCloudIdToBackend(VIRTUAL_CLUSTER))
                .thenAnswer(invocation -> activeBackends.get());
        Mockito.when(systemInfoService.getBackendInCurrentCluster(
                Mockito.eq(VIRTUAL_CLUSTER), Mockito.anyLong()))
                .thenAnswer(invocation -> activeBackends.get().get(invocation.getArgument(1, Long.class)));

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(currentEnv);
            mockedEnv.when(Env::getCurrentSystemInfo).thenReturn(systemInfoService);

            GroupCommitManager manager = new GroupCommitManager();
            Assert.assertEquals(BACKEND_A_ID,
                    manager.selectBackendForGroupCommitInternal(TABLE_ID, VIRTUAL_CLUSTER));

            Mockito.clearInvocations(systemInfoService);
            activeBackends.set(ImmutableMap.of(BACKEND_A_ID, backendA1, BACKEND_B_ID, backendA2));
            backendA1.setLoadDisabled(true);

            Assert.assertEquals(BACKEND_B_ID,
                    manager.selectBackendForGroupCommitInternal(TABLE_ID, VIRTUAL_CLUSTER));
            Mockito.verify(systemInfoService).getBackendInCurrentCluster(VIRTUAL_CLUSTER, BACKEND_A_ID);
            Mockito.verify(systemInfoService).getCloudIdToBackend(VIRTUAL_CLUSTER);
        }

        Mockito.verify(systemInfoService, Mockito.never()).getPhysicalCluster(Mockito.anyString());
        Mockito.verify(systemInfoService, Mockito.never()).getBackend(Mockito.anyLong());
    }

    @Test
    public void testLocalGroupCommitStillUsesGlobalBackendLookup() throws Exception {
        Config.cloud_unique_id = "";
        Config.deploy_mode = "";
        Backend backend = createBackend(BACKEND_A_ID, PHYSICAL_CLUSTER_A);

        Mockito.when(systemInfoService.getAllBackendsByAllCluster())
                .thenReturn(ImmutableMap.of(BACKEND_A_ID, backend));
        Mockito.when(systemInfoService.getBackend(BACKEND_A_ID)).thenReturn(backend);

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(currentEnv);
            mockedEnv.when(Env::getCurrentSystemInfo).thenReturn(systemInfoService);

            GroupCommitManager manager = new GroupCommitManager();
            Assert.assertEquals(BACKEND_A_ID,
                    manager.selectBackendForGroupCommitInternal(TABLE_ID, null));
            Assert.assertEquals(BACKEND_A_ID,
                    manager.selectBackendForGroupCommitInternal(TABLE_ID, null));
        }

        Mockito.verify(systemInfoService, Mockito.never()).getPhysicalCluster(Mockito.anyString());
        Mockito.verify(systemInfoService, Mockito.never()).getCloudIdToBackend(Mockito.anyString());
        Mockito.verify(systemInfoService, Mockito.never())
                .getBackendInCurrentCluster(Mockito.any(), Mockito.anyLong());
        Mockito.verify(systemInfoService).getBackend(BACKEND_A_ID);
    }

    private Backend createBackend(long id, String physicalCluster) {
        Backend backend = new Backend(id, "127.0.0.1", 9050);
        backend.setCloudClusterName(physicalCluster);
        backend.setAlive(true);
        return backend;
    }
}
