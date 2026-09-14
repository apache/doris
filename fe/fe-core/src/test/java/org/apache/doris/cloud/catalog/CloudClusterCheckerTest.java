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

package org.apache.doris.cloud.catalog;

import org.apache.doris.catalog.Env;
import org.apache.doris.cloud.CacheHotspotManager;
import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.common.Config;
import org.apache.doris.persist.EditLog;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.Method;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

class CloudClusterCheckerTest {
    private String originalCloudUniqueId;
    private CloudSystemInfoService service;
    private CloudEnv env;
    private MockedStatic<Env> mockedEnv;

    @BeforeEach
    void setUp() {
        originalCloudUniqueId = Config.cloud_unique_id;
        Config.cloud_unique_id = "test_cloud";
        service = Mockito.spy(new CloudSystemInfoService());
        env = Mockito.mock(CloudEnv.class);
        AtomicLong nextId = new AtomicLong(10000);
        Mockito.when(env.getNextId()).thenAnswer(invocation -> nextId.incrementAndGet());
        Mockito.when(env.getEditLog()).thenReturn(Mockito.mock(EditLog.class));
        Mockito.when(env.getCacheHotspotMgr()).thenReturn(Mockito.mock(CacheHotspotManager.class));
        mockedEnv = mockEnv();
    }

    private MockedStatic<Env> mockEnv() {
        MockedStatic<Env> mock = Mockito.mockStatic(Env.class);
        mock.when(Env::getCurrentEnv).thenReturn(env);
        mock.when(Env::getCurrentSystemInfo).thenReturn(service);
        return mock;
    }

    @AfterEach
    void tearDown() {
        mockedEnv.close();
        Config.cloud_unique_id = originalCloudUniqueId;
    }

    @Test
    void testCheckerDeletesRecreatedGroupById() throws Exception {
        Cloud.ClusterPB oldGroup = physical("old", "reused", "127.0.0.1");
        Cloud.ClusterPB newGroup = physical("new", "reused", "127.0.0.2");
        syncPhysical(oldGroup);
        long oldBackendId = service.getCloudClusterIdToBackend(false).get("old").get(0).getId();

        syncPhysical(newGroup);

        Assertions.assertNull(service.getBackend(oldBackendId));
        Assertions.assertNull(service.getComputeGroupById("old"));
        Assertions.assertFalse(service.getCloudClusterIdToBackend(false).containsKey("old"));
        Assertions.assertEquals("new", service.getCloudClusterIdByName("reused"));
        syncPhysical(newGroup);
        Assertions.assertEquals(1, service.getAllBackendsByAllCluster().size());
        Assertions.assertEquals("new", service.getCloudClusterIdByName("reused"));
    }

    @Test
    void testCheckerRenamesGroupAfterOldNameIsReused() throws Exception {
        Cloud.ClusterPB oldGroup = physical("old", "reused", "127.0.0.1");
        Cloud.ClusterPB renamed = oldGroup.toBuilder().setClusterName("renamed").build();
        Cloud.ClusterPB replacement = physical("new", "reused", "127.0.0.2");
        syncPhysical(oldGroup);
        syncPhysical(renamed, replacement);

        Assertions.assertEquals("old", service.getCloudClusterIdByName("renamed"));
        Assertions.assertEquals("new", service.getCloudClusterIdByName("reused"));
        syncPhysical(renamed, replacement);
        Assertions.assertEquals("new", service.getCloudClusterIdByName("reused"));
    }

    @Test
    void testPhysicalRenameDoesNotRemoveAnotherGroupsName() {
        service.addComputeGroup("old", new CloudComputeGroupMeta("old", "reused",
                CloudComputeGroupMeta.ComputeTypeEnum.COMPUTE));
        service.addComputeGroup("new", new CloudComputeGroupMeta("new", "reused",
                CloudComputeGroupMeta.ComputeTypeEnum.COMPUTE));
        service.updateClusterNameToId("renamed", "reused", "old");
        Assertions.assertEquals("new", service.getCloudClusterIdByName("reused"));
        Assertions.assertEquals("old", service.getCloudClusterIdByName("renamed"));
    }

    @Test
    void testVirtualRenameDoesNotRemoveAnotherGroupsName() {
        service.addComputeGroup("old", new CloudComputeGroupMeta("old", "reused",
                CloudComputeGroupMeta.ComputeTypeEnum.VIRTUAL));
        service.addComputeGroup("new", new CloudComputeGroupMeta("new", "reused",
                CloudComputeGroupMeta.ComputeTypeEnum.VIRTUAL));
        service.renameVirtualComputeGroup("old", "reused", new CloudComputeGroupMeta("old", "renamed",
                CloudComputeGroupMeta.ComputeTypeEnum.VIRTUAL));
        Assertions.assertEquals("new", service.getCloudClusterIdByName("reused"));
        Assertions.assertEquals("old", service.getCloudClusterIdByName("renamed"));
    }

    @Test
    void testLaterCycleRepairsNameAfterStaleVirtualSnapshot() throws Exception {
        Cloud.ClusterPB active = physical("active", "active", "127.0.0.1");
        Cloud.ClusterPB standby = physical("standby", "standby", "127.0.0.2");
        Cloud.ClusterPB replacement = physical("new", "reused", "127.0.0.3");
        syncPhysical(active, standby);
        Cloud.GetInstanceResponse stale = instance(active, standby, virtual("old", "reused"));
        CountDownLatch responseFetched = new CountDownLatch(1);
        CountDownLatch applyResponse = new CountDownLatch(1);
        Mockito.doAnswer(invocation -> {
            responseFetched.countDown();
            Assertions.assertTrue(applyResponse.await(30, TimeUnit.SECONDS));
            return stale;
        }).when(service).getCloudInstance();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<?> staleCycle = executor.submit(() -> {
                try (MockedStatic<Env> ignored = mockEnv()) {
                    new CloudInstanceStatusChecker(service).runAfterCatalogReady();
                }
            });
            Assertions.assertTrue(responseFetched.await(30, TimeUnit.SECONDS));
            syncPhysical(active, standby, replacement);
            Assertions.assertEquals("new", service.getCloudClusterIdByName("reused"));
            applyResponse.countDown();
            staleCycle.get(30, TimeUnit.SECONDS);
            Assertions.assertEquals("old", service.getCloudClusterIdByName("reused"));

            // A current physical snapshot must repair even a non-empty, wrong mapping.
            syncPhysical(active, standby, replacement);
            Assertions.assertEquals("new", service.getCloudClusterIdByName("reused"));
            syncInstance(active, standby, replacement);
            Assertions.assertNull(service.getComputeGroupById("old"));
            Assertions.assertEquals("new", service.getCloudClusterIdByName("reused"));
            syncPhysical(active, standby, replacement);
            Assertions.assertEquals("new", service.getCloudClusterIdByName("reused"));
        } finally {
            applyResponse.countDown();
            executor.shutdownNow();
            Assertions.assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS));
        }
    }

    @Test
    void testLaterCyclesRepairMissingPhysicalAndVirtualNames() throws Exception {
        Cloud.ClusterPB active = physical("active", "active", "127.0.0.1");
        Cloud.ClusterPB standby = physical("standby", "standby", "127.0.0.2");
        Cloud.ClusterPB current = physical("new", "reused", "127.0.0.3");
        syncPhysical(active, standby, current);
        syncInstance(active, standby, virtual("old", "reused"));
        syncInstance(active, standby, current);
        Assertions.assertNull(service.getCloudClusterIdByName("reused"));
        syncPhysical(active, standby, current);
        Assertions.assertEquals("new", service.getCloudClusterIdByName("reused"));

        // Reverse the types: an obsolete physical snapshot overwrites a current virtual group.
        syncPhysical(active, standby);
        Cloud.ClusterPB currentVirtual = virtual("virtual", "reused");
        syncInstance(active, standby, currentVirtual);
        syncPhysical(active, standby, current);
        syncPhysical(active, standby);
        Assertions.assertNull(service.getCloudClusterIdByName("reused"));
        syncInstance(active, standby, currentVirtual);
        Assertions.assertEquals("virtual", service.getCloudClusterIdByName("reused"));
        syncInstance(active, standby, currentVirtual);
        Assertions.assertEquals("virtual", service.getCloudClusterIdByName("reused"));
        Assertions.assertNull(service.getComputeGroupById("new"));
    }

    @Test
    void testRejectedVirtualPolicyDoesNotPublishRenamedGroup() throws Exception {
        assertRejectedVirtualRename(virtual("virtual", "renamed").toBuilder()
                .clearClusterPolicy().build());
    }

    @Test
    void testRejectedVirtualSubgroupsDoNotPublishRenamedGroup() throws Exception {
        assertRejectedVirtualRename(virtual("virtual", "renamed").toBuilder()
                .clearClusterNames().addClusterNames("active").build());
    }

    private void assertRejectedVirtualRename(Cloud.ClusterPB rejected) throws Exception {
        Cloud.ClusterPB active = physical("active", "active", "127.0.0.1");
        Cloud.ClusterPB standby = physical("standby", "standby", "127.0.0.2");
        syncPhysical(active, standby);
        syncInstance(active, standby, virtual("virtual", "original"));

        syncInstance(active, standby, rejected);

        Assertions.assertEquals("original", service.getComputeGroupById("virtual").getName());
        Assertions.assertEquals("virtual", service.getCloudClusterIdByName("original"));
        Assertions.assertNull(service.getCloudClusterIdByName("renamed"));

        // The rejected record must not make the still-present group look obsolete.
        // Once it actually disappears from MS, neither name may point to its removed ID.
        syncInstance(active, standby);
        Assertions.assertNull(service.getComputeGroupById("virtual"));
        Assertions.assertNull(service.getCloudClusterIdByName("original"));
        Assertions.assertNull(service.getCloudClusterIdByName("renamed"));
    }

    @Test
    void testAcceptedVirtualRenamePublishesNewName() throws Exception {
        Cloud.ClusterPB active = physical("active", "active", "127.0.0.1");
        Cloud.ClusterPB standby = physical("standby", "standby", "127.0.0.2");
        syncPhysical(active, standby);
        syncInstance(active, standby, virtual("virtual", "original"));

        syncInstance(active, standby, virtual("virtual", "renamed"));

        Assertions.assertEquals("renamed", service.getComputeGroupById("virtual").getName());
        Assertions.assertEquals("virtual", service.getCloudClusterIdByName("renamed"));
        Assertions.assertNull(service.getCloudClusterIdByName("original"));
    }

    @Test
    void testRejectedNewVirtualGroupDoesNotPublishName() {
        syncInstance(virtual("virtual", "rejected").toBuilder().clearClusterPolicy().build());

        Assertions.assertNull(service.getComputeGroupById("virtual"));
        Assertions.assertNull(service.getCloudClusterIdByName("rejected"));
    }

    @Test
    void testVirtualRemovalClearsStaleAliasesButKeepsReusedName() throws Exception {
        Cloud.ClusterPB active = physical("active", "active", "127.0.0.1");
        Cloud.ClusterPB standby = physical("standby", "standby", "127.0.0.2");
        syncPhysical(active, standby);
        syncInstance(active, standby, virtual("virtual", "reused"));
        // Simulate an alias left by an earlier checker that refreshed a rejected rename.
        service.addVirtualClusterInfoToMapsNoLock("virtual", "stale_alias");
        syncPhysical(active, standby, physical("replacement", "reused", "127.0.0.3"));

        syncInstance(active, standby);

        Assertions.assertNull(service.getComputeGroupById("virtual"));
        Assertions.assertNull(service.getCloudClusterIdByName("stale_alias"));
        Assertions.assertEquals("replacement", service.getCloudClusterIdByName("reused"));
    }

    private void syncPhysical(Cloud.ClusterPB... groups) throws Exception {
        Mockito.doReturn(Cloud.GetClusterResponse.newBuilder().setStatus(ok())
                .addAllCluster(List.of(groups)).build()).when(service).getCloudCluster("", "", "");
        // The production reconciliation entry, including add, delete, diff and validation.
        Method method = CloudClusterChecker.class.getDeclaredMethod("checkCloudBackends");
        method.setAccessible(true);
        method.invoke(new CloudClusterChecker(service));
    }

    private void syncInstance(Cloud.ClusterPB... groups) {
        Mockito.doReturn(instance(groups)).when(service).getCloudInstance();
        new CloudInstanceStatusChecker(service).runAfterCatalogReady();
    }

    private Cloud.GetInstanceResponse instance(Cloud.ClusterPB... groups) {
        return Cloud.GetInstanceResponse.newBuilder().setStatus(ok())
                .setInstance(Cloud.InstanceInfoPB.newBuilder().setStatus(Cloud.InstanceInfoPB.Status.NORMAL)
                        .addAllClusters(List.of(groups))).build();
    }

    private Cloud.MetaServiceResponseStatus ok() {
        return Cloud.MetaServiceResponseStatus.newBuilder().setCode(Cloud.MetaServiceCode.OK).build();
    }

    private Cloud.ClusterPB physical(String id, String name, String ip) {
        return Cloud.ClusterPB.newBuilder().setClusterId(id).setClusterName(name)
                .setType(Cloud.ClusterPB.Type.COMPUTE).setClusterStatus(Cloud.ClusterStatus.NORMAL)
                .addNodes(Cloud.NodeInfoPB.newBuilder().setIp(ip).setHeartbeatPort(9050)
                        .setCloudUniqueId(id).setStatus(Cloud.NodeStatusPB.NODE_STATUS_RUNNING)).build();
    }

    private Cloud.ClusterPB virtual(String id, String name) {
        return Cloud.ClusterPB.newBuilder().setClusterId(id).setClusterName(name)
                .setType(Cloud.ClusterPB.Type.VIRTUAL).addAllClusterNames(List.of("active", "standby"))
                .setClusterPolicy(Cloud.ClusterPolicy.newBuilder().setType(Cloud.ClusterPolicy.PolicyType.ActiveStandby)
                        .setActiveClusterName("active")
                        .addStandbyClusterNames("standby")).build();
    }
}
