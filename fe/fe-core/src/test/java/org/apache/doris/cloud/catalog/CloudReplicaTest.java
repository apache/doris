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

import org.apache.doris.catalog.ColocateTableIndex;
import org.apache.doris.catalog.ColocateTableIndex.GroupId;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Replica;
import org.apache.doris.cloud.qe.ComputeGroupException;
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.common.Config;
import org.apache.doris.system.Backend;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class CloudReplicaTest {

    private static final long DB_ID = 10001L;
    private static final long TABLE_ID = 20001L;
    private static final long PARTITION_ID = 30001L;
    private static final long INDEX_ID = 40001L;
    private static final long REPLICA_ID = 50001L;
    private static final long IDX = 0;

    private static final String CLUSTER_ID_1 = "cluster_id_1";
    private static final String CLUSTER_ID_2 = "cluster_id_2";
    private static final String CLUSTER_NAME_1 = "cluster_1";

    private MockedStatic<Env> envMockedStatic;
    private Env mockEnv;
    private ColocateTableIndex mockColocateIndex;
    private CloudSystemInfoService mockInfoService;

    private int savedRehashSeconds;
    private boolean savedEnableCloudMultiReplica;
    private int savedCloudReplicaNum;
    private String savedCloudUniqueId;

    @BeforeEach
    public void setUp() {
        savedRehashSeconds = Config.rehash_tablet_after_be_dead_seconds;
        savedEnableCloudMultiReplica = Config.enable_cloud_multi_replica;
        savedCloudReplicaNum = Config.cloud_replica_num;
        savedCloudUniqueId = Config.cloud_unique_id;
        Config.cloud_unique_id = "test_cloud_unique_id";

        mockEnv = Mockito.mock(Env.class);
        mockColocateIndex = Mockito.mock(ColocateTableIndex.class);
        mockInfoService = Mockito.mock(CloudSystemInfoService.class);

        envMockedStatic = Mockito.mockStatic(Env.class);
        envMockedStatic.when(Env::getCurrentEnv).thenReturn(mockEnv);
        envMockedStatic.when(Env::getCurrentColocateIndex).thenReturn(mockColocateIndex);
        envMockedStatic.when(Env::getCurrentSystemInfo).thenReturn(mockInfoService);
    }

    @AfterEach
    public void tearDown() {
        envMockedStatic.close();
        Config.rehash_tablet_after_be_dead_seconds = savedRehashSeconds;
        Config.enable_cloud_multi_replica = savedEnableCloudMultiReplica;
        Config.cloud_replica_num = savedCloudReplicaNum;
        Config.cloud_unique_id = savedCloudUniqueId;
    }

    private CloudReplica createReplica() {
        return new CloudReplica(REPLICA_ID, -1L, Replica.ReplicaState.NORMAL, 1, 0,
                DB_ID, TABLE_ID, PARTITION_ID, INDEX_ID, IDX);
    }

    private Backend createBackend(long id, boolean alive, boolean decommissioned) {
        return createBackend(id, alive, false, decommissioned);
    }

    private Backend createBackend(long id, boolean alive, boolean decommissioning, boolean decommissioned) {
        Backend be = Mockito.mock(Backend.class);
        Mockito.when(be.getId()).thenReturn(id);
        Mockito.when(be.isAlive()).thenReturn(alive);
        Mockito.when(be.isDecommissioning()).thenReturn(decommissioning);
        Mockito.when(be.isDecommissioned()).thenReturn(decommissioned);
        Mockito.when(be.isQueryAvailable()).thenReturn(alive);
        Mockito.when(be.getLastUpdateMs()).thenReturn(System.currentTimeMillis());
        return be;
    }

    private void setupColocateTable() {
        Mockito.when(mockColocateIndex.isColocateTableNoLock(TABLE_ID)).thenReturn(true);
        Mockito.when(mockColocateIndex.getGroupNoLock(TABLE_ID)).thenReturn(new GroupId(DB_ID, TABLE_ID));
    }

    // ---------------------------------------------------------------
    // Tests for getColocatedBeId: dead BEs stay in hash ring
    // ---------------------------------------------------------------

    @Test
    public void testGetColocatedBeId_deadBeStaysInHashRing() throws ComputeGroupException {
        // Verify that a recently-dead BE stays in the hash ring (grace period),
        // preventing full rehash of all tablets.
        setupColocateTable();
        Config.rehash_tablet_after_be_dead_seconds = 600; // 10 min grace

        Backend be1 = createBackend(1001L, true, false);
        Backend be2 = createBackend(1002L, true, false);
        Backend be3 = createBackend(1003L, false, false); // dead recently
        Mockito.when(be3.getLastUpdateMs()).thenReturn(System.currentTimeMillis()); // just died

        List<Backend> allBes = Arrays.asList(be1, be2, be3);
        Mockito.when(mockInfoService.getBackendsByClusterId(CLUSTER_ID_1)).thenReturn(new ArrayList<>(allBes));
        Mockito.when(mockInfoService.getClusterNameByClusterId(CLUSTER_ID_1)).thenReturn(CLUSTER_NAME_1);

        CloudReplica replica = createReplica();
        long pickedBeId = replica.getColocatedBeId(CLUSTER_ID_1);

        // The picked BE must be alive (dead BE won't be returned), but
        // the hash ring should include the dead BE to avoid full rehash.
        Assertions.assertTrue(pickedBeId == 1001L || pickedBeId == 1002L,
                "Should pick an alive BE, got: " + pickedBeId);
    }

    @Test
    public void testGetColocatedBeId_hashRingStableWithRecentlyDeadBe() throws ComputeGroupException {
        // When a BE dies recently, the hash result for a tablet should be the same
        // as when all BEs were alive (if the tablet was on an alive BE).
        setupColocateTable();
        Config.rehash_tablet_after_be_dead_seconds = 600;

        Backend be1 = createBackend(1001L, true, false);
        Backend be2 = createBackend(1002L, true, false);
        Backend be3 = createBackend(1003L, true, false);

        // First: all alive - get baseline
        List<Backend> allAlive = Arrays.asList(be1, be2, be3);
        Mockito.when(mockInfoService.getBackendsByClusterId(CLUSTER_ID_1)).thenReturn(new ArrayList<>(allAlive));
        Mockito.when(mockInfoService.getClusterNameByClusterId(CLUSTER_ID_1)).thenReturn(CLUSTER_NAME_1);

        CloudReplica replica = createReplica();
        long baselineBeId = replica.getColocatedBeId(CLUSTER_ID_1);

        // Now: be3 dies recently - hash ring includes it in grace period
        Backend be3Dead = createBackend(1003L, false, false);
        Mockito.when(be3Dead.getLastUpdateMs()).thenReturn(System.currentTimeMillis());
        List<Backend> withDeadBe = Arrays.asList(be1, be2, be3Dead);
        Mockito.when(mockInfoService.getBackendsByClusterId(CLUSTER_ID_1)).thenReturn(new ArrayList<>(withDeadBe));

        long afterDeathBeId = replica.getColocatedBeId(CLUSTER_ID_1);

        // If baseline was on be1 or be2 (alive), it should stay there
        if (baselineBeId == 1001L || baselineBeId == 1002L) {
            Assertions.assertEquals(baselineBeId, afterDeathBeId,
                    "Hash ring should be stable for tablets on alive BEs during grace period");
        }
        // If baseline was on be3 (now dead), it will rehash to an alive BE - that's OK
    }

    @Test
    public void testGetColocatedBeId_longDeadBeCausesRehash() throws ComputeGroupException {
        // After grace period expires, dead BE should not be in hash ring.
        setupColocateTable();
        Config.rehash_tablet_after_be_dead_seconds = 600;

        Backend be1 = createBackend(1001L, true, false);
        Backend be2 = createBackend(1002L, true, false);
        Backend be3Dead = createBackend(1003L, false, false);
        // Dead long ago - past grace period
        Mockito.when(be3Dead.getLastUpdateMs()).thenReturn(
                System.currentTimeMillis() - (Config.rehash_tablet_after_be_dead_seconds + 100) * 1000L);

        List<Backend> bes = Arrays.asList(be1, be2, be3Dead);
        Mockito.when(mockInfoService.getBackendsByClusterId(CLUSTER_ID_1)).thenReturn(new ArrayList<>(bes));
        Mockito.when(mockInfoService.getClusterNameByClusterId(CLUSTER_ID_1)).thenReturn(CLUSTER_NAME_1);

        CloudReplica replica = createReplica();
        long pickedBeId = replica.getColocatedBeId(CLUSTER_ID_1);

        // Should pick from only alive BEs (be1 or be2), rehashed on 2-node ring
        Assertions.assertTrue(pickedBeId == 1001L || pickedBeId == 1002L,
                "Should pick an alive BE after grace period, got: " + pickedBeId);
    }

    @Test
    public void testGetColocatedBeId_allDeadThrowsException() {
        setupColocateTable();

        Backend be1Dead = createBackend(1001L, false, false);
        Backend be2Dead = createBackend(1002L, false, false);

        List<Backend> bes = Arrays.asList(be1Dead, be2Dead);
        Mockito.when(mockInfoService.getBackendsByClusterId(CLUSTER_ID_1)).thenReturn(new ArrayList<>(bes));
        Mockito.when(mockInfoService.getClusterNameByClusterId(CLUSTER_ID_1)).thenReturn(CLUSTER_NAME_1);

        CloudReplica replica = createReplica();
        ComputeGroupException ex = Assertions.assertThrows(ComputeGroupException.class,
                () -> replica.getColocatedBeId(CLUSTER_ID_1));
        Assertions.assertTrue(ex.toString().contains(
                ComputeGroupException.FailedTypeEnum.COMPUTE_GROUPS_NO_ALIVE_BE.name()));
    }

    @Test
    public void testGetColocatedBeId_emptyClusterThrowsException() {
        setupColocateTable();

        Mockito.when(mockInfoService.getBackendsByClusterId(CLUSTER_ID_1)).thenReturn(new ArrayList<>());
        Mockito.when(mockInfoService.getClusterNameByClusterId(CLUSTER_ID_1)).thenReturn(CLUSTER_NAME_1);

        CloudReplica replica = createReplica();
        ComputeGroupException ex = Assertions.assertThrows(ComputeGroupException.class,
                () -> replica.getColocatedBeId(CLUSTER_ID_1));
        Assertions.assertTrue(ex.toString().contains(
                ComputeGroupException.FailedTypeEnum.CURRENT_COMPUTE_GROUP_NO_BE.name()));
    }

    @Test
    public void testGetColocatedBeId_decommissionedBeUsedAsFallback() throws ComputeGroupException {
        setupColocateTable();

        Backend beDecommissioned = createBackend(1001L, true, true);
        List<Backend> bes = Arrays.asList(beDecommissioned);
        Mockito.when(mockInfoService.getBackendsByClusterId(CLUSTER_ID_1)).thenReturn(new ArrayList<>(bes));
        Mockito.when(mockInfoService.getClusterNameByClusterId(CLUSTER_ID_1)).thenReturn(CLUSTER_NAME_1);

        CloudReplica replica = createReplica();
        long pickedBeId = replica.getColocatedBeId(CLUSTER_ID_1);
        Assertions.assertEquals(1001L, pickedBeId,
                "Should fall back to decommissioned BE when no normal alive BE");
    }

    @Test
    public void testGetColocatedBeId_decommissioningBeExcludedFromNormalHashRing() throws ComputeGroupException {
        setupColocateTable();

        Backend be1 = createBackend(1001L, true, false);
        Backend be2Decommissioning = createBackend(1002L, true, true, false);

        Mockito.when(mockInfoService.getBackendsByClusterId(CLUSTER_ID_1))
                .thenReturn(new ArrayList<>(Arrays.asList(be1, be2Decommissioning)));
        Mockito.when(mockInfoService.getClusterNameByClusterId(CLUSTER_ID_1)).thenReturn(CLUSTER_NAME_1);

        CloudReplica replica = createReplica();
        long pickedBeId = replica.getColocatedBeId(CLUSTER_ID_1);

        Assertions.assertEquals(1001L, pickedBeId,
                "Should exclude decommissioning BE from normal colocate hash ring");
    }

    @Test
    public void testHashReplicaToBe_decommissioningBeExcludedFromNormalHashRing() throws ComputeGroupException {
        Backend be1 = createBackend(1001L, true, false);
        Backend be2Decommissioning = createBackend(1002L, true, true, false);

        Mockito.when(mockInfoService.getBackendsByClusterId(CLUSTER_ID_1))
                .thenReturn(new ArrayList<>(Arrays.asList(be1, be2Decommissioning)));
        Mockito.when(mockInfoService.getClusterNameByClusterId(CLUSTER_ID_1)).thenReturn(CLUSTER_NAME_1);

        CloudReplica replica = createReplica();
        long pickedBeId = replica.hashReplicaToBe(CLUSTER_ID_1, false);

        Assertions.assertEquals(1001L, pickedBeId,
                "Should exclude decommissioning BE from normal replica hash ring");
    }

    @Test
    public void testGetBackendId_multiReplicaDecommissioningBeExcludedFromNormalHashRing()
            throws ComputeGroupException {
        Mockito.when(mockColocateIndex.isColocateTableNoLock(TABLE_ID)).thenReturn(false);
        Config.enable_cloud_multi_replica = true;
        Config.cloud_replica_num = 2;

        Backend be1 = createBackend(1001L, true, false);
        Backend be2Decommissioning = createBackend(1002L, true, true, false);

        Mockito.when(mockInfoService.getBackendsByClusterId(CLUSTER_ID_1))
                .thenReturn(new ArrayList<>(Arrays.asList(be1, be2Decommissioning)));
        Mockito.when(mockInfoService.getClusterNameByClusterId(CLUSTER_ID_1)).thenReturn(CLUSTER_NAME_1);

        CloudReplica replica = createReplica();
        long pickedBeId = replica.getBackendIdWithClusterId(CLUSTER_ID_1);

        Assertions.assertEquals(1001L, pickedBeId,
                "Should exclude decommissioning BE from normal multi-replica hash ring");
    }

    @Test
    public void testGetBackendId_decommissioningPrimaryBeTriggersRehash() throws ComputeGroupException {
        Mockito.when(mockColocateIndex.isColocateTableNoLock(TABLE_ID)).thenReturn(false);

        Backend be1 = createBackend(1001L, true, false);
        Backend be2Decommissioning = createBackend(1002L, true, true, false);
        Mockito.when(mockInfoService.getBackend(1002L)).thenReturn(be2Decommissioning);
        Mockito.when(mockInfoService.getBackendsByClusterId(CLUSTER_ID_1))
                .thenReturn(new ArrayList<>(Arrays.asList(be1, be2Decommissioning)));
        Mockito.when(mockInfoService.getClusterNameByClusterId(CLUSTER_ID_1)).thenReturn(CLUSTER_NAME_1);

        CloudReplica replica = createReplica();
        replica.updateClusterToPrimaryBe(CLUSTER_ID_1, 1002L);

        long pickedBeId = replica.getBackendIdWithClusterId(CLUSTER_ID_1);

        Assertions.assertEquals(1001L, pickedBeId,
                "Should rehash instead of returning cached decommissioning primary BE");
    }

    // ---------------------------------------------------------------
    // Tests for getAllPrimaryBes: colocate table support
    // ---------------------------------------------------------------

    @Test
    public void testGetAllPrimaryBes_colocateTable_dynamicCompute() throws ComputeGroupException {
        // For colocate tables, primaryClusterToBackends is empty.
        // getAllPrimaryBes() should dynamically compute backends via getColocatedBeId().
        setupColocateTable();

        Backend be1 = createBackend(1001L, true, false);
        Backend be2 = createBackend(2001L, true, false);

        // Two clusters
        Mockito.when(mockInfoService.getCloudClusterIds())
                .thenReturn(Arrays.asList(CLUSTER_ID_1, CLUSTER_ID_2));

        Mockito.when(mockInfoService.getBackendsByClusterId(CLUSTER_ID_1))
                .thenReturn(new ArrayList<>(Arrays.asList(be1)));
        Mockito.when(mockInfoService.getClusterNameByClusterId(CLUSTER_ID_1)).thenReturn("cluster_1");

        Mockito.when(mockInfoService.getBackendsByClusterId(CLUSTER_ID_2))
                .thenReturn(new ArrayList<>(Arrays.asList(be2)));
        Mockito.when(mockInfoService.getClusterNameByClusterId(CLUSTER_ID_2)).thenReturn("cluster_2");

        Mockito.when(mockInfoService.getBackend(1001L)).thenReturn(be1);
        Mockito.when(mockInfoService.getBackend(2001L)).thenReturn(be2);

        CloudReplica replica = createReplica();
        // primaryClusterToBackends is empty by default (ConcurrentHashMap)

        List<Backend> primaryBes = replica.getAllPrimaryBes();

        Assertions.assertEquals(2, primaryBes.size(),
                "Colocate table should return one BE per cluster");
    }

    @Test
    public void testGetAllPrimaryBes_colocateTable_clusterWithAllDeadBes() {
        // If one cluster has all dead BEs, it should be silently skipped.
        setupColocateTable();

        Backend be1 = createBackend(1001L, true, false);
        Backend be2Dead = createBackend(2001L, false, false);

        Mockito.when(mockInfoService.getCloudClusterIds())
                .thenReturn(Arrays.asList(CLUSTER_ID_1, CLUSTER_ID_2));

        // Cluster 1: has alive BE
        Mockito.when(mockInfoService.getBackendsByClusterId(CLUSTER_ID_1))
                .thenReturn(new ArrayList<>(Arrays.asList(be1)));
        Mockito.when(mockInfoService.getClusterNameByClusterId(CLUSTER_ID_1)).thenReturn("cluster_1");
        Mockito.when(mockInfoService.getBackend(1001L)).thenReturn(be1);

        // Cluster 2: all dead
        Mockito.when(mockInfoService.getBackendsByClusterId(CLUSTER_ID_2))
                .thenReturn(new ArrayList<>(Arrays.asList(be2Dead)));
        Mockito.when(mockInfoService.getClusterNameByClusterId(CLUSTER_ID_2)).thenReturn("cluster_2");

        CloudReplica replica = createReplica();
        List<Backend> primaryBes = replica.getAllPrimaryBes();

        // Cluster 2 throws ComputeGroupException, should be caught and skipped
        Assertions.assertEquals(1, primaryBes.size(),
                "Should only return BE from cluster with alive backends");
        Assertions.assertEquals(1001L, primaryBes.get(0).getId());
    }

    @Test
    public void testGetAllPrimaryBes_nonColocateTable_usesPrimaryMap() {
        // For non-colocate tables, should use primaryClusterToBackends as before.
        Mockito.when(mockColocateIndex.isColocateTableNoLock(TABLE_ID)).thenReturn(false);

        Backend be1 = createBackend(1001L, true, false);
        Mockito.when(mockInfoService.getBackend(1001L)).thenReturn(be1);

        CloudReplica replica = createReplica();
        // Manually populate primaryClusterToBackends
        replica.updateClusterToPrimaryBe(CLUSTER_ID_1, 1001L);

        List<Backend> primaryBes = replica.getAllPrimaryBes();
        Assertions.assertEquals(1, primaryBes.size());
        Assertions.assertEquals(1001L, primaryBes.get(0).getId());
    }

    @Test
    public void testGetAllPrimaryBes_colocateTable_emptyClusterList() {
        // If no clusters exist, should return empty list.
        setupColocateTable();
        Mockito.when(mockInfoService.getCloudClusterIds()).thenReturn(new ArrayList<>());

        CloudReplica replica = createReplica();
        List<Backend> primaryBes = replica.getAllPrimaryBes();
        Assertions.assertTrue(primaryBes.isEmpty());
    }
}
