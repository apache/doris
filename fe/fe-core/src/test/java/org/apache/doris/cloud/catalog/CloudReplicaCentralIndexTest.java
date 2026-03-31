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
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.TabletMeta;
import org.apache.doris.thrift.TStorageMedium;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Map;

public class CloudReplicaCentralIndexTest {

    private CloudTabletInvertedIndex index;
    private MockedStatic<Env> mockedEnv;
    private Env mockEnvInstance;

    @BeforeEach
    public void setUp() {
        index = new CloudTabletInvertedIndex();
        mockEnvInstance = Mockito.mock(Env.class);
        Mockito.when(mockEnvInstance.getTabletInvertedIndex()).thenReturn(index);

        mockedEnv = Mockito.mockStatic(Env.class);
        mockedEnv.when(Env::getCurrentEnv).thenReturn(mockEnvInstance);
        mockedEnv.when(Env::getCurrentInvertedIndex).thenReturn(index);
    }

    @AfterEach
    public void tearDown() {
        mockedEnv.close();
    }

    @Test
    public void testPrimaryBeRoundTrip() {
        String cluster = "cluster1";
        long replicaId = 1001L;
        long beId = 2001L;

        // Initially not found
        Assertions.assertEquals(-1L, index.getPrimaryBeId(cluster, replicaId));

        // Set and get
        index.setPrimaryBeId(cluster, replicaId, beId);
        Assertions.assertEquals(beId, index.getPrimaryBeId(cluster, replicaId));

        // Different cluster returns -1
        Assertions.assertEquals(-1L, index.getPrimaryBeId("cluster2", replicaId));

        // Remove
        index.removePrimaryBeId(cluster, replicaId);
        Assertions.assertEquals(-1L, index.getPrimaryBeId(cluster, replicaId));
    }

    @Test
    public void testGetAllPrimaryClusterBeIds() {
        long replicaId = 1001L;

        index.setPrimaryBeId("cluster1", replicaId, 2001L);
        index.setPrimaryBeId("cluster2", replicaId, 2002L);
        index.setPrimaryBeId("cluster1", 9999L, 3001L); // different replica

        Map<String, Long> result = index.getAllPrimaryClusterBeIds(replicaId);
        Assertions.assertEquals(2, result.size());
        Assertions.assertEquals(2001L, result.get("cluster1"));
        Assertions.assertEquals(2002L, result.get("cluster2"));
    }

    @Test
    public void testSecondaryBeLifecycle() {
        String cluster = "cluster1";
        long replicaId = 1001L;
        long beId = 2001L;
        long timestamp = 123456789L;

        // Initially not found
        Assertions.assertEquals(-1L, index.getSecondaryBeId(cluster, replicaId));
        Assertions.assertEquals(-1L, index.getSecondaryTimestamp(cluster, replicaId));

        // Set and get
        index.setSecondaryBe(cluster, replicaId, beId, timestamp);
        Assertions.assertEquals(beId, index.getSecondaryBeId(cluster, replicaId));
        Assertions.assertEquals(timestamp, index.getSecondaryTimestamp(cluster, replicaId));

        // Remove
        index.removeSecondaryBe(cluster, replicaId);
        Assertions.assertEquals(-1L, index.getSecondaryBeId(cluster, replicaId));
        Assertions.assertEquals(-1L, index.getSecondaryTimestamp(cluster, replicaId));
    }

    @Test
    public void testDeleteTabletCleanup() {
        long tabletId = 100L;
        long replicaId = 1001L;

        // Register tablet and replica
        TabletMeta meta = new TabletMeta(1, 2, 3, 4, 0, TStorageMedium.HDD);
        index.addTablet(tabletId, meta);
        CloudReplica replica = new CloudReplica(replicaId, -1L,
                Replica.ReplicaState.NORMAL, 1, 0, 1, 2, 3, 4, 0);
        index.addReplica(tabletId, replica);

        // Set primary and secondary
        index.setPrimaryBeId("cluster1", replicaId, 2001L);
        index.setSecondaryBe("cluster1", replicaId, 2002L, 100L);

        // Delete tablet should clean up both
        index.deleteTablet(tabletId);

        Assertions.assertEquals(-1L, index.getPrimaryBeId("cluster1", replicaId));
        Assertions.assertEquals(-1L, index.getSecondaryBeId("cluster1", replicaId));
    }

    @Test
    public void testDeleteReplicaCleanup() {
        long tabletId = 100L;
        long replicaId = 1001L;

        TabletMeta meta = new TabletMeta(1, 2, 3, 4, 0, TStorageMedium.HDD);
        index.addTablet(tabletId, meta);
        CloudReplica replica = new CloudReplica(replicaId, -1L,
                Replica.ReplicaState.NORMAL, 1, 0, 1, 2, 3, 4, 0);
        index.addReplica(tabletId, replica);

        index.setPrimaryBeId("cluster1", replicaId, 2001L);
        index.setPrimaryBeId("cluster2", replicaId, 2002L);
        index.setSecondaryBe("cluster1", replicaId, 3001L, 100L);

        index.deleteReplica(tabletId, -1);

        Assertions.assertEquals(-1L, index.getPrimaryBeId("cluster1", replicaId));
        Assertions.assertEquals(-1L, index.getPrimaryBeId("cluster2", replicaId));
        Assertions.assertEquals(-1L, index.getSecondaryBeId("cluster1", replicaId));
    }

    @Test
    public void testClearPrimaryBeForReplica() {
        long replicaId = 1001L;

        index.setPrimaryBeId("cluster1", replicaId, 2001L);
        index.setPrimaryBeId("cluster2", replicaId, 2002L);
        index.setPrimaryBeId("cluster1", 9999L, 3001L); // other replica untouched

        index.clearPrimaryBeForReplica(replicaId);

        Assertions.assertEquals(-1L, index.getPrimaryBeId("cluster1", replicaId));
        Assertions.assertEquals(-1L, index.getPrimaryBeId("cluster2", replicaId));
        Assertions.assertEquals(3001L, index.getPrimaryBeId("cluster1", 9999L)); // untouched
    }

    @Test
    public void testInnerClearCleansAllMaps() {
        long replicaId = 1001L;

        index.setPrimaryBeId("cluster1", replicaId, 2001L);
        index.setSecondaryBe("cluster1", replicaId, 3001L, 100L);

        // innerClear is protected, but clear() calls it
        index.clear();

        Assertions.assertEquals(-1L, index.getPrimaryBeId("cluster1", replicaId));
        Assertions.assertEquals(-1L, index.getSecondaryBeId("cluster1", replicaId));
    }

    @Test
    public void testGsonPreProcessPopulatesField() throws Exception {
        long replicaId = 1001L;
        index.setPrimaryBeId("cluster1", replicaId, 2001L);
        index.setPrimaryBeId("cluster2", replicaId, 2002L);

        CloudReplica replica = new CloudReplica(replicaId, -1L,
                Replica.ReplicaState.NORMAL, 1, 0, 1, 2, 3, 4, 0);

        // gsonPreProcess should populate primaryClusterToBackend for serialization
        replica.gsonPreProcess();

        // Access the private field to verify
        java.lang.reflect.Field field = CloudReplica.class.getDeclaredField("primaryClusterToBackend");
        field.setAccessible(true);
        @SuppressWarnings("unchecked")
        Map<String, Long> beMap = (Map<String, Long>) field.get(replica);

        Assertions.assertNotNull(beMap);
        Assertions.assertEquals(2001L, beMap.get("cluster1"));
        Assertions.assertEquals(2002L, beMap.get("cluster2"));
    }

    @Test
    public void testGsonPostProcessPopulatesIndex() throws Exception {
        long replicaId = 1001L;

        CloudReplica replica = new CloudReplica(replicaId, -1L,
                Replica.ReplicaState.NORMAL, 1, 0, 1, 2, 3, 4, 0);

        // Simulate deserialized state: primaryClusterToBackend has data
        java.lang.reflect.Field field = CloudReplica.class.getDeclaredField("primaryClusterToBackend");
        field.setAccessible(true);
        java.util.HashMap<String, Long> beMap = new java.util.HashMap<>();
        beMap.put("cluster1", 2001L);
        beMap.put("cluster2", 2002L);
        field.set(replica, beMap);

        // gsonPostProcess should move data to central index and null out the field
        replica.gsonPostProcess();

        Assertions.assertEquals(2001L, index.getPrimaryBeId("cluster1", replicaId));
        Assertions.assertEquals(2002L, index.getPrimaryBeId("cluster2", replicaId));
        Assertions.assertNull(field.get(replica));
    }

    @Test
    public void testGsonPostProcessLegacyBesFormat() throws Exception {
        long replicaId = 1001L;

        CloudReplica replica = new CloudReplica(replicaId, -1L,
                Replica.ReplicaState.NORMAL, 1, 0, 1, 2, 3, 4, 0);

        // Simulate legacy "bes" format
        java.lang.reflect.Field besField = CloudReplica.class.getDeclaredField("primaryClusterToBackends");
        besField.setAccessible(true);
        java.util.concurrent.ConcurrentHashMap<String, java.util.List<Long>> besMap
                = new java.util.concurrent.ConcurrentHashMap<>();
        besMap.put("cluster1", java.util.List.of(2001L, 2002L));
        besField.set(replica, besMap);

        replica.gsonPostProcess();

        // Should have migrated to central index using first element
        Assertions.assertEquals(2001L, index.getPrimaryBeId("cluster1", replicaId));
        Assertions.assertNull(besField.get(replica));
    }
}
