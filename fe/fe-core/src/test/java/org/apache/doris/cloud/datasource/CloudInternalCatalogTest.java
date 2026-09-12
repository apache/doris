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

package org.apache.doris.cloud.datasource;

import org.apache.doris.catalog.ColocateTableIndex;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Replica;
import org.apache.doris.cloud.catalog.CloudReplica;
import org.apache.doris.cloud.catalog.CloudTablet;
import org.apache.doris.cloud.persist.UpdateCloudReplicaInfo;
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.system.Backend;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.List;

public class CloudInternalCatalogTest {
    private static final long DB_ID = 10001L;
    private static final long TABLE_ID = 20001L;
    private static final long PARTITION_ID = 30001L;
    private static final long INDEX_ID = 40001L;
    private static final long LIVE_BE_ID = 50001L;
    private static final long DEAD_BE_ID = 50002L;
    private static final String STALE_CLUSTER_ID = "stale_cluster";
    private static final String LIVE_CLUSTER_ID = "live_cluster";

    private MockedStatic<Env> envMockedStatic;
    private CloudSystemInfoService systemInfoService;
    private CloudInternalCatalog catalog;
    private MaterializedIndex materializedIndex;

    @BeforeEach
    public void setUpReplayCatalog() {
        systemInfoService = Mockito.mock(CloudSystemInfoService.class);
        ColocateTableIndex colocateTableIndex = Mockito.mock(ColocateTableIndex.class);
        envMockedStatic = Mockito.mockStatic(Env.class);
        envMockedStatic.when(Env::getCurrentSystemInfo).thenReturn(systemInfoService);
        envMockedStatic.when(Env::getCurrentColocateIndex).thenReturn(colocateTableIndex);
        Mockito.when(colocateTableIndex.isColocateTableNoLock(TABLE_ID)).thenReturn(false);

        catalog = Mockito.spy(new CloudInternalCatalog());
        Database database = Mockito.mock(Database.class);
        OlapTable table = Mockito.mock(OlapTable.class);
        Partition partition = Mockito.mock(Partition.class);
        materializedIndex = Mockito.mock(MaterializedIndex.class);
        Mockito.doReturn(database).when(catalog).getDbNullable(DB_ID);
        Mockito.when(database.getTableNullable(TABLE_ID)).thenReturn(table);
        Mockito.when(table.getPartition(PARTITION_ID)).thenReturn(partition);
        Mockito.when(partition.getIndex(INDEX_ID)).thenReturn(materializedIndex);
    }

    @AfterEach
    public void tearDownReplayCatalog() {
        envMockedStatic.close();
    }

    @Test
    public void testReplaySingleReplicaRemovesStaleRoutes() throws Exception {
        boolean savedClean = Config.enable_cloud_replica_stale_route_clean;
        boolean savedUnitTest = FeConstants.runningUnitTest;
        try {
            Config.enable_cloud_replica_stale_route_clean = true;
            FeConstants.runningUnitTest = false;
            setBackend(LIVE_BE_ID, Mockito.mock(Backend.class));
            setBackend(DEAD_BE_ID, null);
            CloudReplica replica = addReplayReplica(60001L, 70001L);
            replica.updateClusterToPrimaryBe(STALE_CLUSTER_ID, DEAD_BE_ID);

            catalog.replayUpdateCloudReplica(new UpdateCloudReplicaInfo(DB_ID, TABLE_ID, PARTITION_ID, INDEX_ID,
                    60001L, 70001L, LIVE_CLUSTER_ID, LIVE_BE_ID));

            Assertions.assertFalse(replica.getPrimaryComputeGroupIds().contains(STALE_CLUSTER_ID));
            Assertions.assertEquals(LIVE_BE_ID, replica.getClusterPrimaryBackendId(LIVE_CLUSTER_ID));
        } finally {
            Config.enable_cloud_replica_stale_route_clean = savedClean;
            FeConstants.runningUnitTest = savedUnitTest;
        }
    }

    @Test
    public void testReplayBatchRemovesStaleRoutesWithAndWithoutReplicaIds() throws Exception {
        boolean savedClean = Config.enable_cloud_replica_stale_route_clean;
        boolean savedUnitTest = FeConstants.runningUnitTest;
        try {
            Config.enable_cloud_replica_stale_route_clean = true;
            FeConstants.runningUnitTest = false;
            setBackend(LIVE_BE_ID, Mockito.mock(Backend.class));
            setBackend(DEAD_BE_ID, null);
            List<CloudReplica> replicas = Arrays.asList(
                    addReplayReplica(60001L, 70001L), addReplayReplica(60002L, 70002L));
            replicas.forEach(replica -> replica.updateClusterToPrimaryBe(STALE_CLUSTER_ID, DEAD_BE_ID));
            List<Long> tabletIds = Arrays.asList(60001L, 60002L);
            List<Long> beIds = Arrays.asList(LIVE_BE_ID, LIVE_BE_ID);

            UpdateCloudReplicaInfo explicitReplicaIds = new UpdateCloudReplicaInfo(
                    DB_ID, TABLE_ID, PARTITION_ID, INDEX_ID, LIVE_CLUSTER_ID, beIds, tabletIds);
            explicitReplicaIds.setReplicaIds(Arrays.asList(70001L, 70002L));
            catalog.replayUpdateCloudReplica(explicitReplicaIds);

            for (CloudReplica replica : replicas) {
                Assertions.assertFalse(replica.getPrimaryComputeGroupIds().contains(STALE_CLUSTER_ID));
                Assertions.assertEquals(LIVE_BE_ID, replica.getClusterPrimaryBackendId(LIVE_CLUSTER_ID));
                replica.updateClusterToPrimaryBe(STALE_CLUSTER_ID, DEAD_BE_ID);
            }

            catalog.replayUpdateCloudReplica(new UpdateCloudReplicaInfo(
                    DB_ID, TABLE_ID, PARTITION_ID, INDEX_ID, LIVE_CLUSTER_ID, beIds, tabletIds));

            for (CloudReplica replica : replicas) {
                Assertions.assertFalse(replica.getPrimaryComputeGroupIds().contains(STALE_CLUSTER_ID));
                Assertions.assertEquals(LIVE_BE_ID, replica.getClusterPrimaryBackendId(LIVE_CLUSTER_ID));
            }
        } finally {
            Config.enable_cloud_replica_stale_route_clean = savedClean;
            FeConstants.runningUnitTest = savedUnitTest;
        }
    }

    @Test
    public void testReplayKeepsStaleRoutesWhenCleanupDisabled() throws Exception {
        boolean savedClean = Config.enable_cloud_replica_stale_route_clean;
        boolean savedUnitTest = FeConstants.runningUnitTest;
        try {
            Config.enable_cloud_replica_stale_route_clean = false;
            FeConstants.runningUnitTest = false;
            setBackend(LIVE_BE_ID, Mockito.mock(Backend.class));
            setBackend(DEAD_BE_ID, null);
            CloudReplica replica = addReplayReplica(60001L, 70001L);
            replica.updateClusterToPrimaryBe(STALE_CLUSTER_ID, DEAD_BE_ID);

            catalog.replayUpdateCloudReplica(new UpdateCloudReplicaInfo(DB_ID, TABLE_ID, PARTITION_ID, INDEX_ID,
                    60001L, 70001L, LIVE_CLUSTER_ID, LIVE_BE_ID));

            Assertions.assertTrue(replica.getPrimaryComputeGroupIds().contains(STALE_CLUSTER_ID));
            Assertions.assertEquals(LIVE_BE_ID, replica.getClusterPrimaryBackendId(LIVE_CLUSTER_ID));
        } finally {
            Config.enable_cloud_replica_stale_route_clean = savedClean;
            FeConstants.runningUnitTest = savedUnitTest;
        }
    }

    @Test
    public void testReplayRemovesDeletedBackendRouteAndKeepsLiveSecondary() throws Exception {
        boolean savedClean = Config.enable_cloud_replica_stale_route_clean;
        boolean savedUnitTest = FeConstants.runningUnitTest;
        try {
            Config.enable_cloud_replica_stale_route_clean = true;
            FeConstants.runningUnitTest = false;
            Backend liveBackend = Mockito.mock(Backend.class);
            setBackend(LIVE_BE_ID, liveBackend);
            setBackend(DEAD_BE_ID, null);
            CloudReplica replica = addReplayReplica(60001L, 70001L);
            replica.updateClusterToPrimaryBe(STALE_CLUSTER_ID, DEAD_BE_ID);
            replica.updateClusterToSecondaryBe(STALE_CLUSTER_ID, LIVE_BE_ID);

            catalog.replayUpdateCloudReplica(new UpdateCloudReplicaInfo(DB_ID, TABLE_ID, PARTITION_ID, INDEX_ID,
                    60001L, 70001L, LIVE_CLUSTER_ID, DEAD_BE_ID));

            Assertions.assertFalse(replica.getPrimaryComputeGroupIds().contains(LIVE_CLUSTER_ID));
            Assertions.assertTrue(replica.getPrimaryComputeGroupIds().contains(STALE_CLUSTER_ID));
            Assertions.assertSame(liveBackend, replica.getSecondaryBackend(STALE_CLUSTER_ID));
        } finally {
            Config.enable_cloud_replica_stale_route_clean = savedClean;
            FeConstants.runningUnitTest = savedUnitTest;
        }
    }

    private CloudReplica addReplayReplica(long tabletId, long replicaId) {
        CloudReplica replica = new CloudReplica(replicaId, -1L, Replica.ReplicaState.NORMAL, 1L, 0,
                DB_ID, TABLE_ID, PARTITION_ID, INDEX_ID, 0L);
        CloudTablet tablet = new CloudTablet(tabletId);
        tablet.addReplica(replica, true);
        Mockito.when(materializedIndex.getTablet(tabletId)).thenReturn(tablet);
        return replica;
    }

    private void setBackend(long backendId, Backend backend) {
        Mockito.when(systemInfoService.getBackendByIdWithBoxedId(backendId)).thenReturn(backend);
        Mockito.when(systemInfoService.getBackend(backendId)).thenReturn(backend);
    }
}
