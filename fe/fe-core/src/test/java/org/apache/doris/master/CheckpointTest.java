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

package org.apache.doris.master;

import org.apache.doris.catalog.ColocateTableIndex;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndex.IndexExtState;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Table;
import org.apache.doris.cloud.catalog.CloudReplica;
import org.apache.doris.cloud.catalog.CloudTablet;
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.system.Backend;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;

public class CheckpointTest {
    private static final long DB_ID = 10001L;
    private static final long TABLE_ID = 20001L;
    private static final long PARTITION_ID = 30001L;
    private static final long INDEX_ID = 40001L;
    private static final long LIVE_BE_ID = 50001L;
    private static final long DEAD_BE_ID = 50002L;
    private static final String STALE_CLUSTER_ID = "stale_cluster";
    private static final String LIVE_CLUSTER_ID = "live_cluster";

    private MockedStatic<Env> envMockedStatic;
    private Env env;
    private Database database;
    private OlapTable table;
    private Partition partition;
    private MaterializedIndex materializedIndex;
    private Backend liveBackend;

    @BeforeEach
    public void setUp() {
        CloudSystemInfoService systemInfoService = Mockito.mock(CloudSystemInfoService.class);
        ColocateTableIndex colocateTableIndex = Mockito.mock(ColocateTableIndex.class);
        envMockedStatic = Mockito.mockStatic(Env.class);
        envMockedStatic.when(Env::getCurrentSystemInfo).thenReturn(systemInfoService);
        envMockedStatic.when(Env::getCurrentColocateIndex).thenReturn(colocateTableIndex);
        Mockito.when(colocateTableIndex.isColocateTableNoLock(TABLE_ID)).thenReturn(false);
        liveBackend = Mockito.mock(Backend.class);
        Mockito.when(systemInfoService.getBackendByIdWithBoxedId(LIVE_BE_ID)).thenReturn(liveBackend);
        Mockito.when(systemInfoService.getBackendByIdWithBoxedId(DEAD_BE_ID)).thenReturn(null);
        Mockito.when(systemInfoService.getBackend(LIVE_BE_ID)).thenReturn(liveBackend);

        env = Mockito.mock(Env.class);
        InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
        database = Mockito.mock(Database.class);
        table = Mockito.mock(OlapTable.class);
        partition = Mockito.mock(Partition.class);
        materializedIndex = Mockito.mock(MaterializedIndex.class);
        envMockedStatic.when(Env::getCurrentEnv).thenReturn(env);
        Mockito.when(env.getInternalCatalog()).thenReturn(catalog);
        Mockito.when(catalog.getDbIds()).thenReturn(Collections.singletonList(DB_ID));
        Mockito.when(catalog.getDbNullable(DB_ID)).thenReturn(database);
        Mockito.when(database.getTables()).thenReturn(Collections.singletonList(table));
        Mockito.when(table.isManagedTable()).thenReturn(true);
        Mockito.when(table.getAllPartitions()).thenReturn(Collections.singletonList(partition));
        Mockito.when(partition.getMaterializedIndices(IndexExtState.ALL, true))
                .thenReturn(Collections.singletonList(materializedIndex));
    }

    @AfterEach
    public void tearDown() {
        envMockedStatic.close();
    }

    @Test
    public void testRemovesStaleRouteAndKeepsLiveRoute() {
        withRouteCleanup(() -> {
            CloudReplica replica = addReplica(materializedIndex, 60001L);
            replica.updateClusterToPrimaryBe(STALE_CLUSTER_ID, DEAD_BE_ID);
            replica.updateClusterToPrimaryBe(LIVE_CLUSTER_ID, LIVE_BE_ID);

            Assertions.assertEquals(1L, Checkpoint.removeInvalidCloudReplicaRoutes());
            Assertions.assertFalse(replica.getPrimaryComputeGroupIds().contains(STALE_CLUSTER_ID));
            Assertions.assertEquals(LIVE_BE_ID, replica.getClusterPrimaryBackendId(LIVE_CLUSTER_ID));
            Assertions.assertEquals(0L, Checkpoint.removeInvalidCloudReplicaRoutes());
        });
    }

    @Test
    public void testKeepsDeadPrimaryWithLiveSecondary() {
        withRouteCleanup(() -> {
            CloudReplica replica = addReplica(materializedIndex, 60001L);
            replica.updateClusterToPrimaryBe(STALE_CLUSTER_ID, DEAD_BE_ID);
            replica.updateClusterToSecondaryBe(STALE_CLUSTER_ID, LIVE_BE_ID);

            Assertions.assertEquals(0L, Checkpoint.removeInvalidCloudReplicaRoutes());
            Assertions.assertEquals(DEAD_BE_ID, replica.getClusterPrimaryBackendId(STALE_CLUSTER_ID));
            Assertions.assertSame(liveBackend, replica.getSecondaryBackend(STALE_CLUSTER_ID));
        });
    }

    @Test
    public void testNonCloudModeIsNoOp() {
        withRouteCleanup(() -> {
            Config.deploy_mode = "local";
            Config.cloud_unique_id = "";
            CloudReplica replica = addReplica(materializedIndex, 60001L);
            replica.updateClusterToPrimaryBe(STALE_CLUSTER_ID, DEAD_BE_ID);

            Assertions.assertEquals(0L, Checkpoint.removeInvalidCloudReplicaRoutes());
            Assertions.assertTrue(replica.getPrimaryComputeGroupIds().contains(STALE_CLUSTER_ID));
            Mockito.verify(env, Mockito.never()).getInternalCatalog();
        });
    }

    @Test
    public void testSkipsNonManagedTable() {
        withRouteCleanup(() -> {
            Table nonManagedTable = Mockito.mock(Table.class);
            Mockito.when(nonManagedTable.isManagedTable()).thenReturn(false);
            Mockito.when(database.getTables()).thenReturn(Arrays.asList(nonManagedTable, table));
            CloudReplica replica = addReplica(materializedIndex, 60001L);
            replica.updateClusterToPrimaryBe(STALE_CLUSTER_ID, DEAD_BE_ID);

            Assertions.assertEquals(1L, Checkpoint.removeInvalidCloudReplicaRoutes());
            Assertions.assertFalse(replica.getPrimaryComputeGroupIds().contains(STALE_CLUSTER_ID));
        });
    }

    @Test
    public void testSweepsShadowIndex() {
        withRouteCleanup(() -> {
            MaterializedIndex shadowIndex = Mockito.mock(MaterializedIndex.class);
            Mockito.when(shadowIndex.getState()).thenReturn(MaterializedIndex.IndexState.SHADOW);
            Mockito.when(partition.getMaterializedIndices(IndexExtState.VISIBLE, true))
                    .thenReturn(Collections.singletonList(materializedIndex));
            Mockito.when(partition.getMaterializedIndices(IndexExtState.ALL, true))
                    .thenReturn(Arrays.asList(materializedIndex, shadowIndex));
            CloudReplica visibleReplica = addReplica(materializedIndex, 60001L);
            CloudReplica shadowReplica = addReplica(shadowIndex, 60002L);
            visibleReplica.updateClusterToPrimaryBe(STALE_CLUSTER_ID, DEAD_BE_ID);
            shadowReplica.updateClusterToPrimaryBe(STALE_CLUSTER_ID, DEAD_BE_ID);

            Assertions.assertEquals(2L, Checkpoint.removeInvalidCloudReplicaRoutes());
            Assertions.assertFalse(visibleReplica.getPrimaryComputeGroupIds().contains(STALE_CLUSTER_ID));
            Assertions.assertFalse(shadowReplica.getPrimaryComputeGroupIds().contains(STALE_CLUSTER_ID));
        });
    }

    @Test
    public void testSkipsNonCloudReplica() {
        withRouteCleanup(() -> {
            CloudTablet tablet = new CloudTablet(60001L);
            tablet.addReplica(Mockito.mock(Replica.class), true);
            Mockito.when(materializedIndex.getTablets()).thenReturn(Collections.singletonList(tablet));

            Assertions.assertEquals(0L, Checkpoint.removeInvalidCloudReplicaRoutes());
        });
    }

    @Test
    public void testCleanupDisabledIsNoOp() {
        withRouteCleanup(() -> {
            Config.enable_cloud_replica_stale_route_clean = false;
            CloudReplica replica = addReplica(materializedIndex, 60001L);
            replica.updateClusterToPrimaryBe(STALE_CLUSTER_ID, DEAD_BE_ID);

            Assertions.assertEquals(0L, Checkpoint.removeInvalidCloudReplicaRoutes());
            Assertions.assertTrue(replica.getPrimaryComputeGroupIds().contains(STALE_CLUSTER_ID));
        });
    }

    private CloudReplica addReplica(MaterializedIndex index, long tabletId) {
        CloudReplica replica = new CloudReplica(tabletId + 10000L, -1L, Replica.ReplicaState.NORMAL, 1L, 0,
                DB_ID, TABLE_ID, PARTITION_ID, INDEX_ID, 0L);
        CloudTablet tablet = new CloudTablet(tabletId);
        tablet.addReplica(replica, true);
        Mockito.when(index.getTablets()).thenReturn(Collections.singletonList(tablet));
        return replica;
    }

    private void withRouteCleanup(Runnable test) {
        boolean savedClean = Config.enable_cloud_replica_stale_route_clean;
        boolean savedUnitTest = FeConstants.runningUnitTest;
        String savedDeployMode = Config.deploy_mode;
        String savedCloudUniqueId = Config.cloud_unique_id;
        try {
            Config.enable_cloud_replica_stale_route_clean = true;
            FeConstants.runningUnitTest = false;
            Config.deploy_mode = "cloud";
            test.run();
        } finally {
            Config.enable_cloud_replica_stale_route_clean = savedClean;
            FeConstants.runningUnitTest = savedUnitTest;
            Config.deploy_mode = savedDeployMode;
            Config.cloud_unique_id = savedCloudUniqueId;
        }
    }
}
