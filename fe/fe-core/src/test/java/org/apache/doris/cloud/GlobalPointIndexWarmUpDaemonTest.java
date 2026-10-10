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

package org.apache.doris.cloud;

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Index;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndex.IndexExtState;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.info.IndexType;
import org.apache.doris.cloud.catalog.CloudReplica;
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.common.Config;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.proto.InternalService;
import org.apache.doris.rpc.BackendServiceProxy;
import org.apache.doris.system.Backend;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

/**
 * A BE whose process never restarts must still get repair sweeps: its lastStartTime does not change,
 * so a daemon that only warmed on a new lastStartTime would never look at it again.
 */
public class GlobalPointIndexWarmUpDaemonTest {
    private static final long BE_ID = 10001L;
    private static final long TABLET_ID = 20001L;

    private String originalDeployMode;
    private boolean originalEnableWarmup;
    private boolean originalEnableRepair;
    private int originalRepairIntervalSec;

    @BeforeEach
    public void setUp() {
        originalDeployMode = Config.deploy_mode;
        Config.deploy_mode = "cloud"; // the daemon only runs in cloud mode
        originalEnableWarmup = Config.enable_global_point_index_warmup;
        originalEnableRepair = Config.enable_global_point_index_repair;
        originalRepairIntervalSec = Config.global_point_index_repair_interval_sec;
        Config.enable_global_point_index_warmup = true;
        Config.enable_global_point_index_repair = true;
        Config.global_point_index_repair_interval_sec = 600;
    }

    @AfterEach
    public void tearDown() {
        Config.deploy_mode = originalDeployMode;
        Config.enable_global_point_index_warmup = originalEnableWarmup;
        Config.enable_global_point_index_repair = originalEnableRepair;
        Config.global_point_index_repair_interval_sec = originalRepairIntervalSec;
    }

    private Backend aliveBackend(long lastStartTime) {
        Backend be = new Backend(BE_ID, "host1", 9050);
        be.setAlive(true);
        be.setLastStartTime(lastStartTime);
        return be;
    }

    /** Builds a minimal one-DB / one-table / one-tablet catalog that maps TABLET_ID to BE_ID. */
    private InternalCatalog mockCatalogWithOneGlobalPointTablet() {
        Index gpIndex = Mockito.mock(Index.class);
        Mockito.when(gpIndex.getIndexType()).thenReturn(IndexType.GLOBAL_POINT);

        Tablet tablet = Mockito.mock(Tablet.class);
        Mockito.when(tablet.getId()).thenReturn(TABLET_ID);
        CloudReplica replica = Mockito.mock(CloudReplica.class);
        try {
            Mockito.when(replica.getBackendIdWithClusterId(Mockito.anyString())).thenReturn(BE_ID);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        Mockito.when(tablet.getReplicas()).thenReturn(Lists.newArrayList(replica));

        MaterializedIndex materializedIndex = Mockito.mock(MaterializedIndex.class);
        Mockito.when(materializedIndex.getTablets()).thenReturn(Lists.newArrayList(tablet));

        Partition partition = Mockito.mock(Partition.class);
        Mockito.when(partition.getMaterializedIndices(IndexExtState.VISIBLE))
                .thenReturn(Lists.newArrayList(materializedIndex));

        OlapTable table = Mockito.mock(OlapTable.class);
        Mockito.when(table.getIndexes()).thenReturn(Lists.newArrayList(gpIndex));
        Mockito.when(table.getPartitions()).thenReturn(Lists.newArrayList(partition));

        Database db = Mockito.mock(Database.class);
        Mockito.doReturn(Lists.<Table>newArrayList(table)).when(db).getTables();

        InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
        Mockito.when(catalog.getDbIds()).thenReturn(Collections.singletonList(1L));
        Mockito.when(catalog.getDbNullable(1L)).thenReturn(db);
        return catalog;
    }

    private CloudSystemInfoService mockSystemInfoService(Backend be) throws Exception {
        CloudSystemInfoService infoService = Mockito.mock(CloudSystemInfoService.class);
        Mockito.when(infoService.getAllBackendsByAllCluster())
                .thenReturn(ImmutableMap.of(BE_ID, be));
        Mockito.when(infoService.getClusterNameByBeAddr(Mockito.anyString())).thenReturn("cluster1");
        Mockito.when(infoService.getPhysicalCluster("cluster1")).thenReturn("physical1");
        Mockito.when(infoService.resolveClusterIdByName("physical1")).thenReturn("clusterId1");
        return infoService;
    }

    @SuppressWarnings("unchecked")
    private static void setPrivateLongMapEntry(Object target, String fieldName, long key, long value)
            throws Exception {
        Field field = GlobalPointIndexWarmUpDaemon.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        ((Map<Long, Long>) field.get(target)).put(key, value);
    }

    private static InternalService.PGpIdxWarmUpResponse okResponse() {
        return InternalService.PGpIdxWarmUpResponse.newBuilder()
                .setStatus(org.apache.doris.proto.Types.PStatus.newBuilder().setStatusCode(0))
                .setAcceptedTablets(1)
                .setRejectedTablets(0)
                .build();
    }

    @Test
    public void testUnchangedLastStartTimeStillSweptAsRepair() throws Exception {
        Backend be = aliveBackend(/* lastStartTime */ 1000L);
        InternalCatalog catalog = mockCatalogWithOneGlobalPointTablet();
        CloudSystemInfoService infoService = mockSystemInfoService(be);

        GlobalPointIndexWarmUpDaemon daemon = new GlobalPointIndexWarmUpDaemon();
        // Already warmed in this BE process, and a repair sweep is due.
        setPrivateLongMapEntry(daemon, "warmedStartTime", BE_ID, be.getLastStartTime());
        setPrivateLongMapEntry(daemon, "lastRepairTimeMs", BE_ID, 0L);

        ArgumentCaptor<InternalService.PGpIdxWarmUpRequest> requestCaptor =
                ArgumentCaptor.forClass(InternalService.PGpIdxWarmUpRequest.class);
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class);
                MockedStatic<BackendServiceProxy> mockedProxy =
                        Mockito.mockStatic(BackendServiceProxy.class)) {
            mockedEnv.when(Env::getCurrentSystemInfo).thenReturn(infoService);
            mockedEnv.when(Env::getCurrentInternalCatalog).thenReturn(catalog);

            BackendServiceProxy proxy = Mockito.mock(BackendServiceProxy.class);
            mockedProxy.when(BackendServiceProxy::getInstance).thenReturn(proxy);
            Future<InternalService.PGpIdxWarmUpResponse> future =
                    CompletableFuture.completedFuture(okResponse());
            Mockito.when(proxy.warmUpGlobalPointIndexAsync(Mockito.any(), requestCaptor.capture()))
                    .thenReturn(future);

            daemon.runAfterCatalogReady();
        }

        Assertions.assertEquals(1, requestCaptor.getAllValues().size(),
                "BE with an unchanged lastStartTime must still be swept, not skipped");
        InternalService.PGpIdxWarmUpRequest sent = requestCaptor.getValue();
        Assertions.assertTrue(sent.getIsRepairSweep(),
                "an already warmed BE that is due for repair must get a repair sweep");
        Assertions.assertEquals(TABLET_ID, sent.getTabletIds(0));
    }

    @Test
    public void testFirstSightingIsInitialWarmUpNotRepair() throws Exception {
        Backend be = aliveBackend(/* lastStartTime */ 2000L);
        InternalCatalog catalog = mockCatalogWithOneGlobalPointTablet();
        CloudSystemInfoService infoService = mockSystemInfoService(be);

        GlobalPointIndexWarmUpDaemon daemon = new GlobalPointIndexWarmUpDaemon();
        // Never seen before.

        ArgumentCaptor<InternalService.PGpIdxWarmUpRequest> requestCaptor =
                ArgumentCaptor.forClass(InternalService.PGpIdxWarmUpRequest.class);
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class);
                MockedStatic<BackendServiceProxy> mockedProxy =
                        Mockito.mockStatic(BackendServiceProxy.class)) {
            mockedEnv.when(Env::getCurrentSystemInfo).thenReturn(infoService);
            mockedEnv.when(Env::getCurrentInternalCatalog).thenReturn(catalog);

            BackendServiceProxy proxy = Mockito.mock(BackendServiceProxy.class);
            mockedProxy.when(BackendServiceProxy::getInstance).thenReturn(proxy);
            Future<InternalService.PGpIdxWarmUpResponse> future =
                    CompletableFuture.completedFuture(okResponse());
            Mockito.when(proxy.warmUpGlobalPointIndexAsync(Mockito.any(), requestCaptor.capture()))
                    .thenReturn(future);

            daemon.runAfterCatalogReady();
        }

        Assertions.assertEquals(1, requestCaptor.getAllValues().size());
        Assertions.assertFalse(requestCaptor.getValue().getIsRepairSweep(),
                "a BE seen for the first time must get an initial warm-up");
    }

    @Test
    public void testUnresolvedClusterMappingRetriesWithoutSendingRpc() throws Exception {
        Backend be = aliveBackend(3000L);
        InternalCatalog catalog = mockCatalogWithOneGlobalPointTablet();
        CloudSystemInfoService infoService = mockSystemInfoService(be);
        // Not in a compute group yet: no RPC, retry next round.
        Mockito.when(infoService.getClusterNameByBeAddr(Mockito.anyString())).thenReturn(null);

        GlobalPointIndexWarmUpDaemon daemon = new GlobalPointIndexWarmUpDaemon();

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class);
                MockedStatic<BackendServiceProxy> mockedProxy =
                        Mockito.mockStatic(BackendServiceProxy.class)) {
            mockedEnv.when(Env::getCurrentSystemInfo).thenReturn(infoService);
            mockedEnv.when(Env::getCurrentInternalCatalog).thenReturn(catalog);
            BackendServiceProxy proxy = Mockito.mock(BackendServiceProxy.class);
            mockedProxy.when(BackendServiceProxy::getInstance).thenReturn(proxy);

            daemon.runAfterCatalogReady();

            Mockito.verify(proxy, Mockito.never())
                    .warmUpGlobalPointIndexAsync(Mockito.any(), Mockito.any());
        }
    }
}
