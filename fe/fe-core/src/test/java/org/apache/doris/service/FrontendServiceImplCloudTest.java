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

import org.apache.doris.catalog.ColocateTableIndex;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.TabletInvertedIndex;
import org.apache.doris.cloud.CacheHotspotManager;
import org.apache.doris.cloud.catalog.CloudEnv;
import org.apache.doris.cloud.catalog.CloudReplica;
import org.apache.doris.common.Config;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TGetTabletReplicaInfosRequest;
import org.apache.doris.thrift.TGetTabletReplicaInfosResult;
import org.apache.doris.thrift.TStatusCode;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Collections;

public class FrontendServiceImplCloudTest {

    @Test
    public void testSecondaryOnlyRouteIsDiscovered() {
        String saved = Config.cloud_unique_id;
        Config.cloud_unique_id = "secondary-peer-test";
        try (MockedStatic<Env> envMock = Mockito.mockStatic(Env.class)) {
            envMock.when(Env::getCurrentEnv).thenReturn(Mockito.mock(CloudEnv.class));
            envMock.when(Env::getCurrentColocateIndex).thenReturn(Mockito.mock(ColocateTableIndex.class));
            SystemInfoService systemInfo = Mockito.mock(SystemInfoService.class);
            envMock.when(Env::getCurrentSystemInfo).thenReturn(systemInfo);
            Backend backend = new Backend(12L, "127.0.0.1", 9050);
            Mockito.when(systemInfo.getBackend(12L)).thenReturn(backend);
            Mockito.when(systemInfo.getBackendByIdWithBoxedId(12L)).thenReturn(backend);
            CloudReplica replica = new CloudReplica(1L, -1L, Replica.ReplicaState.NORMAL, 1L, 0,
                    2L, 3L, 4L, 5L, 0L);
            // Cleanup and secondary publication can leave this valid secondary without a primary key.
            replica.updateClusterToSecondaryBe("cg", 12L);
            TabletInvertedIndex index = Mockito.mock(TabletInvertedIndex.class);
            envMock.when(Env::getCurrentInvertedIndex).thenReturn(index);
            Mockito.when(index.getReplicasByTabletId(789L)).thenReturn(Collections.singletonList(replica));
            TGetTabletReplicaInfosRequest request = new TGetTabletReplicaInfosRequest();
            request.setTabletIds(Collections.singletonList(789L));
            TGetTabletReplicaInfosResult result = new FrontendServiceImpl(Mockito.mock(ExecuteEnv.class))
                    .getTabletReplicaInfos(request);
            Assertions.assertEquals(1, result.getTabletReplicaInfos().get(789L).size());
        } finally {
            Config.cloud_unique_id = saved;
        }
    }

    // Regression test for FrontendServiceImpl.getTabletReplicaInfos NPE:
    // When a warm-up job has been removed from
    // CacheHotspotManager.cloudWarmUpJobs (past
    // history_cloud_warm_up_job_keep_max_second), getCloudWarmUpJob
    // returns null. The previous code called job.getJobId() inside the
    // log message, throwing NPE which bubbled up to BE as
    // "Internal error processing getTabletReplicaInfos".
    @Test
    public void testGetTabletReplicaInfosNullJobReturnsCancelledWithoutNpe() {
        String originalCloudUniqueId = Config.cloud_unique_id;
        Config.cloud_unique_id = "gettabletreplicainfostest";

        CloudEnv cloudEnv = Mockito.mock(CloudEnv.class);
        CacheHotspotManager cacheHotspotManager = Mockito.mock(CacheHotspotManager.class);
        Mockito.when(cloudEnv.getCacheHotspotMgr()).thenReturn(cacheHotspotManager);
        // Simulate job already removed from cloudWarmUpJobs.
        Mockito.when(cacheHotspotManager.getCloudWarmUpJob(123456L)).thenReturn(null);

        try (MockedStatic<Env> envMock = Mockito.mockStatic(Env.class)) {
            envMock.when(Env::getCurrentEnv).thenReturn(cloudEnv);

            FrontendServiceImpl frontendService = new FrontendServiceImpl(Mockito.mock(ExecuteEnv.class));
            TGetTabletReplicaInfosRequest request = new TGetTabletReplicaInfosRequest();
            request.setTabletIds(Collections.singletonList(789L));
            request.setWarmUpJobId(123456L);

            TGetTabletReplicaInfosResult result;
            try {
                result = frontendService.getTabletReplicaInfos(request);
            } catch (NullPointerException e) {
                throw new AssertionError("getTabletReplicaInfos must not NPE when the "
                        + "warm-up job has been removed from CacheHotspotManager", e);
            }

            Assertions.assertNotNull(result.getStatus(), "result.status must be set");
            Assertions.assertEquals(TStatusCode.CANCELLED, result.getStatus().getStatusCode(), "BE must be told to cancel its stale warm-up job entry");
        } finally {
            Config.cloud_unique_id = originalCloudUniqueId;
        }
    }
}
