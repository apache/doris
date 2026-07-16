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

import org.apache.doris.catalog.Env;
import org.apache.doris.cloud.CacheHotspotManager;
import org.apache.doris.cloud.catalog.CloudEnv;
import org.apache.doris.common.Config;
import org.apache.doris.thrift.TGetTabletReplicaInfosRequest;
import org.apache.doris.thrift.TGetTabletReplicaInfosResult;
import org.apache.doris.thrift.TStatusCode;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Collections;

public class FrontendServiceImplCloudTest {

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

            Assert.assertNotNull("result.status must be set", result.getStatus());
            Assert.assertEquals("BE must be told to cancel its stale warm-up job entry",
                    TStatusCode.CANCELLED, result.getStatus().getStatusCode());
        } finally {
            Config.cloud_unique_id = originalCloudUniqueId;
        }
    }
}
