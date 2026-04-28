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

import org.apache.doris.catalog.Env;
import org.apache.doris.cloud.CloudWarmUpJob.JobType;
import org.apache.doris.cloud.CloudWarmUpJob.SyncEvent;
import org.apache.doris.cloud.CloudWarmUpJob.SyncMode;
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.system.Backend;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public class CloudWarmUpJobTest {
    @Test
    public void testEventDrivenRefreshesSourceBackends() {
        CloudSystemInfoService cloudSystemInfoService = Mockito.mock(CloudSystemInfoService.class);
        AtomicReference<String> requestedCluster = new AtomicReference<>();
        Mockito.when(cloudSystemInfoService.getBackendsByClusterName(Mockito.anyString())).thenAnswer(invocation -> {
            requestedCluster.set(invocation.getArgument(0));
            Backend backend1 = new Backend(1L, "host1", 9050);
            backend1.setBePort(9060);
            Backend backend2 = new Backend(2L, "host2", 9050);
            backend2.setBePort(9061);
            return Arrays.asList(backend1, backend2);
        });

        CloudWarmUpJob warmUpJob = new CloudWarmUpJob.Builder()
                .setJobId(100L)
                .setSrcClusterName("src_cluster")
                .setDstClusterName("dst_cluster")
                .setJobType(JobType.CLUSTER)
                .setSyncMode(SyncMode.EVENT_DRIVEN)
                .setSyncEvent(SyncEvent.LOAD)
                .build();
        Map<Long, String> staleBeToThriftAddress = new HashMap<>();
        staleBeToThriftAddress.put(1L, "host1:9060");
        warmUpJob.setBeToThriftAddress(staleBeToThriftAddress);

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentSystemInfo).thenReturn(cloudSystemInfoService);
            warmUpJob.refreshEventDrivenBeToThriftAddress();
        }

        Assert.assertEquals("src_cluster", requestedCluster.get());
        Assert.assertEquals(2, warmUpJob.getBeToThriftAddress().size());
        Assert.assertEquals("host1:9060", warmUpJob.getBeToThriftAddress().get(1L));
        Assert.assertEquals("host2:9061", warmUpJob.getBeToThriftAddress().get(2L));
    }
}
