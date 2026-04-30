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
import org.apache.doris.cloud.CloudWarmUpJob.JobState;
import org.apache.doris.cloud.CloudWarmUpJob.JobType;
import org.apache.doris.cloud.CloudWarmUpJob.SyncEvent;
import org.apache.doris.cloud.CloudWarmUpJob.SyncMode;
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.common.ClientPool;
import org.apache.doris.common.GenericPool;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.BackendService;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TWarmUpTabletsRequest;
import org.apache.doris.thrift.TWarmUpTabletsRequestType;
import org.apache.doris.thrift.TWarmUpTabletsResponse;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public class CloudWarmUpJobTest {
    private GenericPool<BackendService.Client> originalBackendPool;
    private GenericPool<BackendService.Client> mockBackendPool;

    @SuppressWarnings("unchecked")
    @Before
    public void setUp() {
        originalBackendPool = ClientPool.backendPool;
        mockBackendPool = Mockito.mock(GenericPool.class);
        ClientPool.backendPool = mockBackendPool;
    }

    @After
    public void tearDown() {
        ClientPool.backendPool = originalBackendPool;
    }

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

    @Test
    public void testInitClientsKeepsFailFastForWarmUpRpc() throws Exception {
        long jobId = 12345L;
        TNetworkAddress firstAddress = new TNetworkAddress("127.0.0.1", 9050);
        TNetworkAddress secondAddress = new TNetworkAddress("127.0.0.2", 9050);
        BackendService.Client firstClient = Mockito.mock(BackendService.Client.class);
        CloudWarmUpJob job = createRunningJob(jobId, firstAddress, secondAddress);

        Mockito.when(mockBackendPool.borrowObject(firstAddress)).thenReturn(firstClient);
        Mockito.when(mockBackendPool.borrowObject(secondAddress)).thenThrow(new RuntimeException("down"));

        Assert.assertThrows(RuntimeException.class, job::initClients);
        Mockito.verify(mockBackendPool).returnObject(firstAddress, firstClient);
        Mockito.verify(mockBackendPool).invalidateObject(secondAddress, null);
    }

    @Test
    public void testClearJobSkipsUnavailableBackendAndClearsAvailableBackend() throws Exception {
        long jobId = 12346L;
        TNetworkAddress unavailableAddress = new TNetworkAddress("127.0.0.1", 9050);
        TNetworkAddress availableAddress = new TNetworkAddress("127.0.0.2", 9050);
        BackendService.Client availableClient = Mockito.mock(BackendService.Client.class);
        CloudWarmUpJob job = createRunningJob(jobId, unavailableAddress, availableAddress);

        Mockito.when(mockBackendPool.borrowObject(unavailableAddress)).thenThrow(new RuntimeException("down"));
        Mockito.when(mockBackendPool.borrowObject(availableAddress)).thenReturn(availableClient);
        Mockito.when(availableClient.warmUpTablets(Mockito.any(TWarmUpTabletsRequest.class)))
                .thenReturn(new TWarmUpTabletsResponse());

        invokeClearJobOnBEs(job);

        ArgumentCaptor<TWarmUpTabletsRequest> captor = ArgumentCaptor.forClass(TWarmUpTabletsRequest.class);
        Mockito.verify(availableClient).warmUpTablets(captor.capture());
        TWarmUpTabletsRequest request = captor.getValue();
        Assert.assertEquals(TWarmUpTabletsRequestType.CLEAR_JOB, request.getType());
        Assert.assertEquals(jobId, request.getJobId());
        Mockito.verify(mockBackendPool).returnObject(availableAddress, availableClient);
        Mockito.verify(mockBackendPool).invalidateObject(unavailableAddress, null);
    }

    private CloudWarmUpJob createRunningJob(long jobId, TNetworkAddress firstAddress,
            TNetworkAddress secondAddress) {
        CloudWarmUpJob job = new CloudWarmUpJob.Builder()
                .setJobId(jobId)
                .setSrcClusterName("source_cluster")
                .setDstClusterName("target_cluster")
                .build();
        job.setJobState(JobState.RUNNING);
        Map<Long, String> beToThriftAddress = new LinkedHashMap<>();
        beToThriftAddress.put(1L, firstAddress.getHostname() + ":" + firstAddress.getPort());
        beToThriftAddress.put(2L, secondAddress.getHostname() + ":" + secondAddress.getPort());
        job.setBeToThriftAddress(beToThriftAddress);
        return job;
    }

    private void invokeClearJobOnBEs(CloudWarmUpJob job) throws Exception {
        Method method = CloudWarmUpJob.class.getDeclaredMethod("clearJobOnBEs");
        method.setAccessible(true);
        method.invoke(job);
    }
}
