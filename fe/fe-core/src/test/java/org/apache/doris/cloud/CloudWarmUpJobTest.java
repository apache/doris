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
import org.apache.doris.cloud.catalog.CloudEnv;
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.common.ClientPool;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.GenericPool;
import org.apache.doris.persist.EditLog;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.BackendService;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TStatus;
import org.apache.doris.thrift.TStatusCode;
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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public class CloudWarmUpJobTest {
    private static final int COL_ERR_MSG = 11;

    private GenericPool<BackendService.Client> originalBackendPool;
    private GenericPool<BackendService.Client> mockBackendPool;
    private boolean originalRunningUnitTest;

    @SuppressWarnings("unchecked")
    @Before
    public void setUp() {
        originalRunningUnitTest = FeConstants.runningUnitTest;
        FeConstants.runningUnitTest = true;
        originalBackendPool = ClientPool.backendPool;
        mockBackendPool = Mockito.mock(GenericPool.class);
        ClientPool.backendPool = mockBackendPool;
    }

    @After
    public void tearDown() {
        ClientPool.backendPool = originalBackendPool;
        FeConstants.runningUnitTest = originalRunningUnitTest;
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

    @Test
    public void testPendingRetryKeepsErrMsgWhenJobStarts() throws Exception {
        CloudWarmUpJob job = new CloudWarmUpJob.Builder()
                .setJobId(200L)
                .setSrcClusterName("source_cluster")
                .setDstClusterName("target_cluster")
                .setJobType(JobType.CLUSTER)
                .setSyncMode(SyncMode.PERIODIC)
                .setSyncInterval(60L)
                .build();
        job.setErrMsg("previous failure");

        CloudEnv cloudEnv = Mockito.mock(CloudEnv.class);
        CacheHotspotManager cacheHotspotManager = Mockito.mock(CacheHotspotManager.class);
        EditLog editLog = Mockito.mock(EditLog.class);
        Mockito.when(cloudEnv.getCacheHotspotMgr()).thenReturn(cacheHotspotManager);
        Mockito.when(cloudEnv.getEditLog()).thenReturn(editLog);
        Mockito.when(cacheHotspotManager.tryRegisterRunningJob(job)).thenReturn(true);

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(cloudEnv);
            invokeRunPendingJob(job);
        }

        Assert.assertEquals(JobState.RUNNING, job.getJobState());
        Assert.assertEquals("previous failure", job.getJobInfo(null).get(COL_ERR_MSG));
        Mockito.verify(editLog).logModifyCloudWarmUpJob(job);
    }

    @Test
    public void testEventDrivenSuccessfulRetryClearsErrMsg() throws Exception {
        CloudSystemInfoService cloudSystemInfoService = Mockito.mock(CloudSystemInfoService.class);
        Backend backend = new Backend(1L, "host1", 9050);
        backend.setBePort(9060);
        Mockito.when(cloudSystemInfoService.getBackendsByClusterName("source_cluster"))
                .thenReturn(Arrays.asList(backend));

        TNetworkAddress address = new TNetworkAddress("host1", 9060);
        BackendService.Client client = Mockito.mock(BackendService.Client.class);
        Mockito.when(mockBackendPool.borrowObject(address)).thenReturn(client);
        Mockito.when(client.warmUpTablets(Mockito.any(TWarmUpTabletsRequest.class)))
                .thenReturn(okWarmUpResponse());
        CloudEnv cloudEnv = Mockito.mock(CloudEnv.class);
        EditLog editLog = Mockito.mock(EditLog.class);
        Mockito.when(cloudEnv.getEditLog()).thenReturn(editLog);

        CloudWarmUpJob job = new CloudWarmUpJob.Builder()
                .setJobId(201L)
                .setSrcClusterName("source_cluster")
                .setDstClusterName("target_cluster")
                .setJobType(JobType.CLUSTER)
                .setSyncMode(SyncMode.EVENT_DRIVEN)
                .setSyncEvent(SyncEvent.LOAD)
                .build();
        job.setJobState(JobState.RUNNING);
        setStartTimeMs(job, System.currentTimeMillis());
        job.setErrMsg("previous failure");

        boolean runningUnitTest = FeConstants.runningUnitTest;
        FeConstants.runningUnitTest = false;
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentSystemInfo).thenReturn(cloudSystemInfoService);
            mockedEnv.when(Env::getCurrentEnv).thenReturn(cloudEnv);
            job.run();
            job.run();
        } finally {
            FeConstants.runningUnitTest = runningUnitTest;
        }

        Assert.assertEquals("", job.getJobInfo(null).get(COL_ERR_MSG));
        ArgumentCaptor<CloudWarmUpJob> jobCaptor = ArgumentCaptor.forClass(CloudWarmUpJob.class);
        Mockito.verify(editLog, Mockito.times(1)).logModifyCloudWarmUpJob(jobCaptor.capture());
        CloudWarmUpJob replayedJob = copyBySerialization(jobCaptor.getValue());
        Assert.assertEquals("", replayedJob.getJobInfo(null).get(COL_ERR_MSG));
        Mockito.verify(client, Mockito.times(2)).warmUpTablets(Mockito.any(TWarmUpTabletsRequest.class));
        Mockito.verify(mockBackendPool, Mockito.times(2)).returnObject(address, client);
    }

    @Test
    public void testEventDrivenFailedRetryKeepsErrMsg() throws Exception {
        CloudSystemInfoService cloudSystemInfoService = Mockito.mock(CloudSystemInfoService.class);
        Backend backend = new Backend(1L, "host1", 9050);
        backend.setBePort(9060);
        Mockito.when(cloudSystemInfoService.getBackendsByClusterName("source_cluster"))
                .thenReturn(Arrays.asList(backend));

        TNetworkAddress address = new TNetworkAddress("host1", 9060);
        BackendService.Client client = Mockito.mock(BackendService.Client.class);
        Mockito.when(mockBackendPool.borrowObject(address)).thenReturn(client);
        Mockito.when(client.warmUpTablets(Mockito.any(TWarmUpTabletsRequest.class)))
                .thenReturn(failedWarmUpResponse());

        CloudWarmUpJob job = new CloudWarmUpJob.Builder()
                .setJobId(203L)
                .setSrcClusterName("source_cluster")
                .setDstClusterName("target_cluster")
                .setJobType(JobType.CLUSTER)
                .setSyncMode(SyncMode.EVENT_DRIVEN)
                .setSyncEvent(SyncEvent.LOAD)
                .build();
        job.setJobState(JobState.RUNNING);
        setStartTimeMs(job, System.currentTimeMillis());
        job.setErrMsg("previous failure");

        boolean runningUnitTest = FeConstants.runningUnitTest;
        FeConstants.runningUnitTest = false;
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentSystemInfo).thenReturn(cloudSystemInfoService);
            job.run();
        } finally {
            FeConstants.runningUnitTest = runningUnitTest;
        }

        Assert.assertEquals("previous failure", job.getJobInfo(null).get(COL_ERR_MSG));
        Mockito.verify(client).warmUpTablets(Mockito.any(TWarmUpTabletsRequest.class));
        Mockito.verify(mockBackendPool).returnObject(address, client);
    }

    @Test
    public void testRunningRetryClearsErrMsgWhenJobFinishes() throws Exception {
        TNetworkAddress address = new TNetworkAddress("127.0.0.1", 9050);
        BackendService.Client client = Mockito.mock(BackendService.Client.class);
        Mockito.when(mockBackendPool.borrowObject(address)).thenReturn(client);
        Mockito.when(client.warmUpTablets(Mockito.any(TWarmUpTabletsRequest.class)))
                .thenReturn(okWarmUpResponse());

        CloudWarmUpJob job = new CloudWarmUpJob.Builder()
                .setJobId(202L)
                .setSrcClusterName("source_cluster")
                .setDstClusterName("target_cluster")
                .setJobType(JobType.CLUSTER)
                .setSyncMode(SyncMode.PERIODIC)
                .setSyncInterval(60L)
                .build();
        job.setJobState(JobState.RUNNING);
        setStartTimeMs(job, System.currentTimeMillis());
        setSetJobDone(job, true);
        job.setErrMsg("previous failure");

        Map<Long, List<List<Long>>> beToTabletIdBatches = new HashMap<>();
        beToTabletIdBatches.put(1L, Collections.emptyList());
        job.setBeToTabletIdBatches(beToTabletIdBatches);
        Map<Long, String> beToThriftAddress = new HashMap<>();
        beToThriftAddress.put(1L, address.getHostname() + ":" + address.getPort());
        job.setBeToThriftAddress(beToThriftAddress);

        CloudEnv cloudEnv = Mockito.mock(CloudEnv.class);
        CacheHotspotManager cacheHotspotManager = Mockito.mock(CacheHotspotManager.class);
        EditLog editLog = Mockito.mock(EditLog.class);
        Mockito.when(cloudEnv.getCacheHotspotMgr()).thenReturn(cacheHotspotManager);
        Mockito.when(cloudEnv.getEditLog()).thenReturn(editLog);

        boolean runningUnitTest = FeConstants.runningUnitTest;
        FeConstants.runningUnitTest = false;
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(cloudEnv);
            invokeRunRunningJob(job);
        } finally {
            FeConstants.runningUnitTest = runningUnitTest;
        }

        Assert.assertEquals(JobState.PENDING, job.getJobState());
        Assert.assertEquals("", job.getJobInfo(null).get(COL_ERR_MSG));
        Mockito.verify(cacheHotspotManager).notifyJobStop(job);
        Mockito.verify(editLog, Mockito.atLeastOnce()).logModifyCloudWarmUpJob(job);
        Mockito.verify(mockBackendPool).returnObject(address, client);
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

    private void invokeRunPendingJob(CloudWarmUpJob job) throws Exception {
        Method method = CloudWarmUpJob.class.getDeclaredMethod("runPendingJob");
        method.setAccessible(true);
        method.invoke(job);
    }

    private void invokeRunRunningJob(CloudWarmUpJob job) throws Exception {
        Method method = CloudWarmUpJob.class.getDeclaredMethod("runRunningJob");
        method.setAccessible(true);
        method.invoke(job);
    }

    private void setStartTimeMs(CloudWarmUpJob job, long startTimeMs) throws Exception {
        java.lang.reflect.Field field = CloudWarmUpJob.class.getDeclaredField("startTimeMs");
        field.setAccessible(true);
        field.setLong(job, startTimeMs);
    }

    private void setSetJobDone(CloudWarmUpJob job, boolean setJobDone) throws Exception {
        java.lang.reflect.Field field = CloudWarmUpJob.class.getDeclaredField("setJobDone");
        field.setAccessible(true);
        field.setBoolean(job, setJobDone);
    }

    private CloudWarmUpJob copyBySerialization(CloudWarmUpJob job) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            job.write(out);
        }
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            return CloudWarmUpJob.read(in);
        }
    }

    private TWarmUpTabletsResponse okWarmUpResponse() {
        TWarmUpTabletsResponse response = new TWarmUpTabletsResponse();
        response.setStatus(new TStatus(TStatusCode.OK));
        return response;
    }

    private TWarmUpTabletsResponse failedWarmUpResponse() {
        TWarmUpTabletsResponse response = new TWarmUpTabletsResponse();
        response.setStatus(new TStatus(TStatusCode.INTERNAL_ERROR));
        response.getStatus().setErrorMsgs(Collections.emptyList());
        return response;
    }
}
