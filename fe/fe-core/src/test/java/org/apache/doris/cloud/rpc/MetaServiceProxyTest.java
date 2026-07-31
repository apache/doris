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

package org.apache.doris.cloud.rpc;

import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.common.Config;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.rpc.RpcException;

import com.google.common.util.concurrent.SettableFuture;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;

public class MetaServiceProxyTest {
    private static final int CPU_CORES = Runtime.getRuntime().availableProcessors();

    private String originEndpoint;
    private long originReconnectIntervalMs;
    private long originRetryCnt;
    private boolean originRateLimitEnabled;
    private int originRateLimitDefaultQpsPerCore;
    private String originRateLimitQpsPerCoreConfig;
    private int originRateLimitBurstSeconds;
    private long originRateLimitWaitTimeoutMs;

    @Before
    public void setUp() {
        originEndpoint = Config.meta_service_endpoint;
        originReconnectIntervalMs = Config.meta_service_rpc_reconnect_interval_ms;
        originRetryCnt = Config.meta_service_rpc_retry_cnt;
        originRateLimitEnabled = Config.meta_service_rpc_rate_limit_enabled;
        originRateLimitDefaultQpsPerCore = Config.meta_service_rpc_rate_limit_default_qps_per_core;
        originRateLimitQpsPerCoreConfig = Config.meta_service_rpc_rate_limit_qps_per_core_config;
        originRateLimitBurstSeconds = Config.meta_service_rpc_rate_limit_burst_seconds;
        originRateLimitWaitTimeoutMs = Config.meta_service_rpc_rate_limit_wait_timeout_ms;

        Config.meta_service_endpoint = "127.0.0.1:12345";
        Config.meta_service_rpc_reconnect_interval_ms = 0;
        Config.meta_service_rpc_retry_cnt = 1;
        Config.meta_service_rpc_rate_limit_enabled = false;
        Config.meta_service_rpc_rate_limit_default_qps_per_core = 10;
        Config.meta_service_rpc_rate_limit_qps_per_core_config = "";
        Config.meta_service_rpc_rate_limit_burst_seconds = 1;
        Config.meta_service_rpc_rate_limit_wait_timeout_ms = 0;
        MetaServiceProxy.resetMetaServiceRpcRateLimitForTest();
    }

    @After
    public void tearDown() {
        Config.meta_service_endpoint = originEndpoint;
        Config.meta_service_rpc_reconnect_interval_ms = originReconnectIntervalMs;
        Config.meta_service_rpc_retry_cnt = originRetryCnt;
        Config.meta_service_rpc_rate_limit_enabled = originRateLimitEnabled;
        Config.meta_service_rpc_rate_limit_default_qps_per_core = originRateLimitDefaultQpsPerCore;
        Config.meta_service_rpc_rate_limit_qps_per_core_config = originRateLimitQpsPerCoreConfig;
        Config.meta_service_rpc_rate_limit_burst_seconds = originRateLimitBurstSeconds;
        Config.meta_service_rpc_rate_limit_wait_timeout_ms = originRateLimitWaitTimeoutMs;
        MetaServiceProxy.resetMetaServiceRpcRateLimitForTest();
    }

    @Test
    public void testExecuteRequestNoShutdownOnSuccess() throws RpcException {
        MetaServiceProxy proxy = new MetaServiceProxy();
        MetaServiceClient client = mockNormalClient();

        Map<String, MetaServiceClient> serviceMap = Deencapsulation.getField(proxy, "serviceMap");
        serviceMap.put(Config.meta_service_endpoint, client);
        Queue<Long> lastConnTimeMs = Deencapsulation.getField(proxy, "lastConnTimeMs");
        lastConnTimeMs.clear();
        lastConnTimeMs.add(0L);
        lastConnTimeMs.add(0L);
        lastConnTimeMs.add(0L);

        MetaServiceProxy.MetaServiceClientWrapper wrapper = Deencapsulation.getField(proxy, "w");
        Cloud.GetVersionResponse okResponse = Cloud.GetVersionResponse.newBuilder()
                .setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                        .setCode(Cloud.MetaServiceCode.OK))
                .build();
        Cloud.GetVersionResponse response = wrapper.executeRequest("ignored", (ignored) -> okResponse,
                Cloud.GetVersionResponse::getStatus);
        Assert.assertEquals(Cloud.MetaServiceCode.OK, response.getStatus().getCode());
        Mockito.verify(client, Mockito.never()).shutdown(Mockito.anyBoolean());
    }

    @Test
    public void testExecuteRequestShutdownOnFailure() {
        MetaServiceProxy proxy = new MetaServiceProxy();
        MetaServiceClient client = mockNormalClient();

        Map<String, MetaServiceClient> serviceMap = Deencapsulation.getField(proxy, "serviceMap");
        serviceMap.put(Config.meta_service_endpoint, client);
        Queue<Long> lastConnTimeMs = Deencapsulation.getField(proxy, "lastConnTimeMs");
        lastConnTimeMs.clear();
        lastConnTimeMs.add(0L);
        lastConnTimeMs.add(0L);
        lastConnTimeMs.add(0L);

        MetaServiceProxy.MetaServiceClientWrapper wrapper = Deencapsulation.getField(proxy, "w");
        try {
            wrapper.executeRequest("ignored", (ignored) -> {
                throw new RuntimeException("rpc failed");
            }, Cloud.GetVersionResponse::getStatus);
            Assert.fail("should throw RpcException");
        } catch (RpcException ignored) {
            // expected
        }

        Mockito.verify(client).shutdown(true);
    }

    @Test
    public void testGetVisibleVersionAsyncShutdownOnFailure() throws RpcException {
        MetaServiceProxy proxy = new MetaServiceProxy();
        MetaServiceClient client = mockNormalClient();

        SettableFuture<Cloud.GetVersionResponse> future = SettableFuture.create();
        Mockito.when(client.getVisibleVersionAsync(Mockito.any())).thenReturn(future);

        Map<String, MetaServiceClient> serviceMap = Deencapsulation.getField(proxy, "serviceMap");
        serviceMap.put(Config.meta_service_endpoint, client);

        Cloud.GetVersionRequest request = Cloud.GetVersionRequest.newBuilder().build();
        proxy.getVisibleVersionAsync(request);

        future.setException(new RuntimeException("async failed"));

        Mockito.verify(client).shutdown(true);
    }

    @Test
    public void testExecuteRequestNoShutdownOnTooBusyFailure() throws RpcException {
        MetaServiceProxy proxy = new MetaServiceProxy();
        MetaServiceClient client = mockNormalClient();

        Map<String, MetaServiceClient> serviceMap = Deencapsulation.getField(proxy, "serviceMap");
        serviceMap.put(Config.meta_service_endpoint, client);
        Queue<Long> lastConnTimeMs = Deencapsulation.getField(proxy, "lastConnTimeMs");
        lastConnTimeMs.clear();
        lastConnTimeMs.add(0L);
        lastConnTimeMs.add(0L);
        lastConnTimeMs.add(0L);

        MetaServiceProxy.MetaServiceClientWrapper wrapper = Deencapsulation.getField(proxy, "w");
        Cloud.MetaServiceResponseStatus status = Cloud.MetaServiceResponseStatus.newBuilder()
                .setCode(Cloud.MetaServiceCode.MS_TOO_BUSY)
                .setMsg("server is overloaded")
                .build();
        Cloud.GetVersionResponse response = Cloud.GetVersionResponse.newBuilder()
                .setStatus(status)
                .build();

        try {
            wrapper.executeRequest("ignored", (ignored) -> response, Cloud.GetVersionResponse::getStatus);
            Assert.fail("should throw RpcException");
        } catch (RpcException e) {
            Assert.assertEquals("server is overloaded", e.getMessage());
        }
        Mockito.verify(client, Mockito.never()).shutdown(Mockito.anyBoolean());
    }

    @Test
    public void testExecuteRequestRetryOnTooBusy() throws RpcException {
        Config.meta_service_rpc_retry_cnt = 2;
        MetaServiceProxy proxy = new MetaServiceProxy();
        MetaServiceClient client = mockNormalClient();

        Map<String, MetaServiceClient> serviceMap = Deencapsulation.getField(proxy, "serviceMap");
        serviceMap.put(Config.meta_service_endpoint, client);

        MetaServiceProxy.MetaServiceClientWrapper wrapper = Deencapsulation.getField(proxy, "w");
        Cloud.GetVersionResponse tooBusyResponse = Cloud.GetVersionResponse.newBuilder()
                .setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                        .setCode(Cloud.MetaServiceCode.MS_TOO_BUSY)
                        .setMsg("server is overloaded"))
                .build();
        Cloud.GetVersionResponse okResponse = Cloud.GetVersionResponse.newBuilder()
                .setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                        .setCode(Cloud.MetaServiceCode.OK))
                .build();
        AtomicInteger callCount = new AtomicInteger();

        Cloud.GetVersionResponse result = wrapper.executeRequest("ignored", (ignored) ->
                callCount.incrementAndGet() == 1 ? tooBusyResponse : okResponse, Cloud.GetVersionResponse::getStatus);

        Assert.assertEquals(Cloud.MetaServiceCode.OK, result.getStatus().getCode());
        Assert.assertEquals(2, callCount.get());
        Mockito.verify(client, Mockito.never()).shutdown(Mockito.anyBoolean());
    }

    @Test
    public void testExecuteRequestFailureAfterTooBusyRetries() throws RpcException {
        Config.meta_service_rpc_retry_cnt = 2;
        MetaServiceProxy proxy = new MetaServiceProxy();
        MetaServiceClient client = mockNormalClient();

        Map<String, MetaServiceClient> serviceMap = Deencapsulation.getField(proxy, "serviceMap");
        serviceMap.put(Config.meta_service_endpoint, client);

        MetaServiceProxy.MetaServiceClientWrapper wrapper = Deencapsulation.getField(proxy, "w");
        Cloud.GetVersionResponse tooBusyResponse = Cloud.GetVersionResponse.newBuilder()
                .setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                        .setCode(Cloud.MetaServiceCode.MS_TOO_BUSY)
                        .setMsg("server is overloaded"))
                .build();
        AtomicInteger callCount = new AtomicInteger();

        try {
            wrapper.executeRequest("ignored", (ignored) -> {
                callCount.incrementAndGet();
                return tooBusyResponse;
            }, Cloud.GetVersionResponse::getStatus);
            Assert.fail("should throw RpcException");
        } catch (RpcException e) {
            Assert.assertEquals("server is overloaded", e.getMessage());
        }

        Assert.assertEquals(2, callCount.get());
        Mockito.verify(client, Mockito.never()).shutdown(Mockito.anyBoolean());
    }

    @Test
    public void testExecuteRequestRateLimitedWithoutRetryOrShutdown() throws RpcException {
        enableRateLimit(1, "", 1, 0);
        MetaServiceProxy proxy = new MetaServiceProxy();
        MetaServiceClient client = mockNormalClient();
        putClient(proxy, client);

        MetaServiceProxy.MetaServiceClientWrapper wrapper = Deencapsulation.getField(proxy, "w");
        AtomicInteger callCount = new AtomicInteger();
        Cloud.GetVersionResponse okResponse = okGetVersionResponse();
        for (int i = 0; i < CPU_CORES; i++) {
            wrapper.executeRequest("limited", (ignored) -> {
                callCount.incrementAndGet();
                return okResponse;
            }, Cloud.GetVersionResponse::getStatus);
        }

        try {
            wrapper.executeRequest("limited", (ignored) -> {
                callCount.incrementAndGet();
                return okResponse;
            }, Cloud.GetVersionResponse::getStatus);
            Assert.fail("should throw RpcException");
        } catch (RpcException e) {
            Assert.assertTrue(e.getMessage().contains("meta service rpc rate limited"));
        }

        Assert.assertEquals(CPU_CORES, callCount.get());
        Mockito.verify(client, Mockito.never()).shutdown(Mockito.anyBoolean());
    }

    @Test
    public void testRateLimitSharedBetweenProxies() throws RpcException {
        enableRateLimit(1, "", 1, 0);
        MetaServiceProxy firstProxy = new MetaServiceProxy();
        MetaServiceProxy secondProxy = new MetaServiceProxy();
        putClient(firstProxy, mockNormalClient());
        putClient(secondProxy, mockNormalClient());

        MetaServiceProxy.MetaServiceClientWrapper firstWrapper = Deencapsulation.getField(firstProxy, "w");
        MetaServiceProxy.MetaServiceClientWrapper secondWrapper = Deencapsulation.getField(secondProxy, "w");
        Cloud.GetVersionResponse okResponse = okGetVersionResponse();
        for (int i = 0; i < CPU_CORES; i++) {
            firstWrapper.executeRequest("shared", (ignored) -> okResponse, Cloud.GetVersionResponse::getStatus);
        }

        try {
            secondWrapper.executeRequest("shared", (ignored) -> okResponse, Cloud.GetVersionResponse::getStatus);
            Assert.fail("should throw RpcException");
        } catch (RpcException e) {
            Assert.assertTrue(e.getMessage().contains("meta service rpc rate limited"));
        }
    }

    @Test
    public void testGetInstanceRateLimitedBeforeRpc() throws RpcException {
        enableRateLimit(1, "", 1, 0);
        MetaServiceProxy proxy = new MetaServiceProxy();
        MetaServiceClient client = mockNormalClient();
        putClient(proxy, client);
        consumeRateLimitPermits(proxy, "getInstance");

        try {
            proxy.getInstance(Cloud.GetInstanceRequest.newBuilder().build());
            Assert.fail("should throw RpcException");
        } catch (RpcException e) {
            Assert.assertTrue(e.getMessage().contains("meta service rpc rate limited"));
        }

        Mockito.verify(client, Mockito.never()).getInstance(Mockito.any());
        Mockito.verify(client, Mockito.never()).shutdown(Mockito.anyBoolean());
    }

    @Test
    public void testGetVisibleVersionAsyncRateLimitedBeforeRpc() throws RpcException {
        enableRateLimit(1, "", 1, 0);
        MetaServiceProxy proxy = new MetaServiceProxy();
        MetaServiceClient client = mockNormalClient();
        putClient(proxy, client);
        consumeRateLimitPermits(proxy, "getPartitionVersion");

        try {
            proxy.getVisibleVersionAsync(Cloud.GetVersionRequest.newBuilder().build());
            Assert.fail("should throw RpcException");
        } catch (RpcException e) {
            Assert.assertTrue(e.getMessage().contains("meta service rpc rate limited"));
        }

        Mockito.verify(client, Mockito.never()).getVisibleVersionAsync(Mockito.any());
        Mockito.verify(client, Mockito.never()).shutdown(Mockito.anyBoolean());
    }

    @Test
    public void testBatchGetVisibleVersionAsyncConsumesMultipleRateLimitPermits() throws RpcException {
        enableRateLimit(1, "", 1, 0);
        MetaServiceProxy proxy = new MetaServiceProxy();
        MetaServiceClient client = mockNormalClient();
        putClient(proxy, client);
        SettableFuture<Cloud.GetVersionResponse> future = SettableFuture.create();
        Mockito.when(client.getVisibleVersionAsync(Mockito.any())).thenReturn(future);

        proxy.getVisibleVersionAsync(buildBatchTableVersionRequest(CPU_CORES));

        try {
            proxy.getVisibleVersionAsync(Cloud.GetVersionRequest.newBuilder()
                    .setIsTableVersion(true)
                    .build());
            Assert.fail("should throw RpcException");
        } catch (RpcException e) {
            Assert.assertTrue(e.getMessage().contains("meta service rpc rate limited"));
        }
        Mockito.verify(client, Mockito.times(1)).getVisibleVersionAsync(Mockito.any());
    }

    private void enableRateLimit(int defaultQpsPerCore, String qpsPerCoreConfig, int burstSeconds,
            long waitTimeoutMs) {
        Config.meta_service_rpc_rate_limit_enabled = true;
        Config.meta_service_rpc_rate_limit_default_qps_per_core = defaultQpsPerCore;
        Config.meta_service_rpc_rate_limit_qps_per_core_config = qpsPerCoreConfig;
        Config.meta_service_rpc_rate_limit_burst_seconds = burstSeconds;
        Config.meta_service_rpc_rate_limit_wait_timeout_ms = waitTimeoutMs;
        MetaServiceProxy.resetMetaServiceRpcRateLimitForTest();
    }

    private void putClient(MetaServiceProxy proxy, MetaServiceClient client) {
        Map<String, MetaServiceClient> serviceMap = Deencapsulation.getField(proxy, "serviceMap");
        serviceMap.put(Config.meta_service_endpoint, client);
    }

    private void consumeRateLimitPermits(MetaServiceProxy proxy, String methodName) throws RpcException {
        MetaServiceProxy.MetaServiceClientWrapper wrapper = Deencapsulation.getField(proxy, "w");
        Cloud.GetVersionResponse okResponse = okGetVersionResponse();
        for (int i = 0; i < CPU_CORES; i++) {
            wrapper.executeRequest(methodName, (ignored) -> okResponse, Cloud.GetVersionResponse::getStatus);
        }
    }

    private Cloud.GetVersionRequest buildBatchPartitionVersionRequest(int partitionNum) {
        Cloud.GetVersionRequest.Builder builder = Cloud.GetVersionRequest.newBuilder()
                .setBatchMode(true);
        for (int i = 0; i < partitionNum; i++) {
            builder.addDbIds(1L);
            builder.addTableIds(1L);
            builder.addPartitionIds(i);
        }
        return builder.build();
    }

    private Cloud.GetVersionRequest buildBatchTableVersionRequest(int tableNum) {
        Cloud.GetVersionRequest.Builder builder = Cloud.GetVersionRequest.newBuilder()
                .setBatchMode(true)
                .setIsTableVersion(true);
        for (int i = 0; i < tableNum; i++) {
            builder.addDbIds(1L);
            builder.addTableIds(i);
        }
        return builder.build();
    }

    private Cloud.GetVersionResponse okGetVersionResponse() {
        return Cloud.GetVersionResponse.newBuilder()
                .setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                        .setCode(Cloud.MetaServiceCode.OK))
                .build();
    }

    private MetaServiceClient mockNormalClient() {
        MetaServiceClient client = Mockito.mock(MetaServiceClient.class);
        Mockito.when(client.isNormalState()).thenReturn(true);
        Mockito.when(client.isConnectionAgeExpired()).thenReturn(false);
        Mockito.when(client.isUsingLatestChannelConfig()).thenReturn(true);
        return client;
    }
}
