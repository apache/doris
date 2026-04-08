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

public class MetaServiceProxyTest {
    private String originEndpoint;
    private long originReconnectIntervalMs;
    private long originRetryCnt;

    @Before
    public void setUp() {
        originEndpoint = Config.meta_service_endpoint;
        originReconnectIntervalMs = Config.meta_service_rpc_reconnect_interval_ms;
        originRetryCnt = Config.meta_service_rpc_retry_cnt;

        Config.meta_service_endpoint = "127.0.0.1:12345";
        Config.meta_service_rpc_reconnect_interval_ms = 0;
        Config.meta_service_rpc_retry_cnt = 1;
    }

    @After
    public void tearDown() {
        Config.meta_service_endpoint = originEndpoint;
        Config.meta_service_rpc_reconnect_interval_ms = originReconnectIntervalMs;
        Config.meta_service_rpc_retry_cnt = originRetryCnt;
    }

    @Test
    public void testExecuteRequestNoShutdownOnSuccess() throws RpcException {
        MetaServiceProxy proxy = new MetaServiceProxy();
        MetaServiceClient client = Mockito.mock(MetaServiceClient.class);
        Mockito.when(client.isNormalState()).thenReturn(true);
        Mockito.when(client.isConnectionAgeExpired()).thenReturn(false);

        Map<String, MetaServiceClient> serviceMap = Deencapsulation.getField(proxy, "serviceMap");
        serviceMap.put(Config.meta_service_endpoint, client);
        Queue<Long> lastConnTimeMs = Deencapsulation.getField(proxy, "lastConnTimeMs");
        lastConnTimeMs.clear();
        lastConnTimeMs.add(0L);
        lastConnTimeMs.add(0L);
        lastConnTimeMs.add(0L);

        MetaServiceProxy.MetaServiceClientWrapper wrapper = Deencapsulation.getField(proxy, "w");
        String response = wrapper.executeRequest("ignored", (ignored) -> "ok");
        Assert.assertEquals("ok", response);
        Mockito.verify(client, Mockito.never()).shutdown(Mockito.anyBoolean());
    }

    @Test
    public void testExecuteRequestShutdownOnFailure() {
        MetaServiceProxy proxy = new MetaServiceProxy();
        MetaServiceClient client = Mockito.mock(MetaServiceClient.class);
        Mockito.when(client.isNormalState()).thenReturn(true);
        Mockito.when(client.isConnectionAgeExpired()).thenReturn(false);

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
            });
            Assert.fail("should throw RpcException");
        } catch (RpcException ignored) {
            // expected
        }

        Mockito.verify(client).shutdown(true);
    }

    @Test
    public void testGetVisibleVersionAsyncShutdownOnFailure() throws RpcException {
        MetaServiceProxy proxy = new MetaServiceProxy();
        MetaServiceClient client = Mockito.mock(MetaServiceClient.class);
        Mockito.when(client.isNormalState()).thenReturn(true);
        Mockito.when(client.isConnectionAgeExpired()).thenReturn(false);

        SettableFuture<Cloud.GetVersionResponse> future = SettableFuture.create();
        Mockito.when(client.getVisibleVersionAsync(Mockito.any())).thenReturn(future);

        Map<String, MetaServiceClient> serviceMap = Deencapsulation.getField(proxy, "serviceMap");
        serviceMap.put(Config.meta_service_endpoint, client);

        Cloud.GetVersionRequest request = Cloud.GetVersionRequest.newBuilder().build();
        proxy.getVisibleVersionAsync(request);

        future.setException(new RuntimeException("async failed"));

        Mockito.verify(client).shutdown(true);
    }
}
