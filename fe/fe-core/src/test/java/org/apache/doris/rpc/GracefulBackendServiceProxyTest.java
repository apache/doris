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

package org.apache.doris.rpc;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.DNSCache;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.proto.InternalService;
import org.apache.doris.thrift.TNetworkAddress;

import com.google.common.util.concurrent.SettableFuture;
import io.grpc.ManagedChannel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

public class GracefulBackendServiceProxyTest {
    private final TNetworkAddress address = new TNetworkAddress("be.test", 8060);
    private BackendServiceProxy proxy;
    private DNSCache dnsCache;
    private MockedStatic<Env> envStatic;
    private MockedConstruction<BackendServiceClient> clients;

    @BeforeEach
    public void setUp() {
        Env env = Mockito.mock(Env.class);
        dnsCache = Mockito.mock(DNSCache.class);
        Mockito.when(env.getDnsCache()).thenReturn(dnsCache);
        Mockito.when(dnsCache.get(address.hostname)).thenReturn("10.0.0.1");
        envStatic = Mockito.mockStatic(Env.class);
        envStatic.when(Env::getCurrentEnv).thenReturn(env);
        clients = Mockito.mockConstruction(BackendServiceClient.class, (client, context) -> {
            Mockito.when(client.isNormalState()).thenReturn(true);
            Mockito.when(client.isUsingLatestChannelConfig()).thenReturn(true);
        });
        proxy = new BackendServiceProxy();
    }

    @AfterEach
    public void tearDown() {
        clients.close();
        envStatic.close();
    }

    @Test
    public void testRemovalPreservesPendingRpc() throws Exception {
        BackendServiceClient client = getProxy();
        InternalService.PSyncTabletMetaRequest request = InternalService.PSyncTabletMetaRequest.newBuilder().build();
        SettableFuture<InternalService.PSyncTabletMetaResponse> pending = SettableFuture.create();
        Mockito.when(client.syncTabletMeta(request)).thenReturn(pending);
        Assertions.assertSame(pending, proxy.syncTabletMeta(address, request));
        proxy.removeProxy(address);
        assertGracefulClose(client);
        Assertions.assertFalse(pending.isDone());
        Assertions.assertNotSame(client, getProxy());
        InternalService.PSyncTabletMetaResponse response = InternalService.PSyncTabletMetaResponse.newBuilder().build();
        pending.set(response);
        Assertions.assertSame(response, pending.get());
    }

    @Test
    public void testDnsReplacementClosesGracefully() {
        BackendServiceClient client = getProxy();
        Mockito.when(dnsCache.get(address.hostname)).thenReturn("10.0.0.2");
        assertReplacement(client);
    }

    @Test
    public void testAbnormalChannelReplacementClosesGracefully() {
        BackendServiceClient client = getProxy();
        Mockito.when(client.isNormalState()).thenReturn(false);
        assertReplacement(client);
    }

    @Test
    public void testConfigReplacementClosesGracefully() {
        BackendServiceClient client = getProxy();
        Mockito.when(client.isUsingLatestChannelConfig()).thenReturn(false);
        assertReplacement(client);
    }

    @Test
    public void testGracefulCloseDoesNotWaitOrForceShutdown() {
        ManagedChannel channel = Mockito.mock(ManagedChannel.class);
        BackendServiceClient client = Mockito.mock(BackendServiceClient.class, Mockito.CALLS_REAL_METHODS);
        Deencapsulation.setField(client, "channel", channel);
        client.shutdownGracefully();
        Mockito.verify(channel).shutdown();
        Mockito.verifyNoMoreInteractions(channel);
    }

    private BackendServiceClient getProxy() {
        return Deencapsulation.invoke(proxy, "getProxy", address);
    }

    private void assertReplacement(BackendServiceClient client) {
        Assertions.assertNotSame(client, getProxy());
        assertGracefulClose(client);
    }

    private static void assertGracefulClose(BackendServiceClient client) {
        Mockito.verify(client).shutdownGracefully();
        Mockito.verify(client, Mockito.never()).shutdown();
    }
}
