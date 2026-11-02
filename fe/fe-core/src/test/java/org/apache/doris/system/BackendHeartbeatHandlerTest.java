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

package org.apache.doris.system;

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.TokenManager;
import org.apache.doris.common.ClientPool;
import org.apache.doris.common.Config;
import org.apache.doris.common.DNSCache;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.GenericPool;
import org.apache.doris.service.FrontendOptions;
import org.apache.doris.system.HeartbeatResponse.HbStatus;
import org.apache.doris.thrift.HeartbeatService;
import org.apache.doris.thrift.TBackendInfo;
import org.apache.doris.thrift.THeartbeatResult;
import org.apache.doris.thrift.TMasterInfo;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TStatus;
import org.apache.doris.thrift.TStatusCode;

import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;
import org.apache.thrift.TException;
import org.apache.thrift.server.ServerContext;
import org.apache.thrift.server.TServerEventHandler;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

class BackendHeartbeatHandlerTest {
    private static final String HOST = "be-0.be-service.test";
    private static final String OLD_IP = "127.0.0.1";
    private static final String NEW_IP = "127.0.0.2";

    private final DNSCache dnsCache = Mockito.mock(DNSCache.class);
    private final SystemInfoService nodeMgr = Mockito.mock(SystemInfoService.class);
    @SuppressWarnings("unchecked")
    private final GenericPool<HeartbeatService.Client> pool = Mockito.mock(GenericPool.class);
    private final HeartbeatService.Client client = Mockito.mock(HeartbeatService.Client.class);
    private GenericPool<HeartbeatService.Client> originalPool;
    private boolean originalFqdnMode;
    private boolean originalRunningUnitTest;
    private AtomicReference<TMasterInfo> masterInfo;
    private TMasterInfo originalMasterInfo;
    private MockedStatic<Env> mockedEnv;
    private MockedStatic<FrontendOptions> mockedFrontendOptions;
    private HeartbeatMgr mgr;
    private Backend backend;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() throws Exception {
        originalPool = ClientPool.backendHeartbeatPool;
        originalFqdnMode = Config.enable_fqdn_mode;
        originalRunningUnitTest = FeConstants.runningUnitTest;
        ClientPool.backendHeartbeatPool = pool;
        Config.enable_fqdn_mode = true;
        FeConstants.runningUnitTest = false;

        Field field = HeartbeatMgr.class.getDeclaredField("masterInfo");
        field.setAccessible(true);
        masterInfo = (AtomicReference<TMasterInfo>) field.get(null);
        originalMasterInfo = masterInfo.get();

        Env env = Mockito.mock(Env.class);
        TokenManager tokenManager = Mockito.mock(TokenManager.class);
        Mockito.when(tokenManager.acquireToken()).thenReturn("auth-token");
        Mockito.when(env.getTokenManager()).thenReturn(tokenManager);
        Mockito.when(env.getDnsCache()).thenReturn(dnsCache);
        mockedEnv = Mockito.mockStatic(Env.class);
        mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
        mockedFrontendOptions = Mockito.mockStatic(FrontendOptions.class);
        mockedFrontendOptions.when(FrontendOptions::getLocalHostAddress).thenReturn(OLD_IP);

        Mockito.when(dnsCache.get(HOST)).thenReturn(OLD_IP);
        Mockito.when(pool.borrowObject(Mockito.any(TNetworkAddress.class))).thenReturn(client);
        Mockito.when(client.heartbeat(Mockito.any(TMasterInfo.class))).thenReturn(success(123L));
        backend = new Backend(1L, HOST, 9050);
        Mockito.when(nodeMgr.getBackend(backend.getId())).thenReturn(backend);
        mgr = new HeartbeatMgr(nodeMgr, false);
        mgr.setMaster(1, "token", 1L);
    }

    @AfterEach
    void tearDown() {
        ClientPool.backendHeartbeatPool = originalPool;
        Config.enable_fqdn_mode = originalFqdnMode;
        FeConstants.runningUnitTest = originalRunningUnitTest;
        masterInfo.set(originalMasterInfo);
        mockedFrontendOptions.close();
        mockedEnv.close();
    }

    private BackendHbResponse heartbeat() {
        return (BackendHbResponse) mgr.new BackendHeartbeatHandler(backend, Collections.emptyList()).call();
    }

    private static THeartbeatResult success(long startTime) {
        TBackendInfo info = new TBackendInfo();
        info.setBePort(9060);
        info.setHttpPort(8040);
        info.setBeStartTime(startTime);
        return new THeartbeatResult(new TStatus(TStatusCode.OK), info);
    }

    @Test
    void testStableAddressReusesPoolAndPreservesFqdnIdentity() throws Exception {
        Assertions.assertEquals(HbStatus.OK, heartbeat().getStatus());
        Assertions.assertEquals(HbStatus.OK, heartbeat().getStatus());
        TNetworkAddress address = new TNetworkAddress(OLD_IP, 9050);
        Mockito.verify(pool, Mockito.times(2)).borrowObject(address);
        Mockito.verify(pool, Mockito.times(2)).returnObject(address, client);
        Mockito.verify(pool, Mockito.never()).clearPool(Mockito.any());
        ArgumentCaptor<TMasterInfo> requests = ArgumentCaptor.forClass(TMasterInfo.class);
        Mockito.verify(client, Mockito.times(2)).heartbeat(requests.capture());
        for (TMasterInfo request : requests.getAllValues()) {
            Assertions.assertEquals(HOST, request.getBackendIp());
            Assertions.assertEquals(backend.getId(), request.getBackendId());
        }
    }

    @Test
    void testRejectedHeartbeatInvalidatesConnectionOnBothAttempts() throws Exception {
        TStatus status = new TStatus(TStatusCode.INTERNAL_ERROR);
        status.addToErrorMsgs("the host recorded in master does not map to this BE");
        Mockito.when(client.heartbeat(Mockito.any())).thenReturn(new THeartbeatResult(status, new TBackendInfo()));
        Assertions.assertEquals(HbStatus.BAD, heartbeat().getStatus());
        Mockito.verify(pool, Mockito.times(2)).invalidateObject(new TNetworkAddress(OLD_IP, 9050), client);
        Mockito.verify(pool, Mockito.never()).returnObject(Mockito.any(), Mockito.any());
    }

    @Test
    void testRpcExceptionInvalidatesConnectionBeforeRetry() throws Exception {
        Mockito.when(client.heartbeat(Mockito.any())).thenThrow(new TException("connection reset"))
                .thenReturn(success(123L));
        Assertions.assertEquals(HbStatus.OK, heartbeat().getStatus());
        TNetworkAddress address = new TNetworkAddress(OLD_IP, 9050);
        Mockito.verify(pool).invalidateObject(address, client);
        Mockito.verify(pool).returnObject(address, client);
    }

    @Test
    void testFailedDnsLookupDoesNotBorrowConnection() {
        Mockito.when(dnsCache.get(HOST)).thenReturn("");
        BackendHbResponse response = heartbeat();
        Assertions.assertEquals(HbStatus.BAD, response.getStatus());
        Assertions.assertTrue(response.getMsg().contains("Failed to resolve hostname"));
        Mockito.verifyNoInteractions(pool);
    }

    @Test
    void testIpModeDoesNotUseDnsCache() throws Exception {
        Config.enable_fqdn_mode = false;
        backend = new Backend(1L, OLD_IP, 9050);
        Assertions.assertEquals(HbStatus.OK, heartbeat().getStatus());
        Mockito.verify(pool).borrowObject(new TNetworkAddress(OLD_IP, 9050));
        Mockito.verifyNoInteractions(dnsCache);
    }

    @Test
    void testRemovedBackendClosesItsIdlePool() {
        Assertions.assertEquals(HbStatus.OK, heartbeat().getStatus());
        Mockito.when(nodeMgr.getBackend(backend.getId())).thenReturn(null);
        mgr.clearRemovedBackendHeartbeatPools();
        mgr.clearRemovedBackendHeartbeatPools();
        Mockito.verify(pool).clearPool(new TNetworkAddress(OLD_IP, 9050));
    }

    @Test
    void testDnsChangeClosesHealthyConnectionToOldProcess() throws Exception {
        // Both processes keep their sockets healthy and accept the heartbeat. Only the DNS
        // change can move the next heartbeat away from the old process.
        try (HeartbeatEndpoint oldEndpoint = new HeartbeatEndpoint(OLD_IP, 0, request -> success(111L));
                HeartbeatEndpoint newEndpoint = new HeartbeatEndpoint(
                        NEW_IP, oldEndpoint.port(), request -> success(222L))) {
            useRealPool(oldEndpoint.port());
            try {
                Assertions.assertEquals(111L, heartbeat().getBeStartTime());
                Assertions.assertEquals(111L, heartbeat().getBeStartTime());
                Mockito.when(dnsCache.get(HOST)).thenReturn(NEW_IP);
                Assertions.assertEquals(222L, heartbeat().getBeStartTime());
                Assertions.assertTrue(oldEndpoint.disconnected.await(5, TimeUnit.SECONDS));
                Assertions.assertEquals(2, oldEndpoint.calls.get());
                Assertions.assertEquals(1, newEndpoint.calls.get());
            } finally {
                clearRealPool(oldEndpoint.port());
            }
        }
    }

    @Test
    void testIdentityMismatchReconnectsToNewProcessWithinRetry() throws Exception {
        AtomicReference<String> resolvedIp = new AtomicReference<>(OLD_IP);
        Mockito.when(dnsCache.get(HOST)).thenAnswer(invocation -> resolvedIp.get());
        try (HeartbeatEndpoint wrongEndpoint = new HeartbeatEndpoint(OLD_IP, 0, request -> {
            // Simulate DNS converging after a stale lookup reached the wrong BE. The wrong
            // process stays alive, so the client must close the rejected socket itself.
            resolvedIp.set(NEW_IP);
            TStatus status = new TStatus(TStatusCode.INTERNAL_ERROR);
            status.addToErrorMsgs("the host recorded in master does not map to this BE");
            return new THeartbeatResult(status, new TBackendInfo());
        }); HeartbeatEndpoint rightEndpoint = new HeartbeatEndpoint(
                NEW_IP, wrongEndpoint.port(), request -> success(222L))) {
            useRealPool(wrongEndpoint.port());
            try {
                BackendHbResponse response = heartbeat();
                Assertions.assertEquals(HbStatus.OK, response.getStatus());
                Assertions.assertEquals(222L, response.getBeStartTime());
                Assertions.assertTrue(wrongEndpoint.disconnected.await(5, TimeUnit.SECONDS));
                Assertions.assertEquals(1, wrongEndpoint.calls.get());
                Assertions.assertEquals(1, rightEndpoint.calls.get());
            } finally {
                clearRealPool(wrongEndpoint.port());
            }
        }
    }

    private void useRealPool(int port) {
        ClientPool.backendHeartbeatPool = new GenericPool<>(
                "HeartbeatService", new GenericKeyedObjectPoolConfig(), 2000);
        backend = new Backend(1L, HOST, port);
    }

    private void clearRealPool(int port) {
        ClientPool.backendHeartbeatPool.clearPool(new TNetworkAddress(OLD_IP, port));
        ClientPool.backendHeartbeatPool.clearPool(new TNetworkAddress(NEW_IP, port));
    }

    private static class HeartbeatEndpoint implements AutoCloseable {
        private final TServerSocket socket;
        private final TThreadPoolServer server;
        private final ExecutorService serving = Executors.newSingleThreadExecutor();
        private final ExecutorService workers = Executors.newSingleThreadExecutor();
        private final AtomicInteger calls = new AtomicInteger();
        private final CountDownLatch disconnected = new CountDownLatch(1);

        HeartbeatEndpoint(String ip, int port, HeartbeatService.Iface handler) throws Exception {
            socket = new TServerSocket(new ServerSocket(port, 50, InetAddress.getByName(ip)));
            HeartbeatService.Iface checkedHandler = request -> {
                Assertions.assertEquals(HOST, request.getBackendIp());
                calls.incrementAndGet();
                return handler.heartbeat(request);
            };
            server = new TThreadPoolServer(new TThreadPoolServer.Args(socket)
                    .processor(new HeartbeatService.Processor<>(checkedHandler)).executorService(workers));
            CountDownLatch ready = new CountDownLatch(1);
            TServerEventHandler events = Mockito.mock(TServerEventHandler.class);
            Mockito.when(events.createContext(Mockito.any(), Mockito.any()))
                    .thenAnswer(invocation -> Mockito.mock(ServerContext.class));
            Mockito.doAnswer(invocation -> {
                ready.countDown();
                return null;
            }).when(events).preServe();
            Mockito.doAnswer(invocation -> {
                disconnected.countDown();
                return null;
            }).when(events).deleteContext(Mockito.any(), Mockito.any(), Mockito.any());
            server.setServerEventHandler(events);
            serving.submit(server::serve);
            Assertions.assertTrue(ready.await(5, TimeUnit.SECONDS));
        }

        int port() {
            return socket.getServerSocket().getLocalPort();
        }

        @Override
        public void close() throws InterruptedException {
            server.stop();
            socket.close();
            workers.shutdownNow();
            serving.shutdownNow();
            Assertions.assertTrue(workers.awaitTermination(5, TimeUnit.SECONDS));
            Assertions.assertTrue(serving.awaitTermination(5, TimeUnit.SECONDS));
        }
    }
}
