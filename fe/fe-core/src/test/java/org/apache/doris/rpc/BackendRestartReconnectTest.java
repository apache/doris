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
import org.apache.doris.common.Config;
import org.apache.doris.common.DNSCache;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.common.util.NetUtils;
import org.apache.doris.proto.InternalService;
import org.apache.doris.proto.PBackendServiceGrpc;
import org.apache.doris.system.Backend;
import org.apache.doris.system.BackendHbResponse;
import org.apache.doris.thrift.TNetworkAddress;

import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A backend that restarts at a new IP must be dialed at that IP on the next RPC, not after the
 * periodic DNS refresh.
 */
public class BackendRestartReconnectTest {
    private static final String HOST = "be-0.be.example.svc";

    private DNSCache dnsCache;
    private Env env;
    private MockedStatic<Env> envStatic;
    private MockedStatic<NetUtils> resolver;
    private boolean originalFqdnMode;

    @BeforeEach
    public void setUp() {
        originalFqdnMode = Config.enable_fqdn_mode;
        Config.enable_fqdn_mode = true;
        dnsCache = new DNSCache();
        env = Mockito.mock(Env.class);
        Mockito.when(env.getDnsCache()).thenReturn(dnsCache);
        envStatic = Mockito.mockStatic(Env.class);
        envStatic.when(Env::getCurrentEnv).thenReturn(env);
        resolver = Mockito.mockStatic(NetUtils.class);
    }

    @AfterEach
    public void tearDown() {
        Config.enable_fqdn_mode = originalFqdnMode;
        resolver.close();
        envStatic.close();
    }

    @Test
    public void testGetProxyAfterInvalidateDialsNewIp() throws Exception {
        BackendServiceProxy proxy = new BackendServiceProxy();
        TNetworkAddress address = new TNetworkAddress(HOST, 8060);
        resolver.when(() -> NetUtils.getIpByHost(HOST, 0)).thenReturn("10.0.0.1", "10.0.0.2");

        BackendServiceClient oldClient = getProxy(proxy, address);
        Assertions.assertEquals("10.0.0.1", cachedIp(proxy, address));
        // Without an invalidation the cached address and channel are kept.
        Assertions.assertSame(oldClient, getProxy(proxy, address));

        dnsCache.invalidate(HOST);
        BackendServiceClient newClient = getProxy(proxy, address);
        Assertions.assertNotSame(oldClient, newClient);
        Assertions.assertEquals("10.0.0.2", cachedIp(proxy, address));
        Assertions.assertTrue(channel(oldClient).isShutdown());
        newClient.shutdown();
    }

    @Test
    public void testInvalidateForAllDropsCachedChannel() throws Exception {
        BackendServiceProxy proxy = BackendServiceProxy.getInstance();
        TNetworkAddress address = new TNetworkAddress(HOST, 8061);
        resolver.when(() -> NetUtils.getIpByHost(HOST, 0)).thenReturn("10.0.0.1");

        BackendServiceClient client = getProxy(proxy, address);
        BackendServiceProxy.invalidateForAll(address);
        Map<TNetworkAddress, Object> serviceMap = Deencapsulation.getField(proxy, "serviceMap");
        Assertions.assertFalse(serviceMap.containsKey(address));
        Assertions.assertTrue(channel(client).isShutdown());
    }

    @Test
    public void testFailingChannelIsRebuiltToNewIp() throws Exception {
        BackendServiceProxy proxy = new BackendServiceProxy();
        TNetworkAddress address = new TNetworkAddress(HOST, closedPort());
        resolver.when(() -> NetUtils.getIpByHost(HOST, 0)).thenReturn("127.0.0.1", "127.0.0.2");

        BackendServiceClient oldClient = getProxy(proxy, address);
        awaitTransientFailure(channel(oldClient));

        BackendServiceClient newClient = getProxy(proxy, address);
        Assertions.assertNotSame(oldClient, newClient);
        Assertions.assertEquals("127.0.0.2", cachedIp(proxy, address));
        newClient.shutdown();
    }

    @Test
    public void testUnavailableRpcForcesNewLookup() throws Exception {
        BackendServiceProxy proxy = new BackendServiceProxy();
        TNetworkAddress address = new TNetworkAddress(HOST, closedPort());
        resolver.when(() -> NetUtils.getIpByHost(HOST, 0)).thenReturn("127.0.0.1", "127.0.0.2");

        BackendServiceClient client = getProxy(proxy, address);
        ExecutionException e = Assertions.assertThrows(ExecutionException.class,
                () -> client.getInfo(InternalService.PProxyRequest.newBuilder().build())
                        .get(30, TimeUnit.SECONDS));
        Assertions.assertEquals(Status.Code.UNAVAILABLE,
                ((StatusRuntimeException) e.getCause()).getStatus().getCode());

        Assertions.assertEquals("127.0.0.2", dnsCache.get(HOST));
        client.shutdown();
    }

    @Test
    public void testHeartbeatWithNewStartTimeDropsConnections() throws Exception {
        TNetworkAddress address = new TNetworkAddress(HOST, 8062);
        Backend backend = new Backend(10001L, HOST, 9050);
        backend.setBrpcPort(address.getPort());
        resolver.when(() -> NetUtils.getIpByHost(HOST, 0)).thenReturn("10.0.0.1", "10.0.0.2");

        Assertions.assertTrue(backend.handleHbResponse(heartbeat(1000L, address.getPort()), false));
        BackendServiceProxy proxy = BackendServiceProxy.getInstance();
        BackendServiceClient client = getProxy(proxy, address);

        // A heartbeat from the same process keeps the channel and the cached address.
        backend.handleHbResponse(heartbeat(1000L, address.getPort()), false);
        Assertions.assertSame(client, getProxy(proxy, address));
        Assertions.assertEquals("10.0.0.1", cachedIp(proxy, address));

        // A new process start time means the backend restarted, possibly at another address.
        backend.handleHbResponse(heartbeat(2000L, address.getPort()), false);
        Assertions.assertTrue(channel(client).isShutdown());
        BackendServiceClient newClient = getProxy(proxy, address);
        Assertions.assertNotSame(client, newClient);
        Assertions.assertEquals("10.0.0.2", cachedIp(proxy, address));
        BackendServiceProxy.invalidateForAll(address);
    }

    @Test
    public void testFailedHeartbeatThenRecoveryReResolves() throws Exception {
        TNetworkAddress address = new TNetworkAddress(HOST, 8063);
        Backend backend = new Backend(10002L, HOST, 9050);
        backend.setBrpcPort(address.getPort());
        resolver.when(() -> NetUtils.getIpByHost(HOST, 0)).thenReturn("10.0.0.1", "10.0.0.2");
        backend.handleHbResponse(heartbeat(1000L, address.getPort()), false);
        BackendServiceProxy proxy = BackendServiceProxy.getInstance();
        BackendServiceClient client = getProxy(proxy, address);

        // A failed heartbeat lets the next heartbeat resolve the hostname past the JVM cache.
        resolver.clearInvocations();
        int tolerance = (int) Config.max_backend_heartbeat_failure_tolerance_count;
        for (int i = 0; i < tolerance; i++) {
            backend.handleHbResponse(new BackendHbResponse(10002L, HOST, 0, "connection refused"), false);
        }
        resolver.verify(() -> NetUtils.evictJvmDnsCache(HOST), Mockito.times(tolerance));
        Assertions.assertFalse(backend.isAlive());

        // The backend is back with the same start time (e.g. its network moved): still re-resolve.
        backend.handleHbResponse(heartbeat(1000L, address.getPort()), false);
        Assertions.assertTrue(backend.isAlive());
        Assertions.assertTrue(channel(client).isShutdown());
        getProxy(proxy, address);
        Assertions.assertEquals("10.0.0.2", cachedIp(proxy, address));
        BackendServiceProxy.invalidateForAll(address);
    }

    @Test
    public void testFailingChannelUsesLookupMarkedByRacingRpcFailure() throws Exception {
        BackendServiceProxy proxy = new BackendServiceProxy();
        TNetworkAddress address = new TNetworkAddress(HOST, closedPort());
        resolver.when(() -> NetUtils.getIpByHost(HOST, 0)).thenReturn("127.0.0.1", "127.0.0.2");
        BackendServiceClient oldClient = getProxy(proxy, address);
        awaitTransientFailure(channel(oldClient));

        // A failed RPC marks the host (and takes the throttle) right after getProxy read the old IP.
        DNSCache racing = Mockito.spy(dnsCache);
        AtomicBoolean raced = new AtomicBoolean();
        Mockito.doAnswer(invocation -> {
            Object ip = invocation.callRealMethod();
            if (raced.compareAndSet(false, true)) {
                Assertions.assertTrue(racing.invalidateAfterRpcFailure(HOST, "127.0.0.1"));
            }
            return ip;
        }).when(racing).get(HOST);
        Mockito.when(env.getDnsCache()).thenReturn(racing);

        BackendServiceClient newClient = getProxy(proxy, address);
        Assertions.assertTrue(raced.get());
        Assertions.assertNotSame(oldClient, newClient);
        Assertions.assertEquals("127.0.0.2", cachedIp(proxy, address));
        newClient.shutdown();
    }

    @Test
    public void testDeadlineWhileConnectingForcesNewLookup() throws Exception {
        // A peer that accepts TCP connections but never answers the HTTP/2 handshake: the channel
        // never becomes READY, like one to a released pod IP whose traffic goes nowhere.
        List<Socket> accepted = new CopyOnWriteArrayList<>();
        try (ServerSocket silent = new ServerSocket(0, 50, InetAddress.getLoopbackAddress())) {
            Thread acceptor = new Thread(() -> {
                try {
                    while (true) {
                        accepted.add(silent.accept());
                    }
                } catch (IOException closed) {
                    // The test closed the socket.
                }
            }, "silent-peer");
            acceptor.setDaemon(true);
            acceptor.start();

            BackendServiceProxy proxy = new BackendServiceProxy();
            TNetworkAddress address = new TNetworkAddress(HOST, silent.getLocalPort());
            resolver.when(() -> NetUtils.getIpByHost(HOST, 0)).thenReturn("127.0.0.1", "127.0.0.2");
            BackendServiceClient client = getProxy(proxy, address);
            ExecutionException e = Assertions.assertThrows(ExecutionException.class,
                    () -> client.requestCdcClient(InternalService.PRequestCdcClientRequest.newBuilder().build(), 1)
                            .get(30, TimeUnit.SECONDS));
            Assertions.assertEquals(Status.Code.DEADLINE_EXCEEDED,
                    ((StatusRuntimeException) e.getCause()).getStatus().getCode());
            Assertions.assertFalse(accepted.isEmpty(), "the channel never opened a TCP connection");
            Assertions.assertNotEquals(ConnectivityState.READY, channel(client).getState(false));

            Assertions.assertEquals("127.0.0.2", dnsCache.get(HOST));
            client.shutdownGracefully();
        } finally {
            for (Socket socket : accepted) {
                socket.close();
            }
        }
    }

    @Test
    public void testDeadlineOnReadyChannelKeepsAddress() throws Exception {
        // A reachable backend that never answers: a slow call, not a moved backend.
        Server server = NettyServerBuilder.forAddress(new InetSocketAddress("127.0.0.1", 0))
                .addService(new PBackendServiceGrpc.PBackendServiceImplBase() {
                    @Override
                    public void requestCdcClient(InternalService.PRequestCdcClientRequest request,
                            StreamObserver<InternalService.PRequestCdcClientResult> responseObserver) {
                    }
                }).build().start();
        try {
            BackendServiceProxy proxy = new BackendServiceProxy();
            TNetworkAddress address = new TNetworkAddress(HOST, server.getPort());
            resolver.when(() -> NetUtils.getIpByHost(HOST, 0)).thenReturn("127.0.0.1", "127.0.0.2");
            BackendServiceClient client = getProxy(proxy, address);
            ExecutionException e = Assertions.assertThrows(ExecutionException.class,
                    () -> client.requestCdcClient(InternalService.PRequestCdcClientRequest.newBuilder().build(), 1)
                            .get(30, TimeUnit.SECONDS));
            Assertions.assertEquals(Status.Code.DEADLINE_EXCEEDED,
                    ((StatusRuntimeException) e.getCause()).getStatus().getCode());
            Assertions.assertEquals(ConnectivityState.READY, channel(client).getState(false));

            Assertions.assertEquals("127.0.0.1", dnsCache.get(HOST));
            resolver.verify(() -> NetUtils.getIpByHost(HOST, 0), Mockito.times(1));
            client.shutdown();
        } finally {
            server.shutdownNow();
        }
    }

    @Test
    public void testFollowerReplayOfRestartDropsConnections() throws Exception {
        TNetworkAddress address = new TNetworkAddress(HOST, 8064);
        Backend backend = new Backend(10003L, HOST, 9050);
        backend.setBrpcPort(address.getPort());
        resolver.when(() -> NetUtils.getIpByHost(HOST, 0)).thenReturn("10.0.0.1", "10.0.0.2");
        backend.handleHbResponse(heartbeat(1000L, address.getPort()), true);
        BackendServiceProxy proxy = BackendServiceProxy.getInstance();
        BackendServiceClient client = getProxy(proxy, address);

        // A follower learns of the restart from the master's edit log, and holds its own channels.
        backend.handleHbResponse(heartbeat(2000L, address.getPort()), true);
        Assertions.assertTrue(channel(client).isShutdown());
        getProxy(proxy, address);
        Assertions.assertEquals("10.0.0.2", cachedIp(proxy, address));
        BackendServiceProxy.invalidateForAll(address);
    }

    @Test
    public void testCheckpointReplayKeepsConnections() throws Exception {
        TNetworkAddress address = new TNetworkAddress(HOST, 8065);
        Backend backend = new Backend(10004L, HOST, 9050);
        backend.setBrpcPort(address.getPort());
        resolver.when(() -> NetUtils.getIpByHost(HOST, 0)).thenReturn("10.0.0.1", "10.0.0.2");
        backend.handleHbResponse(heartbeat(1000L, address.getPort()), false);
        BackendServiceProxy proxy = BackendServiceProxy.getInstance();
        BackendServiceClient client = getProxy(proxy, address);

        // A checkpoint replays an old restart into its own catalog; the serving FE's channels stay.
        envStatic.when(Env::isCheckpointThread).thenReturn(true);
        backend.handleHbResponse(heartbeat(2000L, address.getPort()), true);
        Assertions.assertFalse(channel(client).isShutdown());
        Assertions.assertSame(client, getProxy(proxy, address));
        Assertions.assertEquals("10.0.0.1", cachedIp(proxy, address));
        resolver.verify(() -> NetUtils.getIpByHost(HOST, 0), Mockito.times(1));
        BackendServiceProxy.invalidateForAll(address);
    }

    private static BackendHbResponse heartbeat(long beStartTime, int brpcPort) {
        return new BackendHbResponse(10001L, 9060, 8040, brpcPort, System.currentTimeMillis(), beStartTime,
                "test", "mix", 0, 0, false, 0);
    }

    private static BackendServiceClient getProxy(BackendServiceProxy proxy, TNetworkAddress address) {
        return Deencapsulation.invoke(proxy, "getProxy", address);
    }

    private static String cachedIp(BackendServiceProxy proxy, TNetworkAddress address) {
        Map<TNetworkAddress, Object> serviceMap = Deencapsulation.getField(proxy, "serviceMap");
        return Deencapsulation.getField(serviceMap.get(address), "realIp");
    }

    private static ManagedChannel channel(BackendServiceClient client) {
        return Deencapsulation.getField(client, "channel");
    }

    private static int closedPort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private static void awaitTransientFailure(ManagedChannel channel) throws Exception {
        long deadline = System.currentTimeMillis() + 30_000;
        while (channel.getState(true) != ConnectivityState.TRANSIENT_FAILURE) {
            Assertions.assertTrue(System.currentTimeMillis() < deadline, "channel never failed to connect");
            Thread.sleep(50);
        }
    }
}
