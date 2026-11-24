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
import org.apache.doris.common.ClientPool;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.GenericPool;
import org.apache.doris.system.HeartbeatResponse.HbStatus;
import org.apache.doris.thrift.HeartbeatService;
import org.apache.doris.thrift.TBackendInfo;
import org.apache.doris.thrift.THeartbeatResult;
import org.apache.doris.thrift.TMasterInfo;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TStatus;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.tls.PrivateRpcTls;

import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.net.ssl.SSLServerSocket;

class PrivateRpcHeartbeatCompatibilityTest {
    @TempDir
    Path certificates;
    private final Map<Field, Object> previous = new LinkedHashMap<>();
    private final List<Peer> peers = new ArrayList<>();
    private final AtomicReference<String> resolved = new AtomicReference<>("127.0.0.1");
    private GenericPool<HeartbeatService.Client> originalPool;
    private boolean runningUnitTest;
    private MockedStatic<Env> mockedEnv;
    private AtomicReference<TMasterInfo> master;
    private TMasterInfo previousMaster;
    private HeartbeatMgr manager;

    @SuppressWarnings("unchecked")
    @BeforeEach
    void setUp() throws Exception {
        for (String name : List.of("enable_tls", "enable_fqdn_mode", "tls_excluded_protocols", "tls_verify_mode",
                "tls_certificate_path", "tls_private_key_path", "tls_ca_certificate_path",
                "tls_cert_refresh_interval_seconds", "thrift_rpc_connect_timeout_ms")) {
            Field field = Config.class.getField(name);
            previous.put(field, field.get(null));
        }
        originalPool = ClientPool.backendHeartbeatPool;
        runningUnitTest = FeConstants.runningUnitTest;
        Path script = Path.of(System.getProperty("user.dir"))
                .resolve("../../docker/thirdparties/docker-compose/iceberg/encryption/generate-tls-fixture.sh")
                .normalize();
        Process generation = new ProcessBuilder("bash", script.toString(), certificates.toString())
                .redirectErrorStream(true).redirectOutput(certificates.resolve("generation.log").toFile()).start();
        try {
            Assertions.assertTrue(generation.waitFor(60, TimeUnit.SECONDS));
            Assertions.assertEquals(0, generation.exitValue(),
                    Files.readString(certificates.resolve("generation.log")));
        } finally {
            if (generation.isAlive()) {
                generation.destroyForcibly();
                generation.waitFor();
            }
        }
        Config.enable_tls = true;
        Config.enable_fqdn_mode = true;
        Config.tls_excluded_protocols = "http,mysql,arrowflight,bdbje";
        Config.tls_verify_mode = "verify_fail_if_no_peer_cert";
        Config.tls_cert_refresh_interval_seconds = 0;
        Config.thrift_rpc_connect_timeout_ms = 3000;
        Config.tls_ca_certificate_path = certificates.resolve("ca.pem").toString();
        clientIdentity();
        GenericKeyedObjectPoolConfig config = new GenericKeyedObjectPoolConfig();
        config.setMaxIdlePerKey(1);
        ClientPool.backendHeartbeatPool = new GenericPool<>("HeartbeatService", config, 3000);
        Env env = Mockito.mock(Env.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(env.getDnsCache().get(Mockito.anyString())).thenAnswer(invocation -> resolved.get());
        Mockito.when(env.getTokenManager().acquireToken()).thenReturn("fixture-token");
        mockedEnv = Mockito.mockStatic(Env.class);
        mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
        Field masterField = HeartbeatMgr.class.getDeclaredField("masterInfo");
        masterField.setAccessible(true);
        master = (AtomicReference<TMasterInfo>) masterField.get(null);
        previousMaster = master.get();
        master.set(new TMasterInfo(new TNetworkAddress("localhost", 1), 1, 1));
        manager = new HeartbeatMgr(Mockito.mock(SystemInfoService.class), false);
        // Exercise the real RPC branch rather than the heartbeat manager's synthetic UT response.
        FeConstants.runningUnitTest = false;
    }

    @AfterEach
    void tearDown() throws Exception {
        try {
            if (manager != null) {
                manager.clearRemovedBackendHeartbeatPools();
                for (String name : List.of("executor", "abortTxnExecutor", "changelogTtlExecutor")) {
                    Field field = HeartbeatMgr.class.getDeclaredField(name);
                    field.setAccessible(true);
                    ExecutorService executor = (ExecutorService) field.get(manager);
                    executor.shutdownNow();
                    Assertions.assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
                }
            }
            Assertions.assertAll(peers.stream().map(peer -> (Executable) peer::close));
        } finally {
            if (master != null) {
                master.set(previousMaster);
            }
            if (mockedEnv != null) {
                mockedEnv.close();
            }
            ClientPool.backendHeartbeatPool = originalPool;
            FeConstants.runningUnitTest = runningUnitTest;
            for (Map.Entry<Field, Object> entry : previous.entrySet()) {
                entry.getKey().set(null, entry.getValue());
            }
        }
    }

    @Test
    void fqdnHeartbeatAcceptsADnsOnlyCertificateAndReusesItsConnection() throws Exception {
        Peer peer = peer("127.0.0.1", 0, "first");
        Backend backend = new Backend(101, "localhost", peer.port());
        Assertions.assertEquals("first", heartbeat(backend).getVersion());
        Assertions.assertEquals("first", heartbeat(backend).getVersion());
    }

    @Test
    void anotherNameAtTheSameAddressCannotReuseAnAuthenticatedConnection() throws Exception {
        Peer peer = peer("127.0.0.1", 0, "first");
        Assertions.assertEquals("first", heartbeat(new Backend(101, "localhost", peer.port())).getVersion());
        HeartbeatResponse rejected = manager.new BackendHeartbeatHandler(
                new Backend(102, "wrong.invalid", peer.port()), List.of()).call();
        Assertions.assertEquals(HbStatus.BAD, rejected.getStatus());
        Assertions.assertEquals("first", heartbeat(new Backend(101, "localhost", peer.port())).getVersion());
    }

    @Test
    void dnsTurnoverSwitchesTheAddressWhileTheOldServerRemainsHealthy() throws Exception {
        Peer first = peer("127.0.0.1", 0, "first");
        peer("127.0.0.2", first.port(), "second");
        Backend backend = new Backend(101, "localhost", first.port());
        Assertions.assertEquals("first", heartbeat(backend).getVersion());
        resolved.set("127.0.0.2");
        Assertions.assertEquals("second", heartbeat(backend).getVersion());
        resolved.set("127.0.0.1");
        Assertions.assertEquals("first", heartbeat(backend).getVersion());
    }

    private BackendHbResponse heartbeat(Backend backend) {
        HeartbeatResponse result = manager.new BackendHeartbeatHandler(backend, List.of()).call();
        Assertions.assertEquals(HbStatus.OK, result.getStatus(), result.getMsg());
        return (BackendHbResponse) result;
    }

    private void clientIdentity() {
        Config.tls_certificate_path = certificates.resolve("fe.pem").toString();
        Config.tls_private_key_path = certificates.resolve("fe.key").toString();
    }

    private Peer peer(String address, int port, String version) throws Exception {
        Config.tls_certificate_path = certificates.resolve("dns-fe.pem").toString();
        Config.tls_private_key_path = certificates.resolve("dns-fe.key").toString();
        SSLServerSocket listener;
        try {
            listener = PrivateRpcTls.thriftServerSocket(new InetSocketAddress(address, port), 16);
        } finally {
            clientIdentity();
        }
        TServerSocket transport = new TServerSocket(listener);
        HeartbeatService.Iface service = request -> new THeartbeatResult(new TStatus(TStatusCode.OK),
                new TBackendInfo(1, 2).setVersion(version));
        TThreadPoolServer server = new TThreadPoolServer(new TThreadPoolServer.Args(transport)
                .processor(new HeartbeatService.Processor<>(service)).minWorkerThreads(1).maxWorkerThreads(4));
        ExecutorService worker = Executors.newSingleThreadExecutor();
        Peer peer = new Peer(listener.getLocalPort(), transport, server, worker, worker.submit(server::serve));
        peers.add(peer);
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!server.isServing() && System.nanoTime() < deadline) {
            Thread.sleep(10);
        }
        Assertions.assertTrue(server.isServing());
        return peer;
    }

    private record Peer(int port, TServerSocket transport, TThreadPoolServer server,
            ExecutorService worker, Future<?> serving) implements AutoCloseable {
        @Override
        public void close() throws Exception {
            server.stop();
            transport.close();
            worker.shutdownNow();
            try {
                serving.get(5, TimeUnit.SECONDS);
            } finally {
                Assertions.assertTrue(worker.awaitTermination(5, TimeUnit.SECONDS));
            }
        }
    }
}
