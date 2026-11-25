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

package org.apache.doris.datasource.doris;

import org.apache.doris.common.ClientPool;
import org.apache.doris.common.Config;
import org.apache.doris.thrift.FrontendService;
import org.apache.doris.thrift.TGetBackendMetaRequest;
import org.apache.doris.thrift.TGetBackendMetaResult;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TStatus;
import org.apache.doris.thrift.TStatusCode;

import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

class FeServiceClientTest {
    @TempDir
    Path certificates;

    @Test
    void rejectsTimeoutThatCannotFitInSocketMilliseconds() {
        Assertions.assertThrows(ArithmeticException.class, () -> new FeServiceClient("remote_catalog",
                Collections.emptyList(), "test_user", "", 1, Integer.MAX_VALUE));
    }

    @Test
    void metadataReadTimeoutUsesSeconds() throws Exception {
        exerciseMetadataRead(false);
    }

    @Test
    void remoteCatalogRemainsPlaintextWhenLocalPrivateTlsIsEnabled() throws Exception {
        Map<Field, Object> previous = new LinkedHashMap<>();
        for (String name : new String[] {"enable_tls", "tls_excluded_protocols", "tls_verify_mode",
                "tls_certificate_path", "tls_private_key_path", "tls_ca_certificate_path",
                "tls_cert_refresh_interval_seconds", "thrift_rpc_connect_timeout_ms"}) {
            Field field = Config.class.getField(name);
            previous.put(field, field.get(null));
        }
        try {
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
            Config.tls_excluded_protocols = "http,mysql,arrowflight,bdbje";
            Config.tls_verify_mode = "verify_fail_if_no_peer_cert";
            Config.tls_certificate_path = certificates.resolve("fe.pem").toString();
            Config.tls_private_key_path = certificates.resolve("fe.key").toString();
            Config.tls_ca_certificate_path = certificates.resolve("ca.pem").toString();
            Config.tls_cert_refresh_interval_seconds = 0;
            Config.thrift_rpc_connect_timeout_ms = 500;
            exerciseMetadataRead(true);
        } finally {
            for (Map.Entry<Field, Object> entry : previous.entrySet()) {
                entry.getKey().set(null, entry.getValue());
            }
        }
    }

    private void exerciseMetadataRead(boolean privateTlsEnabled) throws Exception {
        FrontendService.Iface handler = Mockito.mock(FrontendService.Iface.class);
        Mockito.when(handler.getBackendMeta(Mockito.any())).thenAnswer(invocation -> {
            // Ordinary metadata latency must remain within the configured seconds-based timeout.
            Thread.sleep(100);
            return new TGetBackendMetaResult().setStatus(new TStatus(TStatusCode.OK))
                    .setBackends(Collections.emptyList());
        });
        try (ServerSocket listener = new ServerSocket(0, 50, InetAddress.getLoopbackAddress())) {
            TServerSocket transport = new TServerSocket(listener);
            TThreadPoolServer server = new TThreadPoolServer(new TThreadPoolServer.Args(transport)
                    .processor(new FrontendService.Processor<>(handler)).minWorkerThreads(1).maxWorkerThreads(4));
            ExecutorService executor = Executors.newSingleThreadExecutor();
            Future<?> serving = executor.submit(server::serve);
            TNetworkAddress address = new TNetworkAddress(listener.getInetAddress().getHostAddress(),
                    listener.getLocalPort());
            try {
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                while (!server.isServing() && System.nanoTime() < deadline) {
                    Thread.sleep(10);
                }
                Assertions.assertTrue(server.isServing());
                FeServiceClient client = new FeServiceClient("remote_catalog", Collections.singletonList(address),
                        "test_user", "", 1, 10);
                Assertions.assertTrue(client.listBackends().isEmpty());
                FrontendService.Client pooled = ClientPool.remoteFrontendPool.borrowObject(address, 3000);
                try {
                    Assertions.assertTrue(pooled.getBackendMeta(new TGetBackendMetaRequest()).getBackends().isEmpty());
                    Assertions.assertTrue(ClientPool.remoteFrontendPool.reopen(pooled, 3000));
                    Assertions.assertTrue(pooled.getBackendMeta(new TGetBackendMetaRequest()).getBackends().isEmpty());
                } finally {
                    ClientPool.remoteFrontendPool.returnObject(address, pooled);
                }
                if (privateTlsEnabled) {
                    // A warm remote socket at the same address must not satisfy an internal TLS borrow.
                    Assertions.assertThrows(TTransportException.class,
                            () -> ClientPool.frontendPool.borrowObject(address, 1000));
                    Assertions.assertTrue(client.listBackends().isEmpty());
                }
            } finally {
                ClientPool.remoteFrontendPool.clearPool(address);
                ClientPool.frontendPool.clearPool(address);
                server.stop();
                transport.close();
                executor.shutdownNow();
                try {
                    serving.get(10, TimeUnit.SECONDS);
                } finally {
                    Assertions.assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
                }
            }
        }
    }
}
