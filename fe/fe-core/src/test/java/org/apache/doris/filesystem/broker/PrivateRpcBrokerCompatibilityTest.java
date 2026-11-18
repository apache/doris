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

package org.apache.doris.filesystem.broker;

import org.apache.doris.common.ClientPool;
import org.apache.doris.common.Config;
import org.apache.doris.thrift.TBrokerCheckPathExistRequest;
import org.apache.doris.thrift.TBrokerCheckPathExistResponse;
import org.apache.doris.thrift.TBrokerOperationStatus;
import org.apache.doris.thrift.TBrokerOperationStatusCode;
import org.apache.doris.thrift.TBrokerPingBrokerRequest;
import org.apache.doris.thrift.TBrokerVersion;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TPaloBrokerService;

import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
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

class PrivateRpcBrokerCompatibilityTest {
    @TempDir
    Path certificates;

    @Test
    void heartbeatAndFilesystemKeepTheSamePlaintextBrokerPolicy() throws Exception {
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
            if (!generation.waitFor(60, TimeUnit.SECONDS)) {
                generation.descendants().forEach(ProcessHandle::destroyForcibly);
                generation.destroyForcibly();
                Assertions.fail("Ephemeral certificate generation timed out");
            }
            // TempDir cleanup removes the generator log even when setup fails in CI.
            Assertions.assertEquals(0, generation.exitValue(),
                    Files.readString(certificates.resolve("generation.log")));
            Config.enable_tls = true;
            Config.tls_excluded_protocols = "http,mysql,arrowflight,bdbje";
            Config.tls_verify_mode = "verify_fail_if_no_peer_cert";
            Config.tls_certificate_path = certificates.resolve("fe.pem").toString();
            Config.tls_private_key_path = certificates.resolve("fe.key").toString();
            Config.tls_ca_certificate_path = certificates.resolve("ca.pem").toString();
            Config.tls_cert_refresh_interval_seconds = 0;
            Config.thrift_rpc_connect_timeout_ms = 500;
            exerciseBroker();
        } finally {
            for (Map.Entry<Field, Object> entry : previous.entrySet()) {
                entry.getKey().set(null, entry.getValue());
            }
        }
    }

    private void exerciseBroker() throws Exception {
        TPaloBrokerService.Iface handler = (TPaloBrokerService.Iface) Proxy.newProxyInstance(
                TPaloBrokerService.Iface.class.getClassLoader(), new Class<?>[] {TPaloBrokerService.Iface.class},
                (proxy, method, args) -> {
                    switch (method.getName()) {
                        case "ping":
                            return new TBrokerOperationStatus(TBrokerOperationStatusCode.OK);
                        case "checkPathExist":
                            return new TBrokerCheckPathExistResponse()
                                    .setOpStatus(new TBrokerOperationStatus(TBrokerOperationStatusCode.OK))
                                    .setIsPathExist(true);
                        default:
                            throw new UnsupportedOperationException(method.getName());
                    }
                });
        try (ServerSocket listener = new ServerSocket(0, 50, InetAddress.getByName("127.0.0.1"))) {
            TServerSocket transport = new TServerSocket(listener);
            TThreadPoolServer server = new TThreadPoolServer(new TThreadPoolServer.Args(transport)
                    .processor(new TPaloBrokerService.Processor<>(handler)).minWorkerThreads(1).maxWorkerThreads(4));
            ExecutorService worker = Executors.newSingleThreadExecutor();
            Future<?> serving = worker.submit(server::serve);
            TNetworkAddress address = new TNetworkAddress("127.0.0.1", listener.getLocalPort());
            try {
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                while (!server.isServing() && System.nanoTime() < deadline) {
                    Thread.sleep(10);
                }
                Assertions.assertTrue(server.isServing());
                TPaloBrokerService.Client heartbeat = Assertions.assertDoesNotThrow(
                        () -> ClientPool.brokerPool.borrowObject(address, 3000));
                try {
                    TBrokerPingBrokerRequest ping = new TBrokerPingBrokerRequest()
                            .setVersion(TBrokerVersion.VERSION_ONE).setClientId("fixture-heartbeat");
                    Assertions.assertEquals(TBrokerOperationStatusCode.OK, heartbeat.ping(ping).getStatusCode());
                    Assertions.assertTrue(ClientPool.brokerPool.reopen(heartbeat, 3000));
                    Assertions.assertEquals(TBrokerOperationStatusCode.OK, heartbeat.ping(ping).getStatusCode());
                } finally {
                    ClientPool.brokerPool.invalidateObject(address, heartbeat);
                }
                BrokerClientFactory factory = new BrokerClientFactory();
                TPaloBrokerService.Client filesystem = factory.create(address);
                try {
                    Assertions.assertTrue(factory.validateObject(address, factory.wrap(filesystem)));
                    Assertions.assertTrue(filesystem.checkPathExist(new TBrokerCheckPathExistRequest()
                            .setVersion(TBrokerVersion.VERSION_ONE).setPath("fixture-object")
                            .setProperties(Collections.emptyMap())).isIsPathExist());
                } finally {
                    factory.destroyObject(address, factory.wrap(filesystem));
                }
            } finally {
                ClientPool.brokerPool.clearPool(address);
                server.stop();
                worker.shutdownNow();
                Assertions.assertTrue(worker.awaitTermination(10, TimeUnit.SECONDS));
                serving.get(10, TimeUnit.SECONDS);
            }
        }
    }
}
