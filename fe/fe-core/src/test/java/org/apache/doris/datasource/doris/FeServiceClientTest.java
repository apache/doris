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
import org.apache.doris.thrift.FrontendService;
import org.apache.doris.thrift.TGetBackendMetaResult;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TStatus;
import org.apache.doris.thrift.TStatusCode;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.net.InetAddress;
import java.net.ServerSocket;
import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

class FeServiceClientTest {
    @Test
    void rejectsTimeoutThatCannotFitInSocketMilliseconds() {
        Assertions.assertThrows(ArithmeticException.class, () -> new FeServiceClient("remote_catalog",
                Collections.emptyList(), "test_user", "", 1, Integer.MAX_VALUE));
    }

    @Test
    void metadataReadTimeoutUsesSeconds() throws Exception {
        FrontendService.Iface handler = Mockito.mock(FrontendService.Iface.class);
        Mockito.when(handler.getBackendMeta(Mockito.any())).thenAnswer(invocation -> {
            // Ordinary metadata latency must remain within the configured seconds-based timeout.
            Thread.sleep(100);
            return new TGetBackendMetaResult().setStatus(new TStatus(TStatusCode.OK))
                    .setBackends(Collections.emptyList());
        });
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try (ServerSocket server = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            server.setSoTimeout(5000);
            Future<?> response = executor.submit(() -> {
                try (TSocket socket = new TSocket(server.accept())) {
                    TBinaryProtocol protocol = new TBinaryProtocol(socket);
                    new FrontendService.Processor<>(handler).process(protocol, protocol);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            TNetworkAddress address = new TNetworkAddress(server.getInetAddress().getHostAddress(),
                    server.getLocalPort());
            try {
                FeServiceClient client = new FeServiceClient("remote_catalog", Collections.singletonList(address),
                        "test_user", "", 1, 10);
                Assertions.assertTrue(client.listBackends().isEmpty());
                response.get(5, TimeUnit.SECONDS);
            } finally {
                ClientPool.frontendPool.clearPool(address);
            }
        } finally {
            executor.shutdownNow();
            Assertions.assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }
}
