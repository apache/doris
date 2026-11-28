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

package org.apache.doris.cdcclient.sink;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.doris.cdcclient.common.CdcResourceManager;
import org.apache.doris.cdcclient.common.Env;
import org.apache.doris.cdcclient.exception.StreamLoadException;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicInteger;

import com.sun.net.httpserver.HttpServer;

@Isolated
class DorisBatchStreamLoadTest {

    @Test
    void crossingProcessThresholdFlushesCurrentJobAndCountsRetries() throws Exception {
        Env env = Env.getCurrentEnv();
        CdcResourceManager resources = env.getCdcResourceManager();
        long initialBytes = resources.getSinkByteLimit() - 5;
        List<String> bodies = new CopyOnWriteArrayList<>();
        CountDownLatch retry = new CountDownLatch(1);
        CountDownLatch respond = new CountDownLatch(1);
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/api/db/b/_stream_load", exchange -> {
            try {
                bodies.add(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
                if (bodies.size() == 1) {
                    exchange.sendResponseHeaders(503, -1);
                    return;
                }
                retry.countDown();
                if (!respond.await(15, SECONDS)) {
                    throw new IOException("Timed out waiting for response");
                }
                byte[] response = ("{\"Status\":\"Success\",\"NumberLoadedRows\":1,"
                        + "\"NumberFilteredRows\":0,\"LoadBytes\":3}").getBytes(StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(200, response.length);
                exchange.getResponseBody().write(response);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                exchange.close();
            }
        });
        server.start();
        int previousPort = URI.create("http://" + env.getBackendHostPort()).getPort();
        env.setBackendHttpPort(server.getAddress().getPort());
        DorisBatchStreamLoad first = new DorisBatchStreamLoad("1", "db");
        DorisBatchStreamLoad second = new DorisBatchStreamLoad("2", "db");
        resources.addSinkBufferedBytes(initialBytes);
        try {
            first.writeRecord("db", "a", new byte[2]);
            first.writeRecord("db", "a", new byte[2]);
            assertEquals(initialBytes + 5, resources.getSinkBufferedBytes());
            second.setToken("token");
            CompletableFuture<Void> write = CompletableFuture.runAsync(
                    () -> second.writeRecord("db", "b", "abc".getBytes(StandardCharsets.UTF_8)));
            assertTrue(retry.await(10, SECONDS));
            assertEquals(List.of("abc", "abc"), bodies);
            assertEquals(initialBytes + 8, resources.getSinkBufferedBytes());
            assertFalse(write.isDone());
            respond.countDown();
            write.get(5, SECONDS);
            assertEquals(initialBytes + 5, resources.getSinkBufferedBytes());
        } finally {
            respond.countDown();
            first.close();
            first.close();
            second.close();
            resources.addSinkBufferedBytes(-initialBytes);
            env.setBackendHttpPort(previousPort);
            server.stop(0);
        }
        assertEquals(0, resources.getSinkBufferedBytes());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void closeReleasesPendingBatchesButRetainsInFlightBytes(boolean interruptProducer) throws Exception {
        Env env = Env.getCurrentEnv();
        CdcResourceManager resources = env.getCdcResourceManager();
        CountDownLatch received = new CountDownLatch(1);
        CountDownLatch respond = new CountDownLatch(1);
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            try {
                exchange.getRequestBody().readAllBytes();
                received.countDown();
                if (!respond.await(15, SECONDS)) {
                    throw new IOException("Timed out waiting for response");
                }
                byte[] response = ("{\"Status\":\"Success\",\"NumberLoadedRows\":1,"
                        + "\"NumberFilteredRows\":0,\"LoadBytes\":4}").getBytes(StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(200, response.length);
                exchange.getResponseBody().write(response);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                exchange.close();
            }
        });
        server.start();
        int previousPort = URI.create("http://" + env.getBackendHostPort()).getPort();
        env.setBackendHttpPort(server.getAddress().getPort());
        DorisBatchStreamLoad loader = new DorisBatchStreamLoad("3", "db");
        try {
            loader.setToken("token");
            loader.writeRecord("db", "a", new byte[4]);
            loader.bufferFullFlush("db.a");
            assertTrue(received.await(5, SECONDS));
            loader.writeRecord("db", "b", new byte[3]);
            loader.bufferFullFlush("db.b");
            loader.writeRecord("db", "c", new byte[2]);
            FutureTask<Void> flush = new FutureTask<>(loader::forceFlush, null);
            Thread producer = new Thread(flush);
            producer.start();
            // The worker awaits the HTTP response and the one-slot queue holds db.b.
            await().atMost(5, SECONDS).until(() -> producer.getState() == Thread.State.WAITING);
            loader.writeRecord("db", "d", new byte[1]);
            assertEquals(10, resources.getSinkBufferedBytes());
            if (interruptProducer) {
                producer.interrupt();
                assertThrows(ExecutionException.class, () -> flush.get(5, SECONDS));
                assertEquals(8, resources.getSinkBufferedBytes());
            }
            loader.close();
            loader.close();
            assertThrows(ExecutionException.class, () -> flush.get(5, SECONDS));
            assertEquals(4, resources.getSinkBufferedBytes());
            respond.countDown();
            await().atMost(5, SECONDS).until(() -> resources.getSinkBufferedBytes() == 0);
        } finally {
            respond.countDown();
            loader.close();
            env.setBackendHttpPort(previousPort);
            server.stop(0);
        }
    }

    @Test
    void failedLoadReleasesQueuedAndInFlightRecords() throws Exception {
        Env env = Env.getCurrentEnv();
        CdcResourceManager resources = env.getCdcResourceManager();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        AtomicInteger requests = new AtomicInteger();
        server.createContext("/", exchange -> {
            try {
                exchange.getRequestBody().readAllBytes();
                requests.incrementAndGet();
                exchange.sendResponseHeaders(503, -1);
            } finally {
                exchange.close();
            }
        });
        server.start();
        int previousPort = URI.create("http://" + env.getBackendHostPort()).getPort();
        env.setBackendHttpPort(server.getAddress().getPort());
        DorisBatchStreamLoad loader = new DorisBatchStreamLoad("4", "db");
        try {
            loader.setToken("token");
            loader.writeRecord("db", "a", new byte[2]);
            loader.writeRecord("db", "b", new byte[3]);
            assertThrows(StreamLoadException.class, loader::forceFlush);
            assertEquals(4, requests.get());
            await().atMost(5, SECONDS).until(() -> resources.getSinkBufferedBytes() == 0);
        } finally {
            loader.close();
            env.setBackendHttpPort(previousPort);
            server.stop(0);
        }
    }

    @ParameterizedTest
    @CsvSource(
            delimiter = '|',
            value = {
                "{\"code\":1,\"msg\":\"Error\",\"data\":\"Offset commit rejected\"}|Offset commit rejected",
                "{\"code\":1,\"msg\":\"Offset commit rejected\",\"data\":null}|Offset commit rejected",
                "{\"code\":1,\"msg\":\"\",\"data\":null}|HTTP/1.1 200 OK"
            })
    void commitOffsetPreservesFailureReasonAfterRetries(String response, String expectedReason)
            throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        AtomicInteger requests = new AtomicInteger();
        server.createContext(
                "/api/streaming/commit_offset",
                exchange -> {
                    try {
                        exchange.getRequestBody().readAllBytes();
                        requests.incrementAndGet();
                        byte[] body = response.getBytes(StandardCharsets.UTF_8);
                        exchange.sendResponseHeaders(200, body.length);
                        exchange.getResponseBody().write(body);
                    } finally {
                        exchange.close();
                    }
                });
        server.start();
        try {
            DorisBatchStreamLoad streamLoad = new DorisBatchStreamLoad("1", "test_db");
            try {
                streamLoad.setFrontendAddress("127.0.0.1:" + server.getAddress().getPort());
                streamLoad.setToken("test-token");
                StreamLoadException error = assertThrows(
                        StreamLoadException.class,
                        () -> streamLoad.commitOffset(
                                "2", Collections.emptyList(), 0, new LoadStatistic(), null));
                assertEquals(
                        "StreamLoadException: commit offset failed with: " + expectedReason,
                        ExceptionUtils.getRootCauseMessage(error));
                assertEquals(4, requests.get());
            } finally {
                streamLoad.close();
            }
        } finally {
            server.stop(0);
        }
    }
}
