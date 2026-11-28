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

package org.apache.doris.cdcclient.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.doris.cdcclient.common.Env;
import org.apache.doris.cdcclient.common.ReaderBatch;
import org.apache.doris.cdcclient.source.reader.SourceReader;
import org.apache.doris.cdcclient.source.reader.SplitReadResult;
import org.apache.doris.job.cdc.request.WriteRecordRequest;

import org.apache.flink.api.connector.source.SourceSplit;
import org.junit.jupiter.api.Test;

import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

class PipelineCoordinatorCancellationTest {
    @Test
    void cancellationBeforePreparationStillClosesTheReaderOpenedAfterDetach() throws Exception {
        Env env = Env.getCurrentEnv();
        PipelineCoordinator coordinator = new PipelineCoordinator(1);
        WriteRecordRequest request = new WriteRecordRequest();
        request.setJobId("cancel-before-reader-prepare");
        request.setTaskId("cancel-before-reader-prepare-task");
        request.setDataSource("POSTGRES");
        request.setMeta(Map.of("splitId", "binlog-split"));
        request.setConfig(Collections.emptyMap());
        request.setDorisUser("test_user");
        request.setTargetDb("test_db");
        request.setToken("test_token");
        AtomicBoolean opened = new AtomicBoolean();
        AtomicInteger releases = new AtomicInteger();
        CountDownLatch cleaned = new CountDownLatch(1);
        try {
            installReader(
                    env,
                    request,
                    (proxy, method, args) -> {
                        switch (method.getName()) {
                            case "prepareAndSubmitSplit":
                                coordinator.cancelTask(request.getJobId(), request.getTaskId());
                                env.detachReaderIfOwner(request.getJobId(), request.getTaskId())
                                        .release(request);
                                opened.set(true);
                                return new SplitReadResult();
                            case "release":
                                opened.set(false);
                                if (releases.incrementAndGet() == 2) {
                                    cleaned.countDown();
                                }
                                return null;
                            default:
                                throw new AssertionError(
                                        "Unexpected reader call: " + method.getName());
                        }
                    });
            coordinator.writeRecordsAsync(request);
            assertTrue(cleaned.await(5, TimeUnit.SECONDS));
            assertEquals(2, releases.get());
            assertFalse(opened.get());
            assertNull(env.getReaderIfPresent(request.getJobId()));
        } finally {
            coordinator.closeTaskScheduler();
            env.close(request.getJobId());
        }
    }

    @Test
    void failureAfterFirstSnapshotSplitDoesNotCommitOffset() throws Exception {
        Env env = Env.getCurrentEnv();
        PipelineCoordinator coordinator = new PipelineCoordinator(1);
        HttpServer frontend = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        AtomicInteger commits = new AtomicInteger();
        frontend.createContext(
                "/api/streaming/commit_offset",
                exchange -> {
                    commits.incrementAndGet();
                    byte[] reply = "{\"code\":0}".getBytes(StandardCharsets.UTF_8);
                    try {
                        exchange.sendResponseHeaders(200, reply.length);
                        exchange.getResponseBody().write(reply);
                    } finally {
                        exchange.close();
                    }
                });
        frontend.start();

        WriteRecordRequest request = new WriteRecordRequest();
        request.setJobId("909001");
        request.setTaskId("909002");
        request.setDataSource("POSTGRES");
        request.setMeta(Map.of("splits", List.of("first", "second")));
        request.setConfig(Map.of());
        request.setDorisUser("test_user");
        request.setTargetDb("test_db");
        request.setToken("test_token");
        request.setStreamLoadProps(Map.of());
        request.setFrontendAddress("127.0.0.1:" + frontend.getAddress().getPort());

        SplitReadResult result = new SplitReadResult();
        SourceSplit first = () -> "first";
        SourceSplit second = () -> "second";
        result.setSplits(List.of(first, second));
        IOException failure = new IOException("second split failed");
        AtomicInteger polls = new AtomicInteger();
        AtomicBoolean finished = new AtomicBoolean();
        try {
            installReader(
                    env,
                    request,
                    (proxy, method, args) ->
                            switch (method.getName()) {
                                case "prepareAndSubmitSplit" -> result;
                                case "isSnapshotSplit" -> true;
                                case "isSnapshotFinished" -> false;
                                case "pollRecords" -> {
                                    if (polls.incrementAndGet() == 1) {
                                        // The first split is empty but has a high watermark.
                                        result.setSplitStates(
                                                Map.of(first.splitId(), Map.of("lsn", "100")));
                                        yield ReaderBatch.wrap(Collections.emptyIterator());
                                    }
                                    throw failure;
                                }
                                case "commitSourceOffset" -> null;
                                case "finishSplitRecords" -> {
                                    finished.set(true);
                                    yield null;
                                }
                                default -> throw new AssertionError(
                                        "Unexpected reader call: " + method.getName());
                            });
            assertSame(
                    failure,
                    assertThrows(IOException.class, () -> coordinator.writeRecords(request)));
            assertEquals(2, polls.get());
            assertTrue(finished.get());
            assertEquals(0, commits.get());
        } finally {
            coordinator.closeTaskScheduler();
            coordinator.closeJobStreamLoad(request.getJobId());
            env.close(request.getJobId());
            frontend.stop(0);
        }
    }

    private static void installReader(
            Env env, WriteRecordRequest request, InvocationHandler handler) throws Exception {
        env.getReader(request);
        Field contexts = Env.class.getDeclaredField("jobContexts");
        contexts.setAccessible(true);
        Object context = ((Map<?, ?>) contexts.get(env)).get(request.getJobId());
        Field reader = context.getClass().getDeclaredField("reader");
        reader.setAccessible(true);
        reader.set(
                context,
                Proxy.newProxyInstance(
                        SourceReader.class.getClassLoader(),
                        new Class<?>[] {SourceReader.class},
                        handler));
    }
}
