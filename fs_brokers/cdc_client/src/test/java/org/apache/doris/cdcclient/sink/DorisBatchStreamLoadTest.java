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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.doris.cdcclient.exception.StreamLoadException;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

import com.sun.net.httpserver.HttpServer;

class DorisBatchStreamLoadTest {

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
