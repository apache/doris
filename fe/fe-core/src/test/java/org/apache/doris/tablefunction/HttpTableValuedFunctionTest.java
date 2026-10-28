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

package org.apache.doris.tablefunction;

import org.apache.doris.cloud.security.SecurityChecker;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.httpv2.rest.manager.HttpUtils;
import org.apache.doris.thrift.TBrokerFileStatus;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class HttpTableValuedFunctionTest {
    private SecurityChecker checker;
    private MockedStatic<SecurityChecker> security;
    private MockedStatic<HttpUtils> http;
    private List<String> events;

    @BeforeEach
    public void setUp() throws Exception {
        checker = Mockito.mock(SecurityChecker.class);
        security = Mockito.mockStatic(SecurityChecker.class);
        security.when(SecurityChecker::getInstance).thenReturn(checker);
        http = Mockito.mockStatic(HttpUtils.class);
        events = new ArrayList<>();
        Mockito.doAnswer(invocation -> {
            events.add("start:" + invocation.getArgument(0));
            return null;
        }).when(checker).startSSRFChecking(Mockito.anyString());
        Mockito.doAnswer(invocation -> {
            events.add("stop");
            return null;
        }).when(checker).stopSSRFChecking();
    }

    @AfterEach
    public void tearDown() {
        http.close();
        security.close();
    }

    @ParameterizedTest
    @ValueSource(strings = {"http://example.com/data.csv", "https://example.com/data.csv"})
    public void testHeadRequestRunsInsideSecurityCheck(String uri) throws Exception {
        mockHeadRequests();
        HttpTableValuedFunction tvf = new HttpTableValuedFunction(Map.of(
                "uri", uri, "format", "csv", "http.header.Authorization", "Bearer token"));

        Assertions.assertEquals(List.of("start:" + uri, "head:" + uri, "stop"), events);
        Assertions.assertEquals(List.of(new TBrokerFileStatus(uri, false, 123L, true)), tvf.getFileStatuses());
        http.verify(() -> HttpUtils.getHttpFileSize(uri, Map.of("Authorization", "Bearer token")));
    }

    @Test
    public void testRejectedUrlDoesNotSendHeadRequest() throws Exception {
        String uri = "http://127.0.0.1/data.csv";
        IOException blocked = new IOException("URL blocked");
        Mockito.doThrow(blocked).when(checker).startSSRFChecking(uri);

        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> new HttpTableValuedFunction(Map.of("uri", uri, "format", "csv")));

        Assertions.assertSame(blocked, exception.getCause());
        http.verifyNoInteractions();
        Mockito.verify(checker).stopSSRFChecking();
    }

    @Test
    public void testHeadFailureStopsSecurityCheck() {
        String uri = "https://example.com/data.csv";
        IOException failure = new IOException("HEAD failed");
        http.when(() -> HttpUtils.getHttpFileSize(uri, Map.of())).thenThrow(failure);

        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> new HttpTableValuedFunction(Map.of("uri", uri, "format", "csv")));

        Assertions.assertSame(failure, exception.getCause());
        Assertions.assertEquals(List.of("start:" + uri, "stop"), events);
    }

    @Test
    public void testHuggingFaceChecksEachExpandedHttpUrl() throws Exception {
        mockHeadRequests();
        String uri = "hf://datasets/user/repo/*.csv";
        String first = "https://huggingface.co/datasets/user/repo/resolve/main/a.csv";
        String second = "https://huggingface.co/datasets/user/repo/resolve/main/b.csv";
        try (MockedStatic<HFUtils> hf = Mockito.mockStatic(HFUtils.class)) {
            hf.when(() -> HFUtils.expandGlob(uri)).thenReturn(List.of(first, second));

            HttpTableValuedFunction tvf = new HttpTableValuedFunction(Map.of("uri", uri, "format", "csv"));

            Assertions.assertEquals(List.of("start:" + first, "head:" + first, "stop",
                    "start:" + second, "head:" + second, "stop"), events);
            Assertions.assertEquals(List.of(new TBrokerFileStatus(first, false, 123L, true),
                    new TBrokerFileStatus(second, false, 123L, true)), tvf.getFileStatuses());
        }
    }

    private void mockHeadRequests() {
        http.when(() -> HttpUtils.getHttpFileSize(Mockito.anyString(), Mockito.anyMap()))
                .thenAnswer(invocation -> {
                    events.add("head:" + invocation.getArgument(0));
                    return 123L;
                });
    }
}
